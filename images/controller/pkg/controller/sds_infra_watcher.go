/*
Copyright 2025 Flant JSC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/controller/config"
	"github.com/deckhouse/sds-node-configurator/images/controller/internal"
	"github.com/deckhouse/sds-node-configurator/images/controller/pkg/monitoring"
)

const (
	SdsInfraWatcherCtrlName = "sds-infrastructure-watcher-controller"

	nodeReadyType  = internal.TypeNodeReady
	agentReadyType = internal.TypeAgentReady
)

var (
	sdsNodeConfiguratorSelector = map[string]string{"app": "sds-node-configurator"}
)

func RunSdsInfraWatcher(
	mgr manager.Manager,
	cfg config.Options,
	metrics monitoring.Metrics,
	log logger.Logger,
) error {
	log = log.Named(SdsInfraWatcherCtrlName)
	log.Info("registering runnable with the manager")

	return mgr.Add(manager.RunnableFunc(func(ctx context.Context) error {
		log := log

		log.Info("starts the work")
		cl := mgr.GetClient()

		for {
			select {
			case <-ctx.Done():
				log.Info("context cancelled, stopping")
				return nil
			case <-time.After(cfg.ScanInterval):
			}
			log.Info("starts the reconciliation loop")

			log.Debug("tries to get LVMVolumeGroups")
			lvgs, err := GetLVMVolumeGroups(ctx, cl, metrics)
			if err != nil {
				log.Error("unable to get LVMVolumeGroups", logger.Err(err))
				continue
			}
			log.Debug("successfully got LVMVolumeGroups")
			if len(lvgs) == 0 {
				log.Info("no LVMVolumeGroups found")
				continue
			}
			for _, lvg := range lvgs {
				log.Trace(fmt.Sprintf("LVMVolumeGroup %s conditions: %+v", lvg.Name, lvg.Status.Conditions))
			}

			log.Info("LVMVolumeGroups found. Starts to check their health")
			log.Info("check if every LVMVolumeGroup node does exist")
			lvgNodeNames := getNodeNamesFromLVGs(lvgs)
			log.Trace(fmt.Sprintf("used nodes %v", lvgNodeNames))

			log.Debug("tries to collect nodes used by LVMVolumeGroups")
			usedNodes, missedNodes, err := getNodesByNames(ctx, cl, lvgNodeNames)
			if err != nil {
				log.Error("unable to get nodes", logger.Err(err))
				continue
			}
			log.Debug("successfully collected nodes used by LVMVolumeGroups")

			if len(missedNodes) > 0 {
				log.Warn(fmt.Sprintf("some LVMVolumeGroups use missing nodes: %v. Turn those LVMVolumeGroups condition NodeReady to False", missedNodes))
				lvgsWithMissedNodes := findLVMVolumeGroupsByNodeNames(lvgs, missedNodes)
				for _, lvg := range lvgsWithMissedNodes {
					err = updateLVGConditionIfNeeded(ctx, cl, log, &lvg, metav1.ConditionFalse, nodeReadyType, "MissingNode", "unable to find the used nodes")
					if err != nil {
						log.Error(fmt.Sprintf("unable to add a condition to the LVMVolumeGroup %s", lvg.Name), logger.Err(err))
						continue
					}

					log.Info(fmt.Sprintf("successfully reconciled the LVMVolumeGroup %s phase and condition %s due to missing nodes", lvg.Name, nodeReadyType))
				}
			} else {
				log.Info("no missing nodes used by LVMVolumeGroups were found")
			}

			log.Debug("check if every used node is Ready")
			notReadyNodes := getNotReadyNodes(usedNodes)
			if len(notReadyNodes) > 0 {
				log.Warn(fmt.Sprintf("some LVMVolumeGroup nodes are not Ready: %v. Turn those LVMVolumeGroups condition NodeReady to False", notReadyNodes))
				lvgsNotReady := findLVMVolumeGroupsByNodeNames(lvgs, notReadyNodes)
				for _, lvg := range lvgsNotReady {
					err = updateLVGConditionIfNeeded(ctx, cl, log, &lvg, metav1.ConditionFalse, nodeReadyType, "NodeNotReady", "some of used nodes is not ready")
					if err != nil {
						log.Error(fmt.Sprintf("unable to add a condition to the LVMVolumeGroup %s", lvg.Name), logger.Err(err))
						continue
					}

					log.Info(fmt.Sprintf("successfully reconciled the LVMVolumeGroup %s phase and condition %s due to nodes are not in a Running state", lvg.Name, nodeReadyType))
				}
			} else {
				log.Info("every LVMVolumeGroup node is in a Running state")
			}

			// Create a list of LVGs that are not in notReadyNodes and missedNodes
			allProblemNodes := make(map[string]struct{}, len(missedNodes)+len(notReadyNodes))
			for _, node := range missedNodes {
				allProblemNodes[node] = struct{}{}
			}
			for _, node := range notReadyNodes {
				allProblemNodes[node] = struct{}{}
			}

			// Find LVGs that should be updated to True status
			lvgsToUpdate := make([]v1alpha1.LVMVolumeGroup, 0, len(lvgs))
			for _, lvg := range lvgs {
				shouldUpdate := true
				multipleNodes := false

				if len(lvg.Status.Nodes) > 1 {
					firstName := lvg.Status.Nodes[0].Name
					for _, node := range lvg.Status.Nodes[1:] {
						if node.Name != firstName {
							errMsg := fmt.Sprintf("[RunSdsInfraWatcher] found different node names in lvg.Status.Nodes for %s: %+v", lvg.Name, lvg.Status.Nodes)
							log.Error(errMsg, logger.Err(errors.New(errMsg)))
							multipleNodes = true
						}
					}
				}

				if multipleNodes {
					continue
				}

				for _, node := range lvg.Status.Nodes {
					if _, found := allProblemNodes[node.Name]; found {
						shouldUpdate = false
						break
					}
				}
				if shouldUpdate {
					lvgsToUpdate = append(lvgsToUpdate, lvg)
				}
			}

			// Update status for LVGs that are not in problem nodes
			for _, lvg := range lvgsToUpdate {
				log.Debug(fmt.Sprintf("tries to update the LVMVolumeGroup %s condition %s to True", lvg.Name, nodeReadyType))
				err = updateLVGConditionIfNeeded(ctx, cl, log, &lvg, metav1.ConditionTrue, nodeReadyType, "NodesReady", "selected nodes were found in the cluster and have Ready state")
				if err != nil {
					log.Error(fmt.Sprintf("unable to add a condition to the LVMVolumeGroup %s", lvg.Name), logger.Err(err))
					continue
				}
				log.Info(fmt.Sprintf("successfully reconciled the LVMVolumeGroup %s condition %s to status True", lvg.Name, nodeReadyType))
			}

			log.Info("check if every sds-node-configurator agent's pod is healthy")
			log.Debug(fmt.Sprintf("tries to get pods by the selector %v", sdsNodeConfiguratorSelector))
			sdsPods, err := getPodsBySelector(ctx, cl, cfg.AgentNamespace, sdsNodeConfiguratorSelector)
			if err != nil {
				log.Error(fmt.Sprintf("unable to get pods by the seletor %v", sdsNodeConfiguratorSelector), logger.Err(err))
				continue
			}
			log.Debug(fmt.Sprintf("successfully got pods by the selector %v", sdsNodeConfiguratorSelector))

			if len(sdsPods) == 0 {
				log.Warn("no sds-node-configurator agent's pods found, update every LVMVolumeGroup condition AgentReady to False")

				for _, lvg := range lvgs {
					log.Debug(fmt.Sprintf("tries to update the LVMVolumeGroup %s condition %s to status False due to a missing agent's pod", lvg.Name, agentReadyType))
					err = updateLVGConditionIfNeeded(ctx, cl, log, &lvg, metav1.ConditionFalse, agentReadyType, "NoPod", "unable to find any agent's pod")
					if err != nil {
						log.Error(fmt.Sprintf("unable to add a condition %s to the LVMVolumeGroup %s", agentReadyType, lvg.Name), logger.Err(err))
						continue
					}

					log.Info(fmt.Sprintf("successfully reconciled the LVMVolumeGroup %s phase and condition %s due to missing pods", lvg.Name, agentReadyType))
				}

				log.Info("successfully updated every LVMVolumeGroup status.phase to NotReady due to no sds-node-configurator agent's pods are running")
				continue
			}

			log.Debug("sds-node-configurator agent's pods were found. Check if some pods are missing")
			for _, p := range sdsPods {
				log.Trace(fmt.Sprintf("found a pod: %s", p.Name))
			}

			unmanagedNodes := getNodeNamesWithoutAgent(usedNodes, sdsPods)
			if len(unmanagedNodes) > 0 {
				log.Warn("some LVMVolumeGroups are not managed due to corresponding sds-node-configurator agent's pods are not running. Turn such LVMVolumeGroups to NotReady phase")
				log.Trace(fmt.Sprintf("nodes without the agent: %v", unmanagedNodes))
				lvgsWithoutPod := findLVMVolumeGroupsByNodeNames(lvgs, unmanagedNodes)
				for _, lvg := range lvgsWithoutPod {
					log.Debug(fmt.Sprintf("tries to add a condition %s status False due the LVMVolumeGroup %s node is not managed by a sds-node-configurator agent's pod", agentReadyType, lvg.Name))
					err = updateLVGConditionIfNeeded(ctx, cl, log, &lvg, metav1.ConditionFalse, agentReadyType, "PodNotFound", "unable to find an agent's pod")
					if err != nil {
						log.Error(fmt.Sprintf("unable to add a condition to the LVMVolumeGroup %s", lvg.Name), logger.Err(err))
						continue
					}

					log.Info(fmt.Sprintf("successfully reconciled the LVMVolumeGroup %s condition %s due to missing pods", lvg.Name, agentReadyType))
				}
			} else {
				log.Info("no missing sds-node-configurator agent's pods were found")
			}

			log.Debug("check if every agent's pod is in a Ready state")
			notReadyPods := getNotReadyPods(sdsPods)
			if len(notReadyPods) > 0 {
				podsNames := make([]string, 0, len(notReadyPods))
				for name := range notReadyPods {
					podsNames = append(podsNames, name)
				}

				log.Warn(fmt.Sprintf("there is some sds-node-configurator agent's pods that is not Ready, pods: %s. Turn the LVMVolumeGroups condition AgentReady to False", strings.Join(podsNames, ",")))
				nodeNames := getNodeNamesFromPods(notReadyPods)
				log.Trace(fmt.Sprintf("node names with not Ready sds-node-configurator agent's pods: %v", nodeNames))
				lvgsNotReady := findLVMVolumeGroupsByNodeNames(lvgs, nodeNames)
				for _, lvg := range lvgsNotReady {
					log.Warn(fmt.Sprintf("the LVMVolumeGroup %s is managed by not Ready pod, turns the condition %s to False", lvg.Name, agentReadyType))
					err = updateLVGConditionIfNeeded(ctx, cl, log, &lvg, metav1.ConditionFalse, agentReadyType, "PodNotReady", "the pod is not Ready")
					if err != nil {
						log.Error(fmt.Sprintf("unable to add a condition to the LVMVolumeGroup %s", lvg.Name), logger.Err(err))
						continue
					}

					log.Info(fmt.Sprintf("successfully reconciled the LVMVolumeGroup %s phase and condition %s due to the pod is not Ready", lvg.Name, agentReadyType))
				}
			}

			// Find active pods (not in notReadyPods)
			activePods := make(map[string]v1.Pod, len(sdsPods))
			for _, pod := range sdsPods {
				if _, exists := notReadyPods[pod.Name]; !exists {
					activePods[pod.Name] = pod
				}
			}

			// Find LVGs that should be updated to True status (have active pods)
			lvgsWithActivePods := make([]v1alpha1.LVMVolumeGroup, 0, len(lvgs))
			for _, lvg := range lvgs {
				shouldUpdate := false
				multipleNodes := false

				if len(lvg.Status.Nodes) > 1 {
					firstName := lvg.Status.Nodes[0].Name
					for _, node := range lvg.Status.Nodes[1:] {
						if node.Name != firstName {
							errMsg := fmt.Sprintf("[RunSdsInfraWatcher] found different node names in lvg.Status.Nodes for %s: %+v", lvg.Name, lvg.Status.Nodes)
							log.Error(errMsg, logger.Err(errors.New(errMsg)))
							multipleNodes = true
						}
					}
				}

				if multipleNodes {
					continue
				}

				for _, node := range lvg.Status.Nodes {
					for _, activePod := range activePods {
						if activePod.Spec.NodeName == node.Name {
							shouldUpdate = true
							break
						}
					}
					if shouldUpdate {
						break
					}
				}
				if shouldUpdate {
					lvgsWithActivePods = append(lvgsWithActivePods, lvg)
				}
			}

			// Update status for LVGs that have active pods
			if len(lvgsWithActivePods) > 0 {
				log.Info("found LVGs with active sds-node-configurator agent's pods")
				for _, lvg := range lvgsWithActivePods {
					err = updateLVGConditionIfNeeded(ctx, cl, log, &lvg, metav1.ConditionTrue, agentReadyType, "PodReady", "pod is ready to manage the resource")
					if err != nil {
						log.Error(fmt.Sprintf("unable to add a condition to the LVMVolumeGroup %s", lvg.Name), logger.Err(err))
						continue
					}
					log.Info(fmt.Sprintf("successfully reconciled the LVMVolumeGroup %s", lvg.Name))
				}
			}
		}
	}))
}
