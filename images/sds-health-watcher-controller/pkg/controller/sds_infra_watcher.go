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
	"fmt"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/sds-health-watcher-controller/config"
	"github.com/deckhouse/sds-node-configurator/images/sds-health-watcher-controller/pkg/logger"
	"github.com/deckhouse/sds-node-configurator/images/sds-health-watcher-controller/pkg/monitoring"
)

const (
	SdsInfraWatcherCtrlName = "sds-infrastructure-watcher-controller"

	nodeReadyType  = "NodeReady"
	agentReadyType = "AgentReady"
)

var (
	sdsNodeConfiguratorSelector = map[string]string{"app": "sds-node-configurator"}
)

func RunSdsInfraWatcher(
	ctx context.Context,
	mgr manager.Manager,
	cfg config.Options,
	metrics monitoring.Metrics,
	log logger.Logger,
) {
	log.Info("[RunSdsInfraWatcher] starts the work")
	cl := mgr.GetClient()

	go func() {
		for {
			time.Sleep(cfg.ScanIntervalSec)
			log.Info("[RunSdsInfraWatcher] starts the reconciliation loop")

			log.Debug("[RunSdsInfraWatcher] tries to get LVMVolumeGroups")
			lvgs, err := GetLVMVolumeGroups(ctx, cl, metrics)
			if err != nil {
				log.Error(err, "[RunSdsInfraWatcher] unable to get LVMVolumeGroups")
				continue
			}
			log.Debug("[RunSdsInfraWatcher] successfully got LVMVolumeGroups")
			if len(lvgs) == 0 {
				log.Info("[RunSdsInfraWatcher] no LVMVolumeGroups found")
				continue
			}
			for _, lvg := range lvgs {
				log.Trace(fmt.Sprintf("[RunSdsInfraWatcher] LVMVolumeGroup %s conditions: %+v", lvg.Name, lvg.Status.Conditions))
			}

			log.Info("[RunSdsInfraWatcher] LVMVolumeGroups found. Starts to check their health")
			log.Info("[RunSdsInfraWatcher] check if every LVMVolumeGroup node does exist")
			lvgNodeNames := getNodeNamesFromLVGs(lvgs)
			log.Trace(fmt.Sprintf("[RunSdsInfraWatcher] used nodes %v", lvgNodeNames))

			log.Debug("[RunSdsInfraWatcher] tries to collect nodes used by LVMVolumeGroups")
			usedNodes, missedNodes, err := getNodesByNames(ctx, cl, lvgNodeNames)
			if err != nil {
				log.Error(err, "[RunSdsInfraWatcher] unable to get nodes")
				continue
			}
			log.Debug("[RunSdsInfraWatcher] successfully collected nodes used by LVMVolumeGroups")

			if len(missedNodes) > 0 {
				log.Warning(fmt.Sprintf("[RunSdsInfraWatcher] some LVMVolumeGroups use missing nodes: %v. Turn those LVMVolumeGroups condition NodeReady to False", missedNodes))
				lvgsWithMissedNodes := findLVMVolumeGroupsByNodeNames(lvgs, missedNodes)
				for _, lvg := range lvgsWithMissedNodes {
					err = updateLVGConditionIfNeeded(ctx, cl, log, &lvg, metav1.ConditionFalse, nodeReadyType, "MissingNode", "unable to find the used nodes")
					if err != nil {
						log.Error(err, fmt.Sprintf("[RunSdsInfraWatcher] unable to add a condition to the LVMVolumeGroup %s", lvg.Name))
						continue
					}

					log.Info(fmt.Sprintf("[RunSdsInfraWatcher] successfully reconciled the LVMVolumeGroup %s phase and condition %s due to missing nodes", lvg.Name, nodeReadyType))
				}
			} else {
				log.Info("[RunSdsInfraWatcher] no missing nodes used by LVMVolumeGroups were found")
			}

			log.Debug("[RunSdsInfraWatcher] check if every used node is Ready")
			notReadyNodes := getNotReadyNodes(usedNodes)
			if len(notReadyNodes) > 0 {
				log.Warning(fmt.Sprintf("[RunSdsInfraWatcher] some LVMVolumeGroup nodes are not Ready: %v. Turn those LVMVolumeGroups condition NodeReady to False", notReadyNodes))
				lvgsNotReady := findLVMVolumeGroupsByNodeNames(lvgs, notReadyNodes)
				for _, lvg := range lvgsNotReady {
					err = updateLVGConditionIfNeeded(ctx, cl, log, &lvg, metav1.ConditionFalse, nodeReadyType, "NodeNotReady", "some of used nodes is not ready")
					if err != nil {
						log.Error(err, fmt.Sprintf("[RunSdsInfraWatcher] unable to add a condition to the LVMVolumeGroup %s", lvg.Name))
						continue
					}

					log.Info(fmt.Sprintf("[RunSdsInfraWatcher] successfully reconciled the LVMVolumeGroup %s phase and condition %s due to nodes are not in a Running state", lvg.Name, nodeReadyType))
				}
			} else {
				log.Info("[RunSdsInfraWatcher] every LVMVolumeGroup node is in a Running state")
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
				log.Debug(fmt.Sprintf("[RunSdsInfraWatcher] tries to update the LVMVolumeGroup %s condition %s to True", lvg.Name, nodeReadyType))
				err = updateLVGConditionIfNeeded(ctx, cl, log, &lvg, metav1.ConditionTrue, nodeReadyType, "NodesReady", "selected nodes were found in the cluster and have Ready state")
				if err != nil {
					log.Error(err, fmt.Sprintf("[RunSdsInfraWatcher] unable to add a condition to the LVMVolumeGroup %s", lvg.Name))
					continue
				}
				log.Info(fmt.Sprintf("[RunSdsInfraWatcher] successfully reconciled the LVMVolumeGroup %s condition %s to status True", lvg.Name, nodeReadyType))
			}

			log.Info("[RunSdsInfraWatcher] check if every sds-node-configurator agent's pod is healthy")
			log.Debug(fmt.Sprintf("[RunSdsInfraWatcher] tries to get pods by the selector %v", sdsNodeConfiguratorSelector))
			sdsPods, err := getPodsBySelector(ctx, cl, sdsNodeConfiguratorSelector)
			if err != nil {
				log.Error(err, fmt.Sprintf("[RunSdsInfraWatcher] unable to get pods by the seletor %v", sdsNodeConfiguratorSelector))
				continue
			}
			log.Debug(fmt.Sprintf("[RunSdsInfraWatcher] successfully got pods by the selector %v", sdsNodeConfiguratorSelector))

			if len(sdsPods) == 0 {
				log.Warning("[RunSdsInfraWatcher] no sds-node-configurator agent's pods found, update every LVMVolumeGroup condition AgentReady to False")

				for _, lvg := range lvgs {
					log.Debug(fmt.Sprintf("[RunSdsInfraWatcher] tries to update the LVMVolumeGroup %s condition %s to status False due to a missing agent's pod", lvg.Name, agentReadyType))
					err = updateLVGConditionIfNeeded(ctx, cl, log, &lvg, metav1.ConditionFalse, agentReadyType, "NoPod", "unable to find any agent's pod")
					if err != nil {
						log.Error(err, fmt.Sprintf("[RunSdsInfraWatcher] unable to add a condition %s to the LVMVolumeGroup %s", agentReadyType, lvg.Name))
						continue
					}

					log.Info(fmt.Sprintf("[RunSdsInfraWatcher] successfully reconciled the LVMVolumeGroup %s phase and condition %s due to missing pods", lvg.Name, agentReadyType))
				}

				log.Info("[RunSdsInfraWatcher] successfully updated every LVMVolumeGroup status.phase to NotReady due to no sds-node-configurator agent's pods are running")
				continue
			}

			log.Debug("[RunSdsInfraWatcher] sds-node-configurator agent's pods were found. Check if some pods are missing")
			for _, p := range sdsPods {
				log.Trace(fmt.Sprintf("[RunSdsInfraWatcher] found a pod: %s", p.Name))
			}

			unmanagedNodes := getNodeNamesWithoutAgent(usedNodes, sdsPods)
			if len(unmanagedNodes) > 0 {
				log.Warning("[RunSdsInfraWatcher] some LVMVolumeGroups are not managed due to corresponding sds-node-configurator agent's pods are not running. Turn such LVMVolumeGroups to NotReady phase")
				log.Trace(fmt.Sprintf("[RunSdsInfraWatcher] nodes without the agent: %v", unmanagedNodes))
				lvgsWithoutPod := findLVMVolumeGroupsByNodeNames(lvgs, unmanagedNodes)
				for _, lvg := range lvgsWithoutPod {
					log.Debug(fmt.Sprintf("[RunSdsInfraWatcher] tries to add a condition %s status False due the LVMVolumeGroup %s node is not managed by a sds-node-configurator agent's pod", agentReadyType, lvg.Name))
					err = updateLVGConditionIfNeeded(ctx, cl, log, &lvg, metav1.ConditionFalse, agentReadyType, "PodNotFound", "unable to find an agent's pod")
					if err != nil {
						log.Error(err, fmt.Sprintf("[RunSdsInfraWatcher] unable to add a condition to the LVMVolumeGroup %s", lvg.Name))
						continue
					}

					log.Info(fmt.Sprintf("[RunSdsInfraWatcher] successfully reconciled the LVMVolumeGroup %s condition %s due to missing pods", lvg.Name, agentReadyType))
				}
			} else {
				log.Info("[RunSdsInfraWatcher] no missing sds-node-configurator agent's pods were found")
			}

			log.Debug("[RunSdsInfraWatcher] check if every agent's pod is in a Ready state")
			notReadyPods := getNotReadyPods(sdsPods)
			if len(notReadyPods) > 0 {
				podsNames := make([]string, 0, len(notReadyPods))
				for name := range notReadyPods {
					podsNames = append(podsNames, name)
				}

				log.Warning(fmt.Sprintf("[RunSdsInfraWatcher] there is some sds-node-configurator agent's pods that is not Ready, pods: %s. Turn the LVMVolumeGroups condition AgentReady to False", strings.Join(podsNames, ",")))
				nodeNames := getNodeNamesFromPods(notReadyPods)
				log.Trace(fmt.Sprintf("[RunSdsInfraWatcher] node names with not Ready sds-node-configurator agent's pods: %v", nodeNames))
				lvgsNotReady := findLVMVolumeGroupsByNodeNames(lvgs, nodeNames)
				for _, lvg := range lvgsNotReady {
					log.Warning(fmt.Sprintf("[RunSdsInfraWatcher] the LVMVolumeGroup %s is managed by not Ready pod, turns the condition %s to False", lvg.Name, agentReadyType))
					err = updateLVGConditionIfNeeded(ctx, cl, log, &lvg, metav1.ConditionFalse, agentReadyType, "PodNotReady", "the pod is not Ready")
					if err != nil {
						log.Error(err, fmt.Sprintf("[RunSdsInfraWatcher] unable to add a condition to the LVMVolumeGroup %s", lvg.Name))
						continue
					}

					log.Info(fmt.Sprintf("[RunSdsInfraWatcher] successfully reconciled the LVMVolumeGroup %s phase and condition %s due to the pod is not Ready", lvg.Name, agentReadyType))
				}
			}

			if len(unmanagedNodes) == 0 && len(notReadyPods) == 0 {
				log.Info("[RunSdsInfraWatcher] no problems with sds-node-configurator agent's pods were found")
				for _, lvg := range lvgs {
					err = updateLVGConditionIfNeeded(ctx, cl, log, &lvg, metav1.ConditionTrue, agentReadyType, "PodReady", "pod is ready to manage the resource")
					if err != nil {
						log.Error(err, fmt.Sprintf("[RunSdsInfraWatcher] unable to add a condition to the LVMVolumeGroup %s", lvg.Name))
						continue
					}
					log.Info(fmt.Sprintf("[RunSdsInfraWatcher] successfully reconciled the LVMVolumeGroup %s", lvg.Name))
				}
			}
		}
	}()
}
