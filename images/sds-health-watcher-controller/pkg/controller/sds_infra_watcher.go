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
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
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
	log = log.WithName("RunSdsInfraWatcher")
	log.Info("starts the work")
	cl := mgr.GetClient()

	go func() {
		for {
			time.Sleep(cfg.ScanIntervalSec)
			log.Info("starts the reconciliation loop")

			log.Debug("tries to get LVMVolumeGroups")
			lvgs, err := GetLVMVolumeGroups(ctx, cl, metrics)
			if err != nil {
				log.Error(err, "unable to get LVMVolumeGroups")
				continue
			}
			log.Debug("successfully got LVMVolumeGroups")
			if len(lvgs) == 0 {
				log.Info("no LVMVolumeGroups found")
				continue
			}
			for _, lvg := range lvgs {
				log.Trace("LVMVolumeGroup conditions",
					"lvgName", lvg.Name,
					"conditions", lvg.Status.Conditions)
			}

			log.Info("LVMVolumeGroups found. Starts to check their health")
			log.Info("check if every LVMVolumeGroup node does exist")
			lvgNodeNames := getNodeNamesFromLVGs(lvgs)
			log.Trace("used nodes", "nodes", lvgNodeNames)

			log.Debug("tries to collect nodes used by LVMVolumeGroups")
			usedNodes, missedNodes, err := getNodesByNames(ctx, cl, lvgNodeNames)
			if err != nil {
				log.Error(err, "unable to get nodes")
				continue
			}
			log.Debug("successfully collected nodes used by LVMVolumeGroups")

			if len(missedNodes) > 0 {
				log.Warning("some LVMVolumeGroups use missing nodes. Turn those LVMVolumeGroups condition NodeReady to False",
					"missedNodes", missedNodes)
				lvgsWithMissedNodes := findLVMVolumeGroupsByNodeNames(lvgs, missedNodes)
				for _, lvg := range lvgsWithMissedNodes {
					log := log.WithValues(
						"lvgName", lvg.Name,
						"conditionType", nodeReadyType)
					err = updateLVGConditionIfNeeded(ctx, cl, log, &lvg, metav1.ConditionFalse, nodeReadyType, "MissingNode", "unable to find the used nodes")
					if err != nil {
						log.Error(err, "unable to add a condition to the LVMVolumeGroup")
						continue
					}

					log.Info("successfully reconciled the LVMVolumeGroup phase and condition due to missing nodes")
				}
			} else {
				log.Info("no missing nodes used by LVMVolumeGroups were found")
			}

			log.Debug("check if every used node is Ready")
			notReadyNodes := getNotReadyNodes(usedNodes)
			if len(notReadyNodes) > 0 {
				log.Warning("some LVMVolumeGroup nodes are not Ready. Turn those LVMVolumeGroups condition NodeReady to False",
					"notReadyNodes", notReadyNodes)
				lvgsNotReady := findLVMVolumeGroupsByNodeNames(lvgs, notReadyNodes)
				for _, lvg := range lvgsNotReady {
					log := log.WithValues(
						"lvgName", lvg.Name,
						"conditionType", nodeReadyType)
					err = updateLVGConditionIfNeeded(ctx, cl, log, &lvg, metav1.ConditionFalse, nodeReadyType, "NodeNotReady", "some of used nodes is not ready")
					if err != nil {
						log.Error(err, "unable to add a condition to the LVMVolumeGroup")
						continue
					}

					log.Info("successfully reconciled the LVMVolumeGroup phase and condition due to nodes are not in a Running state")
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
							log.Error(
								errors.New("found different node names in lvg.Status.Nodes"),
								"found different node names in lvg.Status.Nodes",
								"lvgName", lvg.Name,
								"nodes", lvg.Status.Nodes)
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
				log := log.WithValues(
					"lvgName", lvg.Name,
					"conditionType", nodeReadyType)
				log.Debug("tries to update the LVMVolumeGroup condition to True")
				err = updateLVGConditionIfNeeded(ctx, cl, log, &lvg, metav1.ConditionTrue, nodeReadyType, "NodesReady", "selected nodes were found in the cluster and have Ready state")
				if err != nil {
					log.Error(err, "unable to add a condition to the LVMVolumeGroup")
					continue
				}
				log.Info("successfully reconciled the LVMVolumeGroup condition to status True")
			}

			log.Info("check if every sds-node-configurator agent's pod is healthy")
			log.Debug("tries to get pods by the selector",
				"selector", sdsNodeConfiguratorSelector)
			sdsPods, err := getPodsBySelector(ctx, cl, sdsNodeConfiguratorSelector)
			if err != nil {
				log.Error(err, "unable to get pods by the selector",
					"selector", sdsNodeConfiguratorSelector)
				continue
			}
			log.Debug("successfully got pods by the selector",
				"selector", sdsNodeConfiguratorSelector)

			if len(sdsPods) == 0 {
				log.Warning("no sds-node-configurator agent's pods found, update every LVMVolumeGroup condition AgentReady to False")

				for _, lvg := range lvgs {
					log := log.WithValues(
						"lvgName", lvg.Name,
						"conditionType", agentReadyType)
					log.Debug("tries to update the LVMVolumeGroup condition to status False due to a missing agent's pod")
					err = updateLVGConditionIfNeeded(ctx, cl, log, &lvg, metav1.ConditionFalse, agentReadyType, "NoPod", "unable to find any agent's pod")
					if err != nil {
						log.Error(err, "unable to add a condition to the LVMVolumeGroup")
						continue
					}

					log.Info("successfully reconciled the LVMVolumeGroup phase and condition due to missing pods")
				}

				log.Info("successfully updated every LVMVolumeGroup status.phase to NotReady due to no sds-node-configurator agent's pods are running")
				continue
			}

			log.Debug("sds-node-configurator agent's pods were found. Check if some pods are missing")
			for _, p := range sdsPods {
				log.Trace("found a pod", "podName", p.Name)
			}

			unmanagedNodes := getNodeNamesWithoutAgent(usedNodes, sdsPods)
			if len(unmanagedNodes) > 0 {
				log.Warning("some LVMVolumeGroups are not managed due to corresponding sds-node-configurator agent's pods are not running. Turn such LVMVolumeGroups to NotReady phase")
				log.Trace("nodes without the agent", "unmanagedNodes", unmanagedNodes)
				lvgsWithoutPod := findLVMVolumeGroupsByNodeNames(lvgs, unmanagedNodes)
				for _, lvg := range lvgsWithoutPod {
					log := log.WithValues(
						"lvgName", lvg.Name,
						"conditionType", agentReadyType)
					log.Debug("tries to add a condition status False due the LVMVolumeGroup node is not managed by a sds-node-configurator agent's pod")
					err = updateLVGConditionIfNeeded(ctx, cl, log, &lvg, metav1.ConditionFalse, agentReadyType, "PodNotFound", "unable to find an agent's pod")
					if err != nil {
						log.Error(err, "unable to add a condition to the LVMVolumeGroup")
						continue
					}

					log.Info("successfully reconciled the LVMVolumeGroup condition due to missing pods")
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

				log.Warning("there is some sds-node-configurator agent's pods that is not Ready. Turn the LVMVolumeGroups condition AgentReady to False", "pods", strings.Join(podsNames, ","))
				nodeNames := getNodeNamesFromPods(notReadyPods)
				log.Trace("node names with not Ready sds-node-configurator agent's pods",
					"nodeNames", nodeNames)
				lvgsNotReady := findLVMVolumeGroupsByNodeNames(lvgs, nodeNames)
				for _, lvg := range lvgsNotReady {
					log := log.WithValues(
						"lvgName", lvg.Name,
						"conditionType", agentReadyType)
					log.Warning("the LVMVolumeGroup is managed by not Ready pod, turns the condition to False")
					err = updateLVGConditionIfNeeded(ctx, cl, log, &lvg, metav1.ConditionFalse, agentReadyType, "PodNotReady", "the pod is not Ready")
					if err != nil {
						log.Error(err, "unable to add a condition to the LVMVolumeGroup")
						continue
					}

					log.Info("successfully reconciled the LVMVolumeGroup phase and condition due to the pod is not Ready")
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
							log.Error(
								errors.New("found different node names in lvg.Status.Nodes"),
								"found different node names in lvg.Status.Nodes",
								"lvgName", lvg.Name,
								"nodes", lvg.Status.Nodes)
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
					log := log.WithValues("lvgName", lvg.Name)
					err = updateLVGConditionIfNeeded(ctx, cl, log, &lvg, metav1.ConditionTrue, agentReadyType, "PodReady", "pod is ready to manage the resource")
					if err != nil {
						log.Error(err, "unable to add a condition to the LVMVolumeGroup")
						continue
					}
					log.Info("successfully reconciled the LVMVolumeGroup")
				}
			}
		}
	}()
}
