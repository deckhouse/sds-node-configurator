/*
Copyright 2024 Flant JSC

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
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sds-health-watcher-controller/api/v1alpha1"
	"sds-health-watcher-controller/config"
	"sds-health-watcher-controller/internal"
	"sds-health-watcher-controller/pkg/logger"
	"sds-health-watcher-controller/pkg/monitoring"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"time"
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
			log.Debug(fmt.Sprint("[RunSdsInfraWatcher] successfully got LVMVolumeGroups"))

			if len(lvgs) == 0 {
				log.Info("[RunSdsInfraWatcher] no LVMVolumeGroups found")
				continue
			}

			log.Info("[RunSdsInfraWatcher] LVMVolumeGroups found. Starts to check their health")
			log.Info("[RunSdsInfraWatcher] check if every LVMVolumeGroup node does exist")
			nodeNamesToWatch := getNodeNamesFromLVGs(lvgs)
			log.Trace(fmt.Sprintf("[RunSdsInfraWatcher] used nodes %v", nodeNamesToWatch))

			log.Debug(fmt.Sprintf("[RunSdsInfraWatcher] tries to collect nodes used by LVMVolumeGroups"))
			usedNodes, missedNodes, err := getNodesByNames(ctx, cl, nodeNamesToWatch)
			if err != nil {
				log.Error(err, "[RunSdsInfraWatcher] unable to get nodes")
				continue
			}
			log.Debug(fmt.Sprintf("[RunSdsInfraWatcher] successfully collected nodes used by LVMVolumeGroups"))

			if len(missedNodes) > 0 {
				log.Warning(fmt.Sprintf("[RunSdsInfraWatcher] some LVMVolumeGroups use missing nodes: %v. Turn those LVMVolumeGroups condition NodeReady to False", missedNodes))
				lvgsNotReady := findLVMVolumeGroupsByNodeNames(lvgs, missedNodes)
				for _, lvg := range lvgsNotReady {
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
				log.Warning(fmt.Sprintf("[RunSdsInfraWatcher] some LVMVolumeGroups use not Ready nodes: %v. Turn those LVMVolumeGroups condition NodeReady to False", notReadyNodes))
				lvgsNotReady := findLVMVolumeGroupsByNodeNames(lvgs, notReadyNodes)
				for _, lvg := range lvgsNotReady {
					err = updateLVGConditionIfNeeded(ctx, cl, log, &lvg, metav1.ConditionFalse, nodeReadyType, "NodeNotRunning", "some of used nodes not in a Running state")
					if err != nil {
						log.Error(err, fmt.Sprintf("[RunSdsInfraWatcher] unable to add a condition to the LVMVolumeGroup %s", lvg.Name))
						continue
					}

					log.Info(fmt.Sprintf("[RunSdsInfraWatcher] successfully reconciled the LVMVolumeGroup %s phase and condition %s due to nodes are not in a Running state", lvg.Name, nodeReadyType))
				}
			} else {
				log.Info("[RunSdsInfraWatcher] every LVMVolumeGroup node is in a Running state")
			}

			if len(missedNodes) == 0 && len(notReadyNodes) == 0 {
				for _, lvg := range lvgs {
					log.Debug(fmt.Sprintf("[RunSdsInfraWatcher] tries to update the LVMVolumeGroup %s condition %s to True", lvg.Name, nodeReadyType))
					err = updateLVGConditionIfNeeded(ctx, cl, log, &lvg, metav1.ConditionTrue, nodeReadyType, "NodesFound", "selected nodes were found in the cluster")
					if err != nil {
						log.Error(err, fmt.Sprintf("[RunSdsInfraWatcher] unable to add a condition to the LVMVolumeGroup %s", lvg.Name))
						continue
					}
					log.Info(fmt.Sprintf("[RunSdsInfraWatcher] successfully reconciled the LVMVolumeGroup %s condition %s to status True", lvg.Name, nodeReadyType))
				}
			}

			log.Info("[RunSdsInfraWatcher] check if every sds-node-configurator agent's pod is up")
			log.Debug(fmt.Sprintf("[RunSdsInfraWatcher] tries to get pods by the selector %v", sdsNodeConfiguratorSelector))
			sdsPods, err := getPodsBySelector(ctx, cl, sdsNodeConfiguratorSelector)
			if err != nil {
				log.Error(err, fmt.Sprintf("[RunSdsInfraWatcher] unable to get pods by the seletor %v", sdsNodeConfiguratorSelector))
				continue
			}
			log.Debug(fmt.Sprintf("[RunSdsInfraWatcher] successfully got pods by the selector %v", sdsNodeConfiguratorSelector))
			for _, p := range sdsPods {
				log.Trace(fmt.Sprintf("[RunSdsInfraWatcher] found a pod: %s", p.Name))
			}

			if len(sdsPods) == 0 {
				log.Warning("[RunSdsInfraWatcher] no sds-node-configurator agent's pods found, update every LVMVolumeGroup condition AgentReady to False")

				for _, lvg := range lvgs {
					log.Debug(fmt.Sprintf("[RunSdsInfraWatcher] tries to update the LVMVolumeGroup %s condition %s to status False due to a missing agent's pod", lvg.Name, agentReadyType))
					err = updateLVGConditionIfNeeded(ctx, cl, log, &lvg, metav1.ConditionFalse, agentReadyType, "NoPods", "unable to find any agent's pod")
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

			var unmanagedNodes []string
			if len(usedNodes) != len(sdsPods) {
				log.Warning("[RunSdsInfraWatcher] some LVMVolumeGroups are not managed due to corresponding sds-node-configurator agent's pods are not running. Turn such LVMVolumeGroups to NotReady phase")
				unmanagedNodes = getNodeNamesWithoutAgent(usedNodes, sdsPods)
				log.Trace(fmt.Sprintf("[RunSdsInfraWatcher] nodes without the agent: %v", unmanagedNodes))
				lvgsNotReady := findLVMVolumeGroupsByNodeNames(lvgs, unmanagedNodes)
				for _, lvg := range lvgsNotReady {
					err = updateLVGConditionIfNeeded(ctx, cl, log, &lvg, metav1.ConditionFalse, agentReadyType, "NoPods", "unable to find any agent's pod")
					if err != nil {
						log.Error(err, fmt.Sprintf("[RunSdsInfraWatcher] unable to add a condition to the LVMVolumeGroup %s", lvg.Name))
						continue
					}

					log.Info(fmt.Sprintf("[RunSdsInfraWatcher] successfully reconciled the LVMVolumeGroup %s condition %s due to missing pods", lvg.Name, agentReadyType))
				}
			} else {
				log.Info("[RunSdsInfraWatcher] no missing sds-node-configurator agent's pods were found")
			}

			log.Debug("[RunSdsInfraWatcher] check if every agent's pod is in a Running state")
			notRunningPods := getNotRunningPods(sdsPods)
			if len(notRunningPods) > 0 {
				log.Warning(fmt.Sprintf("[RunSdsInfraWatcher] there is some sds-node-configurator agent's pods that is not Running, pods: %v. Turn the LVMVolumeGroups condition AgentReady to False", notRunningPods))
				nodeNames := getNodeNamesFromPods(notRunningPods)
				log.Trace(fmt.Sprintf("[RunSdsInfraWatcher] node names with not running sds-node-configurator agent's pods: %v", nodeNames))
				lvgsNotReady := findLVMVolumeGroupsByNodeNames(lvgs, nodeNames)
				for _, lvg := range lvgsNotReady {
					err = updateLVGConditionIfNeeded(ctx, cl, log, &lvg, metav1.ConditionFalse, agentReadyType, "PodNotRunning", "the pod is not in a Running state")
					if err != nil {
						log.Error(err, fmt.Sprintf("[RunSdsInfraWatcher] unable to add a condition to the LVMVolumeGroup %s", lvg.Name))
						continue
					}

					log.Info(fmt.Sprintf("[RunSdsInfraWatcher] successfully reconciled the LVMVolumeGroup %s phase and condition %s due to the pod is not in a running state", lvg.Name, agentReadyType))
				}

				continue
			}

			if len(unmanagedNodes) == 0 && len(notRunningPods) == 0 {
				log.Info("[RunSdsInfraWatcher] no problems with sds-node-configurator agent's pods were found")
				for _, lvg := range lvgs {
					err = updateLVGConditionIfNeeded(ctx, cl, log, &lvg, metav1.ConditionTrue, agentReadyType, "PodRunning", "pod is running and managing the resource")
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

func getNodeNamesFromPods(pods map[string]v1.Pod) []string {
	result := make([]string, 0, len(pods))

	for _, p := range pods {
		result = append(result, p.Spec.NodeName)
	}

	return result
}

func getNotRunningPods(pods map[string]v1.Pod) map[string]v1.Pod {
	result := make(map[string]v1.Pod, len(pods))

	for _, p := range pods {
		if p.Status.Phase != v1.PodRunning {
			result[p.Name] = p
		}
	}

	return result
}

func getNotReadyNodes(nodes map[string]v1.Node) []string {
	result := make([]string, 0, len(nodes))

	for _, n := range nodes {
		for _, c := range n.Status.Conditions {
			if c.Type == internal.ReadyType && c.Status != v1.ConditionTrue {
				result = append(result, n.Name)
			}
		}
	}

	return result
}

func getNodeNamesWithoutAgent(nodes map[string]v1.Node, pods map[string]v1.Pod) []string {
	result := make([]string, 0, len(nodes))

	for _, n := range nodes {
		if _, exist := pods[n.Name]; !exist {
			result = append(result, n.Name)
		}
	}

	return result
}

func getPodsBySelector(ctx context.Context, cl client.Client, selector map[string]string) (map[string]v1.Pod, error) {
	podList := &v1.PodList{}
	err := cl.List(ctx, podList, &client.ListOptions{Namespace: internal.SdsNodeConfiguratorNamespace, LabelSelector: labels.Set(selector).AsSelector()})
	if err != nil {
		return nil, err
	}

	pods := make(map[string]v1.Pod, len(podList.Items))
	for _, p := range podList.Items {
		fmt.Println(p.Name)
		pods[p.Spec.NodeName] = p
	}

	return pods, nil
}

func findLVMVolumeGroupsByNodeNames(lvgs map[string]v1alpha1.LvmVolumeGroup, nodeNames []string) map[string]v1alpha1.LvmVolumeGroup {
	result := make(map[string]v1alpha1.LvmVolumeGroup, len(lvgs))

	names := make(map[string]struct{}, len(nodeNames))
	for _, n := range nodeNames {
		names[n] = struct{}{}
	}

	for _, lvg := range lvgs {
		for _, n := range lvg.Status.Nodes {
			if _, use := names[n.Name]; use {
				result[lvg.Name] = lvg
			}
		}
	}

	return result
}

func getNodesByNames(ctx context.Context, cl client.Client, names []string) (map[string]v1.Node, []string, error) {
	nodeList := &v1.NodeList{}

	err := cl.List(ctx, nodeList)
	if err != nil {
		return nil, nil, err
	}

	nodes := make(map[string]v1.Node, len(nodeList.Items))
	for _, n := range nodeList.Items {
		nodes[n.Name] = n
	}

	missedNodes := make([]string, 0, len(names))
	for _, name := range names {
		if _, exist := nodes[name]; !exist {
			missedNodes = append(missedNodes, name)
		}
	}

	return nodes, missedNodes, nil
}

func getNodeNamesFromLVGs(lvgs map[string]v1alpha1.LvmVolumeGroup) []string {
	nodes := make([]string, 0, len(lvgs))

	for _, lvg := range lvgs {
		for _, n := range lvg.Status.Nodes {
			nodes = append(nodes, n.Name)
		}
	}

	return nodes
}

func GetLVMVolumeGroups(ctx context.Context, cl client.Client, metrics monitoring.Metrics) (map[string]v1alpha1.LvmVolumeGroup, error) {
	lvgList := &v1alpha1.LvmVolumeGroupList{}

	start := time.Now()
	err := cl.List(ctx, lvgList)
	metrics.ApiMethodsDuration(SdsInfraWatcherCtrlName, "list").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.ApiMethodsExecutionCount(SdsInfraWatcherCtrlName, "list").Inc()
	if err != nil {
		metrics.ApiMethodsErrors(SdsInfraWatcherCtrlName, "list").Inc()
		return nil, fmt.Errorf("[GetApiLVMVolumeGroups] unable to list LvmVolumeGroups, err: %w", err)
	}

	lvgs := make(map[string]v1alpha1.LvmVolumeGroup, len(lvgList.Items))
	for _, lvg := range lvgList.Items {
		lvgs[lvg.Name] = lvg
	}

	return lvgs, nil
}

func updateLVGConditionIfNeeded(ctx context.Context, cl client.Client, log logger.Logger, lvg *v1alpha1.LvmVolumeGroup, status metav1.ConditionStatus, conType, reason, message string) error {
	exist := false
	index := 0
	condition := metav1.Condition{
		Type:               conType,
		Status:             status,
		ObservedGeneration: lvg.Generation,
		LastTransitionTime: metav1.NewTime(time.Now()),
		Reason:             reason,
		Message:            message,
	}

	if lvg.Status.Conditions == nil {
		log.Debug(fmt.Sprintf("[updateLVGConditionIfNeeded] the LVMVolumeGroup %s conditions is nil. Initialize them", lvg.Name))
		lvg.Status.Conditions = make([]metav1.Condition, 0, 2)
	}

	if len(lvg.Status.Conditions) > 0 {
		log.Debug(fmt.Sprintf("[updateLVGConditionIfNeeded] there are some conditions in the LVMVolumeGroup %s. Tries to find a condition %s", lvg.Name, conType))
		for i, c := range lvg.Status.Conditions {
			if c.Type == conType {
				if c.Status == metav1.ConditionTrue && status == metav1.ConditionTrue {
					log.Debug(fmt.Sprintf("[updateLVGConditionIfNeeded] no need to update %s condition of the LVMVolumeGroup %s", conType, lvg.Name))
					return nil
				}

				index = i
				exist = true
				log.Debug(fmt.Sprintf("[updateLVGConditionIfNeeded] a condition %s was found in the LVMVolumeGroup %s at the index %d", conType, lvg.Name, i))
			}
		}

		if !exist {
			log.Debug(fmt.Sprintf("[updateLVGConditionIfNeeded] a condition %s was not found. Append it in the end of the LVMVolumeGroup %s conditions", conType, lvg.Name))
			lvg.Status.Conditions = append(lvg.Status.Conditions, condition)
		} else {
			log.Debug(fmt.Sprintf("[updateLVGConditionIfNeeded] insert the condition %s at index %d of the LVMVolumeGroup %s conditions", conType, index, lvg.Name))
			lvg.Status.Conditions[index] = condition
		}
	} else {
		log.Debug(fmt.Sprintf("[updateLVGConditionIfNeeded] no conditions were found in the LVMVolumeGroup %s. Append the condition %s in the end", lvg.Name, conType))
		lvg.Status.Conditions = append(lvg.Status.Conditions, condition)
	}

	return cl.Status().Update(ctx, lvg)
}
