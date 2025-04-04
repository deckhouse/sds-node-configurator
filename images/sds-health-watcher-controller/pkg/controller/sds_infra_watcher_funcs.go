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
	"time"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/sds-health-watcher-controller/internal"
	"github.com/deckhouse/sds-node-configurator/images/sds-health-watcher-controller/pkg/logger"
	"github.com/deckhouse/sds-node-configurator/images/sds-health-watcher-controller/pkg/monitoring"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func getNodeNamesFromPods(pods map[string]v1.Pod) []string {
	result := make([]string, 0, len(pods))

	for _, p := range pods {
		result = append(result, p.Spec.NodeName)
	}

	return result
}

func getNotReadyPods(pods map[string]v1.Pod) map[string]v1.Pod {
	result := make(map[string]v1.Pod, len(pods))

	for _, p := range pods {
		for _, c := range p.Status.Conditions {
			if c.Type == internal.TypeReady && c.Status != v1.ConditionTrue {
				result[p.Name] = p
			}
		}
	}

	return result
}

func getNotReadyNodes(nodes map[string]v1.Node) []string {
	result := make([]string, 0, len(nodes))

	for _, n := range nodes {
		for _, c := range n.Status.Conditions {
			if c.Type == internal.TypeReady && c.Status != v1.ConditionTrue {
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
		pods[p.Spec.NodeName] = p
	}

	return pods, nil
}

func findLVMVolumeGroupsByNodeNames(lvgs map[string]v1alpha1.LVMVolumeGroup, nodeNames []string) map[string]v1alpha1.LVMVolumeGroup {
	result := make(map[string]v1alpha1.LVMVolumeGroup, len(lvgs))

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

func getNodesByNames(ctx context.Context, cl client.Client, lvgNodeNames []string) (map[string]v1.Node, []string, error) {
	nodeList := &v1.NodeList{}

	err := cl.List(ctx, nodeList)
	if err != nil {
		return nil, nil, err
	}

	nodes := make(map[string]v1.Node, len(nodeList.Items))
	for _, n := range nodeList.Items {
		nodes[n.Name] = n
	}

	missedNodes := make([]string, 0, len(lvgNodeNames))
	usedNodes := make(map[string]v1.Node, len(lvgNodeNames))
	for _, name := range lvgNodeNames {
		if _, exist := nodes[name]; !exist {
			missedNodes = append(missedNodes, name)
		}
		usedNodes[name] = nodes[name]
	}

	return usedNodes, missedNodes, nil
}

func getNodeNamesFromLVGs(lvgs map[string]v1alpha1.LVMVolumeGroup) []string {
	nodes := make([]string, 0, len(lvgs))

	for _, lvg := range lvgs {
		for _, n := range lvg.Status.Nodes {
			nodes = append(nodes, n.Name)
		}
	}

	return nodes
}

func GetLVMVolumeGroups(ctx context.Context, cl client.Client, metrics monitoring.Metrics) (map[string]v1alpha1.LVMVolumeGroup, error) {
	lvgList := &v1alpha1.LVMVolumeGroupList{}

	start := time.Now()
	err := cl.List(ctx, lvgList)
	metrics.APIMethodsDuration(SdsInfraWatcherCtrlName, "list").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.APIMethodsExecutionCount(SdsInfraWatcherCtrlName, "list").Inc()
	if err != nil {
		metrics.APIMethodsErrors(SdsInfraWatcherCtrlName, "list").Inc()
		return nil, fmt.Errorf("[GetApiLVMVolumeGroups] unable to list LVMVolumeGroups, err: %w", err)
	}

	lvgs := make(map[string]v1alpha1.LVMVolumeGroup, len(lvgList.Items))
	for _, lvg := range lvgList.Items {
		lvgs[lvg.Name] = lvg
	}

	return lvgs, nil
}

func updateLVGConditionIfNeeded(ctx context.Context, cl client.Client, log logger.Logger, lvg *v1alpha1.LVMVolumeGroup, status metav1.ConditionStatus, conType, reason, message string) error {
	exist := false
	index := 0
	newCondition := metav1.Condition{
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
				log.Trace(fmt.Sprintf("[updateLVGConditionIfNeeded] old condition: %+v, new condition: %+v", c, newCondition))
				if checkIfEqualConditions(c, newCondition) {
					log.Debug(fmt.Sprintf("[updateLVGConditionIfNeeded] no need to update condition %s in the LVMVolumeGroup %s as new and old condition states are the same", conType, lvg.Name))
					return nil
				}

				index = i
				exist = true
				log.Debug(fmt.Sprintf("[updateLVGConditionIfNeeded] a condition %s was found in the LVMVolumeGroup %s at the index %d", conType, lvg.Name, i))
			}
		}

		if !exist {
			log.Debug(fmt.Sprintf("[updateLVGConditionIfNeeded] a condition %s was not found. Append it in the end of the LVMVolumeGroup %s conditions", conType, lvg.Name))
			lvg.Status.Conditions = append(lvg.Status.Conditions, newCondition)
		} else {
			log.Debug(fmt.Sprintf("[updateLVGConditionIfNeeded] insert the condition %s at index %d of the LVMVolumeGroup %s conditions", conType, index, lvg.Name))
			lvg.Status.Conditions[index] = newCondition
		}
	} else {
		log.Debug(fmt.Sprintf("[updateLVGConditionIfNeeded] no conditions were found in the LVMVolumeGroup %s. Append the condition %s in the end", lvg.Name, conType))
		lvg.Status.Conditions = append(lvg.Status.Conditions, newCondition)
	}

	return cl.Status().Update(ctx, lvg)
}

func checkIfEqualConditions(first, second metav1.Condition) bool {
	return first.Type == second.Type &&
		first.Status == second.Status &&
		first.Reason == second.Reason &&
		first.Message == second.Message &&
		first.ObservedGeneration == second.ObservedGeneration
}
