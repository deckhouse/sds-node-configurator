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
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-common-lib/conditions"
	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/controller/internal"
	"github.com/deckhouse/sds-node-configurator/images/controller/pkg/logger"
	"github.com/deckhouse/sds-node-configurator/images/controller/pkg/monitoring"
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

// updateLVGConditionIfNeeded sets a condition on the LVMVolumeGroup, skipping the
// write when nothing changed and retrying on conflict.
//
// The retry matters here: the agent on every node writes conditions on the same
// LVMVolumeGroup, so a write built from the copy the caller happens to hold loses
// the race often enough to be routine.
//
// On success, lvg.Status and lvg.ResourceVersion are replaced with server-side values.
// Callers must not hold unsaved in-memory Status modifications before calling this.
func updateLVGConditionIfNeeded(ctx context.Context, cl client.Client, log logger.Logger, lvg *v1alpha1.LVMVolumeGroup, status metav1.ConditionStatus, conType, reason, message string) error {
	log.Debug(fmt.Sprintf("[updateLVGConditionIfNeeded] set the condition type %s status %s reason %s message %s on the LVMVolumeGroup %s", conType, status, reason, message, lvg.Name))

	// Callers pass raw error text, while the schema caps the message at 32768.
	// Over the cap the API server rejects the whole status write, so the group
	// would keep reporting its previous verdict and the reconcile would fail on
	// the write rather than on what actually went wrong. conditions.Set does not
	// truncate: it is a thin wrapper over meta.SetStatusCondition.
	message = conditions.TruncateMessage(message)

	// The state UpdateStatus read and wrote, kept so the server-side values can be
	// mirrored onto the caller's object below.
	var written *v1alpha1.LVMVolumeGroup

	err := conditions.UpdateStatus(ctx, cl, lvg, func(fresh *v1alpha1.LVMVolumeGroup) {
		written = fresh
		conditions.Set(&fresh.Status.Conditions, metav1.Condition{
			Type:               conType,
			Status:             status,
			ObservedGeneration: fresh.Generation,
			Reason:             reason,
			Message:            message,
		})
	})
	if err != nil {
		return err
	}

	lvg.Status = written.Status
	lvg.ResourceVersion = written.ResourceVersion
	return nil
}
