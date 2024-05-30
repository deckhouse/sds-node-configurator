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
	"sds-health-watcher-controller/api/v1alpha1"
	"sds-health-watcher-controller/config"
	"sds-health-watcher-controller/pkg/logger"
	"sds-health-watcher-controller/pkg/monitoring"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"time"
)

const (
	SdsHealthWatcherCtrlName = "sds-health-watcher-controller"

	pending     phase = "Pending"
	ready       phase = "Ready"
	notReady    phase = "NotReady"
	terminating phase = "Terminating"
)

var (
	sdsNodeConfiguratorSelector = map[string]string{"app": "sds-node-configurator"}
)

type phase string

func RunHealthWatcher(
	ctx context.Context,
	mgr manager.Manager,
	cfg config.Options,
	metrics monitoring.Metrics,
	log logger.Logger,
) {
	log.Info("[RunHealthWatcher] starts the work")
	cl := mgr.GetClient()
	//cache := mgr.GetCache()

	go func() {
		for {
			time.Sleep(5 * time.Second)

			log.Debug(fmt.Sprint("[RunHealthWatcher] tries to get LVMVolumeGroups"))
			lvgs, err := GetLVMVolumeGroups(ctx, cl, metrics)
			if err != nil {
				log.Error(err, "[RunHealthWatcher] unable to get LVMVolumeGroups")
				continue
			}
			log.Debug(fmt.Sprint("[RunHealthWatcher] successfully got LVMVolumeGroups"))

			if len(lvgs) == 0 {
				log.Info("[RunHealthWatcher] no LVMVolumeGroups found")
				continue
			}

			log.Info("[RunHealthWatcher] LVMVolumeGroups found. Starts to check their health")

			log.Debug(fmt.Sprintf("[RunHealthWatcher] collect used by LVMVolumeGroups nodes names"))
			nodeNamesToWatch := getNodeNamesFromLVGs(lvgs)
			log.Trace(fmt.Sprintf("[RunHealthWatcher] used nodes %v", nodeNamesToWatch))

			usedNodes, missedNodes, err := getNodesByNames(ctx, cl, nodeNamesToWatch)
			if err != nil {
				log.Error(err, "[RunHealthWatcher] unable to get nodes")
				continue
			}

			if len(missedNodes) > 0 {
				log.Warning(fmt.Sprintf("[RunHealthWatcher] LVMVolumeGroups use missing nodes: %v. Turn LVMVolumeGroups phases to NotReady", missedNodes))
				lvgsNotReady := findLVMVolumeGroupsByNodes(lvgs, missedNodes)
				for _, lvg := range lvgsNotReady {
					log.Debug(fmt.Sprintf("[RunHealthWatcher] tries to update the LVMVolumeGroup %s status.phase to %s", lvg.Name, notReady))
					err = updateLVMVolumeGroupPhase(ctx, cl, lvg, notReady)
					if err != nil {
						log.Error(err, fmt.Sprintf("[RunHealthWatcher] unable to update the LVMVolumeGroup %s status.phase", lvg.Name))
						continue
					}
					log.Info(fmt.Sprintf("[RunHealthWatcher] successfully updated the LVMVolumeGroup %s status.phase to %s due to a missing node", lvg.Name, notReady))
				}
			}

			if len(missedNodes) == 0 {
				log.Info("[RunHealthWatcher] no missing nodes used by LVMVolumeGroups found")
			}

			sdsPods, err := getPodsBySelector(ctx, cl, sdsNodeConfiguratorSelector)
			if err != nil {
				log.Error(err, fmt.Sprintf("[RunHealthWatcher] unable to get pod by the seletor %v", sdsNodeConfiguratorSelector))
				continue
			}

			if len(sdsPods) == 0 {
				log.Warning("[RunHealthWatcher] no sds-node-configurator controllers pods found, update every LVMVolumeGroup status.phase to NotReady")

				for _, lvg := range lvgs {
					err = updateLVMVolumeGroupPhase(ctx, cl, lvg, notReady)
					if err != nil {
						log.Error(err, fmt.Sprintf("[RunHealthWatcher] unable to update status phase of the LVMVolumeGroup %s", lvg.Name))
						continue
					}
				}

				log.Info("[RunHealthWatcher] successfully updated every LVMVolumeGroup status.phase to NotReady due to no agent pods are running")
				continue
			}

			if len(usedNodes) != len(sdsPods) {
				log.Warning("[RunHealthWatcher] some LVMVolumeGroups are not managed due to its pods are not running. Turn such LVMVolumeGroups to NotReady phase")
				unmanagedNodes := getNodeNamesWithoutAgent(usedNodes, sdsPods)
				fmt.Println(unmanagedNodes)
				//lvgsNotReady := findLVMVolumeGroupsByNodes(lvgs, unmanagedNodes)
				//for _, lvg := range lvgsNotReady {
				//	log.Debug(fmt.Sprintf("[RunHealthWatcher] tries to update the LVMVolumeGroup %s status.phase to %s", lvg.Name, notReady))
				//	err = updateLVMVolumeGroupPhase(ctx, cl, lvg, notReady)
				//	if err != nil {
				//		log.Error(err, fmt.Sprintf("[RunHealthWatcher] unable to update the LVMVolumeGroup %s status.phase", lvg.Name))
				//		continue
				//	}
				//	log.Info(fmt.Sprintf("[RunHealthWatcher] successfully updated the LVMVolumeGroup %s status.phase to %s due to its node is not managed", lvg.Name, notReady))
				//}
			}

			// Смотреть за нодой
			// Смотреть за подами
			// Выставлять Phase в LVG
		}
	}()
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
	err := cl.List(ctx, podList, client.MatchingLabels(selector))
	if err != nil {
		return nil, err
	}

	pods := make(map[string]v1.Pod, len(podList.Items))
	for _, p := range podList.Items {
		pods[p.Spec.NodeName] = p
	}

	return pods, nil
}

func updateLVMVolumeGroupPhase(ctx context.Context, cl client.Client, lvg v1alpha1.LvmVolumeGroup, phase phase) error {
	lvg.Status.Phase = string(phase)
	return cl.Status().Update(ctx, &lvg)
}

func findLVMVolumeGroupsByNodes(lvgs map[string]v1alpha1.LvmVolumeGroup, nodeNames []string) map[string]v1alpha1.LvmVolumeGroup {
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
			delete(nodes, name)
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
	metrics.ApiMethodsDuration(SdsHealthWatcherCtrlName, "list").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.ApiMethodsExecutionCount(SdsHealthWatcherCtrlName, "list").Inc()
	if err != nil {
		metrics.ApiMethodsErrors(SdsHealthWatcherCtrlName, "list").Inc()
		return nil, fmt.Errorf("[GetApiLVMVolumeGroups] unable to list LvmVolumeGroups, err: %w", err)
	}

	lvgs := make(map[string]v1alpha1.LvmVolumeGroup, len(lvgList.Items))
	for _, lvg := range lvgList.Items {
		lvgs[lvg.Name] = lvg
	}

	return lvgs, nil
}
