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

package repository

import (
	"context"
	"time"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/logger"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/monitoring"
)

type LVGClient struct {
	cl                              client.Client
	log                             logger.Logger
	currentNodeName, controllerName string
	metrics                         monitoring.Metrics
}

func NewLVGClient(
	cl client.Client,
	log logger.Logger,
	metrics monitoring.Metrics,
	currentNodeName string,
	controllerName string,
) *LVGClient {
	return &LVGClient{
		cl:              cl,
		log:             log,
		metrics:         metrics,
		currentNodeName: currentNodeName,
		controllerName:  controllerName,
	}
}

func (lvgCl *LVGClient) GetLVMVolumeGroup(ctx context.Context, name string) (*v1alpha1.LVMVolumeGroup, error) {
	obj := &v1alpha1.LVMVolumeGroup{}
	start := time.Now()
	err := lvgCl.cl.Get(ctx, client.ObjectKey{
		Name: name,
	}, obj)
	lvgCl.metrics.APIMethodsDuration(lvgCl.controllerName, "get").Observe(lvgCl.metrics.GetEstimatedTimeInSeconds(start))
	lvgCl.metrics.APIMethodsExecutionCount(lvgCl.controllerName, "get").Inc()
	if err != nil {
		lvgCl.metrics.APIMethodsErrors(lvgCl.controllerName, "get").Inc()
		return nil, err
	}
	return obj, nil
}

func (lvgCl *LVGClient) UpdateLVGConditionIfNeeded(
	ctx context.Context,
	lvg *v1alpha1.LVMVolumeGroup,
	status v1.ConditionStatus,
	conType, reason, message string,
) error {
	log := lvgCl.log.WithName("UpdateLVGConditionIfNeeded").WithValues("lvgName", lvg.Name, "conditionType", conType)
	exist := false
	index := 0
	newCondition := v1.Condition{
		Type:               conType,
		Status:             status,
		ObservedGeneration: lvg.Generation,
		LastTransitionTime: v1.NewTime(time.Now()),
		Reason:             reason,
		Message:            message,
	}

	if lvg.Status.Conditions == nil {
		log.Debug("the LVMVolumeGroup conditions is nil. Initialize them")
		lvg.Status.Conditions = make([]v1.Condition, 0, 5)
	}

	if len(lvg.Status.Conditions) > 0 {
		log.Debug("there are some conditions in the LVMVolumeGroup. Tries to find a condition")
		for i, c := range lvg.Status.Conditions {
			if c.Type == conType {
				if checkIfEqualConditions(c, newCondition) {
					log.Debug("no need to update condition as new and old condition states are the same")
					return nil
				}

				index = i
				exist = true
				log := log.WithValues("index", index)
				log.Debug("a condition was found in the LVMVolumeGroup at the index")
			}
		}

		if !exist {
			log.Debug("a condition was not found. Append it in the end of the LVMVolumeGroup conditions")
			lvg.Status.Conditions = append(lvg.Status.Conditions, newCondition)
		} else {
			log := log.WithValues("index", index)
			log.Debug("insert the condition at index of the LVMVolumeGroup conditions",
				"status", status,
				"reason", reason,
				"message", message)
			lvg.Status.Conditions[index] = newCondition
		}
	} else {
		log.Debug("no conditions were found in the LVMVolumeGroup. Append the condition in the end")
		lvg.Status.Conditions = append(lvg.Status.Conditions, newCondition)
	}

	log.Debug("tries to update the condition",
		"status", status,
		"reason", reason,
		"message", message)
	return lvgCl.cl.Status().Update(ctx, lvg)
}

func (lvgCl *LVGClient) DeleteLVMVolumeGroup(
	ctx context.Context,
	lvg *v1alpha1.LVMVolumeGroup,
) error {
	log := lvgCl.log.WithName("DeleteLVMVolumeGroup").WithValues("lvgName", lvg.Name, "nodeName", lvgCl.currentNodeName, "vgName", lvg.Spec.ActualVGNameOnTheNode)
	log.Debug("Node does not belong to VG. It will be removed from LVM resource")
	for i, node := range lvg.Status.Nodes {
		if node.Name == lvgCl.currentNodeName {
			// delete node
			lvg.Status.Nodes = append(lvg.Status.Nodes[:i], lvg.Status.Nodes[i+1:]...)
			log.Info("deleted node from LVMVolumeGroup", "deletedNodeName", node.Name)
		}
	}

	// If current LVMVolumeGroup has no nodes left, delete it.
	if len(lvg.Status.Nodes) == 0 {
		start := time.Now()
		err := lvgCl.cl.Delete(ctx, lvg)
		lvgCl.metrics.APIMethodsDuration(lvgCl.controllerName, "delete").Observe(lvgCl.metrics.GetEstimatedTimeInSeconds(start))
		lvgCl.metrics.APIMethodsExecutionCount(lvgCl.controllerName, "delete").Inc()
		if err != nil {
			lvgCl.metrics.APIMethodsErrors(lvgCl.controllerName, "delete").Inc()
			return err
		}
		log.Info("the LVMVolumeGroup deleted")
	}

	return nil
}

func checkIfEqualConditions(first, second v1.Condition) bool {
	return first.Type == second.Type &&
		first.Status == second.Status &&
		first.Reason == second.Reason &&
		first.Message == second.Message &&
		first.ObservedGeneration == second.ObservedGeneration
}
