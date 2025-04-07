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

package utils

import (
	"context"
	"fmt"
	"time"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/logger"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/monitoring"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
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
		lvgCl.log.Debug(fmt.Sprintf("[updateLVGConditionIfNeeded] the LVMVolumeGroup %s conditions is nil. Initialize them", lvg.Name))
		lvg.Status.Conditions = make([]v1.Condition, 0, 5)
	}

	if len(lvg.Status.Conditions) > 0 {
		lvgCl.log.Debug(fmt.Sprintf("[updateLVGConditionIfNeeded] there are some conditions in the LVMVolumeGroup %s. Tries to find a condition %s", lvg.Name, conType))
		for i, c := range lvg.Status.Conditions {
			if c.Type == conType {
				if checkIfEqualConditions(c, newCondition) {
					lvgCl.log.Debug(fmt.Sprintf("[updateLVGConditionIfNeeded] no need to update condition %s in the LVMVolumeGroup %s as new and old condition states are the same", conType, lvg.Name))
					return nil
				}

				index = i
				exist = true
				lvgCl.log.Debug(fmt.Sprintf("[updateLVGConditionIfNeeded] a condition %s was found in the LVMVolumeGroup %s at the index %d", conType, lvg.Name, i))
			}
		}

		if !exist {
			lvgCl.log.Debug(fmt.Sprintf("[updateLVGConditionIfNeeded] a condition %s was not found. Append it in the end of the LVMVolumeGroup %s conditions", conType, lvg.Name))
			lvg.Status.Conditions = append(lvg.Status.Conditions, newCondition)
		} else {
			lvgCl.log.Debug(fmt.Sprintf("[updateLVGConditionIfNeeded] insert the condition %s status %s reason %s message %s at index %d of the LVMVolumeGroup %s conditions", conType, status, reason, message, index, lvg.Name))
			lvg.Status.Conditions[index] = newCondition
		}
	} else {
		lvgCl.log.Debug(fmt.Sprintf("[updateLVGConditionIfNeeded] no conditions were found in the LVMVolumeGroup %s. Append the condition %s in the end", lvg.Name, conType))
		lvg.Status.Conditions = append(lvg.Status.Conditions, newCondition)
	}

	lvgCl.log.Debug(fmt.Sprintf("[updateLVGConditionIfNeeded] tries to update the condition type %s status %s reason %s message %s of the LVMVolumeGroup %s", conType, status, reason, message, lvg.Name))
	return lvgCl.cl.Status().Update(ctx, lvg)
}

func (lvgCl *LVGClient) DeleteLVMVolumeGroup(
	ctx context.Context,
	lvg *v1alpha1.LVMVolumeGroup,
) error {
	lvgCl.log.Debug(fmt.Sprintf(`[DeleteLVMVolumeGroup] Node "%s" does not belong to VG "%s". It will be removed from LVM resource, name "%s"'`, lvgCl.currentNodeName, lvg.Spec.ActualVGNameOnTheNode, lvg.Name))
	for i, node := range lvg.Status.Nodes {
		if node.Name == lvgCl.currentNodeName {
			// delete node
			lvg.Status.Nodes = append(lvg.Status.Nodes[:i], lvg.Status.Nodes[i+1:]...)
			lvgCl.log.Info(fmt.Sprintf(`[DeleteLVMVolumeGroup] deleted node "%s" from LVMVolumeGroup "%s"`, node.Name, lvg.Name))
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
		lvgCl.log.Info(fmt.Sprintf("[DeleteLVMVolumeGroup] the LVMVolumeGroup %s deleted", lvg.Name))
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
