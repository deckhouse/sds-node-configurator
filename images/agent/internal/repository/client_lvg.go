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
	"fmt"
	"time"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-common-lib/conditions"
	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/logger"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/monitoring"
)

type LVGClient struct {
	cl                              client.Client
	log                             logger.Logger
	currentNodeName, controllerName string
	metrics                         *monitoring.Metrics
}

func NewLVGClient(
	cl client.Client,
	log logger.Logger,
	metrics *monitoring.Metrics,
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

// UpdateLVGConditionIfNeeded sets a condition on the LVMVolumeGroup, skipping the
// write when nothing changed and retrying on conflict.
// On success, lvg.Status and lvg.ResourceVersion are replaced with server-side values.
// Callers must not hold unsaved in-memory Status modifications before calling this method.
func (lvgCl *LVGClient) UpdateLVGConditionIfNeeded(
	ctx context.Context,
	lvg *v1alpha1.LVMVolumeGroup,
	status v1.ConditionStatus,
	conType, reason, message string,
) error {
	lvgCl.log.Debug(fmt.Sprintf("[UpdateLVGConditionIfNeeded] set the condition type %s status %s reason %s message %s on the LVMVolumeGroup %s", conType, status, reason, message, lvg.Name))

	// The state UpdateStatus read and wrote, kept so the server-side values can be
	// mirrored onto the caller's object below.
	var written *v1alpha1.LVMVolumeGroup

	err := conditions.UpdateStatus(ctx, lvgCl.cl, lvg, func(fresh *v1alpha1.LVMVolumeGroup) {
		written = fresh
		conditions.Set(&fresh.Status.Conditions, v1.Condition{
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
