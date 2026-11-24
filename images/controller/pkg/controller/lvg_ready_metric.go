/*
Copyright 2026 Flant JSC

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
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/deckhouse/sds-common-lib/conditions"
	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/controller/internal"
	"github.com/deckhouse/sds-node-configurator/images/controller/pkg/monitoring"
)

const lvgReadyListTimeout = 5 * time.Second

var lvgReadinessCausePriority = []string{
	internal.TypeNodeReady,
	internal.TypeAgentReady,
	internal.TypeVGReady,
	internal.TypeVGConfigurationApplied,
}

func RunLVGReadyMetric(mgr manager.Manager, log logger.Logger) error {
	return monitoring.RegisterLVGReadiness(ctrlmetrics.Registry, lvgReadinessSource(mgr.GetClient(), mgr.Elected(), log.Named("lvg-ready-metric")))
}

func lvgReadinessSource(cl client.Reader, elected <-chan struct{}, log logger.Logger) func() []monitoring.LVGReadiness {
	return func() []monitoring.LVGReadiness {
		select {
		case <-elected:
		default:
			return nil
		}

		ctx, cancel := context.WithTimeout(context.Background(), lvgReadyListTimeout)
		defer cancel()

		lvgs := &v1alpha1.LVMVolumeGroupList{}
		if err := cl.List(ctx, lvgs, client.UnsafeDisableDeepCopy); err != nil {
			log.Error("unable to list the LVMVolumeGroups", logger.Err(err))
			return nil
		}

		result := make([]monitoring.LVGReadiness, 0, len(lvgs.Items))
		for i := range lvgs.Items {
			result = append(result, lvgReadiness(&lvgs.Items[i]))
		}
		return result
	}
}

func lvgReadiness(lvg *v1alpha1.LVMVolumeGroup) monitoring.LVGReadiness {
	r := monitoring.LVGReadiness{
		Name:      lvg.Name,
		Node:      lvg.Spec.Local.NodeName,
		Condition: internal.TypeReady,
		Reason:    conditions.ReasonPending,
	}

	ready := conditions.Get(lvg.Status.Conditions, internal.TypeReady)
	if ready != nil && ready.Status == metav1.ConditionTrue {
		r.Ready = true
		r.Reason = ready.Reason
		return r
	}

	if cause := lvgReadinessCause(lvg); cause != nil {
		r.Condition = cause.Type
		r.Reason = cause.Reason
		return r
	}

	if ready != nil {
		r.Reason = ready.Reason
	}
	return r
}

func lvgReadinessCause(lvg *v1alpha1.LVMVolumeGroup) *metav1.Condition {
	blocking := func(c *metav1.Condition) bool {
		return c != nil &&
			c.Type != internal.TypeReady &&
			c.Reason != internal.ReasonCreating &&
			c.Reason != internal.ReasonTerminating &&
			blocksTheVerdict(c)
	}

	for _, conType := range lvgReadinessCausePriority {
		if c := conditions.Get(lvg.Status.Conditions, conType); blocking(c) {
			return c
		}
	}

	for i := range lvg.Status.Conditions {
		if c := &lvg.Status.Conditions[i]; blocking(c) {
			return c
		}
	}

	return nil
}
