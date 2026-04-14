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

package llv_extender

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/cache"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/logger"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/monitoring"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/test_utils"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/utils"
)

const testNodeName = "test_node"

func TestLLVExtender(t *testing.T) {
	t.Run("ShouldReconcileCreate", func(t *testing.T) {
		t.Run("local_lvg_returns_true", func(t *testing.T) {
			r := setupExtenderReconciler()
			lvg := &v1alpha1.LVMVolumeGroup{}
			lvg.Spec.Local.NodeName = testNodeName
			assert.True(t, r.ShouldReconcileCreate(lvg))
		})

		t.Run("non_local_lvg_returns_false", func(t *testing.T) {
			r := setupExtenderReconciler()
			lvg := &v1alpha1.LVMVolumeGroup{}
			lvg.Spec.Local.NodeName = "other_node"
			assert.False(t, r.ShouldReconcileCreate(lvg))
		})
	})

	t.Run("ShouldReconcileUpdate", func(t *testing.T) {
		t.Run("local_lvg_returns_true", func(t *testing.T) {
			r := setupExtenderReconciler()
			lvg := &v1alpha1.LVMVolumeGroup{}
			lvg.Spec.Local.NodeName = testNodeName
			assert.True(t, r.ShouldReconcileUpdate(nil, lvg))
		})

		t.Run("non_local_lvg_returns_false", func(t *testing.T) {
			r := setupExtenderReconciler()
			lvg := &v1alpha1.LVMVolumeGroup{}
			lvg.Spec.Local.NodeName = "other_node"
			assert.False(t, r.ShouldReconcileUpdate(nil, lvg))
		})
	})

	t.Run("shouldLLVExtenderReconcileEvent", func(t *testing.T) {
		t.Run("empty_status_returns_false", func(t *testing.T) {
			r := setupExtenderReconciler()
			lvg := &v1alpha1.LVMVolumeGroup{}
			assert.False(t, r.shouldLLVExtenderReconcileEvent(lvg))
		})

		t.Run("not_belonging_to_node_returns_false", func(t *testing.T) {
			r := setupExtenderReconciler()
			lvg := &v1alpha1.LVMVolumeGroup{
				Status: v1alpha1.LVMVolumeGroupStatus{
					Phase: internal.PhaseReady,
					Nodes: []v1alpha1.LVMVolumeGroupNode{
						{Name: "other_node"},
					},
					ExtentSize: resource.MustParse("4Mi"),
				},
			}
			assert.False(t, r.shouldLLVExtenderReconcileEvent(lvg))
		})

		t.Run("not_ready_phase_returns_false", func(t *testing.T) {
			r := setupExtenderReconciler()
			lvg := &v1alpha1.LVMVolumeGroup{
				Status: v1alpha1.LVMVolumeGroupStatus{
					Phase: "NotReady",
					Nodes: []v1alpha1.LVMVolumeGroupNode{
						{Name: testNodeName},
					},
					ExtentSize: resource.MustParse("4Mi"),
				},
			}
			assert.False(t, r.shouldLLVExtenderReconcileEvent(lvg))
		})

		t.Run("zero_extent_size_returns_false", func(t *testing.T) {
			r := setupExtenderReconciler()
			lvg := &v1alpha1.LVMVolumeGroup{
				Status: v1alpha1.LVMVolumeGroupStatus{
					Phase: internal.PhaseReady,
					Nodes: []v1alpha1.LVMVolumeGroupNode{
						{Name: testNodeName},
					},
				},
			}
			assert.False(t, r.shouldLLVExtenderReconcileEvent(lvg))
		})

		t.Run("all_conditions_met_returns_true", func(t *testing.T) {
			r := setupExtenderReconciler()
			lvg := &v1alpha1.LVMVolumeGroup{
				Status: v1alpha1.LVMVolumeGroupStatus{
					Phase: internal.PhaseReady,
					Nodes: []v1alpha1.LVMVolumeGroupNode{
						{Name: testNodeName},
					},
					ExtentSize: resource.MustParse("4Mi"),
				},
			}
			assert.True(t, r.shouldLLVExtenderReconcileEvent(lvg))
		})
	})
}

func setupExtenderReconciler() *Reconciler {
	r := NewReconciler(
		test_utils.NewFakeClient(&v1alpha1.LVMVolumeGroup{}, &v1alpha1.LVMLogicalVolume{}),
		logger.Logger{},
		monitoring.GetMetrics(""),
		cache.New(),
		utils.NewCommands(),
		ReconcilerConfig{NodeName: testNodeName},
	)
	return r.(*Reconciler)
}
