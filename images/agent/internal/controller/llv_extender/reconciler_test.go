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
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/cache"
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

// A whole-VG volume must be left alone. The VG size LVM reports is the raw
// device sum and is not a multiple of the extent, so rounding a "100%FREE"
// request up asks for one extent more than the VG holds; the reconciler then
// decides the LV has to grow, finds no free space, and parks the volume in
// Failed with "unable to extend LV, err: not enough space". Whether a given
// disk hit it was decided by whether its VG size happened to be aligned, so a
// fifteen-OSD sds-elastic cluster lost a volume or two and never reached Ready.
func TestLLVExtenderPercentSizeVolumeIsNotExtended(t *testing.T) {
	const (
		vgNameOnNode = "test_vg"
		lvNameOnNode = "test_lv"
		lvgName      = "test_lvg"
		llvName      = "test_llv"
	)

	// 20470Mi is not a multiple of the 4Mi extent; the LV created with
	// `lvcreate -l 100%FREE` therefore stops at 20468Mi.
	vgSize := resource.MustParse("20470Mi")
	lvSize := resource.MustParse("20468Mi")
	extentSize := resource.MustParse("4Mi")

	llv := &v1alpha1.LVMLogicalVolume{
		ObjectMeta: metav1.ObjectMeta{Name: llvName},
		Spec: v1alpha1.LVMLogicalVolumeSpec{
			LVMVolumeGroupName:    lvgName,
			ActualLVNameOnTheNode: lvNameOnNode,
			Type:                  internal.Thick,
			Size:                  "100%FREE",
		},
		Status: &v1alpha1.LVMLogicalVolumeStatus{
			Phase:      v1alpha1.PhaseCreated,
			ActualSize: lvSize,
		},
	}

	lvg := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: metav1.ObjectMeta{Name: lvgName},
		Spec: v1alpha1.LVMVolumeGroupSpec{
			ActualVGNameOnTheNode: vgNameOnNode,
			Local:                 v1alpha1.LVMVolumeGroupLocalSpec{NodeName: testNodeName},
		},
		Status: v1alpha1.LVMVolumeGroupStatus{
			VGSize:     vgSize,
			VGFree:     *resource.NewQuantity(0, resource.BinarySI),
			ExtentSize: extentSize,
		},
	}

	r := NewReconciler(
		test_utils.NewFakeClient(&v1alpha1.LVMLogicalVolume{}, &v1alpha1.LVMVolumeGroup{}),
		logger.NewNop(),
		monitoring.GetMetrics(""),
		cache.New(),
		utils.NewCommands(),
		ReconcilerConfig{NodeName: testNodeName},
	).(*Reconciler)

	ctx := context.Background()
	// Status is a subresource on the fake client, so it is written separately
	// from the object itself.
	llvStatus := llv.Status
	llv.Status = nil
	require.NoError(t, r.cl.Create(ctx, llv))
	llv.Status = llvStatus
	require.NoError(t, r.cl.Status().Update(ctx, llv))
	require.NoError(t, r.cl.Create(ctx, lvg))

	r.sdsCache.StoreLVs([]internal.LVData{{
		LVName: lvNameOnNode,
		VGName: vgNameOnNode,
		LVSize: lvSize,
	}}, bytes.Buffer{})

	shouldRetry := r.ReconcileLVMLogicalVolumeExtension(ctx, lvg)
	assert.False(t, shouldRetry, "a volume that already owns every free extent needs no retry")

	updated := &v1alpha1.LVMLogicalVolume{}
	require.NoError(t, r.cl.Get(ctx, client.ObjectKey{Name: llvName}, updated))
	require.NotNil(t, updated.Status)
	assert.Equal(t, v1alpha1.PhaseCreated, updated.Status.Phase,
		"the volume must stay Created; extending it would fail with \"not enough space\"")
}

// A "not enough space" verdict is computed from the group's free space, which
// lags behind the LV the agent has just created. The volume this test starts
// from is the shape a run leaves behind: the LV is already the size it asked
// for, but an earlier pass - which saw free extents that were in truth already
// spent - parked the volume in Failed. Nothing else ever revisits it, so the
// phase has to be cleared here, on the pass that finds the volume correct.
func TestLLVExtenderClearsAStaleFailure(t *testing.T) {
	const (
		vgNameOnNode = "test_vg"
		lvNameOnNode = "test_lv"
		lvgName      = "test_lvg"
		llvName      = "test_llv"
	)

	vgSize := resource.MustParse("20476Mi")
	lvSize := resource.MustParse("20476Mi")
	extentSize := resource.MustParse("4Mi")

	llv := &v1alpha1.LVMLogicalVolume{
		ObjectMeta: metav1.ObjectMeta{Name: llvName},
		Spec: v1alpha1.LVMLogicalVolumeSpec{
			LVMVolumeGroupName:    lvgName,
			ActualLVNameOnTheNode: lvNameOnNode,
			Type:                  internal.Thick,
			Size:                  "100%FREE",
		},
		Status: &v1alpha1.LVMLogicalVolumeStatus{
			Phase:      v1alpha1.PhaseFailed,
			Reason:     "unable to extend LV, err: not enough space",
			ActualSize: lvSize,
		},
	}

	lvg := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: metav1.ObjectMeta{Name: lvgName},
		Spec: v1alpha1.LVMVolumeGroupSpec{
			ActualVGNameOnTheNode: vgNameOnNode,
			Local:                 v1alpha1.LVMVolumeGroupLocalSpec{NodeName: testNodeName},
		},
		Status: v1alpha1.LVMVolumeGroupStatus{
			VGSize:     vgSize,
			VGFree:     *resource.NewQuantity(0, resource.BinarySI),
			ExtentSize: extentSize,
		},
	}

	r := NewReconciler(
		test_utils.NewFakeClient(&v1alpha1.LVMLogicalVolume{}, &v1alpha1.LVMVolumeGroup{}),
		logger.NewNop(),
		monitoring.GetMetrics(""),
		cache.New(),
		utils.NewCommands(),
		ReconcilerConfig{NodeName: testNodeName},
	).(*Reconciler)

	ctx := context.Background()
	llvStatus := llv.Status
	llv.Status = nil
	require.NoError(t, r.cl.Create(ctx, llv))
	llv.Status = llvStatus
	require.NoError(t, r.cl.Status().Update(ctx, llv))
	require.NoError(t, r.cl.Create(ctx, lvg))

	r.sdsCache.StoreLVs([]internal.LVData{{
		LVName: lvNameOnNode,
		VGName: vgNameOnNode,
		LVSize: lvSize,
	}}, bytes.Buffer{})

	shouldRetry := r.ReconcileLVMLogicalVolumeExtension(ctx, lvg)
	assert.False(t, shouldRetry, "clearing the phase of a correct volume needs no retry")

	updated := &v1alpha1.LVMLogicalVolume{}
	require.NoError(t, r.cl.Get(ctx, client.ObjectKey{Name: llvName}, updated))
	require.NotNil(t, updated.Status)
	assert.Equal(t, v1alpha1.PhaseCreated, updated.Status.Phase,
		"a volume that is already the size it asked for must not stay Failed")
	assert.Empty(t, updated.Status.Reason, "the stale failure message must go with the phase")
}

func setupExtenderReconciler() *Reconciler {
	r := NewReconciler(
		test_utils.NewFakeClient(&v1alpha1.LVMVolumeGroup{}, &v1alpha1.LVMLogicalVolume{}),
		logger.NewNop(),
		monitoring.GetMetrics(""),
		cache.New(),
		utils.NewCommands(),
		ReconcilerConfig{NodeName: testNodeName},
	)
	return r.(*Reconciler)
}
