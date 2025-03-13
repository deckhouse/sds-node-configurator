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

package llv

import (
	"bytes"
	"context"
	"testing"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/cache"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/logger"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/monitoring"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/test_utils"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/utils"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestLVMLogicalVolumeWatcher(t *testing.T) {
	var (
		vgName = "test-vg"
		ctx    = context.Background()
	)

	t.Run("subtractQuantity_returns_correct_value", func(t *testing.T) {
		mini := resource.NewQuantity(1000, resource.BinarySI)
		sub := resource.NewQuantity(300, resource.BinarySI)
		expected := resource.NewQuantity(700, resource.BinarySI)

		actual := subtractQuantity(*mini, *sub)
		assert.Equal(t, expected, &actual)
	})

	t.Run("checkIfLVBelongsToLLV", func(t *testing.T) {
		t.Run("llv_thin_returns_true", func(t *testing.T) {
			const poolName = "test-pool"
			llv := &v1alpha1.LVMLogicalVolume{
				ObjectMeta: v1.ObjectMeta{},
				Spec: v1alpha1.LVMLogicalVolumeSpec{
					Type: internal.Thin,
					Thin: &v1alpha1.LVMLogicalVolumeThinSpec{PoolName: poolName},
				},
			}
			lv := &internal.LVData{PoolName: poolName}

			assert.True(t, checkIfLVBelongsToLLV(llv, lv))
		})

		t.Run("llv_thin_returns_false", func(t *testing.T) {
			const poolName = "test-pool"
			llv := &v1alpha1.LVMLogicalVolume{
				ObjectMeta: v1.ObjectMeta{},
				Spec: v1alpha1.LVMLogicalVolumeSpec{
					Type: internal.Thin,
					Thin: &v1alpha1.LVMLogicalVolumeThinSpec{PoolName: poolName},
				},
			}
			lv := &internal.LVData{PoolName: "another-name"}

			assert.False(t, checkIfLVBelongsToLLV(llv, lv))
		})

		t.Run("llv_thick_returns_true", func(t *testing.T) {
			llv := &v1alpha1.LVMLogicalVolume{
				ObjectMeta: v1.ObjectMeta{},
				Spec: v1alpha1.LVMLogicalVolumeSpec{
					Type: internal.Thick,
				},
			}
			lv := &internal.LVData{LVAttr: "-wi-a-----"}

			assert.True(t, checkIfLVBelongsToLLV(llv, lv))
		})

		t.Run("llv_thick_returns_false", func(t *testing.T) {
			llv := &v1alpha1.LVMLogicalVolume{
				ObjectMeta: v1.ObjectMeta{},
				Spec: v1alpha1.LVMLogicalVolumeSpec{
					Type: internal.Thick,
				},
			}
			lv1 := &internal.LVData{LVAttr: "Vwi-a-----"}
			lv2 := &internal.LVData{LVAttr: "twi-a-----"}
			lv3 := &internal.LVData{LVAttr: "-wc-a-----"}

			assert.False(t, checkIfLVBelongsToLLV(llv, lv1))
			assert.False(t, checkIfLVBelongsToLLV(llv, lv2))
			assert.False(t, checkIfLVBelongsToLLV(llv, lv3))
		})
	})

	t.Run("validateLVMLogicalVolume", func(t *testing.T) {
		t.Run("thick_all_good_returns_true", func(t *testing.T) {
			const lvgName = "test-lvg"

			r := setupReconciler()

			lvg := &v1alpha1.LVMVolumeGroup{
				ObjectMeta: v1.ObjectMeta{
					Name: lvgName,
				},
			}

			err := r.cl.Create(ctx, lvg)
			if err != nil {
				t.Error(err)
			} else {
				defer func() {
					err = r.cl.Delete(ctx, lvg)
					if err != nil {
						t.Error(err)
					}
				}()
			}

			llv := &v1alpha1.LVMLogicalVolume{
				Spec: v1alpha1.LVMLogicalVolumeSpec{
					ActualLVNameOnTheNode: "test-lv",
					Type:                  internal.Thick,
					Size:                  "10M",
					LVMVolumeGroupName:    lvgName,
				},
			}

			v, reason := r.validateLVMLogicalVolume(llv, lvg)
			if assert.True(t, v) {
				assert.Equal(t, 0, len(reason))
			}
		})

		t.Run("thick_all_bad_returns_false", func(t *testing.T) {
			lvName := "test-lv"

			r := setupReconciler()

			llv := &v1alpha1.LVMLogicalVolume{
				Spec: v1alpha1.LVMLogicalVolumeSpec{
					ActualLVNameOnTheNode: lvName,
					Type:                  internal.Thick,
					Size:                  "0M",
					LVMVolumeGroupName:    "some-lvg",
					Thin:                  &v1alpha1.LVMLogicalVolumeThinSpec{PoolName: "some-lvg"},
				},
			}

			r.sdsCache.StoreLVs([]internal.LVData{
				{
					LVName: lvName,
				},
			}, bytes.Buffer{})

			v, reason := r.validateLVMLogicalVolume(llv, &v1alpha1.LVMVolumeGroup{})
			if assert.False(t, v) {
				assert.Equal(t, "Zero size for LV. Thin pool specified for Thick LV. ", reason)
			}
		})

		t.Run("thin_all_good_returns_true", func(t *testing.T) {
			const (
				lvgName = "test-lvg"
				tpName  = "test-tp"
			)

			r := setupReconciler()

			lvg := &v1alpha1.LVMVolumeGroup{
				ObjectMeta: v1.ObjectMeta{
					Name: lvgName,
				},
				Status: v1alpha1.LVMVolumeGroupStatus{
					ThinPools: []v1alpha1.LVMVolumeGroupThinPoolStatus{
						{
							Name:            tpName,
							AllocationLimit: internal.AllocationLimitDefaultValue,
						},
					},
				},
			}

			llv := &v1alpha1.LVMLogicalVolume{
				Spec: v1alpha1.LVMLogicalVolumeSpec{
					ActualLVNameOnTheNode: "test-lv",
					Type:                  internal.Thin,
					Size:                  "10M",
					LVMVolumeGroupName:    lvgName,
					Thin:                  &v1alpha1.LVMLogicalVolumeThinSpec{PoolName: tpName},
				},
			}

			v, reason := r.validateLVMLogicalVolume(llv, lvg)
			if assert.True(t, v) {
				assert.Equal(t, 0, len(reason))
			}
		})

		t.Run("thin_all_bad_returns_false", func(t *testing.T) {
			r := setupReconciler()

			llv := &v1alpha1.LVMLogicalVolume{
				Spec: v1alpha1.LVMLogicalVolumeSpec{
					ActualLVNameOnTheNode: "",
					Type:                  internal.Thin,
					Size:                  "0M",
					LVMVolumeGroupName:    "some-lvg",
				},
			}

			r.sdsCache.StoreLVs([]internal.LVData{
				{
					LVName: "test-lv",
				},
			}, bytes.Buffer{})

			v, reason := r.validateLVMLogicalVolume(llv, &v1alpha1.LVMVolumeGroup{})
			if assert.False(t, v) {
				assert.Equal(t, "No LV name specified. Zero size for LV. No thin pool specified. ", reason)
			}
		})
	})

	t.Run("getThinPoolAvailableSpace", func(t *testing.T) {
		free, err := utils.GetThinPoolAvailableSpace(
			resource.MustParse("10Gi"),
			resource.MustParse("5Gi"),
			internal.AllocationLimitDefaultValue,
		)
		if err != nil {
			t.Error(err)
		}
		expected := resource.MustParse("10Gi")

		assert.Equal(t, expected.Value(), free.Value())
	})

	t.Run("belongToNode", func(t *testing.T) {
		const (
			nodeName = "test_node"
		)
		lvg := &v1alpha1.LVMVolumeGroup{
			Status: v1alpha1.LVMVolumeGroupStatus{
				Nodes: []v1alpha1.LVMVolumeGroupNode{
					{
						Name: nodeName,
					},
				},
			},
		}

		t.Run("returns_true", func(t *testing.T) {
			belongs := utils.LVGBelongsToNode(lvg, nodeName)
			assert.True(t, belongs)
		})

		t.Run("returns_false", func(t *testing.T) {
			belongs := utils.LVGBelongsToNode(lvg, "other_node")
			assert.False(t, belongs)
		})
	})

	t.Run("identifyReconcileFunc", func(t *testing.T) {
		t.Run("returns_create", func(t *testing.T) {
			r := setupReconciler()
			llv := &v1alpha1.LVMLogicalVolume{}

			actual := r.identifyReconcileFunc(vgName, llv)

			assert.Equal(t, internal.CreateReconcile, actual)
		})

		t.Run("returns_update", func(t *testing.T) {
			lvName := "test-lv"
			r := setupReconciler()

			llv := &v1alpha1.LVMLogicalVolume{
				Spec: v1alpha1.LVMLogicalVolumeSpec{
					ActualLVNameOnTheNode: lvName,
				},
				Status: &v1alpha1.LVMLogicalVolumeStatus{
					Phase: v1alpha1.PhaseCreated,
				},
			}
			r.sdsCache.StoreLVs([]internal.LVData{
				{
					LVName: lvName,
					VGName: vgName,
				},
			}, bytes.Buffer{})

			actual := r.identifyReconcileFunc(vgName, llv)

			assert.Equal(t, internal.UpdateReconcile, actual)
		})

		t.Run("returns_delete", func(t *testing.T) {
			r := setupReconciler()

			llv := &v1alpha1.LVMLogicalVolume{
				ObjectMeta: v1.ObjectMeta{DeletionTimestamp: &v1.Time{}},
				Status: &v1alpha1.LVMLogicalVolumeStatus{
					Phase: v1alpha1.PhaseCreated,
				},
			}

			actual := r.identifyReconcileFunc(vgName, llv)

			assert.Equal(t, internal.DeleteReconcile, actual)
		})
	})

	t.Run("shouldReconcileByCreateFunc", func(t *testing.T) {
		t.Run("if_lv_is_not_created_returns_true", func(t *testing.T) {
			r := setupReconciler()

			lvName := "test-lv"
			llv := &v1alpha1.LVMLogicalVolume{
				Spec: v1alpha1.LVMLogicalVolumeSpec{
					ActualLVNameOnTheNode: lvName,
				},
				Status: &v1alpha1.LVMLogicalVolumeStatus{
					Phase: v1alpha1.PhaseCreated,
				},
			}

			should := r.shouldReconcileByCreateFunc(vgName, llv)
			assert.True(t, should)
		})

		t.Run("if_lv_is_created_returns_false", func(t *testing.T) {
			r := setupReconciler()
			lvName := "test-lv"
			llv := &v1alpha1.LVMLogicalVolume{
				Spec: v1alpha1.LVMLogicalVolumeSpec{
					ActualLVNameOnTheNode: lvName,
				},
				Status: &v1alpha1.LVMLogicalVolumeStatus{
					Phase: v1alpha1.PhaseCreated,
				},
			}
			r.sdsCache.StoreLVs([]internal.LVData{
				{
					LVName: lvName,
					VGName: vgName,
				},
			}, bytes.Buffer{})
			should := r.shouldReconcileByCreateFunc(vgName, llv)
			assert.False(t, should)
		})

		t.Run("if_deletion_timestamp_is_not_nil_returns_false", func(t *testing.T) {
			r := setupReconciler()
			lvName := "test-lv"
			llv := &v1alpha1.LVMLogicalVolume{
				ObjectMeta: v1.ObjectMeta{DeletionTimestamp: &v1.Time{}},
				Spec: v1alpha1.LVMLogicalVolumeSpec{
					ActualLVNameOnTheNode: lvName,
				},
				Status: &v1alpha1.LVMLogicalVolumeStatus{
					Phase: v1alpha1.PhaseCreated,
				},
			}
			should := r.shouldReconcileByCreateFunc(vgName, llv)
			assert.False(t, should)
		})
	})

	t.Run("shouldReconcileByUpdateFunc", func(t *testing.T) {
		t.Run("if_deletion_timestamp_is_not_nill_returns_false", func(t *testing.T) {
			r := setupReconciler()
			llv := &v1alpha1.LVMLogicalVolume{
				ObjectMeta: v1.ObjectMeta{
					DeletionTimestamp: &v1.Time{},
				},
			}

			should := r.shouldReconcileByUpdateFunc(vgName, llv)
			assert.False(t, should)
		})

		t.Run("if_lv_exists_returns_true", func(t *testing.T) {
			r := setupReconciler()
			lvName := "test-lv"
			llv := &v1alpha1.LVMLogicalVolume{
				Spec: v1alpha1.LVMLogicalVolumeSpec{
					ActualLVNameOnTheNode: lvName,
				},
				Status: &v1alpha1.LVMLogicalVolumeStatus{
					Phase: v1alpha1.PhaseCreated,
				},
			}
			r.sdsCache.StoreLVs([]internal.LVData{
				{
					LVName: lvName,
					VGName: vgName,
				},
			}, bytes.Buffer{})
			should := r.shouldReconcileByUpdateFunc(vgName, llv)
			assert.True(t, should)
		})

		t.Run("if_lv_does_not_exist_returns_false", func(t *testing.T) {
			r := setupReconciler()
			lvName := "test-lv"
			llv := &v1alpha1.LVMLogicalVolume{
				Spec: v1alpha1.LVMLogicalVolumeSpec{
					ActualLVNameOnTheNode: lvName,
				},
				Status: &v1alpha1.LVMLogicalVolumeStatus{
					Phase: v1alpha1.PhaseCreated,
				},
			}
			should := r.shouldReconcileByUpdateFunc(vgName, llv)
			assert.False(t, should)
		})
	})

	t.Run("shouldReconcileByDeleteFunc", func(t *testing.T) {
		t.Run("if_deletion_timestamp_is_not_nil_returns_true", func(t *testing.T) {
			llv := &v1alpha1.LVMLogicalVolume{
				ObjectMeta: v1.ObjectMeta{DeletionTimestamp: &v1.Time{}},
			}

			should := shouldReconcileByDeleteFunc(llv)

			assert.True(t, should)
		})

		t.Run("if_deletion_timestamp_is_nil_returns_false", func(t *testing.T) {
			llv := &v1alpha1.LVMLogicalVolume{}

			should := shouldReconcileByDeleteFunc(llv)

			assert.False(t, should)
		})
	})

	t.Run("updateLVMLogicalVolumePhaseIfNeeded", func(t *testing.T) {
		const reason = "test_reason"
		r := setupReconciler()
		llv := &v1alpha1.LVMLogicalVolume{
			ObjectMeta: v1.ObjectMeta{
				Name: "test",
			},
			Status: &v1alpha1.LVMLogicalVolumeStatus{
				Phase:  v1alpha1.PhaseCreated,
				Reason: "",
			},
		}

		err := r.cl.Create(ctx, llv)
		if err != nil {
			t.Error(err)
			return
		}

		defer func() {
			err = r.cl.Delete(ctx, llv)
			if err != nil {
				t.Error(err)
			}
		}()

		err = r.llvCl.UpdatePhaseIfNeeded(ctx, llv, v1alpha1.PhaseFailed, reason)
		if assert.NoError(t, err) {
			newLLV := &v1alpha1.LVMLogicalVolume{}
			err = r.cl.Get(ctx, client.ObjectKey{
				Name:      llv.Name,
				Namespace: "",
			}, newLLV)

			assert.Equal(t, newLLV.Status.Phase, v1alpha1.PhaseFailed)
			assert.Equal(t, newLLV.Status.Reason, reason)
		}
	})

	t.Run("addLLVFinalizerIfNotExist", func(t *testing.T) {
		t.Run("no_finalizer_adds_one_returns_true", func(t *testing.T) {
			const (
				name = "test-name1"
			)
			r := setupReconciler()
			llv := &v1alpha1.LVMLogicalVolume{
				ObjectMeta: v1.ObjectMeta{
					Name:       name,
					Finalizers: []string{},
				},
			}

			err := r.cl.Create(ctx, llv)
			if err != nil {
				t.Error(err)
				return
			}

			defer func() {
				err = r.cl.Delete(ctx, llv)
				if err != nil {
					t.Error(err)
				}
			}()

			added, err := r.addLLVFinalizerIfNotExist(ctx, llv)
			if assert.NoError(t, err) {
				assert.True(t, added)

				newLLV := &v1alpha1.LVMLogicalVolume{}
				err = r.cl.Get(ctx, client.ObjectKey{
					Name:      llv.Name,
					Namespace: "",
				}, newLLV)

				assert.Contains(t, newLLV.Finalizers, internal.SdsNodeConfiguratorFinalizer)
			}
		})

		t.Run("finalizer_exists_do_not_add_finalizer_returns_false", func(t *testing.T) {
			const (
				name = "test-name2"
			)
			r := setupReconciler()
			llv := &v1alpha1.LVMLogicalVolume{
				ObjectMeta: v1.ObjectMeta{
					Name:       name,
					Finalizers: []string{internal.SdsNodeConfiguratorFinalizer},
				},
			}

			err := r.cl.Create(ctx, llv)
			if err != nil {
				t.Error(err)
				return
			}

			defer func() {
				err = r.cl.Delete(ctx, llv)
				if err != nil {
					t.Error(err)
				}
			}()

			added, err := r.addLLVFinalizerIfNotExist(ctx, llv)
			if assert.NoError(t, err) {
				assert.False(t, added)

				newLLV := &v1alpha1.LVMLogicalVolume{}
				err = r.cl.Get(ctx, client.ObjectKey{
					Name:      llv.Name,
					Namespace: "",
				}, newLLV)

				assert.Contains(t, newLLV.Finalizers, internal.SdsNodeConfiguratorFinalizer)
			}
		})
	})

	t.Run("updateLVMLogicalVolumeSpec", func(t *testing.T) {
		const (
			lvgName = "test-lvg"
		)
		var (
			oldSize = resource.NewQuantity(100000000, resource.BinarySI)
			newSize = resource.NewQuantity(200000000, resource.BinarySI)
		)
		r := setupReconciler()
		llv := &v1alpha1.LVMLogicalVolume{
			ObjectMeta: v1.ObjectMeta{
				Name: lvgName,
			},
			Spec: v1alpha1.LVMLogicalVolumeSpec{
				ActualLVNameOnTheNode: "",
				Type:                  "",
				Size:                  oldSize.String(),
			},
			Status: &v1alpha1.LVMLogicalVolumeStatus{
				Phase:      v1alpha1.PhasePending,
				Reason:     "",
				ActualSize: *oldSize,
			},
		}

		err := r.cl.Create(ctx, llv)
		if err != nil {
			t.Error(err)
			return
		}

		defer func() {
			err = r.cl.Delete(ctx, llv)
			if err != nil {
				t.Error(err)
			}
		}()

		oldLLV := &v1alpha1.LVMLogicalVolume{}
		err = r.cl.Get(ctx, client.ObjectKey{
			Name: llv.Name,
		}, oldLLV)
		if err != nil {
			t.Error(err)
			return
		}

		if assert.NotNil(t, oldLLV) {
			assert.Equal(t, v1alpha1.PhasePending, oldLLV.Status.Phase)
			assert.Equal(t, oldSize.Value(), oldLLV.Status.ActualSize.Value())
		}

		oldLLV.Spec.Size = newSize.String()
		oldLLV.Status.Phase = v1alpha1.PhaseCreated
		oldLLV.Status.ActualSize = *newSize

		err = r.updateLVMLogicalVolumeSpec(ctx, oldLLV)
		if assert.NoError(t, err) {
			newLLV := &v1alpha1.LVMLogicalVolume{}
			err = r.cl.Get(ctx, client.ObjectKey{
				Name: llv.Name,
			}, newLLV)
			if err != nil {
				t.Error(err)
				return
			}

			assert.Equal(t, v1alpha1.PhasePending, newLLV.Status.Phase)
			assert.Equal(t, oldSize.Value(), newLLV.Status.ActualSize.Value())
		}
	})

	t.Run("updateLLVPhaseToCreatedIfNeeded", func(t *testing.T) {
		const (
			lvgName = "test-lvg"
		)
		var (
			oldSize = resource.NewQuantity(100000000, resource.BinarySI)
			newSize = resource.NewQuantity(200000000, resource.BinarySI)
		)
		r := setupReconciler()
		llv := &v1alpha1.LVMLogicalVolume{
			ObjectMeta: v1.ObjectMeta{
				Name: lvgName,
			},
			Spec: v1alpha1.LVMLogicalVolumeSpec{
				ActualLVNameOnTheNode: "",
				Type:                  "",
				Size:                  oldSize.String(),
			},
			Status: &v1alpha1.LVMLogicalVolumeStatus{
				Phase:      v1alpha1.PhasePending,
				Reason:     "",
				ActualSize: *oldSize,
			},
		}

		err := r.cl.Create(ctx, llv)
		if err != nil {
			t.Error(err)
			return
		}

		defer func() {
			err = r.cl.Delete(ctx, llv)
			if err != nil {
				t.Error(err)
			}
		}()

		oldLLV := &v1alpha1.LVMLogicalVolume{}
		err = r.cl.Get(ctx, client.ObjectKey{
			Name: llv.Name,
		}, oldLLV)
		if err != nil {
			t.Error(err)
			return
		}

		if assert.NotNil(t, oldLLV) {
			assert.Equal(t, v1alpha1.PhasePending, oldLLV.Status.Phase)
			assert.Equal(t, oldSize.Value(), oldLLV.Status.ActualSize.Value())
		}

		oldLLV.Spec.Size = newSize.String()

		err = r.llvCl.UpdatePhaseToCreatedIfNeeded(ctx, oldLLV, *newSize)
		if assert.NoError(t, err) {
			newLLV := &v1alpha1.LVMLogicalVolume{}
			err = r.cl.Get(ctx, client.ObjectKey{
				Name: llv.Name,
			}, newLLV)
			if err != nil {
				t.Error(err)
				return
			}

			assert.Equal(t, oldSize.String(), newLLV.Spec.Size)
			assert.Equal(t, v1alpha1.PhaseCreated, newLLV.Status.Phase)
			assert.Equal(t, newSize.Value(), newLLV.Status.ActualSize.Value())
		}
	})

	t.Run("removeLLVFinalizersIfExist", func(t *testing.T) {
		r := setupReconciler()
		llv := &v1alpha1.LVMLogicalVolume{
			ObjectMeta: v1.ObjectMeta{
				Name:       "test-name",
				Finalizers: []string{internal.SdsNodeConfiguratorFinalizer},
			},
		}
		err := r.cl.Create(ctx, llv)
		if err != nil {
			t.Error(err)
			return
		}

		defer func() {
			err = r.cl.Delete(ctx, llv)
			if err != nil {
				t.Error(err)
			}
		}()

		llvWithFinalizer := &v1alpha1.LVMLogicalVolume{}
		err = r.cl.Get(ctx, client.ObjectKey{
			Name: llv.Name,
		}, llvWithFinalizer)
		if err != nil {
			t.Error(err)
			return
		}

		assert.Contains(t, llvWithFinalizer.Finalizers, internal.SdsNodeConfiguratorFinalizer)

		err = r.removeLLVFinalizersIfExist(ctx, llv)
		if assert.NoError(t, err) {
			llvNoFinalizer := &v1alpha1.LVMLogicalVolume{}
			err = r.cl.Get(ctx, client.ObjectKey{
				Name: llv.Name,
			}, llvNoFinalizer)
			if err != nil {
				t.Error(err)
				return
			}

			assert.NotContains(t, llv.Finalizers, internal.SdsNodeConfiguratorFinalizer)
		}
	})

	t.Run("AreSizesEqualWithinDelta", func(t *testing.T) {
		t.Run("returns_true", func(t *testing.T) {
			size := 10000000000

			left := resource.NewQuantity(int64(size), resource.BinarySI)
			right := resource.NewQuantity(int64(size)+internal.ResizeDelta.Value()-1, resource.BinarySI)

			equal := utils.AreSizesEqualWithinDelta(*left, *right, internal.ResizeDelta)

			assert.True(t, equal)
		})

		t.Run("returns_false", func(t *testing.T) {
			size := 10000000000

			left := resource.NewQuantity(int64(size), resource.BinarySI)
			right := resource.NewQuantity(int64(size)+internal.ResizeDelta.Value(), resource.BinarySI)

			equal := utils.AreSizesEqualWithinDelta(*left, *right, internal.ResizeDelta)

			assert.False(t, equal)
		})
	})
}

func setupReconciler() *Reconciler {
	cl := test_utils.NewFakeClient(&v1alpha1.LVMLogicalVolume{})
	log := logger.Logger{}
	metrics := monitoring.Metrics{}

	return NewReconciler(cl, log, metrics, cache.New(), ReconcilerConfig{})
}
