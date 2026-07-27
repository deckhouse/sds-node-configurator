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

package llv

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	"k8s.io/apimachinery/pkg/api/resource"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/cache"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/logger"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/mock_utils"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/monitoring"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/test_utils"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/utils"
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

		// Cloning and restoring go through `lvcreate -s`, which is thin-only. The
		// create path checks the Thick type first, so a Thick LLV carrying a Source
		// used to produce an empty LV with the source dropped silently — a Created
		// volume holding no data. It has to be rejected here instead.
		t.Run("thick_with_source_returns_false", func(t *testing.T) {
			const lvgName = "test-lvg"

			r := setupReconciler()

			lvg := &v1alpha1.LVMVolumeGroup{
				ObjectMeta: v1.ObjectMeta{
					Name: lvgName,
				},
				Status: v1alpha1.LVMVolumeGroupStatus{
					VGSize: resource.MustParse("1Gi"),
				},
			}

			llv := &v1alpha1.LVMLogicalVolume{
				Spec: v1alpha1.LVMLogicalVolumeSpec{
					ActualLVNameOnTheNode: "test-lv",
					Type:                  internal.Thick,
					Size:                  "10M",
					LVMVolumeGroupName:    lvgName,
					Source: &v1alpha1.LVMLogicalVolumeSource{
						Kind: "LVMLogicalVolumeSnapshot",
						Name: "some-snapshot",
					},
				},
			}

			v, reason := r.validateLVMLogicalVolume(llv, lvg)
			if assert.False(t, v) {
				assert.Equal(t, "Source specified for Thick LV: cloning and restoring are supported for Thin LVs only. ", reason)
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

		t.Run("actual_size_larger_than_requested_is_valid", func(t *testing.T) {
			const (
				lvgName = "test-lvg"
				tpName  = "data-thin"
			)

			r := setupReconciler()

			lvg := &v1alpha1.LVMVolumeGroup{
				ObjectMeta: v1.ObjectMeta{
					Name: lvgName,
				},
				Status: v1alpha1.LVMVolumeGroupStatus{
					ExtentSize: resource.MustParse("4Mi"),
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
					Size:                  "40Gi",
					LVMVolumeGroupName:    lvgName,
					Thin:                  &v1alpha1.LVMLogicalVolumeThinSpec{PoolName: tpName},
				},
				Status: &v1alpha1.LVMLogicalVolumeStatus{
					Phase:      v1alpha1.PhaseCreated,
					ActualSize: resource.MustParse("40972Mi"),
				},
			}

			v, reason := r.validateLVMLogicalVolume(llv, lvg)
			if assert.True(t, v) {
				assert.Equal(t, 0, len(reason))
			}
		})
	})

	t.Run("llvExtentSize", func(t *testing.T) {
		t.Run("returns_status_extent_size_when_positive", func(t *testing.T) {
			lvg := &v1alpha1.LVMVolumeGroup{
				Status: v1alpha1.LVMVolumeGroupStatus{
					ExtentSize: resource.MustParse("8Mi"),
				},
			}
			assert.Equal(t, resource.MustParse("8Mi"), llvExtentSize(lvg))
		})

		t.Run("falls_back_to_4Mi_when_zero", func(t *testing.T) {
			lvg := &v1alpha1.LVMVolumeGroup{}
			assert.Equal(t, resource.MustParse("4Mi"), llvExtentSize(lvg))
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

	t.Run("AlignSizeToExtent", func(t *testing.T) {
		extentSize := resource.MustParse("4Mi")

		t.Run("aligns_up_to_extent_boundary", func(t *testing.T) {
			size := resource.MustParse("201Mi")
			aligned, err := utils.AlignSizeToExtent(size, extentSize)
			assert.NoError(t, err)
			expected := resource.MustParse("204Mi")
			assert.Equal(t, expected.Value(), aligned.Value())
		})

		t.Run("already_aligned_stays_same", func(t *testing.T) {
			size := resource.MustParse("204Mi")
			aligned, err := utils.AlignSizeToExtent(size, extentSize)
			assert.NoError(t, err)
			expected := resource.MustParse("204Mi")
			assert.Equal(t, expected.Value(), aligned.Value())
		})

		t.Run("returns_error_for_zero_extent", func(t *testing.T) {
			size := resource.MustParse("100Mi")
			zeroExtent := resource.MustParse("0")
			_, err := utils.AlignSizeToExtent(size, zeroExtent)
			assert.Error(t, err)
		})
	})
}

const (
	cloneVGActual  = "test-vg"
	cloneThinPool  = "test-pool"
	cloneLLVName   = "restored-llv"
	cloneLVName    = "restored-lv"
	cloneSrcName   = "source-llv"
	cloneSrcLVName = "source-lv"
)

type clonedVolumeEnv struct {
	r        *Reconciler
	cl       client.Client
	mockCmds *mock_utils.MockCommands
	lvg      *v1alpha1.LVMVolumeGroup
	llv      *v1alpha1.LVMLogicalVolume
}

// newClonedVolumeEnv builds the fixture shared by the clone/restore tests: a thin
// LVG with a 4Mi extent, a 36Mi source LLV, and a target LLV cloned from it that
// asks for requestedSize.
func newClonedVolumeEnv(ctx context.Context, t *testing.T, requestedSize string) *clonedVolumeEnv {
	t.Helper()

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	mockCmds := mock_utils.NewMockCommands(ctrl)
	cl := test_utils.NewFakeClient(&v1alpha1.LVMLogicalVolume{})
	r := NewReconciler(cl, logger.Logger{}, monitoring.GetMetrics(""), cache.New(), mockCmds, ReconcilerConfig{})

	lvg := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: v1.ObjectMeta{Name: "test-lvg"},
		Spec: v1alpha1.LVMVolumeGroupSpec{
			ActualVGNameOnTheNode: cloneVGActual,
			Local:                 v1alpha1.LVMVolumeGroupLocalSpec{NodeName: "node-1"},
		},
		Status: v1alpha1.LVMVolumeGroupStatus{
			ExtentSize: resource.MustParse("4Mi"),
			ThinPools: []v1alpha1.LVMVolumeGroupThinPoolStatus{
				{
					Name:            cloneThinPool,
					ActualSize:      resource.MustParse("1Gi"),
					AvailableSpace:  resource.MustParse("1Gi"),
					AllocationLimit: "150%",
				},
			},
		},
	}

	sourceLLV := &v1alpha1.LVMLogicalVolume{
		ObjectMeta: v1.ObjectMeta{Name: cloneSrcName},
		Spec: v1alpha1.LVMLogicalVolumeSpec{
			ActualLVNameOnTheNode: cloneSrcLVName,
			Type:                  internal.Thin,
			LVMVolumeGroupName:    lvg.Name,
			Thin:                  &v1alpha1.LVMLogicalVolumeThinSpec{PoolName: cloneThinPool},
			Size:                  "36Mi",
		},
	}
	assert.NoError(t, cl.Create(ctx, sourceLLV))

	llv := &v1alpha1.LVMLogicalVolume{
		ObjectMeta: v1.ObjectMeta{Name: cloneLLVName},
		Spec: v1alpha1.LVMLogicalVolumeSpec{
			ActualLVNameOnTheNode: cloneLVName,
			Type:                  internal.Thin,
			LVMVolumeGroupName:    lvg.Name,
			Thin:                  &v1alpha1.LVMLogicalVolumeThinSpec{PoolName: cloneThinPool},
			Size:                  requestedSize,
			Source:                &v1alpha1.LVMLogicalVolumeSource{Kind: "LVMLogicalVolume", Name: cloneSrcName},
		},
		Status: &v1alpha1.LVMLogicalVolumeStatus{Phase: v1alpha1.PhasePending},
	}
	assert.NoError(t, cl.Create(ctx, llv))

	return &clonedVolumeEnv{r: r, cl: cl, mockCmds: mockCmds, lvg: lvg, llv: llv}
}

func (e *clonedVolumeEnv) expectClone() *gomock.Call {
	return e.mockCmds.EXPECT().
		CreateThinLogicalVolumeFromSource(cloneLVName, cloneVGActual, cloneSrcLVName).
		Return("lvcreate -s", nil).
		Times(1)
}

func (e *clonedVolumeEnv) expectGetLV(size resource.Quantity) *gomock.Call {
	return e.mockCmds.EXPECT().
		GetLV(cloneVGActual, cloneLVName).
		Return(internal.LVData{LVName: cloneLVName, VGName: cloneVGActual, LVSize: size}, "lvs", bytes.Buffer{}, nil).
		Times(1)
}

func (e *clonedVolumeEnv) expectExtendLV(size resource.Quantity) *gomock.Call {
	return e.mockCmds.EXPECT().
		ExtendLV(size.Value(), cloneVGActual, cloneLVName).
		Return("lvextend", nil).
		Times(1)
}

// seedCachedLV puts the LV into the scan cache, which is where the update path
// reads the pre-extension size from (the create path goes straight to LVM
// instead, because the cache has not seen the LV yet).
func (e *clonedVolumeEnv) seedCachedLV(size resource.Quantity) {
	e.r.sdsCache.StoreLVs(
		[]internal.LVData{{LVName: cloneLVName, VGName: cloneVGActual, LVSize: size}},
		bytes.Buffer{},
	)
}

// TestReconcileLLVCreateFunc_ClonedVolumeExtendedToRequestedSize pins the fix
// for restore/clone from a snapshot or another LV: `lvcreate -s` produces an LV
// of the origin size, not the requested size. When the requested (extent-aligned)
// size is larger, the create path must extend the LV before reporting Created —
// otherwise the CSI CreateVolume call waits forever for a size that never
// materialises and the PVC stays Pending.
func TestReconcileLLVCreateFunc_ClonedVolumeExtendedToRequestedSize(t *testing.T) {
	ctx := context.Background()

	originSize := resource.MustParse("36Mi")    // size the clone inherits from the origin LV
	requestedSize := resource.MustParse("52Mi") // PVC-requested size, already a multiple of the 4Mi extent

	env := newClonedVolumeEnv(ctx, t, "52Mi")

	gomock.InOrder(
		env.expectClone(),
		// right after the clone the LV still reports the origin size
		env.expectGetLV(originSize),
		// the clone must be extended to the requested (aligned) size
		env.mockCmds.EXPECT().
			ExtendLV(requestedSize.Value(), cloneVGActual, cloneLVName).
			Return("lvextend", nil).
			Times(1),
		// after the extension the LV reports the requested size
		env.expectGetLV(requestedSize),
	)

	shouldRequeue, err := env.r.reconcileLLVCreateFunc(ctx, env.llv, env.lvg)
	assert.NoError(t, err)
	assert.False(t, shouldRequeue)

	updated := &v1alpha1.LVMLogicalVolume{}
	assert.NoError(t, env.cl.Get(ctx, client.ObjectKey{Name: cloneLLVName}, updated))
	assert.Equal(t, v1alpha1.PhaseCreated, updated.Status.Phase)
	assert.Equal(t, requestedSize.Value(), updated.Status.ActualSize.Value())
}

// TestReconcileLLVCreateFunc_ClonedVolumeNotExtendedWhenAlreadyLargeEnough
// guards the other direction: a same-size (or smaller) restore must not touch
// lvextend at all. The strict mock fails the test on any unexpected ExtendLV.
func TestReconcileLLVCreateFunc_ClonedVolumeNotExtendedWhenAlreadyLargeEnough(t *testing.T) {
	ctx := context.Background()

	originSize := resource.MustParse("36Mi")

	env := newClonedVolumeEnv(ctx, t, "36Mi")

	gomock.InOrder(
		env.expectClone(),
		env.expectGetLV(originSize),
	)

	shouldRequeue, err := env.r.reconcileLLVCreateFunc(ctx, env.llv, env.lvg)
	assert.NoError(t, err)
	assert.False(t, shouldRequeue)

	updated := &v1alpha1.LVMLogicalVolume{}
	assert.NoError(t, env.cl.Get(ctx, client.ObjectKey{Name: cloneLLVName}, updated))
	assert.Equal(t, v1alpha1.PhaseCreated, updated.Status.Phase)
	assert.Equal(t, originSize.Value(), updated.Status.ActualSize.Value())
}

// TestReconcileLLVCreateFunc_ClonedVolumeExtensionNotConfirmed covers the case
// that makes verifying the post-extension size mandatory: ExtendLV reports
// success while the LV did not actually grow (LVM's "No size change." is
// filtered out as benign, so a no-op resize is indistinguishable from a real
// one at the command level). The LLV must NOT be moved to Created at the origin
// size — that would strand the CSI caller waiting for a size that is never
// reached, with no further reconcile scheduled.
func TestReconcileLLVCreateFunc_ClonedVolumeExtensionNotConfirmed(t *testing.T) {
	ctx := context.Background()

	originSize := resource.MustParse("36Mi")
	requestedSize := resource.MustParse("52Mi")

	env := newClonedVolumeEnv(ctx, t, "52Mi")

	gomock.InOrder(
		env.expectClone(),
		env.expectGetLV(originSize),
		env.mockCmds.EXPECT().
			ExtendLV(requestedSize.Value(), cloneVGActual, cloneLVName).
			Return("lvextend", nil).
			Times(1),
		// LVM still reports the origin size: the extension was not effective
		env.expectGetLV(originSize),
	)

	shouldRequeue, err := env.r.reconcileLLVCreateFunc(ctx, env.llv, env.lvg)
	assert.NoError(t, err)
	assert.True(t, shouldRequeue)

	assert.Equal(t, v1alpha1.PhasePending, env.llv.Status.Phase)

	stored := &v1alpha1.LVMLogicalVolume{}
	assert.NoError(t, env.cl.Get(ctx, client.ObjectKey{Name: cloneLLVName}, stored))
	if stored.Status != nil {
		assert.NotEqual(t, v1alpha1.PhaseCreated, stored.Status.Phase)
	}
}

// TestReconcileLLVUpdateFunc_ExtendsToTheAlignedRequestedSize pins the resize
// path onto the extent-aligned size rather than the raw Spec.Size. LVM rounds
// `lvextend -L` up to the extent boundary anyway, so the resulting LV is the
// same either way — but the size the reconciler then verifies against, and the
// ActualSize it publishes, have to be the rounded one. Verifying against the
// raw request would keep the LLV out of Created forever whenever Spec.Size is
// not a multiple of the extent.
func TestReconcileLLVUpdateFunc_ExtendsToTheAlignedRequestedSize(t *testing.T) {
	ctx := context.Background()

	currentSize := resource.MustParse("36Mi")
	// 50Mi is not a multiple of the 4Mi extent, the LV can only reach 52Mi
	alignedSize := resource.MustParse("52Mi")

	env := newClonedVolumeEnv(ctx, t, "50Mi")
	env.seedCachedLV(currentSize)

	gomock.InOrder(
		env.expectExtendLV(alignedSize),
		env.expectGetLV(alignedSize),
	)

	shouldRequeue, err := env.r.reconcileLLVUpdateFunc(ctx, env.llv, env.lvg)
	assert.NoError(t, err)
	assert.False(t, shouldRequeue)

	updated := &v1alpha1.LVMLogicalVolume{}
	assert.NoError(t, env.cl.Get(ctx, client.ObjectKey{Name: cloneLLVName}, updated))
	assert.Equal(t, v1alpha1.PhaseCreated, updated.Status.Phase)
	assert.Equal(t, alignedSize.Value(), updated.Status.ActualSize.Value())
}

// TestReconcileLLVUpdateFunc_RequeuesWhenExtensionNotConfirmed is the resize-path
// counterpart of the create-path test above: a no-op lvextend is reported as
// success, so the LLV must stay out of Created until LVM confirms the new size.
func TestReconcileLLVUpdateFunc_RequeuesWhenExtensionNotConfirmed(t *testing.T) {
	ctx := context.Background()

	currentSize := resource.MustParse("36Mi")
	requestedSize := resource.MustParse("52Mi")

	env := newClonedVolumeEnv(ctx, t, "52Mi")
	env.seedCachedLV(currentSize)

	gomock.InOrder(
		env.expectExtendLV(requestedSize),
		// LVM still reports the pre-extension size
		env.expectGetLV(currentSize),
	)

	shouldRequeue, err := env.r.reconcileLLVUpdateFunc(ctx, env.llv, env.lvg)
	assert.NoError(t, err)
	assert.True(t, shouldRequeue)

	stored := &v1alpha1.LVMLogicalVolume{}
	assert.NoError(t, env.cl.Get(ctx, client.ObjectKey{Name: cloneLLVName}, stored))
	assert.Equal(t, v1alpha1.PhaseResizing, stored.Status.Phase)
	assert.NotEqual(t, requestedSize.Value(), stored.Status.ActualSize.Value())
}

// TestReconcileLLVUpdateFunc_NoExtendWhenAlreadyLargeEnough guards the branch a
// restored volume lands in once the create path has already grown it: the LV is
// big enough, so lvextend must not run again. The strict mock fails the test on
// any unexpected ExtendLV.
func TestReconcileLLVUpdateFunc_NoExtendWhenAlreadyLargeEnough(t *testing.T) {
	ctx := context.Background()

	currentSize := resource.MustParse("52Mi")

	env := newClonedVolumeEnv(ctx, t, "52Mi")
	env.seedCachedLV(currentSize)

	shouldRequeue, err := env.r.reconcileLLVUpdateFunc(ctx, env.llv, env.lvg)
	assert.NoError(t, err)
	assert.False(t, shouldRequeue)

	updated := &v1alpha1.LVMLogicalVolume{}
	assert.NoError(t, env.cl.Get(ctx, client.ObjectKey{Name: cloneLLVName}, updated))
	assert.Equal(t, v1alpha1.PhaseCreated, updated.Status.Phase)
	assert.Equal(t, currentSize.Value(), updated.Status.ActualSize.Value())
}

func setupReconciler() *Reconciler {
	cl := test_utils.NewFakeClient(&v1alpha1.LVMLogicalVolume{})
	log := logger.Logger{}
	metrics := monitoring.GetMetrics("")

	return NewReconciler(cl, log, metrics, cache.New(), utils.NewCommands(), ReconcilerConfig{})
}
