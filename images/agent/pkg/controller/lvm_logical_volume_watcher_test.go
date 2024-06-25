package controller

import (
	"bytes"
	"fmt"
	"sds-node-configurator/api/v1alpha1"
	"sds-node-configurator/internal"
	"sds-node-configurator/pkg/cache"
	"sds-node-configurator/pkg/logger"
	"sds-node-configurator/pkg/monitoring"
	"sds-node-configurator/pkg/utils"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestLVMLogicaVolumeWatcher(t *testing.T) {
	var (
		cl      = NewFakeClient()
		log     = logger.Logger{}
		metrics = monitoring.Metrics{}
		vgName  = "test-vg"
		delta   = resource.MustParse(internal.ResizeDelta)
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
					Type: Thin,
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
					Type: Thin,
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
					Type: Thick,
				},
			}
			lv := &internal.LVData{LVAttr: "-wi-a-----"}

			assert.True(t, checkIfLVBelongsToLLV(llv, lv))
		})

		t.Run("llv_thick_returns_false", func(t *testing.T) {
			llv := &v1alpha1.LVMLogicalVolume{
				ObjectMeta: v1.ObjectMeta{},
				Spec: v1alpha1.LVMLogicalVolumeSpec{
					Type: Thick,
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

			lvg := &v1alpha1.LvmVolumeGroup{
				ObjectMeta: v1.ObjectMeta{
					Name: lvgName,
				},
			}

			err := cl.Create(ctx, lvg)
			if err != nil {
				t.Error(err)
			} else {
				defer func() {
					err = cl.Delete(ctx, lvg)
					if err != nil {
						t.Error(err)
					}
				}()
			}

			llv := &v1alpha1.LVMLogicalVolume{
				Spec: v1alpha1.LVMLogicalVolumeSpec{
					ActualLVNameOnTheNode: "test-lv",
					Type:                  Thick,
					Size:                  resource.MustParse("10M"),
					LvmVolumeGroupName:    lvgName,
				},
			}

			v, r := validateLVMLogicalVolume(&cache.Cache{}, llv, lvg, delta)
			if assert.True(t, v) {
				assert.Equal(t, 0, len(r))
			}
		})

		t.Run("thick_all_bad_returns_false", func(t *testing.T) {
			lvName := "test-lv"

			llv := &v1alpha1.LVMLogicalVolume{
				Spec: v1alpha1.LVMLogicalVolumeSpec{
					ActualLVNameOnTheNode: lvName,
					Type:                  Thick,
					Size:                  resource.MustParse("0M"),
					LvmVolumeGroupName:    "some-lvg",
					Thin:                  &v1alpha1.LVMLogicalVolumeThinSpec{PoolName: "some-lvg"},
				},
			}

			sdsCache := cache.New()
			sdsCache.StoreLVs([]internal.LVData{
				{
					LVName: lvName,
				},
			}, bytes.Buffer{})

			v, r := validateLVMLogicalVolume(sdsCache, llv, &v1alpha1.LvmVolumeGroup{}, delta)
			if assert.False(t, v) {
				assert.Equal(t, "zero size for LV; no LV name specified; thin pool specified for Thick LV; ", r)
			}
		})

		t.Run("thin_all_good_returns_true", func(t *testing.T) {
			const (
				lvgName = "test-lvg"
				tpName  = "test-tp"
			)

			lvg := &v1alpha1.LvmVolumeGroup{
				ObjectMeta: v1.ObjectMeta{
					Name: lvgName,
				},
				Status: v1alpha1.LvmVolumeGroupStatus{
					ThinPools: []v1alpha1.LvmVolumeGroupThinPoolStatus{
						{
							Name: tpName,
						},
					},
				},
			}

			llv := &v1alpha1.LVMLogicalVolume{
				Spec: v1alpha1.LVMLogicalVolumeSpec{
					ActualLVNameOnTheNode: "test-lv",
					Type:                  Thin,
					Size:                  resource.MustParse("10M"),
					LvmVolumeGroupName:    lvgName,
					Thin:                  &v1alpha1.LVMLogicalVolumeThinSpec{PoolName: tpName},
				},
			}

			v, r := validateLVMLogicalVolume(cache.New(), llv, lvg, delta)
			if assert.True(t, v) {
				assert.Equal(t, 0, len(r))
			}
		})

		t.Run("thin_all_bad_returns_false", func(t *testing.T) {

			llv := &v1alpha1.LVMLogicalVolume{
				Spec: v1alpha1.LVMLogicalVolumeSpec{
					ActualLVNameOnTheNode: "",
					Type:                  Thin,
					Size:                  resource.MustParse("0M"),
					LvmVolumeGroupName:    "some-lvg",
				},
			}

			sdsCache := cache.New()
			sdsCache.StoreLVs([]internal.LVData{
				{
					LVName: "test-lv",
				},
			}, bytes.Buffer{})

			v, r := validateLVMLogicalVolume(sdsCache, llv, &v1alpha1.LvmVolumeGroup{}, delta)
			if assert.False(t, v) {
				assert.Equal(t, "zero size for LV; no LV name specified; no thin pool specified; ", r)
			}
		})

	})

	t.Run("getFreeThinPoolSpace", func(t *testing.T) {
		const tpName = "test-tp"
		tps := []v1alpha1.LvmVolumeGroupThinPoolStatus{
			{
				Name:            tpName,
				ActualSize:      resource.MustParse("10Gi"),
				UsedSize:        resource.MustParse("1Gi"),
				AllocatedSize:   resource.MustParse("5Gi"),
				AllocationLimit: "150%",
			},
		}

		free, err := getFreeThinPoolSpace(tps, tpName)
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
		lvg := &v1alpha1.LvmVolumeGroup{
			Status: v1alpha1.LvmVolumeGroupStatus{
				Nodes: []v1alpha1.LvmVolumeGroupNode{
					{
						Name: nodeName,
					},
				},
			},
		}

		t.Run("returns_true", func(t *testing.T) {
			belongs := belongsToNode(lvg, nodeName)
			assert.True(t, belongs)
		})

		t.Run("returns_false", func(t *testing.T) {
			belongs := belongsToNode(lvg, "other_node")
			assert.False(t, belongs)
		})
	})

	t.Run("identifyReconcileFunc", func(t *testing.T) {
		t.Run("returns_create", func(t *testing.T) {
			llv := &v1alpha1.LVMLogicalVolume{}

			actual := identifyReconcileFunc(cache.New(), vgName, llv)

			assert.Equal(t, CreateReconcile, actual)
		})

		t.Run("returns_update", func(t *testing.T) {
			specSize := resource.NewQuantity(40000000000, resource.BinarySI)
			statusSize := resource.NewQuantity(10000000000, resource.BinarySI)
			lvName := "test-lv"
			llv := &v1alpha1.LVMLogicalVolume{
				Spec: v1alpha1.LVMLogicalVolumeSpec{
					ActualLVNameOnTheNode: lvName,
					Size:                  *specSize,
				},
				Status: &v1alpha1.LVMLogicalVolumeStatus{
					Phase:      StatusPhaseCreated,
					ActualSize: *statusSize,
				},
			}
			sdsCache := cache.New()
			sdsCache.StoreLVs([]internal.LVData{
				{
					LVName: lvName,
				},
			}, bytes.Buffer{})

			actual := identifyReconcileFunc(sdsCache, vgName, llv)

			assert.Equal(t, UpdateReconcile, actual)
		})

		t.Run("returns_delete", func(t *testing.T) {
			llv := &v1alpha1.LVMLogicalVolume{
				ObjectMeta: v1.ObjectMeta{DeletionTimestamp: &v1.Time{}},
				Status: &v1alpha1.LVMLogicalVolumeStatus{
					Phase: StatusPhaseCreated,
				},
			}

			actual := identifyReconcileFunc(cache.New(), vgName, llv)

			assert.Equal(t, DeleteReconcile, actual)
		})

		t.Run("returns_empty", func(t *testing.T) {
			specSize := resource.NewQuantity(40000000000, resource.BinarySI)
			statusSize := resource.NewQuantity(40000000000, resource.BinarySI)
			llv := &v1alpha1.LVMLogicalVolume{
				Spec: v1alpha1.LVMLogicalVolumeSpec{
					Size: *specSize,
				},
				Status: &v1alpha1.LVMLogicalVolumeStatus{
					Phase:      StatusPhaseCreated,
					ActualSize: *statusSize,
				},
			}

			actual := identifyReconcileFunc(cache.New(), vgName, llv)

			assert.Equal(t, reconcileType(""), actual)
		})
	})

	t.Run("shouldReconcileByCreateFunc", func(t *testing.T) {
		t.Run("if_status_nill_returns_true", func(t *testing.T) {
			llv := &v1alpha1.LVMLogicalVolume{}

			should := shouldReconcileByCreateFunc(cache.New(), vgName, llv)

			if assert.NoError(t, err) {
				assert.True(t, should)
			}
		})

		t.Run("if_phase_created_returns_false", func(t *testing.T) {
			llv := &v1alpha1.LVMLogicalVolume{
				Status: &v1alpha1.LVMLogicalVolumeStatus{
					Phase: StatusPhaseCreated,
				},
			}

			should := shouldReconcileByCreateFunc(cache.New(), vgName, llv)

			if assert.NoError(t, err) {
				assert.False(t, should)
			}
		})

		t.Run("if_phase_resizing_returns_false", func(t *testing.T) {
			llv := &v1alpha1.LVMLogicalVolume{
				Status: &v1alpha1.LVMLogicalVolumeStatus{
					Phase: StatusPhaseResizing,
				},
			}

			should := shouldReconcileByCreateFunc(cache.New(), vgName, llv)

			if assert.NoError(t, err) {
				assert.False(t, should)
			}
		})
	})

	t.Run("shouldReconcileByUpdateFunc", func(t *testing.T) {
		t.Run("if_deletion_timestamp_is_not_nill_returns_false", func(t *testing.T) {
			llv := &v1alpha1.LVMLogicalVolume{
				ObjectMeta: v1.ObjectMeta{
					DeletionTimestamp: &v1.Time{},
				},
			}

			should := shouldReconcileByUpdateFunc(cache.New(), vgName, llv)

			if assert.NoError(t, err) {
				assert.False(t, should)
			}
		})

		t.Run("if_status_nil_returns_false", func(t *testing.T) {
			llv := &v1alpha1.LVMLogicalVolume{}

			should := shouldReconcileByUpdateFunc(cache.New(), vgName, llv)

			if assert.NoError(t, err) {
				assert.False(t, should)
			}
		})

		t.Run("if_phase_pending_returns_false", func(t *testing.T) {
			llv := &v1alpha1.LVMLogicalVolume{
				Status: &v1alpha1.LVMLogicalVolumeStatus{
					Phase: StatusPhasePending,
				},
			}

			should := shouldReconcileByUpdateFunc(cache.New(), vgName, llv)

			if assert.NoError(t, err) {
				assert.False(t, should)
			}
		})

		t.Run("if_phase_resizing_returns_false", func(t *testing.T) {
			llv := &v1alpha1.LVMLogicalVolume{
				Status: &v1alpha1.LVMLogicalVolumeStatus{
					Phase: StatusPhaseResizing,
				},
			}

			should := shouldReconcileByUpdateFunc(cache.New(), vgName, llv)

			if assert.NoError(t, err) {
				assert.False(t, should)
			}
		})

		t.Run("if_spec_size_less_than_status_one_returns_false_and_error", func(t *testing.T) {
			specSize := resource.NewQuantity(100000000, resource.BinarySI)
			statusSize := resource.NewQuantity(200000000, resource.BinarySI)
			llv := &v1alpha1.LVMLogicalVolume{
				Spec: v1alpha1.LVMLogicalVolumeSpec{
					Size: *specSize,
				},
				Status: &v1alpha1.LVMLogicalVolumeStatus{
					Phase:      StatusPhaseCreated,
					ActualSize: *statusSize,
				},
			}

			should := shouldReconcileByUpdateFunc(cache.New(), vgName, llv)

			if assert.ErrorContains(t, err, fmt.Sprintf("requested size %d is less than actual %d", llv.Spec.Size.Value(), llv.Status.ActualSize.Value())) {
				assert.False(t, should)
			}
		})

		t.Run("if_spec_size_more_than_status_one_but_less_than_delta_returns_false", func(t *testing.T) {
			specSize := resource.NewQuantity(30000, resource.BinarySI)
			statusSize := resource.NewQuantity(20000, resource.BinarySI)
			llv := &v1alpha1.LVMLogicalVolume{
				Spec: v1alpha1.LVMLogicalVolumeSpec{
					Size: *specSize,
				},
				Status: &v1alpha1.LVMLogicalVolumeStatus{
					Phase:      StatusPhaseCreated,
					ActualSize: *statusSize,
				},
			}

			should := shouldReconcileByUpdateFunc(cache.New(), vgName, llv)

			if assert.NoError(t, err) {
				assert.False(t, should)
			}
		})

		t.Run("if_spec_size_more_than_status_returns_true", func(t *testing.T) {
			specSize := resource.NewQuantity(40000000000, resource.BinarySI)
			statusSize := resource.NewQuantity(10000000000, resource.BinarySI)
			llv := &v1alpha1.LVMLogicalVolume{
				Spec: v1alpha1.LVMLogicalVolumeSpec{
					Size: *specSize,
				},
				Status: &v1alpha1.LVMLogicalVolumeStatus{
					Phase:      StatusPhaseCreated,
					ActualSize: *statusSize,
				},
			}

			should := shouldReconcileByUpdateFunc(cache.New(), vgName, llv)

			if assert.NoError(t, err) {
				assert.True(t, should)
			}
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
		llv := &v1alpha1.LVMLogicalVolume{
			ObjectMeta: v1.ObjectMeta{
				Name: "test",
			},
			Status: &v1alpha1.LVMLogicalVolumeStatus{
				Phase:  StatusPhaseCreated,
				Reason: "",
			},
		}

		err := cl.Create(ctx, llv)
		if err != nil {
			t.Error(err)
			return
		}

		defer func() {
			err = cl.Delete(ctx, llv)
			if err != nil {
				t.Error(err)
			}
		}()

		err = updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, llv, StatusPhaseFailed, reason)
		if assert.NoError(t, err) {
			newLLV := &v1alpha1.LVMLogicalVolume{}
			err = cl.Get(ctx, client.ObjectKey{
				Name:      llv.Name,
				Namespace: "",
			}, newLLV)

			assert.Equal(t, newLLV.Status.Phase, StatusPhaseFailed)
			assert.Equal(t, newLLV.Status.Reason, reason)
		}
	})

	t.Run("addLLVFinalizerIfNotExist", func(t *testing.T) {
		t.Run("no_finalizer_adds_one_returns_true", func(t *testing.T) {
			const (
				name = "test-name1"
			)
			llv := &v1alpha1.LVMLogicalVolume{
				ObjectMeta: v1.ObjectMeta{
					Name:       name,
					Finalizers: []string{},
				},
			}

			err := cl.Create(ctx, llv)
			if err != nil {
				t.Error(err)
				return
			}

			defer func() {
				err = cl.Delete(ctx, llv)
				if err != nil {
					t.Error(err)
				}
			}()

			added, err := addLLVFinalizerIfNotExist(ctx, cl, log, metrics, llv)
			if assert.NoError(t, err) {
				assert.True(t, added)

				newLLV := &v1alpha1.LVMLogicalVolume{}
				err = cl.Get(ctx, client.ObjectKey{
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
			llv := &v1alpha1.LVMLogicalVolume{
				ObjectMeta: v1.ObjectMeta{
					Name:       name,
					Finalizers: []string{internal.SdsNodeConfiguratorFinalizer},
				},
			}

			err := cl.Create(ctx, llv)
			if err != nil {
				t.Error(err)
				return
			}

			defer func() {
				err = cl.Delete(ctx, llv)
				if err != nil {
					t.Error(err)
				}
			}()

			added, err := addLLVFinalizerIfNotExist(ctx, cl, log, metrics, llv)
			if assert.NoError(t, err) {
				assert.False(t, added)

				newLLV := &v1alpha1.LVMLogicalVolume{}
				err = cl.Get(ctx, client.ObjectKey{
					Name:      llv.Name,
					Namespace: "",
				}, newLLV)

				assert.Contains(t, newLLV.Finalizers, internal.SdsNodeConfiguratorFinalizer)
			}
		})
	})

	// t.Run("getVirtualLVSize", func(t *testing.T) {
	// 	const (
	// 		tpName = "test_tp"
	// 	)
	// 	lvs := []internal.LVData{
	// 		{
	// 			PoolName: tpName,
	// 			LVSize: *resource.NewQuantity(1000, resource.BinarySI),
	// 		},
	// 		{
	// 			PoolName: tpName,
	// 			LVSize: *resource.NewQuantity(1000, resource.BinarySI),
	// 		},
	// 		{
	// 			PoolName: tpName,
	// 			LVSize: *resource.NewQuantity(1000, resource.BinarySI),
	// 		},
	// 	}

	// 	size := getVirtualLVSize(tpName, lvs)

	// 	assert.Equal(t, int64(3000), size.Value())
	// })

	t.Run("getFreeVGSpace", func(t *testing.T) {
		lvg := &v1alpha1.LvmVolumeGroup{
			Status: v1alpha1.LvmVolumeGroupStatus{
				VGSize:        resource.MustParse("2G"),
				AllocatedSize: resource.MustParse("1G"),
			},
		}

		free := getFreeVGSpace(lvg)
		assert.Equal(t, int64(1000000000), free.Value())
	})

	t.Run("updateLVMLogicalVolume", func(t *testing.T) {
		const (
			lvgName = "test-lvg"
			oldSize = int64(100000000)
			newSize = int64(200000000)
		)
		llv := &v1alpha1.LVMLogicalVolume{
			ObjectMeta: v1.ObjectMeta{
				Name: lvgName,
			},
			Status: &v1alpha1.LVMLogicalVolumeStatus{
				Phase:      StatusPhasePending,
				Reason:     "",
				ActualSize: *resource.NewQuantity(oldSize, resource.BinarySI),
			},
		}

		err := cl.Create(ctx, llv)
		if err != nil {
			t.Error(err)
			return
		}

		defer func() {
			err = cl.Delete(ctx, llv)
			if err != nil {
				t.Error(err)
			}
		}()

		oldLLV := &v1alpha1.LVMLogicalVolume{}
		err = cl.Get(ctx, client.ObjectKey{
			Name: llv.Name,
		}, oldLLV)
		if err != nil {
			t.Error(err)
			return
		}

		if assert.NotNil(t, oldLLV) {
			assert.Equal(t, StatusPhasePending, oldLLV.Status.Phase)
			assert.Equal(t, oldSize, oldLLV.Status.ActualSize.Value())
		}

		oldLLV.Status.Phase = StatusPhaseCreated
		oldLLV.Status.ActualSize = *resource.NewQuantity(newSize, resource.BinarySI)
		err = updateLVMLogicalVolume(ctx, metrics, cl, oldLLV)

		if assert.NoError(t, err) {
			newLLV := &v1alpha1.LVMLogicalVolume{}
			err = cl.Get(ctx, client.ObjectKey{
				Name: llv.Name,
			}, newLLV)
			if err != nil {
				t.Error(err)
				return
			}

			assert.Equal(t, StatusPhaseCreated, newLLV.Status.Phase)
			assert.Equal(t, newSize, newLLV.Status.ActualSize.Value())
		}
	})

	t.Run("removeLLVFinalizersIfExist", func(t *testing.T) {
		llv := &v1alpha1.LVMLogicalVolume{
			ObjectMeta: v1.ObjectMeta{
				Name:       "test-name",
				Finalizers: []string{internal.SdsNodeConfiguratorFinalizer},
			},
		}
		err := cl.Create(ctx, llv)
		if err != nil {
			t.Error(err)
			return
		}

		defer func() {
			err = cl.Delete(ctx, llv)
			if err != nil {
				t.Error(err)
			}
		}()

		llvWithFinalizer := &v1alpha1.LVMLogicalVolume{}
		err = cl.Get(ctx, client.ObjectKey{
			Name: llv.Name,
		}, llvWithFinalizer)
		if err != nil {
			t.Error(err)
			return
		}

		assert.Contains(t, llvWithFinalizer.Finalizers, internal.SdsNodeConfiguratorFinalizer)

		err = removeLLVFinalizersIfExist(ctx, cl, metrics, log, llv)
		if assert.NoError(t, err) {
			llvNoFinalizer := &v1alpha1.LVMLogicalVolume{}
			err = cl.Get(ctx, client.ObjectKey{
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
			delta, err := resource.ParseQuantity(internal.ResizeDelta)
			if err != nil {
				t.Error(err)
			}

			left := resource.NewQuantity(int64(size), resource.BinarySI)
			right := resource.NewQuantity(int64(size)+delta.Value()-1, resource.BinarySI)

			equal := utils.AreSizesEqualWithinDelta(*left, *right, delta)

			assert.True(t, equal)
		})

		t.Run("returns_false", func(t *testing.T) {
			size := 10000000000
			delta, err := resource.ParseQuantity(internal.ResizeDelta)
			if err != nil {
				t.Error(err)
			}

			left := resource.NewQuantity(int64(size), resource.BinarySI)
			right := resource.NewQuantity(int64(size)+delta.Value(), resource.BinarySI)

			equal := utils.AreSizesEqualWithinDelta(*left, *right, delta)

			assert.False(t, equal)
		})

	})
}
