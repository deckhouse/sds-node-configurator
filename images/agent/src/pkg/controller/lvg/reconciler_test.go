package lvg

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/stretchr/testify/assert"
	errors2 "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/strings/slices"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"agent/internal"
	"agent/pkg/cache"
	cutils "agent/pkg/controller/utils"
	"agent/pkg/logger"
	"agent/pkg/monitoring"
	"agent/pkg/test_utils"
)

func TestLVMVolumeGroupWatcherCtrl(t *testing.T) {
	cl := test_utils.NewFakeClient()
	ctx := context.Background()
	log := logger.Logger{}
	metrics := monitoring.GetMetrics("")
	ch := cache.New()

	r := NewReconciler(cl, log, metrics, ch, ReconcilerOptions{})

	t.Run("validateLVGForUpdateFunc", func(t *testing.T) {
		t.Run("without_thin_pools_returns_true", func(t *testing.T) {
			const (
				firstBd  = "first"
				secondBd = "second"

				firstPath  = "first-path"
				secondPath = "second-path"
			)

			bds := map[string]v1alpha1.BlockDevice{
				firstBd: {
					ObjectMeta: v1.ObjectMeta{
						Name: firstBd,
					},
					Status: v1alpha1.BlockDeviceStatus{
						Size:       resource.MustParse("1G"),
						Consumable: true,
						Path:       firstPath,
					},
				},
				secondBd: {
					ObjectMeta: v1.ObjectMeta{
						Name: secondBd,
					},
					Status: v1alpha1.BlockDeviceStatus{
						Size:       resource.MustParse("1G"),
						Consumable: true,
						Path:       secondPath,
					},
				},
			}
			lvg := &v1alpha1.LVMVolumeGroup{
				Spec: v1alpha1.LVMVolumeGroupSpec{},
				Status: v1alpha1.LVMVolumeGroupStatus{
					Phase:                "",
					Conditions:           nil,
					ThinPoolReady:        "",
					ConfigurationApplied: "",
					VGFree:               resource.Quantity{},
				},
			}

			// so second block device is new one
			pvs := []internal.PVData{
				{
					PVName: firstPath,
				},
			}

			ch.StorePVs(pvs, bytes.Buffer{})

			valid, reason := r.validateLVGForUpdateFunc(lvg, bds)
			if assert.True(t, valid) {
				assert.Equal(t, "", reason)
			}
		})

		t.Run("without_thin_pools_returns_false", func(t *testing.T) {
			const (
				firstBd  = "first"
				secondBd = "second"

				firstPath  = "first-path"
				secondPath = "second-path"
			)

			bds := map[string]v1alpha1.BlockDevice{
				firstBd: {
					ObjectMeta: v1.ObjectMeta{
						Name: firstBd,
					},
					Status: v1alpha1.BlockDeviceStatus{
						Size:       resource.MustParse("1G"),
						Consumable: true,
						Path:       firstPath,
					},
				},
				secondBd: {
					ObjectMeta: v1.ObjectMeta{
						Name: secondBd,
					},
					Status: v1alpha1.BlockDeviceStatus{
						Size:       resource.MustParse("1G"),
						Consumable: false,
						Path:       secondPath,
					},
				},
			}
			lvg := &v1alpha1.LVMVolumeGroup{
				Spec: v1alpha1.LVMVolumeGroupSpec{},
				Status: v1alpha1.LVMVolumeGroupStatus{
					Phase:                "",
					Conditions:           nil,
					ThinPoolReady:        "",
					ConfigurationApplied: "",
					VGFree:               resource.Quantity{},
				},
			}

			// so second block device is new one
			pvs := []internal.PVData{
				{
					PVName: firstPath,
				},
			}

			r.sdsCache.StorePVs(pvs, bytes.Buffer{})

			// new block device is not consumable
			valid, _ := r.validateLVGForUpdateFunc(lvg, bds)
			assert.False(t, valid)
		})

		t.Run("with_thin_pools_returns_true", func(t *testing.T) {
			const (
				firstBd  = "first"
				secondBd = "second"

				firstPath  = "first-path"
				secondPath = "second-path"

				vgName = "test-vg"
			)

			bds := map[string]v1alpha1.BlockDevice{
				firstBd: {
					ObjectMeta: v1.ObjectMeta{
						Name: firstBd,
					},
					Status: v1alpha1.BlockDeviceStatus{
						Size:       resource.MustParse("1G"),
						Consumable: true,
						Path:       firstPath,
					},
				},
				secondBd: {
					ObjectMeta: v1.ObjectMeta{
						Name: secondBd,
					},
					Status: v1alpha1.BlockDeviceStatus{
						Size:       resource.MustParse("2G"),
						Consumable: true,
						Path:       secondPath,
					},
				},
			}
			lvg := &v1alpha1.LVMVolumeGroup{
				Spec: v1alpha1.LVMVolumeGroupSpec{
					ThinPools: []v1alpha1.LVMVolumeGroupThinPoolSpec{
						{
							Name:            "new-thin",
							Size:            "2.5G",
							AllocationLimit: "150%",
						},
					},
					ActualVGNameOnTheNode: vgName,
				},
			}

			// so second block device is new one
			pvs := []internal.PVData{
				{
					PVName: firstPath,
				},
			}

			vgs := []internal.VGData{
				{
					VGName: vgName,
					VGSize: resource.MustParse("1G"),
					VGFree: resource.MustParse("1G"),
				},
			}

			r.sdsCache.StorePVs(pvs, bytes.Buffer{})
			r.sdsCache.StoreVGs(vgs, bytes.Buffer{})

			valid, reason := r.validateLVGForUpdateFunc(lvg, bds)
			if assert.True(t, valid) {
				assert.Equal(t, "", reason)
			}
		})

		t.Run("with_thin_pools_returns_false", func(t *testing.T) {
			const (
				firstBd  = "first"
				secondBd = "second"

				firstPath  = "first-path"
				secondPath = "second-path"

				vgName = "test-vg"
			)

			bds := map[string]v1alpha1.BlockDevice{
				firstBd: {
					ObjectMeta: v1.ObjectMeta{
						Name: firstBd,
					},
					Status: v1alpha1.BlockDeviceStatus{
						Size:       resource.MustParse("1G"),
						Consumable: true,
						Path:       firstPath,
					},
				},
				secondBd: {
					ObjectMeta: v1.ObjectMeta{
						Name: secondBd,
					},
					Status: v1alpha1.BlockDeviceStatus{
						Size:       resource.MustParse("2G"),
						Consumable: true,
						Path:       secondPath,
					},
				},
			}
			lvg := &v1alpha1.LVMVolumeGroup{
				Spec: v1alpha1.LVMVolumeGroupSpec{
					ThinPools: []v1alpha1.LVMVolumeGroupThinPoolSpec{
						{
							Name:            "new-thin",
							Size:            "4G",
							AllocationLimit: "150%",
						},
					},
					ActualVGNameOnTheNode: vgName,
				},
			}

			// so second block device is new one
			pvs := []internal.PVData{
				{
					PVName: firstPath,
				},
			}

			vgs := []internal.VGData{
				{
					VGName: vgName,
					VGSize: resource.MustParse("1G"),
					VGFree: resource.MustParse("1G"),
				},
			}

			r.sdsCache.StorePVs(pvs, bytes.Buffer{})
			r.sdsCache.StoreVGs(vgs, bytes.Buffer{})

			valid, _ := r.validateLVGForUpdateFunc(lvg, bds)
			assert.False(t, valid)
		})
	})

	t.Run("validateLVGForCreateFunc", func(t *testing.T) {
		t.Run("without_thin_pools_returns_true", func(t *testing.T) {
			const (
				firstBd  = "first"
				secondBd = "second"
			)
			bds := map[string]v1alpha1.BlockDevice{
				firstBd: {
					ObjectMeta: v1.ObjectMeta{
						Name: firstBd,
					},
					Status: v1alpha1.BlockDeviceStatus{
						Size:       resource.MustParse("1G"),
						Consumable: true,
					},
				},
				secondBd: {
					ObjectMeta: v1.ObjectMeta{
						Name: secondBd,
					},
					Status: v1alpha1.BlockDeviceStatus{
						Size:       resource.MustParse("1G"),
						Consumable: true,
					},
				},
			}
			lvg := &v1alpha1.LVMVolumeGroup{
				Spec: v1alpha1.LVMVolumeGroupSpec{},
			}

			valid, reason := r.validateLVGForCreateFunc(lvg, bds)
			if assert.True(t, valid) {
				assert.Equal(t, "", reason)
			}
		})

		t.Run("without_thin_pools_returns_false", func(t *testing.T) {
			const (
				firstBd = "first"
			)
			bds := map[string]v1alpha1.BlockDevice{
				firstBd: {
					ObjectMeta: v1.ObjectMeta{
						Name: firstBd,
					},
					Status: v1alpha1.BlockDeviceStatus{
						Size:       resource.MustParse("1G"),
						Consumable: false,
					},
				},
			}
			lvg := &v1alpha1.LVMVolumeGroup{
				Spec: v1alpha1.LVMVolumeGroupSpec{},
			}

			valid, _ := r.validateLVGForCreateFunc(lvg, bds)
			assert.False(t, valid)
		})

		t.Run("with_thin_pools_returns_true", func(t *testing.T) {
			const (
				firstBd  = "first"
				secondBd = "second"
			)
			bds := map[string]v1alpha1.BlockDevice{
				firstBd: {
					ObjectMeta: v1.ObjectMeta{
						Name: firstBd,
					},
					Status: v1alpha1.BlockDeviceStatus{
						Size:       resource.MustParse("1G"),
						Consumable: true,
					},
				},
				secondBd: {
					ObjectMeta: v1.ObjectMeta{
						Name: secondBd,
					},
					Status: v1alpha1.BlockDeviceStatus{
						Size:       resource.MustParse("1G"),
						Consumable: true,
					},
				},
			}
			lvg := &v1alpha1.LVMVolumeGroup{
				Spec: v1alpha1.LVMVolumeGroupSpec{
					ThinPools: []v1alpha1.LVMVolumeGroupThinPoolSpec{
						{
							Size: "1G",
						},
					},
				},
			}

			valid, reason := r.validateLVGForCreateFunc(lvg, bds)
			if assert.True(t, valid) {
				assert.Equal(t, "", reason)
			}
		})

		t.Run("with_thin_pools_returns_false", func(t *testing.T) {
			const (
				firstBd  = "first"
				secondBd = "second"
			)
			bds := map[string]v1alpha1.BlockDevice{
				firstBd: {
					ObjectMeta: v1.ObjectMeta{
						Name: firstBd,
					},
					Status: v1alpha1.BlockDeviceStatus{
						Size:       resource.MustParse("1G"),
						Consumable: true,
					},
				},
				secondBd: {
					ObjectMeta: v1.ObjectMeta{
						Name: secondBd,
					},
					Status: v1alpha1.BlockDeviceStatus{
						Size:       resource.MustParse("1G"),
						Consumable: true,
					},
				},
			}
			lvg := &v1alpha1.LVMVolumeGroup{
				Spec: v1alpha1.LVMVolumeGroupSpec{
					ThinPools: []v1alpha1.LVMVolumeGroupThinPoolSpec{
						{
							Size: "3G",
						},
					},
				},
			}

			valid, _ := r.validateLVGForCreateFunc(lvg, bds)
			assert.False(t, valid)
		})
	})

	t.Run("identifyLVGReconcileFunc", func(t *testing.T) {
		t.Run("returns_create", func(t *testing.T) {
			const vgName = "test-vg"
			lvg := &v1alpha1.LVMVolumeGroup{
				Spec: v1alpha1.LVMVolumeGroupSpec{
					ActualVGNameOnTheNode: vgName,
				},
			}

			actual := r.identifyLVGReconcileFunc(lvg)
			assert.Equal(t, CreateReconcile, actual)
		})

		t.Run("returns_update", func(t *testing.T) {
			const vgName = "test-vg"
			lvg := &v1alpha1.LVMVolumeGroup{
				Spec: v1alpha1.LVMVolumeGroupSpec{
					ActualVGNameOnTheNode: vgName,
				},
			}
			vgs := []internal.VGData{
				{
					VGName: vgName,
				},
			}

			r.sdsCache.StoreVGs(vgs, bytes.Buffer{})

			actual := r.identifyLVGReconcileFunc(lvg)
			assert.Equal(t, UpdateReconcile, actual)
		})

		t.Run("returns_delete", func(t *testing.T) {
			const vgName = "test-vg"
			lvg := &v1alpha1.LVMVolumeGroup{
				Spec: v1alpha1.LVMVolumeGroupSpec{
					ActualVGNameOnTheNode: vgName,
				},
			}
			lvg.DeletionTimestamp = &v1.Time{}
			vgs := []internal.VGData{
				{
					VGName: vgName,
				},
			}

			r.sdsCache.StoreVGs(vgs, bytes.Buffer{})

			actual := r.identifyLVGReconcileFunc(lvg)
			assert.Equal(t, DeleteReconcile, actual)
		})
	})

	t.Run("removeLVGFinalizerIfExist", func(t *testing.T) {
		t.Run("not_exist_no_remove", func(t *testing.T) {
			lvg := &v1alpha1.LVMVolumeGroup{}

			removed, err := r.removeLVGFinalizerIfExist(ctx, lvg)
			if err != nil {
				t.Error(err)
			}

			assert.False(t, removed)
		})

		t.Run("does_exist_remove", func(t *testing.T) {
			const lvgName = "test-lvg"
			lvg := &v1alpha1.LVMVolumeGroup{}
			lvg.Name = lvgName
			lvg.Finalizers = append(lvg.Finalizers, internal.SdsNodeConfiguratorFinalizer)

			err := cl.Create(ctx, lvg)
			if err != nil {
				t.Error(err)
			}

			defer func() {
				err = cl.Delete(ctx, lvg)
				if err != nil {
					t.Error(err)
				}
			}()

			removed, err := r.removeLVGFinalizerIfExist(ctx, lvg)
			if err != nil {
				t.Error(err)
			}

			if assert.True(t, removed) {
				updatedLVG := &v1alpha1.LVMVolumeGroup{}
				err = cl.Get(ctx, client.ObjectKey{
					Name: lvgName,
				}, updatedLVG)
				if err != nil {
					t.Error(err)
				}

				assert.False(t, slices.Contains(updatedLVG.Finalizers, internal.SdsNodeConfiguratorFinalizer))
			}
		})
	})

	t.Run("getLVForVG", func(t *testing.T) {
		const (
			firstLV  = "first"
			secondLV = "second"
			vgName   = "test-vg"
		)
		lvs := []internal.LVData{
			{
				LVName: firstLV,
				VGName: vgName,
			},
			{
				LVName: secondLV,
				VGName: "other",
			},
		}

		ch := cache.New()
		ch.StoreLVs(lvs, bytes.Buffer{})
		expected := []string{firstLV}

		actual := r.getLVForVG(vgName)

		assert.ElementsMatch(t, expected, actual)
	})

	t.Run("countVGSizeByBlockDevices", func(t *testing.T) {
		const (
			firstBd  = "first"
			secondBd = "second"
		)
		bds := map[string]v1alpha1.BlockDevice{
			firstBd: {
				ObjectMeta: v1.ObjectMeta{
					Name: firstBd,
				},
				Status: v1alpha1.BlockDeviceStatus{
					Size: resource.MustParse("1G"),
				},
			},
			secondBd: {
				ObjectMeta: v1.ObjectMeta{
					Name: secondBd,
				},
				Status: v1alpha1.BlockDeviceStatus{
					Size: resource.MustParse("1G"),
				},
			},
		}

		expected := resource.MustParse("2G")

		actual := countVGSizeByBlockDevices(bds)
		assert.Equal(t, expected.Value(), actual.Value())
	})

	t.Run("getRequestedSizeFromString", func(t *testing.T) {
		t.Run("for_percent_size", func(t *testing.T) {
			actual, err := cutils.GetRequestedSizeFromString("50%", resource.MustParse("10G"))
			if err != nil {
				t.Error(err)
			}

			expected := resource.MustParse("5G")
			assert.Equal(t, expected.Value(), actual.Value())
		})

		t.Run("for_number_size", func(t *testing.T) {
			actual, err := cutils.GetRequestedSizeFromString("5G", resource.MustParse("10G"))
			if err != nil {
				t.Error(err)
			}

			expected := resource.MustParse("5G")
			assert.Equal(t, expected.Value(), actual.Value())
		})
	})

	t.Run("extractPathsFromBlockDevices", func(t *testing.T) {
		t.Run("if_specified_returns_only_them", func(t *testing.T) {
			const (
				firstBd  = "first"
				secondBd = "second"

				firstPath  = "first-path"
				secondPath = "second-path"
			)
			bdNames := []string{firstBd}
			bds := map[string]v1alpha1.BlockDevice{
				firstBd: {
					ObjectMeta: v1.ObjectMeta{
						Name: firstBd,
					},
					Status: v1alpha1.BlockDeviceStatus{
						Path: firstPath,
					},
				},
				secondBd: {
					ObjectMeta: v1.ObjectMeta{
						Name: secondBd,
					},
					Status: v1alpha1.BlockDeviceStatus{
						Path: secondPath,
					},
				},
			}

			expected := []string{firstPath}
			actual := extractPathsFromBlockDevices(bdNames, bds)
			assert.ElementsMatch(t, expected, actual)
		})

		t.Run("if_nil_returns_all", func(t *testing.T) {
			const (
				firstBd  = "first"
				secondBd = "second"

				firstPath  = "first-path"
				secondPath = "second-path"
			)
			bds := map[string]v1alpha1.BlockDevice{
				firstBd: {
					ObjectMeta: v1.ObjectMeta{
						Name: firstBd,
					},
					Status: v1alpha1.BlockDeviceStatus{
						Path: firstPath,
					},
				},
				secondBd: {
					ObjectMeta: v1.ObjectMeta{
						Name: secondBd,
					},
					Status: v1alpha1.BlockDeviceStatus{
						Path: secondPath,
					},
				},
			}

			expected := []string{firstPath, secondPath}
			actual := extractPathsFromBlockDevices(nil, bds)
			assert.ElementsMatch(t, expected, actual)
		})
	})

	t.Run("validateSpecBlockDevices", func(t *testing.T) {
		t.Run("validation_passes", func(t *testing.T) {
			const (
				nodeName = "nodeName"
			)
			lvg := &v1alpha1.LVMVolumeGroup{
				Spec: v1alpha1.LVMVolumeGroupSpec{
					Local: v1alpha1.LVMVolumeGroupLocalSpec{
						NodeName: nodeName,
					},
					BlockDeviceSelector: &v1.LabelSelector{
						MatchExpressions: []v1.LabelSelectorRequirement{
							{
								Key:      internal.MetadataNameLabelKey,
								Operator: v1.LabelSelectorOpIn,
								Values:   []string{"first", "second"},
							},
						},
					},
				},
			}

			bds := map[string]v1alpha1.BlockDevice{
				"first": {
					ObjectMeta: v1.ObjectMeta{
						Name: "first",
					},
					Status: v1alpha1.BlockDeviceStatus{
						NodeName: nodeName,
					},
				},

				"second": {
					ObjectMeta: v1.ObjectMeta{
						Name: "second",
					},
					Status: v1alpha1.BlockDeviceStatus{
						NodeName: nodeName,
					},
				},
			}

			valid, reason := validateSpecBlockDevices(lvg, bds)
			if assert.True(t, valid) {
				assert.Equal(t, "", reason)
			}
		})

		t.Run("validation_fails_due_to_bd_left_the_selector", func(t *testing.T) {
			lvg := &v1alpha1.LVMVolumeGroup{
				Status: v1alpha1.LVMVolumeGroupStatus{
					Nodes: []v1alpha1.LVMVolumeGroupNode{
						{
							Devices: []v1alpha1.LVMVolumeGroupDevice{
								{
									BlockDevice: "first",
								},
								{
									BlockDevice: "second",
								},
							},
							Name: "some-node",
						},
					},
				},
			}

			bds := map[string]v1alpha1.BlockDevice{
				"second": {
					ObjectMeta: v1.ObjectMeta{
						Name: "second",
					},
				},
			}

			valid, reason := validateSpecBlockDevices(lvg, bds)
			assert.False(t, valid)
			assert.Equal(t, "these BlockDevices no longer match the blockDeviceSelector: first", reason)
		})

		t.Run("validation_fails_due_to_bd_has_dif_node", func(t *testing.T) {
			const (
				nodeName = "nodeName"
			)
			lvg := &v1alpha1.LVMVolumeGroup{
				Spec: v1alpha1.LVMVolumeGroupSpec{
					Local: v1alpha1.LVMVolumeGroupLocalSpec{
						NodeName: nodeName,
					},
					BlockDeviceSelector: &v1.LabelSelector{
						MatchExpressions: []v1.LabelSelectorRequirement{
							{
								Key:      internal.MetadataNameLabelKey,
								Operator: v1.LabelSelectorOpIn,
								Values:   []string{"first", "second"},
							},
						},
					},
				},
			}

			bds := map[string]v1alpha1.BlockDevice{
				"first": {
					ObjectMeta: v1.ObjectMeta{
						Name: "first",
					},
					Status: v1alpha1.BlockDeviceStatus{
						NodeName: nodeName,
					},
				},
				"second": {
					ObjectMeta: v1.ObjectMeta{
						Name: "second",
					},
					Status: v1alpha1.BlockDeviceStatus{
						NodeName: "another-node",
					},
				},
			}

			valid, reason := validateSpecBlockDevices(lvg, bds)
			assert.False(t, valid)
			assert.Equal(t, "block devices second have different node names from LVMVolumeGroup Local.NodeName", reason)
		})

		t.Run("validation_fails_due_to_no_block_devices_were_found", func(t *testing.T) {
			const (
				nodeName = "nodeName"
			)
			lvg := &v1alpha1.LVMVolumeGroup{
				Spec: v1alpha1.LVMVolumeGroupSpec{
					Local: v1alpha1.LVMVolumeGroupLocalSpec{
						NodeName: nodeName,
					},
					BlockDeviceSelector: &v1.LabelSelector{
						MatchExpressions: []v1.LabelSelectorRequirement{
							{
								Key:      internal.MetadataNameLabelKey,
								Operator: v1.LabelSelectorOpIn,
								Values:   []string{"first", "second"},
							},
						},
					},
				},
			}

			valid, reason := validateSpecBlockDevices(lvg, nil)
			assert.False(t, valid)
			assert.Equal(t, "none of specified BlockDevices were found", reason)
		})

		t.Run("validation_fails_due_to_some_blockdevice_were_not_found", func(t *testing.T) {
			const (
				nodeName = "nodeName"
			)
			lvg := &v1alpha1.LVMVolumeGroup{
				Spec: v1alpha1.LVMVolumeGroupSpec{
					Local: v1alpha1.LVMVolumeGroupLocalSpec{
						NodeName: nodeName,
					},
					BlockDeviceSelector: &v1.LabelSelector{
						MatchExpressions: []v1.LabelSelectorRequirement{
							{
								Key:      internal.MetadataNameLabelKey,
								Operator: v1.LabelSelectorOpIn,
								Values:   []string{"first", "second"},
							},
						},
					},
				},
			}

			bds := map[string]v1alpha1.BlockDevice{
				"first": {
					ObjectMeta: v1.ObjectMeta{
						Name: "first",
					},
					Status: v1alpha1.BlockDeviceStatus{
						NodeName: nodeName,
					},
				},
			}

			valid, _ := validateSpecBlockDevices(lvg, bds)
			assert.False(t, valid)
		})
	})

	t.Run("syncThinPoolsAllocationLimit", func(t *testing.T) {
		const lvgName = "test"
		lvg := &v1alpha1.LVMVolumeGroup{
			ObjectMeta: v1.ObjectMeta{
				Name: lvgName,
			},
			Spec: v1alpha1.LVMVolumeGroupSpec{
				ThinPools: []v1alpha1.LVMVolumeGroupThinPoolSpec{
					{
						Name:            "first",
						Size:            "1G",
						AllocationLimit: "200%",
					},
				},
			},
			Status: v1alpha1.LVMVolumeGroupStatus{
				ThinPools: []v1alpha1.LVMVolumeGroupThinPoolStatus{
					{
						Name:            "first",
						AllocationLimit: "150%",
					},
				},
			},
		}

		err := cl.Create(ctx, lvg)
		if err != nil {
			t.Error(err)
		}

		defer func() {
			err = cl.Delete(ctx, lvg)
			if err != nil {
				t.Error(err)
			}
		}()

		err = r.syncThinPoolsAllocationLimit(ctx, lvg)
		if err != nil {
			t.Error(err)
		}

		updatedLVG := &v1alpha1.LVMVolumeGroup{}
		err = cl.Get(ctx, client.ObjectKey{
			Name: lvgName,
		}, updatedLVG)

		assert.Equal(t, lvg.Spec.ThinPools[0].AllocationLimit, lvg.Status.ThinPools[0].AllocationLimit)
	})

	t.Run("addLVGFinalizerIfNotExist", func(t *testing.T) {
		t.Run("not_exist_adds", func(t *testing.T) {
			const (
				lvgName = "test"
			)
			lvg := &v1alpha1.LVMVolumeGroup{}
			lvg.Name = lvgName
			lvg.Finalizers = []string{}

			err := cl.Create(ctx, lvg)
			if err != nil {
				t.Error(err)
			}

			defer func() {
				err = cl.Delete(ctx, lvg)
				if err != nil {
					t.Error(err)
				}
			}()

			added, err := r.addLVGFinalizerIfNotExist(ctx, lvg)
			if err != nil {
				t.Error(err)
			}

			if assert.True(t, added) {
				updatedLVG := &v1alpha1.LVMVolumeGroup{}
				err = cl.Get(ctx, client.ObjectKey{
					Name: lvgName,
				}, updatedLVG)

				assert.True(t, slices.Contains(updatedLVG.Finalizers, internal.SdsNodeConfiguratorFinalizer))
			}
		})

		t.Run("does_exist_no_adds", func(t *testing.T) {
			const (
				lvgName = "test-1"
			)
			lvg := &v1alpha1.LVMVolumeGroup{}
			lvg.Name = lvgName
			lvg.Finalizers = []string{
				internal.SdsNodeConfiguratorFinalizer,
			}

			err := cl.Create(ctx, lvg)
			if err != nil {
				t.Error(err)
			}

			defer func() {
				err = cl.Delete(ctx, lvg)
				if err != nil {
					t.Error(err)
				}
			}()

			added, err := r.addLVGFinalizerIfNotExist(ctx, lvg)
			if err != nil {
				t.Error(err)
			}

			if assert.False(t, added) {
				updatedLVG := &v1alpha1.LVMVolumeGroup{}
				err = cl.Get(ctx, client.ObjectKey{
					Name: lvgName,
				}, updatedLVG)

				assert.True(t, slices.Contains(updatedLVG.Finalizers, internal.SdsNodeConfiguratorFinalizer))
			}
		})
	})

	t.Run("updateLVGConditionIfNeeded", func(t *testing.T) {
		t.Run("diff_states_updates", func(t *testing.T) {
			const (
				lvgName   = "test-name"
				badReason = "bad"
			)
			curTime := v1.NewTime(time.Now())
			lvg := &v1alpha1.LVMVolumeGroup{}
			lvg.Name = lvgName
			lvg.Generation = 1
			lvg.Status.Conditions = []v1.Condition{
				{
					Type:               internal.TypeVGConfigurationApplied,
					Status:             v1.ConditionTrue,
					ObservedGeneration: 1,
					LastTransitionTime: curTime,
					Reason:             "",
					Message:            "",
				},
			}

			err := cl.Create(ctx, lvg)
			if err != nil {
				t.Error(err)
			}

			err = r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, badReason, "")
			if err != nil {
				t.Error(err)
			}

			notUpdatedLVG := &v1alpha1.LVMVolumeGroup{}
			err = cl.Get(ctx, client.ObjectKey{
				Name: lvgName,
			}, notUpdatedLVG)
			if err != nil {
				t.Error(err)
			}

			assert.Equal(t, notUpdatedLVG.Status.Conditions[0].Status, v1.ConditionFalse)
			assert.Equal(t, notUpdatedLVG.Status.Conditions[0].Reason, badReason)

			assert.NotEqual(t, curTime, lvg.Status.Conditions[0].LastTransitionTime)
		})

		t.Run("same_states_does_not_update", func(t *testing.T) {
			const (
				lvgName = "test-name-2"
			)
			curTime := v1.NewTime(time.Now())
			lvg := &v1alpha1.LVMVolumeGroup{}
			lvg.Name = lvgName
			lvg.Generation = 1
			lvg.Status.Conditions = []v1.Condition{
				{
					Type:               internal.TypeVGConfigurationApplied,
					Status:             v1.ConditionTrue,
					ObservedGeneration: 1,
					LastTransitionTime: curTime,
					Reason:             "",
					Message:            "",
				},
			}

			err := cl.Create(ctx, lvg)
			if err != nil {
				t.Error(err)
			}

			err = r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionTrue, internal.TypeVGConfigurationApplied, "", "")
			if err != nil {
				t.Error(err)
			}

			assert.Equal(t, curTime, lvg.Status.Conditions[0].LastTransitionTime)
		})
	})

	t.Run("shouldReconcileLVGByDeleteFunc", func(t *testing.T) {
		t.Run("returns_true", func(t *testing.T) {
			lvg := &v1alpha1.LVMVolumeGroup{}
			lvg.DeletionTimestamp = &v1.Time{}

			assert.True(t, r.shouldReconcileLVGByDeleteFunc(lvg))
		})

		t.Run("returns_false", func(t *testing.T) {
			lvg := &v1alpha1.LVMVolumeGroup{}
			lvg.DeletionTimestamp = nil

			assert.False(t, r.shouldReconcileLVGByDeleteFunc(lvg))
		})
	})

	t.Run("shouldLVGWatcherReconcileUpdateEvent", func(t *testing.T) {
		t.Run("deletion_timestamp_not_nil_returns_true", func(t *testing.T) {
			oldLVG := &v1alpha1.LVMVolumeGroup{}
			newLVG := &v1alpha1.LVMVolumeGroup{}
			newLVG.DeletionTimestamp = &v1.Time{}
			assert.True(t, r.shouldLVGWatcherReconcileUpdateEvent(oldLVG, newLVG))
		})

		t.Run("spec_is_diff_returns_true", func(t *testing.T) {
			oldLVG := &v1alpha1.LVMVolumeGroup{}
			newLVG := &v1alpha1.LVMVolumeGroup{}
			oldLVG.Spec.BlockDeviceSelector = &v1.LabelSelector{MatchLabels: map[string]string{"first": "second"}}
			newLVG.Spec.BlockDeviceSelector = &v1.LabelSelector{MatchLabels: map[string]string{"second": "second"}}
			assert.True(t, r.shouldLVGWatcherReconcileUpdateEvent(oldLVG, newLVG))
		})

		t.Run("condition_vg_configuration_applied_is_updating_returns_false", func(t *testing.T) {
			oldLVG := &v1alpha1.LVMVolumeGroup{}
			newLVG := &v1alpha1.LVMVolumeGroup{}
			newLVG.Name = "test-name"
			newLVG.Labels = map[string]string{LVGMetadateNameLabelKey: "test-name"}
			newLVG.Status.Conditions = []v1.Condition{
				{
					Type:   internal.TypeVGConfigurationApplied,
					Reason: internal.ReasonUpdating,
				},
			}
			assert.False(t, r.shouldLVGWatcherReconcileUpdateEvent(oldLVG, newLVG))
		})

		t.Run("condition_vg_configuration_applied_is_creating_returns_false", func(t *testing.T) {
			oldLVG := &v1alpha1.LVMVolumeGroup{}
			newLVG := &v1alpha1.LVMVolumeGroup{}
			newLVG.Name = "test-name"
			newLVG.Status.Conditions = []v1.Condition{
				{
					Type:   internal.TypeVGConfigurationApplied,
					Reason: internal.ReasonCreating,
				},
			}
			newLVG.Labels = map[string]string{LVGMetadateNameLabelKey: newLVG.Name}
			assert.False(t, r.shouldLVGWatcherReconcileUpdateEvent(oldLVG, newLVG))
		})

		t.Run("label_is_not_the_same_returns_true", func(t *testing.T) {
			oldLVG := &v1alpha1.LVMVolumeGroup{}
			newLVG := &v1alpha1.LVMVolumeGroup{}
			newLVG.Name = "test-name"
			newLVG.Status.Conditions = []v1.Condition{
				{
					Type:   internal.TypeVGConfigurationApplied,
					Reason: internal.ReasonCreating,
				},
			}
			newLVG.Labels = map[string]string{LVGMetadateNameLabelKey: "some-other-name"}
			assert.True(t, r.shouldLVGWatcherReconcileUpdateEvent(oldLVG, newLVG))
		})

		t.Run("dev_size_and_pv_size_are_diff_returns_true", func(t *testing.T) {
			oldLVG := &v1alpha1.LVMVolumeGroup{}
			newLVG := &v1alpha1.LVMVolumeGroup{}
			newLVG.Status.Nodes = []v1alpha1.LVMVolumeGroupNode{
				{
					Devices: []v1alpha1.LVMVolumeGroupDevice{
						{
							BlockDevice: "test",
							DevSize:     resource.MustParse("1G"),
							PVSize:      resource.MustParse("2G"),
						},
					},
					Name: "some-node",
				},
			}
			assert.True(t, r.shouldLVGWatcherReconcileUpdateEvent(oldLVG, newLVG))
		})
	})

	t.Run("shouldUpdateLVGLabels", func(t *testing.T) {
		t.Run("labels_nil_returns_true", func(t *testing.T) {
			lvg := &v1alpha1.LVMVolumeGroup{}
			assert.True(t, r.shouldUpdateLVGLabels(lvg, "key", "value"))
		})
		t.Run("no_such_label_returns_true", func(t *testing.T) {
			lvg := &v1alpha1.LVMVolumeGroup{}
			lvg.Labels = map[string]string{"key": "value"}
			assert.True(t, r.shouldUpdateLVGLabels(lvg, "other-key", "value"))
		})
		t.Run("key_exists_other_value_returns_true", func(t *testing.T) {
			const key = "key"
			lvg := &v1alpha1.LVMVolumeGroup{}
			lvg.Labels = map[string]string{key: "value"}
			assert.True(t, r.shouldUpdateLVGLabels(lvg, key, "other-value"))
		})
		t.Run("all_good_returns_false", func(t *testing.T) {
			const (
				key   = "key"
				value = "value"
			)
			lvg := &v1alpha1.LVMVolumeGroup{}
			lvg.Labels = map[string]string{key: value}
			assert.False(t, r.shouldUpdateLVGLabels(lvg, key, value))
		})
	})

	t.Run("checkIfVGExist", func(t *testing.T) {
		const targetName = "test"
		vgs := []internal.VGData{
			{
				VGName: targetName,
			},
			{
				VGName: "another-name",
			},
		}

		t.Run("returns_true", func(t *testing.T) {
			assert.True(t, checkIfVGExist(targetName, vgs))
		})

		t.Run("returns_false", func(t *testing.T) {
			assert.False(t, checkIfVGExist("not-existed", vgs))
		})
	})

	t.Run("DeleteLVMVolumeGroup", func(t *testing.T) {
		const (
			lvgName  = "test=lvg"
			nodeName = "test-node"
		)

		lvgToDelete := &v1alpha1.LVMVolumeGroup{
			ObjectMeta: v1.ObjectMeta{
				Name: lvgName,
			},
			Status: v1alpha1.LVMVolumeGroupStatus{
				Nodes: []v1alpha1.LVMVolumeGroupNode{
					{
						Name: nodeName,
					},
				},
			},
		}

		err := cl.Create(ctx, lvgToDelete)
		if err != nil {
			t.Error(err)
		}

		defer func() {
			_ = cl.Delete(ctx, lvgToDelete)
		}()

		lvgCheck := &v1alpha1.LVMVolumeGroup{}
		err = cl.Get(ctx, client.ObjectKey{
			Name: lvgName,
		}, lvgCheck)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, lvgName, lvgCheck.Name)

		err = r.lvgCl.DeleteLVMVolumeGroup(ctx, lvgToDelete)
		if err != nil {
			t.Error(err)
		}

		lvgNewCheck := &v1alpha1.LVMVolumeGroup{}
		err = cl.Get(ctx, client.ObjectKey{
			Name: lvgName,
		}, lvgNewCheck)
		if assert.True(t, errors2.IsNotFound(err)) {
			assert.Equal(t, "", lvgNewCheck.Name)
		}
	})

	t.Run("getLVMVolumeGroup_lvg_exists_returns_correct", func(t *testing.T) {
		const name = "test_name"
		lvgToCreate := &v1alpha1.LVMVolumeGroup{
			ObjectMeta: v1.ObjectMeta{
				Name: name,
			},
		}

		err := cl.Create(ctx, lvgToCreate)
		if err != nil {
			t.Error(err)
		} else {
			defer func() {
				err = cl.Delete(ctx, lvgToCreate)
				if err != nil {
					t.Error(err)
				}
			}()
		}

		actual, err := r.lvgCl.GetLVMVolumeGroup(ctx, name)
		if assert.NoError(t, err) {
			assert.NotNil(t, actual)
			assert.Equal(t, name, actual.Name)
		}
	})

	t.Run("getLVMVolumeGroup_lvg_doesnt_exist_returns_nil", func(t *testing.T) {
		const name = "test_name"
		testObj := &v1alpha1.LVMVolumeGroup{
			ObjectMeta: v1.ObjectMeta{
				Name: name,
			},
		}

		err := cl.Create(ctx, testObj)
		if err != nil {
			t.Error(err)
		} else {
			defer func() {
				err = cl.Delete(ctx, testObj)
				if err != nil {
					t.Error(err)
				}
			}()
		}

		actual, err := r.lvgCl.GetLVMVolumeGroup(ctx, "another-name")

		if assert.EqualError(t, err, "lvmvolumegroups.storage.deckhouse.io \"another-name\" not found") {
			assert.Nil(t, actual)
		}
	})
}
