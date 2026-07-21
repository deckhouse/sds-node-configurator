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

package lvg

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	errors2 "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/strings/slices"
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

func TestLVMVolumeGroupWatcherCtrl(t *testing.T) {
	ctx := context.Background()

	t.Run("validateLVGForUpdateFunc", func(t *testing.T) {
		t.Run("without_thin_pools_returns_true", func(t *testing.T) {
			r := setupReconciler()

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

			r.sdsCache.StorePVs(pvs, bytes.Buffer{})

			valid, reason := r.validateLVGForUpdateFunc(ctx, lvg, bds)
			if assert.True(t, valid) {
				assert.Equal(t, "", reason)
			}
		})

		t.Run("without_thin_pools_returns_false", func(t *testing.T) {
			r := setupReconciler()

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
			valid, _ := r.validateLVGForUpdateFunc(ctx, lvg, bds)
			assert.False(t, valid)
		})

		t.Run("with_thin_pools_returns_true", func(t *testing.T) {
			r := setupReconciler()

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

			valid, reason := r.validateLVGForUpdateFunc(ctx, lvg, bds)
			if assert.True(t, valid) {
				assert.Equal(t, "", reason)
			}
		})

		t.Run("with_thin_pools_returns_false", func(t *testing.T) {
			r := setupReconciler()

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
							Name:            "first-thin",
							Size:            "100%",
							AllocationLimit: "150%",
						},
						{
							Name:            "second-thin",
							Size:            "1G",
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

			valid, _ := r.validateLVGForUpdateFunc(ctx, lvg, bds)
			assert.False(t, valid)
		})
	})

	t.Run("validateLVGForCreateFunc", func(t *testing.T) {
		t.Run("without_thin_pools_returns_true", func(t *testing.T) {
			r := setupReconciler()

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

			valid, reason := r.validateLVGForCreateFunc(ctx, lvg, bds)
			if assert.True(t, valid) {
				assert.Equal(t, "", reason)
			}
		})

		t.Run("without_thin_pools_returns_false", func(t *testing.T) {
			r := setupReconciler()
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

			valid, _ := r.validateLVGForCreateFunc(ctx, lvg, bds)
			assert.False(t, valid)
		})

		t.Run("with_thin_pools_returns_true", func(t *testing.T) {
			r := setupReconciler()
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

			valid, reason := r.validateLVGForCreateFunc(ctx, lvg, bds)
			if assert.True(t, valid) {
				assert.Equal(t, "", reason)
			}
		})

		t.Run("with_thin_pools_returns_false", func(t *testing.T) {
			r := setupReconciler()
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

			valid, _ := r.validateLVGForCreateFunc(ctx, lvg, bds)
			assert.False(t, valid)
		})

		t.Run("with_full_vg_percent_thin_pool_returns_true", func(t *testing.T) {
			// A single "100%" thin pool must pass validation. The raw VG size
			// (sum of block devices) is not extent-aligned, so rounding the
			// percentage up would land one extent past the VG and wrongly fail;
			// the pool is created with %FREE and fits.
			r := setupReconciler()
			bds := map[string]v1alpha1.BlockDevice{
				"first": {
					ObjectMeta: v1.ObjectMeta{Name: "first"},
					Status: v1alpha1.BlockDeviceStatus{
						// deliberately not a multiple of the 4Mi extent size
						Size:       resource.MustParse("20481Mi"),
						Consumable: true,
					},
				},
			}
			lvg := &v1alpha1.LVMVolumeGroup{
				Spec: v1alpha1.LVMVolumeGroupSpec{
					ThinPools: []v1alpha1.LVMVolumeGroupThinPoolSpec{
						{Name: "thin", Size: "100%"},
					},
				},
			}

			valid, reason := r.validateLVGForCreateFunc(ctx, lvg, bds)
			if assert.True(t, valid) {
				assert.Equal(t, "", reason)
			}
		})

		t.Run("with_full_vg_percent_thin_pool_and_another_returns_false", func(t *testing.T) {
			// "100%" alongside any other thin pool cannot fit.
			r := setupReconciler()
			bds := map[string]v1alpha1.BlockDevice{
				"first": {
					ObjectMeta: v1.ObjectMeta{Name: "first"},
					Status: v1alpha1.BlockDeviceStatus{
						Size:       resource.MustParse("20481Mi"),
						Consumable: true,
					},
				},
			}
			lvg := &v1alpha1.LVMVolumeGroup{
				Spec: v1alpha1.LVMVolumeGroupSpec{
					ThinPools: []v1alpha1.LVMVolumeGroupThinPoolSpec{
						{Name: "thin", Size: "100%"},
						{Name: "thin-2", Size: "1Gi"},
					},
				},
			}

			valid, _ := r.validateLVGForCreateFunc(ctx, lvg, bds)
			assert.False(t, valid)
		})
	})

	t.Run("identifyLVGReconcileFunc", func(t *testing.T) {
		t.Run("returns_create", func(t *testing.T) {
			r := setupReconciler()
			const vgName = "test-vg"
			lvg := &v1alpha1.LVMVolumeGroup{
				Spec: v1alpha1.LVMVolumeGroupSpec{
					ActualVGNameOnTheNode: vgName,
				},
			}

			actual := r.identifyLVGReconcileFunc(lvg)
			assert.Equal(t, internal.CreateReconcile, actual)
		})

		t.Run("returns_update", func(t *testing.T) {
			r := setupReconciler()
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
			assert.Equal(t, internal.UpdateReconcile, actual)
		})

		t.Run("returns_delete", func(t *testing.T) {
			r := setupReconciler()

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
			assert.Equal(t, internal.DeleteReconcile, actual)
		})
	})

	t.Run("removeLVGFinalizerIfExist", func(t *testing.T) {
		t.Run("not_exist_no_remove", func(t *testing.T) {
			r := setupReconciler()

			lvg := &v1alpha1.LVMVolumeGroup{}

			removed, err := r.removeLVGFinalizerIfExist(ctx, lvg)
			if err != nil {
				t.Error(err)
			}

			assert.False(t, removed)
		})

		t.Run("does_exist_remove", func(t *testing.T) {
			r := setupReconciler()

			const lvgName = "test-lvg"
			lvg := &v1alpha1.LVMVolumeGroup{}
			lvg.Name = lvgName
			lvg.Finalizers = append(lvg.Finalizers, internal.SdsNodeConfiguratorFinalizer)

			err := r.cl.Create(ctx, lvg)
			if err != nil {
				t.Error(err)
			}

			defer func() {
				err = r.cl.Delete(ctx, lvg)
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
				err = r.cl.Get(ctx, client.ObjectKey{
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
		r := setupReconciler()

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

		r.sdsCache.StoreLVs(lvs, bytes.Buffer{})
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
			actual, err := utils.GetRequestedSizeFromString("50%", resource.MustParse("10G"))
			if err != nil {
				t.Error(err)
			}

			expected := resource.MustParse("5G")
			assert.Equal(t, expected.Value(), actual.Value())
		})

		t.Run("for_number_size", func(t *testing.T) {
			actual, err := utils.GetRequestedSizeFromString("5G", resource.MustParse("10G"))
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

		t.Run("validation_passes_with_exists_operator_on_metadata_name", func(t *testing.T) {
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
								Operator: v1.LabelSelectorOpExists,
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

			valid, reason := validateSpecBlockDevices(lvg, bds)
			assert.True(t, valid)
			assert.Empty(t, reason)
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

		t.Run("validation_passes_for_file_only_lvg_without_selector", func(t *testing.T) {
			// A file-only LVMVolumeGroup carries no blockDeviceSelector.
			// validateSpecBlockDevices must not dereference the nil selector
			// (regression: it used to panic in Reconcile).
			lvg := &v1alpha1.LVMVolumeGroup{
				Spec: v1alpha1.LVMVolumeGroupSpec{
					Local: v1alpha1.LVMVolumeGroupLocalSpec{NodeName: "nodeName"},
					FileDevices: []v1alpha1.LVMVolumeGroupFileDeviceSpec{
						{Directory: "/data", Size: resource.MustParse("10Gi")},
					},
				},
			}
			bds := map[string]v1alpha1.BlockDevice{
				"first": {ObjectMeta: v1.ObjectMeta{Name: "first"}},
			}

			assert.NotPanics(t, func() {
				valid, reason := validateSpecBlockDevices(lvg, bds)
				assert.True(t, valid)
				assert.Empty(t, reason)
			})
		})
	})

	t.Run("syncThinPoolsAllocationLimit", func(t *testing.T) {
		r := setupReconciler()

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

		err := r.cl.Create(ctx, lvg)
		if err != nil {
			t.Error(err)
		}

		defer func() {
			err = r.cl.Delete(ctx, lvg)
			if err != nil {
				t.Error(err)
			}
		}()

		err = r.syncThinPoolsAllocationLimit(ctx, lvg)
		if err != nil {
			t.Error(err)
		}

		updatedLVG := &v1alpha1.LVMVolumeGroup{}
		err = r.cl.Get(ctx, client.ObjectKey{
			Name: lvgName,
		}, updatedLVG)

		assert.Equal(t, lvg.Spec.ThinPools[0].AllocationLimit, lvg.Status.ThinPools[0].AllocationLimit)
	})

	t.Run("addLVGFinalizerIfNotExist", func(t *testing.T) {
		t.Run("not_exist_adds", func(t *testing.T) {
			r := setupReconciler()

			const (
				lvgName = "test"
			)
			lvg := &v1alpha1.LVMVolumeGroup{}
			lvg.Name = lvgName
			lvg.Finalizers = []string{}

			err := r.cl.Create(ctx, lvg)
			if err != nil {
				t.Error(err)
			}

			defer func() {
				err = r.cl.Delete(ctx, lvg)
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
				err = r.cl.Get(ctx, client.ObjectKey{
					Name: lvgName,
				}, updatedLVG)

				assert.True(t, slices.Contains(updatedLVG.Finalizers, internal.SdsNodeConfiguratorFinalizer))
			}
		})

		t.Run("does_exist_no_adds", func(t *testing.T) {
			r := setupReconciler()
			const (
				lvgName = "test-1"
			)
			lvg := &v1alpha1.LVMVolumeGroup{}
			lvg.Name = lvgName
			lvg.Finalizers = []string{
				internal.SdsNodeConfiguratorFinalizer,
			}

			err := r.cl.Create(ctx, lvg)
			if err != nil {
				t.Error(err)
			}

			defer func() {
				err = r.cl.Delete(ctx, lvg)
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
				err = r.cl.Get(ctx, client.ObjectKey{
					Name: lvgName,
				}, updatedLVG)

				assert.True(t, slices.Contains(updatedLVG.Finalizers, internal.SdsNodeConfiguratorFinalizer))
			}
		})
	})

	t.Run("updateLVGConditionIfNeeded", func(t *testing.T) {
		t.Run("diff_states_updates", func(t *testing.T) {
			r := setupReconciler()
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

			err := r.cl.Create(ctx, lvg)
			if err != nil {
				t.Error(err)
			}

			err = r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, badReason, "")
			if err != nil {
				t.Error(err)
			}

			notUpdatedLVG := &v1alpha1.LVMVolumeGroup{}
			err = r.cl.Get(ctx, client.ObjectKey{
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
			r := setupReconciler()
			const (
				lvgName = "test-name-2"
			)
			curTime := v1.NewTime(time.Now().Truncate(time.Second))
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

			err := r.cl.Create(ctx, lvg)
			if err != nil {
				t.Error(err)
			}

			err = r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionTrue, internal.TypeVGConfigurationApplied, "", "")
			if err != nil {
				t.Error(err)
			}

			assert.Equal(t, v1.ConditionTrue, lvg.Status.Conditions[0].Status)
			assert.Equal(t, curTime.Unix(), lvg.Status.Conditions[0].LastTransitionTime.Unix())
		})
	})

	t.Run("shouldReconcileLVGByDeleteFunc", func(t *testing.T) {
		t.Run("returns_true", func(t *testing.T) {
			r := setupReconciler()
			lvg := &v1alpha1.LVMVolumeGroup{}
			lvg.DeletionTimestamp = &v1.Time{}

			assert.True(t, r.shouldReconcileLVGByDeleteFunc(lvg))
		})

		t.Run("returns_false", func(t *testing.T) {
			r := setupReconciler()
			lvg := &v1alpha1.LVMVolumeGroup{}
			lvg.DeletionTimestamp = nil

			assert.False(t, r.shouldReconcileLVGByDeleteFunc(lvg))
		})
	})

	t.Run("shouldLVGWatcherReconcileUpdateEvent", func(t *testing.T) {
		const testNode = "test_node"

		t.Run("non_local_lvg_returns_false", func(t *testing.T) {
			r := setupReconciler()
			oldLVG := &v1alpha1.LVMVolumeGroup{}
			newLVG := &v1alpha1.LVMVolumeGroup{}
			newLVG.Spec.Local.NodeName = "other_node"
			newLVG.DeletionTimestamp = &v1.Time{}
			assert.False(t, r.shouldLVGWatcherReconcileUpdateEvent(oldLVG, newLVG))
		})

		t.Run("deletion_timestamp_not_nil_returns_true", func(t *testing.T) {
			r := setupReconciler()
			oldLVG := &v1alpha1.LVMVolumeGroup{}
			newLVG := &v1alpha1.LVMVolumeGroup{}
			newLVG.Spec.Local.NodeName = testNode
			newLVG.DeletionTimestamp = &v1.Time{}
			assert.True(t, r.shouldLVGWatcherReconcileUpdateEvent(oldLVG, newLVG))
		})

		t.Run("spec_is_diff_returns_true", func(t *testing.T) {
			r := setupReconciler()
			oldLVG := &v1alpha1.LVMVolumeGroup{}
			oldLVG.Spec.Local.NodeName = testNode
			newLVG := &v1alpha1.LVMVolumeGroup{}
			newLVG.Spec.Local.NodeName = testNode
			oldLVG.Spec.BlockDeviceSelector = &v1.LabelSelector{MatchLabels: map[string]string{"first": "second"}}
			newLVG.Spec.BlockDeviceSelector = &v1.LabelSelector{MatchLabels: map[string]string{"second": "second"}}
			assert.True(t, r.shouldLVGWatcherReconcileUpdateEvent(oldLVG, newLVG))
		})

		t.Run("condition_vg_configuration_applied_is_updating_returns_false", func(t *testing.T) {
			r := setupReconciler()
			oldLVG := &v1alpha1.LVMVolumeGroup{}
			newLVG := &v1alpha1.LVMVolumeGroup{}
			newLVG.Name = "test-name"
			newLVG.Spec.Local.NodeName = testNode
			newLVG.Labels = map[string]string{internal.LVGMetadataNameLabelKey: "test-name"}
			newLVG.Status.Conditions = []v1.Condition{
				{
					Type:   internal.TypeVGConfigurationApplied,
					Reason: internal.ReasonUpdating,
				},
			}
			assert.False(t, r.shouldLVGWatcherReconcileUpdateEvent(oldLVG, newLVG))
		})

		t.Run("condition_vg_configuration_applied_is_creating_returns_false", func(t *testing.T) {
			r := setupReconciler()
			oldLVG := &v1alpha1.LVMVolumeGroup{}
			newLVG := &v1alpha1.LVMVolumeGroup{}
			newLVG.Name = "test-name"
			newLVG.Spec.Local.NodeName = testNode
			newLVG.Status.Conditions = []v1.Condition{
				{
					Type:   internal.TypeVGConfigurationApplied,
					Reason: internal.ReasonCreating,
				},
			}
			newLVG.Labels = map[string]string{internal.LVGMetadataNameLabelKey: newLVG.Name}
			assert.False(t, r.shouldLVGWatcherReconcileUpdateEvent(oldLVG, newLVG))
		})

		t.Run("label_is_not_the_same_returns_true", func(t *testing.T) {
			r := setupReconciler()
			oldLVG := &v1alpha1.LVMVolumeGroup{}
			newLVG := &v1alpha1.LVMVolumeGroup{}
			newLVG.Name = "test-name"
			newLVG.Spec.Local.NodeName = testNode
			newLVG.Status.Conditions = []v1.Condition{
				{
					Type:   internal.TypeVGConfigurationApplied,
					Reason: internal.ReasonApplied,
				},
			}
			newLVG.Labels = map[string]string{internal.LVGMetadataNameLabelKey: "some-other-name"}
			assert.True(t, r.shouldLVGWatcherReconcileUpdateEvent(oldLVG, newLVG))
		})

		t.Run("status_nodes_changed_returns_true", func(t *testing.T) {
			r := setupReconciler()
			oldLVG := &v1alpha1.LVMVolumeGroup{}
			oldLVG.Spec.Local.NodeName = testNode
			newLVG := &v1alpha1.LVMVolumeGroup{}
			newLVG.Spec.Local.NodeName = testNode
			newLVG.Labels = map[string]string{internal.LVGMetadataNameLabelKey: newLVG.Name}
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

		t.Run("only_conditions_changed_nodes_identical_returns_false", func(t *testing.T) {
			r := setupReconciler()
			nodes := []v1alpha1.LVMVolumeGroupNode{
				{
					Devices: []v1alpha1.LVMVolumeGroupDevice{
						{
							BlockDevice: "test",
							DevSize:     resource.MustParse("10G"),
							PVSize:      resource.MustParse("10G"),
						},
					},
					Name: testNode,
				},
			}
			oldLVG := &v1alpha1.LVMVolumeGroup{}
			oldLVG.Spec.Local.NodeName = testNode
			oldLVG.Labels = map[string]string{internal.LVGMetadataNameLabelKey: "test-lvg"}
			oldLVG.Name = "test-lvg"
			oldLVG.Status.Nodes = nodes
			oldLVG.Status.Conditions = []v1.Condition{
				{
					Type:   internal.TypeVGConfigurationApplied,
					Reason: internal.ReasonApplied,
					Status: v1.ConditionTrue,
				},
			}

			newLVG := oldLVG.DeepCopy()
			newLVG.Status.Conditions = []v1.Condition{
				{
					Type:   internal.TypeVGConfigurationApplied,
					Reason: internal.ReasonApplied,
					Status: v1.ConditionFalse,
				},
			}

			assert.False(t, r.shouldLVGWatcherReconcileUpdateEvent(oldLVG, newLVG))
		})
	})

	t.Run("shouldUpdateLVGLabels", func(t *testing.T) {
		t.Run("labels_nil_returns_true", func(t *testing.T) {
			r := setupReconciler()
			lvg := &v1alpha1.LVMVolumeGroup{}
			assert.True(t, r.shouldUpdateLVGLabels(lvg, "key", "value"))
		})
		t.Run("no_such_label_returns_true", func(t *testing.T) {
			r := setupReconciler()
			lvg := &v1alpha1.LVMVolumeGroup{}
			lvg.Labels = map[string]string{"key": "value"}
			assert.True(t, r.shouldUpdateLVGLabels(lvg, "other-key", "value"))
		})
		t.Run("key_exists_other_value_returns_true", func(t *testing.T) {
			r := setupReconciler()
			const key = "key"
			lvg := &v1alpha1.LVMVolumeGroup{}
			lvg.Labels = map[string]string{key: "value"}
			assert.True(t, r.shouldUpdateLVGLabels(lvg, key, "other-value"))
		})
		t.Run("all_good_returns_false", func(t *testing.T) {
			r := setupReconciler()
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
		r := setupReconciler()
		const (
			lvgName = "test=lvg"
		)

		lvgToDelete := &v1alpha1.LVMVolumeGroup{
			ObjectMeta: v1.ObjectMeta{
				Name: lvgName,
			},
			Status: v1alpha1.LVMVolumeGroupStatus{
				Nodes: []v1alpha1.LVMVolumeGroupNode{
					{
						Name: r.cfg.NodeName,
					},
				},
			},
		}

		err := r.cl.Create(ctx, lvgToDelete)
		if err != nil {
			t.Error(err)
		}

		defer func() {
			_ = r.cl.Delete(ctx, lvgToDelete)
		}()

		lvgCheck := &v1alpha1.LVMVolumeGroup{}
		err = r.cl.Get(ctx, client.ObjectKey{Name: lvgName}, lvgCheck)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, lvgName, lvgCheck.Name)

		err = r.lvgCl.DeleteLVMVolumeGroup(ctx, lvgToDelete)
		if err != nil {
			t.Error(err)
		}

		lvgNewCheck := &v1alpha1.LVMVolumeGroup{}
		err = r.cl.Get(ctx, client.ObjectKey{Name: lvgName}, lvgNewCheck)
		if assert.True(t, errors2.IsNotFound(err)) {
			assert.Equal(t, "", lvgNewCheck.Name)
		}
	})

	t.Run("getLVMVolumeGroup_lvg_exists_returns_correct", func(t *testing.T) {
		r := setupReconciler()
		const name = "test_name"
		lvgToCreate := &v1alpha1.LVMVolumeGroup{
			ObjectMeta: v1.ObjectMeta{
				Name: name,
			},
		}

		err := r.cl.Create(ctx, lvgToCreate)
		if err != nil {
			t.Error(err)
		} else {
			defer func() {
				err = r.cl.Delete(ctx, lvgToCreate)
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

	t.Run("getLVMVolumeGroup_lvg_doesn't_exist_returns_nil", func(t *testing.T) {
		r := setupReconciler()
		const name = "test_name"
		testObj := &v1alpha1.LVMVolumeGroup{
			ObjectMeta: v1.ObjectMeta{
				Name: name,
			},
		}

		err := r.cl.Create(ctx, testObj)
		if err != nil {
			t.Error(err)
		} else {
			defer func() {
				err = r.cl.Delete(ctx, testObj)
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

	t.Run("ShouldReconcileCreate", func(t *testing.T) {
		t.Run("local_lvg_returns_true", func(t *testing.T) {
			r := setupReconciler()
			lvg := &v1alpha1.LVMVolumeGroup{}
			lvg.Spec.Local.NodeName = "test_node"
			assert.True(t, r.ShouldReconcileCreate(lvg))
		})

		t.Run("non_local_lvg_returns_false", func(t *testing.T) {
			r := setupReconciler()
			lvg := &v1alpha1.LVMVolumeGroup{}
			lvg.Spec.Local.NodeName = "other_node"
			assert.False(t, r.ShouldReconcileCreate(lvg))
		})
	})

	t.Run("updateLVGConditionIfNeeded_early_return_propagates_resource_version", func(t *testing.T) {
		r := setupReconciler()
		const lvgName = "test-rv-propagation"
		lvg := &v1alpha1.LVMVolumeGroup{}
		lvg.Name = lvgName
		lvg.Generation = 1
		lvg.Status.Conditions = []v1.Condition{
			{
				Type:               internal.TypeVGConfigurationApplied,
				Status:             v1.ConditionTrue,
				ObservedGeneration: 1,
				LastTransitionTime: v1.NewTime(time.Now().Truncate(time.Second)),
				Reason:             "",
				Message:            "",
			},
		}

		err := r.cl.Create(ctx, lvg)
		if err != nil {
			t.Error(err)
			return
		}

		defer func() {
			_ = r.cl.Delete(ctx, lvg)
		}()

		serverLVG := &v1alpha1.LVMVolumeGroup{}
		err = r.cl.Get(ctx, client.ObjectKey{Name: lvgName}, serverLVG)
		if err != nil {
			t.Error(err)
			return
		}
		serverRV := serverLVG.ResourceVersion

		lvg.ResourceVersion = ""

		err = r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionTrue, internal.TypeVGConfigurationApplied, "", "")
		if assert.NoError(t, err) {
			assert.Equal(t, serverRV, lvg.ResourceVersion)
		}
	})
}

func setupReconciler() *Reconciler {
	return NewReconciler(
		test_utils.NewFakeClient(&v1alpha1.LVMVolumeGroup{}, &v1alpha1.LVMLogicalVolume{}),
		logger.Logger{},
		monitoring.GetMetrics(""),
		cache.New(),
		utils.NewCommands(),
		ReconcilerConfig{NodeName: "test_node", CmdDeadlineDuration: 30 * time.Second})
}

// countVGSizeByFileDevices must sum spec.fileDevices capacity so the
// create-path thin-pool sizing does not treat a file-only VG as zero-sized
// (which would collapse percentage pools to 0 and force absolute pools into
// the full-VG-space branch).
func TestCountVGSizeByFileDevices(t *testing.T) {
	t.Run("no_file_devices", func(t *testing.T) {
		lvg := &v1alpha1.LVMVolumeGroup{}
		got := countVGSizeByFileDevices(lvg)
		assert.Equal(t, int64(0), got.Value())
	})

	t.Run("sums_all_entries", func(t *testing.T) {
		lvg := &v1alpha1.LVMVolumeGroup{
			Spec: v1alpha1.LVMVolumeGroupSpec{
				FileDevices: []v1alpha1.LVMVolumeGroupFileDeviceSpec{
					{Directory: "/data", Size: resource.MustParse("10Gi")},
					{Directory: "/data", Size: resource.MustParse("20Gi")},
				},
			},
		}
		want := resource.MustParse("30Gi")
		got := countVGSizeByFileDevices(lvg)
		assert.Equal(t, want.Value(), got.Value())
	})
}

func TestValidateFileDevice(t *testing.T) {
	ctx := context.Background()

	t.Run("valid_file_device", func(t *testing.T) {
		// Validation is purely structural now: the backing directory is
		// created on demand at provision time, so no host command is invoked.
		r := setupReconciler()

		var reason strings.Builder
		totalSize := resource.MustParse("0")
		fd := v1alpha1.LVMVolumeGroupFileDeviceSpec{
			Directory: "/data",
			Size:      resource.MustParse("10Gi"),
		}
		r.validateFileDevice(ctx, fd, 0, &reason, &totalSize)
		assert.Empty(t, reason.String())
		want := resource.MustParse("10Gi")
		assert.Equal(t, want.Value(), totalSize.Value())
	})

	t.Run("empty_directory", func(t *testing.T) {
		r := setupReconciler()
		var reason strings.Builder
		totalSize := resource.MustParse("0")
		fd := v1alpha1.LVMVolumeGroupFileDeviceSpec{
			Directory: "",
			Size:      resource.MustParse("10Gi"),
		}
		r.validateFileDevice(ctx, fd, 0, &reason, &totalSize)
		assert.Contains(t, reason.String(), "directory is empty")
	})

	t.Run("size_too_small", func(t *testing.T) {
		r := setupReconciler()
		var reason strings.Builder
		totalSize := resource.MustParse("0")
		fd := v1alpha1.LVMVolumeGroupFileDeviceSpec{
			Directory: "/data",
			Size:      resource.MustParse("500Mi"),
		}
		r.validateFileDevice(ctx, fd, 0, &reason, &totalSize)
		assert.Contains(t, reason.String(), "less than the minimum")
	})

	t.Run("size_adds_to_total_vg_size", func(t *testing.T) {
		r := setupReconciler()

		var reason strings.Builder
		totalSize := resource.MustParse("5Gi")
		fd := v1alpha1.LVMVolumeGroupFileDeviceSpec{
			Directory: "/data",
			Size:      resource.MustParse("10Gi"),
		}
		r.validateFileDevice(ctx, fd, 0, &reason, &totalSize)
		assert.Empty(t, reason.String())
		expected := resource.MustParse("5Gi")
		add := resource.MustParse("10Gi")
		expected.Add(add)
		assert.Equal(t, expected.Value(), totalSize.Value())
	})

	t.Run("rejects_relative_directory", func(t *testing.T) {
		r := setupReconciler()
		var reason strings.Builder
		totalSize := resource.MustParse("0")
		fd := v1alpha1.LVMVolumeGroupFileDeviceSpec{
			Directory: "data/lvm",
			Size:      resource.MustParse("10Gi"),
		}
		r.validateFileDevice(ctx, fd, 0, &reason, &totalSize)
		assert.Contains(t, reason.String(), "must be an absolute path")
	})

	t.Run("rejects_dot_dot_segment", func(t *testing.T) {
		r := setupReconciler()
		var reason strings.Builder
		totalSize := resource.MustParse("0")
		fd := v1alpha1.LVMVolumeGroupFileDeviceSpec{
			Directory: "/data/../etc",
			Size:      resource.MustParse("10Gi"),
		}
		r.validateFileDevice(ctx, fd, 0, &reason, &totalSize)
		assert.Contains(t, reason.String(), "must not contain '..'")
	})

	t.Run("rejects_directory_outside_base", func(t *testing.T) {
		r := setupReconciler()
		r.cfg.FileDevicesDirectory = "/opt/deckhouse/sds/file-devices"
		var reason strings.Builder
		totalSize := resource.MustParse("0")
		fd := v1alpha1.LVMVolumeGroupFileDeviceSpec{
			Directory: "/var/lib/kubelet",
			Size:      resource.MustParse("10Gi"),
		}
		r.validateFileDevice(ctx, fd, 0, &reason, &totalSize)
		assert.Contains(t, reason.String(), "must be \"/opt/deckhouse/sds/file-devices\" or a subdirectory")
	})

	t.Run("rejects_base_prefix_sibling", func(t *testing.T) {
		// A path that shares the base as a string prefix but is not actually
		// inside it (e.g. "/opt/deckhouse/sds/file-devices-evil") must be
		// rejected — the separator check guards against this.
		r := setupReconciler()
		r.cfg.FileDevicesDirectory = "/opt/deckhouse/sds/file-devices"
		var reason strings.Builder
		totalSize := resource.MustParse("0")
		fd := v1alpha1.LVMVolumeGroupFileDeviceSpec{
			Directory: "/opt/deckhouse/sds/file-devices-evil",
			Size:      resource.MustParse("10Gi"),
		}
		r.validateFileDevice(ctx, fd, 0, &reason, &totalSize)
		assert.Contains(t, reason.String(), "or a subdirectory of it")
	})

	t.Run("accepts_base_directory_itself", func(t *testing.T) {
		r := setupReconciler()
		r.cfg.FileDevicesDirectory = "/opt/deckhouse/sds/file-devices"
		var reason strings.Builder
		totalSize := resource.MustParse("0")
		fd := v1alpha1.LVMVolumeGroupFileDeviceSpec{
			Directory: "/opt/deckhouse/sds/file-devices",
			Size:      resource.MustParse("10Gi"),
		}
		r.validateFileDevice(ctx, fd, 0, &reason, &totalSize)
		assert.Empty(t, reason.String())
	})

	t.Run("accepts_subdirectory_of_base", func(t *testing.T) {
		r := setupReconciler()
		r.cfg.FileDevicesDirectory = "/opt/deckhouse/sds/file-devices"
		var reason strings.Builder
		totalSize := resource.MustParse("0")
		fd := v1alpha1.LVMVolumeGroupFileDeviceSpec{
			Directory: "/opt/deckhouse/sds/file-devices/vg-a",
			Size:      resource.MustParse("10Gi"),
		}
		r.validateFileDevice(ctx, fd, 0, &reason, &totalSize)
		assert.Empty(t, reason.String())
	})

	t.Run("rejects_duplicate_backing_file", func(t *testing.T) {
		r := setupReconciler()
		lvg := &v1alpha1.LVMVolumeGroup{
			ObjectMeta: v1.ObjectMeta{Name: "vg-a"},
			Spec: v1alpha1.LVMVolumeGroupSpec{
				FileDevices: []v1alpha1.LVMVolumeGroupFileDeviceSpec{
					{Directory: "/data", Size: resource.MustParse("10Gi")},
					{Directory: "/data", Size: resource.MustParse("10Gi")},
				},
			},
		}
		var reason strings.Builder
		r.validateFileDevices(ctx, lvg, &reason, nil)
		assert.Contains(t, reason.String(), "collides with fileDevices")
	})
}

// reconcilerWithMockedCommands wires a Reconciler around a gomock
// Commands so we can exercise provisionFileDevices / cleanupFileDevices
// against deterministic expectations.
func reconcilerWithMockedCommands(t *testing.T, mc *mock_utils.MockCommands) *Reconciler {
	t.Helper()
	return NewReconciler(
		test_utils.NewFakeClient(&v1alpha1.LVMVolumeGroup{}, &v1alpha1.LVMLogicalVolume{}),
		logger.Logger{},
		monitoring.GetMetrics(""),
		cache.New(),
		mc,
		ReconcilerConfig{NodeName: "test_node", CmdDeadlineDuration: 30 * time.Second},
	)
}

// provisionFileDevices is idempotent: when a backing file is already
// attached to a loop device, the existing loop must be reused.
// `losetup --find --show` would otherwise pick a fresh minor on every
// reconcile retry and slowly leak loops up to the system-wide limit.
func TestProvisionFileDevices_ReusesExistingLoop(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)
	ctx := context.Background()

	lvg := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: v1.ObjectMeta{Name: "vg-a"},
		Spec: v1alpha1.LVMVolumeGroupSpec{
			FileDevices: []v1alpha1.LVMVolumeGroupFileDeviceSpec{
				{Directory: "/data", Size: resource.MustParse("10Gi")},
			},
		},
	}
	want := utils.BuildFileDevicePath("/data", "vg-a", resource.MustParse("10Gi"))

	mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), want).Return("losetup -j ...", "/dev/loop7", nil)
	// CreateFileDevice / SetupLoopDevice MUST NOT be invoked.

	got, _, err := r.provisionFileDevices(ctx, lvg)
	assert.NoError(t, err)
	assert.Equal(t, []string{"/dev/loop7"}, got)
}

// When the second file's losetup fails, the first file's freshly
// created loop and backing file must be rolled back so a retry sees a
// clean state instead of a half-attached carcass.
func TestProvisionFileDevices_RollsBackOnPartialFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)
	ctx := context.Background()

	fdA := v1alpha1.LVMVolumeGroupFileDeviceSpec{Directory: "/data", Size: resource.MustParse("10Gi")}
	fdB := v1alpha1.LVMVolumeGroupFileDeviceSpec{Directory: "/data", Size: resource.MustParse("20Gi")}
	lvg := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: v1.ObjectMeta{Name: "vg-a"},
		Spec:       v1alpha1.LVMVolumeGroupSpec{FileDevices: []v1alpha1.LVMVolumeGroupFileDeviceSpec{fdA, fdB}},
	}
	pathA := utils.BuildFileDevicePath(fdA.Directory, "vg-a", fdA.Size)
	pathB := utils.BuildFileDevicePath(fdB.Directory, "vg-a", fdB.Size)

	sizeA := fdA.Size.Value()
	sizeB := fdB.Size.Value()
	gomock.InOrder(
		mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), pathA).Return("losetup -j", "", nil),
		mc.EXPECT().EnsureFileDeviceDirectory(gomock.Any(), fdA.Directory).Return("mkdir -p ...", nil),
		mc.EXPECT().GetAvailableBytes(gomock.Any(), fdA.Directory).Return("stat -f ...", int64(1)<<50, nil),
		mc.EXPECT().CreateFileDevice(gomock.Any(), pathA, sizeA).Return("fallocate ...", nil),
		mc.EXPECT().SetupLoopDevice(gomock.Any(), pathA).Return("losetup --find ...", "/dev/loop0", nil),
		mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), pathB).Return("losetup -j", "", nil),
		mc.EXPECT().EnsureFileDeviceDirectory(gomock.Any(), fdB.Directory).Return("mkdir -p ...", nil),
		mc.EXPECT().GetAvailableBytes(gomock.Any(), fdB.Directory).Return("stat -f ...", int64(1)<<50, nil),
		mc.EXPECT().CreateFileDevice(gomock.Any(), pathB, sizeB).Return("fallocate ...", nil),
		mc.EXPECT().SetupLoopDevice(gomock.Any(), pathB).Return("losetup --find ...", "", errors.New("ENOSPC")),
		// rollback in LIFO order: first pathB (file only, loop never attached), then pathA (loop + file).
		mc.EXPECT().RemoveFileDevice(gomock.Any(), pathB).Return("rm ...", nil),
		mc.EXPECT().DetachLoopDevice(gomock.Any(), "/dev/loop0").Return("losetup -d ...", nil),
		mc.EXPECT().RemoveFileDevice(gomock.Any(), pathA).Return("rm ...", nil),
	)

	_, _, err := r.provisionFileDevices(ctx, lvg)
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "ENOSPC")
	}
}

// A backing file larger than the filesystem's free space must be refused
// before fallocate runs, so a typo in directory/size cannot fill the node's
// root filesystem and trip kubelet DiskPressure eviction. The directory is
// still created (mkdir -p), but no file is allocated and no loop is attached.
func TestProvisionFileDevices_RefusesWhenNotEnoughFreeSpace(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)
	ctx := context.Background()

	fd := v1alpha1.LVMVolumeGroupFileDeviceSpec{Directory: "/data", Size: resource.MustParse("100Gi")}
	lvg := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: v1.ObjectMeta{Name: "vg-a"},
		Spec:       v1alpha1.LVMVolumeGroupSpec{FileDevices: []v1alpha1.LVMVolumeGroupFileDeviceSpec{fd}},
	}
	path := utils.BuildFileDevicePath(fd.Directory, "vg-a", fd.Size)

	gomock.InOrder(
		mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), path).Return("losetup -j", "", nil),
		mc.EXPECT().EnsureFileDeviceDirectory(gomock.Any(), fd.Directory).Return("mkdir -p ...", nil),
		// Only 1Gi free for a 100Gi request → reject before fallocate.
		mc.EXPECT().GetAvailableBytes(gomock.Any(), fd.Directory).Return("stat -f ...", int64(1)<<30, nil),
	)
	// CreateFileDevice / SetupLoopDevice MUST NOT be invoked.

	_, _, err := r.provisionFileDevices(ctx, lvg)
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "not enough free space")
	}
}

// When free space cannot be determined the provision proceeds best-effort:
// fallocate still fails cleanly on a genuine ENOSPC, so a transient stat
// failure must not block provisioning by itself.
func TestProvisionFileDevices_ProceedsWhenFreeSpaceUnknown(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)
	ctx := context.Background()

	fd := v1alpha1.LVMVolumeGroupFileDeviceSpec{Directory: "/data", Size: resource.MustParse("10Gi")}
	lvg := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: v1.ObjectMeta{Name: "vg-a"},
		Spec:       v1alpha1.LVMVolumeGroupSpec{FileDevices: []v1alpha1.LVMVolumeGroupFileDeviceSpec{fd}},
	}
	path := utils.BuildFileDevicePath(fd.Directory, "vg-a", fd.Size)

	gomock.InOrder(
		mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), path).Return("losetup -j", "", nil),
		mc.EXPECT().EnsureFileDeviceDirectory(gomock.Any(), fd.Directory).Return("mkdir -p ...", nil),
		mc.EXPECT().GetAvailableBytes(gomock.Any(), fd.Directory).Return("stat -f ...", int64(0), errors.New("stat: command not found")),
		mc.EXPECT().CreateFileDevice(gomock.Any(), path, fd.Size.Value()).Return("fallocate ...", nil),
		mc.EXPECT().SetupLoopDevice(gomock.Any(), path).Return("losetup --find ...", "/dev/loop0", nil),
	)

	loops, _, err := r.provisionFileDevices(ctx, lvg)
	assert.NoError(t, err)
	assert.Equal(t, []string{"/dev/loop0"}, loops)
}

// cleanupFileDevices must walk the union of spec.fileDevices and
// status.nodes[].fileDevices so files that were created mid-provision
// (and therefore never landed in status) are not leaked when the
// resource is deleted before the discoverer ran.
func TestCleanupFileDevices_UnionOfSpecAndStatus(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)
	ctx := context.Background()

	fdSpec := v1alpha1.LVMVolumeGroupFileDeviceSpec{Directory: "/data", Size: resource.MustParse("10Gi")}
	pathSpec := utils.BuildFileDevicePath(fdSpec.Directory, "vg-a", fdSpec.Size)
	// status has a different (older) entry — still managed-named.
	fdStatusPath := utils.BuildFileDevicePath("/data", "vg-a", resource.MustParse("20Gi"))

	lvg := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: v1.ObjectMeta{Name: "vg-a"},
		Spec:       v1alpha1.LVMVolumeGroupSpec{FileDevices: []v1alpha1.LVMVolumeGroupFileDeviceSpec{fdSpec}},
		Status: v1alpha1.LVMVolumeGroupStatus{
			Nodes: []v1alpha1.LVMVolumeGroupNode{{
				Name: "n",
				FileDevices: []v1alpha1.LVMVolumeGroupFileDevice{
					{FilePath: fdStatusPath, LoopDevice: "/dev/loop9"},
				},
			}},
		},
	}

	// Both targets carry a backing file, so cleanup always re-resolves the
	// loop from the file (the status-recorded minor can be stale after a
	// reboot) before detaching.
	mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), fdStatusPath).Return("losetup -j ...", "/dev/loop9", nil)
	mc.EXPECT().DetachLoopDevice(gomock.Any(), "/dev/loop9").Return("losetup -d ...", nil)
	mc.EXPECT().RemoveFileDevice(gomock.Any(), fdStatusPath).Return("rm ...", nil)
	// The spec-derived entry resolves via losetup -j too; here nothing is attached.
	mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), pathSpec).Return("losetup -j ...", "", nil)
	mc.EXPECT().RemoveFileDevice(gomock.Any(), pathSpec).Return("rm ...", nil)

	assert.NoError(t, r.cleanupFileDevices(ctx, lvg))
}

// A busy loop device must surface as an error so the finalizer stays
// in place and the operator can retry — silently leaving a busy loop
// stranded on the node when the LVG object is gone is the worst case.
func TestCleanupFileDevices_DetachErrorBlocksFinalizer(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)
	ctx := context.Background()

	pathStatus := utils.BuildFileDevicePath("/data", "vg-a", resource.MustParse("20Gi"))
	lvg := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: v1.ObjectMeta{Name: "vg-a"},
		Status: v1alpha1.LVMVolumeGroupStatus{
			Nodes: []v1alpha1.LVMVolumeGroupNode{{
				Name: "n",
				FileDevices: []v1alpha1.LVMVolumeGroupFileDevice{
					{FilePath: pathStatus, LoopDevice: "/dev/loop9"},
				},
			}},
		},
	}

	gomock.InOrder(
		mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), pathStatus).Return("losetup -j ...", "/dev/loop9", nil),
		mc.EXPECT().DetachLoopDevice(gomock.Any(), "/dev/loop9").Return("losetup -d ...", errors.New("EBUSY")),
	)
	// RemoveFileDevice MUST NOT be called when detach failed: we don't
	// want to rm the backing file while the kernel still has it open.

	err := r.cleanupFileDevices(ctx, lvg)
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "EBUSY")
	}
}

// A foreign-looking path that somehow slipped into status (bug, manual
// edit, …) must be skipped, not rm-ed, even if the LVG is being
// deleted. This is the hard owner check that protects unrelated host
// files from being clobbered.
func TestCleanupFileDevices_RefusesUnmanagedPath(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)
	ctx := context.Background()

	lvg := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: v1.ObjectMeta{Name: "vg-a"},
		Status: v1alpha1.LVMVolumeGroupStatus{
			Nodes: []v1alpha1.LVMVolumeGroupNode{{
				Name: "n",
				FileDevices: []v1alpha1.LVMVolumeGroupFileDevice{
					{FilePath: "/var/lib/libvirt/images/disk0.qcow2", LoopDevice: "/dev/loop3"},
				},
			}},
		},
	}
	// No EXPECTations on DetachLoopDevice / RemoveFileDevice: the
	// cleanup must refuse to touch this path.

	assert.NoError(t, r.cleanupFileDevices(ctx, lvg))
}

// When a backing file is known only from spec (status never recorded a
// loop device, e.g. a crash mid-provision) cleanup must resolve the loop
// via losetup -j and detach it BEFORE removing the file, otherwise the
// loop device is stranded on the node bound to a deleted file.
func TestCleanupFileDevices_SpecDerivedDetachesDiscoveredLoop(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)
	ctx := context.Background()

	fdSpec := v1alpha1.LVMVolumeGroupFileDeviceSpec{Directory: "/data", Size: resource.MustParse("10Gi")}
	pathSpec := utils.BuildFileDevicePath(fdSpec.Directory, "vg-a", fdSpec.Size)
	lvg := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: v1.ObjectMeta{Name: "vg-a"},
		Spec:       v1alpha1.LVMVolumeGroupSpec{FileDevices: []v1alpha1.LVMVolumeGroupFileDeviceSpec{fdSpec}},
	}

	gomock.InOrder(
		mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), pathSpec).Return("losetup -j ...", "/dev/loop5", nil),
		mc.EXPECT().DetachLoopDevice(gomock.Any(), "/dev/loop5").Return("losetup -d ...", nil),
		mc.EXPECT().RemoveFileDevice(gomock.Any(), pathSpec).Return("rm ...", nil),
	)

	assert.NoError(t, r.cleanupFileDevices(ctx, lvg))
}

// The loop minor recorded in status can go stale: after a reboot the file
// is re-attached via `losetup --find` to a DIFFERENT minor, and the old
// minor may have been handed to an unrelated device. cleanup must detach
// the minor that currently backs the file (resolved from the backing
// file), never the stale one from status — otherwise it strands our loop
// and may detach a foreign device.
func TestCleanupFileDevices_ReResolvesStaleStatusLoop(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)
	ctx := context.Background()

	path := utils.BuildFileDevicePath("/data", "vg-a", resource.MustParse("10Gi"))
	lvg := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: v1.ObjectMeta{Name: "vg-a"},
		Status: v1alpha1.LVMVolumeGroupStatus{
			Nodes: []v1alpha1.LVMVolumeGroupNode{{
				Name: "n",
				FileDevices: []v1alpha1.LVMVolumeGroupFileDevice{
					// status says /dev/loop0, but the file is really on /dev/loop3 now.
					{FilePath: path, LoopDevice: "/dev/loop0"},
				},
			}},
		},
	}

	gomock.InOrder(
		mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), path).Return("losetup -j ...", "/dev/loop3", nil),
		mc.EXPECT().DetachLoopDevice(gomock.Any(), "/dev/loop3").Return("losetup -d ...", nil),
		mc.EXPECT().RemoveFileDevice(gomock.Any(), path).Return("rm ...", nil),
	)
	// DetachLoopDevice(/dev/loop0) MUST NOT be called — that is the stale minor.

	assert.NoError(t, r.cleanupFileDevices(ctx, lvg))
}

// extendFileDevicesIfNeeded is the update-path entry point that makes
// "add a new fileDevices entry to grow the VG" actually work: it
// provisions the new backing file, attaches it, and vgextends with the
// resulting loop device (which is not yet a PV of the VG).
func TestExtendFileDevicesIfNeeded_AddsNewLoopAsPV(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)
	ctx := context.Background()

	fd := v1alpha1.LVMVolumeGroupFileDeviceSpec{Directory: "/data", Size: resource.MustParse("10Gi")}
	lvg := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: v1.ObjectMeta{Name: "vg-a"},
		Spec:       v1alpha1.LVMVolumeGroupSpec{FileDevices: []v1alpha1.LVMVolumeGroupFileDeviceSpec{fd}},
	}
	path := utils.BuildFileDevicePath(fd.Directory, "vg-a", fd.Size)
	vg := internal.VGData{VGName: "vg-test"}

	gomock.InOrder(
		mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), path).Return("losetup -j", "", nil),
		mc.EXPECT().EnsureFileDeviceDirectory(gomock.Any(), fd.Directory).Return("mkdir -p ...", nil),
		mc.EXPECT().GetAvailableBytes(gomock.Any(), fd.Directory).Return("stat -f ...", int64(1)<<50, nil),
		mc.EXPECT().CreateFileDevice(gomock.Any(), path, fd.Size.Value()).Return("fallocate ...", nil),
		mc.EXPECT().SetupLoopDevice(gomock.Any(), path).Return("losetup --find ...", "/dev/loop4", nil),
		mc.EXPECT().CreatePV("/dev/loop4").Return("pvcreate ...", nil),
		mc.EXPECT().ExtendVG("vg-test", []string{"/dev/loop4"}).Return("vgextend ...", nil),
		mc.EXPECT().UdevadmTrigger(gomock.Any(), []string{"/dev/loop4"}).Return("udevadm ...", nil),
	)

	assert.NoError(t, r.extendFileDevicesIfNeeded(ctx, lvg, vg, nil))
}

// A loop device that is already a PV of the VG must not be re-added.
func TestExtendFileDevicesIfNeeded_SkipsExistingPV(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)
	ctx := context.Background()

	fd := v1alpha1.LVMVolumeGroupFileDeviceSpec{Directory: "/data", Size: resource.MustParse("10Gi")}
	lvg := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: v1.ObjectMeta{Name: "vg-a"},
		Spec:       v1alpha1.LVMVolumeGroupSpec{FileDevices: []v1alpha1.LVMVolumeGroupFileDeviceSpec{fd}},
	}
	path := utils.BuildFileDevicePath(fd.Directory, "vg-a", fd.Size)
	vg := internal.VGData{VGName: "vg-test"}

	// Already attached and already a PV — no CreatePV/ExtendVG expected.
	mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), path).Return("losetup -j", "/dev/loop4", nil)

	pvs := []internal.PVData{{PVName: "/dev/loop4"}}
	assert.NoError(t, r.extendFileDevicesIfNeeded(ctx, lvg, vg, pvs))
}

// extendFileDevicesIfNeeded must consult a FRESH cache read, not only the
// caller's PV snapshot. A loop can become a canonical /dev/loopN PV after the
// snapshot was captured at the top of reconcileLVGUpdateFunc (e.g. a prior
// reconcile pvcreated it but failed before the VG was assembled, or the
// create-rollback kept it). Here the caller's snapshot is empty (nil) but the
// cache reports /dev/loop4 as a PV, so no CreatePV/ExtendVG must be issued —
// otherwise pvcreate would fail "already a PV" and wedge the condition.
func TestExtendFileDevicesIfNeeded_SkipsPVKnownOnlyToCache(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)
	ctx := context.Background()

	fd := v1alpha1.LVMVolumeGroupFileDeviceSpec{Directory: "/data", Size: resource.MustParse("10Gi")}
	lvg := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: v1.ObjectMeta{Name: "vg-a"},
		Spec:       v1alpha1.LVMVolumeGroupSpec{FileDevices: []v1alpha1.LVMVolumeGroupFileDeviceSpec{fd}},
	}
	path := utils.BuildFileDevicePath(fd.Directory, "vg-a", fd.Size)
	vg := internal.VGData{VGName: "vg-test"}

	mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), path).Return("losetup -j", "/dev/loop4", nil)
	// Cache knows /dev/loop4 is a PV; the caller's snapshot (nil) does not.
	r.sdsCache.StorePVs([]internal.PVData{{PVName: "/dev/loop4"}}, bytes.Buffer{})

	assert.NoError(t, r.extendFileDevicesIfNeeded(ctx, lvg, vg, nil))
}

// lvm.static has no udev integration, so an already-attached managed loop
// PV is frequently reported under a /dev/disk/by-id (or /dev/block/MAJ:MIN)
// alias rather than /dev/loopN. provisionFileDevices always returns the
// canonical /dev/loopN, so a literal name match would miss it and wrongly
// re-pvcreate the device. extendFileDevicesIfNeeded must resolve the alias
// and skip the loop instead.
func TestExtendFileDevicesIfNeeded_SkipsExistingPVUnderAlias(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)
	// Resolve the alias PV name to the canonical loop device.
	r.resolver = func(_ context.Context, path string) (string, error) {
		if path == "/dev/disk/by-id/lvm-pv-uuid-xyz" {
			return "/dev/loop4", nil
		}
		return path, nil
	}
	ctx := context.Background()

	fd := v1alpha1.LVMVolumeGroupFileDeviceSpec{Directory: "/data", Size: resource.MustParse("10Gi")}
	lvg := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: v1.ObjectMeta{Name: "vg-a"},
		Spec:       v1alpha1.LVMVolumeGroupSpec{FileDevices: []v1alpha1.LVMVolumeGroupFileDeviceSpec{fd}},
	}
	path := utils.BuildFileDevicePath(fd.Directory, "vg-a", fd.Size)
	vg := internal.VGData{VGName: "vg-test"}

	// Loop is already attached; the cache reports it only under an alias.
	// No CreatePV/ExtendVG must be issued.
	mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), path).Return("losetup -j", "/dev/loop4", nil)

	pvs := []internal.PVData{{PVName: "/dev/disk/by-id/lvm-pv-uuid-xyz"}}
	assert.NoError(t, r.extendFileDevicesIfNeeded(ctx, lvg, vg, pvs))
}

// When the canonical loop cannot be resolved from an alias PV (transient
// readlink/nsenter failure), extendFileDevicesIfNeeded must NOT hand the loop
// to pvcreate: the alias might already be this loop registered as a PV, and a
// duplicate pvcreate would wedge the VGConfigurationApplied condition. The
// loop is skipped this round, but because the skip was forced by a resolver
// failure (not a confirmed already-a-PV), the function returns an error so the
// reconcile requeues instead of silently reporting the configuration applied
// while the new file device never joined the VG.
func TestExtendFileDevicesIfNeeded_SkipsOnResolverFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)
	// The resolver fails for the alias PV, so the loop's PV membership is
	// undeterminable this round.
	r.resolver = func(_ context.Context, _ string) (string, error) {
		return "", errors.New("nsenter readlink timed out")
	}
	ctx := context.Background()

	fd := v1alpha1.LVMVolumeGroupFileDeviceSpec{Directory: "/data", Size: resource.MustParse("10Gi")}
	lvg := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: v1.ObjectMeta{Name: "vg-a"},
		Spec:       v1alpha1.LVMVolumeGroupSpec{FileDevices: []v1alpha1.LVMVolumeGroupFileDeviceSpec{fd}},
	}
	path := utils.BuildFileDevicePath(fd.Directory, "vg-a", fd.Size)
	vg := internal.VGData{VGName: "vg-test"}

	// Loop is already attached. With the resolver failing, CreatePV/ExtendVG
	// MUST NOT be issued (no duplicate pvcreate wedge).
	mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), path).Return("losetup -j", "/dev/loop4", nil)

	pvs := []internal.PVData{{PVName: "/dev/disk/by-id/lvm-pv-uuid-xyz"}}
	assert.Error(t, r.extendFileDevicesIfNeeded(ctx, lvg, vg, pvs))
}

// A resolver that stays broken must not requeue silently forever: after a few
// consecutive no-progress rounds the failure streak escalates so the condition
// switches to ReasonAliasResolutionFailed and the error reports the attempt
// count. This is the observable "resolver broken" signal from review #6.
func TestExtendFileDevicesIfNeeded_EscalatesAfterRepeatedResolverFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)
	r.resolver = func(_ context.Context, _ string) (string, error) {
		return "", errors.New("nsenter readlink timed out")
	}
	ctx := context.Background()

	fd := v1alpha1.LVMVolumeGroupFileDeviceSpec{Directory: "/data", Size: resource.MustParse("10Gi")}
	lvg := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: v1.ObjectMeta{Name: "vg-a"},
		Spec:       v1alpha1.LVMVolumeGroupSpec{FileDevices: []v1alpha1.LVMVolumeGroupFileDeviceSpec{fd}},
	}
	path := utils.BuildFileDevicePath(fd.Directory, "vg-a", fd.Size)
	vg := internal.VGData{VGName: "vg-test"}
	pvs := []internal.PVData{{PVName: "/dev/disk/by-id/lvm-pv-uuid-xyz"}}

	mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), path).Return("losetup -j", "/dev/loop4", nil).AnyTimes()

	for attempt := 1; attempt <= aliasResolveFailureEscalationThreshold; attempt++ {
		err := r.extendFileDevicesIfNeeded(ctx, lvg, vg, pvs)
		if assert.Error(t, err) {
			assert.Contains(t, err.Error(), fmt.Sprintf("attempt %d", attempt))
		}
		assert.Equal(t, attempt, r.aliasResolveFailures[lvg.Name])
	}
	// At the threshold the streak reached the escalation point.
	assert.GreaterOrEqual(t, r.aliasResolveFailures[lvg.Name], aliasResolveFailureEscalationThreshold)
}

// A transient resolver blip must not accumulate toward escalation: once a
// round makes progress (or has nothing to do), the streak resets so the next
// failure starts counting from one again.
func TestExtendFileDevicesIfNeeded_ResolverFailureStreakResetsOnProgress(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)
	ctx := context.Background()

	fd := v1alpha1.LVMVolumeGroupFileDeviceSpec{Directory: "/data", Size: resource.MustParse("10Gi")}
	lvg := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: v1.ObjectMeta{Name: "vg-a"},
		Spec:       v1alpha1.LVMVolumeGroupSpec{FileDevices: []v1alpha1.LVMVolumeGroupFileDeviceSpec{fd}},
	}
	path := utils.BuildFileDevicePath(fd.Directory, "vg-a", fd.Size)
	vg := internal.VGData{VGName: "vg-test"}
	pvs := []internal.PVData{{PVName: "/dev/disk/by-id/lvm-pv-uuid-xyz"}}

	mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), path).Return("losetup -j", "/dev/loop4", nil).AnyTimes()

	// Two failing rounds build a streak of 2.
	r.resolver = func(_ context.Context, _ string) (string, error) {
		return "", errors.New("nsenter readlink timed out")
	}
	assert.Error(t, r.extendFileDevicesIfNeeded(ctx, lvg, vg, pvs))
	assert.Error(t, r.extendFileDevicesIfNeeded(ctx, lvg, vg, pvs))
	assert.Equal(t, 2, r.aliasResolveFailures[lvg.Name])

	// A round that makes no-progress-but-not-due-to-resolver (the loop is a
	// known canonical PV in the cache) clears the streak.
	r.sdsCache.StorePVs([]internal.PVData{{PVName: "/dev/loop4"}}, bytes.Buffer{})
	assert.NoError(t, r.extendFileDevicesIfNeeded(ctx, lvg, vg, nil))
	assert.Equal(t, 0, r.aliasResolveFailures[lvg.Name])
}

// When the extend step (pvcreate/vgextend) fails after a new file device was
// freshly provisioned, the loop device and backing file created in THIS call
// must be rolled back so a failed extend does not leak them on the node (and
// orphan them entirely if the admin then removes the failing spec entry).
func TestExtendFileDevicesIfNeeded_RollsBackProvisionedOnExtendFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)
	ctx := context.Background()

	fd := v1alpha1.LVMVolumeGroupFileDeviceSpec{Directory: "/data", Size: resource.MustParse("10Gi")}
	lvg := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: v1.ObjectMeta{Name: "vg-a"},
		Spec:       v1alpha1.LVMVolumeGroupSpec{FileDevices: []v1alpha1.LVMVolumeGroupFileDeviceSpec{fd}},
	}
	path := utils.BuildFileDevicePath(fd.Directory, "vg-a", fd.Size)
	vg := internal.VGData{VGName: "vg-test"}

	gomock.InOrder(
		mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), path).Return("losetup -j", "", nil),
		mc.EXPECT().EnsureFileDeviceDirectory(gomock.Any(), fd.Directory).Return("mkdir -p ...", nil),
		mc.EXPECT().GetAvailableBytes(gomock.Any(), fd.Directory).Return("stat -f ...", int64(1)<<50, nil),
		mc.EXPECT().CreateFileDevice(gomock.Any(), path, fd.Size.Value()).Return("fallocate ...", nil),
		mc.EXPECT().SetupLoopDevice(gomock.Any(), path).Return("losetup --find ...", "/dev/loop4", nil),
		// Extend fails on pvcreate...
		mc.EXPECT().CreatePV("/dev/loop4").Return("pvcreate ...", errors.New("pvcreate refused")),
		// ...rollback first confirms the loop is not already a PV (it is not)...
		mc.EXPECT().GetAllPVs(gomock.Any()).Return(nil, "pvs", bytes.Buffer{}, nil),
		// ...then tears down the just-provisioned loop + file.
		mc.EXPECT().DetachLoopDevice(gomock.Any(), "/dev/loop4").Return("losetup -d ...", nil),
		mc.EXPECT().RemoveFileDevice(gomock.Any(), path).Return("rm ...", nil),
	)

	assert.Error(t, r.extendFileDevicesIfNeeded(ctx, lvg, vg, nil))
}

// rollbackProvisionedFileDevices must NOT detach the loop or remove the backing
// file of a device whose loop has already become a live PV — by a concurrent
// reconcile or by a pvcreate/vgcreate that materially succeeded but returned a
// non-zero status. Tearing it down deletes the backing file out from under a
// PV the VG is using, which on real clusters left the single backing file
// attached to two loops (one "(deleted)") and doubled the VG. Here CreatePV
// reports failure yet the loop shows up as a PV in the authoritative pvs list,
// so rollback must skip it entirely.
func TestExtendFileDevicesIfNeeded_DoesNotRollBackLoopThatBecamePV(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)
	ctx := context.Background()

	fd := v1alpha1.LVMVolumeGroupFileDeviceSpec{Directory: "/data", Size: resource.MustParse("10Gi")}
	lvg := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: v1.ObjectMeta{Name: "vg-a"},
		Spec:       v1alpha1.LVMVolumeGroupSpec{FileDevices: []v1alpha1.LVMVolumeGroupFileDeviceSpec{fd}},
	}
	path := utils.BuildFileDevicePath(fd.Directory, "vg-a", fd.Size)
	vg := internal.VGData{VGName: "vg-test"}

	gomock.InOrder(
		mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), path).Return("losetup -j", "", nil),
		mc.EXPECT().EnsureFileDeviceDirectory(gomock.Any(), fd.Directory).Return("mkdir -p ...", nil),
		mc.EXPECT().GetAvailableBytes(gomock.Any(), fd.Directory).Return("stat -f ...", int64(1)<<50, nil),
		mc.EXPECT().CreateFileDevice(gomock.Any(), path, fd.Size.Value()).Return("fallocate ...", nil),
		mc.EXPECT().SetupLoopDevice(gomock.Any(), path).Return("losetup --find ...", "/dev/loop4", nil),
		mc.EXPECT().CreatePV("/dev/loop4").Return("pvcreate ...", errors.New("fd leaked on lvm invocation")),
		// The pvcreate actually wrote the PV label despite the error, so the
		// authoritative pvs list reports /dev/loop4 as a PV of the VG.
		mc.EXPECT().GetAllPVs(gomock.Any()).Return(
			[]internal.PVData{{PVName: "/dev/loop4", VGName: "vg-test"}}, "pvs", bytes.Buffer{}, nil),
	)
	// No DetachLoopDevice / RemoveFileDevice expectations: rollback must skip the
	// in-use loop. gomock fails the test if either is called.

	assert.Error(t, r.extendFileDevicesIfNeeded(ctx, lvg, vg, nil))
}

// validateLVGForUpdateFunc must count the capacity of newly-added
// spec.fileDevices entries (not yet PVs in the VG) when validating thin-pool
// sizes — mirroring additionBlockDeviceSpace for block devices. Without it, a
// combined "append a fileDevices entry + grow thin-pools" edit is validated
// against the pre-extend VG size and wrongly rejected.
//
// Two thin-pools are used on purpose: with a single pool, a request that
// exceeds the VG size is silently accepted as a "100% of VG" pool, so the
// missing-capacity bug would not surface. With two pools, the too-small VG
// size pushes the first pool into the full-VG branch and produces a rejection
// ("requests size of full VG space, but there are any other thin-pools"),
// which the fix avoids.
func TestValidateLVGForUpdateFunc_CountsNewFileDeviceSpaceForThinPools(t *testing.T) {
	const vgName = "test-vg-file"

	makeLVG := func(status v1alpha1.LVMVolumeGroupStatus) *v1alpha1.LVMVolumeGroup {
		return &v1alpha1.LVMVolumeGroup{
			ObjectMeta: v1.ObjectMeta{Name: "vg-file"},
			Spec: v1alpha1.LVMVolumeGroupSpec{
				ActualVGNameOnTheNode: vgName,
				FileDevices: []v1alpha1.LVMVolumeGroupFileDeviceSpec{
					{Directory: "/data", Size: resource.MustParse("5G")},
				},
				ThinPools: []v1alpha1.LVMVolumeGroupThinPoolSpec{
					{Name: "tp1", Size: "2G"},
					{Name: "tp2", Size: "2G"},
				},
			},
			Status: status,
		}
	}

	t.Run("new_file_device_capacity_is_counted", func(t *testing.T) {
		r := setupReconciler()
		// VG currently holds 1G; the 5G file device is new (absent from status),
		// so newTotalVGSize = 1G + 5G = 6G and both 2G pools fit.
		vgs := []internal.VGData{{VGName: vgName, VGSize: resource.MustParse("1G"), VGFree: resource.MustParse("1G")}}
		r.sdsCache.StoreVGs(vgs, bytes.Buffer{})
		r.sdsCache.StorePVs(nil, bytes.Buffer{})

		valid, reason := r.validateLVGForUpdateFunc(context.Background(), makeLVG(v1alpha1.LVMVolumeGroupStatus{}), map[string]v1alpha1.BlockDevice{})
		if assert.True(t, valid) {
			assert.Equal(t, "", reason)
		}
	})

	t.Run("existing_file_device_is_not_double_counted", func(t *testing.T) {
		r := setupReconciler()
		// The same 5G file device is already in status (already part of the 1G
		// VG for the purpose of the test), so it is NOT added again:
		// newTotalVGSize stays 1G and the two 2G pools no longer fit.
		existingPath := utils.BuildFileDevicePath("/data", "vg-file", resource.MustParse("5G"))
		status := v1alpha1.LVMVolumeGroupStatus{
			Nodes: []v1alpha1.LVMVolumeGroupNode{{
				Name:        "test_node",
				FileDevices: []v1alpha1.LVMVolumeGroupFileDevice{{FilePath: existingPath}},
			}},
		}
		vgs := []internal.VGData{{VGName: vgName, VGSize: resource.MustParse("1G"), VGFree: resource.MustParse("1G")}}
		r.sdsCache.StoreVGs(vgs, bytes.Buffer{})
		r.sdsCache.StorePVs(nil, bytes.Buffer{})

		valid, _ := r.validateLVGForUpdateFunc(context.Background(), makeLVG(status), map[string]v1alpha1.BlockDevice{})
		assert.False(t, valid)
	})

	t.Run("symlink_canonicalized_status_path_still_matches", func(t *testing.T) {
		r := setupReconciler()
		// losetup reports the loop's backing file with the directory symlink
		// resolved (/data -> /mnt/disk1/data), so status FilePath differs from
		// BuildFileDevicePath in its directory but shares the basename. The
		// existing device must still be recognized (basename match) and NOT
		// counted as new — so newTotalVGSize stays 1G and the two 2G pools no
		// longer fit, exactly like the same-path case above. A full-path compare
		// would miss it, inflate the VG size, and wrongly pass validation.
		specPath := utils.BuildFileDevicePath("/data", "vg-file", resource.MustParse("5G"))
		canonicalStatusPath := "/mnt/disk1" + specPath // /mnt/disk1/data/sds-...img
		status := v1alpha1.LVMVolumeGroupStatus{
			Nodes: []v1alpha1.LVMVolumeGroupNode{{
				Name:        "test_node",
				FileDevices: []v1alpha1.LVMVolumeGroupFileDevice{{FilePath: canonicalStatusPath}},
			}},
		}
		vgs := []internal.VGData{{VGName: vgName, VGSize: resource.MustParse("1G"), VGFree: resource.MustParse("1G")}}
		r.sdsCache.StoreVGs(vgs, bytes.Buffer{})
		r.sdsCache.StorePVs(nil, bytes.Buffer{})

		valid, _ := r.validateLVGForUpdateFunc(context.Background(), makeLVG(status), map[string]v1alpha1.BlockDevice{})
		assert.False(t, valid)
	})
}
