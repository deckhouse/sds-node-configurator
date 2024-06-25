/*
Copyright 2023 Flant JSC

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
	"k8s.io/apimachinery/pkg/api/resource"
	"sds-node-configurator/api/v1alpha1"
	"sds-node-configurator/internal"
	"sds-node-configurator/pkg/logger"
	"sds-node-configurator/pkg/monitoring"
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestLVMVolumeGroupDiscover(t *testing.T) {
	var (
		ctx = context.Background()
		cl  = NewFakeClient()
		log = logger.Logger{}
	)

	t.Run("getThinPools_returns_only_thinPools", func(t *testing.T) {
		lvs := []internal.LVData{
			{
				LVName: "not_thinPool",
				LVAttr: "s",
			},
			{
				LVName: "thinPool1",
				LVAttr: "t",
			},
			{
				LVName: "thinPool2",
				LVAttr: "t",
			},
		}

		expected := []internal.LVData{
			{
				LVName: "thinPool1",
				LVAttr: "t",
			},
			{
				LVName: "thinPool2",
				LVAttr: "t",
			},
		}

		actual := getThinPools(lvs)
		assert.Equal(t, expected, actual)
	})

	t.Run("checkVGHealth_returns_Operational", func(t *testing.T) {
		const (
			vgName = "testVg"
			vgUuid = "testUuid"
		)
		bds := map[string][]v1alpha1.BlockDevice{
			vgName + vgUuid: {{}},
		}
		vgIssues := map[string]string{}
		pvIssues := map[string][]string{}
		lvIssues := map[string]map[string]string{}
		vg := internal.VGData{VGName: vgName, VGUuid: vgUuid}

		health, _ := checkVGHealth(bds, vgIssues, pvIssues, lvIssues, vg)
		assert.Equal(t, health, internal.LVMVGHealthOperational)
	})

	t.Run("checkVGHealth_returns_NonOperational", func(t *testing.T) {
		const (
			vgName = "testVg"
			vgUuid = "testUuid"
		)
		bds := map[string][]v1alpha1.BlockDevice{
			vgName + vgUuid: {},
		}
		vgIssues := map[string]string{}
		pvIssues := map[string][]string{}
		lvIssues := map[string]map[string]string{}
		vg := internal.VGData{VGName: vgName, VGUuid: vgUuid}

		health, _ := checkVGHealth(bds, vgIssues, pvIssues, lvIssues, vg)
		assert.Equal(t, health, internal.LVMVGHealthNonOperational)
	})

	t.Run("getUsedSizeMiB_returns_usedSize_in_M", func(t *testing.T) {
		size, err := resource.ParseQuantity("2G")
		if err != nil {
			t.Error(err)
		}

		lv := internal.LVData{
			LVSize:      size,
			DataPercent: "50",
		}
		expected := "97656250Ki"
		actual, err := getThinPoolUsedSize(lv)

		if assert.NoError(t, err) {
			assert.Equal(t, expected, actual.String())
		}
	})

	t.Run("sortPVsByVG_returns_sorted_pvs", func(t *testing.T) {
		const (
			firstVgName  = "firstVg"
			firstVgUuid  = "firstUUID"
			secondVgName = "secondVg"
			secondVgUuid = "secondUUID"
		)
		pvs := []internal.PVData{
			{
				PVName: "first",
				VGName: firstVgName,
				VGUuid: firstVgUuid,
			},
			{
				PVName: "second",
				VGName: secondVgName,
				VGUuid: secondVgUuid,
			},
		}

		vgs := []internal.VGData{
			{
				VGName: firstVgName,
				VGUuid: firstVgUuid,
			},
			{
				VGName: secondVgName,
				VGUuid: secondVgUuid,
			},
		}

		expected := map[string][]internal.PVData{
			firstVgName + firstVgUuid:   {pvs[0]},
			secondVgName + secondVgUuid: {pvs[1]},
		}

		actual := sortPVsByVG(pvs, vgs)
		assert.Equal(t, expected, actual)
	})

	t.Run("sortBlockDevicesByVG_returns_sorted_bds", func(t *testing.T) {
		const (
			firstVgName  = "firstVg"
			firstVgUuid  = "firstUUID"
			secondVgName = "secondVg"
			secondVgUuid = "secondUUID"
		)
		vgs := []internal.VGData{
			{
				VGName: firstVgName,
				VGUuid: firstVgUuid,
			},
			{
				VGName: secondVgName,
				VGUuid: secondVgUuid,
			},
		}

		bds := map[string]v1alpha1.BlockDevice{
			"first": {
				ObjectMeta: metav1.ObjectMeta{Name: "first"},
				Status: v1alpha1.BlockDeviceStatus{
					ActualVGNameOnTheNode: firstVgName,
					VGUuid:                firstVgUuid,
				},
			},
			"second": {
				ObjectMeta: metav1.ObjectMeta{Name: "second"},
				Status: v1alpha1.BlockDeviceStatus{
					ActualVGNameOnTheNode: secondVgName,
					VGUuid:                secondVgUuid,
				},
			},
		}

		expected := map[string][]v1alpha1.BlockDevice{
			firstVgName + firstVgUuid:   {bds["first"]},
			secondVgName + secondVgUuid: {bds["second"]},
		}

		actual := sortBlockDevicesByVG(bds, vgs)
		assert.Equal(t, expected, actual)
	})

	t.Run("sortLVsByVG_returns_sorted_LVs", func(t *testing.T) {
		const (
			firstVgName  = "firstVg"
			firstVgUuid  = "firstUUID"
			secondVgName = "secondVg"
			secondVgUuid = "secondUUID"
		)
		vgs := []internal.VGData{
			{
				VGName: firstVgName,
				VGUuid: firstVgUuid,
			},
			{
				VGName: secondVgName,
				VGUuid: secondVgUuid,
			},
		}
		lvs := []internal.LVData{
			{
				LVName: "first",
				VGName: firstVgName,
				VGUuid: firstVgUuid,
			},
			{
				LVName: "second",
				VGName: secondVgName,
				VGUuid: secondVgUuid,
			},
		}
		expected := map[string][]internal.LVData{
			firstVgName + firstVgUuid:   {lvs[0]},
			secondVgName + secondVgUuid: {lvs[1]},
		}

		actual := sortThinPoolsByVG(lvs, vgs)
		assert.Equal(t, expected, actual)
	})

	t.Run("configureCandidateNodesDevices_returns_candidates_nodes", func(t *testing.T) {
		const (
			vgName   = "test_vg"
			vgUuid   = "vg_uuid"
			nodeName = "test_node"
		)

		vg := internal.VGData{
			VGName: vgName,
			VGUuid: vgUuid,
		}

		size10G, err := resource.ParseQuantity("10G")
		size1G, err := resource.ParseQuantity("1G")
		if err != nil {
			t.Error(err)
		}

		pvs := []internal.PVData{
			{
				PVName: "test_pv1",
				PVSize: size10G,
				PVUuid: "pv_uuid1",
				VGName: vgName,
				VGUuid: vgUuid,
			},
			{
				PVName: "test_pv2",
				PVSize: size1G,
				PVUuid: "pv_uuid2",
				VGUuid: vgUuid,
				VGName: vgName,
			},
		}

		bds := []v1alpha1.BlockDevice{
			{
				ObjectMeta: metav1.ObjectMeta{Name: "block_device1"},
				Status: v1alpha1.BlockDeviceStatus{
					Path:                  "test_pv1",
					Size:                  resource.MustParse("10G"),
					VGUuid:                vgUuid,
					ActualVGNameOnTheNode: vgName,
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{Name: "block_device2"},
				Status: v1alpha1.BlockDeviceStatus{
					Path:                  "test_pv2",
					Size:                  resource.MustParse("1G"),
					VGUuid:                vgUuid,
					ActualVGNameOnTheNode: vgName,
				},
			},
		}

		expected := map[string][]internal.LVMVGDevice{
			nodeName: {
				{
					Path:        "test_pv1",
					PVSize:      *resource.NewQuantity(size10G.Value(), resource.BinarySI),
					DevSize:     *resource.NewQuantity(size10G.Value(), resource.BinarySI),
					PVUuid:      "pv_uuid1",
					BlockDevice: "block_device1",
				},
				{
					Path:        "test_pv2",
					PVSize:      *resource.NewQuantity(size1G.Value(), resource.BinarySI),
					DevSize:     *resource.NewQuantity(size1G.Value(), resource.BinarySI),
					PVUuid:      "pv_uuid2",
					BlockDevice: "block_device2",
				},
			},
		}
		mp := map[string][]v1alpha1.BlockDevice{vgName + vgUuid: bds}
		ar := map[string][]internal.PVData{vgName + vgUuid: pvs}

		actual := configureCandidateNodeDevices(ar, mp, vg, nodeName)

		assert.Equal(t, expected, actual)
	})

	t.Run("sortBlockDevicesByVG", func(t *testing.T) {
		bds := map[string]v1alpha1.BlockDevice{
			"first": {
				ObjectMeta: metav1.ObjectMeta{Name: "first"},
				Status: v1alpha1.BlockDeviceStatus{
					VGUuid:                "firstUUID",
					ActualVGNameOnTheNode: "firstVG",
				},
			},
			"second": {
				ObjectMeta: metav1.ObjectMeta{Name: "second"},
				Status: v1alpha1.BlockDeviceStatus{
					VGUuid:                "firstUUID",
					ActualVGNameOnTheNode: "firstVG",
				},
			},
		}

		vgs := []internal.VGData{
			{
				VGName: "firstVG",
				VGUuid: "firstUUID",
			},
		}
		actual := sortBlockDevicesByVG(bds, vgs)
		assert.Equal(t, 1, len(actual))

		sorted := actual["firstVGfirstUUID"]
		assert.Equal(t, 2, len(sorted))
	})

	t.Run("getVgType_returns_shared", func(t *testing.T) {
		vg := internal.VGData{VGShared: "shared"}
		expected := "Shared"

		actual := getVgType(vg)

		assert.Equal(t, expected, actual)
	})

	t.Run("getVgType_returns_local", func(t *testing.T) {
		vg := internal.VGData{VGShared: ""}
		expected := "Local"

		actual := getVgType(vg)

		assert.Equal(t, expected, actual)
	})

	t.Run("getSpecThinPools_returns_LVName_LVSize_map", func(t *testing.T) {
		const (
			vgName = "test_vg"
			vgUuid = "test_uuid"
		)

		vg := internal.VGData{VGName: vgName, VGUuid: vgUuid}

		firstSize, err := resource.ParseQuantity("1G")
		secondSize, err := resource.ParseQuantity("2G")
		if err != nil {
			t.Error(err)
		}

		thinPools := map[string][]internal.LVData{
			vgName + vgUuid: {
				{
					LVName: "first",
					LVSize: firstSize,
				},
				{
					LVName: "second",
					LVSize: secondSize,
				},
			},
		}

		expected := map[string]resource.Quantity{
			"first":  firstSize,
			"second": secondSize,
		}

		actual := getSpecThinPools(thinPools, vg)

		assert.Equal(t, expected, actual)
	})

	t.Run("CreateLVMVolumeGroup_creates_expected", func(t *testing.T) {
		const (
			LVMVGName             = "test_lvm"
			ActualVGNameOnTheNode = "test-vg"
			Type                  = "local"
			Health                = internal.LVMVGHealthOperational
			Message               = "No problems detected"
			VGUuid                = "test_uuid"
		)

		size10G, err := resource.ParseQuantity("10G")
		size1G, err := resource.ParseQuantity("1G")
		if err != nil {
			t.Error(err)
		}

		var (
			cl                = NewFakeClient()
			ctx               = context.Background()
			testLogger        = logger.Logger{}
			testMetrics       = monitoring.GetMetrics("")
			blockDevicesNames = []string{"first", "second"}
			specThinPools     = map[string]resource.Quantity{"first": size10G}
			statusThinPools   = []internal.LVMVGStatusThinPool{
				{
					Name:       "first_status_pool",
					ActualSize: size10G,
					UsedSize:   resource.MustParse("4G"),
				},
			}
			nodes = map[string][]internal.LVMVGDevice{
				"test-node-1": {
					{
						Path:        "test/path",
						PVSize:      size1G,
						DevSize:     size1G,
						PVUuid:      "test-pv-uuid",
						BlockDevice: "test-device",
					},
				},
			}
		)

		candidate := internal.LVMVolumeGroupCandidate{
			LVMVGName:             LVMVGName,
			ActualVGNameOnTheNode: ActualVGNameOnTheNode,
			BlockDevicesNames:     blockDevicesNames,
			SpecThinPools:         specThinPools,
			Type:                  Type,
			AllocatedSize:         size10G,
			Health:                Health,
			Message:               Message,
			StatusThinPools:       statusThinPools,
			VGSize:                size10G,
			VGUuid:                VGUuid,
			Nodes:                 nodes,
		}

		expected := v1alpha1.LvmVolumeGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name:            LVMVGName,
				ResourceVersion: "1",
				OwnerReferences: []metav1.OwnerReference{},
			},
			Spec: v1alpha1.LvmVolumeGroupSpec{
				ActualVGNameOnTheNode: ActualVGNameOnTheNode,
				BlockDeviceNames:      blockDevicesNames,
				ThinPools:             convertSpecThinPools(specThinPools),
				Type:                  Type,
			},
			Status: v1alpha1.LvmVolumeGroupStatus{
				AllocatedSize: size10G,
				Nodes:         convertLVMVGNodes(nodes),
				ThinPools:     convertStatusThinPools(v1alpha1.LvmVolumeGroup{}, statusThinPools),
				VGSize:        size10G,
				VGUuid:        VGUuid,
			},
		}

		created, err := CreateLVMVolumeGroupByCandidate(ctx, testLogger, testMetrics, cl, candidate)
		if assert.NoError(t, err) {
			assert.Equal(t, &expected, created)
		}
	})

	t.Run("GetLVMVolumeGroup", func(t *testing.T) {
		const (
			LVMVGName             = "test_lvm"
			ActualVGNameOnTheNode = "test-vg"
			Type                  = "local"
			Health                = internal.LVMVGHealthOperational
			Message               = "No problems detected"
			VGUuid                = "test_uuid"
		)

		size10G, err := resource.ParseQuantity("10G")
		size1G, err := resource.ParseQuantity("1G")
		if err != nil {
			t.Error(err)
		}

		var (
			cl                = NewFakeClient()
			testMetrics       = monitoring.GetMetrics("")
			testLogger        = logger.Logger{}
			ctx               = context.Background()
			blockDevicesNames = []string{"first", "second"}
			specThinPools     = map[string]resource.Quantity{"first": size10G}
			statusThinPools   = []internal.LVMVGStatusThinPool{
				{
					Name:       "first_status_pool",
					ActualSize: size10G,
					UsedSize:   resource.MustParse("4G"),
				},
			}
			nodes = map[string][]internal.LVMVGDevice{
				"test-node-1": {
					{
						Path:        "test/path",
						PVSize:      size1G,
						DevSize:     size1G,
						PVUuid:      "test-pv-uuid",
						BlockDevice: "test-device",
					},
				},
			}
		)

		candidate := internal.LVMVolumeGroupCandidate{
			LVMVGName:             LVMVGName,
			ActualVGNameOnTheNode: ActualVGNameOnTheNode,
			BlockDevicesNames:     blockDevicesNames,
			SpecThinPools:         specThinPools,
			Type:                  Type,
			AllocatedSize:         size10G,
			Health:                Health,
			Message:               Message,
			StatusThinPools:       statusThinPools,
			VGSize:                size10G,
			VGUuid:                VGUuid,
			Nodes:                 nodes,
		}

		expected := map[string]v1alpha1.LvmVolumeGroup{
			LVMVGName: {
				ObjectMeta: metav1.ObjectMeta{
					Name:            LVMVGName,
					ResourceVersion: "1",
					OwnerReferences: nil,
				},
				Spec: v1alpha1.LvmVolumeGroupSpec{
					ActualVGNameOnTheNode: ActualVGNameOnTheNode,
					BlockDeviceNames:      blockDevicesNames,
					ThinPools:             convertSpecThinPools(specThinPools),
					Type:                  Type,
				},
				Status: v1alpha1.LvmVolumeGroupStatus{
					AllocatedSize: size10G,
					Nodes:         convertLVMVGNodes(nodes),
					ThinPools:     convertStatusThinPools(v1alpha1.LvmVolumeGroup{}, statusThinPools),
					VGSize:        size10G,
					VGUuid:        VGUuid,
				},
			},
		}

		created, err := CreateLVMVolumeGroupByCandidate(ctx, testLogger, testMetrics, cl, candidate)
		if assert.NoError(t, err) && assert.NotNil(t, created) {
			actual, err := GetAPILVMVolumeGroups(ctx, cl, testMetrics)
			if assert.NoError(t, err) && assert.Equal(t, 1, len(actual)) {
				assert.Equal(t, expected, actual)
			}
		}
	})

	t.Run("DeleteLVMVolumeGroup", func(t *testing.T) {
		const (
			LVMVGName             = "test_lvm"
			ActualVGNameOnTheNode = "test-vg"
			Type                  = "local"
			Health                = internal.LVMVGHealthOperational
			Message               = "No problems detected"
			VGUuid                = "test_uuid"
		)

		size10G, err := resource.ParseQuantity("10G")
		size1G, err := resource.ParseQuantity("1G")
		if err != nil {
			t.Error(err)
		}

		var (
			cl                = NewFakeClient()
			ctx               = context.Background()
			testMetrics       = monitoring.GetMetrics("")
			testLogger        = logger.Logger{}
			blockDevicesNames = []string{"first", "second"}
			specThinPools     = map[string]resource.Quantity{"first": size10G}
			statusThinPools   = []internal.LVMVGStatusThinPool{
				{
					Name:       "first_status_pool",
					ActualSize: size10G,
					UsedSize:   resource.MustParse("4G"),
				},
			}
			nodes = map[string][]internal.LVMVGDevice{
				"test-node-1": {
					{
						Path:        "test/path",
						PVSize:      size1G,
						DevSize:     size1G,
						PVUuid:      "test-pv-uuid",
						BlockDevice: "test-device",
					},
				},
			}
		)

		candidate := internal.LVMVolumeGroupCandidate{
			LVMVGName:             LVMVGName,
			ActualVGNameOnTheNode: ActualVGNameOnTheNode,
			BlockDevicesNames:     blockDevicesNames,
			SpecThinPools:         specThinPools,
			Type:                  Type,
			AllocatedSize:         size10G,
			Health:                Health,
			Message:               Message,
			StatusThinPools:       statusThinPools,
			VGSize:                size10G,
			VGUuid:                VGUuid,
			Nodes:                 nodes,
		}

		created, err := CreateLVMVolumeGroupByCandidate(ctx, testLogger, testMetrics, cl, candidate)
		if assert.NoError(t, err) && assert.NotNil(t, created) {
			actual, err := GetAPILVMVolumeGroups(ctx, cl, testMetrics)
			if assert.NoError(t, err) && assert.Equal(t, 1, len(actual)) {
				err := DeleteLVMVolumeGroup(ctx, cl, testMetrics, &v1alpha1.LvmVolumeGroup{
					ObjectMeta: metav1.ObjectMeta{
						Name: LVMVGName,
					},
				})
				if assert.NoError(t, err) {
					actual, err := GetAPILVMVolumeGroups(ctx, cl, testMetrics)
					if assert.NoError(t, err) {
						assert.Equal(t, 0, len(actual))
					}
				}
			}
		}
	})

	t.Run("UpdateLVMVolumeGroup", func(t *testing.T) {
		const (
			LVMVGName             = "test_lvm"
			ActualVGNameOnTheNode = "test-vg"
			Type                  = "local"
			Health                = internal.LVMVGHealthOperational
			Message               = "No problems detected"
			VGUuid                = "test_uuid"
		)

		size10G, err := resource.ParseQuantity("10G")
		size1G, err := resource.ParseQuantity("1G")
		if err != nil {
			t.Error(err)
		}

		var (
			cl                = NewFakeClient()
			ctx               = context.Background()
			testMetrics       = monitoring.GetMetrics("")
			testLogger        = logger.Logger{}
			BlockDevicesNames = []string{"first", "second"}
			SpecThinPools     = map[string]resource.Quantity{"first": size1G}
			StatusThinPools   = []internal.LVMVGStatusThinPool{
				{
					Name:       "first_status_pool",
					ActualSize: size10G,
					UsedSize:   resource.MustParse("4G"),
				},
			}
			oldNodes = map[string][]internal.LVMVGDevice{
				"test-node-1": {
					{
						Path:        "test/path",
						PVSize:      size1G,
						DevSize:     size1G,
						PVUuid:      "test-pv-uuid",
						BlockDevice: "test-device",
					},
				},
			}
			newNodes = map[string][]internal.LVMVGDevice{
				"test-node-1": {
					{
						Path:        "test/path",
						PVSize:      size1G,
						DevSize:     size1G,
						PVUuid:      "test-pv-uuid",
						BlockDevice: "test-device",
					},
					{
						Path:        "test/path2",
						PVSize:      size1G,
						DevSize:     size1G,
						PVUuid:      "test-pv-uuid2",
						BlockDevice: "test-device2",
					},
				},
			}
		)

		oldCandidate := internal.LVMVolumeGroupCandidate{
			LVMVGName:             LVMVGName,
			ActualVGNameOnTheNode: ActualVGNameOnTheNode,
			BlockDevicesNames:     BlockDevicesNames,
			SpecThinPools:         SpecThinPools,
			Type:                  Type,
			AllocatedSize:         size10G,
			Health:                Health,
			Message:               Message,
			StatusThinPools:       StatusThinPools,
			VGSize:                size10G,
			VGUuid:                VGUuid,
			Nodes:                 oldNodes,
		}

		newCandidate := internal.LVMVolumeGroupCandidate{
			LVMVGName:             LVMVGName,
			ActualVGNameOnTheNode: ActualVGNameOnTheNode,
			BlockDevicesNames:     BlockDevicesNames,
			SpecThinPools:         SpecThinPools,
			Type:                  Type,
			AllocatedSize:         size10G,
			Health:                Health,
			Message:               Message,
			StatusThinPools:       StatusThinPools,
			VGSize:                size10G,
			VGUuid:                VGUuid,
			Nodes:                 newNodes,
		}

		expected := v1alpha1.LvmVolumeGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name:            LVMVGName,
				ResourceVersion: "2",
				OwnerReferences: nil,
			},
			Spec: v1alpha1.LvmVolumeGroupSpec{
				ActualVGNameOnTheNode: ActualVGNameOnTheNode,
				BlockDeviceNames:      BlockDevicesNames,
				ThinPools:             convertSpecThinPools(SpecThinPools),
				Type:                  Type,
			},
			Status: v1alpha1.LvmVolumeGroupStatus{
				AllocatedSize: size10G,
				Nodes:         convertLVMVGNodes(newNodes),
				ThinPools:     convertStatusThinPools(v1alpha1.LvmVolumeGroup{}, StatusThinPools),
				VGSize:        size10G,
				VGUuid:        VGUuid,
			},
		}

		created, err := CreateLVMVolumeGroupByCandidate(ctx, testLogger, testMetrics, cl, oldCandidate)
		if assert.NoError(t, err) {
			err := UpdateLVMVolumeGroupByCandidate(ctx, cl, testMetrics, log, created, newCandidate)

			if assert.NoError(t, err) {
				lmvs, err := GetAPILVMVolumeGroups(ctx, cl, testMetrics)
				if assert.NoError(t, err) {
					actual := lmvs[LVMVGName]
					assert.Equal(t, expected, actual)
				}
			}
		}
	})

	t.Run("filterResourcesByNode_returns_current_node_resources", func(t *testing.T) {
		var (
			ctx          = context.Background()
			cl           = NewFakeClient()
			testLogger   = logger.Logger{}
			currentNode  = "test_node"
			vgName       = "test_vg"
			firstBDName  = "first_device"
			secondBDName = "second_device"
			firstLVName  = "first_lv"
			secondLVName = "second_lv"
			blockDevices = map[string]v1alpha1.BlockDevice{
				firstBDName: {
					ObjectMeta: metav1.ObjectMeta{
						Name: firstBDName,
					},
					Status: v1alpha1.BlockDeviceStatus{
						NodeName: currentNode,
					},
				},
				secondBDName: {
					ObjectMeta: metav1.ObjectMeta{
						Name: secondBDName,
					},
					Status: v1alpha1.BlockDeviceStatus{
						NodeName: "another_node",
					},
				},
			}

			lvs = map[string]v1alpha1.LvmVolumeGroup{
				firstLVName: {
					ObjectMeta: metav1.ObjectMeta{Name: firstLVName},
					Spec: v1alpha1.LvmVolumeGroupSpec{
						BlockDeviceNames:      []string{firstBDName},
						Type:                  Local,
						ActualVGNameOnTheNode: vgName,
					},
				},
				secondLVName: {
					ObjectMeta: metav1.ObjectMeta{Name: secondLVName},
					Spec: v1alpha1.LvmVolumeGroupSpec{
						BlockDeviceNames:      []string{secondBDName},
						Type:                  Local,
						ActualVGNameOnTheNode: vgName,
					},
				},
			}
		)

		expected := map[string]v1alpha1.LvmVolumeGroup{
			vgName: {
				ObjectMeta: metav1.ObjectMeta{Name: firstLVName},
				Spec: v1alpha1.LvmVolumeGroupSpec{
					BlockDeviceNames:      []string{firstBDName},
					Type:                  Local,
					ActualVGNameOnTheNode: vgName,
				},
			},
		}

		actual := filterLVGsByNode(ctx, cl, testLogger, lvs, blockDevices, currentNode)

		assert.Equal(t, expected, actual)
	})

	t.Run("filterResourcesByNode_returns_no_resources_for_current_node", func(t *testing.T) {
		var (
			currentNode  = "test_node"
			anotherNode  = "another_node"
			firstBDName  = "first_device"
			secondBDName = "second_device"
			firstLVName  = "first_lv"
			secondLVName = "second_lv"
			blockDevices = map[string]v1alpha1.BlockDevice{
				firstBDName: {
					ObjectMeta: metav1.ObjectMeta{
						Name: firstBDName,
					},
					Status: v1alpha1.BlockDeviceStatus{
						NodeName: anotherNode,
					},
				},
				secondBDName: {
					ObjectMeta: metav1.ObjectMeta{
						Name: secondBDName,
					},
					Status: v1alpha1.BlockDeviceStatus{
						NodeName: anotherNode,
					},
				},
			}

			lvs = map[string]v1alpha1.LvmVolumeGroup{
				firstLVName: {
					ObjectMeta: metav1.ObjectMeta{Name: firstLVName},
					Spec: v1alpha1.LvmVolumeGroupSpec{
						BlockDeviceNames: []string{firstBDName},
						Type:             Local,
					},
				},
				secondLVName: {
					ObjectMeta: metav1.ObjectMeta{Name: secondLVName},
					Spec: v1alpha1.LvmVolumeGroupSpec{
						BlockDeviceNames: []string{secondBDName},
						Type:             Local,
					},
				},
			}
		)

		actual := filterLVGsByNode(ctx, cl, log, lvs, blockDevices, currentNode)

		assert.Equal(t, 0, len(actual))
	})

	t.Run("hasLVMVolumeGroupDiff", func(t *testing.T) {
		t.Run("should_return_false", func(t *testing.T) {
			size10G, err := resource.ParseQuantity("10G")
			size1G, err := resource.ParseQuantity("1G")
			size13G, err := resource.ParseQuantity("13G")
			if err != nil {
				t.Error(err)
			}

			var (
				testLogger        = logger.Logger{}
				blockDevicesNames = []string{
					"first",
					"second",
				}
				specThinPools = map[string]resource.Quantity{
					"first":  size10G,
					"second": size1G,
				}
				specType        = "type"
				health          = internal.LVMVGHealthOperational
				message         = "all good"
				statusThinPools = []internal.LVMVGStatusThinPool{
					{
						Name:       "first",
						ActualSize: size10G,
						UsedSize:   resource.MustParse("2G"),
					},
					{
						Name:       "second",
						ActualSize: size10G,
						UsedSize:   resource.MustParse("2G"),
					},
				}
				nodes = map[string][]internal.LVMVGDevice{
					"test_node": {
						{
							Path:        "/test/ds",
							PVSize:      size1G,
							DevSize:     size13G,
							PVUuid:      "testUUID",
							BlockDevice: "something",
						},
					},
				}
			)
			candidate := internal.LVMVolumeGroupCandidate{
				BlockDevicesNames: blockDevicesNames,
				SpecThinPools:     specThinPools,
				Type:              specType,
				AllocatedSize:     size10G,
				Health:            health,
				Message:           message,
				StatusThinPools:   statusThinPools,
				VGSize:            size10G,
				Nodes:             nodes,
			}

			lvmVolumeGroup := v1alpha1.LvmVolumeGroup{
				Spec: v1alpha1.LvmVolumeGroupSpec{
					BlockDeviceNames: blockDevicesNames,
					ThinPools:        convertSpecThinPools(specThinPools),
					Type:             specType,
				},
				Status: v1alpha1.LvmVolumeGroupStatus{
					AllocatedSize: resource.MustParse("9765625Ki"),
					Nodes:         convertLVMVGNodes(nodes),
					ThinPools:     convertStatusThinPools(v1alpha1.LvmVolumeGroup{}, statusThinPools),
					VGSize:        resource.MustParse("9765625Ki"),
				},
			}

			assert.False(t, hasLVMVolumeGroupDiff(testLogger, lvmVolumeGroup, candidate))
		})

		t.Run("should_return_true", func(t *testing.T) {
			size10G, err := resource.ParseQuantity("10G")
			size1G, err := resource.ParseQuantity("1G")
			size13G, err := resource.ParseQuantity("13G")
			if err != nil {
				t.Error(err)
			}

			var (
				blockDevicesNames = []string{
					"first",
					"second",
				}
				specThinPools = map[string]resource.Quantity{
					"first":  size10G,
					"second": size1G,
				}
				specType        = "type"
				allocatedSize   = resource.MustParse("10G")
				health          = internal.LVMVGHealthOperational
				statusThinPools = []internal.LVMVGStatusThinPool{
					{
						Name:       "first",
						ActualSize: size10G,
						UsedSize:   resource.MustParse("2G"),
					},
					{
						Name:       "second",
						ActualSize: size10G,
						UsedSize:   resource.MustParse("2G"),
					},
				}
				vgSize = resource.MustParse("10G")
				nodes  = map[string][]internal.LVMVGDevice{
					"test_node": {
						{
							Path:        "/test/ds",
							PVSize:      size1G,
							DevSize:     size13G,
							PVUuid:      "testUUID",
							BlockDevice: "something",
						},
						{
							Path:        "/test/ds2",
							PVSize:      size1G,
							DevSize:     size13G,
							PVUuid:      "testUUID2",
							BlockDevice: "something2",
						},
					},
				}
			)
			candidate := internal.LVMVolumeGroupCandidate{
				BlockDevicesNames: blockDevicesNames,
				SpecThinPools:     specThinPools,
				Type:              specType,
				AllocatedSize:     size10G,
				Health:            health,
				Message:           "NewMessage",
				StatusThinPools:   statusThinPools,
				VGSize:            size10G,
				Nodes:             nodes,
			}

			lvmVolumeGroup := v1alpha1.LvmVolumeGroup{
				Spec: v1alpha1.LvmVolumeGroupSpec{
					BlockDeviceNames: blockDevicesNames,
					ThinPools:        convertSpecThinPools(specThinPools),
					Type:             specType,
				},
				Status: v1alpha1.LvmVolumeGroupStatus{
					AllocatedSize: allocatedSize,
					Nodes:         convertLVMVGNodes(nodes),
					ThinPools:     convertStatusThinPools(v1alpha1.LvmVolumeGroup{}, statusThinPools),
					VGSize:        vgSize,
				},
			}

			assert.True(t, hasLVMVolumeGroupDiff(logger.Logger{}, lvmVolumeGroup, candidate))
		})
	})

	t.Run("updateLVGConditionIfNeeded", func(t *testing.T) {
		const (
			lvgName = "test-lvg"
			conType = "test-type"
			reason  = "test-reason"
			message = "test-message"
		)
		lvg := &v1alpha1.LvmVolumeGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name: lvgName,
			},
		}

		err := cl.Create(ctx, lvg)
		if err != nil {
			t.Error(err)
		}

		err = updateLVGConditionIfNeeded(ctx, cl, logger.Logger{}, lvg, metav1.ConditionTrue, conType, reason, message)
		if assert.NoError(t, err) {
			err = cl.Get(ctx, client.ObjectKey{
				Name: lvgName,
			}, lvg)
			if err != nil {
				t.Error(err)
			}

			assert.Equal(t, 1, len(lvg.Status.Conditions))
			assert.Equal(t, metav1.ConditionTrue, lvg.Status.Conditions[0].Status)
			assert.Equal(t, conType, lvg.Status.Conditions[0].Type)
			assert.Equal(t, reason, lvg.Status.Conditions[0].Reason)
			assert.Equal(t, message, lvg.Status.Conditions[0].Message)
		}
	})
}

func NewFakeClient() client.WithWatch {
	s := scheme.Scheme
	_ = metav1.AddMetaToScheme(s)
	_ = v1alpha1.AddToScheme(s)

	builder := fake.NewClientBuilder().WithScheme(s).WithStatusSubresource(&v1alpha1.LvmVolumeGroup{})

	cl := builder.Build()
	return cl
}
