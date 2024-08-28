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
	"testing"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"agent/internal"
	"agent/pkg/logger"
	"agent/pkg/monitoring"
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
			vgUUID = "testUuid"
		)
		bds := map[string][]v1alpha1.BlockDevice{
			vgName + vgUUID: {{}},
		}
		vgIssues := map[string]string{}
		pvIssues := map[string][]string{}
		lvIssues := map[string]map[string]string{}
		vg := internal.VGData{VGName: vgName, VGUUID: vgUUID}

		health, _ := checkVGHealth(bds, vgIssues, pvIssues, lvIssues, vg)
		assert.Equal(t, health, internal.LVMVGHealthOperational)
	})

	t.Run("checkVGHealth_returns_NonOperational", func(t *testing.T) {
		const (
			vgName = "testVg"
			vgUUID = "testUuid"
		)
		bds := map[string][]v1alpha1.BlockDevice{
			vgName + vgUUID: {},
		}
		vgIssues := map[string]string{}
		pvIssues := map[string][]string{}
		lvIssues := map[string]map[string]string{}
		vg := internal.VGData{VGName: vgName, VGUUID: vgUUID}

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
			firstVgUUID  = "firstUUID"
			secondVgName = "secondVg"
			secondVgUUID = "secondUUID"
		)
		pvs := []internal.PVData{
			{
				PVName: "first",
				VGName: firstVgName,
				VGUuid: firstVgUUID,
			},
			{
				PVName: "second",
				VGName: secondVgName,
				VGUuid: secondVgUUID,
			},
		}

		vgs := []internal.VGData{
			{
				VGName: firstVgName,
				VGUUID: firstVgUUID,
			},
			{
				VGName: secondVgName,
				VGUUID: secondVgUUID,
			},
		}

		expected := map[string][]internal.PVData{
			firstVgName + firstVgUUID:   {pvs[0]},
			secondVgName + secondVgUUID: {pvs[1]},
		}

		actual := sortPVsByVG(pvs, vgs)
		assert.Equal(t, expected, actual)
	})

	t.Run("sortBlockDevicesByVG_returns_sorted_bds", func(t *testing.T) {
		const (
			firstVgName  = "firstVg"
			firstVgUUID  = "firstUUID"
			secondVgName = "secondVg"
			secondVgUUID = "secondUUID"
		)
		vgs := []internal.VGData{
			{
				VGName: firstVgName,
				VGUUID: firstVgUUID,
			},
			{
				VGName: secondVgName,
				VGUUID: secondVgUUID,
			},
		}

		bds := map[string]v1alpha1.BlockDevice{
			"first": {
				ObjectMeta: metav1.ObjectMeta{Name: "first"},
				Status: v1alpha1.BlockDeviceStatus{
					ActualVGNameOnTheNode: firstVgName,
					VGUuid:                firstVgUUID,
				},
			},
			"second": {
				ObjectMeta: metav1.ObjectMeta{Name: "second"},
				Status: v1alpha1.BlockDeviceStatus{
					ActualVGNameOnTheNode: secondVgName,
					VGUuid:                secondVgUUID,
				},
			},
		}

		expected := map[string][]v1alpha1.BlockDevice{
			firstVgName + firstVgUUID:   {bds["first"]},
			secondVgName + secondVgUUID: {bds["second"]},
		}

		actual := sortBlockDevicesByVG(bds, vgs)
		assert.Equal(t, expected, actual)
	})

	t.Run("sortLVsByVG_returns_sorted_LVs", func(t *testing.T) {
		const (
			firstVgName  = "firstVg"
			firstVgUUID  = "firstUUID"
			secondVgName = "secondVg"
			secondVgUUID = "secondUUID"
		)
		vgs := []internal.VGData{
			{
				VGName: firstVgName,
				VGUUID: firstVgUUID,
			},
			{
				VGName: secondVgName,
				VGUUID: secondVgUUID,
			},
		}
		lvs := []internal.LVData{
			{
				LVName: "first",
				VGName: firstVgName,
				VGUuid: firstVgUUID,
			},
			{
				LVName: "second",
				VGName: secondVgName,
				VGUuid: secondVgUUID,
			},
		}
		expected := map[string][]internal.LVData{
			firstVgName + firstVgUUID:   {lvs[0]},
			secondVgName + secondVgUUID: {lvs[1]},
		}

		actual := sortThinPoolsByVG(lvs, vgs)
		assert.Equal(t, expected, actual)
	})

	t.Run("configureCandidateNodesDevices_returns_candidates_nodes", func(t *testing.T) {
		const (
			vgName   = "test_vg"
			vgUUID   = "vg_uuid"
			nodeName = "test_node"
		)

		vg := internal.VGData{
			VGName: vgName,
			VGUUID: vgUUID,
		}

		size10G, err := resource.ParseQuantity("10G")
		if err != nil {
			t.Error(err)
		}
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
				VGUuid: vgUUID,
			},
			{
				PVName: "test_pv2",
				PVSize: size1G,
				PVUuid: "pv_uuid2",
				VGUuid: vgUUID,
				VGName: vgName,
			},
		}

		bds := []v1alpha1.BlockDevice{
			{
				ObjectMeta: metav1.ObjectMeta{Name: "block_device1"},
				Status: v1alpha1.BlockDeviceStatus{
					Path:                  "test_pv1",
					Size:                  resource.MustParse("10G"),
					VGUuid:                vgUUID,
					ActualVGNameOnTheNode: vgName,
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{Name: "block_device2"},
				Status: v1alpha1.BlockDeviceStatus{
					Path:                  "test_pv2",
					Size:                  resource.MustParse("1G"),
					VGUuid:                vgUUID,
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
					PVUUID:      "pv_uuid1",
					BlockDevice: "block_device1",
				},
				{
					Path:        "test_pv2",
					PVSize:      *resource.NewQuantity(size1G.Value(), resource.BinarySI),
					DevSize:     *resource.NewQuantity(size1G.Value(), resource.BinarySI),
					PVUUID:      "pv_uuid2",
					BlockDevice: "block_device2",
				},
			},
		}
		mp := map[string][]v1alpha1.BlockDevice{vgName + vgUUID: bds}
		ar := map[string][]internal.PVData{vgName + vgUUID: pvs}

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
				VGUUID: "firstUUID",
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
			vgUUID = "test_uuid"
		)

		vg := internal.VGData{VGName: vgName, VGUUID: vgUUID}

		firstSize, err := resource.ParseQuantity("1G")
		if err != nil {
			t.Error(err)
		}
		secondSize, err := resource.ParseQuantity("2G")
		if err != nil {
			t.Error(err)
		}

		thinPools := map[string][]internal.LVData{
			vgName + vgUUID: {
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
			VGUUID                = "test_uuid"
			NodeName              = "test-node"
		)

		size10G := resource.MustParse("10G")
		size1G := resource.MustParse("1G")

		var (
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
						PVUUID:      "test-pv-uuid",
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
			VGUUID:                VGUUID,
			Nodes:                 nodes,
		}

		thinPools, err := convertStatusThinPools(v1alpha1.LVMVolumeGroup{}, statusThinPools)
		if err != nil {
			t.Error(err)
		}
		expected := v1alpha1.LVMVolumeGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name:            LVMVGName,
				ResourceVersion: "1",
				OwnerReferences: []metav1.OwnerReference{},
			},
			Spec: v1alpha1.LVMVolumeGroupSpec{
				ActualVGNameOnTheNode: ActualVGNameOnTheNode,
				ThinPools:             convertSpecThinPools(specThinPools),
				Type:                  Type,
				Local:                 v1alpha1.LVMVolumeGroupLocalSpec{NodeName: NodeName},
				BlockDeviceSelector:   configureBlockDeviceSelector(candidate, NodeName),
			},
			Status: v1alpha1.LVMVolumeGroupStatus{
				AllocatedSize: size10G,
				Nodes:         convertLVMVGNodes(nodes),
				ThinPools:     thinPools,
				VGSize:        size10G,
				VGUuid:        VGUUID,
			},
		}

		created, err := CreateLVMVolumeGroupByCandidate(ctx, log, testMetrics, cl, candidate, NodeName)
		if assert.NoError(t, err) {
			assert.Equal(t, &expected, created)
		}
	})

	t.Run("GetLVMVolumeGroup", func(t *testing.T) {
		const (
			LVMVGName = "test_lvm-1"
		)

		lvg := &v1alpha1.LVMVolumeGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name: LVMVGName,
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

		actual, err := GetAPILVMVolumeGroups(ctx, cl, monitoring.GetMetrics("test-node"))
		if assert.NoError(t, err) {
			_, ok := actual[LVMVGName]
			assert.True(t, ok)
		}
	})

	t.Run("DeleteLVMVolumeGroup", func(t *testing.T) {
		const (
			LVMVGName = "test_lvm-2"
		)

		metrics := monitoring.GetMetrics("test-node")

		lvg := &v1alpha1.LVMVolumeGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name: LVMVGName,
			},
		}
		err := cl.Create(ctx, lvg)
		if err != nil {
			t.Error(err)
		}

		actual, err := GetAPILVMVolumeGroups(ctx, cl, metrics)
		if assert.NoError(t, err) {
			_, ok := actual[LVMVGName]
			assert.True(t, ok)
		}

		err = DeleteLVMVolumeGroup(ctx, cl, log, metrics, lvg, "test-node")
		if assert.NoError(t, err) {
			actual, err = GetAPILVMVolumeGroups(ctx, cl, metrics)
			_, ok := actual[LVMVGName]
			assert.False(t, ok)
		}
	})

	t.Run("UpdateLVMVolumeGroup", func(t *testing.T) {
		const (
			LVMVGName = "test_lvm_x"
		)

		metrics := monitoring.GetMetrics("test-node")

		lvg := &v1alpha1.LVMVolumeGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name: LVMVGName,
			},
		}
		err := cl.Create(ctx, lvg)
		if err != nil {
			t.Error(err)
		}

		actual, err := GetAPILVMVolumeGroups(ctx, cl, metrics)
		if assert.NoError(t, err) {
			createdLvg, ok := actual[LVMVGName]
			assert.True(t, ok)

			candidate := internal.LVMVolumeGroupCandidate{
				LVMVGName:     LVMVGName,
				AllocatedSize: *resource.NewQuantity(1000, resource.BinarySI),
			}
			err = UpdateLVMVolumeGroupByCandidate(ctx, cl, metrics, log, &createdLvg, candidate)
			if assert.NoError(t, err) {
				updated, err := GetAPILVMVolumeGroups(ctx, cl, metrics)
				if assert.NoError(t, err) {
					updatedLvg, ok := updated[LVMVGName]
					assert.True(t, ok)

					assert.Equal(t, candidate.AllocatedSize.Value(), updatedLvg.Status.AllocatedSize.Value())
				}
			}
		}
	})

	t.Run("filterResourcesByNode_returns_current_node_resources", func(t *testing.T) {
		var (
			currentNode  = "test_node"
			vgName       = "test_vg"
			firstLVName  = "first_lv"
			secondLVName = "second_lv"

			lvs = map[string]v1alpha1.LVMVolumeGroup{
				firstLVName: {
					ObjectMeta: metav1.ObjectMeta{Name: firstLVName},
					Spec: v1alpha1.LVMVolumeGroupSpec{
						Type:                  Local,
						ActualVGNameOnTheNode: vgName,
						Local: v1alpha1.LVMVolumeGroupLocalSpec{
							NodeName: "other-node",
						},
					},
				},
				secondLVName: {
					ObjectMeta: metav1.ObjectMeta{Name: secondLVName},
					Spec: v1alpha1.LVMVolumeGroupSpec{
						Type:                  Local,
						ActualVGNameOnTheNode: vgName,
						Local: v1alpha1.LVMVolumeGroupLocalSpec{
							NodeName: currentNode,
						},
					},
				},
			}
		)

		expected := map[string]v1alpha1.LVMVolumeGroup{
			vgName: {
				ObjectMeta: metav1.ObjectMeta{Name: secondLVName},
				Spec: v1alpha1.LVMVolumeGroupSpec{
					Type:                  Local,
					ActualVGNameOnTheNode: vgName,
					Local: v1alpha1.LVMVolumeGroupLocalSpec{
						NodeName: currentNode,
					},
				},
			},
		}

		actual := filterLVGsByNode(lvs, currentNode)

		assert.Equal(t, expected, actual)
	})

	t.Run("filterResourcesByNode_returns_no_resources_for_current_node", func(t *testing.T) {
		var (
			currentNode  = "test_node"
			anotherNode  = "another_node"
			firstLVName  = "first_lv"
			secondLVName = "second_lv"

			lvs = map[string]v1alpha1.LVMVolumeGroup{
				firstLVName: {
					ObjectMeta: metav1.ObjectMeta{Name: firstLVName},
					Spec: v1alpha1.LVMVolumeGroupSpec{
						Type: Local,
						Local: v1alpha1.LVMVolumeGroupLocalSpec{
							NodeName: anotherNode,
						},
					},
				},
				secondLVName: {
					ObjectMeta: metav1.ObjectMeta{Name: secondLVName},
					Spec: v1alpha1.LVMVolumeGroupSpec{
						Type: Local,
						Local: v1alpha1.LVMVolumeGroupLocalSpec{
							NodeName: anotherNode,
						},
					},
				},
			}
		)

		actual := filterLVGsByNode(lvs, currentNode)

		assert.Equal(t, 0, len(actual))
	})

	t.Run("hasLVMVolumeGroupDiff", func(t *testing.T) {
		t.Run("should_return_false", func(t *testing.T) {
			size10G, err := resource.ParseQuantity("10G")
			if err != nil {
				t.Error(err)
			}
			size1G, err := resource.ParseQuantity("1G")
			if err != nil {
				t.Error(err)
			}
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
							PVUUID:      "testUUID",
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

			thinPools, err := convertStatusThinPools(v1alpha1.LVMVolumeGroup{}, statusThinPools)
			if err != nil {
				t.Error(err)
			}
			lvmVolumeGroup := v1alpha1.LVMVolumeGroup{
				Spec: v1alpha1.LVMVolumeGroupSpec{
					ThinPools: convertSpecThinPools(specThinPools),
					Type:      specType,
				},
				Status: v1alpha1.LVMVolumeGroupStatus{
					AllocatedSize: resource.MustParse("9765625Ki"),
					Nodes:         convertLVMVGNodes(nodes),
					ThinPools:     thinPools,
					VGSize:        resource.MustParse("9765625Ki"),
				},
			}

			assert.False(t, hasLVMVolumeGroupDiff(testLogger, lvmVolumeGroup, candidate))
		})

		t.Run("should_return_true", func(t *testing.T) {
			size10G := resource.MustParse("10G")
			size1G := resource.MustParse("1G")
			size13G := resource.MustParse("13G")
			vgFree := resource.MustParse("5G")

			var (
				allocatedSize   = resource.MustParse("10G")
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
							PVUUID:      "testUUID",
							BlockDevice: "something",
						},
						{
							Path:        "/test/ds2",
							PVSize:      size1G,
							DevSize:     size13G,
							PVUUID:      "testUUID2",
							BlockDevice: "something2",
						},
					},
				}
			)
			candidate := internal.LVMVolumeGroupCandidate{
				AllocatedSize:   size10G,
				StatusThinPools: statusThinPools,
				VGSize:          size10G,
				VGFree:          vgFree,
				Nodes:           nodes,
			}

			thinPools, err := convertStatusThinPools(v1alpha1.LVMVolumeGroup{}, statusThinPools)
			if err != nil {
				t.Error(err)
			}

			lvmVolumeGroup := v1alpha1.LVMVolumeGroup{
				Status: v1alpha1.LVMVolumeGroupStatus{
					AllocatedSize: allocatedSize,
					Nodes:         convertLVMVGNodes(nodes),
					ThinPools:     thinPools,
					VGSize:        vgSize,
					VGFree:        *resource.NewQuantity(vgFree.Value()+10000, resource.BinarySI),
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
		lvg := &v1alpha1.LVMVolumeGroup{
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
	builder := fake.NewClientBuilder().WithScheme(s).WithStatusSubresource(&v1alpha1.LVMVolumeGroup{}).WithStatusSubresource(&v1alpha1.LVMLogicalVolume{})

	cl := builder.Build()
	return cl
}
