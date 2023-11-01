package controller

import (
	"context"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"reflect"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"storage-configurator/api/v1alpha1"
	"storage-configurator/internal"
	"testing"
)

func TestLVMVolumeGroupDiscover(t *testing.T) {
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
		lvIssues := map[string][]string{}
		vg := internal.VGData{VGName: vgName, VGUuid: vgUuid}
		var err error = nil

		health, _ := checkVGHealth(err, bds, vgIssues, pvIssues, lvIssues, vg)
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
		lvIssues := map[string][]string{}
		vg := internal.VGData{VGName: vgName, VGUuid: vgUuid}
		var err error = nil

		health, _ := checkVGHealth(err, bds, vgIssues, pvIssues, lvIssues, vg)
		assert.Equal(t, health, internal.LVMVGHealthNonOperational)
	})

	t.Run("getUsedSizeMiB_returns_usedSize_in_M", func(t *testing.T) {
		lv := internal.LVData{
			LVSize:      "2048K",
			DataPercent: "50",
		}
		expected := "1M"
		actual, err := getUsedSizeMiB(lv)

		if assert.NoError(t, err) {
			assert.Equal(t, expected, actual)
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

		actual := sortLVsByVG(lvs, vgs)
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

		pvs := []internal.PVData{
			{
				PVName: "test_pv1",
				PVSize: "pv_size1",
				VGName: vgName,
				VGUuid: vgUuid,
			},
			{
				PVName: "test_pv2",
				PVSize: "pv_size2",
				VGUuid: vgUuid,
				VGName: vgName,
			},
		}

		bds := []v1alpha1.BlockDevice{
			{
				ObjectMeta: metav1.ObjectMeta{Name: "block_device1"},
				Status: v1alpha1.BlockDeviceStatus{
					Path:                  "test_pv1",
					Size:                  "dev_size1",
					PVUuid:                "pv_uuid1",
					NodeName:              nodeName,
					VGUuid:                vgUuid,
					ActualVGNameOnTheNode: vgName,
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{Name: "block_device2"},
				Status: v1alpha1.BlockDeviceStatus{
					Path:                  "test_pv2",
					Size:                  "dev_size2",
					PVUuid:                "pv_uuid2",
					NodeName:              nodeName,
					VGUuid:                vgUuid,
					ActualVGNameOnTheNode: vgName,
				},
			},
		}

		expected := map[string][]internal.LVMVGDevice{
			nodeName: {
				{
					Path:        "test_pv1",
					PVSize:      "pv_size1",
					DevSize:     "dev_size1",
					PVUuid:      "pv_uuid1",
					BlockDevice: "block_device1",
				},
				{
					Path:        "test_pv2",
					PVSize:      "pv_size2",
					DevSize:     "dev_size2",
					PVUuid:      "pv_uuid2",
					BlockDevice: "block_device2",
				},
			},
		}
		mp := map[string][]v1alpha1.BlockDevice{vgName + vgUuid: bds}
		ar := map[string][]internal.PVData{vgName + vgUuid: pvs}

		actual := configureCandidateNodeDevices(ar, mp, vg)

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

	t.Run("getAllocatedSizeMiB_returns_allocatedSize", func(t *testing.T) {
		vg := internal.VGData{
			VGFree: "1024.00K",
			VGSize: "2048.00K",
		}
		expected := "1M"

		actual, err := getAllocatedSizeMiB(vg)

		if assert.NoError(t, err) {
			assert.Equal(t, expected, actual)
		}
	})

	t.Run("getVgType_returns_shared", func(t *testing.T) {
		vg := internal.VGData{VGShared: "shared"}
		expected := "Shared"

		acutal := getVgType(vg)

		assert.Equal(t, expected, acutal)
	})

	t.Run("getVgType_returns_local", func(t *testing.T) {
		vg := internal.VGData{VGShared: ""}
		expected := "Local"

		acutal := getVgType(vg)

		assert.Equal(t, expected, acutal)
	})

	t.Run("getSpecThinPools_returns_LVName_LVSize_map", func(t *testing.T) {
		const (
			vgName = "test_vg"
			vgUuid = "test_uuid"
		)

		vg := internal.VGData{VGName: vgName, VGUuid: vgUuid}

		thinPools := map[string][]internal.LVData{
			vgName + vgUuid: {
				{
					LVName: "first",
					LVSize: "first_size",
				},
				{
					LVName: "second",
					LVSize: "second_size",
				},
			},
		}

		expected := map[string]string{
			"first":  "first_size",
			"second": "second_size",
		}

		actual := getSpecThinPools(thinPools, vg)

		assert.Equal(t, expected, actual)
	})

	t.Run("CreateLVMVolumeGroup", func(t *testing.T) {
		const (
			LVMVGName             = "test_lvm"
			ActualVGNameOnTheNode = "test-vg"
			Type                  = "local"
			AllocatedSize         = "10G"
			Health                = internal.LVMVGHealthOperational
			Message               = "No problems detected"
			VGSize                = "10G"
			VGUuid                = "test_uuid"
		)

		var (
			cl                = NewFakeClient()
			ctx               = context.Background()
			blockDevicesNames = []string{"first", "second"}
			specThinPools     = map[string]string{"first": "first_size"}
			statusThinPools   = []internal.LVMVGStatusThinPool{
				{
					Name:       "first_status_pool",
					ActualSize: "10G",
					UsedSize:   "4G",
				},
			}
			nodes = map[string][]internal.LVMVGDevice{
				"test-node-1": {
					{
						Path:        "test/path",
						PVSize:      "1G",
						DevSize:     "1G",
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
			AllocatedSize:         AllocatedSize,
			Health:                Health,
			Message:               Message,
			StatusThinPools:       statusThinPools,
			VGSize:                VGSize,
			VGUuid:                VGUuid,
			Nodes:                 nodes,
		}

		expected := v1alpha1.LvmVolumeGroup{
			TypeMeta: metav1.TypeMeta{
				Kind:       v1alpha1.LVMVolumeGroupKind,
				APIVersion: v1alpha1.TypeMediaAPIVersion,
			},
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
				AllocatedSize: AllocatedSize,
				Health:        Health,
				Message:       Message,
				Nodes:         convertLVMVGNodes(nodes),
				ThinPools:     convertStatusThinPools(statusThinPools),
				VGSize:        VGSize,
				VGUuid:        VGUuid,
			},
		}

		created, err := CreateLVMVolumeGroup(ctx, cl, candidate)
		if assert.NoError(t, err) {
			assert.Equal(t, &expected, created)
		}
	})

	t.Run("GetLVMVolumeGroup", func(t *testing.T) {
		const (
			LVMVGName             = "test_lvm"
			ActualVGNameOnTheNode = "test-vg"
			Type                  = "local"
			AllocatedSize         = "10G"
			Health                = internal.LVMVGHealthOperational
			Message               = "No problems detected"
			VGSize                = "10G"
			VGUuid                = "test_uuid"
		)

		var (
			cl                = NewFakeClient()
			ctx               = context.Background()
			blockDevicesNames = []string{"first", "second"}
			specThinPools     = map[string]string{"first": "first_size"}
			statusThinPools   = []internal.LVMVGStatusThinPool{
				{
					Name:       "first_status_pool",
					ActualSize: "10G",
					UsedSize:   "4G",
				},
			}
			nodes = map[string][]internal.LVMVGDevice{
				"test-node-1": {
					{
						Path:        "test/path",
						PVSize:      "1G",
						DevSize:     "1G",
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
			AllocatedSize:         AllocatedSize,
			Health:                Health,
			Message:               Message,
			StatusThinPools:       statusThinPools,
			VGSize:                VGSize,
			VGUuid:                VGUuid,
			Nodes:                 nodes,
		}

		expected := map[string]v1alpha1.LvmVolumeGroup{
			LVMVGName: {
				TypeMeta: metav1.TypeMeta{
					Kind:       v1alpha1.LVMVolumeGroupKind,
					APIVersion: v1alpha1.TypeMediaAPIVersion,
				},
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
					AllocatedSize: AllocatedSize,
					Health:        Health,
					Message:       Message,
					Nodes:         convertLVMVGNodes(nodes),
					ThinPools:     convertStatusThinPools(statusThinPools),
					VGSize:        VGSize,
					VGUuid:        VGUuid,
				},
			},
		}

		created, err := CreateLVMVolumeGroup(ctx, cl, candidate)
		if assert.NoError(t, err) && assert.NotNil(t, created) {
			actual, err := GetAPILVMVolumeGroups(ctx, cl)
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
			AllocatedSize         = "10G"
			Health                = internal.LVMVGHealthOperational
			Message               = "No problems detected"
			VGSize                = "10G"
			VGUuid                = "test_uuid"
		)

		var (
			cl                = NewFakeClient()
			ctx               = context.Background()
			blockDevicesNames = []string{"first", "second"}
			specThinPools     = map[string]string{"first": "first_size"}
			statusThinPools   = []internal.LVMVGStatusThinPool{
				{
					Name:       "first_status_pool",
					ActualSize: "10G",
					UsedSize:   "4G",
				},
			}
			nodes = map[string][]internal.LVMVGDevice{
				"test-node-1": {
					{
						Path:        "test/path",
						PVSize:      "1G",
						DevSize:     "1G",
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
			AllocatedSize:         AllocatedSize,
			Health:                Health,
			Message:               Message,
			StatusThinPools:       statusThinPools,
			VGSize:                VGSize,
			VGUuid:                VGUuid,
			Nodes:                 nodes,
		}

		created, err := CreateLVMVolumeGroup(ctx, cl, candidate)
		if assert.NoError(t, err) && assert.NotNil(t, created) {
			actual, err := GetAPILVMVolumeGroups(ctx, cl)
			if assert.NoError(t, err) && assert.Equal(t, 1, len(actual)) {
				err := DeleteLVMVolumeGroup(ctx, cl, LVMVGName)
				if assert.NoError(t, err) {
					actual, err := GetAPILVMVolumeGroups(ctx, cl)
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
			AllocatedSize         = "10G"
			Health                = internal.LVMVGHealthOperational
			Message               = "No problems detected"
			VGSize                = "10G"
			VGUuid                = "test_uuid"
		)

		var (
			cl                = NewFakeClient()
			ctx               = context.Background()
			BlockDevicesNames = []string{"first", "second"}
			SpecThinPools     = map[string]string{"first": "first_size"}
			StatusThinPools   = []internal.LVMVGStatusThinPool{
				{
					Name:       "first_status_pool",
					ActualSize: "10G",
					UsedSize:   "4G",
				},
			}
			oldNodes = map[string][]internal.LVMVGDevice{
				"test-node-1": {
					{
						Path:        "test/path",
						PVSize:      "1G",
						DevSize:     "1G",
						PVUuid:      "test-pv-uuid",
						BlockDevice: "test-device",
					},
				},
			}
			newNodes = map[string][]internal.LVMVGDevice{
				"test-node-1": {
					{
						Path:        "test/path",
						PVSize:      "1G",
						DevSize:     "1G",
						PVUuid:      "test-pv-uuid",
						BlockDevice: "test-device",
					},
					{
						Path:        "test/path2",
						PVSize:      "1G",
						DevSize:     "1G",
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
			AllocatedSize:         AllocatedSize,
			Health:                Health,
			Message:               Message,
			StatusThinPools:       StatusThinPools,
			VGSize:                VGSize,
			VGUuid:                VGUuid,
			Nodes:                 oldNodes,
		}

		newCandidate := internal.LVMVolumeGroupCandidate{
			LVMVGName:             LVMVGName,
			ActualVGNameOnTheNode: ActualVGNameOnTheNode,
			BlockDevicesNames:     BlockDevicesNames,
			SpecThinPools:         SpecThinPools,
			Type:                  Type,
			AllocatedSize:         AllocatedSize,
			Health:                Health,
			Message:               Message,
			StatusThinPools:       StatusThinPools,
			VGSize:                VGSize,
			VGUuid:                VGUuid,
			Nodes:                 newNodes,
		}

		expected := v1alpha1.LvmVolumeGroup{
			TypeMeta: metav1.TypeMeta{
				Kind:       v1alpha1.LVMVolumeGroupKind,
				APIVersion: v1alpha1.TypeMediaAPIVersion,
			},
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
				AllocatedSize: AllocatedSize,
				Health:        Health,
				Message:       Message,
				Nodes:         convertLVMVGNodes(newNodes),
				ThinPools:     convertStatusThinPools(StatusThinPools),
				VGSize:        VGSize,
				VGUuid:        VGUuid,
			},
		}

		created, err := CreateLVMVolumeGroup(ctx, cl, oldCandidate)
		if assert.NoError(t, err) {
			err := UpdateLVMVolumeGroupByCandidate(ctx, cl, *created, newCandidate)

			if assert.NoError(t, err) {
				lmvs, err := GetAPILVMVolumeGroups(ctx, cl)
				if assert.NoError(t, err) {
					actual := lmvs[LVMVGName]
					assert.Equal(t, expected, actual)
				}
			}
		}
	})

	t.Run("deepEqual_specThinPools_returnsTrue", func(t *testing.T) {
		var (
			specThinPools = map[string]string{
				"first":  "first_size",
				"second": "second_size",
			}
		)
		candidate := internal.LVMVolumeGroupCandidate{
			SpecThinPools: specThinPools,
		}

		resource := v1alpha1.LvmVolumeGroup{
			Spec: v1alpha1.LvmVolumeGroupSpec{
				ThinPools: convertSpecThinPools(specThinPools),
			},
		}

		assert.True(t, reflect.DeepEqual(convertSpecThinPools(candidate.SpecThinPools), resource.Spec.ThinPools))
	})

	t.Run("deepEqual_specThinPools_returnsFalse", func(t *testing.T) {
		var (
			specThinPools = map[string]string{
				"first":  "first_size",
				"second": "second_size",
			}

			anotherSpecThinPools = map[string]string{
				"third":  "third_size",
				"second": "second_size",
			}
		)
		candidate := internal.LVMVolumeGroupCandidate{
			SpecThinPools: specThinPools,
		}

		resource := v1alpha1.LvmVolumeGroup{
			Spec: v1alpha1.LvmVolumeGroupSpec{
				ThinPools: convertSpecThinPools(anotherSpecThinPools),
			},
		}

		assert.False(t, reflect.DeepEqual(convertSpecThinPools(candidate.SpecThinPools), resource.Spec.ThinPools))
	})

	t.Run("hasLVMVolumeGroupDiff", func(t *testing.T) {
		t.Run("should_return_false", func(t *testing.T) {
			var (
				blockDevicesNames = []string{
					"first",
					"second",
				}
				specThinPools = map[string]string{
					"first":  "first_size",
					"second": "second_size",
				}
				specType        = "type"
				allocatedSize   = "10G"
				health          = internal.LVMVGHealthOperational
				message         = "all good"
				statusThinPools = []internal.LVMVGStatusThinPool{
					{
						Name:       "first",
						ActualSize: "10G",
						UsedSize:   "2G",
					},
					{
						Name:       "second",
						ActualSize: "10G",
						UsedSize:   "2G",
					},
				}
				vgSize = "10G"
				nodes  = map[string][]internal.LVMVGDevice{
					"test_node": {
						{
							Path:        "/test/ds",
							PVSize:      "1G",
							DevSize:     "13G",
							PVUuid:      "testUUID",
							BlockDevice: "something",
						},
						{
							Path:        "/test/ds2",
							PVSize:      "1G",
							DevSize:     "13G",
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
				AllocatedSize:     allocatedSize,
				Health:            health,
				Message:           message,
				StatusThinPools:   statusThinPools,
				VGSize:            vgSize,
				Nodes:             nodes,
			}

			resource := v1alpha1.LvmVolumeGroup{
				Spec: v1alpha1.LvmVolumeGroupSpec{
					BlockDeviceNames: blockDevicesNames,
					ThinPools:        convertSpecThinPools(specThinPools),
					Type:             specType,
				},
				Status: v1alpha1.LvmVolumeGroupStatus{
					AllocatedSize: allocatedSize,
					Health:        health,
					Message:       message,
					Nodes:         convertLVMVGNodes(nodes),
					ThinPools:     convertStatusThinPools(statusThinPools),
					VGSize:        vgSize,
				},
			}
			assert.False(t, hasLVMVolumeGroupDiff(resource, candidate))
		})

		t.Run("should_return_true", func(t *testing.T) {
			var (
				blockDevicesNames = []string{
					"first",
					"second",
				}
				specThinPools = map[string]string{
					"first":  "first_size",
					"second": "second_size",
				}
				specType        = "type"
				allocatedSize   = "10G"
				health          = internal.LVMVGHealthOperational
				message         = "all good"
				statusThinPools = []internal.LVMVGStatusThinPool{
					{
						Name:       "first",
						ActualSize: "10G",
						UsedSize:   "2G",
					},
					{
						Name:       "second",
						ActualSize: "10G",
						UsedSize:   "2G",
					},
				}
				vgSize = "10G"
				nodes  = map[string][]internal.LVMVGDevice{
					"test_node": {
						{
							Path:        "/test/ds",
							PVSize:      "1G",
							DevSize:     "13G",
							PVUuid:      "testUUID",
							BlockDevice: "something",
						},
						{
							Path:        "/test/ds2",
							PVSize:      "1G",
							DevSize:     "13G",
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
				AllocatedSize:     allocatedSize,
				Health:            health,
				Message:           "NewMessage",
				StatusThinPools:   statusThinPools,
				VGSize:            vgSize,
				Nodes:             nodes,
			}

			resource := v1alpha1.LvmVolumeGroup{
				Spec: v1alpha1.LvmVolumeGroupSpec{
					BlockDeviceNames: blockDevicesNames,
					ThinPools:        convertSpecThinPools(specThinPools),
					Type:             specType,
				},
				Status: v1alpha1.LvmVolumeGroupStatus{
					AllocatedSize: allocatedSize,
					Health:        health,
					Message:       message,
					Nodes:         convertLVMVGNodes(nodes),
					ThinPools:     convertStatusThinPools(statusThinPools),
					VGSize:        vgSize,
				},
			}

			assert.True(t, hasLVMVolumeGroupDiff(resource, candidate))
		})
	})
}

func NewFakeClient() client.WithWatch {
	s := scheme.Scheme
	_ = metav1.AddMetaToScheme(s)
	_ = v1alpha1.AddToScheme(s)

	builder := fake.NewClientBuilder().WithScheme(s)

	cl := builder.Build()
	return cl
}
