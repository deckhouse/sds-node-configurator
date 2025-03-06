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

package bd

import (
	"bytes"
	"context"
	_ "embed"
	"fmt"
	"strconv"
	"testing"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/cache"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/logger"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/monitoring"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/test_utils"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/utils"
	"github.com/stretchr/testify/assert"
	errors2 "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

//go:embed testdata/lsblk_output.json
var testLsblkOutput string

func setupDiscoverer() *Discoverer {
	opts := DiscovererConfig{
		NodeName:  "test-node",
		MachineID: "test-id",
	}
	cl := test_utils.NewFakeClient()
	metrics := monitoring.GetMetrics(opts.NodeName)
	log, _ := logger.NewLogger("1")
	sdsCache := cache.New()

	return NewDiscoverer(cl, log, metrics, sdsCache, opts)
}

func TestBlockDeviceCtrl(t *testing.T) {
	ctx := context.Background()

	t.Run("GetAPIBlockDevices", func(t *testing.T) {
		t.Run("bds_exist_match_labels_and_expressions_return_bds", func(t *testing.T) {
			const (
				name1    = "name1"
				name2    = "name2"
				name3    = "name3"
				hostName = "test-host"
			)

			d := setupDiscoverer()

			bds := []v1alpha1.BlockDevice{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: name1,
						Labels: map[string]string{
							"kubernetes.io/hostname":      hostName,
							"kubernetes.io/metadata.name": name1,
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: name2,
						Labels: map[string]string{
							"kubernetes.io/hostname":      hostName,
							"kubernetes.io/metadata.name": name2,
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: name3,
						Labels: map[string]string{
							"kubernetes.io/hostname":      hostName,
							"kubernetes.io/metadata.name": name3,
						},
					},
				},
			}

			for _, bd := range bds {
				err := d.cl.Create(ctx, &bd)
				if err != nil {
					t.Error(err)
				}
			}

			defer func() {
				for _, bd := range bds {
					err := d.cl.Delete(ctx, &bd)
					if err != nil {
						t.Error(err)
					}
				}
			}()

			lvg := &v1alpha1.LVMVolumeGroup{
				Spec: v1alpha1.LVMVolumeGroupSpec{
					BlockDeviceSelector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"kubernetes.io/hostname": hostName,
						},
						MatchExpressions: []metav1.LabelSelectorRequirement{
							{
								Key:      "kubernetes.io/metadata.name",
								Operator: metav1.LabelSelectorOpIn,
								Values:   []string{name1, name2},
							},
						},
					},
				},
			}

			actualBd, err := d.bdCl.GetAPIBlockDevices(ctx, DiscovererName, lvg.Spec.BlockDeviceSelector)
			if assert.NoError(t, err) {
				assert.Equal(t, 2, len(actualBd))

				_, ok := actualBd[name1]
				assert.True(t, ok)
				_, ok = actualBd[name2]
				assert.True(t, ok)
				_, ok = actualBd[name3]
				assert.False(t, ok)
			}
		})

		t.Run("bds_exist_only_match_labels_return_bds", func(t *testing.T) {
			const (
				name1    = "name11"
				name2    = "name22"
				name3    = "name33"
				hostName = "test-host"
			)

			d := setupDiscoverer()

			bds := []v1alpha1.BlockDevice{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: name1,
						Labels: map[string]string{
							"kubernetes.io/hostname":      hostName,
							"kubernetes.io/metadata.name": name1,
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: name2,
						Labels: map[string]string{
							"kubernetes.io/hostname":      hostName,
							"kubernetes.io/metadata.name": name2,
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: name3,
						Labels: map[string]string{
							"kubernetes.io/hostname":      "other-host",
							"kubernetes.io/metadata.name": name3,
						},
					},
				},
			}

			for _, bd := range bds {
				err := d.cl.Create(ctx, &bd)
				if err != nil {
					t.Error(err)
				}
			}

			defer func() {
				for _, bd := range bds {
					err := d.cl.Delete(ctx, &bd)
					if err != nil {
						t.Error(err)
					}
				}
			}()

			lvg := &v1alpha1.LVMVolumeGroup{
				Spec: v1alpha1.LVMVolumeGroupSpec{
					BlockDeviceSelector: &metav1.LabelSelector{
						MatchLabels: map[string]string{"kubernetes.io/hostname": hostName},
					},
				},
			}

			actualBd, err := d.bdCl.GetAPIBlockDevices(ctx, DiscovererName, lvg.Spec.BlockDeviceSelector)
			if assert.NoError(t, err) {
				assert.Equal(t, 2, len(actualBd))

				_, ok := actualBd[name1]
				assert.True(t, ok)
				_, ok = actualBd[name2]
				assert.True(t, ok)
				_, ok = actualBd[name3]
				assert.False(t, ok)
			}
		})

		t.Run("bds_exist_only_match_expressions_return_bds", func(t *testing.T) {
			const (
				name1    = "name111"
				name2    = "name222"
				name3    = "name333"
				hostName = "test-host"
			)

			d := setupDiscoverer()

			bds := []v1alpha1.BlockDevice{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: name1,
						Labels: map[string]string{
							"kubernetes.io/hostname":      hostName,
							"kubernetes.io/metadata.name": name1,
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: name2,
						Labels: map[string]string{
							"kubernetes.io/hostname":      hostName,
							"kubernetes.io/metadata.name": name2,
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: name3,
						Labels: map[string]string{
							"kubernetes.io/hostname":      hostName,
							"kubernetes.io/metadata.name": name3,
						},
					},
				},
			}

			for _, bd := range bds {
				err := d.cl.Create(ctx, &bd)
				if err != nil {
					t.Error(err)
				}
			}

			defer func() {
				for _, bd := range bds {
					err := d.cl.Delete(ctx, &bd)
					if err != nil {
						t.Error(err)
					}
				}
			}()

			lvg := &v1alpha1.LVMVolumeGroup{
				Spec: v1alpha1.LVMVolumeGroupSpec{
					BlockDeviceSelector: &metav1.LabelSelector{
						MatchExpressions: []metav1.LabelSelectorRequirement{
							{
								Key:      "kubernetes.io/metadata.name",
								Operator: metav1.LabelSelectorOpIn,
								Values:   []string{name1, name2},
							},
						},
					},
				},
			}

			actualBd, err := d.bdCl.GetAPIBlockDevices(ctx, DiscovererName, lvg.Spec.BlockDeviceSelector)
			if assert.NoError(t, err) {
				assert.Equal(t, 2, len(actualBd))
				_, ok := actualBd[name1]
				assert.True(t, ok)
				_, ok = actualBd[name2]
				assert.True(t, ok)
				_, ok = actualBd[name3]
				assert.False(t, ok)
			}
		})
	})

	t.Run("shouldDeleteBlockDevice", func(t *testing.T) {
		t.Run("returns_true", func(t *testing.T) {
			bd := v1alpha1.BlockDevice{
				Status: v1alpha1.BlockDeviceStatus{
					NodeName:   "node",
					Consumable: true,
				},
			}
			actual := map[string]struct{}{}

			assert.True(t, shouldDeleteBlockDevice(bd, actual, "node"))
		})

		t.Run("returns_false_cause_of_dif_node", func(t *testing.T) {
			bd := v1alpha1.BlockDevice{
				Status: v1alpha1.BlockDeviceStatus{
					NodeName:   "node",
					Consumable: true,
				},
			}
			actual := map[string]struct{}{}

			assert.False(t, shouldDeleteBlockDevice(bd, actual, "dif-node"))
		})

		t.Run("returns_false_cause_of_not_consumable", func(t *testing.T) {
			bd := v1alpha1.BlockDevice{
				Status: v1alpha1.BlockDeviceStatus{
					NodeName:   "node",
					Consumable: false,
				},
			}
			actual := map[string]struct{}{}

			assert.False(t, shouldDeleteBlockDevice(bd, actual, "node"))
		})

		t.Run("returns_false_cause_of_not_deprecated", func(t *testing.T) {
			const name = "test"
			bd := v1alpha1.BlockDevice{
				ObjectMeta: metav1.ObjectMeta{
					Name: name,
				},
				Status: v1alpha1.BlockDeviceStatus{
					NodeName:   "node",
					Consumable: true,
				},
			}
			actual := map[string]struct{}{
				name: {},
			}

			assert.False(t, shouldDeleteBlockDevice(bd, actual, "node"))
		})
	})

	t.Run("RemoveDeprecatedAPIDevices", func(t *testing.T) {
		const (
			goodName = "test-candidate1"
			badName  = "test-candidate2"
		)

		d := setupDiscoverer()

		candidates := []internal.BlockDeviceCandidate{
			{
				NodeName:              d.cfg.NodeName,
				Consumable:            false,
				PVUuid:                "142412421",
				VGUuid:                "123123123",
				LVMVolumeGroupName:    "test-lvg",
				ActualVGNameOnTheNode: "test-vg",
				Wwn:                   "12414212",
				Serial:                "1412412412412",
				Path:                  "/dev/vdb",
				Size:                  resource.MustParse("1G"),
				Rota:                  false,
				Model:                 "124124-adf",
				Name:                  goodName,
				HotPlug:               false,
				MachineID:             "1245151241241",
			},
		}

		bds := map[string]v1alpha1.BlockDevice{
			goodName: {
				ObjectMeta: metav1.ObjectMeta{
					Name: goodName,
				},
			},
			badName: {
				ObjectMeta: metav1.ObjectMeta{
					Name: badName,
				},
				Status: v1alpha1.BlockDeviceStatus{
					Consumable: true,
					NodeName:   d.cfg.NodeName,
				},
			},
		}

		for _, bd := range bds {
			err := d.cl.Create(ctx, &bd)
			if err != nil {
				t.Error(err)
			}
		}

		defer func() {
			for _, bd := range bds {
				_ = d.cl.Delete(ctx, &bd)
			}
		}()

		for _, bd := range bds {
			createdBd := &v1alpha1.BlockDevice{}
			err := d.cl.Get(ctx, client.ObjectKey{
				Name: bd.Name,
			}, createdBd)
			if err != nil {
				t.Error(err)
			}
			assert.Equal(t, bd.Name, createdBd.Name)
		}

		d.removeDeprecatedAPIDevices(ctx, candidates, bds)

		_, ok := bds[badName]
		assert.False(t, ok)

		deleted := &v1alpha1.BlockDevice{}
		err := d.cl.Get(ctx, client.ObjectKey{
			Name: badName,
		}, deleted)
		if assert.True(t, errors2.IsNotFound(err)) {
			assert.Equal(t, "", deleted.Name)
		}
	})

	t.Run("GetBlockDeviceCandidates", func(t *testing.T) {
		devices := []internal.Device{
			{
				Name:   "valid1",
				Size:   resource.MustParse("1G"),
				Serial: "131412",
			},
			{
				Name:   "valid2",
				Size:   resource.MustParse("1G"),
				Serial: "12412412",
			},
			{
				Name:   "valid3",
				Size:   resource.MustParse("1G"),
				Serial: "4214215",
			},
			{
				Name:   "invalid",
				FSType: "ext4",
				Size:   resource.MustParse("1G"),
			},
		}

		d := setupDiscoverer()

		d.sdsCache.StoreDevices(devices, bytes.Buffer{})

		candidates := d.getBlockDeviceCandidates()

		assert.Equal(t, 3, len(candidates))
		for i := range candidates {
			assert.Equal(t, devices[i].Name, candidates[i].Path)
			assert.Equal(t, d.cfg.MachineID, candidates[i].MachineID)
			assert.Equal(t, d.cfg.NodeName, candidates[i].NodeName)
		}
	})

	t.Run("CheckConsumable", func(t *testing.T) {
		t.Run("Good device returns true", func(t *testing.T) {
			goodDevice := internal.Device{
				Name:       "goodName",
				MountPoint: "",
				PartUUID:   "",
				HotPlug:    false,
				Model:      "",
				Serial:     "",
				Size:       resource.Quantity{},
				Type:       "",
				Wwn:        "",
				KName:      "",
				PkName:     "",
				FSType:     "",
				Rota:       false,
			}

			shouldBeTrue := checkConsumable(goodDevice)
			assert.True(t, shouldBeTrue)
		})

		t.Run("Bad devices return false", func(t *testing.T) {
			badDevices := internal.Devices{BlockDevices: []internal.Device{
				{
					MountPoint: "",
					HotPlug:    true,
					FSType:     "",
				},
				{
					MountPoint: "bad",
					HotPlug:    false,
					FSType:     "",
				},
				{
					MountPoint: "",
					HotPlug:    false,
					FSType:     "bad",
				},
			}}

			for _, badDevice := range badDevices.BlockDevices {
				shouldBeFalse := checkConsumable(badDevice)
				assert.False(t, shouldBeFalse)
			}
		})
	})

	t.Run("CreateUniqDeviceName", func(t *testing.T) {
		nodeName := "testNode"
		can := internal.BlockDeviceCandidate{
			NodeName: nodeName,
			Wwn:      "ZX128ZX128ZX128",
			Path:     "/dev/sda",
			Size:     resource.Quantity{},
			Model:    "HARD-DRIVE",
		}

		deviceName := createUniqDeviceName(can)
		assert.Equal(t, "dev-", deviceName[0:4], "device name does not start with dev-")
		assert.Equal(t, len(deviceName[4:]), 40, "device name does not contains sha1 sum")
	})

	t.Run("CheckTag", func(t *testing.T) {
		t.Run("Have tag_Returns true and tag", func(t *testing.T) {
			expectedName := "testName"
			tags := fmt.Sprintf("storage.deckhouse.io/enabled=true,storage.deckhouse.io/lvmVolumeGroupName=%s", expectedName)

			shouldBeTrue, actualName := utils.ReadValueFromTags(tags, internal.LVMVolumeGroupTag)
			if assert.True(t, shouldBeTrue) {
				assert.Equal(t, expectedName, actualName)
			}
		})

		t.Run("Haven't tag_Returns false and empty", func(t *testing.T) {
			tags := "someWeirdTags=oMGwtFIsThis"

			shouldBeFalse, actualName := utils.ReadValueFromTags(tags, internal.LVMVolumeGroupTag)
			if assert.False(t, shouldBeFalse) {
				assert.Equal(t, "", actualName)
			}
		})
	})

	t.Run("hasValidSize", func(t *testing.T) {
		sizes := []string{"2G", "1G", "1.5G", "0.9G", "100M"}
		expected := []bool{true, true, true, false, false}

		for i, size := range sizes {
			s, err := resource.ParseQuantity(size)
			if assert.NoError(t, err) {
				valid, err := hasValidSize(s)
				if assert.NoError(t, err) {
					assert.Equal(t, expected[i], valid)
				}
			}
		}
	})

	t.Run("ConfigureBlockDeviceLabels", func(t *testing.T) {
		blockDevice := v1alpha1.BlockDevice{
			Status: v1alpha1.BlockDeviceStatus{
				Type:                  "testTYPE",
				FsType:                "testFS",
				NodeName:              "test_node",
				Consumable:            false,
				PVUuid:                "testPV",
				VGUuid:                "testVGUID",
				LVMVolumeGroupName:    "testLVGName",
				ActualVGNameOnTheNode: "testNameOnNode",
				Wwn:                   "testWWN",
				Serial:                "testSERIAL",
				Path:                  "testPATH",
				Size:                  resource.MustParse("0"),
				Model:                 "Very good model-1241",
				Rota:                  false,
				HotPlug:               false,
				MachineID:             "testMACHINE",
			},
		}
		blockDevice.Labels = map[string]string{
			"some-custom-label1": "v",
			"some-custom-label2": "v",
		}

		expectedLabels := map[string]string{
			internal.MetadataNameLabelKey:                  blockDevice.ObjectMeta.Name,
			internal.HostNameLabelKey:                      blockDevice.Status.NodeName,
			internal.BlockDeviceTypeLabelKey:               blockDevice.Status.Type,
			internal.BlockDeviceFSTypeLabelKey:             blockDevice.Status.FsType,
			internal.BlockDevicePVUUIDLabelKey:             blockDevice.Status.PVUuid,
			internal.BlockDeviceVGUUIDLabelKey:             blockDevice.Status.VGUuid,
			internal.BlockDevicePartUUIDLabelKey:           blockDevice.Status.PartUUID,
			internal.BlockDeviceLVMVolumeGroupNameLabelKey: blockDevice.Status.LVMVolumeGroupName,
			internal.BlockDeviceActualVGNameLabelKey:       blockDevice.Status.ActualVGNameOnTheNode,
			internal.BlockDeviceWWNLabelKey:                blockDevice.Status.Wwn,
			internal.BlockDeviceSerialLabelKey:             blockDevice.Status.Serial,
			internal.BlockDeviceSizeLabelKey:               blockDevice.Status.Size.String(),
			internal.BlockDeviceModelLabelKey:              "Very-good-model-1241",
			internal.BlockDeviceRotaLabelKey:               strconv.FormatBool(blockDevice.Status.Rota),
			internal.BlockDeviceHotPlugLabelKey:            strconv.FormatBool(blockDevice.Status.HotPlug),
			internal.BlockDeviceMachineIDLabelKey:          blockDevice.Status.MachineID,
			"some-custom-label1":                           "v",
			"some-custom-label2":                           "v",
		}

		assert.Equal(t, expectedLabels, configureBlockDeviceLabels(blockDevice))
	})

	t.Run("hasBlockDeviceDiff", func(t *testing.T) {
		candidates := []internal.BlockDeviceCandidate{
			// same state
			{
				NodeName:              "test_node",
				Consumable:            false,
				PVUuid:                "testPV",
				VGUuid:                "testVGUID",
				LVMVolumeGroupName:    "testLVGName",
				ActualVGNameOnTheNode: "testNameOnNode",
				Wwn:                   "testWWN",
				Serial:                "testSERIAL",
				Path:                  "testPATH",
				Size:                  resource.Quantity{},
				Rota:                  false,
				Model:                 "testMODEL",
				Name:                  "testNAME",
				HotPlug:               false,
				KName:                 "testKNAME",
				PkName:                "testPKNAME",
				Type:                  "testTYPE",
				FSType:                "testFS",
				MachineID:             "testMACHINE",
			},
			// diff state
			{
				NodeName:              "test_node",
				Consumable:            true,
				PVUuid:                "testPV2",
				VGUuid:                "testVGUID2",
				LVMVolumeGroupName:    "testLVGName2",
				ActualVGNameOnTheNode: "testNameOnNode2",
				Wwn:                   "testWWN2",
				Serial:                "testSERIAL2",
				Path:                  "testPATH2",
				Size:                  resource.Quantity{},
				Rota:                  true,
				Model:                 "testMODEL2",
				Name:                  "testNAME",
				HotPlug:               true,
				KName:                 "testKNAME2",
				PkName:                "testPKNAME2",
				Type:                  "testTYPE2",
				FSType:                "testFS2",
				MachineID:             "testMACHINE2",
			},
		}
		blockDevice := v1alpha1.BlockDevice{
			Status: v1alpha1.BlockDeviceStatus{
				Type:                  "testTYPE",
				FsType:                "testFS",
				NodeName:              "test_node",
				Consumable:            false,
				PVUuid:                "testPV",
				VGUuid:                "testVGUID",
				LVMVolumeGroupName:    "testLVGName",
				ActualVGNameOnTheNode: "testNameOnNode",
				Wwn:                   "testWWN",
				Serial:                "testSERIAL",
				Path:                  "testPATH",
				Size:                  resource.MustParse("0"),
				Model:                 "testMODEL",
				Rota:                  false,
				HotPlug:               false,
				MachineID:             "testMACHINE",
			},
		}
		labels := configureBlockDeviceLabels(blockDevice)
		blockDevice.Labels = labels
		expected := []bool{false, true}

		for i, candidate := range candidates {
			actual := hasBlockDeviceDiff(blockDevice, candidate)
			assert.Equal(t, expected[i], actual)
		}
	})

	t.Run("validateTestLSBLKOutput", func(t *testing.T) {
		d := setupDiscoverer()
		testLsblkOutputBytes := []byte(testLsblkOutput)
		devices, err := utils.UnmarshalDevices(testLsblkOutputBytes)
		if assert.NoError(t, err) {
			assert.Equal(t, 31, len(devices))
		}
		filteredDevices, err := d.filterDevices(devices)

		for i, device := range filteredDevices {
			println("Filtered device: ", device.Name)
			candidate := internal.BlockDeviceCandidate{
				NodeName:   "test-node",
				Consumable: checkConsumable(device),
				Wwn:        device.Wwn,
				Serial:     device.Serial,
				Path:       device.Name,
				Size:       device.Size,
				Rota:       device.Rota,
				Model:      device.Model,
				HotPlug:    device.HotPlug,
				KName:      device.KName,
				PkName:     device.PkName,
				Type:       device.Type,
				FSType:     device.FSType,
				PartUUID:   device.PartUUID,
			}
			switch i {
			case 0:
				assert.Equal(t, "/dev/md1", device.Name)
				assert.False(t, candidate.Consumable)
			case 1:
				assert.Equal(t, "/dev/md127", device.Name)
				assert.False(t, candidate.Consumable)
			case 2:
				assert.Equal(t, "/dev/nvme4n1", device.Name)
				assert.True(t, candidate.Consumable)
				candidateName := d.createCandidateName(candidate, devices)
				assert.Equal(t, "dev-794d93d177d16bc9a85e2dd2ccbdc7325c287374", candidateName, "device name generated incorrectly")
			case 3:
				assert.Equal(t, "/dev/nvme5n1", device.Name)
				assert.True(t, candidate.Consumable)
				candidateName := d.createCandidateName(candidate, devices)
				assert.Equal(t, "dev-3306e773ab3cde6d519ce8d7c3686bf17a124dcb", candidateName, "device name generated incorrectly")
			case 4:
				assert.Equal(t, "/dev/sda4", device.Name)
				assert.False(t, candidate.Consumable)
				candidateName := d.createCandidateName(candidate, devices)
				assert.Equal(t, "dev-377bc6adf33d84eb5932f5c89798bb6c5949ae2d", candidateName, "device name generated incorrectly")
			case 5:
				assert.Equal(t, "/dev/vdc1", device.Name)
				assert.True(t, candidate.Consumable)
				candidateName := d.createCandidateName(candidate, devices)
				assert.Equal(t, "dev-a9d768213aaead8b42465ec859189de8779f96b7", candidateName, "device name generated incorrectly")
			case 6:
				assert.Equal(t, "/dev/mapper/mpatha", device.Name)
				assert.True(t, candidate.Consumable)
				candidateName := d.createCandidateName(candidate, devices)
				assert.Equal(t, "dev-98ca88ddaaddec43b1c4894756f4856244985511", candidateName, "device name generated incorrectly")
			}
		}

		if assert.NoError(t, err) {
			assert.Equal(t, 7, len(filteredDevices))
		}
	})
}
