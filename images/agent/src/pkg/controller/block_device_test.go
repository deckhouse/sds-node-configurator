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
	"bytes"
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/stretchr/testify/assert"
	errors2 "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"agent/config"
	"agent/internal"
	"agent/pkg/cache"
	"agent/pkg/logger"
	"agent/pkg/monitoring"
	"agent/pkg/utils"
)

func TestBlockDeviceCtrl(t *testing.T) {
	ctx := context.Background()
	cl := NewFakeClient()
	metrics := monitoring.GetMetrics("")
	log, _ := logger.NewLogger("1")
	cfg := config.Options{
		NodeName:  "test-node",
		MachineID: "test-id",
	}

	t.Run("GetAPIBlockDevices", func(t *testing.T) {
		t.Run("bds_exist_match_labels_and_expressions_return_bds", func(t *testing.T) {
			const (
				name1    = "name1"
				name2    = "name2"
				name3    = "name3"
				hostName = "test-host"
			)

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
				err := cl.Create(ctx, &bd)
				if err != nil {
					t.Error(err)
				}
			}

			defer func() {
				for _, bd := range bds {
					err := cl.Delete(ctx, &bd)
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

			actualBd, err := GetAPIBlockDevices(ctx, cl, metrics, lvg.Spec.BlockDeviceSelector, lvg.Spec.Local.NodeName)
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
				name1         = "name11"
				name2         = "name22"
				name3         = "name33"
				name4         = "name44"
				hostName      = "test-host"
				otherHostName = "other-host"
			)

			bds := []v1alpha1.BlockDevice{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: name1,
						Labels: map[string]string{
							"kubernetes.io/hostname":                       hostName,
							"kubernetes.io/metadata.name":                  name1,
							"status.blockdevice.storage.deckhouse.io/size": "1G",
						},
					},
					Status: v1alpha1.BlockDeviceStatus{
						Size:       resource.MustParse("1G"),
						NodeName:   hostName,
						Consumable: true,
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: name2,
						Labels: map[string]string{
							"kubernetes.io/hostname":                       hostName,
							"kubernetes.io/metadata.name":                  name2,
							"status.blockdevice.storage.deckhouse.io/size": "1G",
						},
					},
					Status: v1alpha1.BlockDeviceStatus{
						Size:       resource.MustParse("1G"),
						NodeName:   hostName,
						Consumable: true,
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: name3,
						Labels: map[string]string{
							"kubernetes.io/hostname":                       hostName,
							"kubernetes.io/metadata.name":                  name3,
							"status.blockdevice.storage.deckhouse.io/size": "2G",
						},
					},
					Status: v1alpha1.BlockDeviceStatus{
						Size:       resource.MustParse("1G"),
						NodeName:   hostName,
						Consumable: true,
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: name4,
						Labels: map[string]string{
							"kubernetes.io/hostname":                       otherHostName,
							"kubernetes.io/metadata.name":                  name4,
							"status.blockdevice.storage.deckhouse.io/size": "1G",
						},
					},
					Status: v1alpha1.BlockDeviceStatus{
						Size:       resource.MustParse("1G"),
						NodeName:   otherHostName,
						Consumable: true,
					},
				},
			}

			for _, bd := range bds {
				err := cl.Create(ctx, &bd)
				if err != nil {
					t.Error(err)
				}
			}

			defer func() {
				for _, bd := range bds {
					err := cl.Delete(ctx, &bd)
					if err != nil {
						t.Error(err)
					}
				}
			}()

			lvg := &v1alpha1.LVMVolumeGroup{
				Spec: v1alpha1.LVMVolumeGroupSpec{
					BlockDeviceSelector: &metav1.LabelSelector{
						MatchLabels: map[string]string{"status.blockdevice.storage.deckhouse.io/size": "1G"},
					},
					Local: v1alpha1.LVMVolumeGroupLocalSpec{
						NodeName: hostName,
					},
				},
			}

			actualBd, err := GetAPIBlockDevices(ctx, cl, metrics, lvg.Spec.BlockDeviceSelector, lvg.Spec.Local.NodeName)
			if assert.NoError(t, err) {
				assert.Equal(t, 2, len(actualBd))

				_, ok := actualBd[name1]
				assert.True(t, ok)
				_, ok = actualBd[name2]
				assert.True(t, ok)
				_, ok = actualBd[name3]
				assert.False(t, ok)
				_, ok = actualBd[name4]
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
				err := cl.Create(ctx, &bd)
				if err != nil {
					t.Error(err)
				}
			}

			defer func() {
				for _, bd := range bds {
					err := cl.Delete(ctx, &bd)
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

			actualBd, err := GetAPIBlockDevices(ctx, cl, metrics, lvg.Spec.BlockDeviceSelector, lvg.Spec.Local.NodeName)
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
					NodeName:   cfg.NodeName,
					Consumable: true,
				},
			}
			actual := map[string]struct{}{}

			assert.True(t, shouldDeleteBlockDevice(bd, actual, cfg.NodeName))
		})

		t.Run("returns_false_cause_of_dif_node", func(t *testing.T) {
			bd := v1alpha1.BlockDevice{
				Status: v1alpha1.BlockDeviceStatus{
					NodeName:   cfg.NodeName,
					Consumable: true,
				},
			}
			actual := map[string]struct{}{}

			assert.False(t, shouldDeleteBlockDevice(bd, actual, "dif-node"))
		})

		t.Run("returns_false_cause_of_not_consumable", func(t *testing.T) {
			bd := v1alpha1.BlockDevice{
				Status: v1alpha1.BlockDeviceStatus{
					NodeName:   cfg.NodeName,
					Consumable: false,
				},
			}
			actual := map[string]struct{}{}

			assert.False(t, shouldDeleteBlockDevice(bd, actual, cfg.NodeName))
		})

		t.Run("returns_false_cause_of_not_deprecated", func(t *testing.T) {
			const name = "test"
			bd := v1alpha1.BlockDevice{
				ObjectMeta: metav1.ObjectMeta{
					Name: name,
				},
				Status: v1alpha1.BlockDeviceStatus{
					NodeName:   cfg.NodeName,
					Consumable: true,
				},
			}
			actual := map[string]struct{}{
				name: {},
			}

			assert.False(t, shouldDeleteBlockDevice(bd, actual, cfg.NodeName))
		})
	})

	t.Run("RemoveDeprecatedAPIDevices", func(t *testing.T) {
		const (
			goodName = "test-candidate1"
			badName  = "test-candidate2"
		)

		candidates := []internal.BlockDeviceCandidate{
			{
				NodeName:              cfg.NodeName,
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
					NodeName:   cfg.NodeName,
				},
			},
		}

		for _, bd := range bds {
			err := cl.Create(ctx, &bd)
			if err != nil {
				t.Error(err)
			}
		}

		defer func() {
			for _, bd := range bds {
				_ = cl.Delete(ctx, &bd)
			}
		}()

		for _, bd := range bds {
			createdBd := &v1alpha1.BlockDevice{}
			err := cl.Get(ctx, client.ObjectKey{
				Name: bd.Name,
			}, createdBd)
			if err != nil {
				t.Error(err)
			}
			assert.Equal(t, bd.Name, createdBd.Name)
		}

		RemoveDeprecatedAPIDevices(ctx, cl, *log, monitoring.GetMetrics(cfg.NodeName), candidates, bds, cfg.NodeName)

		_, ok := bds[badName]
		assert.False(t, ok)

		deleted := &v1alpha1.BlockDevice{}
		err := cl.Get(ctx, client.ObjectKey{
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

		sdsCache := cache.New()
		sdsCache.StoreDevices(devices, bytes.Buffer{})

		candidates := GetBlockDeviceCandidates(*log, cfg, sdsCache)

		assert.Equal(t, 3, len(candidates))
		for i := range candidates {
			assert.Equal(t, devices[i].Name, candidates[i].Path)
			assert.Equal(t, cfg.MachineID, candidates[i].MachineID)
			assert.Equal(t, cfg.NodeName, candidates[i].NodeName)
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

			shouldBeTrue := CheckConsumable(goodDevice)
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
				shouldBeFalse := CheckConsumable(badDevice)
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

		deviceName := CreateUniqDeviceName(can)
		assert.Equal(t, "dev-", deviceName[0:4], "device name does not start with dev-")
		assert.Equal(t, len(deviceName[4:]), 40, "device name does not contains sha1 sum")
	})

	t.Run("CheckTag", func(t *testing.T) {
		t.Run("Have tag_Returns true and tag", func(t *testing.T) {
			expectedName := "testName"
			tags := fmt.Sprintf("storage.deckhouse.io/enabled=true,storage.deckhouse.io/lvmVolumeGroupName=%s", expectedName)

			shouldBeTrue, actualName := CheckTag(tags)
			if assert.True(t, shouldBeTrue) {
				assert.Equal(t, expectedName, actualName)
			}
		})

		t.Run("Haven't tag_Returns false and empty", func(t *testing.T) {
			tags := "someWeirdTags=oMGwtFIsThis"

			shouldBeFalse, actualName := CheckTag(tags)
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

		assert.Equal(t, expectedLabels, ConfigureBlockDeviceLabels(blockDevice))
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
		labels := ConfigureBlockDeviceLabels(blockDevice)
		blockDevice.Labels = labels
		expected := []bool{false, true}

		for i, candidate := range candidates {
			actual := hasBlockDeviceDiff(blockDevice, candidate)
			assert.Equal(t, expected[i], actual)
		}
	})

	t.Run("validateTestLSBLKOutput", func(t *testing.T) {
		testLsblkOutputBytes := []byte(testLsblkOutput)
		devices, err := utils.UnmarshalDevices(testLsblkOutputBytes)
		if assert.NoError(t, err) {
			assert.Equal(t, 31, len(devices))
		}
		filteredDevices, err := FilterDevices(*log, devices)

		for i, device := range filteredDevices {
			println("Filtered device: ", device.Name)
			candidate := internal.BlockDeviceCandidate{
				NodeName:   "test-node",
				Consumable: CheckConsumable(device),
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
				candidateName := CreateCandidateName(*log, candidate, devices)
				assert.Equal(t, "dev-794d93d177d16bc9a85e2dd2ccbdc7325c287374", candidateName, "device name generated incorrectly")
			case 3:
				assert.Equal(t, "/dev/nvme5n1", device.Name)
				assert.True(t, candidate.Consumable)
				candidateName := CreateCandidateName(*log, candidate, devices)
				assert.Equal(t, "dev-3306e773ab3cde6d519ce8d7c3686bf17a124dcb", candidateName, "device name generated incorrectly")
			case 4:
				assert.Equal(t, "/dev/sda4", device.Name)
				assert.False(t, candidate.Consumable)
				candidateName := CreateCandidateName(*log, candidate, devices)
				assert.Equal(t, "dev-377bc6adf33d84eb5932f5c89798bb6c5949ae2d", candidateName, "device name generated incorrectly")
			case 5:
				assert.Equal(t, "/dev/vdc1", device.Name)
				assert.True(t, candidate.Consumable)
				candidateName := CreateCandidateName(*log, candidate, devices)
				assert.Equal(t, "dev-a9d768213aaead8b42465ec859189de8779f96b7", candidateName, "device name generated incorrectly")
			case 6:
				assert.Equal(t, "/dev/mapper/mpatha", device.Name)
				assert.True(t, candidate.Consumable)
				candidateName := CreateCandidateName(*log, candidate, devices)
				assert.Equal(t, "dev-98ca88ddaaddec43b1c4894756f4856244985511", candidateName, "device name generated incorrectly")
			}
		}

		if assert.NoError(t, err) {
			assert.Equal(t, 7, len(filteredDevices))
		}
	})
}

var (
	testLsblkOutput = `
	{
		"blockdevices": [
				{
					"name": "/dev/md0",
					"mountpoint": "/boot",
					"partuuid": null,
					"hotplug": false,
					"model": null,
					"serial": null,
					"size": "1022M",
					"fstype": "ext3",
					"type": "raid1",
					"wwn": null,
					"kname": "/dev/md0",
					"pkname": "/dev/nvme3n1p2"
				},{
					"name": "/dev/md1",
					"mountpoint": null,
					"partuuid": null,
					"hotplug": false,
					"model": null,
					"serial": null,
					"size": "892.9G",
					"fstype": "LVM2_member",
					"type": "raid1",
					"wwn": null,
					"kname": "/dev/md1",
					"pkname": "/dev/nvme3n1p3"
				},{
					"name": "/dev/mapper/vg0-root",
					"mountpoint": "/",
					"partuuid": null,
					"hotplug": false,
					"model": null,
					"serial": null,
					"size": "150G",
					"fstype": "ext4",
					"type": "lvm",
					"wwn": null,
					"kname": "/dev/dm-0",
					"pkname": "/dev/md1"
				},{
					"name": "/dev/md127",
					"mountpoint": null,
					"partuuid": null,
					"hotplug": false,
					"model": null,
					"serial": null,
					"size": "3.3T",
					"fstype": "LVM2_member",
					"type": "raid1",
					"wwn": null,
					"kname": "/dev/md127",
					"pkname": null
				},{
					"name": "/dev/mapper/vg0-pvc--nnnn--nnnnn--nnnn--nnnn--nnnnn_00000",
					"mountpoint": null,
					"partuuid": null,
					"hotplug": false,
					"model": null,
					"serial": null,
					"size": "1G",
					"fstype": "drbd",
					"type": "lvm",
					"wwn": null,
					"kname": "/dev/dm-1",
					"pkname": "/dev/md127"
				},{
					"name": "/dev/nvme1n1",
					"mountpoint": null,
					"partuuid": null,
					"hotplug": false,
					"model": "Micron",
					"serial": "000000BBBBB",
					"size": "1.7T",
					"fstype": "ceph_bluestore",
					"type": "disk",
					"wwn": "eui.000000000000000100aaaaa",
					"kname": "/dev/nvme1n1",
					"pkname": null
				},{
					"name": "/dev/nvme4n1",
					"mountpoint": null,
					"partuuid": null,
					"hotplug": false,
					"model": "Micron",
					"serial": "000000AAAA",
					"size": "1.7T",
					"fstype": null,
					"type": "disk",
					"wwn": "eui.000000000000000100aaaab",
					"kname": "/dev/nvme4n1",
					"pkname": null
				},{
					"name": "/dev/nvme5n1",
					"mountpoint": null,
					"partuuid": null,
					"hotplug": false,
					"model": "Micron",
					"serial": "000000AAAAA",
					"size": "1.7T",
					"fstype": null,
					"type": "disk",
					"wwn": "eui.000000000000000100aaaaac",
					"kname": "/dev/nvme5n1",
					"pkname": null
				},{
					"name": "/dev/nvme0n1",
					"mountpoint": null,
					"partuuid": null,
					"hotplug": false,
					"model": "Micron",
					"serial": "000000AAAAAB",
					"size": "1.7T",
					"fstype": "ceph_bluestore",
					"type": "disk",
					"wwn": "eui.000000000000000100aaaaab",
					"kname": "/dev/nvme0n1",
					"pkname": null
				},{
					"name": "/dev/nvme2n1",
					"mountpoint": null,
					"partuuid": null,
					"hotplug": false,
					"model": "SAMSUNG",
					"serial": "000000AAAAAC",
					"size": "894.3G",
					"fstype": null,
					"type": "disk",
					"wwn": "eui.000000000000000100aaaaad",
					"kname": "/dev/nvme2n1",
					"pkname": null
				},{
					"name": "/dev/nvme3n1",
					"mountpoint": null,
					"partuuid": null,
					"hotplug": false,
					"model": "SAMSUNG",
					"serial": "000000AAAAAD",
					"size": "894.3G",
					"fstype": null,
					"type": "disk",
					"wwn": "eui.000000000000000100aaaaad",
					"kname": "/dev/nvme3n1",
					"pkname": null
				},{
					"name": "/dev/nvme2n1p1",
					"mountpoint": null,
					"partuuid": "11111111-e2bb-47fb-8cc1-xxxxxxx",
					"hotplug": false,
					"model": null,
					"serial": null,
					"size": "256M",
					"fstype": "vfat",
					"type": "part",
					"wwn": "eui.000000000000000100aaaaae",
					"kname": "/dev/nvme2n1p1",
					"pkname": "/dev/nvme2n1"
				},{
					"name": "/dev/nvme2n1p2",
					"mountpoint": null,
					"partuuid": "11111111-d3d4-416a-ac76-xxxxxxx",
					"hotplug": false,
					"model": null,
					"serial": null,
					"size": "1G",
					"fstype": "linux_raid_member",
					"type": "part",
					"wwn": "eui.000000000000000100aaaaaf",
					"kname": "/dev/nvme2n1p2",
					"pkname": "/dev/nvme2n1"
				},{
					"name": "/dev/nvme2n1p3",
					"mountpoint": null,
					"partuuid": "11111111-3677-4eb2-9491-xxxxxxx",
					"hotplug": false,
					"model": null,
					"serial": null,
					"size": "893G",
					"fstype": "linux_raid_member",
					"type": "part",
					"wwn": "eui.000000000000000100aaaaag",
					"kname": "/dev/nvme2n1p3",
					"pkname": "/dev/nvme2n1"
				},{
					"name": "/dev/nvme3n1p1",
					"mountpoint": "/boot/efi",
					"partuuid": "11111111-2965-47d3-8983-xxxxxxx",
					"hotplug": false,
					"model": null,
					"serial": null,
					"size": "256M",
					"fstype": "vfat",
					"type": "part",
					"wwn": "eui.000000000000000100aaaaah",
					"kname": "/dev/nvme3n1p1",
					"pkname": "/dev/nvme3n1"
				},{
					"name": "/dev/nvme3n1p2",
					"mountpoint": null,
					"partuuid": "11111111-7fa2-4318-91c4-xxxxxxx",
					"hotplug": false,
					"model": null,
					"serial": null,
					"size": "1G",
					"fstype": "linux_raid_member",
					"type": "part",
					"wwn": "eui.000000000000000100aaaaabs",
					"kname": "/dev/nvme3n1p2",
					"pkname": "/dev/nvme3n1"
				},{
					"name": "/dev/nvme3n1p3",
					"mountpoint": null,
					"partuuid": "11111111-734d-45f4-b60e-xxxxxxx",
					"hotplug": false,
					"model": null,
					"serial": null,
					"size": "893G",
					"fstype": "linux_raid_member",
					"type": "part",
					"wwn": "eui.000000000000000100aaaaaccx",
					"kname": "/dev/nvme3n1p3",
					"pkname": "/dev/nvme3n1"
				},{
					"name": "/dev/sda",
					"mountpoint": null,
					"partuuid": null,
					"hotplug": false,
					"model": "Virtual_Disk",
					"serial": "6006",
					"size": "50G",
					"fstype": null,
					"type": "disk",
					"wwn": "0x6006",
					"kname": "/dev/sda",
					"pkname": null
				},{
					"name": "/dev/sda1",
					"mountpoint": "/data",
					"partuuid": "11111-01",
					"hotplug": false,
					"model": null,
					"serial": null,
					"size": "50G",
					"fstype": "ext4",
					"type": "part",
					"wwn": "0x6006",
					"kname": "/dev/sda1",
					"pkname": "/dev/sda"
				},{
					"name": "/dev/sda",
					"mountpoint": null,
					"partuuid": null,
					"hotplug": false,
					"model": "INTEL",
					"serial": "PHYS729000AAAA",
					"size": "447.1G",
					"fstype": null,
					"type": "disk",
					"wwn": "0x5555555",
					"kname": "/dev/sda",
					"pkname": null
				},{
					"name": "/dev/sda1",
					"mountpoint": "/boot/efi",
					"partuuid": "xxxxx-6a34-4402-a253-nnnnn",
					"hotplug": false,
					"model": null,
					"serial": null,
					"size": "1G",
					"fstype": "vfat",
					"type": "part",
					"wwn": "0x5555555",
					"kname": "/dev/sda1",
					"pkname": "/dev/sda"
				},{
					"name": "/dev/sda2",
					"mountpoint": null,
					"partuuid": "xxxxx-99b4-42c4-9dc4-nnnnnnn",
					"hotplug": false,
					"model": null,
					"serial": null,
					"size": "1G",
					"fstype": "linux_raid_member",
					"type": "part",
					"wwn": "0x5555555",
					"kname": "/dev/sda2",
					"pkname": "/dev/sda"
				},{
					"name": "/dev/sda3",
					"mountpoint": null,
					"partuuid": "xxxxx-f3ef-4b4a-86f8-nnnnnn",
					"hotplug": false,
					"model": null,
					"serial": null,
					"size": "55G",
					"fstype": "linux_raid_member",
					"type": "part",
					"wwn": "0x5555555",
					"kname": "/dev/sda3",
					"pkname": "/dev/sda"
				},{
					"name": "/dev/sda4",
					"mountpoint": null,
					"partuuid": "xxxxx-9f91-41c5-9616-nnnnnn",
					"hotplug": false,
					"model": null,
					"serial": null,
					"size": "390.1G",
					"fstype": "LVM2_member",
					"type": "part",
					"wwn": "0x55cddd",
					"kname": "/dev/sda4",
					"pkname": "/dev/sda"
				},{
					"name": "/dev/mapper/data--linstor-pvc--xxxx--8997--4630--a728--nnnnnn_00000",
					"mountpoint": null,
					"partuuid": null,
					"hotplug": false,
					"model": null,
					"serial": null,
					"size": "30G",
					"fstype": null,
					"type": "lvm",
					"wwn": null,
					"kname": "/dev/dm-18",
					"pkname": "/dev/sda4"
				},{
					"name": "/dev/drbd1028",
					"mountpoint": null,
					"partuuid": null,
					"hotplug": false,
					"model": null,
					"serial": null,
					"size": "50G",
					"fstype": null,
					"type": "disk",
					"wwn": null,
					"kname": "/dev/drbd1028",
					"pkname": "/dev/dm-10"
				},{
					"name": "/dev/vdc",
					"mountpoint": null,
					"partuuid": null,
					"hotplug": false,
					"model": null,
					"serial": "fhmnscgfsllbsi2u5o8v",
					"size": "20G",
					"fstype": null,
					"type": "disk",
					"wwn": null,
					"kname": "/dev/vdc",
					"pkname": null
				},{
					"name": "/dev/vdc1",
					"mountpoint": null,
					"partuuid": "13dcb00e-01",
					"hotplug": false,
					"model": null,
					"serial": null,
					"size": "20G",
					"fstype": null,
					"type": "part",
					"wwn": null,
					"kname": "/dev/vdc1",
					"pkname": "/dev/vdc"
			 	},{
					"name": "/dev/mapper/mpatha",
					"mountpoint": null,
					"partuuid": null,
					"hotplug": false,
					"model": null,
					"serial": null,
					"size": 3650722201600,
					"fstype": null,
					"type": "mpath",
					"wwn": null,
					"kname": "/dev/dm-6",
					"pkname": "/dev/sdf",
					"rota": false
			 	},{
					"name": "/dev/sdf",
					"mountpoint": null,
					"partuuid": null,
					"hotplug": false,
					"model": "test-model",
					"serial": "22222222xxxxx",
					"size": 3650722201600,
					"fstype": "mpath_member",
					"type": "disk",
					"wwn": "2222xxxxxx",
					"kname": "/dev/sdf",
					"pkname": null,
					"rota": false
			 	},{
					"name": "/dev/sdh",
					"mountpoint": null,
					"partuuid": null,
					"hotplug": false,
					"model": "test-model",
					"serial": "22222222xxxxx",
					"size": 3650722201600,
					"fstype": "mpath_member",
					"type": "disk",
					"wwn": "2222xxxxxx",
					"kname": "/dev/sdh",
					"pkname": null,
					"rota": false
		 	 	}
		]
	}`
)
