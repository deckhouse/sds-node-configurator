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
	"sds-node-configurator/api/v1alpha1"
	"sds-node-configurator/internal"
	"sds-node-configurator/pkg/utils"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetCandidates(t *testing.T) {
	t.Run("CheckConsumable", func(t *testing.T) {
		t.Run("Good device returns true", func(t *testing.T) {
			goodDevice := internal.Device{
				Name:       "goodName",
				MountPoint: "",
				PartUUID:   "",
				HotPlug:    false,
				Model:      "",
				Serial:     "",
				Size:       0,
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
				{Name: internal.DRBDName + "badName",
					MountPoint: "",
					PartUUID:   "",
					HotPlug:    false,
					Model:      "",
					Serial:     "",
					Size:       0,
					Type:       "",
					Wwn:        "",
					KName:      "",
					PkName:     "",
					FSType:     "",
					Rota:       false},
				{
					Name:       "goodName",
					MountPoint: "badMountPoint",
					PartUUID:   "",
					HotPlug:    false,
					Model:      "",
					Serial:     "",
					Size:       0,
					Type:       "",
					Wwn:        "",
					KName:      "",
					PkName:     "",
					FSType:     "",
					Rota:       false,
				},
				{
					Name:       "goodName",
					MountPoint: "",
					PartUUID:   "",
					HotPlug:    false,
					Model:      "",
					Serial:     "",
					Size:       0,
					Type:       "",
					Wwn:        "",
					KName:      "",
					PkName:     "",
					FSType:     "badFSType",
					Rota:       false,
				},
				{
					Name:       "goodName",
					MountPoint: "",
					PartUUID:   "",
					HotPlug:    true,
					Model:      "",
					Serial:     "",
					Size:       0,
					Type:       "",
					Wwn:        "",
					KName:      "",
					PkName:     "",
					FSType:     "",
					Rota:       false,
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
			Size:     4294967296,
			Model:    "HARD-DRIVE",
		}

		deviceName := CreateUniqDeviceName(can)
		assert.Equal(t, "dev-", deviceName[0:4], "device name does not start with dev-")
		assert.Equal(t, len(deviceName[4:]), 40, "device name does not contains sha1 sum")
	})

	t.Run("CheckTag", func(t *testing.T) {
		t.Run("Have tag_Returns true and tag", func(t *testing.T) {
			expectedName := "testName"
			tags := "storage.deckhouse.io/enabled=true,storage.deckhouse.io/watcherLVMVGCtrlName=" + expectedName

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
		expected := []bool{true, true, true, false, false, false}

		for i, size := range sizes {
			b, err := utils.QuantityToBytes(size)
			if err != nil {
				panic(err)
			}

			valid, err := hasValidSize(b)
			if assert.NoError(t, err) {
				assert.Equal(t, expected[i], valid)
			}
		}
	})

	t.Run("hasBlockDeviceDiff", func(t *testing.T) {
		candidates := []internal.BlockDeviceCandidate{
			// same state
			{
				NodeName:              "test_node",
				Consumable:            false,
				PVUuid:                "testPV",
				VGUuid:                "testVGUID",
				LvmVolumeGroupName:    "testLVGName",
				ActualVGNameOnTheNode: "testNameOnNode",
				Wwn:                   "testWWN",
				Serial:                "testSERIAL",
				Path:                  "testPATH",
				Size:                  0,
				Rota:                  false,
				Model:                 "testMODEL",
				Name:                  "testNAME",
				HotPlug:               false,
				KName:                 "testKNAME",
				PkName:                "testPKNAME",
				Type:                  "testTYPE",
				FSType:                "testFS",
				MachineId:             "testMACHINE",
			},
			// diff state
			{
				NodeName:              "test_node",
				Consumable:            true,
				PVUuid:                "testPV2",
				VGUuid:                "testVGUID2",
				LvmVolumeGroupName:    "testLVGName2",
				ActualVGNameOnTheNode: "testNameOnNode2",
				Wwn:                   "testWWN2",
				Serial:                "testSERIAL2",
				Path:                  "testPATH2",
				Size:                  0,
				Rota:                  true,
				Model:                 "testMODEL2",
				Name:                  "testNAME",
				HotPlug:               true,
				KName:                 "testKNAME2",
				PkName:                "testPKNAME2",
				Type:                  "testTYPE2",
				FSType:                "testFS2",
				MachineId:             "testMACHINE2",
			},
		}
		resourse := v1alpha1.BlockDevice{
			Status: v1alpha1.BlockDeviceStatus{
				Type:                  "testTYPE",
				FsType:                "testFS",
				NodeName:              "test_node",
				Consumable:            false,
				PVUuid:                "testPV",
				VGUuid:                "testVGUID",
				LvmVolumeGroupName:    "testLVGName",
				ActualVGNameOnTheNode: "testNameOnNode",
				Wwn:                   "testWWN",
				Serial:                "testSERIAL",
				Path:                  "testPATH",
				Size:                  "testSIZE",
				Model:                 "testMODEL",
				Rota:                  false,
				HotPlug:               false,
				MachineID:             "testMACHINE",
			},
		}
		expected := []bool{false, true}

		for i, candidate := range candidates {
			actual := hasBlockDeviceDiff(resourse.Status, candidate)
			assert.Equal(t, expected[i], actual)
		}
	})
}
