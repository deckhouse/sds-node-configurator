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
	"fmt"
	"sds-node-configurator/api/v1alpha1"
	"sds-node-configurator/internal"
	"sds-node-configurator/pkg/logger"
	"sds-node-configurator/pkg/utils"
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/stretchr/testify/assert"
)

func TestBlockDeviceCtrl(t *testing.T) {
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
				Size:                  resource.Quantity{},
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
				Size:                  resource.Quantity{},
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
		blockDevice := v1alpha1.BlockDevice{
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
				Size:                  "0",
				Model:                 "testMODEL",
				Rota:                  false,
				HotPlug:               false,
				MachineID:             "testMACHINE",
			},
		}
		expected := []bool{false, true}

		for i, candidate := range candidates {
			actual := hasBlockDeviceDiff(blockDevice.Status, candidate)
			assert.Equal(t, expected[i], actual)
		}
	})

	t.Run("validateTestLSBLKOutput", func(t *testing.T) {
		log, err := logger.NewLogger("4")
		if err != nil {
			t.Fatal(err)
		}
		testLsblkOutputBytes := []byte(testLsblkOutput)
		devices, err := utils.UnmarshalDevices(testLsblkOutputBytes)
		if assert.NoError(t, err) {
			assert.Equal(t, 17, len(devices))
		}
		filteredDevices, err := FilterDevices(*log, devices)

		for i, device := range filteredDevices {
			println("Filtered device: ", device.Name)
			switch i {
			case 0:
				assert.Equal(t, "/dev/md1", device.Name)
				assert.False(t, CheckConsumable(device))
			case 1:
				assert.Equal(t, "/dev/md127", device.Name)
				assert.False(t, CheckConsumable(device))
			case 2:
				assert.Equal(t, "/dev/nvme4n1", device.Name)
				assert.True(t, CheckConsumable(device))
			case 3:
				assert.Equal(t, "/dev/nvme5n1", device.Name)
				assert.True(t, CheckConsumable(device))
			}

		}
		if assert.NoError(t, err) {
			assert.Equal(t, 4, len(filteredDevices))
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
					"model": "Micron_7450_MTFDKCC1T9TFR",
					"serial": "2310407BBBBB",
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
					"model": "Micron_7450_MTFDKCC1T9TFR",
					"serial": "2310407B1B12",
					"size": "1.7T",
					"fstype": null,
					"type": "disk",
					"wwn": "eui.000000000000000100a07523407b1b12",
					"kname": "/dev/nvme4n1",
					"pkname": null
			 },{
					"name": "/dev/nvme5n1",
					"mountpoint": null,
					"partuuid": null,
					"hotplug": false,
					"model": "Micron_7450_MTFDKCC1T9TFR",
					"serial": "2310407AAAAA",
					"size": "1.7T",
					"fstype": null,
					"type": "disk",
					"wwn": "eui.000000000000000100a07523407b1bbb",
					"kname": "/dev/nvme5n1",
					"pkname": null
			 },{
					"name": "/dev/nvme0n1",
					"mountpoint": null,
					"partuuid": null,
					"hotplug": false,
					"model": "Micron_7450_MTFDKCC1T9TFR",
					"serial": "2310402499EC",
					"size": "1.7T",
					"fstype": "ceph_bluestore",
					"type": "disk",
					"wwn": "eui.000000000000000100a07523402499ec",
					"kname": "/dev/nvme0n1",
					"pkname": null
			 },{
					"name": "/dev/nvme2n1",
					"mountpoint": null,
					"partuuid": null,
					"hotplug": false,
					"model": "SAMSUNG MZQL2960HCJR-00A07",
					"serial": "S64FNE0RB09450",
					"size": "894.3G",
					"fstype": null,
					"type": "disk",
					"wwn": "eui.3634463052b094500025384500000001",
					"kname": "/dev/nvme2n1",
					"pkname": null
			 },{
					"name": "/dev/nvme3n1",
					"mountpoint": null,
					"partuuid": null,
					"hotplug": false,
					"model": "SAMSUNG MZQL2960HCJR-00A07",
					"serial": "S64FNE0R601010",
					"size": "894.3G",
					"fstype": null,
					"type": "disk",
					"wwn": "eui.36344630526010100025384500000001",
					"kname": "/dev/nvme3n1",
					"pkname": null
			 },{
					"name": "/dev/nvme2n1p1",
					"mountpoint": null,
					"partuuid": "0fdb72e6-e2bb-47fb-8cc1-4d0083566c6b",
					"hotplug": false,
					"model": null,
					"serial": null,
					"size": "256M",
					"fstype": "vfat",
					"type": "part",
					"wwn": "eui.3634463052b094500025384500000001",
					"kname": "/dev/nvme2n1p1",
					"pkname": "/dev/nvme2n1"
			 },{
					"name": "/dev/nvme2n1p2",
					"mountpoint": null,
					"partuuid": "4dbeebd3-d3d4-416a-ac76-f9ea30ab2540",
					"hotplug": false,
					"model": null,
					"serial": null,
					"size": "1G",
					"fstype": "linux_raid_member",
					"type": "part",
					"wwn": "eui.3634463052b094500025384500000001",
					"kname": "/dev/nvme2n1p2",
					"pkname": "/dev/nvme2n1"
			 },{
					"name": "/dev/nvme2n1p3",
					"mountpoint": null,
					"partuuid": "77371c7e-3677-4eb2-9491-5cfdfe0e47e3",
					"hotplug": false,
					"model": null,
					"serial": null,
					"size": "893G",
					"fstype": "linux_raid_member",
					"type": "part",
					"wwn": "eui.3634463052b094500025384500000001",
					"kname": "/dev/nvme2n1p3",
					"pkname": "/dev/nvme2n1"
			 },{
					"name": "/dev/nvme3n1p1",
					"mountpoint": "/boot/efi",
					"partuuid": "21e913d5-2965-47d3-8983-cfc17f13aae4",
					"hotplug": false,
					"model": null,
					"serial": null,
					"size": "256M",
					"fstype": "vfat",
					"type": "part",
					"wwn": "eui.36344630526010100025384500000001",
					"kname": "/dev/nvme3n1p1",
					"pkname": "/dev/nvme3n1"
			 },{
					"name": "/dev/nvme3n1p2",
					"mountpoint": null,
					"partuuid": "81aa4d8e-7fa2-4318-91c4-85a2f03c785f",
					"hotplug": false,
					"model": null,
					"serial": null,
					"size": "1G",
					"fstype": "linux_raid_member",
					"type": "part",
					"wwn": "eui.36344630526010100025384500000001",
					"kname": "/dev/nvme3n1p2",
					"pkname": "/dev/nvme3n1"
			 },{
					"name": "/dev/nvme3n1p3",
					"mountpoint": null,
					"partuuid": "f0123e46-734d-45f4-b60e-b9666397251b",
					"hotplug": false,
					"model": null,
					"serial": null,
					"size": "893G",
					"fstype": "linux_raid_member",
					"type": "part",
					"wwn": "eui.36344630526010100025384500000001",
					"kname": "/dev/nvme3n1p3",
					"pkname": "/dev/nvme3n1"
			 }
		]
 }`
)
