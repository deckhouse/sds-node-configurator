package controller_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"storage-configurator/pkg/controller"
)

func TestGetCandidates(t *testing.T) {
	t.Run("GetBlockDevices", func(t *testing.T) {
		t.Run("UnmarshalDevices_Expects_Success", func(t *testing.T) {
			js := `{
   "blockdevices": [
      {
         "name": "/dev/vda",
         "mountpoint": null,
         "partuuid": null,
         "hotplug": false,
         "model": null,
         "serial": null,
         "size": "30G",
         "fstype": null,
         "type": "disk",
         "wwn": null,
         "kname": "/dev/vda",
         "pkname": null,
         "rota": true
      },{
         "name": "/dev/vda1",
         "mountpoint": null,
         "partuuid": "ec0944f8-90a5-4e74-9453-d4d8d03bd53d",
         "hotplug": false,
         "model": null,
         "serial": null,
         "size": "1M",
         "fstype": null,
         "type": "part",
         "wwn": null,
         "kname": "/dev/vda1",
         "pkname": "/dev/vda",
         "rota": true
      }
   ]
}`
			expectedDevices := controller.Devices{BlockDevices: []controller.Device{
				{
					Name:       "/dev/vda",
					MountPoint: "",
					PartUUID:   "",
					HotPlug:    false,
					Model:      "",
					Serial:     "",
					Size:       "30G",
					Type:       "disk",
					Wwn:        "",
					KName:      "/dev/vda",
					PkName:     "",
					FSType:     "",
					Rota:       true,
				},
				{
					Name:       "/dev/vda1",
					MountPoint: "",
					PartUUID:   "ec0944f8-90a5-4e74-9453-d4d8d03bd53d",
					HotPlug:    false,
					Model:      "",
					Serial:     "",
					Size:       "1M",
					Type:       "part",
					Wwn:        "",
					KName:      "/dev/vda1",
					PkName:     "/dev/vda",
					FSType:     "",
					Rota:       true,
				},
			}}

			actualDevices, err := controller.UnmarshalDevices([]byte(js))
			if assert.NoError(t, err) {
				assert.Equal(t, expectedDevices.BlockDevices, actualDevices)
			}
		})

		t.Run("UnmarshalDevices_Expects_Failure", func(t *testing.T) {
			js := `{
   "blockdevices": [
      {
         "name": "/dev/vda",
         "mountpoints": null,
         "partuuid": null,
         "hotplug": false,
         "model": null,
         "serial": null,
         "size": "30G",
         "fstype": null,
         "type": "disk",
         "wwn": null,
         "kname": "/dev/vda",
         "pkname": null,
      },{
         "name": "/dev/vda1",
         "mountpoint": null,
         "partuuid": "ec0944f8-90a5-4e74-9453-d4d8d03bd53d",
         "hotplug": false,
         "model": null,
         "serial": null,
         "size": "1M",
         "fstype": null,
         "type": "part",
         "wwn": null,
         "kname": "/dev/vda1",
         "pkname": "/dev/vda",
         "rota": true
      }
   ]
}`
			_, err := controller.UnmarshalDevices([]byte(js))
			assert.Error(t, err)
		})
	})

	t.Run("GetPhysicalVolumes", func(t *testing.T) {
		t.Run("UnmarshalPVs_Expects_Success", func(t *testing.T) {
			js := `{
      "report": [
          {
              "pv": [
                  {"pv_name":"/dev/vdb", "vg_name":"vgtest", "pv_fmt":"lvm2", "pv_attr":"a--", "pv_size":"1020.00m", 
"pv_free":"1020.00m", "pv_used":"0 ", "pv_uuid":"BmuLLu-9ZSf-eqpf-qR3H-23rQ-fIl7-Ouyl5X", "vg_tags":"", 
"vg_uuid":"JnCFQZ-TTfE-Ed2C-nKoH-yzPH-4fMA-CKwIv7"}
              ]
          }
      ]
  }`
			expectedPVs := controller.Report{PV: []controller.PV{
				{
					PVName: "/dev/vdb",
					VGName: "vgtest",
					PVUsed: "0 ",
					PVUuid: "BmuLLu-9ZSf-eqpf-qR3H-23rQ-fIl7-Ouyl5X",
					VGTags: "",
					VGUuid: "JnCFQZ-TTfE-Ed2C-nKoH-yzPH-4fMA-CKwIv7",
				},
			}}

			actualPVs, err := controller.UnmarshalPVs([]byte(js))
			if assert.NoError(t, err) {
				assert.Equal(t, expectedPVs.PV, actualPVs)
			}
		})

		t.Run("UnmarshalPVs_Expects_Failure", func(t *testing.T) {
			js := `{
      "report": 
          {
              "pv": [
                  {"pv_name":"/dev/vdb", "vg_name":"vgtest", "pv_fmt":"lvm2", "pv_attr":"a--", "pv_size":"1020.00m", 
"pv_free":"1020.00m", "pv_uuid":"BmuLLu-9ZSf-eqpf-qR3H-23rQ-fIl7-Ouyl5X", "vg_tags":"", 
"vg_uuid":"JnCFQZ-TTfE-Ed2C-nKoH-yzPH-4fMA-CKwIv7"}
              ]
          }
  }`

			_, err := controller.UnmarshalPVs([]byte(js))
			assert.Error(t, err)
		})
	})

	t.Run("CheckConsumable", func(t *testing.T) {
		t.Run("Good device returns true", func(t *testing.T) {
			goodDevice := controller.Device{
				Name:       "goodName",
				MountPoint: "",
				PartUUID:   "",
				HotPlug:    false,
				Model:      "",
				Serial:     "",
				Size:       "",
				Type:       "",
				Wwn:        "",
				KName:      "",
				PkName:     "",
				FSType:     "",
				Rota:       false,
			}

			shouldBeTrue := controller.CheckConsumable(goodDevice)
			assert.True(t, shouldBeTrue)
		})

		t.Run("Bad devices return false", func(t *testing.T) {
			badDevices := controller.Devices{BlockDevices: []controller.Device{
				{Name: controller.DRBDName + "badName",
					MountPoint: "",
					PartUUID:   "",
					HotPlug:    false,
					Model:      "",
					Serial:     "",
					Size:       "",
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
					Size:       "",
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
					Size:       "",
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
					Size:       "",
					Type:       "",
					Wwn:        "",
					KName:      "",
					PkName:     "",
					FSType:     "",
					Rota:       false,
				},
			}}

			for _, badDevice := range badDevices.BlockDevices {
				shouldBeFalse := controller.CheckConsumable(badDevice)
				assert.False(t, shouldBeFalse)
			}
		})
	})

	t.Run("CreateUniqDeviceName", func(t *testing.T) {
		nodeName := "testNode"
		can := controller.Candidate{
			NodeName: nodeName,
			Wwn:      "ZX128ZX128ZX128",
			Path:     "/dev/sda",
			Size:     "4Gb",
			Model:    "HARD-DRIVE",
		}

		deviceName := controller.CreateUniqDeviceName(can)
		assert.Equal(t, "dev-", deviceName[0:4], "device name does not start with dev-")
		assert.Equal(t, len(deviceName[4:]), 40, "device name does not contains sha1 sum")
	})

	t.Run("CheckTag", func(t *testing.T) {
		t.Run("Have tag_Returns true and tag", func(t *testing.T) {
			expectedName := "testName"
			tags := "storage.deckhouse.io/enabled=true,storage.deckhouse.io/lvmVolumeGroupName=" + expectedName

			shouldBeTrue, actualName := controller.CheckTag(tags)
			if assert.True(t, shouldBeTrue) {
				assert.Equal(t, expectedName, actualName)
			}
		})

		t.Run("Haven't tag_Returns false and empty", func(t *testing.T) {
			tags := "someWeirdTags=oMGwtFIsThis"

			shouldBeFalse, actualName := controller.CheckTag(tags)
			if assert.False(t, shouldBeFalse) {
				assert.Equal(t, "", actualName)
			}
		})
	})
}
