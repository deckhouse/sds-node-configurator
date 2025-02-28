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

package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"

	"agent/internal"
)

func TestCommands(t *testing.T) {
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

			size30G, err := resource.ParseQuantity("30G")
			if err != nil {
				t.Error(err)
			}
			size1M, err := resource.ParseQuantity("1M")
			if err != nil {
				t.Error(err)
			}
			expectedDevices := internal.Devices{BlockDevices: []internal.Device{
				{
					Name:       "/dev/vda",
					MountPoint: "",
					PartUUID:   "",
					HotPlug:    false,
					Model:      "",
					Serial:     "",
					Size:       size30G,
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
					Size:       size1M,
					Type:       "part",
					Wwn:        "",
					KName:      "/dev/vda1",
					PkName:     "/dev/vda",
					FSType:     "",
					Rota:       true,
				},
			}}

			actualDevices, err := UnmarshalDevices([]byte(js))
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
			_, err := UnmarshalDevices([]byte(js))
			assert.Error(t, err)
		})
	})

	t.Run("GetAllPVs", func(t *testing.T) {
		t.Run("UnmarshalPVs_Expects_Success", func(t *testing.T) {
			js := `{
      "report": [
          {
              "pv": [
                  {"pv_name":"/dev/vdb", "vg_name":"vgtest", "pv_fmt":"lvm2", "pv_attr":"a--", "pv_size":"10G", 
"pv_free":"1020.00m", "pv_used":"0 ", "pv_uuid":"BmuLLu-9ZSf-eqpf-qR3H-23rQ-fIl7-Ouyl5X", "vg_tags":"", 
"vg_uuid":"JnCFQZ-TTfE-Ed2C-nKoH-yzPH-4fMA-CKwIv7"}
              ]
          }
      ]
  }`
			size10G, err := resource.ParseQuantity("10G")
			if err != nil {
				t.Error(err)
			}
			expectedPVs := internal.PV{PV: []internal.PVData{
				{
					PVName: "/dev/vdb",
					VGName: "vgtest",
					PVUsed: "0 ",
					PVUuid: "BmuLLu-9ZSf-eqpf-qR3H-23rQ-fIl7-Ouyl5X",
					VGTags: "",
					VGUuid: "JnCFQZ-TTfE-Ed2C-nKoH-yzPH-4fMA-CKwIv7",
					PVSize: size10G,
				},
			}}

			actualPVs, err := unmarshalPVs([]byte(js))
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

			_, err := unmarshalPVs([]byte(js))
			assert.Error(t, err)
		})
	})

	t.Run("GetAllVGs", func(t *testing.T) {
		t.Run("UnmarshalVGs_Expects_Success", func(t *testing.T) {
			js := `{
      "report": [
          {
              "vg": [
                  {"vg_name":"test-vg", "pv_count":"1", "lv_count":"0", "snap_count":"0", "vg_attr":"wz--n-", 
"vg_size":"2G", "vg_free":"2G", "vg_uuid":"P14t8J-nfUE-hryT-LiTv-JdFD-Wqxg-R8taCa", 
"vg_tags":"test-tag", "vg_shared":"test-shared"}
              ]
          }
      ]
  }`
			size2G, err := resource.ParseQuantity("2G")
			if err != nil {
				t.Error(err)
			}
			expectedVGs := internal.VG{VG: []internal.VGData{
				{
					VGName:   "test-vg",
					VGUUID:   "P14t8J-nfUE-hryT-LiTv-JdFD-Wqxg-R8taCa",
					VGTags:   "test-tag",
					VGSize:   size2G,
					VGShared: "test-shared",
					VGFree:   size2G,
				},
			}}

			actualVGs, err := unmarshalVGs([]byte(js))
			if assert.NoError(t, err) {
				assert.Equal(t, expectedVGs.VG, actualVGs)
			}
		})

		t.Run("UnmarshalVGs_Expects_Failure", func(t *testing.T) {
			js := `{
      "report": 
          {
              "vg": [
                  {"vg_name":"test-vg", "pv_count":"1", "lv_count":"0", "snap_count":"0", "vg_attr":"wz--n-", 
"vg_size":"<2.00g", "vg_free":"<2.00g", "vg_uuid":"P14t8J-nfUE-hryT-LiTv-JdFD-Wqxg-R8taCa", 
"vg_tags":"test-tag", "vg_shared":"test-shared"}
              ]
          }
  }`

			_, err := unmarshalPVs([]byte(js))
			assert.Error(t, err)
		})

		t.Run("UnmarshalVGS_EmptyVG_ReturnsZeroLen", func(t *testing.T) {
			js := `{
      "report": [
          {
              "vg": [
              ]
          }
      ]
  }`

			actualVGs, err := unmarshalVGs([]byte(js))
			if assert.NoError(t, err) {
				assert.Equal(t, 0, len(actualVGs))
			}
		})
	})

	t.Run("GetAllPVs", func(t *testing.T) {
		t.Run("Unmarshal_LV", func(t *testing.T) {
			js := `{
      "report": [
          {
              "lv": [
                  {"lv_name":"mythinpool", "vg_name":"test", "lv_attr":"twi---tzp-", "lv_size":"1G", "pool_lv":"", "origin":"", "data_percent":"", "metadata_percent":"", "move_pv":"", "mirror_log":"", "copy_percent":"", "convert_lv":""}
              ]
          }
      ]
  }`

			pvs, err := unmarshalLVs([]byte(js))
			if assert.NoError(t, err) {
				pv := pvs[0]

				assert.Equal(t, string(pv.LVAttr[0]), "t")
			}
		})

		t.Run("Unmarshal_LV_Empty_ThinDeviceID", func(t *testing.T) {
			// TODO: Cleanup
			js := ` {
      "report": [
          {
              "lv": [
                  {"lv_name":"[lvol0_pmspare]", "vg_name":"vg-1", "lv_attr":"ewi-------", "lv_size":"4194304", "pool_lv":"", "origin":"", "data_percent":"", "metadata_percent":"", "move_pv":"", "mirror_log":"", "copy_percent":"", "convert_lv":"", "vg_uuid":"QVh4uj-O6Wa-6TT8-XdU7-xCQu-M4gR-x9IY36", "lv_tags":"", "thin_id":"2", "metadata_lv":"", "lv_dm_path":"/dev/mapper/vg--1-lvol0_pmspare"},
                  {"lv_name":"thin-1", "vg_name":"vg-1", "lv_attr":"twi-a-tz--", "lv_size":"104857600", "pool_lv":"", "origin":"", "data_percent":"0.00", "metadata_percent":"10.84", "move_pv":"", "mirror_log":"", "copy_percent":"", "convert_lv":"", "vg_uuid":"QVh4uj-O6Wa-6TT8-XdU7-xCQu-M4gR-x9IY36", "lv_tags":"", "thin_id":"", "metadata_lv":"[thin-1_tmeta]", "lv_dm_path":"/dev/mapper/vg--1-thin--1"},
                  {"lv_name":"[thin-1_tdata]", "vg_name":"vg-1", "lv_attr":"Twi-ao----", "lv_size":"104857600", "pool_lv":"", "origin":"", "data_percent":"", "metadata_percent":"", "move_pv":"", "mirror_log":"", "copy_percent":"", "convert_lv":"", "vg_uuid":"QVh4uj-O6Wa-6TT8-XdU7-xCQu-M4gR-x9IY36", "lv_tags":"", "thin_id":"", "metadata_lv":"", "lv_dm_path":"/dev/mapper/vg--1-thin--1_tdata"},
                  {"lv_name":"[thin-1_tmeta]", "vg_name":"vg-1", "lv_attr":"ewi-ao----", "lv_size":"4194304", "pool_lv":"", "origin":"", "data_percent":"", "metadata_percent":"", "move_pv":"", "mirror_log":"", "copy_percent":"", "convert_lv":"", "vg_uuid":"QVh4uj-O6Wa-6TT8-XdU7-xCQu-M4gR-x9IY36", "lv_tags":"", "thin_id":"", "metadata_lv":"", "lv_dm_path":"/dev/mapper/vg--1-thin--1_tmeta"},
                  {"lv_name":"thin-5", "vg_name":"vg-1", "lv_attr":"twi-a-tz--", "lv_size":"54525952", "pool_lv":"", "origin":"", "data_percent":"0.00", "metadata_percent":"10.84", "move_pv":"", "mirror_log":"", "copy_percent":"", "convert_lv":"", "vg_uuid":"QVh4uj-O6Wa-6TT8-XdU7-xCQu-M4gR-x9IY36", "lv_tags":"", "thin_id":"", "metadata_lv":"[thin-5_tmeta]", "lv_dm_path":"/dev/mapper/vg--1-thin--5"},
                  {"lv_name":"[thin-5_tdata]", "vg_name":"vg-1", "lv_attr":"Twi-ao----", "lv_size":"54525952", "pool_lv":"", "origin":"", "data_percent":"", "metadata_percent":"", "move_pv":"", "mirror_log":"", "copy_percent":"", "convert_lv":"", "vg_uuid":"QVh4uj-O6Wa-6TT8-XdU7-xCQu-M4gR-x9IY36", "lv_tags":"", "thin_id":"", "metadata_lv":"", "lv_dm_path":"/dev/mapper/vg--1-thin--5_tdata"},
                  {"lv_name":"[thin-5_tmeta]", "vg_name":"vg-1", "lv_attr":"ewi-ao----", "lv_size":"4194304", "pool_lv":"", "origin":"", "data_percent":"", "metadata_percent":"", "move_pv":"", "mirror_log":"", "copy_percent":"", "convert_lv":"", "vg_uuid":"QVh4uj-O6Wa-6TT8-XdU7-xCQu-M4gR-x9IY36", "lv_tags":"", "thin_id":"", "metadata_lv":"", "lv_dm_path":"/dev/mapper/vg--1-thin--5_tmeta"}
              ]
          }
      ]
      ,
      "log": [
      ]
  }
`

			pvs, err := unmarshalLVs([]byte(js))
			if assert.NoError(t, err) {
				assert.Equal(t, "vg-1", pvs[0].VGName)
			}
		})
	})
}
