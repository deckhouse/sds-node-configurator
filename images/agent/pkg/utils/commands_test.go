package utils

import (
	"github.com/stretchr/testify/assert"
	"storage-configurator/internal"
	"testing"
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
			expectedDevices := internal.Devices{BlockDevices: []internal.Device{
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

			actualDevices, err := unmarshalDevices([]byte(js))
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
			_, err := unmarshalDevices([]byte(js))
			assert.Error(t, err)
		})
	})

	t.Run("GetAllPVs", func(t *testing.T) {
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
			expectedPVs := internal.PV{PV: []internal.PVData{
				{
					PVName: "/dev/vdb",
					VGName: "vgtest",
					PVUsed: "0 ",
					PVUuid: "BmuLLu-9ZSf-eqpf-qR3H-23rQ-fIl7-Ouyl5X",
					VGTags: "",
					VGUuid: "JnCFQZ-TTfE-Ed2C-nKoH-yzPH-4fMA-CKwIv7",
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
"vg_size":"<2.00g", "vg_free":"<2.00g", "vg_uuid":"P14t8J-nfUE-hryT-LiTv-JdFD-Wqxg-R8taCa", 
"vg_tags":"test-tag", "vg_shared":"test-shared"}
              ]
          }
      ]
  }`
			expectedVGs := internal.VG{VG: []internal.VGData{
				{
					VGName:   "test-vg",
					VGUuid:   "P14t8J-nfUE-hryT-LiTv-JdFD-Wqxg-R8taCa",
					VGTags:   "test-tag",
					VGShared: "test-shared",
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
	})
}
