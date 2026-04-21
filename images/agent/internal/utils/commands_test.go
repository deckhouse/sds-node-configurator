/*
Copyright 2025 Flant JSC

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

	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
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

			actualDevices, err := NewCommands().UnmarshalDevices([]byte(js))
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
			_, err := NewCommands().UnmarshalDevices([]byte(js))
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
"vg_uuid":"JnCFQZ-TTfE-Ed2C-nKoH-yzPH-4fMA-CKwIv7", "pe_start":"1048576"},
                  {"pv_name":"/dev/vdc", "vg_name":"vgtest", "pv_fmt":"lvm2", "pv_attr":"a--", "pv_size":"10G",
"pv_free":"2048.00m", "pv_used":"0 ", "pv_uuid":"CmuLLu-9ZSf-eqpf-qR3H-23rQ-fIl7-Ouyl5X", "vg_tags":"",
"vg_uuid":"JnCFQZ-TTfE-Ed2C-nKoH-yzPH-4fMA-CKwIv7", "pe_start":"2097152"}
              ]
          }
      ]
  }`
			size10G, err := resource.ParseQuantity("10G")
			if err != nil {
				t.Error(err)
			}
			peStart1Mi, err := resource.ParseQuantity("1048576")
			if err != nil {
				t.Error(err)
			}
			peStart2Mi, err := resource.ParseQuantity("2097152")
			if err != nil {
				t.Error(err)
			}
			expectedPVs := internal.PV{PV: []internal.PVData{
				{
					PVName:  "/dev/vdb",
					VGName:  "vgtest",
					PVUsed:  "0 ",
					PVUuid:  "BmuLLu-9ZSf-eqpf-qR3H-23rQ-fIl7-Ouyl5X",
					VGTags:  "",
					VGUuid:  "JnCFQZ-TTfE-Ed2C-nKoH-yzPH-4fMA-CKwIv7",
					PVSize:  size10G,
					PEStart: peStart1Mi,
				},
				{
					PVName:  "/dev/vdc",
					VGName:  "vgtest",
					PVUsed:  "0 ",
					PVUuid:  "CmuLLu-9ZSf-eqpf-qR3H-23rQ-fIl7-Ouyl5X",
					VGTags:  "",
					VGUuid:  "JnCFQZ-TTfE-Ed2C-nKoH-yzPH-4fMA-CKwIv7",
					PVSize:  size10G,
					PEStart: peStart2Mi,
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
					VGAttr:   "wz--n-",
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
			// cspell:ignore mythinpool
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
			// cspell:ignore lvol0
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

// TestLvmStaticExtendedArgs pins three behaviors of the argv builder for
// every lvm.static invocation that the agent runs through nsenter:
//
//  1. for a non-empty argument list the function injects a single
//     "--config <LVMGlobalFilter + ' ' + LVMArchiveRetention>" pair
//     IMMEDIATELY after the LVM subcommand (args[0]). Position matters
//     here: lvm.static >= 2.03.41 rejects the command line as
//     "Specify options after a command" if --config precedes the
//     subcommand, see the function comment for the rationale;
//
//  2. the empty-args branch is preserved verbatim: no --config is
//     injected and the resulting argv is identical to the legacy
//     nsenter+lvm.static prefix. This guards against regressions for
//     diagnostic invocations like `lvm.static version`;
//
//  3. the original args[1:] tail is preserved without reordering.
//
// The expected argv always begins with the fixed nsenter prefix
// nsentrerExpendedArgs emits ("-t 1 -m -u -i -n -p -- /<NSENTERCmd
// path>/lvm.static"), so the test simply rebuilds the full slice and
// compares with assert.Equal — any future drift in either the prefix
// or the --config placement fails loudly with a readable diff.
func TestLvmStaticExtendedArgs(t *testing.T) {
	configValue := internal.LVMGlobalFilter + " " + internal.LVMArchiveRetention
	prefix := []string{"-t", "1", "-m", "-u", "-i", "-n", "-p", "--", internal.LVMCmd}

	tests := []struct {
		name string
		args []string
		want []string
	}{
		{
			name: "vgs_with_options_gets_config_after_subcommand",
			args: []string{"vgs", "-o", "+uuid,tags,shared,vg_attr,vg_extent_size", "--units", "B", "--nosuffix", "--reportformat", "json"},
			want: append(append([]string{}, prefix...),
				"vgs", "--config", configValue,
				"-o", "+uuid,tags,shared,vg_attr,vg_extent_size", "--units", "B", "--nosuffix", "--reportformat", "json",
			),
		},
		{
			name: "pvscan_cache_gets_config_after_subcommand",
			args: []string{"pvscan", "--cache"},
			want: append(append([]string{}, prefix...),
				"pvscan", "--config", configValue,
				"--cache",
			),
		},
		{
			name: "single_subcommand_still_gets_config",
			args: []string{"vgscan"},
			want: append(append([]string{}, prefix...),
				"vgscan", "--config", configValue,
			),
		},
		{
			name: "vgcreate_with_pv_list_preserves_tail_order",
			args: []string{"vgcreate", "vg-data", "/dev/sda", "/dev/sdb", "--addtag", "storage.deckhouse.io/enabled=true"},
			want: append(append([]string{}, prefix...),
				"vgcreate", "--config", configValue,
				"vg-data", "/dev/sda", "/dev/sdb", "--addtag", "storage.deckhouse.io/enabled=true",
			),
		},
		{
			name: "empty_args_no_config_injected",
			args: nil,
			want: append([]string{}, prefix...),
		},
		{
			name: "empty_slice_args_no_config_injected",
			args: []string{},
			want: append([]string{}, prefix...),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := lvmStaticExtendedArgs(tt.args)
			assert.Equal(t, tt.want, got)
		})
	}

	t.Run("config_value_contains_all_foreign_prefixes_and_retention", func(t *testing.T) {
		// Defensive cross-check: --config must reject all four foreign
		// device prefixes the post-filter knows about (rbd/drbd/nbd/loop)
		// and must cap /etc/lvm/archive growth. If either drops out due
		// to a refactor, this assertion catches it before the next scan
		// loop silently regresses.
		got := lvmStaticExtendedArgs([]string{"vgs"})
		// configValue is at index len(prefix)+2 (prefix..., "vgs",
		// "--config", configValue).
		require := len(prefix) + 2
		if !assert.Greater(t, len(got), require, "argv too short — --config not injected?") {
			return
		}
		actualConfig := got[require]
		for _, p := range internal.ForeignDeviceBasePrefixes {
			assert.Contains(t, actualConfig, "/dev/"+p,
				"global_filter must reject /dev/%s* canonical paths", p)
		}
		assert.Contains(t, actualConfig, "backup/retain_min=",
			"--config must cap /etc/lvm/archive growth via backup/retain_min")
		assert.Contains(t, actualConfig, "backup/retain_days=",
			"--config must cap /etc/lvm/archive growth via backup/retain_days")
	})
}
