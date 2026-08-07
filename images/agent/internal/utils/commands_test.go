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
	"bytes"
	"context"
	"regexp"
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
		// Defensive cross-check: --config must reject all foreign
		// device prefixes the post-filter knows about (rbd/drbd/nbd)
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

// TestUdevadmTriggerExtendedArgs pins the argv layout that the agent uses
// to refresh the host udev DB after pvcreate/vgcreate. Three invariants are
// verified explicitly:
//
//  1. the first three udevadm tokens are `trigger --action=change` — any
//     accidental drop of `--action=change` would make the trigger emit the
//     default "add" event and `lsblk` would still report fstype: null for
//     the freshly created PVs;
//
//  2. a literal `--` end-of-options separator sits between the udevadm
//     flags and the path list. This guards the (cheap to defend) edge case
//     of a BlockDevice whose Status.Path begins with '-' and would
//     otherwise be parsed by udevadm as a flag;
//
//  3. the whole command is wrapped by the standard nsenter prefix
//     `-t 1 -m -u -i -n -p -- /<NSENTERCmd path>/udevadm`. udevadm relies
//     on the host /run/udev socket which is reachable only inside PID 1's
//     mount/ipc namespaces, so dropping any namespace flag would break the
//     trigger silently.
//
// Path-list ordering must be preserved verbatim (no sort, no dedup):
// downstream metrics group by path string, and reordering would break log
// correlation.
func TestUdevadmTriggerExtendedArgs(t *testing.T) {
	prefix := []string{"-t", "1", "-m", "-u", "-i", "-n", "-p", "--", "udevadm"}

	tests := []struct {
		name  string
		paths []string
		want  []string
	}{
		{
			name:  "single_path",
			paths: []string{"/dev/sda"},
			want: append(append([]string{}, prefix...),
				"trigger", "--action=change", "--", "/dev/sda",
			),
		},
		{
			name:  "multiple_paths_preserve_order",
			paths: []string{"/dev/sdc", "/dev/sda", "/dev/sdb"},
			want: append(append([]string{}, prefix...),
				"trigger", "--action=change", "--", "/dev/sdc", "/dev/sda", "/dev/sdb",
			),
		},
		{
			name:  "path_starting_with_dash_is_isolated_by_end_of_options",
			paths: []string{"--help"},
			want: append(append([]string{}, prefix...),
				"trigger", "--action=change", "--", "--help",
			),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := udevadmTriggerExtendedArgs(tt.paths)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestUdevadmTriggerEmptyPathsIsNoop pins the defensive early-return for an
// empty path list. Without it, `udevadm trigger --action=change` (no
// positional args) would enqueue a change uevent for EVERY block device on
// the host, producing a burst that disrupts other udev consumers
// (multipathd, drbd, …). The function must return ("", nil) and must not
// reach exec.CommandContext.
func TestUdevadmTriggerEmptyPathsIsNoop(t *testing.T) {
	c := commands{}

	t.Run("nil_paths", func(t *testing.T) {
		out, err := c.UdevadmTrigger(context.Background(), nil)
		assert.NoError(t, err)
		assert.Empty(t, out)
	})

	t.Run("empty_paths", func(t *testing.T) {
		out, err := c.UdevadmTrigger(context.Background(), []string{})
		assert.NoError(t, err)
		assert.Empty(t, out)
	})
}

func TestFilterStdErr(t *testing.T) {
	const cmd = "lvextend -l 100%VG /dev/vg/lv"

	tests := []struct {
		name     string
		stdErr   string
		filtered bool // true if the whole output is expected to be filtered out
	}{
		{
			name:     "old_lvm_no_size_change",
			stdErr:   "  No size change.",
			filtered: true,
		},
		{
			name:     "new_lvm_matches_existing_size",
			stdErr:   "  New size (953801 extents) matches existing size (953801 extents).",
			filtered: true,
		},
		{
			name:     "regex_version_mismatch",
			stdErr:   "Regex version mismatch, expected: 10.42 2022-12-11 actual: 10.34 2019-11-21",
			filtered: true,
		},
		{
			name:     "file_descriptor_leaked",
			stdErr:   "File descriptor 7 leaked on lvm invocation. Parent PID 1: /opt/deckhouse/sds/bin/nsenter",
			filtered: true,
		},
		{
			name:     "real_error_is_kept",
			stdErr:   "  Volume group \"vg\" not found.",
			filtered: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			buf.WriteString(tt.stdErr)

			result := filterStdErr(cmd, buf, benignResizeStdErr)

			if tt.filtered {
				assert.Equal(t, 0, result.Len(), "expected stderr to be fully filtered, got: %q", result.String())
			} else {
				assert.Contains(t, result.String(), tt.stdErr)
			}
		})
	}
}

// The benign allowlist is per command, and the split is the whole point: a
// resize that changed nothing is a normal state for lvextend and is not a thing
// pvs or pvcreate can report. Keeping one global set meant a pattern added for a
// write command silently widened what counts as success for the PV listing — and
// that listing is the sole gate in front of every destructive file-device
// decision (cleanupFileDevices unlinks backing files on the strength of it).
//
// Asserted here rather than left to review, the same way the conditions
// watcher's acceptableReasons membership is.
// A Volume Group this module creates carries two tags, and both have to arrive:
// storage.deckhouse.io/enabled=true is what makes it managed, and
// storage.deckhouse.io/lvmVolumeGroupName is what says whose it is. The second one
// is what the discoverer reads to re-import a Volume Group under its original name,
// what ClassifyLoopVGs needs to tell a file-backed Volume Group of ours from an
// image somebody attached with losetup, and what buildFileDeviceFromLoopPV refuses
// to claim a loop PV without.
//
// The shared variant used to emit "--addtag" twice in a row, so vgcreate took the
// second flag as the first one's value and the lvmVolumeGroupName tag as a
// positional device path: the tag never landed. Asserting the argv is cheaper than
// discovering that on a cluster, and it is why the two variants now share
// vgCreateArgs.
func TestVGCreateArgs(t *testing.T) {
	const (
		vgName  = "vg-data"
		lvgName = "lvg-a"
	)
	pvs := []string{"/dev/sda", "/dev/loop0"}

	for _, tt := range []struct {
		name   string
		shared bool
		want   []string
	}{
		{
			name:   "local",
			shared: false,
			want: []string{
				"vgcreate", vgName, "/dev/sda", "/dev/loop0",
				"--addtag", "storage.deckhouse.io/enabled=true",
				"--addtag", "storage.deckhouse.io/lvmVolumeGroupName=" + lvgName,
			},
		},
		{
			name:   "shared",
			shared: true,
			want: []string{
				"vgcreate", "--shared", vgName, "/dev/sda", "/dev/loop0",
				"--addtag", "storage.deckhouse.io/enabled=true",
				"--addtag", "storage.deckhouse.io/lvmVolumeGroupName=" + lvgName,
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got := vgCreateArgs(vgName, lvgName, tt.shared, pvs)
			assert.Equal(t, tt.want, got)

			// Stated separately from the exact-argv assertion, because this is the
			// property that actually broke: one flag per tag, never two in a row.
			var flags int
			for i, arg := range got {
				if arg != "--addtag" {
					continue
				}
				flags++
				if assert.Less(t, i+1, len(got), "--addtag must be followed by a value") {
					assert.NotEqual(t, "--addtag", got[i+1], "--addtag must not be its own value")
				}
			}
			assert.Equal(t, 2, flags, "exactly two tags: the managed marker and the owning LVMVolumeGroup")
		})
	}

	t.Run("a Volume Group with no PVs still gets both tags", func(t *testing.T) {
		got := vgCreateArgs(vgName, lvgName, false, nil)
		assert.Equal(t, []string{
			"vgcreate", vgName,
			"--addtag", "storage.deckhouse.io/enabled=true",
			"--addtag", "storage.deckhouse.io/lvmVolumeGroupName=" + lvgName,
		}, got)
	})
}

func TestBenignStdErrSetsAreScopedPerCommand(t *testing.T) {
	const (
		noSizeChange = "  No size change."
		matchesSize  = "  New size (953801 extents) matches existing size (953801 extents)."
		leakedFD     = "File descriptor 7 leaked on lvm.static invocation. Parent PID 1: /opt/deckhouse/sds/bin/nsenter"
		versionSkew  = "Regex version mismatch, expected: 10.42 2022-12-11 actual: 10.34 2019-11-21"
	)

	filter := func(allow []*regexp.Regexp, line string) string {
		var buf bytes.Buffer
		buf.WriteString(line)
		out := filterStdErr("pvs", buf, allow)
		return out.String()
	}

	t.Run("nsenter artefacts are benign everywhere", func(t *testing.T) {
		for _, line := range []string{leakedFD, versionSkew} {
			assert.Empty(t, filter(benignAlwaysStdErr, line), "line must be benign for any command: %q", line)
			assert.Empty(t, filter(benignResizeStdErr, line), "line must be benign for any command: %q", line)
		}
	})

	t.Run("a no-op resize is benign only for a resize", func(t *testing.T) {
		for _, line := range []string{noSizeChange, matchesSize} {
			assert.Empty(t, filter(benignResizeStdErr, line),
				"lvextend must tolerate its own no-op: %q", line)
			assert.NotEmpty(t, filter(benignAlwaysStdErr, line),
				"pvs/pvcreate/pvresize have no such no-op; swallowing it would let a partial listing gate an unlink: %q", line)
		}
	})

	t.Run("a real diagnostic is never benign", func(t *testing.T) {
		const realErr = `  Couldn't find device with uuid abcd-1234.`
		assert.NotEmpty(t, filter(benignAlwaysStdErr, realErr))
		assert.NotEmpty(t, filter(benignResizeStdErr, realErr))
	})

	t.Run("the no-op resize pattern is anchored", func(t *testing.T) {
		// Unanchored, this would swallow an error message that merely quotes
		// another command's output.
		const quoting = `  Failed: lvextend said "New size (1 extents) matches existing size (1 extents)." and then aborted`
		assert.NotEmpty(t, filter(benignResizeStdErr, quoting),
			"a line embedding the no-op wording is not itself a no-op")
	})
}

// GetLoopBackingFile must strip the " (deleted)" marker losetup appends when the
// backing file was unlinked while the loop is still attached — without stripping
// it, IsManagedFileDevicePath would not recognise the basename and cleanup would
// refuse to detach the loop, stranding the minor on the node — and it must report
// the marker separately, because provisioning has to know the file is gone in
// order not to create a second one at the same path.
func TestParseBackingFile(t *testing.T) {
	tests := []struct {
		name        string
		in          string
		wantPath    string
		wantDeleted bool
	}{
		{"plain", "/data/sds-vg-a.d0.img", "/data/sds-vg-a.d0.img", false},
		{"trailing newline", "/data/sds-vg-a.d0.img\n", "/data/sds-vg-a.d0.img", false},
		{"deleted marker", "/data/sds-vg-a.d0.img (deleted)", "/data/sds-vg-a.d0.img", true},
		{"deleted marker with newline", "/data/sds-vg-a.d0.img (deleted)\n", "/data/sds-vg-a.d0.img", true},
		{"empty", "", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseBackingFile(tt.in)
			assert.Equal(t, tt.wantPath, got.Path)
			assert.Equal(t, tt.wantDeleted, got.Deleted)
			if tt.wantPath != "" {
				assert.True(t, IsManagedFileDevicePath(got.Path, "vg-a"),
					"the parsed path must still be recognised as managed")
			}
		})
	}
}

// parseStatfsSpace turns the "<block-size> <total-blocks> <available-blocks>"
// output of `stat -f -c "%S %b %a"` into a FilesystemSpace; GetFilesystemSpace
// relies on it both to refuse an oversized backing file before fallocate fills
// the node and to work out the reserve, which is a share of the total.
func TestParseStatfsSpace(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    internal.FilesystemSpace
		wantErr bool
	}{
		{"plain", "4096 5000 1000", internal.FilesystemSpace{AvailableBytes: 4096 * 1000, TotalBytes: 4096 * 5000}, false},
		{"trailing newline", "4096 5000 1000\n", internal.FilesystemSpace{AvailableBytes: 4096 * 1000, TotalBytes: 4096 * 5000}, false},
		{"extra whitespace", "  4096   5000   1000  ", internal.FilesystemSpace{AvailableBytes: 4096 * 1000, TotalBytes: 4096 * 5000}, false},
		{"full filesystem", "4096 5000 0", internal.FilesystemSpace{AvailableBytes: 0, TotalBytes: 4096 * 5000}, false},
		{"empty", "", internal.FilesystemSpace{}, true},
		{"two fields", "4096 1000", internal.FilesystemSpace{}, true},
		{"too many fields", "4096 5000 1000 7", internal.FilesystemSpace{}, true},
		{"non-numeric block size", "abc 5000 1000", internal.FilesystemSpace{}, true},
		{"non-numeric total", "4096 abc 1000", internal.FilesystemSpace{}, true},
		{"non-numeric available", "4096 5000 abc", internal.FilesystemSpace{}, true},
		{"negative available", "4096 5000 -1", internal.FilesystemSpace{}, true},
		// A zero total must never reach the caller: it reads TotalBytes <= 0 as
		// "size unknown" and skips the reserve, so a successful stat that said
		// zero would silently disable the guard on the node's root filesystem.
		{"zero total", "4096 0 1000", internal.FilesystemSpace{}, true},
		{"zero block size", "0 5000 1000", internal.FilesystemSpace{}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseStatfsSpace(tt.in)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// An lvm report is not necessarily pure JSON. lvm prints part of its diagnostics
// on stdout — log_print/log_warn advisories — and with --reportformat json they
// land wherever lvm emitted them, which is INSIDE the report. This is the verbatim
// stdout of `lvm vgs --reportformat json` from a node whose /etc/lvm/archive had
// grown past lvm's pruning threshold; before reportJSON it failed to parse on
// every scan with `invalid character 'C' looking for beginning of value`, and that
// stopped the whole cache-filling loop on that node.
func TestReportSurvivesLVMAdvisoriesOnStdout(t *testing.T) {
	const polluted = `  {
      "report": [
  Consider pruning ceph-vg VG archive with more then 12 MiB in 8929 files (check archiving is needed in lvm.conf).
  Consider pruning vg-1 VG archive with more then 1032 MiB in 11272 files (check archiving is needed in lvm.conf).
          {
              "vg": [
                  {"vg_name":"vg-1", "pv_count":"2", "lv_count":"329", "snap_count":"0", "vg_attr":"wz--n-", "vg_size":"11522252210176", "vg_free":"1322367582208", "vg_uuid":"GzKqS7-TW1a-W21I-yZK2-FlTu-WC7c-Iz24OW", "vg_tags":"storage.deckhouse.io/enabled=true", "vg_shared":"", "vg_extent_size":"4194304"}
              ]
          }
      ]
  }
`

	t.Run("the report is parsed and nothing is lost", func(t *testing.T) {
		vgs, err := unmarshalVGs([]byte(polluted))
		assert.NoError(t, err)
		if assert.Len(t, vgs, 1) {
			assert.Equal(t, "vg-1", vgs[0].VGName)
			assert.Equal(t, "GzKqS7-TW1a-W21I-yZK2-FlTu-WC7c-Iz24OW", vgs[0].VGUUID)
			assert.Equal(t, int64(11522252210176), vgs[0].VGSize.Value())
		}
	})

	t.Run("a clean report is handed through untouched", func(t *testing.T) {
		const clean = `{"report":[{"vg":[{"vg_name":"vg-1","vg_uuid":"u-1"}]}]}`
		assert.Equal(t, []byte(clean), reportJSON([]byte(clean)))
	})

	t.Run("the same holds for the PV and LV reports", func(t *testing.T) {
		pvs, err := unmarshalPVs([]byte(`  {
      "report": [
  Consider pruning vg-1 VG archive with more then 1032 MiB in 11272 files (check archiving is needed in lvm.conf).
          {
              "pv": [
                  {"pv_name":"/dev/nvme0n1", "vg_name":"vg-1", "pv_size":"1", "pv_uuid":"pv-1", "vg_uuid":"vg-uuid-1"}
              ]
          }
      ]
  }
`))
		assert.NoError(t, err)
		if assert.Len(t, pvs, 1) {
			assert.Equal(t, "/dev/nvme0n1", pvs[0].PVName)
		}

		lvs, err := unmarshalLVs([]byte(`  {
      "report": [
  Consider pruning vg-1 VG archive with more then 1032 MiB in 11272 files (check archiving is needed in lvm.conf).
          {
              "lv": [
                  {"lv_name":"pvc-1", "vg_name":"vg-1", "vg_uuid":"vg-uuid-1", "lv_size":"1"}
              ]
          }
      ]
  }
`))
		assert.NoError(t, err)
		if assert.Len(t, lvs, 1) {
			assert.Equal(t, "pvc-1", lvs[0].LVName)
		}
	})

	t.Run("a parse failure names what lvm printed", func(t *testing.T) {
		// Not an advisory and not JSON: whatever this is, the error has to carry it,
		// because the buffer it came from is dropped right after.
		_, err := unmarshalVGs([]byte("\x00\x01 not a report at all"))
		if assert.Error(t, err) {
			assert.Contains(t, err.Error(), "lvm printed on stdout:",
				"the parse error must quote stdout, otherwise the only way to learn what lvm said is to reproduce the command on the node")
			assert.Contains(t, err.Error(), "not a report at all")
		}
	})
}

// lvm prints the duplicate-VG-name warning on every invocation, whatever object
// the command asked about, so it must never become that object's health. A `data`
// VG inside one guest's disk colliding with another guest's `data` is what took
// this node's own vg-1 to NotReady.
func TestObjectDiagnosticsDropsNodeWideWarnings(t *testing.T) {
	const (
		dupWarning = `  WARNING: VG name data is used by VGs TNiDBi-Y1g2-GUM5-9Gov-WuN5-GR3j-8zz7aE and x4wwVz-2g1i-7ntB-eM0Q-88tg-zP08-e0bGiR.`
		dupHint    = `  Fix duplicate VG names with vgrename uuid, a device filter, or system IDs.`
		realIssue  = `  Couldn't find device with uuid abcd-1234.`
	)

	filter := func(lines ...string) string {
		var buf bytes.Buffer
		for _, l := range lines {
			buf.WriteString(l + "\n")
		}
		out := ObjectDiagnostics("vgs vg-1", buf)
		return out.String()
	}

	t.Run("a node-wide duplicate-name warning is not about the queried object", func(t *testing.T) {
		assert.Empty(t, filter(dupWarning, dupHint))
		assert.Empty(t, filter(dupWarning, dupHint, dupWarning, dupHint))
	})

	t.Run("a real diagnostic still survives, alongside the dropped ones", func(t *testing.T) {
		got := filter(dupWarning, dupHint, realIssue)
		assert.Contains(t, got, "Couldn't find device with uuid")
		assert.NotContains(t, got, "is used by VGs")
	})

	t.Run("the patterns are anchored to a whole line", func(t *testing.T) {
		const quoting = `  Failed: vgs said "WARNING: VG name data is used by VGs a and b." and then aborted`
		assert.NotEmpty(t, filter(quoting),
			"a line embedding the warning is a diagnostic of its own")
	})
}
