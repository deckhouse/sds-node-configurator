/*
Copyright 2026 Flant JSC

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
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
)

// fakeDM writes the sysfs shape the scan reads: dm/uuid, dm/name and a slaves
// directory holding one entry per path.
func fakeDM(t *testing.T, root, dev, uuid, name string, paths int) {
	t.Helper()

	dmDir := filepath.Join(root, dev, "dm")
	require.NoError(t, os.MkdirAll(dmDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dmDir, "uuid"), []byte(uuid+"\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dmDir, "name"), []byte(name+"\n"), 0o644))

	slaves := filepath.Join(root, dev, "slaves")
	require.NoError(t, os.MkdirAll(slaves, 0o755))
	for i := 0; i < paths; i++ {
		require.NoError(t, os.MkdirAll(filepath.Join(slaves, "sd"+string(rune('a'+i))), 0o755))
	}
}

func withFakeSysBlock(t *testing.T) string {
	t.Helper()

	root := t.TempDir()
	saved := sysBlockPath
	sysBlockPath = root
	withMultipathStatus(t, "", errors.New("no dmsetup in tests"))
	ResetUnreadableMultipathCache()
	t.Cleanup(func() {
		sysBlockPath = saved
		ResetUnreadableMultipathCache()
	})

	return root
}

// withMultipathStatus makes the scan read a fixed status report instead of
// asking the host's dmsetup.
func withMultipathStatus(t *testing.T, report string, err error) {
	t.Helper()

	saved := multipathStatus
	multipathStatus = func() ([]byte, error) { return []byte(report), err }
	ResetUnreadableMultipathCache()
	t.Cleanup(func() {
		multipathStatus = saved
		ResetUnreadableMultipathCache()
	})
}

// The report of the client node the second shape was found on: mpathd and
// mpathg keep one failed path each behind queue_if_no_path, mpathf has one
// active path next to a failed one, mpathc has no path at all.
const statusAfterDrain = `mpathd: 0 4194304 multipath 2 0 0 0 1 1 E 0 1 2 8:112 F 1 0 1
mpathf: 0 2097152 multipath 2 0 0 0 1 1 A 0 2 2 65:64 F 1 0 1 8:0 A 0 0 1
mpathg: 0 4194304 multipath 2 0 0 0 1 1 E 0 1 2 65:240 F 1 0 1
mpathc: 0 2097152 multipath 0 0 0 0
`

func TestUnreadableMultipathDevices(t *testing.T) {
	root := withFakeSysBlock(t)

	// A multipath map that still has paths must not be skipped: LVM on top of a
	// SAN LUN is a supported layout and the agent has to keep seeing it.
	fakeDM(t, root, "dm-1", "mpath-3600healthy", "mpathaa", 2)
	// A multipath map with no paths left is the one that blocks readers.
	fakeDM(t, root, "dm-2", "mpath-3600dead", "mpathbb", 0)
	// Another dm target with no slaves is not a multipath map and is left alone.
	fakeDM(t, root, "dm-3", "CRYPT-LUKS2-abc", "luks-abc", 0)

	// A plain disk is not a dm device at all.
	require.NoError(t, os.MkdirAll(filepath.Join(root, "sda"), 0o755))

	got := UnreadableMultipathDevices()

	assert.Equal(t, []string{"/dev/dm-2", "/dev/mapper/mpathbb"}, got,
		"only the pathless multipath map is reported, by both spellings LVM may scan")
}

func TestUnreadableMultipathDevices_MapsWithOnlyFailedPaths(t *testing.T) {
	root := withFakeSysBlock(t)
	withMultipathStatus(t, statusAfterDrain, nil)

	fakeDM(t, root, "dm-70", "mpath-361452900326008fc40024000000004dd", "mpathd", 1)
	fakeDM(t, root, "dm-72", "mpath-361452900326008fc40024000000004df", "mpathf", 2)
	fakeDM(t, root, "dm-73", "mpath-361452900326008fc40024000000004e1", "mpathg", 1)
	fakeDM(t, root, "dm-71", "mpath-361452900326008fc40024000000004d9", "mpathc", 0)

	got := UnreadableMultipathDevices()

	assert.Equal(t, []string{
		"/dev/dm-70", "/dev/dm-71", "/dev/dm-73",
		"/dev/mapper/mpathc", "/dev/mapper/mpathd", "/dev/mapper/mpathg",
	}, got, "maps whose every path is failed are skipped like maps with no path; the map with an active path stays")
}

func TestUnreadableMultipathDevices_StatusUnavailableFallsBackToSysfs(t *testing.T) {
	root := withFakeSysBlock(t)
	withMultipathStatus(t, "", errors.New("dmsetup: command not found"))

	fakeDM(t, root, "dm-70", "mpath-3600failed", "mpathd", 1)
	fakeDM(t, root, "dm-71", "mpath-3600dead", "mpathc", 0)

	assert.Equal(t, []string{"/dev/dm-71", "/dev/mapper/mpathc"}, UnreadableMultipathDevices(),
		"without the status report only the slave-less map can be told apart, and that one still is")
}

func TestMapsWithoutActivePath(t *testing.T) {
	cases := []struct {
		name   string
		report string
		want   []string
	}{
		{
			name:   "client node after drain",
			report: statusAfterDrain,
			want:   []string{"mpathc", "mpathd", "mpathg"},
		},
		{
			name:   "queue_if_no_path feature tokens do not pass for path state",
			report: "mpathx: 0 8 multipath 2 1 queue_if_no_path 0 1 1 E 0 1 2 8:16 F 1 0 1 \n",
			want:   []string{"mpathx"},
		},
		{
			name:   "an active group state is not an active path",
			report: "mpathy: 0 8 multipath 0 0 1 1 A 0 1 2 8:32 F 3 0 1 \n",
			want:   []string{"mpathy"},
		},
		{
			name:   "two groups, the second one alive",
			report: "mpathz: 0 8 multipath 0 1 alua 2 2 E 0 1 2 8:48 F 1 0 1 A 0 1 2 8:64 A 0 0 1 \n",
			want:   nil,
		},
		{
			name:   "nothing to report",
			report: "No devices found\n",
			want:   nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := mapsWithoutActivePath([]byte(tc.report))

			names := make([]string, 0, len(got))
			for name := range got {
				names = append(names, name)
			}
			sort.Strings(names)
			if tc.want == nil {
				assert.Empty(t, names)
				return
			}
			assert.Equal(t, tc.want, names)
		})
	}
}

func TestUnreadableMultipathDevices_NoSysfs(t *testing.T) {
	root := withFakeSysBlock(t)
	require.NoError(t, os.RemoveAll(root))

	assert.Empty(t, UnreadableMultipathDevices(),
		"an unreadable /sys/block yields no rejects rather than an error: the filter must stay valid")
}

func TestLVMGlobalFilterForRejectsPathlessMaps(t *testing.T) {
	filter := internal.LVMGlobalFilterFor(
		[]string{"/dev/loop7"},
		[]string{"/dev/dm-2", "/dev/mapper/mpathbb"},
	)

	assert.Equal(t,
		`devices/global_filter=["a|^/dev/loop7$|","r|^/dev/dm-2$|","r|^/dev/mapper/mpathbb$|","r|^/dev/rbd|","r|^/dev/drbd|","r|^/dev/nbd|","r|^/dev/loop|","r|^/dev/mapper/shlun-|"]`,
		filter,
		"accepts first, then the named rejects, then the static foreign-device rejects")
}

func TestLVMGlobalFilterForRefusesMangledPaths(t *testing.T) {
	// The value is spliced into a config string whose field separator is `|`, so
	// anything that could break out of the pattern is dropped rather than escaped.
	filter := internal.LVMGlobalFilterFor(nil, []string{
		`/dev/mapper/evil|","a|.*`,
		"/dev/dm-",
		`/dev/sda"`,
		"/dev/sd a",
		"/dev/mapper/[x]",
		"../../etc/passwd",
		"/etc/passwd",
	})

	assert.Equal(t, internal.LVMGlobalFilter, filter,
		"no candidate is a canonical device path, so the filter stays the static one")
	assert.False(t, strings.Contains(filter, "evil"), "a mangled path never reaches the filter")
}

func TestLVMGlobalFilterForAcceptsEverySpellingOfAPublishedDevice(t *testing.T) {
	// The published-devices scan names a device under every spelling lvm may
	// reach it by, whole disks and partitions included; all of them are legal.
	filter := internal.LVMGlobalFilterFor(nil, []string{
		"/dev/sda", "/dev/sdc1", "/dev/nvme0n1p2", "/dev/block/253:5", "/dev/dm-5", "/dev/mapper/mpatha-part1",
	})

	assert.Equal(t,
		`devices/global_filter=["r|^/dev/sda$|","r|^/dev/sdc1$|","r|^/dev/nvme0n1p2$|","r|^/dev/block/253:5$|","r|^/dev/dm-5$|","r|^/dev/mapper/mpatha-part1$|","r|^/dev/rbd|","r|^/dev/drbd|","r|^/dev/nbd|","r|^/dev/loop|","r|^/dev/mapper/shlun-|"]`,
		filter)
}

func TestLVMGlobalFilterForEscapesRegexMetacharactersInNames(t *testing.T) {
	// A device-mapper name may carry `.` and `+`, and both are regex
	// metacharacters. Left as they are, `r|^/dev/mapper/vg0-data.1$|` would also
	// reject /dev/mapper/vg0-dataX1 — a device of the node's own, taken out over a
	// lookalike. Bracket expressions pin the rule to the one path it names.
	filter := internal.LVMGlobalFilterFor(nil, []string{
		"/dev/mapper/vg0-data.1", "/dev/mapper/a+b", "/dev/mapper/mpath.a+1",
	})

	assert.Equal(t,
		`devices/global_filter=["r|^/dev/mapper/vg0-data[.]1$|","r|^/dev/mapper/a[+]b$|","r|^/dev/mapper/mpath[.]a[+]1$|","r|^/dev/rbd|","r|^/dev/drbd|","r|^/dev/nbd|","r|^/dev/loop|","r|^/dev/mapper/shlun-|"]`,
		filter)
}

func TestLVMGlobalFilterForEmptyInputs(t *testing.T) {
	assert.Equal(t, internal.LVMGlobalFilter, internal.LVMGlobalFilterFor(nil, nil))
	assert.Equal(t, internal.LVMGlobalFilterAcceptingLoops([]string{"/dev/loop3"}),
		internal.LVMGlobalFilterFor([]string{"/dev/loop3"}, nil),
		"the loop-only builder stays a special case of the general one")
}
