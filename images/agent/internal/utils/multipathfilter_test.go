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
	"os"
	"path/filepath"
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
	ResetPathlessMultipathCache()
	t.Cleanup(func() {
		sysBlockPath = saved
		ResetPathlessMultipathCache()
	})

	return root
}

func TestPathlessMultipathDevices(t *testing.T) {
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

	got := PathlessMultipathDevices()

	assert.Equal(t, []string{"/dev/dm-2", "/dev/mapper/mpathbb"}, got,
		"only the pathless multipath map is reported, by both spellings LVM may scan")
}

func TestPathlessMultipathDevices_NoSysfs(t *testing.T) {
	root := withFakeSysBlock(t)
	require.NoError(t, os.RemoveAll(root))

	assert.Empty(t, PathlessMultipathDevices(),
		"an unreadable /sys/block yields no rejects rather than an error: the filter must stay valid")
}

func TestLVMGlobalFilterForRejectsPathlessMaps(t *testing.T) {
	filter := internal.LVMGlobalFilterFor(
		[]string{"/dev/loop7"},
		[]string{"/dev/dm-2", "/dev/mapper/mpathbb"},
	)

	assert.Equal(t,
		`devices/global_filter=["a|^/dev/loop7$|","r|^/dev/dm-2$|","r|^/dev/mapper/mpathbb$|","r|^/dev/rbd|","r|^/dev/drbd|","r|^/dev/nbd|","r|^/dev/loop|"]`,
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
		`devices/global_filter=["r|^/dev/sda$|","r|^/dev/sdc1$|","r|^/dev/nvme0n1p2$|","r|^/dev/block/253:5$|","r|^/dev/dm-5$|","r|^/dev/mapper/mpatha-part1$|","r|^/dev/rbd|","r|^/dev/drbd|","r|^/dev/nbd|","r|^/dev/loop|"]`,
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
		`devices/global_filter=["r|^/dev/mapper/vg0-data[.]1$|","r|^/dev/mapper/a[+]b$|","r|^/dev/mapper/mpath[.]a[+]1$|","r|^/dev/rbd|","r|^/dev/drbd|","r|^/dev/nbd|","r|^/dev/loop|"]`,
		filter)
}

func TestLVMGlobalFilterForEmptyInputs(t *testing.T) {
	assert.Equal(t, internal.LVMGlobalFilter, internal.LVMGlobalFilterFor(nil, nil))
	assert.Equal(t, internal.LVMGlobalFilterAcceptingLoops([]string{"/dev/loop3"}),
		internal.LVMGlobalFilterFor([]string{"/dev/loop3"}, nil),
		"the loop-only builder stays a special case of the general one")
}
