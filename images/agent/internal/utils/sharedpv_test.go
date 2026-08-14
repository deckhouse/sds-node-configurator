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
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeDM struct {
	dm                 string
	uuid               string
	name               string
	logical, physical  int
	discardGranularity int
}

func fakeSysBlock(t *testing.T, devices ...fakeDM) {
	t.Helper()

	root := t.TempDir()
	for _, device := range devices {
		base := filepath.Join(root, device.dm)
		require.NoError(t, os.MkdirAll(filepath.Join(base, "dm"), 0o755))
		require.NoError(t, os.MkdirAll(filepath.Join(base, "queue"), 0o755))
		write := func(path, value string) {
			require.NoError(t, os.WriteFile(filepath.Join(base, path), []byte(value+"\n"), 0o644))
		}
		write("dm/uuid", device.uuid)
		write("dm/name", device.name)
		if device.logical > 0 {
			write("queue/logical_block_size", itoa(device.logical))
		}
		if device.physical > 0 {
			write("queue/physical_block_size", itoa(device.physical))
		}
		if device.discardGranularity > 0 {
			write("queue/discard_granularity", itoa(device.discardGranularity))
		}
	}

	old := SysBlockRoot
	SysBlockRoot = root
	t.Cleanup(func() { SysBlockRoot = old })
}

func itoa(v int) string {
	return strconv.Itoa(v)
}

func TestResolveWWIDsFindsTheMapNotThePath(t *testing.T) {
	fakeSysBlock(t,
		fakeDM{dm: "dm-3", uuid: "mpath-36c89f1a1", name: "mpathi", logical: 512, physical: 512, discardGranularity: 8192},
		// A logical volume is a dm device too and must not be mistaken for a LUN.
		fakeDM{dm: "dm-4", uuid: "LVM-abcdef", name: "vgshared-vol1", logical: 512, physical: 512},
	)

	found, missing, err := ResolveWWIDs([]string{"36c89f1a1"})
	require.NoError(t, err)
	assert.Empty(t, missing)
	require.Contains(t, found, "36c89f1a1")
	assert.Equal(t, "/dev/mapper/mpathi", found["36c89f1a1"].Path,
		"a Volume Group on /dev/sdX loses its physical volume the moment a path flaps")
}

func TestResolveWWIDsIsCaseInsensitive(t *testing.T) {
	fakeSysBlock(t, fakeDM{dm: "dm-3", uuid: "mpath-36c89f1a1", name: "mpathi", logical: 512, physical: 512})

	found, missing, err := ResolveWWIDs([]string{"36C89F1A1"})
	require.NoError(t, err)
	assert.Empty(t, missing, "an identifier copied out of an array's UI is the same identifier")
	assert.Len(t, found, 1)
}

func TestResolveWWIDsAcceptsTheDiscoverySpelling(t *testing.T) {
	// The same LUN, written the way a SCSIDevice carries it. Comparing the
	// strings as written would leave the group waiting for a LUN that is there.
	fakeSysBlock(t, fakeDM{dm: "dm-3", uuid: "mpath-36c89f1a1", name: "mpathi", logical: 512, physical: 512})

	found, missing, err := ResolveWWIDs([]string{"naa.6c89f1a1"})
	require.NoError(t, err)
	assert.Empty(t, missing)
	require.Len(t, found, 1)
	assert.Equal(t, "/dev/mapper/mpathi", found["naa.6c89f1a1"].Path,
		"the device is keyed by the identifier as the caller wrote it")
}

func TestMissingLUNIsNotAnError(t *testing.T) {
	fakeSysBlock(t, fakeDM{dm: "dm-3", uuid: "mpath-aaa", name: "mpatha", logical: 512, physical: 512})

	found, missing, err := ResolveWWIDs([]string{"aaa", "bbb"})
	require.NoError(t, err)
	assert.Equal(t, []string{"bbb"}, missing, "a LUN may still be arriving")
	assert.Len(t, found, 1)
}

func TestGeometryClassFollowsLVMsRule(t *testing.T) {
	// 512e is 4K here, because that is how lvm2 decides: 4096 in either the
	// logical or the physical size.
	assert.True(t, SharedDevice{LogicalBlockSize: 512, PhysicalBlockSize: 4096}.FourKGeometry())
	assert.True(t, SharedDevice{LogicalBlockSize: 4096, PhysicalBlockSize: 4096}.FourKGeometry())
	assert.False(t, SharedDevice{LogicalBlockSize: 512, PhysicalBlockSize: 512}.FourKGeometry())
}

func TestMixedGeometryIsRefused(t *testing.T) {
	err := CheckSharedDeviceInvariants([]SharedDevice{
		{WWID: "a", LogicalBlockSize: 512, PhysicalBlockSize: 512},
		{WWID: "b", LogicalBlockSize: 512, PhysicalBlockSize: 4096},
	}, 0)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "geometry classes")
}

func TestSameClassDifferentSizesIsAccepted(t *testing.T) {
	// 512e and 4Kn are one class, and a group may span them.
	err := CheckSharedDeviceInvariants([]SharedDevice{
		{WWID: "a", LogicalBlockSize: 512, PhysicalBlockSize: 4096},
		{WWID: "b", LogicalBlockSize: 4096, PhysicalBlockSize: 4096},
	}, 0)

	assert.NoError(t, err)
}

func TestExtentSizeMustBeAMultipleOfTheUnmapGranularity(t *testing.T) {
	devices := []SharedDevice{{WWID: "a", LogicalBlockSize: 512, PhysicalBlockSize: 512, DiscardGranularity: 3 * 1024}}

	err := CheckSharedDeviceInvariants(devices, 4*1024*1024)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "discard would free nothing")

	// The Huawei array of the stand reports 8 KiB, which every sane extent size
	// is a multiple of.
	devices[0].DiscardGranularity = 8 * 1024
	assert.NoError(t, CheckSharedDeviceInvariants(devices, 4*1024*1024))
}

func TestUnknownGranularityDoesNotBlockCreation(t *testing.T) {
	// An array that reports nothing is not an array that discards nothing, and
	// refusing here would make the module unusable on it.
	err := CheckSharedDeviceInvariants(
		[]SharedDevice{{WWID: "a", LogicalBlockSize: 512, PhysicalBlockSize: 512}}, 4*1024*1024)
	assert.NoError(t, err)
}

func TestSortedPathsAreStable(t *testing.T) {
	devices := map[string]SharedDevice{
		"b": {WWID: "b", Path: "/dev/mapper/mpathb"},
		"a": {WWID: "a", Path: "/dev/mapper/mpatha"},
	}
	assert.Equal(t, []string{"/dev/mapper/mpatha", "/dev/mapper/mpathb"}, SortedPaths(devices),
		"a vgcreate retried after a failure has to be the same command")
}

func TestLVMSizeSpeaksLVMsNotation(t *testing.T) {
	// "4Mi" is a valid quantity everywhere in this module's API and a usage
	// error to lvm, which exits 3 and prints its help without naming the
	// argument it disliked. Found on the stand exactly that way.
	assert.Equal(t, "4096k", lvmSize("4Mi"))
	assert.Equal(t, "1048576k", lvmSize("1Gi"))
	assert.Equal(t, "4m", lvmSize("4m"),
		"lvm's own spelling means mebibytes; as a quantity it would be four thousandths of a byte")
	assert.Equal(t, "nonsense", lvmSize("nonsense"), "lvm rejects it with a better message than this could")
}

func TestLVMSizeConvertsABareByteCount(t *testing.T) {
	// The driver writes the requested size as a plain byte count, which is a
	// valid quantity — and a bare number to lvm means megabytes, so unconverted
	// it asks for a volume a million times too large.
	assert.Equal(t, "1048576k", lvmSize("1073741824"))
}
