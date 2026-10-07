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
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
)

// A fake node: kubelet's volumeDevices tree, /sys/class/block and /sys/dev/block,
// all under temporary directories. A device node needs CAP_MKNOD, so the tree
// holds regular files whose content is "major:minor" and statDeviceNode is
// swapped for one that reads them.
type fakeNode struct {
	t          *testing.T
	kubelet    string
	classBlock string
	devBlock   string
}

func newFakeNode(t *testing.T) *fakeNode {
	t.Helper()

	n := &fakeNode{
		t:          t,
		kubelet:    t.TempDir(),
		classBlock: t.TempDir(),
		devBlock:   t.TempDir(),
	}

	savedClass, savedDev, savedStat := sysClassBlockPath, sysDevBlockPath, statDeviceNode
	savedRoot := PublishedDevicesRoot()
	sysClassBlockPath, sysDevBlockPath = n.classBlock, n.devBlock
	statDeviceNode = func(p string) (uint32, uint32, bool, error) {
		raw, err := os.ReadFile(p)
		if err != nil {
			return 0, 0, false, err
		}
		var major, minor uint32
		if _, err := fmt.Sscanf(strings.TrimSpace(string(raw)), "%d:%d", &major, &minor); err != nil {
			// A regular file that is not a device node.
			return 0, 0, false, nil
		}
		return major, minor, true, nil
	}
	SetPublishedDevicesRoot(n.kubelet)

	t.Cleanup(func() {
		sysClassBlockPath, sysDevBlockPath, statDeviceNode = savedClass, savedDev, savedStat
		SetPublishedDevicesRoot(savedRoot)
	})

	return n
}

// device registers kName in sysfs with the given numbers; dmName, when not empty,
// makes it a device-mapper device.
func (n *fakeNode) device(kName string, major, minor int, dmName string) {
	n.t.Helper()

	dir := filepath.Join(n.classBlock, kName)
	require.NoError(n.t, os.MkdirAll(dir, 0o755))
	require.NoError(n.t, os.WriteFile(filepath.Join(dir, "dev"), []byte(fmt.Sprintf("%d:%d\n", major, minor)), 0o644))
	if dmName != "" {
		require.NoError(n.t, os.MkdirAll(filepath.Join(dir, "dm"), 0o755))
		require.NoError(n.t, os.WriteFile(filepath.Join(dir, "dm", "name"), []byte(dmName+"\n"), 0o644))
	}
	// /sys/dev/block/M:m -> ../../devices/.../<kName>; only the basename is read.
	require.NoError(n.t, os.Symlink(filepath.Join("..", "devices", kName), filepath.Join(n.devBlock, fmt.Sprintf("%d:%d", major, minor))))
}

// partition registers a kernel partition of parent: a subdirectory carrying a
// `partition` file, as sysfs lays it out.
func (n *fakeNode) partition(parent, kName string, major, minor int) {
	n.t.Helper()

	n.device(kName, major, minor, "")
	dir := filepath.Join(n.classBlock, parent, kName)
	require.NoError(n.t, os.MkdirAll(dir, 0o755))
	require.NoError(n.t, os.WriteFile(filepath.Join(dir, "partition"), []byte("1\n"), 0o644))
}

// holder registers kName as stacked on top of parent, the way a kpartx partition
// of a multipath map or an activated Logical Volume shows up in holders/.
func (n *fakeNode) holder(parent, kName string, major, minor int, dmName string) {
	n.t.Helper()

	n.device(kName, major, minor, dmName)
	require.NoError(n.t, os.MkdirAll(filepath.Join(n.classBlock, parent, "holders", kName), 0o755))
}

// publish writes the node kubelet leaves at publish/<pv>/<podUID>.
func (n *fakeNode) publish(pv, podUID string, major, minor int) {
	n.t.Helper()

	dir := filepath.Join(n.kubelet, PublishedSubdir, pv)
	require.NoError(n.t, os.MkdirAll(dir, 0o755))
	require.NoError(n.t, os.WriteFile(filepath.Join(dir, podUID), []byte(fmt.Sprintf("%d:%d", major, minor)), 0o600))
}

// stage writes what a driver leaves at staging/<pv>: here a node right under it.
func (n *fakeNode) stage(pv, entry string, major, minor int) {
	n.t.Helper()

	dir := filepath.Join(n.kubelet, StagingSubdir, pv)
	require.NoError(n.t, os.MkdirAll(dir, 0o755))
	require.NoError(n.t, os.WriteFile(filepath.Join(dir, entry), []byte(fmt.Sprintf("%d:%d", major, minor)), 0o600))
}

func testLog(t *testing.T) logger.Logger {
	t.Helper()
	log, err := logger.NewLogger(logger.WarningLevel)
	require.NoError(t, err)
	return log
}

// publishedSetLogPrefix is the line an operator greps for to learn what the
// agent considers published. The tests below pin it because that is the only
// place the composition of the set is ever written down.
const publishedSetLogPrefix = "devices published to workloads as block volumes, with their partitions and holders"

func capturingLog(t *testing.T) (logger.Logger, *bytes.Buffer) {
	t.Helper()
	var out bytes.Buffer
	log, err := logger.NewLoggerToWriter(&out, logger.InfoLevel)
	require.NoError(t, err)
	return log, &out
}

func TestPublishedDevices_PublishedMultipathMapWithKpartxPartition(t *testing.T) {
	n := newFakeNode(t)

	// A VM's disk on a SAN LUN: the multipath map is published; the guest laid
	// down a partition table, which kpartx mapped as mpatha-part1 on the host.
	n.device("dm-5", 253, 5, "mpatha")
	n.holder("dm-5", "dm-9", 253, 9, "mpatha-part1")
	n.publish("pvc-guest-disk", "pod-uid-1", 253, 5)

	// The node's own storage: a multipath map nobody published.
	n.device("dm-6", 253, 6, "mpathb")

	set, present := RefreshPublishedDevices(testLog(t))

	assert.True(t, present, "the publish directory exists")
	assert.Equal(t, []string{"dm-5", "dm-9"}, set.KNames(), "the map and what kpartx stacked on it")
	assert.Equal(t, []string{"/dev/dm-5", "/dev/dm-9"}, set.RejectPaths(),
		"one rule per device for lvm, by kernel name; the other spellings are for Has")

	assert.True(t, set.Has("/dev/mapper/mpatha-part1"))
	assert.True(t, set.Has("/dev/dm-9"))
	assert.True(t, set.Has("dm-5"))
	assert.False(t, set.Has("/dev/mapper/mpathb"), "the node's own map stays")
	assert.False(t, set.Has("dm-6"))
}

func TestPublishedDevices_PublishedDiskWithKernelPartitions(t *testing.T) {
	n := newFakeNode(t)

	// A raw disk handed to a VM whose installer partitioned it: root-on-LVM puts
	// the PV on sdc1, and the published node names sdc.
	n.device("sdc", 8, 32, "")
	n.partition("sdc", "sdc1", 8, 33)
	n.partition("sdc", "sdc2", 8, 34)
	n.publish("pvc-raw-disk", "pod-uid-2", 8, 32)

	set, _ := RefreshPublishedDevices(testLog(t))

	assert.Equal(t, []string{"sdc", "sdc1", "sdc2"}, set.KNames())
	assert.Equal(t, []string{"/dev/sdc", "/dev/sdc1", "/dev/sdc2"}, set.RejectPaths(), "the partition is rejected on its own")
	assert.True(t, set.Has("/dev/block/8:33"), "a PV reported under its major:minor alias is still recognised")
	assert.False(t, set.Has("/dev/mapper/mpathz"), "a device nobody published is not in the set")
}

func TestPublishedDevices_StagedVolumeCounts(t *testing.T) {
	n := newFakeNode(t)

	// Staged on the node, between two pods: the driver left the device at the
	// staging path. No publish entry for it at all.
	n.device("dm-11", 253, 11, "mpathc")
	n.stage("pvc-staged", "device", 253, 11)

	set, present := RefreshPublishedDevices(testLog(t))

	assert.False(t, present, "nothing was ever published, so publish/ does not exist")
	assert.Equal(t, []string{"dm-11"}, set.KNames(), "a staged volume is a volume this cluster handed out")
}

func TestPublishedDevices_NeverWalksUpwards(t *testing.T) {
	n := newFakeNode(t)

	// A Logical Volume of the node's own Volume Group, published to a VM. Its
	// slave — the node's disk — must not enter the set.
	n.device("nvme0n1", 259, 0, "")
	n.device("dm-3", 253, 3, "vg--local-pvc--1")
	require.NoError(t, os.MkdirAll(filepath.Join(n.classBlock, "dm-3", "slaves", "nvme0n1"), 0o755))
	n.publish("pvc-local-lv", "pod-uid-3", 253, 3)

	set, _ := RefreshPublishedDevices(testLog(t))

	assert.Equal(t, []string{"dm-3"}, set.KNames(), "the published LV and nothing beneath it")
	assert.False(t, set.Has("nvme0n1"), "the node's own disk is the one false positive this code must never produce")
}

func TestPublishedDevices_VanishedDeviceIsSkipped(t *testing.T) {
	n := newFakeNode(t)

	// The node is still on disk but the kernel no longer has the device: no
	// /sys/dev/block entry. Whatever it addressed cannot be in the PV list either.
	n.publish("pvc-gone", "pod-uid-4", 253, 77)
	// And a regular file where a node should be.
	require.NoError(t, os.WriteFile(filepath.Join(n.kubelet, PublishedSubdir, "pvc-gone", "data"), []byte(`{"volumeHandle":"x"}`), 0o600))

	set, present := RefreshPublishedDevices(testLog(t))

	assert.True(t, present)
	assert.True(t, set.Empty())
	assert.Equal(t, internal.LVMGlobalFilter, LVMGlobalFilterForOwnedLoops(), "an empty set leaves the filter as it was")
}

func TestPublishedDevices_MissingRootMeansNothingPublished(t *testing.T) {
	saved := PublishedDevicesRoot()
	SetPublishedDevicesRoot(filepath.Join(t.TempDir(), "kubelet-is-elsewhere"))
	t.Cleanup(func() { SetPublishedDevicesRoot(saved) })

	set, present := RefreshPublishedDevices(testLog(t))

	assert.False(t, present)
	assert.True(t, set.Empty(), "an unreadable tree yields the pre-filter behaviour, not an error")

	SetPublishedDevicesRoot("")
	set, present = RefreshPublishedDevices(testLog(t))
	assert.False(t, present)
	assert.True(t, set.Empty(), "an empty root disables the recognition")
}

func TestPublishedDevices_FilterCarriesTheRejects(t *testing.T) {
	WithEmptySysBlockForTest(t)
	n := newFakeNode(t)

	n.device("dm-5", 253, 5, "mpatha")
	n.publish("pvc-guest-disk", "pod-uid-1", 253, 5)
	ResetPublishedDevicesCache()

	filter := LVMGlobalFilterForOwnedLoops()

	assert.Equal(t,
		`devices/global_filter=["r|^/dev/dm-5$|","r|^/dev/rbd|","r|^/dev/drbd|","r|^/dev/nbd|","r|^/dev/loop|","r|^/dev/mapper/shlun-|"]`,
		filter,
		"the published device is rejected by its kernel name, ahead of the static rejects")
}

func TestPublishedDevices_CacheHonoursTTLAndReset(t *testing.T) {
	n := newFakeNode(t)
	ResetPublishedDevicesCache()

	assert.True(t, PublishedDevices().Empty(), "nothing published yet")

	n.device("dm-5", 253, 5, "mpatha")
	n.publish("pvc-guest-disk", "pod-uid-1", 253, 5)

	assert.True(t, PublishedDevices().Empty(), "within the TTL the cached answer stands")

	ResetPublishedDevicesCache()
	assert.Equal(t, []string{"dm-5"}, PublishedDevices().KNames(), "after a reset the tree is walked again")

	set, _ := RefreshPublishedDevices(testLog(t))
	assert.Equal(t, []string{"dm-5"}, set.KNames(), "a forced refresh always walks the tree")
}

func TestFilterPublishedPVs(t *testing.T) {
	set := newPublishedSet()
	set.add("dm-5", "/dev/dm-5", "/dev/mapper/mpatha", "/dev/block/253:5")
	set.add("sdc1", "/dev/sdc1")
	set.finish()

	pvs := []internal.PVData{
		{PVName: "/dev/mapper/mpatha", VGName: "vg-guest", VGUuid: "guest-1"},
		{PVName: "/dev/sdc1", VGName: "vg-guest-2", VGUuid: "guest-2"},
		{PVName: "/dev/mapper/mpathb", VGName: "vg-own", VGUuid: "own-1"},
		{PVName: "/dev/nvme0n1", VGName: "vg-own", VGUuid: "own-1"},
		{PVName: "/dev/disk/by-id/scsi-3600-unknown-alias", VGName: "vg-unknown", VGUuid: "unknown-1"},
	}

	out := FilterPublishedPVs(testLog(t), pvs, set)

	names := make([]string, 0, len(out))
	for _, pv := range out {
		names = append(names, pv.PVName)
	}
	assert.Equal(t, []string{"/dev/mapper/mpathb", "/dev/nvme0n1", "/dev/disk/by-id/scsi-3600-unknown-alias"}, names,
		"published spellings are dropped; the node's own PVs and an alias the set does not know are kept")

	assert.Equal(t, pvs, FilterPublishedPVs(testLog(t), pvs, PublishedBlockDevices{}), "an empty set is a no-op")
}

func TestNewPublishedBlockDevices(t *testing.T) {
	set := NewPublishedBlockDevices("dm-1", "/dev/sdb", "")

	assert.Equal(t, 2, set.Len())
	assert.True(t, set.Has("/dev/dm-1"))
	assert.True(t, set.Has("sdb"))
	assert.Equal(t, []string{"/dev/dm-1", "/dev/sdb"}, set.RejectPaths())
}

func TestStatBlockDeviceNode_RegularFileIsNotADevice(t *testing.T) {
	p := filepath.Join(t.TempDir(), "data")
	require.NoError(t, os.WriteFile(p, []byte("x"), 0o600))

	major, minor, isBlock, err := statBlockDeviceNode(p)
	require.NoError(t, err)
	assert.False(t, isBlock)
	assert.Zero(t, major)
	assert.Zero(t, minor)

	major, minor, isBlock, err = statBlockDeviceNode(filepath.Join(t.TempDir(), "absent"))
	assert.True(t, errors.Is(err, os.ErrNotExist))
	assert.False(t, isBlock)
	assert.Zero(t, major+minor)
}

// The defect this file's announcement guards against: on a node, an lvm command
// runs before the scanner's first pass (ActivateAllManagedVGs), building its
// filter fills the cache, and a log keyed on "the cache was empty" then said
// nothing — ever, because a steady node has no transitions either. Measured on a
// hypervisor: fifteen devices in the filter, not one line about them.
func TestPublishedDevices_AnnouncesTheSetEvenAfterASilentFill(t *testing.T) {
	n := newFakeNode(t)
	n.device("dm-5", 253, 5, "mpatha")
	n.publish("pvc-guest-disk", "pod-uid-1", 253, 5)

	// The silent path, as the filter builder walks it.
	require.Equal(t, []string{"dm-5"}, PublishedDevices().KNames())

	log, out := capturingLog(t)
	RefreshPublishedDevices(log)

	said := out.String()
	assert.Equal(t, 1, strings.Count(said, publishedSetLogPrefix), "the set is announced exactly once:\n%s", said)
	assert.Contains(t, said, publishedSetLogPrefix+": 1: dm-5")
	assert.Contains(t, said, "block volumes published by kubelet are read from",
		"where the set is read from is said too, so a relocated kubelet root is visible")

	out.Reset()
	RefreshPublishedDevices(log)
	assert.Empty(t, out.String(), "nothing changed, so there is nothing to say: the scanner refreshes on every udev event")
}

// The same defect one step further along: a volume published between two scanner
// passes is picked up by whichever lvm command finds the cache stale, and the
// change must still reach the log.
func TestPublishedDevices_AnnouncesAChangeThatOnlyTheSilentPathSaw(t *testing.T) {
	n := newFakeNode(t)
	log, out := capturingLog(t)

	RefreshPublishedDevices(log)
	assert.Contains(t, out.String(), publishedSetLogPrefix+": none", "an empty set is said in words, not as a bare colon")
	assert.Contains(t, out.String(), "does not exist", "nothing has been published here yet")

	n.device("dm-5", 253, 5, "mpatha")
	n.publish("pvc-guest-disk", "pod-uid-1", 253, 5)
	ResetPublishedDevicesCache() // the TTL, as a node experiences it
	require.Equal(t, []string{"dm-5"}, PublishedDevices().KNames(), "the silent path has the new set")

	out.Reset()
	RefreshPublishedDevices(log)
	said := out.String()
	assert.Contains(t, said, publishedSetLogPrefix+" (was 0): 1: dm-5")
	assert.Contains(t, said, "block volumes published by kubelet are read from",
		"the publish directory appeared, which is the other half of the answer")

	// And back: the pod releases the volume, the directory goes with it.
	require.NoError(t, os.RemoveAll(filepath.Join(n.kubelet, PublishedSubdir)))
	out.Reset()
	RefreshPublishedDevices(log)
	said = out.String()
	assert.Contains(t, said, publishedSetLogPrefix+" (was 1): none")
	assert.Contains(t, said, "does not exist")
}
