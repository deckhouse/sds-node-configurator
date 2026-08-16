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

package bd

import (
	"fmt"
	"strings"
	"testing"

	"github.com/go-logr/logr/funcr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/logger"
)

// orphanedDMDevices reproduces what an e2e node looks like after a spec detached
// a disk without removing the Volume Group on it: the device-mapper nodes of the
// thin pool outlive /dev/sdc, so they are still reported with PkName pointing at
// a device that is no longer in the list.
//
// Taken from a live stand — VG e2e-vg-restart-1786799692 on master-1, where
// `dmsetup ls` still listed the pool while `ls /dev/sdc` said No such file.
func orphanedDMDevices() []internal.Device {
	size := resource.MustParse("2Gi")

	return []internal.Device{
		{
			Name:   "/dev/sdd",
			KName:  "/dev/sdd",
			Type:   "disk",
			Serial: "1587bcc6f6e4117d2ac8bb7001bfc8de",
			Size:   size,
		},
		{
			// The orphan: its parent /dev/sdc was detached and is absent below.
			Name:   "/dev/mapper/e2e--vg--restart-e2e--thin--pool_tdata",
			KName:  "/dev/dm-1",
			PkName: "/dev/sdc",
			Type:   "lvm",
			Size:   size,
		},
	}
}

// A device-mapper node whose backing disk is gone must not take the whole device
// list down with it.
//
// It used to: visitParents returned ErrDeviceListParentNotFound, filterDevices
// turned that into a hard error, and getBlockDeviceCandidates threw away every
// candidate on the node. The agent then created no BlockDevice at all — for any
// disk, healthy ones included — retrying every five seconds for as long as the
// orphan existed. On the e2e stand that meant every spec waiting for a new
// consumable BlockDevice timed out after the first spec that detached a disk.
func TestFilterDevices_OrphanedDMParentDoesNotDropEveryCandidate(t *testing.T) {
	d := setupDiscoverer()

	filtered, err := d.filterDevices(orphanedDMDevices())

	require.NoError(t, err, "an absent parent is a normal node state, not an invalid device list")

	names := make([]string, 0, len(filtered))
	for i := range filtered {
		names = append(names, filtered[i].Name)
	}
	assert.Contains(t, names, "/dev/sdd",
		"the healthy disk still has to be discovered next to the orphan")
}

// Tolerating the orphan must not make it invisible. The walk ends quietly by
// design, and the caller only logs "can't find serial" at Trace, so without this
// pass the state that took the whole e2e suite down leaves no trace at the
// default log level: the device just never becomes a BlockDevice.
func TestOrphanedParents_NamesTheDeviceWhoseParentIsGone(t *testing.T) {
	devices := orphanedDMDevices()

	kNames := make(map[string]struct{}, len(devices))
	for i := range devices {
		kNames[devices[i].KName] = struct{}{}
	}

	orphans := orphanedParents(devices, kNames)

	require.Len(t, orphans, 1, "only the device whose parent is absent")
	assert.Equal(t, "/dev/dm-1", orphans[0].KName)
	assert.Equal(t, "/dev/sdc", orphans[0].PkName, "and the parent it names is what an operator has to look for")
}

// A device that names no parent is not an orphan, and neither is one whose
// parent is present — otherwise every plain disk on the node would produce a
// warning and bury the one that matters.
func TestOrphanedParents_IgnoresIntactChains(t *testing.T) {
	devices := []internal.Device{
		{Name: "/dev/sda", KName: "/dev/sda"},
		{Name: "/dev/sda1", KName: "/dev/sda1", PkName: "/dev/sda"},
	}

	kNames := make(map[string]struct{}, len(devices))
	for i := range devices {
		kNames[devices[i].KName] = struct{}{}
	}

	assert.Empty(t, orphanedParents(devices, kNames))
}

// The orphan inherits nothing, which is the whole point: there is no parent to
// take a serial from. The walk has to end quietly rather than error.
func TestVisitParents_AbsentParentEndsTheWalk(t *testing.T) {
	devicesByKName := map[string]*internal.Device{
		"/dev/dm-1": {Name: "/dev/mapper/pool_tdata", KName: "/dev/dm-1", PkName: "/dev/sdc"},
	}

	visited := 0
	found, err := visitParents(devicesByKName, devicesByKName["/dev/dm-1"], func(*internal.Device) bool {
		visited++
		return true
	}, len(devicesByKName))

	require.NoError(t, err)
	assert.False(t, found, "nothing was inherited")
	assert.Zero(t, visited, "there was no parent to visit")
}

// The guard that does still have to fail, and the one place where one entry does
// still cost the node its whole device list.
//
// That is deliberate rather than an oversight left standing beside the fix above.
// A named-but-absent parent is an ordinary state of a live node — a dm node
// outlives the disk under it — while a cycle in PkName is not a state the kernel
// can produce at all: it would mean a device is its own ancestor. Reaching it says
// the list did not come from the node, so there is nothing in it worth trusting
// device by device, and unlike a missing KName there is no single entry to blame
// and drop.
func TestVisitParents_ACycleStillErrors(t *testing.T) {
	a := &internal.Device{Name: "/dev/dm-0", KName: "/dev/dm-0", PkName: "/dev/dm-1"}
	b := &internal.Device{Name: "/dev/dm-1", KName: "/dev/dm-1", PkName: "/dev/dm-0"}
	devicesByKName := map[string]*internal.Device{"/dev/dm-0": a, "/dev/dm-1": b}

	_, err := visitParents(devicesByKName, a, func(*internal.Device) bool { return true }, len(devicesByKName))

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrDeviceListInvalid)
}

// The other half of the same guard, and the reason the bound is len(devicesByKName)
// rather than a constant: a long chain is a property of one device, not of the
// list. LVM on LUKS on md-raid on multipath on partitions is a legal stack, and
// against a constant of 16 this walk failed — taking every candidate on the node
// with it, which is the failure this file stopped having for a missing parent.
func TestVisitParents_ALongChainIsNotACycle(t *testing.T) {
	const depth = 40

	devicesByKName := make(map[string]*internal.Device, depth)
	for i := range depth {
		kName := fmt.Sprintf("/dev/dm-%d", i)
		dev := &internal.Device{Name: kName, KName: kName}
		if i+1 < depth {
			dev.PkName = fmt.Sprintf("/dev/dm-%d", i+1)
		}
		devicesByKName[kName] = dev
	}

	visited := 0
	found, err := visitParents(devicesByKName, devicesByKName["/dev/dm-0"], func(*internal.Device) bool {
		visited++
		return true
	}, len(devicesByKName))

	require.NoError(t, err)
	assert.False(t, found, "the visitor never interrupted the walk")
	assert.Equal(t, depth-1, visited, "every parent of the chain is visited")
}

// An entry with no KName cannot be indexed, but that is a property of that one
// entry — so it leaves the list rather than failing it. Returning an error here
// used to throw away every candidate on the node, which is the same failure this
// file removed for a named-but-absent parent, reached by a different input.
func TestFilterDevices_AnEntryWithNoKNameDoesNotDropEveryCandidate(t *testing.T) {
	d := setupDiscoverer()

	devices := append(orphanedDMDevices(), internal.Device{
		Name: "/dev/ghost",
		Type: "disk",
		Size: resource.MustParse("2Gi"),
	})

	filtered, err := d.filterDevices(devices)

	require.NoError(t, err, "one unusable entry is not an unusable list")
	assert.Contains(t, deviceNames(filtered), "/dev/sdd", "the healthy disk still has to be discovered")
	assert.NotContains(t, deviceNames(filtered), "/dev/ghost", "and the entry that cannot be indexed is gone")
}

// Two entries under one KName are the shape the netlink device map produces when a
// remove event is missed: udev.DeviceMap.Snapshot builds the list from a stream of
// events rather than from lsblk. Both resolve to one BlockDevice name, so keeping
// both would have them overwrite each other every pass — the second one is dropped,
// and the node keeps every other device.
func TestFilterDevices_ADuplicateKNameDoesNotDropEveryCandidate(t *testing.T) {
	d := setupDiscoverer()

	devices := orphanedDMDevices()
	stale := devices[0]
	stale.Name = "/dev/sdd-stale"
	devices = append(devices, stale)

	filtered, err := d.filterDevices(devices)

	require.NoError(t, err, "a repeated kname is not an unusable list")
	names := deviceNames(filtered)
	assert.Contains(t, names, "/dev/sdd", "the first entry under the kname is the one kept")
	assert.NotContains(t, names, "/dev/sdd-stale", "and the second is dropped rather than fought over")
}

// Dropping it quietly would replace a loud failure with a silent one, so both
// kinds are named — every pass, because unlike an orphaned dm node this is the
// device list itself being wrong rather than a state of the node.
func TestFilterDevices_NamesTheEntriesItCouldNotIndex(t *testing.T) {
	var lines []string
	d := capturingDiscoverer(&lines)

	devices := orphanedDMDevices()
	stale := devices[0]
	stale.Name = "/dev/sdd-stale"
	devices = append(devices, stale, internal.Device{Name: "/dev/ghost", Type: "disk"})

	_, err := d.filterDevices(devices)
	require.NoError(t, err)

	assert.Len(t, linesContaining(lines, "has no kname and cannot be indexed"), 1)
	assert.Len(t, linesContaining(lines, "repeats the kname"), 1)
}

func deviceNames(devices []internal.Device) []string {
	names := make([]string, 0, len(devices))
	for i := range devices {
		names = append(names, devices[i].Name)
	}

	return names
}

// capturingDiscoverer is setupDiscoverer with a logger that keeps what it was
// given, so a test can assert on which level a line went to rather than only on
// whether the code ran.
func capturingDiscoverer(lines *[]string) *Discoverer {
	d := setupDiscoverer()
	d.log = logger.NewLoggerWrap(funcr.New(func(_, args string) {
		*lines = append(*lines, args)
	}, funcr.Options{Verbosity: 10}))

	return d
}

func linesContaining(lines []string, needle string) []string {
	var got []string
	for _, line := range lines {
		if strings.Contains(line, needle) {
			got = append(got, line)
		}
	}

	return got
}

// An orphaned device-mapper node is state that nothing on the node clears by
// itself, and filterDevices runs on every udev event. Reported at Warning every
// pass, the copy worth reading would be buried by thousands of identical ones —
// which is the same failure the message is meant to prevent, moved from the
// resource into the log.
func TestReportOrphanedParents_WarnsOnceWhileTheOrphanPersists(t *testing.T) {
	var lines []string
	d := capturingDiscoverer(&lines)

	for pass := 1; pass <= 3; pass++ {
		_, err := d.filterDevices(orphanedDMDevices())
		require.NoError(t, err)
	}

	warnings := linesContaining(lines, "WARNING [filterDevices] device /dev/mapper")
	assert.Len(t, warnings, 1, "the orphan is named when it appears, not on every pass")

	repeats := linesContaining(lines, "still names the missing parent")
	assert.Len(t, repeats, 2, "the passes that merely saw it again say so at Debug")
}

// And the state going away is worth a line too: without one, the only record of
// the orphan being cleared is that the warnings stop, which is indistinguishable
// from the agent having stopped looking.
func TestReportOrphanedParents_SaysWhenTheOrphanIsGone(t *testing.T) {
	var lines []string
	d := capturingDiscoverer(&lines)

	_, err := d.filterDevices(orphanedDMDevices())
	require.NoError(t, err)

	// The disk is back, so nothing names a missing parent any more.
	healthy := orphanedDMDevices()
	healthy = append(healthy, internal.Device{
		Name:   "/dev/sdc",
		KName:  "/dev/sdc",
		Type:   "disk",
		Serial: "serial-sdc",
		Size:   resource.MustParse("2Gi"),
	})
	_, err = d.filterDevices(healthy)
	require.NoError(t, err)

	assert.Len(t, linesContaining(lines, "no longer names a missing parent"), 1)

	// And it is reported afresh if it comes back, rather than being remembered as
	// already said.
	_, err = d.filterDevices(orphanedDMDevices())
	require.NoError(t, err)
	assert.Len(t, linesContaining(lines, "WARNING [filterDevices] device /dev/mapper"), 2)
}
