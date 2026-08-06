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

// In the package rather than beside the rest of the loop-filter tests, which live
// in utils_test: those need mock_utils, importing it from `package utils` is an
// import cycle, and parseLoopDeviceTable is unexported.

package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
)

func TestParseLoopDeviceTable(t *testing.T) {
	t.Run("device and path, including paths with spaces", func(t *testing.T) {
		got := parseLoopDeviceTable(
			"/dev/loop0 /opt/deckhouse/sds/file-devices/sds-lvg-a.d0.img\n" +
				"/dev/loop1 /var/tmp/a guest disk.img\n")
		assert.Equal(t, []internal.LoopDeviceEntry{
			{Device: "/dev/loop0", Backing: internal.LoopBackingFile{Path: "/opt/deckhouse/sds/file-devices/sds-lvg-a.d0.img"}},
			{Device: "/dev/loop1", Backing: internal.LoopBackingFile{Path: "/var/tmp/a guest disk.img"}},
		}, got)
	})

	t.Run("the deleted marker is split off, not left in the path", func(t *testing.T) {
		got := parseLoopDeviceTable("/dev/loop4   /data/sds-lvg-b.d0.img (deleted)\n")
		assert.Equal(t, []internal.LoopDeviceEntry{
			{Device: "/dev/loop4", Backing: internal.LoopBackingFile{Path: "/data/sds-lvg-b.d0.img", Deleted: true}},
		}, got)
	})

	t.Run("lines with nothing to own are skipped", func(t *testing.T) {
		assert.Empty(t, parseLoopDeviceTable("\n   \n/dev/loop9\n"))
	})
}

// The two call sites that keep the registry truthful — SetupLoopDevice for a loop it
// just attached, FindLoopDeviceByFile for one it found already attached — go through
// this, and the design leans on it: nothing else at the call sites has to remember
// the registry exists. Drop the call and every other test still passes while the
// node's own file-backed Volume Group quietly disappears from lvm's view.
func TestRememberLoopIfManaged(t *testing.T) {
	saved := OwnedLoops()
	t.Cleanup(func() {
		for _, dev := range OwnedLoops() {
			ForgetOwnedLoop(dev)
		}
		for _, dev := range saved {
			RememberOwnedLoop(dev)
		}
	})
	reset := func() {
		for _, dev := range OwnedLoops() {
			ForgetOwnedLoop(dev)
		}
	}

	t.Run("a backing file this agent named is registered", func(t *testing.T) {
		reset()
		RememberLoopIfManaged("/opt/deckhouse/sds/file-devices/sds-lvg-a.d0.img", "/dev/loop3")
		assert.Equal(t, []string{"/dev/loop3"}, OwnedLoops())
	})

	t.Run("a guest's disk is not", func(t *testing.T) {
		reset()
		RememberLoopIfManaged("/var/lib/kubelet/plugins/kubernetes.io/csi/volumeDevices/pvc-1/dev/abc", "/dev/loop4")
		assert.Empty(t, OwnedLoops(),
			"exempting this would put a virtual machine's LVM back in the agent's view")
	})

	t.Run("an image an operator attached by hand is not", func(t *testing.T) {
		reset()
		RememberLoopIfManaged("/backup/node2-root.img", "/dev/loop5")
		assert.Empty(t, OwnedLoops())
	})

	t.Run("nothing to register without a device", func(t *testing.T) {
		reset()
		RememberLoopIfManaged("/opt/deckhouse/sds/file-devices/sds-lvg-a.d0.img", "")
		assert.Empty(t, OwnedLoops())
	})
}

// A targeted lvm report must yield exactly one row, and the three ways it does not
// are the three ways a NAME fails to be an identity.
func TestTheOnlyRowHelpers(t *testing.T) {
	t.Run("VG", func(t *testing.T) {
		got, err := theOnlyVG([]internal.VGData{{VGName: "vg-1", VGUUID: "u-1"}}, "vg-1")
		assert.NoError(t, err)
		assert.Equal(t, "u-1", got.VGUUID)

		// Zero rows used to be an index-out-of-range panic, and lvm exits 0 with an
		// empty report often enough that the agent must not die on it.
		_, err = theOnlyVG(nil, "vg-1")
		if assert.Error(t, err) {
			assert.Contains(t, err.Error(), "no such VG")
		}

		// A guest creating a vg-1 of its own inside a disk this node can see is all it
		// takes; picking a row would describe somebody else's storage under our name.
		_, err = theOnlyVG([]internal.VGData{
			{VGName: "vg-1", VGUUID: "u-ours"},
			{VGName: "vg-1", VGUUID: "u-the-guests"},
		}, "vg-1")
		if assert.Error(t, err) {
			assert.Contains(t, err.Error(), "does not identify one")
			assert.Contains(t, err.Error(), "u-ours")
			assert.Contains(t, err.Error(), "u-the-guests")
		}
	})

	t.Run("LV", func(t *testing.T) {
		got, err := theOnlyLV([]internal.LVData{{LVName: "pvc-1", VGUuid: "u-1"}}, "/dev/vg-1/pvc-1", "vg-1")
		assert.NoError(t, err)
		assert.Equal(t, "pvc-1", got.LVName)

		_, err = theOnlyLV(nil, "/dev/vg-1/pvc-1", "vg-1")
		if assert.Error(t, err) {
			assert.Contains(t, err.Error(), "no such LV")
		}

		// /dev/<vg>/<lv> addresses the Volume Group by name, so a duplicate name makes
		// lvm answer once per candidate and the size of a foreign LV would be reported
		// as our volume's.
		_, err = theOnlyLV([]internal.LVData{
			{LVName: "pvc-1", VGUuid: "u-ours"},
			{LVName: "pvc-1", VGUuid: "u-the-guests"},
		}, "/dev/vg-1/pvc-1", "vg-1")
		if assert.Error(t, err) {
			assert.Contains(t, err.Error(), "u-the-guests")
			assert.Contains(t, err.Error(), "vg-1")
		}
	})

	t.Run("PV", func(t *testing.T) {
		got, err := theOnlyPV([]internal.PVData{{PVName: "/dev/nvme0n1"}}, "/dev/nvme0n1")
		assert.NoError(t, err)
		assert.Equal(t, "/dev/nvme0n1", got.PVName)

		// The only listing that runs against a device the agent may have just detached.
		_, err = theOnlyPV(nil, "/dev/loop7")
		if assert.Error(t, err) {
			assert.Contains(t, err.Error(), "no such PV")
		}
	})
}
