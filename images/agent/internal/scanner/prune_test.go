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

package scanner

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/utils"
)

const (
	guestVGUUID = "guest-vg-uuid"
	ownVGUUID   = "own-vg-uuid"
)

func guestAndOwnScan() ([]internal.PVData, []internal.VGData, []internal.LVData) {
	pvs := []internal.PVData{
		// A guest's Physical Volume, read through the block volume this node
		// published to the guest's virtual machine.
		{PVName: "/dev/mapper/vg0-pvc-guest-disk", VGName: "guest-vg", VGUuid: guestVGUUID},
		{PVName: "/dev/sdb", VGName: "own-vg", VGUuid: ownVGUUID},
	}
	vgs := []internal.VGData{
		{VGName: "guest-vg", VGUUID: guestVGUUID},
		{VGName: "own-vg", VGUUID: ownVGUUID},
	}
	lvs := []internal.LVData{
		{LVName: "guest-thin", VGName: "guest-vg", VGUuid: guestVGUUID},
		{LVName: "pvc-guest-disk", VGName: "own-vg", VGUuid: ownVGUUID},
	}
	return pvs, vgs, lvs
}

// The Volume Group a dropped PV made up goes with it, and so do its Logical
// Volumes — in the same step, because EnsureVGActivation activates from the VG
// list this returns.
func TestDropPublishedPVs_TakesTheVolumeGroupAndItsLogicalVolumesAlong(t *testing.T) {
	pvs, vgs, lvs := guestAndOwnScan()
	// The PV is reported under its mapper name, and the set matches it by that
	// alias — the way FilterPublishedPVs does on a node.
	published := utils.NewPublishedBlockDevices("/dev/mapper/vg0-pvc-guest-disk")

	pvs, vgs, lvs = dropPublishedPVs(logger.NewNop(), pvs, vgs, lvs, published)

	assert.Equal(t, []internal.PVData{{PVName: "/dev/sdb", VGName: "own-vg", VGUuid: ownVGUUID}}, pvs)
	assert.Equal(t, []internal.VGData{{VGName: "own-vg", VGUUID: ownVGUUID}}, vgs)
	assert.Equal(t, []internal.LVData{{LVName: "pvc-guest-disk", VGName: "own-vg", VGUuid: ownVGUUID}}, lvs)
}

// A Volume Group that still has a PV on a device of the node's own stays whole
// — Logical Volumes included — even though one of its PVs was dropped. That is
// the failure direction: a Volume Group of the node's must never vanish over a
// guess, and lvm's own filter settles the device on the next pass.
func TestDropPublishedPVs_KeepsAVolumeGroupThatStillHasAPresentPV(t *testing.T) {
	pvs := []internal.PVData{
		{PVName: "/dev/dm-12", VGName: "mixed-vg", VGUuid: ownVGUUID},
		{PVName: "/dev/sdc", VGName: "mixed-vg", VGUuid: ownVGUUID},
	}
	vgs := []internal.VGData{{VGName: "mixed-vg", VGUUID: ownVGUUID}}
	lvs := []internal.LVData{{LVName: "data", VGName: "mixed-vg", VGUuid: ownVGUUID}}

	pvs, gotVGs, gotLVs := dropPublishedPVs(logger.NewNop(), pvs, vgs, lvs, utils.NewPublishedBlockDevices("dm-12"))

	assert.Equal(t, []internal.PVData{{PVName: "/dev/sdc", VGName: "mixed-vg", VGUuid: ownVGUUID}}, pvs)
	assert.Equal(t, vgs, gotVGs)
	assert.Equal(t, lvs, gotLVs)
}

// Nothing published — the behaviour the agent had before the filter existed.
func TestDropPublishedPVs_EmptySetChangesNothing(t *testing.T) {
	pvs, vgs, lvs := guestAndOwnScan()

	gotPVs, gotVGs, gotLVs := dropPublishedPVs(logger.NewNop(), pvs, vgs, lvs, utils.PublishedBlockDevices{})

	assert.Equal(t, pvs, gotPVs)
	assert.Equal(t, vgs, gotVGs)
	assert.Equal(t, lvs, gotLVs)
}

// A published device nobody's PV sits on drops nothing either.
func TestDropPublishedPVs_UnrelatedPublishedDeviceChangesNothing(t *testing.T) {
	pvs, vgs, lvs := guestAndOwnScan()

	gotPVs, gotVGs, gotLVs := dropPublishedPVs(logger.NewNop(), pvs, vgs, lvs, utils.NewPublishedBlockDevices("dm-99"))

	assert.Equal(t, pvs, gotPVs)
	assert.Equal(t, vgs, gotVGs)
	assert.Equal(t, lvs, gotLVs)
}

// The final prune fires on the PV count before any filter of the pass, so that a
// Volume Group brought back by the raw re-list after activation is taken out
// again — and does not fire when nothing was dropped, even over a Volume Group
// no PV accounts for.
func TestPruneToPresentPVs(t *testing.T) {
	pvs := []internal.PVData{{PVName: "/dev/sdb", VGName: "own-vg", VGUuid: ownVGUUID}}
	vgs := []internal.VGData{
		{VGName: "guest-vg", VGUUID: guestVGUUID},
		{VGName: "own-vg", VGUUID: ownVGUUID},
	}
	lvs := []internal.LVData{
		{LVName: "guest-thin", VGName: "guest-vg", VGUuid: guestVGUUID},
		{LVName: "data", VGName: "own-vg", VGUuid: ownVGUUID},
	}

	t.Run("nothing dropped this pass: caches stay as scanned", func(t *testing.T) {
		gotVGs, gotLVs := pruneToPresentPVs(logger.NewNop(), pvs, vgs, lvs, len(pvs))
		assert.Equal(t, vgs, gotVGs)
		assert.Equal(t, lvs, gotLVs)
	})

	t.Run("a PV was dropped earlier in the pass: orphaned VGs and their LVs go", func(t *testing.T) {
		gotVGs, gotLVs := pruneToPresentPVs(logger.NewNop(), pvs, vgs, lvs, len(pvs)+1)
		assert.Equal(t, []internal.VGData{{VGName: "own-vg", VGUUID: ownVGUUID}}, gotVGs)
		assert.Equal(t, []internal.LVData{{LVName: "data", VGName: "own-vg", VGUuid: ownVGUUID}}, gotLVs)
	})
}
