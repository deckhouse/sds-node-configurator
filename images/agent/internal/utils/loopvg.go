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

// This file answers one question for the rest of the agent: may this Volume
// Group be treated as the module's own storage?
//
// It exists because spec.fileDevices removed `loop` from LVMGlobalFilter. Before
// that, lvm.static run by the agent could not see a single Volume Group on a loop
// device, so "is this ours?" never had to be asked — the answer was structurally
// yes. Now every loop-backed Volume Group on the node is visible, including ones
// that belong to something else entirely: a nested cluster on a rawfile-backed
// PersistentVolume, an image an administrator attached with `losetup` to look
// inside it, a file-based VM disk.
//
// The LVM tag is not a sufficient answer for those. An image of a node disk this
// very module used to manage carries storage.deckhouse.io/enabled=true, and a
// guest running LINSTOR carries the legacy linstor- tag — both of which every
// tag-based check in the agent reads as "mine". The owner marker that does hold
// is the backing file's name (utils.BuildFileDevicePath), because only this agent
// writes it.

package utils

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/logger"
)

// LoopDevicePathPrefix is what makes a PV name a loop device.
//
// The check is by name, exactly as the LVMGlobalFilter reject rule it replaces
// was (`r|^/dev/loop|`), and it inherits that rule's blind spot: a loop PV that
// lvm.static reports under a /dev/disk/by-id or /dev/block/MAJ:MIN alias is not
// recognised here. That is deliberate. Resolving every alias-form PV would cost
// one nsenter per PV on every scan, and misreading an alias-reported *block*
// device as a loop would classify a perfectly ordinary Volume Group as foreign
// and drop it from the cache — a far worse failure than the one being prevented.
// Not recognising an alias means the Volume Group is treated as non-loop-backed,
// i.e. left alone, which is what happened before this file existed too.
const LoopDevicePathPrefix = "/dev/loop"

// LoopVGVerdict says what the agent is allowed to do with one Volume Group.
type LoopVGVerdict uint8

const (
	// LoopVGNotLoopOnly — the Volume Group has at least one PV that is not a loop
	// device (or no PVs at all). Nothing about loop ownership applies to it.
	LoopVGNotLoopOnly LoopVGVerdict = iota
	// LoopVGManaged — every PV is a loop device, the Volume Group is tagged as
	// this module's, and at least one backing file carries the owner pattern. This
	// is a file-backed Volume Group of ours.
	LoopVGManaged
	// LoopVGUnowned — every PV is a loop device and nothing identifies the Volume
	// Group as ours: it is either untagged, or tagged but backed by files this
	// agent did not create. It must not be written to, activated, adopted or
	// allowed to occupy a VG name in the cache.
	LoopVGUnowned
	// LoopVGUnknown — every PV is a loop device but at least one backing file
	// could not be read, so ownership could not be established either way.
	//
	// Callers treat it as LoopVGNotLoopOnly, i.e. they leave the Volume Group
	// alone in the permissive sense: it stays in the cache and stays eligible for
	// activation. Refusing on an unreadable losetup would turn a transient host
	// problem into "the node's own storage disappeared", which is the failure this
	// whole area of the code has already been bitten by twice.
	LoopVGUnknown
)

// LoopVGVerdicts holds one verdict per Volume Group, keyed by VG UUID.
//
// Keyed by UUID rather than by name on purpose: the situation these verdicts
// exist for is precisely two Volume Groups sharing a name, and a name-keyed map
// would collapse them.
type LoopVGVerdicts map[string]LoopVGVerdict

// IsUnowned reports whether this Volume Group is confidently not the module's.
// A UUID that was never classified is not unowned — absence of a verdict is not
// a verdict, and the caller must not act on it.
func (v LoopVGVerdicts) IsUnowned(vgUUID string) bool {
	return v[vgUUID] == LoopVGUnowned
}

// ClassifyLoopVGs works out, for every Volume Group in vgs, whether it is a
// loop-only Volume Group and if so whether it belongs to this module.
//
// Cost: nothing at all for a node with no loop-backed Volume Group, which is
// every node that does not use spec.fileDevices. For a Volume Group of ours it
// is one `losetup` per scan in the common case — the loop devices are walked in
// order and the walk stops at the first backing file that carries the owner
// pattern, which the first one does.
func ClassifyLoopVGs(
	ctx context.Context,
	log logger.Logger,
	commands Commands,
	cmdTimeout time.Duration,
	vgs []internal.VGData,
	pvs []internal.PVData,
) LoopVGVerdicts {
	loopPVsByVG := make(map[string][]string, len(vgs))
	nonLoopVGs := make(map[string]struct{}, len(vgs))
	for _, pv := range pvs {
		if pv.VGUuid == "" {
			continue
		}
		if strings.HasPrefix(pv.PVName, LoopDevicePathPrefix) {
			loopPVsByVG[pv.VGUuid] = append(loopPVsByVG[pv.VGUuid], pv.PVName)
			continue
		}
		nonLoopVGs[pv.VGUuid] = struct{}{}
	}

	verdicts := make(LoopVGVerdicts, len(vgs))
	for _, vg := range vgs {
		loopPVs := loopPVsByVG[vg.VGUUID]
		if _, mixed := nonLoopVGs[vg.VGUUID]; mixed || len(loopPVs) == 0 {
			verdicts[vg.VGUUID] = LoopVGNotLoopOnly
			continue
		}

		// The tag is checked first because it is free, and because a loop-only
		// Volume Group without it can never be ours: the agent tags every Volume
		// Group it creates at vgcreate time. Both conditions are required — the tag
		// alone is what an image of a former node disk also carries, and a managed
		// backing-file name alone is what an unrelated file can be given by hand.
		if !HasManagedTag(vg.VGTags) {
			verdicts[vg.VGUUID] = LoopVGUnowned
			continue
		}

		verdicts[vg.VGUUID] = classifyManagedTaggedLoopVG(ctx, log, commands, cmdTimeout, vg, loopPVs)
	}

	return verdicts
}

// classifyManagedTaggedLoopVG decides ownership of a loop-only Volume Group that
// already carries the managed tag, by looking for a backing file only this agent
// could have named.
func classifyManagedTaggedLoopVG(
	ctx context.Context,
	log logger.Logger,
	commands Commands,
	cmdTimeout time.Duration,
	vg internal.VGData,
	loopPVs []string,
) LoopVGVerdict {
	unreadable := false
	for _, loop := range loopPVs {
		backing, err := RunWithTimeout(ctx, cmdTimeout, func(ctx context.Context) (internal.LoopBackingFile, error) {
			cmd, backing, err := commands.GetLoopBackingFile(ctx, loop)
			log.Debug(cmd)
			return backing, err
		})
		if err != nil {
			log.Warning(fmt.Sprintf("[ClassifyLoopVGs] unable to read the backing file of loop PV %s of VG %s: %v; cannot establish ownership this round",
				loop, vg.VGName, err))
			unreadable = true
			continue
		}
		// An empty path means the device is not attached, which cannot be true of
		// something lvm just reported as a PV. Count it as unreadable rather than as
		// evidence of foreignness.
		if backing.Path == "" {
			unreadable = true
			continue
		}
		// A deleted backing file still identifies its owner: the name is what is
		// being matched, and the marker has already been stripped. Refusing to
		// recognise our own Volume Group because somebody unlinked its file is how
		// it would get dropped from the cache and re-created underneath itself.
		if IsManagedFileDevicePath(backing.Path, "") {
			return LoopVGManaged
		}
	}

	if unreadable {
		return LoopVGUnknown
	}

	log.Warning(fmt.Sprintf("[ClassifyLoopVGs] VG %s (VG_UUID=%s) is tagged %s but every one of its %d loop PV(s) is backed by a file this agent did not create; treating it as foreign and leaving it alone",
		vg.VGName, vg.VGUUID, internal.LVMTags[0], len(loopPVs)))
	return LoopVGUnowned
}

// SkipUnownedLoopVGs returns a copy of vgs without the Volume Groups that live
// entirely on loop devices belonging to somebody else.
//
// It is what every code path that writes to, activates or adopts a Volume Group
// has to run its input through. Those paths gate on the LVM tag alone, and the
// tag is not an ownership proof for a loop-backed Volume Group — see the file
// comment.
// SkipSharedVGs drops Volume Groups created with `vgcreate --shared` from a list
// this module is about to activate.
//
// A shared Volume Group is owned by a lock manager (lvmlockd/sanlock), not by
// whoever finds it in a scan: which node may activate which of its Logical
// Volumes is that manager's decision, and taking it locally is how two nodes end
// up writing the same extents. This module does not run a lock manager, so the
// only correct thing it can do with such a Volume Group is leave it alone.
//
// The tag filter does not cover this: a shared Volume Group can carry the
// module's tag — an earlier version of the agent created shared Volume Groups
// itself, and a pool may be tagged by whoever manages it.
//
// It asks VGData.IsShared rather than vg_shared directly, and that is not a
// detail. vg_shared was empty for every shared group this module has ever
// looked at, because the static lvm the agent carries is built without lockd
// support and computes the field as "no" — so this guard was switched off from
// the day it was written. Measured on a live pool: the scanner activated a
// pool's volume on a node holding no lock for it, one second after the cleanup
// had unmapped it, once a minute, forever.
func SkipSharedVGs(log logger.Logger, action string, vgs []internal.VGData) []internal.VGData {
	out := make([]internal.VGData, 0, len(vgs))
	for _, vg := range vgs {
		if vg.IsShared() {
			log.Warning(fmt.Sprintf("[SkipSharedVGs] refusing to %s VG %s (VG_UUID=%s): it is a shared VG (lock type %q), activation of it belongs to its lock manager",
				action, vg.VGName, vg.VGUUID, vg.SharedDescription()))
			continue
		}
		out = append(out, vg)
	}
	return out
}

func SkipUnownedLoopVGs(log logger.Logger, action string, vgs []internal.VGData, verdicts LoopVGVerdicts) []internal.VGData {
	out := make([]internal.VGData, 0, len(vgs))
	for _, vg := range vgs {
		if verdicts.IsUnowned(vg.VGUUID) {
			log.Warning(fmt.Sprintf("[SkipUnownedLoopVGs] refusing to %s VG %s (VG_UUID=%s): it lives entirely on loop devices whose backing files this agent did not create",
				action, vg.VGName, vg.VGUUID))
			continue
		}
		out = append(out, vg)
	}
	return out
}

// RefuseSharedVG reports whether a Volume Group is served by a lock manager and
// must therefore not be written to by the node-local paths of this module.
//
// It is the check every path that creates, extends or removes a Logical Volume
// has to make before it acts on a group it reached through an LVMVolumeGroup.
// Those resources are not created for shared groups any more, but one made
// earlier still exists on any cluster that ran an older agent — and behind it
// sit reconcilers that would run lvcreate, lvextend and lvremove on a pool's
// group with no lock taken at all. The reason it is written as a refusal rather
// than a repair: what is on the other side is somebody's data, and a volume that
// another node holds a lease on must not be resized or removed because a local
// resource asked for it.
func RefuseSharedVG(log logger.Logger, action, vgName string, vg *internal.VGData) (string, bool) {
	if vg == nil || !vg.IsShared() {
		return "", false
	}

	message := fmt.Sprintf("refusing to %s in the Volume Group %s: it is shared (lock type %q) and its volumes are "+
		"handed out by a lock manager, so nothing here may create, extend or remove them. Use the pool's own "+
		"resources for it; an LVMVolumeGroup pointing at a shared group is a leftover and should be deleted by hand",
		action, vgName, vg.SharedDescription())
	log.Warning(fmt.Sprintf("[RefuseSharedVG] %s", message))
	return message, true
}
