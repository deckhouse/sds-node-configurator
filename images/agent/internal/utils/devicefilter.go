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
	"context"
	"fmt"
	"os/exec"
	"path"
	"strings"
	"time"

	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/logger"
)

// CanonicalPathResolver resolves a /dev/* path to the canonical block
// device it points at, following all symlinks. The default resolver is
// HostNsenterCanonicalResolver, which runs `readlink -f` in PID 1's
// mount namespace because the agent container cannot otherwise see the
// host's /dev symlink tree. The function-typed alias makes the resolver
// trivial to mock in tests.
type CanonicalPathResolver func(ctx context.Context, path string) (string, error)

// HostNsenterCanonicalResolver invokes `nsenter -t 1 -m -- readlink -f
// <path>` and returns the trimmed canonical path printed by readlink.
//
// It deliberately does not consult the in-container /dev/ tree: device
// symlinks under /dev/disk/by-id/ and /dev/block/ are created by udev
// on the host and may resolve differently (or not at all) inside the
// container's mount namespace.
func HostNsenterCanonicalResolver(ctx context.Context, devPath string) (string, error) {
	args := []string{"-t", "1", "-m", "--", "readlink", "-f", devPath}
	cmd := exec.CommandContext(ctx, internal.NSENTERCmd, args...)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("nsenter readlink -f %s: %w, stderr=%q", devPath, err, stderr.String())
	}
	resolved := strings.TrimSpace(stdout.String())
	if resolved == "" {
		return "", fmt.Errorf("nsenter readlink -f %s returned empty path", devPath)
	}
	return resolved, nil
}

// IsForeignDeviceBase reports whether the given canonical basename
// belongs to a storage layer the agent must ignore. The check is a
// strict prefix match against internal.ForeignDeviceBasePrefixes so it
// catches partitions of foreign devices too (e.g. "rbd14p1", "nbd0p1",
// "drbd0"). Loop devices are deliberately absent from the prefix list —
// the agent manages file-backed loop devices as LVM PVs.
func IsForeignDeviceBase(base string) bool {
	for _, prefix := range internal.ForeignDeviceBasePrefixes {
		if strings.HasPrefix(base, prefix) {
			return true
		}
	}
	return false
}

// FilterForeignPVs returns a copy of pvs with PVs whose underlying
// canonical device belongs to a foreign storage layer (Ceph RBD, DRBD,
// NBD) removed.
//
// Loop devices are intentionally NOT dropped here: the agent manages
// file-backed loop devices as LVM PVs (spec.fileDevices), so a blanket
// loop reject would hide its own managed PVs. Ownership of a loop PV is
// instead established later, in the discoverer, via the backing-file owner
// pattern (IsManagedFileDevicePath) gated on the VG's
// storage.deckhouse.io/lvmVolumeGroupName tag. Unmanaged loop PVs that
// form a whole VG (e.g. nested LVM inside a file-backed guest VM disk) are
// dropped separately by FilterForeignLoopPVs so they cannot collide by
// name with a managed VG; see ClassifyLoopVGs for why neither the tag nor
// the backing-file name alone is enough to establish ownership.
//
// lvm.static bundled with the agent has no udev integration and so it
// enumerates devices via /dev/block/MAJOR:MINOR and /dev/disk/by-id/
// directly. With LVM PV signatures present inside guest VM disks
// (nested LVM), lvm.static reports such "ghost" VGs as if they were
// local. Two collisions then become possible:
//
//  1. an LVMVolumeGroup spec.actualVGNameOnTheNode matches more than
//     one VG UUID at once and the agent picks the wrong one (size
//     mismatch, ScanFailed condition);
//
//  2. a BlockDevice CR is created for a /dev/rbdN device that already
//     belongs to a Ceph PVC.
//
// We resolve every reported PV path to its canonical basename in the
// host's mount namespace and drop the PV if the basename starts with
// one of the foreign prefixes. PVs we cannot resolve are kept, on the
// assumption that a transient resolver failure must not silently hide
// a legitimate PV.
//
// Each resolver call runs under RunWithTimeout(cmdTimeout) so a hung
// nsenter-backed readlink cannot block the scan loop indefinitely.
// This mirrors the per-command timeout protection introduced in
// PR #290 for every other lvm.static / nsenter invocation in
// scanner.fillTheCache. A non-positive cmdTimeout disables the
// per-call deadline (useful in unit tests with mock resolvers).
//
// resolver may be nil; HostNsenterCanonicalResolver is used in that
// case.
func FilterForeignPVs(
	ctx context.Context,
	log logger.Logger,
	resolver CanonicalPathResolver,
	pvs []internal.PVData,
	cmdTimeout time.Duration,
) []internal.PVData {
	if resolver == nil {
		resolver = HostNsenterCanonicalResolver
	}

	out := make([]internal.PVData, 0, len(pvs))
	for _, pv := range pvs {
		if pv.PVName == "" {
			out = append(out, pv)
			continue
		}
		resolved, err := RunWithTimeout(ctx, cmdTimeout, func(ctx context.Context) (string, error) {
			return resolver(ctx, pv.PVName)
		})
		if err != nil {
			log.Warning(fmt.Sprintf(
				"[FilterForeignPVs] unable to resolve canonical path for PV %q; keeping it: %v",
				pv.PVName, err,
			))
			out = append(out, pv)
			continue
		}
		base := path.Base(resolved)
		if IsForeignDeviceBase(base) {
			log.Info(fmt.Sprintf(
				"[FilterForeignPVs] dropping PV %q backed by foreign device %q (VG=%q VG_UUID=%q)",
				pv.PVName, resolved, pv.VGName, pv.VGUuid,
			))
			continue
		}
		out = append(out, pv)
	}
	return out
}

// FilterForeignLoopPVs drops PVs that belong to an unmanaged, purely
// loop-backed Volume Group — e.g. nested LVM inside a guest VM's
// file-backed disk attached on the host via losetup.
//
// Loop devices are not rejected by FilterForeignPVs because the agent
// manages its own file-backed loop devices (spec.fileDevices) as PVs.
// But an unmanaged loop-backed VG that reaches the cache is dangerous:
// findDuplicateVGNames runs over every cached VG (before tag filtering),
// so a guest VG that happens to share a name with a managed VG
// (`data`, `vg0`, … are common defaults) is detected as a duplicate and
// takes the *managed* LVMVolumeGroup offline (VGReady=False), and the
// agent's name-keyed cache lookups (FindVG/FindLV) could mix the two.
//
// A VG is dropped here exactly when ClassifyLoopVGs called it
// LoopVGUnowned: every one of its PVs is a /dev/loop* device and nothing
// identifies it as this module's — it is either untagged, or tagged but
// backed by files this agent did not create.
//
// The tag on its own is NOT enough to keep a loop-backed VG, and that is
// the whole reason the decision moved into ClassifyLoopVGs. An image of a
// node disk this module used to manage carries
// storage.deckhouse.io/enabled=true, so `losetup -f /backup/node2.img`
// during a restore used to hand the cache a second VG named `data` — and
// findDuplicateVGNames then took the healthy, live LVMVolumeGroup offline.
// That is the very outage this filter exists to prevent, arriving through
// the exception the filter used to make.
//
// A VG whose ownership could not be established (losetup unreadable) and a
// VG absent from vgs altogether are both KEPT. Dropping on absence is
// unsafe in exactly the case that matters — fillTheCache lists VGs before
// PVs, so a VG the agent has just created itself is missing from vgs while
// its PV is already in pvs, and dropping it wiped the managed file-backed
// VG out of the cache, after which FindVG returned nil, the reconciler
// re-entered the create path and pvcreate failed with "Can't initialize
// physical volume ... without -ff" for good.
//
// Bare loop PVs not part of any VG are kept; they carry no VG name and
// cannot poison name resolution.
func FilterForeignLoopPVs(log logger.Logger, pvs []internal.PVData, verdicts LoopVGVerdicts) []internal.PVData {
	out := make([]internal.PVData, 0, len(pvs))
	for _, pv := range pvs {
		if pv.VGUuid != "" && verdicts.IsUnowned(pv.VGUuid) {
			log.Info(fmt.Sprintf(
				"[FilterForeignLoopPVs] dropping PV %q of foreign loop-backed VG %q (VG_UUID=%q)",
				pv.PVName, pv.VGName, pv.VGUuid,
			))
			continue
		}
		out = append(out, pv)
	}
	return out
}

// PVsReferenceUnknownVG reports whether any PV names a VG that is not in vgs.
// The scanner lists VGs before PVs, so this is how it notices that the VG list it
// holds predates a VG the PV list already knows about — most often one the agent
// created moments earlier.
func PVsReferenceUnknownVG(vgs []internal.VGData, pvs []internal.PVData) bool {
	known := make(map[string]struct{}, len(vgs))
	for _, vg := range vgs {
		known[vg.VGUUID] = struct{}{}
	}
	for _, pv := range pvs {
		if pv.VGUuid == "" {
			continue
		}
		if _, ok := known[pv.VGUuid]; !ok {
			return true
		}
	}
	return false
}

// FilterVGsByPresentPVs returns a copy of vgs that keeps only VGs
// referenced by at least one PV in pvs (matched by VGUuid). It is
// meant to run right after FilterForeignPVs so that phantom VGs whose
// only backing PVs were foreign disappear from the cache.
//
// A VG whose VGUuid is empty (should not happen in healthy lvm output
// but guards against malformed JSON) is dropped as well.
func FilterVGsByPresentPVs(vgs []internal.VGData, pvs []internal.PVData) []internal.VGData {
	referenced := make(map[string]struct{}, len(pvs))
	for _, pv := range pvs {
		if pv.VGUuid != "" {
			referenced[pv.VGUuid] = struct{}{}
		}
	}
	out := make([]internal.VGData, 0, len(vgs))
	for _, vg := range vgs {
		if vg.VGUUID == "" {
			continue
		}
		if _, ok := referenced[vg.VGUUID]; ok {
			out = append(out, vg)
		}
	}
	return out
}

// FilterLVsByPresentVGs returns a copy of lvs that keeps only LVs
// belonging to a VG present in vgs (matched by VGUuid). Mirrors
// FilterVGsByPresentPVs so the three caches stay consistent.
func FilterLVsByPresentVGs(lvs []internal.LVData, vgs []internal.VGData) []internal.LVData {
	referenced := make(map[string]struct{}, len(vgs))
	for _, vg := range vgs {
		if vg.VGUUID != "" {
			referenced[vg.VGUUID] = struct{}{}
		}
	}
	out := make([]internal.LVData, 0, len(lvs))
	for _, lv := range lvs {
		if _, ok := referenced[lv.VGUuid]; ok {
			out = append(out, lv)
		}
	}
	return out
}
