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
// catches partitions of foreign devices too (e.g. "rbd14p1", "loop970",
// "drbd0").
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
// NBD, loopback) removed.
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
// Each resolver call runs under runWithTimeout(cmdTimeout) so a hung
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
		resolved, err := runWithTimeout(ctx, cmdTimeout, func(ctx context.Context) (string, error) {
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
