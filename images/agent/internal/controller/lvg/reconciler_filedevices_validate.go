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

// This file holds the validation half of the spec.fileDevices feature: what makes
// an entry usable, and how a problem with one entry is carried to the condition
// without failing the whole reconcile. The provisioning half lives in
// reconciler_filedevices.go, and the reconcile flow that calls both stays in
// reconciler.go.

package lvg

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/utils"
)

var minFileDeviceSize = resource.MustParse("1Gi")

// isWithinBaseDir is utils.IsWithinBaseDir, aliased so the call sites in this
// package stay short. It lives in utils because the startup reattach in
// cmd/main.go needs the same check and cannot import this package.
var isWithinBaseDir = utils.IsWithinBaseDir

// validateFileDevices checks every spec.fileDevices entry, writes the problems
// it finds to reason and returns the names of the entries that failed. Sizes of
// failing entries are never added to totalVGSize — they will not be provisioned,
// so counting them would inflate the VG size used for thin-pool validation.
//
// Whether a failing entry is fatal is the caller's decision. On create it is:
// there is no VG to keep working. On update it is not — a live Volume Group has
// to stay manageable while one entry is wrong, otherwise a single mistyped entry
// added to a healthy LVMVolumeGroup would block every subsequent reconcile
// (thin-pool growth, PV resize, extending by the entries that *are* valid).
func (r *Reconciler) validateFileDevices(
	ctx context.Context,
	lvg *v1alpha1.LVMVolumeGroup,
	reason *strings.Builder,
	totalVGSize *resource.Quantity,
) map[string]struct{} {
	invalid := make(map[string]struct{}, len(lvg.Spec.FileDevices))
	if len(lvg.Spec.FileDevices) == 0 {
		return invalid
	}

	seen := make(map[string]int, len(lvg.Spec.FileDevices))
	for i, fd := range lvg.Spec.FileDevices {
		entryReason := strings.Builder{}

		key := utils.BuildFileDevicePath(fd.Directory, lvg.Name, fd.Name)
		switch prev, dup := seen[key]; {
		case dup:
			// Unreachable under the current schema, and kept deliberately: the
			// path is derived from (directory, name) and `name` is the list-map
			// key, so two entries can only collide by sharing a name, which the
			// apiserver rejects before the agent sees the object. It stands as a
			// guard against a future schema change quietly removing that
			// guarantee, because silently provisioning one file for two entries
			// would corrupt the VG.
			fmt.Fprintf(&entryReason, "fileDevices[%d] collides with fileDevices[%d]: same backing file would be created. ", i, prev)

		case utils.FileDeviceBasenameTooLong(lvg.Name, fd.Name):
			// The apiserver bounds the LVMVolumeGroup name (253) and the entry
			// name (63) separately; their sum can still overflow NAME_MAX on the
			// node, where it would surface as a bare ENAMETOOLONG from fallocate.
			fmt.Fprintf(&entryReason, "fileDevices[%d].name %q combined with the LVMVolumeGroup name is too long: the backing file name would exceed NAME_MAX. ", i, fd.Name)

		default:
			seen[key] = i
			r.validateFileDevice(ctx, fd, i, &entryReason)
		}

		if entryReason.Len() > 0 {
			invalid[fd.Name] = struct{}{}
			reason.WriteString(entryReason.String())
			continue
		}

		if totalVGSize != nil {
			totalVGSize.Add(fd.Size)
		}
	}

	return invalid
}

func (r *Reconciler) validateFileDevice(
	_ context.Context,
	fd v1alpha1.LVMVolumeGroupFileDeviceSpec,
	index int,
	reason *strings.Builder,
) {
	if fd.Directory == "" {
		fmt.Fprintf(reason, "fileDevices[%d].directory is empty. ", index)
		return
	}

	// The agent runs in PID 1's mount namespace; an absolute path with
	// no `..` segment is the minimum sanity check that keeps `fallocate`
	// from creating runaway files outside whatever directory the cluster
	// admin intended.
	cleaned := filepath.Clean(fd.Directory)
	if !filepath.IsAbs(cleaned) {
		fmt.Fprintf(reason, "fileDevices[%d].directory %q must be an absolute path. ", index, fd.Directory)
		return
	}
	if cleaned != fd.Directory && strings.Contains(fd.Directory, "..") {
		fmt.Fprintf(reason, "fileDevices[%d].directory %q must not contain '..' segments. ", index, fd.Directory)
		return
	}

	// Confine backing files to the configured base directory (module config
	// fileDevicesDirectory, default /opt/deckhouse/sds/file-devices). Without
	// this an arbitrary host path — `/`, `/etc`, `/var/lib/kubelet` — could be
	// targeted, and one oversized file there fills the node's filesystem and
	// trips kubelet DiskPressure eviction. An empty base disables the check
	// (unit tests that do not exercise the allowlist).
	if r.cfg.FileDevicesDirectory != "" && !isWithinBaseDir(cleaned, r.cfg.FileDevicesDirectory) {
		fmt.Fprintf(reason, "fileDevices[%d].directory %q must be %q or a subdirectory of it. ", index, fd.Directory, r.cfg.FileDevicesDirectory)
		return
	}

	if fd.Size.Value() < minFileDeviceSize.Value() {
		// A common cause is a decimal unit (e.g. "1G" = 10^9 bytes) where a
		// binary unit was meant ("1Gi" = 2^30 bytes); the former is below the
		// minimum. Point the user at binary units so they do not get stuck on
		// an immutable, too-small entry.
		fmt.Fprintf(reason, "fileDevices[%d].size %s is less than the minimum %s; use a binary unit such as Gi. ", index, fd.Size.String(), minFileDeviceSize.String())
		return
	}

	// The backing directory is created on demand by provisionFileDevices
	// (mkdir -p in PID 1's mount namespace), so validation only enforces the
	// structural rules above. A genuinely unusable path (read-only FS, a file
	// where a directory is expected, …) surfaces as a provisioning error and
	// is reported on the VGConfigurationApplied condition.
}

// fileDeviceIssues carries the spec.fileDevices problems found on the update
// path. They do not make the LVMVolumeGroup invalid: the offending entries are
// skipped when provisioning and reported on the VGConfigurationApplied
// condition, while everything else keeps reconciling.
type fileDeviceIssues struct {
	reason  string
	invalid map[string]struct{}
}

func (issues fileDeviceIssues) shouldSkip(entryName string) bool {
	_, bad := issues.invalid[entryName]
	return bad
}

// fileDevicesUnappliedError marks a failure that concerns individual
// spec.fileDevices entries and leaves the Volume Group itself intact: a backing
// file that did not fit, a losetup that refused, a grow that did not go through,
// a loop the agent could not yet classify.
//
// It exists so the update path can tell those apart from a failure of the Volume
// Group. Both used to surface as a plain error, which the caller turned into
// VGConfigurationApplied=False/VGExtendFailed — a reason the conditions watcher
// treats as fatal, so appending one oversized entry to a healthy LVMVolumeGroup
// took it to NotReady and stopped the scheduler from placing volumes on it,
// while the Volume Group underneath kept serving every volume it had. The rest
// of the reconcile (thin-pool growth in particular) was skipped as well.
//
// A wrapped error is reported on the condition under a non-fatal reason and
// retried; an unwrapped one keeps its old, fatal handling.
type fileDevicesUnappliedError struct {
	// reason overrides the VGConfigurationApplied reason for this problem.
	// Empty means internal.ReasonFileDeviceNotApplied. Whatever is put here must
	// be listed in the controller's acceptableReasons, or the whole point of
	// separating these errors is lost.
	reason string
	err    error
}

func (e *fileDevicesUnappliedError) Error() string { return e.err.Error() }
func (e *fileDevicesUnappliedError) Unwrap() error { return e.err }

// unappliedFileDevices wraps err as a per-entry file-device problem. A nil err
// stays nil so callers can pass a result through unconditionally.
func unappliedFileDevices(reason string, err error) error {
	if err == nil {
		return nil
	}
	return &fileDevicesUnappliedError{reason: reason, err: err}
}

// splitUnappliedFileDevices separates a per-entry file-device problem — which is
// reported on the condition and retried, without taking the Volume Group out of
// service — from a genuine failure that has to abort the reconcile.
//
// It returns the message to append to the condition, the reason override that
// came with it (empty for the default), and the error the caller must still
// treat as fatal.
func splitUnappliedFileDevices(err error) (msg, reason string, fatal error) {
	if err == nil {
		return "", "", nil
	}

	var unapplied *fileDevicesUnappliedError
	if !errors.As(err, &unapplied) {
		return "", "", err
	}

	msg = strings.TrimSpace(unapplied.Error())
	if msg != "" && !strings.HasSuffix(msg, ".") {
		msg += "."
	}
	return msg + " ", unapplied.reason, nil
}

// fileDeviceDriftReason reports entries that were removed from spec.fileDevices
// while their Physical Volume is still part of the Volume Group.
//
// The apiserver allows the removal because an entry that was never provisioned
// must stay removable — that is what keeps a mistyped entry from wedging a live
// LVMVolumeGroup. Acting on the removal is a different matter: dropping a PV
// from a VG needs pvmove + vgreduce, which can be impossible (no free space on
// the remaining PVs) and is destructive when it is not. The module never shrinks
// a Volume Group, for block devices either, so the honest answer is to keep the
// PV, say so on the condition and let the admin either restore the entry or run
// the documented manual procedure.
func (r *Reconciler) fileDeviceDriftReason(lvg *v1alpha1.LVMVolumeGroup) string {
	if len(lvg.Status.Nodes) == 0 {
		return ""
	}

	specNames := make(map[string]struct{}, len(lvg.Spec.FileDevices))
	for _, fd := range lvg.Spec.FileDevices {
		specNames[fd.Name] = struct{}{}
	}

	reported := make(map[string]struct{})
	orphaned := make([]string, 0)
	for _, n := range lvg.Status.Nodes {
		// Only this node's devices. A Local Volume Group has exactly one entry in
		// status.nodes, but the field is a list and the type may one day be Shared,
		// and a Physical Volume another node reported is not this agent's to report
		// drift on — it would name the same entry on every node at once.
		if n.Name != r.cfg.NodeName {
			continue
		}
		for _, fd := range n.FileDevices {
			// Devices provisioned before this field existed carry no name; they
			// cannot be matched to a spec entry, so they are never reported as
			// drift (the discoverer refills the name on the next pass).
			if fd.Name == "" {
				continue
			}
			if _, ok := specNames[fd.Name]; ok {
				continue
			}
			if _, dup := reported[fd.Name]; dup {
				continue
			}
			reported[fd.Name] = struct{}{}
			orphaned = append(orphaned, fd.Name)
		}
	}

	if len(orphaned) == 0 {
		return ""
	}

	sort.Strings(orphaned)
	return fmt.Sprintf("fileDevices entries %s were removed from the spec but still back Physical Volumes of VG %s; the Volume Group is never shrunk automatically. Restore the entries, or remove the PVs manually (pvmove + vgreduce + pvremove). ",
		strings.Join(orphaned, ", "), lvg.Spec.ActualVGNameOnTheNode)
}

// fileDeviceConditionReason picks the VGConfigurationApplied reason for a
// reconcile that applied everything it could but still has file-device problems
// left to report. Every reason it can return is one the conditions watcher
// treats as "still in service" — by construction, since none of these states
// breaks the Volume Group.
//
// The three classes are kept apart so they can be alerted on separately, which
// is the whole reason drift did not simply reuse ReasonValidationFailed:
//
//   - a malformed entry is a spec problem and needs an edit;
//   - an unapplied entry is a node problem (no space, losetup, pvresize) and may
//     well fix itself once the node has room;
//   - drift needs a human to decide what happens to a Physical Volume the spec
//     no longer mentions.
//
// Precedence follows what the operator should look at first: a malformed entry
// beats everything, an unapplied one beats drift. The message names all of them
// regardless, and overrideReason lets a caller name a more specific cause (a
// stuck alias resolver) than the generic "not applied".
func fileDeviceConditionReason(validationReason, unappliedReason, overrideReason string) string {
	if validationReason != "" {
		return internal.ReasonValidationFailed
	}
	if unappliedReason != "" {
		if overrideReason != "" {
			return overrideReason
		}
		return internal.ReasonFileDeviceNotApplied
	}
	return internal.ReasonFileDeviceDrift
}

func joinFileDeviceIssues(parts ...string) string {
	joined := strings.Builder{}
	for _, p := range parts {
		joined.WriteString(p)
	}
	return joined.String()
}

// countVGSizeByFileDevices sums the capacity contributed by spec.fileDevices.
// File-backed PVs are part of the VG just like block devices, so thin-pool
// sizing on the create path must include them; otherwise a file-only VG is
// treated as zero-sized.
func countVGSizeByFileDevices(lvg *v1alpha1.LVMVolumeGroup) resource.Quantity {
	var totalSize int64
	for _, fd := range lvg.Spec.FileDevices {
		totalSize += fd.Size.Value()
	}
	return *resource.NewQuantity(totalSize, resource.BinarySI)
}
