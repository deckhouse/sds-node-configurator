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

// This file holds the provisioning half of the spec.fileDevices feature: bringing
// an entry up (fallocate, losetup, direct I/O), growing one in place, adding it to
// the Volume Group, undoing what a failed attempt created, and removing
// everything on delete. Validation lives in reconciler_filedevices_validate.go,
// and the reconcile flow that calls both stays in reconciler.go.

package lvg

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/utils"
)

// aliasResolveFailureEscalationThreshold is the number of consecutive
// resolver-only failed rounds after which extendFileDevicesIfNeeded switches
// the VGConfigurationApplied reason to ReasonAliasResolutionFailed.
const aliasResolveFailureEscalationThreshold = 3

// fileDeviceSizeGranularity is the unit a spec.fileDevices size is expressed in.
//
// It is a mebibyte because that is the smallest suffix the CRD's size pattern
// accepts (`^[0-9]+(Mi|Gi|Ti|Pi|Ei)$`), which makes it both the finest size a
// user can ask for and the unit reconstructFileDeviceSize has to round an
// imported size up to in order to stay expressible in the spec at all.
const fileDeviceSizeGranularity int64 = 1 << 20

// fileDeviceGrowTolerance returns the spec-to-PV gap growFileDevicesIfNeeded must
// treat as "already the right size".
//
// One extent covers the ordinary case: LVM takes pe_start off the front of the
// device and then floors the rest to whole extents, so the gap is
// pe_start + ((size - pe_start) mod extent) — with LVM's default 1Mi pe_start, a
// 4Mi extent and a size that is a whole number of mebibytes it never exceeds one
// extent.
//
// It is not enough when the extent is smaller than a mebibyte, which is the case
// for a Volume Group an administrator handed over after `vgcreate -s 128k`.
// reconstructFileDeviceSize rounds an imported size up to a whole mebibyte so it
// can be written into spec.fileDevices[].size at all, so the gap it leaves is
// bounded by max(extent, 1Mi) rather than by the extent. Comparing against the
// extent alone had every reconcile re-run fallocate, losetup -c and pvresize as
// no-ops — the PV size cannot change, so the gap never closes — and flap
// VGConfigurationApplied between Updating and Applied forever, one condition
// write feeding the next watch event.
//
// Widening the tolerance costs nothing in the other direction: a `size` raise the
// user actually asked for is at least one mebibyte, since that is the finest unit
// the CRD accepts.
func fileDeviceGrowTolerance(extentSize resource.Quantity) int64 {
	return max(extentSize.Value(), fileDeviceSizeGranularity)
}

// ensureFileDeviceSpace refuses an allocation of needBytes in directory when it
// would take the filesystem below the configured reserve.
//
// "The file fits" and "the node survives" are different questions, and only the
// second one matters here. Backing files are preallocated, the default directory
// is on the node's root filesystem, and kubelet evicts at
// `nodefs.available<10%` by default — so a check that merely refuses to exceed
// the free space still permits the DiskPressure eviction it exists to prevent.
// The reserve is what closes that gap; see
// ReconcilerConfig.FileDevicesMinFreeSpacePercent for why it is a share of the
// filesystem rather than an absolute number.
//
// The measurement itself stays best-effort: if the filesystem cannot be read the
// allocation goes ahead and fallocate still fails cleanly on a genuine ENOSPC.
// Refusing on an unreadable filesystem would turn a monitoring failure into a
// provisioning failure, and the entry cannot be shrunk back out of the way.
func (r *Reconciler) ensureFileDeviceSpace(ctx context.Context, directory, filePath string, needBytes int64) error {
	// Nothing to allocate, nothing to refuse — and refusing anyway is a dead end
	// rather than a delay. A backing file that is already the full size with no
	// loop attached (the agent was killed between fallocate and losetup, a reboot
	// whose reattach did not happen, a rollback that detached but could not rm) is
	// brought up by losetup alone, which takes not one byte from the filesystem.
	// Comparing zero against a filesystem that has meanwhile fallen below the
	// reserve — quite possibly because of these very backing files — would refuse
	// that forever, and `size` cannot be lowered to escape it.
	if needBytes <= 0 {
		return nil
	}

	space, err := utils.RunWithTimeout(ctx, r.cfg.CmdDeadlineDuration, func(ctx context.Context) (internal.FilesystemSpace, error) {
		cmd, space, err := r.commands.GetFilesystemSpace(ctx, directory)
		r.log.Debug(cmd)
		return space, err
	})
	if err != nil {
		r.log.Warning(fmt.Sprintf("[ensureFileDeviceSpace] unable to check free space in %s, proceeding (fallocate will still fail on ENOSPC): %v", directory, err))
		return nil
	}

	// Divide before multiplying: a filesystem measured in exbibytes overflows
	// int64 the other way round, and losing up to 99 bytes of reserve is not a
	// difference anybody can observe.
	var reserve int64
	if r.cfg.FileDevicesMinFreeSpacePercent > 0 && space.TotalBytes > 0 {
		reserve = space.TotalBytes / 100 * int64(r.cfg.FileDevicesMinFreeSpacePercent)
	}

	// Subtract the reserve from what is available rather than adding it to what
	// is needed: both operands are bounded by the filesystem size, so this cannot
	// overflow the way `needBytes + reserve` can for a `size` near math.MaxInt64.
	if space.AvailableBytes-reserve >= needBytes {
		return nil
	}

	if reserve == 0 {
		return fmt.Errorf("not enough free space in %q to allocate %s: %d bytes needed, %d bytes available",
			directory, filePath, needBytes, space.AvailableBytes)
	}
	return fmt.Errorf("not enough free space in %q to allocate %s while keeping %d%% of the filesystem free for the node: %d bytes needed, %d of %d bytes available, %d bytes must stay free",
		directory, filePath, r.cfg.FileDevicesMinFreeSpacePercent, needBytes, space.AvailableBytes, space.TotalBytes, reserve)
}

// growFileDevicesIfNeeded grows the backing file, the loop device and the
// physical volume of every spec.fileDevices entry whose size was raised.
//
// The whole sequence is online — nothing is unmounted, no logical volume is
// deactivated, no pod is disturbed:
//
//	fallocate -l <new size> <file>   the file grows
//	losetup -c /dev/loopN            LOOP_SET_CAPACITY: the device re-reads it
//	pvresize /dev/loopN              the PV, and with it the VG, grows
//
// and a thin pool sized as a percentage follows via reconcileThinPoolsIfNeeded.
//
// Every step is idempotent and fails towards the smaller size: a file that grew
// while the loop did not is a working file device that is merely still small,
// and so is a loop that grew while the PV did not. Whatever is interrupted —
// agent restart, node reboot, a cancelled context — the next reconcile repeats
// the sequence from wherever it got to. Nothing can be destroyed, because the
// data lives at the head of the file and growth only appends to its tail.
//
// Shrinking is refused rather than attempted, and not because of the file:
// `fallocate -l` cannot make a file smaller — with the default mode it only
// allocates and, where needed, extends — so a smaller requested size would be a
// no-op. It is refused because giving capacity back means shrinking the Volume
// Group (pvmove + vgreduce), which can be impossible when the remaining PVs have
// no room and is destructive when it is not, and which the module does not do for
// block devices either. A CEL rule rejects a smaller size at admission; the
// tolerance check below refuses it a second time, so a bug elsewhere in the
// pipeline cannot turn into an lvm operation nobody asked for.
func (r *Reconciler) growFileDevicesIfNeeded(
	ctx context.Context,
	lvg *v1alpha1.LVMVolumeGroup,
	vg internal.VGData,
	issues fileDeviceIssues,
) error {
	if len(lvg.Spec.FileDevices) == 0 {
		return nil
	}

	// The provisioned devices as the discoverer resolved them: it has already
	// mapped loop PV aliases to canonical names and attributed each device to
	// the spec entry that created it. Sizes here are PV sizes, which lag the
	// backing file by LVM metadata — hence the extent-sized tolerance below.
	provisioned := make(map[string]v1alpha1.LVMVolumeGroupFileDevice)
	for _, n := range lvg.Status.Nodes {
		if n.Name != r.cfg.NodeName {
			continue
		}
		for _, fd := range n.FileDevices {
			if fd.Name != "" {
				provisioned[fd.Name] = fd
			}
		}
	}

	growTolerance := fileDeviceGrowTolerance(extentSizeForThinPoolAlign(lvg, &vg))
	errs := strings.Builder{}

	for _, fd := range lvg.Spec.FileDevices {
		if issues.shouldSkip(fd.Name) {
			continue
		}

		current, ok := provisioned[fd.Name]
		if !ok {
			// Not provisioned yet: extendFileDevicesIfNeeded will create it at
			// the requested size, so there is nothing to grow.
			continue
		}

		requested, have := fd.Size.Value(), current.Size.Value()
		if requested-have <= growTolerance {
			// A PV is always a little smaller than its backing file — LVM takes
			// metadata off the front and then rounds down to whole extents — so
			// "equal" has to mean "within the tolerance". Comparing exactly would
			// make every reconcile try to grow a device that is already the
			// right size, forever. See fileDeviceGrowTolerance for what the
			// tolerance has to cover and why one extent is not always it.
			continue
		}

		if err := ctx.Err(); err != nil {
			return err
		}

		if isApplied(lvg) {
			if err := r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonUpdating, "trying to apply the configuration"); err != nil {
				r.log.Error(err, fmt.Sprintf("[growFileDevicesIfNeeded] unable to add the condition %s status False reason %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, internal.ReasonUpdating, lvg.Name))
				return err
			}
		}

		if err := r.growFileDevice(ctx, lvg, fd, current, requested-have); err != nil {
			r.log.Error(err, fmt.Sprintf("[growFileDevicesIfNeeded] unable to grow the file device %q of the LVMVolumeGroup %s", fd.Name, lvg.Name))
			errs.WriteString(err.Error() + " ")
		}
	}

	if errs.Len() > 0 {
		// A device that did not grow is a device that is still the size it was,
		// backing a Volume Group that is still serving every volume on it. Report
		// it and retry rather than taking the LVMVolumeGroup out of service — the
		// usual cause is a filesystem that is temporarily short of room, and
		// `size` cannot be lowered again to undo the request.
		//
		// The reason travels with the error. Left empty it would fall back to the
		// generic ReasonFileDeviceNotApplied at the end of the reconcile, and
		// ReasonFileDeviceGrowFailed — documented in the FAQ as the signal for
		// exactly this state, and whitelisted in the conditions watcher for it —
		// would never be written by anything, so an alert on it could not fire.
		return unappliedFileDevices(internal.ReasonFileDeviceGrowFailed, errors.New(strings.TrimSpace(errs.String())))
	}
	return nil
}

// growFileDevice runs the three growth steps for one entry. delta is what the
// filesystem has to give up, measured from the current PV size, which
// over-estimates the true delta by the LVM metadata gap — the safe direction
// for a free-space check.
func (r *Reconciler) growFileDevice(
	ctx context.Context,
	lvg *v1alpha1.LVMVolumeGroup,
	fd v1alpha1.LVMVolumeGroupFileDeviceSpec,
	current v1alpha1.LVMVolumeGroupFileDevice,
	delta int64,
) error {
	filePath := utils.BuildFileDevicePath(fd.Directory, lvg.Name, fd.Name)

	// Always resolve the loop from the backing file, never from status. A loop
	// minor is not stable: ReattachFileDevices re-attaches via `losetup --find`
	// after a reboot and may land on a different one, and the kernel can hand a
	// freed minor to an unrelated file — so status.nodes[].fileDevices[].loopDevice
	// can be stale until the next discovery pass, which the reconciler is not
	// ordered after. Growing on a stale value would run losetup -c and pvresize
	// against somebody else's device while the file that needed the capacity never
	// gets it. cleanupFileDevices re-resolves for exactly the same reason.
	loopDev, err := utils.RunWithTimeout(ctx, r.cfg.CmdDeadlineDuration, func(ctx context.Context) (string, error) {
		cmd, existing, err := r.commands.FindLoopDeviceByFile(ctx, filePath)
		r.log.Debug(cmd)
		return existing, err
	})
	if err != nil {
		return fmt.Errorf("query loop for %s: %w", filePath, err)
	}
	if loopDev == "" {
		// The status says it is provisioned but no loop is attached. Leave it
		// to the reattach/provision paths rather than growing a file whose
		// device is missing.
		return fmt.Errorf("no loop device is attached to %s", filePath)
	}
	if current.LoopDevice != "" && current.LoopDevice != loopDev {
		r.log.Warning(fmt.Sprintf("[growFileDevice] %s is attached to %s, not to %s as recorded in the status of the LVMVolumeGroup %s; using the resolved device",
			filePath, loopDev, current.LoopDevice, lvg.Name))
	}

	// The delta has to fit, not the whole file: the existing bytes are already
	// accounted for on the filesystem.
	if err := r.ensureFileDeviceSpace(ctx, fd.Directory, filePath, delta); err != nil {
		return fmt.Errorf("unable to grow %s: %w", filePath, err)
	}

	r.log.Info(fmt.Sprintf("[growFileDevice] growing %s to %d bytes (loop %s) for the LVMVolumeGroup %s", filePath, fd.Size.Value(), loopDev, lvg.Name))

	growCmd, err := utils.RunWithTimeout(ctx, r.cfg.CmdDeadlineDuration, func(ctx context.Context) (string, error) {
		return r.commands.CreateFileDevice(ctx, filePath, fd.Size.Value())
	})
	r.log.Debug(growCmd)
	if err != nil {
		return fmt.Errorf("grow backing file %s: %w", filePath, err)
	}

	capCmd, err := utils.RunWithTimeout(ctx, r.cfg.CmdDeadlineDuration, func(ctx context.Context) (string, error) {
		return r.commands.SetLoopCapacity(ctx, loopDev)
	})
	r.log.Debug(capCmd)
	if err != nil {
		return fmt.Errorf("refresh capacity of %s: %w", loopDev, err)
	}

	start := time.Now()
	resizeCmd, err := utils.RunWithTimeout(ctx, r.cfg.CmdDeadlineDuration, func(ctx context.Context) (string, error) {
		return r.commands.ResizePV(ctx, loopDev)
	})
	r.metrics.UtilsCommandsDuration(ReconcilerName, "pvresize").Observe(r.metrics.GetEstimatedTimeInSeconds(start))
	r.metrics.UtilsCommandsExecutionCount(ReconcilerName, "pvresize").Inc()
	r.log.Debug(resizeCmd)
	if err != nil {
		r.metrics.UtilsCommandsErrorsCount(ReconcilerName, "pvresize").Inc()
		return fmt.Errorf("resize PV %s: %w", loopDev, err)
	}

	r.log.Info(fmt.Sprintf("[growFileDevice] grew %s and its PV %s", filePath, loopDev))
	return nil
}

// extendFileDevicesIfNeeded provisions any spec.fileDevices entries that are not
// yet part of the VG and adds their loop devices as PVs. It is the update-path
// counterpart of provisionFileDevices+createVGComplex and is what makes the
// documented "add a new fileDevices entry to grow the VG" flow actually take
// effect — without it, appending an entry would pass validation and silently do
// nothing.
//
// provisionFileDevices is idempotent (it reuses already-attached loops and only
// creates missing files), so calling it on every update is safe; only loop devices
// that are not already PVs in the VG are handed to vgextend.
func (r *Reconciler) extendFileDevicesIfNeeded(
	ctx context.Context,
	lvg *v1alpha1.LVMVolumeGroup,
	vg internal.VGData,
	pvs []internal.PVData,
	issues fileDeviceIssues,
) error {
	if len(lvg.Spec.FileDevices) == 0 {
		return nil
	}

	// Lenient: a single unusable entry must not stop the entries that are fine,
	// so provisioning reports per-entry failures and still returns the loop
	// devices that did come up. provisionErr is carried to the end of the
	// function and returned there, after everything that can be applied has
	// been.
	//
	// It is returned wrapped: an entry the node could not bring up is capacity
	// that has not arrived, not a Volume Group that has broken, and the caller
	// must keep reconciling the rest instead of taking the LVMVolumeGroup out of
	// service over it.
	// rawProvisionErr is kept alongside the wrapped one so it can be folded into
	// another unapplied error further down without nesting one inside the other.
	loopPaths, provisioned, rawProvisionErr := r.provisionFileDevices(ctx, lvg, issues, true)
	provisionErr := rawProvisionErr
	if provisionErr != nil {
		r.log.Error(provisionErr, fmt.Sprintf("[extendFileDevicesIfNeeded] some file devices of the LVMVolumeGroup %s could not be provisioned", lvg.Name))
		provisionErr = unappliedFileDevices("", provisionErr)
	}
	if len(loopPaths) == 0 && provisionErr != nil {
		return provisionErr
	}

	// provisionFileDevices only rolls back artifacts it created within its own
	// call; once it returns, the new loops/files survive. Unlike createVGComplex
	// (which can roll back the whole VG via cleanupFileDevices because the VG is
	// brand new), here the VG already holds healthy file devices we must NOT
	// touch. So if a later step fails (condition update, vgextend), detach and
	// remove ONLY the devices this provision call just created — they are not
	// yet PVs in the VG. Otherwise a failed extend leaks a loop device and a
	// preallocated file on the node (and orphans them entirely if the admin
	// then removes the failing spec entry). Runs on a detached context because
	// the failure is frequently the reconcile ctx being cancelled.
	//
	// The trigger is an explicit flag rather than the returned error, which is why
	// that error is deliberately NOT a named return: this function also returns
	// provisionErr, and in that case the devices in `provisioned` are the ones
	// that came up *correctly* and have just been added to the VG. Keying the
	// rollback on the returned error would tear those down.
	extendFailed := false
	defer func() {
		if !extendFailed || len(provisioned) == 0 {
			return
		}
		rollbackCtx, cancel := r.newRollbackContext()
		defer cancel()
		r.rollbackProvisionedFileDevices(rollbackCtx, provisioned)
	}()

	// The PV list has to come from LVM, not from the cache — the same reason
	// createVGComplex reads it live. A loop may have become a canonical /dev/loopN
	// PV after the caller's snapshot (a prior reconcile pvcreated it but failed
	// before the VG was assembled, or the create-rollback deliberately kept it),
	// and the cache is precisely where that PV is not: the cache is filled only on
	// udev events, and writing a PV label to a loop device raises none. Reading the
	// cache twice — the caller's snapshot plus a fresh read of the same cache — is
	// not a second opinion, and it left this guard blind in the only scenario it
	// exists for: the loop went to pvcreate again, failed "already a PV", and the
	// caller reported VGExtendFailed, which is fatal, so a healthy Volume Group
	// went NotReady with nothing to bring it back.
	//
	// The caller's snapshot is still folded in: it can only add PVs, and losing a
	// known PV is what makes the decision unsafe.
	pvView := r.newPVView(ctx, "extendFileDevicesIfNeeded", pvs)
	pvs = pvView.pvs

	// The question here is whether a loop is already a Physical Volume *of this
	// Volume Group*, not whether it is a Physical Volume at all. The two answers
	// differ in exactly the state this function exists to repair: a create
	// interrupted between pvcreate and vgextend leaves a loop carrying a PV label
	// and belonging to no Volume Group. Reading that as "already done" is worse
	// than the wedged condition it was meant to avoid — the entry silently never
	// joins the VG while VGConfigurationApplied still reports Applied, so the
	// LVMVolumeGroup looks healthy at a size the operator never asked for.
	//
	// loopVGMembership resolves alias-form PV names on the way, which a literal
	// name match cannot: lvm.static has no udev integration and frequently reports
	// a managed loop PV under a /dev/disk/by-id or /dev/block/MAJ:MIN alias (the
	// same aliasing the discoverer resolves). Missing that would hand a loop that
	// is already in the VG to vgextend again.
	//
	// An orphan PV needs no special handling beyond being listed: vgextend accepts
	// a device that is already a Physical Volume, and extendVGComplex skips the
	// redundant pvcreate through createPVIfNeeded.
	pvsToExtend := make([]string, 0, len(loopPaths))
	skippedOnResolverFailure := false
	var foreignVGLoops []string
	for _, loop := range loopPaths {
		member, unresolved := r.loopVGMembership(ctx, pvView.aliases, loop, pvs)
		switch {
		case member == vg.VGName:
			r.log.Debug(fmt.Sprintf("[extendFileDevicesIfNeeded] loop %s is already a PV of VG %s; skipping", loop, vg.VGName))
		case member != "":
			// The backing file is named for this LVMVolumeGroup, so the loop is one
			// this agent provisioned, but its Physical Volume sits in somebody else's
			// Volume Group. vgextend would fail on it, and skipping it quietly is the
			// very failure mode above — report it instead and let the operator
			// resolve the overlap.
			foreignVGLoops = append(foreignVGLoops, fmt.Sprintf("%s (VG %s)", loop, member))
		case unresolved:
			skippedOnResolverFailure = true
			r.log.Warning(fmt.Sprintf("[extendFileDevicesIfNeeded] loop %s skipped because an alias PV could not be resolved; will retry on the next reconcile", loop))
		default:
			pvsToExtend = append(pvsToExtend, loop)
		}
	}

	// Folded into the provisioning error rather than returned on its own: it is a
	// per-entry problem of the same kind — capacity that has not arrived — and the
	// entries that can still be added must be added regardless.
	if len(foreignVGLoops) > 0 {
		conflict := fmt.Errorf("file device loop(s) %s are Physical Volumes of another Volume Group and cannot join VG %s",
			strings.Join(foreignVGLoops, ", "), vg.VGName)
		r.log.Error(conflict, fmt.Sprintf("[extendFileDevicesIfNeeded] the LVMVolumeGroup %s cannot use every file device it provisioned", lvg.Name))
		rawProvisionErr = errors.Join(rawProvisionErr, conflict)
		provisionErr = unappliedFileDevices("", rawProvisionErr)
	}

	if len(pvsToExtend) == 0 {
		// A skip forced by a resolver failure is not a real "nothing to do":
		// the loop might genuinely not be a PV yet, but we could not confirm
		// it this round. Returning nil here would mark the configuration
		// applied and the new file device would never join the VG, silently.
		// Report it and let the caller requeue. No rollback here on purpose: a
		// loop skipped this round is either already a PV or one we could not
		// classify, and in both cases tearing it down would destroy storage or
		// discard work the next reconcile would immediately redo
		// (provisionFileDevices reuses an attached loop idempotently).
		if skippedOnResolverFailure {
			// Escalate once the failure looks persistent: a resolver that stays
			// broken (missing nsenter binary, a genuinely dangling alias) would
			// otherwise requeue forever under the generic "Updating" reason,
			// indistinguishable from an ordinary in-flight update. After a few
			// consecutive no-progress rounds switch to a dedicated reason and
			// log at Error level so it can be alerted on.
			//
			// The reason travels with the error rather than being written to the
			// condition here: the caller writes the condition once, at the end of
			// the reconcile, and a write from inside this function was overwritten
			// by it a moment later — the escalation never actually reached the
			// resource.
			streak := r.noteAliasResolveFailure(lvg.Name)
			reason := internal.ReasonUpdating
			msg := "unable to resolve alias PV names to decide whether file-backed loop devices are already part of the VG; retrying"
			if streak >= aliasResolveFailureEscalationThreshold {
				reason = internal.ReasonAliasResolutionFailed
				msg = fmt.Sprintf("unable to resolve alias PV names for %d consecutive reconciles; file devices cannot be added to VG %s until path resolution recovers (check the nsenter binary and PV aliases on the node)", streak, vg.VGName)
				r.log.Error(fmt.Errorf("alias PV resolution stuck"), fmt.Sprintf("[extendFileDevicesIfNeeded] %s (LVMVolumeGroup %s)", msg, lvg.Name))
			}
			// provisionErr is carried along rather than dropped: an entry the node
			// could not bring up in this same round is a separate problem with its
			// own fix, and reporting only the resolver would leave the operator
			// looking at PV aliases while the actual complaint is a full filesystem.
			// The reason override stays the resolver's — it is the more specific
			// diagnosis of why nothing joined the VG.
			return unappliedFileDevices(reason, errors.Join(errors.New(msg), rawProvisionErr))
		}
		r.resetAliasResolveFailure(lvg.Name)
		r.log.Debug(fmt.Sprintf("[extendFileDevicesIfNeeded] VG %s of the LVMVolumeGroup %s has no new file devices to add", vg.VGName, lvg.Name))
		return provisionErr
	}

	// We resolved enough to make progress this round; clear any prior
	// resolver-failure streak so a transient blip does not eventually escalate.
	r.resetAliasResolveFailure(lvg.Name)

	if isApplied(lvg) {
		if err := r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonUpdating, "trying to apply the configuration"); err != nil {
			r.log.Error(err, fmt.Sprintf("[extendFileDevicesIfNeeded] unable to add the condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
			extendFailed = true
			return err
		}
	}

	r.log.Info(fmt.Sprintf("[extendFileDevicesIfNeeded] VG %s of the LVMVolumeGroup %s should be extended with file devices %v", vg.VGName, lvg.Name, pvsToExtend))
	if err := r.extendVGComplex(ctx, pvView, pvsToExtend, vg.VGName); err != nil {
		r.log.Error(err, fmt.Sprintf("[extendFileDevicesIfNeeded] unable to extend VG %s of the LVMVolumeGroup %s with file devices", vg.VGName, lvg.Name))
		extendFailed = true
		return err
	}
	r.log.Info(fmt.Sprintf("[extendFileDevicesIfNeeded] VG %s of the LVMVolumeGroup %s was extended with file devices", vg.VGName, lvg.Name))

	// The devices that could be added are in the VG now; the entries that could
	// not be provisioned are still reported, so the condition names them and the
	// reconcile is retried.
	return provisionErr
}

// noteAliasResolveFailure records one more consecutive resolver-only failed
// round for lvgName and returns the new streak length.
func (r *Reconciler) noteAliasResolveFailure(lvgName string) int {
	r.aliasResolveFailuresMu.Lock()
	defer r.aliasResolveFailuresMu.Unlock()
	r.aliasResolveFailures[lvgName]++
	return r.aliasResolveFailures[lvgName]
}

// resetAliasResolveFailure clears the resolver-failure streak for lvgName
// after a round that made progress or genuinely had nothing to do.
func (r *Reconciler) resetAliasResolveFailure(lvgName string) {
	r.aliasResolveFailuresMu.Lock()
	defer r.aliasResolveFailuresMu.Unlock()
	delete(r.aliasResolveFailures, lvgName)
}

// pvView is the authoritative answer to "is this device already an LVM Physical
// Volume?" for the span of one create or extend pass, plus the alias resolutions
// it took to get there.
//
// It exists because that question has exactly one correct source and two wrong
// ones. The correct source is a live `lvm pvs`. The cache is wrong because it is
// filled only on udev events and writing a PV label to a loop device raises none
// — so after precisely the interrupted create/extend this check guards against,
// the cache is the one place the new PV is missing. A snapshot taken earlier in
// the reconcile is wrong for the same reason, only more so.
//
// Getting it wrong is not a missed optimisation: pvcreate over an existing PV
// fails ("Can't initialize physical volume ... without -ff"), and both callers
// turn that into a fatal VGConfigurationApplied reason, so a Volume Group that is
// serving every volume it has goes NotReady with nothing to bring it back.
type pvView struct {
	pvs     []internal.PVData
	names   map[string]struct{}
	aliases *aliasCache
}

// newPVView lists the node's Physical Volumes, folding in any extra sets the
// caller already holds.
//
// extra can only add PVs. Falling back to the cache when LVM cannot be read is
// the pre-existing behaviour: pvcreate may then fail, which is no worse than
// before the guard existed, and refusing to proceed would make an unreadable
// `pvs` stop provisioning altogether.
func (r *Reconciler) newPVView(ctx context.Context, caller string, extra ...[]internal.PVData) *pvView {
	livePVs, cmd, _, err := r.commands.GetAllPVs(ctx)
	r.log.Debug(cmd)
	if err != nil {
		r.log.Warning(fmt.Sprintf("[%s] unable to list PVs to decide which devices are already Physical Volumes, falling back to the cache: %v", caller, err))
		livePVs, _ = r.sdsCache.GetPVs()
	}

	v := &pvView{
		pvs:     make([]internal.PVData, 0, len(livePVs)),
		names:   make(map[string]struct{}, len(livePVs)),
		aliases: newAliasCache(),
	}
	// Deduplicated by PV name: loopPVState walks this slice and resolves every
	// alias-form entry, so a duplicate is a duplicated host command for every loop
	// being considered.
	for _, set := range append([][]internal.PVData{livePVs}, extra...) {
		for _, pv := range set {
			if _, seen := v.names[pv.PVName]; seen {
				continue
			}
			v.names[pv.PVName] = struct{}{}
			v.pvs = append(v.pvs, pv)
		}
	}
	return v
}

// contains reports whether lvm named this exact path as a Physical Volume.
func (v *pvView) contains(path string) bool {
	_, ok := v.names[path]
	return ok
}

// createPVIfNeeded runs pvcreate over path unless it is already a Physical
// Volume.
//
// The guard lives here, in front of the command, rather than at the call sites:
// it is needed by every path that pvcreates — the create flow retrying after it
// was interrupted between pvcreate and vgcreate, the extend flow after a rollback
// deliberately kept a loop that had already become a PV — and a call site that
// forgets it turns a recoverable interruption into a Volume Group stuck NotReady.
//
// vgcreate and vgextend both accept a device that is already a PV, so skipping
// the redundant pvcreate changes nothing about the outcome.
//
// A loop device is additionally checked by alias, because lvm.static without udev
// integration reports a loop PV under /dev/disk/by-id or /dev/block/MAJ:MIN often
// enough that a literal name match misses it. An alias that could not be resolved
// (pvLoopStateUnknown) is treated like a match: running pvcreate over a device
// that may already be a PV fails and wedges VGConfigurationApplied, while skipping
// it costs nothing — vgcreate/vgextend initialize a device that is not yet a PV.
func (r *Reconciler) createPVIfNeeded(ctx context.Context, pvs *pvView, caller, path string) error {
	if pvs.contains(path) {
		r.log.Info(fmt.Sprintf("[%s] %s is already a PV; skipping pvcreate", caller, path))
		return nil
	}
	if strings.HasPrefix(path, utils.LoopDevicePathPrefix) {
		switch r.loopPVState(ctx, pvs.aliases, path, pvs.pvs) {
		case pvLoopStateRegistered:
			r.log.Info(fmt.Sprintf("[%s] loop %s is already a PV (under an alias); skipping pvcreate", caller, path))
			return nil
		case pvLoopStateUnknown:
			r.log.Warning(fmt.Sprintf("[%s] skipping pvcreate for loop %s: an alias PV could not be resolved, so it cannot be ruled out that this device is already a PV", caller, path))
			return nil
		case pvLoopStateAbsent:
			// Confirmed not a PV; fall through to pvcreate.
		}
	}

	start := time.Now()
	command, err := utils.RunWithTimeout(ctx, r.cfg.CmdDeadlineDuration, func(ctx context.Context) (string, error) {
		return r.commands.CreatePV(ctx, path)
	})
	r.metrics.UtilsCommandsDuration(ReconcilerName, "pvcreate").Observe(r.metrics.GetEstimatedTimeInSeconds(start))
	r.metrics.UtilsCommandsExecutionCount(ReconcilerName, "pvcreate").Inc()
	r.log.Debug(command)
	if err != nil {
		r.metrics.UtilsCommandsErrorsCount(ReconcilerName, "pvcreate").Inc()
		r.log.Error(err, fmt.Sprintf("[%s] unable to create PV by path %s", caller, path))
		return err
	}

	// Remember it, so a path repeated within one pass (a device listed twice, a
	// retry inside the same call) is not pvcreated again.
	pvs.names[path] = struct{}{}
	return nil
}

// isAliasPVName reports whether lvm named this PV by one of the udev-independent
// aliases it falls back to (/dev/disk/by-id/..., /dev/block/MAJ:MIN) rather than
// by the canonical device node. Only those need resolving.
func isAliasPVName(pvName string) bool {
	return strings.HasPrefix(pvName, "/dev/disk/") || strings.HasPrefix(pvName, "/dev/block/")
}

// aliasCache memoizes alias→canonical PV resolutions for the span of one
// reconcile pass.
//
// Every resolution is an nsenter+readlink on the host. Without the cache the
// cost is quadratic: loopPVState walks the whole PV list per loop device, so a
// Volume Group at the CRD's 32-entry limit re-resolves every alias-form PV 32
// times, and cleanupFileDevices and rollbackProvisionedFileDevices do the same
// per target. The answer cannot change within a pass — a device node
// does not move under a running reconcile — so resolving once is not merely
// cheaper, it is also more consistent.
type aliasCache struct {
	canonical map[string]string
	failed    map[string]struct{}
}

func newAliasCache() *aliasCache {
	return &aliasCache{canonical: make(map[string]string), failed: make(map[string]struct{})}
}

// resolveAlias returns the canonical path of an alias-form PV name, or ok=false
// when it could not be resolved. Both outcomes are remembered.
func (r *Reconciler) resolveAlias(ctx context.Context, cache *aliasCache, pvName string) (string, bool) {
	if cache == nil {
		cache = newAliasCache()
	}
	if canonical, seen := cache.canonical[pvName]; seen {
		return canonical, true
	}
	if _, seen := cache.failed[pvName]; seen {
		return "", false
	}

	resolved, err := utils.RunWithTimeout(ctx, r.cfg.CmdDeadlineDuration, func(ctx context.Context) (string, error) {
		return r.resolver(ctx, pvName)
	})
	if err != nil {
		r.log.Warning(fmt.Sprintf("[resolveAlias] unable to resolve the canonical path of PV %s: %v", pvName, err))
		cache.failed[pvName] = struct{}{}
		return "", false
	}
	cache.canonical[pvName] = resolved
	return resolved, true
}

// pvLoopState is what loopPVState could establish about a loop device: that it
// is an LVM Physical Volume, that it is not, or that the question could not be
// answered this round.
//
// A tri-state rather than a pair of booleans, because "is it a PV?" and "could I
// tell?" are not independent answers: collapsing them into (registered,
// resolveFailed) makes every call site that reads only the first silently receive
// "or I could not tell", and every call site that reads both write a redundant
// disjunction. Both current callers happen to want the same conservative
// treatment of Unknown, but they want it for different reasons — pvcreate would
// fail on a device that is already a PV, while a rollback would destroy live
// storage — and that is a decision each one should state rather than inherit.
type pvLoopState uint8

const (
	// pvLoopStateAbsent — every alias PV was resolved and none of them is this
	// loop, so it is confirmed not to be a Physical Volume.
	pvLoopStateAbsent pvLoopState = iota
	// pvLoopStateRegistered — an alias PV resolved to this loop device.
	pvLoopStateRegistered
	// pvLoopStateUnknown — at least one alias PV could not be canonicalized, so
	// it cannot be ruled out that it IS this loop. Callers must treat it as
	// "possibly a Physical Volume": the only correct answer to "I do not know
	// whether this is live storage" is to leave it alone.
	pvLoopStateUnknown
)

// loopPVState reports whether the canonical loop device is already present in
// pvs under an alias name (e.g. /dev/disk/by-id/... or /dev/block/MAJ:MIN). It
// only resolves alias-form PV names, so the common case (lvm reports the
// canonical /dev/loopN) costs no extra host command.
//
// A resolver failure yields pvLoopStateUnknown rather than a verdict. Both callers
// — createPVIfNeeded and rollbackProvisionedFileDevices — then act as if the loop
// were a PV, and neither loses anything by it: vgcreate/vgextend initialize a
// device the skipped pvcreate left uninitialized, and a loop left attached is
// picked up idempotently by provisionFileDevices on the next reconcile, once the
// resolver recovers.
//
// (The extend path asks a different question — which Volume Group a loop belongs
// to — and has its own unresolved signal; see loopVGMembership.)
//
// NOTE: this is one of two places that canonicalize an alias-reported loop
// PV. Here the canonical /dev/loopN is known and alias PV names are resolved
// via readlink (r.resolver). The discoverer does the inverse in
// Discoverer.buildFileDeviceFromLoopPV (backing-file → canonical loop via
// losetup). They use different methods because their inputs differ; keep
// their ownership/aliasing assumptions in sync.
func (r *Reconciler) loopPVState(ctx context.Context, cache *aliasCache, loop string, pvs []internal.PVData) pvLoopState {
	unresolved := false
	for _, pv := range pvs {
		if !isAliasPVName(pv.PVName) {
			continue
		}
		resolved, ok := r.resolveAlias(ctx, cache, pv.PVName)
		if !ok {
			r.log.Warning(fmt.Sprintf("[loopPVState] cannot rule out that loop %s is already a PV: %s could not be resolved", loop, pv.PVName))
			unresolved = true
			continue
		}
		if resolved == loop {
			return pvLoopStateRegistered
		}
	}
	if unresolved {
		return pvLoopStateUnknown
	}
	return pvLoopStateAbsent
}

// loopVGMembership reports the Volume Group a loop device currently belongs to,
// or "" when it belongs to none — it is not a PV at all, or it is an orphan PV
// whose VG has already been removed.
//
// unresolved says the answer is a guess: at least one PV that carries a VG name
// was reported under a /dev/disk or /dev/block alias that could not be
// canonicalized, so it cannot be ruled out that it IS this loop. Callers that
// are about to destroy something must treat that as "in use"; the only correct
// answer to "I do not know whether this is live storage" is to leave it alone.
//
// Orphan PVs are deliberately not reported. `vgremove` does not `pvremove`, so
// after the ordinary delete path the loop is still a PV — reporting it would
// make cleanup refuse to run in exactly the case it is meant for.
func (r *Reconciler) loopVGMembership(ctx context.Context, cache *aliasCache, loop string, pvs []internal.PVData) (vgName string, unresolved bool) {
	for _, pv := range pvs {
		if pv.VGName == "" {
			continue
		}
		if pv.PVName == loop {
			return pv.VGName, false
		}
		if !isAliasPVName(pv.PVName) {
			continue
		}
		resolved, ok := r.resolveAlias(ctx, cache, pv.PVName)
		if !ok {
			r.log.Warning(fmt.Sprintf("[loopVGMembership] PV %s of VG %s could not be resolved; cannot rule out that it is %s", pv.PVName, pv.VGName, loop))
			unresolved = true
			continue
		}
		if resolved == loop {
			return pv.VGName, false
		}
	}
	return "", unresolved
}

// newRollbackContext returns a fresh, detached context (bounded by the
// configured command deadline when set) for file-device cleanup that must run
// even when the reconcile context is already cancelled. The failure that
// triggers a rollback is frequently the reconcile ctx being cancelled (SIGTERM,
// deadline), and exec.CommandContext refuses to start a process under an
// already-cancelled context — which would strand the loop device and backing
// file we just created. The returned cancel func is always safe to defer.
func (r *Reconciler) newRollbackContext() (context.Context, context.CancelFunc) {
	if r.cfg.CmdDeadlineDuration > 0 {
		return context.WithTimeout(context.Background(), r.cfg.CmdDeadlineDuration)
	}
	return context.Background(), func() {}
}

// provisionedFileDevice records a backing file + loop device that
// provisionFileDevices newly created (fallocate + losetup) within a single
// call. Reused, already-attached devices are NOT included, so a caller can
// roll back exactly what this call added if a later step fails, without
// touching pre-existing healthy file devices of the same LVG.
type provisionedFileDevice struct {
	filePath string
	loopDev  string
}

// fileDeviceRollback is what provisionFileDevices has to undo for a single entry
// if the entry, or the call as a whole, fails.
//
// createdFile is deliberately NOT "fallocate returned successfully": it is "the
// file did not exist before this call". fallocate is idempotent, so a retry
// after an interrupted create runs it against a file that is already there — and
// that file may already carry the PV label of a live Volume Group whose loop
// simply is not attached at the moment. Removing it would destroy the PV. Only a
// file this call brought into existence may be removed.
//
// "Did not exist" also has to be established, not inferred from a failed check.
// A `stat` that timed out says nothing about the path, and reading it as "the
// file is not there" put a live backing file within reach of this rollback — see
// the three-way switch in provisionFileDevice.
type fileDeviceRollback struct {
	filePath    string
	createdFile bool
	loopDev     string
	// attemptedAttach records that losetup was actually invoked for this file.
	// It gates the "look for a loop the failed command did not report" probe in
	// rollbackFileDeviceEntry: before that point no loop can exist, so probing
	// would only spend a host command in an error path.
	attemptedAttach bool
}

// provisionFileDevices creates one preallocated backing file per
// spec.fileDevices entry and attaches each as a loop device. It is
// idempotent across reconcile retries: if a loop device is already
// attached to the target file, it reuses that loop device instead of
// creating a fresh one (`losetup --find --show` would otherwise hand
// out a new minor on every call, slowly leaking up to the system-wide
// loop limit).
//
// lenient selects what a per-entry failure costs.
//
// On the create path it is false: there is no Volume Group yet, so anything this
// call created is rolled back and the error is returned immediately — a
// half-provisioned VG is not a useful outcome.
//
// On the update path it is true, for the same reason validation is non-fatal
// there (see validateFileDevices): a live Volume Group has to keep reconciling
// while one entry is broken. The failing entry is rolled back on its own, the
// error is recorded, and the remaining entries are still provisioned — otherwise
// one entry on a full filesystem would indefinitely block a healthy entry that
// takes its space from an entirely different one. The aggregated error is
// returned alongside the loop devices that DID come up, so the caller can extend
// the VG with those and report the rest on the condition.
//
// Loop devices and files that pre-existed (left behind by an earlier partial
// step) are preserved on purpose: the next reconcile picks them up as "already
// attached".
func (r *Reconciler) provisionFileDevices(
	ctx context.Context,
	lvg *v1alpha1.LVMVolumeGroup,
	issues fileDeviceIssues,
	lenient bool,
) (loopPaths []string, provisioned []provisionedFileDevice, retErr error) {
	if len(lvg.Spec.FileDevices) == 0 {
		return nil, nil, nil
	}

	// Track resources we (and only we) just created so we can undo them
	// on failure. We deliberately do NOT roll back pre-existing artifacts.
	created := make([]fileDeviceRollback, 0, len(lvg.Spec.FileDevices))
	defer func() {
		// Only the strict (create) path unwinds the whole call: in lenient mode
		// each failing entry has already been rolled back on its own, and the
		// entries that succeeded are healthy and must survive.
		if retErr == nil || lenient {
			return
		}
		// A context per entry, not one for the whole unwind: the deadline is a
		// per-command budget everywhere else in the agent, and up to maxItems
		// entries here each issue up to three host commands. Sharing one budget
		// truncates the tail of the unwind on a loaded node — silently, since every
		// failure inside is only a warning.
		for i := len(created) - 1; i >= 0; i-- {
			rollbackCtx, cancel := r.newRollbackContext()
			r.rollbackFileDeviceEntry(rollbackCtx, created[i])
			cancel()
		}
	}()

	loopPaths = make([]string, 0, len(lvg.Spec.FileDevices))
	provisioned = make([]provisionedFileDevice, 0, len(lvg.Spec.FileDevices))
	seenLoops := make(map[string]struct{}, len(lvg.Spec.FileDevices))
	var entryErrs []error

	for _, fd := range lvg.Spec.FileDevices {
		// An entry that failed validation is never given to fallocate/losetup.
		// On the update path validation is non-fatal so the rest of the Volume
		// Group keeps reconciling, which would otherwise mean happily
		// provisioning the very entry we just declared unusable.
		if issues.shouldSkip(fd.Name) {
			r.log.Debug(fmt.Sprintf("[provisionFileDevices] skipping the invalid fileDevices entry %q of the LVMVolumeGroup %s", fd.Name, lvg.Name))
			continue
		}
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}

		loopDev, rb, err := r.provisionFileDevice(ctx, lvg, fd)
		if err != nil {
			if !lenient {
				// Recorded so the batch unwind in the defer can undo it along
				// with everything provisioned before it.
				created = append(created, rb)
				return nil, nil, err
			}
			// Undo just this entry, right here, and keep going: the other entries
			// may well live on a different filesystem and have nothing to do with
			// why this one failed. Nothing is recorded for the batch unwind — this
			// entry is already dealt with, and the ones that succeed must survive.
			r.log.Error(err, fmt.Sprintf("[provisionFileDevices] unable to provision the fileDevices entry %q of the LVMVolumeGroup %s; the other entries are still processed", fd.Name, lvg.Name))
			rollbackCtx, cancel := r.newRollbackContext()
			r.rollbackFileDeviceEntry(rollbackCtx, rb)
			cancel()
			entryErrs = append(entryErrs, fmt.Errorf("fileDevices entry %q: %w", fd.Name, err))
			continue
		}
		created = append(created, rb)

		if rb.loopDev != "" {
			provisioned = append(provisioned, provisionedFileDevice{filePath: rb.filePath, loopDev: rb.loopDev})
		}
		if _, ok := seenLoops[loopDev]; !ok {
			seenLoops[loopDev] = struct{}{}
			loopPaths = append(loopPaths, loopDev)
		}
	}

	if len(entryErrs) > 0 {
		return loopPaths, provisioned, errors.Join(entryErrs...)
	}
	return loopPaths, provisioned, nil
}

// refuseUnlinkedBackingFile fails when the entry's backing file has been unlinked
// while the loop device it fed is still attached — the state losetup spells
// "<path> (deleted)".
//
// It is a real state with an ordinary cause: an administrator freeing space with
// `rm` on what looks like a plain file (the FAQ documents `fstrim` precisely
// because people reach for that), or a cleanup that removed the file while a
// detach had failed. The Physical Volume stays live and the volumes on it keep
// working, so the honest answer is neither to pretend the entry is healthy nor to
// destroy anything: report it, keep the PV, and let the operator decide. Recovery
// is theirs to choose — restore the file, or `pvmove`+`vgreduce` the PV out.
//
// The error is a per-entry one (fileDevicesUnappliedError), so the rest of the
// Volume Group keeps reconciling and the reason lands on
// VGConfigurationApplied without taking the LVMVolumeGroup out of service.
//
// The loop is looked up through status rather than by scanning every loop device
// on the node: status is where the discoverer records what it provisioned, and a
// scan would cost a command on every provision of a genuinely new entry. If the
// recorded minor has since been handed to something else, its backing file will
// not carry this entry's name and provisioning proceeds — the same conclusion as
// before this check existed.
func (r *Reconciler) refuseUnlinkedBackingFile(
	ctx context.Context,
	lvg *v1alpha1.LVMVolumeGroup,
	fd v1alpha1.LVMVolumeGroupFileDeviceSpec,
	filePath string,
) error {
	recorded := ""
	for _, n := range lvg.Status.Nodes {
		if n.Name != r.cfg.NodeName {
			continue
		}
		for _, status := range n.FileDevices {
			if status.Name == fd.Name && status.LoopDevice != "" {
				recorded = status.LoopDevice
			}
		}
	}
	if recorded == "" {
		return nil
	}

	backing, err := utils.RunWithTimeout(ctx, r.cfg.CmdDeadlineDuration, func(ctx context.Context) (internal.LoopBackingFile, error) {
		cmd, backing, err := r.commands.GetLoopBackingFile(ctx, recorded)
		r.log.Debug(cmd)
		return backing, err
	})
	if err != nil {
		// Unreadable is not evidence of anything. Proceeding matches what happened
		// before this check existed, and the free-space guard and `--nooverlap` are
		// still in front of the actual provisioning.
		r.log.Warning(fmt.Sprintf("[refuseUnlinkedBackingFile] unable to read the backing file of %s recorded for the fileDevices entry %q of the LVMVolumeGroup %s: %v; provisioning as usual",
			recorded, fd.Name, lvg.Name, err))
		return nil
	}
	if !backing.Deleted || filepath.Base(backing.Path) != filepath.Base(filePath) {
		return nil
	}

	err = fmt.Errorf("the backing file %s of the fileDevices entry %q has been unlinked while loop %s still reads from it as a Physical Volume of VG %s; refusing to create a second backing file at the same path, which would add a second Physical Volume to the Volume Group. Restore the file, or move the Physical Volume out (pvmove + vgreduce + pvremove) before removing the entry",
		filePath, fd.Name, recorded, lvg.Spec.ActualVGNameOnTheNode)
	r.log.Error(err, fmt.Sprintf("[refuseUnlinkedBackingFile] the fileDevices entry %q of the LVMVolumeGroup %s cannot be provisioned", fd.Name, lvg.Name))
	return err
}

// provisionFileDevice brings a single spec.fileDevices entry up and reports what
// would have to be undone. The returned fileDeviceRollback is meaningful even
// when err is non-nil: it names the file this call created (if any) and the loop
// it managed to attach, both of which the caller has to clean up.
func (r *Reconciler) provisionFileDevice(
	ctx context.Context,
	lvg *v1alpha1.LVMVolumeGroup,
	fd v1alpha1.LVMVolumeGroupFileDeviceSpec,
) (loopDev string, rb fileDeviceRollback, err error) {
	filePath := utils.BuildFileDevicePath(fd.Directory, lvg.Name, fd.Name)
	sizeBytes := fd.Size.Value()

	type findResult struct {
		cmd     string
		loopDev string
	}
	findRes, err := utils.RunWithTimeout(ctx, r.cfg.CmdDeadlineDuration, func(ctx context.Context) (findResult, error) {
		cmd, existing, err := r.commands.FindLoopDeviceByFile(ctx, filePath)
		return findResult{cmd: cmd, loopDev: existing}, err
	})
	r.log.Debug(findRes.cmd)
	if err != nil {
		return "", fileDeviceRollback{}, fmt.Errorf("query loop for %s: %w", filePath, err)
	}
	if findRes.loopDev != "" {
		r.log.Info(fmt.Sprintf("[provisionFileDevices] %s already attached to %s; reusing", filePath, findRes.loopDev))
		// Nothing to roll back: neither the file nor the loop is ours.
		return findRes.loopDev, fileDeviceRollback{}, nil
	}

	// "No loop is attached to this path" and "this entry has nothing on the node"
	// are not the same statement, and taking the first for the second doubles the
	// Volume Group. `losetup -j` matches the file by inode, so once the backing
	// file has been unlinked it reports nothing at all — while the loop device
	// carries on reading from the unlinked inode as a live Physical Volume.
	// Provisioning from here creates a second file at the same path, a second loop
	// (`--nooverlap` cannot match it: different inode) and a second PV of the same
	// size in the same Volume Group.
	if err := r.refuseUnlinkedBackingFile(ctx, lvg, fd, filePath); err != nil {
		return "", fileDeviceRollback{}, err
	}

	r.log.Info(fmt.Sprintf("[provisionFileDevices] creating file device %s (%d bytes) for LVMVolumeGroup %s", filePath, sizeBytes, lvg.Name))

	// Create the backing directory on demand (mkdir -p, idempotent) so
	// the admin does not have to pre-create it on every node. A failure
	// here (read-only FS, a non-directory in the path) aborts the
	// provision and is reported on the VGConfigurationApplied condition.
	mkdirCmd, err := utils.RunWithTimeout(ctx, r.cfg.CmdDeadlineDuration, func(ctx context.Context) (string, error) {
		return r.commands.EnsureFileDeviceDirectory(ctx, fd.Directory)
	})
	r.log.Debug(mkdirCmd)
	if err != nil {
		r.log.Error(err, fmt.Sprintf("[provisionFileDevices] unable to create directory %s", fd.Directory))
		return "", fileDeviceRollback{}, err
	}

	// How much of the file is already on disk — blocks actually occupied, not the
	// length it claims, so a sparse file cannot pass itself off as complete and
	// skip the free-space guard below. An interrupted earlier attempt (SIGKILL
	// between fallocate and losetup, a node that went down) leaves the file
	// allocated with no loop attached, which is exactly the state we are in here
	// — "no loop" does not mean "no file".
	//
	// Three outcomes, and they must stay three. "The file is not there" is the
	// ordinary one and the only one that lets the rollback below remove what it
	// creates. "The file is there and holds N bytes" sizes the free-space check.
	// "stat never got to look" — the per-command deadline expired, the reconcile
	// context was cancelled, nsenter could not start — is neither, and treating it
	// as the first is what let a transient timeout end with the rollback deleting a
	// backing file that carried a live Physical Volume. Nothing is provisioned on a
	// measurement that did not happen; the entry is reported and retried, which
	// costs one reconcile and is what a timeout deserves.
	var existingBytes int64
	fileAbsent := false
	allocatedBytes, statErr := utils.RunWithTimeout(ctx, r.cfg.CmdDeadlineDuration, func(ctx context.Context) (int64, error) {
		cmd, size, err := r.commands.GetFileAllocatedBytes(ctx, filePath)
		r.log.Debug(cmd)
		return size, err
	})
	switch {
	case statErr == nil:
		existingBytes = allocatedBytes
	case errors.Is(statErr, utils.ErrFileDeviceAbsent):
		fileAbsent = true
		r.log.Debug(fmt.Sprintf("[provisionFileDevices] no existing backing file at %s; nothing is allocated yet", filePath))
	default:
		r.log.Warning(fmt.Sprintf("[provisionFileDevices] unable to tell whether %s already exists and how much of it is allocated: %v; not provisioning this entry until the answer is known", filePath, statErr))
		return "", fileDeviceRollback{}, fmt.Errorf("unable to measure the backing file %s: %w", filePath, statErr)
	}

	// Refuse to allocate a backing file the node's filesystem cannot spare.
	// `fallocate -l` preallocates the full size, so without this guard a single
	// oversized fileDevices entry (or a typo in `directory`/`size`) fills the
	// node's root filesystem and pushes kubelet into DiskPressure eviction — a
	// node-level outage, not a mere condition error. ensureFileDeviceSpace is
	// what keeps a share of the filesystem out of reach for exactly that reason.
	//
	// Only the bytes the file still lacks have to fit: the ones already in it
	// are accounted for on the filesystem, exactly as on the growth path. This
	// distinction is not a refinement — comparing the full size against the
	// remaining free space refuses forever a file that is already complete,
	// and there is no way out of that state through the API (`size` cannot be
	// lowered and the entry cannot be dropped while the VG does not exist).
	missingBytes := max(sizeBytes-existingBytes, 0)
	if err := r.ensureFileDeviceSpace(ctx, fd.Directory, filePath, missingBytes); err != nil {
		return "", fileDeviceRollback{}, fmt.Errorf("unable to create backing file %s: %w (%d of %d bytes already allocated)",
			filePath, err, existingBytes, sizeBytes)
	}

	// createdFile is keyed on the file having been positively established as
	// absent, not on fallocate having succeeded and not on stat having failed:
	// see fileDeviceRollback.
	rb = fileDeviceRollback{filePath: filePath, createdFile: fileAbsent}
	createCmd, err := utils.RunWithTimeout(ctx, r.cfg.CmdDeadlineDuration, func(ctx context.Context) (string, error) {
		return r.commands.CreateFileDevice(ctx, filePath, sizeBytes)
	})
	r.log.Debug(createCmd)
	if err != nil {
		r.log.Error(err, fmt.Sprintf("[provisionFileDevices] unable to create file %s", filePath))
		// A fallocate that failed part-way may still have left a partial file
		// behind. It is not removed here on purpose: the next reconcile
		// completes it, and `size` is what decides the final length, not
		// whatever got written this time.
		return "", fileDeviceRollback{filePath: filePath}, err
	}

	type setupResult struct {
		cmd     string
		loopDev string
	}
	rb.attemptedAttach = true
	setupRes, err := utils.RunWithTimeout(ctx, r.cfg.CmdDeadlineDuration, func(ctx context.Context) (setupResult, error) {
		cmd, loopDev, err := r.commands.SetupLoopDevice(ctx, filePath)
		return setupResult{cmd: cmd, loopDev: loopDev}, err
	})
	r.log.Debug(setupRes.cmd)
	if err != nil {
		r.log.Error(err, fmt.Sprintf("[provisionFileDevices] unable to setup loop device for %s", filePath))
		// losetup can fail with the device already bound, so the rollback is told
		// to look for a loop rather than trusting what the failed command
		// returned; see rollbackFileDeviceEntry.
		return "", rb, err
	}
	rb.loopDev = setupRes.loopDev

	utils.EnableLoopDirectIO(ctx, r.log, r.commands, r.cfg.CmdDeadlineDuration, setupRes.loopDev)

	r.log.Info(fmt.Sprintf("[provisionFileDevices] file %s attached to %s", filePath, setupRes.loopDev))
	return setupRes.loopDev, rb, nil
}

// rollbackFileDeviceEntry undoes one entry's worth of provisioning: the loop
// device this call attached and, only if this call brought it into existence, the
// backing file.
//
// The loop is re-resolved from the backing file instead of being taken from rb
// whenever rb does not name one. A losetup invocation can fail with the device
// already bound — that is precisely how `--direct-io=on` used to fail — and
// removing the file while such a loop is still attached strands the minor on a
// deleted inode whose blocks the filesystem can never reclaim.
//
// The file is kept whenever its loop could not be detached, since the loop may
// still be reading from it.
func (r *Reconciler) rollbackFileDeviceEntry(ctx context.Context, rb fileDeviceRollback) {
	if rb.filePath == "" && rb.loopDev == "" {
		return
	}

	loopDev := rb.loopDev
	if loopDev == "" && rb.attemptedAttach && rb.filePath != "" {
		cmd, found, err := r.commands.FindLoopDeviceByFile(ctx, rb.filePath)
		r.log.Debug(cmd)
		switch {
		case err != nil:
			r.log.Warning(fmt.Sprintf("[rollbackFileDeviceEntry] unable to check whether a loop is attached to %s: %v; keeping both in place", rb.filePath, err))
			return
		case found != "":
			r.log.Warning(fmt.Sprintf("[rollbackFileDeviceEntry] %s turned out to be attached to %s even though the setup command failed; detaching it before removing the file", rb.filePath, found))
			loopDev = found
		}
	}

	if loopDev != "" {
		if cmd, err := r.commands.DetachLoopDevice(ctx, loopDev); err != nil {
			r.log.Warning(fmt.Sprintf("[rollbackFileDeviceEntry] unable to detach %s: %v (cmd: %s); keeping backing file %s in place", loopDev, err, cmd, rb.filePath))
			return
		}
	}

	if rb.createdFile && rb.filePath != "" {
		if cmd, err := r.commands.RemoveFileDevice(ctx, rb.filePath); err != nil {
			r.log.Warning(fmt.Sprintf("[rollbackFileDeviceEntry] unable to remove %s: %v (cmd: %s)", rb.filePath, err, cmd))
		}
	}
}

// rollbackProvisionedFileDevices tears down ONLY the file devices this reconcile
// just provisioned (created a fresh backing file and attached a fresh loop),
// after a later step (pvcreate/vgcreate/vgextend/condition update) failed.
//
// It is the create/extend-path counterpart to cleanupFileDevices and MUST be
// used instead of it on those paths: cleanupFileDevices walks spec+status and
// would remove the backing file of a loop that another, concurrent reconcile —
// or a pvcreate/vgcreate that materially succeeded but returned a non-zero
// status — has already turned into a live PV of the VG. Removing such a file
// leaves a PV backed by a deleted file while the VG keeps using it; the next
// reconcile then re-provisions a second loop and the VG silently doubles in
// size (observed on real clusters as one backing file attached to two loops,
// one shown "(deleted)").
//
// As a hard safety net it lists the current PVs once, authoritatively (a fresh
// `lvm pvs`, not the possibly-stale cache, because the result gates a
// destructive teardown), and SKIPS any provisioned loop that is already an LVM
// PV (matched canonically or via the /dev/disk//dev/block alias the discoverer
// also resolves). If the PV listing fails it tears nothing down — a leaked
// loop/file is recoverable (the next reconcile reuses it via
// FindLoopDeviceByFile), corrupting a live VG is not — and it never removes a
// backing file whose loop it could not detach, since the loop may still
// reference it.
func (r *Reconciler) rollbackProvisionedFileDevices(ctx context.Context, provisioned []provisionedFileDevice) {
	if len(provisioned) == 0 {
		return
	}

	pvs, cmd, _, err := r.commands.GetAllPVs(ctx)
	r.log.Debug(cmd)
	if err != nil {
		r.log.Warning(fmt.Sprintf("[rollbackProvisionedFileDevices] unable to list PVs to confirm rollback is safe; leaving %d provisioned file device(s) in place (a leak is recoverable, corrupting a live VG is not): %v", len(provisioned), err))
		return
	}
	pvNames := make(map[string]struct{}, len(pvs))
	for _, pv := range pvs {
		pvNames[pv.PVName] = struct{}{}
	}
	// One alias cache for the whole rollback: the loops being considered are all
	// checked against the same PV list, so without it every entry re-resolves the
	// same alias-form PVs.
	aliases := newAliasCache()

	// Each entry gets its own deadline. The PV listing above spent the caller's,
	// and one budget shared by every entry's detach and rm would truncate the tail
	// of the rollback on a loaded node — silently, because everything below is
	// reported as a warning. Counted so a truncated rollback is at least visible
	// in the log as a number rather than as an absence of lines.
	torn, kept := 0, 0
	for i := len(provisioned) - 1; i >= 0; i-- {
		p := provisioned[i]
		entryCtx, cancel := r.newRollbackContext()
		if p.loopDev != "" {
			_, isPV := pvNames[p.loopDev]
			if !isPV {
				// lvm.static without udev may report the loop PV under a
				// /dev/disk or /dev/block alias instead of /dev/loopN. Anything
				// short of a confirmed "not a PV" keeps the device: this is a
				// destructive teardown, and an unresolvable alias cannot be ruled
				// out as being this very loop.
				isPV = r.loopPVState(entryCtx, aliases, p.loopDev, pvs) != pvLoopStateAbsent
			}
			if isPV {
				r.log.Warning(fmt.Sprintf("[rollbackProvisionedFileDevices] loop %s (file %s) is already an LVM PV, or could not be confirmed not to be one; skipping rollback so a concurrent or partially-succeeded create does not lose its backing storage", p.loopDev, p.filePath))
				kept++
				cancel()
				continue
			}
			if cmd, derr := r.commands.DetachLoopDevice(entryCtx, p.loopDev); derr != nil {
				r.log.Warning(fmt.Sprintf("[rollbackProvisionedFileDevices] unable to detach %s: %v (cmd: %s); keeping backing file %s in place", p.loopDev, derr, cmd, p.filePath))
				kept++
				cancel()
				continue
			}
		}
		if p.filePath != "" {
			if cmd, rerr := r.commands.RemoveFileDevice(entryCtx, p.filePath); rerr != nil {
				r.log.Warning(fmt.Sprintf("[rollbackProvisionedFileDevices] unable to remove %s: %v (cmd: %s)", p.filePath, rerr, cmd))
				kept++
				cancel()
				continue
			}
		}
		torn++
		cancel()
	}

	if kept > 0 {
		r.log.Warning(fmt.Sprintf("[rollbackProvisionedFileDevices] rolled back %d of %d provisioned file device(s); %d were kept (already a PV, or a command that did not go through) and will be reused by the next reconcile", torn, len(provisioned), kept))
	}
}

// cleanupFileDevices detaches loop devices and removes backing files
// recorded for this LVG. It walks the union of status.nodes[].fileDevices
// (what the discoverer last observed) and spec.fileDevices (what the
// user asked for), so it cannot leak files that were created but never
// reflected in status — e.g. when the agent crashed mid-provision.
//
// `rm` errors are logged as warnings and do not abort the cleanup
// (a stale ENOENT after manual cleanup is harmless), but every
// `losetup -d` failure is reported back as an error: a busy loop
// device means there is still a live reference to the file (a mount,
// an LV, a sidecar) and removing the LVG resource at this point would
// strand state on the node.
//
// Before touching anything it confirms, against a fresh `lvm pvs`, that the loop
// is not a Physical Volume of a Volume Group that still exists. `losetup -d` is
// not the safety net it looks like: on a busy device LOOP_CLR_FD sets
// LO_FLAGS_AUTOCLEAR and returns success, so the `rm` that follows would unlink
// the backing file of a live VG and the volumes on top would keep running off a
// deleted inode until the next reboot. On the ordinary delete path vgremove has
// already run, so the loop PV is an orphan and the check passes; the case it
// stops is the caller reaching here on a cache that merely did not know about
// the VG. If the listing cannot be read, nothing is torn down and the error is
// returned — the finalizer stays and the resource remains the record of what is
// on the node.
func (r *Reconciler) cleanupFileDevices(ctx context.Context, lvg *v1alpha1.LVMVolumeGroup) error {
	type target struct {
		filePath   string
		loopDevice string
	}
	seen := make(map[string]target)
	add := func(t target) {
		if t.filePath == "" && t.loopDevice == "" {
			return
		}
		// Keyed by basename, not by the full path. The two sources spell the same
		// file differently whenever `directory` contains a symlink:
		// status.nodes[].fileDevices[].FilePath comes from `losetup --output
		// BACK-FILE`, which resolves symlink components, while BuildFileDevicePath
		// keeps the literal spec directory. Keyed by path, one file becomes two
		// targets — the second one then finds no loop, tries to `rm` an already
		// removed file and logs a warning about it. The basename
		// `sds-<lvgName>.<entryName>.img` is identical on both sides, which is the
		// same reason validateLVGForUpdateFunc matches on it.
		key := filepath.Base(t.filePath)
		if t.filePath == "" {
			key = "loop:" + t.loopDevice
		}
		// Prefer the entry that carries both fields.
		if prev, ok := seen[key]; ok {
			if prev.loopDevice == "" && t.loopDevice != "" {
				prev.loopDevice = t.loopDevice
				seen[key] = prev
			}
			return
		}
		seen[key] = t
	}
	for _, node := range lvg.Status.Nodes {
		// Only this node's devices. A Local Volume Group has exactly one entry
		// here, but the field is a list and the type may be Shared, and a path
		// another node reported is not this agent's to `rm`.
		if node.Name != r.cfg.NodeName {
			continue
		}
		for _, fd := range node.FileDevices {
			add(target{filePath: fd.FilePath, loopDevice: fd.LoopDevice})
		}
	}
	for _, fd := range lvg.Spec.FileDevices {
		// The same confinement provisioning is subject to. Without it this is the
		// one place a destructive operation reaches outside the configured base
		// directory: validateFileDevice rejects a `directory` outside it, but that
		// only gates provisioning — the rejected entry is skipped through
		// fileDeviceIssues and never given to fallocate/losetup. Cleanup walks the
		// whole spec with no such filter, and the only remaining guard,
		// IsManagedFileDevicePath, matches on the basename alone. So a
		// never-provisioned entry naming `/somewhere/else` would still have its
		// composed path handed to `losetup -j`, and then to `losetup -d` + `rm -f`
		// if something happened to be there under a matching name.
		//
		// A path outside the base directory cannot be one this agent created, so
		// there is nothing to clean up for it either way.
		if r.cfg.FileDevicesDirectory != "" && !isWithinBaseDir(filepath.Clean(fd.Directory), r.cfg.FileDevicesDirectory) {
			r.log.Warning(fmt.Sprintf("[cleanupFileDevices] skipping the fileDevices entry %q of the LVMVolumeGroup %s: its directory %q is outside the configured base directory %q, so nothing there was created by this agent",
				fd.Name, lvg.Name, fd.Directory, r.cfg.FileDevicesDirectory))
			continue
		}
		add(target{filePath: utils.BuildFileDevicePath(fd.Directory, lvg.Name, fd.Name)})
	}

	if len(seen) == 0 {
		// Nothing to do, and nothing to pay for it: a block-device-only
		// LVMVolumeGroup must not spend an `lvm pvs` on every delete.
		return nil
	}

	// Authoritative, not the cache: the result gates an unlink, and the cache is
	// filled only on udev events, which writing LVM metadata to a loop device
	// does not reliably raise. That gap is precisely how a live Volume Group ends
	// up looking absent.
	livePVs, pvsCmd, _, pvsErr := r.commands.GetAllPVs(ctx)
	r.log.Debug(pvsCmd)
	if pvsErr != nil {
		return fmt.Errorf("unable to list PVs to confirm the file devices of the LVMVolumeGroup %s are no longer in use: %w", lvg.Name, pvsErr)
	}

	keys := make([]string, 0, len(seen))
	for k := range seen {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// One alias cache for the whole cleanup: every target is checked against the
	// same PV list, so resolving per target repeats the same nsenter calls.
	aliases := newAliasCache()

	var detachErrs []error
	for _, key := range keys {
		t := seen[key]
		// Defense in depth: never act on a path whose basename does not
		// match the agent's managed pattern. If it slipped into status
		// via a foreign loop PV (or a bug), refuse to rm and warn.
		if t.filePath != "" && !utils.IsManagedFileDevicePath(t.filePath, lvg.Name) {
			r.log.Warning(fmt.Sprintf("[cleanupFileDevices] refusing to act on unmanaged path %q for LVG %s", t.filePath, lvg.Name))
			continue
		}

		// The loop minor backing a file is NOT stable: after a reboot
		// ReattachFileDevices re-attaches via `losetup --find` and may pick a
		// different minor, and the kernel can later hand a freed minor to an
		// unrelated file. So the loopDevice recorded in status can be stale or
		// even point at a foreign device. Whenever we know the backing file,
		// re-resolve the loop from it and never detach a device we have not
		// just confirmed backs THIS file.
		recordedLoop := t.loopDevice
		confirmedFromFile := false
		if t.filePath != "" {
			loop, err := utils.RunWithTimeout(ctx, r.cfg.CmdDeadlineDuration, func(ctx context.Context) (string, error) {
				cmd, loop, err := r.commands.FindLoopDeviceByFile(ctx, t.filePath)
				r.log.Debug(cmd)
				return loop, err
			})
			if err != nil {
				detachErrs = append(detachErrs, fmt.Errorf("query loop for %s: %w", t.filePath, err))
				r.log.Error(err, fmt.Sprintf("[cleanupFileDevices] unable to query loop for %s", t.filePath))
				continue
			}
			t.loopDevice = loop
			confirmedFromFile = loop != ""
		}

		// `losetup -j` matches by inode, so it finds nothing once the backing file has
		// been unlinked — while the loop device carries on reading from the unlinked
		// inode as a live Physical Volume. Resolving from the file alone would then
		// detach nothing, and the minor would outlive the resource that was the last
		// record of it. Fall back to the loop status recorded.
		if !confirmedFromFile && recordedLoop != "" {
			t.loopDevice = recordedLoop
		}

		// Anything not just confirmed from the backing file has to be confirmed the
		// other way round — read the loop's own backing file and require our owner
		// pattern — so a stale or foreign minor recorded in status is never detached.
		// That covers both a loop-only target (no path known at all) and a path whose
		// inode is gone.
		if !confirmedFromFile && t.loopDevice != "" {
			backing, err := utils.RunWithTimeout(ctx, r.cfg.CmdDeadlineDuration, func(ctx context.Context) (internal.LoopBackingFile, error) {
				cmd, backing, err := r.commands.GetLoopBackingFile(ctx, t.loopDevice)
				r.log.Debug(cmd)
				return backing, err
			})
			if err != nil {
				detachErrs = append(detachErrs, fmt.Errorf("read backing file for %s: %w", t.loopDevice, err))
				r.log.Error(err, fmt.Sprintf("[cleanupFileDevices] unable to read backing file for loop %s", t.loopDevice))
				continue
			}
			// The " (deleted)" marker is already off the path, on purpose: an
			// unlinked backing file still names its owner, and a loop whose file is
			// gone is exactly the one that must be detached rather than left behind.
			if !utils.IsManagedFileDevicePath(backing.Path, lvg.Name) {
				r.log.Warning(fmt.Sprintf("[cleanupFileDevices] refusing to detach loop %s backed by unmanaged file %q for LVG %s", t.loopDevice, backing.Path, lvg.Name))
				continue
			}
			if backing.Deleted {
				r.log.Warning(fmt.Sprintf("[cleanupFileDevices] loop %s of the LVMVolumeGroup %s still reads from %s, which has already been unlinked; detaching it so the minor does not outlive the resource", t.loopDevice, lvg.Name, backing.Path))
			}
		}

		// The last check before anything is destroyed: a loop that is still a PV
		// of an existing Volume Group is storage in use, whatever the caller
		// believed about that Volume Group. Do not rely on `losetup -d` refusing
		// — on a busy device it sets autoclear and returns success, and the `rm`
		// that follows unlinks the backing file out from under a live VG.
		if t.loopDevice != "" {
			vgName, unresolved := r.loopVGMembership(ctx, aliases, t.loopDevice, livePVs)
			switch {
			case vgName != "":
				err := fmt.Errorf("loop %s (file %s) is still a Physical Volume of VG %s; refusing to detach it or remove its backing file", t.loopDevice, t.filePath, vgName)
				r.log.Error(err, fmt.Sprintf("[cleanupFileDevices] refusing to clean up a file device still in use by VG %s (LVMVolumeGroup %s)", vgName, lvg.Name))
				detachErrs = append(detachErrs, err)
				continue
			case unresolved:
				err := fmt.Errorf("unable to tell whether loop %s (file %s) is still a Physical Volume of some VG; refusing to clean it up", t.loopDevice, t.filePath)
				r.log.Error(err, fmt.Sprintf("[cleanupFileDevices] alias PV resolution failed while checking the file devices of the LVMVolumeGroup %s", lvg.Name))
				detachErrs = append(detachErrs, err)
				continue
			}
		}

		if t.loopDevice != "" {
			cmd, err := utils.RunWithTimeout(ctx, r.cfg.CmdDeadlineDuration, func(ctx context.Context) (string, error) {
				return r.commands.DetachLoopDevice(ctx, t.loopDevice)
			})
			r.log.Debug(cmd)
			if err != nil {
				detachErrs = append(detachErrs, fmt.Errorf("detach %s: %w", t.loopDevice, err))
				r.log.Error(err, fmt.Sprintf("[cleanupFileDevices] unable to detach loop %s", t.loopDevice))
				continue
			}
		}
		if t.filePath != "" {
			cmd, err := utils.RunWithTimeout(ctx, r.cfg.CmdDeadlineDuration, func(ctx context.Context) (string, error) {
				return r.commands.RemoveFileDevice(ctx, t.filePath)
			})
			r.log.Debug(cmd)
			if err != nil {
				r.log.Warning(fmt.Sprintf("[cleanupFileDevices] unable to remove file %s: %v", t.filePath, err))
			}
		}
	}
	if len(detachErrs) > 0 {
		return errors.Join(detachErrs...)
	}
	return nil
}
