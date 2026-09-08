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
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/monitoring"
)

const (
	lvAttrMinLen           = 5
	lvAttrActiveIndex      = 4
	lvAttrAllocPolicyIndex = 2
	lvAttrActiveChar       = 'a'
	lvAttrAllocContiguous  = 'c'
	lvAttrTypeThinPool     = 't'
	lvAttrTypeThick        = '-'

	activationControllerName = "Activation"
)

func IsLVActive(attr string) bool {
	return len(attr) > lvAttrActiveIndex && attr[lvAttrActiveIndex] == lvAttrActiveChar
}

func IsLVThinPool(attr string) bool {
	return len(attr) > 0 && attr[0] == lvAttrTypeThinPool
}

func IsLVThick(attr string) bool {
	return len(attr) > 0 && attr[0] == lvAttrTypeThick
}

// IsLVAttrKnown reports whether attr carries the LVM attribute fields the agent
// reads from it. `lvs` always reports the full 10-character string, but the cache
// holds a zero-valued LVData for an LV added right after `lvcreate` (see
// cache.AddLV) until the next scan fills it in, so callers have to tell a real
// attribute string from a placeholder before indexing into it.
func IsLVAttrKnown(attr string) bool {
	return len(attr) >= lvAttrMinLen
}

// IsLVContiguous reports whether the LV was allocated with the contiguous
// allocation policy (`lvcreate --contiguous y`).
func IsLVContiguous(attr string) bool {
	return len(attr) > lvAttrAllocPolicyIndex && attr[lvAttrAllocPolicyIndex] == lvAttrAllocContiguous
}

func FilterVGsByTag(vgs []internal.VGData, tags []string) []internal.VGData {
	filtered := make([]internal.VGData, 0, len(vgs))
	for _, vg := range vgs {
		for _, tag := range tags {
			if strings.Contains(vg.VGTags, tag) {
				filtered = append(filtered, vg)
				break
			}
		}
	}
	return filtered
}

// RunWithTimeout invokes fn under a child context with the given timeout, so a
// stuck nsenter-backed LVM command does not block the caller indefinitely.
// A non-positive timeout disables the deadline and is treated as "no timeout".
func RunWithTimeout[T any](ctx context.Context, timeout time.Duration, fn func(context.Context) (T, error)) (T, error) {
	if timeout <= 0 {
		return fn(ctx)
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return fn(ctx)
}

// ReattachFileDevices re-establishes loop device mappings for file-backed
// PVs after a node reboot. It iterates status.nodes[].fileDevices of every
// LVMVolumeGroup belonging to this node and runs `losetup --find --show`
// for files whose loop device is not attached yet. Must be called BEFORE
// ActivateAllManagedVGs so that LVM can see the PVs.
//
// On any partial failure the function still tries every remaining entry
// (best-effort) and returns a non-nil error joining all per-device
// failures so the caller can log/surface it. A non-nil return is NOT
// fatal: the startup caller deliberately continues to
// ActivateAllManagedVGs anyway, because holding every healthy VG on the
// node (including pure block-device ones) hostage to one unrelated file
// device would be worse. The LVG reconciler re-attaches idempotently via
// provisionFileDevices on its next pass, so a transient losetup failure
// recovers without a pod restart.
func ReattachFileDevices(ctx context.Context, log logger.Logger, commands Commands, cmdTimeout time.Duration, lvgs []LVGWithFileDevices) error {
	log = log.Named("ReattachFileDevices")

	var failures []error
	for _, item := range lvgs {
		for _, fd := range item.FileDevices {
			if fd.FilePath == "" {
				continue
			}
			if err := ctx.Err(); err != nil {
				return errors.Join(append(failures, fmt.Errorf("aborted: %w", err))...)
			}

			type findResult struct {
				cmd     string
				loopDev string
			}
			findRes, err := RunWithTimeout(ctx, cmdTimeout, func(ctx context.Context) (findResult, error) {
				cmd, existing, err := commands.FindLoopDeviceByFile(ctx, fd.FilePath)
				return findResult{cmd: cmd, loopDev: existing}, err
			})
			log.Debug(findRes.cmd)
			if err != nil {
				failures = append(failures, fmt.Errorf("LVG %s: query loop for %s: %w", item.LVGName, fd.FilePath, err))
				log.Warn(fmt.Sprintf("unable to query loop for %s: %v", fd.FilePath, err))
				continue
			}
			if findRes.loopDev != "" {
				log.Debug(fmt.Sprintf("%s already attached to %s", fd.FilePath, findRes.loopDev))
				continue
			}

			type setupResult struct {
				cmd     string
				loopDev string
			}
			setupRes, err := RunWithTimeout(ctx, cmdTimeout, func(ctx context.Context) (setupResult, error) {
				cmd, loopDev, err := commands.SetupLoopDevice(ctx, fd.FilePath)
				return setupResult{cmd: cmd, loopDev: loopDev}, err
			})
			log.Debug(setupRes.cmd)
			if err != nil {
				failures = append(failures, fmt.Errorf("LVG %s: setup loop for %s: %w", item.LVGName, fd.FilePath, err))
				log.Error(fmt.Sprintf("unable to reattach %s", fd.FilePath), logger.Err(err))
				continue
			}
			EnableLoopDirectIO(ctx, log, commands, cmdTimeout, setupRes.loopDev)
			log.Info(fmt.Sprintf("reattached %s → %s (LVG %s)", fd.FilePath, setupRes.loopDev, item.LVGName))
		}
	}
	if len(failures) > 0 {
		return errors.Join(failures...)
	}
	return nil
}

// EnableLoopDirectIO turns direct I/O on for a freshly attached loop device and
// never fails the caller for it.
//
// Direct I/O halves the page-cache footprint of a file-backed volume (see
// Commands.SetLoopDirectIO), so it is always worth asking for. But the kernel
// refuses it outright — LOOP_SET_DIRECT_IO returns -EINVAL — on a backing
// filesystem that has no ->direct_IO implementation or whose alignment does not
// fit, and buffered I/O is a performance regression, not a reason to leave a
// Volume Group unprovisioned. Every caller must ignore the outcome, which is
// exactly what this helper enforces by not returning it.
func EnableLoopDirectIO(ctx context.Context, log logger.Logger, commands Commands, cmdTimeout time.Duration, loopDev string) {
	if loopDev == "" {
		return
	}
	cmd, err := RunWithTimeout(ctx, cmdTimeout, func(ctx context.Context) (string, error) {
		return commands.SetLoopDirectIO(ctx, loopDev)
	})
	log.Debug(cmd)
	if err != nil {
		log.Named("EnableLoopDirectIO").Warn(fmt.Sprintf("direct I/O is unavailable for %s, continuing with buffered I/O (expect a higher page-cache footprint): %v", loopDev, err))
	}
}

// LVGWithFileDevices carries the minimal data needed by ReattachFileDevices.
type LVGWithFileDevices struct {
	LVGName     string
	FileDevices []FileDeviceStatus
}

// FileDeviceStatus is the status-level record of a single file device.
type FileDeviceStatus struct {
	FilePath   string
	LoopDevice string
}

func ActivateAllManagedVGs(ctx context.Context, log logger.Logger, commands Commands, metrics *monitoring.Metrics, cmdTimeout time.Duration) error {
	log = log.Named("ActivateVGs")

	log.Info("refreshing LVM metadata cache")
	if cmd, err := RunWithTimeout(ctx, cmdTimeout, func(ctx context.Context) (string, error) {
		return commands.PVScan(ctx)
	}); err != nil {
		log.Warn(fmt.Sprintf("pvscan --cache failed (cmd: %s): %v", cmd, err))
	}
	if cmd, err := RunWithTimeout(ctx, cmdTimeout, func(ctx context.Context) (string, error) {
		return commands.VGScan(ctx)
	}); err != nil {
		log.Warn(fmt.Sprintf("vgscan --cache failed (cmd: %s): %v", cmd, err))
	}

	type vgsResult struct {
		data   []internal.VGData
		cmdStr string
	}
	res, err := RunWithTimeout(ctx, cmdTimeout, func(ctx context.Context) (vgsResult, error) {
		data, cmdStr, _, err := commands.GetAllVGs(ctx)
		return vgsResult{data: data, cmdStr: cmdStr}, err
	})
	log.Debug(fmt.Sprintf("exec cmd: %s", res.cmdStr))
	if err != nil {
		return fmt.Errorf("unable to get VGs: %w", err)
	}

	// The tag is not an ownership proof for a loop-backed Volume Group, and this
	// function is the one that turns a wrong answer into a running guest's volumes
	// appearing on the host. An image of a node disk this module used to manage —
	// attached with `losetup` for a restore, or serving a nested cluster on a
	// rawfile-backed PersistentVolume — carries
	// storage.deckhouse.io/enabled=true, and `vgchange -ay` over it while the
	// guest is live gives the same extents two writers. Before spec.fileDevices
	// removed `loop` from LVMGlobalFilter this could not happen, because lvm.static
	// could not see such a Volume Group at all; ClassifyLoopVGs is what restores
	// that guarantee. See utils/loopvg.go.
	type pvsResult struct {
		data   []internal.PVData
		cmdStr string
	}
	pvsRes, pvsErr := RunWithTimeout(ctx, cmdTimeout, func(ctx context.Context) (pvsResult, error) {
		data, cmdStr, _, err := commands.GetAllPVs(ctx)
		return pvsResult{data: data, cmdStr: cmdStr}, err
	})
	log.Debug(fmt.Sprintf("exec cmd: %s", pvsRes.cmdStr))
	if pvsErr != nil {
		// Without the PV list every loop-only Volume Group is unclassifiable, so
		// activating anything risks activating somebody else's. Volumes not coming
		// up is an outage the scanner heals (EnsureVGActivation retries on every
		// cache fill); two writers on one Volume Group is not healable at all.
		return fmt.Errorf("unable to get PVs to establish which VGs are the module's own: %w", pvsErr)
	}

	verdicts := ClassifyLoopVGs(ctx, log, commands, cmdTimeout, res.data, pvsRes.data)
	managedVGs := SkipUnownedLoopVGs(log, "activate", FilterVGsByTag(res.data, internal.LVMTags), verdicts)
	if len(managedVGs) == 0 {
		log.Info("no managed VGs found, nothing to activate")
		return nil
	}

	log.Info(fmt.Sprintf("found %d managed VGs to activate", len(managedVGs)))

	var activationErrors []error
	for _, vg := range managedVGs {
		shared := vg.VGShared != ""
		cmd, err := RunWithTimeout(ctx, cmdTimeout, func(ctx context.Context) (string, error) {
			return commands.VGActivate(ctx, vg.VGName, shared)
		})
		if err != nil {
			log.Error(fmt.Sprintf("failed to activate VG %s (shared=%t, cmd: %s)", vg.VGName, shared, cmd), logger.Err(err))
			metrics.UtilsCommandsErrorsCount(activationControllerName, "vgchange").Inc()
			metrics.LVMActivationTotal(vg.VGName, "error").Inc()
			activationErrors = append(activationErrors, fmt.Errorf("VG %s: %w", vg.VGName, err))
			continue
		}
		metrics.UtilsCommandsExecutionCount(activationControllerName, "vgchange").Inc()
		metrics.LVMActivationTotal(vg.VGName, "success").Inc()
		log.Info(fmt.Sprintf("activated VG %s (shared=%t)", vg.VGName, shared))
	}

	if len(activationErrors) > 0 {
		return fmt.Errorf("failed to activate %d VGs", len(activationErrors))
	}
	return nil
}

// EnsureVGActivation activates the managed Volume Groups that have inactive
// thick or thin-pool logical volumes.
//
// verdicts must come from ClassifyLoopVGs over the same vgs/pvs the caller is
// about to cache: this function issues `vgchange -ay`, and a loop-backed Volume
// Group that merely carries the managed tag is not necessarily the module's —
// see ActivateAllManagedVGs and utils/loopvg.go for what goes wrong when it is
// assumed to be.
func EnsureVGActivation(
	ctx context.Context,
	log logger.Logger,
	commands Commands,
	metrics *monitoring.Metrics,
	vgs []internal.VGData,
	lvs []internal.LVData,
	verdicts LoopVGVerdicts,
	cmdTimeout time.Duration,
) bool {
	log = log.Named("EnsureActivation")

	managedVGs := SkipUnownedLoopVGs(log, "activate", FilterVGsByTag(vgs, internal.LVMTags), verdicts)
	if len(managedVGs) == 0 {
		return false
	}

	managedVGSet := make(map[string]internal.VGData, len(managedVGs))
	for _, vg := range managedVGs {
		managedVGSet[vg.VGName] = vg
	}

	vgsToActivate := make(map[string]internal.VGData)
	for _, lv := range lvs {
		vg, managed := managedVGSet[lv.VGName]
		if !managed {
			continue
		}

		if !IsLVThinPool(lv.LVAttr) && !IsLVThick(lv.LVAttr) {
			continue
		}

		if !IsLVActive(lv.LVAttr) {
			log.Warn(fmt.Sprintf("detected inactive LV %s/%s (attr=%s)", lv.VGName, lv.LVName, lv.LVAttr))
			vgsToActivate[vg.VGName] = vg
		}
	}

	if len(vgsToActivate) == 0 {
		return false
	}

	log.Info(fmt.Sprintf("activating %d VGs with inactive LVs", len(vgsToActivate)))
	activated := false
	for _, vg := range vgsToActivate {
		shared := vg.VGShared != ""
		cmd, err := RunWithTimeout(ctx, cmdTimeout, func(ctx context.Context) (string, error) {
			return commands.VGActivate(ctx, vg.VGName, shared)
		})
		if err != nil {
			log.Error(fmt.Sprintf("failed to activate VG %s (cmd: %s)", vg.VGName, cmd), logger.Err(err))
			metrics.UtilsCommandsErrorsCount(activationControllerName, "vgchange").Inc()
			metrics.LVMActivationTotal(vg.VGName, "error").Inc()
			continue
		}
		metrics.UtilsCommandsExecutionCount(activationControllerName, "vgchange").Inc()
		metrics.LVMActivationTotal(vg.VGName, "success").Inc()
		log.Info(fmt.Sprintf("activated VG %s (shared=%t)", vg.VGName, shared))
		activated = true
	}

	return activated
}
