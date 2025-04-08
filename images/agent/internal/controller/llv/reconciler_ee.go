//go:build !ce

/*
Copyright 2025 Flant JSC

Licensed under the Deckhouse Platform Enterprise Edition (EE) license.
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://github.com/deckhouse/deckhouse/blob/main/ee/LICENSE
*/

package llv

import (
	"context"
	"errors"
	"fmt"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/cache"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/utils"
	"github.com/deckhouse/sds-node-configurator/lib/go/common/pkg/feature"
)

func (r *Reconciler) cleanupVolume(ctx context.Context, llv *v1alpha1.LVMLogicalVolume, lv *cache.LVData, vgName string, cleanupMethod string) (shouldRequeue bool, err error) {
	if !feature.VolumeCleanupEnabled() {
		return false, fmt.Errorf("volume cleanup is not supported in your edition")
	}

	if cleanupMethod == v1alpha1.VolumeCleanupDiscard && lv.Data.PoolName != "" {
		err := errors.New("Discard cleanup method is disabled for thin volumes")
		r.log.Error(err, "[deleteLVIfNeeded] Discard cleanup method is not allowed for thin volumes")
		return false, err
	}

	lvName := llv.Spec.ActualLVNameOnTheNode
	started, prevFailedMethod := r.startCleanupRunning(vgName, lvName)
	if !started {
		r.log.Debug(fmt.Sprintf("[deleteLVIfNeeded] cleanup already running for LV %s in VG %s", lvName, vgName))
		return false, errAlreadyRunning
	}
	r.log.Trace(fmt.Sprintf("[deleteLVIfNeeded] starting cleaning up for LV %s in VG %s with method %s", lvName, vgName, cleanupMethod))
	defer func() {
		r.log.Trace(fmt.Sprintf("[deleteLVIfNeeded] stopping cleaning up for LV %s in VG %s with method %s", lvName, vgName, cleanupMethod))
		err := r.stopCleanupRunning(vgName, lvName, prevFailedMethod)
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[deleteLVIfNeeded] can't unregister running cleanup for LV %s in VG %s", lvName, vgName))
		}
	}()

	// prevent doing cleanup with previously failed method
	if prevFailedMethod != nil && *prevFailedMethod == cleanupMethod {
		r.log.Debug(fmt.Sprintf("[deleteLVIfNeeded] was already failed with method %s for LV %s in VG %s", *prevFailedMethod, lvName, vgName))
		return false, errCleanupSameAsPreviouslyFailed
	}

	if err := r.llvCl.UpdatePhaseIfNeeded(
		ctx,
		llv,
		v1alpha1.PhaseCleaning,
		fmt.Sprintf("Cleaning up volume %s in %s group using %s", lvName, vgName, cleanupMethod),
	); err != nil {
		r.log.Error(err, "[deleteLVIfNeeded] changing phase to Cleaning")
		return true, fmt.Errorf("changing phase to Cleaning :%w", err)
	}

	prevFailedMethod = &cleanupMethod
	r.log.Debug(fmt.Sprintf("[deleteLVIfNeeded] running cleanup for LV %s in VG %s with method %s", lvName, vgName, cleanupMethod))
	if shouldRetry, err := utils.VolumeCleanup(ctx, r.log, r.sdsCache, lv, cleanupMethod); err != nil {
		r.log.Error(err, fmt.Sprintf("[deleteLVIfNeeded] unable to clean up LV %s in VG %s with method %s", lvName, vgName, cleanupMethod))
		if shouldRetry {
			prevFailedMethod = nil
		}
		return shouldRetry, err
	}
	prevFailedMethod = nil
	return false, nil
}
