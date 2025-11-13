//go:build !ce

/*
Copyright 2025 Flant JSC
Licensed under the Deckhouse Platform Enterprise Edition (EE) license. See https://github.com/deckhouse/deckhouse/blob/main/ee/LICENSE
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
	log := r.log.WithName("cleanupVolume").WithValues(
		"vgName", vgName,
		"method", cleanupMethod)
	if !feature.VolumeCleanupEnabled() {
		return false, fmt.Errorf("volume cleanup is not supported in your edition")
	}

	if cleanupMethod == v1alpha1.VolumeCleanupDiscard && lv.Data.PoolName != "" {
		err := errors.New("Discard cleanup method is disabled for thin volumes")
		log.Error(err, "Discard cleanup method is not allowed for thin volumes")
		return false, err
	}

	lvName := llv.Spec.ActualLVNameOnTheNode
	log = log.WithValues("lvName", lvName)
	started, prevFailedMethod := r.startCleanupRunning(vgName, lvName)
	if !started {
		log.Debug("cleanup already running for LV in VG")
		return false, errAlreadyRunning
	}
	log.Trace("starting cleaning up for LV in VG with method")
	defer func() {
		log.Trace("stopping cleaning up for LV in VG with method")
		err := r.stopCleanupRunning(vgName, lvName, prevFailedMethod)
		if err != nil {
			log.Error(err, "can't unregister running cleanup for LV in VG")
		}
	}()

	// prevent doing cleanup with previously failed method
	if prevFailedMethod != nil && *prevFailedMethod == cleanupMethod {
		log.Debug("was already failed with method for LV in VG", "failedMethod", *prevFailedMethod)
		return false, errCleanupSameAsPreviouslyFailed
	}

	if err := r.llvCl.UpdatePhaseIfNeeded(
		ctx,
		llv,
		v1alpha1.PhaseCleaning,
		fmt.Sprintf("Cleaning up volume %s in %s group using %s", lvName, vgName, cleanupMethod),
	); err != nil {
		log.Error(err, "changing phase to Cleaning")
		return true, fmt.Errorf("changing phase to Cleaning :%w", err)
	}

	prevFailedMethod = &cleanupMethod
	log.Debug("running cleanup for LV in VG with method")
	if shouldRetry, err := utils.VolumeCleanup(ctx, r.log, r.sdsCache, lv, cleanupMethod); err != nil {
		log.Error(err, "unable to clean up LV in VG with method")
		if shouldRetry {
			prevFailedMethod = nil
		}
		return shouldRetry, err
	}
	prevFailedMethod = nil
	return false, nil
}
