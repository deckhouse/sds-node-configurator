//go:build !ce

/*
Copyright 2025 Flant JSC
Licensed under the Deckhouse Platform Enterprise Edition (EE) license. See https://github.com/deckhouse/deckhouse/blob/main/ee/LICENSE
*/

package lsllv

import (
	"context"
	"fmt"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/cache"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/utils"
)

// cleanupVolume erases the volume with the policy the pool demands.
//
// The volume has to be active for this: the erase writes to the device, not to
// metadata. It is activated exclusively here and released afterwards, which is
// safe because a volume being deleted has no attachment left — the attachment
// reconciler deactivates on its way out, and an attachment that still existed
// would have kept the volume from reaching deletion at all.
func (r *Reconciler) cleanupVolume(
	ctx context.Context,
	volume *v1alpha1.LVMSharedLogicalVolume,
	group *v1alpha1.LVMSharedVolumeGroup,
	method string,
) (shouldRequeue bool, err error) {
	vgName := group.Spec.ActualVGNameOnTheNode
	lvName := volume.Spec.ActualLVNameOnTheNode

	lv := r.findLV(vgName, lvName)
	if lv == nil {
		return false, nil
	}

	if !isActive(lv.LVAttr) {
		if cmd, err := r.commands.LVActivateShared(ctx, vgName, []string{lvName}, false); err != nil {
			return false, fmt.Errorf("activate %s/%s for cleanup (cmd: %s): %w", vgName, lvName, cmd, err)
		}
	}

	shouldRequeue, err = utils.VolumeCleanup(ctx, r.log, r.sdsCache, &cache.LVData{Data: *lv}, method)
	if err != nil {
		return shouldRequeue, err
	}
	if shouldRequeue {
		return true, nil
	}

	if cmd, deactivateErr := r.commands.LVDeactivateShared(ctx, vgName, []string{lvName}); deactivateErr != nil {
		// lvremove takes the lock this holds, so leaving it active would only
		// move the failure one step further along.
		return true, fmt.Errorf("release %s/%s after cleanup (cmd: %s): %w", vgName, lvName, cmd, deactivateErr)
	}

	return false, nil
}

func isActive(attr string) bool {
	return len(attr) > 4 && attr[4] == 'a'
}
