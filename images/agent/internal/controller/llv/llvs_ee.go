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
	"github.com/deckhouse/sds-node-configurator/lib/go/common/pkg/feature"
	"k8s.io/apimachinery/pkg/types"
)

func (r *Reconciler) handleLLVSSource(ctx context.Context, llv *v1alpha1.LVMLogicalVolume, lvg *v1alpha1.LVMVolumeGroup) (string, bool, error) {
	if !feature.SnapshotsEnabled() {
		return "", false, errors.New("LVMLocalVolumeSnapshot as a source is not supported: snapshot feature is disabled")
	}

	sourceLLVS := &v1alpha1.LVMLogicalVolumeSnapshot{}
	if err := r.cl.Get(ctx, types.NamespacedName{Name: llv.Spec.Source.Name}, sourceLLVS); err != nil {
		r.log.Error(err, fmt.Sprintf("[reconcileLLVCreateFunc] unable to get source LVMLogicalVolumeSnapshot %s for the LVMLogicalVolume %s", llv.Spec.Source.Name, llv.Name))
		return "", true, err
	}

	if sourceLLVS.Status.ActualVGNameOnTheNode != lvg.Spec.ActualVGNameOnTheNode || sourceLLVS.Status.NodeName != lvg.Spec.Local.NodeName {
		return "", false, errors.New("restored volume should be in the same volume group as the origin volume")
	}

	cmd, err := r.commands.CreateThinLogicalVolumeFromSource(llv.Spec.ActualLVNameOnTheNode, sourceLLVS.Status.ActualVGNameOnTheNode, sourceLLVS.Spec.ActualSnapshotNameOnTheNode)

	return cmd, err != nil, err
}
