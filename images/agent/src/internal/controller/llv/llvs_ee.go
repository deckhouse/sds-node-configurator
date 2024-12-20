//go:build ee

package llv

import (
	"agent/internal/utils"
	"context"
	"errors"
	"fmt"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/types"
)

func (r *Reconciler) handleLLVSSource(ctx context.Context, llv *v1alpha1.LVMLogicalVolume, lvg *v1alpha1.LVMVolumeGroup) (string, bool, error) {
	sourceLLVS := &v1alpha1.LVMLogicalVolumeSnapshot{}
	if err := r.cl.Get(ctx, types.NamespacedName{Name: llv.Spec.Source.Name}, sourceLLVS); err != nil {
		r.log.Error(err, fmt.Sprintf("[reconcileLLVCreateFunc] unable to get source LVMLogicalVolumeSnapshot %s for the LVMLogicalVolume %s", llv.Spec.Source.Name, llv.Name))
		return "", true, err
	}

	if sourceLLVS.Status.ActualVGNameOnTheNode != lvg.Spec.ActualVGNameOnTheNode || sourceLLVS.Status.NodeName != lvg.Spec.Local.NodeName {
		return "", false, errors.New("restored volume should be in the same volume group as the origin volume")
	}

	cmd, err := utils.CreateThinLogicalVolumeFromSource(llv.Spec.ActualLVNameOnTheNode, sourceLLVS.Status.ActualVGNameOnTheNode, sourceLLVS.Spec.ActualSnapshotNameOnTheNode)

	return cmd, err != nil, err
}
