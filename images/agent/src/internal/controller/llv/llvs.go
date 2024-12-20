//go:build !ee

package llv

import (
	"context"
	"errors"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
)

func (r *Reconciler) handleLLVSSource(_ context.Context, _ *v1alpha1.LVMLogicalVolume, _ *v1alpha1.LVMVolumeGroup) (string, bool, error) {
	return "", false, errors.New("LLVS as a source is not supported")
}
