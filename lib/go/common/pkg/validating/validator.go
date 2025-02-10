/*
Copyright 2025 Flant JSC

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

package validating

import (
	"context"
	"fmt"

	snc "github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/lib/go/common/pkg/feature"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func ValidateLVMLogicalVolumeSnapshot(ctx context.Context, cl client.Client, llvs *snc.LVMLogicalVolumeSnapshot, llv *snc.LVMLogicalVolume) (string, error) {
	if !feature.SnapshotsEnabled() {
		msg := "The snapshot feature is not available in your edition"
		return msg, nil
	}

	if llvs.DeletionTimestamp != nil {
		return "", nil
	}

	if llvs.Status == nil {
		// cl.Get(ctx, llvs.Spec.LVMLogicalVolumeName, llv)
		err := cl.Get(ctx, types.NamespacedName{Name: llvs.Spec.LVMLogicalVolumeName}, llv)
		if err != nil {
			return "", fmt.Errorf("failed to get source LVMLogicalVolume %s: %s", llvs.Spec.LVMLogicalVolumeName, err)
		}

		if llv.Spec.Thin == nil {
			return "Source LVMLogicalVolume %s is not thin provisioned. Snapshots are only supported for thin provisioned logical volumes", nil
		}
	}

	return "", nil
}
