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

package lvg

import (
	"context"
	"fmt"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/utils"
)

// servingLVGName returns the name of the LVMVolumeGroup that serves the Volume
// Group lvg names, when that is another resource; an empty string means lvg is
// the one to serve it.
//
// Several resources can describe one Volume Group of a node when the webhook
// that rejects the second one was unavailable or raced the first. Each of them
// would pass validation, report the Volume Group Ready, and retag it to its own
// name in updateVGTagIfNeeded, so the tag would flip between them on every pass
// and deleting the one the tag happened to name would remove the Volume Group
// the other still describes. Exactly one of them is served: the one the Volume
// Group is tagged for, or, while the tag names none of them, the oldest.
func servingLVGName(lvg *v1alpha1.LVMVolumeGroup, all []v1alpha1.LVMVolumeGroup, vgTagOwner string) string {
	serving := lvg
	for i := range all {
		other := &all[i]
		if other.Name == lvg.Name ||
			other.Spec.Local.NodeName != lvg.Spec.Local.NodeName ||
			other.Spec.ActualVGNameOnTheNode != lvg.Spec.ActualVGNameOnTheNode {
			continue
		}
		if other.Name == vgTagOwner {
			return other.Name
		}
		if serving.Name == vgTagOwner {
			continue
		}
		if older(other, serving) {
			serving = other
		}
	}

	if serving.Name == lvg.Name {
		return ""
	}
	return serving.Name
}

func older(a, b *v1alpha1.LVMVolumeGroup) bool {
	if !a.CreationTimestamp.Equal(&b.CreationTimestamp) {
		return a.CreationTimestamp.Before(&b.CreationTimestamp)
	}
	return a.Name < b.Name
}

// duplicateOf reports the LVMVolumeGroup that serves the Volume Group of lvg
// instead of it. See servingLVGName.
func (r *Reconciler) duplicateOf(ctx context.Context, lvg *v1alpha1.LVMVolumeGroup) (string, error) {
	var lvgs v1alpha1.LVMVolumeGroupList
	if err := r.cl.List(ctx, &lvgs); err != nil {
		return "", fmt.Errorf("list LVMVolumeGroups: %w", err)
	}

	var tagOwner string
	if found, vg := r.tryGetVG(lvg.Spec.ActualVGNameOnTheNode); found {
		_, tagOwner = utils.ReadValueFromTags(vg.VGTags, internal.LVMVolumeGroupTag)
	}

	return servingLVGName(lvg, lvgs.Items, tagOwner), nil
}
