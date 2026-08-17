//go:build ce

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

package lsllv

import (
	"context"
	"fmt"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
)

// cleanupVolume refuses rather than pretends.
//
// Erasing a volume is an Enterprise Edition feature, and shared pools are one
// too, so this branch should be unreachable. If it is reached, the volume is
// left alone: removing it unerased would hand its contents to whoever gets the
// capacity next, and that is a worse outcome than a deletion that stops and
// says why.
func (r *Reconciler) cleanupVolume(
	_ context.Context,
	volume *v1alpha1.LVMSharedLogicalVolume,
	_ *v1alpha1.LVMSharedVolumeGroup,
	method string,
) (shouldRequeue bool, err error) {
	return false, fmt.Errorf(
		"volume cleanup %q is requested for %s but is not available in this edition; "+
			"the volume is left in place rather than removed unerased", method, volume.Name)
}
