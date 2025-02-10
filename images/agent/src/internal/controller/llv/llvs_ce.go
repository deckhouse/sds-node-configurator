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

package llv

import (
	"context"
	"errors"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
)

func (r *Reconciler) handleLLVSSource(_ context.Context, _ *v1alpha1.LVMLogicalVolume, _ *v1alpha1.LVMVolumeGroup) (string, bool, error) {
	return "", false, errors.New("LVMLocalVolumeSnapshot as a source is not supported in your edition")
}
