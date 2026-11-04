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

package llv

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/cache"
	"github.com/deckhouse/sds-node-configurator/lib/go/common/pkg/feature"
)

func TestCleanupVolumeAcceptedWhereEnabled(t *testing.T) {
	r := setupReconciler()
	llv := &v1alpha1.LVMLogicalVolume{Spec: v1alpha1.LVMLogicalVolumeSpec{ActualLVNameOnTheNode: "lv"}}
	lv := &cache.LVData{Data: internal.LVData{VGName: "vg", LVName: "lv", PoolName: "pool"}, Exist: true}

	_, err := r.cleanupVolume(context.Background(), llv, lv, "vg", v1alpha1.VolumeCleanupDiscard)
	require.Error(t, err)

	rejected := strings.Contains(err.Error(), "not supported in your edition")
	assert.Equal(t, !feature.VolumeCleanupEnabled(), rejected, "VolumeCleanupEnabled() = %v, cleanupVolume returned %q", feature.VolumeCleanupEnabled(), err)
	if feature.VolumeCleanupEnabled() {
		assert.Contains(t, err.Error(), "Discard cleanup method is disabled for thin volumes")
	}
}
