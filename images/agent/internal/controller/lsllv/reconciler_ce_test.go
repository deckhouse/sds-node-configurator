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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/controller"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/mock_utils"
)

func TestCleanupUnavailableRefusesToRemove(t *testing.T) {
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	volume := testVolume(deleting)
	// The marker is still set, and then everything stops: removing a volume
	// unerased would hand its contents to whoever gets the capacity next, which
	// is worse than a deletion that refuses and says why.
	r, cl := testReconciler(t, commands, []internal.LVData{existingLV("")}, testGroup(testNode), volume)

	commands.EXPECT().SetLVTagShared(gomock.Any(), testVG, testLV, PendingCleanupTag, true).Return("lvchange --addtag", nil)

	_, err := r.Reconcile(context.Background(),
		controller.ReconcileRequest[*v1alpha1.LVMSharedLogicalVolume]{Object: volume})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "left in place rather than removed unerased")

	got := &v1alpha1.LVMSharedLogicalVolume{}
	require.NoError(t, cl.Get(context.Background(), client.ObjectKey{Name: "vol-1"}, got))
	assert.Contains(t, got.Finalizers, internal.SdsNodeConfiguratorFinalizer)
}
