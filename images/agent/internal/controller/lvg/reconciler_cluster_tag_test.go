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
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/cache"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/mock_utils"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/monitoring"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/test_utils"
)

const (
	testClusterTag  = "storage.deckhouse.io/cluster=0b9c6d3e-4a59-4f6e-9a39-2a1f5ad1c0de"
	otherClusterTag = "storage.deckhouse.io/cluster=11111111-2222-3333-4444-555555555555"
)

func reconcilerWithClusterTag(mc *mock_utils.MockCommands, clusterTag string) *Reconciler {
	cl := test_utils.NewFakeClient(&v1alpha1.LVMVolumeGroup{}, &v1alpha1.LVMLogicalVolume{})
	return NewReconciler(cl, UncachedReader{Reader: cl}, logger.NewNop(), monitoring.GetMetrics(""), cache.New(), mc, nil,
		ReconcilerConfig{NodeName: "test_node", CmdDeadlineDuration: 30 * time.Second, ClusterTag: clusterTag})
}

func TestEnsureClusterTag_TagsAnUntaggedVG(t *testing.T) {
	ctrl := gomock.NewController(t)
	mc := mock_utils.NewMockCommands(ctrl)
	mc.EXPECT().VGChangeAddTag(gomock.Any(), "data", testClusterTag).Return("vgchange --addtag", nil)

	r := reconcilerWithClusterTag(mc, testClusterTag)
	assert.NoError(t, r.ensureClusterTag(context.Background(), internal.VGData{VGName: "data", VGTags: internal.LVMTags[0]}))
}

func TestEnsureClusterTag_LeavesATaggedVGAlone(t *testing.T) {
	ctrl := gomock.NewController(t)
	mc := mock_utils.NewMockCommands(ctrl)

	r := reconcilerWithClusterTag(mc, testClusterTag)
	assert.NoError(t, r.ensureClusterTag(context.Background(), internal.VGData{VGName: "data", VGTags: internal.LVMTags[0] + "," + testClusterTag}))
}

func TestEnsureClusterTag_LeavesAnotherClustersVGAlone(t *testing.T) {
	ctrl := gomock.NewController(t)
	mc := mock_utils.NewMockCommands(ctrl)

	r := reconcilerWithClusterTag(mc, testClusterTag)
	err := r.ensureClusterTag(context.Background(), internal.VGData{VGName: "data", VGTags: internal.LVMTags[0] + "," + otherClusterTag})
	assert.ErrorContains(t, err, otherClusterTag)
}

func TestEnsureClusterTag_DoesNothingWithoutAClusterTag(t *testing.T) {
	ctrl := gomock.NewController(t)
	mc := mock_utils.NewMockCommands(ctrl)

	r := reconcilerWithClusterTag(mc, "")
	assert.NoError(t, r.ensureClusterTag(context.Background(), internal.VGData{VGName: "data", VGTags: internal.LVMTags[0] + "," + otherClusterTag}))
}

func TestEnsureClusterTag_ReportsAFailedVgchange(t *testing.T) {
	ctrl := gomock.NewController(t)
	mc := mock_utils.NewMockCommands(ctrl)
	mc.EXPECT().VGChangeAddTag(gomock.Any(), "data", testClusterTag).Return("vgchange --addtag", errors.New("lvm failed"))

	r := reconcilerWithClusterTag(mc, testClusterTag)
	assert.Error(t, r.ensureClusterTag(context.Background(), internal.VGData{VGName: "data", VGTags: internal.LVMTags[0]}))
}
