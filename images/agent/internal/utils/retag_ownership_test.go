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

package utils_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/mock_utils"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/monitoring"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/utils"
)

const legacyLinstorTag = "linstor-legacy"

func reTagScan(mc *mock_utils.MockCommands, vgs []internal.VGData, lvs []internal.LVData) {
	pvs := make([]internal.PVData, 0, len(vgs))
	for i, vg := range vgs {
		pvs = append(pvs, internal.PVData{PVName: "/dev/sd" + string(rune('b'+i)), VGUuid: vg.VGUUID, VGName: vg.VGName})
	}
	mc.EXPECT().GetAllVGs(gomock.Any()).Return(vgs, "lvm vgs", bytes.Buffer{}, nil)
	mc.EXPECT().GetAllPVs(gomock.Any()).Return(pvs, "lvm pvs", bytes.Buffer{}, nil)
	mc.EXPECT().GetAllLVs(gomock.Any()).Return(lvs, "lvm lvs", bytes.Buffer{}, nil)
}

func expectRetagged(t *testing.T, mc *mock_utils.MockCommands, vgName string) {
	mc.EXPECT().LVChangeDelTag(gomock.Any(), gomock.Any(), legacyLinstorTag).
		DoAndReturn(func(_ context.Context, lv internal.LVData, _ string) (string, error) {
			assert.Equal(t, vgName, lv.VGName)
			return "lvchange --deltag", nil
		})
	mc.EXPECT().VGChangeDelTag(gomock.Any(), vgName, legacyLinstorTag).Return("vgchange --deltag", nil)
	mc.EXPECT().VGChangeAddTag(gomock.Any(), vgName, managedTag).Return("vgchange --addtag", nil).Times(2)
}

func runReTag(t *testing.T, mc *mock_utils.MockCommands, owners utils.VGOwnershipSource) error {
	return utils.ReTagForTest(context.Background(), mc, testLogger(t), monitoring.GetMetrics(ownTestNode), "test", owners, 30*time.Second)
}

func TestReTag_RetagsAVGNamedByAnLVMVolumeGroupOfThisNode(t *testing.T) {
	ctrl := gomock.NewController(t)
	mc := mock_utils.NewMockCommands(ctrl)
	reTagScan(mc,
		[]internal.VGData{{VGName: "ours", VGUUID: "uuid-ours", VGTags: legacyLinstorTag}},
		[]internal.LVData{{LVName: "lv", VGName: "ours", LvTags: legacyLinstorTag}})
	expectRetagged(t, mc, "ours")

	assert.NoError(t, runReTag(t, mc, ownerOf("ours")))
}

func TestReTag_RetagsAVGWithThisClustersTagWithoutTheAPI(t *testing.T) {
	ctrl := gomock.NewController(t)
	mc := mock_utils.NewMockCommands(ctrl)
	tag := utils.ClusterTag(testClusterID)
	reTagScan(mc,
		[]internal.VGData{
			{VGName: "ours", VGUUID: "uuid-ours", VGTags: legacyLinstorTag + "," + tag},
			{VGName: "guest", VGUUID: "uuid-guest", VGTags: legacyLinstorTag},
		},
		[]internal.LVData{
			{LVName: "lv", VGName: "ours", LvTags: legacyLinstorTag},
			{LVName: "lv", VGName: "guest", LvTags: legacyLinstorTag},
		})
	expectRetagged(t, mc, "ours")

	owners := utils.NewVGOwnershipSource(testLogger(t), ownTestNode, tag, time.Second, &failingReader{})
	assert.NoError(t, runReTag(t, mc, owners))
}

func TestReTag_LeavesAVGTaggedByAnotherClusterAlone(t *testing.T) {
	ctrl := gomock.NewController(t)
	mc := mock_utils.NewMockCommands(ctrl)
	reTagScan(mc,
		[]internal.VGData{{VGName: "ours", VGUUID: "uuid-ours", VGTags: legacyLinstorTag + ",storage.deckhouse.io/cluster=other"}},
		[]internal.LVData{{LVName: "lv", VGName: "ours", LvTags: legacyLinstorTag}})

	assert.NoError(t, runReTag(t, mc, ownerOf("ours")))
}

func TestReTag_LeavesAGuestVGWithoutOwnershipAlone(t *testing.T) {
	ctrl := gomock.NewController(t)
	mc := mock_utils.NewMockCommands(ctrl)
	reTagScan(mc,
		[]internal.VGData{{VGName: "guest", VGUUID: "uuid-guest", VGTags: legacyLinstorTag}},
		[]internal.LVData{{LVName: "lv", VGName: "guest", LvTags: legacyLinstorTag}})

	assert.NoError(t, runReTag(t, mc, ownerOf("ours")))
}

func TestReTag_SkipsANameSharedByAnOwnedAndAForeignVG(t *testing.T) {
	ctrl := gomock.NewController(t)
	mc := mock_utils.NewMockCommands(ctrl)
	reTagScan(mc,
		[]internal.VGData{
			{VGName: "data", VGUUID: "uuid-ours", VGTags: legacyLinstorTag},
			{VGName: "data", VGUUID: "uuid-guest", VGTags: legacyLinstorTag},
		},
		[]internal.LVData{{LVName: "lv", VGName: "data", LvTags: legacyLinstorTag}})

	o := utils.NewVGOwnershipFromLVGs([]v1alpha1.LVMVolumeGroup{testLVG("own", ownTestNode, "data", "uuid-ours")}, ownTestNode, "")
	assert.NoError(t, runReTag(t, mc, func(context.Context) utils.VGOwnership { return o }))
}

func TestReTag_DoesNotAskForOwnersWithoutALegacyTag(t *testing.T) {
	ctrl := gomock.NewController(t)
	mc := mock_utils.NewMockCommands(ctrl)
	reTagScan(mc,
		[]internal.VGData{{VGName: "ours", VGUUID: "uuid-ours", VGTags: managedTag}},
		[]internal.LVData{{LVName: "lv", VGName: "ours"}})

	asked := 0
	owners := func(context.Context) utils.VGOwnership {
		asked++
		return utils.NewVGOwnershipFromClusterTag("")
	}
	assert.NoError(t, runReTag(t, mc, owners))
	assert.Zero(t, asked)
}
