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
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/mock_utils"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/monitoring"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/test_utils"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/utils"
)

const (
	ownTestNode   = "test_node"
	testClusterID = "0b9c6d3e-4a59-4f6e-9a39-2a1f5ad1c0de"
	guestOwnerTag = "storage.deckhouse.io/lvmVolumeGroupName=vg-on-guest-worker-0"
)

func testLVG(name, node, vgName, vgUUID string) v1alpha1.LVMVolumeGroup {
	return v1alpha1.LVMVolumeGroup{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1alpha1.LVMVolumeGroupSpec{
			ActualVGNameOnTheNode: vgName,
			Type:                  internal.Local,
			Local:                 v1alpha1.LVMVolumeGroupLocalSpec{NodeName: node},
		},
		Status: v1alpha1.LVMVolumeGroupStatus{VGUuid: vgUUID},
	}
}

func ownerOf(vgNames ...string) utils.VGOwnershipSource {
	lvgs := make([]v1alpha1.LVMVolumeGroup, 0, len(vgNames))
	for _, name := range vgNames {
		lvgs = append(lvgs, testLVG("lvg-"+name, ownTestNode, name, ""))
	}
	o := utils.NewVGOwnershipFromLVGs(lvgs, ownTestNode, utils.ClusterTag(testClusterID))
	return func(context.Context) utils.VGOwnership { return o }
}

func TestClusterTag(t *testing.T) {
	assert.Equal(t, "storage.deckhouse.io/cluster="+testClusterID, utils.ClusterTag(testClusterID))
	assert.Empty(t, utils.ClusterTag(""))
	assert.Empty(t, utils.ClusterTag("a b"))
	assert.Empty(t, utils.ClusterTag("a,b"))
}

func TestHasTagMatchesWholeTags(t *testing.T) {
	tag := utils.ClusterTag(testClusterID)
	assert.True(t, utils.HasTag(managedTag+","+tag, tag))
	assert.False(t, utils.HasTag(managedTag+","+tag+"0", tag))
	assert.False(t, utils.HasTag(managedTag, tag))
	assert.False(t, utils.HasTag(managedTag, ""))
}

func TestForeignClusterTags(t *testing.T) {
	ours := utils.ClusterTag(testClusterID)
	theirs := utils.ClusterTag("11111111-2222-3333-4444-555555555555")
	assert.Equal(t, []string{theirs}, utils.ForeignClusterTags(managedTag+","+ours+","+theirs, ours))
	assert.Empty(t, utils.ForeignClusterTags(managedTag+","+ours, ours))
	assert.Empty(t, utils.ForeignClusterTags("x-storage.deckhouse.io/cluster=a", ours))
}

func TestVGOwnershipFromLVGs(t *testing.T) {
	o := utils.NewVGOwnershipFromLVGs([]v1alpha1.LVMVolumeGroup{
		testLVG("discovered", ownTestNode, "data", "uuid-data"),
		testLVG("fresh", ownTestNode, "fresh", ""),
		testLVG("elsewhere", "other-node", "remote", "uuid-remote"),
		testLVG("imported-guest", ownTestNode, "imported", ""),
	}, ownTestNode, utils.ClusterTag(testClusterID))

	tests := []struct {
		name string
		vg   internal.VGData
		want bool
	}{
		{"described by an LVMVolumeGroup of this node", internal.VGData{VGName: "data", VGUUID: "uuid-data", VGTags: managedTag}, true},
		{"same name, another UUID: a guest's group named like ours", internal.VGData{VGName: "data", VGUUID: "uuid-guest", VGTags: managedTag}, false},
		{"not described yet, so any UUID", internal.VGData{VGName: "fresh", VGUUID: "uuid-any", VGTags: managedTag}, true},
		{"described for another node", internal.VGData{VGName: "remote", VGUUID: "uuid-remote", VGTags: managedTag}, false},
		{"a nested cluster's group", internal.VGData{VGName: "vg-local", VGUUID: "uuid-guest", VGTags: managedTag + "," + guestOwnerTag}, false},
		{"described, tagged by this cluster", internal.VGData{VGName: "data", VGUUID: "uuid-data", VGTags: managedTag + "," + utils.ClusterTag(testClusterID)}, true},
		{"described, but tagged by another cluster", internal.VGData{VGName: "imported", VGUUID: "uuid-imported", VGTags: managedTag + ",storage.deckhouse.io/cluster=other"}, false},
		{"our cluster tag does not stand in for a resource", internal.VGData{VGName: "lost", VGUUID: "uuid-lost", VGTags: managedTag + "," + utils.ClusterTag(testClusterID)}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, o.Owns(tt.vg))
		})
	}
}

func TestVGOwnershipFromClusterTag(t *testing.T) {
	tag := utils.ClusterTag(testClusterID)
	o := utils.NewVGOwnershipFromClusterTag(tag)
	assert.True(t, o.Owns(internal.VGData{VGName: "data", VGTags: managedTag + "," + tag}))
	assert.False(t, o.Owns(internal.VGData{VGName: "vg-local", VGTags: managedTag + "," + guestOwnerTag}))
	assert.False(t, o.Owns(internal.VGData{VGName: "vg-local", VGTags: managedTag + ",storage.deckhouse.io/cluster=other"}))

	none := utils.NewVGOwnershipFromClusterTag("")
	assert.False(t, none.Owns(internal.VGData{VGName: "data", VGTags: managedTag + "," + tag}))
}

type failingReader struct {
	client.Reader
	calls int
}

func (r *failingReader) List(context.Context, client.ObjectList, ...client.ListOption) error {
	r.calls++
	return errors.New("apiserver unavailable")
}

func TestVGOwnershipSourceFallsBackReaderByReader(t *testing.T) {
	cl := test_utils.NewFakeClient()
	lvg := testLVG("own", ownTestNode, "data", "uuid-data")
	require.NoError(t, cl.Create(context.Background(), &lvg))
	tag := utils.ClusterTag(testClusterID)

	cache := &failingReader{}
	o := utils.NewVGOwnershipSource(testLogger(t), ownTestNode, tag, time.Second, cache, cl)(context.Background())
	assert.Equal(t, 1, cache.calls)
	assert.True(t, o.Owns(internal.VGData{VGName: "data", VGUUID: "uuid-data"}))
	assert.False(t, o.Owns(internal.VGData{VGName: "tagged", VGTags: tag}))

	o = utils.NewVGOwnershipSource(testLogger(t), ownTestNode, tag, time.Second, &failingReader{}, &failingReader{})(context.Background())
	assert.False(t, o.Owns(internal.VGData{VGName: "data", VGUUID: "uuid-data"}))
	assert.True(t, o.Owns(internal.VGData{VGName: "tagged", VGTags: tag}))
}

func TestEnsureVGActivation_LeavesANestedClustersVGAlone(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)

	vgs := []internal.VGData{
		{VGName: "csecheck", VGUUID: "uuid-guest", VGTags: managedTag + "," + guestOwnerTag},
		{VGName: "ours", VGUUID: "uuid-ours", VGTags: managedTag},
	}
	lvs := []internal.LVData{
		{LVName: "thin-pool", VGName: "csecheck", LVAttr: "twi---tz--"},
		{LVName: "data", VGName: "ours", LVAttr: "-wi-------"},
	}
	mc.EXPECT().VGActivate(gomock.Any(), "ours", false).Return("vgchange -ay ours", nil)

	activated := utils.EnsureVGActivation(context.Background(), testLogger(t), mc,
		monitoring.GetMetrics(ownTestNode), vgs, lvs, nil, ownerOf("ours"), 30*time.Second)
	assert.True(t, activated)
}

func TestEnsureVGActivation_ActivatesNothingWithoutAnOwner(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)

	vgs := []internal.VGData{{VGName: "e2e-vg", VGUUID: "uuid-guest", VGTags: managedTag}}
	lvs := []internal.LVData{{LVName: "thin-0", VGName: "e2e-vg", LVAttr: "twi---tz--"}}

	assert.False(t, utils.EnsureVGActivation(context.Background(), testLogger(t), mc,
		monitoring.GetMetrics(ownTestNode), vgs, lvs, nil, ownerOf(), 30*time.Second))
	assert.False(t, utils.EnsureVGActivation(context.Background(), testLogger(t), mc,
		monitoring.GetMetrics(ownTestNode), vgs, lvs, nil, nil, 30*time.Second))
}

func TestEnsureVGActivation_AsksForOwnersOnlyWhenSomethingIsInactive(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)

	asked := 0
	owners := func(context.Context) utils.VGOwnership {
		asked++
		return utils.NewVGOwnershipFromClusterTag("")
	}
	vgs := []internal.VGData{{VGName: "ours", VGUUID: "uuid-ours", VGTags: managedTag}}
	lvs := []internal.LVData{{LVName: "data", VGName: "ours", LVAttr: "-wi-a-----"}}

	assert.False(t, utils.EnsureVGActivation(context.Background(), testLogger(t), mc,
		monitoring.GetMetrics(ownTestNode), vgs, lvs, nil, owners, 30*time.Second))
	assert.Zero(t, asked)
}

func TestActivateAllManagedVGs_LeavesANestedClustersVGAlone(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)

	mc.EXPECT().PVScan(gomock.Any()).Return("pvscan --cache", nil)
	mc.EXPECT().VGScan(gomock.Any()).Return("vgscan --cache", nil)
	mc.EXPECT().GetAllVGs(gomock.Any()).Return([]internal.VGData{
		{VGName: "pkarpov_repl_test", VGUUID: "uuid-guest", VGTags: managedTag + "," + guestOwnerTag},
		{VGName: "ours", VGUUID: "uuid-ours", VGTags: managedTag},
	}, "lvm vgs", bytes.Buffer{}, nil)
	mc.EXPECT().GetAllPVs(gomock.Any()).Return([]internal.PVData{
		{PVName: "/dev/mapper/mpatha", VGUuid: "uuid-guest", VGName: "pkarpov_repl_test"},
		{PVName: "/dev/sdb", VGUuid: "uuid-ours", VGName: "ours"},
	}, "lvm pvs", bytes.Buffer{}, nil)
	mc.EXPECT().VGActivate(gomock.Any(), "ours", false).Return("vgchange -ay ours", nil)

	err := utils.ActivateAllManagedVGs(context.Background(), testLogger(t), mc, monitoring.GetMetrics(ownTestNode), ownerOf("ours"), 30*time.Second)
	assert.NoError(t, err)
}

func TestActivateAllManagedVGs_GoesByTheClusterTagWithoutTheAPI(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	tag := utils.ClusterTag(testClusterID)

	mc.EXPECT().PVScan(gomock.Any()).Return("pvscan --cache", nil)
	mc.EXPECT().VGScan(gomock.Any()).Return("vgscan --cache", nil)
	mc.EXPECT().GetAllVGs(gomock.Any()).Return([]internal.VGData{
		{VGName: "guest", VGUUID: "uuid-guest", VGTags: managedTag + "," + guestOwnerTag + ",storage.deckhouse.io/cluster=other"},
		{VGName: "ours", VGUUID: "uuid-ours", VGTags: managedTag + "," + tag},
	}, "lvm vgs", bytes.Buffer{}, nil)
	mc.EXPECT().GetAllPVs(gomock.Any()).Return([]internal.PVData{
		{PVName: "/dev/sdc", VGUuid: "uuid-guest", VGName: "guest"},
		{PVName: "/dev/sdb", VGUuid: "uuid-ours", VGName: "ours"},
	}, "lvm pvs", bytes.Buffer{}, nil)
	mc.EXPECT().VGActivate(gomock.Any(), "ours", false).Return("vgchange -ay ours", nil)

	owners := utils.NewVGOwnershipSource(testLogger(t), ownTestNode, tag, time.Second, &failingReader{})
	err := utils.ActivateAllManagedVGs(context.Background(), testLogger(t), mc, monitoring.GetMetrics(ownTestNode), owners, 30*time.Second)
	assert.NoError(t, err)
}
