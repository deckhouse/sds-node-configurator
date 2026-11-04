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
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/cache"
	mock_utils "github.com/deckhouse/sds-node-configurator/images/agent/internal/mock_utils"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/monitoring"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/test_utils"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/utils"
)

// bdRaceCacheWithTags is bdRaceCache with the Volume Group's tags chosen by the
// test: the owner tag is what the import decision turns on.
func bdRaceCacheWithTags(t *testing.T, tags string) *cache.Cache {
	t.Helper()
	sdsCache := cache.New()
	sdsCache.StoreVGs([]internal.VGData{{
		VGName:       bdRaceVGName,
		VGUUID:       bdRaceVGUUID,
		VGTags:       tags,
		VGSize:       resource.MustParse("10Gi"),
		VGFree:       resource.MustParse("10Gi"),
		VGExtentSize: resource.MustParse("4Mi"),
	}}, bytes.Buffer{})
	sdsCache.StorePVs([]internal.PVData{{
		PVName: bdRacePVName,
		VGName: bdRaceVGName,
		VGUuid: bdRaceVGUUID,
		PVSize: resource.MustParse("10Gi"),
		PVUuid: "pv-uuid-sdc",
	}}, bytes.Buffer{})
	sdsCache.StoreLVs(nil, bytes.Buffer{})

	return sdsCache
}

// A Volume Group whose owner tag names an LVMVolumeGroup this cluster does not have
// was created by another cluster's agent: on a hypervisor, by a nested cluster on a
// LUN this node can see. It used to be imported under that name — which is how 108
// of 113 LVMVolumeGroups on a hypervisor came to describe guests' storage.
func TestLVMVolumeGroupDiscoverReconcile_DoesNotImportAVolumeGroupNamingAnAbsentLVMVolumeGroup(t *testing.T) {
	ctx := context.Background()
	cl := test_utils.NewFakeClient(&v1alpha1.LVMVolumeGroup{})
	// Every PV has its BlockDevice, so nothing but the tag stands between the
	// candidate and an import.
	require.NoError(t, cl.Create(ctx, bdRaceBlockDevice(true)))

	// bdRaceCache tags the Volume Group with bdRaceLVGName, and no such resource
	// exists here.
	sdsCache := bdRaceCache(t)
	d := bdRaceDiscoverer(t, cl, UncachedReader{Reader: cl}, sdsCache)

	before := refusedImportCount(t, d.metrics, bdRaceLVGName)

	assert.False(t, d.LVMVolumeGroupDiscoverReconcile(ctx),
		"the refusal is terminal: nothing on this node can change the answer, so there is nothing to requeue for")

	var lvgs v1alpha1.LVMVolumeGroupList
	require.NoError(t, cl.List(ctx, &lvgs))
	assert.Empty(t, lvgs.Items, "a Volume Group named after another cluster's resource is not imported")

	assert.Equal(t, before+1, refusedImportCount(t, d.metrics, bdRaceLVGName),
		"the refusal is counted under the name the tag carries")

	refused, _ := d.rememberedState()
	assert.Contains(t, refused[bdRaceVGName], "does not exist in this cluster")
	assert.Contains(t, refused[bdRaceVGName], "vgchange --deltag "+internal.LVMVolumeGroupTag+"="+bdRaceLVGName,
		"the remedy is in the message: the refusal leaves nothing in the API to read it from")

	// A standing refusal is a state, not a stream of events.
	assert.False(t, d.LVMVolumeGroupDiscoverReconcile(ctx))
	assert.Equal(t, before+1, refusedImportCount(t, d.metrics, bdRaceLVGName),
		"the same refusal on the next pass is not counted again")
	require.NoError(t, cl.List(ctx, &lvgs))
	assert.Empty(t, lvgs.Items)
}

// The documented way to take such a Volume Group under management when it really is
// this node's: create the LVMVolumeGroup under the tagged name. The next pass finds
// the resource for the Volume Group and takes the update path — adoption, not a
// second import.
func TestLVMVolumeGroupDiscoverReconcile_AdoptsTheVolumeGroupOnceTheNamedLVMVolumeGroupIsCreated(t *testing.T) {
	ctx := context.Background()
	cl := test_utils.NewFakeClient(&v1alpha1.LVMVolumeGroup{})
	require.NoError(t, cl.Create(ctx, bdRaceBlockDevice(true)))

	sdsCache := bdRaceCache(t)
	d := bdRaceDiscoverer(t, cl, UncachedReader{Reader: cl}, sdsCache)

	require.False(t, d.LVMVolumeGroupDiscoverReconcile(ctx))
	var lvgs v1alpha1.LVMVolumeGroupList
	require.NoError(t, cl.List(ctx, &lvgs))
	require.Empty(t, lvgs.Items, "refused first")

	// The operator creates the resource by hand, with no status: that is what the
	// FAQ tells them to do.
	adopted := bdRaceLVG()
	adopted.Status = v1alpha1.LVMVolumeGroupStatus{}
	require.NoError(t, cl.Create(ctx, adopted))

	assert.False(t, d.LVMVolumeGroupDiscoverReconcile(ctx))

	var got v1alpha1.LVMVolumeGroup
	require.NoError(t, cl.Get(ctx, client.ObjectKey{Name: bdRaceLVGName}, &got))
	assert.Equal(t, bdRaceVGUUID, got.Status.VGUuid, "the existing Volume Group is described on the resource the operator created")
	assert.Equal(t, "10Gi", got.Status.VGSize.String())
	require.Len(t, got.Status.Nodes, 1)
	assert.Equal(t, bdRaceNode, got.Status.Nodes[0].Name)

	require.NoError(t, cl.List(ctx, &lvgs))
	assert.Len(t, lvgs.Items, 1, "and no second resource is minted for it")

	refused, _ := d.rememberedState()
	assert.NotContains(t, refused, bdRaceVGName, "the refusal is gone with its cause")
}

// A Volume Group an operator handed over carries the managed tag and no name — the
// agent has not written one yet. That is the import this module documents, and it
// still happens.
func TestLVMVolumeGroupDiscoverReconcile_ImportsAHandedOverVolumeGroupWithoutAnOwnerTag(t *testing.T) {
	ctx := context.Background()
	cl := test_utils.NewFakeClient(&v1alpha1.LVMVolumeGroup{})
	require.NoError(t, cl.Create(ctx, bdRaceBlockDevice(true)))

	sdsCache := bdRaceCacheWithTags(t, "storage.deckhouse.io/enabled=true")
	d := bdRaceDiscoverer(t, cl, UncachedReader{Reader: cl}, sdsCache)

	assert.False(t, d.LVMVolumeGroupDiscoverReconcile(ctx))

	var lvgs v1alpha1.LVMVolumeGroupList
	require.NoError(t, cl.List(ctx, &lvgs))
	require.Len(t, lvgs.Items, 1, "a Volume Group with no owner tag is imported as before")
	assert.True(t, strings.HasPrefix(lvgs.Items[0].Name, "vg-"), "under a generated name")
	assert.Equal(t, bdRaceVGName, lvgs.Items[0].Spec.ActualVGNameOnTheNode)
	assert.Equal(t, bdRaceNode, lvgs.Items[0].Spec.Local.NodeName)
}

// The tag's presence is what refuses, not its usability: a value that is not a
// valid resource name still says "made by an agent of this module", and the old
// fallback — import under a generated name — is exactly the import this refusal
// exists to stop.
func TestLVMVolumeGroupDiscoverReconcile_DoesNotImportUnderAGeneratedNameWhenTheOwnerTagIsUnusable(t *testing.T) {
	ctx := context.Background()
	cl := test_utils.NewFakeClient(&v1alpha1.LVMVolumeGroup{})
	require.NoError(t, cl.Create(ctx, bdRaceBlockDevice(true)))

	// Uppercase is not a DNS-1123 subdomain, so lvgNameForCandidate mints a name.
	sdsCache := bdRaceCacheWithTags(t, "storage.deckhouse.io/enabled=true,"+internal.LVMVolumeGroupTag+"=Guest-LVG")
	d := bdRaceDiscoverer(t, cl, UncachedReader{Reader: cl}, sdsCache)

	beforeUnnamed := refusedImportCount(t, d.metrics, "")
	beforeRaw := refusedImportCount(t, d.metrics, "Guest-LVG")

	assert.False(t, d.LVMVolumeGroupDiscoverReconcile(ctx))

	var lvgs v1alpha1.LVMVolumeGroupList
	require.NoError(t, cl.List(ctx, &lvgs))
	assert.Empty(t, lvgs.Items)

	refused, _ := d.rememberedState()
	assert.Contains(t, refused[bdRaceVGName], "Guest-LVG", "the log carries the raw tag: the operator needs it for vgchange --deltag")

	// The raw tag is another cluster's string and does not become a series of
	// this node's telemetry; an unusable name is counted under an empty label,
	// like every other refusal that had no name of its own.
	assert.Equal(t, beforeUnnamed+1, refusedImportCount(t, d.metrics, ""))
	assert.Equal(t, beforeRaw, refusedImportCount(t, d.metrics, "Guest-LVG"))
}

func TestForeignOwnerRefusalReason(t *testing.T) {
	got := foreignOwnerRefusalReason(internal.LVMVolumeGroupCandidate{
		ActualVGNameOnTheNode: "vg-local",
		OwnerTag:              "lvg-worker-1-8seu3",
	}, "hypervisor-3")

	for _, want := range []string{
		internal.LVMVolumeGroupTag + " tag names the LVMVolumeGroup lvg-worker-1-8seu3, which does not exist in this cluster",
		"create an LVMVolumeGroup named lvg-worker-1-8seu3 with spec.actualVGNameOnTheNode=vg-local and spec.local.nodeName=hypervisor-3",
		"vgchange --deltag " + internal.LVMVolumeGroupTag + "=lvg-worker-1-8seu3 vg-local",
	} {
		assert.Contains(t, got, want)
	}
}

// A file-backed Volume Group is the one kind that keeps re-importing under the name
// in its tag: its Physical Volumes are loop devices over files this agent named
// after the owning LVMVolumeGroup, in its own directory on this node — nothing a
// nested cluster can produce — and the tag is the only place the name survives the
// loss of the resource.
func TestLVMVolumeGroupDiscoverReconcile_StillReImportsAFileBackedVolumeGroupUnderItsTaggedName(t *testing.T) {
	const (
		lvgName  = "lvg-file-backed"
		vgOnNode = "vg-file-backed"
		node     = "test_node"
	)

	cl := test_utils.NewFakeClient(&v1alpha1.LVMVolumeGroup{})
	sdsCache := cache.New()
	sdsCache.StoreVGs([]internal.VGData{{
		VGName:       vgOnNode,
		VGUUID:       "vg-uuid-file-backed",
		VGTags:       "storage.deckhouse.io/enabled=true," + internal.LVMVolumeGroupTag + "=" + lvgName,
		VGSize:       resource.MustParse("10Gi"),
		VGFree:       resource.MustParse("10Gi"),
		VGExtentSize: resource.MustParse("4Mi"),
	}}, bytes.Buffer{})
	sdsCache.StorePVs([]internal.PVData{{
		PVName: "/dev/loop3",
		VGName: vgOnNode,
		VGUuid: "vg-uuid-file-backed",
		PVSize: resource.MustParse("10Gi"),
	}}, bytes.Buffer{})
	sdsCache.StoreLVs(nil, bytes.Buffer{})

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	backing := utils.BuildFileDevicePath("/opt/deckhouse/sds/file-devices", lvgName, "d1")
	mc.EXPECT().GetLoopBackingFile(gomock.Any(), "/dev/loop3").
		Return("losetup -O BACK-FILE", internal.LoopBackingFile{Path: backing}, nil).AnyTimes()
	mc.EXPECT().GetFilesystemSpace(gomock.Any(), gomock.Any()).
		Return("stat -f", internal.FilesystemSpace{AvailableBytes: int64(1) << 40, TotalBytes: int64(1) << 40}, nil).AnyTimes()

	d := NewDiscoverer(cl, UncachedReader{Reader: cl}, logger.NewNop(), monitoring.GetMetrics(node), sdsCache, mc, nil, DiscovererConfig{
		NodeName:                node,
		VolumeGroupScanInterval: time.Second,
		FileDevicesDirectory:    "/opt/deckhouse/sds/file-devices",
	})

	assert.False(t, d.LVMVolumeGroupDiscoverReconcile(context.Background()))

	var lvgs v1alpha1.LVMVolumeGroupList
	require.NoError(t, cl.List(context.Background(), &lvgs))
	require.Len(t, lvgs.Items, 1, "the file-backed Volume Group is re-imported")
	assert.Equal(t, lvgName, lvgs.Items[0].Name, "under the name its backing files carry")
	assert.Equal(t, vgOnNode, lvgs.Items[0].Spec.ActualVGNameOnTheNode)
	require.Len(t, lvgs.Items[0].Spec.FileDevices, 1, "spec.fileDevices is reconstructed from the backing file")
}
