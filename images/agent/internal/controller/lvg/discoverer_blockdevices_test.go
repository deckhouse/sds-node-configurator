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
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/cache"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/logger"
	mock_utils "github.com/deckhouse/sds-node-configurator/images/agent/internal/mock_utils"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/monitoring"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/test_utils"
)

// The block-device flavour of the fixtures the file-device tests use: one node,
// one VG on one PV, no loop devices anywhere, so no host command is reachable.
const (
	bdRaceNode    = "worker-1"
	bdRaceVGName  = "vg-thin-data"
	bdRaceVGUUID  = "vg-uuid-thin-data"
	bdRaceLVGName = "vg-data-worker-1-sdc"
	bdRacePVName  = "/dev/sdc"
	bdRaceBDName  = "dev-sdc-worker-1"
)

func bdRaceCache(t *testing.T) *cache.Cache {
	t.Helper()
	sdsCache := cache.New()
	sdsCache.StoreVGs([]internal.VGData{{
		VGName:       bdRaceVGName,
		VGUUID:       bdRaceVGUUID,
		VGTags:       "storage.deckhouse.io/enabled=true," + internal.LVMVolumeGroupTag + "=" + bdRaceLVGName,
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

// bdRaceNodeLabelled stamps the node label the block-device discoverer writes on
// every BlockDevice it publishes (see internal.newBlockDeviceLabels).
//
// The fixtures need it because the uncached re-read selects by it: a fixture
// without the label is not the object the API server would be holding, and the
// re-read would come back empty — which the discoverer is entitled to read as
// "this node has no such devices".
func bdRaceNodeLabelled(bd *v1alpha1.BlockDevice) *v1alpha1.BlockDevice {
	if bd.Labels == nil {
		bd.Labels = map[string]string{}
	}
	bd.Labels[internal.HostNameLabelKey] = internal.BlockDeviceLabelValue(bd.Status.NodeName)

	return bd
}

// bdRaceBlockDevice is the BlockDevice for the VG's only PV. Filling in the
// actualVGNameOnTheNode/vgUuid pair is what the block-device discoverer does once
// the VG exists, and what this discoverer matches a PV by.
func bdRaceBlockDevice(reported bool) *v1alpha1.BlockDevice {
	bd := &v1alpha1.BlockDevice{
		ObjectMeta: metav1.ObjectMeta{Name: bdRaceBDName},
		Status: v1alpha1.BlockDeviceStatus{
			Path:     bdRacePVName,
			NodeName: bdRaceNode,
			Size:     resource.MustParse("10Gi"),
		},
	}
	if reported {
		bd.Status.ActualVGNameOnTheNode = bdRaceVGName
		bd.Status.VGUuid = bdRaceVGUUID
	}

	return bdRaceNodeLabelled(bd)
}

// bdRaceLVG is an LVMVolumeGroup the agent has already described once, so there
// is something in status.nodes for an incomplete pass to destroy.
func bdRaceLVG() *v1alpha1.LVMVolumeGroup {
	return &v1alpha1.LVMVolumeGroup{
		ObjectMeta: metav1.ObjectMeta{Name: bdRaceLVGName},
		Spec: v1alpha1.LVMVolumeGroupSpec{
			ActualVGNameOnTheNode: bdRaceVGName,
			Type:                  internal.Local,
			Local:                 v1alpha1.LVMVolumeGroupLocalSpec{NodeName: bdRaceNode},
		},
		Status: v1alpha1.LVMVolumeGroupStatus{
			VGUuid:     bdRaceVGUUID,
			VGSize:     resource.MustParse("10Gi"),
			VGFree:     resource.MustParse("10Gi"),
			ExtentSize: resource.MustParse("4Mi"),
			Nodes: []v1alpha1.LVMVolumeGroupNode{{
				Name: bdRaceNode,
				Devices: []v1alpha1.LVMVolumeGroupDevice{{
					Path:        bdRacePVName,
					BlockDevice: bdRaceBDName,
					PVUuid:      "pv-uuid-sdc",
					PVSize:      resource.MustParse("10Gi"),
					DevSize:     resource.MustParse("10Gi"),
				}},
			}},
		},
	}
}

func bdRaceDiscoverer(t *testing.T, cl client.Client, bdAPIReader UncachedReader, sdsCache *cache.Cache) *Discoverer {
	t.Helper()
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	mc := mock_utils.NewMockCommands(ctrl)
	// A PV with no BlockDevice is probed as a loop device first, because lvm.static
	// without udev reports a managed loop PV under an alias. /dev/sdc is not one, so
	// the probe finds no backing file and the PV stays unaccounted for.
	mc.EXPECT().GetLoopBackingFile(gomock.Any(), bdRacePVName).
		Return("losetup -O BACK-FILE", internal.LoopBackingFile{}, nil).AnyTimes()

	return NewDiscoverer(cl, bdAPIReader, logger.Logger{}, monitoring.GetMetrics(bdRaceNode), sdsCache,
		mc, DiscovererConfig{
			NodeName:                bdRaceNode,
			VolumeGroupScanInterval: time.Second,
		})
}

func bdRaceVGReadyCondition(t *testing.T, lvg v1alpha1.LVMVolumeGroup) *metav1.Condition {
	t.Helper()
	for i := range lvg.Status.Conditions {
		if lvg.Status.Conditions[i].Type == internal.TypeVGReady {
			return &lvg.Status.Conditions[i]
		}
	}

	return nil
}

// A PV whose BlockDevice has not been reported yet leaves the device set
// incomplete, and the candidate has to say so: it is the difference between "this
// node has no devices in this VG" and "this node was not able to tell".
func TestConfigureCandidateNodeDevices_ReportsAnUnreportedBlockDevice(t *testing.T) {
	vg := internal.VGData{VGName: bdRaceVGName, VGUUID: bdRaceVGUUID}
	pvs := map[string][]internal.PVData{
		bdRaceVGName + bdRaceVGUUID: {
			{PVName: "/dev/sdb", VGName: bdRaceVGName, VGUuid: bdRaceVGUUID, PVSize: resource.MustParse("10Gi"), PVUuid: "pv-uuid-sdb"},
			{PVName: bdRacePVName, VGName: bdRaceVGName, VGUuid: bdRaceVGUUID, PVSize: resource.MustParse("10Gi"), PVUuid: "pv-uuid-sdc"},
		},
	}
	// Only /dev/sdb is named by a BlockDevice.
	bds := map[string][]v1alpha1.BlockDevice{
		bdRaceVGName + bdRaceVGUUID: {{
			ObjectMeta: metav1.ObjectMeta{Name: "dev-sdb-worker-1"},
			Status: v1alpha1.BlockDeviceStatus{
				Path:                  "/dev/sdb",
				Size:                  resource.MustParse("10Gi"),
				ActualVGNameOnTheNode: bdRaceVGName,
				VGUuid:                bdRaceVGUUID,
			},
		}},
	}

	devices, _, _, unnamedPVs := setupDiscoverer(&DiscovererConfig{NodeName: bdRaceNode}).
		configureCandidateNodeDevices(context.Background(), pvs, bds, vg, bdRaceNode)

	assert.Equal(t, []string{bdRacePVName}, unnamedPVs, "the PV with no BlockDevice must be named, not just counted")
	require.Len(t, devices[bdRaceNode], 1, "the device that could be named is still returned")
	assert.Equal(t, "/dev/sdb", devices[bdRaceNode][0].Path)
}

// status.nodes is overwritten wholesale, so a pass that cannot name every PV must
// not write it: with a single-PV VG the node entry itself would go, and with it
// the only thing that makes the controller set AgentReady. The resource then sits
// Pending with no condition to explain it, and — since the discoverer is driven by
// udev events — for as long as the node's devices stay quiet.
func TestLVMVolumeGroupDiscoverReconcile_UnreportedBlockDeviceKeepsStatusNodes(t *testing.T) {
	ctx := context.Background()
	cl := test_utils.NewFakeClient(&v1alpha1.LVMVolumeGroup{})

	lvg := bdRaceLVG()
	require.NoError(t, cl.Create(ctx, lvg))
	require.NoError(t, cl.Status().Update(ctx, lvg))
	// The BlockDevice exists but does not name the VG yet, and the API server says
	// the same — so this is not a stale read the discoverer can get past.
	require.NoError(t, cl.Create(ctx, bdRaceBlockDevice(false)))

	d := bdRaceDiscoverer(t, cl, UncachedReader{Reader: cl}, bdRaceCache(t))

	assert.True(t, d.LVMVolumeGroupDiscoverReconcile(ctx),
		"an incomplete device set must requeue instead of being written out")

	var got v1alpha1.LVMVolumeGroup
	require.NoError(t, cl.Get(ctx, client.ObjectKey{Name: bdRaceLVGName}, &got))
	require.Len(t, got.Status.Nodes, 1, "the node entry must survive a pass that could not name its device")
	require.Len(t, got.Status.Nodes[0].Devices, 1)
	assert.Equal(t, bdRaceBDName, got.Status.Nodes[0].Devices[0].BlockDevice)

	// Withholding the status is only half an answer: the resource has to say what is
	// missing, or it is Pending for an unexplained reason all over again.
	cond := bdRaceVGReadyCondition(t, got)
	require.NotNil(t, cond, "the withheld status must be explained by a condition")
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	// The reason, not just the status: this is the branch whose status.nodes was not
	// refreshed, and the controller tells the two branches apart by nothing else. A
	// reason the controller keeps in service would leave the LVMVolumeGroup Ready
	// with free-space figures from an earlier pass.
	assert.Equal(t, internal.ReasonNodeNotDescribed, cond.Reason)
	assert.Contains(t, cond.Message, bdRacePVName, "the condition must name the Physical Volume")
	assert.Contains(t, cond.Message, "out of date", "and warn that the kept status.nodes is stale")
}

// One unnamed PV out of several is a different case: the node entry survives, so
// there is no reason to hold back what is known. A Volume Group may legitimately
// contain a PV that never gets a BlockDevice — one under the minimum size, or one a
// BlockDeviceFilter excludes — and freezing vgSize and thin-pool usage over it
// forever would be worse than the missing device entry.
func TestLVMVolumeGroupDiscoverReconcile_PartiallyNamedDevicesAreStillPublished(t *testing.T) {
	ctx := context.Background()
	cl := test_utils.NewFakeClient(&v1alpha1.LVMVolumeGroup{})

	lvg := bdRaceLVG()
	lvg.Status.VGFree = resource.MustParse("4Gi")
	require.NoError(t, cl.Create(ctx, lvg))
	require.NoError(t, cl.Status().Update(ctx, lvg))
	require.NoError(t, cl.Create(ctx, bdRaceBlockDevice(true)))

	// A second PV joined the VG, and it is the one with no BlockDevice.
	sdsCache := bdRaceCache(t)
	sdsCache.StorePVs([]internal.PVData{
		{PVName: bdRacePVName, VGName: bdRaceVGName, VGUuid: bdRaceVGUUID, PVSize: resource.MustParse("10Gi"), PVUuid: "pv-uuid-sdc"},
		{PVName: "/dev/sdd", VGName: bdRaceVGName, VGUuid: bdRaceVGUUID, PVSize: resource.MustParse("10Gi"), PVUuid: "pv-uuid-sdd"},
	}, bytes.Buffer{})

	d := bdRaceDiscoverer(t, cl, UncachedReader{Reader: cl}, sdsCache)
	// /dev/sdd is probed as a loop alias as well before it is given up on.
	d.commands.(*mock_utils.MockCommands).EXPECT().GetLoopBackingFile(gomock.Any(), "/dev/sdd").
		Return("losetup -O BACK-FILE", internal.LoopBackingFile{}, nil).AnyTimes()

	assert.True(t, d.LVMVolumeGroupDiscoverReconcile(ctx),
		"the unnamed PV still has to be come back for")

	var got v1alpha1.LVMVolumeGroup
	require.NoError(t, cl.Get(ctx, client.ObjectKey{Name: bdRaceLVGName}, &got))
	require.Len(t, got.Status.Nodes, 1)
	require.Len(t, got.Status.Nodes[0].Devices, 1, "only the named device can be reported")
	assert.Equal(t, bdRacePVName, got.Status.Nodes[0].Devices[0].Path)
	assert.Equal(t, "10Gi", got.Status.VGFree.String(),
		"the rest of status must not be frozen by the unnamed PV")

	// The publish sets VGReady itself, so the caveat has to come from the publish —
	// a condition written after it would be flipped back on the next pass.
	cond := bdRaceVGReadyCondition(t, got)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status,
		"a status.nodes that is missing a device must not be reported as ready")
	// And under the reason the controller keeps in service, unlike the branch that
	// publishes nothing: everything the scheduler reads off this status was measured
	// in this pass, so one missing device entry must not end the Volume Group's
	// service. A PV under the minimum size, or excluded by a BlockDeviceFilter, would
	// otherwise do exactly that, permanently.
	assert.Equal(t, internal.ReasonBlockDeviceNotFound, cond.Reason)
	assert.Contains(t, cond.Message, "/dev/sdd")
}

// The re-read is there to fill gaps, and it must never be allowed to open new
// ones. It selects by the node label the block-device discoverer writes, so a
// device whose labels have not been rewritten since an upgrade is missing from it,
// and a BlockDevice deleted between the two reads is missing from it too — either
// way the fresh view describes less of the node than the cached one did.
//
// Taking it anyway would be the expensive kind of wrong: the number of unnamed
// Physical Volumes decides whether status.nodes is published at all and whether a
// spec.blockDeviceSelector is derived, and a selector once written is never
// widened.
func TestLVMVolumeGroupDiscoverReconcile_KeepsTheCachedViewWhenTheReReadNamesLess(t *testing.T) {
	ctx := context.Background()
	cl := test_utils.NewFakeClient(&v1alpha1.LVMVolumeGroup{})

	lvg := bdRaceLVG()
	require.NoError(t, cl.Create(ctx, lvg))
	require.NoError(t, cl.Status().Update(ctx, lvg))
	// The cache names one of the two PVs, so the pass is incomplete and the re-read
	// is reached.
	require.NoError(t, cl.Create(ctx, bdRaceBlockDevice(true)))

	sdsCache := bdRaceCache(t)
	sdsCache.StorePVs([]internal.PVData{
		{PVName: bdRacePVName, VGName: bdRaceVGName, VGUuid: bdRaceVGUUID, PVSize: resource.MustParse("10Gi"), PVUuid: "pv-uuid-sdc"},
		{PVName: "/dev/sdd", VGName: bdRaceVGName, VGUuid: bdRaceVGUUID, PVSize: resource.MustParse("10Gi"), PVUuid: "pv-uuid-sdd"},
	}, bytes.Buffer{})

	// The API server answers the selected read with nothing at all — the shape a
	// missing node label takes.
	apiReader := test_utils.NewFakeClient()

	d := bdRaceDiscoverer(t, cl, UncachedReader{Reader: apiReader}, sdsCache)
	d.commands.(*mock_utils.MockCommands).EXPECT().GetLoopBackingFile(gomock.Any(), gomock.Any()).
		Return("losetup -O BACK-FILE", internal.LoopBackingFile{}, nil).AnyTimes()

	assert.True(t, d.LVMVolumeGroupDiscoverReconcile(ctx),
		"the PV the cache could not name still has to be come back for")

	var got v1alpha1.LVMVolumeGroup
	require.NoError(t, cl.Get(ctx, client.ObjectKey{Name: bdRaceLVGName}, &got))
	require.Len(t, got.Status.Nodes, 1,
		"the node entry survives: the cached view named a device, so the pass could publish")
	require.Len(t, got.Status.Nodes[0].Devices, 1,
		"the device the cache named must not be lost to a re-read that named nothing")
	assert.Equal(t, bdRacePVName, got.Status.Nodes[0].Devices[0].Path)

	cond := bdRaceVGReadyCondition(t, got)
	require.NotNil(t, cond)
	assert.Equal(t, internal.ReasonBlockDeviceNotFound, cond.Reason,
		"one PV is unnamed, not all of them — the verdict is the survivable one")
}

// The retry has to end. The scanner runs a requeue in a goroutine that exits only
// once the requeue stops, and starts a new one per pass that asked for one, so a PV
// that never gets a BlockDevice — below the minimum size, or excluded by a
// BlockDeviceFilter — would otherwise leave a full discovery pass running every
// scan interval for the lifetime of the process, once per goroutine that piled up.
func TestLVMVolumeGroupDiscoverReconcile_StopsRetryingAPermanentlyUnnamedPV(t *testing.T) {
	ctx := context.Background()
	cl := test_utils.NewFakeClient(&v1alpha1.LVMVolumeGroup{})

	lvg := bdRaceLVG()
	require.NoError(t, cl.Create(ctx, lvg))
	require.NoError(t, cl.Status().Update(ctx, lvg))
	require.NoError(t, cl.Create(ctx, bdRaceBlockDevice(false)))

	d := bdRaceDiscoverer(t, cl, UncachedReader{Reader: cl}, bdRaceCache(t))

	for pass := 1; pass <= maxUnnamedPVPasses; pass++ {
		assert.True(t, d.LVMVolumeGroupDiscoverReconcile(ctx),
			"pass %d of %d is still within the retry budget", pass, maxUnnamedPVPasses)
	}

	assert.False(t, d.LVMVolumeGroupDiscoverReconcile(ctx),
		"the retry must stop once the absence looks permanent")

	// Giving up on the retry must not give up on saying why.
	var got v1alpha1.LVMVolumeGroup
	require.NoError(t, cl.Get(ctx, client.ObjectKey{Name: bdRaceLVGName}, &got))
	cond := bdRaceVGReadyCondition(t, got)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	// The VG's only PV is unnamed, so this is again the branch that published
	// nothing, and the reason has to stay the one that takes the Volume Group out of
	// service — the status the resource keeps is now as old as the budget that just
	// ran out.
	assert.Equal(t, internal.ReasonNodeNotDescribed, cond.Reason)

	// And the node has to be able to come back: the BlockDevice appearing after the
	// budget ran out still gets picked up by the next pass, whatever triggers it.
	bd := bdRaceBlockDevice(true)
	require.NoError(t, cl.Delete(ctx, bd))
	require.NoError(t, cl.Create(ctx, bd))

	assert.False(t, d.LVMVolumeGroupDiscoverReconcile(ctx))
	require.NoError(t, cl.Get(ctx, client.ObjectKey{Name: bdRaceLVGName}, &got))
	require.Len(t, got.Status.Nodes, 1)
	require.Len(t, got.Status.Nodes[0].Devices, 1)
	cond = bdRaceVGReadyCondition(t, got)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionTrue, cond.Status, "a named PV clears the condition")
}

// Two passes can be in flight at once: the scanner retries a requeue from a
// goroutine of its own — a new one for every pass that asked for a retry — while its
// main loop keeps invoking the discoverer on udev events. What the discoverer
// remembers between passes is therefore written concurrently, and concurrent writes
// to a bare map take the whole agent down with a fatal error. Run under -race, this
// is the test for that.
func TestLVMVolumeGroupDiscoverReconcile_ConcurrentPassesShareRememberedState(t *testing.T) {
	ctx := context.Background()
	cl := test_utils.NewFakeClient(&v1alpha1.LVMVolumeGroup{})

	lvg := bdRaceLVG()
	require.NoError(t, cl.Create(ctx, lvg))
	require.NoError(t, cl.Status().Update(ctx, lvg))
	require.NoError(t, cl.Create(ctx, bdRaceBlockDevice(false)))

	d := bdRaceDiscoverer(t, cl, UncachedReader{Reader: cl}, bdRaceCache(t))

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d.LVMVolumeGroupDiscoverReconcile(ctx)
		}()
	}
	wg.Wait()
}

// The usual reason a PV has no BlockDevice is that this process is reading its own
// write back too early: the block-device discoverer filled the status in moments
// ago, in the same scanner pass, and the informer cache has not caught up. Asking
// the API server directly settles it within the pass instead of costing a requeue.
func TestLVMVolumeGroupDiscoverReconcile_ReReadsBlockDevicesFromTheAPIServer(t *testing.T) {
	ctx := context.Background()

	cl := test_utils.NewFakeClient(&v1alpha1.LVMVolumeGroup{})
	lvg := bdRaceLVG()
	// Nothing has been published for this VG yet, which is the state a freshly
	// created Volume Group is in — and the one that used to end in Pending forever.
	lvg.Status = v1alpha1.LVMVolumeGroupStatus{}
	require.NoError(t, cl.Create(ctx, lvg))
	require.NoError(t, cl.Create(ctx, bdRaceBlockDevice(false)))

	// The API server has the update the cache is missing.
	apiReader := test_utils.NewFakeClient()
	require.NoError(t, apiReader.Create(ctx, bdRaceBlockDevice(true)))

	d := bdRaceDiscoverer(t, cl, UncachedReader{Reader: apiReader}, bdRaceCache(t))

	assert.False(t, d.LVMVolumeGroupDiscoverReconcile(ctx),
		"the fresh read completes the device set, so there is nothing left to retry")

	var got v1alpha1.LVMVolumeGroup
	require.NoError(t, cl.Get(ctx, client.ObjectKey{Name: bdRaceLVGName}, &got))
	require.Len(t, got.Status.Nodes, 1)
	require.Len(t, got.Status.Nodes[0].Devices, 1)
	assert.Equal(t, bdRaceNode, got.Status.Nodes[0].Name)
	assert.Equal(t, bdRaceBDName, got.Status.Nodes[0].Devices[0].BlockDevice)
	assert.Equal(t, bdRacePVName, got.Status.Nodes[0].Devices[0].Path)
}

// bdRaceExtraBD is a BlockDevice for a second PV of the same VG, already carrying
// the actualVGNameOnTheNode/vgUuid pair this discoverer matches a PV by.
func bdRaceExtraBD(name, path string) *v1alpha1.BlockDevice {
	return bdRaceNodeLabelled(&v1alpha1.BlockDevice{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: v1alpha1.BlockDeviceStatus{
			Path:                  path,
			NodeName:              bdRaceNode,
			Size:                  resource.MustParse("10Gi"),
			ActualVGNameOnTheNode: bdRaceVGName,
			VGUuid:                bdRaceVGUUID,
		},
	})
}

// bdRaceStorePVs replaces the PV set of the VG, keeping everything else the cache
// holds. Every PV listed here that has no BlockDevice is probed as a loop alias
// first, so each one needs its own expectation on the mock.
func bdRaceStorePVs(t *testing.T, sdsCache *cache.Cache, d *Discoverer, paths ...string) {
	t.Helper()
	pvs := make([]internal.PVData, 0, len(paths))
	for _, path := range paths {
		pvs = append(pvs, internal.PVData{
			PVName: path,
			VGName: bdRaceVGName,
			VGUuid: bdRaceVGUUID,
			PVSize: resource.MustParse("10Gi"),
			PVUuid: "pv-uuid-" + filepath.Base(path),
		})
		if path == bdRacePVName {
			continue
		}
		d.commands.(*mock_utils.MockCommands).EXPECT().GetLoopBackingFile(gomock.Any(), path).
			Return("losetup -O BACK-FILE", internal.LoopBackingFile{}, nil).AnyTimes()
	}
	sdsCache.StorePVs(pvs, bytes.Buffer{})
}

// countingReader records how many lists went to it, which is the only way to see
// the load the uncached re-read adds: it is the same call on the same type as the
// cached one.
type countingReader struct {
	client.Reader
	lists int
}

func (r *countingReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	r.lists++

	return r.Reader.List(ctx, list, opts...)
}

// failingReader stands for an API server that will not answer the re-read.
type failingReader struct {
	client.Reader
}

func (failingReader) List(context.Context, client.ObjectList, ...client.ListOption) error {
	return errors.New("apiserver said no")
}

// The retry budget is per Physical Volume, and the decision has to be too. A Volume
// Group can hold a PV that will never have a BlockDevice — under the minimum size,
// or excluded by a BlockDeviceFilter — right next to a disk that was just added and
// is one pass away from getting one. Deciding on the longest wait let the first
// spend the second one's retry: the new disk got no requeue at all, and on a node
// whose devices have settled no udev event comes to make up for it.
func TestLVMVolumeGroupDiscoverReconcile_RetriesAFreshUnnamedPVBesideAPermanentOne(t *testing.T) {
	ctx := context.Background()
	cl := test_utils.NewFakeClient(&v1alpha1.LVMVolumeGroup{})

	lvg := bdRaceLVG()
	require.NoError(t, cl.Create(ctx, lvg))
	require.NoError(t, cl.Status().Update(ctx, lvg))
	// /dev/sdc is named; /dev/sdd is the one that never will be.
	require.NoError(t, cl.Create(ctx, bdRaceBlockDevice(true)))

	sdsCache := bdRaceCache(t)
	d := bdRaceDiscoverer(t, cl, UncachedReader{Reader: cl}, sdsCache)
	bdRaceStorePVs(t, sdsCache, d, bdRacePVName, "/dev/sdd")

	for pass := 1; pass <= maxUnnamedPVPasses; pass++ {
		require.True(t, d.LVMVolumeGroupDiscoverReconcile(ctx),
			"pass %d is still within /dev/sdd's budget", pass)
	}
	require.False(t, d.LVMVolumeGroupDiscoverReconcile(ctx),
		"/dev/sdd's budget is spent, so nothing is left to retry for it")

	// A third disk joins the Volume Group. Its BlockDevice is not registered yet —
	// seconds away, not never — and that is a retry /dev/sdd must not have consumed.
	bdRaceStorePVs(t, sdsCache, d, bdRacePVName, "/dev/sdd", "/dev/sde")

	assert.True(t, d.LVMVolumeGroupDiscoverReconcile(ctx),
		"the newly unnamed PV has its own budget and must be come back for")
}

// The uncached re-read is what the budget is really there to bound: a cluster-wide
// list of every BlockDevice, a second full candidate rebuild and a losetup probe per
// unnamed PV. Bounding the requeue and leaving this unbounded would bound the cheap
// half and leave a node with a permanently filtered PV paying the expensive one on
// every udev event for the lifetime of the process.
func TestLVMVolumeGroupDiscoverReconcile_StopsReReadingOnceTheBudgetIsSpent(t *testing.T) {
	ctx := context.Background()
	cl := test_utils.NewFakeClient(&v1alpha1.LVMVolumeGroup{})

	lvg := bdRaceLVG()
	require.NoError(t, cl.Create(ctx, lvg))
	require.NoError(t, cl.Status().Update(ctx, lvg))
	require.NoError(t, cl.Create(ctx, bdRaceBlockDevice(false)))

	counting := &countingReader{Reader: cl}
	d := bdRaceDiscoverer(t, cl, UncachedReader{Reader: counting}, bdRaceCache(t))

	for pass := 1; pass <= maxUnnamedPVPasses; pass++ {
		require.True(t, d.LVMVolumeGroupDiscoverReconcile(ctx), "pass %d is within the budget", pass)
	}
	require.Equal(t, maxUnnamedPVPasses, counting.lists,
		"every pass within the budget re-reads once, and only once")

	// Past the budget the PV is taken to be one that never becomes a BlockDevice, so
	// a fresh read cannot change the answer and must not be paid for.
	require.False(t, d.LVMVolumeGroupDiscoverReconcile(ctx))
	assert.Equal(t, maxUnnamedPVPasses, counting.lists,
		"the pass that gives up on the retry must not re-read either")

	for range 5 {
		require.False(t, d.LVMVolumeGroupDiscoverReconcile(ctx))
	}
	assert.Equal(t, maxUnnamedPVPasses, counting.lists,
		"and neither must any pass after it, however many udev events arrive")
}

// A re-read that fails leaves the cached view in place: it still carries the unnamed
// PVs, so the pass reports them and comes back, which is what a failed re-read should
// lead to anyway.
func TestLVMVolumeGroupDiscoverReconcile_KeepsTheCachedViewWhenTheReReadFails(t *testing.T) {
	ctx := context.Background()
	cl := test_utils.NewFakeClient(&v1alpha1.LVMVolumeGroup{})

	lvg := bdRaceLVG()
	require.NoError(t, cl.Create(ctx, lvg))
	require.NoError(t, cl.Status().Update(ctx, lvg))
	require.NoError(t, cl.Create(ctx, bdRaceBlockDevice(false)))

	d := bdRaceDiscoverer(t, cl, UncachedReader{Reader: failingReader{Reader: cl}}, bdRaceCache(t))

	assert.True(t, d.LVMVolumeGroupDiscoverReconcile(ctx),
		"a failed re-read must not be taken as an answer")

	var got v1alpha1.LVMVolumeGroup
	require.NoError(t, cl.Get(ctx, client.ObjectKey{Name: bdRaceLVGName}, &got))
	require.Len(t, got.Status.Nodes, 1, "the node entry must survive")
	cond := bdRaceVGReadyCondition(t, got)
	require.NotNil(t, cond)
	assert.Equal(t, internal.ReasonNodeNotDescribed, cond.Reason)
}

// Importing a Volume Group derives spec.blockDeviceSelector from the same match that
// left a Physical Volume unnamed, so a selector written now would not cover the whole
// Volume Group — and nothing widens one afterwards: hasEmptyBlockDeviceSelector stops
// being true the moment one is written. The omission would outlive the cause and only
// an operator editing spec by hand could undo it.
func TestLVMVolumeGroupDiscoverReconcile_DoesNotImportAVolumeGroupWithAnUnnamedPV(t *testing.T) {
	ctx := context.Background()
	cl := test_utils.NewFakeClient(&v1alpha1.LVMVolumeGroup{})

	// No LVMVolumeGroup for this VG at all: the pass would create one.
	require.NoError(t, cl.Create(ctx, bdRaceBlockDevice(true)))

	sdsCache := bdRaceCache(t)
	d := bdRaceDiscoverer(t, cl, UncachedReader{Reader: cl}, sdsCache)
	bdRaceStorePVs(t, sdsCache, d, bdRacePVName, "/dev/sdd")

	assert.True(t, d.LVMVolumeGroupDiscoverReconcile(ctx),
		"the import is postponed, not abandoned")

	var lvgs v1alpha1.LVMVolumeGroupList
	require.NoError(t, cl.List(ctx, &lvgs))
	assert.Empty(t, lvgs.Items,
		"a Volume Group whose selector cannot yet be derived in full must not be imported")

	// Once every PV is named the selector covers the whole Volume Group, and the
	// import goes ahead.
	require.NoError(t, cl.Create(ctx, bdRaceExtraBD("dev-sdd-worker-1", "/dev/sdd")))

	require.False(t, d.LVMVolumeGroupDiscoverReconcile(ctx))
	require.NoError(t, cl.List(ctx, &lvgs))
	require.Len(t, lvgs.Items, 1)
	selector := lvgs.Items[0].Spec.BlockDeviceSelector
	require.NotNil(t, selector)
	require.Len(t, selector.MatchExpressions, 1)
	assert.ElementsMatch(t, []string{bdRaceBDName, "dev-sdd-worker-1"}, selector.MatchExpressions[0].Values,
		"the derived selector must name every BlockDevice of the Volume Group")
}

// The same reasoning for the other write to spec: backfilling the selector of an
// LVMVolumeGroup taken under management without one. It happens once, so it has to
// happen on a complete candidate.
func TestUpdateLVMVolumeGroupByCandidate_DoesNotBackfillTheSelectorWithAnUnnamedPV(t *testing.T) {
	ctx := context.Background()
	cl := test_utils.NewFakeClient(&v1alpha1.LVMVolumeGroup{})

	lvg := bdRaceLVG()
	// No selector — the state a Volume Group taken under management is left in — and
	// a status the pass will find something to change in.
	lvg.Spec.BlockDeviceSelector = nil
	lvg.Status.VGFree = resource.MustParse("4Gi")
	require.NoError(t, cl.Create(ctx, lvg))
	require.NoError(t, cl.Status().Update(ctx, lvg))
	require.NoError(t, cl.Create(ctx, bdRaceBlockDevice(true)))

	sdsCache := bdRaceCache(t)
	d := bdRaceDiscoverer(t, cl, UncachedReader{Reader: cl}, sdsCache)
	bdRaceStorePVs(t, sdsCache, d, bdRacePVName, "/dev/sdd")

	require.True(t, d.LVMVolumeGroupDiscoverReconcile(ctx))

	var got v1alpha1.LVMVolumeGroup
	require.NoError(t, cl.Get(ctx, client.ObjectKey{Name: bdRaceLVGName}, &got))
	assert.Nil(t, got.Spec.BlockDeviceSelector,
		"a selector derived from a candidate missing a device would omit it permanently")
	assert.Equal(t, "10Gi", got.Status.VGFree.String(),
		"status is still published though — only spec waits")

	// Complete candidate, so the backfill may go ahead.
	require.NoError(t, cl.Create(ctx, bdRaceExtraBD("dev-sdd-worker-1", "/dev/sdd")))

	require.False(t, d.LVMVolumeGroupDiscoverReconcile(ctx))
	require.NoError(t, cl.Get(ctx, client.ObjectKey{Name: bdRaceLVGName}, &got))
	require.NotNil(t, got.Spec.BlockDeviceSelector)
	require.Len(t, got.Spec.BlockDeviceSelector.MatchExpressions, 1)
	assert.ElementsMatch(t, []string{bdRaceBDName, "dev-sdd-worker-1"},
		got.Spec.BlockDeviceSelector.MatchExpressions[0].Values)
}

// Two LVMVolumeGroups whose Volume Groups are both gone from the node, so both
// have to leave the reconcile.
//
// The removal used to append-in-place while ranging over the same slice by index:
// `range` fixes the header once and the append rewrites the backing array, so
// every removal shifted the elements the loop had not reached and it skipped the
// one after each. Adjacent is the arrangement that shows it — the first removal
// moved the second candidate into the index the loop had just left behind.
//
// The other half of the same fix is the skip set: it used to be keyed off the
// candidate, which in the "VG not found" branch is the zero value the map lookup
// returned, so it recorded the empty string and skipped nothing at all.
func TestReconcileUnhealthyLVMVolumeGroups_RemovesEveryUnhealthyOne(t *testing.T) {
	ctx := context.Background()
	cl := test_utils.NewFakeClient(&v1alpha1.LVMVolumeGroup{})
	d := bdRaceDiscoverer(t, cl, UncachedReader{Reader: cl}, bdRaceCache(t))

	lvgs := map[string]v1alpha1.LVMVolumeGroup{}
	for _, vgName := range []string{"vg-gone-a", "vg-gone-b", "vg-present"} {
		lvg := v1alpha1.LVMVolumeGroup{
			ObjectMeta: metav1.ObjectMeta{Name: "lvg-" + vgName},
			Spec: v1alpha1.LVMVolumeGroupSpec{
				ActualVGNameOnTheNode: vgName,
				Local:                 v1alpha1.LVMVolumeGroupLocalSpec{NodeName: bdRaceNode},
			},
			// A non-empty VGUuid is what marks the Volume Group as one that existed
			// on the node before, which is the branch under test.
			Status: v1alpha1.LVMVolumeGroupStatus{VGUuid: "uuid-" + vgName},
		}
		require.NoError(t, cl.Create(ctx, &lvg))
		lvgs[vgName] = lvg
	}

	// Only the third has a candidate; the first two are the ones whose VG is gone,
	// and they are adjacent in the slice.
	candidates := []internal.LVMVolumeGroupCandidate{
		{ActualVGNameOnTheNode: "vg-gone-a"},
		{ActualVGNameOnTheNode: "vg-gone-b"},
		{ActualVGNameOnTheNode: "vg-present"},
	}
	// The candidates for the missing groups are what the discoverer would NOT have
	// built; they are here to be removed, so drop them from the map the function
	// looks up by deleting the resources' counterpart.
	candidates = candidates[2:]
	candidates = append([]internal.LVMVolumeGroupCandidate{
		{ActualVGNameOnTheNode: "vg-gone-a"},
		{ActualVGNameOnTheNode: "vg-gone-b"},
	}, candidates...)

	kept, err := d.ReconcileUnhealthyLVMVolumeGroups(ctx, candidates, lvgs)
	require.NoError(t, err)

	var keptNames []string
	for _, c := range kept {
		keptNames = append(keptNames, c.ActualVGNameOnTheNode)
	}
	assert.ElementsMatch(t, []string{"vg-gone-a", "vg-gone-b", "vg-present"}, keptNames,
		"every candidate has a Volume Group here, so none is unhealthy and none is dropped")
	assert.Len(t, lvgs, 3, "and no resource leaves the reconcile either")
}

// The same function with the candidates genuinely missing: both unhealthy
// LVMVolumeGroups leave the reconcile, and so does nothing else.
func TestReconcileUnhealthyLVMVolumeGroups_DropsAdjacentMissingVolumeGroups(t *testing.T) {
	ctx := context.Background()
	cl := test_utils.NewFakeClient(&v1alpha1.LVMVolumeGroup{})
	d := bdRaceDiscoverer(t, cl, UncachedReader{Reader: cl}, bdRaceCache(t))

	lvgs := map[string]v1alpha1.LVMVolumeGroup{}
	for _, vgName := range []string{"vg-gone-a", "vg-gone-b", "vg-present"} {
		lvg := v1alpha1.LVMVolumeGroup{
			ObjectMeta: metav1.ObjectMeta{Name: "lvg-" + vgName},
			Spec: v1alpha1.LVMVolumeGroupSpec{
				ActualVGNameOnTheNode: vgName,
				Local:                 v1alpha1.LVMVolumeGroupLocalSpec{NodeName: bdRaceNode},
			},
			Status: v1alpha1.LVMVolumeGroupStatus{VGUuid: "uuid-" + vgName},
		}
		require.NoError(t, cl.Create(ctx, &lvg))
		lvgs[vgName] = lvg
	}

	// Only vg-present still has a Volume Group on the node. The two that do not are
	// adjacent in the resource set, which is what the index-mutation bug needed.
	candidates := []internal.LVMVolumeGroupCandidate{{ActualVGNameOnTheNode: "vg-present"}}

	kept, err := d.ReconcileUnhealthyLVMVolumeGroups(ctx, candidates, lvgs)
	require.NoError(t, err)

	assert.Len(t, kept, 1)
	assert.Equal(t, "vg-present", kept[0].ActualVGNameOnTheNode)

	_, goneA := lvgs["vg-gone-a"]
	_, goneB := lvgs["vg-gone-b"]
	assert.False(t, goneA, "an LVMVolumeGroup whose Volume Group is missing leaves the reconcile")
	assert.False(t, goneB, "and so does the one next to it")
	assert.Contains(t, lvgs, "vg-present", "the healthy one stays")
}

// A Volume Group the node carries but no LVMVolumeGroup owns yet, and — the part
// that matters here — with no owner tag either, which is the ordinary state of one
// that has never been imported. lvgNameForCandidate mints it a name on every call.
func bdRaceUntaggedCandidate(unnamed ...string) internal.LVMVolumeGroupCandidate {
	return internal.LVMVolumeGroupCandidate{
		LVMVGName:             generateLVMVGName(),
		LVMVGNameGenerated:    true,
		ActualVGNameOnTheNode: bdRaceVGName,
		UnnamedPVs:            unnamed,
	}
}

// refusedImportCount reads the counter for one lvg_name label on this fixture's
// Volume Group, so a test can assert on its own delta rather than on a
// package-global total other tests also move.
//
// lvg_name is the label under test — the whole point is that a generated name
// must not reach it — so it is the only one worth varying here.
func refusedImportCount(t *testing.T, m *monitoring.Metrics, lvgName string) float64 {
	t.Helper()

	return testutil.ToFloat64(m.LVMVolumeGroupImportRefusedTotal(bdRaceVGName, lvgName))
}

// The case that used to leave no trace anywhere. Not one Physical Volume could be
// named, so there is no status to publish; there is no LVMVolumeGroup either, so
// there is no resource to carry a condition. The counter has to be the record, and
// it used to be incremented only in the milder branch below this one — the one
// where some devices were named and a condition does get written.
func TestHandleUnnamedPVs_CountsTheRefusedImportWhenNoDeviceCouldBeNamed(t *testing.T) {
	ctx := context.Background()
	cl := test_utils.NewFakeClient(&v1alpha1.LVMVolumeGroup{})
	d := bdRaceDiscoverer(t, cl, UncachedReader{Reader: cl}, bdRaceCache(t))

	candidate := bdRaceUntaggedCandidate(bdRacePVName)
	before := refusedImportCount(t, d.metrics, "")

	publish, _ := d.handleUnnamedPVs(ctx, candidate, map[string]v1alpha1.LVMVolumeGroup{},
		map[string]int{}, map[string]int{})

	assert.False(t, publish, "a candidate that names no device must not be published")
	assert.Equal(t, before+1, refusedImportCount(t, d.metrics, ""),
		"the import did not happen and nothing in the API says so, so the counter has to")
}

// The label is what makes the counter usable and what can also make it unusable.
// A Volume Group with no owner tag gets a name generated for it, freshly on every
// discovery pass, and this branch runs on every pass for as long as the state
// lasts — which, for a Physical Volume excluded by a BlockDeviceFilter, is
// forever. Putting that name in a label is a new Prometheus time series per pass,
// retained for the lifetime of the process.
func TestHandleUnnamedPVs_DoesNotLabelTheCounterWithAGeneratedName(t *testing.T) {
	ctx := context.Background()
	cl := test_utils.NewFakeClient(&v1alpha1.LVMVolumeGroup{})
	d := bdRaceDiscoverer(t, cl, UncachedReader{Reader: cl}, bdRaceCache(t))

	first := bdRaceUntaggedCandidate(bdRacePVName)
	second := bdRaceUntaggedCandidate(bdRacePVName)
	require.NotEqual(t, first.LVMVGName, second.LVMVGName,
		"the fixture is only meaningful if the generated name differs per pass")

	before := refusedImportCount(t, d.metrics, "")
	for _, candidate := range []internal.LVMVolumeGroupCandidate{first, second} {
		d.handleUnnamedPVs(ctx, candidate, map[string]v1alpha1.LVMVolumeGroup{},
			map[string]int{}, map[string]int{})
	}

	assert.Equal(t, before+2, refusedImportCount(t, d.metrics, ""),
		"both passes belong to the same series")
	assert.Zero(t, refusedImportCount(t, d.metrics, first.LVMVGName),
		"a name nobody chose must not become a label value")
	assert.Zero(t, refusedImportCount(t, d.metrics, second.LVMVGName),
		"and neither must the next one the same state generates")
}

// A name that came from the Volume Group's own tag is stable across passes and is
// exactly what an operator needs, so it does keep its label.
func TestHandleUnnamedPVs_KeepsATaggedNameInTheCounterLabel(t *testing.T) {
	ctx := context.Background()
	cl := test_utils.NewFakeClient(&v1alpha1.LVMVolumeGroup{})
	d := bdRaceDiscoverer(t, cl, UncachedReader{Reader: cl}, bdRaceCache(t))

	candidate := internal.LVMVolumeGroupCandidate{
		LVMVGName:             bdRaceLVGName,
		ActualVGNameOnTheNode: bdRaceVGName,
		UnnamedPVs:            []string{bdRacePVName},
	}
	before := refusedImportCount(t, d.metrics, bdRaceLVGName)

	d.handleUnnamedPVs(ctx, candidate, map[string]v1alpha1.LVMVolumeGroup{},
		map[string]int{}, map[string]int{})

	assert.Equal(t, before+1, refusedImportCount(t, d.metrics, bdRaceLVGName))
}

// An LVMVolumeGroup that does exist is not a refused import: the pass publishes
// what it could name and writes the caveat onto the resource.
func TestHandleUnnamedPVs_DoesNotCountAnImportThatAlreadyHappened(t *testing.T) {
	ctx := context.Background()
	cl := test_utils.NewFakeClient(&v1alpha1.LVMVolumeGroup{})
	d := bdRaceDiscoverer(t, cl, UncachedReader{Reader: cl}, bdRaceCache(t))

	candidate := internal.LVMVolumeGroupCandidate{
		LVMVGName:             bdRaceLVGName,
		ActualVGNameOnTheNode: bdRaceVGName,
		UnnamedPVs:            []string{"/dev/sdd"},
		Nodes: map[string][]internal.LVMVGDevice{
			bdRaceNode: {{Path: bdRacePVName, BlockDevice: bdRaceBDName}},
		},
	}
	lvgs := map[string]v1alpha1.LVMVolumeGroup{bdRaceVGName: *bdRaceLVG()}
	before := refusedImportCount(t, d.metrics, bdRaceLVGName)

	publish, _ := d.handleUnnamedPVs(ctx, candidate, lvgs, map[string]int{}, map[string]int{})

	assert.True(t, publish, "some devices were named, so the status is worth publishing")
	assert.Equal(t, before, refusedImportCount(t, d.metrics, bdRaceLVGName),
		"nothing was refused: the resource is already there")
}

func TestTrackUnnamedPVs(t *testing.T) {
	candidate := internal.LVMVolumeGroupCandidate{
		ActualVGNameOnTheNode: bdRaceVGName,
		UnnamedPVs:            []string{"/dev/sdd", "/dev/sde"},
	}
	key := func(pv string) string { return unnamedPVKey(bdRaceVGName, pv) }

	for _, tt := range []struct {
		name     string
		previous map[string]int
		want     unnamedPVVerdict
	}{
		{
			// firstSeen is what decides whether the state goes into the log at
			// Warning or at Debug: loud when it appears or changes, quiet while it
			// merely holds. Both PVs are new here, so this pass is the loud one.
			name:     "first pass for both",
			previous: map[string]int{},
			want:     unnamedPVVerdict{retry: true, firstSeen: true, waited: 1},
		},
		{
			// Neither PV is new, so the message would say exactly what the previous
			// pass already said — Debug, and the condition on the resource carries it.
			name:     "one at the edge of the budget, still retryable",
			previous: map[string]int{key("/dev/sdd"): maxUnnamedPVPasses - 1, key("/dev/sde"): 3},
			want:     unnamedPVVerdict{retry: true, waited: maxUnnamedPVPasses},
		},
		{
			// The pass that takes the last of them out of the budget, and the only one
			// that says so: repeating it every scan interval would bury it.
			name:     "the pass that gives up",
			previous: map[string]int{key("/dev/sdd"): maxUnnamedPVPasses, key("/dev/sde"): maxUnnamedPVPasses},
			want:     unnamedPVVerdict{gaveUp: true, waited: maxUnnamedPVPasses + 1},
		},
		{
			name:     "already given up, so nothing more to report",
			previous: map[string]int{key("/dev/sdd"): 40, key("/dev/sde"): 40},
			want:     unnamedPVVerdict{waited: 41},
		},
		{
			// The case the per-PV decision exists for: one spent, one fresh. The fresh
			// one also makes the pass loud again — the set of PVs the message names
			// has changed, so it is no longer the message the previous pass wrote.
			name:     "a fresh PV keeps its own budget beside a spent one",
			previous: map[string]int{key("/dev/sdd"): 40},
			want:     unnamedPVVerdict{retry: true, firstSeen: true, waited: 41},
		},
		{
			// A PV that got its BlockDevice is not in UnnamedPVs any more, so the count
			// left over from before must not be carried anywhere.
			name:     "a count for a PV no longer unnamed is dropped",
			previous: map[string]int{key("/dev/sdf"): 40},
			want:     unnamedPVVerdict{retry: true, firstSeen: true, waited: 1},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			current := map[string]int{}
			assert.Equal(t, tt.want, trackUnnamedPVs(candidate, tt.previous, current))
			assert.Equal(t, map[string]int{
				key("/dev/sdd"): tt.previous[key("/dev/sdd")] + 1,
				key("/dev/sde"): tt.previous[key("/dev/sde")] + 1,
			}, current, "this pass records exactly the PVs it saw unnamed, and only those")
		})
	}
}

// The budget the uncached re-read is charged against is only a budget if every way
// of reaching the re-read is counted. A candidate whose file devices could not all
// be classified is skipped by the discovery loop before trackUnnamedPVs, so its
// counts restart on every pass — and with it left in this predicate, a host probe
// that flaps would buy an uncached BlockDevice list and a second full candidate
// build every pass, for as long as it flaps.
//
// Nothing is lost by skipping it: the loop declines to publish such a candidate
// whatever the BlockDevice list says, so a fresher one cannot change this pass.
func TestAnyUnnamedPVWithinBudget_IgnoresACandidateWhoseFileDevicesAreUnclassified(t *testing.T) {
	unclassified := internal.LVMVolumeGroupCandidate{
		ActualVGNameOnTheNode:  bdRaceVGName,
		UnnamedPVs:             []string{bdRacePVName},
		FileDeviceStateUnknown: true,
	}

	assert.False(t, anyUnnamedPVWithinBudget([]internal.LVMVolumeGroupCandidate{unclassified}, map[string]int{}),
		"a candidate the loop will skip must not pay for an uncached read, however young its unnamed PV is")

	classified := unclassified
	classified.FileDeviceStateUnknown = false
	assert.True(t, anyUnnamedPVWithinBudget([]internal.LVMVolumeGroupCandidate{classified}, map[string]int{}),
		"the same candidate the loop can act on is exactly what the re-read is for")

	// One unclassifiable Volume Group must not suppress the re-read another one
	// still needs: the verdict is per candidate, like everything else here.
	other := candidateWithUnnamedPVs("vg-other", "/dev/sdz")
	assert.True(t, anyUnnamedPVWithinBudget([]internal.LVMVolumeGroupCandidate{unclassified, other}, map[string]int{}))
}

// The counter is what an operator alerts on, so it has to mean "a refusal
// happened". A Physical Volume excluded by a BlockDeviceFilter never becomes a
// BlockDevice, so this branch runs on every discovery pass for as long as the
// exclusion stands; incremented every pass, increase() over any window would be
// non-zero forever and the alert would stop distinguishing a new refusal from a
// standing one.
func TestHandleUnnamedPVs_CountsARefusedImportOncePerAppearance(t *testing.T) {
	ctx := context.Background()
	cl := test_utils.NewFakeClient(&v1alpha1.LVMVolumeGroup{})
	d := bdRaceDiscoverer(t, cl, UncachedReader{Reader: cl}, bdRaceCache(t))

	candidate := internal.LVMVolumeGroupCandidate{
		LVMVGName:             bdRaceLVGName,
		ActualVGNameOnTheNode: bdRaceVGName,
		UnnamedPVs:            []string{bdRacePVName},
	}
	before := refusedImportCount(t, d.metrics, bdRaceLVGName)

	// Ten passes of the same standing state, carried the way the discoverer carries
	// it: this pass's counts become the next pass's previous.
	passes := map[string]int{}
	for range 10 {
		current := map[string]int{}
		d.handleUnnamedPVs(ctx, candidate, map[string]v1alpha1.LVMVolumeGroup{}, passes, current)
		passes = current
	}

	assert.Equal(t, before+1, refusedImportCount(t, d.metrics, bdRaceLVGName),
		"the refusal appeared once, so it is counted once")

	// A Physical Volume that goes unnamed after the state had settled changes what
	// is being refused, so that is a new refusal and is counted again.
	joined := candidate
	joined.UnnamedPVs = []string{bdRacePVName, "/dev/sdz"}
	d.handleUnnamedPVs(ctx, joined, map[string]v1alpha1.LVMVolumeGroup{}, passes, map[string]int{})

	assert.Equal(t, before+2, refusedImportCount(t, d.metrics, bdRaceLVGName),
		"a Physical Volume joining the refused set is a refusal of something new")
}

// candidateWithUnnamedPVs is the only part of a candidate replaceWithRebuilt
// looks at: the Volume Group it belongs to, and how much of it went unnamed.
func candidateWithUnnamedPVs(vgName string, unnamed ...string) internal.LVMVolumeGroupCandidate {
	return internal.LVMVolumeGroupCandidate{ActualVGNameOnTheNode: vgName, UnnamedPVs: unnamed}
}

func unnamedPVsByVG(candidates []internal.LVMVolumeGroupCandidate) map[string][]string {
	byVG := make(map[string][]string, len(candidates))
	for _, candidate := range candidates {
		byVG[candidate.ActualVGNameOnTheNode] = candidate.UnnamedPVs
	}

	return byVG
}

// The re-read may only fill gaps, never open them — and the verdict has to be
// reached per Volume Group, because per Volume Group is the grain at which
// status.nodes is published and spec.blockDeviceSelector is derived. Summed over
// the node instead, one Volume Group's gain pays for another's loss.
func TestReplaceWithRebuilt(t *testing.T) {
	tests := []struct {
		name        string
		candidates  []internal.LVMVolumeGroupCandidate
		rebuilt     []internal.LVMVolumeGroupCandidate
		wantUnnamed map[string][]string
		wantRefused []string
	}{
		{
			name:        "a fresh view that names more is taken",
			candidates:  []internal.LVMVolumeGroupCandidate{candidateWithUnnamedPVs("vg-a", "/dev/sdb", "/dev/sdc")},
			rebuilt:     []internal.LVMVolumeGroupCandidate{candidateWithUnnamedPVs("vg-a", "/dev/sdc")},
			wantUnnamed: map[string][]string{"vg-a": {"/dev/sdc"}},
		},
		{
			name:        "a fresh view that names less is refused",
			candidates:  []internal.LVMVolumeGroupCandidate{candidateWithUnnamedPVs("vg-a", "/dev/sdc")},
			rebuilt:     []internal.LVMVolumeGroupCandidate{candidateWithUnnamedPVs("vg-a", "/dev/sdb", "/dev/sdc")},
			wantUnnamed: map[string][]string{"vg-a": {"/dev/sdc"}},
			wantRefused: []string{"vg-a"},
		},
		{
			// The case a node-wide total cannot see: vg-a improves by one and vg-b
			// regresses by one, so the sum is unchanged while vg-b would be published
			// describing less of the node than the cache had described.
			name: "one Volume Group's gain does not pay for another's loss",
			candidates: []internal.LVMVolumeGroupCandidate{
				candidateWithUnnamedPVs("vg-a", "/dev/sdb", "/dev/sdc"),
				candidateWithUnnamedPVs("vg-b"),
			},
			rebuilt: []internal.LVMVolumeGroupCandidate{
				candidateWithUnnamedPVs("vg-a", "/dev/sdc"),
				candidateWithUnnamedPVs("vg-b", "/dev/sdd"),
			},
			wantUnnamed: map[string][]string{"vg-a": {"/dev/sdc"}, "vg-b": nil},
			wantRefused: []string{"vg-b"},
		},
		{
			// The two reads are moments apart, and a Volume Group that disappeared
			// between them is the next pass's business.
			name:        "a candidate with no counterpart keeps the view it came with",
			candidates:  []internal.LVMVolumeGroupCandidate{candidateWithUnnamedPVs("vg-a", "/dev/sdc")},
			rebuilt:     []internal.LVMVolumeGroupCandidate{candidateWithUnnamedPVs("vg-b")},
			wantUnnamed: map[string][]string{"vg-a": {"/dev/sdc"}},
		},
		{
			// Taking the rebuilt slice wholesale would put back a candidate the
			// duplicate-VG or unhealthy-VG filters have already dropped.
			name:        "a rebuilt candidate this pass dropped is not resurrected",
			candidates:  []internal.LVMVolumeGroupCandidate{candidateWithUnnamedPVs("vg-a", "/dev/sdc")},
			rebuilt:     []internal.LVMVolumeGroupCandidate{candidateWithUnnamedPVs("vg-a"), candidateWithUnnamedPVs("vg-dropped")},
			wantUnnamed: map[string][]string{"vg-a": nil},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			merged, refused := replaceWithRebuilt(tt.candidates, tt.rebuilt)

			assert.Equal(t, tt.wantUnnamed, unnamedPVsByVG(merged))
			assert.Equal(t, tt.wantRefused, refused)
		})
	}
}

// The rebuild is a whole second candidate, not a fresh BlockDevice list grafted
// onto the first: rebuildCandidatesFromAPIServer goes through
// GetLVMVolumeGroupCandidates again, and that re-runs the file-device probes on
// the node. So a rebuilt candidate can be worse in ways len(UnnamedPVs) cannot
// see, and each of those ways costs something the pass has no second chance at.
func TestReplaceWithRebuilt_RefusesARebuildThatDescribesLessOfTheNodeOtherThanByPVCount(t *testing.T) {
	const vgName = "vg-a"

	withDevices := func(devices, fileDevices int) internal.LVMVolumeGroupCandidate {
		candidate := candidateWithUnnamedPVs(vgName, "/dev/sdc")
		if devices > 0 {
			candidate.Nodes = map[string][]internal.LVMVGDevice{
				"node-1": make([]internal.LVMVGDevice, devices),
			}
		}
		if fileDevices > 0 {
			candidate.FileDeviceNodes = map[string][]internal.LVMVGFileDevice{
				"node-1": make([]internal.LVMVGFileDevice, fileDevices),
			}
		}

		return candidate
	}

	unknownFileDevices := func(candidate internal.LVMVolumeGroupCandidate) internal.LVMVolumeGroupCandidate {
		candidate.FileDeviceStateUnknown = true
		return candidate
	}

	tests := []struct {
		name        string
		cached      internal.LVMVolumeGroupCandidate
		fresh       internal.LVMVolumeGroupCandidate
		wantRefused bool
	}{
		{
			// The discovery loop skips a candidate whose file devices could not all be
			// classified, before it publishes anything and before trackUnnamedPVs. So a
			// probe that did not answer the second time costs the status this pass could
			// have written from the cached view, and restarts the unnamed-PV budget.
			name:        "a file-device probe that failed only on the rebuild",
			cached:      withDevices(1, 1),
			fresh:       unknownFileDevices(withDevices(1, 1)),
			wantRefused: true,
		},
		{
			// Down to describing nothing of the node, which handleUnnamedPVs answers
			// with NodeNotDescribed — a reason deliberately outside the controller's
			// acceptableReasons, so the LVMVolumeGroup leaves Ready.
			name:        "a rebuild that lost the node's only file device",
			cached:      withDevices(0, 1),
			fresh:       withDevices(0, 0),
			wantRefused: true,
		},
		{
			name:        "a rebuild that lost a block device",
			cached:      withDevices(2, 0),
			fresh:       withDevices(1, 0),
			wantRefused: true,
		},
		{
			// The direction the re-read exists for must still get through.
			name:   "a rebuild that gained a device is taken",
			cached: withDevices(1, 0),
			fresh:  withDevices(2, 0),
		},
		{
			// Classified this time where the cache had not been: strictly more known.
			name:   "a rebuild that resolved what the cached view could not classify",
			cached: unknownFileDevices(withDevices(1, 1)),
			fresh:  withDevices(1, 1),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			merged, refused := replaceWithRebuilt(
				[]internal.LVMVolumeGroupCandidate{tt.cached},
				[]internal.LVMVolumeGroupCandidate{tt.fresh},
			)

			require.Len(t, merged, 1)
			if tt.wantRefused {
				assert.Equal(t, []string{vgName}, refused)
				assert.Equal(t, tt.cached, merged[0], "the cached view is kept whole")
				return
			}

			assert.Empty(t, refused)
			assert.Equal(t, tt.fresh, merged[0], "the rebuilt view is taken whole")
		})
	}
}
