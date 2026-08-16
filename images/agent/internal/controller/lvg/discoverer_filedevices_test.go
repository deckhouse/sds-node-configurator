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
	"testing"
	"time"

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
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/mock_utils"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/monitoring"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/test_utils"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/utils"
)

func discovererWithMockedCommands(t *testing.T, mc *mock_utils.MockCommands) *Discoverer {
	t.Helper()
	cl := test_utils.NewFakeClient(&v1alpha1.LVMVolumeGroup{})
	return NewDiscoverer(
		cl,
		UncachedReader{Reader: cl},
		logger.Logger{},
		monitoring.GetMetrics(""),
		cache.New(),
		mc,
		DiscovererConfig{NodeName: "test_node"},
	)
}

// fdTestManagedPath is a backing-file path the agent itself would have produced
// for LVG "vg-a", so IsManagedFileDevicePath accepts it.
func fdTestManagedPath(t *testing.T, lvgName string) string {
	t.Helper()
	return utils.BuildFileDevicePath("/opt/deckhouse/sds/file-devices", lvgName, "d10g")
}

// Ownership of a loop PV is decided solely by the VG's
// storage.deckhouse.io/lvmVolumeGroupName tag. An untagged VG means the agent
// cannot prove the loop is its own, so it must claim nothing rather than guess.
func TestBuildFileDeviceFromLoopPV_RefusesWithoutOwnerTag(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	d := discovererWithMockedCommands(t, mc)

	// No command must run: the refusal happens before any losetup call.
	got, known := d.buildFileDeviceFromLoopPV(context.Background(),
		internal.PVData{PVName: "/dev/loop0"}, "", false)

	assert.Nil(t, got)
	assert.True(t, known, "a VG with no owner tag is a final answer, not a gap")
}

// losetup failing is not proof that the loop is not ours, so the discoverer must
// report the gap rather than let the caller drop the device from status.
//
// On the probe path the same failure is NOT a gap: an unreadable alias PV cannot
// be told apart from the ordinary not-yet-registered BlockDevice, which is benign
// and self-healing, so reporting it would stop status being written during every
// routine VG extension.
func TestBuildFileDeviceFromLoopPV_BackingFileLookupFails(t *testing.T) {
	tests := map[string]struct {
		probe     bool
		wantKnown bool
	}{
		"direct": {probe: false, wantKnown: false},
		"probe":  {probe: true, wantKnown: true},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mc := mock_utils.NewMockCommands(ctrl)
			d := discovererWithMockedCommands(t, mc)

			mc.EXPECT().GetLoopBackingFile(gomock.Any(), "/dev/loop0").
				Return("losetup -O BACK-FILE", internal.LoopBackingFile{}, errors.New("losetup: no such device"))

			got, known := d.buildFileDeviceFromLoopPV(context.Background(),
				internal.PVData{PVName: "/dev/loop0"}, "vg-a", tc.probe)

			assert.Nil(t, got)
			assert.Equal(t, tc.wantKnown, known)
		})
	}
}

// "No backing file" cannot be true of a device lvm has just listed as a PV, so on
// the canonical path it counts as unreadable rather than as evidence that this is
// not a file device. On the probe path it is the expected answer for a device that
// is simply not a loop.
func TestBuildFileDeviceFromLoopPV_EmptyBackingFile(t *testing.T) {
	tests := map[string]struct {
		probe     bool
		wantKnown bool
	}{
		"direct": {probe: false, wantKnown: false},
		"probe":  {probe: true, wantKnown: true},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mc := mock_utils.NewMockCommands(ctrl)
			d := discovererWithMockedCommands(t, mc)

			mc.EXPECT().GetLoopBackingFile(gomock.Any(), "/dev/loop0").
				Return("losetup -O BACK-FILE", internal.LoopBackingFile{}, nil)

			got, known := d.buildFileDeviceFromLoopPV(context.Background(),
				internal.PVData{PVName: "/dev/loop0"}, "vg-a", tc.probe)

			assert.Nil(t, got)
			assert.Equal(t, tc.wantKnown, known)
		})
	}
}

// This is the guard that keeps the agent from writing a foreign path into
// status — and therefore from rm-ing it later during cleanup. A file that
// merely looks like the managed pattern but names a different LVG is foreign.
func TestBuildFileDeviceFromLoopPV_RefusesForeignBackingFile(t *testing.T) {
	tests := map[string]string{
		"unrelated_path":     "/var/lib/other/disk.img",
		"managed_but_not_us": fdTestManagedPath(t, "vg-someone-else"),
	}

	for name, backingFile := range tests {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mc := mock_utils.NewMockCommands(ctrl)
			d := discovererWithMockedCommands(t, mc)

			mc.EXPECT().GetLoopBackingFile(gomock.Any(), "/dev/loop0").
				Return("losetup -O BACK-FILE", internal.LoopBackingFile{Path: backingFile}, nil)

			got, known := d.buildFileDeviceFromLoopPV(context.Background(),
				internal.PVData{PVName: "/dev/loop0"}, "vg-a", false)

			assert.Nil(t, got)
			assert.True(t, known, "a read that succeeded and said \"not ours\" is a verdict, not a gap")
		})
	}
}

// The happy path: a canonical /dev/loopN PV whose backing file is ours.
// Size must come from the PV, not from the backing file — status compares PV
// size against PV size, so taking the raw file size here would make the
// resource churn on every reconcile.
func TestBuildFileDeviceFromLoopPV_CanonicalLoop(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	d := discovererWithMockedCommands(t, mc)

	backingFile := fdTestManagedPath(t, "vg-a")
	pvSize := resource.MustParse("9Gi")

	mc.EXPECT().GetLoopBackingFile(gomock.Any(), "/dev/loop3").
		Return("losetup -O BACK-FILE", internal.LoopBackingFile{Path: backingFile}, nil)
	// A canonical loop needs no re-resolution, so FindLoopDeviceByFile must not run.

	got, known := d.buildFileDeviceFromLoopPV(context.Background(),
		internal.PVData{PVName: "/dev/loop3", PVSize: pvSize, PVUuid: "pv-uuid-1"}, "vg-a", false)

	assert.True(t, known)
	if assert.NotNil(t, got) {
		assert.Equal(t, backingFile, got.FilePath)
		assert.Equal(t, "/dev/loop3", got.LoopDevice)
		assert.Equal(t, pvSize.Value(), got.Size.Value())
		assert.Equal(t, "pv-uuid-1", got.PVUUID)
	}
}

// lvm sometimes reports the PV under a /dev/disk/by-id or /dev/block alias.
// Recording that alias verbatim would make status.loopDevice flip-flop between
// the alias and /dev/loopN across reconciles, so it is re-resolved.
func TestBuildFileDeviceFromLoopPV_ResolvesAliasToCanonicalLoop(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	d := discovererWithMockedCommands(t, mc)

	backingFile := fdTestManagedPath(t, "vg-a")
	alias := "/dev/disk/by-id/loop-vg-a"

	gomock.InOrder(
		mc.EXPECT().GetLoopBackingFile(gomock.Any(), alias).
			Return("losetup -O BACK-FILE", internal.LoopBackingFile{Path: backingFile}, nil),
		mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), backingFile).
			Return("losetup -j", "/dev/loop5", nil),
	)

	got, known := d.buildFileDeviceFromLoopPV(context.Background(),
		internal.PVData{PVName: alias, PVSize: resource.MustParse("9Gi"), PVUuid: "pv-uuid-2"}, "vg-a", true)

	assert.True(t, known)
	if assert.NotNil(t, got) {
		assert.Equal(t, "/dev/loop5", got.LoopDevice, "the alias must be replaced by the canonical loop")
	}
}

// If the canonical loop cannot be resolved the alias is kept: a slightly
// churn-prone status beats dropping a device that genuinely exists.
func TestBuildFileDeviceFromLoopPV_KeepsAliasWhenResolutionFails(t *testing.T) {
	backingFile := fdTestManagedPath(t, "vg-a")
	alias := "/dev/block/7:5"

	tests := map[string]struct {
		canonical string
		err       error
	}{
		"resolve_errors": {canonical: "", err: errors.New("losetup failed")},
		"resolve_empty":  {canonical: "", err: nil},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mc := mock_utils.NewMockCommands(ctrl)
			d := discovererWithMockedCommands(t, mc)

			gomock.InOrder(
				mc.EXPECT().GetLoopBackingFile(gomock.Any(), alias).
					Return("losetup -O BACK-FILE", internal.LoopBackingFile{Path: backingFile}, nil),
				mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), backingFile).
					Return("losetup -j", tc.canonical, tc.err),
			)

			got, known := d.buildFileDeviceFromLoopPV(context.Background(),
				internal.PVData{PVName: alias, PVSize: resource.MustParse("9Gi")}, "vg-a", false)

			assert.True(t, known, "the backing file was read; only the canonical-loop lookup failed")
			if assert.NotNil(t, got) {
				assert.Equal(t, alias, got.LoopDevice)
			}
		})
	}
}

func TestConvertLVMVGFileDevices(t *testing.T) {
	t.Run("empty_input_yields_empty_non_nil_slice", func(t *testing.T) {
		got := convertLVMVGFileDevices(nil)
		assert.NotNil(t, got, "status marshalling relies on an empty slice, not nil")
		assert.Empty(t, got)
	})

	t.Run("maps_every_field", func(t *testing.T) {
		size := resource.MustParse("9Gi")
		got := convertLVMVGFileDevices([]internal.LVMVGFileDevice{
			{FilePath: "/opt/a.img", LoopDevice: "/dev/loop0", Size: size, PVUUID: "uuid-a"},
			{FilePath: "/opt/b.img", LoopDevice: "/dev/loop1", Size: size, PVUUID: "uuid-b"},
		})

		assert.Equal(t, []v1alpha1.LVMVolumeGroupFileDevice{
			{FilePath: "/opt/a.img", LoopDevice: "/dev/loop0", Size: size, PVUuid: "uuid-a"},
			{FilePath: "/opt/b.img", LoopDevice: "/dev/loop1", Size: size, PVUuid: "uuid-b"},
		}, got)
	})
}

// The LVMVolumeGroup discoverer used to give up whenever the node reported no
// BlockDevices, on the assumption that no BlockDevices means no storage. That
// stopped holding with spec.fileDevices: such a VG lives on a loop device, and
// the BlockDevice discoverer deliberately does not publish loops. On a node
// whose only storage is file-backed the discovery loop therefore never ran —
// no candidates, so no VGReady and no AgentReady, and the resource stayed
// Pending with vgSize=0. It looked intermittent only because an unrelated disk
// left behind by another spec was enough to let the loop proceed.
func TestAnyLVGHasFileDevices(t *testing.T) {
	fileBacked := v1alpha1.LVMVolumeGroup{
		Spec: v1alpha1.LVMVolumeGroupSpec{
			ActualVGNameOnTheNode: "vg-file",
			FileDevices: []v1alpha1.LVMVolumeGroupFileDeviceSpec{
				{Name: "d2g", Directory: "/opt/deckhouse/sds/file-devices", Size: resource.MustParse("2Gi")},
			},
		},
	}
	blockBacked := v1alpha1.LVMVolumeGroup{
		Spec: v1alpha1.LVMVolumeGroupSpec{ActualVGNameOnTheNode: "vg-block"},
	}

	t.Run("file_backed_lvg_keeps_discovery_running", func(t *testing.T) {
		assert.True(t, anyLVGHasFileDevices(map[string]v1alpha1.LVMVolumeGroup{"vg-file": fileBacked}))
	})

	t.Run("mixed_set_counts", func(t *testing.T) {
		assert.True(t, anyLVGHasFileDevices(map[string]v1alpha1.LVMVolumeGroup{
			"vg-block": blockBacked, "vg-file": fileBacked,
		}))
	})

	t.Run("block_only_still_short_circuits", func(t *testing.T) {
		assert.False(t, anyLVGHasFileDevices(map[string]v1alpha1.LVMVolumeGroup{"vg-block": blockBacked}))
	})

	t.Run("no_lvgs_at_all", func(t *testing.T) {
		assert.False(t, anyLVGHasFileDevices(nil))
	})
}

// A file-backed Volume Group can only be imported under the name recorded in its
// tag: its backing files carry that name, and IsManagedFileDevicePath is gated on
// it. Under a generated name the agent would not recognise the file devices
// already in the VG, would rebuild spec.fileDevices for the new name, provision a
// second set of files and vgextend them in — doubling the Volume Group with no
// drift reported, since the entry names would match.
//
// So a name collision on a file-backed candidate is refused, not renamed.
func TestLVMVolumeGroupDiscoverReconcile_RefusesToRenameAFileBackedImport(t *testing.T) {
	const (
		takenName = "lvg-taken"
		vgOnNode  = "vg-orphan"
		node      = "test_node"
	)

	// The LVMVolumeGroup that already owns the name belongs to a different VG,
	// so the candidate is not matched to it and takes the import path.
	occupant := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: metav1.ObjectMeta{Name: takenName},
		Spec: v1alpha1.LVMVolumeGroupSpec{
			ActualVGNameOnTheNode: "vg-something-else",
			Type:                  internal.Local,
			Local:                 v1alpha1.LVMVolumeGroupLocalSpec{NodeName: node},
		},
	}

	cl := test_utils.NewFakeClient(&v1alpha1.LVMVolumeGroup{})
	if err := cl.Create(context.Background(), occupant); err != nil {
		t.Fatalf("unable to seed the occupying LVMVolumeGroup: %v", err)
	}
	log := logger.Logger{}
	metrics := monitoring.GetMetrics(node)
	sdsCache := cache.New()

	// One tagged, file-backed VG on the node whose LVMVolumeGroup is gone.
	sdsCache.StoreVGs([]internal.VGData{{
		VGName:       vgOnNode,
		VGUUID:       "vg-uuid-orphan",
		VGTags:       "storage.deckhouse.io/enabled=true,storage.deckhouse.io/lvmVolumeGroupName=" + takenName,
		VGSize:       resource.MustParse("10Gi"),
		VGFree:       resource.MustParse("10Gi"),
		VGExtentSize: resource.MustParse("4Mi"),
	}}, bytes.Buffer{})
	sdsCache.StorePVs([]internal.PVData{{
		PVName: "/dev/loop3",
		VGName: vgOnNode,
		VGUuid: "vg-uuid-orphan",
		PVSize: resource.MustParse("10Gi"),
	}}, bytes.Buffer{})
	sdsCache.StoreLVs(nil, bytes.Buffer{})

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	backing := utils.BuildFileDevicePath("/opt/deckhouse/sds/file-devices", takenName, "d1")
	mc.EXPECT().GetLoopBackingFile(gomock.Any(), "/dev/loop3").
		Return("losetup -O BACK-FILE", internal.LoopBackingFile{Path: backing}, nil).AnyTimes()
	mc.EXPECT().GetFilesystemSpace(gomock.Any(), gomock.Any()).
		Return("stat -f", internal.FilesystemSpace{AvailableBytes: int64(1) << 40, TotalBytes: int64(1) << 40}, nil).AnyTimes()

	d := NewDiscoverer(cl, UncachedReader{Reader: cl}, log, metrics, sdsCache, mc, DiscovererConfig{
		NodeName:                node,
		VolumeGroupScanInterval: time.Second,
	})

	// The candidate cannot be imported, so the loop requeues rather than
	// inventing a name for it.
	assert.True(t, d.LVMVolumeGroupDiscoverReconcile(context.Background()),
		"a refused import must requeue so the collision keeps being reported")

	var lvgs v1alpha1.LVMVolumeGroupList
	assert.NoError(t, cl.List(context.Background(), &lvgs))
	assert.Len(t, lvgs.Items, 1, "no LVMVolumeGroup may be minted for a file-backed VG whose name is taken")
	assert.Equal(t, takenName, lvgs.Items[0].Name)
	assert.Equal(t, "vg-something-else", lvgs.Items[0].Spec.ActualVGNameOnTheNode,
		"the occupant must be left untouched")
}

// A loop PV the node could not be asked about must not be published as an absence.
//
// status.nodes[].fileDevices is read downstream as a statement of fact: an entry
// missing from it is "never provisioned", which stops drift being reported for it,
// skips a requested growth, has validateLVGForUpdateFunc count its capacity as new
// on top of a VG size that already contains it, and takes away the record
// refuseUnlinkedBackingFile needs to avoid creating a second backing file at the
// same path. A single timed-out losetup must therefore leave the resource alone and
// requeue, not rewrite it.
func TestLVMVolumeGroupDiscoverReconcile_KeepsStatusWhenALoopPVCannotBeRead(t *testing.T) {
	const (
		lvgName = "lvg-a"
		vgName  = "vg-a"
		node    = "test_node"
	)
	backing := utils.BuildFileDevicePath("/opt/deckhouse/sds/file-devices", lvgName, "d1")

	existing := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: metav1.ObjectMeta{Name: lvgName},
		Spec: v1alpha1.LVMVolumeGroupSpec{
			ActualVGNameOnTheNode: vgName,
			Type:                  internal.Local,
			Local:                 v1alpha1.LVMVolumeGroupLocalSpec{NodeName: node},
			FileDevices: []v1alpha1.LVMVolumeGroupFileDeviceSpec{
				{Name: "d1", Directory: "/opt/deckhouse/sds/file-devices", Size: resource.MustParse("10Gi")},
			},
		},
		Status: v1alpha1.LVMVolumeGroupStatus{
			Nodes: []v1alpha1.LVMVolumeGroupNode{{
				Name: node,
				FileDevices: []v1alpha1.LVMVolumeGroupFileDevice{{
					Name:       "d1",
					FilePath:   backing,
					LoopDevice: "/dev/loop3",
					Size:       resource.MustParse("10Gi"),
					PVUuid:     "pv-uuid-1",
				}},
			}},
		},
	}

	cl := test_utils.NewFakeClient(&v1alpha1.LVMVolumeGroup{})
	require.NoError(t, cl.Create(context.Background(), existing))
	require.NoError(t, cl.Status().Update(context.Background(), existing))

	sdsCache := cache.New()
	sdsCache.StoreVGs([]internal.VGData{{
		VGName:       vgName,
		VGUUID:       "vg-uuid-a",
		VGTags:       "storage.deckhouse.io/enabled=true,storage.deckhouse.io/lvmVolumeGroupName=" + lvgName,
		VGSize:       resource.MustParse("10Gi"),
		VGFree:       resource.MustParse("10Gi"),
		VGExtentSize: resource.MustParse("4Mi"),
	}}, bytes.Buffer{})
	sdsCache.StorePVs([]internal.PVData{{
		PVName: "/dev/loop3",
		VGName: vgName,
		VGUuid: "vg-uuid-a",
		PVSize: resource.MustParse("10Gi"),
		PVUuid: "pv-uuid-1",
	}}, bytes.Buffer{})
	sdsCache.StoreLVs(nil, bytes.Buffer{})

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	// The one host command that would establish ownership does not answer.
	mc.EXPECT().GetLoopBackingFile(gomock.Any(), "/dev/loop3").
		Return("losetup -O BACK-FILE", internal.LoopBackingFile{}, errors.New("context deadline exceeded")).AnyTimes()
	mc.EXPECT().GetFilesystemSpace(gomock.Any(), gomock.Any()).
		Return("stat -f", internal.FilesystemSpace{AvailableBytes: 1 << 40, TotalBytes: 1 << 40}, nil).AnyTimes()

	d := NewDiscoverer(cl, UncachedReader{Reader: cl}, logger.Logger{}, monitoring.GetMetrics(node), sdsCache, mc, DiscovererConfig{
		NodeName:                node,
		VolumeGroupScanInterval: time.Second,
	})

	assert.True(t, d.LVMVolumeGroupDiscoverReconcile(context.Background()),
		"an incomplete picture of the node must requeue rather than be written out")

	var got v1alpha1.LVMVolumeGroup
	require.NoError(t, cl.Get(context.Background(), client.ObjectKey{Name: lvgName}, &got))
	require.Len(t, got.Status.Nodes, 1)
	require.Len(t, got.Status.Nodes[0].FileDevices, 1,
		"the provisioned file device must still be in status: it was never established to be absent")
	assert.Equal(t, "d1", got.Status.Nodes[0].FileDevices[0].Name)
	assert.Equal(t, backing, got.Status.Nodes[0].FileDevices[0].FilePath)
}

// The diff decides whether status is rewritten, so anything the reconciler reads
// out of status has to be in it. The entry name is one of those things:
// fileDeviceDriftReason skips a nameless device on the assumption that "the
// discoverer refills the name on the next pass", which only holds if a differing
// name counts as a difference.
func TestHasStatusFileDevicesDiff_ComparesTheEntryName(t *testing.T) {
	const path = "/opt/deckhouse/sds/file-devices/sds-vg-a.d1.img"
	device := func(name string) v1alpha1.LVMVolumeGroupFileDevice {
		return v1alpha1.LVMVolumeGroupFileDevice{
			Name:       name,
			FilePath:   path,
			LoopDevice: "/dev/loop0",
			Size:       resource.MustParse("10Gi"),
			PVUuid:     "pv-uuid-1",
		}
	}

	t.Run("identical devices are not a diff", func(t *testing.T) {
		assert.False(t, hasStatusFileDevicesDiff(
			[]v1alpha1.LVMVolumeGroupFileDevice{device("d1")},
			[]v1alpha1.LVMVolumeGroupFileDevice{device("d1")}))
	})

	t.Run("a name that status is missing is a diff", func(t *testing.T) {
		assert.True(t, hasStatusFileDevicesDiff(
			[]v1alpha1.LVMVolumeGroupFileDevice{device("d1")},
			[]v1alpha1.LVMVolumeGroupFileDevice{device("")}))
	})

	t.Run("a changed loop minor is still a diff", func(t *testing.T) {
		stale := device("d1")
		stale.LoopDevice = "/dev/loop7"
		assert.True(t, hasStatusFileDevicesDiff(
			[]v1alpha1.LVMVolumeGroupFileDevice{device("d1")},
			[]v1alpha1.LVMVolumeGroupFileDevice{stale}))
	})
}

// The allocated gauge is documented as bytes this module has taken from the
// filesystem, so it counts only the entries that are actually on the node —
// those present in status.nodes[].fileDevices. Summing the whole spec instead
// over-reports by the size of every entry rejected at validation or refused for
// want of space, which is exactly the situation in which the figure is read.
func TestCollectFileDeviceUsage_CountsOnlyProvisionedEntries(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	d := discovererWithMockedCommands(t, mc)

	lvgs := map[string]v1alpha1.LVMVolumeGroup{
		"vg-a": {
			ObjectMeta: metav1.ObjectMeta{Name: "vg-a"},
			Spec: v1alpha1.LVMVolumeGroupSpec{
				FileDevices: []v1alpha1.LVMVolumeGroupFileDeviceSpec{
					{Name: "live", Directory: "/data", Size: resource.MustParse("10Gi")},
					// Never brought up: no bytes were taken for it.
					{Name: "refused", Directory: "/data", Size: resource.MustParse("500Gi")},
				},
			},
			Status: v1alpha1.LVMVolumeGroupStatus{
				Nodes: []v1alpha1.LVMVolumeGroupNode{{
					Name:        "test_node",
					FileDevices: []v1alpha1.LVMVolumeGroupFileDevice{{Name: "live"}},
				}},
			},
		},
	}

	mc.EXPECT().GetFilesystemSpace(gomock.Any(), "/data").
		Return("stat -f", internal.FilesystemSpace{AvailableBytes: 1 << 40, TotalBytes: 2 << 40}, nil)

	usage := d.collectFileDeviceUsage(context.Background(), lvgs)

	require.Len(t, usage, 1)
	assert.Equal(t, "/data", usage[0].Directory)
	assert.Equal(t, int64(10)<<30, usage[0].AllocatedBytes)
	assert.True(t, usage[0].Known)
}

// Another node's devices are not this node's allocation, and the `stat -f` would
// be run against a directory this agent may not even have.
func TestCollectFileDeviceUsage_IgnoresAnotherNodesDevices(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	d := discovererWithMockedCommands(t, mc)

	lvgs := map[string]v1alpha1.LVMVolumeGroup{
		"vg-a": {
			ObjectMeta: metav1.ObjectMeta{Name: "vg-a"},
			Spec: v1alpha1.LVMVolumeGroupSpec{
				FileDevices: []v1alpha1.LVMVolumeGroupFileDeviceSpec{
					{Name: "live", Directory: "/data", Size: resource.MustParse("10Gi")},
				},
			},
			Status: v1alpha1.LVMVolumeGroupStatus{
				Nodes: []v1alpha1.LVMVolumeGroupNode{{
					Name:        "another_node",
					FileDevices: []v1alpha1.LVMVolumeGroupFileDevice{{Name: "live"}},
				}},
			},
		},
	}

	// No GetFilesystemSpace: nothing is provisioned on this node, so there is no
	// directory to measure.
	assert.Empty(t, d.collectFileDeviceUsage(context.Background(), lvgs))
}
