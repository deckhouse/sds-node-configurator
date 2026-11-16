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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"k8s.io/apimachinery/pkg/api/resource"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-common-lib/conditions"
	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/cache"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/controller"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/mock_utils"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/monitoring"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/test_utils"
)

const (
	releaseVGName = "vg-data"
	releaseVGUuid = "vg-uuid-1"
	releaseLVG    = "lvg-1"
)

var releaseOwnerTags = fmt.Sprintf("%s,%s=%s", internal.LVMTags[0], internal.LVMVolumeGroupTag, releaseLVG)

type releaseFixture struct {
	r       *Reconciler
	mc      *mock_utils.MockCommands
	api     client.WithWatch
	lvg     *v1alpha1.LVMVolumeGroup
	bds     map[string]v1alpha1.BlockDevice
	pvs     []internal.PVData
	vgs     []internal.VGData
	lvs     []internal.LVData
	pvsErr  error
	vgsErr  error
	lvsErr  error
	undated []string
	now     time.Time
}

func newReleaseFixture(t *testing.T, enabled bool) *releaseFixture {
	t.Helper()
	ctrl := gomock.NewController(t)
	mc := mock_utils.NewMockCommands(ctrl)
	cl := test_utils.NewFakeClient(&v1alpha1.LVMVolumeGroup{}, &v1alpha1.LVMLogicalVolume{})
	api := test_utils.NewFakeClient(&v1alpha1.LVMVolumeGroup{}, &v1alpha1.LVMLogicalVolume{})

	f := &releaseFixture{
		mc:  mc,
		api: api,
		r: NewReconciler(cl, UncachedReader{Reader: api}, logger.NewNop(), monitoring.GetMetrics(""), cache.New(), mc, nil,
			ReconcilerConfig{NodeName: "test_node", CmdDeadlineDuration: 30 * time.Second, VolumeGroupScanInterval: 5 * time.Second}),
		now: time.Unix(1_000_000, 0),
		lvg: &v1alpha1.LVMVolumeGroup{
			ObjectMeta: v1.ObjectMeta{Name: releaseLVG},
			Spec: v1alpha1.LVMVolumeGroupSpec{
				ActualVGNameOnTheNode:    releaseVGName,
				Type:                     "Local",
				Local:                    v1alpha1.LVMVolumeGroupLocalSpec{NodeName: "test_node"},
				BlockDeviceSelector:      &v1.LabelSelector{MatchLabels: map[string]string{"pool": "a"}},
				ReleaseEmptyBlockDevices: enabled,
			},
			Status: v1alpha1.LVMVolumeGroupStatus{
				VGUuid:     releaseVGUuid,
				ExtentSize: resource.MustParse("4Mi"),
				Nodes: []v1alpha1.LVMVolumeGroupNode{{
					Name: "test_node",
					Devices: []v1alpha1.LVMVolumeGroupDevice{
						{BlockDevice: "bd-1", PVUuid: "pv-1", Path: "/dev/sda"},
						{BlockDevice: "bd-2", PVUuid: "pv-2", Path: "/dev/sdb"},
					},
				}},
			},
		},
		bds: map[string]v1alpha1.BlockDevice{
			"bd-1": {ObjectMeta: v1.ObjectMeta{Name: "bd-1", Labels: map[string]string{"pool": "a"}}, Status: v1alpha1.BlockDeviceStatus{NodeName: "test_node", Path: "/dev/sda", PVUuid: "pv-1"}},
		},
		pvs: []internal.PVData{
			{PVName: "/dev/sda", PVUuid: "pv-1", VGName: releaseVGName, VGUuid: releaseVGUuid, VGTags: releaseOwnerTags, PVSize: resource.MustParse("10Gi"), PVUsed: "4194304 "},
			{PVName: "/dev/sdb", PVUuid: "pv-2", VGName: releaseVGName, VGUuid: releaseVGUuid, VGTags: releaseOwnerTags, PVSize: resource.MustParse("10Gi"), PVUsed: "0 "},
		},
		vgs: []internal.VGData{
			{VGName: releaseVGName, VGUUID: releaseVGUuid, VGTags: releaseOwnerTags, VGSize: resource.MustParse("20Gi"), VGExtentSize: resource.MustParse("4Mi")},
		},
		lvs: []internal.LVData{{LVName: "data", VGName: releaseVGName, VGUuid: releaseVGUuid, LVAttr: "-wi-a-----"}},
	}
	f.addBD(v1alpha1.BlockDevice{
		ObjectMeta: v1.ObjectMeta{Name: "bd-2", Labels: map[string]string{"pool": "b"}},
		Status:     v1alpha1.BlockDeviceStatus{NodeName: "test_node", Path: "/dev/sdb", PVUuid: "pv-2"},
	})

	if enabled {
		dated := func(ctx context.Context, name string) {
			if _, ok := ctx.Deadline(); !ok {
				f.undated = append(f.undated, name)
			}
		}
		mc.EXPECT().GetAllPVs(gomock.Any()).DoAndReturn(func(ctx context.Context) ([]internal.PVData, string, bytes.Buffer, error) {
			dated(ctx, "pvs")
			return f.pvs, "pvs", bytes.Buffer{}, f.pvsErr
		}).AnyTimes()
		mc.EXPECT().GetAllVGs(gomock.Any()).DoAndReturn(func(ctx context.Context) ([]internal.VGData, string, bytes.Buffer, error) {
			dated(ctx, "vgs")
			return f.vgs, "vgs", bytes.Buffer{}, f.vgsErr
		}).AnyTimes()
		mc.EXPECT().GetAllLVs(gomock.Any()).DoAndReturn(func(ctx context.Context) ([]internal.LVData, string, bytes.Buffer, error) {
			dated(ctx, "lvs")
			return f.lvs, "lvs", bytes.Buffer{}, f.lvsErr
		}).AnyTimes()
		mc.EXPECT().UdevadmTrigger(gomock.Any(), gomock.Any()).Return("udevadm", nil).AnyTimes()
	}

	f.r.now = func() time.Time { return f.now }
	return f
}

func (f *releaseFixture) addBD(bd v1alpha1.BlockDevice) {
	cached := bd.DeepCopy()
	if err := f.r.cl.Create(context.Background(), cached); err != nil {
		panic(err)
	}
	if err := f.api.Create(context.Background(), &bd); err != nil {
		panic(err)
	}
}

func (f *releaseFixture) updateLostBD(t *testing.T, mutate func(bd *v1alpha1.BlockDevice)) {
	t.Helper()
	bd := &v1alpha1.BlockDevice{}
	require.NoError(t, f.api.Get(context.Background(), client.ObjectKey{Name: "bd-2"}, bd))
	mutate(bd)
	require.NoError(t, f.api.Update(context.Background(), bd))
}

func (f *releaseFixture) pass() (string, time.Duration) {
	f.r.sdsCache.StorePVs(f.pvs, bytes.Buffer{})
	f.r.sdsCache.StoreVGs(f.vgs, bytes.Buffer{})
	f.r.sdsCache.StoreLVs(f.lvs, bytes.Buffer{})
	return f.r.releaseLostBlockDevices(context.Background(), f.lvg, f.bds)
}

func (f *releaseFixture) later() {
	f.now = f.now.Add(releaseConfirmWindow + time.Second)
}

func (f *releaseFixture) expectRelease() {
	gomock.InOrder(
		f.mc.EXPECT().ReduceVG(gomock.Any(), releaseVGName, "/dev/sdb").DoAndReturn(func(ctx context.Context, _, _ string) (string, error) {
			if _, ok := ctx.Deadline(); !ok {
				f.undated = append(f.undated, "vgreduce")
			}
			return "vgreduce", nil
		}),
		f.mc.EXPECT().RemovePV([]string{"/dev/sdb"}).Return("pvremove", nil),
	)
}

func (f *releaseFixture) releasedAfterTwoPasses(t *testing.T) {
	t.Helper()
	message, _ := f.pass()
	require.Contains(t, message, "if this still holds on a pass at least 1m0s later")
	f.later()
	f.expectRelease()
	message, _ = f.pass()
	assert.Contains(t, message, "its empty Physical Volume /dev/sdb was removed from VG vg-data")
}

func (f *releaseFixture) staysBlocked(t *testing.T, want string) {
	t.Helper()
	for range 3 {
		f.later()
		message, requeue := f.pass()
		assert.Contains(t, message, want)
		assert.Positive(t, requeue)
	}
}

func TestReleaseLostBlockDevices_NothingLost(t *testing.T) {
	f := newReleaseFixture(t, true)
	f.bds["bd-2"] = v1alpha1.BlockDevice{ObjectMeta: v1.ObjectMeta{Name: "bd-2"}}

	message, requeue := f.pass()
	assert.Empty(t, message)
	assert.Zero(t, requeue)
}

func TestReleaseLostBlockDevices_AllConditionsHoldAfterTwoPasses(t *testing.T) {
	f := newReleaseFixture(t, true)

	message, requeue := f.pass()
	assert.Contains(t, message, "bd-2")
	assert.Contains(t, message, "is removed from VG vg-data if this still holds on a pass at least 1m0s later")
	assert.Equal(t, releaseConfirmWindow, requeue)

	f.later()
	f.expectRelease()
	message, requeue = f.pass()
	assert.Contains(t, message, "its empty Physical Volume /dev/sdb was removed from VG vg-data")
	assert.Equal(t, 5*time.Second, requeue)
	assert.Empty(t, f.undated, "every LVM command of this path runs under a deadline")
}

func TestReleaseLostBlockDevices_PassesCloserThanTheWindowCountOnce(t *testing.T) {
	f := newReleaseFixture(t, true)

	f.pass()
	f.now = f.now.Add(30 * time.Second)
	message, _ := f.pass()
	assert.Contains(t, message, "if this still holds on a pass at least 1m0s later")
}

func TestReleaseLostBlockDevices_ADifferentPhysicalVolumeStartsOver(t *testing.T) {
	f := newReleaseFixture(t, true)

	f.pass()

	f.later()
	f.lvg.Status.Nodes[0].Devices[1].PVUuid = "pv-2b"
	f.pvs[1].PVUuid = "pv-2b"
	f.updateLostBD(t, func(bd *v1alpha1.BlockDevice) { bd.Status.PVUuid = "pv-2b" })
	message, _ := f.pass()
	assert.Contains(t, message, "if this still holds on a pass at least 1m0s later")

	f.later()
	f.expectRelease()
	message, _ = f.pass()
	assert.Contains(t, message, "was removed from VG vg-data")
}

func TestReleaseLostBlockDevices_OptionOffRunsNoLVMCommand(t *testing.T) {
	f := newReleaseFixture(t, false)

	for range 3 {
		f.later()
		message, requeue := f.pass()
		assert.Contains(t, message, "its Physical Volume /dev/sdb is empty; set spec.releaseEmptyBlockDevices to true")
		assert.Contains(t, message, "`vgreduce vg-data /dev/sdb` and `pvremove /dev/sdb`")
		assert.Positive(t, requeue, "the message is refreshed without waiting for an event")
	}
}

func TestReleaseLostBlockDevices_OptionOffWithDataSaysWhatToDo(t *testing.T) {
	f := newReleaseFixture(t, false)
	f.pvs[1].PVUsed = "8388608"

	message, requeue := f.pass()
	assert.Contains(t, message, "its Physical Volume /dev/sdb holds 8Mi of data (2 extents) and is not removed")
	assert.Contains(t, message, "run `pvmove /dev/sdb` on the node")
	assert.Contains(t, message, "then set spec.releaseEmptyBlockDevices to true")
	assert.Positive(t, requeue)

	f.later()
	f.pvs[1].PVUsed = "0"
	message, _ = f.pass()
	assert.Contains(t, message, "its Physical Volume /dev/sdb is empty; set spec.releaseEmptyBlockDevices to true")
}

func TestReleaseLostBlockDevices_OptionOnWithDataWaitsForPvmove(t *testing.T) {
	f := newReleaseFixture(t, true)
	f.pvs[1].PVUsed = "8388608"

	message, requeue := f.pass()
	assert.Contains(t, message, "holds 8Mi of data (2 extents)")
	assert.Contains(t, message, "the agent then removes the empty Physical Volume itself")
	assert.Positive(t, requeue)
}

func TestReleaseLostBlockDevices_EachConditionBlocksOnItsOwn(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(t *testing.T, f *releaseFixture)
		want   string
	}{
		{
			name: "BlockDevice does not exist",
			mutate: func(t *testing.T, f *releaseFixture) {
				assert.NoError(t, f.api.Delete(context.Background(), &v1alpha1.BlockDevice{ObjectMeta: v1.ObjectMeta{Name: "bd-2"}}))
			},
			want: "the BlockDevice resource does not exist",
		},
		{
			name: "labels match the selector",
			mutate: func(t *testing.T, f *releaseFixture) {
				f.updateLostBD(t, func(bd *v1alpha1.BlockDevice) { bd.Labels = map[string]string{"pool": "a"} })
			},
			want: "labels of the BlockDevice match the blockDeviceSelector",
		},
		{
			name: "BlockDevice is Missing",
			mutate: func(t *testing.T, f *releaseFixture) {
				f.updateLostBD(t, func(bd *v1alpha1.BlockDevice) {
					bd.Status.Conditions = []v1.Condition{{Type: v1alpha1.BlockDeviceConditionMissing, Status: v1.ConditionUnknown, Reason: "NotSeen", LastTransitionTime: v1.Now()}}
				})
			},
			want: "marked Missing",
		},
		{
			name: "BlockDevice is on another node",
			mutate: func(t *testing.T, f *releaseFixture) {
				f.updateLostBD(t, func(bd *v1alpha1.BlockDevice) { bd.Status.NodeName = "other" })
			},
			want: "is on the node other",
		},
		{
			name: "BlockDevice is a loop device",
			mutate: func(t *testing.T, f *releaseFixture) {
				f.updateLostBD(t, func(bd *v1alpha1.BlockDevice) { bd.Status.Type = internal.LoopDeviceType })
			},
			want: "is a loop device",
		},
		{
			name: "status has no Physical Volume UUID for the device",
			mutate: func(_ *testing.T, f *releaseFixture) {
				f.lvg.Status.Nodes[0].Devices[1].PVUuid = ""
			},
			want: "has no Physical Volume UUID for it, and a Physical Volume is never picked by its path alone",
		},
		{
			name: "another BlockDevice on the node carries the same Physical Volume",
			mutate: func(_ *testing.T, f *releaseFixture) {
				f.addBD(v1alpha1.BlockDevice{
					ObjectMeta: v1.ObjectMeta{Name: "bd-2-sdx"},
					Status:     v1alpha1.BlockDeviceStatus{NodeName: "test_node", Path: "/dev/sdx", PVUuid: "pv-2"},
				})
			},
			want: "the BlockDevice bd-2-sdx on the same node carries the same Physical Volume pv-2",
		},
		{
			name:   "Physical Volumes cannot be listed",
			mutate: func(_ *testing.T, f *releaseFixture) { f.pvsErr = errors.New("nsenter failed") },
			want:   "unable to list the Physical Volumes",
		},
		{
			name:   "Volume Groups cannot be listed",
			mutate: func(_ *testing.T, f *releaseFixture) { f.vgsErr = errors.New("nsenter failed") },
			want:   "unable to list the Volume Groups",
		},
		{
			name:   "no Physical Volume with the UUID",
			mutate: func(_ *testing.T, f *releaseFixture) { f.pvs = f.pvs[:1] },
			want:   "LVM reports no Physical Volume with the UUID pv-2",
		},
		{
			name: "a Physical Volume on the same path with another UUID",
			mutate: func(_ *testing.T, f *releaseFixture) {
				f.pvs[1].PVUuid = "pv-foreign"
			},
			want: "LVM reports no Physical Volume with the UUID pv-2",
		},
		{
			name: "Physical Volume is in no Volume Group",
			mutate: func(_ *testing.T, f *releaseFixture) {
				f.pvs[1].VGName = ""
				f.pvs[1].VGUuid = ""
				f.pvs[1].VGTags = ""
			},
			want: "its Physical Volume /dev/sdb (UUID pv-2) is in no Volume Group",
		},
		{
			name:   "pv_used is not empty",
			mutate: func(_ *testing.T, f *releaseFixture) { f.pvs[1].PVUsed = "4194304" },
			want:   "holds 4Mi of data",
		},
		{
			name:   "pv_used is unreadable",
			mutate: func(_ *testing.T, f *releaseFixture) { f.pvs[1].PVUsed = "" },
			want:   "unable to tell whether its Physical Volume /dev/sdb is empty",
		},
		{
			name:   "Physical Volume is in another Volume Group",
			mutate: func(_ *testing.T, f *releaseFixture) { f.pvs[1].VGUuid = "other-uuid"; f.pvs[1].VGName = "other" },
			want:   "belongs to VG other",
		},
		{
			name:   "Volume Group of the same UUID under another name",
			mutate: func(_ *testing.T, f *releaseFixture) { f.pvs[1].VGName = "other" },
			want:   "belongs to VG other (UUID vg-uuid-1)",
		},
		{
			name:   "status has no VG UUID",
			mutate: func(_ *testing.T, f *releaseFixture) { f.lvg.Status.VGUuid = "" },
			want:   "belongs to VG vg-data",
		},
		{
			name:   "Volume Group has no owner tag",
			mutate: func(_ *testing.T, f *releaseFixture) { f.pvs[1].VGTags = internal.LVMTags[0] },
			want:   "VG vg-data does not carry the tag storage.deckhouse.io/lvmVolumeGroupName=lvg-1",
		},
		{
			name: "Volume Group is tagged for another LVMVolumeGroup",
			mutate: func(_ *testing.T, f *releaseFixture) {
				f.pvs[1].VGTags = fmt.Sprintf("%s,%s=lvg-other", internal.LVMTags[0], internal.LVMVolumeGroupTag)
			},
			want: "does not carry the tag storage.deckhouse.io/lvmVolumeGroupName=lvg-1",
		},
		{
			name: "Volume Group is not listed by vgs",
			mutate: func(_ *testing.T, f *releaseFixture) {
				f.vgs = nil
				f.pvs[1].VGSystemID = "host-b"
			},
			want: "VG vg-data (system ID host-b) is not listed by vgs on this node",
		},
		{
			name:   "Volume Group is shared",
			mutate: func(_ *testing.T, f *releaseFixture) { f.vgs[0].VGShared = "shared" },
			want:   "VG vg-data is a shared Volume Group",
		},
		{
			name: "no other Physical Volume stays",
			mutate: func(_ *testing.T, f *releaseFixture) {
				f.pvs[0].VGUuid = "other-uuid"
				f.pvs[0].VGName = "other"
			},
			want: "is the only one in VG vg-data",
		},
		{
			name:   "Logical Volumes cannot be listed",
			mutate: func(_ *testing.T, f *releaseFixture) { f.lvsErr = errors.New("lvs failed") },
			want:   "unable to list the Logical Volumes",
		},
		{
			name: "pvmove is running",
			mutate: func(_ *testing.T, f *releaseFixture) {
				f.lvs = append(f.lvs, internal.LVData{LVName: "[pvmove0]", VGName: releaseVGName, VGUuid: releaseVGUuid})
			},
			want: "a pvmove is running in VG vg-data",
		},
		{
			name:   "a Logical Volume is being moved",
			mutate: func(_ *testing.T, f *releaseFixture) { f.lvs[0].MovePv = "/dev/sdb" },
			want:   "a pvmove is running in VG vg-data",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newReleaseFixture(t, true)
			tc.mutate(t, f)
			f.staysBlocked(t, tc.want)
		})
	}
}

func TestReleaseLostBlockDevices_ATwinOnAnotherNodeDoesNotBlock(t *testing.T) {
	f := newReleaseFixture(t, true)
	f.addBD(v1alpha1.BlockDevice{
		ObjectMeta: v1.ObjectMeta{Name: "bd-2-elsewhere"},
		Status:     v1alpha1.BlockDeviceStatus{NodeName: "other", Path: "/dev/sdb", PVUuid: "pv-2"},
	})

	f.releasedAfterTwoPasses(t)
}

func TestReleaseLostBlockDevices_BlockDeviceIsReadPastTheCache(t *testing.T) {
	f := newReleaseFixture(t, true)
	stale := &v1alpha1.BlockDevice{}
	require.NoError(t, f.r.cl.Get(context.Background(), client.ObjectKey{Name: "bd-2"}, stale))
	stale.Labels = map[string]string{"pool": "a"}
	require.NoError(t, f.r.cl.Update(context.Background(), stale))

	f.releasedAfterTwoPasses(t)
}

func thinPoolFixture(t *testing.T, size string, actual string) *releaseFixture {
	t.Helper()
	f := newReleaseFixture(t, true)
	f.lvg.Spec.ThinPools = []v1alpha1.LVMVolumeGroupThinPoolSpec{{Name: "tp", Size: size}}
	poolSize := resource.MustParse(actual)
	f.pvs[0].PVUsed = fmt.Sprintf("%d", poolSize.Value())
	f.lvs = []internal.LVData{
		{LVName: "tp", VGName: releaseVGName, VGUuid: releaseVGUuid, LVAttr: "twi-a-tz--", LVSize: poolSize},
	}
	return f
}

func TestReleaseLostBlockDevices_PercentThinPoolLargerThanItsShareOfTheSmallerVGBlocks(t *testing.T) {
	f := thinPoolFixture(t, "40%", "8Gi")

	f.staysBlocked(t, "without its Physical Volume /dev/sdb, VG vg-data would be 10Gi, and thin pool tp, sized 40% of the Volume Group, would ask for 4Gi while it is 8Gi")
	message, _ := f.pass()
	assert.Contains(t, message, "set the size of thin pool tp in spec.thinPools to an absolute value of at least 8Gi first")
}

func TestReleaseLostBlockDevices_PercentThinPoolThatFitsTheSmallerVGDoesNotBlock(t *testing.T) {
	f := thinPoolFixture(t, "40%", "4Gi")

	f.releasedAfterTwoPasses(t)
}

func TestReleaseLostBlockDevices_AbsoluteThinPoolDoesNotBlock(t *testing.T) {
	f := thinPoolFixture(t, "8Gi", "8Gi")

	f.releasedAfterTwoPasses(t)
}

func TestReleaseLostBlockDevices_FullVGThinPoolDoesNotBlock(t *testing.T) {
	f := thinPoolFixture(t, "100%", "10Gi")

	f.releasedAfterTwoPasses(t)
}

func TestReleaseLostBlockDevices_ABlockedPassResetsTheConfirmation(t *testing.T) {
	f := newReleaseFixture(t, true)

	f.pass()

	f.later()
	f.pvs[1].PVUsed = "4194304"
	message, _ := f.pass()
	assert.Contains(t, message, "holds 4Mi of data")

	f.later()
	f.pvs[1].PVUsed = "0"
	message, _ = f.pass()
	assert.Contains(t, message, "if this still holds on a pass at least 1m0s later")

	f.later()
	f.expectRelease()
	message, _ = f.pass()
	assert.Contains(t, message, "was removed from VG vg-data")
}

func TestReleaseLostBlockDevices_DeviceThatLeftTheListLosesItsConfirmation(t *testing.T) {
	f := newReleaseFixture(t, true)

	f.pass()

	f.later()
	f.bds["bd-2"] = v1alpha1.BlockDevice{ObjectMeta: v1.ObjectMeta{Name: "bd-2"}}
	message, _ := f.pass()
	assert.Empty(t, message)

	delete(f.bds, "bd-2")
	f.later()
	message, _ = f.pass()
	assert.Contains(t, message, "if this still holds on a pass at least 1m0s later")
}

func TestReleaseLostBlockDevices_VGReduceFailureLeavesTheDeviceAlone(t *testing.T) {
	f := newReleaseFixture(t, true)

	f.pass()
	f.later()
	f.mc.EXPECT().ReduceVG(gomock.Any(), releaseVGName, "/dev/sdb").Return("vgreduce", errors.New("Physical volume /dev/sdb still in use"))
	message, requeue := f.pass()
	assert.Contains(t, message, "unable to remove its Physical Volume /dev/sdb from VG vg-data: Physical volume /dev/sdb still in use")
	assert.Positive(t, requeue)
}

func TestReleaseLostBlockDevices_OnlyTheEmptyDeviceOfTwoIsReleased(t *testing.T) {
	f := newReleaseFixture(t, true)
	f.lvg.Status.Nodes[0].Devices = append(f.lvg.Status.Nodes[0].Devices, v1alpha1.LVMVolumeGroupDevice{BlockDevice: "bd-3", PVUuid: "pv-3", Path: "/dev/sdc"})
	f.addBD(v1alpha1.BlockDevice{
		ObjectMeta: v1.ObjectMeta{Name: "bd-3", Labels: map[string]string{"pool": "b"}},
		Status:     v1alpha1.BlockDeviceStatus{NodeName: "test_node", Path: "/dev/sdc", PVUuid: "pv-3"},
	})
	f.pvs = append(f.pvs, internal.PVData{PVName: "/dev/sdc", PVUuid: "pv-3", VGName: releaseVGName, VGUuid: releaseVGUuid, VGTags: releaseOwnerTags, PVSize: resource.MustParse("10Gi"), PVUsed: "12582912"})

	f.pass()
	f.later()
	f.expectRelease()
	message, _ := f.pass()
	assert.Contains(t, message, "bd-2: its empty Physical Volume /dev/sdb was removed")
	assert.Contains(t, message, "bd-3: its Physical Volume /dev/sdc holds 12Mi of data (3 extents)")
}

func TestLostBlockDeviceNames(t *testing.T) {
	f := newReleaseFixture(t, true)
	assert.Equal(t, []string{"bd-2"}, lostBlockDeviceNames(f.lvg, f.bds))

	f.lvg.Status.Nodes = nil
	assert.Empty(t, lostBlockDeviceNames(f.lvg, f.bds))
}

func reconcileWithLostDevice(t *testing.T, enabled bool, pvUsed string) (*v1alpha1.LVMVolumeGroup, controller.Result) {
	t.Helper()
	ctx := context.Background()
	f := newReleaseFixture(t, enabled)
	f.pvs[1].PVUsed = pvUsed

	lvg := f.lvg.DeepCopy()
	lvg.Finalizers = []string{internal.SdsNodeConfiguratorFinalizer}
	status := lvg.Status
	require.NoError(t, f.r.cl.Create(ctx, lvg))
	lvg.Status = status
	require.NoError(t, f.r.cl.Status().Update(ctx, lvg))

	bd1 := f.bds["bd-1"]
	bd1.ResourceVersion = ""
	f.addBD(bd1)

	f.r.sdsCache.StoreVGs(f.vgs, bytes.Buffer{})
	f.r.sdsCache.StorePVs(f.pvs, bytes.Buffer{})
	f.r.sdsCache.StoreLVs(f.lvs, bytes.Buffer{})

	res, err := f.r.Reconcile(ctx, controller.ReconcileRequest[*v1alpha1.LVMVolumeGroup]{Object: lvg})
	require.NoError(t, err)

	got := &v1alpha1.LVMVolumeGroup{}
	require.NoError(t, f.r.cl.Get(ctx, client.ObjectKeyFromObject(lvg), got))
	return got, res
}

func TestReconcile_LostEmptyDeviceWithOptionOffIsExplainedOnTheCondition(t *testing.T) {
	got, res := reconcileWithLostDevice(t, false, "0 ")

	applied := conditions.Get(got.Status.Conditions, internal.TypeVGConfigurationApplied)
	require.NotNil(t, applied)
	assert.Equal(t, v1.ConditionFalse, applied.Status)
	assert.Equal(t, internal.ReasonValidationFailed, applied.Reason)
	assert.Contains(t, applied.Message, "these BlockDevices no longer match the blockDeviceSelector: bd-2")
	assert.Contains(t, applied.Message, "set spec.releaseEmptyBlockDevices to true")
	assert.Positive(t, res.RequeueAfter)
}

func TestReconcile_LostDeviceWithDataIsExplainedOnTheCondition(t *testing.T) {
	got, res := reconcileWithLostDevice(t, true, "20971520")

	applied := conditions.Get(got.Status.Conditions, internal.TypeVGConfigurationApplied)
	require.NotNil(t, applied)
	assert.Equal(t, internal.ReasonValidationFailed, applied.Reason)
	assert.Contains(t, applied.Message, "holds 20Mi of data (5 extents)")
	assert.Contains(t, applied.Message, "pvmove /dev/sdb")
	assert.Positive(t, res.RequeueAfter, "the device is looked at again once the data has been moved")
}

func TestReconcile_DeletionForgetsTheSightingsOfTheLVMVolumeGroup(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)
	r.releaseSightings = map[string]releaseSighting{
		releaseKey("vg-a", "bd-2"): {pvUUID: "pv-2", passes: 1},
		releaseKey("vg-b", "bd-9"): {pvUUID: "pv-9", passes: 1},
	}

	lvg := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: v1.ObjectMeta{Name: "vg-a", Finalizers: []string{internal.SdsNodeConfiguratorFinalizer, consumerFinalizer}},
		Spec: v1alpha1.LVMVolumeGroupSpec{
			ActualVGNameOnTheNode: "data-vg",
			Type:                  internal.Local,
			Local:                 v1alpha1.LVMVolumeGroupLocalSpec{NodeName: "test_node"},
		},
	}
	require.NoError(t, r.cl.Create(ctx, lvg))
	require.NoError(t, r.cl.Delete(ctx, lvg))
	require.NoError(t, r.cl.Get(ctx, client.ObjectKeyFromObject(lvg), lvg))

	_, err := r.Reconcile(ctx, controller.ReconcileRequest[*v1alpha1.LVMVolumeGroup]{Object: lvg})
	require.NoError(t, err)
	assert.Equal(t, map[string]releaseSighting{releaseKey("vg-b", "bd-9"): {pvUUID: "pv-9", passes: 1}}, r.releaseSightings)
}
