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
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/cache"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/logger"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/mock_utils"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/monitoring"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/test_utils"
)

func reconcilerWithReserve(t *testing.T, mc *mock_utils.MockCommands, percent int) *Reconciler {
	t.Helper()
	return NewReconciler(
		test_utils.NewFakeClient(&v1alpha1.LVMVolumeGroup{}, &v1alpha1.LVMLogicalVolume{}),
		logger.Logger{},
		monitoring.GetMetrics(""),
		cache.New(),
		mc,
		ReconcilerConfig{
			NodeName:                       "test_node",
			CmdDeadlineDuration:            30 * time.Second,
			FileDevicesMinFreeSpacePercent: percent,
		},
	)
}

const (
	tenGiB = int64(10) << 30
	oneGiB = int64(1) << 30
)

// The whole point of the reserve: "the file fits" is not "the node survives".
// A backing file that consumes every free byte of the node's root filesystem
// leaves nodefs.available at 0%, far below the 10% at which kubelet starts
// evicting pods — the node-level outage this check exists to prevent, reached by
// a check that only compares against free space.
func TestEnsureFileDeviceSpace_RefusesAnAllocationThatEatsTheReserve(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithReserve(t, mc, 15)

	// 10Gi filesystem with 4Gi free; the reserve is 1.5Gi, so at most 2.5Gi may
	// be taken. 3Gi fits in the free space and must still be refused.
	mc.EXPECT().GetFilesystemSpace(gomock.Any(), "/data").
		Return("stat -f", internal.FilesystemSpace{AvailableBytes: 4 * oneGiB, TotalBytes: tenGiB}, nil)

	err := r.ensureFileDeviceSpace(context.Background(), "/data", "/data/f.img", 3*oneGiB)
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "15%")
	}
}

// The mirror image: the reserve must not refuse an allocation that leaves the
// node exactly the share it was promised, or file devices would be unusable on a
// filesystem that has room for them.
func TestEnsureFileDeviceSpace_AllowsAnAllocationThatLeavesTheReserveIntact(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithReserve(t, mc, 15)

	mc.EXPECT().GetFilesystemSpace(gomock.Any(), "/data").
		Return("stat -f", internal.FilesystemSpace{AvailableBytes: 4 * oneGiB, TotalBytes: tenGiB}, nil)

	// 4Gi free minus the 1.5Gi reserve leaves 2.5Gi; asking for exactly that
	// must pass, so the boundary is inclusive rather than off by one extent.
	require.NoError(t, r.ensureFileDeviceSpace(context.Background(), "/data", "/data/f.img", 4*oneGiB-tenGiB/100*15))
}

// Zero is the documented opt-out for a dedicated data disk, and it has to mean
// the pre-existing behaviour exactly: refuse only what genuinely does not fit.
func TestEnsureFileDeviceSpace_ZeroPercentRestoresThePlainFreeSpaceCheck(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithReserve(t, mc, 0)

	gomock.InOrder(
		mc.EXPECT().GetFilesystemSpace(gomock.Any(), "/data").
			Return("stat -f", internal.FilesystemSpace{AvailableBytes: 4 * oneGiB, TotalBytes: tenGiB}, nil),
		mc.EXPECT().GetFilesystemSpace(gomock.Any(), "/data").
			Return("stat -f", internal.FilesystemSpace{AvailableBytes: 4 * oneGiB, TotalBytes: tenGiB}, nil),
	)

	require.NoError(t, r.ensureFileDeviceSpace(context.Background(), "/data", "/data/f.img", 4*oneGiB))

	err := r.ensureFileDeviceSpace(context.Background(), "/data", "/data/f.img", 4*oneGiB+1)
	if assert.Error(t, err) {
		assert.NotContains(t, err.Error(), "%", "with the reserve off the message must not talk about a share")
	}
}

// A filesystem that cannot be measured must not block provisioning: refusing
// there turns a monitoring failure into a storage failure, and `size` cannot be
// lowered afterwards to get out of it. fallocate still fails cleanly on ENOSPC.
func TestEnsureFileDeviceSpace_ProceedsWhenTheFilesystemCannotBeRead(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithReserve(t, mc, 15)

	mc.EXPECT().GetFilesystemSpace(gomock.Any(), "/data").
		Return("stat -f", internal.FilesystemSpace{}, errors.New("stat: command not found"))

	require.NoError(t, r.ensureFileDeviceSpace(context.Background(), "/data", "/data/f.img", 100*oneGiB))
}

// ---=== cleanupFileDevices: never unlink storage that is still in use ===--- //

// The guard that makes deleting an LVMVolumeGroup on a stale cache survivable.
// `losetup -d` is no protection: on a busy device LOOP_CLR_FD sets autoclear and
// returns success, so the `rm` that follows would unlink the backing file of a
// live Volume Group and the volumes on top would run off a deleted inode until
// the next reboot.
func TestCleanupFileDevices_RefusesALoopStillInAVolumeGroup(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)

	fd := v1alpha1.LVMVolumeGroupFileDeviceSpec{Name: "d10g", Directory: "/data", Size: resource.MustParse("10Gi")}
	lvg := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: v1.ObjectMeta{Name: "vg-a"},
		Spec:       v1alpha1.LVMVolumeGroupSpec{FileDevices: []v1alpha1.LVMVolumeGroupFileDeviceSpec{fd}},
	}

	mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), gomock.Any()).Return("losetup -j", "/dev/loop7", nil)
	mc.EXPECT().GetAllPVs(gomock.Any()).
		Return([]internal.PVData{{PVName: "/dev/loop7", VGName: "data-vg"}}, "lvm pvs", bytes.Buffer{}, nil)
	// No DetachLoopDevice, no RemoveFileDevice: the strict mock enforces it.

	err := r.cleanupFileDevices(context.Background(), lvg)
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "data-vg")
	}
}

// The counterpart, and the ordinary delete path: vgremove does not pvremove, so
// the loop is still a PV afterwards — an orphan one. Reporting that as "in use"
// would make cleanup refuse to run in the only case it exists for.
func TestCleanupFileDevices_ProceedsForAnOrphanPV(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)

	fd := v1alpha1.LVMVolumeGroupFileDeviceSpec{Name: "d10g", Directory: "/data", Size: resource.MustParse("10Gi")}
	lvg := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: v1.ObjectMeta{Name: "vg-a"},
		Spec:       v1alpha1.LVMVolumeGroupSpec{FileDevices: []v1alpha1.LVMVolumeGroupFileDeviceSpec{fd}},
	}

	expectOrphanPV(mc, "/dev/loop7")
	gomock.InOrder(
		mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), gomock.Any()).Return("losetup -j", "/dev/loop7", nil),
		mc.EXPECT().DetachLoopDevice(gomock.Any(), "/dev/loop7").Return("losetup -d", nil),
		mc.EXPECT().RemoveFileDevice(gomock.Any(), gomock.Any()).Return("rm -f", nil),
	)

	require.NoError(t, r.cleanupFileDevices(context.Background(), lvg))
}

// An unreadable PV list is not permission to unlink. A leaked loop and file are
// recoverable — the next reconcile reuses them — while removing the backing file
// of a Volume Group that turns out to be live is not.
func TestCleanupFileDevices_RefusesWhenThePVListCannotBeRead(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)

	fd := v1alpha1.LVMVolumeGroupFileDeviceSpec{Name: "d10g", Directory: "/data", Size: resource.MustParse("10Gi")}
	lvg := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: v1.ObjectMeta{Name: "vg-a"},
		Spec:       v1alpha1.LVMVolumeGroupSpec{FileDevices: []v1alpha1.LVMVolumeGroupFileDeviceSpec{fd}},
	}

	mc.EXPECT().GetAllPVs(gomock.Any()).Return(nil, "lvm pvs", bytes.Buffer{}, errors.New("lvm unavailable"))

	assert.Error(t, r.cleanupFileDevices(context.Background(), lvg))
}

// A file device another node reported in status is not this agent's to remove.
// LVMVolumeGroup.status.nodes is a list, and only the entry for this node
// describes files that exist here.
func TestCleanupFileDevices_IgnoresAnotherNodesStatusEntry(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	// Strict mock with no expectations: the foreign entry must produce no
	// command at all, not even the PV listing.
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)

	lvg := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: v1.ObjectMeta{Name: "vg-a"},
		Status: v1alpha1.LVMVolumeGroupStatus{
			Nodes: []v1alpha1.LVMVolumeGroupNode{{
				Name: "some-other-node",
				FileDevices: []v1alpha1.LVMVolumeGroupFileDevice{
					{FilePath: "/data/sds-vg-a.d10g.img", LoopDevice: "/dev/loop1"},
				},
			}},
		},
	}

	require.NoError(t, r.cleanupFileDevices(context.Background(), lvg))
}
