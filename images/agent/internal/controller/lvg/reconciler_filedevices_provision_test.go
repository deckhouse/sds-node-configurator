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

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	"k8s.io/apimachinery/pkg/api/resource"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/mock_utils"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/utils"
)

func provisionLVG(entries ...v1alpha1.LVMVolumeGroupFileDeviceSpec) *v1alpha1.LVMVolumeGroup {
	return &v1alpha1.LVMVolumeGroup{
		ObjectMeta: v1.ObjectMeta{Name: "vg-a"},
		Spec:       v1alpha1.LVMVolumeGroupSpec{FileDevices: entries},
	}
}

func fileEntry(name, size string) v1alpha1.LVMVolumeGroupFileDeviceSpec {
	return v1alpha1.LVMVolumeGroupFileDeviceSpec{Name: name, Directory: "/data", Size: resource.MustParse(size)}
}

// "No loop is attached" does not mean "no file exists". An attempt killed
// between fallocate and losetup leaves the file fully allocated, and its bytes
// are already accounted for on the filesystem — so only the ones it still lacks
// have to fit.
//
// Checking the full size instead refuses the entry forever on exactly the
// filesystems this feature is for, where the file is a large fraction of the
// whole: 50Gi requested, 50Gi already on disk, 10Gi left free, and no way out
// through the API because `size` cannot be lowered and the entry cannot be
// dropped while the Volume Group does not exist.
func TestProvisionFileDevices_ChecksOnlyTheMissingBytesOfAnExistingFile(t *testing.T) {
	t.Run("a complete file is not measured against the filesystem at all", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mc := mock_utils.NewMockCommands(ctrl)
		r := reconcilerWithMockedCommands(t, mc)

		fd := fileEntry("d50g", "50Gi")
		path := utils.BuildFileDevicePath(fd.Directory, "vg-a", fd.Name)

		gomock.InOrder(
			mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), path).Return("losetup -j", "", nil),
			mc.EXPECT().EnsureFileDeviceDirectory(gomock.Any(), fd.Directory).Return("mkdir -p", nil),
			// The whole 50Gi is already allocated, so there are no bytes to find.
			mc.EXPECT().GetFileAllocatedBytes(gomock.Any(), path).Return("stat -c %b %B", int64(50)<<30, nil),
			// No GetFilesystemSpace: bringing up a file that is already complete costs
			// the filesystem nothing, and asking anyway would refuse the entry whenever
			// the filesystem has meanwhile fallen below the reserve — quite possibly
			// because of these very backing files — with no way back through the API,
			// since `size` cannot be lowered.
			mc.EXPECT().CreateFileDevice(gomock.Any(), path, int64(50)<<30).Return("fallocate", nil),
			mc.EXPECT().SetupLoopDevice(gomock.Any(), path).Return("losetup --find", "/dev/loop2", nil),
			mc.EXPECT().SetLoopDirectIO(gomock.Any(), "/dev/loop2").Return("losetup --direct-io=on", nil),
		)

		loops, _, err := r.provisionFileDevices(context.Background(), provisionLVG(fd), fileDeviceIssues{}, false)
		assert.NoError(t, err)
		assert.Equal(t, []string{"/dev/loop2"}, loops)
	})

	t.Run("a partial file needs only the remainder", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mc := mock_utils.NewMockCommands(ctrl)
		r := reconcilerWithMockedCommands(t, mc)

		fd := fileEntry("d10g", "10Gi")
		path := utils.BuildFileDevicePath(fd.Directory, "vg-a", fd.Name)

		gomock.InOrder(
			mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), path).Return("losetup -j", "", nil),
			mc.EXPECT().EnsureFileDeviceDirectory(gomock.Any(), fd.Directory).Return("mkdir -p", nil),
			mc.EXPECT().GetFileAllocatedBytes(gomock.Any(), path).Return("stat -c %b %B", int64(8)<<30, nil),
			// 3Gi free covers the missing 2Gi even though the total is 10Gi.
			mc.EXPECT().GetFilesystemSpace(gomock.Any(), fd.Directory).Return("stat -f", internal.FilesystemSpace{AvailableBytes: int64(3) << 30}, nil),
			mc.EXPECT().CreateFileDevice(gomock.Any(), path, int64(10)<<30).Return("fallocate", nil),
			mc.EXPECT().SetupLoopDevice(gomock.Any(), path).Return("losetup --find", "/dev/loop2", nil),
			mc.EXPECT().SetLoopDirectIO(gomock.Any(), "/dev/loop2").Return("losetup --direct-io=on", nil),
		)

		_, _, err := r.provisionFileDevices(context.Background(), provisionLVG(fd), fileDeviceIssues{}, false)
		assert.NoError(t, err)
	})

	t.Run("the remainder still has to fit", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mc := mock_utils.NewMockCommands(ctrl)
		r := reconcilerWithMockedCommands(t, mc)

		fd := fileEntry("d10g", "10Gi")
		path := utils.BuildFileDevicePath(fd.Directory, "vg-a", fd.Name)

		gomock.InOrder(
			mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), path).Return("losetup -j", "", nil),
			mc.EXPECT().EnsureFileDeviceDirectory(gomock.Any(), fd.Directory).Return("mkdir -p", nil),
			mc.EXPECT().GetFileAllocatedBytes(gomock.Any(), path).Return("stat -c %b %B", int64(1)<<30, nil),
			// 9Gi missing, 1Gi free.
			mc.EXPECT().GetFilesystemSpace(gomock.Any(), fd.Directory).Return("stat -f", internal.FilesystemSpace{AvailableBytes: int64(1) << 30}, nil),
		)
		// No fallocate: filling the node's filesystem is the failure this guard exists for.

		_, _, err := r.provisionFileDevices(context.Background(), provisionLVG(fd), fileDeviceIssues{}, false)
		if assert.Error(t, err) {
			assert.Contains(t, err.Error(), "not enough free space")
		}
	})
}

// The rollback may only remove a file this call brought into existence. fallocate
// is idempotent, so a retry after an interrupted create runs against a file that
// is already there — and that file may already carry the PV label of a live
// Volume Group whose loop merely is not attached right now (a reattach that
// failed after a reboot). Removing it destroys the PV.
func TestProvisionFileDevices_NeverRemovesAPreExistingBackingFile(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)

	fd := fileEntry("d10g", "10Gi")
	path := utils.BuildFileDevicePath(fd.Directory, "vg-a", fd.Name)

	gomock.InOrder(
		mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), path).Return("losetup -j", "", nil),
		mc.EXPECT().EnsureFileDeviceDirectory(gomock.Any(), fd.Directory).Return("mkdir -p", nil),
		// The file is already there in full — it is not ours to delete, and there is
		// nothing left to allocate, so the filesystem is not measured.
		mc.EXPECT().GetFileAllocatedBytes(gomock.Any(), path).Return("stat -c %b %B", int64(10)<<30, nil),
		mc.EXPECT().CreateFileDevice(gomock.Any(), path, int64(10)<<30).Return("fallocate", nil),
		mc.EXPECT().SetupLoopDevice(gomock.Any(), path).Return("losetup --find", "", errors.New("EBUSY")),
		// Rollback: look for a loop the failed command may have bound anyway...
		mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), path).Return("losetup -j", "", nil),
	)
	// ...and stop there. RemoveFileDevice must NOT be called: gomock fails the
	// test if it is.

	_, _, err := r.provisionFileDevices(context.Background(), provisionLVG(fd), fileDeviceIssues{}, false)
	assert.Error(t, err)
}

// losetup applies --direct-io after attaching the device, and the kernel refuses
// direct I/O outright on a backing filesystem that cannot do it. Requesting it as
// part of the attach therefore produced a failed command with the loop already
// bound — and the rollback, told only that "losetup failed", removed the file and
// left the minor on a deleted inode whose blocks the filesystem could never
// reclaim. Once per reconcile.
//
// So the rollback does not trust the failed command: it looks for the loop.
func TestProvisionFileDevices_DetachesALoopTheFailedSetupLeftBehind(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)

	fd := fileEntry("d10g", "10Gi")
	path := utils.BuildFileDevicePath(fd.Directory, "vg-a", fd.Name)

	gomock.InOrder(
		mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), path).Return("losetup -j", "", nil),
		mc.EXPECT().EnsureFileDeviceDirectory(gomock.Any(), fd.Directory).Return("mkdir -p", nil),
		mc.EXPECT().GetFileAllocatedBytes(gomock.Any(), path).Return("stat -c %b %B", int64(0), utils.ErrFileDeviceAbsent),
		mc.EXPECT().GetFilesystemSpace(gomock.Any(), fd.Directory).Return("stat -f", internal.FilesystemSpace{AvailableBytes: int64(1) << 40}, nil),
		mc.EXPECT().CreateFileDevice(gomock.Any(), path, int64(10)<<30).Return("fallocate", nil),
		// Fails, but the device is bound and nothing was printed on stdout.
		mc.EXPECT().SetupLoopDevice(gomock.Any(), path).Return("losetup --find", "", errors.New("failed to set direct io")),
		// The rollback finds it anyway and detaches before removing the file.
		mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), path).Return("losetup -j", "/dev/loop6", nil),
		mc.EXPECT().DetachLoopDevice(gomock.Any(), "/dev/loop6").Return("losetup -d", nil),
		mc.EXPECT().RemoveFileDevice(gomock.Any(), path).Return("rm", nil),
	)

	_, _, err := r.provisionFileDevices(context.Background(), provisionLVG(fd), fileDeviceIssues{}, false)
	assert.Error(t, err)
}

// Direct I/O is a performance property, not a precondition: the kernel returns
// -EINVAL for it on any backing filesystem without an ->direct_IO implementation,
// and buffered I/O is no reason to leave a Volume Group unprovisioned.
func TestProvisionFileDevices_SurvivesDirectIOBeingUnavailable(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)

	fd := fileEntry("d10g", "10Gi")
	path := utils.BuildFileDevicePath(fd.Directory, "vg-a", fd.Name)

	gomock.InOrder(
		mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), path).Return("losetup -j", "", nil),
		mc.EXPECT().EnsureFileDeviceDirectory(gomock.Any(), fd.Directory).Return("mkdir -p", nil),
		mc.EXPECT().GetFileAllocatedBytes(gomock.Any(), path).Return("stat -c %b %B", int64(0), utils.ErrFileDeviceAbsent),
		mc.EXPECT().GetFilesystemSpace(gomock.Any(), fd.Directory).Return("stat -f", internal.FilesystemSpace{AvailableBytes: int64(1) << 40}, nil),
		mc.EXPECT().CreateFileDevice(gomock.Any(), path, int64(10)<<30).Return("fallocate", nil),
		mc.EXPECT().SetupLoopDevice(gomock.Any(), path).Return("losetup --find", "/dev/loop2", nil),
		mc.EXPECT().SetLoopDirectIO(gomock.Any(), "/dev/loop2").Return("losetup --direct-io=on", errors.New("EINVAL")),
	)
	// No rollback: the device is usable.

	loops, provisioned, err := r.provisionFileDevices(context.Background(), provisionLVG(fd), fileDeviceIssues{}, false)
	assert.NoError(t, err)
	assert.Equal(t, []string{"/dev/loop2"}, loops)
	assert.Len(t, provisioned, 1)
}

// On the update path a live Volume Group has to keep reconciling around one
// broken entry — the same reason validation is non-fatal there. Aborting on the
// first failure means an entry on a full filesystem indefinitely blocks a healthy
// entry that takes its space from an entirely different one.
func TestProvisionFileDevices_LenientKeepsGoingAfterAFailedEntry(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)

	bad := v1alpha1.LVMVolumeGroupFileDeviceSpec{Name: "bad", Directory: "/full", Size: resource.MustParse("10Gi")}
	good := fileEntry("good", "10Gi")
	badPath := utils.BuildFileDevicePath(bad.Directory, "vg-a", bad.Name)
	goodPath := utils.BuildFileDevicePath(good.Directory, "vg-a", good.Name)

	gomock.InOrder(
		// The first entry has nowhere to go.
		mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), badPath).Return("losetup -j", "", nil),
		mc.EXPECT().EnsureFileDeviceDirectory(gomock.Any(), bad.Directory).Return("mkdir -p", nil),
		mc.EXPECT().GetFileAllocatedBytes(gomock.Any(), badPath).Return("stat -c %b %B", int64(0), utils.ErrFileDeviceAbsent),
		mc.EXPECT().GetFilesystemSpace(gomock.Any(), bad.Directory).Return("stat -f", internal.FilesystemSpace{AvailableBytes: int64(1) << 20}, nil),
		// The second one is provisioned regardless.
		mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), goodPath).Return("losetup -j", "", nil),
		mc.EXPECT().EnsureFileDeviceDirectory(gomock.Any(), good.Directory).Return("mkdir -p", nil),
		mc.EXPECT().GetFileAllocatedBytes(gomock.Any(), goodPath).Return("stat -c %b %B", int64(0), utils.ErrFileDeviceAbsent),
		mc.EXPECT().GetFilesystemSpace(gomock.Any(), good.Directory).Return("stat -f", internal.FilesystemSpace{AvailableBytes: int64(1) << 40}, nil),
		mc.EXPECT().CreateFileDevice(gomock.Any(), goodPath, int64(10)<<30).Return("fallocate", nil),
		mc.EXPECT().SetupLoopDevice(gomock.Any(), goodPath).Return("losetup --find", "/dev/loop3", nil),
		mc.EXPECT().SetLoopDirectIO(gomock.Any(), "/dev/loop3").Return("losetup --direct-io=on", nil),
	)

	loops, provisioned, err := r.provisionFileDevices(context.Background(), provisionLVG(bad, good), fileDeviceIssues{}, true)

	// The failure is reported, and the healthy device is still returned so the
	// caller can extend the VG with it.
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), `fileDevices entry "bad"`)
		assert.Contains(t, err.Error(), "not enough free space")
	}
	assert.Equal(t, []string{"/dev/loop3"}, loops)
	assert.Len(t, provisioned, 1)
}

// The strict (create) path keeps its all-or-nothing behaviour: without a Volume
// Group, a half-provisioned set of file devices is not a state worth keeping, and
// everything created so far is unwound.
func TestProvisionFileDevices_StrictStillAbortsOnTheFirstFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)

	bad := v1alpha1.LVMVolumeGroupFileDeviceSpec{Name: "bad", Directory: "/full", Size: resource.MustParse("10Gi")}
	good := fileEntry("good", "10Gi")
	badPath := utils.BuildFileDevicePath(bad.Directory, "vg-a", bad.Name)

	gomock.InOrder(
		mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), badPath).Return("losetup -j", "", nil),
		mc.EXPECT().EnsureFileDeviceDirectory(gomock.Any(), bad.Directory).Return("mkdir -p", nil),
		mc.EXPECT().GetFileAllocatedBytes(gomock.Any(), badPath).Return("stat -c %b %B", int64(0), utils.ErrFileDeviceAbsent),
		mc.EXPECT().GetFilesystemSpace(gomock.Any(), bad.Directory).Return("stat -f", internal.FilesystemSpace{AvailableBytes: int64(1) << 20}, nil),
	)
	// The second entry is never even looked at.

	loops, _, err := r.provisionFileDevices(context.Background(), provisionLVG(bad, good), fileDeviceIssues{}, false)
	assert.Error(t, err)
	assert.Nil(t, loops)
}

// A `stat` that did not get to look says nothing about the path, and the two
// things provisioning derives from it pull in opposite directions when the
// difference is lost.
//
// The dangerous one is the rollback: createdFile means "this call brought the
// file into existence, so the rollback may remove it", and reading a timed-out
// stat as "the file is not there" put a backing file the agent did not create —
// possibly one carrying the Physical Volume of a live Volume Group whose loop is
// merely not attached — within reach of `rm -f`. The other is the free-space
// guard, which would then be asked for the whole size of a file that is already
// complete and refuse it forever.
//
// Nothing is provisioned on a measurement that did not happen. The entry is
// reported and retried, which is what a timeout deserves.
func TestProvisionFileDevices_RefusesToActOnAnUnreadableBackingFile(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)

	fd := fileEntry("d10g", "10Gi")
	path := utils.BuildFileDevicePath(fd.Directory, "vg-a", fd.Name)

	gomock.InOrder(
		mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), path).Return("losetup -j", "", nil),
		mc.EXPECT().EnsureFileDeviceDirectory(gomock.Any(), fd.Directory).Return("mkdir -p", nil),
		// Not ErrFileDeviceAbsent: the command was killed by the per-command
		// deadline, so it never established anything about the path.
		mc.EXPECT().GetFileAllocatedBytes(gomock.Any(), path).
			Return("stat -c %b %B", int64(0), context.DeadlineExceeded),
	)
	// And nothing else. No fallocate, no losetup — and above all no
	// RemoveFileDevice, which is what the mock controller asserts by not
	// expecting it.

	loops, provisioned, err := r.provisionFileDevices(context.Background(), provisionLVG(fd), fileDeviceIssues{}, false)
	assert.Error(t, err)
	assert.Empty(t, loops)
	assert.Empty(t, provisioned)
}

// The lenient (update) path treats the same unreadable file as one more entry
// that could not be brought up: reported, retried, and not allowed to stop the
// entries that are fine — which on a live Volume Group is the whole point.
func TestProvisionFileDevices_LenientSkipsAnUnreadableBackingFile(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)

	unreadable := fileEntry("d10g", "10Gi")
	healthy := fileEntry("d20g", "20Gi")
	unreadablePath := utils.BuildFileDevicePath(unreadable.Directory, "vg-a", unreadable.Name)
	healthyPath := utils.BuildFileDevicePath(healthy.Directory, "vg-a", healthy.Name)

	mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), unreadablePath).Return("losetup -j", "", nil)
	mc.EXPECT().EnsureFileDeviceDirectory(gomock.Any(), unreadable.Directory).Return("mkdir -p", nil).AnyTimes()
	mc.EXPECT().GetFileAllocatedBytes(gomock.Any(), unreadablePath).
		Return("stat -c %b %B", int64(0), errors.New("nsenter: cannot open /proc/1/ns/mnt"))

	mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), healthyPath).Return("losetup -j", "", nil)
	mc.EXPECT().GetFileAllocatedBytes(gomock.Any(), healthyPath).Return("stat -c %b %B", int64(0), utils.ErrFileDeviceAbsent)
	mc.EXPECT().GetFilesystemSpace(gomock.Any(), healthy.Directory).Return("stat -f", internal.FilesystemSpace{AvailableBytes: int64(1) << 40}, nil)
	mc.EXPECT().CreateFileDevice(gomock.Any(), healthyPath, int64(20)<<30).Return("fallocate", nil)
	mc.EXPECT().SetupLoopDevice(gomock.Any(), healthyPath).Return("losetup --find", "/dev/loop4", nil)
	mc.EXPECT().SetLoopDirectIO(gomock.Any(), "/dev/loop4").Return("losetup --direct-io=on", nil)

	loops, provisioned, err := r.provisionFileDevices(context.Background(), provisionLVG(unreadable, healthy), fileDeviceIssues{}, true)
	assert.Error(t, err)
	assert.Equal(t, []string{"/dev/loop4"}, loops)
	assert.Len(t, provisioned, 1)
}
