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

// A backing file unlinked while its loop device is still attached: the Physical
// Volume is live on an inode nobody can open again, and `losetup -j <path>` — which
// matches by inode — reports nothing at all. Taking that for "this entry has
// nothing on the node" is how the Volume Group doubles.

package lvg

import (
	"bytes"
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

// unlinkedLVG is an LVMVolumeGroup whose single entry status says is provisioned on
// /dev/loop4.
func unlinkedLVG() (*v1alpha1.LVMVolumeGroup, v1alpha1.LVMVolumeGroupFileDeviceSpec, string) {
	fd := v1alpha1.LVMVolumeGroupFileDeviceSpec{Name: "d10g", Directory: "/data", Size: resource.MustParse("10Gi")}
	path := utils.BuildFileDevicePath(fd.Directory, "vg-a", fd.Name)
	return &v1alpha1.LVMVolumeGroup{
		ObjectMeta: v1.ObjectMeta{Name: "vg-a"},
		Spec:       v1alpha1.LVMVolumeGroupSpec{FileDevices: []v1alpha1.LVMVolumeGroupFileDeviceSpec{fd}},
		Status: v1alpha1.LVMVolumeGroupStatus{
			Nodes: []v1alpha1.LVMVolumeGroupNode{{
				Name: "test_node",
				FileDevices: []v1alpha1.LVMVolumeGroupFileDevice{
					{Name: fd.Name, FilePath: path, LoopDevice: "/dev/loop4", Size: resource.MustParse("10Gi")},
				},
			}},
		},
	}, fd, path
}

// The failure this guard exists for: a fresh file at the same path is a different
// inode, `losetup --nooverlap` cannot match the old loop, and vgextend then puts a
// second Physical Volume of the same size into the Volume Group — half of it on an
// inode nobody can open.
func TestProvisionFileDevices_RefusesToReprovisionAnUnlinkedBackingFile(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)

	lvg, _, path := unlinkedLVG()

	// losetup matches by inode, so the unlinked path looks unattached.
	mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), path).Return("losetup -j", "", nil)
	// The loop status recorded still holds the file, and losetup says it is gone.
	mc.EXPECT().GetLoopBackingFile(gomock.Any(), "/dev/loop4").
		Return("losetup -O BACK-FILE", internal.LoopBackingFile{Path: path, Deleted: true}, nil)
	// No mkdir, no fallocate, no losetup --find: the strict mock fails the test if
	// provisioning goes ahead.

	loops, provisioned, err := r.provisionFileDevices(context.Background(), lvg, fileDeviceIssues{}, true)
	assert.Empty(t, loops)
	assert.Empty(t, provisioned)
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "unlinked")
		assert.Contains(t, err.Error(), "pvmove", "the message has to say how to get out of it")
	}
}

// It is a per-entry problem, not a broken Volume Group: the Physical Volume is live
// and every volume on it keeps working, so the reason must be one the conditions
// watcher treats as still in service.
func TestExtendFileDevicesIfNeeded_UnlinkedBackingFileIsNotAVGFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)

	lvg, _, path := unlinkedLVG()
	vg := internal.VGData{VGName: "vg-test"}

	mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), path).Return("losetup -j", "", nil)
	mc.EXPECT().GetLoopBackingFile(gomock.Any(), "/dev/loop4").
		Return("losetup -O BACK-FILE", internal.LoopBackingFile{Path: path, Deleted: true}, nil)
	mc.EXPECT().GetAllPVs(gomock.Any()).Return(nil, "lvm pvs", bytes.Buffer{}, nil).AnyTimes()

	msg, reason, fatal := splitUnappliedFileDevices(
		r.extendFileDevicesIfNeeded(context.Background(), lvg, vg, nil, fileDeviceIssues{}))

	assert.NoError(t, fatal, "the Volume Group is intact; only this entry cannot be brought up")
	assert.Contains(t, msg, "unlinked")
	assert.Empty(t, reason, "no override: this is the plain not-applied case")
}

// An unreadable losetup is not evidence of anything. Proceeding matches what
// happened before the check existed, and the free-space guard and `--nooverlap` are
// still in front of the actual provisioning.
func TestProvisionFileDevices_ProceedsWhenTheRecordedLoopCannotBeRead(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)

	lvg, fd, path := unlinkedLVG()

	gomock.InOrder(
		mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), path).Return("losetup -j", "", nil),
		mc.EXPECT().GetLoopBackingFile(gomock.Any(), "/dev/loop4").
			Return("losetup -O BACK-FILE", internal.LoopBackingFile{}, errors.New("losetup: not a loop device")),
		mc.EXPECT().EnsureFileDeviceDirectory(gomock.Any(), fd.Directory).Return("mkdir -p", nil),
		mc.EXPECT().GetFileAllocatedBytes(gomock.Any(), path).Return("stat", int64(0), utils.ErrFileDeviceAbsent),
		mc.EXPECT().GetFilesystemSpace(gomock.Any(), fd.Directory).
			Return("stat -f", internal.FilesystemSpace{AvailableBytes: int64(1) << 50}, nil),
		mc.EXPECT().CreateFileDevice(gomock.Any(), path, fd.Size.Value()).Return("fallocate", nil),
		mc.EXPECT().SetupLoopDevice(gomock.Any(), path).Return("losetup --find", "/dev/loop9", nil),
		mc.EXPECT().SetLoopDirectIO(gomock.Any(), "/dev/loop9").Return("losetup --direct-io=on", nil),
	)

	loops, _, err := r.provisionFileDevices(context.Background(), lvg, fileDeviceIssues{}, true)
	assert.NoError(t, err)
	assert.Equal(t, []string{"/dev/loop9"}, loops)
}

// A loop minor is not stable, and the recorded one may since have been handed to
// something unrelated. Its backing file then does not carry this entry's name, and
// provisioning is right to go ahead.
func TestProvisionFileDevices_IgnoresARecordedLoopThatNowBacksSomethingElse(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)

	lvg, fd, path := unlinkedLVG()

	gomock.InOrder(
		mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), path).Return("losetup -j", "", nil),
		// The minor was reused for a completely different file, which happens to be
		// deleted too — but it is not ours.
		mc.EXPECT().GetLoopBackingFile(gomock.Any(), "/dev/loop4").
			Return("losetup -O BACK-FILE", internal.LoopBackingFile{Path: "/tmp/somebody-elses.img", Deleted: true}, nil),
		mc.EXPECT().EnsureFileDeviceDirectory(gomock.Any(), fd.Directory).Return("mkdir -p", nil),
		mc.EXPECT().GetFileAllocatedBytes(gomock.Any(), path).Return("stat", int64(0), utils.ErrFileDeviceAbsent),
		mc.EXPECT().GetFilesystemSpace(gomock.Any(), fd.Directory).
			Return("stat -f", internal.FilesystemSpace{AvailableBytes: int64(1) << 50}, nil),
		mc.EXPECT().CreateFileDevice(gomock.Any(), path, fd.Size.Value()).Return("fallocate", nil),
		mc.EXPECT().SetupLoopDevice(gomock.Any(), path).Return("losetup --find", "/dev/loop9", nil),
		mc.EXPECT().SetLoopDirectIO(gomock.Any(), "/dev/loop9").Return("losetup --direct-io=on", nil),
	)

	_, _, err := r.provisionFileDevices(context.Background(), lvg, fileDeviceIssues{}, true)
	assert.NoError(t, err)
}

// Deleting the resource must still take the loop down. `losetup -j` finds nothing
// once the inode is gone, so resolving from the file alone detached nothing and the
// minor outlived the LVMVolumeGroup that was the last record of it — nothing would
// ever collect it, since the reconciler is gone with the resource and the discoverer
// has no VG to import.
func TestCleanupFileDevices_DetachesALoopWhoseBackingFileWasUnlinked(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)

	lvg, _, path := unlinkedLVG()

	mc.EXPECT().GetAllPVs(gomock.Any()).Return(nil, "lvm pvs", bytes.Buffer{}, nil)
	// The file is gone, so the inode-based lookup reports nothing.
	mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), path).Return("losetup -j", "", nil)
	// The recorded loop is confirmed the other way round, and its unlinked backing
	// file still carries our owner pattern.
	mc.EXPECT().GetLoopBackingFile(gomock.Any(), "/dev/loop4").
		Return("losetup -O BACK-FILE", internal.LoopBackingFile{Path: path, Deleted: true}, nil)
	mc.EXPECT().DetachLoopDevice(gomock.Any(), "/dev/loop4").Return("losetup -d", nil)
	// `rm -f` on an already-missing file is a no-op the cleanup tolerates.
	mc.EXPECT().RemoveFileDevice(gomock.Any(), path).Return("rm -f", nil)

	assert.NoError(t, r.cleanupFileDevices(context.Background(), lvg))
}

// The fallback must not become a way to detach somebody else's minor. A loop the
// kernel has since handed to an unrelated file is left alone, however confidently
// status names it.
func TestCleanupFileDevices_LeavesARecordedLoopThatNowBacksSomethingElse(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)

	lvg, _, path := unlinkedLVG()

	mc.EXPECT().GetAllPVs(gomock.Any()).Return(nil, "lvm pvs", bytes.Buffer{}, nil)
	mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), path).Return("losetup -j", "", nil)
	mc.EXPECT().GetLoopBackingFile(gomock.Any(), "/dev/loop4").
		Return("losetup -O BACK-FILE", internal.LoopBackingFile{Path: "/var/lib/libvirt/guest.qcow2"}, nil)
	// No DetachLoopDevice, no RemoveFileDevice: gomock fails the test on either.

	assert.NoError(t, r.cleanupFileDevices(context.Background(), lvg))
}

// A loop the file still points at costs no second confirmation: the ordinary delete
// path must not spend a losetup per target for a check it has already made.
func TestCleanupFileDevices_DoesNotReconfirmALoopResolvedFromItsFile(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)

	lvg, _, path := unlinkedLVG()

	mc.EXPECT().GetAllPVs(gomock.Any()).Return(nil, "lvm pvs", bytes.Buffer{}, nil)
	mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), path).Return("losetup -j", "/dev/loop4", nil)
	// No GetLoopBackingFile: the file named the loop, which is the stronger check.
	mc.EXPECT().DetachLoopDevice(gomock.Any(), "/dev/loop4").Return("losetup -d", nil)
	mc.EXPECT().RemoveFileDevice(gomock.Any(), path).Return("rm -f", nil)

	assert.NoError(t, r.cleanupFileDevices(context.Background(), lvg))
}

// cleanupFileDevices walks the union of spec and status, and the two spell the same
// file differently when `directory` contains a symlink: status carries the path
// losetup reported, with symlink components resolved, while the spec path is
// literal. Keyed by full path that is one file in two targets, and the second one
// then finds no loop, tries to `rm` an already removed file and warns about it.
func TestCleanupFileDevices_DeduplicatesSpecAndStatusByBasename(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)

	fd := v1alpha1.LVMVolumeGroupFileDeviceSpec{Name: "d10g", Directory: "/data", Size: resource.MustParse("10Gi")}
	// /data is a symlink to /mnt/disk1/data on the node, so losetup reported the
	// resolved path.
	resolved := "/mnt/disk1/data/" + "sds-vg-a." + fd.Name + ".img"
	lvg := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: v1.ObjectMeta{Name: "vg-a"},
		Spec:       v1alpha1.LVMVolumeGroupSpec{FileDevices: []v1alpha1.LVMVolumeGroupFileDeviceSpec{fd}},
		Status: v1alpha1.LVMVolumeGroupStatus{
			Nodes: []v1alpha1.LVMVolumeGroupNode{{
				Name:        "test_node",
				FileDevices: []v1alpha1.LVMVolumeGroupFileDevice{{Name: fd.Name, FilePath: resolved, LoopDevice: "/dev/loop4"}},
			}},
		},
	}

	// Exactly one target: one loop lookup, one detach, one rm. Every expectation is
	// Times(1), so a second pass over the same file fails the test.
	mc.EXPECT().GetAllPVs(gomock.Any()).Return(nil, "lvm pvs", bytes.Buffer{}, nil).Times(1)
	mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), resolved).Return("losetup -j", "/dev/loop4", nil).Times(1)
	mc.EXPECT().DetachLoopDevice(gomock.Any(), "/dev/loop4").Return("losetup -d", nil).Times(1)
	mc.EXPECT().RemoveFileDevice(gomock.Any(), resolved).Return("rm -f", nil).Times(1)

	assert.NoError(t, r.cleanupFileDevices(context.Background(), lvg))
}
