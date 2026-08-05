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
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/mock_utils"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/utils"
)

// ---=== cleanupFileDevices ===--- //

// expectNoLivePVs says the node has no Physical Volumes at all, which is what
// cleanupFileDevices reads to confirm none of the loops it is about to tear down
// still belongs to a Volume Group. Every cleanup test that expects work to
// happen needs it; a test that expects the cleanup to refuse should state the
// live PV explicitly instead.
func expectNoLivePVs(mc *mock_utils.MockCommands) {
	mc.EXPECT().GetAllPVs(gomock.Any()).Return(nil, "lvm pvs", bytes.Buffer{}, nil).AnyTimes()
}

// expectOrphanPV is the ordinary post-vgremove state: the loop is still a PV
// because vgremove does not pvremove, but it belongs to no Volume Group, so
// cleanup must go ahead.
func expectOrphanPV(mc *mock_utils.MockCommands, loop string) {
	mc.EXPECT().GetAllPVs(gomock.Any()).
		Return([]internal.PVData{{PVName: loop}}, "lvm pvs", bytes.Buffer{}, nil).AnyTimes()
}

// A status entry that lost its filePath leaves only a loop minor to go on.
// Minors are recycled by the kernel, so before detaching the agent must read
// the loop's backing file and confirm the file is its own. This is the last
// guard between a stale status entry and someone else's storage.
func TestCleanupFileDevices_RefusesLoopBackedByForeignFile(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)
	expectNoLivePVs(mc)

	lvg := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: v1.ObjectMeta{Name: "vg-a"},
		Status: v1alpha1.LVMVolumeGroupStatus{
			Nodes: []v1alpha1.LVMVolumeGroupNode{{
				Name:        "test_node",
				FileDevices: []v1alpha1.LVMVolumeGroupFileDevice{{LoopDevice: "/dev/loop9"}},
			}},
		},
	}

	// The minor now backs a file that is not ours: no detach, no rm.
	mc.EXPECT().GetLoopBackingFile(gomock.Any(), "/dev/loop9").
		Return("losetup -O BACK-FILE", internal.LoopBackingFile{Path: "/var/lib/someone-else/disk.img"}, nil)

	assert.NoError(t, r.cleanupFileDevices(context.Background(), lvg))
}

// Not being able to read the backing file is not permission to detach: the
// target is reported as an error so the LVG delete does not silently finish
// while node state is still unknown.
func TestCleanupFileDevices_LoopOnlyBackingFileLookupFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)
	expectNoLivePVs(mc)

	lvg := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: v1.ObjectMeta{Name: "vg-a"},
		Status: v1alpha1.LVMVolumeGroupStatus{
			Nodes: []v1alpha1.LVMVolumeGroupNode{{
				Name:        "test_node",
				FileDevices: []v1alpha1.LVMVolumeGroupFileDevice{{LoopDevice: "/dev/loop9"}},
			}},
		},
	}

	mc.EXPECT().GetLoopBackingFile(gomock.Any(), "/dev/loop9").
		Return("losetup -O BACK-FILE", internal.LoopBackingFile{}, errors.New("losetup: no such device"))

	err := r.cleanupFileDevices(context.Background(), lvg)
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "read backing file for /dev/loop9")
	}
}

// A loop-only target whose backing file IS ours must be detached — the mirror
// image of the refusal above, so the guard cannot be satisfied by never acting.
func TestCleanupFileDevices_DetachesLoopBackedByOwnFile(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)
	expectNoLivePVs(mc)

	ourFile := utils.BuildFileDevicePath("/data", "vg-a", "d10g")
	lvg := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: v1.ObjectMeta{Name: "vg-a"},
		Status: v1alpha1.LVMVolumeGroupStatus{
			Nodes: []v1alpha1.LVMVolumeGroupNode{{
				Name:        "test_node",
				FileDevices: []v1alpha1.LVMVolumeGroupFileDevice{{LoopDevice: "/dev/loop9"}},
			}},
		},
	}

	gomock.InOrder(
		mc.EXPECT().GetLoopBackingFile(gomock.Any(), "/dev/loop9").
			Return("losetup -O BACK-FILE", internal.LoopBackingFile{Path: ourFile}, nil),
		mc.EXPECT().DetachLoopDevice(gomock.Any(), "/dev/loop9").Return("losetup -d", nil),
	)
	// No RemoveFileDevice: the target carries no filePath, only a loop.

	assert.NoError(t, r.cleanupFileDevices(context.Background(), lvg))
}

// status and spec describe the same backing file from two angles — status
// knows the loop, spec knows the deterministic path. They must collapse into
// one target, otherwise the file is processed twice and the second pass
// detaches a loop that no longer exists.
func TestCleanupFileDevices_DeduplicatesSpecAndStatusTargets(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)
	expectNoLivePVs(mc)

	fd := v1alpha1.LVMVolumeGroupFileDeviceSpec{Name: "d10g", Directory: "/data", Size: resource.MustParse("10Gi")}
	path := utils.BuildFileDevicePath(fd.Directory, "vg-a", fd.Name)

	lvg := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: v1.ObjectMeta{Name: "vg-a"},
		Spec:       v1alpha1.LVMVolumeGroupSpec{FileDevices: []v1alpha1.LVMVolumeGroupFileDeviceSpec{fd}},
		Status: v1alpha1.LVMVolumeGroupStatus{
			Nodes: []v1alpha1.LVMVolumeGroupNode{{
				Name:        "test_node",
				FileDevices: []v1alpha1.LVMVolumeGroupFileDevice{{FilePath: path, LoopDevice: "/dev/loop4"}},
			}},
		},
	}

	// Exactly one pass: resolve the loop from the file, detach it, remove the file.
	gomock.InOrder(
		mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), path).Return("losetup -j", "/dev/loop4", nil),
		mc.EXPECT().DetachLoopDevice(gomock.Any(), "/dev/loop4").Return("losetup -d", nil),
		mc.EXPECT().RemoveFileDevice(gomock.Any(), path).Return("rm", nil),
	)

	assert.NoError(t, r.cleanupFileDevices(context.Background(), lvg))
}

// An `rm` that fails is not fatal — a file already removed by hand is
// harmless — but the loop must still have been detached first.
func TestCleanupFileDevices_RemoveFailureIsNotFatal(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)
	expectNoLivePVs(mc)

	fd := v1alpha1.LVMVolumeGroupFileDeviceSpec{Name: "d10g", Directory: "/data", Size: resource.MustParse("10Gi")}
	path := utils.BuildFileDevicePath(fd.Directory, "vg-a", fd.Name)
	lvg := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: v1.ObjectMeta{Name: "vg-a"},
		Spec:       v1alpha1.LVMVolumeGroupSpec{FileDevices: []v1alpha1.LVMVolumeGroupFileDeviceSpec{fd}},
	}

	gomock.InOrder(
		mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), path).Return("losetup -j", "/dev/loop4", nil),
		mc.EXPECT().DetachLoopDevice(gomock.Any(), "/dev/loop4").Return("losetup -d", nil),
		mc.EXPECT().RemoveFileDevice(gomock.Any(), path).Return("rm", errors.New("ENOENT")),
	)

	assert.NoError(t, r.cleanupFileDevices(context.Background(), lvg))
}

// ---=== rollbackProvisionedFileDevices ===--- //

// The rollback is destructive, so it may only run on authoritative data. If
// `pvs` cannot be listed the agent tears nothing down: a leaked loop is
// recoverable on the next reconcile, a PV whose backing file was deleted from
// under a live VG is not.
func TestRollbackProvisionedFileDevices_KeepsEverythingWhenPVListingFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)

	mc.EXPECT().GetAllPVs(gomock.Any()).
		Return(nil, "lvm pvs", bytes.Buffer{}, errors.New("lvm: command failed"))
	// No DetachLoopDevice, no RemoveFileDevice.

	r.rollbackProvisionedFileDevices(context.Background(), []provisionedFileDevice{
		{filePath: "/data/a.img", loopDev: "/dev/loop0"},
	})
}

// A loop that already became a PV belongs to the VG now — even if the step
// that reported failure came afterwards. Removing its backing file here is the
// observed "one file, two loops, VG silently doubles" corruption.
func TestRollbackProvisionedFileDevices_SkipsLoopThatIsAlreadyAPV(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)

	mc.EXPECT().GetAllPVs(gomock.Any()).
		Return([]internal.PVData{{PVName: "/dev/loop0"}}, "lvm pvs", bytes.Buffer{}, nil)
	// No teardown for /dev/loop0; /dev/loop1 is not a PV and must be rolled back.
	gomock.InOrder(
		mc.EXPECT().DetachLoopDevice(gomock.Any(), "/dev/loop1").Return("losetup -d", nil),
		mc.EXPECT().RemoveFileDevice(gomock.Any(), "/data/b.img").Return("rm", nil),
	)

	r.rollbackProvisionedFileDevices(context.Background(), []provisionedFileDevice{
		{filePath: "/data/a.img", loopDev: "/dev/loop0"},
		{filePath: "/data/b.img", loopDev: "/dev/loop1"},
	})
}

// The same protection has to work when lvm reports the PV under a
// /dev/disk or /dev/block alias instead of /dev/loopN.
func TestRollbackProvisionedFileDevices_SkipsLoopRegisteredUnderAlias(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)
	r.resolver = func(_ context.Context, path string) (string, error) {
		if path == "/dev/disk/by-id/loop-a" {
			return "/dev/loop0", nil
		}
		return path, nil
	}

	mc.EXPECT().GetAllPVs(gomock.Any()).
		Return([]internal.PVData{{PVName: "/dev/disk/by-id/loop-a"}}, "lvm pvs", bytes.Buffer{}, nil)
	// Nothing is torn down: the alias resolves to the loop we provisioned.

	r.rollbackProvisionedFileDevices(context.Background(), []provisionedFileDevice{
		{filePath: "/data/a.img", loopDev: "/dev/loop0"},
	})
}

// An unresolvable alias is treated as "possibly ours": the rollback is skipped
// rather than risk detaching a live PV on a guess.
func TestRollbackProvisionedFileDevices_SkipsWhenAliasCannotBeResolved(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)
	r.resolver = func(context.Context, string) (string, error) {
		return "", errors.New("readlink failed")
	}

	mc.EXPECT().GetAllPVs(gomock.Any()).
		Return([]internal.PVData{{PVName: "/dev/block/7:0"}}, "lvm pvs", bytes.Buffer{}, nil)

	r.rollbackProvisionedFileDevices(context.Background(), []provisionedFileDevice{
		{filePath: "/data/a.img", loopDev: "/dev/loop0"},
	})
}

// A loop that could not be detached still holds the file open, so the file
// must survive: removing it would leave a PV backed by a deleted inode.
func TestRollbackProvisionedFileDevices_KeepsFileWhenDetachFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)

	mc.EXPECT().GetAllPVs(gomock.Any()).Return(nil, "lvm pvs", bytes.Buffer{}, nil)
	mc.EXPECT().DetachLoopDevice(gomock.Any(), "/dev/loop0").Return("losetup -d", errors.New("device busy"))
	// No RemoveFileDevice.

	r.rollbackProvisionedFileDevices(context.Background(), []provisionedFileDevice{
		{filePath: "/data/a.img", loopDev: "/dev/loop0"},
	})
}

// ---=== loopPVState ===--- //

// Canonical /dev/loopN PVs are matched by name upstream; this helper only
// exists for aliases, so plain names must be skipped without a resolver call.
func TestLoopPVState_IgnoresNonAliasPVs(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	r := reconcilerWithMockedCommands(t, mock_utils.NewMockCommands(ctrl))

	resolverCalls := 0
	r.resolver = func(_ context.Context, path string) (string, error) {
		resolverCalls++
		return path, nil
	}

	state := r.loopPVState(context.Background(), newAliasCache(), "/dev/loop0",
		[]internal.PVData{{PVName: "/dev/sda1"}, {PVName: "/dev/loop7"}})

	assert.Equal(t, pvLoopStateAbsent, state, "every alias was accounted for, so the answer is a confirmed no")
	assert.Zero(t, resolverCalls, "non-alias PV names must not be resolved")
}

// An alias that resolves to the loop is a confirmed match, and it must win over
// an unresolvable alias listed after it — a positive answer is never downgraded
// to "unknown".
func TestLoopPVState_ResolvedAliasIsAConfirmedMatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	r := reconcilerWithMockedCommands(t, mock_utils.NewMockCommands(ctrl))

	r.resolver = func(_ context.Context, path string) (string, error) {
		if path == "/dev/disk/by-id/dm-name-broken" {
			return "", errors.New("readlink failed")
		}
		return "/dev/loop0", nil
	}

	state := r.loopPVState(context.Background(), newAliasCache(), "/dev/loop0", []internal.PVData{
		{PVName: "/dev/block/7:0"},
		{PVName: "/dev/disk/by-id/dm-name-broken"},
	})

	assert.Equal(t, pvLoopStateRegistered, state)
}

// An unresolvable alias is not a verdict. It must come back as Unknown rather
// than as a confirmed absence: pvcreate over a device that may already be a PV
// fails and wedges VGConfigurationApplied, and a rollback would destroy live
// storage.
func TestLoopPVState_UnresolvableAliasIsUnknown(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	r := reconcilerWithMockedCommands(t, mock_utils.NewMockCommands(ctrl))

	r.resolver = func(_ context.Context, _ string) (string, error) {
		return "", errors.New("nsenter: no such binary")
	}

	state := r.loopPVState(context.Background(), newAliasCache(), "/dev/loop0",
		[]internal.PVData{{PVName: "/dev/disk/by-id/whatever"}})

	assert.Equal(t, pvLoopStateUnknown, state)
}

// The rollback is destructive, so Unknown has to keep the device just as a
// confirmed PV does.
func TestRollbackProvisionedFileDevices_KeepsALoopWhoseAliasCannotBeResolved(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)

	// The loop is not listed under its canonical name, and the one alias PV in
	// the listing cannot be canonicalized.
	mc.EXPECT().GetAllPVs(gomock.Any()).Return(
		[]internal.PVData{{PVName: "/dev/disk/by-id/whatever", VGName: "vg-a"}}, "lvm pvs", bytes.Buffer{}, nil)
	r.resolver = func(_ context.Context, _ string) (string, error) {
		return "", errors.New("readlink failed")
	}
	// Neither a detach nor an rm may be issued.

	r.rollbackProvisionedFileDevices(context.Background(), []provisionedFileDevice{
		{filePath: "/data/a.img", loopDev: "/dev/loop0"},
	})
}

// pvcreate skips on Unknown for the same reason, and nothing else about the
// create flow may run over a device whose state is unknown.
func TestCreatePVIfNeeded_SkipsPVCreateWhenAnAliasCannotBeResolved(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)

	r.resolver = func(_ context.Context, _ string) (string, error) {
		return "", errors.New("readlink failed")
	}
	// No CreatePV expectation: issuing it is the bug.

	view := &pvView{
		pvs:     []internal.PVData{{PVName: "/dev/disk/by-id/whatever"}},
		names:   map[string]struct{}{"/dev/disk/by-id/whatever": {}},
		aliases: newAliasCache(),
	}

	assert.NoError(t, r.createPVIfNeeded(context.Background(), view, "test", "/dev/loop0"))
}

// ---=== provisionFileDevices error paths ===--- //

// A directory that cannot be created (read-only filesystem, a file where a
// directory is expected) aborts the provision before anything is allocated.
func TestProvisionFileDevices_DirectoryCreationFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)

	fd := v1alpha1.LVMVolumeGroupFileDeviceSpec{Name: "d10g", Directory: "/data", Size: resource.MustParse("10Gi")}
	lvg := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: v1.ObjectMeta{Name: "vg-a"},
		Spec:       v1alpha1.LVMVolumeGroupSpec{FileDevices: []v1alpha1.LVMVolumeGroupFileDeviceSpec{fd}},
	}
	path := utils.BuildFileDevicePath(fd.Directory, "vg-a", fd.Name)

	gomock.InOrder(
		mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), path).Return("losetup -j", "", nil),
		mc.EXPECT().EnsureFileDeviceDirectory(gomock.Any(), fd.Directory).
			Return("mkdir -p", errors.New("read-only file system")),
	)
	// Nothing was created, so nothing is rolled back.

	_, _, err := r.provisionFileDevices(context.Background(), lvg, fileDeviceIssues{}, false)
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "read-only file system")
	}
}

// If the existing-loop probe itself fails the agent must not fall through to
// fallocate: it cannot tell whether a loop is already attached, and attaching
// a second one is exactly the leak the probe exists to prevent.
func TestProvisionFileDevices_LoopProbeFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)

	fd := v1alpha1.LVMVolumeGroupFileDeviceSpec{Name: "d10g", Directory: "/data", Size: resource.MustParse("10Gi")}
	lvg := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: v1.ObjectMeta{Name: "vg-a"},
		Spec:       v1alpha1.LVMVolumeGroupSpec{FileDevices: []v1alpha1.LVMVolumeGroupFileDeviceSpec{fd}},
	}
	path := utils.BuildFileDevicePath(fd.Directory, "vg-a", fd.Name)

	mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), path).
		Return("losetup -j", "", errors.New("losetup unavailable"))

	_, _, err := r.provisionFileDevices(context.Background(), lvg, fileDeviceIssues{}, false)
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "query loop for "+path)
	}
}

// When fallocate itself fails the rollback leaves the path alone: no loop was
// attached, and any partially written file is reclaimed by the next reconcile,
// which re-runs fallocate over the same deterministic path. Removing it here
// would only add an rm that can fail on its own.
func TestProvisionFileDevices_FileCreationFailsWithoutRemovingThePath(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)

	fd := v1alpha1.LVMVolumeGroupFileDeviceSpec{Name: "d10g", Directory: "/data", Size: resource.MustParse("10Gi")}
	lvg := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: v1.ObjectMeta{Name: "vg-a"},
		Spec:       v1alpha1.LVMVolumeGroupSpec{FileDevices: []v1alpha1.LVMVolumeGroupFileDeviceSpec{fd}},
	}
	path := utils.BuildFileDevicePath(fd.Directory, "vg-a", fd.Name)

	gomock.InOrder(
		mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), path).Return("losetup -j", "", nil),
		mc.EXPECT().EnsureFileDeviceDirectory(gomock.Any(), fd.Directory).Return("mkdir -p", nil),
		mc.EXPECT().GetFileAllocatedBytes(gomock.Any(), path).Return("stat -c %b %B", int64(0), utils.ErrFileDeviceAbsent),
		mc.EXPECT().GetFilesystemSpace(gomock.Any(), fd.Directory).Return("stat -f", internal.FilesystemSpace{AvailableBytes: int64(1) << 50}, nil),
		mc.EXPECT().CreateFileDevice(gomock.Any(), path, fd.Size.Value()).
			Return("fallocate", errors.New("ENOSPC")),
	)
	// Neither DetachLoopDevice nor RemoveFileDevice: nothing was attached and
	// the path is left for the next reconcile to re-allocate.

	_, _, err := r.provisionFileDevices(context.Background(), lvg, fileDeviceIssues{}, false)
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "ENOSPC")
	}
}

// A cancelled reconcile (SIGTERM, deadline) must stop before touching the node.
func TestProvisionFileDevices_StopsOnCancelledContext(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)

	lvg := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: v1.ObjectMeta{Name: "vg-a"},
		Spec: v1alpha1.LVMVolumeGroupSpec{FileDevices: []v1alpha1.LVMVolumeGroupFileDeviceSpec{
			{Name: "d10g", Directory: "/data", Size: resource.MustParse("10Gi")},
		}},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, err := r.provisionFileDevices(ctx, lvg, fileDeviceIssues{}, false)
	assert.ErrorIs(t, err, context.Canceled)
}

// An LVG with no fileDevices is a plain block-device VG: the whole path is a
// no-op, not an empty provisioning run.
func TestProvisionFileDevices_NoFileDevicesIsNoOp(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	r := reconcilerWithMockedCommands(t, mock_utils.NewMockCommands(ctrl))

	loops, provisioned, err := r.provisionFileDevices(context.Background(),
		&v1alpha1.LVMVolumeGroup{ObjectMeta: v1.ObjectMeta{Name: "vg-a"}}, fileDeviceIssues{}, false)

	assert.NoError(t, err)
	assert.Nil(t, loops)
	assert.Nil(t, provisioned)
}

func TestExtendFileDevicesIfNeeded_NoFileDevicesIsNoOp(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	r := reconcilerWithMockedCommands(t, mock_utils.NewMockCommands(ctrl))

	assert.NoError(t, r.extendFileDevicesIfNeeded(context.Background(),
		&v1alpha1.LVMVolumeGroup{ObjectMeta: v1.ObjectMeta{Name: "vg-a"}},
		internal.VGData{VGName: "vg-a"}, nil, fileDeviceIssues{}))
}

// ---=== the create-path guard ===--- //

// The cache is filled only by the scanner, which runs on udev events, and writing
// LVM metadata to a loop device does not reliably raise one. So after the agent
// creates a file-backed VG the cache can still hold a snapshot from before
// vgcreate. Taking the create path on that view is destructive: CreateVGComplex
// re-runs pvcreate on a device that is already a PV of the VG it means to create,
// LVM answers "without -ff", and the LVMVolumeGroup stays Pending for good.
func TestShouldReconcileLVGByCreateFunc_DoesNotRecreateAVGThatExistsOnTheNode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)

	lvg := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: v1.ObjectMeta{Name: "vg-a"},
		Spec:       v1alpha1.LVMVolumeGroupSpec{ActualVGNameOnTheNode: "e2e-vg-fd"},
	}

	// Cache empty, node says otherwise.
	mc.EXPECT().GetAllVGs(gomock.Any()).
		Return([]internal.VGData{{VGName: "e2e-vg-fd", VGUUID: "uuid-1"}}, "lvm vgs", bytes.Buffer{}, nil)
	// A block-backed VG: ownership is settled by the PV list alone, no losetup.
	mc.EXPECT().GetAllPVs(gomock.Any()).
		Return([]internal.PVData{{PVName: "/dev/sdb", VGUuid: "uuid-1", VGName: "e2e-vg-fd"}}, "lvm pvs", bytes.Buffer{}, nil)

	shouldCreate, vgStateKnown := r.shouldReconcileLVGByCreateFunc(context.Background(), lvg)
	assert.False(t, shouldCreate,
		"a VG present on the node must never be re-created just because the cache missed it")
	assert.True(t, vgStateKnown, "LVM answered, so the caller must not report this as an unreadable node")
}

// A Volume Group of the right name that lives entirely on loop devices the agent
// did not create is not ours, so it must not stand in the way of creating one.
//
// Before spec.fileDevices removed `loop` from LVMGlobalFilter such a VG was
// invisible to lvm.static. Taking it for ours has no way out: create is refused
// because "the VG is there", update finds nothing in the cache, and the resource
// sits in CacheStale forever while the condition points the operator at a cache
// that is not the problem.
func TestShouldReconcileLVGByCreateFunc_IgnoresAForeignLoopVGOfTheSameName(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)

	lvg := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: v1.ObjectMeta{Name: "vg-a"},
		Spec:       v1alpha1.LVMVolumeGroupSpec{ActualVGNameOnTheNode: "data"},
	}

	// `losetup -f /backup/node2-root.img` during a restore. The image carries the
	// managed tag, because this module used to manage the node it came from.
	mc.EXPECT().GetAllVGs(gomock.Any()).
		Return([]internal.VGData{{VGName: "data", VGUUID: "uuid-image", VGTags: "storage.deckhouse.io/enabled=true"}}, "lvm vgs", bytes.Buffer{}, nil)
	mc.EXPECT().GetAllPVs(gomock.Any()).
		Return([]internal.PVData{{PVName: "/dev/loop7", VGUuid: "uuid-image", VGName: "data"}}, "lvm pvs", bytes.Buffer{}, nil)
	mc.EXPECT().GetLoopBackingFile(gomock.Any(), "/dev/loop7").
		Return("losetup -O BACK-FILE", internal.LoopBackingFile{Path: "/backup/node2-root.img"}, nil)

	shouldCreate, vgStateKnown := r.shouldReconcileLVGByCreateFunc(context.Background(), lvg)
	assert.True(t, shouldCreate, "a foreign loop-backed VG must not block creating ours")
	assert.True(t, vgStateKnown)
}

// The PV list is what tells a foreign loop VG from ours, so failing to read it
// falls back to the safe answer: assume the Volume Group is there rather than
// create one over storage that may well be.
func TestShouldReconcileLVGByCreateFunc_FailsSafeWhenPVsCannotBeListed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)

	lvg := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: v1.ObjectMeta{Name: "vg-a"},
		Spec:       v1alpha1.LVMVolumeGroupSpec{ActualVGNameOnTheNode: "e2e-vg-fd"},
	}

	mc.EXPECT().GetAllVGs(gomock.Any()).
		Return([]internal.VGData{{VGName: "e2e-vg-fd", VGUUID: "uuid-1"}}, "lvm vgs", bytes.Buffer{}, nil)
	mc.EXPECT().GetAllPVs(gomock.Any()).
		Return(nil, "lvm pvs", bytes.Buffer{}, errors.New("lvm unavailable"))

	shouldCreate, vgStateKnown := r.shouldReconcileLVGByCreateFunc(context.Background(), lvg)
	assert.False(t, shouldCreate)
	assert.False(t, vgStateKnown, "an unreadable node must not be reported as a confirmed VG")
}

// A VG that is genuinely absent everywhere still has to be created, otherwise the
// guard would block the feature outright.
func TestShouldReconcileLVGByCreateFunc_CreatesWhenTheVGIsAbsentEverywhere(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)

	lvg := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: v1.ObjectMeta{Name: "vg-a"},
		Spec:       v1alpha1.LVMVolumeGroupSpec{ActualVGNameOnTheNode: "e2e-vg-fd"},
	}

	mc.EXPECT().GetAllVGs(gomock.Any()).
		Return([]internal.VGData{{VGName: "someone-else", VGUUID: "uuid-2"}}, "lvm vgs", bytes.Buffer{}, nil)

	shouldCreate, vgStateKnown := r.shouldReconcileLVGByCreateFunc(context.Background(), lvg)
	assert.True(t, shouldCreate)
	assert.True(t, vgStateKnown)
}

// If LVM cannot be consulted the answer is "it exists": a create run against
// storage that may well be there is destructive, whereas waiting is not.
func TestShouldReconcileLVGByCreateFunc_FailsSafeWhenLVMCannotBeQueried(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)

	lvg := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: v1.ObjectMeta{Name: "vg-a"},
		Spec:       v1alpha1.LVMVolumeGroupSpec{ActualVGNameOnTheNode: "e2e-vg-fd"},
	}

	mc.EXPECT().GetAllVGs(gomock.Any()).
		Return(nil, "lvm vgs", bytes.Buffer{}, errors.New("lvm unavailable"))

	shouldCreate, vgStateKnown := r.shouldReconcileLVGByCreateFunc(context.Background(), lvg)
	assert.False(t, shouldCreate)
	// The refusal has to be distinguishable from a confirmed "the VG is there":
	// the caller reports one as CacheStale and the other as VGCheckFailed, and
	// only the second one means the node itself is unreadable.
	assert.False(t, vgStateKnown, "an unreadable node must not be reported as a confirmed VG")
}

// A VG the cache already knows about must not cost an extra vgs call on every
// reconcile — the guard is only for the case where the cache says nothing.
func TestShouldReconcileLVGByCreateFunc_TrustsThePositiveCache(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)
	r.sdsCache.StoreVGs([]internal.VGData{{VGName: "e2e-vg-fd", VGUUID: "uuid-1"}}, bytes.Buffer{})

	lvg := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: v1.ObjectMeta{Name: "vg-a"},
		Spec:       v1alpha1.LVMVolumeGroupSpec{ActualVGNameOnTheNode: "e2e-vg-fd"},
	}

	// No GetAllVGs expectation: consulting LVM here would be pure overhead.
	shouldCreate, vgStateKnown := r.shouldReconcileLVGByCreateFunc(context.Background(), lvg)
	assert.False(t, shouldCreate)
	assert.True(t, vgStateKnown)
}

// ---=== runEventReconcile: the "nothing matched" branch ===--- //

// Reaching the default branch means the cache and the node disagree, and the
// reconcile has nothing it can safely do. Requeueing silently is what made this
// indistinguishable from an ordinary in-flight update: the resource kept saying
// Pending with no hint of why. Both ways of getting here now write a condition,
// and they must not write the same one — CacheStale clears itself, VGCheckFailed
// does not, and only the second means the node is unreadable.
func TestRunEventReconcile_ReportsWhyItHasNothingToDo(t *testing.T) {
	tests := []struct {
		name       string
		vgs        []internal.VGData
		vgsErr     error
		pvs        []internal.PVData
		wantReason string
	}{
		{
			name:       "the node has the VG the cache is missing",
			vgs:        []internal.VGData{{VGName: "data-vg", VGUUID: "uuid-1"}},
			pvs:        []internal.PVData{{PVName: "/dev/sdb", VGUuid: "uuid-1", VGName: "data-vg"}},
			wantReason: internal.ReasonCacheStale,
		},
		{
			name:       "the node cannot be read at all",
			vgsErr:     errors.New("lvm unavailable"),
			wantReason: internal.ReasonVGCheckFailed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mc := mock_utils.NewMockCommands(ctrl)
			r := reconcilerWithMockedCommands(t, mc)
			ctx := context.Background()

			lvg := &v1alpha1.LVMVolumeGroup{
				ObjectMeta: v1.ObjectMeta{Name: "vg-a"},
				Spec: v1alpha1.LVMVolumeGroupSpec{
					ActualVGNameOnTheNode: "data-vg",
					Type:                  internal.Local,
					Local:                 v1alpha1.LVMVolumeGroupLocalSpec{NodeName: "test_node"},
				},
			}
			require.NoError(t, r.cl.Create(ctx, lvg))

			// Exactly one vgs call: the answer is threaded out of
			// identifyLVGReconcileFunc rather than asked for twice.
			mc.EXPECT().GetAllVGs(gomock.Any()).
				Return(tt.vgs, "lvm vgs", bytes.Buffer{}, tt.vgsErr).Times(1)
			if len(tt.vgs) > 0 {
				// Only paid for when the name matched, which is the rare branch.
				mc.EXPECT().GetAllPVs(gomock.Any()).
					Return(tt.pvs, "lvm pvs", bytes.Buffer{}, nil).Times(1)
			}

			requeueAfter, err := r.runEventReconcile(ctx, lvg, nil)
			assert.NoError(t, err)
			assert.Positive(t, requeueAfter, "nothing else would wake this LVMVolumeGroup until the resync hours later")

			stored := &v1alpha1.LVMVolumeGroup{}
			require.NoError(t, r.cl.Get(ctx, client.ObjectKey{Name: lvg.Name}, stored))
			var got *v1.Condition
			for i := range stored.Status.Conditions {
				if stored.Status.Conditions[i].Type == internal.TypeVGConfigurationApplied {
					got = &stored.Status.Conditions[i]
				}
			}
			if assert.NotNil(t, got, "the branch must not stay silent") {
				assert.Equal(t, tt.wantReason, got.Reason)
				assert.Equal(t, v1.ConditionFalse, got.Status)
			}
		})
	}
}

// ---=== noProgressRequeueAfter: the retry has to slow down ===--- //

// An entry the node could not bring up is retried, because nothing else wakes
// the reconcile when the filesystem gains room. The same state also covers an
// entry that will never fit — a mistyped `size`, or an LVMVolumeGroupSet template
// rolled out to a fleet — and at a fixed VolumeGroupScanInterval (5s by default)
// that is every affected node running a `stat -f`, a `losetup -j` per entry and a
// live `lvm pvs` every five seconds, forever, over a request that cannot be
// satisfied. Nothing distinguishes the two from the inside, so the interval backs
// off to a ceiling instead.
func TestFileDeviceRequeueAfter(t *testing.T) {
	newReconciler := func(t *testing.T, scanInterval time.Duration) *Reconciler {
		t.Helper()
		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)
		r := reconcilerWithMockedCommands(t, mock_utils.NewMockCommands(ctrl))
		r.cfg.VolumeGroupScanInterval = scanInterval
		return r
	}

	t.Run("the interval grows, stays bounded and settles", func(t *testing.T) {
		r := newReconciler(t, time.Second)

		first := r.noProgressRequeueAfter("vg-a")
		assert.Equal(t, time.Second, first, "the first retry keeps the ordinary cadence")

		second := r.noProgressRequeueAfter("vg-a")
		assert.Greater(t, second, first, "a repeated failure must be retried less eagerly")

		// Far more rounds than the shift cap, to prove the growth is bounded rather
		// than merely slow: an unbounded shift reaches 1<<64, which is zero, and a
		// zero RequeueAfter means "do not requeue" — the retry would vanish exactly
		// when it had been failing longest.
		prev, last := second, time.Duration(0)
		for range 100 {
			last = r.noProgressRequeueAfter("vg-a")
			assert.Positive(t, last, "a retry interval must never collapse to zero")
			assert.LessOrEqual(t, last, noProgressRetryMaxInterval, "the backoff must stay under the ceiling")
			assert.GreaterOrEqual(t, last, prev, "the backoff must never shrink while the entry keeps failing")
			prev = last
		}
		// Bounded by the shift cap rather than by the ceiling here, because a 1s base
		// reaches 1s<<6 first. Either bound is fine; what must not happen is growth
		// that continues until the shift overflows.
		assert.Equal(t, time.Second<<noProgressRetryMaxShift, last, "the backoff must settle")
	})

	t.Run("the production cadence reaches the ceiling", func(t *testing.T) {
		// The default scan interval is 5s, and 5s<<6 is over the ceiling, so this is
		// the path an operator actually sees: a few minutes of quickening retries and
		// then a steady five-minute poll.
		r := newReconciler(t, defaultRequeueInterval)

		var last time.Duration
		for range 50 {
			last = r.noProgressRequeueAfter("vg-a")
		}
		assert.Equal(t, noProgressRetryMaxInterval, last)
	})

	t.Run("a round that applied everything clears the backoff", func(t *testing.T) {
		r := newReconciler(t, time.Second)

		for range 5 {
			r.noProgressRequeueAfter("vg-a")
		}
		assert.Greater(t, r.noProgressRequeueAfter("vg-a"), time.Second)

		// A filesystem that was briefly short of room must not leave the
		// LVMVolumeGroup on a long interval once the entry is applied.
		r.resetNoProgressRetries("vg-a")
		assert.Equal(t, time.Second, r.noProgressRequeueAfter("vg-a"))
	})

	t.Run("the backoff is per LVMVolumeGroup", func(t *testing.T) {
		r := newReconciler(t, time.Second)

		for range 5 {
			r.noProgressRequeueAfter("vg-a")
		}
		assert.Equal(t, time.Second, r.noProgressRequeueAfter("vg-b"),
			"one wedged LVMVolumeGroup must not slow down another's first retry")
	})

	t.Run("an unset scan interval still yields a usable interval", func(t *testing.T) {
		// A zero RequeueAfter means "do not requeue" to controller-runtime, so a
		// Config without VolumeGroupScanInterval must not turn a deliberate retry
		// into a silent drop.
		r := newReconciler(t, 0)
		assert.Equal(t, defaultRequeueInterval, r.noProgressRequeueAfter("vg-a"))
	})

	// The backoff is shared with the other state that does not clear itself:
	// ReasonVGCheckFailed, where the agent cannot read the node's Volume Groups at
	// all. A missing lvm.static or a broken nsenter is not a question whose answer
	// changes in five seconds, and polling it at the scan interval has every
	// affected node re-running `vgs` — plus a `pvs` and a `losetup` per loop PV via
	// vgExistsOnNode — forever. Its sibling, ReasonCacheStale, DOES clear itself on
	// the next scan and deliberately keeps the flat interval.
	t.Run("the streak is shared with an unreadable node and cleared by progress", func(t *testing.T) {
		r := newReconciler(t, time.Second)

		// Four rounds of "cannot read the node's VGs".
		for i := 0; i < 4; i++ {
			r.noProgressRequeueAfter("vg-a")
		}
		assert.Greater(t, r.noProgressRequeueAfter("vg-a"), time.Second,
			"a node that cannot be read must not keep being polled at the scan interval")

		// The moment the answer becomes knowable — the cache merely lagging, say —
		// the streak has to go, or the next genuine problem starts mid-escalation.
		r.resetNoProgressRetries("vg-a")
		assert.Equal(t, time.Second, r.noProgressRequeueAfter("vg-a"))
	})
}
