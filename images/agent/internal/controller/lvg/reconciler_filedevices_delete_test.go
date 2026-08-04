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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/mock_utils"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/utils"
)

// deletingFileBackedLVG puts a file-backed LVMVolumeGroup into the cluster and
// marks it for deletion the way the apiserver does: a Delete against an object
// carrying a finalizer sets deletionTimestamp and leaves the object in place.
func deletingFileBackedLVG(t *testing.T, cl client.Client) *v1alpha1.LVMVolumeGroup {
	t.Helper()

	lvg := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: v1.ObjectMeta{
			Name:       "vg-a",
			Finalizers: []string{internal.SdsNodeConfiguratorFinalizer},
		},
		Spec: v1alpha1.LVMVolumeGroupSpec{
			ActualVGNameOnTheNode: "data-vg",
			Type:                  internal.Local,
			Local:                 v1alpha1.LVMVolumeGroupLocalSpec{NodeName: "test_node"},
			FileDevices: []v1alpha1.LVMVolumeGroupFileDeviceSpec{
				{Name: "d10g", Directory: "/data", Size: resource.MustParse("10Gi")},
			},
		},
	}

	ctx := context.Background()
	require.NoError(t, cl.Create(ctx, lvg))
	require.NoError(t, cl.Delete(ctx, lvg))
	require.NoError(t, cl.Get(ctx, client.ObjectKey{Name: lvg.Name}, lvg))
	require.NotNil(t, lvg.DeletionTimestamp, "the fake client must keep a finalized object around with a deletionTimestamp")

	return lvg
}

// expectVGAbsentOnNode makes the live confirmation deleteLVGIfNeeded now runs
// agree with the empty cache: LVM says the Volume Group is not there either, so
// "it was never created" is an established fact rather than a cache artefact and
// the backing files may be removed.
func expectVGAbsentOnNode(mc *mock_utils.MockCommands) {
	mc.EXPECT().GetAllVGs(gomock.Any()).Return(nil, "lvm vgs", bytes.Buffer{}, nil).AnyTimes()
}

// A Volume Group missing from the cache does not mean nothing was provisioned on
// the node. File devices come up before the VG is assembled, and when pvcreate
// succeeded while vgcreate did not, rollbackProvisionedFileDevices deliberately
// keeps the loop and its backing file rather than tearing down something that is
// already a PV.
//
// The LVMVolumeGroup is then the only record of what is on the node, so dropping
// its finalizer without cleaning up strands a preallocated file, a loop minor and
// an orphan PV that nothing collects: the reconciler goes away with the resource
// and the discoverer has no VG to import. Deleting an LVMVolumeGroup stuck in
// VGCreationFailed is exactly what an operator does about it, and repeating that
// fills the node's filesystem — the DiskPressure eviction the free-space guard
// exists to prevent.
func TestDeleteLVGIfNeeded_CleansUpFileDevicesWhenTheVGWasNeverCreated(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)
	expectNoLivePVs(mc)
	expectVGAbsentOnNode(mc)

	lvg := deletingFileBackedLVG(t, r.cl)
	path := utils.BuildFileDevicePath("/data", lvg.Name, "d10g")

	gomock.InOrder(
		// The loop is re-resolved from the backing file rather than trusted from
		// status, so a recycled minor is never detached by mistake.
		mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), path).Return("losetup -j", "/dev/loop3", nil),
		mc.EXPECT().DetachLoopDevice(gomock.Any(), "/dev/loop3").Return("losetup -d", nil),
		mc.EXPECT().RemoveFileDevice(gomock.Any(), path).Return("rm -f", nil),
	)

	// The return value is deliberately not asserted. Dropping the last finalizer
	// of an object that already carries a deletionTimestamp is what makes the
	// apiserver delete it, so the DeleteLVMVolumeGroup call that follows races
	// with a deletion that has already happened and can come back NotFound. That
	// behaviour predates file devices and is the same for a block-device-only
	// LVMVolumeGroup; what matters here is that the cleanup ran BEFORE the
	// finalizer was released, which the ordered mock and the check below assert.
	//
	// waitForCache is asserted, though: LVM confirmed the Volume Group is not on
	// the node, so this is the branch that cleans up rather than the one that
	// waits, and getting that backwards would make the whole test vacuous.
	_, waitForCache, _ := r.deleteLVGIfNeeded(context.Background(), lvg)
	assert.False(t, waitForCache)

	err := r.cl.Get(context.Background(), client.ObjectKey{Name: lvg.Name}, &v1alpha1.LVMVolumeGroup{})
	assert.True(t, apierrors.IsNotFound(err), "the finalizer must be released once the file devices are gone, got %v", err)
}

// The finalizer is what keeps the resource — and with it the record of the
// backing file's path — alive. A cleanup that could not finish must therefore
// leave it in place and retry, not release the resource and lose the only
// pointer to what is still allocated on the node.
func TestDeleteLVGIfNeeded_KeepsTheFinalizerWhenCleanupFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)
	expectNoLivePVs(mc)
	expectVGAbsentOnNode(mc)

	lvg := deletingFileBackedLVG(t, r.cl)
	path := utils.BuildFileDevicePath("/data", lvg.Name, "d10g")

	mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), path).Return("losetup -j", "", errors.New("losetup: cannot open /dev/loop-control"))

	deleted, _, err := r.deleteLVGIfNeeded(context.Background(), lvg)
	assert.Error(t, err)
	assert.False(t, deleted)

	stored := &v1alpha1.LVMVolumeGroup{}
	require.NoError(t, r.cl.Get(context.Background(), client.ObjectKey{Name: lvg.Name}, stored))
	assert.Contains(t, stored.Finalizers, internal.SdsNodeConfiguratorFinalizer,
		"the finalizer must survive so the reconcile is retried instead of stranding the backing file")
}

// A block-device-only LVMVolumeGroup names no file device in either spec or
// status, so the cleanup walks an empty set and must not touch the node beyond
// the one `vgs` that confirms the Volume Group really is gone. In particular it
// must not spend an `lvm pvs` — that listing only exists to protect backing
// files, and there are none here.
func TestDeleteLVGIfNeeded_RunsNoHostCommandsForABlockOnlyLVG(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	// A strict mock fails the test on any call that is not expected below.
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)
	expectVGAbsentOnNode(mc)

	lvg := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: v1.ObjectMeta{
			Name:       "vg-block",
			Finalizers: []string{internal.SdsNodeConfiguratorFinalizer},
		},
		Spec: v1alpha1.LVMVolumeGroupSpec{
			ActualVGNameOnTheNode: "data-vg",
			Type:                  internal.Local,
			Local:                 v1alpha1.LVMVolumeGroupLocalSpec{NodeName: "test_node"},
		},
	}
	ctx := context.Background()
	require.NoError(t, r.cl.Create(ctx, lvg))
	require.NoError(t, r.cl.Delete(ctx, lvg))
	require.NoError(t, r.cl.Get(ctx, client.ObjectKey{Name: lvg.Name}, lvg))

	// See the note above about the trailing delete; the assertion that carries
	// the meaning here is the strict mock, which fails on any host command.
	_, waitForCache, _ := r.deleteLVGIfNeeded(ctx, lvg)
	assert.False(t, waitForCache, "the Volume Group is absent on the node, so there is no cache to wait for")

	err := r.cl.Get(ctx, client.ObjectKey{Name: lvg.Name}, &v1alpha1.LVMVolumeGroup{})
	assert.True(t, apierrors.IsNotFound(err), "expected the finalizer to be released, got %v", err)
}

// The cache is filled only on udev events, and writing LVM metadata to a loop
// device does not reliably raise one — which is why shouldReconcileLVGByCreateFunc
// confirms against LVM before it takes the create path. Deletion needs the same
// confirmation for a stronger reason: create wedges a condition, this branch
// unlinks backing files and releases the finalizer, so acting on a cache that has
// merely not caught up destroys a live Volume Group and then deletes the only
// record of it.
func TestDeleteLVGIfNeeded_RefusesToCleanUpWhenTheVGIsOnlyMissingFromTheCache(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)

	lvg := deletingFileBackedLVG(t, r.cl)

	// The cache says nothing; LVM says the Volume Group is right there.
	mc.EXPECT().GetAllVGs(gomock.Any()).
		Return([]internal.VGData{{VGName: "data-vg", VGUUID: "uuid-1"}}, "lvm vgs", bytes.Buffer{}, nil)
	// The PV list settles whether that Volume Group is the module's own or a
	// loop-backed stranger of the same name; a block PV needs no losetup.
	mc.EXPECT().GetAllPVs(gomock.Any()).
		Return([]internal.PVData{{PVName: "/dev/sdb", VGUuid: "uuid-1", VGName: "data-vg"}}, "lvm pvs", bytes.Buffer{}, nil)
	// No losetup and no rm: the strict mock fails the test if the cleanup runs.

	deleted, _, err := r.deleteLVGIfNeeded(context.Background(), lvg)
	assert.NoError(t, err)
	assert.False(t, deleted, "the resource must survive so the ordinary delete path can remove the VG first")

	stored := &v1alpha1.LVMVolumeGroup{}
	require.NoError(t, r.cl.Get(context.Background(), client.ObjectKey{Name: lvg.Name}, stored))
	assert.Contains(t, stored.Finalizers, internal.SdsNodeConfiguratorFinalizer)
}

// An unreadable node is treated as "the Volume Group exists", for the same
// reason the create path does: nothing may be destroyed on a guess.
func TestDeleteLVGIfNeeded_RefusesToCleanUpWhenLVMCannotBeQueried(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)

	lvg := deletingFileBackedLVG(t, r.cl)

	mc.EXPECT().GetAllVGs(gomock.Any()).
		Return(nil, "lvm vgs", bytes.Buffer{}, errors.New("lvm unavailable"))

	deleted, _, err := r.deleteLVGIfNeeded(context.Background(), lvg)
	assert.NoError(t, err)
	assert.False(t, deleted)

	stored := &v1alpha1.LVMVolumeGroup{}
	require.NoError(t, r.cl.Get(context.Background(), client.ObjectKey{Name: lvg.Name}, stored))
	assert.Contains(t, stored.Finalizers, internal.SdsNodeConfiguratorFinalizer)
}

// The confinement that keeps a mistyped `directory` from filling an arbitrary
// host path is enforced in validateFileDevice, and that only gates provisioning:
// the rejected entry is marked in fileDeviceIssues and never given to
// fallocate/losetup. Cleanup walks the whole spec with no such filter, and its
// only guard — IsManagedFileDevicePath — matches on the basename alone.
//
// So without this check, deleting an LVMVolumeGroup that never passed validation
// (the ordinary outcome of a typo in `directory`) hands the composed path to
// `losetup -j`, and then to `losetup -d` + `rm -f` if anything happens to sit
// there under a matching name. Nothing outside the base directory can have been
// created by this agent, so there is nothing to clean up for such an entry
// either way.
func TestCleanupFileDevices_SkipsEntriesOutsideTheBaseDirectory(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)
	r.cfg.FileDevicesDirectory = "/opt/deckhouse/sds/file-devices"

	lvg := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: v1.ObjectMeta{Name: "vg-a"},
		Spec: v1alpha1.LVMVolumeGroupSpec{
			ActualVGNameOnTheNode: "data-vg",
			FileDevices: []v1alpha1.LVMVolumeGroupFileDeviceSpec{
				{Name: "outside", Directory: "/var/lib/libvirt/images", Size: resource.MustParse("1Gi")},
				{Name: "escaped", Directory: "/opt/deckhouse/sds/file-devices/../../..", Size: resource.MustParse("1Gi")},
			},
		},
	}

	// Not a single host command: with every entry skipped there is nothing to
	// clean up, so cleanup must not even pay for the `lvm pvs` it gates on. Any
	// call at all here is the bug — a mocked Commands with no EXPECT fails the
	// test on the first invocation.
	assert.NoError(t, r.cleanupFileDevices(context.Background(), lvg))
}

// The counterpart: an entry inside the base directory is still cleaned up, so the
// guard above cannot be satisfied by simply doing nothing.
func TestCleanupFileDevices_ActsOnEntriesInsideTheBaseDirectory(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)
	r.cfg.FileDevicesDirectory = "/opt/deckhouse/sds/file-devices"

	base := "/opt/deckhouse/sds/file-devices/sub"
	lvg := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: v1.ObjectMeta{Name: "vg-a"},
		Spec: v1alpha1.LVMVolumeGroupSpec{
			ActualVGNameOnTheNode: "data-vg",
			FileDevices: []v1alpha1.LVMVolumeGroupFileDeviceSpec{
				{Name: "inside", Directory: base, Size: resource.MustParse("1Gi")},
			},
		},
	}
	path := utils.BuildFileDevicePath(base, "vg-a", "inside")

	// vgremove does not pvremove, so on the ordinary delete path the loop is an
	// orphan PV: present in the listing with no VG name, which is what lets
	// cleanup proceed.
	mc.EXPECT().GetAllPVs(gomock.Any()).Return([]internal.PVData{{PVName: "/dev/loop0"}}, "lvm pvs ...", bytes.Buffer{}, nil)
	gomock.InOrder(
		mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), path).Return("losetup -j ...", "/dev/loop0", nil),
		mc.EXPECT().DetachLoopDevice(gomock.Any(), "/dev/loop0").Return("losetup -d ...", nil),
		mc.EXPECT().RemoveFileDevice(gomock.Any(), path).Return("rm -f ...", nil),
	)

	assert.NoError(t, r.cleanupFileDevices(context.Background(), lvg))
}

// A Volume Group the cache does not know about but the node does is not a
// Volume Group that was never created, and every delete decision below this
// point would be made on that wrong answer.
//
// Falling through to the ordinary delete path is not a safer route to the same
// place, because it reads the same stale cache at every step: getLVForVG finds
// no logical volumes and waves the resource past the "delete used LVs first"
// guard, deleteVGIfExist finds no Volume Group and reports success without doing
// anything, and for an LVMVolumeGroup with no file devices there is nothing for
// cleanupFileDevices to walk. The finalizer would come off and the resource would
// be deleted while its Volume Group, and whatever is on it, stayed on the node
// with no owner.
//
// So the reconcile stops here and is requeued, and it says why on the condition.
func TestDeleteLVGIfNeeded_WaitsForTheCacheWhenTheVGIsOnTheNode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)

	lvg := deletingFileBackedLVG(t, r.cl)

	// LVM says the Volume Group is there, on a plain block device, so it is
	// unambiguously the module's own and unambiguously not gone.
	mc.EXPECT().GetAllVGs(gomock.Any()).
		Return([]internal.VGData{{VGName: lvg.Spec.ActualVGNameOnTheNode, VGUUID: "vg-uuid-1"}}, "lvm vgs", bytes.Buffer{}, nil)
	mc.EXPECT().GetAllPVs(gomock.Any()).
		Return([]internal.PVData{{PVName: "/dev/sdb", VGName: lvg.Spec.ActualVGNameOnTheNode, VGUuid: "vg-uuid-1"}}, "lvm pvs", bytes.Buffer{}, nil)
	// Nothing is detached, nothing is removed: no FindLoopDeviceByFile,
	// no DetachLoopDevice, no RemoveFileDevice is expected, and the mock
	// controller fails the test if any of them is called.

	deleted, waitForCache, err := r.deleteLVGIfNeeded(context.Background(), lvg)
	assert.NoError(t, err)
	assert.False(t, deleted)
	assert.True(t, waitForCache, "the caller must requeue instead of running the delete path on a stale cache")

	stored := &v1alpha1.LVMVolumeGroup{}
	require.NoError(t, r.cl.Get(context.Background(), client.ObjectKey{Name: lvg.Name}, stored))
	assert.Contains(t, stored.Finalizers, internal.SdsNodeConfiguratorFinalizer,
		"the finalizer is the only record of what is on the node while the cache catches up")

	var applied *v1.Condition
	for i := range stored.Status.Conditions {
		if stored.Status.Conditions[i].Type == internal.TypeVGConfigurationApplied {
			applied = &stored.Status.Conditions[i]
		}
	}
	require.NotNil(t, applied, "waiting silently is indistinguishable from an update in flight")
	assert.Equal(t, internal.ReasonCacheStale, applied.Reason)
}
