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

const growDir = "/opt/deckhouse/sds/file-devices"

// growLVG builds an LVMVolumeGroup whose single entry asks for specSize while
// the node reports a physical volume of pvSize.
func growLVG(specSize, pvSize string, loop string) *v1alpha1.LVMVolumeGroup {
	lvg := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: v1.ObjectMeta{Name: "vg-a"},
		Spec: v1alpha1.LVMVolumeGroupSpec{
			ActualVGNameOnTheNode: "vg-a-on-node",
			FileDevices: []v1alpha1.LVMVolumeGroupFileDeviceSpec{
				{Name: "d1", Directory: growDir, Size: resource.MustParse(specSize)},
			},
		},
	}
	if pvSize != "" {
		lvg.Status.Nodes = []v1alpha1.LVMVolumeGroupNode{{
			Name: "test_node",
			FileDevices: []v1alpha1.LVMVolumeGroupFileDevice{{
				Name:       "d1",
				FilePath:   utils.BuildFileDevicePath(growDir, "vg-a", "d1"),
				LoopDevice: loop,
				Size:       resource.MustParse(pvSize),
			}},
		}}
	}
	return lvg
}

func growVG() internal.VGData {
	return internal.VGData{VGName: "vg-a-on-node", VGExtentSize: resource.MustParse("4Mi")}
}

// The happy path, in the only order that is safe: the file first, then the
// device, then the physical volume. Reversing any pair would point a larger
// device at a smaller file.
func TestGrowFileDevicesIfNeeded_GrowsFileThenLoopThenPV(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)

	lvg := growLVG("2Gi", "1020Mi", "/dev/loop0")
	path := utils.BuildFileDevicePath(growDir, "vg-a", "d1")

	gomock.InOrder(
		mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), path).Return("losetup -j ...", "/dev/loop0", nil),
		mc.EXPECT().GetFilesystemSpace(gomock.Any(), growDir).Return("stat -f ...", internal.FilesystemSpace{AvailableBytes: int64(1) << 40}, nil),
		mc.EXPECT().CreateFileDevice(gomock.Any(), path, int64(2)<<30).Return("fallocate ...", nil),
		mc.EXPECT().SetLoopCapacity(gomock.Any(), "/dev/loop0").Return("losetup -c ...", nil),
		mc.EXPECT().ResizePV(gomock.Any(), "/dev/loop0").Return("pvresize ...", nil),
	)

	assert.NoError(t, r.growFileDevicesIfNeeded(context.Background(), lvg, growVG(), fileDeviceIssues{}))
}

// A physical volume is always a little smaller than its backing file: LVM takes
// metadata off the front and rounds down to whole extents. Comparing sizes
// exactly would make every single reconcile try to grow a device that is
// already the requested size.
func TestGrowFileDevicesIfNeeded_ToleratesTheMetadataGap(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)

	// 1Gi requested, 1020Mi reported — exactly one 4Mi extent of difference.
	lvg := growLVG("1Gi", "1020Mi", "/dev/loop0")

	// No expectations at all: gomock fails the test if anything is called.
	assert.NoError(t, r.growFileDevicesIfNeeded(context.Background(), lvg, growVG(), fileDeviceIssues{}))
}

// The invariant that ties the two halves of the feature together: a size the
// discoverer reconstructed on import must never look like a size the user asked to
// grow to.
//
// It is asserted end to end — reconstructFileDeviceSize feeds the spec,
// growFileDevicesIfNeeded reads it back — and across every extent size a Volume
// Group can have, because the two sides had drifted apart precisely where nothing
// checked them together. `vgcreate -s 128k` is legal, so an imported Volume Group
// can have a 128Ki extent; reconstruction rounds the size up to a whole mebibyte to
// stay expressible under the CRD's pattern, which is eight extents. A tolerance of
// one extent then read that as a growth request, and since the PV size cannot
// change, every reconcile re-ran fallocate, losetup -c and pvresize as no-ops and
// flapped VGConfigurationApplied between Updating and Applied forever.
//
// gomock is the assertion here: no expectations are registered, so any host command
// fails the test.
//
// The PV size is derived from the file size via pvSizeForFile, the arithmetic LVM
// itself does, so every case describes a state a node can actually be in.
func TestGrowFileDevicesIfNeeded_DoesNotGrowAFreshlyImportedEntry(t *testing.T) {
	for _, extentRaw := range []string{"128Ki", "512Ki", "1Mi", "4Mi", "64Mi"} {
		for _, fileRaw := range []string{"1Gi", "1536Mi", "2Gi", "20Gi", "8191Mi"} {
			t.Run("extent_"+extentRaw+"_file_"+fileRaw, func(t *testing.T) {
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()
				mc := mock_utils.NewMockCommands(ctrl)
				r := reconcilerWithMockedCommands(t, mc)

				extent := resource.MustParse(extentRaw)
				pvSize := pvSizeForFile(resource.MustParse(fileRaw), extent)
				// Exactly what CreateLVMVolumeGroupByCandidate would write into the
				// spec for a Volume Group with this extent and this PV.
				imported := reconstructFileDeviceSize(pvSize, extent)

				lvg := growLVG(imported.String(), pvSize.String(), "/dev/loop0")
				vg := internal.VGData{VGName: "vg-a-on-node", VGExtentSize: extent}

				assert.NoError(t, r.growFileDevicesIfNeeded(context.Background(), lvg, vg, fileDeviceIssues{}))
			})
		}
	}
}

// fallocate to a smaller size truncates the file. Under a live physical volume
// that destroys data, so a shrink must never reach the node — the CEL rule
// rejects it at admission and this is the second line.
func TestGrowFileDevicesIfNeeded_NeverShrinks(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)

	lvg := growLVG("1Gi", "10Gi", "/dev/loop0")

	assert.NoError(t, r.growFileDevicesIfNeeded(context.Background(), lvg, growVG(), fileDeviceIssues{}))
}

// Only the delta has to fit: the bytes already in the file are accounted for on
// the filesystem. Checking the full requested size would refuse growth that is
// perfectly possible.
func TestGrowFileDevicesIfNeeded_ChecksTheDeltaAgainstFreeSpace(t *testing.T) {
	t.Run("delta fits, total would not", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mc := mock_utils.NewMockCommands(ctrl)
		r := reconcilerWithMockedCommands(t, mc)

		lvg := growLVG("10Gi", "5Gi", "/dev/loop0")
		path := utils.BuildFileDevicePath(growDir, "vg-a", "d1")

		gomock.InOrder(
			mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), path).Return("losetup -j ...", "/dev/loop0", nil),
			// 6Gi free: less than the 10Gi total, more than the 5Gi delta.
			mc.EXPECT().GetFilesystemSpace(gomock.Any(), growDir).Return("stat -f ...", internal.FilesystemSpace{AvailableBytes: int64(6) << 30}, nil),
			mc.EXPECT().CreateFileDevice(gomock.Any(), path, int64(10)<<30).Return("fallocate ...", nil),
			mc.EXPECT().SetLoopCapacity(gomock.Any(), "/dev/loop0").Return("losetup -c ...", nil),
			mc.EXPECT().ResizePV(gomock.Any(), "/dev/loop0").Return("pvresize ...", nil),
		)

		assert.NoError(t, r.growFileDevicesIfNeeded(context.Background(), lvg, growVG(), fileDeviceIssues{}))
	})

	t.Run("delta does not fit", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mc := mock_utils.NewMockCommands(ctrl)
		r := reconcilerWithMockedCommands(t, mc)

		lvg := growLVG("10Gi", "1Gi", "/dev/loop0")

		// No fallocate: the node must not be filled up.
		mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), utils.BuildFileDevicePath(growDir, "vg-a", "d1")).
			Return("losetup -j ...", "/dev/loop0", nil)
		mc.EXPECT().GetFilesystemSpace(gomock.Any(), growDir).Return("stat -f ...", internal.FilesystemSpace{AvailableBytes: int64(1) << 30}, nil)

		err := r.growFileDevicesIfNeeded(context.Background(), lvg, growVG(), fileDeviceIssues{})
		if assert.Error(t, err) {
			assert.Contains(t, err.Error(), "not enough free space")
		}
	})
}

// Each step fails towards the smaller size, so an interrupted growth leaves a
// working — merely smaller — device, and the next reconcile picks up where it
// stopped. What must not happen is the later steps running anyway.
func TestGrowFileDevicesIfNeeded_StopsAtTheFailedStep(t *testing.T) {
	t.Run("fallocate fails", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mc := mock_utils.NewMockCommands(ctrl)
		r := reconcilerWithMockedCommands(t, mc)

		path := utils.BuildFileDevicePath(growDir, "vg-a", "d1")
		gomock.InOrder(
			mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), path).Return("losetup -j ...", "/dev/loop0", nil),
			mc.EXPECT().GetFilesystemSpace(gomock.Any(), growDir).Return("stat -f ...", internal.FilesystemSpace{AvailableBytes: int64(1) << 40}, nil),
			mc.EXPECT().CreateFileDevice(gomock.Any(), path, int64(2)<<30).Return("fallocate ...", errors.New("ENOSPC")),
		)
		// No SetLoopCapacity, no ResizePV.

		assert.Error(t, r.growFileDevicesIfNeeded(context.Background(),
			growLVG("2Gi", "1020Mi", "/dev/loop0"), growVG(), fileDeviceIssues{}))
	})

	t.Run("losetup -c fails", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mc := mock_utils.NewMockCommands(ctrl)
		r := reconcilerWithMockedCommands(t, mc)

		path := utils.BuildFileDevicePath(growDir, "vg-a", "d1")
		gomock.InOrder(
			mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), path).Return("losetup -j ...", "/dev/loop0", nil),
			mc.EXPECT().GetFilesystemSpace(gomock.Any(), growDir).Return("stat -f ...", internal.FilesystemSpace{AvailableBytes: int64(1) << 40}, nil),
			mc.EXPECT().CreateFileDevice(gomock.Any(), path, int64(2)<<30).Return("fallocate ...", nil),
			mc.EXPECT().SetLoopCapacity(gomock.Any(), "/dev/loop0").Return("losetup -c ...", errors.New("EBUSY")),
		)
		// No ResizePV: the device has not grown, so there is nothing to resize onto.

		assert.Error(t, r.growFileDevicesIfNeeded(context.Background(),
			growLVG("2Gi", "1020Mi", "/dev/loop0"), growVG(), fileDeviceIssues{}))
	})
}

// An entry that has not reached the node yet belongs to the provisioning path,
// which creates it at the requested size. Growing it here would mean acting on
// a file that does not exist.
func TestGrowFileDevicesIfNeeded_SkipsUnprovisionedEntries(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)

	assert.NoError(t, r.growFileDevicesIfNeeded(context.Background(),
		growLVG("2Gi", "", ""), growVG(), fileDeviceIssues{}))
}

// An entry that failed validation is not provisioned, and it must not be grown
// either — the update path keeps reconciling everything else around it.
func TestGrowFileDevicesIfNeeded_SkipsInvalidEntries(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)

	issues := fileDeviceIssues{reason: "bad", invalid: map[string]struct{}{"d1": {}}}
	assert.NoError(t, r.growFileDevicesIfNeeded(context.Background(),
		growLVG("2Gi", "1020Mi", "/dev/loop0"), growVG(), issues))
}

// The loop is always resolved from the backing file, never taken from the
// status: a loop minor is not stable across a reattach, and the status only
// catches up on the next discovery pass. If nothing is attached, the file must
// not be grown — its device would stay behind.
func TestGrowFileDevicesIfNeeded_ResolvesTheLoopFromTheBackingFile(t *testing.T) {
	t.Run("resolved", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mc := mock_utils.NewMockCommands(ctrl)
		r := reconcilerWithMockedCommands(t, mc)

		path := utils.BuildFileDevicePath(growDir, "vg-a", "d1")
		gomock.InOrder(
			mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), path).Return("losetup -j ...", "/dev/loop7", nil),
			mc.EXPECT().GetFilesystemSpace(gomock.Any(), growDir).Return("stat -f ...", internal.FilesystemSpace{AvailableBytes: int64(1) << 40}, nil),
			mc.EXPECT().CreateFileDevice(gomock.Any(), path, int64(2)<<30).Return("fallocate ...", nil),
			mc.EXPECT().SetLoopCapacity(gomock.Any(), "/dev/loop7").Return("losetup -c ...", nil),
			mc.EXPECT().ResizePV(gomock.Any(), "/dev/loop7").Return("pvresize ...", nil),
		)

		assert.NoError(t, r.growFileDevicesIfNeeded(context.Background(),
			growLVG("2Gi", "1020Mi", ""), growVG(), fileDeviceIssues{}))
	})

	t.Run("no loop attached", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mc := mock_utils.NewMockCommands(ctrl)
		r := reconcilerWithMockedCommands(t, mc)

		path := utils.BuildFileDevicePath(growDir, "vg-a", "d1")
		mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), path).Return("losetup -j ...", "", nil)
		// No fallocate: growing a file with no device behind it is pointless and
		// would silently diverge the file from the PV.

		err := r.growFileDevicesIfNeeded(context.Background(),
			growLVG("2Gi", "1020Mi", ""), growVG(), fileDeviceIssues{})
		if assert.Error(t, err) {
			assert.Contains(t, err.Error(), "no loop device")
		}
	})
}

// A loop minor is not tied to a file for life: ReattachFileDevices re-attaches
// via `losetup --find` after a reboot and may land on a different one, and the
// kernel can hand a freed minor to something else entirely. Growing on the
// minor recorded in the status would then run losetup -c and pvresize against a
// stranger's device while the file that needed the capacity never got it.
func TestGrowFileDevicesIfNeeded_IgnoresAStaleLoopInTheStatus(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)

	path := utils.BuildFileDevicePath(growDir, "vg-a", "d1")
	gomock.InOrder(
		// The status says /dev/loop0; the file is in fact on /dev/loop9.
		mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), path).Return("losetup -j ...", "/dev/loop9", nil),
		mc.EXPECT().GetFilesystemSpace(gomock.Any(), growDir).Return("stat -f ...", internal.FilesystemSpace{AvailableBytes: int64(1) << 40}, nil),
		mc.EXPECT().CreateFileDevice(gomock.Any(), path, int64(2)<<30).Return("fallocate ...", nil),
		mc.EXPECT().SetLoopCapacity(gomock.Any(), "/dev/loop9").Return("losetup -c ...", nil),
		mc.EXPECT().ResizePV(gomock.Any(), "/dev/loop9").Return("pvresize ...", nil),
	)

	assert.NoError(t, r.growFileDevicesIfNeeded(context.Background(),
		growLVG("2Gi", "1020Mi", "/dev/loop0"), growVG(), fileDeviceIssues{}))
}

// A growth that did not go through has its own VGConfigurationApplied reason,
// documented in the FAQ and whitelisted in the conditions watcher's
// acceptableReasons. Both are worthless if the agent never writes it — which is
// what happened while the error was wrapped with an empty reason and fell back to
// the generic FileDeviceNotApplied, indistinguishable from an entry that was
// never provisioned. The reason has to travel with the error, so it is asserted
// here rather than left to the whitelist test, which only checks membership.
func TestGrowFileDevicesIfNeeded_ReportsGrowFailedReason(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)

	path := utils.BuildFileDevicePath(growDir, "vg-a", "d1")
	gomock.InOrder(
		mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), path).Return("losetup -j ...", "/dev/loop0", nil),
		mc.EXPECT().GetFilesystemSpace(gomock.Any(), growDir).Return("stat -f ...", internal.FilesystemSpace{AvailableBytes: int64(1) << 40}, nil),
		mc.EXPECT().CreateFileDevice(gomock.Any(), path, int64(2)<<30).Return("fallocate ...", errors.New("ENOSPC")),
	)

	err := r.growFileDevicesIfNeeded(context.Background(),
		growLVG("2Gi", "1020Mi", "/dev/loop0"), growVG(), fileDeviceIssues{})

	// Not fatal: the Volume Group is still the size it was and still serving every
	// volume on it, so the reconcile has to carry on and report this at the end.
	msg, reason, fatal := splitUnappliedFileDevices(err)
	assert.NoError(t, fatal)
	assert.Contains(t, msg, "ENOSPC")
	assert.Equal(t, internal.ReasonFileDeviceGrowFailed, reason)

	// And the reason survives the precedence rules that pick what lands on the
	// condition, instead of being collapsed into the generic one.
	assert.Equal(t, internal.ReasonFileDeviceGrowFailed, fileDeviceConditionReason("", msg, reason))
}
