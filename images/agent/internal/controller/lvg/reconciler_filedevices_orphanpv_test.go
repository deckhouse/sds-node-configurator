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

// The update path has to decide whether a loop device is already a Physical Volume
// *of the Volume Group being extended*. Answering the weaker question — "is it a
// Physical Volume at all" — silently drops the one state the guard exists for: a
// create interrupted between pvcreate and vgextend leaves an orphan PV, and the
// spec.fileDevices entry then never joins the VG while the LVMVolumeGroup reports
// Applied at a size nobody asked for.

package lvg

import (
	"bytes"
	"context"
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

// orphanPVLVG is an LVMVolumeGroup with a single entry whose backing file is
// already attached to loop, so provisioning reuses it and the whole decision falls
// to the membership check.
func orphanPVLVG() (*v1alpha1.LVMVolumeGroup, string) {
	fd := v1alpha1.LVMVolumeGroupFileDeviceSpec{Name: "d1", Directory: "/data", Size: resource.MustParse("1Gi")}
	lvg := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: v1.ObjectMeta{Name: "lvg-a"},
		Spec: v1alpha1.LVMVolumeGroupSpec{
			ActualVGNameOnTheNode: "vg-a",
			FileDevices:           []v1alpha1.LVMVolumeGroupFileDeviceSpec{fd},
		},
	}

	return lvg, utils.BuildFileDevicePath(fd.Directory, lvg.Name, fd.Name)
}

// The regression: pvcreate ran, vgextend did not, and the loop is a Physical
// Volume of nothing. It has to be handed to vgextend — skipping it leaves the
// entry out of the Volume Group for good, with the condition saying Applied.
func TestExtendFileDevicesIfNeeded_AddsAnOrphanPVToTheVG(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)

	lvg, path := orphanPVLVG()
	vg := internal.VGData{VGName: "vg-a"}

	mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), path).Return("losetup -j", "/dev/loop0", nil)
	// /dev/loop0 carries a PV label and belongs to no Volume Group.
	mc.EXPECT().GetAllPVs(gomock.Any()).Return(
		[]internal.PVData{{PVName: "/dev/loop0"}}, "lvm pvs", bytes.Buffer{}, nil).AnyTimes()
	// No pvcreate: createPVIfNeeded sees the existing PV label.
	mc.EXPECT().ExtendVG("vg-a", []string{"/dev/loop0"}).Return("vgextend", nil)
	mc.EXPECT().UdevadmTrigger(gomock.Any(), []string{"/dev/loop0"}).Return("udevadm trigger", nil)

	assert.NoError(t, r.extendFileDevicesIfNeeded(context.Background(), lvg, vg, nil, fileDeviceIssues{}))
}

// The other half of the same decision: a loop that is already a Physical Volume of
// this Volume Group is done, and running vgextend over it again is an error the
// caller treats as fatal.
func TestExtendFileDevicesIfNeeded_LeavesALoopAlreadyInTheVGAlone(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)

	lvg, path := orphanPVLVG()
	vg := internal.VGData{VGName: "vg-a"}

	mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), path).Return("losetup -j", "/dev/loop0", nil)
	mc.EXPECT().GetAllPVs(gomock.Any()).Return(
		[]internal.PVData{{PVName: "/dev/loop0", VGName: "vg-a"}}, "lvm pvs", bytes.Buffer{}, nil).AnyTimes()
	// The strict mock fails the test if ExtendVG is called.

	assert.NoError(t, r.extendFileDevicesIfNeeded(context.Background(), lvg, vg, nil, fileDeviceIssues{}))
}

// A loop this agent provisioned whose Physical Volume ended up in somebody else's
// Volume Group cannot be extended into ours, and must not be passed over in
// silence either: it is reported as a per-entry problem, which keeps the Volume
// Group in service and retries.
func TestExtendFileDevicesIfNeeded_ReportsALoopHeldByAnotherVG(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)

	lvg, path := orphanPVLVG()
	vg := internal.VGData{VGName: "vg-a"}

	mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), path).Return("losetup -j", "/dev/loop0", nil)
	mc.EXPECT().GetAllPVs(gomock.Any()).Return(
		[]internal.PVData{{PVName: "/dev/loop0", VGName: "someone-elses-vg"}}, "lvm pvs", bytes.Buffer{}, nil).AnyTimes()

	msg, _, fatal := splitUnappliedFileDevices(
		r.extendFileDevicesIfNeeded(context.Background(), lvg, vg, nil, fileDeviceIssues{}))

	assert.NoError(t, fatal, "an overlap with another Volume Group must not take this one out of service")
	assert.Contains(t, msg, "/dev/loop0")
	assert.Contains(t, msg, "someone-elses-vg")
}
