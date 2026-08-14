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

// The code paths that act on a Volume Group solely because it carries an LVM tag.
// Since spec.fileDevices removed `loop` from LVMGlobalFilter all of them can see
// loop-backed Volume Groups that are not the module's, and each does something
// irreversible about it: two activate a possibly-live guest's logical volumes on
// the host, the third rewrites the tag that identified it.

package utils_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/mock_utils"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/monitoring"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/utils"
)

// restoredImageVG is a Volume Group inside an image of a node disk this module
// used to manage, attached with `losetup -f` for a restore. It carries
// storage.deckhouse.io/enabled=true, because the module put it there on the node
// the image came from.
const restoredImageVG = "data"

// `vgchange -ay` over a guest's Volume Group while the guest is running gives the
// same extents two writers. The LVM tag cannot rule that out, so the backing file
// has to.
func TestActivateAllManagedVGs_SkipsAForeignLoopBackedVG(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)

	mc.EXPECT().PVScan(gomock.Any()).Return("pvscan --cache", nil)
	mc.EXPECT().VGScan(gomock.Any()).Return("vgscan --cache", nil)
	mc.EXPECT().GetAllVGs(gomock.Any()).Return([]internal.VGData{
		{VGName: restoredImageVG, VGUUID: "uuid-image", VGTags: managedTag},
		{VGName: "ours", VGUUID: "uuid-ours", VGTags: managedTag},
	}, "lvm vgs", bytes.Buffer{}, nil)
	mc.EXPECT().GetAllPVs(gomock.Any()).Return([]internal.PVData{
		{PVName: "/dev/loop7", VGUuid: "uuid-image", VGName: restoredImageVG},
		{PVName: "/dev/sdb", VGUuid: "uuid-ours", VGName: "ours"},
	}, "lvm pvs", bytes.Buffer{}, nil)
	mc.EXPECT().GetLoopBackingFile(gomock.Any(), "/dev/loop7").
		Return("losetup -O BACK-FILE", internal.LoopBackingFile{Path: "/backup/node2-root.img"}, nil)
	// Only ours. gomock fails the test if VGActivate is called for the image.
	mc.EXPECT().VGActivate(gomock.Any(), "ours").Return("vgchange -ay ours", nil)

	err := utils.ActivateAllManagedVGs(context.Background(), testLogger(t), mc, monitoring.GetMetrics("test_node"), 30*time.Second)
	assert.NoError(t, err)
}

// Without the PV list every loop-only Volume Group is unclassifiable, and
// activating anything then risks activating somebody else's. Volumes not coming up
// is an outage the scanner heals on the next cache fill (EnsureVGActivation
// retries); two writers on one Volume Group is not healable at all.
func TestActivateAllManagedVGs_RefusesWhenPVsCannotBeListed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)

	mc.EXPECT().PVScan(gomock.Any()).Return("pvscan --cache", nil)
	mc.EXPECT().VGScan(gomock.Any()).Return("vgscan --cache", nil)
	mc.EXPECT().GetAllVGs(gomock.Any()).
		Return([]internal.VGData{{VGName: "ours", VGUUID: "uuid-ours", VGTags: managedTag}}, "lvm vgs", bytes.Buffer{}, nil)
	mc.EXPECT().GetAllPVs(gomock.Any()).
		Return(nil, "lvm pvs", bytes.Buffer{}, errors.New("lvm unavailable"))
	// No VGActivate at all.

	err := utils.ActivateAllManagedVGs(context.Background(), testLogger(t), mc, monitoring.GetMetrics("test_node"), 30*time.Second)
	assert.Error(t, err)
}

// EnsureVGActivation runs on every cache fill, so a foreign Volume Group let
// through here is activated again after every udev burst — even if an operator
// deactivated it by hand in between.
func TestEnsureVGActivation_SkipsAForeignLoopBackedVG(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)

	vgs := []internal.VGData{
		{VGName: restoredImageVG, VGUUID: "uuid-image", VGTags: managedTag},
		{VGName: "ours", VGUUID: "uuid-ours", VGTags: managedTag},
	}
	// An inactive thick LV in each, so both look like something to activate.
	lvs := []internal.LVData{
		{LVName: "guest-lv", VGName: restoredImageVG, LVAttr: "-wi-------"},
		{LVName: "our-lv", VGName: "ours", LVAttr: "-wi-------"},
	}
	verdicts := utils.LoopVGVerdicts{
		"uuid-image": utils.LoopVGUnowned,
		"uuid-ours":  utils.LoopVGNotLoopOnly,
	}

	mc.EXPECT().VGActivate(gomock.Any(), "ours").Return("vgchange -ay ours", nil)

	activated := utils.EnsureVGActivation(context.Background(), testLogger(t), mc,
		monitoring.GetMetrics("test_node"), vgs, lvs, verdicts, 30*time.Second)
	assert.True(t, activated)
}

// ReTag is worse than activation: its write is what makes a Volume Group the
// module's, since it replaces the legacy tag with storage.deckhouse.io/enabled=true
// and the discoverer adopts it afterwards. Its only gate is the legacy tag, which a
// guest running LINSTOR inside a file-backed disk carries too — and afterwards the
// tag that identified the guest's Volume Group is gone.
func TestReTag_LeavesAForeignLoopBackedVGAlone(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)

	const legacy = "linstor-legacy"
	mc.EXPECT().GetAllVGs(gomock.Any()).Return([]internal.VGData{
		{VGName: "guest-vg", VGUUID: "uuid-guest", VGTags: legacy},
		{VGName: "ours", VGUUID: "uuid-ours", VGTags: legacy},
	}, "lvm vgs", bytes.Buffer{}, nil)
	mc.EXPECT().GetAllPVs(gomock.Any()).Return([]internal.PVData{
		{PVName: "/dev/loop7", VGUuid: "uuid-guest", VGName: "guest-vg"},
		{PVName: "/dev/sdb", VGUuid: "uuid-ours", VGName: "ours"},
	}, "lvm pvs", bytes.Buffer{}, nil)
	// The guest's Volume Group is loop-only and untagged as far as the managed tag
	// goes, so it is settled without reading a backing file.
	mc.EXPECT().GetAllLVs(gomock.Any()).Return([]internal.LVData{
		{LVName: "guest-lv", VGName: "guest-vg", LvTags: legacy},
		{LVName: "our-lv", VGName: "ours", LvTags: legacy},
	}, "lvm lvs", bytes.Buffer{}, nil)

	// Only our Volume Group and its logical volume may be rewritten. Every
	// expectation below is exact, so a call naming "guest-vg" fails the test.
	mc.EXPECT().LVChangeDelTag(gomock.Any(), gomock.Any(), legacy).
		DoAndReturn(func(_ context.Context, lv internal.LVData, _ string) (string, error) {
			assert.Equal(t, "ours", lv.VGName, "the guest's logical volume must not be re-tagged")
			return "lvchange --deltag", nil
		})
	mc.EXPECT().VGChangeDelTag(gomock.Any(), "ours", legacy).Return("vgchange --deltag", nil)
	mc.EXPECT().VGChangeAddTag(gomock.Any(), "ours", managedTag).
		Return("vgchange --addtag", nil).Times(2) // once from the LV pass, once from the VG pass

	err := utils.ReTagForTest(context.Background(), mc, testLogger(t), monitoring.GetMetrics("test_node"), "test", 30*time.Second)
	assert.NoError(t, err)
}

// The legacy tag is the only thing ReTag keys on, so failing to read the PV list
// leaves it unable to tell whose Volume Group it is about to rewrite. Retagging is
// a one-off migration — not doing it costs nothing until the next restart, while
// doing it to the wrong Volume Group cannot be undone, because the tag it replaces
// is gone afterwards.
func TestReTag_RefusesWhenPVsCannotBeListed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)

	mc.EXPECT().GetAllVGs(gomock.Any()).
		Return([]internal.VGData{{VGName: "ours", VGUUID: "uuid-ours", VGTags: "linstor-legacy"}}, "lvm vgs", bytes.Buffer{}, nil)
	mc.EXPECT().GetAllPVs(gomock.Any()).
		Return(nil, "lvm pvs", bytes.Buffer{}, errors.New("lvm unavailable"))
	// No GetAllLVs, no vgchange, no lvchange.

	err := utils.ReTagForTest(context.Background(), mc, testLogger(t), monitoring.GetMetrics("test_node"), "test", 30*time.Second)
	assert.Error(t, err)
}
