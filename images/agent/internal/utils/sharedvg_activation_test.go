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

package utils_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/mock_utils"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/monitoring"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/utils"
)

// A shared Volume Group is owned by a lock manager, and which node may activate
// which of its Logical Volumes is that manager's decision. This module does not
// run one, so activating a shared VG locally is how two nodes end up writing the
// same extents — the module's own tag on such a VG does not make it ours to
// activate, and an earlier version of this agent could put that tag there itself.
func TestActivateAllManagedVGs_SkipsSharedVG(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)

	mc.EXPECT().PVScan(gomock.Any()).Return("pvscan --cache", nil)
	mc.EXPECT().VGScan(gomock.Any()).Return("vgscan --cache", nil)
	mc.EXPECT().GetAllVGs(gomock.Any()).Return([]internal.VGData{
		{VGName: "pool", VGUUID: "uuid-pool", VGTags: managedTag, VGShared: "sanlock"},
		{VGName: "ours", VGUUID: "uuid-ours", VGTags: managedTag},
	}, "lvm vgs", bytes.Buffer{}, nil)
	mc.EXPECT().GetAllPVs(gomock.Any()).Return([]internal.PVData{
		{PVName: "/dev/mapper/mpatha", VGUuid: "uuid-pool", VGName: "pool"},
		{PVName: "/dev/sdb", VGUuid: "uuid-ours", VGName: "ours"},
	}, "lvm pvs", bytes.Buffer{}, nil)
	// Only ours. gomock fails the test if VGActivate is called for the pool.
	mc.EXPECT().VGActivate(gomock.Any(), "ours").Return("vgchange -ay ours", nil)

	err := utils.ActivateAllManagedVGs(context.Background(), testLogger(t), mc, monitoring.GetMetrics("test_node"), 30*time.Second)
	assert.NoError(t, err)
}

// The same guarantee on the path the scanner takes: EnsureVGActivation sees an
// inactive Logical Volume and activates its Volume Group, which for a shared one
// would silently undo the lock manager's decision on every cache fill.
func TestEnsureVGActivation_SkipsSharedVG(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)

	vgs := []internal.VGData{
		{VGName: "pool", VGUUID: "uuid-pool", VGTags: managedTag, VGShared: "sanlock"},
	}
	lvs := []internal.LVData{
		{VGName: "pool", LVName: "pvc-1", LVAttr: "-wi-------"},
	}
	verdicts := utils.LoopVGVerdicts{"uuid-pool": utils.LoopVGNotLoopOnly}

	activated := utils.EnsureVGActivation(context.Background(), testLogger(t), mc,
		monitoring.GetMetrics("test_node"), vgs, lvs, verdicts, 30*time.Second)
	assert.False(t, activated, "a shared VG with an inactive LV must not be activated")
}
