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
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"github.com/deckhouse/sds-node-configurator/images/agent/internal/logger"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/mock_utils"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/utils"
)

func testLogger(t *testing.T) logger.Logger {
	t.Helper()
	log, err := logger.NewLogger(logger.ErrorLevel)
	if err != nil {
		t.Fatalf("init logger: %v", err)
	}
	return log
}

// ReattachFileDevices must reuse an existing loop attachment instead of
// allocating a fresh one — otherwise a node reboot followed by a
// successful reattach leaks loop minors on every restart.
func TestReattachFileDevices_SkipsAlreadyAttached(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)

	mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), "/data/sds-vg-a-deadbeef0011.img").Return("losetup -j ...", "/dev/loop0", nil)
	// SetupLoopDevice MUST NOT be called when the file is already attached.

	err := utils.ReattachFileDevices(context.Background(), testLogger(t), mc, 30*time.Second, []utils.LVGWithFileDevices{
		{LVGName: "vg-a", FileDevices: []utils.FileDeviceStatus{{FilePath: "/data/sds-vg-a-deadbeef0011.img", LoopDevice: "/dev/loop0"}}},
	})
	assert.NoError(t, err)
}

// When losetup fails on a single file, ReattachFileDevices must still
// process every other entry and return a joined error so the caller can
// abort the activation step.
func TestReattachFileDevices_AggregatesErrors(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)

	mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), "/data/sds-vg-a-aaaaaaaaaaaa.img").Return("losetup -j ...", "", nil)
	mc.EXPECT().SetupLoopDevice(gomock.Any(), "/data/sds-vg-a-aaaaaaaaaaaa.img").Return("losetup --find --show ...", "", errors.New("ENOENT"))
	mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), "/data/sds-vg-a-bbbbbbbbbbbb.img").Return("losetup -j ...", "", nil)
	mc.EXPECT().SetupLoopDevice(gomock.Any(), "/data/sds-vg-a-bbbbbbbbbbbb.img").Return("losetup --find --show ...", "/dev/loop3", nil)

	err := utils.ReattachFileDevices(context.Background(), testLogger(t), mc, 30*time.Second, []utils.LVGWithFileDevices{
		{LVGName: "vg-a", FileDevices: []utils.FileDeviceStatus{
			{FilePath: "/data/sds-vg-a-aaaaaaaaaaaa.img"},
			{FilePath: "/data/sds-vg-a-bbbbbbbbbbbb.img"},
		}},
	})
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "aaaaaaaaaaaa.img")
	}
}

// FindLoopDeviceByFile failures must also be surfaced (and remaining
// files still processed best-effort).
func TestReattachFileDevices_QueryFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)

	mc.EXPECT().FindLoopDeviceByFile(gomock.Any(), "/data/sds-vg-a-cafebabec0de.img").Return("losetup -j ...", "", errors.New("EIO"))

	err := utils.ReattachFileDevices(context.Background(), testLogger(t), mc, 30*time.Second, []utils.LVGWithFileDevices{
		{LVGName: "vg-a", FileDevices: []utils.FileDeviceStatus{{FilePath: "/data/sds-vg-a-cafebabec0de.img"}}},
	})
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "EIO")
	}
}
