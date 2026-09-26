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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/mock_utils"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/monitoring"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/utils"
)

func ensureVGActivation(t *testing.T, mc *mock_utils.MockCommands, lvs []internal.LVData) bool {
	t.Helper()
	vgs := []internal.VGData{{VGName: "vg-thin", VGUUID: "uuid-thin", VGTags: managedTag}}
	return utils.EnsureVGActivation(context.Background(), testLogger(t), mc,
		monitoring.GetMetrics("test_node"), vgs, lvs, nil, 30*time.Second)
}

// An inactive thin LV reads as empty, and its pool being up does not bring it
// up, so it has to trigger the activation on its own.
func TestEnsureVGActivation_ActivatesAnInactiveThinLVUnderAnActivePool(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)

	mc.EXPECT().VGActivate(gomock.Any(), "vg-thin", false).Return("vgchange -ay vg-thin", nil)

	activated := ensureVGActivation(t, mc, []internal.LVData{
		{LVName: "tp", VGName: "vg-thin", LVAttr: "twi-aotz--"},
		{LVName: "thick", VGName: "vg-thin", LVAttr: "-wi-a-----"},
		{LVName: "pvc-1", VGName: "vg-thin", LVAttr: "Vwi---tz--"},
	})
	assert.True(t, activated)
}

func TestEnsureVGActivation_LeavesAnActiveThinLVAlone(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	// No VGActivate at all.

	activated := ensureVGActivation(t, mc, []internal.LVData{
		{LVName: "tp", VGName: "vg-thin", LVAttr: "twi-aotz--"},
		{LVName: "pvc-1", VGName: "vg-thin", LVAttr: "Vwi-a-tz--"},
	})
	assert.False(t, activated)
}

// `vgchange -ay` passes over an LV with the activation skip flag, so counting it
// would issue the same no-op activation on every cache fill.
func TestEnsureVGActivation_IgnoresAnInactiveLVWithActivationSkip(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mc := mock_utils.NewMockCommands(ctrl)
	// No VGActivate at all.

	activated := ensureVGActivation(t, mc, []internal.LVData{
		{LVName: "tp", VGName: "vg-thin", LVAttr: "twi-aotz--"},
		{LVName: "pvc-1", VGName: "vg-thin", LVAttr: "Vwi-a-tz--"},
		{LVName: "snap-1", VGName: "vg-thin", LVAttr: "Vri---tz-k"},
	})
	assert.False(t, activated)
}
