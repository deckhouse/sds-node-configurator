/*
Copyright 2025 Flant JSC

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

package lsvg

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/mock_utils"
)

// fencedNode leaves behind what the fencing handler leaves behind.
func fencedNode(t *testing.T, dir string, maps ...string) {
	t.Helper()
	body := `{"vg":"` + testVG + `","vgUUID":"vg-uuid","mapsCovered":` +
		itoa(len(maps)) + `,"complete":true,"coveredMaps":[`
	for i, m := range maps {
		if i > 0 {
			body += ","
		}
		body += `"` + m + `"`
	}
	body += `]}`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "killpath-"+testVG+".json"), []byte(body), 0o644))
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	digits := ""
	for n > 0 {
		digits = string(rune('0'+n%10)) + digits
		n /= 10
	}
	return digits
}

func TestAFencedNodeReturnsToThePoolByItself(t *testing.T) {
	// What used to need an operator with two commands. On a platform meant to
	// run without one, a node whose LUNs are back has to rejoin on its own: the
	// error targets the barrier left hold nothing, so removing them destroys
	// nothing, and the volumes come back only when their attachments ask.
	fakeSysBlockWithLUN(t, 8192)
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	group := ownedGroup()
	r, _, dir := testReconciler(t, nodeWith(map[string]string{SanlockHostIDAnnotation: "7"}), commands, nil, group)
	fencedNode(t, dir, "vgshared-vol1", "vgshared-vol2")

	commands.EXPECT().RemoveDMDevice(gomock.Any(), "vgshared-vol1").Return("dmsetup remove", nil)
	commands.EXPECT().RemoveDMDevice(gomock.Any(), "vgshared-vol2").Return("dmsetup remove", nil)
	commands.EXPECT().CreateVGShared(gomock.Any(), gomock.Any()).Return("vgcreate", nil).AnyTimes()
	commands.EXPECT().VGLockStart(gomock.Any(), testVG, 7).Return("vgchange --lock-start", nil).AnyTimes()

	reconcile(t, r, group)

	_, err := os.Stat(filepath.Join(dir, "killpath-"+testVG+".json"))
	assert.True(t, os.IsNotExist(err),
		"the record of the fencing stops being true once the last error target is gone")
}

func TestAFencedNodeWaitsWhileItsLUNsAreStillMissing(t *testing.T) {
	// Rejoining while the paths are broken buys a second fencing one io_timeout
	// later. The node waits instead — and says why, because a node out of its
	// pool with no explanation is the failure this is meant to prevent.
	fakeSysBlockWithLUN(t, 8192)
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	group := ownedGroup(func(g *v1alpha1.LVMSharedVolumeGroup) { g.Spec.Devices[0].WWID = "not-here-any-more" })
	r, _, dir := testReconciler(t, nodeWith(map[string]string{SanlockHostIDAnnotation: "7"}), commands, nil, group)
	fencedNode(t, dir, "vgshared-vol1")

	// No removal and no lock start: neither is safe while the LUN is gone.
	res := reconcile(t, r, group)

	assert.NotZero(t, res.RequeueAfter, "the paths come back on their own, and so does the node")
	_, err := os.Stat(filepath.Join(dir, "killpath-"+testVG+".json"))
	assert.NoError(t, err, "the record stands until the recovery finishes")
}
