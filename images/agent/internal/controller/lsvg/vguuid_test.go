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

package lsvg

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/mock_utils"
)

func readVGUUIDs(t *testing.T, dir string) map[string]string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, vgUUIDFileName))
	if err != nil {
		return nil
	}
	known := map[string]string{}
	require.NoError(t, json.Unmarshal(raw, &known))
	return known
}

func TestTheFencingHandlerGetsTheIdentityItCannotAskFor(t *testing.T) {
	// sanlock hands the handler a name and nothing else, and by then the storage
	// is gone, so `vgs` cannot be asked. Measured on a live pool without this:
	// the handler ran at exactly 8 x io_timeout and answered "vg-uuid.json has
	// no entry for vgext" — mapsFound 0, mapsCovered 0, complete false. The
	// volume stayed writable on a node that had just been told it may not write.
	fakeSysBlockWithLUN(t, 8192)
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	group := testGroup(testNode)
	commands.EXPECT().GetVG(testVG).
		Return(internal.VGData{VGName: testVG, VGUUID: "vg-uuid-of-the-pool"}, "vgs", bytes.Buffer{}, nil).AnyTimes()
	r, _, dir := testReconciler(t, nodeWith(map[string]string{
		SanlockHostIDAnnotation:                        "7",
		LockspaceGenerationAnnotationPrefix + "pool-1": "1",
	}), commands, nil, group)
	commands.EXPECT().VGLockStart(gomock.Any(), gomock.Any(), gomock.Any()).Return("", nil).AnyTimes()

	reconcile(t, r, group)

	assert.Equal(t, map[string]string{testVG: "vg-uuid-of-the-pool"}, readVGUUIDs(t, dir),
		"the handler reads this file while the node is losing its storage")
}

func TestAnotherPoolsIdentityIsNotLostWhenOneIsRecorded(t *testing.T) {
	// One file, several pools on the node. Rewriting it from one group's
	// knowledge would leave every other pool on this node unfenceable.
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	group := testGroup(testNode)
	r, _, dir := testReconciler(t, nodeWith(nil), commands, nil, group)
	require.NoError(t, os.WriteFile(filepath.Join(dir, vgUUIDFileName),
		[]byte(`{"other-pool":"uuid-of-the-other-one"}`), 0o644))

	r.rememberVGUUID(testVG, "uuid-of-this-one")

	assert.Equal(t, map[string]string{
		"other-pool": "uuid-of-the-other-one",
		testVG:       "uuid-of-this-one",
	}, readVGUUIDs(t, dir))
}

func TestAGroupThatIsGoneLeavesNoIdentityBehind(t *testing.T) {
	// A stale entry is not harmless: it points the handler at a UUID that no
	// longer exists, so it finds no maps and reports a barrier it never raised.
	// The file on the stand held exactly that — one pool, one obsolete UUID.
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	group := testGroup(testNode)
	r, _, dir := testReconciler(t, nodeWith(nil), commands, nil, group)
	require.NoError(t, os.WriteFile(filepath.Join(dir, vgUUIDFileName),
		[]byte(`{"other-pool":"keep-me","`+testVG+`":"drop-me"}`), 0o644))

	r.forgetVGUUID(testVG)

	assert.Equal(t, map[string]string{"other-pool": "keep-me"}, readVGUUIDs(t, dir))
}

func TestAGroupWithoutAKnownUUIDIsNotRecordedAsEmpty(t *testing.T) {
	// An entry saying "this group has no identity" would send the handler
	// looking for maps prefixed with nothing at all.
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	group := testGroup(testNode)
	r, _, dir := testReconciler(t, nodeWith(nil), commands, nil, group)

	r.rememberVGUUID(testVG, "")

	assert.Nil(t, readVGUUIDs(t, dir), "nothing is written when there is nothing to say")
}
