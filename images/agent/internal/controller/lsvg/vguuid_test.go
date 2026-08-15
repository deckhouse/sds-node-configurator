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
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/mock_utils"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/utils"
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

func TestEveryMemberForgetsAGroupThatIsGoneNotOnlyItsOwner(t *testing.T) {
	// Only the owner runs the removal, so only the owner used to drop the entry.
	// The other members were left pointing the fencing handler at a group that
	// no longer exists — seen on the stand, where a pool was removed cleanly and
	// two of its three nodes still named it in vg-uuid.json afterwards.
	fakeSysBlockWithLUN(t, 8192)
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	group := deletedGroup(func(g *v1alpha1.LVMSharedVolumeGroup) {
		g.Spec.MetadataOwner = "another-node"
	})
	r, _, dir := testReconciler(t, nodeWith(map[string]string{SanlockHostIDAnnotation: "7"}), commands, nil, group)
	require.NoError(t, os.WriteFile(filepath.Join(dir, vgUUIDFileName),
		[]byte(`{"other-pool":"keep-me","`+testVG+`":"drop-me"}`), 0o644))

	reconcile(t, r, group)

	assert.Equal(t, map[string]string{"other-pool": "keep-me"}, readVGUUIDs(t, dir),
		"a member that left the pool has no business naming its group to the handler")
}

func TestANodePublishesWhatItCanSayAboutTheReservationChannel(t *testing.T) {
	// Established by reading and never by trying: switching a pool to
	// reservations is a one-way door in the middle of its own procedure, so the
	// preconditions are answered before anybody opens it.
	fakeSysBlockWithLUN(t, 8192)
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	group := testGroup(testNode)
	// The image does not carry the tooling every reservation command runs from.
	commands.EXPECT().MissingReservationTools(gomock.Any()).
		Return([]string{"/usr/sbin/lvmpersist"}, nil).AnyTimes()
	commands.EXPECT().MultipathConfiguration(gomock.Any()).Return("\treservation_key \"file\"\n", nil).AnyTimes()
	commands.EXPECT().RecordedReservationKey(gomock.Any()).Return("", nil).AnyTimes()
	commands.EXPECT().ReservationKeyOf(gomock.Any(), gomock.Any()).Return("0x1", nil).AnyTimes()
	r, cl, _ := testReconciler(t, nodeWith(map[string]string{
		SanlockHostIDAnnotation:                        "7",
		LockspaceGenerationAnnotationPrefix + "pool-1": "1",
	}), commands, nil, group)
	commands.EXPECT().VGLockStart(gomock.Any(), gomock.Any(), gomock.Any()).Return("", nil).AnyTimes()

	reconcile(t, r, group)

	published := &v1alpha1.LVMSharedVolumeGroup{}
	require.NoError(t, cl.Get(context.Background(), client.ObjectKey{Name: group.Name}, published))
	require.NotEmpty(t, published.Status.Nodes)
	pr := published.Status.Nodes[0].PersistentReservations
	require.NotNil(t, pr, "a node says what it knows about the channel whether or not anybody asked yet")
	assert.False(t, pr.Ready)
	assert.Equal(t, utils.ReasonReservationToolsMissing, pr.Reason)
	assert.Contains(t, pr.Message, "/usr/sbin/lvmpersist", "the message names what is missing")
}
