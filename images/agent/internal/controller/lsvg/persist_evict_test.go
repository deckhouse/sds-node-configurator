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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/mock_utils"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/utils"
)

// poolWithEvictionRequested is a running pool under reservations, with a person
// asking for one of its members to be cut off.
func poolWithEvictionRequested(keys map[string]string) *v1alpha1.LVMSharedVolumeGroup {
	group := poolAskingForReservations()
	group.Annotations = map[string]string{EvictNodeAnnotation: "second-node"}
	group.Status = &v1alpha1.LVMSharedVolumeGroupStatus{}
	for _, name := range []string{testNode, "second-node"} {
		entry := nodeSaid(name, true, PRStateEnabled)
		entry.PersistentReservations.Key = keys[name]
		group.Status.Nodes = append(group.Status.Nodes, entry)
	}
	return group
}

func TestEvictionRemovesTheKeyFromEveryPath(t *testing.T) {
	// A registration lives per initiator-target pair, so a key taken off one
	// path leaves the node writing through the others. It is removed with
	// sg_persist rather than lvmpersist because on the multipath map every
	// preempt is refused — measured, not assumed.
	fakeSysBlockWithLUN(t, 8192)
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	group := poolWithEvictionRequested(map[string]string{
		testNode:      "0x1000000000010001",
		"second-node": "0x1000000000010002",
	})

	for _, path := range []string{"/dev/sdb", "/dev/sdc"} {
		commands.EXPECT().PreemptRegistration(gomock.Any(), path, "0x1000000000010001", "0x1000000000010002").
			Return("", nil)
	}
	// Read before the preempt, and read again afterwards to see what is left.
	gomock.InOrder(
		commands.EXPECT().ReadRegistrationKeys(gomock.Any(), gomock.Any()).
			Return([]string{"0x1000000000010001", "0x1000000000010002"}, "", nil).Times(2),
		commands.EXPECT().ReadRegistrationKeys(gomock.Any(), gomock.Any()).
			Return([]string{"0x1000000000010001"}, "", nil).Times(2),
	)

	r, _, _ := testReconciler(t, nodeWith(nil), commands, nil, group)
	r.evictRequestedNode(context.Background(), group)
}

func TestEvictionIsSkippedOnPathsWhereTheKeyIsAlreadyGone(t *testing.T) {
	// Repeating a preempt for a key that is no longer there is answered with a
	// conflict, so a finished eviction would start reporting failures the moment
	// it succeeded. It reads before it writes.
	fakeSysBlockWithLUN(t, 8192)
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	group := poolWithEvictionRequested(map[string]string{
		testNode:      "0x1000000000010001",
		"second-node": "0x1000000000010002",
	})

	commands.EXPECT().ReadRegistrationKeys(gomock.Any(), gomock.Any()).
		Return([]string{"0x1000000000010001"}, "", nil).AnyTimes()
	// No PreemptRegistration: there is nothing left to preempt.

	r, _, _ := testReconciler(t, nodeWith(nil), commands, nil, group)
	r.evictRequestedNode(context.Background(), group)
}

func TestNothingIsEvictedWhenTheTargetNeverReportedItsKey(t *testing.T) {
	// The defect this guards against is the one the whole pool keeps producing:
	// acting on a value nobody filled in. Which key on the LUN belongs to the
	// node that stopped answering is precisely what is unknown, and preempting
	// the unfamiliar one would cut off whichever node registered last.
	fakeSysBlockWithLUN(t, 8192)
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	group := poolWithEvictionRequested(map[string]string{
		testNode: "0x1000000000010001",
	})

	// No ReadRegistrationKeys, no PreemptRegistration: nothing is touched.
	r, cl, _ := testReconciler(t, nodeWith(nil), commands, nil, group)
	r.evictRequestedNode(context.Background(), group)

	published := &v1alpha1.LVMSharedVolumeGroup{}
	require.NoError(t, cl.Get(context.Background(), client.ObjectKey{Name: group.Name}, published))
	entry := entryOf(t, published)
	assert.Equal(t, ReasonEvictionImpossible, entry.Reason)
	assert.Contains(t, entry.Message, "second-node")
}

func TestATargetSharingThisNodesKeyIsNotEvicted(t *testing.T) {
	// Two members reporting one key is a misconfiguration, and preempting it
	// would take this node off the LUNs along with the target.
	fakeSysBlockWithLUN(t, 8192)
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	group := poolWithEvictionRequested(map[string]string{
		testNode: "0x1000000000010001",
		// The same key, written the other way round: a string comparison would
		// miss it and evict both nodes.
		"second-node": "0X1000000000010001",
	})

	r, _, _ := testReconciler(t, nodeWith(nil), commands, nil, group)
	r.evictRequestedNode(context.Background(), group)
}

func TestOnlyOneNodeCarriesTheEvictionOut(t *testing.T) {
	// Three members preempting in turn would each take the reservation from the
	// one before it. The pool's metadata owner does it — and when the owner is
	// the target, the first member holding a registration, by name.
	group := poolAskingForReservations()
	group.Status = &v1alpha1.LVMSharedVolumeGroupStatus{
		Nodes: []v1alpha1.LVMSharedVolumeGroupNodeStatus{
			nodeSaid("second-node", true, PRStateEnabled),
			nodeSaid("third-node", true, PRStateEnabled),
		},
	}

	assert.Equal(t, testNode, evictionExecutor(group, "second-node"))
	assert.Equal(t, "second-node", evictionExecutor(group, testNode),
		"the owner cannot evict itself, so the pool falls back to a member")
}

func TestReservationKeysAreReadOutOfWhatTheToolPrints(t *testing.T) {
	keys := utils.ParseRegistrationKeys(
		"  PR generation=0x2, 2 registered reservation keys follow:\n" +
			"    0x100000000001002d\n" +
			"    0x100000000002002e\n")

	assert.Equal(t, []string{"0x100000000001002d", "0x100000000002002e"}, keys)
	assert.Empty(t, utils.ParseRegistrationKeys("  there are NO registered reservation keys\n"))
}

func TestANodesKeyIsPublishedOnlyWhenEveryLUNAgrees(t *testing.T) {
	// The published key is what a neighbour would fence with, so one that is
	// only probably this node's is worse than none at all.
	assert.Equal(t, "0x1002d", utils.SingleReservationKey([]string{"0x1002d", "0X1002D"}))
	assert.Empty(t, utils.SingleReservationKey([]string{"0x1002d", "0x1002e"}))
	assert.Empty(t, utils.SingleReservationKey([]string{"0x1002d", "0x0"}))
}

func TestAKeyNobodyClaimsIsNamedRatherThanTakenForNobodys(t *testing.T) {
	// lvm2 derives the key from the host id and the lockspace generation, so a
	// node that restarted its lockspace since it last published is registered
	// under a key nobody has seen — measured on the stand, where host id 1 went
	// from 0x1000000000010001 to 0x1000000000040001. Preempting the published
	// key would then remove nothing and report the node fenced.
	fakeSysBlockWithLUN(t, 8192)
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	group := poolWithEvictionRequested(map[string]string{
		testNode:      "0x1000000000010001",
		"second-node": "0x1000000000010002",
	})

	commands.EXPECT().PreemptRegistration(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return("", nil).AnyTimes()
	gomock.InOrder(
		commands.EXPECT().ReadRegistrationKeys(gomock.Any(), gomock.Any()).
			Return([]string{"0x1000000000010001", "0x1000000000010002"}, "", nil).Times(2),
		// Afterwards: the published key is gone, and a key of the same node
		// under a newer lockspace generation is not.
		commands.EXPECT().ReadRegistrationKeys(gomock.Any(), gomock.Any()).
			Return([]string{"0x1000000000010001", "0x1000000000040002"}, "", nil).Times(2),
	)

	r, cl, _ := testReconciler(t, nodeWith(nil), commands, nil, group)
	r.evictRequestedNode(context.Background(), group)

	published := &v1alpha1.LVMSharedVolumeGroup{}
	require.NoError(t, cl.Get(context.Background(), client.ObjectKey{Name: group.Name}, published))
	entry := entryOf(t, published)
	assert.Equal(t, ReasonEvictionIncomplete, entry.Reason)
	assert.Contains(t, entry.Message, "0x1000000000040002")
}

func TestMultipathdIsToldTheKeyThisNodeRegisteredWith(t *testing.T) {
	// The registration is made with sg_persist, not through the library
	// multipathd shares with mpathpersist, so multipathd knows nothing of it —
	// `getprkey` answers "none" while the array holds three keys. A path that
	// comes back after a failure is then left unregistered, and under a Write
	// Exclusive, all registrants reservation it refuses this node's writes while
	// looking like a path that healed.
	fakeSysBlockWithLUN(t, 8192)
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	group := poolAskingForReservations()

	commands.EXPECT().MissingReservationTools(gomock.Any()).Return(nil, nil).AnyTimes()
	commands.EXPECT().MultipathConfiguration(gomock.Any()).Return("\treservation_key \"file\"\n", nil).AnyTimes()
	commands.EXPECT().RecordedReservationKey(gomock.Any()).Return("0x1000000000040001", nil)
	commands.EXPECT().ReservationKeyOf(gomock.Any(), "mpathi").Return("none", nil)
	commands.EXPECT().SetReservationKey(gomock.Any(), "mpathi", "0x1000000000040001").Return("", nil)

	r, _, _ := testReconciler(t, nodeWith(nil), commands, nil, group)

	verdict := r.persistentReservations(context.Background(), group)

	require.NotNil(t, verdict)
	assert.Equal(t, "0x1000000000040001", verdict.Key,
		"and the node publishes it, so a neighbour has something to fence by")
}

func TestAKeyMultipathdAlreadyHasIsNotWrittenAgain(t *testing.T) {
	fakeSysBlockWithLUN(t, 8192)
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	group := poolAskingForReservations()

	commands.EXPECT().MissingReservationTools(gomock.Any()).Return(nil, nil).AnyTimes()
	commands.EXPECT().MultipathConfiguration(gomock.Any()).Return("\treservation_key \"file\"\n", nil).AnyTimes()
	commands.EXPECT().RecordedReservationKey(gomock.Any()).Return("0x1000000000040001", nil)
	// The same key, spelled the other way round: comparing the strings would
	// rewrite it on every pass.
	commands.EXPECT().ReservationKeyOf(gomock.Any(), "mpathi").Return("0X1000000000040001", nil)
	// No SetReservationKey.

	r, _, _ := testReconciler(t, nodeWith(nil), commands, nil, group)

	assert.Equal(t, "0x1000000000040001", r.persistentReservations(context.Background(), group).Key)
}

func TestAnEvictedNodeDoesNotLetItselfBackIn(t *testing.T) {
	// An eviction removes the node's registration, and nothing about that stops
	// the node from making another one: it is the same command the ordinary pass
	// runs whenever its lockspace is missing, and the lockspace goes missing
	// precisely because the eviction worked. On the stand the key was back on
	// the array minutes after it had been preempted.
	fakeSysBlockWithLUN(t, 8192)
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	group := poolAskingForReservations()
	group.Annotations = map[string]string{EvictNodeAnnotation: testNode}
	group.Status = &v1alpha1.LVMSharedVolumeGroupStatus{
		Nodes: []v1alpha1.LVMSharedVolumeGroupNodeStatus{nodeSaid(testNode, true, PRStateEnabled)},
	}

	// No VGPersistStart and no VGLockStart: the node stays out.
	r, cl, _ := testReconciler(t, nodeWith(map[string]string{SanlockHostIDAnnotation: "7"}), commands, nil, group)

	res := reconcile(t, r, group)

	assert.NotZero(t, res.RequeueAfter, "it keeps looking, because the annotation may be removed")

	published := &v1alpha1.LVMSharedVolumeGroup{}
	require.NoError(t, cl.Get(context.Background(), client.ObjectKey{Name: group.Name}, published))
	entry := entryOf(t, published)
	assert.Equal(t, ReasonEvicted, entry.Reason)
	assert.False(t, entry.LockspaceStarted)
	assert.Contains(t, entry.Message, EvictNodeAnnotation)
}

func TestAnEvictionNobodyCanCarryOutSaysSo(t *testing.T) {
	// The executor is chosen among members that hold a registration, and after a
	// restart there may be none — the annotation is set, nothing happens, and
	// the status looks exactly like an eviction in progress. Silence here is
	// indistinguishable from work, and this one will never happen.
	fakeSysBlockWithLUN(t, 8192)
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	group := poolAskingForReservations()
	// The owner is the target, and nobody else has published a state.
	group.Annotations = map[string]string{EvictNodeAnnotation: testNode}
	group.Spec.MetadataOwner = testNode
	// Members sorted: the target is first by name, so the node that speaks is
	// this one — and every member works that out identically.
	group.Spec.Nodes = []string{testNode, "zz-other-node"}
	group.Status = &v1alpha1.LVMSharedVolumeGroupStatus{}

	r, cl, _ := testReconciler(t, nodeWith(nil), commands, nil, group)
	r.cfg.NodeName = "zz-other-node"

	r.evictRequestedNode(context.Background(), group)

	published := &v1alpha1.LVMSharedVolumeGroup{}
	require.NoError(t, cl.Get(context.Background(), client.ObjectKey{Name: group.Name}, published))
	require.NotEmpty(t, published.Status.Nodes)
	assert.Equal(t, ReasonEvictionImpossible, published.Status.Nodes[0].Reason)
	assert.Contains(t, published.Status.Nodes[0].Message, "no member of the pool holds a registration")
}
