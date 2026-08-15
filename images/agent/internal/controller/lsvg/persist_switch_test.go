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
)

// poolAskingForReservations is a group whose pool has asked for the switch.
func poolAskingForReservations(opts ...func(*v1alpha1.LVMSharedVolumeGroup)) *v1alpha1.LVMSharedVolumeGroup {
	group := ownedGroup()
	group.Spec.PersistentReservations = PersistentReservationsRequired
	group.Spec.Nodes = []string{testNode, "second-node"}
	for _, opt := range opts {
		opt(group)
	}
	return group
}

func nodeSaid(name string, ready bool, state string) v1alpha1.LVMSharedVolumeGroupNodeStatus {
	return v1alpha1.LVMSharedVolumeGroupNodeStatus{
		Name:                   name,
		PersistentReservations: &v1alpha1.NodePersistentReservations{Ready: ready, State: state},
	}
}

func TestTheOneWayDoorIsNotOpenedWhileAMemberCannotTakePart(t *testing.T) {
	// After `vgchange --setpersist require` the group answers "Persistent
	// reservation is not started" to everything until `--persist start` works.
	// A member that cannot register would be left outside a pool the others are
	// holding, and the way back is another maintenance window — so the verdicts
	// are read first, and a member that has not answered counts as not ready.
	fakeSysBlockWithLUN(t, 8192)
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	group := poolAskingForReservations(func(g *v1alpha1.LVMSharedVolumeGroup) {
		g.Status = &v1alpha1.LVMSharedVolumeGroupStatus{
			Nodes: []v1alpha1.LVMSharedVolumeGroupNodeStatus{
				nodeSaid(testNode, true, PRStateOff),
				nodeSaid("second-node", false, PRStateOff),
			},
		}
	})
	r, cl, _ := testReconciler(t, nodeWith(map[string]string{SanlockHostIDAnnotation: "7"}), commands, nil, group)

	// No VGSetPersist expectation: attempting it is the failure this guards.
	res := reconcile(t, r, group)

	assert.NotZero(t, res.RequeueAfter, "the member may become ready, so this keeps looking")

	published := &v1alpha1.LVMSharedVolumeGroup{}
	require.NoError(t, cl.Get(context.Background(), client.ObjectKey{Name: group.Name}, published))
	entry := entryOf(t, published)
	assert.Equal(t, ReasonPRNotReady, entry.Reason)
	assert.Contains(t, entry.Message, "second-node")
}

func TestTheExecutorWaitsForTheMembersToStepAside(t *testing.T) {
	// `vgchange --setlockargs persist` checks this twice and differently — keys
	// still on the array, and sanlock not yet convinced the neighbours have gone
	// — so the switch starts only once they say they have.
	fakeSysBlockWithLUN(t, 8192)
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	group := poolAskingForReservations(func(g *v1alpha1.LVMSharedVolumeGroup) {
		g.Status = &v1alpha1.LVMSharedVolumeGroupStatus{
			Nodes: []v1alpha1.LVMSharedVolumeGroupNodeStatus{
				nodeSaid(testNode, true, PRStateOff),
				nodeSaid("second-node", true, PRStateOff),
			},
		}
	})
	r, _, _ := testReconciler(t, nodeWith(map[string]string{SanlockHostIDAnnotation: "7"}), commands, nil, group)

	// No VGSetPersist expectation: the neighbour still holds the group.
	res := reconcile(t, r, group)

	assert.NotZero(t, res.RequeueAfter)
}

func TestTheSwitchRunsInTheOrderTheArrayAccepts(t *testing.T) {
	// Not a design — what lvm2 and the array accept, established by running it:
	// --setpersist require (which needs this node's own lockspace up), then
	// --persist start, then --lock-start, then --setlockargs persist.
	fakeSysBlockWithLUN(t, 8192)
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	group := poolAskingForReservations(func(g *v1alpha1.LVMSharedVolumeGroup) {
		g.Status = &v1alpha1.LVMSharedVolumeGroupStatus{
			Nodes: []v1alpha1.LVMSharedVolumeGroupNodeStatus{
				nodeSaid(testNode, true, PRStateOff),
				nodeSaid("second-node", true, PRStateStopped),
			},
		}
	})
	r, cl, _ := testReconciler(t, nodeWith(map[string]string{SanlockHostIDAnnotation: "7"}), commands, nil, group)

	// The executor's own lockspace has to be up before the first command —
	// --setpersist require fails without one — so a start may happen ahead of
	// the sequence as well as inside it.
	commands.EXPECT().VGLockStart(gomock.Any(), testVG, 7).Return("vgchange --lock-start", nil).AnyTimes()
	commands.EXPECT().VGPersistSetting(gomock.Any(), testVG).Return("", nil).AnyTimes()

	setPersist := commands.EXPECT().VGSetPersist(gomock.Any(), testVG, gomock.Any()).Return("vgchange --setpersist require", nil)
	persistStart := commands.EXPECT().VGPersistStart(gomock.Any(), testVG, gomock.Any()).
		After(setPersist).Return("vgchange --persist start", nil)
	setLockArgs := commands.EXPECT().VGSetLockArgsPersist(gomock.Any(), testVG, gomock.Any()).
		After(persistStart).Return("vgchange --setlockargs persist", nil)
	// And the executor comes back: --setlockargs persist stops its lockspace and
	// drops its registration as part of writing the new lock args.
	commands.EXPECT().VGPersistStart(gomock.Any(), testVG, gomock.Any()).
		After(setLockArgs).Return("vgchange --persist start", nil)

	reconcile(t, r, group)

	published := &v1alpha1.LVMSharedVolumeGroup{}
	require.NoError(t, cl.Get(context.Background(), client.ObjectKey{Name: group.Name}, published))
	entry := entryOf(t, published)
	require.NotNil(t, entry.PersistentReservations)
	assert.Equal(t, PRStateEnabled, entry.PersistentReservations.State)
}

func TestAGroupLeftBetweenStatesSaysSoAndKeepsTrying(t *testing.T) {
	// The one place this module opens a door it cannot close. Between a
	// successful --setpersist require and a successful --persist start the group
	// answers "Persistent reservation is not started" to everything, so giving
	// up here would leave the pool unusable.
	fakeSysBlockWithLUN(t, 8192)
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	group := poolAskingForReservations(func(g *v1alpha1.LVMSharedVolumeGroup) {
		g.Status = &v1alpha1.LVMSharedVolumeGroupStatus{
			Nodes: []v1alpha1.LVMSharedVolumeGroupNodeStatus{
				nodeSaid(testNode, true, PRStateOff),
				nodeSaid("second-node", true, PRStateStopped),
			},
		}
	})
	r, cl, _ := testReconciler(t, nodeWith(map[string]string{SanlockHostIDAnnotation: "7"}), commands, nil, group)

	commands.EXPECT().VGLockStart(gomock.Any(), testVG, 7).Return("vgchange --lock-start", nil).AnyTimes()
	commands.EXPECT().VGPersistSetting(gomock.Any(), testVG).Return("", nil).AnyTimes()
	commands.EXPECT().VGSetPersist(gomock.Any(), testVG, gomock.Any()).Return("vgchange --setpersist require", nil)
	commands.EXPECT().VGPersistStart(gomock.Any(), testVG, gomock.Any()).
		Return("vgchange --persist start", assert.AnError)

	res := reconcile(t, r, group)

	assert.NotZero(t, res.RequeueAfter, "giving up here leaves the pool unusable")

	published := &v1alpha1.LVMSharedVolumeGroup{}
	require.NoError(t, cl.Get(context.Background(), client.ObjectKey{Name: group.Name}, published))
	entry := entryOf(t, published)
	assert.Equal(t, ReasonPRSwitchIncomplete, entry.Reason)
	assert.Contains(t, entry.Message, "unusable")
}

func TestAMemberDoesNotStepAsideWhileItServesAVolume(t *testing.T) {
	// Stepping aside means giving up the lockspace, and a node cannot do that
	// under an active volume without leaving it mapped with no lock behind it.
	// The switch waits for the workload to be evacuated rather than taking it
	// down.
	fakeSysBlockWithLUN(t, 8192)
	fakeActiveLV(t, "vol1")
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	group := poolAskingForReservations(func(g *v1alpha1.LVMSharedVolumeGroup) {
		g.Spec.MetadataOwner = "second-node"
	})
	r, cl, _ := testReconciler(t, nodeWith(map[string]string{SanlockHostIDAnnotation: "7"}), commands, nil, group)

	// No VGPersistStop and no VGLockStop: giving up the lockspace here is the
	// failure this guards.
	res := reconcile(t, r, group)

	assert.NotZero(t, res.RequeueAfter)

	published := &v1alpha1.LVMSharedVolumeGroup{}
	require.NoError(t, cl.Get(context.Background(), client.ObjectKey{Name: group.Name}, published))
	entry := entryOf(t, published)
	assert.Equal(t, ReasonPRWaitingForVolumes, entry.Reason)
	assert.Contains(t, entry.Message, "Evacuate")
}

// entryOf finds what a given node published, because the order of the entries is
// the API server's business and not a test's.
func entryOf(t *testing.T, group *v1alpha1.LVMSharedVolumeGroup) v1alpha1.LVMSharedVolumeGroupNodeStatus {
	t.Helper()
	require.NotNil(t, group.Status)
	for _, entry := range group.Status.Nodes {
		if entry.Name == testNode {
			return entry
		}
	}
	t.Fatalf("node %s said nothing", testNode)
	return v1alpha1.LVMSharedVolumeGroupNodeStatus{}
}

func TestTheSwitchCarriesThisNodesHostID(t *testing.T) {
	// lvm2 derives the key a node registers with from its host id, and reports
	// its absence as "A local pr_key or host_id is required to use PR (see
	// lvmlocal.conf)". The file cannot hold it — it is baked into the image, and
	// the id is the one thing that differs per node — so every reservation
	// command carries it, the way lock-start already does.
	fakeSysBlockWithLUN(t, 8192)
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	group := poolAskingForReservations(func(g *v1alpha1.LVMSharedVolumeGroup) {
		g.Status = &v1alpha1.LVMSharedVolumeGroupStatus{
			Nodes: []v1alpha1.LVMSharedVolumeGroupNodeStatus{
				nodeSaid(testNode, true, PRStateOff),
				nodeSaid("second-node", true, PRStateStopped),
			},
		}
	})
	r, _, _ := testReconciler(t, nodeWith(map[string]string{SanlockHostIDAnnotation: "7"}), commands, nil, group)

	commands.EXPECT().VGLockStart(gomock.Any(), testVG, 7).Return("", nil).AnyTimes()
	commands.EXPECT().VGPersistSetting(gomock.Any(), testVG).Return("", nil).AnyTimes()
	commands.EXPECT().VGSetPersist(gomock.Any(), testVG, 7).Return("", nil)
	commands.EXPECT().VGPersistStart(gomock.Any(), testVG, 7).Return("", nil).Times(2)
	commands.EXPECT().VGSetLockArgsPersist(gomock.Any(), testVG, 7).Return("", nil)

	reconcile(t, r, group)
}

func TestAMemberWithoutAHostIDIsNotTakenThroughTheDoor(t *testing.T) {
	// Every reservation command would stop before it started, and the group
	// would be left needing them.
	fakeSysBlockWithLUN(t, 8192)
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	group := poolAskingForReservations(func(g *v1alpha1.LVMSharedVolumeGroup) {
		g.Status = &v1alpha1.LVMSharedVolumeGroupStatus{
			Nodes: []v1alpha1.LVMSharedVolumeGroupNodeStatus{
				nodeSaid(testNode, true, PRStateOff),
				nodeSaid("second-node", true, PRStateStopped),
			},
		}
	})
	// No host id on the node, and so no VGSetPersist expectation.
	r, cl, _ := testReconciler(t, nodeWith(nil), commands, nil, group)

	res := reconcile(t, r, group)

	assert.NotZero(t, res.RequeueAfter, "the id arrives from the pool controller, so this keeps looking")

	published := &v1alpha1.LVMSharedVolumeGroup{}
	require.NoError(t, cl.Get(context.Background(), client.ObjectKey{Name: group.Name}, published))
	assert.Equal(t, ReasonPRNotReady, entryOf(t, published).Reason)
}

func TestAMemberThatStepsAsideStopsSayingItsLockspaceIsRunning(t *testing.T) {
	// Found on the stand: two members reported `Stopped` and
	// `lockspaceStarted: true` in the same entry, because the flag was read out
	// of the status about to be overwritten. Everything that reads it reads it
	// to decide whether the node still holds leases — the pool's readiness, the
	// removal protocol, and the moment a LUN may be taken away from a node.
	fakeSysBlockWithLUN(t, 8192)
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	group := poolAskingForReservations(func(g *v1alpha1.LVMSharedVolumeGroup) {
		g.Spec.MetadataOwner = "second-node"
		g.Status = &v1alpha1.LVMSharedVolumeGroupStatus{
			Nodes: []v1alpha1.LVMSharedVolumeGroupNodeStatus{
				// This node is in the lockspace and says so, which is what it
				// has to stop saying once it steps aside.
				{Name: testNode, LockspaceStarted: true,
					PersistentReservations: &v1alpha1.NodePersistentReservations{Ready: true, State: PRStateOff}},
				nodeSaid("second-node", true, PRStateOff),
			},
		}
	})
	r, cl, _ := testReconciler(t, nodeWith(map[string]string{SanlockHostIDAnnotation: "7"}), commands, nil, group)

	commands.EXPECT().VGPersistStop(gomock.Any(), testVG, 7).Return("", nil)
	commands.EXPECT().VGLockStop(gomock.Any(), testVG).Return("", nil)

	reconcile(t, r, group)

	published := &v1alpha1.LVMSharedVolumeGroup{}
	require.NoError(t, cl.Get(context.Background(), client.ObjectKey{Name: group.Name}, published))
	entry := entryOf(t, published)
	assert.Equal(t, PRStateStopped, entry.PersistentReservations.State)
	assert.False(t, entry.LockspaceStarted, "it has just left the lockspace")
}

func TestASwitchInterruptedBehindTheDoorIsResumedRatherThanRestarted(t *testing.T) {
	// Once `--setpersist require` has succeeded the group answers "Cannot access
	// VG due to failed lock" to everything that takes a lock — `--setpersist`
	// among them. A procedure that always started at the beginning could never
	// finish what it had started, and the pool would stay unusable for as long
	// as it kept trying. Found on the stand, from inside that state.
	fakeSysBlockWithLUN(t, 8192)
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	group := poolAskingForReservations(func(g *v1alpha1.LVMSharedVolumeGroup) {
		g.Status = &v1alpha1.LVMSharedVolumeGroupStatus{
			Nodes: []v1alpha1.LVMSharedVolumeGroupNodeStatus{
				nodeSaid(testNode, true, PRStateOff),
				nodeSaid("second-node", true, PRStateStopped),
			},
		}
	})
	r, _, _ := testReconciler(t, nodeWith(map[string]string{SanlockHostIDAnnotation: "7"}), commands, nil, group)

	commands.EXPECT().VGPersistSetting(gomock.Any(), testVG).Return(PersistRequired, nil).AnyTimes()
	// No VGSetPersist: repeating it is what fails, and there is nothing to set.
	commands.EXPECT().VGPersistStart(gomock.Any(), testVG, 7).Return("", nil).Times(2)
	commands.EXPECT().VGLockStart(gomock.Any(), testVG, 7).Return("", nil).AnyTimes()
	commands.EXPECT().VGSetLockArgsPersist(gomock.Any(), testVG, 7).Return("", nil)

	reconcile(t, r, group)
}

func TestANodeUnderReservationsRegistersBeforeItStartsItsLockspace(t *testing.T) {
	// The order is not ours to choose: under reservations the sanlock lease is
	// renewed with a SCSI command an unregistered initiator may not issue, so a
	// lockspace started without a registration cannot be kept — the node reads
	// the LUN, fails every write, and is fenced by its own watchdog.
	fakeSysBlockWithLUN(t, 8192)
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	group := testGroup(testNode)
	group.Spec.PersistentReservations = PersistentReservationsRequired
	group.Status = &v1alpha1.LVMSharedVolumeGroupStatus{
		Nodes: []v1alpha1.LVMSharedVolumeGroupNodeStatus{nodeSaid(testNode, true, PRStateEnabled)},
	}

	register := commands.EXPECT().VGPersistStart(gomock.Any(), testVG, 7).Return("", nil)
	commands.EXPECT().VGLockStart(gomock.Any(), testVG, 7).After(register).Return("", nil)

	r, _, _ := testReconciler(t, nodeWith(map[string]string{SanlockHostIDAnnotation: "7"}), commands, nil, group)
	reconcile(t, r, group)
}
