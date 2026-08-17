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
	"bytes"
	"context"
	"errors"
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
	fakeActiveLV(t, "vol1")
	fakeActiveLV(t, "vol2")
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

func TestAnErrorTargetSomethingStillHoldsOpenIsPublished(t *testing.T) {
	// Seen on hardware: the paths came back, the recovery stopped the dead
	// lockspace and could not remove the volume's error target because kubelet
	// still had the mount of a pod nobody had deleted. Waiting is the right
	// answer — the map must not be removed under a stale page cache while the
	// lock belongs to another node — but the wait was published nowhere, so the
	// pool showed a member that would not come back and no reason for it.
	fakeSysBlockWithLUN(t, 8192)
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	group := ownedGroup()
	r, cl, dir := testReconciler(t, nodeWith(map[string]string{SanlockHostIDAnnotation: "7"}), commands, nil, group)
	const volumeMap = testVG + "-pvc--52529f60"
	fencedNode(t, dir, volumeMap)

	commands.EXPECT().RemoveDMDevice(gomock.Any(), volumeMap).
		Return("dmsetup remove", errors.New("Device or resource busy"))
	commands.EXPECT().CreateVGShared(gomock.Any(), gomock.Any()).Return("vgcreate", nil).AnyTimes()
	commands.EXPECT().VGLockStart(gomock.Any(), testVG, 7).Return("vgchange --lock-start", nil).AnyTimes()

	res := reconcile(t, r, group)

	assert.NotZero(t, res.RequeueAfter, "the opener lets go on its own, and the next pass finishes this")

	published := &v1alpha1.LVMSharedVolumeGroup{}
	require.NoError(t, cl.Get(context.Background(), client.ObjectKey{Name: group.Name}, published))
	require.NotNil(t, published.Status)
	require.NotEmpty(t, published.Status.Nodes)
	assert.False(t, published.Status.Nodes[0].LockspaceStarted)
	assert.Equal(t, ReasonBarrierNotCleared, published.Status.Nodes[0].Reason)
	assert.Contains(t, published.Status.Nodes[0].Message, volumeMap,
		"the message names the map, which is what the operator has to look for")
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

func TestAReservationOnTheLUNIsNamedForWhatItIs(t *testing.T) {
	// Most reasons a lockspace does not start resolve on their own, and this one
	// never does: a SCSI reservation left by another initiator lets the node read
	// the volume group and refuses its writes, so the delta lease — taken with
	// COMPARE AND WRITE — comes back as a conflict. Retrying that every thirty
	// seconds with nothing published is indistinguishable from a healthy node.
	fakeSysBlockWithLUN(t, 8192)
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	group := testGroup(testNode)
	commands.EXPECT().VGLockStart(gomock.Any(), testVG, 7).
		Return("vgchange --lock-start", errors.New("add_lockspace fail result -286"))
	r, cl, _ := testReconciler(t, nodeWith(map[string]string{SanlockHostIDAnnotation: "7"}), commands, nil, group)

	res := reconcile(t, r, group)

	assert.NotZero(t, res.RequeueAfter, "the retry stays: the reservation may be cleared at any moment")

	published := &v1alpha1.LVMSharedVolumeGroup{}
	require.NoError(t, cl.Get(context.Background(), client.ObjectKey{Name: group.Name}, published))
	require.NotNil(t, published.Status)
	require.NotEmpty(t, published.Status.Nodes)
	assert.Equal(t, testNode, published.Status.Nodes[0].Name)
	assert.False(t, published.Status.Nodes[0].LockspaceStarted)
	assert.Equal(t, ReasonReservationConflict, published.Status.Nodes[0].Reason)
	assert.Contains(t, published.Status.Nodes[0].Message, "sg_persist",
		"the message names the command that shows the reservation")
	assert.Contains(t, published.Status.Nodes[0].Message, "does not clear it",
		"clearing somebody else's registration is not this module's decision")
}

func TestAFencedLeaseAreaDoesNotTrapTheNodeInARetry(t *testing.T) {
	// A handler that covered the lease area too — an older one, or one written
	// by hand — leaves a map sanlock still holds open. Removing it answers
	// "Device or resource busy", and a node that only retries that answers it
	// every thirty seconds for the rest of its life while looking healthy.
	//
	// The lockspace is dead the moment its lease storage becomes an error
	// target, so it is stopped rather than waited on, and what the kernel still
	// refuses is handed to the kernel to do on close.
	fakeSysBlockWithLUN(t, 8192)
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	group := ownedGroup()
	r, _, dir := testReconciler(t, nodeWith(map[string]string{SanlockHostIDAnnotation: "7"}), commands, nil, group)
	leaseMap := utils.DMName(testVG, utils.LeaseAreaLVName)
	fakeActiveLV(t, utils.LeaseAreaLVName)
	fencedNode(t, dir, leaseMap)

	busy := errors.New("device-mapper: remove ioctl on " + leaseMap + " failed: Device or resource busy")
	stopped := commands.EXPECT().VGLockStop(gomock.Any(), testVG).Return("vgchange --lock-stop", nil)
	commands.EXPECT().RemoveDMDevice(gomock.Any(), leaseMap).After(stopped).Return("dmsetup remove", busy)
	commands.EXPECT().RemoveDMDeviceDeferred(gomock.Any(), leaseMap).Return("dmsetup remove --deferred", nil)
	commands.EXPECT().CreateVGShared(gomock.Any(), gomock.Any()).Return("vgcreate", nil).AnyTimes()
	commands.EXPECT().VGLockStart(gomock.Any(), testVG, 7).Return("vgchange --lock-start", nil).AnyTimes()

	res := reconcile(t, r, group)

	assert.NotZero(t, res.RequeueAfter, "the map goes on the last close, and the pass after that finishes the recovery")
	_, err := os.Stat(filepath.Join(dir, "killpath-"+testVG+".json"))
	assert.NoError(t, err, "the node has not rejoined yet, and the record says so")
}

func TestALeaseAreaNobodyCanUnmapIsStatedRatherThanRetried(t *testing.T) {
	// Everything reversible is spent by here: the lockspace was stopped, the
	// removal refused, and the kernel would not take it on close either. What
	// is left — restarting the lock daemons of this node — has a blast radius,
	// so the module says what it sees instead of taking the decision.
	fakeSysBlockWithLUN(t, 8192)
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	group := ownedGroup()
	r, cl, dir := testReconciler(t, nodeWith(map[string]string{SanlockHostIDAnnotation: "7"}), commands, nil, group)
	leaseMap := utils.DMName(testVG, utils.LeaseAreaLVName)
	fakeActiveLV(t, utils.LeaseAreaLVName)
	fencedNode(t, dir, leaseMap)

	commands.EXPECT().VGLockStop(gomock.Any(), testVG).Return("vgchange --lock-stop", nil)
	commands.EXPECT().RemoveDMDevice(gomock.Any(), leaseMap).Return("dmsetup remove", errors.New("Device or resource busy"))
	commands.EXPECT().RemoveDMDeviceDeferred(gomock.Any(), leaseMap).
		Return("dmsetup remove --deferred", errors.New("Invalid argument"))
	commands.EXPECT().CreateVGShared(gomock.Any(), gomock.Any()).Return("vgcreate", nil).AnyTimes()
	commands.EXPECT().VGLockStart(gomock.Any(), testVG, 7).Return("vgchange --lock-start", nil).AnyTimes()

	reconcile(t, r, group)

	published := &v1alpha1.LVMSharedVolumeGroup{}
	require.NoError(t, cl.Get(context.Background(), client.ObjectKey{Name: group.Name}, published))
	require.NotNil(t, published.Status)
	require.NotEmpty(t, published.Status.Nodes)
	assert.Equal(t, ReasonBarrierNotCleared, published.Status.Nodes[0].Reason)
	assert.Contains(t, published.Status.Nodes[0].Message, "No volume of this group is at risk",
		"a node out of the pool is not a node losing data, and the message must not read like one")
}

func TestAMapThatIsAlreadyGoneFinishesTheRecovery(t *testing.T) {
	// The pass after a deferred removal fires finds no map to remove, and the
	// removal says so by succeeding: device-mapper answers "No such device" and
	// the command treats that as the outcome it was asked for. A recovery that
	// read its own success as a failure would keep the node out of the pool
	// forever — which is the state the whole barrier return exists to end.
	fakeSysBlockWithLUN(t, 8192)
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	group := ownedGroup()
	r, _, dir := testReconciler(t, nodeWith(map[string]string{SanlockHostIDAnnotation: "7"}), commands, nil, group)
	fencedNode(t, dir, utils.DMName(testVG, utils.LeaseAreaLVName), "vgshared-vol1")

	// Both removals report the missing mapping as success, which is what the
	// real command does.
	commands.EXPECT().RemoveDMDevice(gomock.Any(), gomock.Any()).Return("dmsetup remove", nil).Times(2)
	commands.EXPECT().CreateVGShared(gomock.Any(), gomock.Any()).Return("vgcreate", nil).AnyTimes()
	commands.EXPECT().VGLockStart(gomock.Any(), testVG, 7).Return("vgchange --lock-start", nil).AnyTimes()
	commands.EXPECT().GetVG(testVG).Return(internal.VGData{VGName: testVG, VGUUID: "vg-uuid"}, "vgs", bytes.Buffer{}, nil).AnyTimes()
	commands.EXPECT().VGLockStop(gomock.Any(), testVG).Return("vgchange --lock-stop", nil).AnyTimes()

	reconcile(t, r, group)

	_, err := os.Stat(filepath.Join(dir, "killpath-"+testVG+".json"))
	assert.True(t, os.IsNotExist(err), "with the last error target gone, the node was never fenced any more")
}

func TestOneNodeSayingWhatItSeesDoesNotSpeakForTheOthers(t *testing.T) {
	// Whether a lockspace is running is a fact about a node, and every member of
	// a pool has its own answer. Written as a condition on the group it would be
	// one value with three authors: true, and useless to a reader who cannot
	// tell whose answer arrived last.
	fakeSysBlockWithLUN(t, 8192)
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)

	// A member that does not own the metadata, so the pass goes straight to the
	// lock start and to what this node has to say about it.
	group := testGroup(testNode)
	group.Status = &v1alpha1.LVMSharedVolumeGroupStatus{
		Nodes: []v1alpha1.LVMSharedVolumeGroupNodeStatus{{
			Name:             "another-node",
			LockspaceStarted: true,
			Reason:           ReasonLockspaceStarted,
		}},
	}
	r, cl, _ := testReconciler(t, nodeWith(map[string]string{SanlockHostIDAnnotation: "7"}), commands, nil, group)

	commands.EXPECT().CreateVGShared(gomock.Any(), gomock.Any()).Return("vgcreate", nil).AnyTimes()
	commands.EXPECT().VGLockStart(gomock.Any(), testVG, 7).
		Return("vgchange --lock-start", errors.New("add_lockspace fail result -286")).AnyTimes()

	reconcile(t, r, group)

	published := &v1alpha1.LVMSharedVolumeGroup{}
	require.NoError(t, cl.Get(context.Background(), client.ObjectKey{Name: group.Name}, published))
	require.Len(t, published.Status.Nodes, 2, "the other node's answer is still there")

	byName := map[string]v1alpha1.LVMSharedVolumeGroupNodeStatus{}
	for _, node := range published.Status.Nodes {
		byName[node.Name] = node
	}
	assert.True(t, byName["another-node"].LockspaceStarted, "and it is still its own answer")
	assert.False(t, byName[testNode].LockspaceStarted)
	assert.Equal(t, ReasonReservationConflict, byName[testNode].Reason)
}
