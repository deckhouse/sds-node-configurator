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
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/mock_utils"
)

// deletedGroup is a pool whose resource is on its way out: the finalizer is what
// keeps it around while the nodes unwind.
func deletedGroup(opts ...func(*v1alpha1.LVMSharedVolumeGroup)) *v1alpha1.LVMSharedVolumeGroup {
	group := ownedGroup()
	now := metav1.Now()
	group.DeletionTimestamp = &now
	group.Finalizers = []string{internal.SdsNodeConfiguratorFinalizer}
	for _, opt := range opts {
		opt(group)
	}
	return group
}

func TestAPoolWithVolumesInItIsNotRemoved(t *testing.T) {
	// The group holds somebody's data. Deleting the resource that describes a
	// pool is not a decision to destroy what is stored in it: the volumes have
	// resources of their own, and deleting those is how they go.
	fakeSysBlockWithLUN(t, 8192)
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	group := deletedGroup()
	volume := &v1alpha1.LVMSharedLogicalVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "vol-1"},
		Spec:       v1alpha1.LVMSharedLogicalVolumeSpec{LVMSharedVolumeGroupName: group.Name},
	}
	r, cl, _ := testReconciler(t, nodeWith(map[string]string{SanlockHostIDAnnotation: "7"}), commands, nil, group, volume)

	// No RemoveVGShared and no VGLockStop, and the second one is the point:
	// removing a volume takes the group's lock, so a teardown that stopped the
	// lockspace before the group was empty would leave the pool unable to delete
	// the volumes its own message asks an operator to delete. Measured on the
	// stand exactly that way — the owner stopped, and the volume's cleanup
	// failed from then on, whatever anybody did next.
	//
	// The lockspace is started instead, because a pool that cannot lose its
	// volumes cannot be removed: refraining from stopping it fixes the next
	// pool and leaves this one stuck.
	commands.EXPECT().VGLockStart(gomock.Any(), testVG, 7).Return("vgchange --lock-start", nil)

	res := reconcile(t, r, group)

	assert.NotZero(t, res.RequeueAfter, "the volumes may still be deleted, and then the group goes")

	published := &v1alpha1.LVMSharedVolumeGroup{}
	require.NoError(t, cl.Get(context.Background(), client.ObjectKey{Name: group.Name}, published))
	require.NotEmpty(t, published.Finalizers, "the resource stays while the group is still there")
	require.NotEmpty(t, published.Status.Nodes)
	assert.Equal(t, ReasonVolumesRemain, published.Status.Nodes[0].Reason)
	assert.Contains(t, published.Status.Nodes[0].Message, "LVMSharedLogicalVolume")
}

func TestTheOwnerWaitsForTheOtherMembersToLeaveTheLockspace(t *testing.T) {
	// vgremove answers "Lockspace for ... not stopped on other hosts" until every
	// other member has left, and no node can stop another node's lockspace. So
	// the owner waits for what the members say about themselves.
	fakeSysBlockWithLUN(t, 8192)
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	group := deletedGroup(func(g *v1alpha1.LVMSharedVolumeGroup) {
		g.Status = &v1alpha1.LVMSharedVolumeGroupStatus{
			Nodes: []v1alpha1.LVMSharedVolumeGroupNodeStatus{
				{Name: "another-node", LockspaceStarted: true},
			},
		}
	})
	r, _, _ := testReconciler(t, nodeWith(map[string]string{SanlockHostIDAnnotation: "7"}), commands, nil, group)

	// The owner keeps its own lockspace — vgremove needs it — so starting it is
	// expected even while it waits for the others.
	commands.EXPECT().VGLockStart(gomock.Any(), testVG, 7).Return("vgchange --lock-start", nil).AnyTimes()

	// No RemoveVGShared expectation: running it now is what earns the refusal.
	res := reconcile(t, r, group)

	assert.NotZero(t, res.RequeueAfter, "the other member leaves on its own, and then this proceeds")
}

func TestTheLockManagerAskingForTimeIsNotAFailure(t *testing.T) {
	// sanlock will not vouch for the absence of other owners until it has sat out
	// its own interval, and says so with "unknown host state (wait and retry)".
	// Treating that as a failed removal would leave the pool half-deleted and
	// somebody reading a log for a fault that is not there.
	fakeSysBlockWithLUN(t, 8192)
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	group := deletedGroup()
	r, cl, _ := testReconciler(t, nodeWith(map[string]string{SanlockHostIDAnnotation: "7"}), commands, nil, group)

	commands.EXPECT().VGLockStart(gomock.Any(), testVG, 7).Return("vgchange --lock-start", nil).AnyTimes()
	commands.EXPECT().RemoveVGShared(gomock.Any(), testVG).
		Return("vgremove", errors.New("unknown host state (wait and retry)"))

	res := reconcile(t, r, group)

	assert.NotZero(t, res.RequeueAfter)

	published := &v1alpha1.LVMSharedVolumeGroup{}
	require.NoError(t, cl.Get(context.Background(), client.ObjectKey{Name: group.Name}, published))
	assert.NotEmpty(t, published.Finalizers, "nothing is finished, so nothing is released")
	if published.Status != nil {
		for _, node := range published.Status.Nodes {
			assert.NotEqual(t, ReasonRemovalFailed, node.Reason,
				"the protocol asking for time is not a failure to report")
		}
	}
}

func TestAnEmptyPoolIsRemovedAndTheResourceReleased(t *testing.T) {
	// The whole point: the Volume Group lives on a LUN that outlives the
	// resource, so deleting the resource has to take the group with it — and the
	// finalizer goes last, because it is what keeps the work from being
	// forgotten halfway through.
	fakeSysBlockWithLUN(t, 8192)
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	group := deletedGroup()
	r, cl, _ := testReconciler(t, nodeWith(map[string]string{SanlockHostIDAnnotation: "7"}), commands, nil, group)

	// The owner does not stop its own lockspace before the removal: vgremove is
	// run from a node that holds one, and an owner that stopped first got
	// "Cannot access VG due to failed lock" on the stand and never recovered.
	// No VGLockStop expectation is declared, so attempting it fails this test.
	commands.EXPECT().VGLockStart(gomock.Any(), testVG, 7).Return("vgchange --lock-start", nil).AnyTimes()
	commands.EXPECT().RemoveVGShared(gomock.Any(), testVG).Return("vgremove", nil)

	reconcile(t, r, group)

	published := &v1alpha1.LVMSharedVolumeGroup{}
	err := cl.Get(context.Background(), client.ObjectKey{Name: group.Name}, published)
	if err == nil {
		assert.Empty(t, published.Finalizers, "the group is gone, so nothing holds the resource back")
	}
}

func TestAPoolGainsTheDeviceItsSpecGained(t *testing.T) {
	// A pool grows by being given another LUN, and nothing else in this
	// reconciler would ever notice: the group exists, so every pass used to stop
	// right there.
	fakeSysBlockWithLUN(t, 8192)
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	group := ownedGroup()
	commands.EXPECT().GetVG(testVG).Return(internal.VGData{VGName: testVG, VGUUID: "vg-uuid"}, "vgs", bytes.Buffer{}, nil).AnyTimes()
	commands.EXPECT().GetAllPVs(gomock.Any()).
		Return([]internal.PVData{{PVName: "/dev/mapper/other", VGName: testVG}}, "pvs", bytes.Buffer{}, nil).AnyTimes()
	r, _, _ := testReconciler(t, nodeWith(map[string]string{SanlockHostIDAnnotation: "7"}), commands, nil, group)

	// lvm knows the group and its current device; the spec's device is not in it.
	commands.EXPECT().ExtendVGShared(gomock.Any(), testVG, []string{"/dev/mapper/mpathi"}).Return("vgextend", nil)
	commands.EXPECT().VGLockStart(gomock.Any(), testVG, 7).Return("vgchange --lock-start", nil).AnyTimes()

	reconcile(t, r, group)
}

func TestADeviceAlreadyInTheGroupIsNotAddedAgain(t *testing.T) {
	// vgextend refuses a physical volume that is already in, loudly, and a pass
	// that runs it every minute turns a healthy pool into a log full of failures.
	fakeSysBlockWithLUN(t, 8192)
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	group := ownedGroup()
	commands.EXPECT().GetVG(testVG).Return(internal.VGData{VGName: testVG, VGUUID: "vg-uuid"}, "vgs", bytes.Buffer{}, nil).AnyTimes()
	commands.EXPECT().GetAllPVs(gomock.Any()).
		Return([]internal.PVData{{PVName: "/dev/mapper/mpathi", VGName: testVG}}, "pvs", bytes.Buffer{}, nil).AnyTimes()
	r, _, _ := testReconciler(t, nodeWith(map[string]string{SanlockHostIDAnnotation: "7"}), commands, nil, group)

	commands.EXPECT().VGLockStart(gomock.Any(), testVG, 7).Return("vgchange --lock-start", nil).AnyTimes()

	// No ExtendVGShared expectation: there is nothing to add.
	reconcile(t, r, group)
}

func TestAGroupWhoseDevicesCannotBeListedIsNotExtended(t *testing.T) {
	// Not knowing what the group already holds is not permission to add to it:
	// every device would look missing, and vgextend would run against the ones
	// already in. This is the same shape as reading lvm's "[unknown]" as a name.
	fakeSysBlockWithLUN(t, 8192)
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	group := ownedGroup()
	commands.EXPECT().GetVG(testVG).Return(internal.VGData{VGName: testVG, VGUUID: "vg-uuid"}, "vgs", bytes.Buffer{}, nil).AnyTimes()
	commands.EXPECT().GetAllPVs(gomock.Any()).
		Return(nil, "pvs", bytes.Buffer{}, errors.New("lvm is not answering")).AnyTimes()
	r, _, _ := testReconciler(t, nodeWith(map[string]string{SanlockHostIDAnnotation: "7"}), commands, nil, group)

	commands.EXPECT().VGLockStart(gomock.Any(), testVG, 7).Return("vgchange --lock-start", nil).AnyTimes()

	// No ExtendVGShared expectation: silence is not an empty group.
	reconcile(t, r, group)
}

func TestADeletedPoolWhoseLockspaceIsDownGetsItBack(t *testing.T) {
	// The recovery half. A pool that entered the deadlock — lockspace stopped
	// while a volume remained — cannot get out of it on its own unless the
	// lockspace comes back: the volume needs the group's lock to go, and until
	// the volume goes the group cannot be removed. Refusing to stop the
	// lockspace fixes the next pool; this is what fixes the one already stuck.
	fakeSysBlockWithLUN(t, 8192)
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	group := deletedGroup()
	volume := &v1alpha1.LVMSharedLogicalVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "vol-1"},
		Spec:       v1alpha1.LVMSharedLogicalVolumeSpec{LVMSharedVolumeGroupName: group.Name},
	}
	// No lockspace annotation on the node: this is a node that has stopped it.
	r, _, _ := testReconciler(t, nodeWith(map[string]string{SanlockHostIDAnnotation: "7"}), commands, nil, group, volume)

	commands.EXPECT().VGLockStart(gomock.Any(), testVG, 7).Return("vgchange --lock-start", nil)

	res := reconcile(t, r, group)

	assert.NotZero(t, res.RequeueAfter, "and it keeps waiting for the volume to be deleted")
}

func TestAGaugeDoesNotOutliveThePoolItDescribes(t *testing.T) {
	// A gauge vec keeps a series until somebody deletes it, so a removed pool
	// would go on reporting its last value — zero, most likely, which reads as
	// "this pool is fine" about a pool that is gone.
	fakeSysBlockWithLUN(t, 8192)
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	group := deletedGroup()
	r, _, _ := testReconciler(t, nodeWith(map[string]string{SanlockHostIDAnnotation: "7"}), commands, nil, group)

	commands.EXPECT().VGLockStart(gomock.Any(), testVG, 7).Return("vgchange --lock-start", nil).AnyTimes()
	commands.EXPECT().RemoveVGShared(gomock.Any(), testVG).Return("vgremove", nil)

	r.metrics.SharedPoolUnlockedMappings(testVG).Set(3)
	require.Equal(t, 3.0, testutil.ToFloat64(r.metrics.SharedPoolUnlockedMappings(testVG)))

	reconcile(t, r, group)

	assert.Equal(t, 0.0, testutil.ToFloat64(r.metrics.SharedPoolUnlockedMappings(testVG)),
		"the series is gone, so asking for it again yields a fresh zero rather than the old three")
}

func TestAMemberLeavingAPoolUnderReservationsGivesUpItsKey(t *testing.T) {
	// `vgremove` on a group held under reservations refuses while anybody else
	// is still registered — "Found 3 PR keys on /dev/mapper/mpathi. Stop PR for
	// VG vghw on other hosts (vgchange --persist stop)" — so a member that
	// leaves without giving its key up leaves a pool nobody can remove. Found on
	// the stand, taking a switched pool down.
	fakeSysBlockWithLUN(t, 8192)
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	group := testGroup("another-node")
	group.Spec.PersistentReservations = PersistentReservationsRequired
	group.Status = &v1alpha1.LVMSharedVolumeGroupStatus{
		Nodes: []v1alpha1.LVMSharedVolumeGroupNodeStatus{nodeSaid(testNode, true, PRStateEnabled)},
	}

	// The lockspace first, the registration after: lvm2 refuses the other order.
	lockStop := commands.EXPECT().VGLockStop(gomock.Any(), testVG).Return("", nil)
	commands.EXPECT().VGPersistStop(gomock.Any(), testVG, gomock.Any()).After(lockStop).Return("", nil)

	node := nodeWith(map[string]string{
		SanlockHostIDAnnotation:                     "7",
		LockspaceStartedAnnotationPrefix + "pool-1": "vg-uuid",
	})
	r, _, _ := testReconciler(t, node, commands, nil, group)

	reconcile(t, r, group)
}

func TestThePoolGivesItsLUNsBackWithoutAReservation(t *testing.T) {
	// Two things need this. A registration left by a member that is already gone
	// blocks the removal outright — "Found 3 PR keys ... Stop PR for VG on other
	// hosts" — and a reservation left behind refuses the next pool's lockspace,
	// which reads as a sanlock fault on a LUN that looks healthy.
	fakeSysBlockWithLUN(t, 8192)
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	group := deletedGroup(func(g *v1alpha1.LVMSharedVolumeGroup) {
		g.Spec.PersistentReservations = PersistentReservationsRequired
		g.Status = &v1alpha1.LVMSharedVolumeGroupStatus{
			Nodes: []v1alpha1.LVMSharedVolumeGroupNodeStatus{nodeSaid(testNode, false, PRStateEnabled)},
		}
	})

	commands.EXPECT().VGLockStop(gomock.Any(), testVG).Return("", nil).AnyTimes()
	commands.EXPECT().VGPersistStop(gomock.Any(), testVG, gomock.Any()).Return("", nil).AnyTimes()
	// The removal restarts the lockspace so the volumes can go, and under
	// reservations that means registering first.
	commands.EXPECT().VGPersistStart(gomock.Any(), testVG, gomock.Any()).Return("", nil).AnyTimes()
	commands.EXPECT().VGLockStart(gomock.Any(), testVG, gomock.Any()).Return("", nil).AnyTimes()
	clear := commands.EXPECT().VGPersistClear(gomock.Any(), testVG, gomock.Any()).Return("", nil)
	commands.EXPECT().RemoveVGShared(gomock.Any(), testVG).After(clear).Return("", nil)

	r, _, _ := testReconciler(t, nodeWith(map[string]string{SanlockHostIDAnnotation: "7"}), commands, nil, group)
	reconcile(t, r, group)
}
