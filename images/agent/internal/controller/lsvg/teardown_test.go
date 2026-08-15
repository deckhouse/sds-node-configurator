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
	"errors"
	"testing"

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

	// No RemoveVGShared expectation: attempting it is the failure this guards.
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

	commands.EXPECT().RemoveVGShared(gomock.Any(), testVG).Return("vgremove", nil)

	reconcile(t, r, group)

	published := &v1alpha1.LVMSharedVolumeGroup{}
	err := cl.Get(context.Background(), client.ObjectKey{Name: group.Name}, published)
	if err == nil {
		assert.Empty(t, published.Finalizers, "the group is gone, so nothing holds the resource back")
	}
}
