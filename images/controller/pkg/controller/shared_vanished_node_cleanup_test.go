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

package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/controller/internal"
	"github.com/deckhouse/sds-node-configurator/images/controller/pkg/logger"
)

// vanishedTestClient differs from hostIDTestClient in one way that matters: the
// groups have a status subresource, as they do in the cluster, so a status patch
// here goes where it goes there.
func vanishedTestClient(objects ...client.Object) client.WithWatch {
	s := scheme.Scheme
	_ = metav1.AddMetaToScheme(s)
	_ = v1alpha1.AddToScheme(s)
	_ = corev1.AddToScheme(s)

	return fake.NewClientBuilder().WithScheme(s).WithObjects(objects...).
		WithStatusSubresource(&v1alpha1.LVMSharedVolumeGroup{}).Build()
}

func vanishedTestAttachment(name, node string) *v1alpha1.LVMSharedLogicalVolumeAttachment {
	return &v1alpha1.LVMSharedLogicalVolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{
			Name:       name,
			Finalizers: []string{internal.SdsNodeConfiguratorFinalizer},
		},
		Spec: v1alpha1.LVMSharedLogicalVolumeAttachmentSpec{
			LVMSharedLogicalVolumeName: "vol-1",
			NodeName:                   node,
		},
		Status: &v1alpha1.LVMSharedLogicalVolumeAttachmentStatus{Phase: "Attached"},
	}
}

func groupWithMembersInStatus(name string, names ...string) *v1alpha1.LVMSharedVolumeGroup {
	group := hostIDTestGroup(name, "4Mi", names...)
	group.Status = &v1alpha1.LVMSharedVolumeGroupStatus{}
	for _, node := range names {
		group.Status.Nodes = append(group.Status.Nodes, v1alpha1.LVMSharedVolumeGroupNodeStatus{
			Name:             node,
			LockspaceStarted: true,
		})
	}
	return group
}

func TestWhatAVanishedNodeLeftBehindIsReleased(t *testing.T) {
	// A node fenced by the platform is deleted and rebuilt under a new name, so
	// the agent that would have stopped the lockspace, retracted the node's entry
	// and released its attachments is gone with the node. Measured on the stand:
	// the attachment stayed Attached against a node that had not existed for ten
	// minutes, and the group went on listing a member holding leases.
	log, _ := logger.NewLogger(logger.WarningLevel)
	group := groupWithMembersInStatus("vg1", "alive", "vanished")
	cl := vanishedTestClient(
		hostIDTestNode("alive", "1"),
		group,
		vanishedTestAttachment("vol-1.vanished", "vanished"),
		vanishedTestAttachment("vol-1.alive", "alive"),
	)

	require.NoError(t, cleanUpAfterVanishedNodes(context.Background(), cl, *log))

	published := &v1alpha1.LVMSharedVolumeGroup{}
	require.NoError(t, cl.Get(context.Background(), client.ObjectKey{Name: "vg1"}, published))
	require.NotNil(t, published.Status)
	require.Len(t, published.Status.Nodes, 1, "only the node that exists may be listed")
	assert.Equal(t, "alive", published.Status.Nodes[0].Name)

	gone := &v1alpha1.LVMSharedLogicalVolumeAttachment{}
	err := cl.Get(context.Background(), client.ObjectKey{Name: "vol-1.vanished"}, gone)
	assert.True(t, apierrors.IsNotFound(err),
		"the finalizer comes off with the attachment: nothing on that node will ever remove it")

	kept := &v1alpha1.LVMSharedLogicalVolumeAttachment{}
	require.NoError(t, cl.Get(context.Background(), client.ObjectKey{Name: "vol-1.alive"}, kept),
		"an attachment of a node that exists is none of this controller's business")
}

func TestAnEmptyNodeListIsNotEveryNodeVanishing(t *testing.T) {
	// A List that came back short would read as "the cluster has no nodes", and
	// acting on that would release every attachment of every pool at once. An
	// empty cluster does not exist; a failed read looks exactly like one.
	log, _ := logger.NewLogger(logger.WarningLevel)
	cl := vanishedTestClient(
		groupWithMembersInStatus("vg1", "n1"),
		vanishedTestAttachment("vol-1.n1", "n1"),
	)

	require.NoError(t, cleanUpAfterVanishedNodes(context.Background(), cl, *log))

	kept := &v1alpha1.LVMSharedLogicalVolumeAttachment{}
	require.NoError(t, cl.Get(context.Background(), client.ObjectKey{Name: "vol-1.n1"}, kept))
	published := &v1alpha1.LVMSharedVolumeGroup{}
	require.NoError(t, cl.Get(context.Background(), client.ObjectKey{Name: "vg1"}, published))
	require.Len(t, published.Status.Nodes, 1)
}

func TestAHealthyPoolIsNotWrittenTo(t *testing.T) {
	// Every node event of every cluster reaches this controller, and a pass that
	// wrote something on a healthy pool would make each of them a write.
	log, _ := logger.NewLogger(logger.WarningLevel)
	group := groupWithMembersInStatus("vg1", "n1", "n2")
	cl := vanishedTestClient(
		hostIDTestNode("n1", "1"), hostIDTestNode("n2", "2"), group,
		vanishedTestAttachment("vol-1.n1", "n1"),
	)

	before := &v1alpha1.LVMSharedVolumeGroup{}
	require.NoError(t, cl.Get(context.Background(), client.ObjectKey{Name: "vg1"}, before))

	require.NoError(t, cleanUpAfterVanishedNodes(context.Background(), cl, *log))

	after := &v1alpha1.LVMSharedVolumeGroup{}
	require.NoError(t, cl.Get(context.Background(), client.ObjectKey{Name: "vg1"}, after))
	assert.Equal(t, before.ResourceVersion, after.ResourceVersion, "nothing to correct, nothing written")

	kept := &v1alpha1.LVMSharedLogicalVolumeAttachment{}
	require.NoError(t, cl.Get(context.Background(), client.ObjectKey{Name: "vol-1.n1"}, kept))
	assert.Contains(t, kept.Finalizers, internal.SdsNodeConfiguratorFinalizer)
}

func TestNodesAreNotListedWhenNoPoolExists(t *testing.T) {
	// The cheap exit for the ordinary cluster: no shared pools, no attachments,
	// so no reason to list the nodes at all.
	log, _ := logger.NewLogger(logger.WarningLevel)
	cl := vanishedTestClient(hostIDTestNode("n1", ""))

	require.NoError(t, cleanUpAfterVanishedNodes(context.Background(), cl, *log))

	nodes := &corev1.NodeList{}
	require.NoError(t, cl.List(context.Background(), nodes))
	assert.Len(t, nodes.Items, 1)
}
