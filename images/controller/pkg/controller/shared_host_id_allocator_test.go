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

package controller

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/controller/pkg/logger"
)

func hostIDTestClient(objects ...client.Object) client.WithWatch {
	s := scheme.Scheme
	_ = metav1.AddMetaToScheme(s)
	_ = v1alpha1.AddToScheme(s)
	_ = corev1.AddToScheme(s)

	return fake.NewClientBuilder().WithScheme(s).WithObjects(objects...).Build()
}

func hostIDTestNode(name string, id string) *corev1.Node {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}}
	if id != "" {
		node.Annotations = map[string]string{SanlockHostIDAnnotation: id}
	}
	return node
}

func hostIDTestGroup(name string, align string, nodes ...string) *v1alpha1.LVMSharedVolumeGroup {
	group := &v1alpha1.LVMSharedVolumeGroup{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1alpha1.LVMSharedVolumeGroupSpec{
			ActualVGNameOnTheNode: name,
			Nodes:                 nodes,
			VolumeCleanup:         v1alpha1.VolumeCleanupDiscard,
		},
	}
	if align != "" {
		group.Spec.LVM = &v1alpha1.LVMSharedVolumeGroupLVMSpec{SanlockAlignSize: align}
	}
	return group
}

func hostIDOfNode(t *testing.T, cl client.Client, name string) int {
	t.Helper()
	node := &corev1.Node{}
	require.NoError(t, cl.Get(context.Background(), client.ObjectKey{Name: name}, node))
	raw, ok := node.Annotations[SanlockHostIDAnnotation]
	if !ok {
		return 0
	}
	id, err := strconv.Atoi(raw)
	require.NoError(t, err)
	return id
}

func TestHostIDsAreAllocatedOnlyToPoolMembers(t *testing.T) {
	log, _ := logger.NewLogger(logger.WarningLevel)
	cl := hostIDTestClient(
		hostIDTestNode("n1", ""), hostIDTestNode("n2", ""), hostIDTestNode("outsider", ""),
		hostIDTestGroup("vg1", "4Mi", "n1", "n2"),
	)

	require.NoError(t, reconcileHostIDs(context.Background(), cl, *log))

	assert.Equal(t, 1, hostIDOfNode(t, cl, "n1"))
	assert.Equal(t, 2, hostIDOfNode(t, cl, "n2"))
	assert.Equal(t, 0, hostIDOfNode(t, cl, "outsider"),
		"a node outside every pool costs a host_id slot for nothing")
}

func TestHostIDIsStableAcrossReconciles(t *testing.T) {
	log, _ := logger.NewLogger(logger.WarningLevel)
	cl := hostIDTestClient(
		hostIDTestNode("n1", ""), hostIDTestNode("n2", ""),
		hostIDTestGroup("vg1", "4Mi", "n1", "n2"),
	)

	require.NoError(t, reconcileHostIDs(context.Background(), cl, *log))
	first := hostIDOfNode(t, cl, "n1")

	require.NoError(t, reconcileHostIDs(context.Background(), cl, *log))
	assert.Equal(t, first, hostIDOfNode(t, cl, "n1"),
		"renumbering a node that may hold a lockspace is never safe")
}

func TestHostIDSurvivesLeavingThePool(t *testing.T) {
	// A node leaving a pool keeps its id. Its delta lease may still be alive,
	// and handing the id to someone else would make the new owner wait out
	// host_dead_seconds — a stall that is hard to recognise for a resource
	// there are a thousand of.
	log, _ := logger.NewLogger(logger.WarningLevel)
	cl := hostIDTestClient(
		hostIDTestNode("n1", ""), hostIDTestNode("n2", ""),
		hostIDTestGroup("vg1", "4Mi", "n1", "n2"),
	)
	require.NoError(t, reconcileHostIDs(context.Background(), cl, *log))
	n2ID := hostIDOfNode(t, cl, "n2")

	group := &v1alpha1.LVMSharedVolumeGroup{}
	require.NoError(t, cl.Get(context.Background(), client.ObjectKey{Name: "vg1"}, group))
	group.Spec.Nodes = []string{"n1"}
	require.NoError(t, cl.Update(context.Background(), group))

	require.NoError(t, reconcileHostIDs(context.Background(), cl, *log))
	assert.Equal(t, n2ID, hostIDOfNode(t, cl, "n2"))
}

func TestOneIDPerNodeAcrossPools(t *testing.T) {
	// lvmlockd reads one file for every lockspace it starts, so a node in two
	// pools uses the same id in both.
	log, _ := logger.NewLogger(logger.WarningLevel)
	cl := hostIDTestClient(
		hostIDTestNode("n1", ""), hostIDTestNode("n2", ""),
		hostIDTestGroup("vg1", "4Mi", "n1", "n2"),
		hostIDTestGroup("vg2", "4Mi", "n1"),
	)

	require.NoError(t, reconcileHostIDs(context.Background(), cl, *log))

	assert.NotEqual(t, hostIDOfNode(t, cl, "n1"), hostIDOfNode(t, cl, "n2"))
	assert.NotZero(t, hostIDOfNode(t, cl, "n1"))
}

func TestSmallestCeilingWins(t *testing.T) {
	// A node in two pools has one id and has to fit under both ceilings, so
	// the tighter one decides.
	ceilings := ceilingsByNode([]v1alpha1.LVMSharedVolumeGroup{
		*hostIDTestGroup("wide", "8Mi", "n1", "n2"),
		*hostIDTestGroup("narrow", "1Mi", "n1"),
	})
	assert.Equal(t, 250, ceilings["n1"], "n1 is in both pools and the 1Mi one is tighter")
	assert.Equal(t, 2000, ceilings["n2"], "n2 is only in the 8Mi pool")
	assert.NotContains(t, ceilings, "outsider")
}

func TestCeilingPerAlignment(t *testing.T) {
	assert.Equal(t, 250, ceilingForGroup(hostIDTestGroup("vg", "1Mi")))
	assert.Equal(t, 500, ceilingForGroup(hostIDTestGroup("vg", "2Mi")))
	assert.Equal(t, 1000, ceilingForGroup(hostIDTestGroup("vg", "4Mi")))
	assert.Equal(t, 2000, ceilingForGroup(hostIDTestGroup("vg", "8Mi")))

	// An absent block and an unknown value both fall back to the default
	// rather than to the largest ceiling: ids a lockspace cannot hold fail
	// long after the mistake is made.
	assert.Equal(t, 1000, ceilingForGroup(hostIDTestGroup("vg", "")))
	assert.Equal(t, 1000, ceilingForGroup(hostIDTestGroup("vg", "3Mi")))
}

func TestCeilingIsEnforcedWhenFull(t *testing.T) {
	taken := map[int]string{}
	for id := 1; id <= 250; id++ {
		taken[id] = "n" + strconv.Itoa(id)
	}

	_, err := lowestFreeHostID(taken, 250)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot be raised on an existing volume group")

	// The same set of ids is not full under a larger alignment.
	id, err := lowestFreeHostID(taken, 500)
	require.NoError(t, err)
	assert.Equal(t, 251, id)
}

func TestFreedIDIsReused(t *testing.T) {
	// Ids come back into use when their nodes are gone, otherwise a cluster
	// with churn drifts towards its ceiling for no reason.
	taken := map[int]string{1: "n1", 3: "n3"}
	id, err := lowestFreeHostID(taken, 1000)
	require.NoError(t, err)
	assert.Equal(t, 2, id)
}

func TestDuplicateIDIsReportedNotResolved(t *testing.T) {
	log, _ := logger.NewLogger(logger.WarningLevel)
	cl := hostIDTestClient(
		hostIDTestNode("n1", "7"), hostIDTestNode("n2", "7"),
		hostIDTestGroup("vg1", "4Mi", "n1", "n2"),
	)

	err := reconcileHostIDs(context.Background(), cl, *log)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "claimed by both")
}

func TestUnreadableAnnotationDoesNotBlockOthers(t *testing.T) {
	// A hand-mangled annotation is left alone — overwriting it could point two
	// nodes at one lease — but it must not stop the rest of the pool.
	log, _ := logger.NewLogger(logger.WarningLevel)
	cl := hostIDTestClient(
		hostIDTestNode("n1", "not-a-number"), hostIDTestNode("n2", ""),
		hostIDTestGroup("vg1", "4Mi", "n1", "n2"),
	)

	require.NoError(t, reconcileHostIDs(context.Background(), cl, *log))
	assert.NotZero(t, hostIDOfNode(t, cl, "n2"))

	node := &corev1.Node{}
	require.NoError(t, cl.Get(context.Background(), client.ObjectKey{Name: "n1"}, node))
	assert.Equal(t, "not-a-number", node.Annotations[SanlockHostIDAnnotation])
}

func TestAllocationIsDeterministic(t *testing.T) {
	// A reconcile that fails halfway is retried, and the retry has to hand out
	// the same ids as the attempt before it.
	log, _ := logger.NewLogger(logger.WarningLevel)
	build := func() client.WithWatch {
		return hostIDTestClient(
			hostIDTestNode("beta", ""), hostIDTestNode("alpha", ""), hostIDTestNode("gamma", ""),
			hostIDTestGroup("vg1", "4Mi", "gamma", "alpha", "beta"),
		)
	}

	first := build()
	require.NoError(t, reconcileHostIDs(context.Background(), first, *log))
	second := build()
	require.NoError(t, reconcileHostIDs(context.Background(), second, *log))

	for _, name := range []string{"alpha", "beta", "gamma"} {
		assert.Equal(t, hostIDOfNode(t, first, name), hostIDOfNode(t, second, name), name)
	}
	assert.Equal(t, 1, hostIDOfNode(t, first, "alpha"))
}
