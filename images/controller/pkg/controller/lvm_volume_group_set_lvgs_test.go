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
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/controller/pkg/monitoring"
)

func lvgOnNode(name, node, vg, phase string) v1alpha1.LVMVolumeGroup {
	return v1alpha1.LVMVolumeGroup{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1alpha1.LVMVolumeGroupSpec{
			ActualVGNameOnTheNode: vg,
			Local:                 v1alpha1.LVMVolumeGroupLocalSpec{NodeName: node},
		},
		Status: v1alpha1.LVMVolumeGroupStatus{Phase: phase},
	}
}

func lvgsByName(lvgs ...v1alpha1.LVMVolumeGroup) map[string]v1alpha1.LVMVolumeGroup {
	m := make(map[string]v1alpha1.LVMVolumeGroup, len(lvgs))
	for _, lvg := range lvgs {
		m[lvg.Name] = lvg
	}
	return m
}

func nodesByName(names ...string) map[string]v1.Node {
	m := make(map[string]v1.Node, len(names))
	for _, name := range names {
		m[name] = v1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}}
	}
	return m
}

func nodeNames(nodes map[string]v1.Node) map[string]struct{} {
	names := make(map[string]struct{}, len(nodes))
	for name := range nodes {
		names[name] = struct{}{}
	}
	return names
}

func blockLVGSet(name, vg string) *v1alpha1.LVMVolumeGroupSet {
	return &v1alpha1.LVMVolumeGroupSet{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1alpha1.LVMVolumeGroupSetSpec{
			Strategy:     strategyPerNode,
			NodeSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"set": name}},
			LVGTemplate: v1alpha1.LVMVolumeGroupTemplate{
				ActualVGNameOnTheNode: vg,
				Type:                  "Local",
				BlockDeviceSelector:   &metav1.LabelSelector{MatchLabels: map[string]string{"disk": "data"}},
			},
		},
	}
}

func TestConfigureLVGNameFromSetSkipsTakenNames(t *testing.T) {
	lvgSet := &v1alpha1.LVMVolumeGroupSet{ObjectMeta: metav1.ObjectMeta{Name: "set-a"}}

	assert.Equal(t, "set-a-0", configureLVGNameFromSet(lvgSet, nil))
	assert.Equal(t, "set-a-1", configureLVGNameFromSet(lvgSet, lvgsByName(lvgOnNode("set-a-0", "node-0", "vg", ""))))
	assert.Equal(t, "set-a-0", configureLVGNameFromSet(lvgSet, lvgsByName(lvgOnNode("set-a-1", "node-1", "vg", ""))),
		"a name freed by a deleted group is handed out again")
}

// The status lists what is there now: a group that is gone drops out, a group
// the set keeps on its node under another name is listed, the node comes from
// the group itself, and only the Ready groups are counted as ready.
func TestSetLVMVolumeGroupSetLVGs(t *testing.T) {
	lvgSet := blockLVGSet("set-a", "vg")
	lvgSet.Status.CreatedLVGs = []v1alpha1.LVMVolumeGroupSetStatusLVG{
		{LVMVolumeGroupName: "set-a-0", NodeName: "node-0"},
		{LVMVolumeGroupName: "set-a-1", NodeName: "node-1"},
		{LVMVolumeGroupName: "set-a-2", NodeName: "node-2"},
	}

	lvgs := lvgsByName(
		// set-a-0 was deleted and its name went to the group recreated on node-2.
		lvgOnNode("set-a-0", "node-2", "vg", v1alpha1.PhasePending),
		lvgOnNode("set-a-1", "node-1", "vg", v1alpha1.PhaseReady),
		lvgOnNode("adopted", "node-3", "vg", v1alpha1.PhaseReady),
		lvgOnNode("other-vg", "node-4", "another-vg", v1alpha1.PhaseReady),
	)

	nodes := nodesByName("node-1", "node-2", "node-3", "node-4")
	setLVMVolumeGroupSetLVGs(lvgSet, lvgs, nodes, nodeNames(nodes))

	assert.Equal(t, []v1alpha1.LVMVolumeGroupSetStatusLVG{
		{LVMVolumeGroupName: "set-a-0", NodeName: "node-2"},
		{LVMVolumeGroupName: "set-a-1", NodeName: "node-1"},
		{LVMVolumeGroupName: "adopted", NodeName: "node-3"},
	}, lvgSet.Status.CreatedLVGs)
	assert.Equal(t, 3, lvgSet.Status.CurrentLVMVolumeGroupsCount)
	assert.Equal(t, 4, lvgSet.Status.DesiredLVMVolumeGroupsCount)
	assert.Equal(t, 2, lvgSet.Status.ReadyLVMVolumeGroupsCount)
}

// A group whose node was deleted from the cluster drops out of the status but
// is not deleted, and a node that is still there but no longer selected keeps
// its group listed.
func TestSetLVMVolumeGroupSetLVGsDropsDeletedNodes(t *testing.T) {
	lvgSet := blockLVGSet("set-a", "vg")
	lvgSet.Status.CreatedLVGs = []v1alpha1.LVMVolumeGroupSetStatusLVG{
		{LVMVolumeGroupName: "set-a-0", NodeName: "node-0"},
		{LVMVolumeGroupName: "set-a-1", NodeName: "node-deleted"},
		{LVMVolumeGroupName: "set-a-2", NodeName: "node-unselected"},
	}
	lvgs := lvgsByName(
		lvgOnNode("set-a-0", "node-0", "vg", v1alpha1.PhaseReady),
		lvgOnNode("set-a-1", "node-deleted", "vg", v1alpha1.PhaseReady),
		lvgOnNode("set-a-2", "node-unselected", "vg", v1alpha1.PhaseReady),
	)
	selected := nodesByName("node-0")

	setLVMVolumeGroupSetLVGs(lvgSet, lvgs, selected, nodeNames(nodesByName("node-0", "node-unselected")))

	assert.Equal(t, []v1alpha1.LVMVolumeGroupSetStatusLVG{
		{LVMVolumeGroupName: "set-a-0", NodeName: "node-0"},
		{LVMVolumeGroupName: "set-a-2", NodeName: "node-unselected"},
	}, lvgSet.Status.CreatedLVGs)
	assert.Equal(t, 2, lvgSet.Status.CurrentLVMVolumeGroupsCount)
	assert.Equal(t, 1, lvgSet.Status.DesiredLVMVolumeGroupsCount)
	assert.Equal(t, 2, lvgSet.Status.ReadyLVMVolumeGroupsCount)
}

// The same through a reconcile: the node of one of the set's groups is deleted,
// the status follows, and the group stays where it is.
func TestReconcileLVMVolumeGroupSetDropsADeletedNode(t *testing.T) {
	ctx := context.Background()
	cl := newFakeClientWithLVGSetStatus()

	for _, name := range []string{"node-0", "node-1"} {
		require.NoError(t, cl.Create(ctx, &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{"set": "set-a"}}}))
	}
	lvgSet := blockLVGSet("set-a", "vg")
	require.NoError(t, cl.Create(ctx, lvgSet))

	_, err := reconcileLVMVolumeGroupSet(ctx, cl, cl, logger.NewNop(), monitoring.GetMetrics(""), lvgSet)
	require.NoError(t, err)
	require.Len(t, lvgSet.Status.CreatedLVGs, 2)

	require.NoError(t, cl.Delete(ctx, &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}}))
	_, err = reconcileLVMVolumeGroupSet(ctx, cl, cl, logger.NewNop(), monitoring.GetMetrics(""), lvgSet)
	require.NoError(t, err)

	stored := &v1alpha1.LVMVolumeGroupSet{}
	require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(lvgSet), stored))
	assert.Equal(t, []v1alpha1.LVMVolumeGroupSetStatusLVG{{LVMVolumeGroupName: "set-a-0", NodeName: "node-0"}}, stored.Status.CreatedLVGs)
	assert.Equal(t, 1, stored.Status.CurrentLVMVolumeGroupsCount)
	assert.Equal(t, 1, stored.Status.DesiredLVMVolumeGroupsCount)
	assert.NoError(t, cl.Get(ctx, client.ObjectKey{Name: "set-a-1"}, &v1alpha1.LVMVolumeGroup{}), "the group of the deleted node is kept")
}

func TestShouldLVGSetWatcherReconcileLVGUpdateEvent(t *testing.T) {
	base := lvgOnNode("set-a-0", "node-0", "vg", v1alpha1.PhasePending)
	base.Generation = 1

	specEdited := base
	specEdited.Generation = 2
	assert.True(t, shouldLVGSetWatcherReconcileLVGUpdateEvent(&base, &specEdited))

	becameReady := base
	becameReady.Status.Phase = v1alpha1.PhaseReady
	assert.True(t, shouldLVGSetWatcherReconcileLVGUpdateEvent(&base, &becameReady))

	statusDetail := base
	statusDetail.Status.VGUuid = "uuid"
	assert.False(t, shouldLVGSetWatcherReconcileLVGUpdateEvent(&base, &statusDetail),
		"the agent rewrites the status of a group all the time; only its phase is the set's business")
}

func TestLVGSetRequestsForLVG(t *testing.T) {
	ctx := context.Background()
	cl := newFakeClientWithLVGSetStatus()
	require.NoError(t, cl.Create(ctx, blockLVGSet("set-a", "vg")))
	require.NoError(t, cl.Create(ctx, blockLVGSet("set-b", "another-vg")))

	lvg := lvgOnNode("set-a-0", "node-0", "vg", "")
	assert.Equal(t, []reconcile.Request{{NamespacedName: types.NamespacedName{Name: "set-a"}}},
		lvgSetRequestsForLVG(ctx, cl, logger.NewNop(), &lvg))

	unrelated := lvgOnNode("manual", "node-0", "manual-vg", "")
	assert.Empty(t, lvgSetRequestsForLVG(ctx, cl, logger.NewNop(), &unrelated))
}

// A group deleted from the set comes back under its name on the next pass, and
// the status follows the groups as they are.
func TestProvideLVMVolumeGroupsPerNodeRecreatesADeletedGroup(t *testing.T) {
	ctx := context.Background()
	cl := newFakeClientWithLVGSetStatus()

	lvgSet := blockLVGSet("set-a", "vg")
	require.NoError(t, cl.Create(ctx, lvgSet))
	nodes := nodesByName("node-0", "node-1")

	require.NoError(t, provideLVMVolumeGroupsPerNode(ctx, cl, cl, logger.NewNop(), monitoring.GetMetrics(""), lvgSet, nodes))
	require.Equal(t, []v1alpha1.LVMVolumeGroupSetStatusLVG{
		{LVMVolumeGroupName: "set-a-0", NodeName: "node-0"},
		{LVMVolumeGroupName: "set-a-1", NodeName: "node-1"},
	}, lvgSet.Status.CreatedLVGs)
	assert.Equal(t, 0, lvgSet.Status.ReadyLVMVolumeGroupsCount)

	ready := &v1alpha1.LVMVolumeGroup{}
	require.NoError(t, cl.Get(ctx, client.ObjectKey{Name: "set-a-1"}, ready))
	ready.Status.Phase = v1alpha1.PhaseReady
	require.NoError(t, cl.Update(ctx, ready))
	require.NoError(t, cl.Delete(ctx, &v1alpha1.LVMVolumeGroup{ObjectMeta: metav1.ObjectMeta{Name: "set-a-0"}}))

	require.NoError(t, provideLVMVolumeGroupsPerNode(ctx, cl, cl, logger.NewNop(), monitoring.GetMetrics(""), lvgSet, nodes))

	recreated := &v1alpha1.LVMVolumeGroup{}
	require.NoError(t, cl.Get(ctx, client.ObjectKey{Name: "set-a-0"}, recreated))
	assert.Equal(t, "node-0", recreated.Spec.Local.NodeName)

	var all v1alpha1.LVMVolumeGroupList
	require.NoError(t, cl.List(ctx, &all))
	assert.Len(t, all.Items, 2, "one group per node, not one more for the deleted one")

	stored := &v1alpha1.LVMVolumeGroupSet{}
	require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(lvgSet), stored))
	assert.Equal(t, 2, stored.Status.CurrentLVMVolumeGroupsCount)
	assert.Equal(t, 2, stored.Status.DesiredLVMVolumeGroupsCount)
	assert.Equal(t, 1, stored.Status.ReadyLVMVolumeGroupsCount)
}

// A group already in line with the template is not written to: the set is
// reconciled on every phase change of its groups.
func TestUpdateLVMVolumeGroupByConfiguredFromSetSkipsAGroupInLine(t *testing.T) {
	ctx := context.Background()
	cl := NewFakeClient()

	lvgSet := blockLVGSet("set-a", "vg")
	existing := configureLVGBySet(lvgSet, v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-0"}})
	existing.Name = "set-a-0"
	require.NoError(t, cl.Create(ctx, existing))
	resourceVersion := existing.ResourceVersion

	updated, err := updateLVMVolumeGroupByConfiguredFromSet(ctx, cl, existing, configureLVGBySet(lvgSet, v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-0"}}))
	require.NoError(t, err)
	assert.False(t, updated)
	assert.Equal(t, resourceVersion, existing.ResourceVersion)
}

// What the API server hands back for a group is not byte for byte what the
// template holds: an empty fileDevices list is dropped by omitempty, and a size is
// written back in its canonical form. Neither is a difference to correct.
func TestUpdateLVMVolumeGroupByConfiguredFromSetComparesValues(t *testing.T) {
	for _, tc := range []struct {
		name        string
		fileDevices []v1alpha1.LVMVolumeGroupFileDeviceSpec
	}{
		{"empty file devices", []v1alpha1.LVMVolumeGroupFileDeviceSpec{}},
		{"size in another notation", []v1alpha1.LVMVolumeGroupFileDeviceSpec{
			{Name: "d10g", Directory: "/data", Size: resource.MustParse("10240Mi")},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			cl := NewFakeClient()

			lvgSet := blockLVGSet("set-a", "vg")
			lvgSet.Spec.LVGTemplate.FileDevices = tc.fileDevices
			node := v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-0"}}

			created := configureLVGBySet(lvgSet, node)
			created.Name = "set-a-0"
			require.NoError(t, cl.Create(ctx, created))
			existing := &v1alpha1.LVMVolumeGroup{}
			require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(created), existing))

			updated, err := updateLVMVolumeGroupByConfiguredFromSet(ctx, cl, existing, configureLVGBySet(lvgSet, node))
			require.NoError(t, err)
			assert.False(t, updated)
		})
	}
}

// A set already Created for its spec stays Created through the passes its
// groups trigger; Pending is written only for a spec not acted on yet.
func TestReconcileLVMVolumeGroupSetWritesPendingOnlyForANewSpec(t *testing.T) {
	ctx := context.Background()
	base := newFakeClientWithLVGSetStatus()

	var phases []string
	cl := interceptor.NewClient(base, interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, c client.Client, subResource string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if lvgSet, ok := obj.(*v1alpha1.LVMVolumeGroupSet); ok {
				phases = append(phases, lvgSet.Status.Phase)
			}
			return c.SubResource(subResource).Update(ctx, obj, opts...)
		},
	})

	require.NoError(t, cl.Create(ctx, &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-0", Labels: map[string]string{"set": "set-a"}}}))
	lvgSet := blockLVGSet("set-a", "vg")
	require.NoError(t, cl.Create(ctx, lvgSet))

	_, err := reconcileLVMVolumeGroupSet(ctx, cl, cl, logger.NewNop(), monitoring.GetMetrics(""), lvgSet)
	require.NoError(t, err)
	require.Contains(t, phases, v1alpha1.PhasePending, "a set seen for the first time is Pending until it is acted on")
	require.Equal(t, phaseCreated, lvgSet.Status.Phase)

	ready := &v1alpha1.LVMVolumeGroup{}
	require.NoError(t, cl.Get(ctx, client.ObjectKey{Name: "set-a-0"}, ready))
	ready.Status.Phase = v1alpha1.PhaseReady
	require.NoError(t, cl.Update(ctx, ready))

	phases = nil
	_, err = reconcileLVMVolumeGroupSet(ctx, cl, cl, logger.NewNop(), monitoring.GetMetrics(""), lvgSet)
	require.NoError(t, err)
	assert.NotContains(t, phases, v1alpha1.PhasePending)
	assert.Equal(t, phaseCreated, lvgSet.Status.Phase)
	assert.Equal(t, 1, lvgSet.Status.ReadyLVMVolumeGroupsCount)
}

// A set whose selector no longer matches any node keeps its groups, and its
// status says so: the desired count drops to zero, the groups still there are
// counted, the ones that are gone are not.
func TestReconcileLVMVolumeGroupSetCountsGroupsWithNoNodeSelected(t *testing.T) {
	ctx := context.Background()
	cl := newFakeClientWithLVGSetStatus()

	lvgSet := blockLVGSet("set-a", "vg")
	require.NoError(t, cl.Create(ctx, lvgSet))
	lvgSet.Status.CreatedLVGs = []v1alpha1.LVMVolumeGroupSetStatusLVG{
		{LVMVolumeGroupName: "set-a-0", NodeName: "node-0"},
		{LVMVolumeGroupName: "set-a-1", NodeName: "node-1"},
	}
	lvgSet.Status.CurrentLVMVolumeGroupsCount = 2
	lvgSet.Status.DesiredLVMVolumeGroupsCount = 2
	require.NoError(t, cl.Status().Update(ctx, lvgSet))

	kept := lvgOnNode("set-a-1", "node-1", "vg", v1alpha1.PhaseReady)
	require.NoError(t, cl.Create(ctx, &kept))
	require.NoError(t, cl.Create(ctx, &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}}))

	requeue, err := reconcileLVMVolumeGroupSet(ctx, cl, cl, logger.NewNop(), monitoring.GetMetrics(""), lvgSet)
	require.NoError(t, err)
	assert.True(t, requeue)

	stored := &v1alpha1.LVMVolumeGroupSet{}
	require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(lvgSet), stored))
	assert.Equal(t, phaseNotCreated, stored.Status.Phase)
	assert.Equal(t, []v1alpha1.LVMVolumeGroupSetStatusLVG{{LVMVolumeGroupName: "set-a-1", NodeName: "node-1"}}, stored.Status.CreatedLVGs)
	assert.Equal(t, 1, stored.Status.CurrentLVMVolumeGroupsCount)
	assert.Equal(t, 0, stored.Status.DesiredLVMVolumeGroupsCount)
	assert.Equal(t, 1, stored.Status.ReadyLVMVolumeGroupsCount)
}
