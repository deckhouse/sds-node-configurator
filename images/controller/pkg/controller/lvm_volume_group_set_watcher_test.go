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
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/controller/pkg/logger"
	"github.com/deckhouse/sds-node-configurator/images/controller/pkg/monitoring"
)

// newFakeClientWithLVGSetStatus mirrors NewFakeClient but declares the status
// subresource for LVMVolumeGroupSet, which provideLVMVolumeGroupsPerNode writes
// through as it records each created LVMVolumeGroup.
func newFakeClientWithLVGSetStatus() client.WithWatch {
	s := scheme.Scheme
	_ = metav1.AddMetaToScheme(s)
	_ = v1alpha1.AddToScheme(s)
	_ = v1.AddToScheme(s)

	return fake.NewClientBuilder().
		WithScheme(s).
		WithStatusSubresource(&v1alpha1.LVMVolumeGroupSet{}).
		Build()
}

// A file-only LVMVolumeGroupSet template carries no blockDeviceSelector;
// the child LVMVolumeGroup is admitted only if configureLVGBySet copies
// fileDevices over. Without it the child has neither selector nor
// fileDevices and is rejected by its own CEL rule.
func TestConfigureLVGBySet_CopiesFileDevices(t *testing.T) {
	fileDevices := []v1alpha1.LVMVolumeGroupFileDeviceSpec{
		{Name: "d50g", Directory: "/data/lvm-backing", Size: resource.MustParse("50Gi")},
	}
	lvgSet := &v1alpha1.LVMVolumeGroupSet{
		ObjectMeta: metav1.ObjectMeta{Name: "set-a"},
		Spec: v1alpha1.LVMVolumeGroupSetSpec{
			LVGTemplate: v1alpha1.LVMVolumeGroupTemplate{
				ActualVGNameOnTheNode: "vg-file",
				Type:                  "Local",
				FileDevices:           fileDevices,
			},
		},
	}

	lvg := configureLVGBySet(lvgSet, v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-0"}})

	assert.Equal(t, fileDevices, lvg.Spec.FileDevices)
	assert.Nil(t, lvg.Spec.BlockDeviceSelector)
	assert.Equal(t, "node-0", lvg.Spec.Local.NodeName)
}

// Editing fileDevices on the set template (the documented "add an entry to
// grow capacity" flow) must propagate to already-created child LVGs.
func TestUpdateLVMVolumeGroupByConfiguredFromSet_PropagatesFileDevices(t *testing.T) {
	configured := &v1alpha1.LVMVolumeGroup{
		Spec: v1alpha1.LVMVolumeGroupSpec{
			FileDevices: []v1alpha1.LVMVolumeGroupFileDeviceSpec{
				{Name: "d10g", Directory: "/data", Size: resource.MustParse("10Gi")},
				{Name: "d20g", Directory: "/data", Size: resource.MustParse("20Gi")},
			},
		},
	}
	existing := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "set-a-0"},
		Spec: v1alpha1.LVMVolumeGroupSpec{
			FileDevices: []v1alpha1.LVMVolumeGroupFileDeviceSpec{
				{Name: "d10g", Directory: "/data", Size: resource.MustParse("10Gi")},
			},
		},
	}

	ctx := context.Background()
	cl := NewFakeClient()
	assert.NoError(t, cl.Create(ctx, existing))

	err := updateLVMVolumeGroupByConfiguredFromSet(ctx, cl, existing, configured)
	assert.NoError(t, err)
	assert.Equal(t, configured.Spec.FileDevices, existing.Spec.FileDevices)
}

// Existing children are matched by (node, VG name), not by generated name:
// configureLVGNameFromSet derives the name from the number of already-created
// LVGs, so a set whose status drifted would otherwise mint a duplicate LVG for
// a node that already has one — and a second backing file with it.
func TestMatchConfiguredLVGWithExistingOne(t *testing.T) {
	configured := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "set-a-3"},
		Spec: v1alpha1.LVMVolumeGroupSpec{
			ActualVGNameOnTheNode: "vg-file",
			Local:                 v1alpha1.LVMVolumeGroupLocalSpec{NodeName: "node-0"},
		},
	}

	t.Run("matches_same_node_and_vg_under_a_different_name", func(t *testing.T) {
		existing := map[string]v1alpha1.LVMVolumeGroup{
			"set-a-0": {
				ObjectMeta: metav1.ObjectMeta{Name: "set-a-0"},
				Spec: v1alpha1.LVMVolumeGroupSpec{
					ActualVGNameOnTheNode: "vg-file",
					Local:                 v1alpha1.LVMVolumeGroupLocalSpec{NodeName: "node-0"},
				},
			},
		}

		got := matchConfiguredLVGWithExistingOne(configured, existing)
		if assert.NotNil(t, got) {
			assert.Equal(t, "set-a-0", got.Name)
		}
	})

	t.Run("does_not_match_another_node", func(t *testing.T) {
		existing := map[string]v1alpha1.LVMVolumeGroup{
			"set-a-0": {
				ObjectMeta: metav1.ObjectMeta{Name: "set-a-0"},
				Spec: v1alpha1.LVMVolumeGroupSpec{
					ActualVGNameOnTheNode: "vg-file",
					Local:                 v1alpha1.LVMVolumeGroupLocalSpec{NodeName: "node-1"},
				},
			},
		}

		assert.Nil(t, matchConfiguredLVGWithExistingOne(configured, existing))
	})

	t.Run("does_not_match_another_vg_on_the_same_node", func(t *testing.T) {
		existing := map[string]v1alpha1.LVMVolumeGroup{
			"other": {
				ObjectMeta: metav1.ObjectMeta{Name: "other"},
				Spec: v1alpha1.LVMVolumeGroupSpec{
					ActualVGNameOnTheNode: "vg-block",
					Local:                 v1alpha1.LVMVolumeGroupLocalSpec{NodeName: "node-0"},
				},
			},
		}

		assert.Nil(t, matchConfiguredLVGWithExistingOne(configured, existing))
	})
}

// The fleet rollout: a file-only set must create exactly one LVMVolumeGroup per
// selected node, each carrying the template's fileDevices and its own node
// name. Nothing here depends on BlockDevices — that is the whole point of
// spec.fileDevices, and a regression that reintroduced such a dependency would
// silently produce zero LVGs on a diskless fleet.
func TestProvideLVMVolumeGroupsPerNode_CreatesOneFileBackedLVGPerNode(t *testing.T) {
	ctx := context.Background()
	cl := newFakeClientWithLVGSetStatus()

	fileDevices := []v1alpha1.LVMVolumeGroupFileDeviceSpec{
		{Name: "d10g", Directory: "/opt/deckhouse/sds/file-devices", Size: resource.MustParse("10Gi")},
	}
	lvgSet := &v1alpha1.LVMVolumeGroupSet{
		ObjectMeta: metav1.ObjectMeta{Name: "set-a"},
		Spec: v1alpha1.LVMVolumeGroupSetSpec{
			Strategy: strategyPerNode,
			LVGTemplate: v1alpha1.LVMVolumeGroupTemplate{
				Metadata:              v1alpha1.LVMVolumeGroupTemplateMeta{Labels: map[string]string{"set": "a"}},
				ActualVGNameOnTheNode: "vg-file",
				Type:                  "Local",
				FileDevices:           fileDevices,
			},
		},
	}
	assert.NoError(t, cl.Create(ctx, lvgSet))

	nodes := map[string]v1.Node{
		"node-0": {ObjectMeta: metav1.ObjectMeta{Name: "node-0"}},
		"node-1": {ObjectMeta: metav1.ObjectMeta{Name: "node-1"}},
	}

	assert.NoError(t, provideLVMVolumeGroupsPerNode(ctx, cl, logger.Logger{}, monitoring.GetMetrics(""), lvgSet, nodes))

	var created v1alpha1.LVMVolumeGroupList
	assert.NoError(t, cl.List(ctx, &created))
	assert.Len(t, created.Items, len(nodes))

	byNode := map[string]v1alpha1.LVMVolumeGroup{}
	for _, lvg := range created.Items {
		byNode[lvg.Spec.Local.NodeName] = lvg
	}
	assert.Len(t, byNode, len(nodes), "every node must get its own LVMVolumeGroup")
	for node, lvg := range byNode {
		assert.Equal(t, fileDevices, lvg.Spec.FileDevices, "node %s lost the template's fileDevices", node)
		assert.Equal(t, "vg-file", lvg.Spec.ActualVGNameOnTheNode)
		assert.Equal(t, map[string]string{"set": "a"}, lvg.Labels)
		assert.Nil(t, lvg.Spec.BlockDeviceSelector)
	}
}

// Re-running the set over the same nodes must converge, not fan out: a second
// pass has to update the LVGs already there. Creating a second one per node
// would mean a second backing file per node on every controller restart.
func TestProvideLVMVolumeGroupsPerNode_IsIdempotentAndGrowsFileDevices(t *testing.T) {
	ctx := context.Background()
	cl := newFakeClientWithLVGSetStatus()

	first := []v1alpha1.LVMVolumeGroupFileDeviceSpec{
		{Name: "d10g", Directory: "/opt/deckhouse/sds/file-devices", Size: resource.MustParse("10Gi")},
	}
	lvgSet := &v1alpha1.LVMVolumeGroupSet{
		ObjectMeta: metav1.ObjectMeta{Name: "set-a"},
		Spec: v1alpha1.LVMVolumeGroupSetSpec{
			Strategy: strategyPerNode,
			LVGTemplate: v1alpha1.LVMVolumeGroupTemplate{
				ActualVGNameOnTheNode: "vg-file",
				Type:                  "Local",
				FileDevices:           first,
			},
		},
	}
	assert.NoError(t, cl.Create(ctx, lvgSet))

	nodes := map[string]v1.Node{"node-0": {ObjectMeta: metav1.ObjectMeta{Name: "node-0"}}}
	assert.NoError(t, provideLVMVolumeGroupsPerNode(ctx, cl, logger.Logger{}, monitoring.GetMetrics(""), lvgSet, nodes))

	// Grow the template the way an operator would: append, never edit in place
	// (existing entries are immutable by CEL rule).
	grown := append(append([]v1alpha1.LVMVolumeGroupFileDeviceSpec{}, first...),
		v1alpha1.LVMVolumeGroupFileDeviceSpec{Name: "d20g", Directory: "/opt/deckhouse/sds/file-devices", Size: resource.MustParse("20Gi")})
	lvgSet.Spec.LVGTemplate.FileDevices = grown

	assert.NoError(t, provideLVMVolumeGroupsPerNode(ctx, cl, logger.Logger{}, monitoring.GetMetrics(""), lvgSet, nodes))

	var after v1alpha1.LVMVolumeGroupList
	assert.NoError(t, cl.List(ctx, &after))
	if assert.Len(t, after.Items, 1, "the second pass must update, not create a second LVMVolumeGroup") {
		assert.Equal(t, grown, after.Items[0].Spec.FileDevices)
	}
}

// The set owns spec.fileDevices of the LVMVolumeGroups it creates, so an entry
// dropped from the template is dropped from the children too. That is only safe
// to do unconditionally because removing an entry no longer wedges the child:
// the apiserver accepts it, and if the entry still backs a live Physical Volume
// the agent reports drift on the child's condition rather than destroying
// anything. Before, the child's CEL rule rejected the update and the set
// retried the same failing write forever.
func TestProvideLVMVolumeGroupsPerNode_PropagatesFileDeviceRemoval(t *testing.T) {
	ctx := context.Background()
	cl := newFakeClientWithLVGSetStatus()

	both := []v1alpha1.LVMVolumeGroupFileDeviceSpec{
		{Name: "keep", Directory: "/opt/deckhouse/sds/file-devices", Size: resource.MustParse("10Gi")},
		{Name: "dropped", Directory: "/opt/deckhouse/sds/file-devices", Size: resource.MustParse("10Gi")},
	}
	lvgSet := &v1alpha1.LVMVolumeGroupSet{
		ObjectMeta: metav1.ObjectMeta{Name: "set-shrink"},
		Spec: v1alpha1.LVMVolumeGroupSetSpec{
			Strategy: strategyPerNode,
			LVGTemplate: v1alpha1.LVMVolumeGroupTemplate{
				ActualVGNameOnTheNode: "vg-file",
				Type:                  "Local",
				FileDevices:           both,
			},
		},
	}
	assert.NoError(t, cl.Create(ctx, lvgSet))

	nodes := map[string]v1.Node{"node-0": {ObjectMeta: metav1.ObjectMeta{Name: "node-0"}}}
	assert.NoError(t, provideLVMVolumeGroupsPerNode(ctx, cl, logger.Logger{}, monitoring.GetMetrics(""), lvgSet, nodes))

	var created v1alpha1.LVMVolumeGroupList
	assert.NoError(t, cl.List(ctx, &created))
	assert.Len(t, created.Items[0].Spec.FileDevices, 2)

	lvgSet.Spec.LVGTemplate.FileDevices = both[:1]
	assert.NoError(t, provideLVMVolumeGroupsPerNode(ctx, cl, logger.Logger{}, monitoring.GetMetrics(""), lvgSet, nodes))

	var after v1alpha1.LVMVolumeGroupList
	assert.NoError(t, cl.List(ctx, &after))
	if assert.Len(t, after.Items, 1) {
		if assert.Len(t, after.Items[0].Spec.FileDevices, 1, "the dropped entry must be removed from the child") {
			assert.Equal(t, "keep", after.Items[0].Spec.FileDevices[0].Name)
		}
	}
}
