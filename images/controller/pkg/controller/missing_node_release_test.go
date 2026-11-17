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
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/controller/internal"
	"github.com/deckhouse/sds-node-configurator/images/controller/pkg/monitoring"
)

const (
	testGrace        = 10 * time.Minute
	testGoneNode     = "node-gone"
	testPresentNode  = "node-present"
	testForeignFinal = "example.com/consumer"
)

var releaseNow = time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)

func missingSinceValue(ago time.Duration) string {
	return releaseNow.Add(-ago).Format(time.RFC3339)
}

func releaseLVG(name, node string, deleting bool, finalizers ...string) *v1alpha1.LVMVolumeGroup {
	lvg := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: metav1.ObjectMeta{Name: name, Finalizers: finalizers},
		Spec: v1alpha1.LVMVolumeGroupSpec{
			ActualVGNameOnTheNode: "vg-" + name,
			Type:                  "Local",
			Local:                 v1alpha1.LVMVolumeGroupLocalSpec{NodeName: node},
		},
	}
	if deleting {
		lvg.DeletionTimestamp = &metav1.Time{Time: releaseNow.Add(-time.Hour)}
	}
	return lvg
}

func withMissingSince(lvg *v1alpha1.LVMVolumeGroup, ago time.Duration) *v1alpha1.LVMVolumeGroup {
	lvg.Annotations = map[string]string{v1alpha1.NodeMissingSinceAnnotation: missingSinceValue(ago)}
	return lvg
}

func releaseLLV(name, lvgName string, deleting bool, finalizers ...string) *v1alpha1.LVMLogicalVolume {
	llv := &v1alpha1.LVMLogicalVolume{
		ObjectMeta: metav1.ObjectMeta{Name: name, Finalizers: finalizers},
		Spec: v1alpha1.LVMLogicalVolumeSpec{
			ActualLVNameOnTheNode: name,
			Type:                  "Thick",
			Size:                  "1Gi",
			LVMVolumeGroupName:    lvgName,
		},
	}
	if deleting {
		llv.DeletionTimestamp = &metav1.Time{Time: releaseNow.Add(-time.Hour)}
	}
	return llv
}

func releaseLLVS(name, llvName string, status *v1alpha1.LVMLogicalVolumeSnapshotStatus, finalizers ...string) *v1alpha1.LVMLogicalVolumeSnapshot {
	return &v1alpha1.LVMLogicalVolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Finalizers:        finalizers,
			DeletionTimestamp: &metav1.Time{Time: releaseNow.Add(-time.Hour)},
		},
		Spec:   v1alpha1.LVMLogicalVolumeSnapshotSpec{LVMLogicalVolumeName: llvName},
		Status: status,
	}
}

func notReadyNode(name string) *v1.Node {
	return &v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: v1.NodeStatus{Conditions: []v1.NodeCondition{
			{Type: v1.NodeReady, Status: v1.ConditionUnknown, Reason: "NodeStatusUnknown"},
		}},
	}
}

func releaseScheme(t *testing.T) *apiruntime.Scheme {
	t.Helper()
	s := apiruntime.NewScheme()
	require.NoError(t, v1.AddToScheme(s))
	require.NoError(t, v1alpha1.AddToScheme(s))
	return s
}

type releaseFixture struct {
	cl       client.WithWatch
	recorder *events.FakeRecorder
	r        *missingNodeRelease
}

// newReleaseFixture builds the release over one fake store, read both as the
// cache and as the API reader. cacheFuncs, when given, intercept the cache side
// only.
func newReleaseFixture(t *testing.T, grace time.Duration, cacheFuncs *interceptor.Funcs, objs ...client.Object) releaseFixture {
	t.Helper()
	store := fake.NewClientBuilder().WithScheme(releaseScheme(t)).WithObjects(objs...).Build()
	var cache client.Client = store
	if cacheFuncs != nil {
		cache = interceptor.NewClient(store, *cacheFuncs)
	}
	recorder := events.NewFakeRecorder(100)
	return releaseFixture{
		cl:       store,
		recorder: recorder,
		r: &missingNodeRelease{
			cl:       cache,
			reader:   store,
			recorder: recorder,
			log:      logger.NewNop(),
			metrics:  monitoring.GetMetrics(""),
			grace:    grace,
			interval: time.Minute,
		},
	}
}

func (f releaseFixture) events() []string {
	var events []string
	for {
		select {
		case e := <-f.recorder.Events:
			events = append(events, e)
		default:
			return events
		}
	}
}

func (f releaseFixture) gone(t *testing.T, obj client.Object) bool {
	t.Helper()
	err := f.cl.Get(context.Background(), client.ObjectKeyFromObject(obj), obj)
	if apierrors.IsNotFound(err) {
		return true
	}
	require.NoError(t, err)
	return false
}

func TestReleaseVerdict(t *testing.T) {
	since := releaseNow.Add(-time.Hour)
	for _, tc := range []struct {
		name       string
		lvg        *v1alpha1.LVMVolumeGroup
		since      time.Time
		grace      time.Duration
		blocker    string
		release    bool
		heldReason string
	}{
		{name: "not being deleted", lvg: releaseLVG("a", testGoneNode, false, internal.AgentFinalizer), since: since, grace: testGrace},
		{name: "no agent finalizer", lvg: releaseLVG("a", testGoneNode, true, testForeignFinal), since: since, grace: testGrace},
		{name: "within the grace period", lvg: releaseLVG("a", testGoneNode, true, internal.AgentFinalizer), since: releaseNow.Add(-testGrace + time.Second), grace: testGrace},
		{name: "past the grace period", lvg: releaseLVG("a", testGoneNode, true, internal.AgentFinalizer), since: releaseNow.Add(-testGrace), grace: testGrace, release: true},
		{name: "foreign finalizer", lvg: releaseLVG("a", testGoneNode, true, testForeignFinal, internal.AgentFinalizer), since: since, grace: testGrace, heldReason: heldReasonForeignFinalizers},
		{name: "blocked", lvg: releaseLVG("a", testGoneNode, true, internal.AgentFinalizer), since: since, grace: testGrace, blocker: heldReasonVolumesLeft, heldReason: heldReasonVolumesLeft},
		{name: "disabled", lvg: releaseLVG("a", testGoneNode, true, internal.AgentFinalizer), since: since, heldReason: heldReasonReleaseDisabled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			release, reason := releaseVerdict(tc.lvg, tc.since, releaseNow, tc.grace, tc.blocker)
			assert.Equal(t, tc.release, release)
			assert.Equal(t, tc.heldReason, reason)
		})
	}
}

// A group being deleted whose node has been gone past the grace period loses
// the agent's finalizer and, with nothing else holding it, goes away; the
// release is announced by an Event.
func TestMissingNodeReleaseReleasesAGroupPastTheGracePeriod(t *testing.T) {
	lvg := withMissingSince(releaseLVG("lvg-a", testGoneNode, true, internal.AgentFinalizer), testGrace+time.Minute)
	f := newReleaseFixture(t, testGrace, nil, lvg)

	require.NoError(t, f.r.pass(context.Background(), releaseNow))

	assert.True(t, f.gone(t, lvg))
	events := f.events()
	require.Len(t, events, 1)
	assert.Contains(t, events[0], eventReasonAgentFinalizerReleased)
	assert.Contains(t, events[0], testGoneNode)
}

// The node is found gone for the first time: the time goes into the annotation
// and nothing is released before the grace period has run from it.
func TestMissingNodeReleaseStartsTheGracePeriod(t *testing.T) {
	lvg := releaseLVG("lvg-a", testGoneNode, true, internal.AgentFinalizer)
	f := newReleaseFixture(t, testGrace, nil, lvg)

	require.NoError(t, f.r.pass(context.Background(), releaseNow))

	require.False(t, f.gone(t, lvg))
	assert.Equal(t, releaseNow.Format(time.RFC3339), lvg.Annotations[v1alpha1.NodeMissingSinceAnnotation])
	assert.Equal(t, []string{internal.AgentFinalizer}, lvg.Finalizers)
	events := f.events()
	require.Len(t, events, 1)
	assert.Contains(t, events[0], eventReasonNodeMissing)
	assert.Equal(t, float64(releaseNow.Unix()),
		testutil.ToFloat64(monitoring.GetMetrics("").LVGNodeMissingSince("lvg-a", testGoneNode)))

	// A restarted controller counts from the annotation, not from its own start.
	require.NoError(t, f.r.pass(context.Background(), releaseNow.Add(testGrace-time.Second)))
	require.False(t, f.gone(t, lvg))
	require.NoError(t, f.r.pass(context.Background(), releaseNow.Add(testGrace)))
	assert.True(t, f.gone(t, lvg))
}

// A node that is in the cluster is never taken for gone, whatever its state:
// NotReady, without an agent, or present in the API server but not yet in the
// cache.
func TestMissingNodeReleaseKeepsTheResourcesOfAPresentNode(t *testing.T) {
	hideNodes := &interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*v1.NodeList); ok {
				return nil
			}
			return c.List(ctx, list, opts...)
		},
	}
	for _, tc := range []struct {
		name       string
		cacheFuncs *interceptor.Funcs
	}{
		{name: "NotReady node without an agent Pod"},
		{name: "node missing from the cache only", cacheFuncs: hideNodes},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// A stale annotation from an earlier absence must not count either.
			lvg := withMissingSince(releaseLVG("lvg-a", testPresentNode, true, internal.AgentFinalizer), time.Hour)
			llv := releaseLLV("llv-a", "lvg-a", true, internal.AgentFinalizer)
			f := newReleaseFixture(t, testGrace, tc.cacheFuncs, notReadyNode(testPresentNode), lvg, llv)

			require.NoError(t, f.r.pass(context.Background(), releaseNow))

			require.False(t, f.gone(t, lvg))
			require.False(t, f.gone(t, llv))
			assert.Equal(t, []string{internal.AgentFinalizer}, lvg.Finalizers)
			assert.Equal(t, []string{internal.AgentFinalizer}, llv.Finalizers)
			assert.NotContains(t, lvg.Annotations, v1alpha1.NodeMissingSinceAnnotation, "the node is back, so its absence is over")
			assert.Empty(t, f.events())
		})
	}
}

// A node the API server could not be asked about is not gone: nothing is
// written and nothing is released.
func TestMissingNodeReleaseDoesNothingWhenTheNodeCannotBeChecked(t *testing.T) {
	lvg := withMissingSince(releaseLVG("lvg-a", testGoneNode, true, internal.AgentFinalizer), time.Hour)
	store := fake.NewClientBuilder().WithScheme(releaseScheme(t)).WithObjects(lvg).Build()
	reader := interceptor.NewClient(store, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*v1.Node); ok {
				return errors.New("connection refused")
			}
			return c.Get(ctx, key, obj, opts...)
		},
	})
	r := &missingNodeRelease{cl: store, reader: reader, recorder: events.NewFakeRecorder(10), log: logger.NewNop(),
		metrics: monitoring.GetMetrics(""), grace: testGrace}

	require.NoError(t, r.pass(context.Background(), releaseNow))

	require.NoError(t, store.Get(context.Background(), client.ObjectKeyFromObject(lvg), lvg))
	assert.Equal(t, []string{internal.AgentFinalizer}, lvg.Finalizers)
	assert.Equal(t, missingSinceValue(time.Hour), lvg.Annotations[v1alpha1.NodeMissingSinceAnnotation])
}

// A group nobody asked to delete stays, with its node's absence recorded on it.
func TestMissingNodeReleaseLeavesAGroupNotBeingDeleted(t *testing.T) {
	lvg := withMissingSince(releaseLVG("lvg-a", testGoneNode, false, internal.AgentFinalizer), 24*time.Hour)
	f := newReleaseFixture(t, testGrace, nil, lvg)

	require.NoError(t, f.r.pass(context.Background(), releaseNow))

	require.False(t, f.gone(t, lvg))
	assert.Nil(t, lvg.DeletionTimestamp)
	assert.Equal(t, []string{internal.AgentFinalizer}, lvg.Finalizers)
	assert.Empty(t, f.events())
}

// Only the agent's finalizer is ever removed, and only once it is the last one:
// the consumer of a volume lets go of it first.
func TestMissingNodeReleaseWaitsForForeignFinalizers(t *testing.T) {
	lvg := withMissingSince(releaseLVG("lvg-a", testGoneNode, false, internal.AgentFinalizer), time.Hour)
	llv := releaseLLV("llv-a", "lvg-a", true, testForeignFinal, internal.AgentFinalizer)
	f := newReleaseFixture(t, testGrace, nil, lvg, llv)

	require.NoError(t, f.r.pass(context.Background(), releaseNow))

	require.False(t, f.gone(t, llv))
	assert.Equal(t, []string{testForeignFinal, internal.AgentFinalizer}, llv.Finalizers)
	assert.Equal(t, float64(1), testutil.ToFloat64(
		monitoring.GetMetrics("").MissingNodeResourcesHeld(kindLVMLogicalVolume, testGoneNode, heldReasonForeignFinalizers)))

	// The consumer removes its finalizer; the next pass removes the agent's.
	llv.Finalizers = []string{internal.AgentFinalizer}
	require.NoError(t, f.cl.Update(context.Background(), llv))
	require.NoError(t, f.r.pass(context.Background(), releaseNow))
	assert.True(t, f.gone(t, llv))
}

// A volume finds its node through its group: the one it names, or, when that
// one is gone, the one on the node and Volume Group in its status. The group
// itself is not being deleted.
func TestMissingNodeReleaseResolvesVolumesThroughTheirGroup(t *testing.T) {
	lvg := withMissingSince(releaseLVG("lvg-a", testGoneNode, false, internal.AgentFinalizer), time.Hour)
	named := releaseLLV("llv-named", "lvg-a", true, internal.AgentFinalizer)
	byStatus := releaseLLV("llv-by-status", "lvg-removed", true, internal.AgentFinalizer)
	byStatus.Status = &v1alpha1.LVMLogicalVolumeStatus{NodeName: testGoneNode, ActualVGNameOnTheNode: "vg-lvg-a"}
	elsewhere := releaseLLV("llv-elsewhere", "lvg-b", true, internal.AgentFinalizer)
	f := newReleaseFixture(t, testGrace, nil, notReadyNode(testPresentNode), lvg,
		releaseLVG("lvg-b", testPresentNode, false, internal.AgentFinalizer), named, byStatus, elsewhere)

	require.NoError(t, f.r.pass(context.Background(), releaseNow))

	assert.True(t, f.gone(t, named))
	assert.True(t, f.gone(t, byStatus))
	assert.False(t, f.gone(t, elsewhere))
	assert.False(t, f.gone(t, lvg))
}

// A snapshot finds its group through the volume it was taken from, or, when
// that volume is gone, through the node and Volume Group in its status.
func TestMissingNodeReleaseResolvesSnapshotsThroughTheirGroup(t *testing.T) {
	lvg := withMissingSince(releaseLVG("lvg-a", testGoneNode, false, internal.AgentFinalizer), time.Hour)
	llv := releaseLLV("llv-a", "lvg-a", false, internal.AgentFinalizer)
	throughVolume := releaseLLVS("llvs-volume", "llv-a", nil, internal.AgentFinalizer)
	byStatus := releaseLLVS("llvs-status", "llv-removed",
		&v1alpha1.LVMLogicalVolumeSnapshotStatus{NodeName: testGoneNode, ActualVGNameOnTheNode: "vg-lvg-a"}, internal.AgentFinalizer)
	unknown := releaseLLVS("llvs-unknown", "llv-removed", nil, internal.AgentFinalizer)
	f := newReleaseFixture(t, testGrace, nil, lvg, llv, throughVolume, byStatus, unknown)

	require.NoError(t, f.r.pass(context.Background(), releaseNow))

	assert.True(t, f.gone(t, throughVolume))
	assert.True(t, f.gone(t, byStatus))
	assert.False(t, f.gone(t, unknown), "a snapshot with no group to find its node by is left alone")
	assert.False(t, f.gone(t, llv), "the volume is not being deleted")
}

// A group being deleted waits for the volumes and snapshots on it, which find
// their node through it, and is released once they are gone.
func TestMissingNodeReleaseReleasesTheGroupAfterItsVolumes(t *testing.T) {
	lvg := withMissingSince(releaseLVG("lvg-a", testGoneNode, true, internal.AgentFinalizer), time.Hour)
	llv := releaseLLV("llv-a", "lvg-a", true, internal.AgentFinalizer)
	kept := releaseLLV("llv-kept", "lvg-a", false, internal.AgentFinalizer)
	f := newReleaseFixture(t, testGrace, nil, lvg, llv, kept)

	require.NoError(t, f.r.pass(context.Background(), releaseNow))
	assert.True(t, f.gone(t, llv))
	require.False(t, f.gone(t, lvg))
	assert.Equal(t, float64(1), testutil.ToFloat64(
		monitoring.GetMetrics("").MissingNodeResourcesHeld(kindLVMVolumeGroup, testGoneNode, heldReasonVolumesLeft)))

	require.NoError(t, f.cl.Delete(context.Background(), kept))
	require.NoError(t, f.r.pass(context.Background(), releaseNow))
	require.False(t, f.gone(t, lvg), "the volume being deleted still holds the group")
	require.NoError(t, f.r.pass(context.Background(), releaseNow))
	assert.True(t, f.gone(t, lvg))
}

func TestMissingNodeReleaseDisabled(t *testing.T) {
	lvg := withMissingSince(releaseLVG("lvg-a", testGoneNode, true, internal.AgentFinalizer), 24*time.Hour)
	f := newReleaseFixture(t, 0, nil, lvg)

	require.NoError(t, f.r.pass(context.Background(), releaseNow))

	require.False(t, f.gone(t, lvg))
	assert.Equal(t, []string{internal.AgentFinalizer}, lvg.Finalizers)
	assert.Equal(t, float64(1), testutil.ToFloat64(
		monitoring.GetMetrics("").MissingNodeResourcesHeld(kindLVMVolumeGroup, testGoneNode, heldReasonReleaseDisabled)))
}

// An annotation that cannot be read starts the grace period over rather than
// releasing anything at once.
func TestMissingNodeReleaseRewritesAnUnreadableAnnotation(t *testing.T) {
	lvg := releaseLVG("lvg-a", testGoneNode, true, internal.AgentFinalizer)
	lvg.Annotations = map[string]string{v1alpha1.NodeMissingSinceAnnotation: "yesterday"}
	f := newReleaseFixture(t, testGrace, nil, lvg)

	require.NoError(t, f.r.pass(context.Background(), releaseNow))

	require.False(t, f.gone(t, lvg))
	assert.Equal(t, releaseNow.Format(time.RFC3339), lvg.Annotations[v1alpha1.NodeMissingSinceAnnotation])
	assert.True(t, strings.Contains(strings.Join(f.events(), "\n"), eventReasonNodeMissing))
}
