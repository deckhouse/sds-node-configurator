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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-common-lib/conditions"
	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/controller/internal"
)

// drainEvents returns what the fake recorder has received so far.
func drainEvents(rec *events.FakeRecorder) []string {
	var got []string
	for {
		select {
		case e := <-rec.Events:
			got = append(got, e)
		default:
			return got
		}
	}
}

// The aggregate Ready of an LVMVolumeGroup records an Event when it moves and
// only then. A group still waiting for its stages, or being deleted, is not
// broken, so going there records nothing; coming out of it does.
func TestLVGReadyRecordsTransitions(t *testing.T) {
	ctx := context.Background()
	writes := 0
	lvg := lvgWith(falseCond(internal.TypeVGConfigurationApplied, internal.ReasonCreating))
	cl := newVerdictTestClient(t, &writes, lvg)
	fake := events.NewFakeRecorder(10)
	rec := withoutProgressEvents(fake)

	setStages := func(conds ...metav1.Condition) {
		t.Helper()
		got := &v1alpha1.LVMVolumeGroup{}
		require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(lvg), got))
		for _, c := range conds {
			conditions.Set(&got.Status.Conditions, c)
		}
		require.NoError(t, cl.Status().Update(ctx, got))
	}
	applied := metav1.Condition{Type: internal.TypeVGConfigurationApplied, Status: metav1.ConditionTrue, Reason: "Applied"}
	vgReady := metav1.Condition{Type: internal.TypeVGReady, Status: metav1.ConditionTrue, Reason: "Applied"}
	ready := "Normal ValidConditionStates every condition has a proper state"

	steps := []struct {
		name   string
		stages []metav1.Condition
		want   []string
	}{
		{"created", []metav1.Condition{falseCond(internal.TypeVGConfigurationApplied, internal.ReasonCreating)}, nil},
		{"applied", []metav1.Condition{applied}, []string{ready}},
		{"resync", []metav1.Condition{applied}, nil},
		{"breaks", []metav1.Condition{falseCond(internal.TypeVGConfigurationApplied, "VGCreationFailed")},
			[]string{"Warning InvalidConditionStates conditions VGConfigurationApplied has False status"}},
		{"breaks further", []metav1.Condition{falseCond(internal.TypeVGReady, "ScanFailed")}, nil},
		{"recovers", []metav1.Condition{applied, vgReady}, []string{ready}},
		{"deleted", []metav1.Condition{falseCond(internal.TypeVGConfigurationApplied, internal.ReasonTerminating)}, nil},
	}

	for _, s := range steps {
		setStages(s.stages...)
		_, err := updateLVGReadyConditionAndPhase(ctx, cl, rec, lvg)
		require.NoError(t, err, s.name)
		assert.Equal(t, s.want, drainEvents(fake), s.name)
	}
}

// The Ready of an LVMVolumeGroupSet records an Event when it moves and only
// then. Pending is where every new spec starts, so going there records nothing.
func TestLVGSetReadyRecordsTransitions(t *testing.T) {
	ctx := context.Background()
	cl := newFakeClientWithLVGSetStatus()
	lvgSet := &v1alpha1.LVMVolumeGroupSet{ObjectMeta: metav1.ObjectMeta{Name: "set-a"}}
	require.NoError(t, cl.Create(ctx, lvgSet))
	fake := events.NewFakeRecorder(10)
	rec := withoutProgressEvents(fake)

	steps := []struct {
		name   string
		phase  string
		reason string
		want   []string
	}{
		{"new spec", v1alpha1.PhasePending, reasonWorkInProgress, nil},
		{"created", phaseCreated, "", []string{"Normal Reconciled the LVMVolumeGroupSet is in the Created phase"}},
		{"resync", phaseCreated, "", nil},
		{"breaks", phaseNotCreated, "no nodes found by specified nodeSelector",
			[]string{"Warning ReconcileFailed no nodes found by specified nodeSelector"}},
		{"breaks otherwise", phaseNotCreated, "unable to create the LVMVolumeGroup", nil},
		{"recovers", phaseCreated, "", []string{"Normal Reconciled the LVMVolumeGroupSet is in the Created phase"}},
	}

	for _, s := range steps {
		require.NoError(t, updateLVMVolumeGroupSetPhaseIfNeeded(ctx, cl, rec, logger.NewNop(), lvgSet, s.phase, s.reason), s.name)
		assert.Equal(t, s.want, drainEvents(fake), s.name)
	}
}

// The filter drops only Warnings under a progress reason: a Normal Event under
// the same word, or a Warning under any other, goes through.
func TestWithoutProgressEvents(t *testing.T) {
	assert.Nil(t, withoutProgressEvents(nil), "a nil recorder must stay nil so that the helpers record nothing")

	fake := events.NewFakeRecorder(10)
	rec := withoutProgressEvents(fake)
	lvg := &v1alpha1.LVMVolumeGroup{}

	rec.Eventf(lvg, nil, "Warning", conditions.ReasonPending, "Ready", "%s", "waiting")
	rec.Eventf(lvg, nil, "Warning", internal.ReasonTerminating, "Ready", "%s", "deleting")
	rec.Eventf(lvg, nil, "Normal", conditions.ReasonPending, "Ready", "%s", "kept")
	rec.Eventf(lvg, nil, "Warning", "InvalidConditionStates", "Ready", "%s", "kept")

	assert.Equal(t, []string{"Normal Pending kept", "Warning InvalidConditionStates kept"}, drainEvents(fake))
}
