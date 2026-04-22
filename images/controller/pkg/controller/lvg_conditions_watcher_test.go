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
	"encoding/json"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/controller/internal"
)

func TestLVGConditionsWatcher(t *testing.T) {
	cl := NewFakeClient()
	ctx := context.Background()

	t.Run("getCRD", func(t *testing.T) {
		targetName := "target"
		crds := []v1.CustomResourceDefinition{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name: targetName,
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Name: "other-name",
				},
			},
		}

		for _, crd := range crds {
			err := cl.Create(ctx, &crd)
			if err != nil {
				t.Error(err)
			}
		}

		crd, err := getCRD(ctx, cl, targetName)
		if err != nil {
			t.Error(err)
		}

		assert.Equal(t, targetName, crd.Name)
	})

	t.Run("getTargetConditionsCount", func(t *testing.T) {
		first, err := json.Marshal("first")
		if err != nil {
			t.Error(err)
		}
		second, err := json.Marshal("second")
		if err != nil {
			t.Error(err)
		}
		third, err := json.Marshal("third")
		if err != nil {
			t.Error(err)
		}
		crd := &v1.CustomResourceDefinition{
			Spec: v1.CustomResourceDefinitionSpec{
				Versions: []v1.CustomResourceDefinitionVersion{
					{
						Schema: &v1.CustomResourceValidation{
							OpenAPIV3Schema: &v1.JSONSchemaProps{
								Properties: map[string]v1.JSONSchemaProps{
									"status": {
										Properties: map[string]v1.JSONSchemaProps{
											"conditions": {
												Items: &v1.JSONSchemaPropsOrArray{
													Schema: &v1.JSONSchemaProps{
														Properties: map[string]v1.JSONSchemaProps{
															"type": {
																Enum: []v1.JSON{
																	{
																		Raw: first,
																	},
																	{
																		Raw: second,
																	},
																	{
																		Raw: third,
																	},
																},
															},
														},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		}

		count, err := getTargetConditionsCount(crd)
		if err != nil {
			t.Error(err)
		}

		assert.Equal(t, 3, count)
	})
}

// TestDecideLVGReadyAndPhase is a table-driven test of the pure decision
// function that replaced the inline branching inside reconcileLVGConditions.
// No fake client is needed — the function does not perform any I/O.
func TestDecideLVGReadyAndPhase(t *testing.T) {
	const (
		vgApplied = internal.TypeVGConfigurationApplied
		nodeReady = "NodesReady"
		podReady  = "AgentReady"
	)

	tests := []struct {
		name             string
		conditions       []metav1.Condition
		targetCount      int
		wantPhase        string
		wantReadyStatus  metav1.ConditionStatus
		wantReadyReason  string
		wantReadyMessage string
		wantChanged      bool
	}{
		{
			name: "missing_conditions_fewer_than_target",
			conditions: []metav1.Condition{
				{Type: vgApplied, Status: metav1.ConditionTrue, Reason: "Applied"},
			},
			targetCount:      3,
			wantPhase:        v1alpha1.PhasePending,
			wantReadyStatus:  metav1.ConditionFalse,
			wantReadyReason:  internal.ReasonPending,
			wantReadyMessage: "wait for conditions to got configured",
			wantChanged:      true,
		},
		{
			name: "reason_creating_short_circuits_to_pending",
			conditions: []metav1.Condition{
				{Type: vgApplied, Status: metav1.ConditionFalse, Reason: internal.ReasonCreating},
				{Type: nodeReady, Status: metav1.ConditionTrue, Reason: "NodesReady"},
			},
			targetCount:      2,
			wantPhase:        v1alpha1.PhasePending,
			wantReadyStatus:  metav1.ConditionFalse,
			wantReadyReason:  internal.ReasonPending,
			wantReadyMessage: "condition " + vgApplied + " has Creating reason",
			wantChanged:      true,
		},
		{
			name: "reason_terminating_short_circuits_to_terminating",
			conditions: []metav1.Condition{
				{Type: vgApplied, Status: metav1.ConditionFalse, Reason: internal.ReasonTerminating},
				{Type: nodeReady, Status: metav1.ConditionTrue, Reason: "NodesReady"},
			},
			targetCount:      2,
			wantPhase:        v1alpha1.PhaseTerminating,
			wantReadyStatus:  metav1.ConditionFalse,
			wantReadyReason:  internal.ReasonTerminating,
			wantReadyMessage: "condition " + vgApplied + " has Terminating reason",
			wantChanged:      true,
		},
		{
			name: "false_non_acceptable_reason_yields_not_ready",
			conditions: []metav1.Condition{
				{Type: vgApplied, Status: metav1.ConditionFalse, Reason: "SomeOtherFailure"},
				{Type: nodeReady, Status: metav1.ConditionTrue, Reason: "NodesReady"},
			},
			targetCount:      2,
			wantPhase:        v1alpha1.PhaseNotReady,
			wantReadyStatus:  metav1.ConditionFalse,
			wantReadyReason:  "InvalidConditionStates",
			wantReadyMessage: "conditions " + vgApplied + " has False status",
			wantChanged:      true,
		},
		{
			name: "false_acceptable_reason_updating_does_not_block_ready",
			conditions: []metav1.Condition{
				{Type: vgApplied, Status: metav1.ConditionFalse, Reason: internal.ReasonUpdating},
				{Type: nodeReady, Status: metav1.ConditionTrue, Reason: "NodesReady"},
			},
			targetCount:      2,
			wantPhase:        v1alpha1.PhaseReady,
			wantReadyStatus:  metav1.ConditionTrue,
			wantReadyReason:  "ValidConditionStates",
			wantReadyMessage: "every condition has a proper state",
			wantChanged:      true,
		},
		{
			name: "false_acceptable_reason_validation_failed_does_not_block_ready",
			conditions: []metav1.Condition{
				{Type: vgApplied, Status: metav1.ConditionFalse, Reason: internal.ReasonValidationFailed},
				{Type: nodeReady, Status: metav1.ConditionTrue, Reason: "NodesReady"},
			},
			targetCount:      2,
			wantPhase:        v1alpha1.PhaseReady,
			wantReadyStatus:  metav1.ConditionTrue,
			wantReadyReason:  "ValidConditionStates",
			wantReadyMessage: "every condition has a proper state",
			wantChanged:      true,
		},
		{
			name: "all_true_happy_path",
			conditions: []metav1.Condition{
				{Type: vgApplied, Status: metav1.ConditionTrue, Reason: "Applied"},
				{Type: nodeReady, Status: metav1.ConditionTrue, Reason: "NodesReady"},
				{Type: podReady, Status: metav1.ConditionTrue, Reason: "PodReady"},
			},
			targetCount:      3,
			wantPhase:        v1alpha1.PhaseReady,
			wantReadyStatus:  metav1.ConditionTrue,
			wantReadyReason:  "ValidConditionStates",
			wantReadyMessage: "every condition has a proper state",
			wantChanged:      true,
		},
		{
			name: "ready_condition_itself_is_ignored_even_if_stale_false",
			conditions: []metav1.Condition{
				{Type: internal.TypeReady, Status: metav1.ConditionFalse, Reason: "Stale"},
				{Type: vgApplied, Status: metav1.ConditionTrue, Reason: "Applied"},
				{Type: nodeReady, Status: metav1.ConditionTrue, Reason: "NodesReady"},
			},
			targetCount:      3,
			wantPhase:        v1alpha1.PhaseReady,
			wantReadyStatus:  metav1.ConditionTrue,
			wantReadyReason:  "ValidConditionStates",
			wantReadyMessage: "every condition has a proper state",
			wantChanged:      true,
		},
		{
			name: "multiple_false_non_acceptable_conditions_are_joined",
			conditions: []metav1.Condition{
				{Type: vgApplied, Status: metav1.ConditionFalse, Reason: "SomeOtherFailure"},
				{Type: nodeReady, Status: metav1.ConditionFalse, Reason: "SomeOtherFailure"},
			},
			targetCount:      2,
			wantPhase:        v1alpha1.PhaseNotReady,
			wantReadyStatus:  metav1.ConditionFalse,
			wantReadyReason:  "InvalidConditionStates",
			wantReadyMessage: "conditions " + vgApplied + "," + nodeReady + " has False status",
			wantChanged:      true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			phase, readyStatus, readyReason, readyMessage, changed := decideLVGReadyAndPhase(tc.conditions, tc.targetCount)

			assert.Equal(t, tc.wantChanged, changed, "changed")
			assert.Equal(t, tc.wantPhase, phase, "phase")
			assert.Equal(t, tc.wantReadyStatus, readyStatus, "readyStatus")
			assert.Equal(t, tc.wantReadyReason, readyReason, "readyReason")
			assert.Equal(t, tc.wantReadyMessage, readyMessage, "readyMessage")
		})
	}
}

// TestUpdateLVGReadyConditionAndPhaseIfNeeded verifies the atomic behavior of
// the helper on top of a fake client: a single Status().Update() is issued
// when (and only when) something actually changed. ResourceVersion is used as
// a proxy for counting writes — the controller-runtime fake client bumps it
// on every successful Update.
func TestUpdateLVGReadyConditionAndPhaseIfNeeded(t *testing.T) {
	ctx := context.Background()

	const lvgName = "test-lvg"

	const (
		readyReason  = "ValidConditionStates"
		readyMessage = "every condition has a proper state"
		notReadyReas = "InvalidConditionStates"
		notReadyMsg  = "conditions X has False status"
	)

	t.Run("creates_missing_ready_condition_and_flips_phase_in_one_update", func(t *testing.T) {
		seeded := &v1alpha1.LVMVolumeGroup{
			ObjectMeta: metav1.ObjectMeta{Name: lvgName},
			Status: v1alpha1.LVMVolumeGroupStatus{
				Phase: v1alpha1.PhaseNotReady,
				Conditions: []metav1.Condition{
					{Type: internal.TypeVGConfigurationApplied, Status: metav1.ConditionTrue, Reason: "Applied"},
				},
			},
		}
		cl := newFakeClientWithLVG(seeded)

		var before v1alpha1.LVMVolumeGroup
		require.NoError(t, cl.Get(ctx, client.ObjectKey{Name: lvgName}, &before))
		rvBefore := before.ResourceVersion

		err := updateLVGReadyConditionAndPhaseIfNeeded(
			ctx, cl, &before,
			metav1.ConditionTrue, readyReason, readyMessage, v1alpha1.PhaseReady,
		)
		require.NoError(t, err)

		var after v1alpha1.LVMVolumeGroup
		require.NoError(t, cl.Get(ctx, client.ObjectKey{Name: lvgName}, &after))

		assert.Equal(t, v1alpha1.PhaseReady, after.Status.Phase, "phase must be updated")

		readyCount, readyIdx := countReady(after.Status.Conditions)
		require.Equal(t, 1, readyCount, "exactly one Ready condition after the update")
		ready := after.Status.Conditions[readyIdx]
		assert.Equal(t, metav1.ConditionTrue, ready.Status)
		assert.Equal(t, readyReason, ready.Reason)
		assert.Equal(t, readyMessage, ready.Message)

		assert.NotEqual(t, rvBefore, after.ResourceVersion, "exactly one Status().Update() must have occurred")
	})

	t.Run("noop_when_ready_condition_and_phase_already_match", func(t *testing.T) {
		lvg := &v1alpha1.LVMVolumeGroup{
			ObjectMeta: metav1.ObjectMeta{Name: lvgName},
			Status: v1alpha1.LVMVolumeGroupStatus{
				Phase: v1alpha1.PhaseReady,
				Conditions: []metav1.Condition{
					{
						Type:               internal.TypeReady,
						Status:             metav1.ConditionTrue,
						Reason:             readyReason,
						Message:            readyMessage,
						ObservedGeneration: 0,
					},
				},
			},
		}
		cl := newFakeClientWithLVG(lvg)

		var before v1alpha1.LVMVolumeGroup
		require.NoError(t, cl.Get(ctx, client.ObjectKey{Name: lvgName}, &before))
		rvBefore := before.ResourceVersion

		err := updateLVGReadyConditionAndPhaseIfNeeded(
			ctx, cl, &before,
			metav1.ConditionTrue, readyReason, readyMessage, v1alpha1.PhaseReady,
		)
		require.NoError(t, err)

		var after v1alpha1.LVMVolumeGroup
		require.NoError(t, cl.Get(ctx, client.ObjectKey{Name: lvgName}, &after))
		assert.Equal(t, rvBefore, after.ResourceVersion, "no Status().Update() expected")
	})

	t.Run("atomic_update_bumps_rv_once_even_when_both_fields_change", func(t *testing.T) {
		lvg := &v1alpha1.LVMVolumeGroup{
			ObjectMeta: metav1.ObjectMeta{Name: lvgName},
			Status: v1alpha1.LVMVolumeGroupStatus{
				Phase: v1alpha1.PhaseNotReady,
				Conditions: []metav1.Condition{
					{
						Type:    internal.TypeReady,
						Status:  metav1.ConditionFalse,
						Reason:  notReadyReas,
						Message: notReadyMsg,
					},
					{Type: internal.TypeVGConfigurationApplied, Status: metav1.ConditionTrue, Reason: "Applied"},
				},
			},
		}
		cl := newFakeClientWithLVG(lvg)

		var before v1alpha1.LVMVolumeGroup
		require.NoError(t, cl.Get(ctx, client.ObjectKey{Name: lvgName}, &before))
		rvBefore := parseRV(t, before.ResourceVersion)

		err := updateLVGReadyConditionAndPhaseIfNeeded(
			ctx, cl, &before,
			metav1.ConditionTrue, readyReason, readyMessage, v1alpha1.PhaseReady,
		)
		require.NoError(t, err)

		var after v1alpha1.LVMVolumeGroup
		require.NoError(t, cl.Get(ctx, client.ObjectKey{Name: lvgName}, &after))
		rvAfter := parseRV(t, after.ResourceVersion)

		assert.Equal(t, v1alpha1.PhaseReady, after.Status.Phase)
		readyCount, readyIdx := countReady(after.Status.Conditions)
		require.Equal(t, 1, readyCount, "no duplicate Ready condition")
		assert.Equal(t, metav1.ConditionTrue, after.Status.Conditions[readyIdx].Status)

		assert.Equal(t, rvBefore+1, rvAfter, "exactly one Status().Update() must have occurred")
	})

	t.Run("mutates_existing_ready_condition_without_duplicating", func(t *testing.T) {
		lvg := &v1alpha1.LVMVolumeGroup{
			ObjectMeta: metav1.ObjectMeta{Name: lvgName},
			Status: v1alpha1.LVMVolumeGroupStatus{
				Phase: v1alpha1.PhaseNotReady,
				Conditions: []metav1.Condition{
					{Type: internal.TypeReady, Status: metav1.ConditionFalse, Reason: notReadyReas, Message: notReadyMsg},
					{Type: internal.TypeVGConfigurationApplied, Status: metav1.ConditionTrue, Reason: "Applied"},
				},
			},
		}
		cl := newFakeClientWithLVG(lvg)

		var got v1alpha1.LVMVolumeGroup
		require.NoError(t, cl.Get(ctx, client.ObjectKey{Name: lvgName}, &got))

		err := updateLVGReadyConditionAndPhaseIfNeeded(
			ctx, cl, &got,
			metav1.ConditionTrue, readyReason, readyMessage, v1alpha1.PhaseReady,
		)
		require.NoError(t, err)

		var after v1alpha1.LVMVolumeGroup
		require.NoError(t, cl.Get(ctx, client.ObjectKey{Name: lvgName}, &after))

		readyCount, readyIdx := countReady(after.Status.Conditions)
		require.Equal(t, 1, readyCount, "exactly one Ready condition after mutation")
		assert.Equal(t, metav1.ConditionTrue, after.Status.Conditions[readyIdx].Status)
		assert.Equal(t, readyReason, after.Status.Conditions[readyIdx].Reason)
		assert.Equal(t, readyMessage, after.Status.Conditions[readyIdx].Message)
	})
}

// newFakeClientWithLVG builds a fake client that (a) knows about our API types
// and (b) treats LVMVolumeGroup as a status-subresource-aware type, i.e. seeds
// the initial object (with its Status) via WithObjects and honors
// Status().Update() calls the way a real API server would. This is what the
// tests need to observe resourceVersion bumps per Status().Update().
func newFakeClientWithLVG(seed *v1alpha1.LVMVolumeGroup) client.WithWatch {
	s := scheme.Scheme
	_ = metav1.AddMetaToScheme(s)
	_ = v1alpha1.AddToScheme(s)
	_ = v1.AddToScheme(s)

	return fake.NewClientBuilder().
		WithScheme(s).
		WithStatusSubresource(&v1alpha1.LVMVolumeGroup{}).
		WithObjects(seed).
		Build()
}

func countReady(conds []metav1.Condition) (count, idx int) {
	idx = -1
	for i, c := range conds {
		if c.Type == internal.TypeReady {
			count++
			idx = i
		}
	}
	return count, idx
}

// parseRV parses a controller-runtime fake-client resourceVersion (it is an
// integer encoded as a string).
func parseRV(t *testing.T, rv string) int {
	t.Helper()
	n, err := strconv.Atoi(rv)
	if err != nil {
		t.Fatalf("unable to parse resourceVersion %q: %v", rv, err)
	}
	return n
}
