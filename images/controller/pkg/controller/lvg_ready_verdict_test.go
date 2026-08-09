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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/deckhouse/sds-common-lib/conditions"
	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/controller/internal"
)

// lvgWith builds an LVMVolumeGroup carrying the given conditions. Types not
// listed are filled in as True, so a case only has to state what it is about —
// a missing type is a verdict of its own and would otherwise mask everything.
func lvgWith(conds ...metav1.Condition) *v1alpha1.LVMVolumeGroup {
	present := map[string]bool{}
	for _, c := range conds {
		present[c.Type] = true
	}

	lvg := &v1alpha1.LVMVolumeGroup{}
	lvg.Name = "test-lvg"
	lvg.Generation = 4
	lvg.Status.Conditions = append([]metav1.Condition{}, conds...)

	for _, conType := range internal.LVGConditionTypes {
		if present[conType] {
			continue
		}
		lvg.Status.Conditions = append(lvg.Status.Conditions, metav1.Condition{
			Type:   conType,
			Status: metav1.ConditionTrue,
			Reason: "Applied",
		})
	}

	return lvg
}

func falseCond(conType, reason string) metav1.Condition {
	return metav1.Condition{Type: conType, Status: metav1.ConditionFalse, Reason: reason}
}

func TestDecideLVGReadyAndPhase(t *testing.T) {
	for _, tc := range []struct {
		name       string
		lvg        *v1alpha1.LVMVolumeGroup
		wantPhase  string
		wantStatus metav1.ConditionStatus
		wantReason string
	}{
		{
			name:       "every condition True",
			lvg:        lvgWith(),
			wantPhase:  v1alpha1.PhaseReady,
			wantStatus: metav1.ConditionTrue,
			wantReason: "ValidConditionStates",
		},
		{
			name: "a condition has not been reported yet",
			lvg: func() *v1alpha1.LVMVolumeGroup {
				lvg := lvgWith()
				// Drop one, which is what a fresh LVMVolumeGroup looks like.
				lvg.Status.Conditions = lvg.Status.Conditions[1:]
				return lvg
			}(),
			wantPhase:  v1alpha1.PhasePending,
			wantStatus: metav1.ConditionFalse,
			wantReason: conditions.ReasonPending,
		},
		{
			name:       "a stage is still being created",
			lvg:        lvgWith(falseCond(internal.TypeVGConfigurationApplied, internal.ReasonCreating)),
			wantPhase:  v1alpha1.PhasePending,
			wantStatus: metav1.ConditionFalse,
			wantReason: conditions.ReasonPending,
		},
		{
			name:       "a stage is terminating",
			lvg:        lvgWith(falseCond(internal.TypeVGConfigurationApplied, internal.ReasonTerminating)),
			wantPhase:  v1alpha1.PhaseTerminating,
			wantStatus: metav1.ConditionFalse,
			wantReason: internal.ReasonTerminating,
		},
		{
			name:       "a stage failed for a reason that takes the group out of service",
			lvg:        lvgWith(falseCond(internal.TypeVGConfigurationApplied, "VGCreationFailed")),
			wantPhase:  v1alpha1.PhaseNotReady,
			wantStatus: metav1.ConditionFalse,
			wantReason: "InvalidConditionStates",
		},
		{
			// The whole point of acceptableReasons: the Volume Group keeps
			// serving its volumes, so it must not drop out of Ready.
			name:       "a stage is False for an acceptable reason",
			lvg:        lvgWith(falseCond(internal.TypeVGConfigurationApplied, internal.ReasonFileDeviceDrift)),
			wantPhase:  v1alpha1.PhaseReady,
			wantStatus: metav1.ConditionTrue,
			wantReason: "ValidConditionStates",
		},
		{
			// A stage that says it has not reached a verdict is not evidence
			// that the Volume Group is healthy. Reporting Ready here would let
			// the scheduler place volumes on nobody's word.
			name:       "a stage reports Unknown",
			lvg:        lvgWith(metav1.Condition{Type: internal.TypeVGReady, Status: metav1.ConditionUnknown, Reason: "Probing"}),
			wantPhase:  v1alpha1.PhaseNotReady,
			wantStatus: metav1.ConditionFalse,
			wantReason: "InvalidConditionStates",
		},
		{
			// acceptableReasons excuse a False — "something went wrong but the
			// volumes keep working" — which is a claim only a writer that
			// reached a verdict can make. It must not excuse an Unknown.
			name:       "an acceptable reason does not excuse Unknown",
			lvg:        lvgWith(metav1.Condition{Type: internal.TypeVGConfigurationApplied, Status: metav1.ConditionUnknown, Reason: internal.ReasonFileDeviceDrift}),
			wantPhase:  v1alpha1.PhaseNotReady,
			wantStatus: metav1.ConditionFalse,
			wantReason: "InvalidConditionStates",
		},
		{
			// Ready is the verdict, not an input: a stale False Ready must not
			// keep the group out of Ready once every stage is True.
			name:       "a stale False Ready is ignored",
			lvg:        lvgWith(falseCond(internal.TypeReady, "InvalidConditionStates")),
			wantPhase:  v1alpha1.PhaseReady,
			wantStatus: metav1.ConditionTrue,
			wantReason: "ValidConditionStates",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := decideLVGReadyAndPhase(tc.lvg)

			assert.Equal(t, tc.wantPhase, got.phase)
			assert.Equal(t, internal.TypeReady, got.ready.Type)
			assert.Equal(t, tc.wantStatus, got.ready.Status)
			assert.Equal(t, tc.wantReason, got.ready.Reason)
			assert.Equal(t, tc.lvg.Generation, got.ready.ObservedGeneration,
				"the verdict must say which generation it describes")
			assert.NotEmpty(t, got.ready.Message, "the CRD requires a non-empty message")
		})
	}
}

// Creating and Terminating outrank a plain False whatever order the conditions
// happen to be in, which is what the loop that used to break here did.
func TestDecideLVGReadyAndPhaseRanksCreatingAndTerminatingFirst(t *testing.T) {
	for _, tc := range []struct {
		name      string
		reason    string
		wantPhase string
	}{
		{"Creating", internal.ReasonCreating, v1alpha1.PhasePending},
		{"Terminating", internal.ReasonTerminating, v1alpha1.PhaseTerminating},
	} {
		t.Run(tc.name+" after a failed stage", func(t *testing.T) {
			lvg := lvgWith(
				falseCond(internal.TypeVGConfigurationApplied, "VGCreationFailed"),
				falseCond(internal.TypeVGReady, tc.reason),
			)

			assert.Equal(t, tc.wantPhase, decideLVGReadyAndPhase(lvg).phase)
		})

		t.Run(tc.name+" before a failed stage", func(t *testing.T) {
			lvg := lvgWith(
				falseCond(internal.TypeVGConfigurationApplied, tc.reason),
				falseCond(internal.TypeVGReady, "VGCreationFailed"),
			)

			assert.Equal(t, tc.wantPhase, decideLVGReadyAndPhase(lvg).phase)
		})
	}
}

// The function has to be total. A verdict with an empty phase or an empty status
// would leave the LVMVolumeGroup reporting neither, which is the state the
// scheduler and the e2e suite cannot interpret.
func TestDecideLVGReadyAndPhaseIsTotal(t *testing.T) {
	for _, lvg := range []*v1alpha1.LVMVolumeGroup{
		{},
		lvgWith(),
		lvgWith(falseCond(internal.TypeNodeReady, "NodeNotReady")),
		lvgWith(falseCond(internal.TypeAgentReady, "")),
		lvgWith(metav1.Condition{Type: internal.TypeVGReady, Status: metav1.ConditionUnknown}),
	} {
		got := decideLVGReadyAndPhase(lvg)

		assert.NotEmpty(t, got.phase)
		assert.NotEmpty(t, got.ready.Status)
		assert.NotEmpty(t, got.ready.Reason)
		assert.NotEmpty(t, got.ready.Message)
	}
}

func newVerdictTestClient(t *testing.T, statusWrites *int, objs ...client.Object) client.Client {
	t.Helper()

	scheme := apiruntime.NewScheme()
	require.NoError(t, v1alpha1.AddToScheme(scheme))

	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.LVMVolumeGroup{}).
		WithObjects(objs...).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceUpdate: func(
				ctx context.Context, cl client.Client, subResource string,
				obj client.Object, opts ...client.SubResourceUpdateOption,
			) error {
				if subResource == "status" {
					*statusWrites++
				}
				return cl.Status().Update(ctx, obj, opts...)
			},
		}).
		Build()
}

// The condition and the phase have to reach the API server in one write.
//
// Two writes let a reader catch the object with a phase its conditions do not
// support, and if the second one failed the two stayed in disagreement until
// something else triggered a reconcile — which is how an LVMVolumeGroup ended up
// stuck reporting NotReady.
func TestTheReadyConditionAndThePhaseAreWrittenTogether(t *testing.T) {
	writes := 0
	lvg := lvgWith(falseCond(internal.TypeVGConfigurationApplied, "VGCreationFailed"))
	cl := newVerdictTestClient(t, &writes, lvg)

	verdict, err := updateLVGReadyConditionAndPhase(context.Background(), cl, lvg)
	require.NoError(t, err)

	assert.Equal(t, 1, writes, "the condition and the phase must be one status write")
	assert.Equal(t, v1alpha1.PhaseNotReady, verdict.phase)

	got := &v1alpha1.LVMVolumeGroup{}
	require.NoError(t, cl.Get(context.Background(), client.ObjectKeyFromObject(lvg), got))

	assert.Equal(t, v1alpha1.PhaseNotReady, got.Status.Phase)
	ready := findLVGCondition(t, got, internal.TypeReady)
	assert.Equal(t, metav1.ConditionFalse, ready.Status)
	assert.Equal(t, "InvalidConditionStates", ready.Reason)
}

// A resync that changes nothing must not write at all: the phase and the
// condition are recomputed on every pass, and an unconditional write would turn
// a quiet cluster into a stream of etcd writes and watch events.
func TestNothingIsWrittenWhenTheVerdictHasNotChanged(t *testing.T) {
	writes := 0
	lvg := lvgWith()
	cl := newVerdictTestClient(t, &writes, lvg)
	ctx := context.Background()

	_, err := updateLVGReadyConditionAndPhase(ctx, cl, lvg)
	require.NoError(t, err)
	require.Equal(t, 1, writes)

	_, err = updateLVGReadyConditionAndPhase(ctx, cl, lvg)
	require.NoError(t, err)

	assert.Equal(t, 1, writes, "the second pass changed nothing and must not write")
}

// The caller keeps using lvg after the write — the reconcile logs the verdict and
// other code reads the phase — so the server-side values are mirrored back.
func TestTheWriteMirrorsServerSideValuesOntoTheCallersObject(t *testing.T) {
	writes := 0
	lvg := lvgWith()
	cl := newVerdictTestClient(t, &writes, lvg)

	before := lvg.ResourceVersion

	_, err := updateLVGReadyConditionAndPhase(context.Background(), cl, lvg)
	require.NoError(t, err)

	assert.Equal(t, v1alpha1.PhaseReady, lvg.Status.Phase)
	assert.NotEqual(t, before, lvg.ResourceVersion, "the resource version should have moved on")
	assert.Equal(t, metav1.ConditionTrue, findLVGCondition(t, lvg, internal.TypeReady).Status)
}

func findLVGCondition(t *testing.T, lvg *v1alpha1.LVMVolumeGroup, conType string) metav1.Condition {
	t.Helper()

	for _, c := range lvg.Status.Conditions {
		if c.Type == conType {
			return c
		}
	}
	t.Fatalf("condition %s was not written", conType)
	return metav1.Condition{}
}

// A declared field nothing writes is the same defect as a declared condition
// nobody sets: status.observedGeneration would sit at 0 forever, and a reader
// could not tell whether the phase describes the spec they just applied or the
// one before it.
//
// It is stamped from the object the verdict was computed against, so it always
// agrees with the aggregate condition written in the same pass.
func TestTheWriteRecordsTheGenerationTheVerdictDescribes(t *testing.T) {
	writes := 0
	lvg := lvgWith()
	lvg.Generation = 9
	cl := newVerdictTestClient(t, &writes, lvg)

	verdict, err := updateLVGReadyConditionAndPhase(context.Background(), cl, lvg)
	require.NoError(t, err)

	got := &v1alpha1.LVMVolumeGroup{}
	require.NoError(t, cl.Get(context.Background(), client.ObjectKeyFromObject(lvg), got))

	assert.Equal(t, int64(9), got.Status.ObservedGeneration)
	assert.Equal(t, got.Status.ObservedGeneration, findLVGCondition(t, got, internal.TypeReady).ObservedGeneration,
		"the status and the aggregate condition must name the same generation")
	assert.Equal(t, int64(9), verdict.ready.ObservedGeneration)
}
