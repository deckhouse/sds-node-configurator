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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/controller/internal"
	"github.com/deckhouse/sds-node-configurator/images/controller/pkg/monitoring"
)

func readinessLVG(name, node string, conds ...metav1.Condition) *v1alpha1.LVMVolumeGroup {
	lvg := &v1alpha1.LVMVolumeGroup{}
	lvg.Name = name
	lvg.Spec.Local.NodeName = node
	lvg.Status.Conditions = conds
	return lvg
}

func readinessCond(conType string, status metav1.ConditionStatus, reason string) metav1.Condition {
	return metav1.Condition{Type: conType, Status: status, Reason: reason}
}

func TestLVGReadiness(t *testing.T) {
	healthy := func(except ...string) []metav1.Condition {
		skip := map[string]bool{}
		for _, conType := range except {
			skip[conType] = true
		}
		var out []metav1.Condition
		for _, conType := range []string{internal.TypeVGConfigurationApplied, internal.TypeVGReady, internal.TypeNodeReady, internal.TypeAgentReady} {
			if !skip[conType] {
				out = append(out, readinessCond(conType, metav1.ConditionTrue, "Applied"))
			}
		}
		return out
	}
	with := func(base []metav1.Condition, extra ...metav1.Condition) []metav1.Condition {
		return append(append([]metav1.Condition{}, base...), extra...)
	}

	for _, tc := range []struct {
		name          string
		conds         []metav1.Condition
		wantReady     bool
		wantCondition string
		wantReason    string
	}{
		{
			name:          "Ready True",
			conds:         with(healthy(), readinessCond(internal.TypeReady, metav1.ConditionTrue, "ValidConditionStates")),
			wantReady:     true,
			wantCondition: internal.TypeReady,
			wantReason:    "ValidConditionStates",
		},
		{
			name:          "Ready True wins over a stage the verdict has not caught up with",
			conds:         with(healthy(internal.TypeAgentReady), readinessCond(internal.TypeAgentReady, metav1.ConditionFalse, "PodNotReady"), readinessCond(internal.TypeReady, metav1.ConditionTrue, "ValidConditionStates")),
			wantReady:     true,
			wantCondition: internal.TypeReady,
			wantReason:    "ValidConditionStates",
		},
		{
			name:          "no conditions yet",
			wantCondition: internal.TypeReady,
			wantReason:    "Pending",
		},
		{
			name:          "being created",
			conds:         with(healthy(internal.TypeVGConfigurationApplied), readinessCond(internal.TypeVGConfigurationApplied, metav1.ConditionFalse, internal.ReasonCreating), readinessCond(internal.TypeReady, metav1.ConditionFalse, "Pending")),
			wantCondition: internal.TypeReady,
			wantReason:    "Pending",
		},
		{
			name:          "being deleted",
			conds:         with(healthy(internal.TypeVGConfigurationApplied), readinessCond(internal.TypeVGConfigurationApplied, metav1.ConditionFalse, internal.ReasonTerminating), readinessCond(internal.TypeReady, metav1.ConditionFalse, internal.ReasonTerminating)),
			wantCondition: internal.TypeReady,
			wantReason:    internal.ReasonTerminating,
		},
		{
			name: "being deleted on a node that is not Ready",
			conds: with(healthy(internal.TypeVGConfigurationApplied, internal.TypeNodeReady),
				readinessCond(internal.TypeVGConfigurationApplied, metav1.ConditionFalse, internal.ReasonTerminating),
				readinessCond(internal.TypeNodeReady, metav1.ConditionFalse, "NodeNotReady"),
				readinessCond(internal.TypeReady, metav1.ConditionFalse, internal.ReasonTerminating)),
			wantCondition: internal.TypeNodeReady,
			wantReason:    "NodeNotReady",
		},
		{
			name: "the node outranks what its agent last reported",
			conds: with(healthy(internal.TypeVGReady, internal.TypeNodeReady, internal.TypeAgentReady),
				readinessCond(internal.TypeVGReady, metav1.ConditionFalse, "ScanFailed"),
				readinessCond(internal.TypeAgentReady, metav1.ConditionFalse, "PodNotFound"),
				readinessCond(internal.TypeNodeReady, metav1.ConditionFalse, "MissingNode"),
				readinessCond(internal.TypeReady, metav1.ConditionFalse, "InvalidConditionStates")),
			wantCondition: internal.TypeNodeReady,
			wantReason:    "MissingNode",
		},
		{
			name: "the agent outranks the Volume Group stages",
			conds: with(healthy(internal.TypeVGConfigurationApplied, internal.TypeAgentReady),
				readinessCond(internal.TypeVGConfigurationApplied, metav1.ConditionFalse, "VGCheckFailed"),
				readinessCond(internal.TypeAgentReady, metav1.ConditionFalse, "PodNotReady"),
				readinessCond(internal.TypeReady, metav1.ConditionFalse, "InvalidConditionStates")),
			wantCondition: internal.TypeAgentReady,
			wantReason:    "PodNotReady",
		},
		{
			name: "VGReady outranks VGConfigurationApplied",
			conds: with(healthy(internal.TypeVGConfigurationApplied, internal.TypeVGReady),
				readinessCond(internal.TypeVGConfigurationApplied, metav1.ConditionFalse, "VGExtendFailed"),
				readinessCond(internal.TypeVGReady, metav1.ConditionFalse, "ScanFailed"),
				readinessCond(internal.TypeReady, metav1.ConditionFalse, "InvalidConditionStates")),
			wantCondition: internal.TypeVGReady,
			wantReason:    "ScanFailed",
		},
		{
			name: "a stage that keeps a fresh group Pending is named",
			conds: []metav1.Condition{
				readinessCond(internal.TypeVGConfigurationApplied, metav1.ConditionTrue, "Applied"),
				readinessCond(internal.TypeVGReady, metav1.ConditionFalse, "NodeNotDescribed"),
				readinessCond(internal.TypeNodeReady, metav1.ConditionTrue, "NodesReady"),
				readinessCond(internal.TypeReady, metav1.ConditionFalse, "Pending"),
			},
			wantCondition: internal.TypeVGReady,
			wantReason:    "NodeNotDescribed",
		},
		{
			name: "an acceptable reason is not named as the cause",
			conds: with(healthy(internal.TypeVGConfigurationApplied, internal.TypeVGReady),
				readinessCond(internal.TypeVGConfigurationApplied, metav1.ConditionFalse, internal.ReasonThinPoolCreationFailed),
				readinessCond(internal.TypeVGReady, metav1.ConditionFalse, "NoBlockDevices"),
				readinessCond(internal.TypeReady, metav1.ConditionFalse, "InvalidConditionStates")),
			wantCondition: internal.TypeVGReady,
			wantReason:    "NoBlockDevices",
		},
		{
			name: "only acceptable reasons left behind a stale verdict",
			conds: with(healthy(internal.TypeVGConfigurationApplied),
				readinessCond(internal.TypeVGConfigurationApplied, metav1.ConditionFalse, internal.ReasonThinPoolCreationFailed),
				readinessCond(internal.TypeReady, metav1.ConditionFalse, "InvalidConditionStates")),
			wantCondition: internal.TypeReady,
			wantReason:    "InvalidConditionStates",
		},
		{
			name: "Unknown blocks",
			conds: with(healthy(internal.TypeAgentReady),
				readinessCond(internal.TypeAgentReady, metav1.ConditionUnknown, "Probing"),
				readinessCond(internal.TypeReady, metav1.ConditionFalse, "InvalidConditionStates")),
			wantCondition: internal.TypeAgentReady,
			wantReason:    "Probing",
		},
		{
			name: "a condition type outside the known stages is still named",
			conds: with(healthy(),
				readinessCond("Other", metav1.ConditionFalse, "Broken"),
				readinessCond(internal.TypeReady, metav1.ConditionFalse, "InvalidConditionStates")),
			wantCondition: "Other",
			wantReason:    "Broken",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := lvgReadiness(readinessLVG("vg-a", "node-1", tc.conds...))
			assert.Equal(t, monitoring.LVGReadiness{
				Name:      "vg-a",
				Node:      "node-1",
				Condition: tc.wantCondition,
				Reason:    tc.wantReason,
				Ready:     tc.wantReady,
			}, got)
		})
	}
}

func lvgReadinessScheme(t *testing.T) *apiruntime.Scheme {
	t.Helper()
	scheme := apiruntime.NewScheme()
	require.NoError(t, v1alpha1.AddToScheme(scheme))
	return scheme
}

func TestLVGReadinessSourceReportsNothingUntilElected(t *testing.T) {
	cl := fake.NewClientBuilder().WithScheme(lvgReadinessScheme(t)).
		WithObjects(readinessLVG("vg-a", "node-1")).
		Build()

	elected := make(chan struct{})
	source := lvgReadinessSource(cl, elected, logger.NewNop())

	assert.Empty(t, source())

	close(elected)
	assert.Equal(t, []monitoring.LVGReadiness{
		{Name: "vg-a", Node: "node-1", Condition: internal.TypeReady, Reason: "Pending"},
	}, source())
}

func TestLVGReadinessSourceReportsEveryGroup(t *testing.T) {
	cl := fake.NewClientBuilder().WithScheme(lvgReadinessScheme(t)).
		WithObjects(
			readinessLVG("vg-a", "node-1", readinessCond(internal.TypeReady, metav1.ConditionTrue, "ValidConditionStates")),
			readinessLVG("vg-b", "node-2",
				readinessCond(internal.TypeAgentReady, metav1.ConditionFalse, "PodNotFound"),
				readinessCond(internal.TypeReady, metav1.ConditionFalse, "InvalidConditionStates")),
		).
		Build()

	elected := make(chan struct{})
	close(elected)

	assert.ElementsMatch(t, []monitoring.LVGReadiness{
		{Name: "vg-a", Node: "node-1", Condition: internal.TypeReady, Reason: "ValidConditionStates", Ready: true},
		{Name: "vg-b", Node: "node-2", Condition: internal.TypeAgentReady, Reason: "PodNotFound"},
	}, lvgReadinessSource(cl, elected, logger.NewNop())())
}

func TestLVGReadinessSourceReportsNothingWhenTheListFails(t *testing.T) {
	cl := fake.NewClientBuilder().WithScheme(lvgReadinessScheme(t)).
		WithObjects(readinessLVG("vg-a", "node-1")).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
				return errors.New("boom")
			},
		}).
		Build()

	elected := make(chan struct{})
	close(elected)

	assert.Empty(t, lvgReadinessSource(cl, elected, logger.NewNop())())
}
