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

package internal

import (
	"os"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"

	"github.com/deckhouse/sds-common-lib/conditions"
	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
)

func TestReadyConditionForPhase(t *testing.T) {
	for _, tc := range []struct {
		phase  string
		status metav1.ConditionStatus
		reason string
	}{
		{v1alpha1.PhaseCreated, metav1.ConditionTrue, conditions.ReasonReconciled},
		{v1alpha1.PhaseFailed, metav1.ConditionFalse, conditions.ReasonReconcileFailed},
		{v1alpha1.PhaseResizing, metav1.ConditionFalse, ReasonResizing},
		{v1alpha1.PhaseCleaning, metav1.ConditionFalse, ReasonCleaning},
		{v1alpha1.PhasePending, metav1.ConditionFalse, conditions.ReasonPending},
		{"", metav1.ConditionUnknown, conditions.ReasonPending},
	} {
		t.Run(tc.phase, func(t *testing.T) {
			cond := ReadyConditionForPhase(7, tc.phase, "", "LVMLogicalVolume")

			assert.Equal(t, conditions.TypeReady, cond.Type)
			assert.Equal(t, tc.status, cond.Status)
			assert.Equal(t, tc.reason, cond.Reason)
			assert.Equal(t, int64(7), cond.ObservedGeneration)
		})
	}
}

// status.reason carries raw LVM output, and the schema caps the condition
// message at 32768. Over the cap the API server rejects the whole status write,
// so the resource keeps reporting its previous verdict and the agent fails on
// the write instead of on the command that actually went wrong.
func TestReadyConditionForPhaseTruncatesAnOversizedMessage(t *testing.T) {
	// Multi-byte on purpose. The schema's maxLength is an OpenAPI string
	// length, counted in runes, and TruncateMessage counts the same way — a
	// byte-counting assertion here would fail on a message that is in fact
	// within the limit.
	huge := strings.Repeat("я", conditions.MaxMessageLen+100)

	cond := ReadyConditionForPhase(1, v1alpha1.PhaseFailed, huge, "LVMLogicalVolume")

	assert.LessOrEqual(t, utf8.RuneCountInString(cond.Message), conditions.MaxMessageLen)
}

// status.reason carries free-form text, including raw output from a failed LVM
// command. metav1.Condition wants a machine-readable reason, so the two must not
// be conflated: the text belongs in the message.
func TestReadyConditionKeepsFreeFormTextOutOfTheReason(t *testing.T) {
	llvmErr := `  Volume group "test-vg" not found.\n  Cannot process volume group test-vg`

	cond := ReadyConditionForPhase(1, v1alpha1.PhaseFailed, llvmErr, "LVMLogicalVolume")

	assert.Equal(t, llvmErr, cond.Message)
	assert.Equal(t, conditions.ReasonReconcileFailed, cond.Reason,
		"the reason must stay machine-readable no matter what status.reason holds")
}

func TestReadyConditionFallsBackToAPhraseWhenReasonIsEmpty(t *testing.T) {
	cond := ReadyConditionForPhase(1, v1alpha1.PhasePending, "", "LVMLogicalVolumeSnapshot")

	assert.Equal(t, "the LVMLogicalVolumeSnapshot is in the Pending phase", cond.Message,
		"the CRD requires a non-empty message")
}

// Every phase the CRDs admit has to map to a definite condition. A phase that
// falls through to the default reports Unknown, which reads as "nothing has
// looked at this resource yet" — actively misleading for a phase the agent writes
// on purpose. PhaseCleaning was exactly that case: it is set from a multi-line
// call the initial survey missed.
//
// Only the resources the agent writes are listed. LVMVolumeGroupSet is the
// controller's, and its phases are checked against the same enum over there.
func TestEveryPhaseInTheCRDEnumsIsHandled(t *testing.T) {
	for _, crd := range []string{
		"lvmlogicalvolume",
		"lvmlogicalvolumesnapshot",
	} {
		t.Run(crd, func(t *testing.T) {
			for _, phase := range phaseEnum(t, "../../../crds/"+crd+".yaml") {
				cond := ReadyConditionForPhase(1, phase, "", crd)
				assert.NotEqual(t, metav1.ConditionUnknown, cond.Status,
					"phase %q is in the CRD enum but falls through to the default", phase)
			}
		})
	}
}

func phaseEnum(t *testing.T, path string) []string {
	t.Helper()

	raw, err := os.ReadFile(path)
	require.NoError(t, err)

	var crd struct {
		Spec struct {
			Versions []struct {
				Schema struct {
					OpenAPIV3Schema struct {
						Properties struct {
							Status struct {
								Properties struct {
									Phase struct {
										Enum []string `json:"enum"`
									} `json:"phase"`
								} `json:"properties"`
							} `json:"status"`
						} `json:"properties"`
					} `json:"openAPIV3Schema"`
				} `json:"schema"`
			} `json:"versions"`
		} `json:"spec"`
	}
	require.NoError(t, yaml.Unmarshal(raw, &crd))
	require.Len(t, crd.Spec.Versions, 1, "the lookup below assumes a single served version")

	enum := crd.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties.Status.Properties.Phase.Enum
	require.NotEmpty(t, enum, "the phase enum was not found in %s", path)
	return enum
}

// The agent's condition type constants are its own copies: the controller holds
// the canonical list, and the two live in separate Go modules with no shared
// package between them, so nothing but this stops the two string literals from
// drifting apart.
//
// The CRD enum is the one surface both sides agree on, and it is load-bearing
// twice over — the API server rejects a type outside it, and the controller
// refuses to compute Ready until every type in the enum has been written.
func TestTheAgentsConditionTypesAreInTheCRDEnum(t *testing.T) {
	enum := lvgConditionTypeEnum(t)

	for _, conType := range []string{TypeVGConfigurationApplied, TypeVGReady} {
		assert.Contains(t, enum, conType,
			"the agent writes condition %s, which crds/lvmvolumegroup.yaml does not admit", conType)
	}
}

func lvgConditionTypeEnum(t *testing.T) []string {
	t.Helper()

	raw, err := os.ReadFile("../../../crds/lvmvolumegroup.yaml")
	require.NoError(t, err)

	var crd struct {
		Spec struct {
			Versions []struct {
				Schema struct {
					OpenAPIV3Schema struct {
						Properties struct {
							Status struct {
								Properties struct {
									Conditions struct {
										Items struct {
											Properties struct {
												Type struct {
													Enum []string `json:"enum"`
												} `json:"type"`
											} `json:"properties"`
										} `json:"items"`
									} `json:"conditions"`
								} `json:"properties"`
							} `json:"status"`
						} `json:"properties"`
					} `json:"openAPIV3Schema"`
				} `json:"schema"`
			} `json:"versions"`
		} `json:"spec"`
	}
	require.NoError(t, yaml.Unmarshal(raw, &crd))
	require.Len(t, crd.Spec.Versions, 1, "the lookup below assumes a single served version")

	enum := crd.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties.Status.Properties.Conditions.Items.Properties.Type.Enum
	require.NotEmpty(t, enum, "the conditions type enum was not found in the CRD")
	return enum
}
