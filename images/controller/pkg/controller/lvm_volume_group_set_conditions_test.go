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

func TestReadyConditionForSetPhase(t *testing.T) {
	for _, tc := range []struct {
		phase  string
		status metav1.ConditionStatus
		reason string
	}{
		{phaseCreated, metav1.ConditionTrue, conditions.ReasonReconciled},
		{v1alpha1.PhasePending, metav1.ConditionFalse, conditions.ReasonPending},
		{phaseNotCreated, metav1.ConditionFalse, conditions.ReasonReconcileFailed},
		{"", metav1.ConditionUnknown, conditions.ReasonPending},
	} {
		t.Run(tc.phase, func(t *testing.T) {
			cond := readyConditionForSetPhase(3, tc.phase, "")

			assert.Equal(t, conditions.TypeReady, cond.Type)
			assert.Equal(t, tc.status, cond.Status)
			assert.Equal(t, tc.reason, cond.Reason)
			assert.Equal(t, int64(3), cond.ObservedGeneration)
			assert.NotEmpty(t, cond.Message, "the CRD requires a non-empty message")
		})
	}
}

// status.reason holds free-form text, including raw error strings from a failed
// attempt to provide the LVMVolumeGroups. metav1.Condition wants a
// machine-readable reason, so the text goes to the message instead.
func TestReadyConditionForSetPhaseKeepsFreeFormTextOutOfTheReason(t *testing.T) {
	text := "no nodes found by specified nodeSelector"

	cond := readyConditionForSetPhase(1, phaseNotCreated, text)

	assert.Equal(t, text, cond.Message)
	assert.Equal(t, conditions.ReasonReconcileFailed, cond.Reason)
}

// That free-form text is unbounded, and the schema caps the condition message
// at 32768. Over the cap the API server rejects the whole status write, so the
// set keeps reporting its previous verdict and the reconcile fails on the write
// instead of on what actually went wrong.
func TestReadyConditionForSetPhaseTruncatesAnOversizedMessage(t *testing.T) {
	// Multi-byte on purpose. The schema's maxLength is an OpenAPI string
	// length, counted in runes, and TruncateMessage counts the same way — a
	// byte-counting assertion here would fail on a message that is in fact
	// within the limit.
	//
	// Written as an escape rather than the character itself: the module linter
	// rejects non-ASCII bytes in Go sources.
	huge := strings.Repeat("\u044f", conditions.MaxMessageLen+100)

	cond := readyConditionForSetPhase(1, phaseNotCreated, huge)

	assert.LessOrEqual(t, utf8.RuneCountInString(cond.Message), conditions.MaxMessageLen)
}

// Every phase the CRD admits, apart from the empty string it explicitly allows,
// has to map to a definite condition. Falling through to the default reports
// Unknown, which reads as "nothing has looked at this resource yet".
func TestEverySetPhaseInTheCRDEnumIsHandled(t *testing.T) {
	raw, err := os.ReadFile("../../../../crds/lvmvolumegroupset.yaml")
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
	require.NotEmpty(t, enum, "the phase enum was not found in the CRD")

	for _, phase := range enum {
		if phase == "" {
			// The enum admits it, and it genuinely means not-yet-observed.
			continue
		}

		cond := readyConditionForSetPhase(1, phase, "")
		assert.NotEqual(t, metav1.ConditionUnknown, cond.Status,
			"phase %q is in the CRD enum but falls through to the default", phase)
	}
}
