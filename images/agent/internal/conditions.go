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
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/deckhouse/sds-common-lib/conditions"
	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
)

// ReadyConditionForPhase translates a phase about to be written onto a resource
// into the Ready condition that goes with it.
//
// In resources designed around conditions the phase is derived from them. Here it
// is the other way round, because the phase came first and is the load-bearing
// contract: the strings are published API, and the CSI driver in
// sds-local-volume polls them to decide when a volume or a snapshot is usable.
// Deriving the condition from the phase keeps the two in step by construction,
// with one place to look.
//
// reason is the resource's status.reason, which carries free-form text including
// raw error output from failed LVM commands. It becomes the condition's message;
// the condition's own reason is machine-readable, as metav1.Condition requires.
//
// kind names the resource, used only to phrase the message when status.reason is
// empty.
func ReadyConditionForPhase(generation int64, phase, reason, kind string) metav1.Condition {
	cond := metav1.Condition{
		Type:               conditions.TypeReady,
		ObservedGeneration: generation,
		Message:            reason,
	}

	switch phase {
	case v1alpha1.PhaseCreated:
		cond.Status = metav1.ConditionTrue
		cond.Reason = conditions.ReasonReconciled
	case v1alpha1.PhaseFailed:
		cond.Status = metav1.ConditionFalse
		cond.Reason = conditions.ReasonReconcileFailed
	case v1alpha1.PhaseResizing:
		cond.Status = metav1.ConditionFalse
		cond.Reason = ReasonResizing
	case v1alpha1.PhaseCleaning:
		cond.Status = metav1.ConditionFalse
		cond.Reason = ReasonCleaning
	case v1alpha1.PhasePending:
		cond.Status = metav1.ConditionFalse
		cond.Reason = conditions.ReasonPending
	default:
		// Nothing has reported on the resource yet.
		cond.Status = metav1.ConditionUnknown
		cond.Reason = conditions.ReasonPending
	}

	if cond.Message == "" {
		cond.Message = fmt.Sprintf("the %s is in the %s phase", kind, phase)
	}

	return cond
}
