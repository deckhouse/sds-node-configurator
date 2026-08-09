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
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/strings/slices"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-common-lib/conditions"
	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/controller/internal"
)

// missingConditionTypes returns the condition types the LVMVolumeGroup is
// expected to publish but has not been given yet, in the order they are
// declared. An empty result means every stage has reported at least once, so
// the aggregate Ready condition can be computed from their states.
func missingConditionTypes(lvg *v1alpha1.LVMVolumeGroup) []string {
	var missing []string

	for _, conType := range internal.LVGConditionTypes {
		if conditions.Get(lvg.Status.Conditions, conType) == nil {
			missing = append(missing, conType)
		}
	}

	return missing
}

// lvgVerdict is the aggregate Ready condition and the phase that belong
// together. They are one decision, so they are represented as one value and
// written in one call — see updateLVGReadyConditionAndPhase.
type lvgVerdict struct {
	phase string
	ready metav1.Condition
}

// decideLVGReadyAndPhase derives the aggregate Ready condition and the phase from
// the conditions the agent and the other controllers have published.
//
// It is pure, and total: every input yields a definite verdict, so there is no
// state in which the LVMVolumeGroup is left without one.
//
// The precedence reproduces what the branching in reconcileLVGConditions did:
// a missing condition outranks everything, then the first Creating or
// Terminating reason encountered, then any unacceptable False, and a Volume
// Group with none of those is Ready.
func decideLVGReadyAndPhase(lvg *v1alpha1.LVMVolumeGroup) lvgVerdict {
	verdict := func(phase string, status metav1.ConditionStatus, reason, message string) lvgVerdict {
		return lvgVerdict{
			phase: phase,
			ready: metav1.Condition{
				Type:               internal.TypeReady,
				Status:             status,
				ObservedGeneration: lvg.Generation,
				Reason:             reason,
				// The messages below list condition types, so they stay well
				// inside the schema's 32768 in practice. Truncating anyway is
				// what makes "every condition this module writes fits the
				// schema" true by construction rather than by argument: the
				// list is as long as the conditions the agents happen to have
				// written, and nothing here bounds that.
				Message: conditions.TruncateMessage(message),
			},
		}
	}

	if missing := missingConditionTypes(lvg); len(missing) > 0 {
		return verdict(v1alpha1.PhasePending, metav1.ConditionFalse, conditions.ReasonPending,
			fmt.Sprintf("waiting for the conditions %s to be configured", strings.Join(missing, ",")))
	}

	falseConditions := make([]string, 0, len(lvg.Status.Conditions))
	for _, c := range lvg.Status.Conditions {
		// Ready is the verdict being computed, not an input to it.
		if c.Type == internal.TypeReady || c.Status == metav1.ConditionTrue {
			continue
		}

		// These two outrank a plain False, whichever comes first, matching the
		// loop that used to break here.
		switch c.Reason {
		case internal.ReasonCreating:
			return verdict(v1alpha1.PhasePending, metav1.ConditionFalse, conditions.ReasonPending,
				fmt.Sprintf("condition %s has Creating reason", c.Type))
		case internal.ReasonTerminating:
			return verdict(v1alpha1.PhaseTerminating, metav1.ConditionFalse, internal.ReasonTerminating,
				fmt.Sprintf("condition %s has Terminating reason", c.Type))
		}

		// Unknown counts against the Volume Group the same way an unacceptable
		// False does. It means the writer has not reached a verdict, and calling
		// the group Ready on a stage that says it does not know would let the
		// scheduler place volumes on evidence nobody produced.
		//
		// acceptableReasons does not apply: those reasons say "this went wrong
		// but the Volume Group is still serving its volumes", which is a claim
		// only a writer that reached a verdict can make.
		if c.Status == metav1.ConditionUnknown ||
			(c.Status == metav1.ConditionFalse && !slices.Contains(acceptableReasons, c.Reason)) {
			falseConditions = append(falseConditions, c.Type)
		}
	}

	if len(falseConditions) > 0 {
		return verdict(v1alpha1.PhaseNotReady, metav1.ConditionFalse, "InvalidConditionStates",
			fmt.Sprintf("conditions %s has False status", strings.Join(falseConditions, ",")))
	}

	return verdict(v1alpha1.PhaseReady, metav1.ConditionTrue, "ValidConditionStates",
		"every condition has a proper state")
}

// updateLVGReadyConditionAndPhase writes the aggregate Ready condition and the
// phase in a single status update, and returns the verdict it wrote.
//
// The single write is the point. These used to be two calls, and a reader could
// catch the object between them with a phase that its conditions did not
// support; worse, if the second call failed the two stayed in disagreement until
// something else happened to trigger a reconcile, which is how an LVMVolumeGroup
// ended up stuck reporting NotReady.
//
// The verdict is decided inside the mutation rather than by the caller, so a
// conflict retry re-decides it against the conditions as they are now instead of
// re-writing a conclusion drawn from a view that has since been overtaken —
// which matters here, because the agent on every node writes into the same
// condition list.
//
// On success, lvg.Status and lvg.ResourceVersion are replaced with server-side
// values, same as updateLVGConditionIfNeeded.
func updateLVGReadyConditionAndPhase(
	ctx context.Context,
	cl client.Client,
	lvg *v1alpha1.LVMVolumeGroup,
) (lvgVerdict, error) {
	var (
		written *v1alpha1.LVMVolumeGroup
		decided lvgVerdict
	)

	err := conditions.UpdateStatus(ctx, cl, lvg, func(fresh *v1alpha1.LVMVolumeGroup) {
		written = fresh
		decided = decideLVGReadyAndPhase(fresh)

		conditions.Set(&fresh.Status.Conditions, decided.ready)
		fresh.Status.Phase = decided.phase
		// The generation the verdict was computed against, taken from the same
		// object the stage conditions were read from.
		fresh.Status.ObservedGeneration = fresh.Generation
	})
	if err != nil {
		return lvgVerdict{}, err
	}

	lvg.Status = written.Status
	lvg.ResourceVersion = written.ResourceVersion
	return decided, nil
}
