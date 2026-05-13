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
	"time"

	"gopkg.in/yaml.v3"
	v1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/strings/slices"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/controller/internal"
)

func getTargetConditionsCount(lvgCrd *v1.CustomResourceDefinition) (int, error) {
	type item struct {
		Type       string `json:"type"`
		Properties struct {
			LastTransitionTime struct {
				Type string `json:"type"`
			} `json:"lastTransitionTime"`
			Message struct {
				Type string `json:"type"`
			} `json:"message"`
			ObservedGeneration struct {
				Type string `json:"type"`
			} `json:"observedGeneration"`
			Reason struct {
				Type string `json:"type"`
			} `json:"reason"`
			Status struct {
				Type string `json:"type"`
			} `json:"status"`
			Type struct {
				Type string   `json:"type"`
				Enum []string `json:"enum"`
			} `json:"type"`
		} `json:"properties"`
	}
	i := item{}
	json, err := lvgCrd.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties["status"].Properties["conditions"].Items.MarshalJSON()
	if err != nil {
		return 0, err
	}

	err = yaml.Unmarshal(json, &i)
	if err != nil {
		return 0, err
	}

	return len(i.Properties.Type.Enum), nil
}

func getCRD(ctx context.Context, cl client.Client, crdName string) (*v1.CustomResourceDefinition, error) {
	crd := &v1.CustomResourceDefinition{}
	err := cl.Get(ctx, client.ObjectKey{
		Name: crdName,
	}, crd)

	return crd, err
}

// decideLVGReadyAndPhase is a pure function that derives the desired LVG Ready
// condition and phase from the current status.conditions slice. A changed=false
// return means the function has no opinion (in practice: no False conditions
// were observed and the current Ready/phase pair can be left untouched by the
// caller). Intentionally mirrors the old branching in reconcileLVGConditions
// so it can be unit-tested without a client.
func decideLVGReadyAndPhase(
	conditions []metav1.Condition,
	targetConCount int,
) (phase string, readyStatus metav1.ConditionStatus, readyReason, readyMessage string, changed bool) {
	if len(conditions) < targetConCount {
		return v1alpha1.PhasePending, metav1.ConditionFalse, internal.ReasonPending,
			"wait for conditions to got configured", true
	}

	ready := true
	falseConditions := make([]string, 0, len(conditions))
	for _, c := range conditions {
		if c.Type == internal.TypeReady {
			continue
		}
		if c.Status == metav1.ConditionTrue {
			continue
		}

		switch c.Reason {
		case internal.ReasonCreating:
			return v1alpha1.PhasePending, metav1.ConditionFalse, internal.ReasonPending,
				fmt.Sprintf("condition %s has Creating reason", c.Type), true
		case internal.ReasonTerminating:
			return v1alpha1.PhaseTerminating, metav1.ConditionFalse, internal.ReasonTerminating,
				fmt.Sprintf("condition %s has Terminating reason", c.Type), true
		}

		if c.Status == metav1.ConditionFalse && !slices.Contains(acceptableReasons, c.Reason) {
			falseConditions = append(falseConditions, c.Type)
			ready = false
		}
	}

	if len(falseConditions) > 0 {
		return v1alpha1.PhaseNotReady, metav1.ConditionFalse, "InvalidConditionStates",
			fmt.Sprintf("conditions %s has False status", strings.Join(falseConditions, ",")), true
	}

	if ready {
		return v1alpha1.PhaseReady, metav1.ConditionTrue, "ValidConditionStates",
			"every condition has a proper state", true
	}

	return "", "", "", "", false
}

// updateLVGReadyConditionAndPhaseIfNeeded mutates the Ready condition and Phase
// on lvg in memory, then issues a single Status().Update() if either of them
// actually changed. This is deliberately atomic: splitting the write into two
// calls opened a race window in which a concurrent writer (sds-infra-watcher,
// the agent, lvg-status-watcher) could bump resourceVersion between the two
// updates and cause a 409 Conflict on the second one, leaving Phase stuck.
// Any error (including 409) is returned to the caller so controller-runtime
// can requeue the reconcile and re-read a fresh object.
func updateLVGReadyConditionAndPhaseIfNeeded(
	ctx context.Context,
	cl client.Client,
	lvg *v1alpha1.LVMVolumeGroup,
	readyStatus metav1.ConditionStatus,
	readyReason, readyMessage, phase string,
) error {
	newReady := metav1.Condition{
		Type:               internal.TypeReady,
		Status:             readyStatus,
		ObservedGeneration: lvg.Generation,
		LastTransitionTime: metav1.NewTime(time.Now()),
		Reason:             readyReason,
		Message:            readyMessage,
	}

	changed := false
	if lvg.Status.Conditions == nil {
		lvg.Status.Conditions = make([]metav1.Condition, 0, 2)
	}

	idx := -1
	for i, c := range lvg.Status.Conditions {
		if c.Type == internal.TypeReady {
			idx = i
			break
		}
	}
	if idx == -1 {
		lvg.Status.Conditions = append(lvg.Status.Conditions, newReady)
		changed = true
	} else if !checkIfEqualConditions(lvg.Status.Conditions[idx], newReady) {
		lvg.Status.Conditions[idx] = newReady
		changed = true
	}

	if lvg.Status.Phase != phase {
		lvg.Status.Phase = phase
		changed = true
	}

	if !changed {
		return nil
	}

	return cl.Status().Update(ctx, lvg)
}
