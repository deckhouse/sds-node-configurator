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

package internal

import (
	"slices"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/deckhouse/sds-common-lib/conditions"
	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
)

// LVGEventConditionTypes are the conditions of an LVMVolumeGroup whose
// transitions the agent records as Events. They are the two the agent owns.
// Ready is the controller's verdict over them and is recorded there; it does
// not move at all for the configuration failures the controller tolerates,
// ValidationFailed among them, so without these the Events would not say that
// the agent refused a spec.
var LVGEventConditionTypes = []string{TypeVGConfigurationApplied, TypeVGReady}

// progressReasons are the reasons under which the agent reports a condition
// as False for a resource that is on its way somewhere rather than broken: a
// volume being created, resized, wiped or held for deletion by another
// controller, a Volume Group whose configuration is being applied or removed.
var progressReasons = []string{
	conditions.ReasonPending,
	conditions.ReasonDeleting,
	ReasonCreating,
	ReasonUpdating,
	ReasonResizing,
	ReasonCleaning,
	ReasonTerminating,
}

// NewEventRecorder returns the recorder a controller of the agent records the
// condition transitions of its resources with. The manager offers only the
// core/v1 recorder at this controller-runtime version.
//
// The shared helper records every move of a condition to False as a Warning,
// which is right where False means broken. Here False also means "not there
// yet", and the agent on every node would record a Warning for every volume it
// creates, resizes or deletes, so moves into a progress state are not
// recorded. The move out of it still is: a volume that becomes Ready gets its
// Normal Event, and one that fails gets its Warning.
func NewEventRecorder(mgr manager.Manager, name string) conditions.EventRecorder {
	return WithoutProgressEvents(mgr.GetEventRecorder(name))
}

// WithoutProgressEvents drops the Warning Events whose reason is one of
// progressReasons and passes the rest on to rec. A nil rec stays nil, which the
// helpers read as "record nothing".
func WithoutProgressEvents(rec conditions.EventRecorder) conditions.EventRecorder {
	if rec == nil {
		return nil
	}
	return progressFilter{rec}
}

type progressFilter struct{ rec conditions.EventRecorder }

func (f progressFilter) Eventf(regarding, related runtime.Object, eventtype, reason, action, note string, args ...any) {
	if eventtype == corev1.EventTypeWarning && slices.Contains(progressReasons, reason) {
		return
	}
	f.rec.Eventf(regarding, related, eventtype, reason, action, note, args...)
}

// LVGConditions and LLVConditions are how conditions.WithEvents reads the
// conditions of the two kinds. The status of an LVMLogicalVolume is a pointer
// and may still be nil.
func LVGConditions(lvg *v1alpha1.LVMVolumeGroup) []metav1.Condition {
	return lvg.Status.Conditions
}

func LLVConditions(llv *v1alpha1.LVMLogicalVolume) []metav1.Condition {
	if llv.Status == nil {
		return nil
	}
	return llv.Status.Conditions
}
