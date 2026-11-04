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
	"slices"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/deckhouse/sds-common-lib/conditions"
	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/controller/internal"
)

// progressReasons are the reasons under which this controller reports Ready
// as False for a resource that is on its way somewhere rather than broken: a
// new LVMVolumeGroup whose agent has not reported yet, a group being deleted,
// a set whose new spec has not been acted on yet.
var progressReasons = []string{
	conditions.ReasonPending,
	internal.ReasonTerminating,
}

// newEventRecorder returns the recorder a controller records the Ready
// transitions of its resources with. The manager offers only the core/v1
// recorder at this controller-runtime version.
//
// The shared helper records every move of Ready to False as a Warning, which
// is right where False means broken. Here False also means "not there yet",
// and a Warning on every group created or deleted would bury the ones that
// matter, so moves into a progress state are not recorded. The move out of it
// still is: a group that becomes Ready gets its Normal Event, and one that
// fails gets its Warning.
func newEventRecorder(mgr manager.Manager, name string) conditions.EventRecorder {
	return withoutProgressEvents(conditions.FromLegacyRecorder(mgr.GetEventRecorderFor(name)))
}

// withoutProgressEvents drops the Warning Events whose reason is one of
// progressReasons and passes the rest on to rec. A nil rec stays nil, which the
// helpers read as "record nothing".
func withoutProgressEvents(rec conditions.EventRecorder) conditions.EventRecorder {
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

// lvgConditions and lvgSetConditions are how conditions.WithEvents reads the
// conditions of the two kinds.
func lvgConditions(lvg *v1alpha1.LVMVolumeGroup) []metav1.Condition {
	return lvg.Status.Conditions
}

func lvgSetConditions(lvgSet *v1alpha1.LVMVolumeGroupSet) []metav1.Condition {
	return lvgSet.Status.Conditions
}
