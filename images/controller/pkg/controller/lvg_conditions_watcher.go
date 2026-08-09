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
	"reflect"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/controller/config"
	"github.com/deckhouse/sds-node-configurator/images/controller/internal"
	"github.com/deckhouse/sds-node-configurator/images/controller/pkg/logger"
)

const SdsLVGConditionsWatcherCtrlName = "sds-conditions-watcher-controller"

// acceptableReasons are the VGConfigurationApplied=False reasons that describe a
// Volume Group which is still serving its volumes, so they must not drag the
// aggregate Ready condition — and with it the phase, and with it the scheduler's
// willingness to place new volumes — down with them.
//
// The three file-device reasons belong here for the same reason
// ReasonValidationFailed does — in every one of them the Volume Group is intact
// and serving, and what is left to do is either an operator decision or a retry:
//
//   - ReasonFileDeviceDrift — an entry backing a live Physical Volume was
//     removed from the spec. Only an operator can decide what happens to that
//     PV; taking the Volume Group out of service while they decide would turn a
//     report into an outage.
//   - ReasonFileDeviceNotApplied — an entry could not be brought up on the node
//     (no free space, losetup refused, a grow that did not go through). That is
//     capacity which has not arrived, not storage which has broken.
//   - ReasonAliasResolutionFailed — the agent cannot canonicalize alias-form PV
//     names and so cannot yet decide whether a loop device is already in the VG.
//     Worth alerting on, since new file devices will not join until it clears,
//     but the existing ones are untouched.
//   - ReasonFileDeviceGrowFailed — raising an entry's size did not go through.
//     The growth sequence fails towards the smaller size at every step, so the
//     Volume Group is still the size it was and still serving every volume.
//   - ReasonCacheStale — the agent's LVM cache has not caught up with a Volume
//     Group that is on the node. Nothing is wrong with the Volume Group; that is
//     precisely why the agent refuses to act on it.
//
// ReasonVGCheckFailed is deliberately NOT here. It means the agent cannot read
// the node's Volume Groups at all, and an LVMVolumeGroup whose backing storage
// the agent has lost sight of is exactly what the scheduler should stop placing
// volumes on.
var (
	acceptableReasons = []string{
		internal.ReasonUpdating,
		internal.ReasonValidationFailed,
		internal.ReasonFileDeviceDrift,
		internal.ReasonFileDeviceNotApplied,
		internal.ReasonAliasResolutionFailed,
		internal.ReasonFileDeviceGrowFailed,
		internal.ReasonCacheStale,
	}
)

func RunLVGConditionsWatcher(
	mgr manager.Manager,
	cfg config.Options,
	log logger.Logger,
) error {
	cl := mgr.GetClient()

	c, err := controller.New(SdsLVGConditionsWatcherCtrlName, mgr, controller.Options{
		Reconciler: reconcile.Func(func(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
			log.Info(fmt.Sprintf("[RunLVGConditionsWatcher] Reconciler got a request %s", request.String()))

			lvg := &v1alpha1.LVMVolumeGroup{}
			err := cl.Get(ctx, request.NamespacedName, lvg)
			if err != nil {
				log.Error(err, fmt.Sprintf("[RunLVGConditionsWatcher] unable to get the LVMVolumeGroup %s", request.Name))
				return reconcile.Result{}, err
			}

			if lvg.Name == "" {
				log.Info(fmt.Sprintf("[RunLVGConditionsWatcher] seems like the LVMVolumeGroup for the request %s was deleted. Reconcile will stop.", request.Name))
				return reconcile.Result{}, nil
			}

			shouldRequeue, err := reconcileLVGConditions(ctx, cl, log, lvg)
			if err != nil {
				log.Error(err, fmt.Sprintf("[RunLVGConditionsWatcher] unable to reconcile the LVMVolumeGroup %s", lvg.Name))
			}

			if shouldRequeue {
				log.Warning(fmt.Sprintf("[RunLVGConditionsWatcher] the LVMVolumeGroup %s request will be requeued in %s", lvg.Name, cfg.ScanInterval.String()))
				return reconcile.Result{
					RequeueAfter: cfg.ScanInterval,
				}, nil
			}

			log.Info(fmt.Sprintf("[RunLVGConditionsWatcher] Reconciler successfully reconciled the LVMVolumeGroup %s", lvg.Name))
			return reconcile.Result{}, nil
		}),
	})

	if err != nil {
		log.Error(err, "[RunLVGConditionsWatcher] unable to create a controller")
		return err
	}

	err = c.Watch(source.Kind(mgr.GetCache(), &v1alpha1.LVMVolumeGroup{}, handler.TypedFuncs[*v1alpha1.LVMVolumeGroup, reconcile.Request]{
		CreateFunc: func(_ context.Context, e event.TypedCreateEvent[*v1alpha1.LVMVolumeGroup], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			log.Info(fmt.Sprintf("[RunLVGConditionsWatcher] got a create event for the LVMVolumeGroup %s", e.Object.GetName()))

			request := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: e.Object.GetNamespace(), Name: e.Object.GetName()}}
			q.Add(request)

			log.Info(fmt.Sprintf("[RunLVGConditionsWatcher] createFunc added a request for the LVMVolumeGroup %s to the Reconcilers queue", e.Object.GetName()))
		},
		UpdateFunc: func(_ context.Context, e event.TypedUpdateEvent[*v1alpha1.LVMVolumeGroup], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			log.Info(fmt.Sprintf("[RunLVGConditionsWatcher] got a update event for the LVMVolumeGroup %s", e.ObjectNew.GetName()))
			if reflect.DeepEqual(e.ObjectOld.Status.Conditions, e.ObjectNew.Status.Conditions) {
				log.Info(fmt.Sprintf("[RunLVGConditionsWatcher] no condition changes for the LVMVolumeGroup %s. No need to reconcile", e.ObjectNew.Name))
				return
			}

			request := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: e.ObjectNew.GetNamespace(), Name: e.ObjectNew.GetName()}}
			q.Add(request)
		},
	}))
	if err != nil {
		log.Error(err, "[RunLVGConditionsWatcher] unable to watch the events")
		return err
	}

	return nil
}

func reconcileLVGConditions(ctx context.Context, cl client.Client, log logger.Logger, lvg *v1alpha1.LVMVolumeGroup) (bool, error) {
	log.Debug(fmt.Sprintf("[reconcileLVGConditions] starts the reconciliation for the LVMVolumeGroup %s", lvg.Name))

	if lvg.Status.Conditions == nil {
		log.Info(fmt.Sprintf("[reconcileLVGConditions] the LVMVolumeGroup %s has no conditions, retry later", lvg.Name))
		return true, nil
	}

	verdict, err := updateLVGReadyConditionAndPhase(ctx, cl, lvg)
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLVGConditions] unable to update the condition %s and the phase of the LVMVolumeGroup %s", internal.TypeReady, lvg.Name))
		return true, err
	}

	log.Info(fmt.Sprintf("[reconcileLVGConditions] reconciled the LVMVolumeGroup %s to phase %s, condition %s=%s (%s)",
		lvg.Name, verdict.phase, internal.TypeReady, verdict.ready.Status, verdict.ready.Reason))
	log.Debug(fmt.Sprintf("[reconcileLVGConditions] the LVMVolumeGroup %s condition %s message: %s",
		lvg.Name, internal.TypeReady, verdict.ready.Message))

	return false, nil
}
