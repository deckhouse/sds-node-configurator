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

const (
	SdsLVGConditionsWatcherCtrlName = "sds-conditions-watcher-controller"

	lvgCrdName = "lvmvolumegroups.storage.deckhouse.io"
)

var (
	acceptableReasons = []string{internal.ReasonUpdating, internal.ReasonValidationFailed}
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

	crd, err := getCRD(ctx, cl, lvgCrdName)
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLVGConditions] unable to get crd %s", lvgCrdName))
		return true, err
	}

	targetConCount, err := getTargetConditionsCount(crd)
	if err != nil {
		log.Error(err, "[reconcileLVGConditions] unable to get target conditions count")
		return true, err
	}

	phase, readyStatus, readyReason, readyMessage, changed := decideLVGReadyAndPhase(lvg.Status.Conditions, targetConCount)
	if !changed {
		log.Debug(fmt.Sprintf("[reconcileLVGConditions] the LVMVolumeGroup %s Ready condition/phase are already consistent, nothing to update", lvg.Name))
		return false, nil
	}

	if err := updateLVGReadyConditionAndPhaseIfNeeded(ctx, cl, lvg, readyStatus, readyReason, readyMessage, phase); err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLVGConditions] unable to atomically update the LVMVolumeGroup %s Ready condition and phase", lvg.Name))
		return true, err
	}

	log.Info(fmt.Sprintf("[reconcileLVGConditions] successfully reconciled the LVMVolumeGroup %s to phase=%s readyStatus=%s", lvg.Name, phase, readyStatus))
	return false, nil
}
