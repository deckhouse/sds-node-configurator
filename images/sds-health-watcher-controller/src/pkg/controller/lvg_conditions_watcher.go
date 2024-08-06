/*
Copyright 2024 Flant JSC

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
	"fmt"
	"reflect"
	"strings"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/strings/slices"
	"sds-health-watcher-controller/config"
	"sds-health-watcher-controller/internal"
	"sds-health-watcher-controller/pkg/logger"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
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

			lvg := &v1alpha1.LvmVolumeGroup{}
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
				log.Warning(fmt.Sprintf("[RunLVGConditionsWatcher] the LVMVolumeGroup %s request will be requeued in %s", lvg.Name, cfg.ScanIntervalSec.String()))
				return reconcile.Result{
					RequeueAfter: cfg.ScanIntervalSec,
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

	err = c.Watch(source.Kind(mgr.GetCache(), &v1alpha1.LvmVolumeGroup{}), handler.Funcs{
		CreateFunc: func(_ context.Context, e event.CreateEvent, q workqueue.RateLimitingInterface) {
			log.Info(fmt.Sprintf("[RunLVGConditionsWatcher] got a create event for the LVMVolumeGroup %s", e.Object.GetName()))

			request := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: e.Object.GetNamespace(), Name: e.Object.GetName()}}
			q.Add(request)

			log.Info(fmt.Sprintf("[RunLVGConditionsWatcher] createFunc added a request for the LVMVolumeGroup %s to the Reconcilers queue", e.Object.GetName()))
		},
		UpdateFunc: func(_ context.Context, e event.UpdateEvent, q workqueue.RateLimitingInterface) {
			log.Info(fmt.Sprintf("[RunLVGConditionsWatcher] got a update event for the LVMVolumeGroup %s", e.ObjectNew.GetName()))

			oldLVG, ok := e.ObjectOld.(*v1alpha1.LvmVolumeGroup)
			if !ok {
				err = errors.New("unable to cast event object to a given type")
				log.Error(err, "[RunLVGConditionsWatcher] an error occurred while handling a update event")
				return
			}
			log.Debug(fmt.Sprintf("[RunLVGConditionsWatcher] successfully casted an old state of the LVMVolumeGroup %s", oldLVG.Name))

			newLVG, ok := e.ObjectNew.(*v1alpha1.LvmVolumeGroup)
			if !ok {
				err = errors.New("unable to cast event object to a given type")
				log.Error(err, "[RunLVGConditionsWatcher] an error occurred while handling a update event")
				return
			}
			log.Debug(fmt.Sprintf("[RunLVGConditionsWatcher] successfully casted a new state of the LVMVolumeGroup %s", newLVG.Name))

			if reflect.DeepEqual(oldLVG.Status.Conditions, newLVG.Status.Conditions) {
				log.Info(fmt.Sprintf("[RunLVGConditionsWatcher] no condition changes for the LVMVolumeGroup %s. No need to reconcile", newLVG.Name))
				return
			}

			request := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: e.ObjectNew.GetNamespace(), Name: e.ObjectNew.GetName()}}
			q.Add(request)
		},
	})
	if err != nil {
		log.Error(err, "[RunLVGConditionsWatcher] unable to watch the events")
		return err
	}

	return nil
}

func reconcileLVGConditions(ctx context.Context, cl client.Client, log logger.Logger, lvg *v1alpha1.LvmVolumeGroup) (bool, error) {
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

	if len(lvg.Status.Conditions) < targetConCount {
		log.Info(fmt.Sprintf("[reconcileLVGConditions] the LVMVolumeGroup %s misses some conditions, wait for them to got configured", lvg.Name))
		log.Debug(fmt.Sprintf("[reconcileLVGConditions] the LVMVolumeGroup %s conditions current count: %d, target count: %d", lvg.Name, len(lvg.Status.Conditions), targetConCount))
		err = updateLVMVolumeGroupPhaseIfNeeded(ctx, cl, lvg, internal.PhasePending)
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLVGConditions] unable to update the LVMVolumeGroup %s phase", lvg.Name))
			return true, err
		}

		err = updateLVGConditionIfNeeded(ctx, cl, log, lvg, metav1.ConditionFalse, internal.TypeReady, internal.ReasonPending, "wait for conditions to got configured")
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLVGConditions] unable to add the condition %s to the LVMVolumeGroup %s", internal.TypeReady, lvg.Name))
			return true, err
		}

		return false, nil
	}

	log.Info(fmt.Sprintf("[reconcileLVGConditions] the LVMVolumeGroup %s conditions are fully configured. Check their states", lvg.Name))

	ready := true
	falseConditions := make([]string, 0, len(lvg.Status.Conditions))
	for _, c := range lvg.Status.Conditions {
		log.Debug(fmt.Sprintf("[reconcileLVGConditions] check condition %s of the LVMVolumeGroup %s", c.Type, lvg.Name))
		log.Trace(fmt.Sprintf("[reconcileLVGConditions] check condition %+v of the LVMVolumeGroup %s", c, lvg.Name))
		if c.Type == internal.TypeReady {
			log.Debug(fmt.Sprintf("[reconcileLVGConditions] the condition %s of the LVMVolumeGroup %s is ours, skip it", c.Type, lvg.Name))
			continue
		}

		if c.Status == metav1.ConditionTrue {
			log.Debug(fmt.Sprintf("[reconcileLVGConditions] the LVMVolumeGroup %s condition %s has status True", lvg.Name, c.Type))
			continue
		}

		if c.Reason == internal.ReasonCreating {
			ready = false
			falseConditions = nil
			log.Debug(fmt.Sprintf("[reconcileLVGConditions] the LVMVolumeGroup %s condition %s has Creating reason. Turn the LVMVolumeGroup Ready condition and phase to Pending", lvg.Name, c.Type))
			err = updateLVMVolumeGroupPhaseIfNeeded(ctx, cl, lvg, internal.PhasePending)
			if err != nil {
				log.Error(err, fmt.Sprintf("[reconcileLVGConditions] unable to update the LVMVolumeGroup %s phase", lvg.Name))
				return true, err
			}

			err = updateLVGConditionIfNeeded(ctx, cl, log, lvg, metav1.ConditionFalse, internal.TypeReady, internal.ReasonPending, fmt.Sprintf("condition %s has Creating reason", c.Type))
			if err != nil {
				log.Error(err, fmt.Sprintf("[reconcileLVGConditions] unable to add the condition %s to the LVMVolumeGroup %s", internal.TypeReady, lvg.Name))
				return true, err
			}

			break
		}

		if c.Reason == internal.ReasonTerminating {
			ready = false
			falseConditions = nil
			log.Debug(fmt.Sprintf("[reconcileLVGConditions] the LVMVolumeGroup %s condition %s has Terminating reason. Turn the LVMVolumeGroup Ready condition and phase to Terminating", lvg.Name, c.Type))
			err := updateLVMVolumeGroupPhaseIfNeeded(ctx, cl, lvg, internal.PhaseTerminating)
			if err != nil {
				log.Error(err, fmt.Sprintf("[reconcileLVGConditions] unable to update the LVMVolumeGroup %s phase", lvg.Name))
				return true, err
			}

			err = updateLVGConditionIfNeeded(ctx, cl, log, lvg, metav1.ConditionFalse, internal.TypeReady, internal.ReasonTerminating, fmt.Sprintf("condition %s has Terminating reason", c.Type))
			if err != nil {
				log.Error(err, fmt.Sprintf("[reconcileLVGConditions] unable to add the condition %s to the LVMVolumeGroup %s", internal.TypeReady, lvg.Name))
				return true, err
			}
			break
		}

		if c.Status == metav1.ConditionFalse &&
			!slices.Contains(acceptableReasons, c.Reason) {
			log.Warning(fmt.Sprintf("[reconcileLVGConditions] the condition %s of the LVMVolumeGroup %s has status False and its reason is not acceptable", c.Type, lvg.Name))
			falseConditions = append(falseConditions, c.Type)
			ready = false
		}
	}

	if len(falseConditions) > 0 {
		err := updateLVMVolumeGroupPhaseIfNeeded(ctx, cl, lvg, internal.PhaseNotReady)
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLVGConditions] unable to update the LVMVolumeGroup %s phase", lvg.Name))
			return true, err
		}

		err = updateLVGConditionIfNeeded(ctx, cl, log, lvg, metav1.ConditionFalse, internal.TypeReady, "InvalidConditionStates", fmt.Sprintf("conditions %s has False status", strings.Join(falseConditions, ",")))
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLVGConditions] unable to add the condition %s to the LVMVolumeGroup %s", internal.TypeReady, lvg.Name))
			return true, err
		}

		log.Info(fmt.Sprintf("[reconcileLVGConditions] successfully reconciled the LVMVolumeGroup %s condition %s to NotReady", lvg.Name, internal.TypeReady))
	}

	if ready {
		log.Info(fmt.Sprintf("[reconcileLVGConditions] the LVMVolumeGroup %s has no conditions with status False", lvg.Name))

		log.Debug(fmt.Sprintf("[reconcileLVGConditions] tries to add a condition %s to the LVMVolumeGroup %s", internal.TypeReady, lvg.Name))
		err := updateLVGConditionIfNeeded(ctx, cl, log, lvg, metav1.ConditionTrue, internal.TypeReady, "ValidConditionStates", "every condition has a proper state")
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLVGConditions] unable to update the condition %s of the LVMVolumeGroup %s", internal.TypeReady, lvg.Name))
			return true, err
		}

		err = updateLVMVolumeGroupPhaseIfNeeded(ctx, cl, lvg, internal.PhaseReady)
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLVGConditions] unable to update the LVMVolumeGroup %s phase", lvg.Name))
		}
		log.Info(fmt.Sprintf("[reconcileLVGConditions] successfully reconciled the LVMVolumeGroup %s phase to Ready", lvg.Name))
		log.Info(fmt.Sprintf("[reconcileLVGConditions] successfully reconciled conditions of the LVMVolumeGroup %s", lvg.Name))
	}

	return false, nil
}
