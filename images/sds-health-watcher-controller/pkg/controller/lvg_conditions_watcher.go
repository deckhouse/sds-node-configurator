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
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/strings/slices"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/sds-health-watcher-controller/config"
	"github.com/deckhouse/sds-node-configurator/images/sds-health-watcher-controller/internal"
	"github.com/deckhouse/sds-node-configurator/images/sds-health-watcher-controller/pkg/logger"
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
	log = log.WithName("RunLVGConditionsWatcher")
	cl := mgr.GetClient()

	c, err := controller.New(SdsLVGConditionsWatcherCtrlName, mgr, controller.Options{
		Reconciler: reconcile.Func(func(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
			log := log.WithName("Reconcile").WithValues("lvgName", request.Name)
			log.Info("Reconciler got a request")

			lvg := &v1alpha1.LVMVolumeGroup{}
			err := cl.Get(ctx, request.NamespacedName, lvg)
			if err != nil {
				log.Error(err, "unable to get the LVMVolumeGroup")
				return reconcile.Result{}, err
			}

			if lvg.Name == "" {
				log.Info("seems like the LVMVolumeGroup for the request was deleted. Reconcile will stop")
				return reconcile.Result{}, nil
			}

			log = log.WithValues("lvgName", lvg.Name)
			shouldRequeue, err := reconcileLVGConditions(ctx, cl, log, lvg)
			if err != nil {
				log.Error(err, "unable to reconcile the LVMVolumeGroup")
			}

			if shouldRequeue {
				log.Warning("the LVMVolumeGroup request will be requeued", "requeueIn", cfg.ScanIntervalSec)
				return reconcile.Result{
					RequeueAfter: cfg.ScanIntervalSec,
				}, nil
			}

			log.Info("Reconciler successfully reconciled the LVMVolumeGroup")
			return reconcile.Result{}, nil
		}),
	})

	if err != nil {
		log.Error(err, "unable to create a controller")
		return err
	}

	err = c.Watch(source.Kind(mgr.GetCache(), &v1alpha1.LVMVolumeGroup{}, handler.TypedFuncs[*v1alpha1.LVMVolumeGroup, reconcile.Request]{
		CreateFunc: func(_ context.Context, e event.TypedCreateEvent[*v1alpha1.LVMVolumeGroup], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			log := log.WithName("CreateFunc").WithValues("lvgName", e.Object.GetName())
			log.Info("got a create event for the LVMVolumeGroup")

			request := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: e.Object.GetNamespace(), Name: e.Object.GetName()}}
			q.Add(request)

			log.Info("createFunc added a request for the LVMVolumeGroup to the Reconcilers queue")
		},
		UpdateFunc: func(_ context.Context, e event.TypedUpdateEvent[*v1alpha1.LVMVolumeGroup], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			log := log.WithName("UpdateFunc").WithValues("lvgName", e.ObjectNew.GetName())
			log.Info("got an update event for the LVMVolumeGroup")
			if reflect.DeepEqual(e.ObjectOld.Status.Conditions, e.ObjectNew.Status.Conditions) {
				log.Info("no condition changes for the LVMVolumeGroup. No need to reconcile")
				return
			}

			request := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: e.ObjectNew.GetNamespace(), Name: e.ObjectNew.GetName()}}
			q.Add(request)
		},
	}))
	if err != nil {
		log.Error(err, "unable to watch the events")
		return err
	}

	return nil
}

func reconcileLVGConditions(ctx context.Context, cl client.Client, log logger.Logger, lvg *v1alpha1.LVMVolumeGroup) (bool, error) {
	log = log.WithName("reconcileLVGConditions")
	log.Debug("starts the reconciliation for the LVMVolumeGroup")

	if lvg.Status.Conditions == nil {
		log.Info("the LVMVolumeGroup has no conditions, retry later")
		return true, nil
	}

	crd, err := getCRD(ctx, cl, lvgCrdName)
	if err != nil {
		log.Error(err, "unable to get crd", "crdName", lvgCrdName)
		return true, err
	}

	targetConCount, err := getTargetConditionsCount(crd)
	if err != nil {
		log.Error(err, "unable to get target conditions count")
		return true, err
	}

	if len(lvg.Status.Conditions) < targetConCount {
		log.Info("the LVMVolumeGroup misses some conditions, wait for them to got configured",
			"currentCount", len(lvg.Status.Conditions),
			"targetCount", targetConCount)
		err = updateLVMVolumeGroupPhaseIfNeeded(ctx, cl, lvg, v1alpha1.PhasePending)
		if err != nil {
			log.Error(err, "unable to update the LVMVolumeGroup phase")
			return true, err
		}

		err = updateLVGConditionIfNeeded(ctx, cl, log, lvg, metav1.ConditionFalse, internal.TypeReady, internal.ReasonPending, "wait for conditions to got configured")
		if err != nil {
			log.Error(err, "unable to add the condition to the LVMVolumeGroup", "conditionType", internal.TypeReady)
			return true, err
		}

		return false, nil
	}

	log.Info("the LVMVolumeGroup conditions are fully configured. Check their states")

	ready := true
	falseConditions := make([]string, 0, len(lvg.Status.Conditions))
	for _, c := range lvg.Status.Conditions {
		log := log.WithValues("conditionType", c.Type)
		log.Debug("check condition")
		log.Trace("check condition details", "condition", c)
		if c.Type == internal.TypeReady {
			log.Debug("the condition is ours, skip it")
			continue
		}

		if c.Status == metav1.ConditionTrue {
			log.Debug("condition has status True")
			continue
		}

		if c.Reason == internal.ReasonCreating {
			ready = false
			falseConditions = nil
			log.Debug("condition has Creating reason. Turn the LVMVolumeGroup Ready condition and phase to Pending")
			err = updateLVMVolumeGroupPhaseIfNeeded(ctx, cl, lvg, v1alpha1.PhasePending)
			if err != nil {
				log.Error(err, "unable to update the LVMVolumeGroup phase")
				return true, err
			}

			err = updateLVGConditionIfNeeded(ctx, cl, log, lvg, metav1.ConditionFalse, internal.TypeReady, internal.ReasonPending, fmt.Sprintf("condition %s has Creating reason", c.Type))
			if err != nil {
				log.Error(err, "unable to add the condition to the LVMVolumeGroup", "conditionType", internal.TypeReady)
				return true, err
			}

			break
		}

		if c.Reason == internal.ReasonTerminating {
			ready = false
			falseConditions = nil
			log.Debug("condition has Terminating reason. Turn the LVMVolumeGroup Ready condition and phase to Terminating")
			err := updateLVMVolumeGroupPhaseIfNeeded(ctx, cl, lvg, v1alpha1.PhaseTerminating)
			if err != nil {
				log.Error(err, "unable to update the LVMVolumeGroup phase")
				return true, err
			}

			err = updateLVGConditionIfNeeded(ctx, cl, log, lvg, metav1.ConditionFalse, internal.TypeReady, internal.ReasonTerminating, fmt.Sprintf("condition %s has Terminating reason", c.Type))
			if err != nil {
				log.Error(err, "unable to add the condition to the LVMVolumeGroup", "conditionType", internal.TypeReady)
				return true, err
			}
			break
		}

		if c.Status == metav1.ConditionFalse &&
			!slices.Contains(acceptableReasons, c.Reason) {
			log.Warning("condition has status False and its reason is not acceptable", "reason", c.Reason)
			falseConditions = append(falseConditions, c.Type)
			ready = false
		}
	}

	if len(falseConditions) > 0 {
		err := updateLVMVolumeGroupPhaseIfNeeded(ctx, cl, lvg, v1alpha1.PhaseNotReady)
		if err != nil {
			log.Error(err, "unable to update the LVMVolumeGroup phase")
			return true, err
		}

		err = updateLVGConditionIfNeeded(ctx, cl, log, lvg, metav1.ConditionFalse, internal.TypeReady, "InvalidConditionStates", fmt.Sprintf("conditions %s has False status", strings.Join(falseConditions, ",")))
		if err != nil {
			log.Error(err, "unable to add the condition to the LVMVolumeGroup", "conditionType", internal.TypeReady)
			return true, err
		}

		log.Info("successfully reconciled the LVMVolumeGroup condition to NotReady", "conditionType", internal.TypeReady)
	}

	if ready {
		log.Info("the LVMVolumeGroup has no conditions with status False")

		log.Debug("tries to add a condition to the LVMVolumeGroup", "conditionType", internal.TypeReady)
		err := updateLVGConditionIfNeeded(ctx, cl, log, lvg, metav1.ConditionTrue, internal.TypeReady, "ValidConditionStates", "every condition has a proper state")
		if err != nil {
			log.Error(err, "unable to update the condition of the LVMVolumeGroup", "conditionType", internal.TypeReady)
			return true, err
		}

		err = updateLVMVolumeGroupPhaseIfNeeded(ctx, cl, lvg, v1alpha1.PhaseReady)
		if err != nil {
			log.Error(err, "unable to update the LVMVolumeGroup phase")
		}
		log.Info("successfully reconciled the LVMVolumeGroup phase to Ready")
		log.Info("successfully reconciled conditions of the LVMVolumeGroup")
	}

	return false, nil
}
