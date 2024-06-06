package controller

import (
	"context"
	"errors"
	"fmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"reflect"
	"sds-health-watcher-controller/api/v1alpha1"
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
	"strings"
)

const (
	SdsLVGConditionsWatcherCtrlName = "sds-conditions-watcher-controller"
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
				log.Info(fmt.Sprintf("[RunLVGConditionsWatcher] seems like the LVMVolumeGroup for the request %s was deleted. Reconcile retrying will stop.", request.Name))
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
		CreateFunc: func(ctx context.Context, e event.CreateEvent, q workqueue.RateLimitingInterface) {
			log.Info(fmt.Sprintf("[RunLVGConditionsWatcher] got a create event for the LVMVolumeGroup %s", e.Object.GetName()))

			request := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: e.Object.GetNamespace(), Name: e.Object.GetName()}}
			q.Add(request)

			log.Info(fmt.Sprintf("[RunLVGConditionsWatcher] createFunc added a request for the LVMVolumeGroup %s to the Reconcilers queue", e.Object.GetName()))
		},
		UpdateFunc: func(ctx context.Context, e event.UpdateEvent, q workqueue.RateLimitingInterface) {
			log.Info(fmt.Sprintf("[RunLVGConditionsWatcher] got a create event for the LVMVolumeGroup %s", e.ObjectNew.GetName()))

			oldLVG, ok := e.ObjectOld.(*v1alpha1.LvmVolumeGroup)
			if !ok {
				err = errors.New("unable to cast event object to a given type")
				log.Error(err, "[RunLVGConditionsWatcher] an error occurred while handling a create event")
				return
			}
			log.Debug(fmt.Sprintf("[RunLVGConditionsWatcher] successfully casted an old state of the LVMVolumeGroup %s", oldLVG.Name))

			newLVG, ok := e.ObjectNew.(*v1alpha1.LvmVolumeGroup)
			if !ok {
				err = errors.New("unable to cast event object to a given type")
				log.Error(err, "[RunLVGConditionsWatcher] an error occurred while handling a create event")
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

	ready := true
	falseConditions := make([]string, 0, len(lvg.Status.Conditions))
	for _, c := range lvg.Status.Conditions {
		if c.Type == internal.Ready {
		} else if c.Status == metav1.ConditionTrue {
			log.Debug(fmt.Sprintf("[reconcileLVGConditions] the LVMVolumeGroup %s condition %s has status True", lvg.Name, c.Type))
		} else if c.Reason == internal.Pending {
			ready = false
			log.Debug(fmt.Sprintf("[reconcileLVGConditions] the LVMVolumeGroup %s condition %s has Pending reason. Turn the LVMVolumeGroup LVGReady condition to Pending", lvg.Name, c.Type))
			err := updateLVMVolumeGroupPhase(ctx, cl, lvg, internal.Pending)
			if err != nil {
				log.Error(err, fmt.Sprintf("[reconcileLVGConditions] unable to update the LVMVolumeGroup %s phase", lvg.Name))
				return true, err
			}

			err = updateLVGConditionIfNeeded(ctx, cl, log, lvg, metav1.ConditionFalse, internal.Ready, internal.Pending, fmt.Sprintf("condition %s has Pending reason", c.Type))
			if err != nil {
				log.Error(err, fmt.Sprintf("[reconcileLVGConditions] unable to add the condition %s to the LVMVolumeGroup %s", internal.Ready, lvg.Name))
				return true, err
			}

			break
		} else if c.Reason == internal.Terminating {
			ready = false
			log.Debug(fmt.Sprintf("[reconcileLVGConditions] the LVMVolumeGroup %s condition %s has Terminating reason. Turn the LVMVolumeGroup LVGReady condition to Terminating", lvg.Name, c.Type))
			err := updateLVMVolumeGroupPhase(ctx, cl, lvg, internal.Terminating)
			if err != nil {
				log.Error(err, fmt.Sprintf("[reconcileLVGConditions] unable to update the LVMVolumeGroup %s phase", lvg.Name))
				return true, err
			}

			err = updateLVGConditionIfNeeded(ctx, cl, log, lvg, metav1.ConditionFalse, internal.Ready, internal.Terminating, fmt.Sprintf("condition %s has Terminating reason", c.Type))
			if err != nil {
				log.Error(err, fmt.Sprintf("[reconcileLVGConditions] unable to add the condition %s to the LVMVolumeGroup %s", internal.Ready, lvg.Name))
				return true, err
			}
			break
		} else if c.Status == metav1.ConditionFalse && c.Reason != internal.Pending {
			log.Warning(fmt.Sprintf("[reconcileLVGConditions] the condition %s of the LVMVolumeGroup %s has status False and it is not cause of Pending", c.Type, lvg.Name))
			ready = false
		}
	}

	if len(falseConditions) > 0 {
		err := updateLVMVolumeGroupPhase(ctx, cl, lvg, internal.NotReady)
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLVGConditions] unable to update the LVMVolumeGroup %s phase", lvg.Name))
			return true, err
		}

		err = updateLVGConditionIfNeeded(ctx, cl, log, lvg, metav1.ConditionFalse, internal.Ready, "InvalidConditionStates", fmt.Sprintf("conditions %s has False status", strings.Join(falseConditions, ",")))
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLVGConditions] unable to add the condition %s to the LVMVolumeGroup %s", internal.Ready, lvg.Name))
			return true, err
		}

		log.Info(fmt.Sprintf("[reconcileLVGConditions] successfully updated the LVMVolumeGroup %s condition %s to NotReady", lvg.Name, internal.Ready))
	}

	if ready {
		log.Info(fmt.Sprintf("[reconcileLVGConditions] the LVMVolumeGroup %s has no conditions with status False", lvg.Name))

		if lvg.Status.Phase == internal.Ready {
			log.Debug(fmt.Sprintf("[reconcileLVGConditions] the LVMVolumeGroup %s has already Ready phase. No need to update", lvg.Name))
			return false, nil
		}

		log.Debug(fmt.Sprintf("[reconcileLVGConditions] tries to add a condition %s to the LVMVolumeGroup %s", internal.Ready, lvg.Name))
		err := updateLVGConditionIfNeeded(ctx, cl, log, lvg, metav1.ConditionTrue, internal.Ready, "ValidConditionStates", "every condition has a proper state")
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLVGConditions] unable to update the condition %s of the LVMVolumeGroup %s", internal.Ready, lvg.Name))
			return true, err
		}

		log.Info(fmt.Sprintf("[reconcileLVGConditions] successfully reconciled conditions of the LVMVolumeGroup %s", lvg.Name))

		err = updateLVMVolumeGroupPhase(ctx, cl, lvg, internal.Ready)
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLVGConditions] unable to update the LVMVolumeGroup %s phase", lvg.Name))

		}
		log.Info(fmt.Sprintf("[reconcileLVGConditions] successfully updated the LVMVolumeGroup %s phase to Ready", lvg.Name))
	}

	return false, nil
}

func updateLVMVolumeGroupPhase(ctx context.Context, cl client.Client, lvg *v1alpha1.LvmVolumeGroup, phase string) error {
	lvg.Status.Phase = phase
	return cl.Status().Update(ctx, lvg)
}
