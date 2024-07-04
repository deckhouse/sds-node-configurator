package controller

import (
	"context"
	"fmt"
	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	errors2 "k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
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
	LVGStatusWatcherCtrl = "lvg-status-watcher-controller"
)

func RunLVGStatusWatcher(
	mgr manager.Manager,
	log logger.Logger,
) error {
	cl := mgr.GetClient()

	c, err := controller.New(LVGStatusWatcherCtrl, mgr, controller.Options{
		Reconciler: reconcile.Func(func(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
			log.Info(fmt.Sprintf("[RunLVGStatusWatcher] Reconciler got a request %s", request.String()))

			lvg := &v1alpha1.LvmVolumeGroup{}
			err := cl.Get(ctx, request.NamespacedName, lvg)
			if err != nil {
				if errors2.IsNotFound(err) {
					log.Warning(fmt.Sprintf("[RunLVGStatusWatcher] seems like the LVMVolumeGroup was deleted as it is unable to get it, err: %s. Stop to reconcile the resource", err.Error()))
					return reconcile.Result{}, nil
				}
				log.Error(err, fmt.Sprintf("[RunLVGStatusWatcher] unable to get the LVMVolumeGroup %s", lvg.Name))
				return reconcile.Result{}, err
			}

			if lvg.Name == "" {
				log.Info(fmt.Sprintf("[RunLVGStatusWatcher] seems like the LVMVolumeGroup for the request %s was deleted. Reconcile will stop.", request.Name))
				return reconcile.Result{}, nil
			}

			err = reconcileLVGStatus(ctx, cl, log, lvg)
			if err != nil {
				log.Error(err, fmt.Sprintf("[RunLVGStatusWatcher] unable to reconcile the LVMVolumeGroup %s", lvg.Name))
				return reconcile.Result{}, err
			}

			log.Info(fmt.Sprintf("[RunLVGStatusWatcher] Reconciler successfully reconciled the LVMVolumeGroup %s", lvg.Name))
			return reconcile.Result{}, nil
		}),
	})

	if err != nil {
		log.Error(err, "[RunLVGStatusWatcher] unable to create a controller")
		return err
	}

	err = c.Watch(source.Kind(mgr.GetCache(), &v1alpha1.LvmVolumeGroup{}), handler.Funcs{
		CreateFunc: func(ctx context.Context, e event.CreateEvent, q workqueue.RateLimitingInterface) {
			log.Info(fmt.Sprintf("[RunLVGStatusWatcher] got a create event for the LVMVolumeGroup %s", e.Object.GetName()))
			request := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: e.Object.GetNamespace(), Name: e.Object.GetName()}}
			q.Add(request)
			log.Info(fmt.Sprintf("[RunLVGStatusWatcher] CreateFunc added a request for the LVMVolumeGroup %s to the Reconcilers queue", e.Object.GetName()))
		},
		UpdateFunc: func(ctx context.Context, e event.UpdateEvent, q workqueue.RateLimitingInterface) {
			log.Info(fmt.Sprintf("[RunLVGStatusWatcher] got an update event for the LVMVolumeGroup %s", e.ObjectNew.GetName()))
			request := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: e.ObjectNew.GetNamespace(), Name: e.ObjectNew.GetName()}}
			q.Add(request)
			log.Info(fmt.Sprintf("[RunLVGStatusWatcher] UpdateFunc added a request for the LVMVolumeGroup %s to the Reconcilers queue", e.ObjectNew.GetName()))
		},
	})
	if err != nil {
		log.Error(err, "[RunLVGStatusWatcher] unable to watch the events")
		return err
	}

	return nil
}

func reconcileLVGStatus(ctx context.Context, cl client.Client, log logger.Logger, lvg *v1alpha1.LvmVolumeGroup) error {
	log.Debug(fmt.Sprintf("[reconcileLVGStatus] starts to reconcile the LVMVolumeGroup %s", lvg.Name))
	shouldUpdate := false

	log.Debug(fmt.Sprintf("[reconcileLVGStatus] starts to check ThinPools Ready status for the LVMVolumeGroup %s", lvg.Name))
	totalTPCount := getUniqueThinPoolCount(lvg.Spec.ThinPools, lvg.Status.ThinPools)
	actualTPCount := getActualThinPoolReadyCount(lvg.Status.ThinPools)
	if totalTPCount > actualTPCount {
		log.Warning(fmt.Sprintf("[reconcileLVGStatus] some ThinPools of the LVMVolumeGroup %s is not Ready", lvg.Name))
	}
	tpReady := fmt.Sprintf("%d/%d", actualTPCount, totalTPCount)
	if lvg.Status.ThinPoolReady != tpReady {
		lvg.Status.ThinPoolReady = tpReady
		shouldUpdate = true
	}

	appliedStatus := getVGConfigurationAppliedStatus(lvg)
	if lvg.Status.ConfigurationApplied != string(appliedStatus) {
		lvg.Status.ConfigurationApplied = string(appliedStatus)
		shouldUpdate = true
	}

	var err error
	if shouldUpdate {
		err = cl.Status().Update(ctx, lvg)
	}
	return err
}

func getActualThinPoolReadyCount(statusTp []v1alpha1.LvmVolumeGroupThinPoolStatus) int {
	count := 0

	for _, tp := range statusTp {
		if tp.Ready {
			count++
		}
	}

	return count
}

func getUniqueThinPoolCount(specTp []v1alpha1.LvmVolumeGroupThinPoolSpec, statusTp []v1alpha1.LvmVolumeGroupThinPoolStatus) int {
	unique := make(map[string]struct{}, len(specTp)+len(statusTp))

	for _, tp := range specTp {
		unique[tp.Name] = struct{}{}
	}

	for _, tp := range statusTp {
		unique[tp.Name] = struct{}{}
	}

	return len(unique)
}

func getVGConfigurationAppliedStatus(lvg *v1alpha1.LvmVolumeGroup) v1.ConditionStatus {
	for _, c := range lvg.Status.Conditions {
		if c.Type == internal.TypeVGConfigurationApplied {
			return c.Status
		}
	}

	return v1.ConditionFalse
}
