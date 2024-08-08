package controller

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"agent/config"
	"agent/internal"
	"agent/pkg/cache"
	"agent/pkg/logger"
	"agent/pkg/monitoring"
	"agent/pkg/utils"
	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

const (
	LVMLogicalVolumeExtenderCtrlName = "lvm-logical-volume-extender-controller"
)

func RunLVMLogicalVolumeExtenderWatcherController(
	mgr manager.Manager,
	cfg config.Options,
	log logger.Logger,
	metrics monitoring.Metrics,
	sdsCache *cache.Cache,
) error {
	cl := mgr.GetClient()
	mgrCache := mgr.GetCache()

	c, err := controller.New(LVMLogicalVolumeExtenderCtrlName, mgr, controller.Options{
		Reconciler: reconcile.Func(func(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
			log.Info(fmt.Sprintf("[RunLVMLogicalVolumeExtenderWatcherController] starts the reconciliation for the LVMVolumeGroup %s", request.NamespacedName.String()))

			log.Debug(fmt.Sprintf("[RunLVMLogicalVolumeExtenderWatcherController] tries to get the LVMVolumeGroup %s", request.Name))
			lvg, err := getLVMVolumeGroup(ctx, cl, metrics, request.Name)
			if err != nil {
				if k8serr.IsNotFound(err) {
					log.Error(err, fmt.Sprintf("[RunLVMLogicalVolumeExtenderWatcherController] LVMVolumeGroup %s not found (probably was deleted). Stop the reconcile", request.Name))
					return reconcile.Result{}, nil
				}

				log.Error(err, fmt.Sprintf("[RunLVMLogicalVolumeExtenderWatcherController] unable to get the LVMVolumeGroup %s", request.Name))
				return reconcile.Result{}, err
			}
			log.Debug(fmt.Sprintf("[RunLVMLogicalVolumeExtenderWatcherController] successfully got the LVMVolumeGroup %s", request.Name))

			if !shouldLLVExtenderReconcileEvent(log, lvg, cfg.NodeName) {
				log.Info(fmt.Sprintf("[RunLVMLogicalVolumeExtenderWatcherController] no need to reconcile a request for the LVMVolumeGroup %s", lvg.Name))
				return reconcile.Result{}, nil
			}

			shouldRequeue := ReconcileLVMLogicalVolumeExtension(ctx, cl, metrics, log, sdsCache, lvg)
			if shouldRequeue {
				log.Warning(fmt.Sprintf("[RunLVMLogicalVolumeExtenderWatcherController] Reconciler needs a retry for the LVMVolumeGroup %s. Retry in %s", lvg.Name, cfg.VolumeGroupScanIntervalSec.String()))
				return reconcile.Result{
					RequeueAfter: cfg.VolumeGroupScanIntervalSec,
				}, nil
			}

			log.Info(fmt.Sprintf("[RunLVMLogicalVolumeExtenderWatcherController] successfully reconciled LVMLogicalVolumes for the LVMVolumeGroup %s", lvg.Name))
			return reconcile.Result{}, nil
		}),
	})
	if err != nil {
		log.Error(err, "[RunLVMLogicalVolumeExtenderWatcherController] unable to create a controller")
		return err
	}

	err = c.Watch(source.Kind(mgrCache, &v1alpha1.LvmVolumeGroup{}, handler.TypedFuncs[*v1alpha1.LvmVolumeGroup]{
		CreateFunc: func(_ context.Context, e event.TypedCreateEvent[*v1alpha1.LvmVolumeGroup], q workqueue.RateLimitingInterface) {
			log.Info(fmt.Sprintf("[RunLVMLogicalVolumeExtenderWatcherController] got a Create event for the LVMVolumeGroup %s", e.Object.GetName()))
			request := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: e.Object.GetNamespace(), Name: e.Object.GetName()}}
			q.Add(request)
			log.Info(fmt.Sprintf("[RunLVMLogicalVolumeExtenderWatcherController] added the LVMVolumeGroup %s to the Reconcilers queue", e.Object.GetName()))
		},
		UpdateFunc: func(_ context.Context, e event.TypedUpdateEvent[*v1alpha1.LvmVolumeGroup], q workqueue.RateLimitingInterface) {
			log.Info(fmt.Sprintf("[RunLVMLogicalVolumeExtenderWatcherController] got an Update event for the LVMVolumeGroup %s", e.ObjectNew.GetName()))
			request := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: e.ObjectNew.GetNamespace(), Name: e.ObjectNew.GetName()}}
			q.Add(request)
			log.Info(fmt.Sprintf("[RunLVMLogicalVolumeExtenderWatcherController] added the LVMVolumeGroup %s to the Reconcilers queue", e.ObjectNew.GetName()))
		},
	}))
	if err != nil {
		log.Error(err, "[RunLVMLogicalVolumeExtenderWatcherController] unable to watch the events")
		return err
	}

	return nil
}

func shouldLLVExtenderReconcileEvent(log logger.Logger, newLVG *v1alpha1.LvmVolumeGroup, nodeName string) bool {
	// for new LVMVolumeGroups
	if reflect.DeepEqual(newLVG.Status, v1alpha1.LvmVolumeGroupStatus{}) {
		log.Debug(fmt.Sprintf("[RunLVMLogicalVolumeExtenderWatcherController] the LVMVolumeGroup %s should not be reconciled as its Status is not initialized yet", newLVG.Name))
		return false
	}

	if !belongsToNode(newLVG, nodeName) {
		log.Debug(fmt.Sprintf("[RunLVMLogicalVolumeExtenderWatcherController] the LVMVolumeGroup %s should not be reconciled as it does not belong to the node %s", newLVG.Name, nodeName))
		return false
	}

	if newLVG.Status.Phase != internal.PhaseReady {
		log.Debug(fmt.Sprintf("[RunLVMLogicalVolumeExtenderWatcherController] the LVMVolumeGroup %s should not be reconciled as its Status.Phase is not Ready", newLVG.Name))
		return false
	}

	return true
}

func ReconcileLVMLogicalVolumeExtension(ctx context.Context, cl client.Client, metrics monitoring.Metrics, log logger.Logger, sdsCache *cache.Cache, lvg *v1alpha1.LvmVolumeGroup) bool {
	log.Debug(fmt.Sprintf("[ReconcileLVMLogicalVolumeExtension] tries to get LLV resources with percent size for the LVMVolumeGroup %s", lvg.Name))
	llvs, err := getAllLLVsWithPercentSize(ctx, cl, lvg.Name)
	if err != nil {
		log.Error(err, "[ReconcileLVMLogicalVolumeExtension] unable to get LLV resources")
		return true
	}
	log.Debug(fmt.Sprintf("[ReconcileLVMLogicalVolumeExtension] successfully got LLV resources for the LVMVolumeGroup %s", lvg.Name))

	if len(llvs) == 0 {
		log.Info(fmt.Sprintf("[ReconcileLVMLogicalVolumeExtension] no LVMLogicalVolumes with percent size were found for the LVMVolumeGroup %s", lvg.Name))
		return false
	}

	shouldRetry := false
	for _, llv := range llvs {
		log.Info(fmt.Sprintf("[ReconcileLVMLogicalVolumeExtension] starts to reconcile the LVMLogicalVolume %s", llv.Name))
		llvRequestedSize, err := getLLVRequestedSize(&llv, lvg)
		if err != nil {
			log.Error(err, fmt.Sprintf("[ReconcileLVMLogicalVolumeExtension] unable to get requested size of the LVMLogicalVolume %s", llv.Name))
			shouldRetry = true
			continue
		}
		log.Debug(fmt.Sprintf("[ReconcileLVMLogicalVolumeExtension] successfully got the requested size of the LVMLogicalVolume %s, size: %s", llv.Name, llvRequestedSize.String()))

		lv := sdsCache.FindLV(lvg.Spec.ActualVGNameOnTheNode, llv.Spec.ActualLVNameOnTheNode)
		if lv == nil {
			err = fmt.Errorf("LV %s not found", llv.Spec.ActualLVNameOnTheNode)
			log.Error(err, fmt.Sprintf("[ReconcileLVMLogicalVolumeExtension] unable to find LV %s of the LVMLogicalVolume %s", llv.Spec.ActualLVNameOnTheNode, llv.Name))
			err = updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, &llv, LLVStatusPhaseFailed, err.Error())
			if err != nil {
				log.Error(err, fmt.Sprintf("[ReconcileLVMLogicalVolumeExtension] unable to update the LVMLogicalVolume %s", llv.Name))
			}
			shouldRetry = true
			continue
		}

		if utils.AreSizesEqualWithinDelta(llvRequestedSize, lv.LVSize, internal.ResizeDelta) {
			log.Info(fmt.Sprintf("[ReconcileLVMLogicalVolumeExtension] the LVMLogicalVolume %s should not be extended", llv.Name))
			continue
		}

		if llvRequestedSize.Value() < lv.LVSize.Value() {
			log.Warning(fmt.Sprintf("[ReconcileLVMLogicalVolumeExtension] the LVMLogicalVolume %s requested size %s is less than actual one on the node %s", llv.Name, llvRequestedSize.String(), lv.LVSize.String()))
			continue
		}

		freeSpace := getFreeLVGSpaceForLLV(lvg, &llv)
		if llvRequestedSize.Value()+internal.ResizeDelta.Value() > freeSpace.Value() {
			err = errors.New("not enough space")
			log.Error(err, fmt.Sprintf("[ReconcileLVMLogicalVolumeExtension] unable to extend the LV %s of the LVMLogicalVolume %s", llv.Spec.ActualLVNameOnTheNode, llv.Name))
			err = updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, &llv, LLVStatusPhaseFailed, fmt.Sprintf("unable to extend LV, err: %s", err.Error()))
			if err != nil {
				log.Error(err, fmt.Sprintf("[ReconcileLVMLogicalVolumeExtension] unable to update the LVMLogicalVolume %s", llv.Name))
				shouldRetry = true
			}
			continue
		}

		log.Info(fmt.Sprintf("[ReconcileLVMLogicalVolumeExtension] the LVMLogicalVolume %s should be extended from %s to %s size", llv.Name, llv.Status.ActualSize.String(), llvRequestedSize.String()))
		err = updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, &llv, LLVStatusPhaseResizing, "")
		if err != nil {
			log.Error(err, fmt.Sprintf("[ReconcileLVMLogicalVolumeExtension] unable to update the LVMLogicalVolume %s", llv.Name))
			shouldRetry = true
			continue
		}

		cmd, err := utils.ExtendLV(llvRequestedSize.Value(), lvg.Spec.ActualVGNameOnTheNode, llv.Spec.ActualLVNameOnTheNode)
		if err != nil {
			log.Error(err, fmt.Sprintf("[ReconcileLVMLogicalVolumeExtension] unable to extend LV %s of the LVMLogicalVolume %s, cmd: %s", llv.Spec.ActualLVNameOnTheNode, llv.Name, cmd))
			err = updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, &llv, LLVStatusPhaseFailed, fmt.Sprintf("unable to extend LV, err: %s", err.Error()))
			if err != nil {
				log.Error(err, fmt.Sprintf("[ReconcileLVMLogicalVolumeExtension] unable to update the LVMLogicalVolume %s", llv.Name))
			}
			shouldRetry = true
			continue
		}
		log.Info(fmt.Sprintf("[ReconcileLVMLogicalVolumeExtension] the LVMLogicalVolume %s has been successfully extended", llv.Name))

		var (
			maxAttempts     = 5
			currentAttempts = 0
		)
		for currentAttempts < maxAttempts {
			lv = sdsCache.FindLV(lvg.Spec.ActualVGNameOnTheNode, llv.Spec.ActualLVNameOnTheNode)
			if utils.AreSizesEqualWithinDelta(lv.LVSize, llvRequestedSize, internal.ResizeDelta) {
				log.Debug(fmt.Sprintf("[ReconcileLVMLogicalVolumeExtension] LV %s of the LVMLogicalVolume %s was successfully updated in the cache", lv.LVName, llv.Name))
				break
			}

			log.Warning(fmt.Sprintf("[ReconcileLVMLogicalVolumeExtension] LV %s size of the LVMLogicalVolume %s was not yet updated in the cache, retry...", lv.LVName, llv.Name))
			currentAttempts++
			time.Sleep(1 * time.Second)
		}

		if currentAttempts == maxAttempts {
			err = fmt.Errorf("LV %s is not updated in the cache", lv.LVName)
			log.Error(err, fmt.Sprintf("[ReconcileLVMLogicalVolumeExtension] unable to resize the LVMLogicalVolume %s", llv.Name))
			shouldRetry = true

			if err = updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, &llv, LLVStatusPhaseFailed, err.Error()); err != nil {
				log.Error(err, fmt.Sprintf("[ReconcileLVMLogicalVolumeExtension] unable to update the LVMLogicalVolume %s", llv.Name))
			}
			continue
		}

		updated, err := updateLLVPhaseToCreatedIfNeeded(ctx, cl, &llv, lv.LVSize)
		if err != nil {
			log.Error(err, fmt.Sprintf("[ReconcileLVMLogicalVolumeExtension] unable to update the LVMLogicalVolume %s", llv.Name))
			shouldRetry = true
			continue
		}

		if updated {
			log.Info(fmt.Sprintf("[ReconcileLVMLogicalVolumeExtension] the LVMLogicalVolume %s was successfully updated", llv.Name))
		} else {
			log.Info(fmt.Sprintf("[ReconcileLVMLogicalVolumeExtension] no need to update the LVMLogicalVolume %s", llv.Name))
		}
	}

	return shouldRetry
}

func getAllLLVsWithPercentSize(ctx context.Context, cl client.Client, lvgName string) ([]v1alpha1.LVMLogicalVolume, error) {
	llvList := &v1alpha1.LVMLogicalVolumeList{}
	err := cl.List(ctx, llvList)
	if err != nil {
		return nil, err
	}

	result := make([]v1alpha1.LVMLogicalVolume, 0, len(llvList.Items))
	for _, llv := range llvList.Items {
		if llv.Spec.LvmVolumeGroupName == lvgName && isPercentSize(llv.Spec.Size) {
			result = append(result, llv)
		}
	}

	return result, nil
}
