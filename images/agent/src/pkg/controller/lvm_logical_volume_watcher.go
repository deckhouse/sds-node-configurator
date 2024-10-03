package controller

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/google/go-cmp/cmp"
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

	"agent/config"
	"agent/internal"
	"agent/pkg/cache"
	"agent/pkg/logger"
	"agent/pkg/monitoring"
	"agent/pkg/utils"
)

const (
	Thick = "Thick"
	Thin  = "Thin"

	CreateReconcile reconcileType = "Create"
	UpdateReconcile reconcileType = "Update"
	DeleteReconcile reconcileType = "Delete"

	lvmLogicalVolumeWatcherCtrlName = "lvm-logical-volume-watcher-controller"

	LLVStatusPhaseCreated  = "Created"
	LLVStatusPhasePending  = "Pending"
	LLVStatusPhaseResizing = "Resizing"
	LLVStatusPhaseFailed   = "Failed"
)

type (
	reconcileType string
)

func RunLVMLogicalVolumeWatcherController(
	mgr manager.Manager,
	cfg config.Options,
	log logger.Logger,
	metrics monitoring.Metrics,
	sdsCache *cache.Cache,
) (controller.Controller, error) {
	cl := mgr.GetClient()

	c, err := controller.New(lvmLogicalVolumeWatcherCtrlName, mgr, controller.Options{
		Reconciler: reconcile.Func(func(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
			log.Info(fmt.Sprintf("[RunLVMLogicalVolumeWatcherController] Reconciler starts reconciliation of the LVMLogicalVolume: %s", request.Name))

			log.Debug(fmt.Sprintf("[RunLVMLogicalVolumeWatcherController] tries to get the LVMLogicalVolume %s", request.Name))
			llv := &v1alpha1.LVMLogicalVolume{}
			err := cl.Get(ctx, request.NamespacedName, llv)
			if err != nil {
				if k8serr.IsNotFound(err) {
					log.Debug(fmt.Sprintf("[ReconcileLVMLogicalVolume] LVMLogicalVolume %s not found. Object has probably been deleted", request.NamespacedName))
					return reconcile.Result{}, nil
				}
				return reconcile.Result{}, err
			}

			lvg, err := getLVMVolumeGroup(ctx, cl, metrics, llv.Spec.LVMVolumeGroupName)
			if err != nil {
				if k8serr.IsNotFound(err) {
					log.Error(err, fmt.Sprintf("[ReconcileLVMLogicalVolume] LVMVolumeGroup %s not found for LVMLogicalVolume %s. Retry in %s", llv.Spec.LVMVolumeGroupName, llv.Name, cfg.VolumeGroupScanIntervalSec.String()))
					err = updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, llv, LLVStatusPhaseFailed, fmt.Sprintf("LVMVolumeGroup %s not found", llv.Spec.LVMVolumeGroupName))
					if err != nil {
						log.Error(err, fmt.Sprintf("[ReconcileLVMLogicalVolume] unable to update the LVMLogicalVolume %s", llv.Name))
						return reconcile.Result{}, err
					}

					return reconcile.Result{
						RequeueAfter: cfg.VolumeGroupScanIntervalSec,
					}, nil
				}

				err = updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, llv, LLVStatusPhaseFailed, fmt.Sprintf("Unable to get selected LVMVolumeGroup, err: %s", err.Error()))
				if err != nil {
					log.Error(err, fmt.Sprintf("[ReconcileLVMLogicalVolume] unable to update the LVMLogicalVolume %s", llv.Name))
				}
				return reconcile.Result{}, err
			}

			if !belongsToNode(lvg, cfg.NodeName) {
				log.Info(fmt.Sprintf("[ReconcileLVMLogicalVolume] the LVMVolumeGroup %s of the LVMLogicalVolume %s does not belongs to the current node: %s. Reconciliation stopped", lvg.Name, llv.Name, cfg.NodeName))
				return reconcile.Result{}, nil
			}
			log.Info(fmt.Sprintf("[ReconcileLVMLogicalVolume] the LVMVolumeGroup %s of the LVMLogicalVolume %s belongs to the current node: %s. Reconciliation continues", lvg.Name, llv.Name, cfg.NodeName))

			// this case prevents the unexpected behavior when the controller runs up with existing LVMLogicalVolumes
			if vgs, _ := sdsCache.GetVGs(); len(vgs) == 0 {
				log.Warning(fmt.Sprintf("[RunLVMLogicalVolumeWatcherController] unable to reconcile the request as no VG was found in the cache. Retry in %s", cfg.VolumeGroupScanIntervalSec.String()))
				return reconcile.Result{RequeueAfter: cfg.VolumeGroupScanIntervalSec}, nil
			}

			log.Debug(fmt.Sprintf("[ReconcileLVMLogicalVolume] tries to add the finalizer %s to the LVMLogicalVolume %s", internal.SdsNodeConfiguratorFinalizer, llv.Name))
			added, err := addLLVFinalizerIfNotExist(ctx, cl, log, metrics, llv)
			if err != nil {
				log.Error(err, fmt.Sprintf("[ReconcileLVMLogicalVolume] unable to update the LVMLogicalVolume %s", llv.Name))
				return reconcile.Result{}, err
			}
			if added {
				log.Debug(fmt.Sprintf("[ReconcileLVMLogicalVolume] successfully added the finalizer %s to the LVMLogicalVolume %s", internal.SdsNodeConfiguratorFinalizer, llv.Name))
			} else {
				log.Debug(fmt.Sprintf("[ReconcileLVMLogicalVolume] no need to add the finalizer %s to the LVMLogicalVolume %s", internal.SdsNodeConfiguratorFinalizer, llv.Name))
			}

			log.Info(fmt.Sprintf("[ReconcileLVMLogicalVolume] starts to validate the LVMLogicalVolume %s", llv.Name))
			valid, reason := validateLVMLogicalVolume(sdsCache, llv, lvg)
			if !valid {
				log.Warning(fmt.Sprintf("[ReconcileLVMLogicalVolume] the LVMLogicalVolume %s is not valid, reason: %s", llv.Name, reason))
				err = updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, llv, LLVStatusPhaseFailed, reason)
				if err != nil {
					log.Error(err, fmt.Sprintf("[ReconcileLVMLogicalVolume] unable to update the LVMLogicalVolume %s", llv.Name))
					return reconcile.Result{}, err
				}

				return reconcile.Result{}, nil
			}
			log.Info(fmt.Sprintf("[ReconcileLVMLogicalVolume] successfully validated the LVMLogicalVolume %s", llv.Name))

			shouldRequeue, err := ReconcileLVMLogicalVolume(ctx, cl, log, metrics, sdsCache, llv, lvg)
			if err != nil {
				log.Error(err, fmt.Sprintf("[RunLVMLogicalVolumeWatcherController] an error occurred while reconciling the LVMLogicalVolume: %s", request.Name))
				updErr := updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, llv, LLVStatusPhaseFailed, err.Error())
				if updErr != nil {
					log.Error(updErr, fmt.Sprintf("[RunLVMLogicalVolumeWatcherController] unable to update the LVMLogicalVolume %s", llv.Name))
					return reconcile.Result{}, updErr
				}
			}
			if shouldRequeue {
				log.Info(fmt.Sprintf("[RunLVMLogicalVolumeWatcherController] some issues were occurred while reconciliation the LVMLogicalVolume %s. Requeue the request in %s", request.Name, cfg.LLVRequeueIntervalSec.String()))
				return reconcile.Result{RequeueAfter: cfg.LLVRequeueIntervalSec}, nil
			}

			log.Info(fmt.Sprintf("[RunLVMLogicalVolumeWatcherController] successfully ended reconciliation of the LVMLogicalVolume %s", request.Name))
			return reconcile.Result{}, nil
		}),
		MaxConcurrentReconciles: 10,
	})

	if err != nil {
		log.Error(err, "[RunLVMLogicalVolumeWatcherController] unable to create controller")
		return nil, err
	}

	err = c.Watch(source.Kind(mgr.GetCache(), &v1alpha1.LVMLogicalVolume{}, handler.TypedFuncs[*v1alpha1.LVMLogicalVolume, reconcile.Request]{
		CreateFunc: func(_ context.Context, e event.TypedCreateEvent[*v1alpha1.LVMLogicalVolume], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			log.Info(fmt.Sprintf("[RunLVMLogicalVolumeWatcherController] got a create event for the LVMLogicalVolume: %s", e.Object.GetName()))
			request := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: e.Object.GetNamespace(), Name: e.Object.GetName()}}
			q.Add(request)
			log.Info(fmt.Sprintf("[RunLVMLogicalVolumeWatcherController] added the request of the LVMLogicalVolume %s to Reconciler", e.Object.GetName()))
		},

		UpdateFunc: func(_ context.Context, e event.TypedUpdateEvent[*v1alpha1.LVMLogicalVolume], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			log.Info(fmt.Sprintf("[RunLVMLogicalVolumeWatcherController] got an update event for the LVMLogicalVolume: %s", e.ObjectNew.GetName()))
			// TODO: Figure out how to log it in our logger.
			if cfg.Loglevel == "4" {
				fmt.Println("==============START DIFF==================")
				fmt.Println(cmp.Diff(e.ObjectOld, e.ObjectNew))
				fmt.Println("==============END DIFF==================")
			}

			if reflect.DeepEqual(e.ObjectOld.Spec, e.ObjectNew.Spec) && e.ObjectNew.DeletionTimestamp == nil {
				log.Info(fmt.Sprintf("[RunLVMLogicalVolumeWatcherController] no target changes were made for the LVMLogicalVolume %s. No need to reconcile the request", e.ObjectNew.Name))
				return
			}

			request := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: e.ObjectNew.Namespace, Name: e.ObjectNew.Name}}
			q.Add(request)
			log.Info(fmt.Sprintf("[RunLVMLogicalVolumeWatcherController] added the request of the LVMLogicalVolume %s to Reconciler", e.ObjectNew.GetName()))
		},
	}))

	if err != nil {
		log.Error(err, "[RunLVMLogicalVolumeWatcherController] the controller is unable to watch")
		return nil, err
	}

	return c, err
}

func ReconcileLVMLogicalVolume(ctx context.Context, cl client.Client, log logger.Logger, metrics monitoring.Metrics, sdsCache *cache.Cache, llv *v1alpha1.LVMLogicalVolume, lvg *v1alpha1.LVMVolumeGroup) (bool, error) {
	log.Debug(fmt.Sprintf("[ReconcileLVMLogicalVolume] starts the reconciliation for the LVMLogicalVolume %s", llv.Name))

	log.Debug(fmt.Sprintf("[ReconcileLVMLogicalVolume] tries to identify the reconciliation type for the LVMLogicalVolume %s", llv.Name))
	log.Trace(fmt.Sprintf("[ReconcileLVMLogicalVolume] %+v", llv))

	switch identifyReconcileFunc(sdsCache, lvg.Spec.ActualVGNameOnTheNode, llv) {
	case CreateReconcile:
		return reconcileLLVCreateFunc(ctx, cl, log, metrics, sdsCache, llv, lvg)
	case UpdateReconcile:
		return reconcileLLVUpdateFunc(ctx, cl, log, metrics, sdsCache, llv, lvg)
	case DeleteReconcile:
		return reconcileLLVDeleteFunc(ctx, cl, log, metrics, sdsCache, llv, lvg)
	default:
		log.Info(fmt.Sprintf("[runEventReconcile] the LVMLogicalVolume %s has compeleted configuration and should not be reconciled", llv.Name))
		if llv.Status.Phase != LLVStatusPhaseCreated {
			log.Warning(fmt.Sprintf("[runEventReconcile] the LVMLogicalVolume %s should not be reconciled but has an unexpected phase: %s. Setting the phase to %s", llv.Name, llv.Status.Phase, LLVStatusPhaseCreated))
			err := updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, llv, LLVStatusPhaseCreated, "")
			if err != nil {
				return true, err
			}
		}
	}

	return false, nil
}

func reconcileLLVCreateFunc(
	ctx context.Context,
	cl client.Client,
	log logger.Logger,
	metrics monitoring.Metrics,
	sdsCache *cache.Cache,
	llv *v1alpha1.LVMLogicalVolume,
	lvg *v1alpha1.LVMVolumeGroup,
) (bool, error) {
	log.Debug(fmt.Sprintf("[reconcileLLVCreateFunc] starts reconciliation for the LVMLogicalVolume %s", llv.Name))

	// this check prevents infinite resource updating after retries
	if llv.Status == nil {
		err := updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, llv, LLVStatusPhasePending, "")
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLLVCreateFunc] unable to update the LVMLogicalVolume %s", llv.Name))
			return true, err
		}
	}
	llvRequestSize, err := getLLVRequestedSize(llv, lvg)
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLLVCreateFunc] unable to get LVMLogicalVolume %s requested size", llv.Name))
		return false, err
	}

	freeSpace := getFreeLVGSpaceForLLV(lvg, llv)
	log.Trace(fmt.Sprintf("[reconcileLLVCreateFunc] the LVMLogicalVolume %s, LV: %s, VG: %s type: %s requested size: %s, free space: %s", llv.Name, llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode, llv.Spec.Type, llvRequestSize.String(), freeSpace.String()))

	if !utils.AreSizesEqualWithinDelta(llvRequestSize, freeSpace, internal.ResizeDelta) {
		if freeSpace.Value() < llvRequestSize.Value()+internal.ResizeDelta.Value() {
			err = errors.New("not enough space")
			log.Error(err, fmt.Sprintf("[reconcileLLVCreateFunc] the LV %s requested size %s of the LVMLogicalVolume %s is more than the actual free space %s", llv.Spec.ActualLVNameOnTheNode, llvRequestSize.String(), llv.Name, freeSpace.String()))

			// we return true cause the user might manage LVMVolumeGroup free space without changing the LLV
			return true, err
		}
	}

	var cmd string
	switch llv.Spec.Type {
	case Thick:
		log.Debug(fmt.Sprintf("[reconcileLLVCreateFunc] LV %s will be created in VG %s with size: %s", llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode, llvRequestSize.String()))
		cmd, err = utils.CreateThickLogicalVolume(lvg.Spec.ActualVGNameOnTheNode, llv.Spec.ActualLVNameOnTheNode, llvRequestSize.Value(), isContiguous(llv))
	case Thin:
		if llv.Spec.Source == "" {
			log.Debug(fmt.Sprintf("[reconcileLLVCreateFunc] LV %s of the LVMLogicalVolume %s will be create in Thin-pool %s with size %s", llv.Spec.ActualLVNameOnTheNode, llv.Name, llv.Spec.Thin.PoolName, llvRequestSize.String()))
			cmd, err = utils.CreateThinLogicalVolume(lvg.Spec.ActualVGNameOnTheNode, llv.Spec.Thin.PoolName, llv.Spec.ActualLVNameOnTheNode, llvRequestSize.Value())
		} else {
			log.Debug(fmt.Sprintf("[reconcileLLVCreateFunc] Snapshot (for source %s) LV %s of the LVMLogicalVolume %s will be create in Thin-pool %s with size %s", llv.Spec.Source, llv.Spec.ActualLVNameOnTheNode, llv.Name, llv.Spec.Thin.PoolName, llvRequestSize.String()))
			sourceLlv := &v1alpha1.LVMLogicalVolume{}
			if err = cl.Get(ctx, types.NamespacedName{Namespace: llv.Namespace, Name: llv.Spec.Source}, sourceLlv); err != nil {
				log.Error(err, fmt.Sprintf("[reconcileLLVCreateFunc] unable to find source LogicalVolume %s (%s)", llv.Spec.Source, llv.Namespace))
			} else {
				cmd, err = utils.CreateThinLogicalVolumeSnapshot(string(llv.UID), lvg.Spec.ActualVGNameOnTheNode, sourceLlv)
			}
		}
	}
	log.Debug(fmt.Sprintf("[reconcileLLVCreateFunc] runs cmd: %s", cmd))
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLLVCreateFunc] unable to create a %s LogicalVolume for the LVMLogicalVolume %s", llv.Spec.Type, llv.Name))
		return true, err
	}

	log.Info(fmt.Sprintf("[reconcileLLVCreateFunc] successfully created LV %s in VG %s for LVMLogicalVolume resource with name: %s", llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode, llv.Name))

	log.Debug(fmt.Sprintf("[reconcileLLVCreateFunc] adds the LV %s to the cache", llv.Spec.ActualLVNameOnTheNode))
	sdsCache.AddLV(lvg.Spec.ActualVGNameOnTheNode, llv.Spec.ActualLVNameOnTheNode)
	log.Debug(fmt.Sprintf("[reconcileLLVCreateFunc] tries to get the LV %s actual size", llv.Spec.ActualLVNameOnTheNode))
	actualSize := getLVActualSize(sdsCache, lvg.Spec.ActualVGNameOnTheNode, llv.Spec.ActualLVNameOnTheNode)
	if actualSize.Value() == 0 {
		log.Warning(fmt.Sprintf("[reconcileLLVCreateFunc] unable to get actual size for LV %s in VG %s (likely LV was not found in the cache), retry...", llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode))
		return true, nil
	}
	log.Debug(fmt.Sprintf("[reconcileLLVCreateFunc] successfully got the LV %s actual size", llv.Spec.ActualLVNameOnTheNode))
	log.Trace(fmt.Sprintf("[reconcileLLVCreateFunc] the LV %s in VG: %s has actual size: %s", llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode, actualSize.String()))

	updated, err := updateLLVPhaseToCreatedIfNeeded(ctx, cl, llv, actualSize)
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLLVCreateFunc] unable to update the LVMLogicalVolume %s", llv.Name))
		return true, err
	}

	if updated {
		log.Info(fmt.Sprintf("[reconcileLLVCreateFunc] successfully updated the LVMLogicalVolume %s status phase to Created", llv.Name))
	} else {
		log.Warning(fmt.Sprintf("[reconcileLLVCreateFunc] LVMLogicalVolume %s status phase was not updated to Created due to the resource has already have the same phase", llv.Name))
	}

	log.Info(fmt.Sprintf("[reconcileLLVCreateFunc] successfully ended the reconciliation for the LVMLogicalVolume %s", llv.Name))
	return false, nil
}

func reconcileLLVUpdateFunc(
	ctx context.Context,
	cl client.Client,
	log logger.Logger,
	metrics monitoring.Metrics,
	sdsCache *cache.Cache,
	llv *v1alpha1.LVMLogicalVolume,
	lvg *v1alpha1.LVMVolumeGroup,
) (bool, error) {
	log.Debug(fmt.Sprintf("[reconcileLLVUpdateFunc] starts reconciliation for the LVMLogicalVolume %s", llv.Name))

	// status might be nil if a user creates the resource with LV name which matches existing LV on the node
	if llv.Status == nil {
		err := updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, llv, LLVStatusPhasePending, "")
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLLVUpdateFunc] unable to update the LVMLogicalVolume %s", llv.Name))
			return true, err
		}
	}

	// it needs to get current LV size from the node as status might be nil
	log.Debug(fmt.Sprintf("[reconcileLLVUpdateFunc] tries to get LVMLogicalVolume %s actual size before the extension", llv.Name))
	actualSize := getLVActualSize(sdsCache, lvg.Spec.ActualVGNameOnTheNode, llv.Spec.ActualLVNameOnTheNode)
	if actualSize.Value() == 0 {
		log.Warning(fmt.Sprintf("[reconcileLLVUpdateFunc] LV %s of the LVMLogicalVolume %s has zero size (likely LV was not updated in the cache) ", llv.Spec.ActualLVNameOnTheNode, llv.Name))
		return true, nil
	}
	log.Debug(fmt.Sprintf("[reconcileLLVUpdateFunc] successfully got LVMLogicalVolume %s actual size %s before the extension", llv.Name, actualSize.String()))

	log.Debug(fmt.Sprintf("[reconcileLLVUpdateFunc] tries to count the LVMLogicalVolume %s requested size", llv.Name))
	llvRequestSize, err := getLLVRequestedSize(llv, lvg)
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLLVCreateFunc] unable to get LVMLogicalVolume %s requested size", llv.Name))
		return false, err
	}
	log.Debug(fmt.Sprintf("[reconcileLLVUpdateFunc] successfully counted the LVMLogicalVolume %s requested size: %s", llv.Name, llvRequestSize.String()))

	if utils.AreSizesEqualWithinDelta(actualSize, llvRequestSize, internal.ResizeDelta) {
		log.Warning(fmt.Sprintf("[reconcileLLVUpdateFunc] the LV %s in VG %s has the same actual size %s as the requested size %s", llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode, actualSize.String(), llvRequestSize.String()))

		updated, err := updateLLVPhaseToCreatedIfNeeded(ctx, cl, llv, actualSize)
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLLVUpdateFunc] unable to update the LVMLogicalVolume %s", llv.Name))
			return true, err
		}

		if updated {
			log.Info(fmt.Sprintf("[reconcileLLVUpdateFunc] successfully updated the LVMLogicalVolume %s status phase to Created", llv.Name))
		} else {
			log.Info(fmt.Sprintf("[reconcileLLVUpdateFunc] no need to update the LVMLogicalVolume %s status phase to Created", llv.Name))
		}

		log.Info(fmt.Sprintf("[reconcileLLVUpdateFunc] successfully ended reconciliation for the LVMLogicalVolume %s", llv.Name))

		return false, nil
	}

	extendingSize := subtractQuantity(llvRequestSize, actualSize)
	log.Trace(fmt.Sprintf("[reconcileLLVUpdateFunc] the LV %s in VG %s has extending size %s", llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode, extendingSize.String()))
	if extendingSize.Value() < 0 {
		err = fmt.Errorf("specified LV size %dB is less than actual one on the node %dB", llvRequestSize.Value(), actualSize.Value())
		log.Error(err, fmt.Sprintf("[reconcileLLVUpdateFunc] unable to extend the LVMLogicalVolume %s", llv.Name))
		return false, err
	}

	log.Info(fmt.Sprintf("[reconcileLLVUpdateFunc] the LVMLogicalVolume %s should be resized", llv.Name))
	// this check prevents infinite resource updates after retry
	if llv.Status.Phase != Failed {
		err := updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, llv, LLVStatusPhaseResizing, "")
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLLVUpdateFunc] unable to update the LVMLogicalVolume %s", llv.Name))
			return true, err
		}
	}

	freeSpace := getFreeLVGSpaceForLLV(lvg, llv)
	log.Trace(fmt.Sprintf("[reconcileLLVUpdateFunc] the LVMLogicalVolume %s, LV: %s, VG: %s, type: %s, extending size: %s, free space: %s", llv.Name, llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode, llv.Spec.Type, extendingSize.String(), freeSpace.String()))

	if !utils.AreSizesEqualWithinDelta(freeSpace, extendingSize, internal.ResizeDelta) {
		if freeSpace.Value() < extendingSize.Value()+internal.ResizeDelta.Value() {
			err = errors.New("not enough space")
			log.Error(err, fmt.Sprintf("[reconcileLLVUpdateFunc] the LV %s requested size %s of the LVMLogicalVolume %s is more than actual free space %s", llv.Spec.ActualLVNameOnTheNode, llvRequestSize.String(), llv.Name, freeSpace.String()))

			// returns true cause a user might manage LVG free space without changing the LLV
			return true, err
		}
	}

	log.Debug(fmt.Sprintf("[reconcileLLVUpdateFunc] LV %s of the LVMLogicalVolume %s will be extended with size: %s", llv.Spec.ActualLVNameOnTheNode, llv.Name, llvRequestSize.String()))
	cmd, err := utils.ExtendLV(llvRequestSize.Value(), lvg.Spec.ActualVGNameOnTheNode, llv.Spec.ActualLVNameOnTheNode)
	log.Debug(fmt.Sprintf("[reconcileLLVUpdateFunc] runs cmd: %s", cmd))
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLLVUpdateFunc] unable to ExtendLV, name: %s, type: %s", llv.Spec.ActualLVNameOnTheNode, llv.Spec.Type))
		return true, err
	}

	log.Info(fmt.Sprintf("[reconcileLLVUpdateFunc] successfully extended LV %s in VG %s for LVMLogicalVolume resource with name: %s", llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode, llv.Name))

	log.Debug(fmt.Sprintf("[reconcileLLVUpdateFunc] tries to get LVMLogicalVolume %s actual size after the extension", llv.Name))
	newActualSize := getLVActualSize(sdsCache, lvg.Spec.ActualVGNameOnTheNode, llv.Spec.ActualLVNameOnTheNode)

	// this case might be triggered if sds cache will not update lv state in time
	if newActualSize.Value() == actualSize.Value() {
		log.Warning(fmt.Sprintf("[reconcileLLVUpdateFunc] LV %s of the LVMLogicalVolume %s was extended but cache is not updated yet. It will be retried", llv.Spec.ActualLVNameOnTheNode, llv.Name))
		return true, nil
	}

	log.Debug(fmt.Sprintf("[reconcileLLVUpdateFunc] successfully got LVMLogicalVolume %s actual size before the extension", llv.Name))
	log.Trace(fmt.Sprintf("[reconcileLLVUpdateFunc] the LV %s in VG %s actual size %s", llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode, newActualSize.String()))

	// need this here as a user might create the LLV with existing LV
	updated, err := updateLLVPhaseToCreatedIfNeeded(ctx, cl, llv, newActualSize)
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLLVUpdateFunc] unable to update the LVMLogicalVolume %s", llv.Name))
		return true, err
	}

	if updated {
		log.Info(fmt.Sprintf("[reconcileLLVUpdateFunc] successfully updated the LVMLogicalVolume %s status phase to Created", llv.Name))
	} else {
		log.Info(fmt.Sprintf("[reconcileLLVUpdateFunc] no need to update the LVMLogicalVolume %s status phase to Created", llv.Name))
	}

	log.Info(fmt.Sprintf("[reconcileLLVUpdateFunc] successfully ended reconciliation for the LVMLogicalVolume %s", llv.Name))
	return false, nil
}

func reconcileLLVDeleteFunc(
	ctx context.Context,
	cl client.Client,
	log logger.Logger,
	metrics monitoring.Metrics,
	sdsCache *cache.Cache,
	llv *v1alpha1.LVMLogicalVolume,
	lvg *v1alpha1.LVMVolumeGroup,
) (bool, error) {
	log.Debug(fmt.Sprintf("[reconcileLLVDeleteFunc] starts reconciliation for the LVMLogicalVolume %s", llv.Name))

	// The controller won't remove the LLV resource and LV volume till the resource has any other finalizer.
	if len(llv.Finalizers) != 0 {
		if len(llv.Finalizers) > 1 ||
			llv.Finalizers[0] != internal.SdsNodeConfiguratorFinalizer {
			log.Debug(fmt.Sprintf("[reconcileLLVDeleteFunc] unable to delete LVMLogicalVolume %s for now due to it has any other finalizer", llv.Name))
			return false, nil
		}
	}

	err := deleteLVIfNeeded(log, sdsCache, lvg.Spec.ActualVGNameOnTheNode, llv)
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLLVDeleteFunc] unable to delete the LV %s in VG %s", llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode))
		return true, err
	}

	log.Info(fmt.Sprintf("[reconcileLLVDeleteFunc] successfully deleted the LV %s in VG %s", llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode))

	err = removeLLVFinalizersIfExist(ctx, cl, metrics, log, llv)
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLLVDeleteFunc] unable to remove finalizers from the LVMVolumeGroup %s", llv.Name))
		return true, err
	}

	log.Info(fmt.Sprintf("[reconcileLLVDeleteFunc] successfully ended reconciliation for the LVMLogicalVolume %s", llv.Name))
	return false, nil
}
