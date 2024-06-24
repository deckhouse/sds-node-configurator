package controller

import (
	"context"
	"errors"
	"fmt"
	"github.com/google/go-cmp/cmp"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"reflect"
	"sds-node-configurator/api/v1alpha1"
	"sds-node-configurator/config"
	"sds-node-configurator/internal"
	"sds-node-configurator/pkg/cache"
	"sds-node-configurator/pkg/logger"
	"sds-node-configurator/pkg/monitoring"
	"sds-node-configurator/pkg/utils"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

const (
	Thick = "Thick"
	Thin  = "Thin"

	CreateReconcile reconcileType = "Create"
	UpdateReconcile reconcileType = "Update"
	DeleteReconcile reconcileType = "Delete"

	lvmLogicalVolumeWatcherCtrlName = "lvm-logical-volume-watcher-controller"

	createdStatusPhase  = "Created"
	pendingStatusPhase  = "Pending"
	resizingStatusPhase = "Resizing"
	failedStatusPhase   = "Failed"
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
	kubeCache := mgr.GetCache()

	c, err := controller.New(lvmLogicalVolumeWatcherCtrlName, mgr, controller.Options{
		Reconciler: reconcile.Func(func(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
			log.Info(fmt.Sprintf("[RunLVMLogicalVolumeWatcherController] Reconciler starts reconciliation of the LVMLogicalVolume: %s", request.Name))

			// this case prevents the unexpected behavior when the controller runs up with existing LVMLogicalVolumes
			if vgs, _ := sdsCache.GetVGs(); len(vgs) == 0 {
				log.Warning(fmt.Sprintf("[RunLVMLogicalVolumeWatcherController] unable to reconcile the request as no VG was found in the cache. Retry in %s", cfg.VolumeGroupScanIntervalSec.String()))
				return reconcile.Result{RequeueAfter: cfg.VolumeGroupScanIntervalSec}, nil
			}

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

			lvg, err := getLVMVolumeGroup(ctx, cl, metrics, llv.Spec.LvmVolumeGroupName)
			if err != nil {
				if k8serr.IsNotFound(err) {
					log.Error(err, fmt.Sprintf("[ReconcileLVMLogicalVolume] LVMVolumeGroup %s not found for LVMLogicalVolume %s", llv.Spec.LvmVolumeGroupName, llv.Name))
					err = updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, llv, failedStatusPhase, fmt.Sprintf("LVMVolumeGroup %s not found", llv.Spec.LvmVolumeGroupName))
					if err != nil {
						log.Error(err, fmt.Sprintf("[ReconcileLVMLogicalVolume] unable to update the LVMLogicalVolume %s", llv.Name))
						return reconcile.Result{}, err
					}
					// no need to retry as it is likely a user change the selected LVMVolumeGroup than create the whole LVMVolumeGroup for a single LV
					return reconcile.Result{}, nil
				}

				err = updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, llv, failedStatusPhase, fmt.Sprintf("Unable to get selected LVMVolumeGroup, err: %s", err.Error()))
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

			log.Debug(fmt.Sprintf("[ReconcileLVMLogicalVolume] tries to parse resize delta %s", internal.ResizeDelta))
			delta, err := resource.ParseQuantity(internal.ResizeDelta)
			if err != nil {
				log.Error(err, fmt.Sprintf("[ReconcileLVMLogicalVolume] unable to parse resize delta %s", internal.ResizeDelta))
				return reconcile.Result{}, nil
			}
			log.Debug(fmt.Sprintf("[ReconcileLVMLogicalVolume] successfully parsed resize delta %s", internal.ResizeDelta))

			log.Info(fmt.Sprintf("[ReconcileLVMLogicalVolume] starts to validate the LVMLogicalVolume %s", llv.Name))
			valid, reason := validateLVMLogicalVolume(sdsCache, llv, lvg, delta)
			if !valid {
				log.Warning(fmt.Sprintf("[ReconcileLVMLogicalVolume] the LVMLogicalVolume %s is not valid, reason: %s", llv.Name, reason))
				err = updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, llv, failedStatusPhase, reason)
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
			}
			if shouldRequeue {
				log.Info(fmt.Sprintf("[RunLVMLogicalVolumeWatcherController] some issues were occured while reconciliation the LVMLogicalVolume %s. Requeue the request in %s", request.Name, cfg.LLVRequeueIntervalSec.String()))
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

	err = c.Watch(source.Kind(kubeCache, &v1alpha1.LVMLogicalVolume{}), handler.Funcs{
		CreateFunc: func(ctx context.Context, e event.CreateEvent, q workqueue.RateLimitingInterface) {
			log.Info(fmt.Sprintf("[RunLVMLogicalVolumeWatcherController] got a create event for the LVMLogicalVolume: %s", e.Object.GetName()))
			request := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: e.Object.GetNamespace(), Name: e.Object.GetName()}}

			q.Add(request)
			log.Info(fmt.Sprintf("[RunLVMLogicalVolumeWatcherController] added the request of the LVMLogicalVolume %s to Reconciler", e.Object.GetName()))
		},

		UpdateFunc: func(ctx context.Context, e event.UpdateEvent, q workqueue.RateLimitingInterface) {
			log.Info(fmt.Sprintf("[RunLVMLogicalVolumeWatcherController] got an update event for the LVMLogicalVolume: %s", e.ObjectNew.GetName()))

			log.Debug(fmt.Sprintf("[RunLVMLogicalVolumeWatcherController] tries to cast the request's old object %s to LVMLogicalVolume", e.ObjectOld.GetName()))
			oldLLV, ok := e.ObjectOld.(*v1alpha1.LVMLogicalVolume)
			if !ok {
				err = errors.New("unable to cast event object to a given type")
				log.Error(err, "[RunLVMLogicalVolumeWatcherController] an error occurs while handling an update event")
				return
			}
			log.Debug(fmt.Sprintf("[RunLVMLogicalVolumeWatcherController] successfully casted the request's old object %s to LVMLogicalVolume", e.ObjectOld.GetName()))

			log.Debug(fmt.Sprintf("[RunLVMLogicalVolumeWatcherController] tries to cast the request's new object %s to LVMLogicalVolume", e.ObjectOld.GetName()))
			newLLV, ok := e.ObjectNew.(*v1alpha1.LVMLogicalVolume)
			if !ok {
				err = errors.New("unable to cast event object to a given type")
				log.Error(err, "[RunLVMLogicalVolumeWatcherController] an error occurs while handling update event")
				return
			}
			log.Debug(fmt.Sprintf("[RunLVMLogicalVolumeWatcherController] successfully casted the request's new object %s to LVMLogicalVolume", e.ObjectOld.GetName()))

			// TODO: Figure out how to log it in our logger.
			if cfg.Loglevel == "4" {
				fmt.Println("==============START DIFF==================")
				fmt.Println(cmp.Diff(oldLLV, newLLV))
				fmt.Println("==============END DIFF==================")
			}

			if reflect.DeepEqual(oldLLV.Spec, newLLV.Spec) && newLLV.DeletionTimestamp == nil {
				log.Info(fmt.Sprintf("[RunLVMLogicalVolumeWatcherController] no target changes were made for the LVMLogicalVolume %s. No need to reconcile the request", newLLV.Name))
				return
			}

			request := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: newLLV.Namespace, Name: newLLV.Name}}
			q.Add(request)
			log.Info(fmt.Sprintf("[RunLVMLogicalVolumeWatcherController] added the request of the LVMLogicalVolume %s to Reconciler", e.ObjectNew.GetName()))
		},
	})

	if err != nil {
		log.Error(err, "[RunLVMLogicalVolumeWatcherController] the controller is unable to watch")
		return nil, err
	}

	return c, err
}

func ReconcileLVMLogicalVolume(ctx context.Context, cl client.Client, log logger.Logger, metrics monitoring.Metrics, sdsCache *cache.Cache, llv *v1alpha1.LVMLogicalVolume, lvg *v1alpha1.LvmVolumeGroup) (bool, error) {
	log.Debug(fmt.Sprintf("[ReconcileLVMLogicalVolume] starts the reconciliation for the LVMLogicalVolume %s", llv.Name))

	log.Debug(fmt.Sprintf("[ReconcileLVMLogicalVolume] tries to identify the reconciliation type for the LVMLogicalVolume %s", llv.Name))
	log.Trace(fmt.Sprintf("[ReconcileLVMLogicalVolume] %+v", llv))

	switch identifyReconcileFunc(sdsCache, lvg.Spec.ActualVGNameOnTheNode, llv) {
	case CreateReconcile:
		return reconcileLLVCreateFunc(ctx, cl, log, metrics, llv, lvg)
	case UpdateReconcile:
		return reconcileLLVUpdateFunc(ctx, cl, log, metrics, llv, lvg)
	case DeleteReconcile:
		return reconcileLLVDeleteFunc(ctx, cl, log, metrics, sdsCache, llv, lvg)
	default:
		log.Info(fmt.Sprintf("[runEventReconcile] the LVMLogicalVolume %s has compeleted configuration and should not be reconciled", llv.Name))
		if llv.Status.Phase != createdStatusPhase {
			log.Warning(fmt.Sprintf("[runEventReconcile] the LVMLogicalVolume %s should not be reconciled but has an unexpected phase: %s. Setting the phase to %s", llv.Name, llv.Status.Phase, createdStatusPhase))
			err := updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, llv, createdStatusPhase, "")
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
	llv *v1alpha1.LVMLogicalVolume,
	lvg *v1alpha1.LvmVolumeGroup,
) (bool, error) {
	log.Debug(fmt.Sprintf("[reconcileLLVCreateFunc] starts reconciliation for the LVMLogicalVolume %s", llv.Name))

	// this check prevents infinite resource updating after retries
	if llv.Status == nil {
		err := updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, llv, pendingStatusPhase, "")
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLLVCreateFunc] unable to update the LVMLogicalVolume %s", llv.Name))
			return true, err
		}
	}

	log.Debug(fmt.Sprintf("[reconcileLLVCreateFunc] tries to parse the resize delta: %s", internal.ResizeDelta))
	delta, err := resource.ParseQuantity(internal.ResizeDelta)
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLLVCreateFunc] unable to parse the resize delta, value: %s", internal.ResizeDelta))
		updErr := updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, llv, failedStatusPhase, fmt.Sprintf("Unable to parse the resize delta, value: %s, err: %s", internal.ResizeDelta, err.Error()))
		if updErr != nil {
			log.Error(updErr, fmt.Sprintf("[reconcileLLVCreateFunc] unable to update the LVMLogicalVolume %s", llv.Name))
			return true, updErr
		}

		return true, err
	}
	log.Debug(fmt.Sprintf("[reconcileLLVCreateFunc] successfully parsed the resize delta: %s", internal.ResizeDelta))

	var freeSpace resource.Quantity
	switch llv.Spec.Type {
	case Thick:
		freeSpace = getFreeVGSpace(lvg)
	case Thin:
		freeSpace, err = getFreeThinPoolSpace(lvg.Status.ThinPools, llv.Spec.Thin.PoolName)
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLLVCreateFunc] unable to count free thin pool %s space of the LVMLogicalVolume %s", llv.Spec.Thin.PoolName, llv.Name))
			updErr := updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, llv, failedStatusPhase, err.Error())
			if updErr != nil {
				log.Error(updErr, fmt.Sprintf("[reconcileLLVCreateFunc] unable to update the LVMLogicalVolume, name: %s", llv.Name))
				return true, updErr
			}

			return false, err
		}
	}
	log.Trace(fmt.Sprintf("[reconcileLLVCreateFunc] the LVMLogicalVolume %s, LV: %s, VG: %s type: %s requested size: %s, free space: %s", llv.Name, llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode, llv.Spec.Type, llv.Spec.Size.String(), freeSpace.String()))

	if freeSpace.Value() < llv.Spec.Size.Value()+delta.Value() {
		err = errors.New("not enough space")
		log.Error(err, fmt.Sprintf("[reconcileLLVCreateFunc] the LV %s requested size %s of the LVMLogicalVolume %s is more than the actual free space %s", llv.Spec.ActualLVNameOnTheNode, llv.Spec.Size.String(), llv.Name, freeSpace.String()))
		updErr := updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, llv, failedStatusPhase, err.Error())
		if updErr != nil {
			log.Error(updErr, fmt.Sprintf("[reconcileLLVCreateFunc] unable to update the LVMLogicalVolume %s", llv.Name))
			return true, updErr
		}

		// we return true cause the user might manage LVMVolumeGroup free space without changing the LLV
		return true, err
	}

	switch llv.Spec.Type {
	case Thick:
		log.Debug(fmt.Sprintf("[reconcileLLVCreateFunc] LV %s will be created in VG %s with size: %s", llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode, llv.Spec.Size.String()))
		cmd, err := utils.CreateThickLogicalVolume(lvg.Spec.ActualVGNameOnTheNode, llv.Spec.ActualLVNameOnTheNode, llv.Spec.Size.Value(), isContiguous(llv))
		log.Debug(fmt.Sprintf("[reconcileLLVCreateFunc] runs cmd: %s", cmd))
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLLVCreateFunc] unable to create a thick LogicalVolume for the LVMLogicalVolume %s", llv.Name))
			updErr := updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, llv, failedStatusPhase, err.Error())
			if updErr != nil {
				log.Error(updErr, fmt.Sprintf("[reconcileLLVCreateFunc] unable to update the LVMLogicalVolume, name: %s", llv.Name))
				return true, updErr
			}
			return true, err
		}
	case Thin:
		log.Debug(fmt.Sprintf("[reconcileLLVCreateFunc] LV %s of the LVMLogicalVolume %s will be create in Thin-pool %s with size %s", llv.Spec.ActualLVNameOnTheNode, llv.Name, llv.Spec.Thin.PoolName, llv.Spec.Size.String()))
		cmd, err := utils.CreateThinLogicalVolume(lvg.Spec.ActualVGNameOnTheNode, llv.Spec.Thin.PoolName, llv.Spec.ActualLVNameOnTheNode, llv.Spec.Size.Value())
		log.Debug(fmt.Sprintf("[reconcileLLVCreateFunc] runs cmd: %s", cmd))
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLLVCreateFunc] unable to create Thin LogicalVolume %s for the LVMLogicalVolume %s", llv.Spec.ActualLVNameOnTheNode, llv.Name))
			updErr := updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, llv, failedStatusPhase, fmt.Sprintf("Unable to create Thin LV, err: %s", err.Error()))
			if updErr != nil {
				log.Error(updErr, fmt.Sprintf("[reconcileLLVCreateFunc] unable to update the LVMLogicalVolume, name: %s", llv.Name))
				return true, updErr
			}

			return true, err
		}
	}

	log.Info(fmt.Sprintf("[reconcileLLVCreateFunc] successfully created LV %s in VG %s for LVMLogicalVolume resource with name: %s", llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode, llv.Name))

	log.Debug(fmt.Sprintf("[reconcileLLVCreateFunc] tries to get the LV %s actual size", llv.Spec.ActualLVNameOnTheNode))
	actualSize, err := getLVActualSize(log, lvg.Spec.ActualVGNameOnTheNode, llv.Spec.ActualLVNameOnTheNode)
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLLVCreateFunc] unable to get actual size for LV %s in VG %s", llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode))
		updErr := updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, llv, failedStatusPhase, fmt.Sprintf("Unable to get actual LV size, LV name: %s, VG name: %s, err: %s", llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode, err.Error()))
		if updErr != nil {
			log.Error(updErr, fmt.Sprintf("[reconcileLLVCreateFunc] unable to update the LVMLogicalVolume, name: %s", llv.Name))
			return true, updErr
		}

		return true, err
	}
	log.Debug(fmt.Sprintf("[reconcileLLVCreateFunc] successfully got the LV %s actual size", llv.Spec.ActualLVNameOnTheNode))
	log.Trace(fmt.Sprintf("[reconcileLLVCreateFunc] the LV %s in VG: %s has actual size: %s", llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode, actualSize.String()))

	llv.Status.Phase = createdStatusPhase
	llv.Status.ActualSize = actualSize
	if llv.Spec.Thick != nil {
		if *llv.Spec.Thick.Contiguous == false {
			llv.Status.Contiguous = nil
		} else {
			llv.Status.Contiguous = llv.Spec.Thick.Contiguous
		}
	}
	log.Trace(fmt.Sprintf("[reconcileLLVCreateFunc] the LVMLogicalVolume %s status.phase set to %s and actual size to %+v", llv.Name, createdStatusPhase, actualSize))
	err = cl.Status().Update(ctx, llv)
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLLVCreateFunc] unable to update the LVMLogicalVolume, name: %s", llv.Name))
		return true, err
	}

	log.Info(fmt.Sprintf("[reconcileLLVCreateFunc] successfully ended the reconciliation for the LVMLogicalVolume %s", llv.Name))
	return false, nil
}

func reconcileLLVUpdateFunc(
	ctx context.Context,
	cl client.Client,
	log logger.Logger,
	metrics monitoring.Metrics,
	llv *v1alpha1.LVMLogicalVolume,
	lvg *v1alpha1.LvmVolumeGroup,
) (bool, error) {
	log.Debug(fmt.Sprintf("[reconcileLLVUpdateFunc] starts reconciliation for the LVMLogicalVolume %s", llv.Name))

	// this check prevents infinite resource updates after retry
	if llv.Status.Phase != Failed {
		err := updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, llv, resizingStatusPhase, "")
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLLVUpdateFunc] unable to update the LVMLogicalVolume, name: %s", llv.Name))
			return true, err
		}
	}

	extendingSize := subtractQuantity(llv.Spec.Size, llv.Status.ActualSize)
	log.Trace(fmt.Sprintf("[reconcileLLVUpdateFunc] the LV %s in VG %s has extending size %d", llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode, extendingSize.Value()))
	log.Debug(fmt.Sprintf("[reconcileLLVUpdateFunc] tries to parse the resize delta: %s", internal.ResizeDelta))
	delta, err := resource.ParseQuantity(internal.ResizeDelta)
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLLVUpdateFunc] unable to parse the resize delta, value: %s", internal.ResizeDelta))
		updErr := updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, llv, failedStatusPhase, fmt.Sprintf("Unable to parse the resize delta, value: %s, err: %s", internal.ResizeDelta, err.Error()))
		if updErr != nil {
			log.Error(updErr, fmt.Sprintf("[reconcileLLVUpdateFunc] unable to update the LVMLogicalVolume %s", llv.Name))
			return true, updErr
		}

		return true, err
	}
	log.Debug(fmt.Sprintf("[reconcileLLVUpdateFunc] successfully parsed the resize delta: %s", internal.ResizeDelta))

	var freeSpace resource.Quantity
	switch llv.Spec.Type {
	case Thick:
		freeSpace = getFreeVGSpace(lvg)
	case Thin:
		freeSpace, err = getFreeThinPoolSpace(lvg.Status.ThinPools, llv.Spec.Thin.PoolName)
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLLVUpdateFunc] unable to count free space in Thin-pool %s of the LVMLogicalVolume %s", llv.Spec.Thin.PoolName, llv.Name))
			updErr := updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, llv, failedStatusPhase, err.Error())
			if updErr != nil {
				log.Error(updErr, fmt.Sprintf("[reconcileLLVUpdateFunc] unable to update the LVMLogicalVolume %s", llv.Name))
				return true, updErr
			}

			// returns false, cause the error might be occurred cause of string parsing (so the string has to be changed that goes to another update)
			return false, err
		}
	}

	log.Trace(fmt.Sprintf("[reconcileLLVUpdateFunc] the LVMLogicalVolume %s, LV: %s, VG: %s, type: %s, extending size: %s, free space: %s", llv.Name, llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode, llv.Spec.Type, extendingSize.String(), freeSpace.String()))
	if freeSpace.Value() < extendingSize.Value()+delta.Value() {
		err = errors.New("not enough space")
		log.Error(err, fmt.Sprintf("[reconcileLLVUpdateFunc] the LV %s requested size %s of the LVMLogicalVolume %s is more than actual free space %s", llv.Spec.ActualLVNameOnTheNode, llv.Spec.Size.String(), llv.Name, freeSpace.String()))
		updErr := updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, llv, failedStatusPhase, err.Error())
		if updErr != nil {
			log.Error(updErr, fmt.Sprintf("[reconcileLLVUpdateFunc] unable to update the LVMLogicalVolume %s", llv.Name))
			return true, updErr
		}

		// returns true cause a user might manage LVG free space without changing the LLV
		return true, err
	}

	log.Debug(fmt.Sprintf("[reconcileLLVUpdateFunc] tries to get LVMLogicalVolume %s actual size before the extension", llv.Name))
	actualSize, err := getLVActualSize(log, lvg.Spec.ActualVGNameOnTheNode, llv.Spec.ActualLVNameOnTheNode)
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLLVUpdateFunc] unable to get actual size for LV %s from VG %s", llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode))
		updErr := updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, llv, failedStatusPhase, fmt.Sprintf("Unable to get actual LV size, LV name: %s, VG name: %s, err: %s", llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode, err.Error()))
		if updErr != nil {
			log.Error(updErr, fmt.Sprintf("[reconcileLLVUpdateFunc] unable to update the LVMLogicalVolume %s", llv.Name))
			return true, updErr
		}
		return true, err
	}
	log.Debug(fmt.Sprintf("[reconcileLLVUpdateFunc] successfully got LVMLogicalVolume %s actual size before the extension", llv.Name))

	if utils.AreSizesEqualWithinDelta(actualSize, llv.Spec.Size, delta) {
		log.Warning(fmt.Sprintf("[reconcileLLVUpdateFunc] the LV %s in VG %s has the same actual size %s as the requested size %s", llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode, actualSize.String(), llv.Spec.Size.String()))
		llv.Status.Phase = createdStatusPhase
		llv.Status.ActualSize = actualSize
		log.Trace(fmt.Sprintf("[reconcileLLVUpdateFunc] the LVMLogicalVolume %s status.phase set to %s and actual size to %s", llv.Name, createdStatusPhase, actualSize.String()))
		err = cl.Status().Update(ctx, llv)
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLLVUpdateFunc] unable to update the LVMLogicalVolume %s", llv.Name))
			return true, err
		}

		return false, nil
	}

	log.Debug(fmt.Sprintf("[reconcileLLVUpdateFunc] LV %s of the LVMLogicalVolume %s will be extended with size: %s", llv.Spec.ActualLVNameOnTheNode, llv.Name, llv.Spec.Size.String()))
	cmd, err := utils.ExtendLV(llv.Spec.Size.Value(), lvg.Spec.ActualVGNameOnTheNode, llv.Spec.ActualLVNameOnTheNode)
	log.Debug(fmt.Sprintf("[reconcileLLVUpdateFunc] runs cmd: %s", cmd))
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLLVUpdateFunc] unable to ExtendLV, name: %s, type: %s", llv.Spec.ActualLVNameOnTheNode, llv.Spec.Type))
		updErr := updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, llv, failedStatusPhase, err.Error())
		if updErr != nil {
			log.Error(updErr, fmt.Sprintf("[reconcileLLVUpdateFunc] unable to update the LVMLogicalVolume %s", llv.Name))
			return true, updErr
		}

		return true, err
	}

	log.Info(fmt.Sprintf("[reconcileLLVUpdateFunc] successfully extended LV %s in VG %s for LVMLogicalVolume resource with name: %s", llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode, llv.Name))

	log.Debug(fmt.Sprintf("[reconcileLLVUpdateFunc] tries to get LVMLogicalVolume %s actual size after the extension", llv.Name))
	newActualSize, err := getLVActualSize(log, lvg.Spec.ActualVGNameOnTheNode, llv.Spec.ActualLVNameOnTheNode)
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLLVUpdateFunc] unable to get actual size for LV %s in VG %s", llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode))
		updErr := updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, llv, failedStatusPhase, fmt.Sprintf("Unable to get LV actual size, err: %s", err.Error()))
		if updErr != nil {
			log.Error(updErr, fmt.Sprintf("[reconcileLLVUpdateFunc] unable to update the LVMLogicalVolume %s", llv.Name))
			return true, updErr
		}

		return true, err
	}
	log.Debug(fmt.Sprintf("[reconcileLLVUpdateFunc] successfully got LVMLogicalVolume %s actual size before the extension", llv.Name))
	log.Trace(fmt.Sprintf("[reconcileLLVUpdateFunc] the LV %s in VG %s actual size %s", llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode, newActualSize.String()))

	llv.Status.Phase = createdStatusPhase
	llv.Status.ActualSize = newActualSize
	log.Trace(fmt.Sprintf("[reconcileLLVUpdateFunc] the LVMLogicalVolume %s status.phase set to %s and actual size to %+v", llv.Name, createdStatusPhase, newActualSize))
	err = cl.Status().Update(ctx, llv)
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLLVUpdateFunc] unable to update LVMLogicalVolume %s", llv.Name))
		return true, err
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
	lvg *v1alpha1.LvmVolumeGroup,
) (bool, error) {
	log.Debug(fmt.Sprintf("[reconcileLLVDeleteFunc] starts reconciliation for the LVMLogicalVolume %s", llv.Name))

	// The controller won't remove the LLV resource and LV volume till the resource has any other finalizer.
	if len(llv.Finalizers) != 1 ||
		llv.Finalizers[0] != internal.SdsNodeConfiguratorFinalizer {
		log.Debug(fmt.Sprintf("[reconcileLLVDeleteFunc] unable to delete LVMLogicalVolume %s for now due to it has any other finalizer", llv.Name))
		return false, nil
	}

	// TODO: добавить в статус isContiguous nill-able
	err := deleteLVIfExists(log, sdsCache, lvg.Spec.ActualVGNameOnTheNode, llv)
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLLVDeleteFunc] unable to delete the LV %s in VG %s", llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode))
		updErr := updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, llv, failedStatusPhase, fmt.Sprintf("Unable to delete the the LV %s in VG %s, err: %s", llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode, err.Error()))
		if updErr != nil {
			log.Error(updErr, fmt.Sprintf("[reconcileLLVDeleteFunc] unable to update the LVMLogicalVolume %s", llv.Name))
			return true, updErr
		}
		return true, err
	}

	log.Info(fmt.Sprintf("[reconcileLLVDeleteFunc] successfully deleted the LV %s in VG %s", llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode))

	err = removeLLVFinalizersIfExist(ctx, cl, metrics, log, llv)
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLLVDeleteFunc] unable to remove finalizers from the LVMVolumeGroup %s", llv.Name))
		updErr := updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, llv, failedStatusPhase, fmt.Sprintf("Unable to remove finalizer %s, err: %s", internal.SdsNodeConfiguratorFinalizer, err.Error()))
		if updErr != nil {
			log.Error(updErr, fmt.Sprintf("[reconcileLLVDeleteFunc] unable to update the LVMLogicalVolume %s", llv.Name))
			return true, updErr
		}
	}

	log.Info(fmt.Sprintf("[reconcileLLVDeleteFunc] successfully ended reconciliation for the LVMLogicalVolume %s", llv.Name))
	return false, nil
}
