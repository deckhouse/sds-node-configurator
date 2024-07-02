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

	StatusPhaseCreated  = "Created"
	StatusPhasePending  = "Pending"
	StatusPhaseResizing = "Resizing"
	StatusPhaseFailed   = "Failed"
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
					log.Error(err, fmt.Sprintf("[ReconcileLVMLogicalVolume] LVMVolumeGroup %s not found for LVMLogicalVolume %s. Retry in %s", llv.Spec.LvmVolumeGroupName, llv.Name, cfg.VolumeGroupScanIntervalSec.String()))
					err = updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, llv, StatusPhaseFailed, fmt.Sprintf("LVMVolumeGroup %s not found", llv.Spec.LvmVolumeGroupName))
					if err != nil {
						log.Error(err, fmt.Sprintf("[ReconcileLVMLogicalVolume] unable to update the LVMLogicalVolume %s", llv.Name))
						return reconcile.Result{}, err
					}

					return reconcile.Result{
						RequeueAfter: cfg.VolumeGroupScanIntervalSec,
					}, nil
				}

				err = updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, llv, StatusPhaseFailed, fmt.Sprintf("Unable to get selected LVMVolumeGroup, err: %s", err.Error()))
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
				err = updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, llv, StatusPhaseFailed, reason)
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
				updErr := updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, llv, StatusPhaseFailed, err.Error())
				if updErr != nil {
					log.Error(updErr, fmt.Sprintf("[RunLVMLogicalVolumeWatcherController] unable to update the LVMLogicalVolume %s", llv.Name))
					return reconcile.Result{}, updErr
				}
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
		return reconcileLLVCreateFunc(ctx, cl, log, metrics, sdsCache, llv, lvg)
	case UpdateReconcile:
		return reconcileLLVUpdateFunc(ctx, cl, log, metrics, sdsCache, llv, lvg)
	case DeleteReconcile:
		return reconcileLLVDeleteFunc(ctx, cl, log, metrics, sdsCache, llv, lvg)
	default:
		log.Info(fmt.Sprintf("[runEventReconcile] the LVMLogicalVolume %s has compeleted configuration and should not be reconciled", llv.Name))
		if llv.Status.Phase != StatusPhaseCreated {
			log.Warning(fmt.Sprintf("[runEventReconcile] the LVMLogicalVolume %s should not be reconciled but has an unexpected phase: %s. Setting the phase to %s", llv.Name, llv.Status.Phase, StatusPhaseCreated))
			err := updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, llv, StatusPhaseCreated, "")
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
	lvg *v1alpha1.LvmVolumeGroup,
) (bool, error) {
	log.Debug(fmt.Sprintf("[reconcileLLVCreateFunc] starts reconciliation for the LVMLogicalVolume %s", llv.Name))

	// this check prevents infinite resource updating after retries
	if llv.Status == nil {
		err := updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, llv, StatusPhasePending, "")
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLLVCreateFunc] unable to update the LVMLogicalVolume %s", llv.Name))
			return true, err
		}
	}

	freeSpace := getFreeLVGSpaceForLLV(lvg, llv)
	log.Trace(fmt.Sprintf("[reconcileLLVCreateFunc] the LVMLogicalVolume %s, LV: %s, VG: %s type: %s requested size: %s, free space: %s", llv.Name, llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode, llv.Spec.Type, llv.Spec.Size.String(), freeSpace.String()))

	log.Debug(fmt.Sprintf("[reconcileLLVCreateFunc] tries to parse the resize delta: %s", internal.ResizeDelta))
	delta, err := resource.ParseQuantity(internal.ResizeDelta)
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLLVCreateFunc] unable to parse the resize delta, value: %s", internal.ResizeDelta))
		return true, err
	}
	log.Debug(fmt.Sprintf("[reconcileLLVCreateFunc] successfully parsed the resize delta: %s", internal.ResizeDelta))

	if freeSpace.Value() < llv.Spec.Size.Value()+delta.Value() {
		err = errors.New("not enough space")
		log.Error(err, fmt.Sprintf("[reconcileLLVCreateFunc] the LV %s requested size %s of the LVMLogicalVolume %s is more than the actual free space %s", llv.Spec.ActualLVNameOnTheNode, llv.Spec.Size.String(), llv.Name, freeSpace.String()))

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
			return true, err
		}
	case Thin:
		log.Debug(fmt.Sprintf("[reconcileLLVCreateFunc] LV %s of the LVMLogicalVolume %s will be create in Thin-pool %s with size %s", llv.Spec.ActualLVNameOnTheNode, llv.Name, llv.Spec.Thin.PoolName, llv.Spec.Size.String()))
		cmd, err := utils.CreateThinLogicalVolume(lvg.Spec.ActualVGNameOnTheNode, llv.Spec.Thin.PoolName, llv.Spec.ActualLVNameOnTheNode, llv.Spec.Size.Value())
		log.Debug(fmt.Sprintf("[reconcileLLVCreateFunc] runs cmd: %s", cmd))
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLLVCreateFunc] unable to create Thin LogicalVolume %s for the LVMLogicalVolume %s", llv.Spec.ActualLVNameOnTheNode, llv.Name))
			return true, err
		}
	}

	log.Info(fmt.Sprintf("[reconcileLLVCreateFunc] successfully created LV %s in VG %s for LVMLogicalVolume resource with name: %s", llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode, llv.Name))

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
	lvg *v1alpha1.LvmVolumeGroup,
) (bool, error) {
	log.Debug(fmt.Sprintf("[reconcileLLVUpdateFunc] starts reconciliation for the LVMLogicalVolume %s", llv.Name))

	// status might be nil if a user creates the resource with LV name which matches existing LV on the node
	if llv.Status == nil {
		err := updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, llv, StatusPhasePending, "")
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLLVUpdateFunc] unable to update the LVMLogicalVolume %s", llv.Name))
			return true, err
		}
	}

	// it needs to get current LV size from the node as status might be nil
	log.Debug(fmt.Sprintf("[reconcileLLVUpdateFunc] tries to get LVMLogicalVolume %s actual size before the extension", llv.Name))
	actualSize := getLVActualSize(sdsCache, lvg.Spec.ActualVGNameOnTheNode, llv.Spec.ActualLVNameOnTheNode)
	if actualSize.Value() == 0 {
		err := fmt.Errorf("LV %s has zero size (likely LV was not found in the cache)", llv.Spec.ActualLVNameOnTheNode)
		log.Error(err, fmt.Sprintf("[reconcileLLVUpdateFunc] unable to get actual LV %s size of the LVMLogicalVolume %s", llv.Spec.ActualLVNameOnTheNode, llv.Name))
		return true, err
	}
	log.Debug(fmt.Sprintf("[reconcileLLVUpdateFunc] successfully got LVMLogicalVolume %s actual size %s before the extension", llv.Name, actualSize.String()))

	log.Debug(fmt.Sprintf("[reconcileLLVUpdateFunc] tries to parse the resize delta: %s", internal.ResizeDelta))
	delta, err := resource.ParseQuantity(internal.ResizeDelta)
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLLVUpdateFunc] unable to parse the resize delta, value: %s", internal.ResizeDelta))
		return true, err
	}
	log.Debug(fmt.Sprintf("[reconcileLLVUpdateFunc] successfully parsed the resize delta: %s", internal.ResizeDelta))

	if utils.AreSizesEqualWithinDelta(actualSize, llv.Spec.Size, delta) {
		log.Warning(fmt.Sprintf("[reconcileLLVUpdateFunc] the LV %s in VG %s has the same actual size %s as the requested size %s", llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode, actualSize.String(), llv.Spec.Size.String()))

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

	extendingSize := subtractQuantity(llv.Spec.Size, actualSize)
	log.Trace(fmt.Sprintf("[reconcileLLVUpdateFunc] the LV %s in VG %s has extending size %s", llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode, extendingSize.String()))
	if extendingSize.Value() < 0 {
		err = fmt.Errorf("specified LV size %dB is less than actual one on the node %dB", llv.Spec.Size.Value(), actualSize.Value())
		log.Error(err, fmt.Sprintf("[reconcileLLVUpdateFunc] unable to extend the LVMLogicalVolume %s", llv.Name))
		return false, err
	}

	log.Info(fmt.Sprintf("[reconcileLLVUpdateFunc] the LVMLogicalVolume %s should be resized", llv.Name))
	// this check prevents infinite resource updates after retry
	if llv.Status.Phase != Failed {
		err := updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, llv, StatusPhaseResizing, "")
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLLVUpdateFunc] unable to update the LVMLogicalVolume %s", llv.Name))
			return true, err
		}
	}

	freeSpace := getFreeLVGSpaceForLLV(lvg, llv)
	log.Trace(fmt.Sprintf("[reconcileLLVUpdateFunc] the LVMLogicalVolume %s, LV: %s, VG: %s, type: %s, extending size: %s, free space: %s", llv.Name, llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode, llv.Spec.Type, extendingSize.String(), freeSpace.String()))
	if freeSpace.Value() < extendingSize.Value()+delta.Value() {
		err = errors.New("not enough space")
		log.Error(err, fmt.Sprintf("[reconcileLLVUpdateFunc] the LV %s requested size %s of the LVMLogicalVolume %s is more than actual free space %s", llv.Spec.ActualLVNameOnTheNode, llv.Spec.Size.String(), llv.Name, freeSpace.String()))

		// returns true cause a user might manage LVG free space without changing the LLV
		return true, err
	}

	log.Debug(fmt.Sprintf("[reconcileLLVUpdateFunc] LV %s of the LVMLogicalVolume %s will be extended with size: %s", llv.Spec.ActualLVNameOnTheNode, llv.Name, llv.Spec.Size.String()))
	cmd, err := utils.ExtendLV(llv.Spec.Size.Value(), lvg.Spec.ActualVGNameOnTheNode, llv.Spec.ActualLVNameOnTheNode)
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
	lvg *v1alpha1.LvmVolumeGroup,
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
