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
) (controller.Controller, error) {
	cl := mgr.GetClient()
	cache := mgr.GetCache()

	c, err := controller.New(lvmLogicalVolumeWatcherCtrlName, mgr, controller.Options{
		Reconciler: reconcile.Func(func(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
			log.Debug(fmt.Sprintf("[RunLVMLogicalVolumeWatcherController] Reconciler starts reconciliation of LLV: %s", request.Name))
			shouldRequeue, err := ReconcileLVMLogicalVolume(ctx, cl, log, metrics, cfg, request)
			if err != nil {
				log.Error(err, fmt.Sprintf("[RunLVMLogicalVolumeWatcherController] an error occurred while reconciling LLV: %s", request.Name))
			}
			if shouldRequeue {
				log.Info(fmt.Sprintf("[RunLVMLogicalVolumeWatcherController] requeue reconciliation of LLV: %s after %ss", request.Name, cfg.LLVRequeueIntervalSec))
				return reconcile.Result{RequeueAfter: cfg.LLVRequeueIntervalSec}, nil
			}
			log.Debug(fmt.Sprintf("[RunLVMLogicalVolumeWatcherController] ends reconciliation of LLV: %s without requeue", request.Name))
			return reconcile.Result{Requeue: false}, nil
		}),
		MaxConcurrentReconciles: 10,
	})

	if err != nil {
		log.Error(err, "[RunLVMLogicalVolumeWatcherController] unable to create controller")
		return nil, err
	}

	err = c.Watch(source.Kind(cache, &v1alpha1.LVMLogicalVolume{}), handler.Funcs{
		CreateFunc: func(ctx context.Context, e event.CreateEvent, q workqueue.RateLimitingInterface) {
			log.Debug(fmt.Sprintf("[RunLVMLogicalVolumeWatcherController] CreateFunc starts reconciliation of LLV: %s", e.Object.GetName()))
			request := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: e.Object.GetNamespace(), Name: e.Object.GetName()}}

			q.Add(request)
			log.Debug(fmt.Sprintf("[RunLVMLogicalVolumeWatcherController] CreateFunc ends reconciliation of LLV: %s", e.Object.GetName()))
		},

		UpdateFunc: func(ctx context.Context, e event.UpdateEvent, q workqueue.RateLimitingInterface) {
			log.Debug(fmt.Sprintf("[RunLVMLogicalVolumeWatcherController] UpdateFunc starts reconciliation of LLV: %s", e.ObjectNew.GetName()))

			oldLLV, ok := e.ObjectOld.(*v1alpha1.LVMLogicalVolume)
			if !ok {
				err = errors.New("unable to cast event object to a given type")
				log.Error(err, "[UpdateFunc] an error occurs while handling update event")
				return
			}
			log.Trace("[RunLVMLogicalVolumeWatcherController] UpdateFunc get old LVMLogicalVolume: ", oldLLV.Name, oldLLV)

			newLLV, ok := e.ObjectNew.(*v1alpha1.LVMLogicalVolume)
			if !ok {
				err = errors.New("unable to cast event object to a given type")
				log.Error(err, "[UpdateFunc] an error occurs while handling update event")
				return
			}
			log.Trace("[RunLVMLogicalVolumeWatcherController] UpdateFunc get new LVMLogicalVolume: ", newLLV.Name, newLLV)

			// TODO: Figure out how to log it in our logger.
			if cfg.Loglevel == "4" {
				fmt.Println("==============START DIFF==================")
				fmt.Println(cmp.Diff(oldLLV, newLLV))
				fmt.Println("==============END DIFF==================")
			}

			if reflect.DeepEqual(oldLLV.Spec, newLLV.Spec) && newLLV.DeletionTimestamp == nil {
				log.Debug(fmt.Sprintf("[UpdateFunc] the LVMLogicalVolume %s has not been changed", newLLV.Name))
				log.Debug(fmt.Sprintf("[RunLVMLogicalVolumeWatcherController] UpdateFunc ends reconciliation of LLV: %s", newLLV.Name))
				return
			}
			log.Debug(fmt.Sprintf("[UpdateFunc] the LVMLogicalVolume %s has been changed. Add to the queue", newLLV.Name))

			request := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: newLLV.Namespace, Name: newLLV.Name}}
			q.Add(request)
			log.Debug(fmt.Sprintf("[RunLVMLogicalVolumeWatcherController] UpdateFunc ends reconciliation of LLV: %s", newLLV.Name))
		},
	})

	if err != nil {
		log.Error(err, "[RunLVMLogicalVolumeWatcherController] the controller is unable to watch")
		return nil, err
	}

	return c, err
}

func ReconcileLVMLogicalVolume(ctx context.Context, cl client.Client, log logger.Logger, metrics monitoring.Metrics, cfg config.Options, request reconcile.Request) (bool, error) {
	llv := &v1alpha1.LVMLogicalVolume{}
	err := cl.Get(ctx, request.NamespacedName, llv)
	if err != nil {
		if k8serr.IsNotFound(err) {
			log.Debug(fmt.Sprintf("[ReconcileLVMLogicalVolume] LVMLogicalVolume %s not found. Object has probably been deleted.", request.NamespacedName))
			return false, nil
		}
		return true, fmt.Errorf("[ReconcileLVMLogicalVolume] unable to get LVMLogicalVolume: %w", err)
	}

	lvg, err := getLVMVolumeGroup(ctx, cl, metrics, llv.Spec.LvmVolumeGroupName)
	if err != nil {
		if k8serr.IsNotFound(err) {
			log.Debug(fmt.Sprintf("[ReconcileLVMLogicalVolume] LVMVolumeGroup %s not found for LVMLogicalVolume %s", llv.Spec.LvmVolumeGroupName, llv.Name))
			err = updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, llv, failedStatusPhase, fmt.Sprintf("Unable to get selected LVMVolumeGroup %s, err: %s", llv.Spec.LvmVolumeGroupName, err.Error()))
			if err != nil {
				return true, fmt.Errorf("[ReconcileLVMLogicalVolume] unable to update the LVMLogicalVolume %s status.phase to %s: %w", llv.Name, failedStatusPhase, err)
			}
			return true, nil
		}
		reconcileErr := fmt.Errorf("[ReconcileLVMLogicalVolume] unable to get LVMVolumeGroup %s: %w", llv.Spec.LvmVolumeGroupName, err)
		updateErr := updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, llv, failedStatusPhase, fmt.Sprintf("Unable to get selected LVMVolumeGroup, err: %s", err.Error()))
		if updateErr != nil {
			return true, fmt.Errorf("%s. Also failed to update LVMLogicalVolume %s status.phase to %s: %w", reconcileErr, llv.Name, failedStatusPhase, updateErr)
		}
		return false, reconcileErr
	}

	if !belongsToNode(lvg, cfg.NodeName) {
		log.Debug(fmt.Sprintf("[ReconcileLVMLogicalVolume] the LVMVolumeGroup %s does not belongs to the current node: %s. Reconciliation stopped", lvg.Name, cfg.NodeName))
		return false, nil
	}

	log.Info(fmt.Sprintf("[ReconcileLVMLogicalVolume] the LVMVolumeGroup %s belongs to the current node: %s. Reconciliation continues", lvg.Name, cfg.NodeName))
	log.Debug(fmt.Sprintf("[ReconcileLVMLogicalVolume] tries to validate the LVMLogicalVolume %s", llv.Name))
	valid, reason := validateLVMLogicalVolume(ctx, cl, llv)
	if !valid {
		log.Warning(fmt.Sprintf("[ReconcileLVMLogicalVolume] the LVMLogicalVolume %s is not valid, reason: %s", llv.Name, reason))
		err = updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, llv, failedStatusPhase, reason)
		if err != nil {
			log.Error(err, fmt.Sprintf("[ReconcileLVMLogicalVolume] unable to update the LVMLogicalVolume %s", llv.Name))
		}
		return true, err
	}
	log.Debug(fmt.Sprintf("[ReconcileLVMLogicalVolume] successfully validated the LVMLogicalVolume %s", llv.Name))

	log.Debug("[ReconcileLVMLogicalVolume] Identify reconcile func. vgName: "+lvg.Spec.ActualVGNameOnTheNode+", llv:", llv.Name, llv)

	recType, err := identifyReconcileFunc(log, lvg.Spec.ActualVGNameOnTheNode, llv)
	if err != nil {
		reconcileErr := fmt.Errorf("[ReconcileLVMLogicalVolume] unable to identify the reconcile func: %w", err)
		updateErr := updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, llv, failedStatusPhase, fmt.Sprintf("An error occurred while identifying the reconcile func, err: %s", err.Error()))
		if updateErr != nil {
			return true, fmt.Errorf("%s. Also failed to update LVMLogicalVolume %s status.phase to %s: %w", reconcileErr, llv.Name, failedStatusPhase, updateErr)
		}
		return false, reconcileErr
	}

	switch recType {
	case CreateReconcile:
		log.Debug(fmt.Sprintf("[runEventReconcile] CreateReconcile starts reconciliation for the LVMLogicalVolume: %s", llv.Name))
		return reconcileLLVCreateFunc(ctx, cl, log, metrics, llv, lvg)
	case UpdateReconcile:
		log.Debug(fmt.Sprintf("[runEventReconcile] UpdateReconcile starts reconciliation for the LVMLogicalVolume: %s", llv.Name))
		return reconcileLLVUpdateFunc(ctx, cl, log, metrics, llv, lvg)
	case DeleteReconcile:
		log.Debug(fmt.Sprintf("[runEventReconcile] DeleteReconcile starts reconciliation for the LVMLogicalVolume: %s", llv.Name))
		return reconcileLLVDeleteFunc(ctx, cl, log, metrics, llv, lvg)
	default:
		log.Debug(fmt.Sprintf("[runEventReconcile] the LVMLogicalVolume %s should not be reconciled", llv.Name))
		if llv.Status.Phase != createdStatusPhase {
			log.Warning(fmt.Sprintf("[runEventReconcile] the LVMLogicalVolume %s should not be reconciled but has an unexpected phase: %s. Setting the phase to %s", llv.Name, llv.Status.Phase, createdStatusPhase))
			err = updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, llv, createdStatusPhase, "")
			if err != nil {
				return true, fmt.Errorf("[runEventReconcile] unable to update the LVMLogicalVolume %s status.phase to %s: %w", llv.Name, createdStatusPhase, err)
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
	log.Info(fmt.Sprintf("[reconcileLLVCreateFunc] starts reconciliation for the LVMLogicalVolume %s", llv.Name))

	err := updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, llv, pendingStatusPhase, "")
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLLVCreateFunc] unable to update the LVMLogicalVolume %s", llv.Name))
		return true, err
	}

	vgName := lvg.Spec.ActualVGNameOnTheNode
	lvName := llv.Spec.ActualLVNameOnTheNode
	added, err := addLLVFinalizerIfNotExist(ctx, cl, log, metrics, llv)
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLLVCreateFunc] unable to update the LVMLogicalVolume %s", llv.Name))
		return true, err
	}
	log.Debug(fmt.Sprintf("[reconcileLLVCreateFunc] a finalizer to the LVMLogicalVolume %s was added: %t", llv.Name, added))

	switch llv.Spec.Type {
	case Thick:
		freeSpace := getFreeVGSpace(lvg)

		log.Trace(fmt.Sprintf("[reconcileLLVCreateFunc] the LVMLogicalVolume %s, LV: %s, VG: %s type: %s requested size: %d, free size: %d", llv.Name, lvName, vgName, llv.Spec.Type, llv.Spec.Size.Value(), freeSpace.Value()))
		if freeSpace.Value() < llv.Spec.Size.Value() {
			err = errors.New("not enough space")
			log.Error(err, fmt.Sprintf("[reconcileLLVCreateFunc] the LV %s requested size is more than the VG %s free space", lvName, vgName))
			updErr := updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, llv, failedStatusPhase, "Not enough space in VG")
			if updErr != nil {
				log.Error(updErr, fmt.Sprintf("[reconcileLLVCreateFunc] unable to update the LVMLogicalVolume %s", llv.Name))
				return true, updErr
			}

			// we return true cause the user might manage LVMVolumeGroup free space without changing the LLV
			return true, err
		}

		log.Debug(fmt.Sprintf("[reconcileLLVCreateFunc] LV %s will be create in VG %s with Quantity value: %d", lvName, vgName, llv.Spec.Size.Value()))
		cmd, err := utils.CreateThickLogicalVolume(vgName, lvName, llv.Spec.Size.Value())
		log.Debug(fmt.Sprintf("[reconcileLLVCreateFunc] runs cmd: %s", cmd))
		if err != nil {
			log.Error(err, "[reconcileLLVCreateFunc] unable to create a thick LogicalVolume")
			updErr := updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, llv, failedStatusPhase, fmt.Sprintf("Unable to create Thick LV, err: %s", err.Error()))
			if updErr != nil {
				log.Error(updErr, fmt.Sprintf("[reconcileLLVCreateFunc] unable to update the LVMLogicalVolume, name: %s", llv.Name))
			}
			return true, err
		}
	case Thin:
		freeSpace, err := getFreeThinPoolSpace(lvg.Status.ThinPools, llv.Spec.Thin.PoolName)
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLLVCreateFunc] unable to count free space in LV, name: %s", llv.Spec.Thin.PoolName))
			updErr := updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, llv, failedStatusPhase, fmt.Sprintf("Unable to get free LV space, err: %s", err.Error()))
			if updErr != nil {
				log.Error(updErr, fmt.Sprintf("[reconcileLLVCreateFunc] unable to update the LVMLogicalVolume, name: %s", llv.Name))
				return true, updErr
			}

			return false, err
		}

		log.Trace(fmt.Sprintf("[reconcileLLVCreateFunc] the LVMLogicalVolume %s, type: %s requested size: %d, free size: %d", llv.Name, llv.Spec.Type, llv.Spec.Size.Value(), freeSpace.Value()))
		if freeSpace.Value() < llv.Spec.Size.Value() {
			err = errors.New("not enough space")
			log.Error(err, fmt.Sprintf("[reconcileLLVCreateFunc] the LVMLogicalVolume %s requested size is more than the Thin-pool %s free space", llv.Name, llv.Spec.Thin.PoolName))
			updErr := updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, llv, failedStatusPhase, fmt.Sprintf("Not enough space in Thin-pool %s in LVMVolumeGroup %s", llv.Spec.Thin.PoolName, lvg.Name))
			if updErr != nil {
				log.Error(updErr, fmt.Sprintf("[reconcileLLVCreateFunc] unable to update the LVMLogicalVolume, name: %s", llv.Name))
				return true, updErr
			}

			// we return true cause the user might manage ThinPool free space without changing the LLV
			return true, err
		}

		log.Debug(fmt.Sprintf("[reconcileLLVCreateFunc] LV %s will be create in Thin-pool %s with size %s", lvName, llv.Spec.Thin.PoolName, llv.Spec.Size.String()))
		cmd, err := utils.CreateThinLogicalVolume(vgName, llv.Spec.Thin.PoolName, lvName, llv.Spec.Size.Value())
		log.Debug(fmt.Sprintf("[reconcileLLVCreateFunc] runs cmd: %s", cmd))
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLLVCreateFunc] unable to create Thin LogicalVolume for the LVMLogicalVolume %s", llv.Name))
			updErr := updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, llv, failedStatusPhase, fmt.Sprintf("Unable to create Thin LV, err: %s", err.Error()))
			if updErr != nil {
				log.Error(updErr, fmt.Sprintf("[reconcileLLVCreateFunc] unable to update the LVMLogicalVolume, name: %s", llv.Name))
				return true, updErr
			}

			return true, err
		}
	}

	log.Info(fmt.Sprintf("[reconcileLLVCreateFunc] successfully created LV %s in VG %s for LVMLogicalVolume resource with name: %s", lvName, vgName, llv.Name))

	actualSize, err := getLVActualSize(log, vgName, lvName)
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLLVCreateFunc] unable to get actual size for LV %s in VG %s", lvName, vgName))
		updErr := updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, llv, failedStatusPhase, fmt.Sprintf("Unable to get actual LV size, LV name: %s, VG name: %s, err: %s", lvName, vgName, err.Error()))
		if updErr != nil {
			log.Error(updErr, fmt.Sprintf("[reconcileLLVCreateFunc] unable to update the LVMLogicalVolume, name: %s", llv.Name))
			return true, updErr
		}

		return true, err
	}
	log.Trace(fmt.Sprintf("[reconcileLLVCreateFunc] the LV %s in VG: %s has actual size: %s", lvName, vgName, actualSize.String()))

	if llv.Status == nil {
		llv.Status = new(v1alpha1.LVMLogicalVolumeStatus)
	}
	llv.Status.Phase = createdStatusPhase
	llv.Status.ActualSize = actualSize
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
	log.Info("[reconcileLLVUpdateFunc] starts reconciliation")

	err := updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, llv, resizingStatusPhase, "")
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLLVUpdateFunc] unable to update the LVMLogicalVolume, name: %s", llv.Name))
		return true, err
	}

	vgName := lvg.Spec.ActualVGNameOnTheNode
	lvName := llv.Spec.ActualLVNameOnTheNode
	extendingSize := subtractQuantity(llv.Spec.Size, llv.Status.ActualSize)
	log.Trace(fmt.Sprintf("[reconcileLLVUpdateFunc] the LV %s in VG %s has extending size %d", lvName, vgName, extendingSize.Value()))
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

	switch llv.Spec.Type {
	case Thick:
		freeSpace := getFreeVGSpace(lvg)

		log.Trace(fmt.Sprintf("[reconcileLLVUpdateFunc] the LVMLogicalVolume %s, LV: %s, VG: %s Thick extending size: %d, free size: %d", llv.Name, lvName, vgName, extendingSize.Value(), freeSpace.Value()))
		if freeSpace.Value() < extendingSize.Value()+delta.Value() {
			err = errors.New("not enough space")
			log.Error(err, fmt.Sprintf("[reconcileLLVUpdateFunc] the LV %s requested size is more than the VG %s free space", lvName, vgName))
			updErr := updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, llv, failedStatusPhase, fmt.Sprintf("Not enough space on VG, requested: %d, free: %d", llv.Spec.Size.Value(), freeSpace.Value()))
			if updErr != nil {
				log.Error(updErr, fmt.Sprintf("[reconcileLLVUpdateFunc] unable to update the LVMLogicalVolume %s", llv.Name))
				return true, updErr
			}

			// returns true cause a user might manage LVG free space without changing the LLV
			return true, err
		}

		log.Debug(fmt.Sprintf("[reconcileLLVUpdateFunc] LV %s will be extended in VG %s with Quantity value: %d", lvName, vgName, llv.Spec.Size.Value()))
	case Thin:
		freeSpace, err := getFreeThinPoolSpace(lvg.Status.ThinPools, llv.Spec.Thin.PoolName)
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLLVUpdateFunc] unable to count free space in Thin-pool, name: %s", llv.Spec.Thin.PoolName))
			updErr := updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, llv, failedStatusPhase, fmt.Sprintf("Unable to count free Thin-pool space, err: %s", err.Error()))
			if updErr != nil {
				log.Error(updErr, fmt.Sprintf("[reconcileLLVUpdateFunc] unable to update the LVMLogicalVolume %s", llv.Name))
				return true, updErr
			}

			return false, err
		}

		log.Trace(fmt.Sprintf("[reconcileLLVUpdateFunc] the LVMLogicalVolume %s Thin extending size: %d, free size: %d", llv.Name, extendingSize.Value(), freeSpace.Value()))
		if freeSpace.Value() < extendingSize.Value() {
			err = errors.New("not enough space")
			log.Error(err, fmt.Sprintf("[reconcileLLVUpdateFunc] the LVMLogicalVolume %s requested size is more than the Thin-pool %s free space", llv.Name, llv.Spec.Thin.PoolName))
			updErr := updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, llv, failedStatusPhase, "Not enough space in a Thin-pool")
			if updErr != nil {
				log.Error(updErr, fmt.Sprintf("[reconcileLLVUpdateFunc] unable to update the LVMLogicalVolume %s", llv.Name))
				return true, updErr
			}

			// return true cause a user might manage Thin pool free space without changing the LLV
			return true, err
		}

		log.Debug(fmt.Sprintf("[reconcileLLVUpdateFunc] LV %s will be extended in Thin-pool %s with Quantity value: %d", lvName, llv.Spec.Thin.PoolName, llv.Spec.Size.Value()))
	}

	log.Debug(fmt.Sprintf("[reconcileLLVUpdateFunc] tries to get LVMLogicalVolume %s actual size", llv.Name))
	actualSize, err := getLVActualSize(log, vgName, lvName)
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLLVUpdateFunc] unable to get actual size for LV %s from VG %s", lvName, vgName))
		updErr := updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, llv, failedStatusPhase, fmt.Sprintf("Unable to get actual LV size, LV name: %s, VG name: %s, err: %s", lvName, vgName, err.Error()))
		if updErr != nil {
			log.Error(updErr, fmt.Sprintf("[reconcileLLVUpdateFunc] unable to update the LVMLogicalVolume %s", llv.Name))
			return true, updErr
		}
		return true, err
	}

	if utils.AreSizesEqualWithinDelta(actualSize, llv.Spec.Size, delta) {
		log.Warning(fmt.Sprintf("[reconcileLLVUpdateFunc] the LV %s in VG %s has the same actual size %s as the requested size %s", lvName, vgName, actualSize.String(), llv.Spec.Size.String()))
		llv.Status.Phase = createdStatusPhase
		llv.Status.ActualSize = actualSize
		log.Trace(fmt.Sprintf("[reconcileLLVUpdateFunc] the LVMLogicalVolume %s status.phase set to %s and actual size to %+v", llv.Name, createdStatusPhase, actualSize))
		err = updateLVMLogicalVolume(ctx, metrics, cl, llv)
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLLVUpdateFunc] unable to update the LVMLogicalVolume %s", llv.Name))
			return true, err
		}

		return false, nil
	}

	cmd, err := utils.ExtendLV(llv.Spec.Size.Value(), vgName, lvName)
	log.Debug(fmt.Sprintf("[reconcileLLVUpdateFunc] runs cmd: %s", cmd))
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLLVUpdateFunc] unable to ExtendLV, name: %s, type: %s", lvName, llv.Spec.Type))
		updErr := updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, llv, failedStatusPhase, fmt.Sprintf("Unable to extend %s LV, err: %s", llv.Spec.Type, err.Error()))
		if updErr != nil {
			log.Error(updErr, fmt.Sprintf("[reconcileLLVUpdateFunc] unable to update the LVMLogicalVolume %s", llv.Name))
			return true, updErr
		}

		return true, err
	}

	log.Info(fmt.Sprintf("[reconcileLLVUpdateFunc] successfully extended LV %s in VG %s for LVMLogicalVolume resource with name: %s", lvName, vgName, llv.Name))

	newActualSize, err := getLVActualSize(log, vgName, lvName)
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLLVUpdateFunc] unable to get actual size for LV %s in VG %s", lvName, vgName))
		updErr := updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, llv, failedStatusPhase, fmt.Sprintf("Unable to get LV actual size, err: %s", err.Error()))
		if updErr != nil {
			log.Error(updErr, fmt.Sprintf("[reconcileLLVUpdateFunc] unable to update the LVMLogicalVolume %s", llv.Name))
			return true, updErr
		}

		return true, err
	}
	log.Trace(fmt.Sprintf("[reconcileLLVUpdateFunc] the LV %s in VG %s actual size %s", lvName, vgName, newActualSize.String()))

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
	llv *v1alpha1.LVMLogicalVolume,
	lvg *v1alpha1.LvmVolumeGroup,
) (bool, error) {
	log.Info(fmt.Sprintf("[reconcileLLVDeleteFunc] starts reconciliation for the LVMLogicalVolume %s", llv.Name))

	// The controller won't remove the LLV resource and LV volume till the resource has any other finalizer.
	if len(llv.Finalizers) != 1 ||
		llv.Finalizers[0] != internal.SdsNodeConfiguratorFinalizer {
		log.Debug(fmt.Sprintf("[reconcileLLVDeleteFunc] unable to delete LVMLogicalVolume %s for now due to it has any other finalizer", llv.Name))
		return false, nil
	}

	vgName := lvg.Spec.ActualVGNameOnTheNode
	lvName := llv.Spec.ActualLVNameOnTheNode

	err := deleteLVIfExists(log, vgName, lvName)
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLLVDeleteFunc] unable to delete the LV %s in VG %s", lvName, vgName))
		updErr := updateLVMLogicalVolumePhaseIfNeeded(ctx, cl, log, metrics, llv, failedStatusPhase, fmt.Sprintf("Unable to delete the the LV %s in VG %s, err: %s", lvName, vgName, err.Error()))
		if updErr != nil {
			log.Error(updErr, fmt.Sprintf("[reconcileLLVDeleteFunc] unable to update the LVMLogicalVolume %s", llv.Name))
			return true, updErr
		}
		return true, err
	}

	log.Info(fmt.Sprintf("[reconcileLLVDeleteFunc] successfully deleted the LV %s in VG %s", lvName, vgName))

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
