package controller

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"sds-node-configurator/api/v1alpha1"
	"sds-node-configurator/config"
	"sds-node-configurator/internal"
	"sds-node-configurator/pkg/logger"
	"sds-node-configurator/pkg/monitoring"
	"sds-node-configurator/pkg/utils"
	"time"

	"github.com/google/go-cmp/cmp"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
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
				log.Info(fmt.Sprintf("[RunLVMLogicalVolumeWatcherController] requeue reconciliation of LLV: %s after %s", request.Name, cfg.LLVRequeInterval))
				return reconcile.Result{RequeueAfter: cfg.LLVRequeInterval}, nil
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

func identifyReconcileFunc(log logger.Logger, vgName string, llv *v1alpha1.LVMLogicalVolume) (reconcileType, error) {
	should, err := shouldReconcileByCreateFunc(log, vgName, llv)
	if err != nil {
		return "", err
	}
	if should {
		return CreateReconcile, nil
	}

	should, err = shouldReconcileByUpdateFunc(llv)
	if err != nil {
		return "", err
	}
	if should {
		return UpdateReconcile, nil
	}

	should = shouldReconcileByDeleteFunc(llv)
	if should {
		return DeleteReconcile, nil
	}

	return "", nil
}

func reconcileLLVDeleteFunc(
	ctx context.Context,
	cl client.Client,
	log logger.Logger,
	metrics monitoring.Metrics,
	llv *v1alpha1.LVMLogicalVolume,
	lvg *v1alpha1.LvmVolumeGroup,
) {
	log.Info("[reconcileLLVDeleteFunc] starts reconciliation")

	vgName := lvg.Spec.ActualVGNameOnTheNode
	lvName := llv.Spec.ActualLVNameOnTheNode

	err := deleteLVIfExists(log, vgName, lvName)

	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLLVDeleteFunc] unable to delete the LV %s in VG %s", lvName, vgName))
		err = updateLVMLogicalVolumePhase(ctx, cl, log, metrics, llv, failedStatusPhase, fmt.Sprintf("Unable to delete the the LV %s in VG %s, err: %s", lvName, vgName, err.Error()))
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLLVDeleteFunc] unable to update the LVMLogicalVolume %s", llv.Name))
		}
		return
	}

	log.Info(fmt.Sprintf("[reconcileLLVDeleteFunc] successfully deleted the LV %s in VG %s", lvName, vgName))

	err = removeLLVFinalizersIfExist(ctx, cl, metrics, log, llv)
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLLVDeleteFunc] unable to remove finalizers from the LVMVolumeGroup %s", llv.Name))
		err = updateLVMLogicalVolumePhase(ctx, cl, log, metrics, llv, failedStatusPhase, fmt.Sprintf("Unable to remove finalizer %s, err: %s", internal.SdsNodeConfiguratorFinalizer, err.Error()))
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLLVDeleteFunc] unable to update the LVMLogicalVolume %s", llv.Name))
		}
	}

	log.Info("[reconcileLLVDeleteFunc] ends reconciliation")
}

func shouldReconcileByDeleteFunc(llv *v1alpha1.LVMLogicalVolume) bool {
	if llv.DeletionTimestamp == nil {
		return false
	}

	return true
}

func removeLLVFinalizersIfExist(
	ctx context.Context,
	cl client.Client,
	metrics monitoring.Metrics,
	log logger.Logger,
	llv *v1alpha1.LVMLogicalVolume,
) error {
	var removed bool
	for i, f := range llv.Finalizers {
		if f == internal.SdsNodeConfiguratorFinalizer {
			llv.Finalizers = append(llv.Finalizers[:i], llv.Finalizers[i+1:]...)
			removed = true
			log.Debug(fmt.Sprintf("[removeLLVFinalizersIfExist] removed finalizer %s from the LVMLogicalVolume %s", internal.SdsNodeConfiguratorFinalizer, llv.Name))
			break
		}
	}

	if removed {
		log.Trace(fmt.Sprintf("[removeLLVFinalizersIfExist] removed finalizer %s from the LVMLogicalVolume %s", internal.SdsNodeConfiguratorFinalizer, llv.Name))
		err := updateLVMLogicalVolume(ctx, metrics, cl, llv)
		if err != nil {
			log.Error(err, fmt.Sprintf("[updateLVMLogicalVolume] unable to update the LVMVolumeGroup %s", llv.Name))
			return err
		}
	}

	return nil
}

func deleteLVIfExists(log logger.Logger, vgName, lvName string) error {
	lv, err := FindLV(log, vgName, lvName)
	if err != nil {
		return err
	}

	if lv == nil {
		log.Warning(fmt.Sprintf("[deleteLVIfExists] did not find LV %s in VG %s", lvName, vgName))
		return nil
	}

	cmd, err := utils.RemoveLV(vgName, lvName)
	log.Debug(fmt.Sprintf("[deleteLVIfExists] runs cmd: %s", cmd))
	if err != nil {
		log.Error(err, "[deleteLVIfExists] unable to RemoveLV")
		return err
	}

	return nil
}

func reconcileLLVUpdateFunc(
	ctx context.Context,
	cl client.Client,
	log logger.Logger,
	metrics monitoring.Metrics,
	llv *v1alpha1.LVMLogicalVolume,
	lvg *v1alpha1.LvmVolumeGroup,
) {
	log.Info("[reconcileLLVUpdateFunc] starts reconciliation")

	err := updateLVMLogicalVolumePhase(ctx, cl, log, metrics, llv, resizingStatusPhase, "")
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLLVUpdateFunc] unable to update the LVMLogicalVolume, name: %s", llv.Name))
	}

	vgName := lvg.Spec.ActualVGNameOnTheNode
	lvName := llv.Spec.ActualLVNameOnTheNode
	extendingSize := subtractQuantity(llv.Spec.Size, llv.Status.ActualSize)
	log.Trace(fmt.Sprintf("[reconcileLLVUpdateFunc] the LV %s in VG %s has extending size %d", lvName, vgName, extendingSize.Value()))
	delta, err := resource.ParseQuantity(internal.ResizeDelta)
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLLVUpdateFunc] unable to parse the resize delta, value: %s", internal.ResizeDelta))
		err = updateLVMLogicalVolumePhase(ctx, cl, log, metrics, llv, failedStatusPhase, fmt.Sprintf("Unable to parse the resize delta, value: %s, err: %s", internal.ResizeDelta, err.Error()))
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLLVUpdateFunc] unable to update the LVMLogicalVolume %s", llv.Name))
		}
		return
	}

	switch llv.Spec.Type {
	case Thick:
		freeSpace, err := getFreeVGSpace(lvg)
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLLVUpdateFunc] unable to count free space in VG, name: %s", vgName))
			err = updateLVMLogicalVolumePhase(ctx, cl, log, metrics, llv, failedStatusPhase, fmt.Sprintf("Unable to count free VG space, VG name %s, err: %s", vgName, err.Error()))
			if err != nil {
				log.Error(err, fmt.Sprintf("[reconcileLLVUpdateFunc] unable to update the LVMLogicalVolume %s", llv.Name))
			}
			return
		}

		log.Trace(fmt.Sprintf("[reconcileLLVUpdateFunc] the LVMLogicalVolume %s, LV: %s, VG: %s Thick extending size: %d, free size: %d", llv.Name, lvName, vgName, extendingSize.Value(), freeSpace.Value()))
		if freeSpace.Value() < extendingSize.Value()+delta.Value() {
			err = errors.New("not enough space")
			log.Error(err, fmt.Sprintf("[reconcileLLVUpdateFunc] the LV %s requested size is more than the VG %s free space", lvName, vgName))
			err = updateLVMLogicalVolumePhase(ctx, cl, log, metrics, llv, failedStatusPhase, fmt.Sprintf("Not enough space on VG, requested: %d, free: %d", llv.Spec.Size.Value(), freeSpace.Value()))
			if err != nil {
				log.Error(err, fmt.Sprintf("[reconcileLLVUpdateFunc] unable to update the LVMLogicalVolume %s", llv.Name))
			}
			return
		}

		log.Debug(fmt.Sprintf("[reconcileLLVUpdateFunc] LV %s will be extended in VG %s with Quantity value: %d", lvName, vgName, llv.Spec.Size.Value()))
	case Thin:
		// freeSpace, err := getFreeLVSpace(log, llv.Spec.Thin.PoolName)
		// if err != nil {
		// 	log.Error(err, fmt.Sprintf("[reconcileLLVUpdateFunc] unable to count free space in Thin-pool, name: %s", llv.Spec.Thin.PoolName))
		// 	err = updateLVMLogicalVolumePhase(ctx, cl, log, metrics, llv, failedStatusPhase, fmt.Sprintf("Unable to count free Thin-pool space, err: %s", err.Error()))
		// 	if err != nil {
		// 		log.Error(err, fmt.Sprintf("[reconcileLLVUpdateFunc] unable to update the LVMLogicalVolume %s", llv.Name))
		// 	}
		// 	return
		// }

		// log.Trace(fmt.Sprintf("[reconcileLLVUpdateFunc] the LVMLogicalVolume %s Thin extending size: %d, free size: %d", llv.Name, extendingSize.Value(), freeSpace.Value()))
		// TODO: uncomment after implementing overcommit.
		// if freeSpace.Value() < extendingSize.Value() {
		// 	err = errors.New("not enough space")
		// 	log.Error(err, fmt.Sprintf("[reconcileLLVUpdateFunc] the LVMLogicalVolume %s requested size is more than the Thin-pool %s free space", llv.Name, llv.Spec.Thin.PoolName))
		// 	err = updateLVMLogicalVolumePhase(ctx, cl, log, metrics, llv, failedStatusPhase, "Not enough space in a Thin-pool")
		// 	if err != nil {
		// 		log.Error(err, fmt.Sprintf("[reconcileLLVUpdateFunc] unable to update the LVMLogicalVolume %s", llv.Name))
		// 	}
		// 	return
		// }

		log.Debug(fmt.Sprintf("[reconcileLLVUpdateFunc] LV %s will be extended in Thin-pool %s with Quantity value: %d", lvName, llv.Spec.Thin.PoolName, llv.Spec.Size.Value()))
	}

	actualSize, err := getLVActualSize(log, vgName, lvName)
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLLVUpdateFunc] unable to get actual size for LV %s from VG %s", lvName, vgName))
		err = updateLVMLogicalVolumePhase(ctx, cl, log, metrics, llv, failedStatusPhase, fmt.Sprintf("Unable to get actual LV size, LV name: %s, VG name: %s, err: %s", lvName, vgName, err.Error()))
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLLVUpdateFunc] unable to update the LVMLogicalVolume %s", llv.Name))
		}
		return
	}

	if utils.AreSizesEqualWithinDelta(actualSize, llv.Spec.Size, delta) {
		log.Warning(fmt.Sprintf("[reconcileLLVUpdateFunc] the LV %s in VG %s has the same actual size %s as the requested size %s", lvName, vgName, actualSize.String(), llv.Spec.Size.String()))
		llv.Status.Phase = createdStatusPhase
		llv.Status.ActualSize = actualSize
		log.Trace(fmt.Sprintf("[reconcileLLVUpdateFunc] the LVMLogicalVolume %s status.phase set to %s and actual size to %+v", llv.Name, createdStatusPhase, actualSize))
		err = updateLVMLogicalVolume(ctx, metrics, cl, llv)
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLLVUpdateFunc] unable to update the LVMLogicalVolume %s", llv.Name))
		}
		return
	}

	cmd, err := utils.ExtendLV(llv.Spec.Size.Value(), vgName, lvName)
	log.Debug(fmt.Sprintf("[reconcileLLVUpdateFunc] runs cmd: %s", cmd))
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLLVUpdateFunc] unable to ExtendLV, name: %s, type: %s", lvName, llv.Spec.Type))
		err = updateLVMLogicalVolumePhase(ctx, cl, log, metrics, llv, failedStatusPhase, fmt.Sprintf("Unable to extend %s LV, err: %s", llv.Spec.Type, err.Error()))
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLLVUpdateFunc] unable to update the LVMLogicalVolume %s", llv.Name))
			return
		}
		log.Info("[reconcileLLVUpdateFunc] ends reconciliation")
		return
	}

	log.Info(fmt.Sprintf("[reconcileLLVUpdateFunc] successfully extended LV %s in VG %s for LVMLogicalVolume resource with name: %s", lvName, vgName, llv.Name))

	newActualSize, err := getLVActualSize(log, vgName, lvName)
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLLVUpdateFunc] unable to get actual size for LV %s in VG %s", lvName, vgName))
		err = updateLVMLogicalVolumePhase(ctx, cl, log, metrics, llv, failedStatusPhase, fmt.Sprintf("Unable to get LV actual size, err: %s", err.Error()))
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLLVUpdateFunc] unable to update the LVMLogicalVolume %s", llv.Name))
		}
		return
	}
	log.Trace(fmt.Sprintf("[reconcileLLVUpdateFunc] the LV %s in VG %s actual size %s", lvName, vgName, newActualSize.String()))

	llv.Status.Phase = createdStatusPhase
	llv.Status.ActualSize = newActualSize
	log.Trace(fmt.Sprintf("[reconcileLLVUpdateFunc] the LVMLogicalVolume %s status.phase set to %s and actual size to %+v", llv.Name, createdStatusPhase, newActualSize))
	err = updateLVMLogicalVolume(ctx, metrics, cl, llv)
	if err != nil {
		log.Error(err, "[reconcileLLVUpdateFunc] unable to updateLVMLogicalVolume")
		return
	}

	log.Info("[reconcileLLVUpdateFunc] ends reconciliation")
}

func shouldReconcileByUpdateFunc(llv *v1alpha1.LVMLogicalVolume) (bool, error) {
	if llv.DeletionTimestamp != nil {
		return false, nil
	}

	if llv.Status == nil {
		return false, nil
	}

	if llv.Status.Phase == pendingStatusPhase || llv.Status.Phase == resizingStatusPhase {
		return false, nil
	}

	delta, err := resource.ParseQuantity(internal.ResizeDelta)
	if err != nil {
		return false, err
	}

	if llv.Spec.Size.Value()+delta.Value() < llv.Status.ActualSize.Value() {
		return false, fmt.Errorf("requested size %d is less than actual %d", llv.Spec.Size.Value(), llv.Status.ActualSize.Value())
	}

	if utils.AreSizesEqualWithinDelta(llv.Spec.Size, llv.Status.ActualSize, delta) {
		return false, nil
	}

	return true, nil
}

func reconcileLLVCreateFunc(
	ctx context.Context,
	cl client.Client,
	log logger.Logger,
	metrics monitoring.Metrics,
	llv *v1alpha1.LVMLogicalVolume,
	lvg *v1alpha1.LvmVolumeGroup,
) {
	log.Info("[reconcileLLVCreateFunc] starts reconciliation")

	err := updateLVMLogicalVolumePhase(ctx, cl, log, metrics, llv, pendingStatusPhase, "")
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLLVCreateFunc] unable to update the LVMLogicalVolume %s", llv.Name))
		return
	}

	vgName := lvg.Spec.ActualVGNameOnTheNode
	lvName := llv.Spec.ActualLVNameOnTheNode
	added, err := addLLVFinalizerIfNotExist(ctx, cl, log, metrics, llv)
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLLVCreateFunc] unable to update the LVMLogicalVolume %s", llv.Name))
		return
	}
	log.Debug(fmt.Sprintf("[reconcileLLVCreateFunc] a finalizer to the LVMLogicalVolume %s was added: %t", llv.Name, added))

	switch llv.Spec.Type {
	case Thick:
		freeSpace, err := getFreeVGSpace(lvg)
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLLVCreateFunc] unable to count free space in VG, name: %s", vgName))
			err = updateLVMLogicalVolumePhase(ctx, cl, log, metrics, llv, failedStatusPhase, fmt.Sprintf("Unable to get free VG space, err: %s", err.Error()))
			if err != nil {
				log.Error(err, fmt.Sprintf("[reconcileLLVCreateFunc] unable to updateLVMLogicalVolumePhase for LVMLogicalVolume %s", llv.Name))
			}
			return
		}

		log.Trace(fmt.Sprintf("[reconcileLLVCreateFunc] the LVMLogicalVolume %s, LV: %s, VG: %s type: %s requested size: %d, free size: %d", llv.Name, lvName, vgName, llv.Spec.Type, llv.Spec.Size.Value(), freeSpace.Value()))
		if freeSpace.Value() < llv.Spec.Size.Value() {
			err = errors.New("not enough space")
			log.Error(err, fmt.Sprintf("[reconcileLLVCreateFunc] the LV %s requested size is more than the VG %s free space", lvName, vgName))
			err = updateLVMLogicalVolumePhase(ctx, cl, log, metrics, llv, failedStatusPhase, "Not enough space in VG")
			if err != nil {
				log.Error(err, fmt.Sprintf("[reconcileLLVCreateFunc] unable to update the LVMLogicalVolume %s", llv.Name))
			}
			return
		}

		log.Debug(fmt.Sprintf("[reconcileLLVCreateFunc] LV %s will be create in VG %s with Quantity value: %d", lvName, vgName, llv.Spec.Size.Value()))
		cmd, err := utils.CreateThickLogicalVolume(vgName, lvName, llv.Spec.Size.Value())
		log.Debug(fmt.Sprintf("[reconcileLLVCreateFunc] runs cmd: %s", cmd))
		if err != nil {
			log.Error(err, "[reconcileLLVCreateFunc] unable to create a thick LogicalVolume")
			err = updateLVMLogicalVolumePhase(ctx, cl, log, metrics, llv, failedStatusPhase, fmt.Sprintf("Unable to create Thick LV, err: %s", err.Error()))
			if err != nil {
				log.Error(err, fmt.Sprintf("[reconcileLLVCreateFunc] unable to update the LVMLogicalVolume, name: %s", llv.Name))
			}
			return
		}
	case Thin:
		// freeSpace, err := getFreeLVSpace(log, llv.Spec.Thin.PoolName)
		// if err != nil {
		// 	log.Error(err, fmt.Sprintf("[reconcileLLVCreateFunc] unable to count free space in LV, name: %s", llv.Spec.Thin.PoolName))
		// 	err = updateLVMLogicalVolumePhase(ctx, cl, log, metrics, llv, failedStatusPhase, fmt.Sprintf("Unable to get free LV space, err: %s", err.Error()))
		// 	if err != nil {
		// 		log.Error(err, fmt.Sprintf("[reconcileLLVCreateFunc] unable to update the LVMLogicalVolume, name: %s", llv.Name))
		// 	}
		// 	return
		// }

		// log.Trace(fmt.Sprintf("[reconcileLLVCreateFunc] the LVMLogicalVolume %s, type: %s requested size: %d, free size: %d", llv.Name, llv.Spec.Type, llv.Spec.Size.Value(), freeSpace.Value()))
		// TODO: uncomment after implementing overcommit.
		// if freeSpace.Value() < llv.Spec.Size.Value() {
		// 	err = errors.New("not enough space")
		// 	log.Error(err, fmt.Sprintf("[reconcileLLVCreateFunc] the LVMLogicalVolume %s requested size is more than the Thin-pool %s free space", llv.Name, llv.Spec.Thin.PoolName))
		// 	err = updateLVMLogicalVolumePhase(ctx, cl, log, metrics, llv, failedStatusPhase, fmt.Sprintf("Not enough space in Thin-pool %s in LVMVolumeGroup %s", llv.Spec.Thin.PoolName, lvg.Name))
		// 	if err != nil {
		// 		log.Error(err, fmt.Sprintf("[reconcileLLVCreateFunc] unable to update the LVMLogicalVolume, name: %s", llv.Name))
		// 	}
		// 	return
		// }

		log.Debug(fmt.Sprintf("[reconcileLLVCreateFunc] LV %s will be create in Thin-pool %s with size %s", lvName, llv.Spec.Thin.PoolName, llv.Spec.Size.String()))
		cmd, err := utils.CreateThinLogicalVolume(vgName, llv.Spec.Thin.PoolName, lvName, llv.Spec.Size.Value())
		log.Debug(fmt.Sprintf("[reconcileLLVCreateFunc] runs cmd: %s", cmd))
		if err != nil {
			log.Error(err, "[reconcileLLVCreateFunc] unable to CreateThickLogicalVolume")
			err = updateLVMLogicalVolumePhase(ctx, cl, log, metrics, llv, failedStatusPhase, fmt.Sprintf("Unable to create Thin LV, err: %s", err.Error()))
			if err != nil {
				log.Error(err, fmt.Sprintf("[reconcileLLVCreateFunc] unable to update the LVMLogicalVolume, name: %s", llv.Name))
			}
			return
		}
	}

	log.Info(fmt.Sprintf("[reconcileLLVCreateFunc] successfully created LV %s in VG %s for LVMLogicalVolume resource with name: %s", lvName, vgName, llv.Name))

	actualSize, err := getLVActualSize(log, vgName, lvName)
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLLVCreateFunc] unable to get actual size for LV %s in VG %s", lvName, vgName))
		err = updateLVMLogicalVolumePhase(ctx, cl, log, metrics, llv, failedStatusPhase, fmt.Sprintf("Unable to get actual LV size, LV name: %s, VG name: %s, err: %s", lvName, vgName, err.Error()))
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLLVCreateFunc] unable to update the LVMLogicalVolume, name: %s", llv.Name))
		}
		return
	}
	log.Trace(fmt.Sprintf("[reconcileLLVCreateFunc] the LV %s in VG: %s has actual size: %s", lvName, vgName, actualSize.String()))

	if llv.Status == nil {
		llv.Status = new(v1alpha1.LVMLogicalVolumeStatus)
	}
	llv.Status.Phase = createdStatusPhase
	llv.Status.ActualSize = actualSize
	log.Trace(fmt.Sprintf("[reconcileLLVCreateFunc] the LVMLogicalVolume %s status.phase set to %s and actual size to %+v", llv.Name, createdStatusPhase, actualSize))
	err = updateLVMLogicalVolume(ctx, metrics, cl, llv)
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLLVCreateFunc] unable to update the LVMLogicalVolume, name: %s", llv.Name))
		return
	}

	log.Info("[reconcileLLVCreateFunc] ends reconciliation")
}

func getLVActualSize(log logger.Logger, vgName, lvName string) (resource.Quantity, error) {
	lv, cmd, _, err := utils.GetLV(vgName, lvName)
	log.Debug(fmt.Sprintf("[getActualSize] runs cmd: %s", cmd))
	if err != nil {
		return resource.Quantity{}, err
	}

	result := resource.NewQuantity(lv.LVSize.Value(), resource.BinarySI)

	return *result, nil

}

func addLLVFinalizerIfNotExist(ctx context.Context, cl client.Client, log logger.Logger, metrics monitoring.Metrics, llv *v1alpha1.LVMLogicalVolume) (bool, error) {
	if slices.Contains(llv.Finalizers, internal.SdsNodeConfiguratorFinalizer) {
		return false, nil
	}

	llv.Finalizers = append(llv.Finalizers, internal.SdsNodeConfiguratorFinalizer)

	log.Trace(fmt.Sprintf("[addLLVFinalizerIfNotExist] added finalizer %s to the LVMLogicalVolume %s", internal.SdsNodeConfiguratorFinalizer, llv.Name))
	err := updateLVMLogicalVolume(ctx, metrics, cl, llv)
	if err != nil {
		return false, err
	}

	return true, nil
}

func shouldReconcileByCreateFunc(log logger.Logger, vgName string, llv *v1alpha1.LVMLogicalVolume) (bool, error) {
	if llv.DeletionTimestamp != nil {
		return false, nil
	}

	if llv.Status == nil {
		return true, nil
	}

	if llv.Status.Phase == createdStatusPhase ||
		llv.Status.Phase == resizingStatusPhase {
		return false, nil
	}

	lv, err := FindLV(log, vgName, llv.Spec.ActualLVNameOnTheNode)
	if err == nil && lv != nil && lv.LVName == llv.Spec.ActualLVNameOnTheNode {
		return false, nil
	}

	if lv != nil {
		return false, nil
	}

	return true, nil
}

// func getFreeLVSpace(log logger.Logger, thinPoolName string) (resource.Quantity, error) {
// 	lvs, cmd, _, err := utils.GetAllLVs()
// 	log.Debug(fmt.Sprintf("[getFreeLVSpace] runs cmd: %s", cmd))
// 	if err != nil {
// 		log.Error(err, "[getFreeVGSpace] unable to GetAllLVs")
// 		return resource.Quantity{}, err
// 	}

// 	for _, lv := range lvs {
// 		if lv.LVName == thinPoolName {
// 			used, err := getLVUsedSize(lv)
// 			vlsSize := getVirtualLVSize(lv.LVName, lvs)

// 			if err != nil {
// 				log.Error(err, "[getFreeLVSpace] unable to getLVUsedSize")
// 				return resource.Quantity{}, err
// 			}

// 			free := subtractQuantity(lv.LVSize, *used)
// 			free = subtractQuantity(free, vlsSize)

// 			return free, nil
// 		}
// 	}

// 	return resource.Quantity{}, nil
// }

// func getVirtualLVSize(thinPool string, lvs []internal.LVData) resource.Quantity {
// 	sum := int64(0)

// 	for _, lv := range lvs {
// 		if lv.PoolLv == thinPool {
// 			sum += lv.LVSize.Value()
// 		}
// 	}

// 	return *resource.NewQuantity(sum, resource.BinarySI)
// }

func subtractQuantity(currentQuantity, quantityToSubtract resource.Quantity) resource.Quantity {
	resultingQuantity := currentQuantity.DeepCopy()
	resultingQuantity.Sub(quantityToSubtract)
	return resultingQuantity
}

func getFreeVGSpace(lvg *v1alpha1.LvmVolumeGroup) (resource.Quantity, error) {
	total, err := resource.ParseQuantity(lvg.Status.VGSize)
	if err != nil {
		return resource.Quantity{}, err
	}

	allocated, err := resource.ParseQuantity(lvg.Status.AllocatedSize)
	if err != nil {
		return resource.Quantity{}, err
	}

	return subtractQuantity(total, allocated), nil
}

func belongsToNode(lvg *v1alpha1.LvmVolumeGroup, nodeName string) bool {
	var belongs bool
	for _, node := range lvg.Status.Nodes {
		if node.Name == nodeName {
			belongs = true
		}
	}

	return belongs
}

func updateLVMLogicalVolumePhase(ctx context.Context, cl client.Client, log logger.Logger, metrics monitoring.Metrics, llv *v1alpha1.LVMLogicalVolume, phase, reason string) error {
	if llv.Status == nil {
		llv.Status = new(v1alpha1.LVMLogicalVolumeStatus)
	}
	llv.Status.Phase = phase
	llv.Status.Reason = reason

	err := updateLVMLogicalVolume(ctx, metrics, cl, llv)
	if err != nil {
		return err
	}
	log.Debug(fmt.Sprintf("[updateLVMLogicalVolumePhase] updated LVMLogicalVolume %s status.phase to %s and reason to %s", llv.Name, phase, reason))
	return nil
}

func updateLVMLogicalVolume(ctx context.Context, metrics monitoring.Metrics, cl client.Client, llv *v1alpha1.LVMLogicalVolume) error {
	var err error

	for i := 0; i < internal.KubernetesApiRequestLimit; i++ {
		err = cl.Update(ctx, llv)
		if err == nil {
			return nil
		}
		time.Sleep(internal.KubernetesApiRequestTimeout * time.Second)
	}

	return err
}

func FindLV(log logger.Logger, vgName, lvName string) (*internal.LVData, error) {
	log.Debug(fmt.Sprintf("[FindLV] Try to find LV: %s in VG: %s", lvName, vgName))
	lv, cmd, _, err := utils.GetLV(vgName, lvName)

	log.Debug(fmt.Sprintf("[FindLV] runs cmd: %s", cmd))
	if err != nil {
		if strings.Contains(err.Error(), "Failed to find logical volume") {
			log.Debug("[FindLV] LV not found")
			return nil, nil
		}
		log.Error(err, "[shouldReconcileByCreateFunc] unable to GetLV")
		return nil, err
	}
	return &lv, nil

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

	lvg, err := getLVMVolumeGroup(ctx, cl, metrics, "", llv.Spec.LvmVolumeGroupName)
	if err != nil {
		if k8serr.IsNotFound(err) {
			log.Debug(fmt.Sprintf("[ReconcileLVMLogicalVolume] LVMVolumeGroup %s not found for LVMLogicalVolume %s", llv.Spec.LvmVolumeGroupName, llv.Name))
			err = updateLVMLogicalVolumePhase(ctx, cl, log, metrics, llv, failedStatusPhase, fmt.Sprintf("Unable to get selected LVMVolumeGroup %s, err: %s", llv.Spec.LvmVolumeGroupName, err.Error()))
			if err != nil {
				return true, fmt.Errorf("[ReconcileLVMLogicalVolume] unable to update the LVMLogicalVolume %s status.phase to %s: %w", llv.Name, failedStatusPhase, err)
			}
			return true, nil
		}
		reconcileErr := fmt.Errorf("[ReconcileLVMLogicalVolume] unable to get LVMVolumeGroup %s: %w", llv.Spec.LvmVolumeGroupName, err)
		updateErr := updateLVMLogicalVolumePhase(ctx, cl, log, metrics, llv, failedStatusPhase, fmt.Sprintf("Unable to get selected LVMVolumeGroup, err: %s", err.Error()))
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
	log.Debug("[ReconcileLVMLogicalVolume] Identify reconcile func. vgName: "+lvg.Spec.ActualVGNameOnTheNode+", llv:", llv.Name, llv)

	recType, err := identifyReconcileFunc(log, lvg.Spec.ActualVGNameOnTheNode, llv)
	if err != nil {
		reconcileErr := fmt.Errorf("[ReconcileLVMLogicalVolume] unable to identify the reconcile func: %w", err)
		updateErr := updateLVMLogicalVolumePhase(ctx, cl, log, metrics, llv, failedStatusPhase, fmt.Sprintf("An error occurred while identifying the reconcile func, err: %s", err.Error()))
		if updateErr != nil {
			return true, fmt.Errorf("%s. Also failed to update LVMLogicalVolume %s status.phase to %s: %w", reconcileErr, llv.Name, failedStatusPhase, updateErr)
		}
		return false, reconcileErr
	}

	switch recType {
	case CreateReconcile:
		log.Debug(fmt.Sprintf("[runEventReconcile] CreateReconcile starts reconciliation for the LVMLogicalVolume: %s", llv.Name))
		reconcileLLVCreateFunc(ctx, cl, log, metrics, llv, lvg)
	case UpdateReconcile:
		log.Debug(fmt.Sprintf("[runEventReconcile] UpdateReconcile starts reconciliation for the LVMLogicalVolume: %s", llv.Name))
		reconcileLLVUpdateFunc(ctx, cl, log, metrics, llv, lvg)
	case DeleteReconcile:
		log.Debug(fmt.Sprintf("[runEventReconcile] DeleteReconcile starts reconciliation for the LVMLogicalVolume: %s", llv.Name))
		reconcileLLVDeleteFunc(ctx, cl, log, metrics, llv, lvg)
	default:
		log.Debug(fmt.Sprintf("[runEventReconcile] the LVMLogicalVolume %s should not be reconciled", llv.Name))
		if llv.Status.Phase != createdStatusPhase {
			log.Warning(fmt.Sprintf("[runEventReconcile] the LVMLogicalVolume %s should not be reconciled but has an unexpected phase: %s. Setting the phase to %s", llv.Name, llv.Status.Phase, createdStatusPhase))
			err = updateLVMLogicalVolumePhase(ctx, cl, log, metrics, llv, createdStatusPhase, "")
			if err != nil {
				return true, fmt.Errorf("[runEventReconcile] unable to update the LVMLogicalVolume %s status.phase to %s: %w", llv.Name, createdStatusPhase, err)
			}
		}
	}
	return false, nil
}
