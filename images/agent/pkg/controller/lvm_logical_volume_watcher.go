package controller

import (
	"context"
	"errors"
	"fmt"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/strings/slices"
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
	Thick deviceType = "Thick"
	Thin  deviceType = "Thin"

	lvmLogicalVolumeWatcherCtrlName = "lvm-logical-volume-watcher-controller"

	createdStatusPhase  = "Created"
	pendingStatusPhase  = "Pending"
	resizingStatusPhase = "Resizing"
	failedStatusPhase   = "Failed"
)

type (
	deviceType string
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
			return reconcile.Result{}, nil
		}),
	})
	if err != nil {
		log.Error(err, "[RunLVMLogicalVolumeWatcherController] unable to create controller")
		return nil, err
	}

	err = c.Watch(source.Kind(cache, &v1alpha1.LvmLogicalVolume{}), handler.Funcs{
		CreateFunc: func(ctx context.Context, e event.CreateEvent, q workqueue.RateLimitingInterface) {
			log.Info("[RunLVMLogicalVolumeWatcherController] CreateFunc starts reconciliation")

			llv, ok := e.Object.(*v1alpha1.LvmLogicalVolume)
			if !ok {
				err = errors.New("unable to cast event object to a given type")
				log.Error(err, "[CreateFunc] an error occurs while handling create event")
				return
			}

			lvg, err := getLVMVolumeGroup(ctx, cl, metrics, "", llv.Spec.LvmVolumeGroup)
			if err != nil {
				log.Error(err, "[CreateFunc] unable to getLVMVolumeGroup")
				err = updateLVMLogicalVolumePhase(ctx, cl, metrics, llv, failedStatusPhase, "Unable to get selected LVMVolumeGroup")
				if err != nil {
					log.Error(err, "[CreateFunc] unable to updateLVMLogicalVolumePhase")
				}
				return
			}

			if !belongsToNode(lvg, cfg.NodeName) {
				log.Debug(fmt.Sprintf("[CreateFunc] the LVMVolumeGroup %s does not belongs to the current node: %s. Reconciliation stopped", lvg.Name, cfg.NodeName))
				return
			}
			log.Debug(fmt.Sprintf("[CreateFunc] the LVMVolumeGroup %s belongs to the current node: %s", lvg.Name, cfg.NodeName))

			reconcileLLVCreateFunc(ctx, cl, log, metrics, llv, lvg)
			reconcileLLVUpdateFunc(ctx, cl, log, metrics, llv, lvg)
			reconcileLLVDeleteFunc(ctx, cl, log, metrics, llv)

			log.Info("[RunLVMLogicalVolumeWatcherController] CreateFunc ends reconciliation")
		},

		UpdateFunc: func(ctx context.Context, e event.UpdateEvent, q workqueue.RateLimitingInterface) {
			log.Info("[RunLVMLogicalVolumeWatcherController] UpdateFunc starts reconciliation")

			llv, ok := e.ObjectNew.(*v1alpha1.LvmLogicalVolume)
			if !ok {
				err = errors.New("unable to cast event object to a given type")
				log.Error(err, "[UpdateFunc] an error occurs while handling update event")
				return
			}

			lvg, err := getLVMVolumeGroup(ctx, cl, metrics, "", llv.Spec.LvmVolumeGroup)
			if err != nil {
				log.Error(err, "[UpdateFunc] unable to getLVMVolumeGroup")
				err = updateLVMLogicalVolumePhase(ctx, cl, metrics, llv, failedStatusPhase, "Unable to get selected LVMVolumeGroup")
				if err != nil {
					log.Error(err, "[UpdateFunc] unable to updateLVMLogicalVolumePhase")
				}
				return
			}

			if !belongsToNode(lvg, cfg.NodeName) {
				log.Debug(fmt.Sprintf("[UpdateFunc] the LVMVolumeGroup %s does not belongs to the current node: %s. Reconciliation stopped", lvg.Name, cfg.NodeName))
				return
			}
			log.Debug(fmt.Sprintf("[UpdateFunc] the LVMVolumeGroup %s belongs to the current node: %s", lvg.Name, cfg.NodeName))

			reconcileLLVUpdateFunc(ctx, cl, log, metrics, llv, lvg)
			reconcileLLVDeleteFunc(ctx, cl, log, metrics, llv)

			log.Info("[RunLVMLogicalVolumeWatcherController] UpdateFunc ends reconciliation")
		},
	})
	if err != nil {
		log.Error(err, "[RunLVMLogicalVolumeWatcherController] the controller is unable to watch")
		return nil, err
	}

	return c, err
}

func reconcileLLVDeleteFunc(
	ctx context.Context,
	cl client.Client,
	log logger.Logger,
	metrics monitoring.Metrics,
	llv *v1alpha1.LvmLogicalVolume,
) {
	log.Info("[reconcileLLVDeleteFunc] starts reconciliation")

	shouldReconcile := shouldReconcileByDeleteFunc(llv)
	if !shouldReconcile {
		log.Debug(fmt.Sprintf("[reconcileLLVDeleteFunc] the LVMLogicalVolume %s should not be reconciled", llv.Name))
		return
	}

	err := deleteLVifExisted(log, llv)
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLLVDeleteFunc] unable to delete LV %s", llv.Name))
		return
	}

	log.Info(fmt.Sprintf("[reconcileLLVDeleteFunc] successfully deleted LV %s", llv.Name))

	err = removeLLVFinalizers(ctx, cl, metrics, log, llv)
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLLVDeleteFunc] unable to remove finalizers from the LVMVolumeGroup %s", llv.Name))
	}

	log.Info("[reconcileLLVDeleteFunc] ends reconciliation")
}

func shouldReconcileByDeleteFunc(llv *v1alpha1.LvmLogicalVolume) bool {
	if llv.DeletionTimestamp == nil {
		return false
	}

	return true
}

func removeLLVFinalizers(
	ctx context.Context,
	cl client.Client,
	metrics monitoring.Metrics,
	log logger.Logger,
	llv *v1alpha1.LvmLogicalVolume,
) error {
	var removed bool
	for i, f := range llv.Finalizers {
		if f == internal.SdsNodeConfiguratorFinalizer {
			llv.Finalizers = append(llv.Finalizers[:i], llv.Finalizers[i+1:]...)
			removed = true
			log.Debug(fmt.Sprintf("[removeLLVFinalizers] removed finalizer %s from the LVMLogicalVolume %s", internal.SdsNodeConfiguratorFinalizer, llv.Name))
			break
		}
	}

	if removed {
		err := updateLVMLogicalVolume(ctx, metrics, cl, llv)
		if err != nil {
			log.Error(err, fmt.Sprintf("[updateLVMLogicalVolume] unable to update the LVMVolumeGroup %s", llv.Name))
			return err
		}
	}

	return nil
}

func deleteLVifExisted(log logger.Logger, llv *v1alpha1.LvmLogicalVolume) error {
	lvs, cmd, _, err := utils.GetAllLVs()
	log.Debug(fmt.Sprintf("[deleteLVifExisted] runs cmd: %s", cmd))
	if err != nil {
		return err
	}

	var (
		lv *internal.LVData
	)
	for _, l := range lvs {
		if l.LVName == llv.Name {
			lv = &l
			break
		}
	}

	if lv == nil {
		log.Debug(fmt.Sprintf("[deleteLVifExisted] did not find LV %s", lv.LVName))
		return errors.New("lv does not exist")
	}

	cmd, err = utils.RemoveLV(lv.VGName, lv.LVName)
	log.Debug("[deleteLVifExisted] runs cmd: %s", cmd)
	if err != nil {
		log.Error(err, "[deleteLVifExisted] unable to RemoveLV")
		return err
	}

	return nil
}

func getExtendingSize(log logger.Logger, metrics monitoring.Metrics, llv *v1alpha1.LvmLogicalVolume) (resource.Quantity, error) {
	lvs, cmd, _, err := utils.GetAllLVs()
	log.Debug(fmt.Sprintf("[getExtendingSize] runs cmd %s", cmd))
	if err != nil {
		log.Error(err, "[getExtendingSize] unable to GetAllLVs")
		return resource.Quantity{}, err
	}

	var (
		oldSize resource.Quantity
		newSize = llv.Spec.Size
	)
	for _, lv := range lvs {
		if lv.LVName == llv.Name {
			oldSize = lv.LVSize
			break
		}
	}

	if newSize.Value() < oldSize.Value() {
		return resource.Quantity{}, fmt.Errorf("selected size is less than exising one")
	}

	return subtractQuantity(newSize, oldSize), nil
}

func reconcileLLVUpdateFunc(
	ctx context.Context,
	cl client.Client,
	log logger.Logger,
	metrics monitoring.Metrics,
	llv *v1alpha1.LvmLogicalVolume,
	lvg *v1alpha1.LvmVolumeGroup,
) {
	log.Info("[reconcileLLVUpdateFunc] starts reconciliation")

	shouldReconcile, err := shouldReconcileByUpdateFunc(log, llv)
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLLVUpdateFunc] unable to check if LV %s should be reconciled", llv.Name))
		err = updateLVMLogicalVolumePhase(ctx, cl, metrics, llv, failedStatusPhase, "Unable to check LV state")
		return
	}

	if !shouldReconcile {
		log.Debug(fmt.Sprintf("[reconcileLLVUpdateFunc] the LVMLogicalVolume %s should not be reconciled", llv.Name))
		return
	}

	err = updateLVMLogicalVolumePhase(ctx, cl, metrics, llv, resizingStatusPhase, "")
	if err != nil {
		log.Error(err, "[reconcileLLVUpdateFunc] unable to updateLVMLogicalVolumePhase")
	}
	log.Debug(fmt.Sprintf("[reconcileLLVUpdateFunc] updated LVMLogicaVolume %s status.phase to %s", llv.Name, resizingStatusPhase))
	log.Debug(fmt.Sprintf("[reconcileLLVUpdateFunc] the LVMLogicalVolume %s spec.thin.poolname: \"%s\"", llv.Name, llv.Spec.Thin.PoolName))

	extendingSize, err := getExtendingSize(log, metrics, llv)
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLLVUpdateFunc] error occurs while getting extending size for the LVMLogicalVolume %s", llv.Name))
		err = updateLVMLogicalVolumePhase(ctx, cl, metrics, llv, failedStatusPhase, "Unable to count extending LV size")
		if err != nil {
			log.Error(err, "[reconcileLLVUpdateFunc] unable to updateLVMLogicalVolumePhase")
		}
		return
	}

	switch getLVMLogicalVolumeType(llv) {
	case Thick:
		freeSpace, err := getFreeVGSpace(log, lvg.Spec.ActualVGNameOnTheNode)
		log.Trace(fmt.Sprintf("[reconcileLLVUpdateFunc] free VG space %d", freeSpace.Value()))
		if err != nil {
			log.Error(err, "[reconcileLLVUpdateFunc] unable to getFreeVGSpace")
			err = updateLVMLogicalVolumePhase(ctx, cl, metrics, llv, failedStatusPhase, "Unable to get free VG space")
			if err != nil {
				log.Error(err, "[reconcileLLVUpdateFunc] unable to updateLVMLogicalVolumePhase")
			}
			return
		}

		if freeSpace.Value() < extendingSize.Value() {
			err = errors.New("not enough space")
			log.Error(err, fmt.Sprintf("[reconcileLLVUpdateFunc] the LVMLogicalVolume %s requested size is more than the VG %s free space", llv.Name, lvg.Spec.ActualVGNameOnTheNode))
			err = updateLVMLogicalVolumePhase(ctx, cl, metrics, llv, failedStatusPhase, "Not enough space on VG")
			if err != nil {
				log.Error(err, "[reconcileLLVUpdateFunc] unable to updateLVMLogicalVolumePhase")
			}
			return
		}

		cmd, err := utils.ExtendLV(llv.Spec.Size.String(), lvg.Spec.ActualVGNameOnTheNode, llv.Name)
		log.Debug(fmt.Sprintf("[reconcileLLVUpdateFunc] runs cmd: %s", cmd))
		if err != nil {
			log.Error(err, "[reconcileLLVUpdateFunc] unable to ExtendLV")
			err = updateLVMLogicalVolumePhase(ctx, cl, metrics, llv, failedStatusPhase, "Unable to extend Thick LV")
			if err != nil {
				log.Error(err, "[reconcileLLVUpdateFunc] unable to updateLVMLogicalVolumePhase")
			}
			return
		}
	case Thin:
		freeSpace, err := getFreeLVSpace(log, llv.Spec.Thin.PoolName)
		log.Trace(fmt.Sprintf("[reconcileLLVUpdateFunc] free Thin-pool space %d", freeSpace.Value()))
		if err != nil {
			log.Error(err, "[reconcileLLVUpdateFunc] unable to getFreeVGSpace")
			err = updateLVMLogicalVolumePhase(ctx, cl, metrics, llv, failedStatusPhase, "Not enough space on LV")
			if err != nil {
				log.Error(err, "[reconcileLLVUpdateFunc] unable to updateLVMLogicalVolumePhase")
			}
			return
		}

		if freeSpace.Value() < extendingSize.Value() {
			err = errors.New("not enough space")
			log.Error(err, fmt.Sprintf("[reconcileLLVUpdateFunc] the LVMLogicalVolume %s requested size is more than the Thin-pool %s free space", llv.Name, llv.Spec.Thin.PoolName))
			err = updateLVMLogicalVolumePhase(ctx, cl, metrics, llv, failedStatusPhase, "Not enough space on Thin pool")
			if err != nil {
				log.Error(err, "[reconcileLLVUpdateFunc] unable to updateLVMLogicalVolumePhase")
			}
			return
		}

		cmd, err := utils.ExtendLV(llv.Spec.Size.String(), lvg.Spec.ActualVGNameOnTheNode, llv.Name)
		log.Debug(fmt.Sprintf("[reconcileLLVUpdateFunc] runs cmd: %s", cmd))
		if err != nil {
			log.Error(err, "[reconcileLLVUpdateFunc] unable to ExtendLV")
			err = updateLVMLogicalVolumePhase(ctx, cl, metrics, llv, failedStatusPhase, "Unable to extend Thin pool")
			if err != nil {
				log.Error(err, "[reconcileLLVUpdateFunc] unable to updateLVMLogicalVolumePhase")
			}
			return
		}

	}

	log.Info(fmt.Sprintf("[reconcileLLVUpdateFunc] successfully extended Logical Volume for LVMLogicalVolume, name: %s", llv.Name))

	llv.Status.Phase = createdStatusPhase
	llv.Status.ActualSize = llv.Spec.Size
	err = updateLVMLogicalVolume(ctx, metrics, cl, llv)
	if err != nil {
		log.Error(err, "[reconcileLLVUpdateFunc] unable to updateLVMLogicalVolume")
		return
	}

	log.Info("[reconcileLLVUpdateFunc] ends reconciliation")
}

func shouldReconcileByUpdateFunc(log logger.Logger, llv *v1alpha1.LvmLogicalVolume) (bool, error) {
	if llv.DeletionTimestamp != nil {
		return false, nil
	}

	if llv.Status.Phase == pendingStatusPhase || llv.Status.Phase == resizingStatusPhase {
		return false, nil
	}

	should, err := shouldReconcileByCreateFunc(log, llv)
	if err != nil {
		return false, err
	}

	if should {
		return false, nil
	}

	if llv.Spec.Size.Value() == llv.Status.ActualSize.Value() {
		return false, nil
	}

	return true, nil
}

func reconcileLLVCreateFunc(
	ctx context.Context,
	cl client.Client,
	log logger.Logger,
	metrics monitoring.Metrics,
	llv *v1alpha1.LvmLogicalVolume,
	lvg *v1alpha1.LvmVolumeGroup,
) {
	log.Info("[reconcileLLVCreateFunc] starts reconciliation")

	shouldReconcile, err := shouldReconcileByCreateFunc(log, llv)
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLLVCreateFunc] unable to check if LV %s should be reconciled", llv.Name))
		err = updateLVMLogicalVolumePhase(ctx, cl, metrics, llv, failedStatusPhase, "Unable to check LV state")
		return
	}

	if !shouldReconcile {
		log.Debug(fmt.Sprintf("[reconcileLLVCreateFunc] the LVMLogicalVolume %s should not be reconciled", llv.Name))
		return
	}

	err = updateLVMLogicalVolumePhase(ctx, cl, metrics, llv, pendingStatusPhase, "")
	if err != nil {
		log.Error(err, "[reconcileLLVCreateFunc] unable to updateLVMLogicalVolumePhase")
		return
	}
	log.Debug(fmt.Sprintf("[reconcileLLVCreateFunc] updated LVMLogicaVolume %s status.phase to %s", llv.Name, pendingStatusPhase))
	log.Debug(fmt.Sprintf("[reconcileLLVCreateFunc] the LVMLogicalVolume %s spec.thin.poolname: \"%s\"", llv.Name, llv.Spec.Thin.PoolName))

	added, err := addLLVFinalizerIfNotExist(ctx, cl, metrics, llv)
	if err != nil {
		log.Error(err, "[reconcileLLVCreateFunc] unable to updateLVMLogicalVolumePhase")
		return
	}

	log.Debug(fmt.Sprintf("[reconcileLLVCreateFunc] a finalizer to the LVMLogicalVolume %s was added: %t", llv.Name, added))

	switch getLVMLogicalVolumeType(llv) {
	case Thick:
		freeSpace, err := getFreeVGSpace(log, lvg.Spec.ActualVGNameOnTheNode)
		log.Trace(fmt.Sprintf("[reconcileLLVCreateFunc] free VG space %d", freeSpace.Value()))
		if err != nil {
			log.Error(err, "[reconcileLLVCreateFunc] unable to getFreeVGSpace")
			err = updateLVMLogicalVolumePhase(ctx, cl, metrics, llv, failedStatusPhase, "Unable to get free VG space")
			if err != nil {
				log.Error(err, "[reconcileLLVCreateFunc] unable to updateLVMLogicalVolumePhase")
			}
			return
		}

		if freeSpace.Value() < llv.Spec.Size.Value() {
			err = errors.New("not enough space")
			log.Error(err, fmt.Sprintf("[reconcileLLVCreateFunc] the LVMLogicalVolume %s requested size is more than the VG %s free space", llv.Name, lvg.Spec.ActualVGNameOnTheNode))
			err = updateLVMLogicalVolumePhase(ctx, cl, metrics, llv, failedStatusPhase, "Not enough space on VG")
			if err != nil {
				log.Error(err, "[reconcileLLVCreateFunc] unable to updateLVMLogicalVolumePhase")
			}
			return
		}

		cmd, err := utils.CreateThickLogicalVolume(lvg.Spec.ActualVGNameOnTheNode, llv.Name, llv.Spec.Size.String())
		log.Debug(fmt.Sprintf("[reconcileLLVCreateFunc] runs cmd: %s", cmd))
		if err != nil {
			log.Error(err, "[reconcileLLVCreateFunc] unable to CreateThickLogicalVolume")
			err = updateLVMLogicalVolumePhase(ctx, cl, metrics, llv, failedStatusPhase, "Unable to create Thick LV")
			if err != nil {
				log.Error(err, "[reconcileLLVCreateFunc] unable to updateLVMLogicalVolumePhase")
			}
			return
		}
	case Thin:
		freeSpace, err := getFreeLVSpace(log, llv.Spec.Thin.PoolName)
		log.Trace(fmt.Sprintf("[reconcileLLVCreateFunc] free Thin-pool space %d", freeSpace.Value()))
		if err != nil {
			log.Error(err, "[reconcileLLVCreateFunc] unable to getFreeVGSpace")
			err = updateLVMLogicalVolumePhase(ctx, cl, metrics, llv, failedStatusPhase, "Not enough space on LV")
			if err != nil {
				log.Error(err, "[reconcileLLVCreateFunc] unable to updateLVMLogicalVolumePhase")
			}
			return
		}

		if freeSpace.Value() < llv.Spec.Size.Value() {
			err = errors.New("not enough space")
			log.Error(err, fmt.Sprintf("[reconcileLLVCreateFunc] the LVMLogicalVolume %s requested size is more than the Thin-pool %s free space", llv.Name, llv.Spec.Thin.PoolName))
			err = updateLVMLogicalVolumePhase(ctx, cl, metrics, llv, failedStatusPhase, "Unable to create Thin LV")
			if err != nil {
				log.Error(err, "[reconcileLLVCreateFunc] unable to updateLVMLogicalVolumePhase")
			}
			return
		}

		cmd, err := utils.CreateThinLogicalVolume(lvg.Spec.ActualVGNameOnTheNode, llv.Spec.Thin.PoolName, llv.Name, llv.Spec.Size.String())
		log.Debug(fmt.Sprintf("[reconcileLLVCreateFunc] runs cmd: %s", cmd))
		if err != nil {
			log.Error(err, "[reconcileLLVCreateFunc] unable to CreateThickLogicalVolume")
			err = updateLVMLogicalVolumePhase(ctx, cl, metrics, llv, failedStatusPhase, "Unable to create Thin LV")
			if err != nil {
				log.Error(err, "[reconcileLLVCreateFunc] unable to updateLVMLogicalVolumePhase")
			}
			return
		}
	}

	log.Info(fmt.Sprintf("[reconcileLLVCreateFunc] successfully created Logical Volume for LVMLogicalVolume, name: %s", llv.Name))

	llv.Status.Phase = createdStatusPhase
	llv.Status.ActualSize = llv.Spec.Size
	err = updateLVMLogicalVolume(ctx, metrics, cl, llv)
	if err != nil {
		log.Error(err, "[reconcileLLVCreateFunc] unable to updateLVMLogicalVolume")
		return
	}

	log.Info("[reconcileLLVCreateFunc] ends reconciliation")
}

func addLLVFinalizerIfNotExist(ctx context.Context, cl client.Client, metrics monitoring.Metrics, llv *v1alpha1.LvmLogicalVolume) (bool, error) {
	if slices.Contains(llv.Finalizers, internal.SdsNodeConfiguratorFinalizer) {
		return false, nil
	}

	llv.Finalizers = append(llv.Finalizers, internal.SdsNodeConfiguratorFinalizer)
	err := cl.Update(ctx, llv)
	if err != nil {
		return false, err
	}

	return true, nil
}

func shouldReconcileByCreateFunc(log logger.Logger, llv *v1alpha1.LvmLogicalVolume) (bool, error) {
	lvs, cmd, _, err := utils.GetAllLVs()
	log.Debug(fmt.Sprintf("[shouldReconcileByCreateFunc] runs cmd: %s", cmd))
	if err != nil {
		log.Error(err, "[shouldReconcileByCreateFunc] unable to GetAllLVs")
		return false, err
	}

	for _, lv := range lvs {
		if lv.LVName == llv.Name {
			return false, nil
		}
	}

	return true, nil
}

func getFreeLVSpace(log logger.Logger, thinPoolName string) (resource.Quantity, error) {
	lvs, cmd, _, err := utils.GetAllLVs()
	log.Debug(fmt.Sprintf("[getFreeLVSpace] runs cmd: %s", cmd))
	if err != nil {
		log.Error(err, "[getFreeVGSpace] unable to GetAllLVs")
		return resource.Quantity{}, err
	}

	for _, lv := range lvs {
		if lv.LVName == thinPoolName {
			used, err := getLVUsedSize(lv)
			vlsSize := getVirtualLVSize(lv.LVName, lvs)

			if err != nil {
				log.Error(err, "[getFreeLVSpace] unable to getLVUsedSize")
				return resource.Quantity{}, err
			}

			free := subtractQuantity(lv.LVSize, *used)
			free = subtractQuantity(free, vlsSize)

			return free, nil
		}
	}

	return resource.Quantity{}, nil
}

func getVirtualLVSize(thinPool string, lvs []internal.LVData) resource.Quantity {
	sum := int64(0)

	for _, lv := range lvs {
		if lv.PoolLv == thinPool {
			sum += lv.LVSize.Value()
		}
	}

	return *resource.NewQuantity(sum, resource.BinarySI)
}

func subtractQuantity(min, sub resource.Quantity) resource.Quantity {
	val := min
	val.Sub(sub)
	return val
}

func getFreeVGSpace(log logger.Logger, vgName string) (resource.Quantity, error) {
	vgs, cmd, _, err := utils.GetAllVGs()
	log.Debug(fmt.Sprintf("[getFreeVGSpace] runs cmd: %s", cmd))
	if err != nil {
		log.Error(err, "[getFreeVGSpace] unable to run cmd")
		return resource.Quantity{}, err
	}

	for _, vg := range vgs {
		if vg.VGName == vgName {
			return vg.VGFree, nil
		}
	}

	return resource.Quantity{}, nil
}

func getLVMLogicalVolumeType(llv *v1alpha1.LvmLogicalVolume) deviceType {
	if llv.Spec.Thin.PoolName == "" {
		return Thick
	}

	return Thin
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

func updateLVMLogicalVolumePhase(ctx context.Context, cl client.Client, metrics monitoring.Metrics, llv *v1alpha1.LvmLogicalVolume, phase, reason string) error {
	llv.Status.Phase = phase
	llv.Status.Reason = reason

	err := updateLVMLogicalVolume(ctx, metrics, cl, llv)
	if err != nil {
		return err
	}

	return nil
}

func updateLVMLogicalVolume(ctx context.Context, metrics monitoring.Metrics, cl client.Client, llv *v1alpha1.LvmLogicalVolume) error {
	err := cl.Update(ctx, llv)
	if err != nil {
		return err
	}

	return nil
}
