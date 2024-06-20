package controller

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"sds-node-configurator/api/v1alpha1"
	"sds-node-configurator/internal"
	"sds-node-configurator/pkg/logger"
	"sds-node-configurator/pkg/monitoring"
	"sds-node-configurator/pkg/utils"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/strings/slices"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

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

func getFreeThinPoolSpace(thinPools []v1alpha1.LVGStatusThinPool, poolName string) (resource.Quantity, error) {
	for _, thinPool := range thinPools {
		if thinPool.Name == poolName {
			limits := strings.Split(thinPool.AllocationLimit, "%")
			percent, err := strconv.Atoi(limits[0])
			if err != nil {
				return resource.Quantity{}, err
			}

			factor := float64(percent)
			factor = factor / 100
			free := float64(thinPool.ActualSize.Value())*factor - float64(thinPool.AllocatedSize.Value())

			// this means free value is like 10000.00000 number, so we do not need to round it up
			if isIntegral(free) {
				return *resource.NewQuantity(int64(free), resource.BinarySI), nil
			}

			// otherwise, we need to add 1 to the free value as parsing float to int rounds the value down
			return *resource.NewQuantity(int64(free)+1, resource.BinarySI), nil
		}
	}

	return resource.Quantity{}, nil
}

func isIntegral(val float64) bool {
	return val == float64(int(val))
}

func subtractQuantity(currentQuantity, quantityToSubtract resource.Quantity) resource.Quantity {
	resultingQuantity := currentQuantity.DeepCopy()
	resultingQuantity.Sub(quantityToSubtract)
	return resultingQuantity
}

func getFreeVGSpace(lvg *v1alpha1.LvmVolumeGroup) resource.Quantity {
	return subtractQuantity(lvg.Status.VGSize, lvg.Status.AllocatedSize)
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

func validateLVMLogicalVolume(ctx context.Context, cl client.Client, llv *v1alpha1.LVMLogicalVolume) (bool, string) {
	reason := strings.Builder{}

	if llv.Spec.Size.Value() == 0 {
		reason.WriteString("zero size for LV; ")
	}

	if len(llv.Spec.ActualLVNameOnTheNode) == 0 {
		reason.WriteString("no LV name specified; ")
	}

	lvg := &v1alpha1.LvmVolumeGroup{}
	err := cl.Get(ctx, client.ObjectKey{
		Name: llv.Spec.LvmVolumeGroupName,
	}, lvg)
	if err != nil {
		reason.WriteString(fmt.Sprintf("%s; ", err.Error()))
	}

	switch llv.Spec.Type {
	case Thin:
		if llv.Spec.Thin == nil {
			reason.WriteString("no thin pool specified; ")
			break
		}

		exist := false
		for _, tp := range lvg.Status.ThinPools {
			if tp.Name == llv.Spec.Thin.PoolName {
				exist = true
				break
			}
		}

		if !exist {
			reason.WriteString("selected thin pool does not exist in selected LVMVolumeGroup; ")
		}
	case Thick:
		if llv.Spec.Thin != nil {
			reason.WriteString("thin pool specified for Thick LV; ")
		}
	}

	if reason.Len() > 0 {
		return false, reason.String()
	}

	return true, ""
}

// TODO: validation if Thick с ThinPool указан
func updateLVMLogicalVolumePhaseIfNeeded(ctx context.Context, cl client.Client, log logger.Logger, metrics monitoring.Metrics, llv *v1alpha1.LVMLogicalVolume, phase, reason string) error {
	if llv.Status != nil &&
		llv.Status.Phase == phase &&
		llv.Status.Reason == reason {
		log.Debug(fmt.Sprintf("[updateLVMLogicalVolumePhaseIfNeeded] no need to update the LVMLogicalVolume %s phase and reason", llv.Name))
		return nil
	}

	if llv.Status == nil {
		llv.Status = new(v1alpha1.LVMLogicalVolumeStatus)
	}

	llv.Status.Phase = phase
	llv.Status.Reason = reason

	err := cl.Status().Update(ctx, llv)
	if err != nil {
		return err
	}

	log.Debug(fmt.Sprintf("[updateLVMLogicalVolumePhaseIfNeeded] updated LVMLogicalVolume %s status.phase to %s and reason to %s", llv.Name, phase, reason))
	return nil
}

func updateLVMLogicalVolume(ctx context.Context, metrics monitoring.Metrics, cl client.Client, llv *v1alpha1.LVMLogicalVolume) error {
	var err error

	// TODO: это не очень будет работать, потому что при распространенной ошибки при апдейте из-за устаревшей версии (resource version)
	// мы не сможем обновить ресурс никак, пока заново его не получим, а потому тут можно сразу возвращать ошибку и уходить на ретрай
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
