package controller

import (
	"context"
	"fmt"
	"sds-node-configurator/pkg/cache"
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

func identifyReconcileFunc(sdsCache *cache.Cache, vgName string, llv *v1alpha1.LVMLogicalVolume) reconcileType {
	should := shouldReconcileByCreateFunc(sdsCache, vgName, llv)
	if should {
		return CreateReconcile
	}

	should = shouldReconcileByUpdateFunc(sdsCache, vgName, llv)
	if should {
		return UpdateReconcile
	}

	should = shouldReconcileByDeleteFunc(llv)
	if should {
		return DeleteReconcile
	}

	return ""
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

func checkIfLVBelongsToLLV(llv *v1alpha1.LVMLogicalVolume, lv *internal.LVData) bool {
	switch llv.Spec.Type {
	case Thin:
		if lv.PoolName != llv.Spec.Thin.PoolName {
			return false
		}
	case Thick:
		contiguous := string(lv.LVAttr[2]) == "c"
		if string(lv.LVAttr[0]) != "-" ||
			contiguous != isContiguous(llv) {
			return false
		}
	}

	return true
}

func deleteLVIfExists(log logger.Logger, sdsCache *cache.Cache, vgName string, llv *v1alpha1.LVMLogicalVolume) error {
	lv := FindLV(sdsCache, vgName, llv.Spec.ActualLVNameOnTheNode)
	if lv == nil {
		log.Warning(fmt.Sprintf("[deleteLVIfExists] did not find LV %s in VG %s", llv.Spec.ActualLVNameOnTheNode, vgName))
		return nil
	}

	// this case prevents unexpected same-name LV deletions which does not actually belong to our LLV
	if !checkIfLVBelongsToLLV(llv, lv) {
		log.Warning(fmt.Sprintf("[deleteLVIfExists] no need to delete LV %s as it doesnt belong to LVMLogicalVolume %s", lv.LVName, llv.Name))
		return nil
	}

	cmd, err := utils.RemoveLV(vgName, llv.Spec.ActualLVNameOnTheNode)
	log.Debug(fmt.Sprintf("[deleteLVIfExists] runs cmd: %s", cmd))
	if err != nil {
		log.Error(err, fmt.Sprintf("[deleteLVIfExists] unable to remove LV %s from VG %s", llv.Spec.ActualLVNameOnTheNode, vgName))
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

func shouldReconcileByCreateFunc(sdsCache *cache.Cache, vgName string, llv *v1alpha1.LVMLogicalVolume) bool {
	if llv.DeletionTimestamp != nil {
		return false
	}

	lv := FindLV(sdsCache, vgName, llv.Spec.ActualLVNameOnTheNode)
	if lv != nil {
		return false
	}

	return true
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

func validateLVMLogicalVolume(sdsCache *cache.Cache, llv *v1alpha1.LVMLogicalVolume, lvg *v1alpha1.LvmVolumeGroup, delta resource.Quantity) (bool, string) {
	if llv.DeletionTimestamp != nil {
		// as the configuration doesn't matter if we want to delete it
		return true, ""
	}

	reason := strings.Builder{}

	if len(llv.Spec.ActualLVNameOnTheNode) == 0 {
		reason.WriteString("no LV name specified; ")
	}

	if llv.Spec.Size.Value() == 0 {
		reason.WriteString("zero size for LV; ")
	}

	if llv.Status != nil {
		if llv.Spec.Size.Value()+delta.Value() < llv.Status.ActualSize.Value() {
			reason.WriteString("desired LV size is less than actual one")
		}
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

		// if a specified Thin LV name matches the existing Thick one
		lv := FindLV(sdsCache, lvg.Spec.ActualVGNameOnTheNode, llv.Spec.ActualLVNameOnTheNode)
		if lv != nil && lv.PoolName != llv.Spec.Thin.PoolName {
			reason.WriteString(fmt.Sprintf("specified LV %s is already created and does not belong to selected thin pool %s", lv.LVName, llv.Spec.Thin.PoolName))
		}
	case Thick:
		if llv.Spec.Thin != nil {
			reason.WriteString("thin pool specified for Thick LV; ")
		}

		// if a specified Thick LV name matches the existing Thin one
		lv := FindLV(sdsCache, lvg.Spec.ActualVGNameOnTheNode, llv.Spec.ActualLVNameOnTheNode)
		if lv != nil && len(lv.LVAttr) == 0 {
			reason.WriteString(fmt.Sprintf("LV %s was found on the node, but can't be validated due to its attributes is empty string", lv.LVName))
			break
		}

		if lv != nil {
			if string(lv.LVAttr[0]) != "-" {
				reason.WriteString(fmt.Sprintf("specified LV %s is already created and it is not Thick", lv.LVName))
			}

			contiguous := string(lv.LVAttr[2]) == "c"
			if contiguous != isContiguous(llv) {
				reason.WriteString(fmt.Sprintf("specified LV %s is already created and its contiguous state does not match the specified one", lv.LVName))
			}
		}
	}

	if reason.Len() > 0 {
		return false, reason.String()
	}

	return true, ""
}

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

	log.Debug(fmt.Sprintf("[updateLVMLogicalVolumePhaseIfNeeded] tries to update the LVMLogicalVolume %s status with phase: %s, reason: %s", llv.Name, phase, reason))
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

func FindLV(sdsCache *cache.Cache, vgName, lvName string) *internal.LVData {
	lvs, _ := sdsCache.GetLVs()
	for _, lv := range lvs {
		if lv.VGName == vgName && lv.LVName == lvName {
			return &lv
		}
	}

	return nil
}

func shouldReconcileByUpdateFunc(sdsCache *cache.Cache, vgName string, llv *v1alpha1.LVMLogicalVolume) bool {
	if llv.DeletionTimestamp != nil {
		return false
	}

	lv := FindLV(sdsCache, vgName, llv.Spec.ActualLVNameOnTheNode)
	if lv == nil {
		return false
	}

	return true
}

func isContiguous(llv *v1alpha1.LVMLogicalVolume) bool {
	if llv.Spec.Thick == nil {
		return false
	}

	return *llv.Spec.Thick.Contiguous
}
