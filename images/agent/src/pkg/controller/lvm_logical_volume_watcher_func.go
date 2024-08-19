package controller

import (
	"context"
	"fmt"
	"strings"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/api/resource"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/strings/slices"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"agent/internal"
	"agent/pkg/cache"
	"agent/pkg/logger"
	"agent/pkg/monitoring"
	"agent/pkg/utils"
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
	return llv.DeletionTimestamp != nil
}

//nolint:unparam
func checkIfConditionIsTrue(lvg *v1alpha1.LvmVolumeGroup, conType string) bool {
	// this check prevents infinite resource updating after a retry
	for _, c := range lvg.Status.Conditions {
		if c.Type == conType && c.Status == v1.ConditionTrue {
			return true
		}
	}

	return false
}

func isPercentSize(size string) bool {
	return strings.Contains(size, "%")
}

func getLLVRequestedSize(llv *v1alpha1.LVMLogicalVolume, lvg *v1alpha1.LvmVolumeGroup) (resource.Quantity, error) {
	switch llv.Spec.Type {
	case Thick:
		return getRequestedSizeFromString(llv.Spec.Size, lvg.Status.VGSize)
	case Thin:
		for _, tp := range lvg.Status.ThinPools {
			if tp.Name == llv.Spec.Thin.PoolName {
				totalSize, err := getThinPoolSpaceWithAllocationLimit(tp.ActualSize, tp.AllocationLimit)
				if err != nil {
					return resource.Quantity{}, err
				}

				return getRequestedSizeFromString(llv.Spec.Size, totalSize)
			}
		}
	}
	return resource.Quantity{}, nil
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
		err := updateLVMLogicalVolumeSpec(ctx, metrics, cl, llv)
		if err != nil {
			log.Error(err, fmt.Sprintf("[updateLVMLogicalVolumeSpec] unable to update the LVMVolumeGroup %s", llv.Name))
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

func updateLLVPhaseToCreatedIfNeeded(ctx context.Context, cl client.Client, llv *v1alpha1.LVMLogicalVolume, actualSize resource.Quantity) (bool, error) {
	var contiguous *bool
	if llv.Spec.Thick != nil {
		if *llv.Spec.Thick.Contiguous {
			contiguous = llv.Spec.Thick.Contiguous
		}
	}

	if llv.Status.Phase != LLVStatusPhaseCreated ||
		llv.Status.ActualSize.Value() != actualSize.Value() ||
		llv.Status.Reason != "" ||
		llv.Status.Contiguous != contiguous {
		llv.Status.Phase = LLVStatusPhaseCreated
		llv.Status.Reason = ""
		llv.Status.ActualSize = actualSize
		llv.Status.Contiguous = contiguous
		err := cl.Status().Update(ctx, llv)
		if err != nil {
			return false, err
		}

		return true, err
	}

	return false, nil
}

func deleteLVIfNeeded(log logger.Logger, sdsCache *cache.Cache, vgName string, llv *v1alpha1.LVMLogicalVolume) error {
	lv := sdsCache.FindLV(vgName, llv.Spec.ActualLVNameOnTheNode)
	if lv == nil {
		log.Warning(fmt.Sprintf("[deleteLVIfNeeded] did not find LV %s in VG %s", llv.Spec.ActualLVNameOnTheNode, vgName))
		return nil
	}

	// this case prevents unexpected same-name LV deletions which does not actually belong to our LLV
	if !checkIfLVBelongsToLLV(llv, lv) {
		log.Warning(fmt.Sprintf("[deleteLVIfNeeded] no need to delete LV %s as it doesnt belong to LVMLogicalVolume %s", lv.LVName, llv.Name))
		return nil
	}

	cmd, err := utils.RemoveLV(vgName, llv.Spec.ActualLVNameOnTheNode)
	log.Debug(fmt.Sprintf("[deleteLVIfNeeded] runs cmd: %s", cmd))
	if err != nil {
		log.Error(err, fmt.Sprintf("[deleteLVIfNeeded] unable to remove LV %s from VG %s", llv.Spec.ActualLVNameOnTheNode, vgName))
		return err
	}

	return nil
}

func getLVActualSize(sdsCache *cache.Cache, vgName, lvName string) resource.Quantity {
	lv := sdsCache.FindLV(vgName, lvName)
	if lv == nil {
		return resource.Quantity{}
	}

	result := resource.NewQuantity(lv.LVSize.Value(), resource.BinarySI)

	return *result
}

func addLLVFinalizerIfNotExist(ctx context.Context, cl client.Client, log logger.Logger, metrics monitoring.Metrics, llv *v1alpha1.LVMLogicalVolume) (bool, error) {
	if slices.Contains(llv.Finalizers, internal.SdsNodeConfiguratorFinalizer) {
		return false, nil
	}

	llv.Finalizers = append(llv.Finalizers, internal.SdsNodeConfiguratorFinalizer)

	log.Trace(fmt.Sprintf("[addLLVFinalizerIfNotExist] added finalizer %s to the LVMLogicalVolume %s", internal.SdsNodeConfiguratorFinalizer, llv.Name))
	err := updateLVMLogicalVolumeSpec(ctx, metrics, cl, llv)
	if err != nil {
		return false, err
	}

	return true, nil
}

func shouldReconcileByCreateFunc(sdsCache *cache.Cache, vgName string, llv *v1alpha1.LVMLogicalVolume) bool {
	if llv.DeletionTimestamp != nil {
		return false
	}

	lv := sdsCache.FindLV(vgName, llv.Spec.ActualLVNameOnTheNode)
	return lv == nil
}

func getFreeLVGSpaceForLLV(lvg *v1alpha1.LvmVolumeGroup, llv *v1alpha1.LVMLogicalVolume) resource.Quantity {
	switch llv.Spec.Type {
	case Thick:
		return lvg.Status.VGFree
	case Thin:
		for _, tp := range lvg.Status.ThinPools {
			if tp.Name == llv.Spec.Thin.PoolName {
				return tp.AvailableSpace
			}
		}
	}

	return resource.Quantity{}
}

func subtractQuantity(currentQuantity, quantityToSubtract resource.Quantity) resource.Quantity {
	resultingQuantity := currentQuantity.DeepCopy()
	resultingQuantity.Sub(quantityToSubtract)
	return resultingQuantity
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

func validateLVMLogicalVolume(sdsCache *cache.Cache, llv *v1alpha1.LVMLogicalVolume, lvg *v1alpha1.LvmVolumeGroup) (bool, string) {
	if llv.DeletionTimestamp != nil {
		// as the configuration doesn't matter if we want to delete it
		return true, ""
	}

	reason := strings.Builder{}

	if len(llv.Spec.ActualLVNameOnTheNode) == 0 {
		reason.WriteString("No LV name specified. ")
	}

	llvRequestedSize, err := getLLVRequestedSize(llv, lvg)
	if err != nil {
		reason.WriteString(err.Error())
	}

	if llvRequestedSize.Value() == 0 {
		reason.WriteString("Zero size for LV. ")
	}

	if llv.Status != nil {
		if llvRequestedSize.Value()+internal.ResizeDelta.Value() < llv.Status.ActualSize.Value() {
			reason.WriteString("Desired LV size is less than actual one. ")
		}
	}

	switch llv.Spec.Type {
	case Thin:
		if llv.Spec.Thin == nil {
			reason.WriteString("No thin pool specified. ")
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
			reason.WriteString("Selected thin pool does not exist in selected LVMVolumeGroup. ")
		}
	case Thick:
		if llv.Spec.Thin != nil {
			reason.WriteString("Thin pool specified for Thick LV. ")
		}
	}

	// if a specified Thick LV name matches the existing Thin one
	lv := sdsCache.FindLV(lvg.Spec.ActualVGNameOnTheNode, llv.Spec.ActualLVNameOnTheNode)
	if lv != nil {
		if len(lv.LVAttr) == 0 {
			reason.WriteString(fmt.Sprintf("LV %s was found on the node, but can't be validated due to its attributes is empty string. ", lv.LVName))
		} else if !checkIfLVBelongsToLLV(llv, lv) {
			reason.WriteString(fmt.Sprintf("Specified LV %s is already created and it is doesnt match the one on the node.", lv.LVName))
		}
	}

	if reason.Len() > 0 {
		return false, reason.String()
	}

	return true, ""
}

func updateLVMLogicalVolumePhaseIfNeeded(ctx context.Context, cl client.Client, log logger.Logger, _ monitoring.Metrics, llv *v1alpha1.LVMLogicalVolume, phase, reason string) error {
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

func updateLVMLogicalVolumeSpec(ctx context.Context, _ monitoring.Metrics, cl client.Client, llv *v1alpha1.LVMLogicalVolume) error {
	return cl.Update(ctx, llv)
}

func shouldReconcileByUpdateFunc(sdsCache *cache.Cache, vgName string, llv *v1alpha1.LVMLogicalVolume) bool {
	if llv.DeletionTimestamp != nil {
		return false
	}

	lv := sdsCache.FindLV(vgName, llv.Spec.ActualLVNameOnTheNode)
	return lv != nil
}

func isContiguous(llv *v1alpha1.LVMLogicalVolume) bool {
	if llv.Spec.Thick == nil {
		return false
	}

	return *llv.Spec.Thick.Contiguous
}
