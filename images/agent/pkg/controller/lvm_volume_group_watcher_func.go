/*
Copyright 2023 Flant JSC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"errors"
	"fmt"
	"k8s.io/apimachinery/pkg/api/resource"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/strings/slices"
	"reflect"
	"sds-node-configurator/api/v1alpha1"
	"sds-node-configurator/internal"
	"sds-node-configurator/pkg/cache"
	"sds-node-configurator/pkg/logger"
	"sds-node-configurator/pkg/monitoring"
	"sds-node-configurator/pkg/utils"
	"strings"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

func checkIfVGExist(vgName string, vgs []internal.VGData) bool {
	for _, vg := range vgs {
		if vg.VGName == vgName {
			return true
		}
	}

	return false
}

func shouldReconcileUpdateEvent(log logger.Logger, oldLVG, newLVG *v1alpha1.LvmVolumeGroup) bool {
	if newLVG.DeletionTimestamp != nil {
		log.Debug(fmt.Sprintf("[shouldReconcileUpdateEvent] update event should be reconciled as LVMVolumeGroup %s has deletionTimestamp", newLVG.Name))
		return true
	}

	if !reflect.DeepEqual(oldLVG.Annotations, newLVG.Annotations) {
		log.Debug(fmt.Sprintf("[shouldReconcileUpdateEvent] update event should be reconciled as LVMVolumeGroup %s annotations have been changed", newLVG.Name))
		return true
	}

	if !reflect.DeepEqual(oldLVG.Spec, newLVG.Spec) {
		log.Debug(fmt.Sprintf("[shouldReconcileUpdateEvent] update event should be reconciled as LVMVolumeGroup %s configuration has been changed", newLVG.Name))
		return true
	}

	return false
}

func shouldReconcileLVGByDeleteFunc(lvg *v1alpha1.LvmVolumeGroup) bool {
	_, exist := lvg.Annotations[delAnnotation]
	if lvg.DeletionTimestamp != nil || exist {
		return true
	}

	return false
}

func updateLVGConditionIfNeeded(ctx context.Context, cl client.Client, log logger.Logger, lvg *v1alpha1.LvmVolumeGroup, status v1.ConditionStatus, conType, reason, message string) error {
	exist := false
	index := 0
	newCondition := v1.Condition{
		Type:               conType,
		Status:             status,
		ObservedGeneration: lvg.Generation,
		LastTransitionTime: v1.NewTime(time.Now()),
		Reason:             reason,
		Message:            message,
	}

	if lvg.Status.Conditions == nil {
		log.Debug(fmt.Sprintf("[updateLVGConditionIfNeeded] the LVMVolumeGroup %s conditions is nil. Initialize them", lvg.Name))
		lvg.Status.Conditions = make([]v1.Condition, 0, 5)
	}

	if len(lvg.Status.Conditions) > 0 {
		log.Debug(fmt.Sprintf("[updateLVGConditionIfNeeded] there are some conditions in the LVMVolumeGroup %s. Tries to find a condition %s", lvg.Name, conType))
		for i, c := range lvg.Status.Conditions {
			if c.Type == conType {
				if checkIfEqualConditions(c, newCondition) {
					log.Debug(fmt.Sprintf("[updateLVGConditionIfNeeded] no need to update condition %s in the LVMVolumeGroup %s as new and old condition states are the same", conType, lvg.Name))
					return nil
				}

				index = i
				exist = true
				log.Debug(fmt.Sprintf("[updateLVGConditionIfNeeded] a condition %s was found in the LVMVolumeGroup %s at the index %d", conType, lvg.Name, i))
			}
		}

		if !exist {
			log.Debug(fmt.Sprintf("[updateLVGConditionIfNeeded] a condition %s was not found. Append it in the end of the LVMVolumeGroup %s conditions", conType, lvg.Name))
			lvg.Status.Conditions = append(lvg.Status.Conditions, newCondition)
		} else {
			log.Debug(fmt.Sprintf("[updateLVGConditionIfNeeded] insert the condition %s status %s reason %s message %s at index %d of the LVMVolumeGroup %s conditions", conType, status, reason, message, index, lvg.Name))
			lvg.Status.Conditions[index] = newCondition
		}
	} else {
		log.Debug(fmt.Sprintf("[updateLVGConditionIfNeeded] no conditions were found in the LVMVolumeGroup %s. Append the condition %s in the end", lvg.Name, conType))
		lvg.Status.Conditions = append(lvg.Status.Conditions, newCondition)
	}

	log.Debug(fmt.Sprintf("[updateLVGConditionIfNeeded] tries to update the condition type %s status %s reason %s message %s of the LVMVolumeGroup %s", conType, status, reason, message, lvg.Name))
	return cl.Status().Update(ctx, lvg)
}

func checkIfEqualConditions(first, second v1.Condition) bool {
	return first.Type == second.Type &&
		first.Status == second.Status &&
		first.Reason == second.Reason &&
		first.Message == second.Message &&
		first.ObservedGeneration == second.ObservedGeneration
}

func addLVGFinalizerIfNotExist(ctx context.Context, cl client.Client, lvg *v1alpha1.LvmVolumeGroup) (bool, error) {
	if slices.Contains(lvg.Finalizers, internal.SdsNodeConfiguratorFinalizer) {
		return false, nil
	}

	lvg.Finalizers = append(lvg.Finalizers, internal.SdsNodeConfiguratorFinalizer)
	err := cl.Update(ctx, lvg)
	if err != nil {
		return false, err
	}

	return true, nil
}

func syncThinPoolsAllocationLimit(ctx context.Context, cl client.Client, log logger.Logger, lvg *v1alpha1.LvmVolumeGroup) error {
	updated := false

	tpSpecLimits := make(map[string]string, len(lvg.Spec.ThinPools))
	for _, tp := range lvg.Spec.ThinPools {
		tpSpecLimits[tp.Name] = tp.AllocationLimit
	}

	for i := range lvg.Status.ThinPools {
		if specLimits, matched := tpSpecLimits[lvg.Status.ThinPools[i].Name]; matched {
			if lvg.Status.ThinPools[i].AllocationLimit != specLimits {
				log.Debug(fmt.Sprintf("[syncThinPoolsAllocationLimit] thin-pool %s status AllocationLimit: %s of the LVMVolumeGroup %s should be updated by spec one: %s", lvg.Status.ThinPools[i].Name, lvg.Status.ThinPools[i].AllocationLimit, lvg.Name, specLimits))
				updated = true
				lvg.Status.ThinPools[i].AllocationLimit = specLimits
			}
		} else {
			log.Debug(fmt.Sprintf("[syncThinPoolsAllocationLimit] status thin-pool %s of the LVMVolumeGroup %s was not found as used in spec", lvg.Status.ThinPools[i].Name, lvg.Name))
		}
	}

	if updated {
		fmt.Printf("%+v", lvg.Status.ThinPools)
		log.Debug(fmt.Sprintf("[syncThinPoolsAllocationLimit] tries to update the LVMVolumeGroup %s", lvg.Name))
		err := cl.Status().Update(ctx, lvg)
		if err != nil {
			return err
		}
		log.Debug(fmt.Sprintf("[syncThinPoolsAllocationLimit] successfully updated the LVMVolumeGroup %s", lvg.Name))
	} else {
		log.Debug(fmt.Sprintf("[syncThinPoolsAllocationLimit] every status thin-pool AllocationLimit value is synced with spec one for the LVMVolumeGroup %s", lvg.Name))
	}

	return nil
}

func validateSpecBlockDevices(lvg *v1alpha1.LvmVolumeGroup, blockDevices map[string]v1alpha1.BlockDevice) (bool, string) {
	reason := strings.Builder{}

	targetNodeName := ""
	for _, bdName := range lvg.Spec.BlockDeviceNames {
		bd, exist := blockDevices[bdName]

		if !exist {
			reason.WriteString(fmt.Sprintf("the BlockDevice %s does not exist", bdName))
			continue
		}

		if targetNodeName == "" {
			targetNodeName = bd.Status.NodeName
		}

		if bd.Status.NodeName != targetNodeName {
			reason.WriteString(fmt.Sprintf("the BlockDevice %s has the node %s though the target node %s", bd.Name, bd.Status.NodeName, targetNodeName))
		}
	}

	if reason.Len() != 0 {
		return false, reason.String()
	}

	return true, ""
}

func checkIfLVGBelongsToNode(lvg *v1alpha1.LvmVolumeGroup, blockDevices map[string]v1alpha1.BlockDevice, nodeName string) bool {
	bd := blockDevices[lvg.Spec.BlockDeviceNames[0]]
	if bd.Status.NodeName != nodeName {
		return false
	}

	return true
}

func extractPathsFromBlockDevices(blockDevicesNames []string, blockDevices map[string]v1alpha1.BlockDevice) []string {
	paths := make([]string, 0, len(blockDevicesNames))

	for _, bdName := range blockDevicesNames {
		bd := blockDevices[bdName]
		paths = append(paths, bd.Status.Path)
	}

	return paths
}

func validateLVGForCreateFunc(log logger.Logger, lvg *v1alpha1.LvmVolumeGroup, blockDevices map[string]v1alpha1.BlockDevice) (bool, string) {
	reason := strings.Builder{}

	log.Debug(fmt.Sprintf("[validateLVGForCreateFunc] check if every selected BlockDevice of the LVMVolumeGroup %s is consumable", lvg.Name))
	// totalVGSize needs to count if there is enough space for requested thin-pools
	var totalVGSize int64
	for _, bdName := range lvg.Spec.BlockDeviceNames {
		bd := blockDevices[bdName]
		totalVGSize += bd.Status.Size.Value()

		if !bd.Status.Consumable {
			log.Warning(fmt.Sprintf("[validateLVGForCreateFunc] BlockDevice %s is not consumable", bdName))
			log.Trace(fmt.Sprintf("[validateLVGForCreateFunc] BlockDevice name: %s, status: %+v", bdName, bd.Status))
			reason.WriteString(fmt.Sprintf("BlockDevice %s is not consumable.", bdName))
		}
	}

	if reason.Len() == 0 {
		log.Debug(fmt.Sprintf("[validateLVGForCreateFunc] all BlockDevices of the LVMVolumeGroup %s are consumable", lvg.Name))
	}

	if lvg.Spec.ThinPools != nil {
		log.Debug(fmt.Sprintf("[validateLVGForCreateFunc] the LVMVolumeGroup %s has thin-pools. Validate if VG size has enough space for the thin-pools", lvg.Name))
		log.Trace(fmt.Sprintf("[validateLVGForCreateFunc] the LVMVolumeGroup %s has thin-pools %v", lvg.Name, lvg.Spec.ThinPools))
		log.Trace(fmt.Sprintf("[validateLVGForCreateFunc] total LVMVolumeGroup %s size: %d", lvg.Name, totalVGSize))

		var totalThinPoolSize int64
		for _, tp := range lvg.Spec.ThinPools {
			if tp.Size.Value() == 0 {
				reason.WriteString(fmt.Sprintf("[validateLVGForCreateFunc] thin pool %s has zero size", tp.Name))
				continue
			}

			totalThinPoolSize += tp.Size.Value()
		}
		log.Trace(fmt.Sprintf("[validateLVGForCreateFunc] LVMVolumeGroup %s thin-pools requested space: %d", lvg.Name, totalThinPoolSize))

		resizeDelta, err := resource.ParseQuantity(internal.ResizeDelta)
		if err != nil {
			log.Error(err, fmt.Sprintf("[validateLVGForCreateFunc] unable to parse the resize delta %s", internal.ResizeDelta))
		}
		log.Debug(fmt.Sprintf("[validateLVGForCreateFunc] successfully parsed the resize delta: %s", resizeDelta.String()))

		if totalThinPoolSize+resizeDelta.Value() >= totalVGSize {
			log.Trace(fmt.Sprintf("[validateLVGForCreateFunc] total thin pool size: %s, total vg size: %s", resource.NewQuantity(totalThinPoolSize, resource.BinarySI).String(), resource.NewQuantity(totalVGSize, resource.BinarySI).String()))
			log.Warning(fmt.Sprintf("[validateLVGForCreateFunc] requested thin pool size is more than VG total size for the LVMVolumeGroup %s", lvg.Name))
			reason.WriteString(fmt.Sprintf("required space for thin-pools %d is more than VG size %d", totalThinPoolSize, totalVGSize))
		}
	}

	if reason.Len() != 0 {
		return false, reason.String()
	}

	return true, ""
}

func validateLVGForUpdateFunc(log logger.Logger, lvg *v1alpha1.LvmVolumeGroup, blockDevices map[string]v1alpha1.BlockDevice, pvs []internal.PVData) (bool, string) {
	reason := strings.Builder{}

	log.Debug(fmt.Sprintf("[validateLVGForUpdateFunc] check if every new BlockDevice of the LVMVolumeGroup %s is comsumable", lvg.Name))
	actualPVPaths := make(map[string]struct{}, len(pvs))
	for _, pv := range pvs {
		actualPVPaths[pv.PVName] = struct{}{}
	}

	//TODO: add a check if BlockDevice size got less than PV size

	// Check if added BlockDevices are consumable
	// additionBlockDeviceSpace value is needed to count if VG will have enough space for thin-pools
	var additionBlockDeviceSpace int64
	for _, bdName := range lvg.Spec.BlockDeviceNames {
		specBd := blockDevices[bdName]
		if _, found := actualPVPaths[specBd.Status.Path]; !found {
			log.Debug(fmt.Sprintf("[validateLVGForUpdateFunc] unable to find the PV %s for BlockDevice %s. Check if the BlockDevice is already used", specBd.Status.Path, specBd.Name))
			for _, n := range lvg.Status.Nodes {
				for _, d := range n.Devices {
					if d.BlockDevice == specBd.Name {
						log.Warning(fmt.Sprintf("[validateLVGForUpdateFunc] BlockDevice %s misses the PV %s. That might be because the corresponding device was removed from the node. Unable to validate BlockDevices", specBd.Name, specBd.Status.Path))
						reason.WriteString(fmt.Sprintf("BlockDevice %s misses the PV %s (that might be because the device was removed from the node)", specBd.Name, specBd.Status.Path))
					}

					if reason.Len() == 0 {
						log.Debug(fmt.Sprintf("[validateLVGForUpdateFunc] BlockDevice %s does not miss a PV", d.BlockDevice))
					}
				}
			}

			log.Debug(fmt.Sprintf("[validateLVGForUpdateFunc] PV %s for BlockDevice %s of the LVMVolumeGroup %s is not created yet, check if the BlockDevice is consumable", specBd.Status.Path, bdName, lvg.Name))
			if reason.Len() > 0 {
				log.Debug("[validateLVGForUpdateFunc] some BlockDevices misses its PVs, unable to check if they are consumable")
				continue
			}

			if !blockDevices[bdName].Status.Consumable {
				reason.WriteString(fmt.Sprintf("BlockDevice %s is not consumable. ", bdName))
				continue
			}

			log.Debug(fmt.Sprintf("[validateLVGForUpdateFunc] BlockDevice %s is consumable", bdName))
			additionBlockDeviceSpace += specBd.Status.Size.Value()
		}
	}

	if lvg.Spec.ThinPools != nil {
		log.Debug(fmt.Sprintf("[validateLVGForUpdateFunc] the LVMVolumeGroup %s has thin-pools. Validate them", lvg.Name))
		usedThinPools := make(map[string]v1alpha1.StatusThinPool, len(lvg.Status.ThinPools))
		for _, tp := range lvg.Status.ThinPools {
			usedThinPools[tp.Name] = tp
		}

		// check if added thin-pools has valid requested size
		resizeDelta, err := resource.ParseQuantity(internal.ResizeDelta)
		if err != nil {
			log.Error(err, fmt.Sprintf("[validateLVGForCreateFunc] unable to parse the resize delta %s", internal.ResizeDelta))
		}
		log.Debug(fmt.Sprintf("[validateLVGForCreateFunc] successfully parsed the resize delta: %s", resizeDelta.String()))

		var addingThinPoolSize int64
		for _, specTp := range lvg.Spec.ThinPools {
			if specTp.Size.Value() == 0 {
				reason.WriteString(fmt.Sprintf("[validateLVGForCreateFunc] thin pool %s has zero size", specTp.Name))
				continue
			}

			if statusTp, used := usedThinPools[specTp.Name]; !used {
				log.Debug(fmt.Sprintf("[validateLVGForUpdateFunc] thin-pool %s of the LVMVolumeGroup %s is not used yet, check its requested size", specTp.Name, lvg.Name))
				addingThinPoolSize += specTp.Size.Value()
			} else {
				log.Debug(fmt.Sprintf("[validateLVGForUpdateFunc] thin-pool %s of the LVMVolumeGroup %s is already created, check its requested size", specTp.Name, lvg.Name))
				if specTp.Size.Value()+resizeDelta.Value() < statusTp.ActualSize.Value() {
					log.Debug(fmt.Sprintf("[validateLVGForUpdateFunc] the LVMVolumeGroup %s Spec.ThinPool %s size %s is less than Status one: %s", lvg.Name, specTp.Name, specTp.Size.String(), statusTp.ActualSize.String()))
					reason.WriteString(fmt.Sprintf("requested Spec.ThinPool %s size is less than actual one", specTp.Name))
					continue
				}

				thinPoolSizeDiff := specTp.Size.Value() - statusTp.ActualSize.Value()
				if thinPoolSizeDiff > resizeDelta.Value() {
					log.Debug(fmt.Sprintf("[validateLVGForUpdateFunc] the LVMVolumeGroup %s Spec.ThinPool %s size %s more than Status one: %s", lvg.Name, specTp.Name, specTp.Size.String(), statusTp.ActualSize.String()))
					addingThinPoolSize += thinPoolSizeDiff
				}
			}
		}

		totalFreeSpace := lvg.Status.VGSize.Value() - lvg.Status.AllocatedSize.Value() + additionBlockDeviceSpace
		log.Trace(fmt.Sprintf("[validateLVGForUpdateFunc] new LVMVolumeGroup %s thin-pools requested %d size, additional BlockDevices space %d, total: %d", lvg.Name, addingThinPoolSize, additionBlockDeviceSpace, totalFreeSpace))
		if addingThinPoolSize != 0 && addingThinPoolSize+resizeDelta.Value() > totalFreeSpace {
			reason.WriteString("added thin-pools requested sizes are more than allowed free space in VG")
		}
	}

	if reason.Len() != 0 {
		return false, reason.String()
	}

	return true, ""
}

func identifyLVGReconcileFunc(lvg *v1alpha1.LvmVolumeGroup, sdsCache *cache.Cache) reconcileType {
	if shouldReconcileLVGByCreateFunc(lvg, sdsCache) {
		return CreateReconcile
	}

	if shouldReconcileLVGByUpdateFunc(lvg, sdsCache) {
		return UpdateReconcile
	}

	if shouldReconcileLVGByDeleteFunc(lvg) {
		return DeleteReconcile
	}

	return "none"
}

func shouldReconcileLVGByCreateFunc(lvg *v1alpha1.LvmVolumeGroup, ch *cache.Cache) bool {
	_, exist := lvg.Annotations[delAnnotation]
	if lvg.DeletionTimestamp != nil || exist {
		return false
	}

	vgs, _ := ch.GetVGs()
	for _, vg := range vgs {
		if vg.VGName == lvg.Spec.ActualVGNameOnTheNode {
			return false
		}
	}

	return true
}

func shouldReconcileLVGByUpdateFunc(lvg *v1alpha1.LvmVolumeGroup, ch *cache.Cache) bool {
	_, exist := lvg.Annotations[delAnnotation]
	if lvg.DeletionTimestamp != nil || exist {
		return false
	}

	vgs, _ := ch.GetVGs()
	for _, vg := range vgs {
		if vg.VGName == lvg.Spec.ActualVGNameOnTheNode {
			return true
		}
	}

	return false
}

func ReconcileThinPoolsIfNeeded(ctx context.Context, cl client.Client, log logger.Logger, metrics monitoring.Metrics, lvg *v1alpha1.LvmVolumeGroup, vg internal.VGData, lvs []internal.LVData) error {
	actualThinPools := make(map[string]v1alpha1.StatusThinPool, len(lvg.Status.ThinPools))
	for _, tp := range lvg.Status.ThinPools {
		actualThinPools[tp.Name] = tp
	}

	lvsMap := make(map[string]struct{}, len(lvs))
	for _, lv := range lvs {
		lvsMap[lv.LVName] = struct{}{}
	}

	errs := strings.Builder{}
	for _, specTp := range lvg.Spec.ThinPools {
		if statusTp, exist := actualThinPools[specTp.Name]; !exist {
			if _, lvExist := lvsMap[specTp.Name]; lvExist {
				log.Warning(fmt.Sprintf("[ReconcileThinPoolsIfNeeded] thin-pool %s is created on the node, but isn't shown in the LVMVolumeGroup %s status. Check the status after the next resources update", specTp.Name, lvg.Name))
				continue
			}
			log.Debug(fmt.Sprintf("[ReconcileThinPoolsIfNeeded] thin-pool %s of the LVMVolumeGroup %s is not created yet. Create it", specTp.Name, lvg.Name))
			err := updateLVGConditionIfNeeded(ctx, cl, log, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonUpdating, "trying to apply the configuration")
			if err != nil {
				log.Error(err, fmt.Sprintf("[UpdateVGTagIfNeeded] unable to add the condition %s status False reason %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, internal.ReasonUpdating, lvg.Name))
				return err
			}

			start := time.Now()
			cmd, err := utils.CreateThinPool(specTp.Name, vg.VGName, specTp.Size.Value())
			metrics.UtilsCommandsDuration(LVMVolumeGroupWatcherCtrlName, "lvcreate").Observe(metrics.GetEstimatedTimeInSeconds(start))
			metrics.UtilsCommandsExecutionCount(LVMVolumeGroupWatcherCtrlName, "lvcreate").Inc()
			if err != nil {
				metrics.UtilsCommandsErrorsCount(LVMVolumeGroupWatcherCtrlName, "lvcreate").Inc()
				log.Error(err, fmt.Sprintf("[ReconcileThinPoolsIfNeeded] unable to create thin-pool %s of the LVMVolumeGroup %s, cmd: %s", specTp.Name, lvg.Name, cmd))
				errs.WriteString(fmt.Sprintf("unable to create thin-pool %s, err: %s. ", specTp.Name, err.Error()))
				continue
			}

			log.Info(fmt.Sprintf("[ReconcileThinPoolsIfNeeded] thin-pool %s of the LVMVolumeGroup %s has been successfully created", specTp.Name, lvg.Name))
		} else {
			log.Debug(fmt.Sprintf("[ReconcileThinPoolsIfNeeded] thin-pool %s of the LVMVolumeGroup %s is already created. Check its size", specTp.Name, lvg.Name))
			delta, err := resource.ParseQuantity(internal.ResizeDelta)
			if err != nil {
				log.Error(err, fmt.Sprintf("[ReconcileThinPoolsIfNeeded] unable to parse the resize delta: %s", internal.ResizeDelta))
				errs.WriteString(err.Error())
				continue
			}
			log.Debug(fmt.Sprintf("[ReconcileThinPoolsIfNeeded] successfully parsed the resize delta %s", internal.ResizeDelta))

			if utils.AreSizesEqualWithinDelta(specTp.Size, statusTp.ActualSize, delta) {
				log.Debug(fmt.Sprintf("[ReconcileThinPoolsIfNeeded] the LVMVolumeGroup %s requested thin pool %s size is equal to actual one within delta %s", lvg.Name, specTp.Size.String(), delta.String()))
				continue
			}

			log.Debug(fmt.Sprintf("[ReconcileThinPoolsIfNeeded] the LVMVolumeGroup %s requested thin pool %s size is more than actual one. Resize it", lvg.Name, specTp.Size.String()))
			err = updateLVGConditionIfNeeded(ctx, cl, log, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonUpdating, "trying to apply the configuration")
			if err != nil {
				log.Error(err, fmt.Sprintf("[UpdateVGTagIfNeeded] unable to add the condition %s status False reason %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, internal.ReasonUpdating, lvg.Name))
				return err
			}
			err = ResizeThinPool(log, metrics, lvg, specTp, statusTp)
			if err != nil {
				log.Error(err, fmt.Sprintf("[ReconcileThinPoolsIfNeeded] unable to resize thin-pool %s of the LVMVolumeGroup %s", specTp.Name, lvg.Name))
				errs.WriteString(fmt.Sprintf("unable to resize thin-pool %s, err: %s. ", specTp.Name, err.Error()))
				continue
			}
		}
	}

	if errs.Len() != 0 {
		return errors.New(errs.String())
	}

	return nil
}

func ResizePVIfNeeded(ctx context.Context, cl client.Client, log logger.Logger, metrics monitoring.Metrics, lvg *v1alpha1.LvmVolumeGroup) error {
	delta, err := resource.ParseQuantity(internal.ResizeDelta)
	if err != nil {
		log.Error(err, fmt.Sprintf("[ResizePVIfNeeded] unable to parse the resize delta: %s", internal.ResizeDelta))
	}
	log.Debug(fmt.Sprintf("[ResizePVIfNeeded] successfully parsed the resize delta %s", internal.ResizeDelta))

	if len(lvg.Status.Nodes) == 0 {
		log.Warning(fmt.Sprintf("[ResizePVIfNeeded] the LVMVolumeGroup %s nodes are empty. Wait for the next update", lvg.Name))
		return nil
	}

	errs := strings.Builder{}
	for _, n := range lvg.Status.Nodes {
		for _, d := range n.Devices {
			if d.DevSize.Value()-d.PVSize.Value() > delta.Value() {
				err = updateLVGConditionIfNeeded(ctx, cl, log, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonUpdating, "trying to apply the configuration")
				if err != nil {
					log.Error(err, fmt.Sprintf("[UpdateVGTagIfNeeded] unable to add the condition %s status False reason %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, internal.ReasonUpdating, lvg.Name))
					return err
				}
				log.Debug(fmt.Sprintf("[ResizePVIfNeeded] the LVMVolumeGroup %s BlockDevice %s PVSize is less than actual device size. Resize PV", lvg.Name, d.BlockDevice))

				start := time.Now()
				cmd, err := utils.ResizePV(d.Path)
				metrics.UtilsCommandsDuration(LVMVolumeGroupWatcherCtrlName, "pvresize").Observe(metrics.GetEstimatedTimeInSeconds(start))
				metrics.UtilsCommandsExecutionCount(LVMVolumeGroupWatcherCtrlName, "pvresize")
				if err != nil {
					metrics.UtilsCommandsErrorsCount(LVMVolumeGroupWatcherCtrlName, "pvresize").Inc()
					log.Error(err, fmt.Sprintf("[ResizePVIfNeeded] unable to resize PV %s of BlockDevice %s of LVMVolumeGroup %s, cmd: %s", d.Path, d.BlockDevice, lvg.Name, cmd))
					errs.WriteString(fmt.Sprintf("unable to resize PV %s, err: %s. ", d.Path, err.Error()))
					continue
				}

				log.Info(fmt.Sprintf("[ResizePVIfNeeded] successfully resized PV %s of BlockDevice %s of LVMVolumeGroup %s", d.Path, d.BlockDevice, lvg.Name))
			} else {
				log.Debug(fmt.Sprintf("[ResizePVIfNeeded] no need to resize PV %s of BlockDevice %s of the LVMVolumeGroup %s", d.Path, d.BlockDevice, lvg.Name))
			}
		}
	}

	if errs.Len() != 0 {
		return errors.New(errs.String())
	}

	return nil
}

func ExtendVGIfNeeded(ctx context.Context, cl client.Client, log logger.Logger, metrics monitoring.Metrics, lvg *v1alpha1.LvmVolumeGroup, vg internal.VGData, pvs []internal.PVData, blockDevices map[string]v1alpha1.BlockDevice) error {
	for _, n := range lvg.Status.Nodes {
		for _, d := range n.Devices {
			log.Trace(fmt.Sprintf("[ExtendVGIfNeeded] the LVMVolumeGroup %s status block device: %s", lvg.Name, d.BlockDevice))
		}
	}

	pvsMap := make(map[string]struct{}, len(pvs))
	for _, pv := range pvs {
		pvsMap[pv.PVName] = struct{}{}
	}

	devicesToExtend := make([]string, 0, len(lvg.Spec.BlockDeviceNames))
	for _, bdName := range lvg.Spec.BlockDeviceNames {
		bd := blockDevices[bdName]
		if _, exist := pvsMap[bd.Status.Path]; !exist {
			log.Debug(fmt.Sprintf("[ExtendVGIfNeeded] the BlockDevice %s of LVMVolumeGroup %s Spec is not counted as used", bdName, lvg.Name))
			devicesToExtend = append(devicesToExtend, bdName)
		}
	}

	if len(devicesToExtend) == 0 {
		log.Debug(fmt.Sprintf("[ExtendVGIfNeeded] VG %s of the LVMVolumeGroup %s should not be extended", vg.VGName, lvg.Name))
		return nil
	}

	err := updateLVGConditionIfNeeded(ctx, cl, log, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonUpdating, "trying to apply the configuration")
	if err != nil {
		log.Error(err, fmt.Sprintf("[UpdateVGTagIfNeeded] unable to add the condition %s status False reason %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, internal.ReasonUpdating, lvg.Name))
		return err
	}

	log.Debug(fmt.Sprintf("[ExtendVGIfNeeded] VG %s should be extended as there are some BlockDevices were added to Spec field of the LVMVolumeGroup %s", vg.VGName, lvg.Name))
	paths := extractPathsFromBlockDevices(devicesToExtend, blockDevices)
	err = ExtendVGComplex(metrics, paths, vg.VGName, log)
	if err != nil {
		log.Error(err, fmt.Sprintf("[ExtendVGIfNeeded] unable to extend VG %s of the LVMVolumeGroup %s", vg.VGName, lvg.Name))
		return err
	}
	log.Info(fmt.Sprintf("[ExtendVGIfNeeded] VG %s of the LVMVolumeGroup %s was extended", vg.VGName, lvg.Name))

	return nil
}

func tryGetVG(sdsCache *cache.Cache, vgName string) (bool, internal.VGData) {
	vgs, _ := sdsCache.GetVGs()
	for _, vg := range vgs {
		if vg.VGName == vgName {
			return true, vg
		}
	}

	return false, internal.VGData{}
}

func removeLVGFinalizerIfExist(ctx context.Context, cl client.Client, lvg *v1alpha1.LvmVolumeGroup) (bool, error) {
	if !slices.Contains(lvg.Finalizers, internal.SdsNodeConfiguratorFinalizer) {
		return false, nil
	}

	for i := range lvg.Finalizers {
		if lvg.Finalizers[i] == internal.SdsNodeConfiguratorFinalizer {
			lvg.Finalizers = append(lvg.Finalizers[:i], lvg.Finalizers[i+1:]...)
			break
		}
	}

	err := cl.Update(ctx, lvg)
	if err != nil {
		return false, err
	}

	return true, nil
}

func checkIfVGHasLV(ch *cache.Cache, vgName string) []string {
	lvs, _ := ch.GetLVs()
	usedLVs := make([]string, 0, len(lvs))
	for _, lv := range lvs {
		if lv.VGName == vgName {
			usedLVs = append(usedLVs, lv.LVName)
		}
	}

	return usedLVs
}

func getLVMVolumeGroup(ctx context.Context, cl client.Client, metrics monitoring.Metrics, namespace, name string) (*v1alpha1.LvmVolumeGroup, error) {
	obj := &v1alpha1.LvmVolumeGroup{}
	start := time.Now()
	err := cl.Get(ctx, client.ObjectKey{
		Name:      name,
		Namespace: namespace,
	}, obj)
	metrics.ApiMethodsDuration(LVMVolumeGroupWatcherCtrlName, "get").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.ApiMethodsExecutionCount(LVMVolumeGroupWatcherCtrlName, "get").Inc()
	if err != nil {
		metrics.ApiMethodsErrors(LVMVolumeGroupWatcherCtrlName, "get").Inc()
		return nil, err
	}
	return obj, nil
}

func DeleteVGIfExist(log logger.Logger, metrics monitoring.Metrics, sdsCache *cache.Cache, vgName string) error {
	vgs, _ := sdsCache.GetVGs()
	if !checkIfVGExist(vgName, vgs) {
		log.Debug(fmt.Sprintf("[DeleteVGIfExist] no VG %s found, nothing to delete", vgName))
		return nil
	}

	pvs, _ := sdsCache.GetPVs()
	if len(pvs) == 0 {
		err := errors.New("no any PV found")
		log.Error(err, fmt.Sprintf("[DeleteVGIfExist] no any PV was found while deleting VG %s", vgName))
		return err
	}

	start := time.Now()
	command, err := utils.RemoveVG(vgName)
	metrics.UtilsCommandsDuration(LVMVolumeGroupWatcherCtrlName, "vgremove").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.UtilsCommandsExecutionCount(LVMVolumeGroupWatcherCtrlName, "vgremove").Inc()
	log.Debug(command)
	if err != nil {
		metrics.UtilsCommandsErrorsCount(LVMVolumeGroupWatcherCtrlName, "vgremove").Inc()
		log.Error(err, "RemoveVG "+command)
		return err
	}

	var pvsToRemove []string
	for _, pv := range pvs {
		if pv.VGName == vgName {
			pvsToRemove = append(pvsToRemove, pv.PVName)
		}
	}

	start = time.Now()
	command, err = utils.RemovePV(pvsToRemove)
	metrics.UtilsCommandsDuration(LVMVolumeGroupWatcherCtrlName, "pvremove").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.UtilsCommandsExecutionCount(LVMVolumeGroupWatcherCtrlName, "pvremove").Inc()
	log.Debug(command)
	if err != nil {
		metrics.UtilsCommandsErrorsCount(LVMVolumeGroupWatcherCtrlName, "pvremove").Inc()
		log.Error(err, "RemovePV "+command)
		return err
	}

	return nil
}

func ExtendVGComplex(metrics monitoring.Metrics, extendPVs []string, VGName string, log logger.Logger) error {
	for _, pvPath := range extendPVs {
		start := time.Now()
		command, err := utils.CreatePV(pvPath)
		metrics.UtilsCommandsDuration(LVMVolumeGroupWatcherCtrlName, "pvcreate").Observe(metrics.GetEstimatedTimeInSeconds(start))
		metrics.UtilsCommandsExecutionCount(LVMVolumeGroupWatcherCtrlName, "pvcreate").Inc()
		log.Debug(command)
		if err != nil {
			metrics.UtilsCommandsErrorsCount(LVMVolumeGroupWatcherCtrlName, "pvcreate").Inc()
			log.Error(err, "CreatePV ")
			return err
		}
	}

	start := time.Now()
	command, err := utils.ExtendVG(VGName, extendPVs)
	metrics.UtilsCommandsDuration(LVMVolumeGroupWatcherCtrlName, "vgextend").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.UtilsCommandsExecutionCount(LVMVolumeGroupWatcherCtrlName, "vgextend").Inc()
	log.Debug(command)
	if err != nil {
		metrics.UtilsCommandsErrorsCount(LVMVolumeGroupWatcherCtrlName, "vgextend").Inc()
		log.Error(err, "ExtendVG ")
		return err
	}
	return nil
}

func CreateVGComplex(metrics monitoring.Metrics, log logger.Logger, lvg *v1alpha1.LvmVolumeGroup, blockDevices map[string]v1alpha1.BlockDevice) error {
	paths := extractPathsFromBlockDevices(lvg.Spec.BlockDeviceNames, blockDevices)

	log.Trace(fmt.Sprintf("[CreateVGComplex] LVMVolumeGroup %s devices paths %v", lvg.Name, paths))
	for _, path := range paths {
		start := time.Now()
		command, err := utils.CreatePV(path)
		metrics.UtilsCommandsDuration(LVMVolumeGroupWatcherCtrlName, "pvcreate").Observe(metrics.GetEstimatedTimeInSeconds(start))
		metrics.UtilsCommandsExecutionCount(LVMVolumeGroupWatcherCtrlName, "pvcreate").Inc()
		log.Debug(command)
		if err != nil {
			metrics.UtilsCommandsErrorsCount(LVMVolumeGroupWatcherCtrlName, "pvcreate").Inc()
			log.Error(err, fmt.Sprintf("[CreateVGComplex] unable to create PV by path %s", path))
			return err
		}
	}

	log.Debug(fmt.Sprintf("[CreateVGComplex] successfully created all PVs for the LVMVolumeGroup %s", lvg.Name))
	log.Debug(fmt.Sprintf("[CreateVGComplex] the LVMVolumeGroup %s type is %s", lvg.Name, lvg.Spec.Type))
	switch lvg.Spec.Type {
	case Local:
		start := time.Now()
		cmd, err := utils.CreateVGLocal(lvg.Spec.ActualVGNameOnTheNode, lvg.Name, paths)
		metrics.UtilsCommandsDuration(LVMVolumeGroupWatcherCtrlName, "vgcreate").Observe(metrics.GetEstimatedTimeInSeconds(start))
		metrics.UtilsCommandsExecutionCount(LVMVolumeGroupWatcherCtrlName, "vgcreate").Inc()
		log.Debug(cmd)
		if err != nil {
			metrics.UtilsCommandsErrorsCount(LVMVolumeGroupWatcherCtrlName, "vgcreate").Inc()
			log.Error(err, "error CreateVGLocal")
			return err
		}
	case Shared:
		start := time.Now()
		cmd, err := utils.CreateVGShared(lvg.Spec.ActualVGNameOnTheNode, lvg.Name, paths)
		metrics.UtilsCommandsDuration(LVMVolumeGroupWatcherCtrlName, "vgcreate").Observe(metrics.GetEstimatedTimeInSeconds(start))
		metrics.UtilsCommandsExecutionCount(LVMVolumeGroupWatcherCtrlName, "vgcreate").Inc()
		log.Debug(cmd)
		if err != nil {
			metrics.UtilsCommandsErrorsCount(LVMVolumeGroupWatcherCtrlName, "vgcreate").Inc()
			log.Error(err, "error CreateVGShared")
			return err
		}
	}

	log.Debug(fmt.Sprintf("[CreateVGComplex] successfully create VG %s of the LVMVolumeGroup %s", lvg.Spec.ActualVGNameOnTheNode, lvg.Name))

	return nil
}

func UpdateVGTagIfNeeded(ctx context.Context, cl client.Client, log logger.Logger, metrics monitoring.Metrics, lvg *v1alpha1.LvmVolumeGroup, vg internal.VGData) (bool, error) {
	found, tagName := CheckTag(vg.VGTags)
	if found && lvg.Name != tagName {
		err := updateLVGConditionIfNeeded(ctx, cl, log, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonUpdating, "trying to apply the configuration")
		if err != nil {
			log.Error(err, fmt.Sprintf("[UpdateVGTagIfNeeded] unable to add the condition %s status False reason %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, internal.ReasonUpdating, lvg.Name))
			return false, err
		}

		start := time.Now()
		cmd, err := utils.VGChangeDelTag(vg.VGName, fmt.Sprintf("%s=%s", LVMVolumeGroupTag, tagName))
		metrics.UtilsCommandsDuration(LVMVolumeGroupWatcherCtrlName, "vgchange").Observe(metrics.GetEstimatedTimeInSeconds(start))
		metrics.UtilsCommandsExecutionCount(LVMVolumeGroupWatcherCtrlName, "vgchange").Inc()
		log.Debug(fmt.Sprintf("[UpdateVGTagIfNeeded] exec cmd: %s", cmd))
		if err != nil {
			log.Error(err, fmt.Sprintf("[UpdateVGTagIfNeeded] unable to delete LVMVolumeGroupTag: %s=%s, vg: %s", LVMVolumeGroupTag, tagName, vg.VGName))
			metrics.UtilsCommandsErrorsCount(LVMVolumeGroupWatcherCtrlName, "vgchange").Inc()
			return false, err
		}

		start = time.Now()
		cmd, err = utils.VGChangeAddTag(vg.VGName, fmt.Sprintf("%s=%s", LVMVolumeGroupTag, lvg.Name))
		metrics.UtilsCommandsDuration(LVMVolumeGroupWatcherCtrlName, "vgchange").Observe(metrics.GetEstimatedTimeInSeconds(start))
		metrics.UtilsCommandsExecutionCount(LVMVolumeGroupWatcherCtrlName, "vgchange").Inc()
		log.Debug(fmt.Sprintf("[UpdateVGTagIfNeeded] exec cmd: %s", cmd))
		if err != nil {
			log.Error(err, fmt.Sprintf("[UpdateVGTagIfNeeded] unable to add LVMVolumeGroupTag: %s=%s, vg: %s", LVMVolumeGroupTag, lvg.Name, vg.VGName))
			metrics.UtilsCommandsErrorsCount(LVMVolumeGroupWatcherCtrlName, "vgchange").Inc()
			return false, err
		}

		return true, nil
	}

	return false, nil
}

func ResizeThinPool(log logger.Logger, metrics monitoring.Metrics, lvg *v1alpha1.LvmVolumeGroup, specThinPool v1alpha1.SpecThinPool, statusThinPool v1alpha1.StatusThinPool) error {
	volumeGroupFreeSpaceBytes := lvg.Status.VGSize.Value() - lvg.Status.AllocatedSize.Value()
	addSizeBytes := specThinPool.Size.Value() - statusThinPool.ActualSize.Value()

	log.Trace(fmt.Sprintf("[ResizeThinPool] volumeGroupSize = %s", lvg.Status.VGSize.String()))
	log.Trace(fmt.Sprintf("[ResizeThinPool] volumeGroupAllocatedSize = %s", lvg.Status.AllocatedSize.String()))
	log.Trace(fmt.Sprintf("[ResizeThinPool] volumeGroupFreeSpaceBytes = %d", volumeGroupFreeSpaceBytes))
	log.Trace(fmt.Sprintf("[ResizeThinPool] addSizeBytes = %d", addSizeBytes))

	log.Info(fmt.Sprintf("[ResizeThinPool] Start resizing thin pool: %s; with new size: %s", specThinPool.Name, specThinPool.Size.String()))

	start := time.Now()
	cmd, err := utils.ExtendLV(specThinPool.Size.Value(), lvg.Spec.ActualVGNameOnTheNode, specThinPool.Name)
	metrics.UtilsCommandsDuration(LVMVolumeGroupWatcherCtrlName, "lvextend").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.UtilsCommandsExecutionCount(LVMVolumeGroupWatcherCtrlName, "lvextend").Inc()
	if err != nil {
		metrics.UtilsCommandsErrorsCount(LVMVolumeGroupWatcherCtrlName, "lvextend").Inc()
		log.Error(err, fmt.Sprintf("[ResizeThinPool] unable to extend LV, name: %s, cmd: %s", specThinPool.Name, cmd))
		return err
	}

	return nil

}
