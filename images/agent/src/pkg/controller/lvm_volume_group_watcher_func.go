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
	"agent/config"
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

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

func DeleteLVMVolumeGroup(ctx context.Context, cl client.Client, log logger.Logger, metrics monitoring.Metrics, lvg *v1alpha1.LVMVolumeGroup, currentNode string) error {
	log.Debug(fmt.Sprintf(`[DeleteLVMVolumeGroup] Node "%s" does not belong to VG "%s". It will be removed from LVM resource, name "%s"'`, currentNode, lvg.Spec.ActualVGNameOnTheNode, lvg.Name))
	for i, node := range lvg.Status.Nodes {
		if node.Name == currentNode {
			// delete node
			lvg.Status.Nodes = append(lvg.Status.Nodes[:i], lvg.Status.Nodes[i+1:]...)
			log.Info(fmt.Sprintf(`[DeleteLVMVolumeGroup] deleted node "%s" from LVMVolumeGroup "%s"`, node.Name, lvg.Name))
		}
	}

	// If current LVMVolumeGroup has no nodes left, delete it.
	if len(lvg.Status.Nodes) == 0 {
		start := time.Now()
		err := cl.Delete(ctx, lvg)
		metrics.APIMethodsDuration(LVMVolumeGroupDiscoverCtrlName, "delete").Observe(metrics.GetEstimatedTimeInSeconds(start))
		metrics.APIMethodsExecutionCount(LVMVolumeGroupDiscoverCtrlName, "delete").Inc()
		if err != nil {
			metrics.APIMethodsErrors(LVMVolumeGroupDiscoverCtrlName, "delete").Inc()
			return err
		}
		log.Info(fmt.Sprintf("[DeleteLVMVolumeGroup] the LVMVolumeGroup %s deleted", lvg.Name))
	}

	return nil
}

func checkIfVGExist(vgName string, vgs []internal.VGData) bool {
	for _, vg := range vgs {
		if vg.VGName == vgName {
			return true
		}
	}

	return false
}

func shouldUpdateLVGLabels(log logger.Logger, lvg *v1alpha1.LVMVolumeGroup, labelKey, labelValue string) bool {
	if lvg.Labels == nil {
		log.Debug(fmt.Sprintf("[shouldUpdateLVGLabels] the LVMVolumeGroup %s has no labels.", lvg.Name))
		return true
	}

	val, exist := lvg.Labels[labelKey]
	if !exist {
		log.Debug(fmt.Sprintf("[shouldUpdateLVGLabels] the LVMVolumeGroup %s has no label %s.", lvg.Name, labelKey))
		return true
	}

	if val != labelValue {
		log.Debug(fmt.Sprintf("[shouldUpdateLVGLabels] the LVMVolumeGroup %s has label %s but the value is incorrect - %s (should be %s)", lvg.Name, labelKey, val, labelValue))
		return true
	}

	return false
}

func shouldLVGWatcherReconcileUpdateEvent(log logger.Logger, oldLVG, newLVG *v1alpha1.LVMVolumeGroup) bool {
	if newLVG.DeletionTimestamp != nil {
		log.Debug(fmt.Sprintf("[shouldLVGWatcherReconcileUpdateEvent] update event should be reconciled as the LVMVolumeGroup %s has deletionTimestamp", newLVG.Name))
		return true
	}

	if shouldUpdateLVGLabels(log, newLVG, LVGMetadateNameLabelKey, newLVG.Name) {
		log.Debug(fmt.Sprintf("[shouldLVGWatcherReconcileUpdateEvent] update event should be reconciled as the LVMVolumeGroup's %s labels have been changed", newLVG.Name))
		return true
	}

	if !reflect.DeepEqual(oldLVG.Spec, newLVG.Spec) {
		log.Debug(fmt.Sprintf("[shouldLVGWatcherReconcileUpdateEvent] update event should be reconciled as the LVMVolumeGroup %s configuration has been changed", newLVG.Name))
		return true
	}

	for _, c := range newLVG.Status.Conditions {
		if c.Type == internal.TypeVGConfigurationApplied {
			if c.Reason == internal.ReasonUpdating || c.Reason == internal.ReasonCreating {
				log.Debug(fmt.Sprintf("[shouldLVGWatcherReconcileUpdateEvent] update event should not be reconciled as the LVMVolumeGroup %s reconciliation still in progress", newLVG.Name))
				return false
			}
		}
	}

	for _, n := range newLVG.Status.Nodes {
		for _, d := range n.Devices {
			if !utils.AreSizesEqualWithinDelta(d.PVSize, d.DevSize, internal.ResizeDelta) {
				log.Debug(fmt.Sprintf("[shouldLVGWatcherReconcileUpdateEvent] update event should be reconciled as the LVMVolumeGroup %s PV size is different to device size", newLVG.Name))
				return true
			}
		}
	}

	return false
}

func shouldReconcileLVGByDeleteFunc(lvg *v1alpha1.LVMVolumeGroup) bool {
	return lvg.DeletionTimestamp != nil
}

func updateLVGConditionIfNeeded(ctx context.Context, cl client.Client, log logger.Logger, lvg *v1alpha1.LVMVolumeGroup, status v1.ConditionStatus, conType, reason, message string) error {
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

func addLVGFinalizerIfNotExist(ctx context.Context, cl client.Client, lvg *v1alpha1.LVMVolumeGroup) (bool, error) {
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

func syncThinPoolsAllocationLimit(ctx context.Context, cl client.Client, log logger.Logger, lvg *v1alpha1.LVMVolumeGroup) error {
	updated := false

	tpSpecLimits := make(map[string]string, len(lvg.Spec.ThinPools))
	for _, tp := range lvg.Spec.ThinPools {
		tpSpecLimits[tp.Name] = tp.AllocationLimit
	}

	var (
		space resource.Quantity
		err   error
	)
	for i := range lvg.Status.ThinPools {
		if specLimits, matched := tpSpecLimits[lvg.Status.ThinPools[i].Name]; matched {
			if lvg.Status.ThinPools[i].AllocationLimit != specLimits {
				log.Debug(fmt.Sprintf("[syncThinPoolsAllocationLimit] thin-pool %s status AllocationLimit: %s of the LVMVolumeGroup %s should be updated by spec one: %s", lvg.Status.ThinPools[i].Name, lvg.Status.ThinPools[i].AllocationLimit, lvg.Name, specLimits))
				updated = true
				lvg.Status.ThinPools[i].AllocationLimit = specLimits

				space, err = getThinPoolAvailableSpace(lvg.Status.ThinPools[i].ActualSize, lvg.Status.ThinPools[i].AllocatedSize, specLimits)
				if err != nil {
					log.Error(err, fmt.Sprintf("[syncThinPoolsAllocationLimit] unable to get thin pool %s available space", lvg.Status.ThinPools[i].Name))
					return err
				}
				log.Debug(fmt.Sprintf("[syncThinPoolsAllocationLimit] successfully got a new available space %s of the thin-pool %s", space.String(), lvg.Status.ThinPools[i].Name))
				lvg.Status.ThinPools[i].AvailableSpace = space
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

func validateSpecBlockDevices(lvg *v1alpha1.LVMVolumeGroup, blockDevices map[string]v1alpha1.BlockDevice) (bool, string) {
	bdNames := make([]string, 0, len(blockDevices))
	for _, bd := range blockDevices {
		if bd.Status.NodeName != lvg.Spec.Local.NodeName {
			bdNames = append(bdNames, bd.Name)
		}
	}

	if len(bdNames) != 0 {
		return false, fmt.Sprintf("block devices %s have different node names from LVMVolumeGroup Local.NodeName", strings.Join(bdNames, ","))
	}

	return true, ""
}

func deleteLVGIfNeeded(ctx context.Context, cl client.Client, log logger.Logger, metrics monitoring.Metrics, cfg config.Options, sdsCache *cache.Cache, lvg *v1alpha1.LVMVolumeGroup) (bool, error) {
	vgs, _ := sdsCache.GetVGs()
	if !checkIfVGExist(lvg.Spec.ActualVGNameOnTheNode, vgs) && lvg.DeletionTimestamp != nil {
		log.Info(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] VG %s was not yet created for the LVMVolumeGroup %s and the resource is marked as deleting. Delete the resource", lvg.Spec.ActualVGNameOnTheNode, lvg.Name))
		removed, err := removeLVGFinalizerIfExist(ctx, cl, lvg)
		if err != nil {
			log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to remove the finalizer %s from the LVMVolumeGroup %s", internal.SdsNodeConfiguratorFinalizer, lvg.Name))
			return false, err
		}

		if removed {
			log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] successfully removed the finalizer %s from the LVMVolumeGroup %s", internal.SdsNodeConfiguratorFinalizer, lvg.Name))
		} else {
			log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] no need to remove the finalizer %s from the LVMVolumeGroup %s", internal.SdsNodeConfiguratorFinalizer, lvg.Name))
		}

		err = DeleteLVMVolumeGroup(ctx, cl, log, metrics, lvg, cfg.NodeName)
		if err != nil {
			log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to delete the LVMVolumeGroup %s", lvg.Name))
			return false, err
		}
		log.Info(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] successfully deleted the LVMVolumeGroup %s", lvg.Name))
		return true, nil
	}
	return false, nil
}

//func validateSpecBlockDevices(lvg *v1alpha1.LVMVolumeGroup, blockDevices map[string]v1alpha1.BlockDevice) (bool, string) {
//	reason := strings.Builder{}
//
//	targetNodeName := ""
//	for _, bdName := range lvg.Spec.BlockDeviceNames {
//		bd, exist := blockDevices[bdName]
//
//		if !exist {
//			reason.WriteString(fmt.Sprintf("the BlockDevice %s does not exist", bdName))
//			continue
//		}
//
//		if targetNodeName == "" {
//			targetNodeName = bd.Status.NodeName
//		}
//
//		if bd.Status.NodeName != targetNodeName {
//			reason.WriteString(fmt.Sprintf("the BlockDevice %s has the node %s though the target node %s", bd.Name, bd.Status.NodeName, targetNodeName))
//		}
//	}
//
//	if reason.Len() != 0 {
//		return false, reason.String()
//	}
//
//	return true, ""
//}

func checkIfLVGBelongsToNode(lvg *v1alpha1.LVMVolumeGroup, nodeName string) bool {
	return lvg.Spec.Local.NodeName == nodeName
}

func extractPathsFromBlockDevices(targetDevices []string, blockDevices map[string]v1alpha1.BlockDevice) []string {
	var paths []string
	if len(targetDevices) > 0 {
		paths = make([]string, 0, len(targetDevices))
		for _, bdName := range targetDevices {
			bd := blockDevices[bdName]
			paths = append(paths, bd.Status.Path)
		}
	} else {
		paths = make([]string, 0, len(blockDevices))
		for _, bd := range blockDevices {
			paths = append(paths, bd.Status.Path)
		}
	}

	return paths
}

func getRequestedSizeFromString(size string, targetSpace resource.Quantity) (resource.Quantity, error) {
	switch isPercentSize(size) {
	case true:
		strPercent := strings.Split(size, "%")[0]
		percent, err := strconv.Atoi(strPercent)
		if err != nil {
			return resource.Quantity{}, err
		}
		lvSize := targetSpace.Value() * int64(percent) / 100
		return *resource.NewQuantity(lvSize, resource.BinarySI), nil
	case false:
		return resource.ParseQuantity(size)
	}

	return resource.Quantity{}, nil
}

func countVGSizeByBlockDevices(blockDevices map[string]v1alpha1.BlockDevice) resource.Quantity {
	var totalVGSize int64
	for _, bd := range blockDevices {
		totalVGSize += bd.Status.Size.Value()
	}
	return *resource.NewQuantity(totalVGSize, resource.BinarySI)
}

func validateLVGForCreateFunc(log logger.Logger, lvg *v1alpha1.LVMVolumeGroup, blockDevices map[string]v1alpha1.BlockDevice) (bool, string) {
	reason := strings.Builder{}

	log.Debug(fmt.Sprintf("[validateLVGForCreateFunc] check if every selected BlockDevice of the LVMVolumeGroup %s is consumable", lvg.Name))
	// totalVGSize needs to count if there is enough space for requested thin-pools
	totalVGSize := countVGSizeByBlockDevices(blockDevices)
	for _, bd := range blockDevices {
		if !bd.Status.Consumable {
			log.Warning(fmt.Sprintf("[validateLVGForCreateFunc] BlockDevice %s is not consumable", bd.Name))
			log.Trace(fmt.Sprintf("[validateLVGForCreateFunc] BlockDevice name: %s, status: %+v", bd.Name, bd.Status))
			reason.WriteString(fmt.Sprintf("BlockDevice %s is not consumable. ", bd.Name))
		}
	}

	if reason.Len() == 0 {
		log.Debug(fmt.Sprintf("[validateLVGForCreateFunc] all BlockDevices of the LVMVolumeGroup %s are consumable", lvg.Name))
	}

	if lvg.Spec.ThinPools != nil {
		log.Debug(fmt.Sprintf("[validateLVGForCreateFunc] the LVMVolumeGroup %s has thin-pools. Validate if VG size has enough space for the thin-pools", lvg.Name))
		log.Trace(fmt.Sprintf("[validateLVGForCreateFunc] the LVMVolumeGroup %s has thin-pools %v", lvg.Name, lvg.Spec.ThinPools))
		log.Trace(fmt.Sprintf("[validateLVGForCreateFunc] total LVMVolumeGroup %s size: %s", lvg.Name, totalVGSize.String()))

		var totalThinPoolSize int64
		for _, tp := range lvg.Spec.ThinPools {
			tpRequestedSize, err := getRequestedSizeFromString(tp.Size, totalVGSize)
			if err != nil {
				reason.WriteString(err.Error())
				continue
			}

			if tpRequestedSize.Value() == 0 {
				reason.WriteString(fmt.Sprintf("Thin-pool %s has zero size. ", tp.Name))
				continue
			}

			// means a user want a thin-pool with 100%FREE size
			if utils.AreSizesEqualWithinDelta(tpRequestedSize, totalVGSize, internal.ResizeDelta) {
				if len(lvg.Spec.ThinPools) > 1 {
					reason.WriteString(fmt.Sprintf("Thin-pool %s requested size of full VG space, but there is any other thin-pool. ", tp.Name))
				}
			}

			totalThinPoolSize += tpRequestedSize.Value()
		}
		log.Trace(fmt.Sprintf("[validateLVGForCreateFunc] LVMVolumeGroup %s thin-pools requested space: %d", lvg.Name, totalThinPoolSize))

		if totalThinPoolSize != totalVGSize.Value() && totalThinPoolSize+internal.ResizeDelta.Value() >= totalVGSize.Value() {
			log.Trace(fmt.Sprintf("[validateLVGForCreateFunc] total thin pool size: %s, total vg size: %s", resource.NewQuantity(totalThinPoolSize, resource.BinarySI).String(), totalVGSize.String()))
			log.Warning(fmt.Sprintf("[validateLVGForCreateFunc] requested thin pool size is more than VG total size for the LVMVolumeGroup %s", lvg.Name))
			reason.WriteString(fmt.Sprintf("Required space for thin-pools %d is more than VG size %d.", totalThinPoolSize, totalVGSize.Value()))
		}
	}

	if reason.Len() != 0 {
		return false, reason.String()
	}

	return true, ""
}

func validateLVGForUpdateFunc(log logger.Logger, sdsCache *cache.Cache, lvg *v1alpha1.LVMVolumeGroup, blockDevices map[string]v1alpha1.BlockDevice) (bool, string) {
	reason := strings.Builder{}
	pvs, _ := sdsCache.GetPVs()
	log.Debug(fmt.Sprintf("[validateLVGForUpdateFunc] check if every new BlockDevice of the LVMVolumeGroup %s is comsumable", lvg.Name))
	actualPVPaths := make(map[string]struct{}, len(pvs))
	for _, pv := range pvs {
		actualPVPaths[pv.PVName] = struct{}{}
	}

	//TODO: add a check if BlockDevice size got less than PV size

	// Check if added BlockDevices are consumable
	// additionBlockDeviceSpace value is needed to count if VG will have enough space for thin-pools
	var additionBlockDeviceSpace int64
	for _, bd := range blockDevices {
		if _, found := actualPVPaths[bd.Status.Path]; !found {
			log.Debug(fmt.Sprintf("[validateLVGForUpdateFunc] unable to find the PV %s for BlockDevice %s. Check if the BlockDevice is already used", bd.Status.Path, bd.Name))
			for _, n := range lvg.Status.Nodes {
				for _, d := range n.Devices {
					if d.BlockDevice == bd.Name {
						log.Warning(fmt.Sprintf("[validateLVGForUpdateFunc] BlockDevice %s misses the PV %s. That might be because the corresponding device was removed from the node. Unable to validate BlockDevices", bd.Name, bd.Status.Path))
						reason.WriteString(fmt.Sprintf("BlockDevice %s misses the PV %s (that might be because the device was removed from the node). ", bd.Name, bd.Status.Path))
					}

					if reason.Len() == 0 {
						log.Debug(fmt.Sprintf("[validateLVGForUpdateFunc] BlockDevice %s does not miss a PV", d.BlockDevice))
					}
				}
			}

			log.Debug(fmt.Sprintf("[validateLVGForUpdateFunc] PV %s for BlockDevice %s of the LVMVolumeGroup %s is not created yet, check if the BlockDevice is consumable", bd.Status.Path, bd.Name, lvg.Name))
			if reason.Len() > 0 {
				log.Debug("[validateLVGForUpdateFunc] some BlockDevices misses its PVs, unable to check if they are consumable")
				continue
			}

			if !bd.Status.Consumable {
				reason.WriteString(fmt.Sprintf("BlockDevice %s is not consumable. ", bd.Name))
				continue
			}

			log.Debug(fmt.Sprintf("[validateLVGForUpdateFunc] BlockDevice %s is consumable", bd.Name))
			additionBlockDeviceSpace += bd.Status.Size.Value()
		}
	}

	if lvg.Spec.ThinPools != nil {
		log.Debug(fmt.Sprintf("[validateLVGForUpdateFunc] the LVMVolumeGroup %s has thin-pools. Validate them", lvg.Name))
		actualThinPools := make(map[string]internal.LVData, len(lvg.Spec.ThinPools))
		for _, tp := range lvg.Spec.ThinPools {
			lv := sdsCache.FindLV(lvg.Spec.ActualVGNameOnTheNode, tp.Name)
			if lv != nil {
				if !isThinPool(lv.Data) {
					reason.WriteString(fmt.Sprintf("LV %s is already created on the node and it is not a thin-pool", lv.Data.LVName))
					continue
				}

				actualThinPools[lv.Data.LVName] = lv.Data
			}
		}

		// check if added thin-pools has valid requested size
		var (
			addingThinPoolSize int64
			hasFullThinPool    = false
		)

		vg := sdsCache.FindVG(lvg.Spec.ActualVGNameOnTheNode)
		if vg == nil {
			reason.WriteString(fmt.Sprintf("Missed VG %s in the cache", lvg.Spec.ActualVGNameOnTheNode))
			return false, reason.String()
		}

		newTotalVGSize := resource.NewQuantity(vg.VGSize.Value()+additionBlockDeviceSpace, resource.BinarySI)
		for _, specTp := range lvg.Spec.ThinPools {
			// might be a case when Thin-pool is already created, but is not shown in status
			tpRequestedSize, err := getRequestedSizeFromString(specTp.Size, *newTotalVGSize)
			if err != nil {
				reason.WriteString(err.Error())
				continue
			}

			if tpRequestedSize.Value() == 0 {
				reason.WriteString(fmt.Sprintf("Thin-pool %s has zero size. ", specTp.Name))
				continue
			}

			log.Debug(fmt.Sprintf("[validateLVGForUpdateFunc] the LVMVolumeGroup %s thin-pool %s requested size %s, Status VG size %s", lvg.Name, specTp.Name, tpRequestedSize.String(), lvg.Status.VGSize.String()))
			switch utils.AreSizesEqualWithinDelta(tpRequestedSize, *newTotalVGSize, internal.ResizeDelta) {
			// means a user wants 100% of VG space
			case true:
				hasFullThinPool = true
				if len(lvg.Spec.ThinPools) > 1 {
					// as if a user wants thin-pool with 100%VG size, there might be only one thin-pool
					reason.WriteString(fmt.Sprintf("Thin-pool %s requests size of full VG space, but there are any other thin-pools. ", specTp.Name))
				}
			case false:
				if actualThinPool, created := actualThinPools[specTp.Name]; !created {
					log.Debug(fmt.Sprintf("[validateLVGForUpdateFunc] thin-pool %s of the LVMVolumeGroup %s is not yet created, adds its requested size", specTp.Name, lvg.Name))
					addingThinPoolSize += tpRequestedSize.Value()
				} else {
					log.Debug(fmt.Sprintf("[validateLVGForUpdateFunc] thin-pool %s of the LVMVolumeGroup %s is already created, check its requested size", specTp.Name, lvg.Name))
					if tpRequestedSize.Value()+internal.ResizeDelta.Value() < actualThinPool.LVSize.Value() {
						log.Debug(fmt.Sprintf("[validateLVGForUpdateFunc] the LVMVolumeGroup %s Spec.ThinPool %s size %s is less than Status one: %s", lvg.Name, specTp.Name, tpRequestedSize.String(), actualThinPool.LVSize.String()))
						reason.WriteString(fmt.Sprintf("Requested Spec.ThinPool %s size %s is less than actual one %s. ", specTp.Name, tpRequestedSize.String(), actualThinPool.LVSize.String()))
						continue
					}

					thinPoolSizeDiff := tpRequestedSize.Value() - actualThinPool.LVSize.Value()
					if thinPoolSizeDiff > internal.ResizeDelta.Value() {
						log.Debug(fmt.Sprintf("[validateLVGForUpdateFunc] the LVMVolumeGroup %s Spec.ThinPool %s size %s more than Status one: %s", lvg.Name, specTp.Name, tpRequestedSize.String(), actualThinPool.LVSize.String()))
						addingThinPoolSize += thinPoolSizeDiff
					}
				}
			}
		}

		if !hasFullThinPool {
			allocatedSize := getVGAllocatedSize(*vg)
			totalFreeSpace := newTotalVGSize.Value() - allocatedSize.Value()
			log.Trace(fmt.Sprintf("[validateLVGForUpdateFunc] new LVMVolumeGroup %s thin-pools requested %d size, additional BlockDevices space %d, total: %d", lvg.Name, addingThinPoolSize, additionBlockDeviceSpace, totalFreeSpace))
			if addingThinPoolSize != 0 && addingThinPoolSize+internal.ResizeDelta.Value() > totalFreeSpace {
				reason.WriteString("Added thin-pools requested sizes are more than allowed free space in VG.")
			}
		}
	}

	if reason.Len() != 0 {
		return false, reason.String()
	}

	return true, ""
}

func identifyLVGReconcileFunc(lvg *v1alpha1.LVMVolumeGroup, sdsCache *cache.Cache) reconcileType {
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

func shouldReconcileLVGByCreateFunc(lvg *v1alpha1.LVMVolumeGroup, ch *cache.Cache) bool {
	if lvg.DeletionTimestamp != nil {
		return false
	}

	vg := ch.FindVG(lvg.Spec.ActualVGNameOnTheNode)
	return vg == nil
}

func shouldReconcileLVGByUpdateFunc(lvg *v1alpha1.LVMVolumeGroup, ch *cache.Cache) bool {
	if lvg.DeletionTimestamp != nil {
		return false
	}

	vg := ch.FindVG(lvg.Spec.ActualVGNameOnTheNode)
	return vg != nil
}

func ReconcileThinPoolsIfNeeded(ctx context.Context, cl client.Client, log logger.Logger, metrics monitoring.Metrics, lvg *v1alpha1.LVMVolumeGroup, vg internal.VGData, lvs []internal.LVData) error {
	actualThinPools := make(map[string]internal.LVData, len(lvs))
	for _, lv := range lvs {
		if string(lv.LVAttr[0]) == "t" {
			actualThinPools[lv.LVName] = lv
		}
	}

	errs := strings.Builder{}
	for _, specTp := range lvg.Spec.ThinPools {
		tpRequestedSize, err := getRequestedSizeFromString(specTp.Size, lvg.Status.VGSize)
		if err != nil {
			log.Error(err, fmt.Sprintf("[ReconcileThinPoolsIfNeeded] unable to get requested thin-pool %s size of the LVMVolumeGroup %s", specTp.Name, lvg.Name))
			return err
		}

		if actualTp, exist := actualThinPools[specTp.Name]; !exist {
			log.Debug(fmt.Sprintf("[ReconcileThinPoolsIfNeeded] thin-pool %s of the LVMVolumeGroup %s is not created yet. Create it", specTp.Name, lvg.Name))
			if checkIfConditionIsTrue(lvg, internal.TypeVGConfigurationApplied) {
				err := updateLVGConditionIfNeeded(ctx, cl, log, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonUpdating, "trying to apply the configuration")
				if err != nil {
					log.Error(err, fmt.Sprintf("[ReconcileThinPoolsIfNeeded] unable to add the condition %s status False reason %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, internal.ReasonUpdating, lvg.Name))
					return err
				}
			}

			var cmd string
			start := time.Now()
			if utils.AreSizesEqualWithinDelta(tpRequestedSize, lvg.Status.VGSize, internal.ResizeDelta) {
				log.Debug(fmt.Sprintf("[ReconcileThinPoolsIfNeeded] thin-pool %s of the LVMVolumeGroup %s will be created with size 100FREE", specTp.Name, lvg.Name))
				cmd, err = utils.CreateThinPoolFullVGSpace(specTp.Name, vg.VGName)
			} else {
				log.Debug(fmt.Sprintf("[ReconcileThinPoolsIfNeeded] thin-pool %s of the LVMVolumeGroup %s will be created with size %s", specTp.Name, lvg.Name, tpRequestedSize.String()))
				cmd, err = utils.CreateThinPool(specTp.Name, vg.VGName, tpRequestedSize.Value())
			}
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
			// thin-pool exists
			if utils.AreSizesEqualWithinDelta(tpRequestedSize, actualTp.LVSize, internal.ResizeDelta) {
				log.Debug(fmt.Sprintf("[ReconcileThinPoolsIfNeeded] the LVMVolumeGroup %s requested thin pool %s size is equal to actual one", lvg.Name, tpRequestedSize.String()))
				continue
			}

			log.Debug(fmt.Sprintf("[ReconcileThinPoolsIfNeeded] the LVMVolumeGroup %s requested thin pool %s size is more than actual one. Resize it", lvg.Name, tpRequestedSize.String()))
			if checkIfConditionIsTrue(lvg, internal.TypeVGConfigurationApplied) {
				err = updateLVGConditionIfNeeded(ctx, cl, log, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonUpdating, "trying to apply the configuration")
				if err != nil {
					log.Error(err, fmt.Sprintf("[ReconcileThinPoolsIfNeeded] unable to add the condition %s status False reason %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, internal.ReasonUpdating, lvg.Name))
					return err
				}
			}
			err = ExtendThinPool(log, metrics, lvg, specTp)
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

func ResizePVIfNeeded(ctx context.Context, cl client.Client, log logger.Logger, metrics monitoring.Metrics, lvg *v1alpha1.LVMVolumeGroup) error {
	if len(lvg.Status.Nodes) == 0 {
		log.Warning(fmt.Sprintf("[ResizePVIfNeeded] the LVMVolumeGroup %s nodes are empty. Wait for the next update", lvg.Name))
		return nil
	}

	errs := strings.Builder{}
	for _, n := range lvg.Status.Nodes {
		for _, d := range n.Devices {
			if d.DevSize.Value()-d.PVSize.Value() > internal.ResizeDelta.Value() {
				if checkIfConditionIsTrue(lvg, internal.TypeVGConfigurationApplied) {
					err := updateLVGConditionIfNeeded(ctx, cl, log, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonUpdating, "trying to apply the configuration")
					if err != nil {
						log.Error(err, fmt.Sprintf("[UpdateVGTagIfNeeded] unable to add the condition %s status False reason %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, internal.ReasonUpdating, lvg.Name))
						return err
					}
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

func ExtendVGIfNeeded(ctx context.Context, cl client.Client, log logger.Logger, metrics monitoring.Metrics, lvg *v1alpha1.LVMVolumeGroup, vg internal.VGData, pvs []internal.PVData, blockDevices map[string]v1alpha1.BlockDevice) error {
	for _, n := range lvg.Status.Nodes {
		for _, d := range n.Devices {
			log.Trace(fmt.Sprintf("[ExtendVGIfNeeded] the LVMVolumeGroup %s status block device: %s", lvg.Name, d.BlockDevice))
		}
	}

	pvsMap := make(map[string]struct{}, len(pvs))
	for _, pv := range pvs {
		pvsMap[pv.PVName] = struct{}{}
	}

	devicesToExtend := make([]string, 0, len(blockDevices))
	for _, bd := range blockDevices {
		if _, exist := pvsMap[bd.Status.Path]; !exist {
			log.Debug(fmt.Sprintf("[ExtendVGIfNeeded] the BlockDevice %s of LVMVolumeGroup %s Spec is not counted as used", bd.Name, lvg.Name))
			devicesToExtend = append(devicesToExtend, bd.Name)
		}
	}

	if len(devicesToExtend) == 0 {
		log.Debug(fmt.Sprintf("[ExtendVGIfNeeded] VG %s of the LVMVolumeGroup %s should not be extended", vg.VGName, lvg.Name))
		return nil
	}

	if checkIfConditionIsTrue(lvg, internal.TypeVGConfigurationApplied) {
		err := updateLVGConditionIfNeeded(ctx, cl, log, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonUpdating, "trying to apply the configuration")
		if err != nil {
			log.Error(err, fmt.Sprintf("[UpdateVGTagIfNeeded] unable to add the condition %s status False reason %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, internal.ReasonUpdating, lvg.Name))
			return err
		}
	}

	log.Debug(fmt.Sprintf("[ExtendVGIfNeeded] VG %s should be extended as there are some BlockDevices were added to Spec field of the LVMVolumeGroup %s", vg.VGName, lvg.Name))
	paths := extractPathsFromBlockDevices(devicesToExtend, blockDevices)
	err := ExtendVGComplex(metrics, paths, vg.VGName, log)
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

func removeLVGFinalizerIfExist(ctx context.Context, cl client.Client, lvg *v1alpha1.LVMVolumeGroup) (bool, error) {
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

func getLVForVG(ch *cache.Cache, vgName string) []string {
	lvs, _ := ch.GetLVs()
	usedLVs := make([]string, 0, len(lvs))
	for _, lv := range lvs {
		if lv.VGName == vgName {
			usedLVs = append(usedLVs, lv.LVName)
		}
	}

	return usedLVs
}

func getLVMVolumeGroup(ctx context.Context, cl client.Client, metrics monitoring.Metrics, name string) (*v1alpha1.LVMVolumeGroup, error) {
	obj := &v1alpha1.LVMVolumeGroup{}
	start := time.Now()
	err := cl.Get(ctx, client.ObjectKey{
		Name: name,
	}, obj)
	metrics.APIMethodsDuration(LVMVolumeGroupWatcherCtrlName, "get").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.APIMethodsExecutionCount(LVMVolumeGroupWatcherCtrlName, "get").Inc()
	if err != nil {
		metrics.APIMethodsErrors(LVMVolumeGroupWatcherCtrlName, "get").Inc()
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
	log.Debug(fmt.Sprintf("[DeleteVGIfExist] VG %s was successfully deleted from the node", vgName))
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
	log.Debug(fmt.Sprintf("[DeleteVGIfExist] successfully delete PVs of VG %s from the node", vgName))

	return nil
}

func ExtendVGComplex(metrics monitoring.Metrics, extendPVs []string, vgName string, log logger.Logger) error {
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
	command, err := utils.ExtendVG(vgName, extendPVs)
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

func CreateVGComplex(metrics monitoring.Metrics, log logger.Logger, lvg *v1alpha1.LVMVolumeGroup, blockDevices map[string]v1alpha1.BlockDevice) error {
	paths := extractPathsFromBlockDevices(nil, blockDevices)

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

func UpdateVGTagIfNeeded(ctx context.Context, cl client.Client, log logger.Logger, metrics monitoring.Metrics, lvg *v1alpha1.LVMVolumeGroup, vg internal.VGData) (bool, error) {
	found, tagName := CheckTag(vg.VGTags)
	if found && lvg.Name != tagName {
		if checkIfConditionIsTrue(lvg, internal.TypeVGConfigurationApplied) {
			err := updateLVGConditionIfNeeded(ctx, cl, log, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonUpdating, "trying to apply the configuration")
			if err != nil {
				log.Error(err, fmt.Sprintf("[UpdateVGTagIfNeeded] unable to add the condition %s status False reason %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, internal.ReasonUpdating, lvg.Name))
				return false, err
			}
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

func ExtendThinPool(log logger.Logger, metrics monitoring.Metrics, lvg *v1alpha1.LVMVolumeGroup, specThinPool v1alpha1.LVMVolumeGroupThinPoolSpec) error {
	volumeGroupFreeSpaceBytes := lvg.Status.VGSize.Value() - lvg.Status.AllocatedSize.Value()
	tpRequestedSize, err := getRequestedSizeFromString(specThinPool.Size, lvg.Status.VGSize)
	if err != nil {
		return err
	}

	log.Trace(fmt.Sprintf("[ExtendThinPool] volumeGroupSize = %s", lvg.Status.VGSize.String()))
	log.Trace(fmt.Sprintf("[ExtendThinPool] volumeGroupAllocatedSize = %s", lvg.Status.AllocatedSize.String()))
	log.Trace(fmt.Sprintf("[ExtendThinPool] volumeGroupFreeSpaceBytes = %d", volumeGroupFreeSpaceBytes))

	log.Info(fmt.Sprintf("[ExtendThinPool] start resizing thin pool: %s; with new size: %s", specThinPool.Name, tpRequestedSize.String()))

	var cmd string
	start := time.Now()
	if utils.AreSizesEqualWithinDelta(tpRequestedSize, lvg.Status.VGSize, internal.ResizeDelta) {
		log.Debug(fmt.Sprintf("[ExtendThinPool] thin-pool %s of the LVMVolumeGroup %s will be extend to size 100VG", specThinPool.Name, lvg.Name))
		cmd, err = utils.ExtendLVFullVGSpace(lvg.Spec.ActualVGNameOnTheNode, specThinPool.Name)
	} else {
		log.Debug(fmt.Sprintf("[ExtendThinPool] thin-pool %s of the LVMVolumeGroup %s will be extend to size %s", specThinPool.Name, lvg.Name, tpRequestedSize.String()))
		cmd, err = utils.ExtendLV(tpRequestedSize.Value(), lvg.Spec.ActualVGNameOnTheNode, specThinPool.Name)
	}
	metrics.UtilsCommandsDuration(LVMVolumeGroupWatcherCtrlName, "lvextend").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.UtilsCommandsExecutionCount(LVMVolumeGroupWatcherCtrlName, "lvextend").Inc()
	if err != nil {
		metrics.UtilsCommandsErrorsCount(LVMVolumeGroupWatcherCtrlName, "lvextend").Inc()
		log.Error(err, fmt.Sprintf("[ExtendThinPool] unable to extend LV, name: %s, cmd: %s", specThinPool.Name, cmd))
		return err
	}

	return nil
}

func addLVGLabelIfNeeded(ctx context.Context, cl client.Client, log logger.Logger, lvg *v1alpha1.LvmVolumeGroup, labelKey, labelValue string) (bool, error) {
	if !shouldUpdateLVGLabels(log, lvg, labelKey, labelValue) {
		return false, nil
	}

	if lvg.Labels == nil {
		lvg.Labels = make(map[string]string)
	}

	lvg.Labels[labelKey] = labelValue
	err := cl.Update(ctx, lvg)
	if err != nil {
		return false, err
	}

	return true, nil
}
