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
	"sds-node-configurator/api/v1alpha1"
	"sds-node-configurator/internal"
	"sds-node-configurator/pkg/cache"
	"sds-node-configurator/pkg/logger"
	"sds-node-configurator/pkg/monitoring"
	"sds-node-configurator/pkg/utils"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type StatusLVMVolumeGroup struct {
	Health  string
	Phase   string
	Message string
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

func updateLVMVolumeGroupHealthStatus(ctx context.Context, cl client.Client, metrics monitoring.Metrics, lvg *v1alpha1.LvmVolumeGroup, health, message string) error {
	lvg.Status.Health = health
	lvg.Status.Message = message

	start := time.Now()
	err := cl.Update(ctx, lvg)
	metrics.ApiMethodsDuration(LVMVolumeGroupWatcherCtrlName, "update").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.ApiMethodsExecutionCount(LVMVolumeGroupWatcherCtrlName, "update").Inc()
	if err != nil {
		metrics.ApiMethodsErrors(LVMVolumeGroupWatcherCtrlName, "update").Inc()
		return err
	}
	return nil
}

func getBlockDevice(ctx context.Context, cl client.Client, metrics monitoring.Metrics, name string) (*v1alpha1.BlockDevice, error) {
	obj := &v1alpha1.BlockDevice{}

	start := time.Now()
	err := cl.Get(ctx, client.ObjectKey{
		Name: name,
	}, obj)
	metrics.ApiMethodsDuration(LVMVolumeGroupWatcherCtrlName, "get").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.ApiMethodsExecutionCount(LVMVolumeGroupWatcherCtrlName, "get").Inc()
	if err != nil {
		metrics.ApiMethodsErrors(LVMVolumeGroupWatcherCtrlName, "get").Inc()
		return nil, err
	}
	return obj, nil
}

//func CheckLVMVGNodeOwnership(ctx context.Context, cl client.Client, metrics monitoring.Metrics, lvmVolumeGroup *v1alpha1.LvmVolumeGroup, nodeName string) (bool, error) {
//	if lvmVolumeGroup == nil {
//		return false, errors.New("lvmVolumeGroup is nil")
//	}
//
//	membership := 0
//	if lvmVolumeGroup.Spec.Type == Local {
//		for _, blockDev := range lvmVolumeGroup.Spec.BlockDeviceNames {
//			device, err := getBlockDevice(ctx, cl, metrics, blockDev)
//			if err != nil {
//				err = fmt.Errorf("unable to get BlockDevice %s, err: %w", blockDev, err)
//				return false, err
//			}
//
//			if device.Status.NodeName == nodeName {
//				membership++
//			}
//		}
//
//		if membership == len(lvmVolumeGroup.Spec.BlockDeviceNames) {
//			if lvmVolumeGroup.Spec.ActualVGNameOnTheNode == "" {
//				err := fmt.Errorf("actualVGNameOnTheNode is empty")
//				status.Health = NonOperational
//				status.Phase = Failed
//				status.Message = "actualVGNameOnTheNode is empty"
//				return false, &status, err
//			}
//			return true, &status, nil
//		}
//
//		if membership > 0 {
//			status.Health = NonOperational
//			status.Phase = Failed
//			status.Message = "selected block devices are from different nodes for local LVMVolumeGroup"
//			return false, &status, nil
//		}
//
//		if membership == 0 {
//			return false, &status, nil
//		}
//	}
//
//	return false, &status, nil
//}

//func ValidateOperationTypeLVMGroup(ctx context.Context, cl client.Client, metrics monitoring.Metrics, lvmVolumeGroup *v1alpha1.LvmVolumeGroup, l logger.Logger) (extendPV, shrinkPV []string, err error) {
//	pvs, cmdStr, _, err := utils.GetAllPVs()
//	l.Debug(fmt.Sprintf("GetAllPVs exec cmd: %s", cmdStr))
//	if err != nil {
//		return nil, nil, err
//	}
//
//	for _, devName := range lvmVolumeGroup.Spec.BlockDeviceNames {
//		dev, err := getBlockDevice(ctx, cl, metrics, lvmVolumeGroup.Namespace, devName)
//		if err != nil {
//			return nil, nil, err
//		}
//
//		if dev.Status.Consumable == true {
//			isReallyConsumable := true
//			for _, pv := range pvs {
//				if pv.PVName == dev.Status.Path && pv.VGName == lvmVolumeGroup.Spec.ActualVGNameOnTheNode {
//					isReallyConsumable = false
//					break
//				}
//			}
//			if isReallyConsumable {
//				extendPV = append(extendPV, dev.Status.Path)
//			}
//
//			continue
//		}
//
//		if dev.Status.ActualVGNameOnTheNode != lvmVolumeGroup.Spec.ActualVGNameOnTheNode && (len(dev.Status.VGUuid) != 0) {
//			err = fmt.Errorf("block device %s is already in use by another VG: %s with uuid %s. Our VG: %s with uuid %s", devName, dev.Status.ActualVGNameOnTheNode, dev.Status.VGUuid, lvmVolumeGroup.Spec.ActualVGNameOnTheNode, dev.Status.VGUuid)
//			return nil, nil, err
//		}
//		// TODO: realisation of shrinkPV
//	}
//
//	return extendPV, shrinkPV, nil
//}

func CreateEventLVMVolumeGroup(ctx context.Context, cl client.Client, metrics monitoring.Metrics, reason, actions, nodeName string, obj *v1alpha1.LvmVolumeGroup) error {
	e := &v1.Event{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Event",
			APIVersion: "events.k8s.io/v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: obj.Name + "-",
			Namespace:    nameSpaceEvent,
		},
		Reason: reason,
		InvolvedObject: v1.ObjectReference{
			Kind:       obj.Kind,
			Name:       obj.Name,
			UID:        obj.UID,
			APIVersion: "apiextensions.k8s.io/v1",
		},
		Type: v1.EventTypeNormal,
		EventTime: metav1.MicroTime{
			Time: time.Now(),
		},
		Action:              actions,
		ReportingInstance:   nodeName,
		ReportingController: LVMVolumeGroupWatcherCtrlName,
		Message:             "Event Message",
	}

	start := time.Now()
	err := cl.Create(ctx, e)
	metrics.ApiMethodsDuration(LVMVolumeGroupWatcherCtrlName, "create").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.ApiMethodsExecutionCount(LVMVolumeGroupWatcherCtrlName, "create").Inc()
	if err != nil {
		metrics.ApiMethodsErrors(LVMVolumeGroupWatcherCtrlName, "create").Inc()
		return err
	}
	return nil
}

func DeleteVGIfExist(log logger.Logger, metrics monitoring.Metrics, sdsCache *cache.Cache, vgName string) error {
	vgs, _ := sdsCache.GetVGs()
	if len(vgs) == 0 {
		log.Debug("[DeleteVGIfExist] no VG found, nothing to delete")
		return nil
	}

	pvs, _ := sdsCache.GetPVs()
	if len(pvs) == 0 {
		err := errors.New("no PV found")
		log.Error(err, fmt.Sprintf("[DeleteVGIfExist] no PV was found while deleting VG %s", vgName))
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

func GetVGFromNode(vgName string, log logger.Logger, metrics monitoring.Metrics) (bool, internal.VGData, error) {
	start := time.Now()
	var vg internal.VGData
	vgs, command, _, err := utils.GetAllVGs()
	metrics.UtilsCommandsDuration(LVMVolumeGroupWatcherCtrlName, "vgs").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.UtilsCommandsExecutionCount(LVMVolumeGroupWatcherCtrlName, "vgs").Inc()
	log.Debug(command)
	if err != nil {
		metrics.UtilsCommandsErrorsCount(LVMVolumeGroupWatcherCtrlName, "vgs").Inc()
		log.Error(err, " error CreateEventLVMVolumeGroup")
		return false, vg, err
	}

	for _, vg := range vgs {
		if vg.VGName == vgName {
			return true, vg, nil
		}
	}
	return false, vg, nil
}

//func ValidateConsumableDevices(ctx context.Context, cl client.Client, metrics monitoring.Metrics, group *v1alpha1.LvmVolumeGroup) (bool, error) {
//	if group == nil {
//		return false, fmt.Errorf("lvmVolumeGroup is nil")
//	}
//
//	for _, device := range group.Spec.BlockDeviceNames {
//		d, err := getBlockDevice(ctx, cl, metrics, group.Namespace, device)
//		if err != nil {
//			return false, err
//		}
//
//		if d.Status.Consumable == false {
//			return false, nil
//		}
//	}
//
//	return true, nil
//}

func GetPathsConsumableDevicesFromLVMVG(ctx context.Context, cl client.Client, mertics monitoring.Metrics, group *v1alpha1.LvmVolumeGroup) ([]string, error) {
	if group == nil {
		return nil, fmt.Errorf("lvmVolumeGroup is nil")
	}

	var paths []string
	for _, device := range group.Spec.BlockDeviceNames {
		d, err := getBlockDevice(ctx, cl, mertics, device)
		paths = append(paths, d.Status.Path)
		if err != nil {
			return nil, err
		}
	}

	return paths, nil
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

func UpdateVGTagIfNeeded(log logger.Logger, metrics monitoring.Metrics, vg internal.VGData, lvgName string) (bool, error) {
	found, tagName := CheckTag(vg.VGTags)
	if found && lvgName != tagName {
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
		cmd, err = utils.VGChangeAddTag(vg.VGName, fmt.Sprintf("%s=%s", LVMVolumeGroupTag, lvgName))
		metrics.UtilsCommandsDuration(LVMVolumeGroupWatcherCtrlName, "vgchange").Observe(metrics.GetEstimatedTimeInSeconds(start))
		metrics.UtilsCommandsExecutionCount(LVMVolumeGroupWatcherCtrlName, "vgchange").Inc()
		log.Debug(fmt.Sprintf("[UpdateVGTagIfNeeded] exec cmd: %s", cmd))
		if err != nil {
			log.Error(err, fmt.Sprintf("[UpdateVGTagIfNeeded] unable to add LVMVolumeGroupTag: %s=%s, vg: %s", LVMVolumeGroupTag, lvgName, vg.VGName))
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

	//err := CreateEventLVMVolumeGroup(ctx, cl, metrics, EventReasonResizing, EventActionResizing, nodeName, lvg)
	//if err != nil {
	//	log.Error(err, fmt.Sprintf("[ResizeThinPool] error CreateEventLVMVolumeGroup, resource name: %s", lvg.Name))
	//}

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
