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
	"sds-node-configurator/pkg/logger"
	"sds-node-configurator/pkg/monitoring"
	"sds-node-configurator/pkg/utils"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
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

func updateLVMVolumeGroupHealthStatus(ctx context.Context, cl client.Client, metrics monitoring.Metrics, name, namespace, message, health string) error {
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
		return err
	}

	if obj.Status.Health == health && health == Operational {
		return nil
	}

	if obj.Status.Health == health && obj.Status.Message == message {
		return nil
	}

	obj.Status.Health = health
	obj.Status.Message = message

	start = time.Now()
	err = cl.Update(ctx, obj)
	metrics.ApiMethodsDuration(LVMVolumeGroupWatcherCtrlName, "update").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.ApiMethodsExecutionCount(LVMVolumeGroupWatcherCtrlName, "update").Inc()
	if err != nil {
		metrics.ApiMethodsErrors(LVMVolumeGroupWatcherCtrlName, "update").Inc()
		return err
	}
	return nil
}

func getBlockDevice(ctx context.Context, cl client.Client, metrics monitoring.Metrics, namespace, name string) (*v1alpha1.BlockDevice, error) {
	obj := &v1alpha1.BlockDevice{}

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

func CheckLVMVGNodeOwnership(ctx context.Context, cl client.Client, metrics monitoring.Metrics, lvmVolumeGroup *v1alpha1.LvmVolumeGroup, namespace, nodeName string) (bool, *StatusLVMVolumeGroup, error) {
	status := StatusLVMVolumeGroup{}
	if lvmVolumeGroup == nil {
		return false, nil, errors.New("lvmVolumeGroup is nil")
	}

	membership := 0
	if lvmVolumeGroup.Spec.Type == Local {
		for _, blockDev := range lvmVolumeGroup.Spec.BlockDeviceNames {
			device, err := getBlockDevice(ctx, cl, metrics, namespace, blockDev)
			if err != nil {
				err = fmt.Errorf("error getBlockDevice: %s", err)
				status.Health = NonOperational
				status.Phase = Failed
				status.Message = err.Error()
				return false, &status, err
			}
			if device.Status.NodeName == nodeName {
				membership++
			}
		}

		if membership == len(lvmVolumeGroup.Spec.BlockDeviceNames) {
			if lvmVolumeGroup.Spec.ActualVGNameOnTheNode == "" {
				err := fmt.Errorf("actualVGNameOnTheNode is empty")
				status.Health = NonOperational
				status.Phase = Failed
				status.Message = "actualVGNameOnTheNode is empty"
				return false, &status, err
			}
			return true, &status, nil
		}

		if membership > 0 {
			status.Health = NonOperational
			status.Phase = Failed
			status.Message = "selected block devices are from different nodes for local LVMVolumeGroup"
			return false, &status, nil
		}

		if membership == 0 {
			return false, &status, nil
		}
	}

	return false, &status, nil
}

func ValidateOperationTypeLVMGroup(ctx context.Context, cl client.Client, metrics monitoring.Metrics, lvmVolumeGroup *v1alpha1.LvmVolumeGroup, l logger.Logger) (extendPV, shrinkPV []string, err error) {
	pvs, cmdStr, _, err := utils.GetAllPVs()
	l.Debug(fmt.Sprintf("GetAllPVs exec cmd: %s", cmdStr))
	if err != nil {
		return nil, nil, err
	}

	for _, devName := range lvmVolumeGroup.Spec.BlockDeviceNames {
		dev, err := getBlockDevice(ctx, cl, metrics, lvmVolumeGroup.Namespace, devName)
		if err != nil {
			return nil, nil, err
		}

		if dev.Status.Consumable == true {
			isReallyConsumable := true
			for _, pv := range pvs {
				if pv.PVName == dev.Status.Path && pv.VGName == lvmVolumeGroup.Spec.ActualVGNameOnTheNode {
					isReallyConsumable = false
					break
				}
			}
			if isReallyConsumable {
				extendPV = append(extendPV, dev.Status.Path)
			}

			continue
		}

		if dev.Status.ActualVGNameOnTheNode != lvmVolumeGroup.Spec.ActualVGNameOnTheNode && (len(dev.Status.VGUuid) != 0) {
			err = fmt.Errorf("block device %s is already in use by another VG: %s with uuid %s. Our VG: %s with uuid %s", devName, dev.Status.ActualVGNameOnTheNode, dev.Status.VGUuid, lvmVolumeGroup.Spec.ActualVGNameOnTheNode, dev.Status.VGUuid)
			return nil, nil, err
		}
		// TODO: realisation of shrinkPV
	}

	return extendPV, shrinkPV, nil
}

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

func DeleteVG(vgName string, log logger.Logger, metrics monitoring.Metrics) error {
	// if VG exist
	start := time.Now()
	vgs, command, _, err := utils.GetAllVGs()
	metrics.UtilsCommandsDuration(LVMVolumeGroupWatcherCtrlName, "vgs").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.UtilsCommandsExecutionCount(LVMVolumeGroupWatcherCtrlName, "vgs").Inc()
	log.Debug(command)
	if err != nil {
		metrics.UtilsCommandsErrorsCount(LVMVolumeGroupWatcherCtrlName, "vgs").Inc()
		log.Error(err, "GetAllVGs "+command)
		return err
	}

	if len(vgs) == 0 {
		return nil
	}

	// if exist LV in VG
	start = time.Now()
	lvs, command, _, err := utils.GetAllLVs()
	metrics.UtilsCommandsDuration(LVMVolumeGroupWatcherCtrlName, "lvs").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.UtilsCommandsExecutionCount(LVMVolumeGroupWatcherCtrlName, "lvs").Inc()
	log.Debug(command)
	if err != nil {
		metrics.UtilsCommandsErrorsCount(LVMVolumeGroupWatcherCtrlName, "lvs").Inc()
		log.Error(err, "GetAllLVs "+command)
		return err
	}

	for _, lv := range lvs {
		if lv.VGName == vgName {
			return fmt.Errorf(fmt.Sprintf(`[ERROR] VG "%s" contains LV "%s"`, vgName, lv.LVName))
		}
	}

	start = time.Now()
	pvs, command, _, err := utils.GetAllPVs()
	metrics.UtilsCommandsDuration(LVMVolumeGroupWatcherCtrlName, "pvs").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.UtilsCommandsExecutionCount(LVMVolumeGroupWatcherCtrlName, "pvs").Inc()
	log.Debug(command)
	if err != nil {
		metrics.UtilsCommandsErrorsCount(LVMVolumeGroupWatcherCtrlName, "pvs").Inc()
		log.Error(err, "RemoveVG "+command)
		return err
	}

	start = time.Now()
	command, err = utils.RemoveVG(vgName)
	metrics.UtilsCommandsDuration(LVMVolumeGroupWatcherCtrlName, "vgremove").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.UtilsCommandsExecutionCount(LVMVolumeGroupWatcherCtrlName, "vgremove").Inc()
	log.Debug(command)
	if err != nil {
		metrics.UtilsCommandsErrorsCount(LVMVolumeGroupWatcherCtrlName, "vgremove").Inc()
		log.Error(err, "RemoveVG "+command)
		return err
	}

	var listDeletingPV []string

	for _, pv := range pvs {
		if pv.VGName == vgName {
			listDeletingPV = append(listDeletingPV, pv.PVName)
		}
	}

	start = time.Now()
	command, err = utils.RemovePV(listDeletingPV)
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

func ValidateConsumableDevices(ctx context.Context, cl client.Client, metrics monitoring.Metrics, group *v1alpha1.LvmVolumeGroup) (bool, error) {
	if group == nil {
		return false, fmt.Errorf("lvmVolumeGroup is nil")
	}

	for _, device := range group.Spec.BlockDeviceNames {
		d, err := getBlockDevice(ctx, cl, metrics, group.Namespace, device)
		if err != nil {
			return false, err
		}

		if d.Status.Consumable == false {
			return false, nil
		}
	}

	return true, nil
}

func GetPathsConsumableDevicesFromLVMVG(ctx context.Context, cl client.Client, mertics monitoring.Metrics, group *v1alpha1.LvmVolumeGroup) ([]string, error) {
	if group == nil {
		return nil, fmt.Errorf("lvmVolumeGroup is nil")
	}

	var paths []string
	for _, device := range group.Spec.BlockDeviceNames {
		d, err := getBlockDevice(ctx, cl, mertics, group.Namespace, device)
		paths = append(paths, d.Status.Path)
		if err != nil {
			return nil, err
		}
	}

	return paths, nil
}

func ExtendVGComplex(metrics monitoring.Metrics, extendPVs []string, VGName string, l logger.Logger) error {
	for _, pvPath := range extendPVs {
		start := time.Now()
		command, err := utils.CreatePV(pvPath)
		metrics.UtilsCommandsDuration(LVMVolumeGroupWatcherCtrlName, "pvcreate").Observe(metrics.GetEstimatedTimeInSeconds(start))
		metrics.UtilsCommandsExecutionCount(LVMVolumeGroupWatcherCtrlName, "pvcreate").Inc()
		l.Debug(command)
		if err != nil {
			metrics.UtilsCommandsErrorsCount(LVMVolumeGroupWatcherCtrlName, "pvcreate").Inc()
			l.Error(err, "CreatePV ")
			return err
		}
	}

	start := time.Now()
	command, err := utils.ExtendVG(VGName, extendPVs)
	metrics.UtilsCommandsDuration(LVMVolumeGroupWatcherCtrlName, "vgextend").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.UtilsCommandsExecutionCount(LVMVolumeGroupWatcherCtrlName, "vgextend").Inc()
	l.Debug(command)
	if err != nil {
		metrics.UtilsCommandsErrorsCount(LVMVolumeGroupWatcherCtrlName, "vgextend").Inc()
		l.Error(err, "ExtendVG ")
		return err
	}
	return nil
}

func CreateVGComplex(ctx context.Context, cl client.Client, metrics monitoring.Metrics, group *v1alpha1.LvmVolumeGroup, l logger.Logger) error {
	allDevicesConsumable, err := ValidateConsumableDevices(ctx, cl, metrics, group)
	if err != nil {
		l.Error(err, " error ValidateConsumableDevices")
		return err
	}
	if !allDevicesConsumable {
		err = fmt.Errorf("not all devices is consumable")
		l.Error(err, "error ValidateConsumableDevices")
		return err
	}
	paths, err := GetPathsConsumableDevicesFromLVMVG(ctx, cl, metrics, group)
	if err != nil {
		l.Error(err, "error GetPathsConsumableDevicesFromLVMVG")
		return err
	}

	for _, path := range paths {
		p := path
		start := time.Now()
		command, err := utils.CreatePV(p)
		metrics.UtilsCommandsDuration(LVMVolumeGroupWatcherCtrlName, "pvcreate").Observe(metrics.GetEstimatedTimeInSeconds(start))
		metrics.UtilsCommandsExecutionCount(LVMVolumeGroupWatcherCtrlName, "pvcreate").Inc()
		l.Debug(command)
		if err != nil {
			metrics.UtilsCommandsErrorsCount(LVMVolumeGroupWatcherCtrlName, "pvcreate").Inc()
			l.Error(err, "CreatePV "+p)
			return err
		}
	}

	if group.Spec.Type == Local {
		start := time.Now()
		cmd, err := utils.CreateVGLocal(group.Spec.ActualVGNameOnTheNode, group.Name, paths)
		metrics.UtilsCommandsDuration(LVMVolumeGroupWatcherCtrlName, "vgcreate").Observe(metrics.GetEstimatedTimeInSeconds(start))
		metrics.UtilsCommandsExecutionCount(LVMVolumeGroupWatcherCtrlName, "vgcreate").Inc()
		l.Debug(cmd)
		if err != nil {
			metrics.UtilsCommandsErrorsCount(LVMVolumeGroupWatcherCtrlName, "vgcreate").Inc()
			l.Error(err, "error CreateVGLocal")
			return err
		}
	}

	if group.Spec.Type == Shared {
		start := time.Now()
		cmd, err := utils.CreateVGShared(group.Spec.ActualVGNameOnTheNode, group.Name, paths)
		metrics.UtilsCommandsDuration(LVMVolumeGroupWatcherCtrlName, "vgcreate").Observe(metrics.GetEstimatedTimeInSeconds(start))
		metrics.UtilsCommandsExecutionCount(LVMVolumeGroupWatcherCtrlName, "vgcreate").Inc()
		l.Debug(cmd)
		if err != nil {
			metrics.UtilsCommandsErrorsCount(LVMVolumeGroupWatcherCtrlName, "vgcreate").Inc()
			l.Error(err, "error CreateVGShared")
			return err
		}
	}
	return nil
}

func UpdateLVMVolumeGroupTagsName(log logger.Logger, metrics monitoring.Metrics, vg internal.VGData, lvg *v1alpha1.LvmVolumeGroup) (bool, error) {
	const tag = "storage.deckhouse.io/lvmVolumeGroupName"

	found, tagName := CheckTag(vg.VGTags)
	if found && lvg.Name != tagName {
		start := time.Now()
		cmd, err := utils.VGChangeDelTag(vg.VGName, fmt.Sprintf("%s=%s", tag, tagName))
		metrics.UtilsCommandsDuration(LVMVolumeGroupWatcherCtrlName, "vgchange").Observe(metrics.GetEstimatedTimeInSeconds(start))
		metrics.UtilsCommandsExecutionCount(LVMVolumeGroupWatcherCtrlName, "vgchange").Inc()
		log.Debug(fmt.Sprintf("[UpdateLVMVolumeGroupTagsName] exec cmd: %s", cmd))
		if err != nil {
			log.Error(err, fmt.Sprintf("[UpdateLVMVolumeGroupTagsName] unable to delete tag: %s=%s, vg: %s", tag, tagName, vg.VGName))
			metrics.UtilsCommandsErrorsCount(LVMVolumeGroupWatcherCtrlName, "vgchange").Inc()
			return false, err
		}

		start = time.Now()
		cmd, err = utils.VGChangeAddTag(vg.VGName, fmt.Sprintf("%s=%s", tag, lvg.Name))
		metrics.UtilsCommandsDuration(LVMVolumeGroupWatcherCtrlName, "vgchange").Observe(metrics.GetEstimatedTimeInSeconds(start))
		metrics.UtilsCommandsExecutionCount(LVMVolumeGroupWatcherCtrlName, "vgchange").Inc()
		log.Debug(fmt.Sprintf("[UpdateLVMVolumeGroupTagsName] exec cmd: %s", cmd))
		if err != nil {
			log.Error(err, fmt.Sprintf("[UpdateLVMVolumeGroupTagsName] unable to add tag: %s=%s, vg: %s", tag, lvg.Name, vg.VGName))
			metrics.UtilsCommandsErrorsCount(LVMVolumeGroupWatcherCtrlName, "vgchange").Inc()
			return false, err
		}

		return true, nil
	}

	return false, nil
}

func ResizeThinPool(ctx context.Context, cl client.Client, log logger.Logger, metrics monitoring.Metrics, lvg *v1alpha1.LvmVolumeGroup, specThinPool v1alpha1.SpecThinPool, statusThinPool v1alpha1.StatusThinPool, nodeName string, resizeDelta resource.Quantity) (bool, error) {
	volumeGroupSize, err := resource.ParseQuantity(lvg.Status.VGSize)
	if err != nil {
		log.Error(err, fmt.Sprintf("[ResizeThinPool] error ParseQuantity, resource name: %s", lvg.Name))
		return true, err
	}

	volumeGroupAllocatedSize, err := resource.ParseQuantity(lvg.Status.VGSize)
	if err != nil {
		log.Error(err, fmt.Sprintf("[ResizeThinPool] error ParseQuantity, resource name: %s", lvg.Name))
		return true, err
	}

	volumeGroupFreeSpaceBytes := volumeGroupSize.Value() - volumeGroupAllocatedSize.Value()
	addSizeBytes := specThinPool.Size.Value() - statusThinPool.ActualSize.Value()

	log.Debug(fmt.Sprintf("[ResizeThinPool] volumeGroupSize = %s", volumeGroupSize.String()))
	log.Debug(fmt.Sprintf("[ResizeThinPool] volumeGroupAllocatedSize = %s", volumeGroupAllocatedSize.String()))
	log.Debug(fmt.Sprintf("[ResizeThinPool] volumeGroupFreeSpaceBytes = %d", volumeGroupFreeSpaceBytes))
	log.Debug(fmt.Sprintf("[ResizeThinPool] addSizeBytes = %d", addSizeBytes))

	if addSizeBytes <= 0 {
		err = fmt.Errorf("thin pool name: %s; add size value <= 0, specThinPool.Size: %s, statusThinPool.ActualSize: %s", specThinPool.Name, specThinPool.Size.String(), statusThinPool.ActualSize.String())
		log.Error(err, "[ResizeThinPool]: ")
		return false, err
	}

	log.Debug(fmt.Sprintf("[ResizeThinPool] Identified a thin pool requiring resize: %s", specThinPool.Name))
	if volumeGroupFreeSpaceBytes < addSizeBytes+resizeDelta.Value() {
		err = fmt.Errorf("not enough space for resizing in the thin pool: %s; specThinPool.Size: %s, volumeGroupFreeSpace: %s, resizeDelta: %s", specThinPool.Name, specThinPool.Size.String(), resource.NewQuantity(volumeGroupFreeSpaceBytes, resource.BinarySI).String(), resizeDelta.String())
		log.Error(err, "[ResizeThinPool]: ")
	}

	log.Info(fmt.Sprintf("[ResizeThinPool] Start resizing thin pool: %s; with new size: %s", specThinPool.Name, specThinPool.Size.String()))

	err = CreateEventLVMVolumeGroup(ctx, cl, metrics, EventReasonResizing, EventActionResizing, nodeName, lvg)
	if err != nil {
		log.Error(err, fmt.Sprintf("[ResizeThinPool] error CreateEventLVMVolumeGroup, resource name: %s", lvg.Name))
	}
	start := time.Now()
	cmd, err := utils.ExtendLV(specThinPool.Size.Value(), lvg.Spec.ActualVGNameOnTheNode, specThinPool.Name)
	metrics.UtilsCommandsDuration(LVMVolumeGroupWatcherCtrlName, "lvextend").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.UtilsCommandsExecutionCount(LVMVolumeGroupWatcherCtrlName, "lvextend").Inc()
	log.Debug(cmd)
	if err != nil {
		metrics.UtilsCommandsErrorsCount(LVMVolumeGroupWatcherCtrlName, "lvextend").Inc()
		log.Error(err, fmt.Sprintf("[ResizeThinPool] error ExtendLV, pool name: %s", specThinPool.Name))
		return true, err
	}

	return false, nil

}
