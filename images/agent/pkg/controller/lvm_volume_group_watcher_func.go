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
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sds-node-configurator/api/v1alpha1"
	"sds-node-configurator/internal"
	"sds-node-configurator/pkg/cache"
	"sds-node-configurator/pkg/logger"
	"sds-node-configurator/pkg/monitoring"
	"sds-node-configurator/pkg/utils"
	"time"

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
	err := cl.Status().Update(ctx, lvg)
	metrics.ApiMethodsDuration(LVMVolumeGroupWatcherCtrlName, "update").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.ApiMethodsExecutionCount(LVMVolumeGroupWatcherCtrlName, "update").Inc()
	if err != nil {
		metrics.ApiMethodsErrors(LVMVolumeGroupWatcherCtrlName, "update").Inc()
		return err
	}
	return nil
}

func DeleteVGIfExist(log logger.Logger, metrics monitoring.Metrics, sdsCache *cache.Cache, vgName string) error {
	vgs, _ := sdsCache.GetVGs()

	vgExist := false
	for _, vg := range vgs {
		if vg.VGName == vgName {
			vgExist = true
			break
		}
	}

	if !vgExist {
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
		err := updateLVGConditionIfNeeded(ctx, cl, log, lvg, v1.ConditionFalse, internal.VGConfigurationAppliedType, internal.Pending, "trying to apply the configuration")
		if err != nil {
			log.Error(err, fmt.Sprintf("[UpdateVGTagIfNeeded] unable to add the condition %s status False reason %s to the LVMVolumeGroup %s", internal.VGConfigurationAppliedType, internal.Pending, lvg.Name))
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
