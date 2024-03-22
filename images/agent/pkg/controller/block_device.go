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
	"crypto/sha1"
	"fmt"
	"os"
	"regexp"
	"sds-node-configurator/api/v1alpha1"
	"sds-node-configurator/config"
	"sds-node-configurator/internal"
	"sds-node-configurator/pkg/logger"
	"sds-node-configurator/pkg/monitoring"
	"sds-node-configurator/pkg/utils"
	"strings"
	"time"

	"k8s.io/api/policy/v1beta1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

const (
	blockDeviceCtrlName = "block-device-controller"
)

func RunBlockDeviceController(
	ctx context.Context,
	mgr manager.Manager,
	cfg config.Options,
	log logger.Logger,
	metrics monitoring.Metrics,
) (controller.Controller, error) {
	cl := mgr.GetClient()
	cache := mgr.GetCache()

	c, err := controller.New(blockDeviceCtrlName, mgr, controller.Options{
		Reconciler: reconcile.Func(func(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
			return reconcile.Result{}, nil
		}),
	})

	if err != nil {
		log.Error(err, "[RunBlockDeviceController] unable to create controller")
		return nil, err
	}

	err = c.Watch(source.Kind(cache, &v1alpha1.BlockDevice{}), &handler.EnqueueRequestForObject{})
	if err != nil {
		log.Error(err, "[RunBlockDeviceController] unable to controller watch")
	}

	log.Info("[RunBlockDeviceController] Start loop scan block devices")

	go func() {
		for {
			time.Sleep(cfg.BlockDeviceScanInterval * time.Second)
			reconcileStart := time.Now()

			log.Info("[RunBlockDeviceController] START reconcile of block devices")

			candidates, err := GetBlockDeviceCandidates(log, cfg, metrics)
			if err != nil {
				log.Error(err, "[RunBlockDeviceController] unable to GetBlockDeviceCandidates")
				continue
			}

			apiBlockDevices, err := GetAPIBlockDevices(ctx, cl, metrics)
			if err != nil {
				log.Error(err, "[RunBlockDeviceController] unable to GetAPIBlockDevices")
				continue
			}

			// create new API devices
			for _, candidate := range candidates {
				if blockDevice, exist := apiBlockDevices[candidate.Name]; exist {
					if !hasBlockDeviceDiff(blockDevice.Status, candidate) {
						log.Debug(fmt.Sprintf(`[RunBlockDeviceController] no data to update for block device, name: "%s"`, candidate.Name))
						continue
					}

					if err := UpdateAPIBlockDevice(ctx, cl, metrics, blockDevice, candidate); err != nil {
						log.Error(err, "[RunBlockDeviceController] unable to update blockDevice, name: %s", blockDevice.Name)
						continue
					}

					log.Info(fmt.Sprintf(`[RunBlockDeviceController] updated APIBlockDevice, name: %s`, blockDevice.Name))
				} else {
					device, err := CreateAPIBlockDevice(ctx, cl, metrics, candidate)
					if err != nil {
						log.Error(err, fmt.Sprintf("[RunBlockDeviceController] unable to create block device blockDevice, name: %s", candidate.Name))
						continue
					}
					log.Info(fmt.Sprintf("[RunBlockDeviceController] created new APIBlockDevice: %s", candidate.Name))

					// add new api device to the map, so it won't be deleted as fantom
					apiBlockDevices[candidate.Name] = *device
				}
			}

			// delete api device if device no longer exists, but we still have its api resource
			RemoveDeprecatedAPIDevices(ctx, cl, log, metrics, candidates, apiBlockDevices, cfg.NodeName)

			log.Info("[RunBlockDeviceController] END reconcile of block devices")
			metrics.ReconcileDuration(blockDeviceCtrlName).Observe(metrics.GetEstimatedTimeInSeconds(reconcileStart))
			metrics.ReconcilesCountTotal(blockDeviceCtrlName).Inc()
		}
	}()

	return c, err
}

func hasBlockDeviceDiff(res v1alpha1.BlockDeviceStatus, candidate internal.BlockDeviceCandidate) bool {
	candSizeTmp := resource.NewQuantity(candidate.Size.Value(), resource.BinarySI)
	return candidate.NodeName != res.NodeName ||
		candidate.Consumable != res.Consumable ||
		candidate.PVUuid != res.PVUuid ||
		candidate.VGUuid != res.VGUuid ||
		candidate.PartUUID != res.PartUUID ||
		candidate.LvmVolumeGroupName != res.LvmVolumeGroupName ||
		candidate.ActualVGNameOnTheNode != res.ActualVGNameOnTheNode ||
		candidate.Wwn != res.Wwn ||
		candidate.Serial != res.Serial ||
		candidate.Path != res.Path ||
		candSizeTmp.String() != res.Size ||
		candidate.Rota != res.Rota ||
		candidate.Model != res.Model ||
		candidate.HotPlug != res.HotPlug ||
		candidate.Type != res.Type ||
		v1beta1.FSType(candidate.FSType) != res.FsType ||
		candidate.MachineId != res.MachineID
}

func GetAPIBlockDevices(ctx context.Context, kc kclient.Client, metrics monitoring.Metrics) (map[string]v1alpha1.BlockDevice, error) {
	listDevice := &v1alpha1.BlockDeviceList{
		TypeMeta: metav1.TypeMeta{
			Kind:       v1alpha1.BlockDeviceKind,
			APIVersion: v1alpha1.TypeMediaAPIVersion,
		},
		ListMeta: metav1.ListMeta{},
		Items:    []v1alpha1.BlockDevice{},
	}

	start := time.Now()
	err := kc.List(ctx, listDevice)
	metrics.ApiMethodsDuration(blockDeviceCtrlName, "list").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.ApiMethodsExecutionCount(blockDeviceCtrlName, "list").Inc()
	if err != nil {
		metrics.ApiMethodsErrors(blockDeviceCtrlName, "list").Inc()
		return nil, fmt.Errorf("unable to kc.List, error: %w", err)
	}

	devices := make(map[string]v1alpha1.BlockDevice, len(listDevice.Items))
	for _, blockDevice := range listDevice.Items {
		devices[blockDevice.Name] = blockDevice
	}
	return devices, nil
}

func RemoveDeprecatedAPIDevices(
	ctx context.Context,
	cl kclient.Client,
	log logger.Logger,
	metrics monitoring.Metrics,
	candidates []internal.BlockDeviceCandidate,
	apiBlockDevices map[string]v1alpha1.BlockDevice,
	nodeName string) {

	actualCandidates := make(map[string]struct{}, len(candidates))
	for _, candidate := range candidates {
		actualCandidates[candidate.Name] = struct{}{}
	}

	for name, device := range apiBlockDevices {
		if shouldDeleteBlockDevice(device, actualCandidates, nodeName) {
			err := DeleteAPIBlockDevice(ctx, cl, metrics, name)
			if err != nil {
				log.Error(err, fmt.Sprintf("[RunBlockDeviceController] unable to delete APIBlockDevice, name: %s", name))
				continue
			}

			delete(apiBlockDevices, name)
			log.Info(fmt.Sprintf("[RunBlockDeviceController] device deleted, name: %s", name))
		}
	}
}

func shouldDeleteBlockDevice(bd v1alpha1.BlockDevice, actualCandidates map[string]struct{}, nodeName string) bool {
	if bd.Status.NodeName == nodeName &&
		bd.Status.Consumable &&
		isBlockDeviceDeprecated(bd.Name, actualCandidates) {
		return true
	}

	return false
}

func isBlockDeviceDeprecated(blockDevice string, actualCandidates map[string]struct{}) bool {
	_, ok := actualCandidates[blockDevice]
	return !ok
}

func GetBlockDeviceCandidates(log logger.Logger, cfg config.Options, metrics monitoring.Metrics) ([]internal.BlockDeviceCandidate, error) {
	start := time.Now()
	devices, cmdStr, err := utils.GetBlockDevices()
	metrics.UtilsCommandsDuration(blockDeviceCtrlName, "lsblk").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.UtilsCommandsExecutionCount(blockDeviceCtrlName, "lsblk").Inc()
	log.Debug(fmt.Sprintf("[GetBlockDeviceCandidates] exec cmd: %s", cmdStr))
	if err != nil {
		metrics.UtilsCommandsErrorsCount(blockDeviceCtrlName, "lsblk").Inc()
		return nil, fmt.Errorf("unable to GetBlockDevices, err: %w", err)
	}

	filteredDevices, err := FilterDevices(log, devices)
	if err != nil {
		log.Error(err, "[GetBlockDeviceCandidates] unable to filter devices")
		return nil, err
	}

	start = time.Now()
	pvs, cmdStr, _, err := utils.GetAllPVs()
	metrics.UtilsCommandsDuration(blockDeviceCtrlName, "pvs").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.UtilsCommandsExecutionCount(blockDeviceCtrlName, "pvs").Inc()
	log.Debug(fmt.Sprintf("[GetBlockDeviceCandidates] exec cmd: %s", cmdStr))
	if err != nil {
		metrics.UtilsCommandsErrorsCount(blockDeviceCtrlName, "pvs").Inc()
		log.Error(err, "[GetBlockDeviceCandidates] unable to GetAllPVs")
		return nil, err
	}

	var delFlag bool

	var candidates []internal.BlockDeviceCandidate
	for _, device := range filteredDevices {
		log.Trace(fmt.Sprintf("[GetBlockDeviceCandidates] Process device: %+v", device))
		candidate := internal.BlockDeviceCandidate{
			NodeName:   cfg.NodeName,
			Consumable: CheckConsumable(device),
			Wwn:        device.Wwn,
			Serial:     device.Serial,
			Path:       device.Name,
			Size:       device.Size,
			Rota:       device.Rota,
			Model:      device.Model,
			HotPlug:    device.HotPlug,
			KName:      device.KName,
			PkName:     device.PkName,
			Type:       device.Type,
			FSType:     device.FSType,
			MachineId:  cfg.MachineId,
			PartUUID:   device.PartUUID,
		}

		log.Trace(fmt.Sprintf("[GetBlockDeviceCandidates] Get following candidate: %+v", candidate))
		candidateName := CreateCandidateName(log, candidate)

		if candidateName == "" {
			log.Trace("[GetBlockDeviceCandidates] candidateName is empty. Skipping device")
			continue
		}

		candidate.Name = candidateName
		log.Trace(fmt.Sprintf("[GetBlockDeviceCandidates] Generated a unique candidate name: %s", candidate.Name))

		delFlag = false
		for _, pv := range pvs {
			if pv.PVName == device.Name {
				log.Trace(fmt.Sprintf("[GetBlockDeviceCandidates] The device is a PV. Found PV name: %s", pv.PVName))
				if candidate.FSType == internal.LVMFSType {
					hasTag, lvmVGName := CheckTag(pv.VGTags)
					if hasTag {
						candidate.PVUuid = pv.PVUuid
						candidate.VGUuid = pv.VGUuid
						candidate.ActualVGNameOnTheNode = pv.VGName
						candidate.LvmVolumeGroupName = lvmVGName
					} else {
						if len(pv.VGName) != 0 {
							log.Trace(fmt.Sprintf("[GetBlockDeviceCandidates] The device is a PV with VG named %s that lacks our tag %s. Removing it from Kubernetes", pv.VGName, internal.LVMTags[0]))
							delFlag = true
						} else {
							candidate.PVUuid = pv.PVUuid
						}
					}
				}
			}
		}
		log.Trace(fmt.Sprintf("[GetBlockDeviceCandidates] delFlag: %t", delFlag))
		if delFlag {
			continue
		}
		log.Trace("[GetBlockDeviceCandidates] Append candidate to candidates")
		candidates = append(candidates, candidate)
	}

	return candidates, nil
}

func FilterDevices(log logger.Logger, devices []internal.Device) ([]internal.Device, error) {
	log.Trace(fmt.Sprintf("[filterDevices] devices before type filtration: %+v", devices))

	validTypes := make([]internal.Device, 0, len(devices))

	for _, device := range devices {
		if !strings.HasPrefix(device.Name, internal.DRBDName) &&
			hasValidType(device.Type) &&
			hasValidFSType(device.FSType) {
			validTypes = append(validTypes, device)
		}
	}

	log.Trace(fmt.Sprintf("[filterDevices] devices after type filtration: %+v", validTypes))

	pkNames := make(map[string]struct{}, len(validTypes))
	for _, device := range devices {
		if device.PkName != "" {
			log.Trace(fmt.Sprintf("[filterDevices] find parent %s for child : %+v.", device.PkName, device))
			pkNames[device.PkName] = struct{}{}
		}
	}
	log.Trace(fmt.Sprintf("[filterDevices] pkNames: %+v", pkNames))

	filtered := make([]internal.Device, 0, len(validTypes))
	for _, device := range validTypes {
		if !isParent(device.KName, pkNames) || device.FSType == internal.LVMFSType {
			validSize, err := hasValidSize(device.Size)
			if err != nil {
				return nil, err
			}

			if validSize {
				filtered = append(filtered, device)
			}
		}
	}

	log.Trace(fmt.Sprintf("[filterDevices] final filtered devices: %+v", filtered))

	return filtered, nil
}

func hasValidSize(size resource.Quantity) (bool, error) {
	limitSize, err := resource.ParseQuantity(internal.BlockDeviceValidSize)
	if err != nil {
		return false, err
	}

	return size.Value() >= limitSize.Value(), nil
}

func isParent(kName string, pkNames map[string]struct{}) bool {
	_, ok := pkNames[kName]
	return ok
}

func hasValidType(deviceType string) bool {
	for _, invalidType := range internal.InvalidDeviceTypes {
		if deviceType == invalidType {
			return false
		}
	}

	return true
}

func hasValidFSType(fsType string) bool {
	if fsType == "" {
		return true
	}

	for _, allowedType := range internal.AllowedFSTypes {
		if fsType == allowedType {
			return true
		}
	}

	return false
}

func CheckConsumable(device internal.Device) bool {
	if device.MountPoint != "" {
		return false
	}

	if device.FSType != "" {
		return false
	}

	if device.HotPlug {
		return false
	}

	return true
}

func CheckTag(tags string) (bool, string) {
	if !strings.Contains(tags, internal.LVMTags[0]) {
		return false, ""
	}

	splitTags := strings.Split(tags, ",")
	for _, tag := range splitTags {
		if strings.HasPrefix(tag, "storage.deckhouse.io/lvmVolumeGroupName") {
			kv := strings.Split(tag, "=")
			return true, kv[1]
		}
	}

	return true, ""
}

func CreateCandidateName(log logger.Logger, candidate internal.BlockDeviceCandidate) string {
	if len(candidate.Serial) == 0 {
		log.Trace(fmt.Sprintf("[CreateCandidateName] Serial number is empty for device: %s", candidate.Path))
		if candidate.Type == internal.TypePart {
			if len(candidate.PartUUID) == 0 {
				log.Warning(fmt.Sprintf("[CreateCandidateName] Type = part and cannot get PartUUID; skipping this device, path: %s", candidate.Path))
				return ""
			}
			log.Trace(fmt.Sprintf("[CreateCandidateName] Type = part and PartUUID is not empty; skiping getting serial number for device: %s", candidate.Path))
		} else {
			log.Debug(fmt.Sprintf("[CreateCandidateName] Serial number is empty and device type is not part; trying to obtain serial number or its equivalent for device: %s, with type: %s", candidate.Path, candidate.Type))

			isMdRaid := false
			matched, err := regexp.MatchString(`raid.*`, candidate.Type)
			if err != nil {
				log.Error(err, "[CreateCandidateName] failed to match regex - unable to determine if the device is an mdraid. Attempting to retrieve serial number directly from the device")
			} else if matched {
				log.Trace("[CreateCandidateName] device is mdraid")
				isMdRaid = true
			}
			serial, err := readSerialBlockDevice(candidate.Path, isMdRaid)
			if err != nil {
				log.Warning(fmt.Sprintf("[CreateCandidateName] Unable to obtain serial number or its equivalent; skipping device: %s. Error: %s", candidate.Path, err))
				return ""
			}
			log.Info(fmt.Sprintf("[CreateCandidateName] Successfully obtained serial number or its equivalent: %s for device: %s", serial, candidate.Path))
			candidate.Serial = serial
			log.Trace(fmt.Sprintf("[CreateCandidateName] Serial number is now: %s. Creating candidate name", candidate.Serial))
		}
	}

	return CreateUniqDeviceName(candidate)
}

func CreateUniqDeviceName(can internal.BlockDeviceCandidate) string {
	temp := fmt.Sprintf("%s%s%s%s%s", can.NodeName, can.Wwn, can.Model, can.Serial, can.PartUUID)
	s := fmt.Sprintf("dev-%x", sha1.Sum([]byte(temp)))
	return s
}

func readSerialBlockDevice(deviceName string, isMdRaid bool) (string, error) {
	if len(deviceName) < 6 {
		return "", fmt.Errorf("device name is too short")
	}
	strPath := fmt.Sprintf("/sys/block/%s/serial", deviceName[5:])

	if isMdRaid {
		strPath = fmt.Sprintf("/sys/block/%s/md/uuid", deviceName[5:])
	}

	serial, err := os.ReadFile(strPath)
	if err != nil {
		return "", fmt.Errorf("unable to read serial from block device: %s, error: %s", deviceName, err)
	}
	if len(serial) == 0 {
		return "", fmt.Errorf("serial is empty")
	}
	return string(serial), nil
}

func UpdateAPIBlockDevice(ctx context.Context, kc kclient.Client, metrics monitoring.Metrics, res v1alpha1.BlockDevice, candidate internal.BlockDeviceCandidate) error {
	candidateSizeTmp := resource.NewQuantity(candidate.Size.Value(), resource.BinarySI)
	device := &v1alpha1.BlockDevice{
		TypeMeta: metav1.TypeMeta{
			Kind:       v1alpha1.BlockDeviceKind,
			APIVersion: v1alpha1.TypeMediaAPIVersion,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            res.Name,
			ResourceVersion: res.ResourceVersion,
			OwnerReferences: res.OwnerReferences,
		},
		Status: v1alpha1.BlockDeviceStatus{
			Type:                  candidate.Type,
			FsType:                v1beta1.FSType(candidate.FSType),
			NodeName:              candidate.NodeName,
			Consumable:            candidate.Consumable,
			PVUuid:                candidate.PVUuid,
			VGUuid:                candidate.VGUuid,
			PartUUID:              candidate.PartUUID,
			LvmVolumeGroupName:    candidate.LvmVolumeGroupName,
			ActualVGNameOnTheNode: candidate.ActualVGNameOnTheNode,
			Wwn:                   candidate.Wwn,
			Serial:                candidate.Serial,
			Path:                  candidate.Path,
			Size:                  candidateSizeTmp.String(),
			Model:                 candidate.Model,
			Rota:                  candidate.Rota,
			HotPlug:               candidate.HotPlug,
			MachineID:             candidate.MachineId,
		},
	}

	start := time.Now()
	err := kc.Update(ctx, device)
	metrics.ApiMethodsDuration(blockDeviceCtrlName, "update").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.ApiMethodsExecutionCount(blockDeviceCtrlName, "update").Inc()
	if err != nil {
		metrics.ApiMethodsErrors(blockDeviceCtrlName, "update").Inc()
		return err
	}

	return nil
}

func CreateAPIBlockDevice(ctx context.Context, kc kclient.Client, metrics monitoring.Metrics, candidate internal.BlockDeviceCandidate) (*v1alpha1.BlockDevice, error) {
	candidateSizeTmp := resource.NewQuantity(candidate.Size.Value(), resource.BinarySI)
	device := &v1alpha1.BlockDevice{
		TypeMeta: metav1.TypeMeta{
			Kind:       v1alpha1.BlockDeviceKind,
			APIVersion: v1alpha1.TypeMediaAPIVersion,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            candidate.Name,
			OwnerReferences: []metav1.OwnerReference{},
		},
		Status: v1alpha1.BlockDeviceStatus{
			Type:                  candidate.Type,
			FsType:                v1beta1.FSType(candidate.FSType),
			NodeName:              candidate.NodeName,
			Consumable:            candidate.Consumable,
			PVUuid:                candidate.PVUuid,
			VGUuid:                candidate.VGUuid,
			PartUUID:              candidate.PartUUID,
			LvmVolumeGroupName:    candidate.LvmVolumeGroupName,
			ActualVGNameOnTheNode: candidate.ActualVGNameOnTheNode,
			Wwn:                   candidate.Wwn,
			Serial:                candidate.Serial,
			Path:                  candidate.Path,
			Size:                  candidateSizeTmp.String(),
			Model:                 candidate.Model,
			Rota:                  candidate.Rota,
			MachineID:             candidate.MachineId,
		},
	}

	start := time.Now()
	err := kc.Create(ctx, device)
	metrics.ApiMethodsDuration(blockDeviceCtrlName, "create").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.ApiMethodsExecutionCount(blockDeviceCtrlName, "create").Inc()
	if err != nil {
		metrics.ApiMethodsErrors(blockDeviceCtrlName, "create").Inc()
		return nil, err
	}
	return device, nil
}

func DeleteAPIBlockDevice(ctx context.Context, kc kclient.Client, metrics monitoring.Metrics, deviceName string) error {
	device := &v1alpha1.BlockDevice{
		ObjectMeta: metav1.ObjectMeta{
			Name: deviceName,
		},
		TypeMeta: metav1.TypeMeta{
			Kind:       v1alpha1.BlockDeviceKind,
			APIVersion: v1alpha1.TypeMediaAPIVersion,
		},
	}

	start := time.Now()
	err := kc.Delete(ctx, device)
	metrics.ApiMethodsDuration(blockDeviceCtrlName, "delete").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.ApiMethodsExecutionCount(blockDeviceCtrlName, "delete").Inc()
	if err != nil {
		metrics.ApiMethodsErrors(blockDeviceCtrlName, "delete").Inc()
		return err
	}
	return nil
}

func ReTag(log logger.Logger, metrics monitoring.Metrics) error {
	// thin pool
	log.Debug("[ReTag] start re-tagging LV")
	start := time.Now()
	lvs, cmdStr, _, err := utils.GetAllLVs()
	metrics.UtilsCommandsDuration(blockDeviceCtrlName, "lvs").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.UtilsCommandsExecutionCount(blockDeviceCtrlName, "lvs").Inc()
	log.Debug(fmt.Sprintf("[ReTag] exec cmd: %s", cmdStr))
	if err != nil {
		metrics.UtilsCommandsErrorsCount(blockDeviceCtrlName, "lvs").Inc()
		log.Error(err, "[ReTag] unable to GetAllLVs")
		return err
	}

	for _, lv := range lvs {
		tags := strings.Split(lv.LvTags, ",")
		for _, tag := range tags {
			if strings.Contains(tag, internal.LVMTags[0]) {
				continue
			}

			if strings.Contains(tag, internal.LVMTags[1]) {
				start = time.Now()
				cmdStr, err = utils.LVChangeDelTag(lv, tag)
				metrics.UtilsCommandsDuration(blockDeviceCtrlName, "lvchange").Observe(metrics.GetEstimatedTimeInSeconds(start))
				metrics.UtilsCommandsExecutionCount(blockDeviceCtrlName, "lvchange").Inc()
				if err != nil {
					metrics.UtilsCommandsErrorsCount(blockDeviceCtrlName, "lvchange").Inc()
					log.Error(err, "[ReTag] unable to LVChangeDelTag")
					return err
				}

				start = time.Now()
				cmdStr, err = utils.VGChangeAddTag(lv.VGName, internal.LVMTags[0])
				metrics.UtilsCommandsDuration(blockDeviceCtrlName, "vgchange").Observe(metrics.GetEstimatedTimeInSeconds(start))
				metrics.UtilsCommandsExecutionCount(blockDeviceCtrlName, "vgchange").Inc()
				log.Debug(fmt.Sprintf("[ReTag] exec cmd: %s", cmdStr))
				if err != nil {
					metrics.UtilsCommandsErrorsCount(blockDeviceCtrlName, "vgchange").Inc()
					log.Error(err, "[ReTag] unable to VGChangeAddTag")
					return err
				}
			}
		}
	}
	log.Debug("[ReTag] end re-tagging LV")

	log.Debug("[ReTag] start re-tagging LVM")
	start = time.Now()
	vgs, cmdStr, _, err := utils.GetAllVGs()
	metrics.UtilsCommandsDuration(blockDeviceCtrlName, "vgs").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.UtilsCommandsExecutionCount(blockDeviceCtrlName, "vgs").Inc()
	log.Debug(fmt.Sprintf("[ReTag] exec cmd: %s", cmdStr))
	if err != nil {
		metrics.UtilsCommandsErrorsCount(blockDeviceCtrlName, cmdStr).Inc()
		log.Error(err, "[ReTag] unable to GetAllVGs")
		return err
	}

	for _, vg := range vgs {
		tags := strings.Split(vg.VGTags, ",")
		for _, tag := range tags {
			if strings.Contains(tag, internal.LVMTags[0]) {
				continue
			}

			if strings.Contains(tag, internal.LVMTags[1]) {
				start = time.Now()
				cmdStr, err = utils.VGChangeDelTag(vg.VGName, tag)
				metrics.UtilsCommandsDuration(blockDeviceCtrlName, "vgchange").Observe(metrics.GetEstimatedTimeInSeconds(start))
				metrics.UtilsCommandsExecutionCount(blockDeviceCtrlName, "vgchange").Inc()
				log.Debug(fmt.Sprintf("[ReTag] exec cmd: %s", cmdStr))
				if err != nil {
					metrics.UtilsCommandsErrorsCount(blockDeviceCtrlName, "vgchange").Inc()
					log.Error(err, "[ReTag] unable to VGChangeDelTag")
					return err
				}

				start = time.Now()
				cmdStr, err = utils.VGChangeAddTag(vg.VGName, internal.LVMTags[0])
				metrics.UtilsCommandsDuration(blockDeviceCtrlName, "vgchange").Observe(metrics.GetEstimatedTimeInSeconds(start))
				metrics.UtilsCommandsExecutionCount(blockDeviceCtrlName, "vgchange").Inc()
				log.Debug(fmt.Sprintf("[ReTag] exec cmd: %s", cmdStr))
				if err != nil {
					metrics.UtilsCommandsErrorsCount(blockDeviceCtrlName, "vgchange").Inc()
					log.Error(err, "[ReTag] unable to VGChangeAddTag")
					return err
				}
			}
		}
	}
	log.Debug("[ReTag] stop re-tagging LVM")

	return nil
}
