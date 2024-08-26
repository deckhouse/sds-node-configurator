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
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"agent/config"
	"agent/internal"
	"agent/pkg/cache"
	"agent/pkg/logger"
	"agent/pkg/monitoring"
	"agent/pkg/utils"
)

const (
	BlockDeviceCtrlName    = "block-device-controller"
	BlockDeviceLabelPrefix = "status.blockdevice.storage.deckhouse.io"
)

func RunBlockDeviceController(
	mgr manager.Manager,
	cfg config.Options,
	log logger.Logger,
	metrics monitoring.Metrics,
	sdsCache *cache.Cache,
) (controller.Controller, error) {
	cl := mgr.GetClient()

	c, err := controller.New(BlockDeviceCtrlName, mgr, controller.Options{
		Reconciler: reconcile.Func(func(ctx context.Context, _ reconcile.Request) (reconcile.Result, error) {
			log.Info("[RunBlockDeviceController] Reconciler starts BlockDevice resources reconciliation")

			shouldRequeue := BlockDeviceReconcile(ctx, cl, log, metrics, cfg, sdsCache)
			if shouldRequeue {
				log.Warning(fmt.Sprintf("[RunBlockDeviceController] Reconciler needs a retry in %f", cfg.BlockDeviceScanIntervalSec.Seconds()))
				return reconcile.Result{
					RequeueAfter: cfg.BlockDeviceScanIntervalSec,
				}, nil
			}
			log.Info("[RunBlockDeviceController] Reconciler successfully ended BlockDevice resources reconciliation")
			return reconcile.Result{}, nil
		}),
	})

	if err != nil {
		log.Error(err, "[RunBlockDeviceController] unable to create controller")
		return nil, err
	}

	return c, err
}

func BlockDeviceReconcile(ctx context.Context, cl kclient.Client, log logger.Logger, metrics monitoring.Metrics, cfg config.Options, sdsCache *cache.Cache) bool {
	reconcileStart := time.Now()

	log.Info("[RunBlockDeviceController] START reconcile of block devices")

	candidates := GetBlockDeviceCandidates(log, cfg, sdsCache)
	if len(candidates) == 0 {
		log.Info("[RunBlockDeviceController] no block devices candidates found. Stop reconciliation")
		return false
	}

	apiBlockDevices, err := GetAPIBlockDevices(ctx, cl, metrics)
	if err != nil {
		log.Error(err, "[RunBlockDeviceController] unable to GetAPIBlockDevices")
		return true
	}

	if len(apiBlockDevices) == 0 {
		log.Debug("[RunBlockDeviceController] no BlockDevice resources were found")
	}

	// create new API devices
	for _, candidate := range candidates {
		blockDevice, exist := apiBlockDevices[candidate.Name]
		if exist {
			if !hasBlockDeviceDiff(blockDevice, candidate) {
				log.Debug(fmt.Sprintf(`[RunBlockDeviceController] no data to update for block device, name: "%s"`, candidate.Name))
				continue
			}

			if err = UpdateAPIBlockDevice(ctx, cl, metrics, blockDevice, candidate); err != nil {
				log.Error(err, "[RunBlockDeviceController] unable to update blockDevice, name: %s", blockDevice.Name)
				continue
			}

			log.Info(fmt.Sprintf(`[RunBlockDeviceController] updated APIBlockDevice, name: %s`, blockDevice.Name))
			continue
		}

		device, err := CreateAPIBlockDevice(ctx, cl, metrics, candidate)
		if err != nil {
			log.Error(err, fmt.Sprintf("[RunBlockDeviceController] unable to create block device blockDevice, name: %s", candidate.Name))
			continue
		}
		log.Info(fmt.Sprintf("[RunBlockDeviceController] created new APIBlockDevice: %s", candidate.Name))

		// add new api device to the map, so it won't be deleted as fantom
		apiBlockDevices[candidate.Name] = *device
	}

	// delete api device if device no longer exists, but we still have its api resource
	RemoveDeprecatedAPIDevices(ctx, cl, log, metrics, candidates, apiBlockDevices, cfg.NodeName)

	log.Info("[RunBlockDeviceController] END reconcile of block devices")
	metrics.ReconcileDuration(BlockDeviceCtrlName).Observe(metrics.GetEstimatedTimeInSeconds(reconcileStart))
	metrics.ReconcilesCountTotal(BlockDeviceCtrlName).Inc()

	return false
}

func hasBlockDeviceDiff(blockDevice v1alpha1.BlockDevice, candidate internal.BlockDeviceCandidate) bool {
	return candidate.NodeName != blockDevice.Status.NodeName ||
		candidate.Consumable != blockDevice.Status.Consumable ||
		candidate.PVUuid != blockDevice.Status.PVUuid ||
		candidate.VGUuid != blockDevice.Status.VGUuid ||
		candidate.PartUUID != blockDevice.Status.PartUUID ||
		candidate.LvmVolumeGroupName != blockDevice.Status.LvmVolumeGroupName ||
		candidate.ActualVGNameOnTheNode != blockDevice.Status.ActualVGNameOnTheNode ||
		candidate.Wwn != blockDevice.Status.Wwn ||
		candidate.Serial != blockDevice.Status.Serial ||
		candidate.Path != blockDevice.Status.Path ||
		candidate.Size.Value() != blockDevice.Status.Size.Value() ||
		candidate.Rota != blockDevice.Status.Rota ||
		candidate.Model != blockDevice.Status.Model ||
		candidate.HotPlug != blockDevice.Status.HotPlug ||
		candidate.Type != blockDevice.Status.Type ||
		candidate.FSType != blockDevice.Status.FsType ||
		candidate.MachineID != blockDevice.Status.MachineID ||
		!reflect.DeepEqual(ConfigureBlockDeviceLabels(blockDevice), blockDevice.Labels)
}

func GetAPIBlockDevices(ctx context.Context, kc kclient.Client, metrics monitoring.Metrics) (map[string]v1alpha1.BlockDevice, error) {
	listDevice := &v1alpha1.BlockDeviceList{}

	start := time.Now()
	err := kc.List(ctx, listDevice)
	metrics.APIMethodsDuration(BlockDeviceCtrlName, "list").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.APIMethodsExecutionCount(BlockDeviceCtrlName, "list").Inc()
	if err != nil {
		metrics.APIMethodsErrors(BlockDeviceCtrlName, "list").Inc()
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
	nodeName string,
) {
	actualCandidates := make(map[string]struct{}, len(candidates))
	for _, candidate := range candidates {
		actualCandidates[candidate.Name] = struct{}{}
	}

	for name, device := range apiBlockDevices {
		if shouldDeleteBlockDevice(device, actualCandidates, nodeName) {
			err := DeleteAPIBlockDevice(ctx, cl, metrics, &device)
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

func GetBlockDeviceCandidates(log logger.Logger, cfg config.Options, sdsCache *cache.Cache) []internal.BlockDeviceCandidate {
	var candidates []internal.BlockDeviceCandidate
	devices, _ := sdsCache.GetDevices()
	if len(devices) == 0 {
		log.Debug("[GetBlockDeviceCandidates] no devices found, returns empty candidates")
		return candidates
	}

	filteredDevices, err := FilterDevices(log, devices)
	if err != nil {
		log.Error(err, "[GetBlockDeviceCandidates] unable to filter devices")
		return nil
	}

	if len(filteredDevices) == 0 {
		log.Debug("[GetBlockDeviceCandidates] no filtered devices left, returns empty candidates")
		return candidates
	}

	pvs, _ := sdsCache.GetPVs()
	if len(pvs) == 0 {
		log.Debug("[GetBlockDeviceCandidates] no PVs found")
	}

	var delFlag bool
	candidates = make([]internal.BlockDeviceCandidate, 0, len(filteredDevices))

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
			MachineID:  cfg.MachineID,
			PartUUID:   device.PartUUID,
		}

		log.Trace(fmt.Sprintf("[GetBlockDeviceCandidates] Get following candidate: %+v", candidate))
		candidateName := CreateCandidateName(log, candidate, devices)

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
						log.Debug(fmt.Sprintf("[GetBlockDeviceCandidates] PV %s of BlockDevice %s has tag, fill the VG information", pv.PVName, candidate.Name))
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
		log.Trace(fmt.Sprintf("[GetBlockDeviceCandidates] configured candidate %+v", candidate))
		candidates = append(candidates, candidate)
	}

	return candidates
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

func CreateCandidateName(log logger.Logger, candidate internal.BlockDeviceCandidate, devices []internal.Device) string {
	if len(candidate.Serial) == 0 {
		log.Trace(fmt.Sprintf("[CreateCandidateName] Serial number is empty for device: %s", candidate.Path))
		if candidate.Type == internal.PartType {
			if len(candidate.PartUUID) == 0 {
				log.Warning(fmt.Sprintf("[CreateCandidateName] Type = part and cannot get PartUUID; skipping this device, path: %s", candidate.Path))
				return ""
			}
			log.Trace(fmt.Sprintf("[CreateCandidateName] Type = part and PartUUID is not empty; skiping getting serial number for device: %s", candidate.Path))
		} else {
			log.Debug(fmt.Sprintf("[CreateCandidateName] Serial number is empty and device type is not part; trying to obtain serial number or its equivalent for device: %s, with type: %s", candidate.Path, candidate.Type))

			switch candidate.Type {
			case internal.MultiPathType:
				log.Debug(fmt.Sprintf("[CreateCandidateName] device %s type = %s; get serial number from parent device.", candidate.Path, candidate.Type))
				log.Trace(fmt.Sprintf("[CreateCandidateName] device: %+v. Device list: %+v", candidate, devices))
				serial, err := getSerialForMultipathDevice(candidate, devices)
				if err != nil {
					log.Warning(fmt.Sprintf("[CreateCandidateName] Unable to obtain serial number or its equivalent; skipping device: %s. Error: %s", candidate.Path, err))
					return ""
				}
				candidate.Serial = serial
				log.Info(fmt.Sprintf("[CreateCandidateName] Successfully obtained serial number or its equivalent: %s for device: %s", candidate.Serial, candidate.Path))
			default:
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
			}
		}
	}

	log.Trace(fmt.Sprintf("[CreateCandidateName] Serial number is now: %s. Creating candidate name", candidate.Serial))
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

func UpdateAPIBlockDevice(ctx context.Context, kc kclient.Client, metrics monitoring.Metrics, blockDevice v1alpha1.BlockDevice, candidate internal.BlockDeviceCandidate) error {
	blockDevice.Status = v1alpha1.BlockDeviceStatus{
		Type:                  candidate.Type,
		FsType:                candidate.FSType,
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
		Size:                  *resource.NewQuantity(candidate.Size.Value(), resource.BinarySI),
		Model:                 candidate.Model,
		Rota:                  candidate.Rota,
		HotPlug:               candidate.HotPlug,
		MachineID:             candidate.MachineID,
	}

	blockDevice.Labels = ConfigureBlockDeviceLabels(blockDevice)

	start := time.Now()
	err := kc.Update(ctx, &blockDevice)
	metrics.APIMethodsDuration(BlockDeviceCtrlName, "update").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.APIMethodsExecutionCount(BlockDeviceCtrlName, "update").Inc()
	if err != nil {
		metrics.APIMethodsErrors(BlockDeviceCtrlName, "update").Inc()
		return err
	}

	return nil
}

func ConfigureBlockDeviceLabels(blockDevice v1alpha1.BlockDevice) map[string]string {
	var labels map[string]string
	if blockDevice.Labels == nil {
		labels = make(map[string]string, 16)
	} else {
		labels = make(map[string]string, len(blockDevice.Labels))
	}

	for k, v := range blockDevice.Labels {
		labels[k] = v
	}

	labels["kubernetes.io/metadata.name"] = blockDevice.ObjectMeta.Name
	labels["kubernetes.io/hostname"] = blockDevice.Status.NodeName
	labels[BlockDeviceLabelPrefix+"/type"] = blockDevice.Status.Type
	labels[BlockDeviceLabelPrefix+"/fstype"] = blockDevice.Status.FsType
	labels[BlockDeviceLabelPrefix+"/pvuuid"] = blockDevice.Status.PVUuid
	labels[BlockDeviceLabelPrefix+"/vguuid"] = blockDevice.Status.VGUuid
	labels[BlockDeviceLabelPrefix+"/partuuid"] = blockDevice.Status.PartUUID
	labels[BlockDeviceLabelPrefix+"/lvmvolumegroupname"] = blockDevice.Status.LvmVolumeGroupName
	labels[BlockDeviceLabelPrefix+"/actualvgnameonthenode"] = blockDevice.Status.ActualVGNameOnTheNode
	labels[BlockDeviceLabelPrefix+"/wwn"] = blockDevice.Status.Wwn
	labels[BlockDeviceLabelPrefix+"/serial"] = blockDevice.Status.Serial
	labels[BlockDeviceLabelPrefix+"/size"] = blockDevice.Status.Size.String()
	labels[BlockDeviceLabelPrefix+"/model"] = blockDevice.Status.Model
	labels[BlockDeviceLabelPrefix+"/rota"] = strconv.FormatBool(blockDevice.Status.Rota)
	labels[BlockDeviceLabelPrefix+"/hotplug"] = strconv.FormatBool(blockDevice.Status.HotPlug)
	labels[BlockDeviceLabelPrefix+"/machineid"] = blockDevice.Status.MachineID

	return labels
}

func CreateAPIBlockDevice(ctx context.Context, kc kclient.Client, metrics monitoring.Metrics, candidate internal.BlockDeviceCandidate) (*v1alpha1.BlockDevice, error) {
	blockDevice := &v1alpha1.BlockDevice{
		ObjectMeta: metav1.ObjectMeta{
			Name: candidate.Name,
		},
		Status: v1alpha1.BlockDeviceStatus{
			Type:                  candidate.Type,
			FsType:                candidate.FSType,
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
			Size:                  *resource.NewQuantity(candidate.Size.Value(), resource.BinarySI),
			Model:                 candidate.Model,
			Rota:                  candidate.Rota,
			MachineID:             candidate.MachineID,
		},
	}

	blockDevice.Labels = ConfigureBlockDeviceLabels(*blockDevice)
	start := time.Now()

	err := kc.Create(ctx, blockDevice)
	metrics.APIMethodsDuration(BlockDeviceCtrlName, "create").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.APIMethodsExecutionCount(BlockDeviceCtrlName, "create").Inc()
	if err != nil {
		metrics.APIMethodsErrors(BlockDeviceCtrlName, "create").Inc()
		return nil, err
	}
	return blockDevice, nil
}

func DeleteAPIBlockDevice(ctx context.Context, kc kclient.Client, metrics monitoring.Metrics, device *v1alpha1.BlockDevice) error {
	start := time.Now()
	err := kc.Delete(ctx, device)
	metrics.APIMethodsDuration(BlockDeviceCtrlName, "delete").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.APIMethodsExecutionCount(BlockDeviceCtrlName, "delete").Inc()
	if err != nil {
		metrics.APIMethodsErrors(BlockDeviceCtrlName, "delete").Inc()
		return err
	}
	return nil
}

func ReTag(ctx context.Context, log logger.Logger, metrics monitoring.Metrics) error {
	// thin pool
	log.Debug("[ReTag] start re-tagging LV")
	start := time.Now()
	lvs, cmdStr, _, err := utils.GetAllLVs(ctx)
	metrics.UtilsCommandsDuration(BlockDeviceCtrlName, "lvs").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.UtilsCommandsExecutionCount(BlockDeviceCtrlName, "lvs").Inc()
	log.Debug(fmt.Sprintf("[ReTag] exec cmd: %s", cmdStr))
	if err != nil {
		metrics.UtilsCommandsErrorsCount(BlockDeviceCtrlName, "lvs").Inc()
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
				metrics.UtilsCommandsDuration(BlockDeviceCtrlName, "lvchange").Observe(metrics.GetEstimatedTimeInSeconds(start))
				metrics.UtilsCommandsExecutionCount(BlockDeviceCtrlName, "lvchange").Inc()
				log.Debug(fmt.Sprintf("[ReTag] exec cmd: %s", cmdStr))
				if err != nil {
					metrics.UtilsCommandsErrorsCount(BlockDeviceCtrlName, "lvchange").Inc()
					log.Error(err, "[ReTag] unable to LVChangeDelTag")
					return err
				}

				start = time.Now()
				cmdStr, err = utils.VGChangeAddTag(lv.VGName, internal.LVMTags[0])
				metrics.UtilsCommandsDuration(BlockDeviceCtrlName, "vgchange").Observe(metrics.GetEstimatedTimeInSeconds(start))
				metrics.UtilsCommandsExecutionCount(BlockDeviceCtrlName, "vgchange").Inc()
				log.Debug(fmt.Sprintf("[ReTag] exec cmd: %s", cmdStr))
				if err != nil {
					metrics.UtilsCommandsErrorsCount(BlockDeviceCtrlName, "vgchange").Inc()
					log.Error(err, "[ReTag] unable to VGChangeAddTag")
					return err
				}
			}
		}
	}
	log.Debug("[ReTag] end re-tagging LV")

	log.Debug("[ReTag] start re-tagging LVM")
	start = time.Now()
	vgs, cmdStr, _, err := utils.GetAllVGs(ctx)
	metrics.UtilsCommandsDuration(BlockDeviceCtrlName, "vgs").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.UtilsCommandsExecutionCount(BlockDeviceCtrlName, "vgs").Inc()
	log.Debug(fmt.Sprintf("[ReTag] exec cmd: %s", cmdStr))
	if err != nil {
		metrics.UtilsCommandsErrorsCount(BlockDeviceCtrlName, cmdStr).Inc()
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
				metrics.UtilsCommandsDuration(BlockDeviceCtrlName, "vgchange").Observe(metrics.GetEstimatedTimeInSeconds(start))
				metrics.UtilsCommandsExecutionCount(BlockDeviceCtrlName, "vgchange").Inc()
				log.Debug(fmt.Sprintf("[ReTag] exec cmd: %s", cmdStr))
				if err != nil {
					metrics.UtilsCommandsErrorsCount(BlockDeviceCtrlName, "vgchange").Inc()
					log.Error(err, "[ReTag] unable to VGChangeDelTag")
					return err
				}

				start = time.Now()
				cmdStr, err = utils.VGChangeAddTag(vg.VGName, internal.LVMTags[0])
				metrics.UtilsCommandsDuration(BlockDeviceCtrlName, "vgchange").Observe(metrics.GetEstimatedTimeInSeconds(start))
				metrics.UtilsCommandsExecutionCount(BlockDeviceCtrlName, "vgchange").Inc()
				log.Debug(fmt.Sprintf("[ReTag] exec cmd: %s", cmdStr))
				if err != nil {
					metrics.UtilsCommandsErrorsCount(BlockDeviceCtrlName, "vgchange").Inc()
					log.Error(err, "[ReTag] unable to VGChangeAddTag")
					return err
				}
			}
		}
	}
	log.Debug("[ReTag] stop re-tagging LVM")

	return nil
}

func getSerialForMultipathDevice(candidate internal.BlockDeviceCandidate, devices []internal.Device) (string, error) {
	parentDevice := getParentDevice(candidate.PkName, devices)
	if parentDevice.Name == "" {
		err := fmt.Errorf("parent device %s not found for multipath device: %s in device list", candidate.PkName, candidate.Path)
		return "", err
	}

	if parentDevice.FSType != internal.MultiPathMemberFSType {
		err := fmt.Errorf("parent device %s for multipath device %s is not a multipath member (fstype != %s)", parentDevice.Name, candidate.Path, internal.MultiPathMemberFSType)
		return "", err
	}

	if parentDevice.Serial == "" {
		err := fmt.Errorf("serial number is empty for parent device %s", parentDevice.Name)
		return "", err
	}

	return parentDevice.Serial, nil
}

func getParentDevice(pkName string, devices []internal.Device) internal.Device {
	for _, device := range devices {
		if device.Name == pkName {
			return device
		}
	}
	return internal.Device{}
}
