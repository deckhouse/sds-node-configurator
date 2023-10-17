package controller

import (
	"context"
	"crypto/sha1"
	"fmt"
	"k8s.io/api/policy/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
	"storage-configurator/api/v1alpha1"
	"storage-configurator/config"
	"storage-configurator/internal"
	"storage-configurator/pkg/log"
	"storage-configurator/pkg/utils"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

const (
	blockDeviceCtrlName = "block-device-controller"
)

func RunBlockDeviceController(
	ctx context.Context,
	mgr manager.Manager,
	cfg config.Options,
	log log.Logger,
) (controller.Controller, error) {
	cl := mgr.GetClient()
	cache := mgr.GetCache()

	c, err := controller.New(blockDeviceCtrlName, mgr, controller.Options{
		Reconciler: reconcile.Func(func(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
			return reconcile.Result{}, nil
		}),
	})
	if err != nil {
		log.Error(err, "Unable to create controller")
		return nil, err
	}

	err = c.Watch(source.Kind(cache, &v1alpha1.BlockDevice{}), &handler.EnqueueRequestForObject{})
	if err != nil {
		log.Error(err, "Unable to controller watch")
	}

	log.Info("Start loop scan block devices")
	go func() {
		for {
			time.Sleep(cfg.ScanInterval * time.Second)

			candidates, err := GetCandidates(log, cfg)
			if err != nil {
				log.Error(err, "Unable to GetCandidates")
			}

			// reconciliation of local devices with an external list
			// read kubernetes list device
			apiBlockDevices, err := GetAPIBlockDevices(ctx, cl)
			if err != nil {
				log.Error(err, "Unable to GetAPIBlockDevices")
			}

			// create new API devices
			for _, candidate := range candidates {
				if _, ok := apiBlockDevices[candidate.Name]; ok {
					log.Info(fmt.Sprintf("Block device %s is already exist", candidate.Name))
					continue
				} else {
					deviceStatus, err := CreateAPIBlockDevice(ctx, cl, candidate)
					if err != nil {
						log.Error(err, "Unable to CreateAPIBlockDevice")
						continue
					}
					log.Info("Created new APIBlockDevice: " + candidate.Name)

					// add new api device to the map, so it won't be deleted as fantom
					apiBlockDevices[candidate.Name] = *deviceStatus
				}
			}

			// delete api device if device no longer exists, but we still have its api resource
			RemoveDeprecatedAPIDevices(ctx, cl, log, candidates, apiBlockDevices, cfg.NodeName)
		}
	}()

	return c, err
}

func GetAPIBlockDevices(ctx context.Context, kc kclient.Client) (map[string]v1alpha1.BlockDeviceStatus, error) {
	deviceStatuses := make(map[string]v1alpha1.BlockDeviceStatus)
	listDevice := &v1alpha1.BlockDeviceList{
		TypeMeta: metav1.TypeMeta{
			Kind:       v1alpha1.BlockDeviceKind,
			APIVersion: v1alpha1.TypeMediaAPIVersion,
		},
		ListMeta: metav1.ListMeta{},
		Items:    []v1alpha1.BlockDevice{},
	}

	err := kc.List(ctx, listDevice)
	if err != nil {
		return nil, fmt.Errorf("Unable to kc.List, error: %w", err)
	}

	for _, blockDevice := range listDevice.Items {
		deviceStatuses[blockDevice.Name] = blockDevice.Status
	}
	return deviceStatuses, nil
}

func RemoveDeprecatedAPIDevices(
	ctx context.Context,
	cl kclient.Client,
	log log.Logger,
	candidates []internal.Candidate,
	apiBlockDevices map[string]v1alpha1.BlockDeviceStatus,
	nodeName string) {

	actualCandidates := make(map[string]struct{}, len(candidates))
	for _, candidate := range candidates {
		actualCandidates[candidate.Name] = struct{}{}
	}

	for name, status := range apiBlockDevices {
		if checkAPIBlockDeviceDeprecated(name, actualCandidates) &&
			status.NodeName == nodeName {
			err := DeleteAPIBlockDevice(ctx, cl, name)
			if err != nil {
				log.Error(err, fmt.Sprintf("Unable to delete APIBlockDevice, name: %s", name))
				continue
			}
			delete(apiBlockDevices, name)
			log.Info(fmt.Sprintf("Device deleted, name: %s", name))
		}
	}
}

func checkAPIBlockDeviceDeprecated(apiDeviceName string, actualCandidates map[string]struct{}) bool {
	_, ok := actualCandidates[apiDeviceName]
	return !ok
}

func GetCandidates(log log.Logger, cfg config.Options) ([]internal.Candidate, error) {
	devices, cmdStr, err := utils.GetBlockDevices()
	log.Debug(fmt.Sprintf("[GetCandidates] exec cmd: %s", cmdStr))
	if err != nil {
		return nil, fmt.Errorf("unable to GetBlockDevices, err: %w", err)
	}

	filteredDevices := filterDevices(devices)

	pvs, cmdStr, err := utils.GetAllPVs()
	log.Debug(fmt.Sprintf("[GetCandidates] exec cmd: %s", cmdStr))
	if err != nil {
		return nil, fmt.Errorf("unable to GetAllPVs, err: %w", err)
	}

	// Наполняем кандидатов информацией.
	var candidates []internal.Candidate
	for _, device := range filteredDevices {
		candidate := internal.Candidate{
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
		}

		candidate.Name = CreateUniqDeviceName(candidate)

		for _, pv := range pvs {
			if pv.PVName == device.Name {
				candidate.ActualVGNameOnTheNode = pv.VGName

				if candidate.FSType == internal.LVMFSType {
					hasTag, lvmVGName := CheckTag(pv.VGTags)
					if hasTag {
						candidate.PVUuid = pv.PVUuid
						candidate.VGUuid = pv.VGUuid
						candidate.ActualVGNameOnTheNode = pv.VGName
						candidate.LvmVolumeGroupName = lvmVGName
					}
				}
			}
		}

		candidates = append(candidates, candidate)
	}

	return candidates, nil
}

func filterDevices(devices []internal.Device) []internal.Device {
	pkNames := make(map[string]struct{})

	for _, device := range devices {
		pkNames[device.PkName] = struct{}{}
	}

	filtered := make([]internal.Device, 0, len(devices))

	for _, device := range devices {
		if !isParent(device.KName, pkNames) &&
			hasValidType(device.Type) &&
			hasValidFSType(device.FSType) {
			filtered = append(filtered, device)
		}
	}

	return filtered
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

	if strings.HasPrefix(device.Name, internal.DRBDName) {
		return false
	}

	return true
}

func CheckTag(tags string) (bool, string) {
	if !strings.Contains(tags, "storage.deckhouse.io/enabled=true") {
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

func CreateUniqDeviceName(can internal.Candidate) string {
	temp := fmt.Sprintf("%s%s%s%s%s", can.NodeName, can.Wwn, can.Path, can.Size, can.Model)
	s := fmt.Sprintf("dev-%x", sha1.Sum([]byte(temp)))
	return s
}

func CreateAPIBlockDevice(ctx context.Context, kc kclient.Client, candidate internal.Candidate) (*v1alpha1.BlockDeviceStatus, error) {
	device := &v1alpha1.BlockDevice{
		ObjectMeta: metav1.ObjectMeta{
			Name:            candidate.Name,
			OwnerReferences: []metav1.OwnerReference{},
		},
		TypeMeta: metav1.TypeMeta{
			Kind:       v1alpha1.BlockDeviceKind,
			APIVersion: v1alpha1.TypeMediaAPIVersion,
		},
		Status: v1alpha1.BlockDeviceStatus{
			Type:                  candidate.Type,
			FsType:                v1beta1.FSType(candidate.FSType),
			NodeName:              candidate.NodeName,
			Consumable:            candidate.Consumable,
			PVUuid:                candidate.PVUuid,
			VGUuid:                candidate.VGUuid,
			LvmVolumeGroupName:    candidate.LvmVolumeGroupName,
			ActualVGNameOnTheNode: candidate.ActualVGNameOnTheNode,
			Wwn:                   candidate.Wwn,
			Serial:                candidate.Serial,
			Path:                  candidate.Path,
			Size:                  candidate.Size,
			Model:                 candidate.Model,
			Rota:                  candidate.Rota,
			MachineID:             candidate.MachineId,
		},
	}

	err := kc.Create(ctx, device)
	if err != nil {
		return nil, fmt.Errorf(
			"unable to create APIBlockDevice with name \"%s\", err: %w",
			device.Name, err)
	}
	return &device.Status, nil
}

func DeleteAPIBlockDevice(ctx context.Context, kc kclient.Client, deviceName string) error {
	device := &v1alpha1.BlockDevice{
		ObjectMeta: metav1.ObjectMeta{
			Name: deviceName,
		},
		TypeMeta: metav1.TypeMeta{
			Kind:       v1alpha1.BlockDeviceKind,
			APIVersion: v1alpha1.TypeMediaAPIVersion,
		},
	}

	err := kc.Delete(ctx, device)
	if err != nil {
		return fmt.Errorf(
			"unable to delete APIBlockDevice with name \"%s\", err: %w",
			deviceName, err)
	}
	return nil
}
