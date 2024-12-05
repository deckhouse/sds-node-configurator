package bd

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
	"github.com/gosimple/slug"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"agent/internal"
	"agent/internal/cache"
	"agent/internal/controller"
	"agent/internal/logger"
	"agent/internal/monitoring"
	"agent/internal/utils"
)

const DiscovererName = "block-device-controller"

type Discoverer struct {
	cl       client.Client
	log      logger.Logger
	bdCl     *utils.BDClient
	metrics  monitoring.Metrics
	sdsCache *cache.Cache
	cfg      DiscovererConfig
}

type DiscovererConfig struct {
	BlockDeviceScanInterval time.Duration
	MachineID               string
	NodeName                string
}

func NewDiscoverer(
	cl client.Client,
	log logger.Logger,
	metrics monitoring.Metrics,
	sdsCache *cache.Cache,
	cfg DiscovererConfig,
) *Discoverer {
	return &Discoverer{
		cl:       cl,
		log:      log,
		bdCl:     utils.NewBDClient(cl, metrics),
		metrics:  metrics,
		sdsCache: sdsCache,
		cfg:      cfg,
	}
}

func (d *Discoverer) Name() string {
	return DiscovererName
}

func (d *Discoverer) Discover(ctx context.Context) (controller.Result, error) {
	d.log.Info("[RunBlockDeviceController] Reconciler starts BlockDevice resources reconciliation")

	shouldRequeue := d.blockDeviceReconcile(ctx)
	if shouldRequeue {
		d.log.Warning(fmt.Sprintf("[RunBlockDeviceController] Reconciler needs a retry in %f", d.cfg.BlockDeviceScanInterval.Seconds()))
		return controller.Result{RequeueAfter: d.cfg.BlockDeviceScanInterval}, nil
	}
	d.log.Info("[RunBlockDeviceController] Reconciler successfully ended BlockDevice resources reconciliation")
	return controller.Result{}, nil
}

func (d *Discoverer) blockDeviceReconcile(ctx context.Context) bool {
	reconcileStart := time.Now()

	d.log.Info("[RunBlockDeviceController] START reconcile of block devices")

	candidates := d.getBlockDeviceCandidates()
	if len(candidates) == 0 {
		d.log.Info("[RunBlockDeviceController] no block devices candidates found. Stop reconciliation")
		return false
	}

	apiBlockDevices, err := d.bdCl.GetAPIBlockDevices(ctx, DiscovererName, nil)
	if err != nil {
		d.log.Error(err, "[RunBlockDeviceController] unable to GetAPIBlockDevices")
		return true
	}

	if len(apiBlockDevices) == 0 {
		d.log.Debug("[RunBlockDeviceController] no BlockDevice resources were found")
	}

	// create new API devices
	for _, candidate := range candidates {
		blockDevice, exist := apiBlockDevices[candidate.Name]
		if exist {
			if !hasBlockDeviceDiff(blockDevice, candidate) {
				d.log.Debug(fmt.Sprintf(`[RunBlockDeviceController] no data to update for block device, name: "%s"`, candidate.Name))
				continue
			}

			if err = d.updateAPIBlockDevice(ctx, blockDevice, candidate); err != nil {
				d.log.Error(err, "[RunBlockDeviceController] unable to update blockDevice, name: %s", blockDevice.Name)
				continue
			}

			d.log.Info(fmt.Sprintf(`[RunBlockDeviceController] updated APIBlockDevice, name: %s`, blockDevice.Name))
			continue
		}

		device, err := d.createAPIBlockDevice(ctx, candidate)
		if err != nil {
			d.log.Error(err, fmt.Sprintf("[RunBlockDeviceController] unable to create block device blockDevice, name: %s", candidate.Name))
			continue
		}
		d.log.Info(fmt.Sprintf("[RunBlockDeviceController] created new APIBlockDevice: %s", candidate.Name))

		// add new api device to the map, so it won't be deleted as fantom
		apiBlockDevices[candidate.Name] = *device
	}

	// delete api device if device no longer exists, but we still have its api resource
	d.removeDeprecatedAPIDevices(ctx, candidates, apiBlockDevices)

	d.log.Info("[RunBlockDeviceController] END reconcile of block devices")
	d.metrics.ReconcileDuration(DiscovererName).Observe(d.metrics.GetEstimatedTimeInSeconds(reconcileStart))
	d.metrics.ReconcilesCountTotal(DiscovererName).Inc()

	return false
}

func (d *Discoverer) removeDeprecatedAPIDevices(
	ctx context.Context,
	candidates []internal.BlockDeviceCandidate,
	apiBlockDevices map[string]v1alpha1.BlockDevice,
) {
	actualCandidates := make(map[string]struct{}, len(candidates))
	for _, candidate := range candidates {
		actualCandidates[candidate.Name] = struct{}{}
	}

	for name, device := range apiBlockDevices {
		if shouldDeleteBlockDevice(device, actualCandidates, d.cfg.NodeName) {
			err := d.deleteAPIBlockDevice(ctx, &device)
			if err != nil {
				d.log.Error(err, fmt.Sprintf("[RunBlockDeviceController] unable to delete APIBlockDevice, name: %s", name))
				continue
			}

			delete(apiBlockDevices, name)
			d.log.Info(fmt.Sprintf("[RunBlockDeviceController] device deleted, name: %s", name))
		}
	}
}

func (d *Discoverer) getBlockDeviceCandidates() []internal.BlockDeviceCandidate {
	var candidates []internal.BlockDeviceCandidate
	devices, _ := d.sdsCache.GetDevices()
	if len(devices) == 0 {
		d.log.Debug("[GetBlockDeviceCandidates] no devices found, returns empty candidates")
		return candidates
	}

	filteredDevices, err := d.filterDevices(devices)
	if err != nil {
		d.log.Error(err, "[GetBlockDeviceCandidates] unable to filter devices")
		return nil
	}

	if len(filteredDevices) == 0 {
		d.log.Debug("[GetBlockDeviceCandidates] no filtered devices left, returns empty candidates")
		return candidates
	}

	pvs, _ := d.sdsCache.GetPVs()
	if len(pvs) == 0 {
		d.log.Debug("[GetBlockDeviceCandidates] no PVs found")
	}

	var delFlag bool
	candidates = make([]internal.BlockDeviceCandidate, 0, len(filteredDevices))

	for _, device := range filteredDevices {
		d.log.Trace(fmt.Sprintf("[GetBlockDeviceCandidates] Process device: %+v", device))
		candidate := internal.BlockDeviceCandidate{
			NodeName:   d.cfg.NodeName,
			Consumable: checkConsumable(device),
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
			MachineID:  d.cfg.MachineID,
			PartUUID:   device.PartUUID,
		}

		d.log.Trace(fmt.Sprintf("[GetBlockDeviceCandidates] Get following candidate: %+v", candidate))
		candidateName := d.createCandidateName(candidate, devices)

		if candidateName == "" {
			d.log.Trace("[GetBlockDeviceCandidates] candidateName is empty. Skipping device")
			continue
		}

		candidate.Name = candidateName
		d.log.Trace(fmt.Sprintf("[GetBlockDeviceCandidates] Generated a unique candidate name: %s", candidate.Name))

		delFlag = false
		for _, pv := range pvs {
			if pv.PVName == device.Name {
				d.log.Trace(fmt.Sprintf("[GetBlockDeviceCandidates] The device is a PV. Found PV name: %s", pv.PVName))
				if candidate.FSType == internal.LVMFSType {
					hasTag, lvmVGName := utils.ReadValueFromTags(pv.VGTags, internal.LVMVolumeGroupTag)
					if hasTag {
						d.log.Debug(fmt.Sprintf("[GetBlockDeviceCandidates] PV %s of BlockDevice %s has tag, fill the VG information", pv.PVName, candidate.Name))
						candidate.PVUuid = pv.PVUuid
						candidate.VGUuid = pv.VGUuid
						candidate.ActualVGNameOnTheNode = pv.VGName
						candidate.LVMVolumeGroupName = lvmVGName
					} else {
						if len(pv.VGName) != 0 {
							d.log.Trace(fmt.Sprintf("[GetBlockDeviceCandidates] The device is a PV with VG named %s that lacks our tag %s. Removing it from Kubernetes", pv.VGName, internal.LVMTags[0]))
							delFlag = true
						} else {
							candidate.PVUuid = pv.PVUuid
						}
					}
				}
			}
		}
		d.log.Trace(fmt.Sprintf("[GetBlockDeviceCandidates] delFlag: %t", delFlag))
		if delFlag {
			continue
		}
		d.log.Trace(fmt.Sprintf("[GetBlockDeviceCandidates] configured candidate %+v", candidate))
		candidates = append(candidates, candidate)
	}

	return candidates
}

func (d *Discoverer) filterDevices(devices []internal.Device) ([]internal.Device, error) {
	d.log.Trace(fmt.Sprintf("[filterDevices] devices before type filtration: %+v", devices))

	validTypes := make([]internal.Device, 0, len(devices))

	for _, device := range devices {
		if !strings.HasPrefix(device.Name, internal.DRBDName) &&
			hasValidType(device.Type) &&
			hasValidFSType(device.FSType) {
			validTypes = append(validTypes, device)
		}
	}

	d.log.Trace(fmt.Sprintf("[filterDevices] devices after type filtration: %+v", validTypes))

	pkNames := make(map[string]struct{}, len(validTypes))
	for _, device := range devices {
		if device.PkName != "" {
			d.log.Trace(fmt.Sprintf("[filterDevices] find parent %s for child : %+v.", device.PkName, device))
			pkNames[device.PkName] = struct{}{}
		}
	}
	d.log.Trace(fmt.Sprintf("[filterDevices] pkNames: %+v", pkNames))

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

	d.log.Trace(fmt.Sprintf("[filterDevices] final filtered devices: %+v", filtered))

	return filtered, nil
}

func (d *Discoverer) createCandidateName(candidate internal.BlockDeviceCandidate, devices []internal.Device) string {
	if len(candidate.Serial) == 0 {
		d.log.Trace(fmt.Sprintf("[CreateCandidateName] Serial number is empty for device: %s", candidate.Path))
		if candidate.Type == internal.PartType {
			if len(candidate.PartUUID) == 0 {
				d.log.Warning(fmt.Sprintf("[CreateCandidateName] Type = part and cannot get PartUUID; skipping this device, path: %s", candidate.Path))
				return ""
			}
			d.log.Trace(fmt.Sprintf("[CreateCandidateName] Type = part and PartUUID is not empty; skiping getting serial number for device: %s", candidate.Path))
		} else {
			d.log.Debug(fmt.Sprintf("[CreateCandidateName] Serial number is empty and device type is not part; trying to obtain serial number or its equivalent for device: %s, with type: %s", candidate.Path, candidate.Type))

			switch candidate.Type {
			case internal.MultiPathType:
				d.log.Debug(fmt.Sprintf("[CreateCandidateName] device %s type = %s; get serial number from parent device.", candidate.Path, candidate.Type))
				d.log.Trace(fmt.Sprintf("[CreateCandidateName] device: %+v. Device list: %+v", candidate, devices))
				serial, err := getSerialForMultipathDevice(candidate, devices)
				if err != nil {
					d.log.Warning(fmt.Sprintf("[CreateCandidateName] Unable to obtain serial number or its equivalent; skipping device: %s. Error: %s", candidate.Path, err))
					return ""
				}
				candidate.Serial = serial
				d.log.Info(fmt.Sprintf("[CreateCandidateName] Successfully obtained serial number or its equivalent: %s for device: %s", candidate.Serial, candidate.Path))
			default:
				isMdRaid := false
				matched, err := regexp.MatchString(`raid.*`, candidate.Type)
				if err != nil {
					d.log.Error(err, "[CreateCandidateName] failed to match regex - unable to determine if the device is an mdraid. Attempting to retrieve serial number directly from the device")
				} else if matched {
					d.log.Trace("[CreateCandidateName] device is mdraid")
					isMdRaid = true
				}
				serial, err := readSerialBlockDevice(candidate.Path, isMdRaid)
				if err != nil {
					d.log.Warning(fmt.Sprintf("[CreateCandidateName] Unable to obtain serial number or its equivalent; skipping device: %s. Error: %s", candidate.Path, err))
					return ""
				}
				d.log.Info(fmt.Sprintf("[CreateCandidateName] Successfully obtained serial number or its equivalent: %s for device: %s", serial, candidate.Path))
				candidate.Serial = serial
			}
		}
	}

	d.log.Trace(fmt.Sprintf("[CreateCandidateName] Serial number is now: %s. Creating candidate name", candidate.Serial))
	return createUniqDeviceName(candidate)
}

func (d *Discoverer) updateAPIBlockDevice(
	ctx context.Context,
	blockDevice v1alpha1.BlockDevice,
	candidate internal.BlockDeviceCandidate,
) error {
	blockDevice.Status = v1alpha1.BlockDeviceStatus{
		Type:                  candidate.Type,
		FsType:                candidate.FSType,
		NodeName:              candidate.NodeName,
		Consumable:            candidate.Consumable,
		PVUuid:                candidate.PVUuid,
		VGUuid:                candidate.VGUuid,
		PartUUID:              candidate.PartUUID,
		LVMVolumeGroupName:    candidate.LVMVolumeGroupName,
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

	blockDevice.Labels = configureBlockDeviceLabels(blockDevice)

	start := time.Now()
	err := d.cl.Update(ctx, &blockDevice)
	d.metrics.APIMethodsDuration(DiscovererName, "update").Observe(d.metrics.GetEstimatedTimeInSeconds(start))
	d.metrics.APIMethodsExecutionCount(DiscovererName, "update").Inc()
	if err != nil {
		d.metrics.APIMethodsErrors(DiscovererName, "update").Inc()
		return err
	}

	return nil
}

func (d *Discoverer) createAPIBlockDevice(ctx context.Context, candidate internal.BlockDeviceCandidate) (*v1alpha1.BlockDevice, error) {
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
			LVMVolumeGroupName:    candidate.LVMVolumeGroupName,
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

	blockDevice.Labels = configureBlockDeviceLabels(*blockDevice)
	start := time.Now()

	err := d.cl.Create(ctx, blockDevice)
	d.metrics.APIMethodsDuration(DiscovererName, "create").Observe(d.metrics.GetEstimatedTimeInSeconds(start))
	d.metrics.APIMethodsExecutionCount(DiscovererName, "create").Inc()
	if err != nil {
		d.metrics.APIMethodsErrors(DiscovererName, "create").Inc()
		return nil, err
	}
	return blockDevice, nil
}

func (d *Discoverer) deleteAPIBlockDevice(ctx context.Context, device *v1alpha1.BlockDevice) error {
	start := time.Now()
	err := d.cl.Delete(ctx, device)
	d.metrics.APIMethodsDuration(DiscovererName, "delete").Observe(d.metrics.GetEstimatedTimeInSeconds(start))
	d.metrics.APIMethodsExecutionCount(DiscovererName, "delete").Inc()
	if err != nil {
		d.metrics.APIMethodsErrors(DiscovererName, "delete").Inc()
		return err
	}
	return nil
}

func hasBlockDeviceDiff(blockDevice v1alpha1.BlockDevice, candidate internal.BlockDeviceCandidate) bool {
	return candidate.NodeName != blockDevice.Status.NodeName ||
		candidate.Consumable != blockDevice.Status.Consumable ||
		candidate.PVUuid != blockDevice.Status.PVUuid ||
		candidate.VGUuid != blockDevice.Status.VGUuid ||
		candidate.PartUUID != blockDevice.Status.PartUUID ||
		candidate.LVMVolumeGroupName != blockDevice.Status.LVMVolumeGroupName ||
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
		!reflect.DeepEqual(configureBlockDeviceLabels(blockDevice), blockDevice.Labels)
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

func checkConsumable(device internal.Device) bool {
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

func createUniqDeviceName(can internal.BlockDeviceCandidate) string {
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

func configureBlockDeviceLabels(blockDevice v1alpha1.BlockDevice) map[string]string {
	var lbls map[string]string
	if blockDevice.Labels == nil {
		lbls = make(map[string]string, 16)
	} else {
		lbls = make(map[string]string, len(blockDevice.Labels))
	}

	for key, value := range blockDevice.Labels {
		lbls[key] = value
	}

	slug.Lowercase = false
	slug.MaxLength = 63
	lbls[internal.MetadataNameLabelKey] = slug.Make(blockDevice.ObjectMeta.Name)
	lbls[internal.HostNameLabelKey] = slug.Make(blockDevice.Status.NodeName)
	lbls[internal.BlockDeviceTypeLabelKey] = slug.Make(blockDevice.Status.Type)
	lbls[internal.BlockDeviceFSTypeLabelKey] = slug.Make(blockDevice.Status.FsType)
	lbls[internal.BlockDevicePVUUIDLabelKey] = blockDevice.Status.PVUuid
	lbls[internal.BlockDeviceVGUUIDLabelKey] = blockDevice.Status.VGUuid
	lbls[internal.BlockDevicePartUUIDLabelKey] = blockDevice.Status.PartUUID
	lbls[internal.BlockDeviceLVMVolumeGroupNameLabelKey] = slug.Make(blockDevice.Status.LVMVolumeGroupName)
	lbls[internal.BlockDeviceActualVGNameLabelKey] = slug.Make(blockDevice.Status.ActualVGNameOnTheNode)
	lbls[internal.BlockDeviceWWNLabelKey] = slug.Make(blockDevice.Status.Wwn)
	lbls[internal.BlockDeviceSerialLabelKey] = slug.Make(blockDevice.Status.Serial)
	lbls[internal.BlockDeviceSizeLabelKey] = blockDevice.Status.Size.String()
	lbls[internal.BlockDeviceModelLabelKey] = slug.Make(blockDevice.Status.Model)
	lbls[internal.BlockDeviceRotaLabelKey] = strconv.FormatBool(blockDevice.Status.Rota)
	lbls[internal.BlockDeviceHotPlugLabelKey] = strconv.FormatBool(blockDevice.Status.HotPlug)
	lbls[internal.BlockDeviceMachineIDLabelKey] = slug.Make(blockDevice.Status.MachineID)

	return lbls
}
