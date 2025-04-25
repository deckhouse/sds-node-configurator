/*
Copyright 2025 Flant JSC

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

package bd

import (
	"context"
	"crypto/sha1"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/cache"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/controller"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/logger"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/monitoring"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/utils"
)

const DiscovererName = "block-device-controller"

type Discoverer struct {
	cl                      client.Client
	log                     logger.Logger
	bdCl                    *utils.BDClient
	blockDeviceFilterClient *utils.BlockDeviceFilterClient
	metrics                 monitoring.Metrics
	sdsCache                *cache.Cache
	cfg                     DiscovererConfig
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
		cl:                      cl,
		log:                     log,
		bdCl:                    utils.NewBDClient(cl, metrics),
		blockDeviceFilterClient: utils.NewBlockDeviceFilterClient(cl, metrics),
		metrics:                 metrics,
		sdsCache:                sdsCache,
		cfg:                     cfg,
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

	d.log.Debug("[RunBlockDeviceController] Getting block device filters")
	selector, err := d.blockDeviceFilterClient.GetAPIBlockDeviceFilters(ctx, DiscovererName)
	if err != nil {
		d.log.Error(err, "[RunBlockDeviceController] unable to GetAPIBlockDeviceFilters")
		return true
	}
	deviceMatchesSelector := func(blockDevice *v1alpha1.BlockDevice) bool {
		return selector.Matches(labels.Set(blockDevice.Labels))
	}

	apiBlockDevices, err := d.bdCl.GetAPIBlockDevices(ctx, DiscovererName, nil)
	if err != nil {
		d.log.Error(err, "[RunBlockDeviceController] unable to GetAPIBlockDevices")
		return true
	}

	if len(apiBlockDevices) == 0 {
		d.log.Debug("[RunBlockDeviceController] no BlockDevice resources were found")
	}

	blockDevicesToDelete := make([]*v1alpha1.BlockDevice, 0, len(candidates))

	// create new API devices
	for _, candidate := range candidates {
		blockDevice, exist := apiBlockDevices[candidate.Name]
		if exist {
			addToDeleteListIfNotMatched := func() {
				if !deviceMatchesSelector(&blockDevice) {
					d.log.Debug("[RunBlockDeviceController] block device doesn't match labels and will be deleted")
					blockDevicesToDelete = append(blockDevicesToDelete, &blockDevice)
				}
			}

			if !candidate.HasBlockDeviceDiff(blockDevice) {
				d.log.Debug(fmt.Sprintf(`[RunBlockDeviceController] no data to update for block device, name: "%s"`, candidate.Name))
				addToDeleteListIfNotMatched()
				continue
			}

			if err = d.updateAPIBlockDevice(ctx, blockDevice, candidate); err != nil {
				d.log.Error(err, "[RunBlockDeviceController] unable to update blockDevice, name: %s", blockDevice.Name)
				continue
			}

			d.log.Info(fmt.Sprintf(`[RunBlockDeviceController] updated APIBlockDevice, name: %s`, blockDevice.Name))
			addToDeleteListIfNotMatched()
			continue
		}

		device := candidate.AsAPIBlockDevice()
		if !deviceMatchesSelector(&device) {
			d.log.Debug("[RunBlockDeviceController] block device doesn't match labels and will not be created")
			continue
		}

		err := d.createAPIBlockDevice(ctx, &device)
		if err != nil {
			d.log.Error(err, fmt.Sprintf("[RunBlockDeviceController] unable to create block device blockDevice, name: %s", candidate.Name))
			continue
		}
		// blockDevicesToFilter = append(blockDevicesToFilter, device)
		d.log.Info(fmt.Sprintf("[RunBlockDeviceController] created new APIBlockDevice: %s", candidate.Name))

		// add new api device to the map, so it won't be deleted as fantom
		apiBlockDevices[candidate.Name] = device
	}

	// delete devices doesn't match the filters
	for _, device := range blockDevicesToDelete {
		name := device.Name
		err := d.deleteAPIBlockDevice(ctx, device)
		if err != nil {
			d.log.Error(err, fmt.Sprintf("[RunBlockDeviceController] unable to delete APIBlockDevice, name: %s", name))
			continue
		}
		delete(apiBlockDevices, name)
		d.log.Info(fmt.Sprintf("[RunBlockDeviceController] device deleted, name: %s", name))
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
		candidate := internal.NewBlockDeviceCandidateByDevice(&device, d.cfg.NodeName, d.cfg.MachineID)

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
	candidate.UpdateAPIBlockDevice(&blockDevice)

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

func (d *Discoverer) createAPIBlockDevice(ctx context.Context, blockDevice *v1alpha1.BlockDevice) error {
	start := time.Now()

	err := d.cl.Create(ctx, blockDevice)
	d.metrics.APIMethodsDuration(DiscovererName, "create").Observe(d.metrics.GetEstimatedTimeInSeconds(start))
	d.metrics.APIMethodsExecutionCount(DiscovererName, "create").Inc()
	if err != nil {
		d.metrics.APIMethodsErrors(DiscovererName, "create").Inc()
		return err
	}
	return nil
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
