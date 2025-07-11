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
	"errors"
	"fmt"
	"os"
	"regexp"
	"slices"
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
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/repository"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/utils"
)

const (
	DiscovererName = "block-device-controller"
)

var (
	ErrDeviceListInvalid                             = errors.New("device list invalid")
	ErrDeviceListKNameIsEmpty                        = fmt.Errorf("kname is empty: %w", ErrDeviceListInvalid)
	ErrDEviceListParentVisitingRecursionLimitReached = fmt.Errorf("max parent recursion reached: %w", ErrDeviceListInvalid)
	ErrDeviceListParentNotFound                      = fmt.Errorf("parent not found: %w", ErrDeviceListInvalid)
)

type Discoverer struct {
	cl                      client.Client
	log                     logger.Logger
	bdCl                    *repository.BDClient
	blockDeviceFilterClient *repository.BlockDeviceFilterClient
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
		bdCl:                    repository.NewBDClient(cl, metrics),
		blockDeviceFilterClient: repository.NewBlockDeviceFilterClient(cl, metrics),
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

	shouldRequeue, err := d.blockDeviceReconcile(ctx)
	if err != nil {
		d.log.Error(err, "reconciling block devices")
	}
	if shouldRequeue {
		d.log.Warning(fmt.Sprintf("[RunBlockDeviceController] Reconciler needs a retry in %f", d.cfg.BlockDeviceScanInterval.Seconds()))
		return controller.Result{RequeueAfter: d.cfg.BlockDeviceScanInterval}, nil
	}
	d.log.Info("[RunBlockDeviceController] Reconciler successfully ended BlockDevice resources reconciliation")
	return controller.Result{}, err
}

func (d *Discoverer) blockDeviceReconcile(ctx context.Context) (bool, error) {
	reconcileStart := time.Now()

	d.log.Info("[RunBlockDeviceController] START reconcile of block devices")

	candidates, err := d.getBlockDeviceCandidates()
	if err != nil {
		d.log.Error(err, "[RunBlockDeviceController] unable to get block device candidates")
		return true, fmt.Errorf("getting block device candidates: %w", err)
	}

	d.log.Debug("[RunBlockDeviceController] Getting block device filters")
	selector, err := d.blockDeviceFilterClient.GetAPIBlockDeviceFilters(ctx, DiscovererName)
	if err != nil {
		d.log.Error(err, "[RunBlockDeviceController] unable to GetAPIBlockDeviceFilters")
		return true, fmt.Errorf("getting BlockDeviceFilters from API: %w", err)
	}
	deviceMatchesSelector := func(blockDevice *v1alpha1.BlockDevice) bool {
		return selector.Matches(labels.Set(blockDevice.Labels))
	}

	apiBlockDevices, err := d.bdCl.GetAPIBlockDevices(ctx, DiscovererName, nil)
	if err != nil {
		d.log.Error(err, "[RunBlockDeviceController] unable to GetAPIBlockDevices")
		return true, fmt.Errorf("getting BlockDevices from API: %w", err)
	}

	if len(apiBlockDevices) == 0 {
		d.log.Debug("[RunBlockDeviceController] no BlockDevice resources were found")
	}

	blockDevicesToDelete := make([]*v1alpha1.BlockDevice, 0, len(candidates))

	// create new API devices
	for _, candidate := range candidates {
		blockDevice, exist := apiBlockDevices[candidate.Name]
		if exist {
			addToDeleteListIfNotMatched := func(blockDevice v1alpha1.BlockDevice) {
				if !deviceMatchesSelector(&blockDevice) {
					d.log.Debug("[RunBlockDeviceController] block device doesn't match labels and will be deleted")
					blockDevicesToDelete = append(blockDevicesToDelete, &blockDevice)
				}
			}

			if !candidate.HasBlockDeviceDiff(blockDevice) {
				d.log.Debug(fmt.Sprintf(`[RunBlockDeviceController] no data to update for block device, name: "%s"`, candidate.Name))
				addToDeleteListIfNotMatched(blockDevice)
				continue
			}

			if err = d.updateAPIBlockDevice(ctx, blockDevice, candidate); err != nil {
				d.log.Error(err, "[RunBlockDeviceController] unable to update blockDevice, name: %s", blockDevice.Name)
				continue
			}

			d.log.Info(fmt.Sprintf(`[RunBlockDeviceController] updated APIBlockDevice, name: %s`, blockDevice.Name))
			addToDeleteListIfNotMatched(blockDevice)
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

	return false, nil
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

func (d *Discoverer) getBlockDeviceCandidates() ([]internal.BlockDeviceCandidate, error) {
	var candidates []internal.BlockDeviceCandidate
	devices, _ := d.sdsCache.GetDevices()
	if len(devices) == 0 {
		d.log.Debug("[GetBlockDeviceCandidates] no devices found, returns empty candidates")
		return candidates, nil
	}

	filteredDevices, err := d.filterDevices(devices)
	if err != nil {
		d.log.Error(err, "[GetBlockDeviceCandidates] unable to filter devices")
		return nil, fmt.Errorf("filtering devices: %w", err)
	}

	if len(filteredDevices) == 0 {
		d.log.Debug("[GetBlockDeviceCandidates] no filtered devices left, returns empty candidates")
		return candidates, nil
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

	return candidates, nil
}

// Calls visitor function for each parent of the device
//
// Once maxDepth reached or travel function returns false it stops
// Returns true if interrupted by visitor
func visitParents(devicesByKName map[string]*internal.Device, device *internal.Device, visitor func(parent *internal.Device) bool, maxDepth int) (bool, error) {
	if maxDepth <= 0 {
		return false, ErrDEviceListParentVisitingRecursionLimitReached
	}
	if device.PkName == "" {
		return false, nil
	}

	parent, found := devicesByKName[device.PkName]
	if !found {
		return false, ErrDeviceListParentNotFound
	}

	if !visitor(parent) {
		return true, nil
	}

	return visitParents(devicesByKName, parent, visitor, maxDepth-1)
}

// Removing devices we don't need
//
// Generally we remove parent devices:
//
// - sda - remove
//   - sda1 - keep
//   - sda2 - keep
//
// In mpath case we should copy serial and wwn from the parent device
// Also mpath devices appears once but their parents multiple times. So only way to filter them out is to remove them by "fstype": "mpath_member"
func (d *Discoverer) filterDevices(devices []internal.Device) ([]internal.Device, error) {
	d.log.Trace(fmt.Sprintf("[filterDevices] devices before type filtration: %+v", devices))

	filteredDevices := slices.Clone(devices)
	start := time.Now()
	// arrange devices by pkname to fast access
	devicesByKName := make(map[string]*internal.Device, len(filteredDevices))
	for _, device := range filteredDevices {
		if device.KName == "" {
			return devices, fmt.Errorf("empty kname is unexpected for device: %v", device)
		}
		firstDevice, alreadyExists := devicesByKName[device.KName]
		if alreadyExists {
			d.log.Error(ErrDeviceListInvalid, "second device with same kname", "first", firstDevice, "second", device)
			return devices, fmt.Errorf("%w: second device with kname %s found", ErrDeviceListInvalid, device.KName)
		}
		devicesByKName[device.KName] = &device
	}
	d.log.Trace("[filterDevices] Made map by KName", "duration", time.Since(start))

	start = time.Now()
	// feel up missing serial and wwn for mpath and partitions
	for i := range filteredDevices {
		device := &filteredDevices[i]

		if device.Serial == "" {
			found, err := visitParents(devicesByKName, device, func(parent *internal.Device) bool {
				if parent.Serial == "" {
					if parent.SerialInherited == "" {
						return true
					}
					device.SerialInherited = parent.SerialInherited
					return false
				}
				device.SerialInherited = parent.Serial
				return false
			}, 16)

			if err != nil {
				return nil, fmt.Errorf("looking serial for device %v: %w", device, err)
			}

			if !found {
				d.log.Trace(fmt.Sprintf("[filterDevices] Can't find serial for device %s, kname: %s, pkname: %s", device.Name, device.KName, device.PkName))
			}
		}

		if device.Wwn == "" {
			found, err := visitParents(devicesByKName, device, func(parent *internal.Device) bool {
				if parent.Wwn == "" {
					if parent.WWNInherited == "" {
						return true
					}
					device.WWNInherited = parent.WWNInherited
					return false
				}
				device.WWNInherited = parent.Wwn
				return false
			}, 16)

			if err != nil {
				return nil, fmt.Errorf("looking WWN for device %v: %w", device, err)
			}

			if !found {
				d.log.Trace(fmt.Sprintf("[filterDevices] Can't find wwn for device %s, kname: %s, pkname: %s", device.Name, device.KName, device.PkName))
			}
		}
	}
	d.log.Trace("Found missing Serial and Wwn", "duration", time.Since(start))

	// deleting parent devices

	// making pkname set
	pkNames := make(map[string]struct{}, len(filteredDevices))
	for _, device := range filteredDevices {
		if device.PkName != "" {
			d.log.Trace(fmt.Sprintf("[filterDevices] find parent %s for child : %+v.", device.PkName, device))
			pkNames[device.PkName] = struct{}{}
		}
	}

	filteredDevices = slices.DeleteFunc(
		filteredDevices,
		func(device internal.Device) bool {
			if device.FSType == "mpath_member" {
				d.log.Trace("[filterDevices] filtered out", "name", device.Name, "kname", device.KName, "reason", "mpath_member")
				return true
			}

			if strings.HasPrefix(device.Name, internal.DRBDName) {
				d.log.Trace("[filterDevices] filtered out", "name", device.Name, "kname", device.KName, "reason", "drbd")
				return true
			}
			if !hasValidType(device.Type) {
				d.log.Trace(
					"[filterDevices] filtered out",
					"name", device.Name,
					"kname", device.KName,
					"reason", "type",
					"type", device.Type,
				)
				return true
			}
			if !hasValidFSType(device.FSType) {
				d.log.Trace(
					"[filterDevices] filtered out",
					"name", device.Name,
					"kname", device.KName,
					"reason", "fstype",
					"fstype", device.FSType,
				)
				return true
			}

			_, hasChildren := pkNames[device.KName]
			if hasChildren && device.FSType != internal.LVMFSType {
				d.log.Trace(
					"[filterDevices] filtered out",
					"name", device.Name,
					"kname", device.KName,
					"reason", "has children but not LVM",
					"fstype", device.FSType,
					"has_children", hasChildren,
				)
				return true
			}

			validSize, err := hasValidSize(device.Size)
			if err != nil || !validSize {
				d.log.Trace(
					"[filterDevices] filtered out",
					"name", device.Name,
					"kname", device.KName,
					"reason", "invalid size",
					"size", device.Size,
				)
				return true
			}

			return false
		},
	)

	d.log.Trace(fmt.Sprintf("[filterDevices] final filtered devices: %+v", filteredDevices))

	return filteredDevices, nil
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
