/*
	Copyright 2026 Flant JSC

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
	"sync"
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
)

type Discoverer struct {
	cl                      client.Client
	log                     logger.Logger
	bdCl                    *repository.BDClient
	blockDeviceFilterClient *repository.BlockDeviceFilterClient
	metrics                 *monitoring.Metrics
	sdsCache                *cache.Cache
	cfg                     DiscovererConfig
	// reportedOrphans holds the KNames of the devices whose parent was missing on
	// the previous pass, so an orphan is named when it appears or goes away rather
	// than once per pass for as long as it exists. Nothing on the node clears an
	// orphaned device-mapper node by itself, and filterDevices runs on every udev
	// event: at Warning every time, the first and useful copy would be buried by
	// the identical ones behind it. The same rule the LVMVolumeGroup discoverer
	// applies to a refused import.
	//
	// Guarded because filterDevices is reachable from the scanner's retry goroutine
	// as well as from its main loop, and concurrent writes to a bare map take the
	// process down.
	orphanMu        sync.Mutex
	reportedOrphans map[string]struct{}
}

type DiscovererConfig struct {
	BlockDeviceScanInterval time.Duration
	MachineID               string
	NodeName                string
}

func NewDiscoverer(
	cl client.Client,
	log logger.Logger,
	metrics *monitoring.Metrics,
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
		reportedOrphans:         make(map[string]struct{}),
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
		if !exist {
			legacyBlockDevice, found := findLegacyNonConsumableBlockDevice(candidate, apiBlockDevices)
			if found {
				d.log.Info(fmt.Sprintf(
					`[RunBlockDeviceController] found legacy non-consumable BlockDevice, candidate name: "%s", legacy name: "%s", path: "%s"`,
					candidate.Name, legacyBlockDevice.Name, candidate.Path,
				))
				// Adopt the legacy name only for this update path. The candidate
				// slice keeps the new name, and old non-consumable devices are not
				// removed by removeDeprecatedAPIDevices.
				candidate.Name = legacyBlockDevice.Name
				blockDevice = legacyBlockDevice
				exist = true
			}
		}

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
// Once the visitor returns false, or the chain ends, it stops.
// Returns true if interrupted by visitor
//
// maxDepth is what makes a cycle in the parent chain an error rather than an
// infinite walk, and callers pass len(devicesByKName) for it. That bound is exact:
// a walk that visits more devices than the list holds must have visited one of
// them twice, and there is no other way to exceed it. A constant is not exact, and
// the difference matters — it conflates a chain that loops, which makes the list
// invalid, with a chain that is merely long, which is a property of one device.
// LVM on LUKS on md-raid on multipath on partitions is a legal stack and the node
// is free to keep growing it, so a constant would eventually fail the node's whole
// device list over the length of one device's chain: the same failure this
// function stopped having for a parent that is named but absent.
func visitParents(devicesByKName map[string]*internal.Device, device *internal.Device, visitor func(parent *internal.Device) bool, maxDepth int) (bool, error) {
	if maxDepth <= 0 {
		return false, ErrDEviceListParentVisitingRecursionLimitReached
	}
	if device.PkName == "" {
		return false, nil
	}

	parent, found := devicesByKName[device.PkName]
	if !found {
		// A parent that is named but absent ends the walk with nothing inherited,
		// exactly like a device that names no parent at all. It is not a broken
		// list: a device-mapper node outlives the disk under it, so a detached
		// disk leaves dm entries whose PkName points at a device that is gone —
		// `dmsetup ls` still shows them while /dev/sdX does not exist.
		//
		// This used to return ErrDeviceListParentNotFound, which filterDevices
		// turned into a hard error and getBlockDeviceCandidates propagated,
		// throwing away every candidate on the node. One orphaned dm node was
		// therefore enough to stop the agent creating any BlockDevice at all,
		// permanently and on a five-second retry loop, on a node that was
		// otherwise healthy. The caller already handles "not found" by logging
		// that it could not resolve a serial and moving on, which is the correct
		// outcome here.
		//
		// It is silent because it says nothing new: filterDevices names the
		// orphans once per pass before it walks anything, which is the one place
		// where each is reported once rather than once per walk that reaches it.
		return false, nil
	}

	if !visitor(parent) {
		return true, nil
	}

	return visitParents(devicesByKName, parent, visitor, maxDepth-1)
}

// orphanedParents returns the devices whose PkName names a device that is not in
// the list, in the order they appear.
//
// Tolerating them is what visitParents does now, and it has to: a device-mapper
// node outlives the disk under it, so one detached disk used to cost the node
// every BlockDevice it had. But tolerating them quietly would replace a loud
// failure with a silent one — the device simply never becomes a BlockDevice, with
// no error, no warning, and nothing for an operator to search for. Naming them is
// what keeps the state findable.
func orphanedParents(devices []internal.Device, kNamesInList map[string]struct{}) []internal.Device {
	var orphans []internal.Device

	for _, device := range devices {
		if device.PkName == "" {
			continue
		}
		if _, found := kNamesInList[device.PkName]; !found {
			orphans = append(orphans, device)
		}
	}

	return orphans
}

// reportOrphanedParents says it out loud once per orphan — when it appears, and
// again when it goes away — rather than once per pass for as long as it exists.
//
// Warning rather than Debug: an orphaned device-mapper node is leftover state
// that nothing on the node clears by itself, and the answer is a `dmsetup remove`
// somebody has to run. That is also exactly why it must not be repeated every
// pass. filterDevices runs on every udev event, so a state that never resolves
// itself would print the same line for the lifetime of the process and bury the
// copy that was worth reading — the same reason the LVMVolumeGroup discoverer
// reports a refused import only when it appears or changes.
//
// The recovery gets a line of its own, because otherwise the only record of an
// orphan going away is the absence of a message.
func (d *Discoverer) reportOrphanedParents(devices []internal.Device, kNamesInList map[string]struct{}) {
	orphans := orphanedParents(devices, kNamesInList)

	current := make(map[string]struct{}, len(orphans))
	for _, device := range orphans {
		current[device.KName] = struct{}{}
	}

	d.orphanMu.Lock()
	previous := d.reportedOrphans
	d.reportedOrphans = current
	d.orphanMu.Unlock()

	for _, device := range orphans {
		if _, known := previous[device.KName]; known {
			d.log.Debug(fmt.Sprintf("[filterDevices] device %s (kname %s) still names the missing parent %s",
				device.Name, device.KName, device.PkName))
			continue
		}

		d.log.Warning(fmt.Sprintf("[filterDevices] device %s (kname %s) names the parent %s, which is not in the device list; "+
			"it inherits no serial or WWN and will most likely not become a BlockDevice. "+
			"A device-mapper node left behind by a detached disk looks exactly like this — check `dmsetup ls` and `dmsetup deps` for %s",
			device.Name, device.KName, device.PkName, device.KName))
	}

	for kName := range previous {
		if _, still := current[kName]; !still {
			d.log.Info(fmt.Sprintf("[filterDevices] device with kname %s no longer names a missing parent", kName))
		}
	}
}

// reportUnusableDevices says which entries could not be indexed by KName and were
// therefore dropped from the pass.
//
// Loud, and every time rather than on a transition, because unlike an orphaned
// device-mapper node this is not a state of the node: an entry with no KName, or a
// second entry under a KName the list already holds, is the device list itself
// being wrong. Nothing on the node clears it and nothing here can act on it, so it
// has to be findable — and it is rare enough that repeating it costs nothing. The
// orphan report is the one that has to hold its tongue, because an orphan is
// ordinary and lasts.
//
// It is a Warning and not an error because the pass goes on: dropping the entry
// costs one undiscovered device, where failing the list used to cost the node
// every BlockDevice it had.
func (d *Discoverer) reportUnusableDevices(unusable []internal.Device) {
	for _, device := range unusable {
		if device.KName == "" {
			d.log.Warning(fmt.Sprintf("[filterDevices] device %s has no kname and cannot be indexed; skipping it. "+
				"Every other device on this node is still discovered", device.Name))
			continue
		}

		d.log.Warning(fmt.Sprintf("[filterDevices] device %s repeats the kname %s of an earlier device in the list; skipping it. "+
			"Two entries under one kname resolve to one BlockDevice, so keeping both would have them overwrite each other every pass. "+
			"Every other device on this node is still discovered", device.Name, device.KName))
	}
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
	//
	// An entry that cannot go into the map leaves the list instead of failing it.
	// Both ways of being unusable — no KName at all, or a KName another entry
	// already holds — are properties of ONE entry, exactly like the named-but-absent
	// parent this file stopped failing over: keeping the old behaviour here would
	// mean a single bad record still cost the node every BlockDevice it has, on the
	// five-second retry loop, for as long as the record existed. The duplicate is
	// not hypothetical either — with Features.NetlinkBlockDeviceDiscovery the list
	// comes from the agent's own bookkeeping (udev.DeviceMap.Snapshot), built from a
	// stream of events, so one missed remove leaves two entries under one KName.
	//
	// Dropped from filteredDevices rather than merely left out of the map: a device
	// the map does not hold is a device nothing can inherit from and, worse for the
	// duplicate, two entries under one KName resolve to one BlockDevice name and
	// would fight over the same resource every pass.
	devicesByKName := make(map[string]*internal.Device, len(filteredDevices))
	var unusable []internal.Device
	// The KNames the list holds, kept as a set of its own so the orphan report at
	// the end can ask "is this parent in the list" without reading through
	// devicesByKName — whose pointers address a backing array the compaction below
	// rewrites.
	kNamesInList := make(map[string]struct{}, len(filteredDevices))
	filteredDevices = slices.DeleteFunc(filteredDevices, func(device internal.Device) bool {
		if device.KName == "" {
			unusable = append(unusable, device)
			return true
		}
		if _, alreadyExists := kNamesInList[device.KName]; alreadyExists {
			unusable = append(unusable, device)
			return true
		}

		kNamesInList[device.KName] = struct{}{}

		return false
	})
	for i := range filteredDevices {
		// The element, not a copy of it. The loop below fills SerialInherited and
		// WWNInherited through &filteredDevices[i], so with copies in the map a
		// parent's inherited value was never visible to its children and every walk
		// went to the top of the chain again. Harmless but pointless work, and a
		// trap for anyone who later reads a mutable field back out of this map.
		//
		// Taken after DeleteFunc has finished moving elements, because it takes
		// addresses into the backing array the compaction rewrites.
		devicesByKName[filteredDevices[i].KName] = &filteredDevices[i]
	}
	d.log.Trace("[filterDevices] Made map by KName", "duration", time.Since(start))

	d.reportUnusableDevices(unusable)

	// Before the type filtering, deliberately. The orphan this exists for is a thin
	// pool's dm node, which carries type "lvm" and is therefore dropped by
	// hasValidType a hundred lines below whatever its parent does — so reporting
	// only the survivors would say nothing at all in the case taken from the stand.
	// What the line is for is the leftover state on the node, not the candidate: an
	// entry in `dmsetup ls` pointing at a disk that is gone stays there until
	// somebody removes it.
	//
	// It reads the KName set rather than devicesByKName, whose pointers address the
	// backing array the compaction below rewrites.
	d.reportOrphanedParents(filteredDevices, kNamesInList)

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
			}, len(devicesByKName))

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
			}, len(devicesByKName))

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

			for _, foreign := range []struct{ prefix, reason string }{
				{internal.DRBDName, "drbd"},
				{internal.RBDName, "rbd"},
				{internal.NBDName, "nbd"},
			} {
				if strings.HasPrefix(device.Name, foreign.prefix) {
					d.log.Trace("[filterDevices] filtered out", "name", device.Name, "kname", device.KName, "reason", foreign.reason)
					return true
				}
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
					// Fall back to SerialInherited (filled by filterDevices from parents)
					// before giving up — same value getSerialForMultipathDevice would
					// return when the parent is an mpath_member.
					if candidate.SerialInherited != "" {
						d.log.Info(fmt.Sprintf("[CreateCandidateName] Using inherited serial %s for mpath device %s after parent lookup failed: %s", candidate.SerialInherited, candidate.Path, err))
						candidate.Serial = candidate.SerialInherited
					} else {
						d.log.Warning(fmt.Sprintf("[CreateCandidateName] Unable to obtain serial number or its equivalent; skipping device: %s. Error: %s", candidate.Path, err))
						return ""
					}
				} else {
					candidate.Serial = serial
					d.log.Info(fmt.Sprintf("[CreateCandidateName] Successfully obtained serial number or its equivalent: %s for device: %s", candidate.Serial, candidate.Path))
				}
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
					// crypt/dm devices rarely expose /sys/block/<name>/serial; use
					// the parent serial discovered by filterDevices instead of
					// skipping the device entirely.
					if candidate.SerialInherited != "" {
						d.log.Info(fmt.Sprintf("[CreateCandidateName] Using inherited serial %s for device %s after direct read failed: %s", candidate.SerialInherited, candidate.Path, err))
						candidate.Serial = candidate.SerialInherited
					} else {
						d.log.Warning(fmt.Sprintf("[CreateCandidateName] Unable to obtain serial number or its equivalent; skipping device: %s. Error: %s", candidate.Path, err))
						return ""
					}
				} else {
					d.log.Info(fmt.Sprintf("[CreateCandidateName] Successfully obtained serial number or its equivalent: %s for device: %s", serial, candidate.Path))
					candidate.Serial = serial
				}
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

func findLegacyNonConsumableBlockDevice(
	candidate internal.BlockDeviceCandidate,
	apiBlockDevices map[string]v1alpha1.BlockDevice,
) (v1alpha1.BlockDevice, bool) {
	if candidate.Consumable {
		return v1alpha1.BlockDevice{}, false
	}

	var matched v1alpha1.BlockDevice
	found := false
	for _, blockDevice := range apiBlockDevices {
		if !legacyNonConsumableBlockDeviceMatches(candidate, blockDevice) {
			continue
		}
		if found {
			return v1alpha1.BlockDevice{}, false
		}
		matched = blockDevice
		found = true
	}

	return matched, found
}

func legacyNonConsumableBlockDeviceMatches(
	candidate internal.BlockDeviceCandidate,
	blockDevice v1alpha1.BlockDevice,
) bool {
	if blockDevice.Status.NodeName != candidate.NodeName || blockDevice.Status.Consumable {
		return false
	}

	if candidate.PVUuid != "" && candidate.PVUuid == blockDevice.Status.PVUuid {
		return true
	}
	if candidate.PartUUID != "" && candidate.PartUUID == blockDevice.Status.PartUUID {
		return true
	}

	if sameBlockDeviceTypeAndSize(candidate, blockDevice) {
		if wwn := candidate.GetWWN(); wwn != "" {
			return wwn == blockDevice.Status.Wwn
		}
		if serial := candidate.GetSerial(); serial != "" {
			return serial == blockDevice.Status.Serial
		}
		if blockDevice.Status.Wwn != "" || blockDevice.Status.Serial != "" {
			return false
		}
	}

	return candidate.Path != "" &&
		candidate.Path == blockDevice.Status.Path &&
		sameBlockDeviceTypeAndSize(candidate, blockDevice)
}

func sameBlockDeviceTypeAndSize(candidate internal.BlockDeviceCandidate, blockDevice v1alpha1.BlockDevice) bool {
	return candidate.Type == blockDevice.Status.Type &&
		candidate.Size.Value() == blockDevice.Status.Size.Value()
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
