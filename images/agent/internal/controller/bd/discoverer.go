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
	"log/slog"
	"os"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-common-lib/conditions"
	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/cache"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/controller"
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

	// publishedDevices answers which devices this cluster has published to
	// workloads as block volumes (utils.PublishedDevices in production). A device
	// that is somebody's volume is not a candidate: a BlockDevice on it would offer
	// the operator to build storage over a consumer's data, and the LVM signature
	// on it — a nested cluster's — is not this node's. A field so a test can state
	// the set.
	publishedDevices func() utils.PublishedBlockDevices

	// unreadableMultipathDevices answers which multipath maps on the node cannot
	// be read (utils.UnreadableMultipathDevices in production): no slaves left,
	// or not one active path among them. Such a map is not a candidate either —
	// there is nothing behind it, and lsblk still reports its size — and the
	// BlockDevice it once had goes Missing like any device that left the node.
	// A field so a test can state the set.
	unreadableMultipathDevices func() []string

	// multipathPaths answers which devices are paths of a multipath map
	// (utils.MultipathPathDevices in production), read from the maps' slaves in
	// sysfs. udev marks a path mpath_member only if multipath claimed it when it
	// appeared, which it does not for a path that came before its WWID was
	// registered; such a path is still a raw way into a LUN that a map serves.
	// A field so a test can state the set.
	multipathPaths func() map[string]struct{}

	// scsiTargetOf answers which iSCSI or Fibre Channel target a device is a LUN
	// of (utils.SCSITargetOf in production). The answer goes into a label a CSI
	// driver selects by in its BlockDeviceFilter, to hide every LUN of a target
	// it owns — one that appeared a moment ago as well, before the driver knew
	// its WWN. A field so a test can state it.
	scsiTargetOf func(kName string) (string, error)
}

type DiscovererConfig struct {
	BlockDeviceScanInterval time.Duration
	MachineID               string
	NodeName                string
	// MissingBlockDeviceRetention is how long a BlockDevice may stay
	// Missing=True before collectMissingBlockDevices deletes it. Zero keeps it
	// forever.
	MissingBlockDeviceRetention time.Duration
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
		log:                     log.Named(DiscovererName),
		bdCl:                    repository.NewBDClient(cl, metrics),
		blockDeviceFilterClient: repository.NewBlockDeviceFilterClient(cl, metrics),
		metrics:                 metrics,
		sdsCache:                sdsCache,
		cfg:                     cfg,
		reportedOrphans:         make(map[string]struct{}),
		publishedDevices:        utils.PublishedDevices,

		unreadableMultipathDevices: utils.UnreadableMultipathDevices,
		multipathPaths:             utils.MultipathPathDevices,
		scsiTargetOf:               utils.SCSITargetOf,
	}
}

func (d *Discoverer) Name() string {
	return DiscovererName
}

func (d *Discoverer) Discover(ctx context.Context) (controller.Result, error) {
	log := d.log.Named("discover")

	log.Info("reconciler starts BlockDevice resources reconciliation")

	shouldRequeue, err := d.blockDeviceReconcile(ctx)
	if err != nil {
		d.log.Error("reconciling block devices", logger.Err(err))
	}
	if shouldRequeue {
		log.Warn(fmt.Sprintf("reconciler needs a retry in %f", d.cfg.BlockDeviceScanInterval.Seconds()))
		return controller.Result{RequeueAfter: d.cfg.BlockDeviceScanInterval}, nil
	}
	log.Info("reconciler successfully ended BlockDevice resources reconciliation")
	return controller.Result{}, err
}

func (d *Discoverer) blockDeviceReconcile(ctx context.Context) (bool, error) {
	log := d.log

	reconcileStart := time.Now()

	log.Info("START reconcile of block devices")

	candidates, err := d.getBlockDeviceCandidates()
	if err != nil {
		d.log.Error("unable to get block device candidates", logger.Err(err))
		return true, fmt.Errorf("getting block device candidates: %w", err)
	}

	log.Debug("getting block device filters")
	selector, err := d.blockDeviceFilterClient.GetAPIBlockDeviceFilters(ctx, DiscovererName)
	if err != nil {
		d.log.Error("unable to GetAPIBlockDeviceFilters", logger.Err(err))
		return true, fmt.Errorf("getting BlockDeviceFilters from API: %w", err)
	}
	deviceMatchesSelector := func(blockDevice *v1alpha1.BlockDevice) bool {
		return selector.Matches(labels.Set(blockDevice.Labels))
	}

	apiBlockDevices, err := d.bdCl.GetAPIBlockDevices(ctx, DiscovererName, nil)
	if err != nil {
		d.log.Error("unable to GetAPIBlockDevices", logger.Err(err))
		return true, fmt.Errorf("getting BlockDevices from API: %w", err)
	}

	if len(apiBlockDevices) == 0 {
		log.Debug("no BlockDevice resources were found")
	}

	blockDevicesToDelete := make([]*v1alpha1.BlockDevice, 0, len(candidates))
	withheld := false

	// create new API devices
	runBlockDeviceControllerLog := d.log
	for i, candidate := range candidates {
		blockDevice, exist := apiBlockDevices[candidate.Name]
		if !exist {
			legacyBlockDevice, found := findLegacyNonConsumableBlockDevice(candidate, apiBlockDevices)
			if found {
				log.Info(fmt.Sprintf(
					"found legacy non-consumable BlockDevice, candidate name: \"%s\", legacy name: \"%s\", path: \"%s\"",
					candidate.Name, legacyBlockDevice.Name, candidate.Path,
				))
				candidate.Name = legacyBlockDevice.Name
				candidates[i].Name = legacyBlockDevice.Name
				blockDevice = legacyBlockDevice
				exist = true
			}
		}

		if exist {
			addToDeleteListIfNotMatched := func(blockDevice v1alpha1.BlockDevice) {
				if !deviceMatchesSelector(&blockDevice) {
					runBlockDeviceControllerLog.Debug("block device doesn't match labels and will be deleted")
					blockDevicesToDelete = append(blockDevicesToDelete, &blockDevice)
				}
			}

			if !candidate.HasBlockDeviceDiff(blockDevice) {
				log.Debug(fmt.Sprintf("no data to update for block device, name: \"%s\"", candidate.Name))
				addToDeleteListIfNotMatched(blockDevice)
				continue
			}

			// By pointer: the filter below has to judge the labels just
			// written, or a device whose update is what takes it out of the
			// filter stays published until the next pass — and the next pass
			// waits for a udev event.
			if err = d.updateAPIBlockDevice(ctx, &blockDevice, candidate); err != nil {
				d.log.Error("unable to update the BlockDevice", logger.Err(err), slog.String("blockDevice", blockDevice.Name))
				continue
			}

			log.Info(fmt.Sprintf("updated APIBlockDevice, name: %s", blockDevice.Name))
			addToDeleteListIfNotMatched(blockDevice)
			continue
		}

		// Not offered until its target is known: whether a filter hides the
		// device may depend on it, and a BlockDevice that a filter would have
		// hidden can be taken by an LVMVolumeGroup in the meantime. The agent
		// rescans only on udev events, so the retry has to be asked for.
		if candidate.SCSITargetUnresolved {
			log.Warn(fmt.Sprintf("not creating a BlockDevice for %s until the SCSI target it is a LUN of can be read", candidate.Path))
			withheld = true
			continue
		}

		device := candidate.AsAPIBlockDevice()
		if !deviceMatchesSelector(&device) {
			log.Debug("block device doesn't match labels and will not be created")
			continue
		}

		err := d.createAPIBlockDevice(ctx, &device)
		if err != nil {
			log.Error(fmt.Sprintf("unable to create block device blockDevice, name: %s", candidate.Name), logger.Err(err))
			continue
		}
		log.Info(fmt.Sprintf("created new APIBlockDevice: %s", candidate.Name))

		// add new api device to the map, so it won't be deleted as fantom
		apiBlockDevices[candidate.Name] = device
	}

	// delete devices doesn't match the filters
	for _, device := range blockDevicesToDelete {
		name := device.Name
		err := d.deleteAPIBlockDevice(ctx, device)
		if err != nil {
			log.Error(fmt.Sprintf("unable to delete APIBlockDevice, name: %s", name), logger.Err(err))
			continue
		}
		delete(apiBlockDevices, name)
		log.Info(fmt.Sprintf("device deleted, name: %s", name))
	}
	// delete api device if device no longer exists, but we still have its api resource
	d.removeDeprecatedAPIDevices(ctx, candidates, apiBlockDevices)

	log.Info("END reconcile of block devices")
	d.metrics.ReconcileDuration(DiscovererName).Observe(d.metrics.GetEstimatedTimeInSeconds(reconcileStart))
	d.metrics.ReconcilesCountTotal(DiscovererName).Inc()

	return withheld, nil
}

func (d *Discoverer) removeDeprecatedAPIDevices(
	ctx context.Context,
	candidates []internal.BlockDeviceCandidate,
	apiBlockDevices map[string]v1alpha1.BlockDevice,
) {
	log := d.log

	actualCandidates := make(map[string]struct{}, len(candidates))
	for _, candidate := range candidates {
		actualCandidates[candidate.Name] = struct{}{}
	}

	for name, device := range apiBlockDevices {
		switch {
		case shouldDeleteBlockDevice(device, actualCandidates, d.cfg.NodeName):
			err := d.deleteAPIBlockDevice(ctx, &device)
			if err != nil {
				log.Error(fmt.Sprintf("unable to delete APIBlockDevice, name: %s", name), logger.Err(err))
				continue
			}

			delete(apiBlockDevices, name)
			log.Info(fmt.Sprintf("device deleted, name: %s", name))

		case isMissingBlockDeviceCandidate(device, actualCandidates, d.cfg.NodeName):
			if err := d.markBlockDeviceMissing(ctx, device); err != nil {
				log.Error(fmt.Sprintf("unable to mark APIBlockDevice missing, name: %s", name), logger.Err(err))
			}
		}
	}

	d.collectMissingBlockDevices(ctx, actualCandidates, apiBlockDevices)
}

// missingGracePeriod is how long a non-consumable BlockDevice's device may be
// absent from a scan before BlockDeviceConditionMissing escalates from
// Unknown to True. Long enough that a dropped iSCSI session reconnecting on
// its own, or a udev event burst that briefly straddles the device coming
// back, never gets flagged; short enough that a genuinely gone device is
// still caught well within an on-call shift.
const missingGracePeriod = 10 * time.Minute

// isMissingBlockDeviceCandidate reports whether device belongs to this node,
// no longer has a matching candidate, and is exempt from
// shouldDeleteBlockDevice's own deletion (Consumable == false) -- the
// population markBlockDeviceMissing needs to watch, since nothing else ever
// revisits it once its device is gone.
func isMissingBlockDeviceCandidate(device v1alpha1.BlockDevice, actualCandidates map[string]struct{}, nodeName string) bool {
	return device.Status.NodeName == nodeName &&
		!device.Status.Consumable &&
		isBlockDeviceDeprecated(device.Name, actualCandidates)
}

// markBlockDeviceMissing publishes or escalates BlockDeviceConditionMissing
// for a device isMissingBlockDeviceCandidate selected. See that condition's
// own doc comment for the Unknown-then-True progression; this is the only
// place that ever sets it, and block_device_candidate.go's UpdateAPIBlockDevice
// is the only place that ever clears it, once the device is seen again.
func (d *Discoverer) markBlockDeviceMissing(ctx context.Context, device v1alpha1.BlockDevice) error {
	published := conditions.Get(device.Status.Conditions, v1alpha1.BlockDeviceConditionMissing)

	newCondition := metav1.Condition{
		Type:               v1alpha1.BlockDeviceConditionMissing,
		Status:             metav1.ConditionUnknown,
		Reason:             v1alpha1.ReasonDeviceNotFound,
		Message:            fmt.Sprintf("device %q was not found on node %q during the most recent scan", device.Status.Path, device.Status.NodeName),
		ObservedGeneration: device.Generation,
	}

	switch {
	case published == nil:
		// First pass without the device: publish Unknown and start the grace
		// period clock via LastTransitionTime. Not yet actionable on its own.
	case published.Status == metav1.ConditionTrue:
		// Already escalated; nothing left to do until the device reappears,
		// which UpdateAPIBlockDevice handles on a later pass.
		return nil
	case time.Since(published.LastTransitionTime.Time) >= missingGracePeriod:
		newCondition.Status = metav1.ConditionTrue
		newCondition.Message = fmt.Sprintf(
			"device %q has not been found on node %q for over %s",
			device.Status.Path, device.Status.NodeName, missingGracePeriod,
		)
	default:
		// Still within the grace period: leave it Unknown. conditions.Set
		// below is then a no-op (same status/reason/message), so this does
		// not touch LastTransitionTime or write to the API on every pass.
	}

	if !conditions.Set(&device.Status.Conditions, newCondition) {
		return nil
	}

	start := time.Now()
	err := d.cl.Update(ctx, &device)
	d.metrics.APIMethodsDuration(DiscovererName, "update").Observe(d.metrics.GetEstimatedTimeInSeconds(start))
	d.metrics.APIMethodsExecutionCount(DiscovererName, "update").Inc()
	if err != nil {
		d.metrics.APIMethodsErrors(DiscovererName, "update").Inc()
		return err
	}

	d.log.Warn(fmt.Sprintf(
		"BlockDevice %q marked %s=%s: %s",
		device.Name, v1alpha1.BlockDeviceConditionMissing, newCondition.Status, newCondition.Message,
	))
	return nil
}

func (d *Discoverer) getBlockDeviceCandidates() ([]internal.BlockDeviceCandidate, error) {
	log := d.log

	var candidates []internal.BlockDeviceCandidate
	devices, _ := d.sdsCache.GetDevices()
	if len(devices) == 0 {
		log.Debug("no devices found, returns empty candidates")
		return candidates, nil
	}

	filteredDevices, err := d.filterDevices(devices)
	if err != nil {
		d.log.Error("unable to filter devices", logger.Err(err))
		return nil, fmt.Errorf("filtering devices: %w", err)
	}

	if len(filteredDevices) == 0 {
		log.Debug("no filtered devices left, returns empty candidates")
		return candidates, nil
	}

	pvs, _ := d.sdsCache.GetPVs()
	if len(pvs) == 0 {
		log.Debug("no PVs found")
	}

	var delFlag bool
	candidates = make([]internal.BlockDeviceCandidate, 0, len(filteredDevices))

	for _, device := range filteredDevices {
		log.Trace(fmt.Sprintf("process device: %+v", device))
		candidate := internal.NewBlockDeviceCandidateByDevice(&device, d.cfg.NodeName, d.cfg.MachineID)
		scsiTarget, err := d.scsiTargetOf(device.KName)
		if err != nil {
			log.Warn(fmt.Sprintf("unable to read the SCSI target of device %s (kname %s)", device.Name, device.KName), logger.Err(err))
			candidate.SCSITargetUnresolved = true
		}
		candidate.SCSITarget = scsiTarget

		log.Trace(fmt.Sprintf("get following candidate: %+v", candidate))
		candidateName := d.createCandidateName(candidate, devices)

		if candidateName == "" {
			log.Trace("candidateName is empty. Skipping device")
			continue
		}

		candidate.Name = candidateName
		log.Trace(fmt.Sprintf("generated a unique candidate name: %s", candidate.Name))

		delFlag = false
		for _, pv := range pvs {
			if pv.PVName == device.Name {
				log.Trace(fmt.Sprintf("the device is a PV. Found PV name: %s", pv.PVName))
				if candidate.FSType == internal.LVMFSType {
					hasTag, lvmVGName := utils.ReadValueFromTags(pv.VGTags, internal.LVMVolumeGroupTag)
					if hasTag {
						log.Debug(fmt.Sprintf("PV %s of BlockDevice %s has tag, fill the VG information", pv.PVName, candidate.Name))
						candidate.PVUuid = pv.PVUuid
						candidate.VGUuid = pv.VGUuid
						candidate.ActualVGNameOnTheNode = pv.VGName
						candidate.LVMVolumeGroupName = lvmVGName
					} else {
						if len(pv.VGName) != 0 {
							log.Trace(fmt.Sprintf("the device is a PV with VG named %s that lacks our tag %s. Removing it from Kubernetes", pv.VGName, internal.LVMTags[0]))
							delFlag = true
						} else {
							candidate.PVUuid = pv.PVUuid
							candidate.MarkOrphanPhysicalVolume(&device)
						}
					}
				}
			}
		}
		log.Trace(fmt.Sprintf("delFlag: %t", delFlag))
		if delFlag {
			continue
		}
		log.Trace(fmt.Sprintf("configured candidate %+v", candidate))
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
	log := d.log

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
			log.Debug(fmt.Sprintf("device %s (kname %s) still names the missing parent %s",
				device.Name, device.KName, device.PkName))
			continue
		}

		log.Warn(fmt.Sprintf("device %s (kname %s) names the parent %s, which is not in the device list; "+
			"it inherits no serial or WWN and will most likely not become a BlockDevice. "+
			"A device-mapper node left behind by a detached disk looks exactly like this — check `dmsetup ls` and `dmsetup deps` for %s",
			device.Name, device.KName, device.PkName, device.KName))
	}

	for kName := range previous {
		if _, still := current[kName]; !still {
			log.Info(fmt.Sprintf("device with kname %s no longer names a missing parent", kName))
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
	log := d.log

	for _, device := range unusable {
		if device.KName == "" {
			log.Warn(fmt.Sprintf("device %s has no kname and cannot be indexed; skipping it. "+
				"Every other device on this node is still discovered", device.Name))
			continue
		}

		log.Warn(fmt.Sprintf("device %s repeats the kname %s of an earlier device in the list; skipping it. "+
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
	log := d.log

	log.Trace(fmt.Sprintf("devices before type filtration: %+v", devices))

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
	log.Trace("made map by KName", "duration", time.Since(start))

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
				log.Trace(fmt.Sprintf("can't find serial for device %s, kname: %s, pkname: %s", device.Name, device.KName, device.PkName))
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
				log.Trace(fmt.Sprintf("can't find wwn for device %s, kname: %s, pkname: %s", device.Name, device.KName, device.PkName))
			}
		}
	}
	d.log.Trace("Found missing Serial and Wwn", "duration", time.Since(start))

	// deleting parent devices

	// making pkname set
	pkNames := make(map[string]struct{}, len(filteredDevices))
	for _, device := range filteredDevices {
		if device.PkName != "" {
			log.Trace(fmt.Sprintf("find parent %s for child : %+v.", device.PkName, device))
			pkNames[device.PkName] = struct{}{}
		}
	}

	published := d.publishedDevices()

	unreadable := make(map[string]struct{})
	for _, dev := range d.unreadableMultipathDevices() {
		unreadable[dev] = struct{}{}
	}

	multipathPaths := d.multipathPaths()

	filteredDevices = slices.DeleteFunc(
		filteredDevices,
		func(device internal.Device) bool {
			log := d.log

			// Before every other rule, because the others describe the device and
			// this one describes who it belongs to. The set already holds the
			// partitions and holders of a published device, so a guest's partition
			// table does not bring the device back one level down.
			if published.Has(device.KName) {
				log.Trace("filtered out", "name", device.Name, "kname", device.KName, "reason", "published to a workload as a block volume")
				return true
			}

			if _, ok := unreadable[device.KName]; ok {
				log.Trace("filtered out", "name", device.Name, "kname", device.KName, "reason", "multipath map without an active path")
				return true
			}

			if device.FSType == "mpath_member" {
				log.Trace("filtered out", "name", device.Name, "kname", device.KName, "reason", "mpath_member")
				return true
			}

			if _, ok := multipathPaths[device.KName]; ok {
				log.Trace("filtered out", "name", device.Name, "kname", device.KName, "reason", "path of a multipath map")
				return true
			}

			for _, foreign := range []struct{ prefix, reason string }{
				{internal.DRBDName, "drbd"},
				{internal.RBDName, "rbd"},
				{internal.NBDName, "nbd"},
			} {
				if strings.HasPrefix(device.Name, foreign.prefix) {
					log.Trace("filtered out", "name", device.Name, "kname", device.KName, "reason", foreign.reason)
					return true
				}
			}
			if !hasValidType(device.Type) {
				log.Trace(
					"filtered out",
					"name", device.Name,
					"kname", device.KName,
					"reason", "type",
					"type", device.Type,
				)
				return true
			}
			if !hasValidFSType(device.FSType) {
				log.Trace(
					"filtered out",
					"name", device.Name,
					"kname", device.KName,
					"reason", "fstype",
					"fstype", device.FSType,
				)
				return true
			}

			_, hasChildren := pkNames[device.KName]
			if hasChildren && device.FSType != internal.LVMFSType {
				log.Trace(
					"filtered out",
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
				log.Trace(
					"filtered out",
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

	log.Trace(fmt.Sprintf("final filtered devices: %+v", filteredDevices))

	return filteredDevices, nil
}

func (d *Discoverer) createCandidateName(candidate internal.BlockDeviceCandidate, devices []internal.Device) string {
	log := d.log

	if len(candidate.Serial) == 0 {
		log.Trace(fmt.Sprintf("serial number is empty for device: %s", candidate.Path))
		if candidate.Type == internal.PartType {
			if len(candidate.PartUUID) == 0 {
				log.Warn(fmt.Sprintf("type = part and cannot get PartUUID; skipping this device, path: %s", candidate.Path))
				return ""
			}
			log.Trace(fmt.Sprintf("type = part and PartUUID is not empty; skiping getting serial number for device: %s", candidate.Path))
		} else {
			log.Debug(fmt.Sprintf("serial number is empty and device type is not part; trying to obtain serial number or its equivalent for device: %s, with type: %s", candidate.Path, candidate.Type))

			switch candidate.Type {
			case internal.MultiPathType:
				log.Debug(fmt.Sprintf("device %s type = %s; get serial number from parent device.", candidate.Path, candidate.Type))
				log.Trace(fmt.Sprintf("device: %+v. Device list: %+v", candidate, devices))
				serial, err := getSerialForMultipathDevice(candidate, devices)
				if err != nil {
					// Fall back to SerialInherited (filled by filterDevices from parents)
					// before giving up — same value getSerialForMultipathDevice would
					// return when the parent is an mpath_member.
					if candidate.SerialInherited != "" {
						log.Info(fmt.Sprintf("using inherited serial %s for mpath device %s after parent lookup failed: %s", candidate.SerialInherited, candidate.Path, err))
						candidate.Serial = candidate.SerialInherited
					} else {
						log.Warn(fmt.Sprintf("unable to obtain serial number or its equivalent; skipping device: %s. Error: %s", candidate.Path, err))
						return ""
					}
				} else {
					candidate.Serial = serial
					log.Info(fmt.Sprintf("successfully obtained serial number or its equivalent: %s for device: %s", candidate.Serial, candidate.Path))
				}
			default:
				isMdRaid := false
				matched, err := regexp.MatchString(`raid.*`, candidate.Type)
				if err != nil {
					d.log.Error("failed to match regex - unable to determine if the device is an mdraid. Attempting to retrieve serial number directly from the device", logger.Err(err))
				} else if matched {
					log.Trace("device is mdraid")
					isMdRaid = true
				}
				serial, err := readSerialBlockDevice(candidate.Path, isMdRaid)
				if err != nil {
					// crypt/dm devices rarely expose /sys/block/<name>/serial; use
					// the parent serial discovered by filterDevices instead of
					// skipping the device entirely.
					if candidate.SerialInherited != "" {
						log.Info(fmt.Sprintf("using inherited serial %s for device %s after direct read failed: %s", candidate.SerialInherited, candidate.Path, err))
						candidate.Serial = candidate.SerialInherited
					} else {
						log.Warn(fmt.Sprintf("unable to obtain serial number or its equivalent; skipping device: %s. Error: %s", candidate.Path, err))
						return ""
					}
				} else {
					log.Info(fmt.Sprintf("successfully obtained serial number or its equivalent: %s for device: %s", serial, candidate.Path))
					candidate.Serial = serial
				}
			}
		}
	}

	log.Trace(fmt.Sprintf("serial number is now: %s. Creating candidate name", candidate.Serial))
	return createUniqDeviceName(candidate)
}

func (d *Discoverer) updateAPIBlockDevice(
	ctx context.Context,
	blockDevice *v1alpha1.BlockDevice,
	candidate internal.BlockDeviceCandidate,
) error {
	candidate.UpdateAPIBlockDevice(blockDevice)

	start := time.Now()
	err := d.cl.Update(ctx, blockDevice)
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
