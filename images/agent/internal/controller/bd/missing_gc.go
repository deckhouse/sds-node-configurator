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
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-common-lib/conditions"
	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
)

// lvgReferences is what the LVMVolumeGroups of the cluster say about
// BlockDevices: which ones they list as their Physical Volumes, and which
// LVMVolumeGroup names exist at all.
//
// Both, because status.nodes alone does not cover the case the Missing
// condition was introduced for. The LVMVolumeGroup discoverer rebuilds
// status.nodes from the Physical Volumes it can still name, so a Physical
// Volume whose device is gone drops out of it; the BlockDevice's own
// status.lvmVolumeGroupName, read from the Volume Group's tag while the device
// was there, is then the only record left that it belonged to one. Deleting it
// would erase the evidence of a lost Physical Volume under a live
// LVMVolumeGroup.
//
// The name is checked against LVMVolumeGroups that exist here rather than
// taken as-is, since the agent of a nested cluster writes the same tag on the
// LUNs a hypervisor sees too: a BlockDevice naming an LVMVolumeGroup this
// cluster does not have is the leak the deletion exists for.
type lvgReferences struct {
	blockDevices map[string]struct{}
	names        map[string]struct{}
}

func newLVGReferences(lvgs []v1alpha1.LVMVolumeGroup) lvgReferences {
	refs := lvgReferences{
		blockDevices: make(map[string]struct{}),
		names:        make(map[string]struct{}, len(lvgs)),
	}
	for _, lvg := range lvgs {
		refs.names[lvg.Name] = struct{}{}
		for _, node := range lvg.Status.Nodes {
			for _, dev := range node.Devices {
				refs.blockDevices[dev.BlockDevice] = struct{}{}
			}
		}
	}
	return refs
}

// holds reports whether an LVMVolumeGroup references blockDevice.
func (r lvgReferences) holds(blockDevice v1alpha1.BlockDevice) bool {
	if _, ok := r.blockDevices[blockDevice.Name]; ok {
		return true
	}
	if name := blockDevice.Status.LVMVolumeGroupName; name != "" {
		if _, ok := r.names[name]; ok {
			return true
		}
	}
	return false
}

// missingTrue returns the Missing condition of blockDevice when it is True,
// nil otherwise. Unknown is the grace period (see missingGracePeriod) and is
// never acted on.
func missingTrue(blockDevice v1alpha1.BlockDevice) *metav1.Condition {
	cond := conditions.Get(blockDevice.Status.Conditions, v1alpha1.BlockDeviceConditionMissing)
	if cond == nil || cond.Status != metav1.ConditionTrue {
		return nil
	}
	return cond
}

// isMissingBlockDeviceExpired is every check on a BlockDevice before its
// deletion that does not need the LVMVolumeGroups: it is this node's, it is
// not seen in the current scan, it is Missing=True and has been for at least
// retention. retention <= 0 disables the deletion.
//
// Consumable is checked although isMissingBlockDeviceCandidate already
// excludes it: a Consumable device that is gone is shouldDeleteBlockDevice's
// to delete, and this path must not become a second way to reach it.
//
// The clock starts at the condition's last transition, which is when it
// became True: missingGracePeriod after the device was last seen.
func isMissingBlockDeviceExpired(
	blockDevice v1alpha1.BlockDevice,
	actualCandidates map[string]struct{},
	nodeName string,
	now time.Time,
	retention time.Duration,
) bool {
	if retention <= 0 || !isMissingBlockDeviceCandidate(blockDevice, actualCandidates, nodeName) {
		return false
	}
	cond := missingTrue(blockDevice)
	if cond == nil {
		return false
	}
	return now.Sub(cond.LastTransitionTime.Time) >= retention
}

// shouldCollectMissingBlockDevice decides whether the agent deletes a Missing
// BlockDevice: isMissingBlockDeviceExpired, and no LVMVolumeGroup references
// it.
//
// Every reason of the condition is treated alike. DeviceNotFound is the only
// one the agent publishes, and it already covers a device withheld from
// discovery because it is published to a workload: filterDevices drops such a
// device before it becomes a candidate, so it goes Missing like one that was
// unmapped. It comes back as a new BlockDevice when the publication ends, so
// deleting its record loses nothing either.
func shouldCollectMissingBlockDevice(
	blockDevice v1alpha1.BlockDevice,
	actualCandidates map[string]struct{},
	nodeName string,
	refs lvgReferences,
	now time.Time,
	retention time.Duration,
) bool {
	return isMissingBlockDeviceExpired(blockDevice, actualCandidates, nodeName, now, retention) &&
		!refs.holds(blockDevice)
}

// collectMissingBlockDevices deletes the node's BlockDevices that
// shouldCollectMissingBlockDevice selects, and publishes how many
// Missing=True ones remain.
//
// Here, in the per-node agent, rather than in the controller: this pass is
// what sets the condition and the only one that knows the device is still
// absent from the node right now, so the record cannot be deleted in the same
// moment the device came back. The LVMVolumeGroups are read through the
// manager's cache, which the LVMVolumeGroup discoverer keeps warm anyway.
//
// They are listed only when a device is due, and a failed list deletes
// nothing: without them nothing proves a device unreferenced.
//
// The agent has no event recorder, so the deletion is logged at Info.
func (d *Discoverer) collectMissingBlockDevices(
	ctx context.Context,
	actualCandidates map[string]struct{},
	apiBlockDevices map[string]v1alpha1.BlockDevice,
) {
	log := d.log
	now := time.Now()

	var due []v1alpha1.BlockDevice
	for _, device := range apiBlockDevices {
		if isMissingBlockDeviceExpired(device, actualCandidates, d.cfg.NodeName, now, d.cfg.MissingBlockDeviceRetention) {
			due = append(due, device)
		}
	}

	if len(due) > 0 {
		lvgs, err := d.listLVMVolumeGroups(ctx)
		if err != nil {
			log.Error("unable to list LVMVolumeGroups; not deleting Missing BlockDevices in this pass", logger.Err(err))
		} else {
			refs := newLVGReferences(lvgs)
			for _, device := range due {
				if !shouldCollectMissingBlockDevice(device, actualCandidates, d.cfg.NodeName, refs, now, d.cfg.MissingBlockDeviceRetention) {
					log.Debug(fmt.Sprintf("Missing BlockDevice %q is referenced by an LVMVolumeGroup; keeping it", device.Name))
					continue
				}
				cond := missingTrue(device)
				if err := d.deleteAPIBlockDevice(ctx, &device); client.IgnoreNotFound(err) != nil {
					log.Error(fmt.Sprintf("unable to delete Missing BlockDevice %q", device.Name), logger.Err(err))
					continue
				}
				delete(apiBlockDevices, device.Name)
				log.Info(fmt.Sprintf(
					"deleted BlockDevice %q: %s=True (%s) since %s, longer than the retention of %s, and no LVMVolumeGroup references it",
					device.Name, v1alpha1.BlockDeviceConditionMissing, cond.Reason,
					cond.LastTransitionTime.UTC().Format(time.RFC3339), d.cfg.MissingBlockDeviceRetention,
				))
			}
		}
	}

	byReason := map[string]int{v1alpha1.ReasonDeviceNotFound: 0}
	for _, device := range apiBlockDevices {
		if device.Status.NodeName != d.cfg.NodeName {
			continue
		}
		if cond := missingTrue(device); cond != nil {
			byReason[cond.Reason]++
		}
	}
	d.metrics.SetBlockDevicesMissing(byReason)
}

func (d *Discoverer) listLVMVolumeGroups(ctx context.Context) ([]v1alpha1.LVMVolumeGroup, error) {
	list := &v1alpha1.LVMVolumeGroupList{}
	start := time.Now()
	err := d.cl.List(ctx, list)
	d.metrics.APIMethodsDuration(DiscovererName, "list").Observe(d.metrics.GetEstimatedTimeInSeconds(start))
	d.metrics.APIMethodsExecutionCount(DiscovererName, "list").Inc()
	if err != nil {
		d.metrics.APIMethodsErrors(DiscovererName, "list").Inc()
		return nil, err
	}
	return list.Items, nil
}
