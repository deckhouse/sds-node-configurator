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

package internal

const (
	SdsNodeConfiguratorNamespace = "d8-sds-node-configurator"

	ReasonUpdating         = "Updating"
	ReasonCreating         = "Creating"
	ReasonTerminating      = "Terminating"
	ReasonValidationFailed = "ValidationFailed"
	// ReasonFileDeviceDrift mirrors the agent-side constant of the same name
	// (images/agent/internal/const.go). It means spec.fileDevices no longer
	// describes the file-backed Physical Volumes the Volume Group actually has,
	// which happens when an entry backing a live PV is removed from the spec.
	//
	// It has to be listed in acceptableReasons alongside ReasonValidationFailed:
	// the spec is well-formed, everything that could be applied has been, and the
	// Volume Group keeps serving its volumes. Only an operator can decide what
	// happens to the orphaned Physical Volume, and taking the whole Volume Group
	// out of service while they decide would turn a report into an outage.
	ReasonFileDeviceDrift = "FileDeviceDrift"
	// ReasonFileDeviceNotApplied mirrors the agent-side constant of the same name.
	// A spec.fileDevices entry could not be brought up on the node — no free
	// space, losetup refused, a grow that did not go through — while the Volume
	// Group itself is intact and every volume on it keeps working.
	//
	// It belongs in acceptableReasons for the same reason ReasonFileDeviceDrift
	// does: it reports capacity that has not arrived, not storage that has
	// broken. Treating it as a failure would let one oversized entry appended to
	// a healthy LVMVolumeGroup stop the scheduler from placing volumes on it.
	ReasonFileDeviceNotApplied = "FileDeviceNotApplied"
	// ReasonAliasResolutionFailed mirrors the agent-side constant of the same
	// name: the agent cannot canonicalize the alias-form PV names it needs to
	// decide whether a file-backed loop device is already part of the VG, and has
	// been failing at it long enough for the failure to look persistent.
	//
	// New file devices cannot join the Volume Group until it clears, so it is
	// worth alerting on — but the Volume Group and its volumes are untouched, so
	// it must not take it out of service.
	ReasonAliasResolutionFailed = "AliasResolutionFailed"
	// ReasonFileDeviceGrowFailed mirrors the agent-side constant of the same
	// name: raising spec.fileDevices[].size did not go through — no room for the
	// larger backing file, losetup or pvresize refusing, a reconcile cut short.
	//
	// It belongs in acceptableReasons because the growth sequence is ordered so
	// that every step fails towards the smaller size: whatever stopped it, the
	// Volume Group is still the size it was and still serving every volume on it.
	// This is capacity that has not arrived, not storage that has broken.
	ReasonFileDeviceGrowFailed = "FileDeviceGrowFailed"
	// ReasonCacheStale mirrors the agent-side constant of the same name: the node
	// has a Volume Group the agent's LVM cache does not know about yet, so the
	// reconcile has nothing to work from and waits. Nothing is wrong with the
	// Volume Group — it is the reason the agent refuses to touch it — so this
	// must not take the LVMVolumeGroup out of service either.
	ReasonCacheStale = "CacheStale"
	// ReasonBlockDeviceNotFound mirrors the agent-side constant of the same name:
	// some of a Volume Group's Physical Volumes have no BlockDevice resource to name
	// them, so status.nodes[].devices does not list those devices. Either the
	// block-device discoverer has not registered them yet, which takes seconds, or
	// they are devices that never become BlockDevices — under the minimum size, or
	// excluded by a BlockDeviceFilter.
	//
	// It belongs in acceptableReasons because it says nothing about the Volume Group
	// itself: the status carrying it was written in that same pass, so vgSize, vgFree
	// and thin-pool usage are current, the missing device entry is not part of that
	// arithmetic, and every volume on the Volume Group keeps working. A PV
	// deliberately kept below the minimum size or filtered out would otherwise take
	// an LVMVolumeGroup that is serving perfectly well out of service permanently.
	//
	// The agent's other unnamed-PV reason, NodeNotDescribed, is deliberately not
	// mirrored here: it means no PV could be named at all, so status.nodes was left
	// as an earlier pass wrote it and its free space is stale. Like VGCheckFailed it
	// lives on the agent side only, which is what keeps it out of acceptableReasons.
	ReasonBlockDeviceNotFound = "BlockDeviceNotFound"

	TypeReady                  = "Ready"
	TypeVGConfigurationApplied = "VGConfigurationApplied"
	// TypeVGReady mirrors the agent-side constant of the same name
	// (images/agent/internal/const.go): the agent reports whether the Volume Group
	// it manages on the node is usable.
	TypeVGReady    = "VGReady"
	TypeNodeReady  = "NodeReady"
	TypeAgentReady = "AgentReady"
)

// LVGConditionTypes is every condition type published on an LVMVolumeGroup, by
// the agent on each node and by the controllers here. The aggregate Ready
// condition is only meaningful once all of them have been observed at least
// once, so the conditions watcher uses this list to tell "not ready" apart from
// "not yet reported".
//
// It has to be kept in step with the type enum in crds/lvmvolumegroup.yaml, which
// TestLVGConditionTypesMatchTheCRDEnum asserts. It used to be derived from that
// enum at runtime — the controller fetched its own CustomResourceDefinition and
// counted the enum entries — which cost the controller a cluster-wide read on
// CRDs, made the reconcile fail whenever that read failed, and hid the fact that
// VGReady had no constant on this side at all.
var LVGConditionTypes = []string{
	TypeVGConfigurationApplied,
	TypeVGReady,
	TypeNodeReady,
	TypeAgentReady,
	// Ready is written by the conditions watcher itself, and was counted by the
	// enum-length check it replaces: a fresh LVMVolumeGroup reports Pending until
	// its first Ready has been written. Dropping it from the list would change
	// that sequence, which the e2e suite asserts on.
	TypeReady,
}

// Which component writes which LVMVolumeGroup condition.
//
// This matters more here than in the other storage modules: the conditions
// watcher refuses to compute Ready until every type in LVGConditionTypes has
// been written at least once, so a declared type nobody writes does not merely
// go missing — it wedges every LVMVolumeGroup in the cluster at Pending.
//
// The agent's types are written from a different Go module, so no single test
// can drive both sides. The split lets each side be checked where it lives.
var (
	// LVGConditionsOwnedByAgent are written by the agent on each node, from what
	// it observes about the Volume Group there.
	LVGConditionsOwnedByAgent = []string{
		TypeVGConfigurationApplied,
		TypeVGReady,
	}
	// LVGConditionsOwnedByController are written by the controllers here: the
	// infrastructure watcher reports on the node and the agent pod, and the
	// conditions watcher aggregates the rest into Ready.
	LVGConditionsOwnedByController = []string{
		TypeNodeReady,
		TypeAgentReady,
		TypeReady,
	}
)
