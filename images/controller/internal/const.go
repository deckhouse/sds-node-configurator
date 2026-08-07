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

	ReasonPending          = "Pending"
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

	TypeReady                  = "Ready"
	TypeVGConfigurationApplied = "VGConfigurationApplied"
)
