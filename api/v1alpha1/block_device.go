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

package v1alpha1

import (
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// The resource displays up-to-date information about available block devices on nodes that can be used by the `sds-node-configurator` controller for LVM operations.
//
// > The resource is created and managed by the controller only. Manual modification of the resource is prohibited.
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=bd
// +kubebuilder:metadata:labels={"heritage=deckhouse","module=sds-node-configurator","backup.deckhouse.io/cluster-config=true"}
// +kubebuilder:printcolumn:name="Node",type="string",JSONPath=".status.nodeName",description="Node the block device is on."
// +kubebuilder:printcolumn:name="Consumable",type="boolean",JSONPath=".status.consumable",description="Defines whether the current block device can be used by `sds-node-configurator`."
// +kubebuilder:printcolumn:name="Reason",type="string",JSONPath=".status.conditions[?(@.type==\"Consumable\")].reason",priority=1,description="Why the device is or is not consumable."
// +kubebuilder:printcolumn:name="Size",type="string",JSONPath=".status.size",description="Size of the included device."
// +kubebuilder:printcolumn:name="Path",type="string",JSONPath=".status.path",description="Actual device path (name)."
// +kubebuilder:printcolumn:name="VG",type="string",JSONPath=".status.actualVGNameOnTheNode",priority=1,description="Volume Group the block device is in."
// +kubebuilder:printcolumn:name="LVMVolumeGroup",type="string",JSONPath=".status.lvmVolumeGroupName",priority=1,description="LVMVolumeGroup resource the block device is in."
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp",description="Age of this resource"
// +crd-enricher:crd:minimal=true
// +crd-enricher:crd:preserveUnknownFields=false
type BlockDevice struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +optional
	Status BlockDeviceStatus `json:"status"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// BlockDeviceList contains a list of empty block device
type BlockDeviceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`
	Items           []BlockDevice `json:"items"`
}

// +k8s:deepcopy-gen=true
type BlockDeviceStatus struct {
	// The device's type (disk, partition, RAID, and others).
	Type string `json:"type"`
	// The device's file system type.
	// +optional
	// +kubebuilder:validation:Enum=LVM2_member;""
	FsType string `json:"fsType"`
	// The name of the Kubernetes node the device is on.
	NodeName string `json:"nodeName"`
	// The flag that defines whether this device can be used by the controller to create an LVM Physical Volume.
	Consumable bool `json:"consumable"`
	// The LVM Physical Volume UUID.
	//
	// > **Optional**: exists only if the device is used as a LVM Physical Volume.
	// +optional
	PVUuid string `json:"pvUUID"`
	// The UUID of the LVM Volume Group to which this device belongs as a Physical Volume.
	//
	// > **Optional**: exists only if the device is used as a Physical Volume and is included in a Volume Group.
	// +optional
	VGUuid string `json:"vgUUID"`
	// The UUID of the partition.
	//
	// > **Optional**: exists only if the device is a partition.
	// +optional
	PartUUID string `json:"partUUID"`
	// The LVMVolumeGroup resource name.
	//
	// > **Optional**: exists only if the device is used as a Physical Volume, is included in a Volume Group, and is specified in an LVMVolumeGroup resource.
	// +optional
	LVMVolumeGroupName string `json:"lvmVolumeGroupName"`
	// The actual name of the LVM Volume Group the device is included in.
	//
	// > **Optional**: exists if only the device is used as a Physical Volume and is included in a Volume Group.
	// +optional
	ActualVGNameOnTheNode string `json:"actualVGNameOnTheNode"`
	// The device unique wwn (World Wide Name) identifier.
	//
	// > **Optional**: might not exist if WWN is not supported for this type of disk by the manufacturer.
	// +optional
	Wwn string `json:"wwn"`
	// The device unique serial number.
	// +optional
	Serial string `json:"serial"`
	// The device path on the node (/dev/sda).
	Path string `json:"path"`
	// The device size.
	Size resource.Quantity `json:"size"`
	// Device model.
	//
	// > **Optional**: might not exist if the manufacturer does not provide this information for the given type of disks.
	// +optional
	Model string `json:"model"`
	// Device disk type. Can be:
	// - `true`: For HDD;
	// - `false`: For SSD.
	Rota bool `json:"rota"`
	// Device connection type. Can be:
	// - `true`: If the device has been hot-plugged;
	// - `false`: Otherwise.
	HotPlug bool `json:"hotPlug"`
	// Unique identifier of the machine the device is on (normally at `/etc/machine-id`).
	MachineID string `json:"machineId"`

	// Latest state. Known type: `Consumable`, which carries the
	// reason behind `status.consumable`: `Available`, `OrphanPhysicalVolume`,
	// `Mounted`, `HasFilesystem`, `HasPartitionTable` or `HotPlugged`.
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	// +crd-enricher:unset:items.description
	// +crd-enricher:raw:items.properties.type.description=Condition type, for example `Consumable`.
	// +crd-enricher:raw:items.properties.status.description=Current status of the condition.
	// +crd-enricher:raw:items.properties.observedGeneration.description=Value of `metadata.generation` the condition was last set against.
	// +crd-enricher:raw:items.properties.lastTransitionTime.description=Timestamp of the last status transition for this condition.
	// +crd-enricher:raw:items.properties.reason.description="Machine-readable reason for the current status, suitable for\nlog filtering."
	// +crd-enricher:raw:items.properties.message.description=Human-readable explanation of the current status.
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

// BlockDeviceConditionConsumable reports whether the agent may use the device
// to create an LVM Physical Volume.
//
// It is not Ready, and deliberately so. Ready would be a verdict about
// nothing: this kind is a report of what the agent found on the node, not a
// resource reconciled towards a desired state. Worse, it would be misleading —
// a device already serving as a Physical Volume carries fsType LVM2_member and
// is therefore not consumable, so Ready=False would be the normal state of
// every disk in a working cluster and an alert on it would never stop firing.
//
// What status.consumable could not say is why. The answers are
// distinguishable and actionable in different ways, and they are what the
// reason carries.
const BlockDeviceConditionConsumable = "Consumable"

// The reasons BlockDeviceConditionConsumable is published with. They are this
// module's own: the shared vocabulary in sds-common-lib describes the progress
// of a reconcile, and none of these is about one.
const (
	// ReasonDeviceAvailable is set on Consumable=True for a device that carries
	// nothing.
	ReasonDeviceAvailable = "Available"
	// ReasonDeviceMounted is set on Consumable=False when the device is
	// mounted. Unmounting it makes the device available.
	ReasonDeviceMounted = "Mounted"
	// ReasonDeviceHasFilesystem is set on Consumable=False when the device
	// carries a filesystem. This covers a device already in use as a Physical
	// Volume (fsType LVM2_member), which is the common case and not a problem.
	ReasonDeviceHasFilesystem = "HasFilesystem"
	// ReasonDeviceHotPlugged is set on Consumable=False for a hot-plugged
	// device. The agent refuses those regardless of their contents, since a
	// Volume Group on removable media loses its Physical Volume without notice.
	ReasonDeviceHotPlugged = "HotPlugged"
	// ReasonDeviceHasPartitionTable is set on Consumable=False when the device
	// carries a partition table, even one with no partitions in it: the kernel
	// shows no partitions then, but pvcreate still refuses the device — LVM
	// before 2.03.27 as partitioned, later versions by stopping at the
	// partition-table signature they would have to wipe. Wiping the table
	// (wipefs --all) makes the device available.
	ReasonDeviceHasPartitionTable = "HasPartitionTable"
	// ReasonDeviceOrphanPhysicalVolume is set on Consumable=True for an LVM
	// Physical Volume that belongs to no Volume Group, typically one left by a
	// Volume Group creation that failed after pvcreate. vgcreate and vgextend
	// take such a Physical Volume as it is.
	ReasonDeviceOrphanPhysicalVolume = "OrphanPhysicalVolume"
)

// BlockDeviceConditionMissing reports that the device this BlockDevice
// describes was not found on its node during the most recent scan.
//
// The agent publishes it only for a device it would otherwise never delete on
// its own -- one that is not Consumable (see shouldDeleteBlockDevice), typically
// because it is already an LVM Physical Volume. The controller publishes it for
// every BlockDevice of a node no agent runs on, since nothing on that node
// scans its devices any more. Losing that device out from
// under a live LVMVolumeGroup is worth surfacing; silently doing nothing,
// which is what happened before this condition existed, left status.path
// pointing at a device that no longer existed with no indication anywhere
// that anything was wrong.
//
// Absent while the device is seen normally -- there is no "not missing" to
// report. Unknown while it has been briefly absent: a flapping iSCSI path or
// a udev event burst straddling the device coming back is the common case,
// and this clears itself the moment the device reappears without ever
// alerting anyone. Only escalates to True once the device has been missing
// continuously for missingGracePeriod (see discoverer.go), since by then a
// transient blip is no longer the likely explanation.
const BlockDeviceConditionMissing = "Missing"

// The reasons BlockDeviceConditionMissing is published with, at both Unknown
// and True -- the two statuses are already what distinguishes "recently gone"
// from "confirmed gone".
const (
	// ReasonDeviceNotFound is set by the agent of the node: its scan did not
	// find the device.
	ReasonDeviceNotFound = "DeviceNotFound"
	// ReasonNodeNotFound is set by the controller: the node the BlockDevice
	// belongs to is not in the cluster, so no agent will ever scan it again.
	ReasonNodeNotFound = "NodeNotFound"
	// ReasonAgentNotRunning is set by the controller: the node is in the
	// cluster, but no agent Pod runs on it, typically because the node lost
	// the labels the agent is placed by.
	ReasonAgentNotRunning = "AgentNotRunning"
)

// BlockDeviceConditionTypes is every condition type a BlockDevice publishes.
// See LVGConditionTypes in the controller for why the set is written down.
var BlockDeviceConditionTypes = []string{
	BlockDeviceConditionConsumable,
	BlockDeviceConditionMissing,
}
