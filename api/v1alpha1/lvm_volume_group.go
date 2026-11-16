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

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type LVMVolumeGroupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []LVMVolumeGroup `json:"items"`
}

// An interface for managing Volume Groups and Thin pools on the nodes.
//
// > These resources might be created both by a user and the `sds-node-configurator` controller. `sds-node-configurator` controller will automatically create an LVMVolumeGroup resource if it detects an existing Volume Group on a node tagged with 'storage.deckhouse.io/enabled=true'. Controller will fill in both the 'spec' and 'status' fields.
// > `spec.thinPools` field must be filled in by yourself.
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=lvg
// +kubebuilder:subresource:status
// +kubebuilder:metadata:labels={"heritage=deckhouse","module=sds-node-configurator","backup.deckhouse.io/cluster-config=true"}
// +kubebuilder:printcolumn:name="ThinPools",type="string",JSONPath=".status.thinPoolReady",description="Current and total Thin pools count."
// +kubebuilder:printcolumn:name="Configuration Applied",type="string",JSONPath=".status.configurationApplied",description="If last configuration has been successfully applied."
// +kubebuilder:printcolumn:name="phase",type="string",JSONPath=".status.phase",description="Resource phase."
// +kubebuilder:printcolumn:name="Ready",type="string",JSONPath=".status.conditions[?(@.type==\"Ready\")].status",description="Whether every stage condition has been reported and passed."
// +kubebuilder:printcolumn:name="node",type="string",JSONPath=".status.nodes..name",description="Node the Volume Group is on."
// +kubebuilder:printcolumn:name="size",type="string",JSONPath=".status.vgSize",description="Total Volume Group size."
// +kubebuilder:printcolumn:name="allocated size",type="string",JSONPath=".status.allocatedSize",description="Actual Volume Group size."
// +kubebuilder:printcolumn:name="VG",type="string",JSONPath=".spec.actualVGNameOnTheNode",description="Actual Volume Group name."
// +kubebuilder:printcolumn:name="type",type="string",JSONPath=".spec.type",priority=1,description="Volume Group type."
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp",description="Age of this resource."
// +crd-enricher:crd:minimal=true
// +crd-enricher:crd:preserveUnknownFields=false
type LVMVolumeGroup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   LVMVolumeGroupSpec   `json:"spec"`
	Status LVMVolumeGroupStatus `json:"status,omitempty"`
}

// The per-entry rules -- 'directory' is immutable, 'size' may only grow -- live
// on LVMVolumeGroupFileDeviceSpec itself rather than here. See the comment on
// that type.

// +k8s:deepcopy-gen=true
// +kubebuilder:validation:XValidation:rule="(self.type == \"Local\" && has(self.local)) || self.type != \"Local\"",message="The 'local' field is required when the 'type' field is 'Local'."
// +kubebuilder:validation:XValidation:rule="has(self.blockDeviceSelector) || (has(self.fileDevices) && size(self.fileDevices) > 0)",message="At least one of 'blockDeviceSelector' or 'fileDevices' must be specified."
type LVMVolumeGroupSpec struct {
	// Desired name of a Volume Group. Must be unique for the node it is on.
	// Must be a valid LVM Volume Group name: up to 127 characters from a-z, A-Z, 0-9, +, _, . and -, not starting with a hyphen, and other than `.` and `..`.
	//
	// > This field is immutable.
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="The actualVGNameOnTheNode field is immutable."
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=127
	// +kubebuilder:validation:XValidation:rule="self.matches('^[a-zA-Z0-9+_.-]+$')",message="A Volume Group name may contain only the characters a-z, A-Z, 0-9, +, _, . and -."
	// +kubebuilder:validation:XValidation:rule="!self.startsWith('-')",message="A Volume Group name must not start with a hyphen."
	// +kubebuilder:validation:XValidation:rule="self != '.' && self != '..'",message="A Volume Group name must not be '.' or '..'."
	ActualVGNameOnTheNode string `json:"actualVGNameOnTheNode"`
	// Desired block device selector.
	//
	// > Note: Selected block devices must belong to the same node for a volume group of type ‘Local’.
	// +optional
	// +crd-enricher:unset:x-kubernetes-map-type
	// +crd-enricher:raw:properties.matchLabels.description=Desired block device selector labels.
	// +crd-enricher:raw:properties.matchExpressions.description=Desired block device selector expressions.
	// +crd-enricher:unset:properties.matchExpressions.x-kubernetes-list-type
	// +crd-enricher:unset:properties.matchExpressions.items.description
	// +crd-enricher:raw:properties.matchExpressions.items.properties.key.description=Label key to check.
	// +crd-enricher:raw:properties.matchExpressions.items.properties.operator.description=Operator used to compare values.
	// +crd-enricher:raw:properties.matchExpressions.items.properties.operator.enum=[In, NotIn, Exists, DoesNotExist]
	// +crd-enricher:raw:properties.matchExpressions.items.properties.values.description=Array of values to compare. Required if the operator is `In` or `NotIn`.
	// +crd-enricher:unset:properties.matchExpressions.items.properties.values.x-kubernetes-list-type
	BlockDeviceSelector *metav1.LabelSelector `json:"blockDeviceSelector,omitempty"`
	// Desired thin pool configuration.
	// +optional
	// +kubebuilder:validation:MaxItems=256
	ThinPools []LVMVolumeGroupThinPoolSpec `json:"thinPools"`
	// Type of a Volume Group.
	//
	// Possible values:
	//
	// - `Local`: Local volume group; the devices used are not distributed (not Shared LUN).
	// +kubebuilder:validation:Enum=Local
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="The type field is immutable."
	Type string `json:"type"`
	// Desired configuration for a local Volume Group.
	// +optional
	Local LVMVolumeGroupLocalSpec `json:"local"`
	// File-backed block devices to create as LVM PVs for this Volume Group.
	//
	// The agent will create a preallocated file in the specified directory,
	// attach it as a loop device, and use it as a Physical Volume.
	// File devices and block devices can be used together in the same VG.
	//
	// Each entry is identified by its `name`. `directory` cannot be changed,
	// and `size` can only be increased: raising it grows the backing file, the
	// loop device and the Physical Volume in place, without unmounting anything
	// or interrupting the volumes on top. Lowering it is rejected: giving
	// capacity back means shrinking the Volume Group, which the module does
	// not do for file devices; for block devices it only removes an empty
	// Physical Volume (see `releaseEmptyBlockDevices`).
	//
	// Capacity can therefore be added two ways: raise the size of an existing
	// entry, or add a new entry under a new name when the extra space has to
	// come from a different filesystem. Prefer fewer, larger entries — every
	// entry costs a loop device, a PV and VG metadata, and thick logical volumes
	// requesting contiguous space get harder to place as free space is spread
	// across many small PVs.
	//
	// An entry that was never provisioned (it is not present in
	// `status.nodes[].fileDevices`, for example because it failed validation)
	// can be removed from the spec. Removing an entry that backs a live PV is
	// not supported — the module never shrinks a Volume Group; the agent
	// reports the drift on the `VGConfigurationApplied` condition (reason
	// `FileDeviceDrift`) and takes no destructive action. The Volume Group
	// keeps working and stays `Ready`. Freeing space inside a file device is
	// done with `fstrim` instead, see the module FAQ.
	// +optional
	// +kubebuilder:validation:MaxItems=32
	// +listType=map
	// +listMapKey=name
	FileDevices []LVMVolumeGroupFileDeviceSpec `json:"fileDevices,omitempty"`
	// Lets the agent take a block device out of the Volume Group once it no longer
	// matches `blockDeviceSelector`, provided its Physical Volume is empty.
	//
	// When `true`, the agent runs `vgreduce` and `pvremove` for such a device,
	// and only when all of these hold:
	//
	// - the BlockDevice exists, its labels do not match `blockDeviceSelector`, and it has no `Missing` condition;
	// - no other BlockDevice on the node carries the same Physical Volume;
	// - the Physical Volume holds no data (`pv_used` is zero when checked right before `vgreduce`);
	// - the Physical Volume belongs to this Volume Group, which carries the tag of this resource, is neither shared nor owned by another host, and keeps at least one other Physical Volume;
	// - no `pvmove` is running in the Volume Group;
	// - no thin pool sized as a percentage would be larger than its share of the Volume Group without this Physical Volume;
	// - all of the above has held on two reconciliation passes at least a minute apart.
	//
	// A Physical Volume that still holds data is never touched: the condition
	// `VGConfigurationApplied` names the device and the amount of data on it, and the
	// data has to be moved off with `pvmove` on the node first. With the default
	// `false` the agent removes nothing and the same condition tells what to do.
	//
	// Use it with a `blockDeviceSelector` on labels that stay with the device, not on the
	// `status.blockdevice.storage.deckhouse.io/*` labels, which the agent changes itself.
	//
	// File devices (`fileDevices`) are not affected by this field.
	// +optional
	ReleaseEmptyBlockDevices bool `json:"releaseEmptyBlockDevices,omitempty"`
}

// The first rule is checked on every entry; the two below it are transition
// rules, evaluated only against the entry that carried the same `name` before
// the edit.
//
// That correlation is the apiserver's, not ours: `fileDevices` is an
// x-kubernetes-list-type=map keyed on `name`, which makes this a correlatable
// position, so `oldSelf` here is the entry with the same name -- and the rules
// are simply not evaluated for an entry that had no predecessor. Appending an
// entry and removing one are therefore exempt by construction rather than by
// being spelled out, and a reorder is a no-op because nothing is matched by
// position. See
// https://kubernetes.io/docs/tasks/extend-kubernetes/custom-resources/custom-resource-definitions/#transition-rules
//
// Expressing them per entry instead of as one nested all() over both the old
// and the new list also keeps the cost linear in maxItems rather than
// quadratic, and lets each violation report itself -- one message naming both
// invariants tells an admin who edited `directory` about shrinking too.
//
// The minimum size is rejected at admission for the same reason a decimal unit
// is: `size` cannot be lowered afterwards, so an entry accepted below the
// minimum is an entry the agent refuses forever and the user can only escape by
// removing and re-adding it. The agent keeps its own check for objects that were
// imported or already exist.

// +k8s:deepcopy-gen=true
// +kubebuilder:validation:XValidation:rule="quantity(self.size).compareTo(quantity('1Gi')) >= 0",message="A fileDevices entry's 'size' must be at least 1Gi."
// +kubebuilder:validation:XValidation:rule="self.directory == oldSelf.directory",message="A fileDevices entry's 'directory' is immutable."
// +kubebuilder:validation:XValidation:rule="quantity(self.size).compareTo(quantity(oldSelf.size)) >= 0",message="A fileDevices entry's 'size' can only be increased. Shrinking a Volume Group is not supported."
type LVMVolumeGroupFileDeviceSpec struct {
	// Name is the entry's primary key (x-kubernetes-list-map-keys) and the only
	// part of the backing file's path the user controls, which is what keeps
	// `size` editable in principle and lets two entries of equal size share a
	// directory.

	// Identifier of this file device within the Volume Group, unique among
	// the entries of this LVMVolumeGroup. It becomes part of the backing
	// file's name on the node, so it is restricted to lowercase
	// alphanumerics and '-'.
	//
	// > This field is the entry's key: an entry is identified by it, and
	// > changing it is not an edit but the removal of one entry and the
	// > addition of another. If the old name had already been provisioned,
	// > its Physical Volume stays in the Volume Group and the agent reports
	// > it on the `VGConfigurationApplied` condition.
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	Name string `json:"name"`
	// The pattern is rejected at admission for the same reason `size` and
	// `name` are: `directory` is immutable once the entry exists, so a malformed
	// value is one the agent refuses forever and the user can only escape by
	// removing and re-adding the entry. This is the same shape the
	// fileDevicesDirectory module setting enforces -- an absolute path of at
	// least one non-empty component. A '..' component cannot be excluded here
	// (an RE2 regexp has no lookahead), so the agent keeps its own check for
	// that, and for the base-directory subtree.

	// Absolute directory on the node's filesystem where the backing file
	// will be created.
	//
	// Must be the module's configured base directory
	// (fileDevicesDirectory, default /var/lib/deckhouse/sds/file-devices) or a
	// subdirectory of it; the agent rejects paths outside that subtree so a
	// stray entry cannot fill an arbitrary host path. The previous default,
	// /opt/deckhouse/sds/file-devices, is still accepted: this field is
	// immutable, so an entry created before the directory moved cannot be
	// edited to the new one.
	//
	// The agent creates the directory automatically (mkdir -p) if it does
	// not exist. The path must be absolute and must not contain '..'
	// segments. Provisioning fails if the path is on a read-only
	// filesystem, a non-directory component is in the way, or there is not
	// enough free space for the requested size.
	// +kubebuilder:validation:MaxLength=4096
	// +kubebuilder:validation:Pattern=`^/[^/]+(/[^/]+)*$`
	Directory string `json:"directory"`
	// Size of the preallocated backing file. Must be at least 1Gi;
	// a smaller value is rejected at admission, because `size` cannot
	// be lowered afterwards and an entry accepted below the minimum is
	// one the agent refuses forever.
	//
	// Only binary units are accepted. Decimal units are rejected at
	// admission for the same reason: `1G` is 10^9 bytes, which is below
	// the 1Gi (2^30 bytes) minimum, and the mistake is easy to miss.
	//
	// The value must be a whole number of the unit it names: `1536Mi`,
	// not `1.5Gi`. A Physical Volume is always a little smaller than
	// its backing file (LVM keeps metadata at the head of the device
	// and then rounds down to whole extents), and the reconciler treats
	// a difference of up to one extent as "already the right size". That
	// tolerance covers the metadata gap exactly as long as the requested
	// size is a whole number of mebibytes; a finer value can leave a
	// permanent gap the agent would try to close on every reconcile.
	// +crd-enricher:raw:type=string
	// +crd-enricher:raw:maxLength=32
	// +crd-enricher:unset:anyOf
	// +crd-enricher:unset:x-kubernetes-int-or-string
	// +crd-enricher:raw:pattern=^[0-9]+(Mi|Gi|Ti|Pi|Ei)$
	Size resource.Quantity `json:"size"`
}

// +k8s:deepcopy-gen=true
type LVMVolumeGroupStatus struct {
	// Amount of space currently occupied on the Volume Group.
	// +optional
	AllocatedSize resource.Quantity `json:"allocatedSize"`
	// Information about the nodes the Volume Group is on.
	// +optional
	Nodes []LVMVolumeGroupNode `json:"nodes"`
	// Current state of the Volume Group's thin pools.
	//
	// > Might be empty if there are no thin pools in the Volume Group.
	// +optional
	ThinPools []LVMVolumeGroupThinPoolStatus `json:"thinPools"`
	// Volume Group capacity.
	// +optional
	VGSize resource.Quantity `json:"vgSize"`
	// Volume Group UUID.
	// +optional
	VGUuid string `json:"vgUUID"`
	// General LVMVolumeGroup condition.
	// +optional
	// +kubebuilder:validation:Enum=Pending;Ready;NotReady;Terminating;""
	Phase string `json:"phase"`
	// The value of `metadata.generation` the conditions watcher last computed a
	// verdict against. When it trails `metadata.generation`, the phase and the
	// aggregate `Ready` condition still describe the previous spec.
	//
	// Only the conditions watcher sets it, because it is the only writer that
	// looks at the resource as a whole. The agent and the infrastructure watcher
	// each own individual stage conditions and stamp the generation on those.
	// +kubebuilder:validation:Minimum=0
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// LVMVolumeGroup conditions.
	// +optional
	// +listType=map
	// +listMapKey=type
	// +crd-enricher:unset:items.description
	// +crd-enricher:raw:items.properties.type.description=Condition type.
	// +crd-enricher:raw:items.properties.type.enum=[VGConfigurationApplied, VGReady, NodeReady, AgentReady, Ready]
	// +crd-enricher:raw:items.properties.status.description=Condition status.
	// +crd-enricher:raw:items.properties.reason.description="Machine-readable reason for the current status, suitable for log filtering\nand alerting."
	// +crd-enricher:raw:items.properties.message.description=Information about the current status.
	// +crd-enricher:raw:items.properties.lastTransitionTime.description="Timestamp of the last status transition for this condition. It is not bumped\nwhen only the reason or the message changes."
	// +crd-enricher:raw:items.properties.observedGeneration.description=The value of `metadata.generation` this condition was set against.
	Conditions []metav1.Condition `json:"conditions"`
	// Shows the number of healthy and total thin pools.
	// +optional
	ThinPoolReady string `json:"thinPoolReady"`
	// Indicates if the last configuration has been successfully applied.
	// +optional
	ConfigurationApplied string `json:"configurationApplied"`
	// Volume Group free space.
	// +optional
	VGFree resource.Quantity `json:"vgFree"`
	// Physical extent size of the Volume Group.
	// +optional
	ExtentSize resource.Quantity `json:"extentSize"`
}

// +k8s:deepcopy-gen=true
type LVMVolumeGroupDevice struct {
	// Name of the corresponding block device resource.
	// +optional
	BlockDevice string `json:"blockDevice"`
	// Block device size.
	// +optional
	DevSize resource.Quantity `json:"devSize"`
	// Physical Volume size.
	// +optional
	PVSize resource.Quantity `json:"pvSize"`
	// LVM Physical Volume UUID.
	// +optional
	PVUuid string `json:"pvUUID"`
	// Device path on the node (e.g., `/dev/sda`).
	// +optional
	Path string `json:"path"`
}

// +k8s:deepcopy-gen=true
type LVMVolumeGroupNode struct {
	// Information about the devices used in the Volume Group on the current node.
	// +optional
	Devices []LVMVolumeGroupDevice `json:"devices"`
	// Information about the file-backed devices used in the Volume Group on the current node.
	// +optional
	FileDevices []LVMVolumeGroupFileDevice `json:"fileDevices,omitempty"`
	// Node name.
	// +optional
	Name string `json:"name"`
}

// +k8s:deepcopy-gen=true
type LVMVolumeGroupFileDevice struct {
	// Name is what lets the reconciler tell "this entry was never provisioned"
	// (safe to drop from the spec) from "this entry backs a live PV" (dropping
	// it needs pvmove/vgreduce, which the module does not do).

	// Name of the `spec.fileDevices` entry this device was provisioned from.
	//
	// An entry present here backs a live Physical Volume and cannot be
	// removed from the spec; an entry absent from this list has not been
	// provisioned and may be removed.
	// +optional
	Name string `json:"name"`
	// Path to the backing file on the node.
	// +optional
	FilePath string `json:"filePath"`
	// Loop device path (e.g., `/dev/loop0`).
	// +optional
	LoopDevice string `json:"loopDevice"`
	// Size of the Physical Volume created on the backing file.
	// This is slightly smaller than the requested `spec.fileDevices[].size`
	// because LVM reserves space for PV metadata (the same way
	// `status.nodes[].devices[].pvSize` differs from the raw device size).
	// +optional
	Size resource.Quantity `json:"size"`
	// LVM Physical Volume UUID.
	// +optional
	PVUuid string `json:"pvUUID"`
}

// +k8s:deepcopy-gen=true
type LVMVolumeGroupThinPoolStatus struct {
	// Thin pool name.
	Name string `json:"name"`
	// Thin pool capacity.
	ActualSize resource.Quantity `json:"actualSize"`
	// Thin pool used size.
	UsedSize resource.Quantity `json:"usedSize"`
	// Total requested size of logical volumes in the thin pool.
	// +optional
	AllocatedSize resource.Quantity `json:"allocatedSize"`
	// Space left for new thin volumes and snapshots: the pool size multiplied by allocationLimit, minus allocatedSize. This is not the physical free space of the pool (see usedSize) and can be negative.
	// +optional
	AvailableSpace resource.Quantity `json:"availableSpace"`
	// Overprovisioning limit applied to the thin pool, taken from the spec.
	// +optional
	// +kubebuilder:default="150%"
	// +kubebuilder:validation:Pattern=`^[1-9][0-9]{2,3}%$`
	AllocationLimit string `json:"allocationLimit"`
	// Thin pool health status.
	// +optional
	Ready bool `json:"ready"`
	// Information about the status.
	// +optional
	Message string `json:"message"`
}

// +k8s:deepcopy-gen=true
type LVMVolumeGroupThinPoolSpec struct {
	// Desired thin pool name.
	// Must be a valid LVM logical volume name: up to 127 characters from a-z, A-Z, 0-9, +, _, . and -, not starting with a hyphen or with snapshot or pvmove, not `.` or `..`, and not containing a suffix LVM reserves for its internal volumes, such as _tdata or _tmeta.
	//
	// > This field is immutable.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=127
	// +kubebuilder:validation:XValidation:rule="self.matches('^[a-zA-Z0-9+_.-]+$')",message="A logical volume name may contain only the characters a-z, A-Z, 0-9, +, _, . and -."
	// +kubebuilder:validation:XValidation:rule="!self.startsWith('-')",message="A logical volume name must not start with a hyphen."
	// +kubebuilder:validation:XValidation:rule="self != '.' && self != '..'",message="A logical volume name must not be '.' or '..'."
	// +kubebuilder:validation:XValidation:rule="!self.startsWith('snapshot') && !self.startsWith('pvmove')",message="LVM reserves logical volume names that start with snapshot or pvmove."
	// +kubebuilder:validation:XValidation:rule="!self.matches('_(cdata|cmeta|corig|cpool|cvol|wcorig|mimage|mlog|rimage|rmeta|tdata|tmeta|vdata|imeta|iorig|pmspare|vorigin)')",message="LVM reserves logical volume names that contain _cdata, _cmeta, _corig, _cpool, _cvol, _wcorig, _mimage, _mlog, _rimage, _rmeta, _tdata, _tmeta, _vdata, _imeta, _iorig, _pmspare or _vorigin."
	Name string `json:"name"`
	// Desired thin pool size. Might be specified as number or percent size of total Volume Group space.
	//
	// > Note, that if you specify the percent size, the thin pool will be automatically extended when Volume Group is extended.
	// +kubebuilder:validation:XIntOrString
	// +kubebuilder:validation:Type=""
	// +kubebuilder:validation:Pattern=`^[0-9]+(\.[0-9]+)?(E|P|T|G|M|k|Ei|Pi|Ti|Gi|Mi|Ki)?$|^[1-9][0-9]?%$|100%`
	Size string `json:"size"`
	// Overprovisioning limit of the thin pool: the total size of the thin volumes and snapshots in the pool may not exceed the pool size multiplied by this value. `100%` disables overprovisioning.
	//
	// The value can be changed at any time. Lowering it below the size already allocated keeps the existing volumes but refuses new volumes, expansions and snapshots in the pool.
	// +optional
	// +kubebuilder:default="150%"
	// +kubebuilder:validation:Pattern=`^[1-9][0-9]{2,3}%$`
	AllocationLimit string `json:"allocationLimit"`
}

// +k8s:deepcopy-gen=true
type LVMVolumeGroupLocalSpec struct {
	// Desired node for the LVM Volume Group.
	//
	// > This field is immutable.
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="The nodeName field is immutable."
	NodeName string `json:"nodeName"`
}
