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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type LVMVolumeGroupSetList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []LVMVolumeGroupSet `json:"items"`
}

// An interface for creating several LVMVolumeGroups by common template at once.
//
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=lvgset
// +kubebuilder:subresource:status
// +kubebuilder:metadata:labels={"heritage=deckhouse","module=sds-node-configurator","backup.deckhouse.io/cluster-config=true"}
// +kubebuilder:printcolumn:name="Strategy",type="string",JSONPath=".spec.strategy",description="Specified LVMVolumeGroups strategy creation."
// +kubebuilder:printcolumn:name="VG",type="string",JSONPath=".spec.lvmVolumeGroupTemplate.actualVGNameOnTheNode",description="Actual Volume Group name."
// +kubebuilder:printcolumn:name="type",type="string",JSONPath=".spec.lvmVolumeGroupTemplate.type",priority=1,description="Volume Group type."
// +kubebuilder:printcolumn:name="Current LVG Count",type="integer",JSONPath=".status.currentLVMVolumeGroupsCount",description="Current LVMVolumeGroups count created by the set."
// +kubebuilder:printcolumn:name="Desired LVG Count",type="integer",JSONPath=".status.desiredLVMVolumeGroupsCount",description="Desired LVMVolumeGroups count created by the set."
// +kubebuilder:printcolumn:name="Ready LVG Count",type="integer",JSONPath=".status.readyLVMVolumeGroupsCount",description="Number of LVMVolumeGroups of the set in the Ready phase."
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase",description="Resource phase."
// +kubebuilder:printcolumn:name="Ready",type="string",JSONPath=".status.conditions[?(@.type==\"Ready\")].status"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp",description="Age of this resource."
// +crd-enricher:crd:minimal=true
// +crd-enricher:crd:preserveUnknownFields=false
type LVMVolumeGroupSet struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   LVMVolumeGroupSetSpec   `json:"spec"`
	Status LVMVolumeGroupSetStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen=true
type LVMVolumeGroupSetSpec struct {
	// Node selector for nodes that will be used by LVMVolumeGroups.
	// +crd-enricher:unset:x-kubernetes-map-type
	// +crd-enricher:raw:properties.matchLabels.description=Labels for the node selector.
	// +crd-enricher:raw:properties.matchExpressions.description=Expressions for the node selector.
	// +crd-enricher:unset:properties.matchExpressions.x-kubernetes-list-type
	// +crd-enricher:unset:properties.matchExpressions.items.description
	// +crd-enricher:raw:properties.matchExpressions.items.properties.key.description=Label key to check.
	// +crd-enricher:raw:properties.matchExpressions.items.properties.operator.description=Operator for comparing values.
	// +crd-enricher:raw:properties.matchExpressions.items.properties.operator.enum=[In, NotIn, Exists, DoesNotExist]
	// +crd-enricher:raw:properties.matchExpressions.items.properties.values.description=An array of values to compare. Required if the operator is `In` or `NotIn`.
	// +crd-enricher:unset:properties.matchExpressions.items.properties.values.x-kubernetes-list-type
	NodeSelector *metav1.LabelSelector `json:"nodeSelector"`
	// Common template for LVMVolumeGroup resources provided by the set.
	LVGTemplate LVMVolumeGroupTemplate `json:"lvmVolumeGroupTemplate"`
	// Strategy (rule) to provide LVMVolumeGroup resources by the set.
	// +kubebuilder:validation:Enum=PerNode
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="The strategy field is immutable."
	Strategy string `json:"strategy"`
}

// The per-entry rules live on LVMVolumeGroupFileDeviceSpec, shared with
// LVMVolumeGroup; see the comment on that type.

// +k8s:deepcopy-gen=true
// +kubebuilder:validation:XValidation:rule="has(self.blockDeviceSelector) || (has(self.fileDevices) && size(self.fileDevices) > 0)",message="At least one of 'blockDeviceSelector' or 'fileDevices' must be specified."
type LVMVolumeGroupTemplate struct {
	// Metadata of LVMVolumeGroup resources.
	// +optional
	Metadata LVMVolumeGroupTemplateMeta `json:"metadata"`
	// BlockDevice resources selector used in LVMVolumeGroups configurations.
	// +optional
	// +crd-enricher:unset:x-kubernetes-map-type
	// +crd-enricher:raw:properties.matchLabels.description=Labels for the block device selector.
	// +crd-enricher:raw:properties.matchExpressions.description=Expressions for the block device selector.
	// +crd-enricher:unset:properties.matchExpressions.x-kubernetes-list-type
	// +crd-enricher:unset:properties.matchExpressions.items.description
	// +crd-enricher:raw:properties.matchExpressions.items.properties.key.description=Label key to check.
	// +crd-enricher:raw:properties.matchExpressions.items.properties.operator.description=Operator for comparing values.
	// +crd-enricher:raw:properties.matchExpressions.items.properties.operator.enum=[In, NotIn, Exists, DoesNotExist]
	// +crd-enricher:raw:properties.matchExpressions.items.properties.values.description=An array of values to compare. Required if the operator is `In` or `NotIn`.
	// +crd-enricher:unset:properties.matchExpressions.items.properties.values.x-kubernetes-list-type
	BlockDeviceSelector *metav1.LabelSelector `json:"blockDeviceSelector,omitempty"`
	// Desired name of a Volume Group in LVMVolumeGroups. Must be unique for the node it is on.
	//
	// > This field is immutable.
	// > Note that this Volume Group name will be common for every LVMVolumeGroup created by the set.
	// Must be a valid LVM Volume Group name: up to 127 characters from a-z, A-Z, 0-9, +, _, . and -, not starting with a hyphen, and other than `.` and `..`.
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="The actualVGNameOnTheNode field is immutable."
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=127
	// +kubebuilder:validation:XValidation:rule="self.matches('^[a-zA-Z0-9+_.-]+$')",message="A Volume Group name may contain only the characters a-z, A-Z, 0-9, +, _, . and -."
	// +kubebuilder:validation:XValidation:rule="!self.startsWith('-')",message="A Volume Group name must not start with a hyphen."
	// +kubebuilder:validation:XValidation:rule="self != '.' && self != '..'",message="A Volume Group name must not be '.' or '..'."
	ActualVGNameOnTheNode string `json:"actualVGNameOnTheNode"`
	// Desired thin pool configuration in LVMVolumeGroups.
	//
	// > Note that the configuration will be common for every LVMVolumeGroup created by the set.
	// +optional
	// +kubebuilder:validation:MaxItems=256
	// +crd-enricher:raw:items.properties.size.description="Desired thin pool size. Might be specified as a number or percentage of total Volume Group space.\n\n> Note that if you specify the percent size, the thin pool will be automatically extended when Volume Group is extended."
	ThinPools []LVMVolumeGroupThinPoolSpec `json:"thinPools"`
	// Type of a VolumeGroup in LVMVolumeGroups.
	//
	// Possible values:
	//
	// - `Local`: Local volume group; the devices used are not distributed (not Shared LUN).
	// +kubebuilder:validation:Enum=Local
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="The type field is immutable."
	Type string `json:"type"`
	// The entries are checked by the same three rules as on LVMVolumeGroup, for
	// the same reasons. Enforcing them here as well keeps a template edit from
	// wedging every child LVMVolumeGroup at once -- a malformed `directory`
	// accepted here would do exactly that, since it cannot be changed
	// afterwards.

	// File-backed block devices to create as LVM PVs in LVMVolumeGroups.
	//
	// The agent will create a preallocated file in the specified directory,
	// attach it as a loop device, and use it as a Physical Volume.
	//
	// Each entry is identified by its `name`. `directory` cannot be changed,
	// and `size` can only be increased: raising it grows the backing file, the
	// loop device and the Physical Volume in place on every node the set covers,
	// without unmounting anything or interrupting the volumes on top. Lowering
	// it is rejected: giving capacity back means shrinking the Volume Group,
	// which the module does not do for file devices; for block devices it only
	// removes an empty Physical Volume (see `releaseEmptyBlockDevices`).
	//
	// Capacity can therefore be added two ways: raise the size of an existing
	// entry, or add a new entry under a new name when the extra space has to
	// come from a different filesystem — at most 32 entries per Volume Group.
	//
	// > Note that the configuration will be common for every LVMVolumeGroup created by the set.
	// > Entries removed from this template are removed from the child LVMVolumeGroups; if such
	// > an entry already backs a live Physical Volume, the agent reports the drift on the
	// > child's `VGConfigurationApplied` condition and takes no destructive action.
	// +optional
	// +kubebuilder:validation:MaxItems=32
	// +listType=map
	// +listMapKey=name
	// +crd-enricher:raw:items.properties.name.description="Identifier of this file device within each Volume Group the set creates.\nBecomes part of the backing file's name on the node."
	// +crd-enricher:raw:items.properties.directory.description="Absolute directory on the node's filesystem where the backing file\nwill be created. The agent creates the directory automatically\n(mkdir -p) if it does not exist; the path must be absolute and\nmust not contain '..' segments."
	// +crd-enricher:raw:items.properties.size.description="Size of the preallocated backing file. Must be at least 1Gi;\na smaller value is rejected at admission, since `size` cannot be\nlowered afterwards.\n\nOnly binary units are accepted; `1G` (10^9 bytes) is below the\n1Gi (2^30 bytes) minimum and is rejected at admission. The value\nmust be a whole number of the unit it names — `1536Mi`, not\n`1.5Gi`; see the same field on LVMVolumeGroup for why."
	FileDevices []LVMVolumeGroupFileDeviceSpec `json:"fileDevices,omitempty"`
	// Lets the agent take a block device out of the Volume Group once it no longer
	// matches `blockDeviceSelector`, provided its Physical Volume is empty.
	//
	// The value is copied to every LVMVolumeGroup created by the set; see the same
	// field on LVMVolumeGroup for the conditions under which the agent acts.
	//
	// > Note that a change of `blockDeviceSelector` in the template reaches all the nodes
	// > the set covers at once, and on each of them the device is released independently.
	// +optional
	ReleaseEmptyBlockDevices bool `json:"releaseEmptyBlockDevices,omitempty"`
}

// +k8s:deepcopy-gen=true
type LVMVolumeGroupTemplateMeta struct {
	// Required labels for LVMVolumeGroup resources.
	// +optional
	Labels map[string]string `json:"labels"`
}

// +k8s:deepcopy-gen=true
type LVMVolumeGroupSetStatus struct {
	// Brief information about LVMVolumeGroups of the set: the ones it created that
	// still exist and the one it keeps on each selected node.
	// +optional
	CreatedLVGs []LVMVolumeGroupSetStatusLVG `json:"createdLVMVolumeGroups"`
	// Current number of LVMVolumeGroups of the set.
	// +optional
	CurrentLVMVolumeGroupsCount int `json:"currentLVMVolumeGroupsCount"`
	// Desired number of LVMVolumeGroups that should be created by the set.
	// +optional
	DesiredLVMVolumeGroupsCount int `json:"desiredLVMVolumeGroupsCount"`
	// Number of LVMVolumeGroups of the set in the `Ready` phase.
	// +optional
	ReadyLVMVolumeGroupsCount int `json:"readyLVMVolumeGroupsCount"`
	// LVMVolumeGroups creation phase.
	// +optional
	// +kubebuilder:validation:Enum=Created;Pending;NotCreated;""
	Phase string `json:"phase"`
	// Reason carries free-form detail about the current phase, including raw error
	// text. Machine-readable reasons live on Conditions.

	// Reason for the current phase.
	// +optional
	Reason string `json:"reason"`
	// Phase is derived from Conditions.

	// The latest observations of the resource state. Condition type: `Ready`.
	// +optional
	// +listType=map
	// +listMapKey=type
	// +crd-enricher:unset:items.description
	// +crd-enricher:raw:items.properties.type.description="Condition type. `Ready` is `True` once the set of LVMVolumeGroup resources is in place, `False`\nwhile it is not, and `Unknown` before anything has observed it."
	// +crd-enricher:raw:items.properties.status.description=Current status of the condition.
	// +crd-enricher:raw:items.properties.observedGeneration.description=The value of `metadata.generation` this condition was set against.
	// +crd-enricher:raw:items.properties.lastTransitionTime.description="Timestamp of the last status transition for this condition. It is not bumped\nwhen only the reason or the message changes."
	// +crd-enricher:raw:items.properties.reason.description="Machine-readable reason for the current status, suitable for log filtering\nand alerting."
	// +crd-enricher:raw:items.properties.message.description=Human-readable explanation of the current status.
	Conditions []metav1.Condition `json:"conditions,omitempty"`
	// The value of `metadata.generation` the controller has last acted on. When it trails
	// `metadata.generation`, the controller has not yet processed the latest spec, and the
	// conditions below still describe the previous one.
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// +k8s:deepcopy-gen=true
type LVMVolumeGroupSetStatusLVG struct {
	// Name of the created LVMVolumeGroup resource.
	// +optional
	LVMVolumeGroupName string `json:"lvmVolumeGroupName"`
	// Name of the node where the LVMVolumeGroup resource was created.
	// +optional
	NodeName string `json:"nodeName"`
}
