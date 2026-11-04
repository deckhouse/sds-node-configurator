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

// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type LVMLogicalVolumeList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []LVMLogicalVolume `json:"items"`
}

// LVMLogicalVolume resource defines the storage where a PV will be created.
//
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=llv
// +kubebuilder:subresource:status
// +kubebuilder:metadata:labels={"heritage=deckhouse","module=sds-node-configurator","backup.deckhouse.io/cluster-config=true"}
// +kubebuilder:printcolumn:name="LV Name",type="string",JSONPath=".spec.actualLVNameOnTheNode",description="Actual Logical Volume name on the node."
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase",description="Current resource status."
// +kubebuilder:printcolumn:name="Ready",type="string",JSONPath=".status.conditions[?(@.type==\"Ready\")].status"
// +kubebuilder:printcolumn:name="LVMVolumeGroup",type="string",JSONPath=".spec.lvmVolumeGroupName",description="Selected LVMVolumeGroup resource."
// +kubebuilder:printcolumn:name="Node",type="string",JSONPath=".status.nodeName",description="Kubernetes node the Logical Volume resides on."
// +kubebuilder:printcolumn:name="ThinPool",type="string",JSONPath=".spec.thin.poolName",description="Selected thin pool in LVMVolumeGroup. Might be empty if the LVMVolumeGroup is thick."
// +kubebuilder:printcolumn:name="Size",type="string",JSONPath=".status.actualSize",description="Actual LVMLogicalVolume size."
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp",description="Age of this resource."
// +crd-enricher:crd:minimal=true
// +crd-enricher:crd:preserveUnknownFields=false
type LVMLogicalVolume struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec LVMLogicalVolumeSpec `json:"spec"`
	// Describes the resource status.
	Status *LVMLogicalVolumeStatus `json:"status,omitempty"`
}

const (
	VolumeCleanupDiscard              = "Discard"
	VolumeCleanupRandomFillSinglePass = "RandomFillSinglePass"
	VolumeCleanupRandomFillThreePass  = "RandomFillThreePass"
)

// +k8s:deepcopy-gen=true
// +kubebuilder:validation:XValidation:rule="(self.type == \"Thin\" && has(self.thin) && !has(self.thick)) || self.type != \"Thin\"",message="Field 'thin' is required and field 'thick' is forbidden when 'type' is 'Thin'."
// +kubebuilder:validation:XValidation:rule="( (!has(self.thick) || !has(self.thick.contiguous)) && (!has(oldSelf.thick) || !has(oldSelf.thick.contiguous)) ) || ( has(self.thick) && has(self.thick.contiguous) && has(oldSelf.thick) && has(oldSelf.thick.contiguous) )",message="Field 'contiguous' is immutable and cannot be added if not specified at creation."
// +kubebuilder:validation:XValidation:rule="self.type == \"Thick\" || !has(self.volumeCleanup) || self.volumeCleanup != \"Discard\"",message="'volumeCleanup: Discard' is supported only for 'Thick' volumes"
type LVMLogicalVolumeSpec struct {
	// Logical Volume name on the node.
	// Besides the pattern, LVM does not accept names longer than 127 characters, starting with snapshot or pvmove, or containing a suffix it reserves for its internal volumes, such as _tdata or _tmeta.
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="Value is immutable."
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=127
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([a-z0-9-._]{0,251}[a-z0-9])?$`
	// +kubebuilder:validation:XValidation:rule="!self.startsWith('snapshot') && !self.startsWith('pvmove')",message="LVM reserves logical volume names that start with snapshot or pvmove."
	// +kubebuilder:validation:XValidation:rule="!self.matches('_(cdata|cmeta|corig|cpool|cvol|wcorig|mimage|mlog|rimage|rmeta|tdata|tmeta|vdata|imeta|iorig|pmspare|vorigin)')",message="LVM reserves logical volume names that contain _cdata, _cmeta, _corig, _cpool, _cvol, _wcorig, _mimage, _mlog, _rimage, _rmeta, _tdata, _tmeta, _vdata, _imeta, _iorig, _pmspare or _vorigin."
	ActualLVNameOnTheNode string `json:"actualLVNameOnTheNode"`
	// Logical Volume type.
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="Value is immutable."
	// +kubebuilder:validation:Enum=Thick;Thin
	Type string `json:"type"`
	// Desired Logical Volume size. It can be specified as a numerical value or percentage of the total Volume Group or thin pool space. For thin pools, the total space is calculated as the actual thin pool size multiplied by the `allocationLimit` value.
	//
	// > Note that if you specify a percentage, the Logical Volume will be automatically extended when Volume Group, thin pool, or thin pool's `allocationLimit` value is increased.
	// +kubebuilder:validation:XIntOrString
	// +kubebuilder:validation:Type=""
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`^[0-9]+(\.[0-9]+)?(E|P|T|G|M|k|Ei|Pi|Ti|Gi|Mi|Ki)?$|^[1-9][0-9]?%$|100%`
	Size string `json:"size"`
	// Name of a LVMVolumeGroup resource whose Volume Group will be used for Logical Volume creation.
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="Value is immutable."
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([a-z0-9-.]{0,251}[a-z0-9])?$`
	LVMVolumeGroupName string `json:"lvmVolumeGroupName"`
	// Source of the volume, if present.
	// +optional
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="Value is immutable."
	Source *LVMLogicalVolumeSource `json:"source"`
	// Defines the used thin pool. Specified only for Thin Logical Volume.
	// +optional
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="Value is immutable."
	Thin *LVMLogicalVolumeThinSpec `json:"thin"`
	// +optional
	// +kubebuilder:validation:XValidation:rule="(!has(oldSelf.contiguous) || has(self.contiguous))",message="Field 'contiguous' cannot be removed."
	Thick *LVMLogicalVolumeThickSpec `json:"thick"`
	// Volume cleanup method after the PV is deleted. If this parameter is not set, there's no guarantee that the data will be completely deleted.
	//
	// Allowed values:
	//
	// - `RandomFillSinglePass`: Volume will be overwritten with random data once before deletion. This option is not recommended for solid-state drives (SSDs), as overwriting reduces the drive's lifespan. For thin volumes, only the space in use at the moment of volume deletion will be overwritten.
	// - `RandomFillThreePass`: Volume will be overwritten with random data three times before deletion. This option is also not recommended for SSDs, as overwriting reduces the drive's lifespan. For thin volumes, only the space in use at the moment of volume deletion will be overwritten.
	// - `Discard`: Before deletion, all blocks of the volume will be marked as free using the `discard` system call. This option is applicable only to SSDs and thick volumes.
	// +kubebuilder:validation:Enum=RandomFillThreePass;RandomFillSinglePass;Discard
	VolumeCleanup *string `json:"volumeCleanup,omitempty"`
}

// +k8s:deepcopy-gen=true
type LVMLogicalVolumeThinSpec struct {
	// Name of the used thin pool.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`^.*$`
	PoolName string `json:"poolName"`
}

// +k8s:deepcopy-gen=true
type LVMLogicalVolumeThickSpec struct {
	// If set to `true`, the logical volume will be created with the `contiguous` attribute.
	//
	// > Use this attribute carefully, as it may prevent from Logical Volume creation even if there is enough space for it.
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="Value is immutable."
	Contiguous *bool `json:"contiguous,omitempty"`
}

// +k8s:deepcopy-gen=true
type LVMLogicalVolumeStatus struct {
	// Current resource's phase.
	// +optional
	// +kubebuilder:validation:Enum=Created;Pending;Cleaning;Resizing;Failed
	Phase string `json:"phase"`
	// Reason carries free-form detail about the current phase, including raw error
	// text. Machine-readable reasons live on Conditions.

	// Reason of the phase.
	// +optional
	Reason string `json:"reason"`
	// Actual Logical Volume size on the node.
	// +optional
	ActualSize resource.Quantity `json:"actualSize"`
	// Shows if the created Logical Volume has the `contiguous` attribute.
	// +optional
	Contiguous *bool `json:"contiguous"`
	// Name of the Kubernetes node the Logical Volume resides on.
	// +optional
	NodeName string `json:"nodeName,omitempty"`
	// Name of the Volume Group on the node the Logical Volume resides in.
	// +optional
	ActualVGNameOnTheNode string `json:"actualVGNameOnTheNode,omitempty"`
	// Phase is derived from Conditions.

	// The latest observations of the resource state. Condition type: `Ready`.
	// +optional
	// +listType=map
	// +listMapKey=type
	// +crd-enricher:unset:items.description
	// +crd-enricher:raw:items.properties.type.description="Condition type. `Ready` is `True` once the Logical Volume is in place, `False`\nwhile it is not, and `Unknown` before anything has observed it."
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
type LVMLogicalVolumeSource struct {
	// Kind of the source. LVMLogicalVolume kind means that the current resource is a clone. LVMLogicalVolumeSnapshot kind means that the current resource is a restored volume.
	// +kubebuilder:validation:Enum=LVMLogicalVolume;LVMLogicalVolumeSnapshot
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="Value is immutable."
	Kind string `json:"kind"`
	// Name of the resource from which the current resource is created.
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="Value is immutable."
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
}
