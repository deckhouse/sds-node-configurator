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
type LVMLogicalVolumeSnapshotList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []LVMLogicalVolumeSnapshot `json:"items"`
}

// LVMLogicalVolumeSnapshot resource defines the storage where a VolumeSnapshot will be created.
//
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=llvs
// +kubebuilder:subresource:status
// +kubebuilder:metadata:labels={"heritage=deckhouse","module=sds-node-configurator","backup.deckhouse.io/cluster-config=true"}
// +kubebuilder:printcolumn:name="Snapshotted LLV Name",type="string",JSONPath=".spec.lvmLogicalVolumeName",description="Snapshotted LLV name"
// +kubebuilder:printcolumn:name="NodeName",type="string",JSONPath=".status.nodeName",description="Name of the Kubernetes node the snapshot resides in."
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase",description="Current resource status."
// +kubebuilder:printcolumn:name="Ready",type="string",JSONPath=".status.conditions[?(@.type==\"Ready\")].status"
// +kubebuilder:printcolumn:name="Size",type="string",JSONPath=".status.size",description="Snapshotted Logical Volume size."
// +kubebuilder:printcolumn:name="UsedSize",type="string",JSONPath=".status.usedSize",description="Space the snapshot maps when it is created, shared blocks included."
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp",description="Age of this resource."
// +crd-enricher:crd:minimal=true
// +crd-enricher:crd:preserveUnknownFields=false
type LVMLogicalVolumeSnapshot struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec LVMLogicalVolumeSnapshotSpec `json:"spec"`
	// Describes the resource status.
	Status *LVMLogicalVolumeSnapshotStatus `json:"status,omitempty"`
}

func (llvs *LVMLogicalVolumeSnapshot) ActualSnapshotNameOnTheNode() string {
	if llvs.Spec.ActualSnapshotNameOnTheNode != "" {
		return llvs.Spec.ActualSnapshotNameOnTheNode
	}
	return llvs.Name
}

// +k8s:deepcopy-gen=true
type LVMLogicalVolumeSnapshotSpec struct {
	// Name of the volume snapshot on the node.
	// Optional: if omitted or empty, the agent uses the LLVS object name
	// (`metadata.name`). Once set to a non-empty value, the field is immutable.
	// The name the agent uses, this field or `metadata.name`, must be a valid LVM logical volume name: up to 127 characters from a-z, A-Z, 0-9, +, _, . and -, not starting with a hyphen or with snapshot or pvmove, not `.` or `..`, and not containing a suffix LVM reserves for its internal volumes, such as _tdata or _tmeta.
	// +optional
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="Value is immutable."
	// +kubebuilder:validation:MaxLength=127
	// +kubebuilder:validation:XValidation:rule="self == '' || (self.matches('^[a-zA-Z0-9+_.-]+$'))",message="A logical volume name may contain only the characters a-z, A-Z, 0-9, +, _, . and -."
	// +kubebuilder:validation:XValidation:rule="self == '' || (!self.startsWith('-'))",message="A logical volume name must not start with a hyphen."
	// +kubebuilder:validation:XValidation:rule="self == '' || (self != '.' && self != '..')",message="A logical volume name must not be '.' or '..'."
	// +kubebuilder:validation:XValidation:rule="self == '' || (!self.startsWith('snapshot') && !self.startsWith('pvmove'))",message="LVM reserves logical volume names that start with snapshot or pvmove."
	// +kubebuilder:validation:XValidation:rule="self == '' || (!self.matches('_(cdata|cmeta|corig|cpool|cvol|wcorig|mimage|mlog|rimage|rmeta|tdata|tmeta|vdata|imeta|iorig|pmspare|vorigin)'))",message="LVM reserves logical volume names that contain _cdata, _cmeta, _corig, _cpool, _cvol, _wcorig, _mimage, _mlog, _rimage, _rmeta, _tdata, _tmeta, _vdata, _imeta, _iorig, _pmspare or _vorigin."
	ActualSnapshotNameOnTheNode string `json:"actualSnapshotNameOnTheNode,omitempty"`
	// Name of the snapshotted LLV resource.
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="Value is immutable."
	// +kubebuilder:validation:MinLength=1
	LVMLogicalVolumeName string `json:"lvmLogicalVolumeName"`
}

// +k8s:deepcopy-gen=true
type LVMLogicalVolumeSnapshotStatus struct {
	// Name of the Kubernetes node the snapshot resides in.
	// +optional
	NodeName string `json:"nodeName"`
	// Name of the Volume Group, where the snapshot resides in.
	// +optional
	ActualVGNameOnTheNode string `json:"actualVGNameOnTheNode"`
	// Name of the volume the snapshot is created from.
	// +optional
	ActualLVNameOnTheNode string `json:"actualLVNameOnTheNode"`
	// Current resource's phase.
	//
	// Possible values:
	//
	// - `Created`: Snapshot was created successfully.
	// - `Pending`: Snapshot is being created.
	// - `Failed`: An error occurred while creating the snapshot.
	// +optional
	// +kubebuilder:validation:Enum=Created;Pending;Failed
	Phase string `json:"phase"`
	// Reason carries free-form detail about the current phase, including raw error
	// text. Machine-readable reasons live on Conditions.

	// Reason of the phase.
	// +optional
	Reason string `json:"reason"`
	// Snapshotted Logical Volume size on the node.
	// +optional
	Size resource.Quantity `json:"size"`
	// Space the snapshot Logical Volume maps on the node when the snapshot is created: its
	// Data% times its size. The snapshot is thin, so this counts the blocks it shares with
	// the origin as well, and it does not grow as the origin is written to. It is not the
	// space that deleting the snapshot would free.
	// +optional
	UsedSize resource.Quantity `json:"usedSize"`
	// The latest observations of the resource state. Condition type: `Ready`.
	// +optional
	// +listType=map
	// +listMapKey=type
	// +crd-enricher:unset:items.description
	// +crd-enricher:raw:items.properties.type.description="Condition type. `Ready` is `True` once the snapshot is in place, `False`\nwhile it is not, and `Unknown` before anything has observed it."
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
