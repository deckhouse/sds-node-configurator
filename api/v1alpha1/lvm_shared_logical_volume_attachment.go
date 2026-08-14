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
type LVMSharedLogicalVolumeAttachmentList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []LVMSharedLogicalVolumeAttachment `json:"items"`
}

// LVMSharedLogicalVolumeAttachment asks for one volume to be active on one
// node, and reports what happened. Deleting it is how the volume is released:
// the agent deactivates it and the exclusive lock is dropped.
//
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type LVMSharedLogicalVolumeAttachment struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   LVMSharedLogicalVolumeAttachmentSpec    `json:"spec"`
	Status *LVMSharedLogicalVolumeAttachmentStatus `json:"status,omitempty"`
}

const (
	// LVMSharedLogicalVolumeAccessModeRWO activates exclusively. LVM's refusal
	// to activate the same volume exclusively elsewhere is what keeps two nodes
	// from writing to it — not any bookkeeping in a controller.
	LVMSharedLogicalVolumeAccessModeRWO = "ReadWriteOnce"
	// LVMSharedLogicalVolumeAccessModeRWX activates in shared mode, for block
	// volumes whose consumer arbitrates access itself. An ordinary filesystem
	// under a shared activation is a corrupted filesystem.
	LVMSharedLogicalVolumeAccessModeRWX = "ReadWriteMany"
)

// +k8s:deepcopy-gen=true
type LVMSharedLogicalVolumeAttachmentSpec struct {
	LVMSharedLogicalVolumeName string `json:"lvmSharedLogicalVolumeName"`
	NodeName                   string `json:"nodeName"`
	AccessMode                 string `json:"accessMode,omitempty"`
}

// +k8s:deepcopy-gen=true
type LVMSharedLogicalVolumeAttachmentStatus struct {
	Phase              string `json:"phase,omitempty"`
	ObservedGeneration int64  `json:"observedGeneration,omitempty"`
	DevicePath         string `json:"devicePath,omitempty"`

	// ObservedSize is published here as well as on the volume because growing
	// an attached volume is done by this node — the metadata owner cannot, the
	// lock is held here — and the consumer needs to know the new size arrived.
	ObservedSize string             `json:"observedSize,omitempty"`
	Conditions   []metav1.Condition `json:"conditions,omitempty"`
}
