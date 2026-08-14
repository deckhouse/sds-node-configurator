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
type LVMSharedVolumeGroupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []LVMSharedVolumeGroup `json:"items"`
}

// LVMSharedVolumeGroup is one Volume Group on storage that several nodes see at
// once. Unlike LVMVolumeGroup it is addressed by the WWIDs of its LUNs, because
// paths and map names differ from node to node and only the WWID is the same
// everywhere.
//
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type LVMSharedVolumeGroup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   LVMSharedVolumeGroupSpec    `json:"spec"`
	Status *LVMSharedVolumeGroupStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen=true
type LVMSharedVolumeGroupSpec struct {
	// ActualVGNameOnTheNode is limited to 44 characters: lvmlockd prefixes it
	// with "lvm_" and checks against 64, while sanlock truncates a lockspace
	// name at 48 without a word.
	ActualVGNameOnTheNode string `json:"actualVGNameOnTheNode"`

	// Devices are the LUNs of the group, by WWID. All of them must belong to
	// one geometry class, which LVM decides by "physical or logical block size
	// is 4096", so 512e and 4Kn are the same class and honest 512 is not.
	Devices []LVMSharedVolumeGroupDevice `json:"devices"`

	// Nodes are the members of the group: the agent on a listed node starts the
	// sanlock lockspace and may activate the group's volumes.
	Nodes []string `json:"nodes,omitempty"`

	// MetadataOwner is the single node performing metadata operations, and the
	// only writer of Status. It must be one of Nodes.
	MetadataOwner string `json:"metadataOwner,omitempty"`

	// VolumeCleanup is the floor for how a volume of this group is erased. It
	// is required: capacity carved out of a shared LUN is handed to the next
	// tenant, so "no policy" is not a defensible default.
	VolumeCleanup string `json:"volumeCleanup"`

	// SanlockLVExtend is how much the hidden lease volume grows by when it runs
	// out of slots. Empty means the LVM default.
	SanlockLVExtend string `json:"sanlockLVExtend,omitempty"`
}

// +k8s:deepcopy-gen=true
type LVMSharedVolumeGroupDevice struct {
	WWID string `json:"wwid"`
}

// +k8s:deepcopy-gen=true
type LVMSharedVolumeGroupStatus struct {
	Phase              string             `json:"phase,omitempty"`
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	VGUUID             string             `json:"vgUUID,omitempty"`
	VGSize             string             `json:"vgSize,omitempty"`
	VGFree             string             `json:"vgFree,omitempty"`
	ExtentSize         string             `json:"extentSize,omitempty"`
	LeaseAreaSize      string             `json:"leaseAreaSize,omitempty"`
	LogicalVolumeCount int32              `json:"logicalVolumeCount,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
}
