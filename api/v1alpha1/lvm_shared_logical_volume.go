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
type LVMSharedLogicalVolumeList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []LVMSharedLogicalVolume `json:"items"`
}

// LVMSharedLogicalVolume describes a volume of a shared Volume Group, not where
// it is in use: metadata operations belong to the group's metadata owner and
// activation belongs to the attached node, which is why attachment is a
// separate resource.
//
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type LVMSharedLogicalVolume struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   LVMSharedLogicalVolumeSpec    `json:"spec"`
	Status *LVMSharedLogicalVolumeStatus `json:"status,omitempty"`
}

// LVMSharedLogicalVolumeTypeThick is the only type there is: under lvmlockd a
// thin pool cannot be activated in shared mode, so every volume of one would be
// pinned to a single node — the property a shared group exists to avoid.
const LVMSharedLogicalVolumeTypeThick = "Thick"

// +k8s:deepcopy-gen=true
type LVMSharedLogicalVolumeSpec struct {
	LVMSharedVolumeGroupName string `json:"lvmSharedVolumeGroupName"`
	ActualLVNameOnTheNode    string `json:"actualLVNameOnTheNode"`
	Type                     string `json:"type,omitempty"`
	Size                     string `json:"size"`

	// VolumeCleanup is the effective policy, coming from the storage class. The
	// group's field is the floor, and a weaker policy here is rejected.
	VolumeCleanup string `json:"volumeCleanup"`
}

// +k8s:deepcopy-gen=true
type LVMSharedLogicalVolumeStatus struct {
	Phase              string             `json:"phase,omitempty"`
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	ActualSize         string             `json:"actualSize,omitempty"`
	LVUUID             string             `json:"lvUUID,omitempty"`
	Contiguous         bool               `json:"contiguous,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
}
