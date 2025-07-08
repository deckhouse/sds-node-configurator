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

// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type LVMLogicalVolume struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   LVMLogicalVolumeSpec    `json:"spec"`
	Status *LVMLogicalVolumeStatus `json:"status,omitempty"`
}

const (
	VolumeCleanupDiscard              = "Discard"
	VolumeCleanupRandomFillSinglePass = "RandomFillSinglePass"
	VolumeCleanupRandomFillThreePass  = "RandomFillThreePass"
)

// +k8s:deepcopy-gen=true
type LVMLogicalVolumeSpec struct {
	ActualLVNameOnTheNode string                     `json:"actualLVNameOnTheNode"`
	Type                  string                     `json:"type"`
	Size                  string                     `json:"size"`
	LVMVolumeGroupName    string                     `json:"lvmVolumeGroupName"`
	Source                *LVMLogicalVolumeSource    `json:"source"`
	Thin                  *LVMLogicalVolumeThinSpec  `json:"thin"`
	Thick                 *LVMLogicalVolumeThickSpec `json:"thick"`
	VolumeCleanup         *string                    `json:"volumeCleanup,omitempty"`
}

// +k8s:deepcopy-gen=true
type LVMLogicalVolumeThinSpec struct {
	PoolName string `json:"poolName"`
}

// +k8s:deepcopy-gen=true
type LVMLogicalVolumeThickSpec struct {
	Contiguous *bool `json:"contiguous,omitempty"`
}

// +k8s:deepcopy-gen=true
type LVMLogicalVolumeStatus struct {
	Phase      string            `json:"phase"`
	Reason     string            `json:"reason"`
	ActualSize resource.Quantity `json:"actualSize"`
	Contiguous *bool             `json:"contiguous"`
}

// +k8s:deepcopy-gen=true
type LVMLogicalVolumeSource struct {
	// Either LVMLogicalVolume or LVMLogicalVolumeSnapshot
	Kind string `json:"kind"`
	Name string `json:"name"`
}
