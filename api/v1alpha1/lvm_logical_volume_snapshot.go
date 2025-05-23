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

// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type LVMLogicalVolumeSnapshot struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   LVMLogicalVolumeSnapshotSpec    `json:"spec"`
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
	ActualSnapshotNameOnTheNode string `json:"actualSnapshotNameOnTheNode"`
	LVMLogicalVolumeName        string `json:"lvmLogicalVolumeName"`
}

// +k8s:deepcopy-gen=true
type LVMLogicalVolumeSnapshotStatus struct {
	NodeName              string            `json:"nodeName"`
	ActualVGNameOnTheNode string            `json:"actualVGNameOnTheNode"`
	ActualLVNameOnTheNode string            `json:"actualLVNameOnTheNode"`
	Phase                 string            `json:"phase"`
	Reason                string            `json:"reason"`
	Size                  resource.Quantity `json:"size"`
	UsedSize              resource.Quantity `json:"usedSize"`
}
