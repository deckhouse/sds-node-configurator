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
// BlockDevice empty block device
type BlockDevice struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

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
	Type                  string            `json:"type"`
	FsType                string            `json:"fsType"`
	NodeName              string            `json:"nodeName"`
	Consumable            bool              `json:"consumable"`
	PVUuid                string            `json:"pvUUID"`
	VGUuid                string            `json:"vgUUID"`
	PartUUID              string            `json:"partUUID"`
	LVMVolumeGroupName    string            `json:"lvmVolumeGroupName"`
	ActualVGNameOnTheNode string            `json:"actualVGNameOnTheNode"`
	Wwn                   string            `json:"wwn"`
	Serial                string            `json:"serial"`
	Path                  string            `json:"path"`
	Size                  resource.Quantity `json:"size"`
	Model                 string            `json:"model"`
	Rota                  bool              `json:"rota"`
	HotPlug               bool              `json:"hotPlug"`
	MachineID             string            `json:"machineId"`
}
