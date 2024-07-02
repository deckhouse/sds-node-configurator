/*
Copyright 2024 Flant JSC

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

type LvmVolumeGroupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []LvmVolumeGroup `json:"items"`
}

type LvmVolumeGroup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   LvmVolumeGroupSpec   `json:"spec"`
	Status LvmVolumeGroupStatus `json:"status,omitempty"`
}

type LvmVolumeGroupSpec struct {
	ActualVGNameOnTheNode string                       `json:"actualVGNameOnTheNode"`
	BlockDeviceNames      []string                     `json:"blockDeviceNames"`
	ThinPools             []LvmVolumeGroupThinPoolSpec `json:"thinPools"`
	Type                  string                       `json:"type"`
}

type LvmVolumeGroupStatus struct {
	AllocatedSize        resource.Quantity              `json:"allocatedSize"`
	Nodes                []LvmVolumeGroupNode           `json:"nodes"`
	ThinPools            []LvmVolumeGroupThinPoolStatus `json:"thinPools"`
	VGSize               resource.Quantity              `json:"vgSize"`
	VGUuid               string                         `json:"vgUUID"`
	Phase                string                         `json:"phase"`
	Conditions           []metav1.Condition             `json:"conditions"`
	ThinPoolReady        string                         `json:"thinPoolReady"`
	ConfigurationApplied string                         `json:"configurationApplied"`
	VGFree               resource.Quantity              `json:"vgFree"`
}

type LvmVolumeGroupDevice struct {
	BlockDevice string            `json:"blockDevice"`
	DevSize     resource.Quantity `json:"devSize"`
	PVSize      resource.Quantity `json:"pvSize"`
	PVUuid      string            `json:"pvUUID"`
	Path        string            `json:"path"`
}

type LvmVolumeGroupNode struct {
	Devices []LvmVolumeGroupDevice `json:"devices"`
	Name    string                 `json:"name"`
}

type LvmVolumeGroupThinPoolStatus struct {
	Name            string            `json:"name"`
	ActualSize      resource.Quantity `json:"actualSize"`
	UsedSize        resource.Quantity `json:"usedSize"`
	AllocatedSize   resource.Quantity `json:"allocatedSize"`
	AvailableSpace  resource.Quantity `json:"availableSpace"`
	AllocationLimit string            `json:"allocationLimit"`
	Ready           bool              `json:"ready"`
	Message         string            `json:"message"`
}

type LvmVolumeGroupThinPoolSpec struct {
	Name            string            `json:"name"`
	Size            resource.Quantity `json:"size"`
	AllocationLimit string            `json:"allocationLimit"`
}
