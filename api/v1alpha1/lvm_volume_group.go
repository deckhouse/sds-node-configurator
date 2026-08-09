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
type LVMVolumeGroupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []LVMVolumeGroup `json:"items"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type LVMVolumeGroup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   LVMVolumeGroupSpec   `json:"spec"`
	Status LVMVolumeGroupStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen=true
type LVMVolumeGroupSpec struct {
	ActualVGNameOnTheNode string                         `json:"actualVGNameOnTheNode"`
	BlockDeviceSelector   *metav1.LabelSelector          `json:"blockDeviceSelector,omitempty"`
	ThinPools             []LVMVolumeGroupThinPoolSpec   `json:"thinPools"`
	Type                  string                         `json:"type"`
	Local                 LVMVolumeGroupLocalSpec        `json:"local"`
	FileDevices           []LVMVolumeGroupFileDeviceSpec `json:"fileDevices,omitempty"`
}

// +k8s:deepcopy-gen=true
type LVMVolumeGroupFileDeviceSpec struct {
	// Name identifies the entry within spec.fileDevices. It is the entry's
	// primary key (x-kubernetes-list-map-keys) and the only part of the
	// backing file's path the user controls, which is what keeps `size`
	// editable in principle and lets two entries of equal size share a
	// directory.
	Name      string            `json:"name"`
	Directory string            `json:"directory"`
	Size      resource.Quantity `json:"size"`
}

// +k8s:deepcopy-gen=true
type LVMVolumeGroupStatus struct {
	AllocatedSize        resource.Quantity              `json:"allocatedSize"`
	Nodes                []LVMVolumeGroupNode           `json:"nodes"`
	ThinPools            []LVMVolumeGroupThinPoolStatus `json:"thinPools"`
	VGSize               resource.Quantity              `json:"vgSize"`
	VGUuid               string                         `json:"vgUUID"`
	Phase string `json:"phase"`
	// ObservedGeneration is the value of metadata.generation the conditions
	// watcher last computed a verdict against. When it trails
	// metadata.generation the phase and the aggregate Ready below still describe
	// the previous spec.
	//
	// Only the conditions watcher sets it, because it is the only writer that
	// looks at the resource as a whole. The agent and the infrastructure watcher
	// each own individual stage conditions and stamp the generation on those.
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	Conditions         []metav1.Condition `json:"conditions"`
	ThinPoolReady        string                         `json:"thinPoolReady"`
	ConfigurationApplied string                         `json:"configurationApplied"`
	VGFree               resource.Quantity              `json:"vgFree"`
	ExtentSize           resource.Quantity              `json:"extentSize"`
}

// +k8s:deepcopy-gen=true
type LVMVolumeGroupDevice struct {
	BlockDevice string            `json:"blockDevice"`
	DevSize     resource.Quantity `json:"devSize"`
	PVSize      resource.Quantity `json:"pvSize"`
	PVUuid      string            `json:"pvUUID"`
	Path        string            `json:"path"`
}

// +k8s:deepcopy-gen=true
type LVMVolumeGroupNode struct {
	Devices     []LVMVolumeGroupDevice     `json:"devices"`
	FileDevices []LVMVolumeGroupFileDevice `json:"fileDevices,omitempty"`
	Name        string                     `json:"name"`
}

// +k8s:deepcopy-gen=true
type LVMVolumeGroupFileDevice struct {
	// Name is the spec.fileDevices entry this device was provisioned from.
	// It is what lets the reconciler tell "this entry was never provisioned"
	// (safe to drop from the spec) from "this entry backs a live PV"
	// (dropping it needs pvmove/vgreduce, which the module does not do).
	Name       string            `json:"name"`
	FilePath   string            `json:"filePath"`
	LoopDevice string            `json:"loopDevice"`
	Size       resource.Quantity `json:"size"`
	PVUuid     string            `json:"pvUUID"`
}

// +k8s:deepcopy-gen=true
type LVMVolumeGroupThinPoolStatus struct {
	Name            string            `json:"name"`
	ActualSize      resource.Quantity `json:"actualSize"`
	UsedSize        resource.Quantity `json:"usedSize"`
	AllocatedSize   resource.Quantity `json:"allocatedSize"`
	AvailableSpace  resource.Quantity `json:"availableSpace"`
	AllocationLimit string            `json:"allocationLimit"`
	Ready           bool              `json:"ready"`
	Message         string            `json:"message"`
}

// +k8s:deepcopy-gen=true
type LVMVolumeGroupThinPoolSpec struct {
	Name            string `json:"name"`
	Size            string `json:"size"`
	AllocationLimit string `json:"allocationLimit"`
}

// +k8s:deepcopy-gen=true
type LVMVolumeGroupLocalSpec struct {
	NodeName string `json:"nodeName"`
}
