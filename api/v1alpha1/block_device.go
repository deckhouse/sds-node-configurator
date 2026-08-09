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

	// Conditions hold the latest state. Known type: Consumable.
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

// BlockDeviceConditionConsumable reports whether the agent may use the device
// to create an LVM Physical Volume.
//
// It is not Ready, and deliberately so. Ready would be a verdict about
// nothing: this kind is a report of what the agent found on the node, not a
// resource reconciled towards a desired state. Worse, it would be misleading —
// a device already serving as a Physical Volume carries fsType LVM2_member and
// is therefore not consumable, so Ready=False would be the normal state of
// every disk in a working cluster and an alert on it would never stop firing.
//
// What status.consumable could not say is why. The three answers are
// distinguishable and actionable in different ways, and they are what the
// reason carries.
const BlockDeviceConditionConsumable = "Consumable"

// The reasons BlockDeviceConditionConsumable is published with. They are this
// module's own: the shared vocabulary in sds-common-lib describes the progress
// of a reconcile, and none of these is about one.
const (
	// ReasonDeviceAvailable is set on Consumable=True.
	ReasonDeviceAvailable = "Available"
	// ReasonDeviceMounted is set on Consumable=False when the device is
	// mounted. Unmounting it makes the device available.
	ReasonDeviceMounted = "Mounted"
	// ReasonDeviceHasFilesystem is set on Consumable=False when the device
	// carries a filesystem. This covers a device already in use as a Physical
	// Volume (fsType LVM2_member), which is the common case and not a problem.
	ReasonDeviceHasFilesystem = "HasFilesystem"
	// ReasonDeviceHotPlugged is set on Consumable=False for a hot-plugged
	// device. The agent refuses those regardless of their contents, since a
	// Volume Group on removable media loses its Physical Volume without notice.
	ReasonDeviceHotPlugged = "HotPlugged"
)

// BlockDeviceConditionTypes is every condition type a BlockDevice publishes.
// See LVGConditionTypes in the controller for why the set is written down.
var BlockDeviceConditionTypes = []string{
	BlockDeviceConditionConsumable,
}
