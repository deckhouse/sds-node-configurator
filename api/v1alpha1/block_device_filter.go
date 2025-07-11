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

//go:generate go tool controller-gen paths=$GOFILE crd output:crd:dir=../../crds/filter

// +groupName=storage.deckhouse.io

// +kubebuilder:resource:scope=Cluster,shortName=bdf
// +kubebuilder:object:root=true
// +kubebuilder:metadata:labels={"module=sds-node-configurator","heritage=deckhouse","backup.deckhouse.io/cluster-config=true"}
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// The filters on BlockDevice list
//
// There is the way to hide node devices from the user
type BlockDeviceFilter struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	// +kubebuilder:validation:Required
	Spec BlockDeviceFilterSpec `json:"spec"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// BlockDeviceList contains a list of empty block device
type BlockDeviceFilterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`
	Items           []BlockDeviceFilter `json:"items"`
}

// +k8s:deepcopy-gen=true
// Defines the state of block device selector
type BlockDeviceFilterSpec struct {
	// +kubebuilder:validation:Required
	// +kubebuilder:pruning:PreserveUnknownFields
	// Selectors on block devices to keep.
	//
	// Block devices not matched all the selectors will be hidden
	BlockDeviceSelector *metav1.LabelSelector `json:"blockDeviceSelector"`
}
