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

//go:generate go run sigs.k8s.io/controller-tools/cmd/controller-gen paths=$GOFILE crd output:crd:dir=../../crds/filter

// +groupName=storage.deckhouse.io
// +kubebuilder:webhook:admissionReviewVersions=v1,failurePolicy=fail,groups=storage.deckhouse.io,mutating=false,name=blockdevicefilter.storage.deckhouse.io,path=/validate-bdf,resources=blockdevicefilter,sideEffects=None,verbs=create;update,versions=v1alpha1

// +kubebuilder:resource:scope=Cluster,shortName=bdf
// +kubebuilder:subresource:status
// +kubebuilder:object:root=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// The filters on BlockDevice list
//
// There is the way to hide node devices from the user
type BlockDeviceFilter struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              BlockDeviceFilterSpec `json:"spec"`
}

// Defines the state of block device selector
type BlockDeviceFilterSpec struct {
	// Selectors on block devices to keep.
	//
	// Block devices not matched all the selectors will be hidden
	BlockDeviceSelector *metav1.LabelSelector `json:"blockDeviceSelector"`
}
