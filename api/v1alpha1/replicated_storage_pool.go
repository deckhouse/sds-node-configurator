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
	"k8s.io/apimachinery/pkg/runtime"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type ReplicatedStoragePoolList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []ReplicatedStoragePool `json:"items"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type ReplicatedStoragePool struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ReplicatedStoragePoolSpec   `json:"spec"`
	Status ReplicatedStoragePoolStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen=true
type ReplicatedStoragePoolSpec struct {
	// Type defines the volumes type: LVM (for Thick) or LVMThin (for Thin)
	Type string `json:"type"`
	// LvmVolumeGroups is the list of LVMVolumeGroup resources used for storage
	LvmVolumeGroups []ReplicatedStoragePoolLVG `json:"lvmVolumeGroups"`
}

// +k8s:deepcopy-gen=true
type ReplicatedStoragePoolLVG struct {
	// Name is the LVMVolumeGroup resource name
	Name string `json:"name"`
	// ThinPoolName is the thin pool name (required for LVMThin type)
	ThinPoolName string `json:"thinPoolName,omitempty"`
}

// +k8s:deepcopy-gen=true
type ReplicatedStoragePoolStatus struct {
	// Phase is the current state: Updating, Failed, Completed
	Phase string `json:"phase,omitempty"`
	// Reason provides additional information about the current state
	Reason string `json:"reason,omitempty"`
}

// DeepCopyInto copies the receiver into out
func (in *ReplicatedStoragePool) DeepCopyInto(out *ReplicatedStoragePool) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	out.Status = in.Status
}

// DeepCopy creates a deep copy of ReplicatedStoragePool
func (in *ReplicatedStoragePool) DeepCopy() *ReplicatedStoragePool {
	if in == nil {
		return nil
	}
	out := new(ReplicatedStoragePool)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject creates a deep copy as runtime.Object
func (in *ReplicatedStoragePool) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto copies the receiver into out
func (in *ReplicatedStoragePoolList) DeepCopyInto(out *ReplicatedStoragePoolList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		in, out := &in.Items, &out.Items
		*out = make([]ReplicatedStoragePool, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

// DeepCopy creates a deep copy of ReplicatedStoragePoolList
func (in *ReplicatedStoragePoolList) DeepCopy() *ReplicatedStoragePoolList {
	if in == nil {
		return nil
	}
	out := new(ReplicatedStoragePoolList)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject creates a deep copy as runtime.Object
func (in *ReplicatedStoragePoolList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto copies the receiver into out
func (in *ReplicatedStoragePoolSpec) DeepCopyInto(out *ReplicatedStoragePoolSpec) {
	*out = *in
	if in.LvmVolumeGroups != nil {
		in, out := &in.LvmVolumeGroups, &out.LvmVolumeGroups
		*out = make([]ReplicatedStoragePoolLVG, len(*in))
		copy(*out, *in)
	}
}

// DeepCopy creates a deep copy of ReplicatedStoragePoolSpec
func (in *ReplicatedStoragePoolSpec) DeepCopy() *ReplicatedStoragePoolSpec {
	if in == nil {
		return nil
	}
	out := new(ReplicatedStoragePoolSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto copies the receiver into out
func (in *ReplicatedStoragePoolLVG) DeepCopyInto(out *ReplicatedStoragePoolLVG) {
	*out = *in
}

// DeepCopy creates a deep copy of ReplicatedStoragePoolLVG
func (in *ReplicatedStoragePoolLVG) DeepCopy() *ReplicatedStoragePoolLVG {
	if in == nil {
		return nil
	}
	out := new(ReplicatedStoragePoolLVG)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto copies the receiver into out
func (in *ReplicatedStoragePoolStatus) DeepCopyInto(out *ReplicatedStoragePoolStatus) {
	*out = *in
}

// DeepCopy creates a deep copy of ReplicatedStoragePoolStatus
func (in *ReplicatedStoragePoolStatus) DeepCopy() *ReplicatedStoragePoolStatus {
	if in == nil {
		return nil
	}
	out := new(ReplicatedStoragePoolStatus)
	in.DeepCopyInto(out)
	return out
}

