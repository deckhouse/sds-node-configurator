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
type ReplicatedStorageClassList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []ReplicatedStorageClass `json:"items"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type ReplicatedStorageClass struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ReplicatedStorageClassSpec   `json:"spec"`
	Status ReplicatedStorageClassStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen=true
type ReplicatedStorageClassSpec struct {
	// StoragePool is the name of the ReplicatedStoragePool resource
	StoragePool string `json:"storagePool"`
	// ReclaimPolicy defines what happens to the volume when the PVC is deleted (Delete or Retain)
	ReclaimPolicy string `json:"reclaimPolicy"`
	// Replication mode: None, Availability, ConsistencyAndAvailability
	Replication string `json:"replication,omitempty"`
	// VolumeAccess mode: Local, EventuallyLocal, PreferablyLocal, Any
	VolumeAccess string `json:"volumeAccess,omitempty"`
	// Topology mode: TransZonal, Zonal, Ignored
	Topology string `json:"topology"`
	// Zones is the list of zones where volumes should be replicated
	Zones []string `json:"zones,omitempty"`
}

// +k8s:deepcopy-gen=true
type ReplicatedStorageClassStatus struct {
	// Phase is the current state: Failed, Created
	Phase string `json:"phase,omitempty"`
	// Reason provides additional information about the current state
	Reason string `json:"reason,omitempty"`
}

// DeepCopyInto copies the receiver into out
func (in *ReplicatedStorageClass) DeepCopyInto(out *ReplicatedStorageClass) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	out.Status = in.Status
}

// DeepCopy creates a deep copy of ReplicatedStorageClass
func (in *ReplicatedStorageClass) DeepCopy() *ReplicatedStorageClass {
	if in == nil {
		return nil
	}
	out := new(ReplicatedStorageClass)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject creates a deep copy as runtime.Object
func (in *ReplicatedStorageClass) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto copies the receiver into out
func (in *ReplicatedStorageClassList) DeepCopyInto(out *ReplicatedStorageClassList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		in, out := &in.Items, &out.Items
		*out = make([]ReplicatedStorageClass, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

// DeepCopy creates a deep copy of ReplicatedStorageClassList
func (in *ReplicatedStorageClassList) DeepCopy() *ReplicatedStorageClassList {
	if in == nil {
		return nil
	}
	out := new(ReplicatedStorageClassList)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject creates a deep copy as runtime.Object
func (in *ReplicatedStorageClassList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto copies the receiver into out
func (in *ReplicatedStorageClassSpec) DeepCopyInto(out *ReplicatedStorageClassSpec) {
	*out = *in
	if in.Zones != nil {
		in, out := &in.Zones, &out.Zones
		*out = make([]string, len(*in))
		copy(*out, *in)
	}
}

// DeepCopy creates a deep copy of ReplicatedStorageClassSpec
func (in *ReplicatedStorageClassSpec) DeepCopy() *ReplicatedStorageClassSpec {
	if in == nil {
		return nil
	}
	out := new(ReplicatedStorageClassSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto copies the receiver into out
func (in *ReplicatedStorageClassStatus) DeepCopyInto(out *ReplicatedStorageClassStatus) {
	*out = *in
}

// DeepCopy creates a deep copy of ReplicatedStorageClassStatus
func (in *ReplicatedStorageClassStatus) DeepCopy() *ReplicatedStorageClassStatus {
	if in == nil {
		return nil
	}
	out := new(ReplicatedStorageClassStatus)
	in.DeepCopyInto(out)
	return out
}

