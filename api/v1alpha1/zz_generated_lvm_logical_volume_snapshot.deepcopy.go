//go:build !ignore_autogenerated
// +build !ignore_autogenerated

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
// Code generated by deepcopy-gen. DO NOT EDIT.

package v1alpha1

import (
	runtime "k8s.io/apimachinery/pkg/runtime"
)

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *LVMLogicalVolumeSnapshot) DeepCopyInto(out *LVMLogicalVolumeSnapshot) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	out.Spec = in.Spec
	if in.Status != nil {
		in, out := &in.Status, &out.Status
		*out = new(LVMLogicalVolumeSnapshotStatus)
		(*in).DeepCopyInto(*out)
	}
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new LVMLogicalVolumeSnapshot.
func (in *LVMLogicalVolumeSnapshot) DeepCopy() *LVMLogicalVolumeSnapshot {
	if in == nil {
		return nil
	}
	out := new(LVMLogicalVolumeSnapshot)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject is an autogenerated deepcopy function, copying the receiver, creating a new runtime.Object.
func (in *LVMLogicalVolumeSnapshot) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *LVMLogicalVolumeSnapshotList) DeepCopyInto(out *LVMLogicalVolumeSnapshotList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		in, out := &in.Items, &out.Items
		*out = make([]LVMLogicalVolumeSnapshot, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new LVMLogicalVolumeSnapshotList.
func (in *LVMLogicalVolumeSnapshotList) DeepCopy() *LVMLogicalVolumeSnapshotList {
	if in == nil {
		return nil
	}
	out := new(LVMLogicalVolumeSnapshotList)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject is an autogenerated deepcopy function, copying the receiver, creating a new runtime.Object.
func (in *LVMLogicalVolumeSnapshotList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *LVMLogicalVolumeSnapshotSpec) DeepCopyInto(out *LVMLogicalVolumeSnapshotSpec) {
	*out = *in
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new LVMLogicalVolumeSnapshotSpec.
func (in *LVMLogicalVolumeSnapshotSpec) DeepCopy() *LVMLogicalVolumeSnapshotSpec {
	if in == nil {
		return nil
	}
	out := new(LVMLogicalVolumeSnapshotSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *LVMLogicalVolumeSnapshotStatus) DeepCopyInto(out *LVMLogicalVolumeSnapshotStatus) {
	*out = *in
	out.Size = in.Size.DeepCopy()
	out.ActualSize = in.ActualSize.DeepCopy()
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new LVMLogicalVolumeSnapshotStatus.
func (in *LVMLogicalVolumeSnapshotStatus) DeepCopy() *LVMLogicalVolumeSnapshotStatus {
	if in == nil {
		return nil
	}
	out := new(LVMLogicalVolumeSnapshotStatus)
	in.DeepCopyInto(out)
	return out
}
