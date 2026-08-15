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

// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type LVMSharedVolumeGroupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []LVMSharedVolumeGroup `json:"items"`
}

// LVMSharedVolumeGroup is one Volume Group on storage that several nodes see at
// once. Unlike LVMVolumeGroup it is addressed by the WWIDs of its LUNs, because
// paths and map names differ from node to node and only the WWID is the same
// everywhere.
//
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type LVMSharedVolumeGroup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   LVMSharedVolumeGroupSpec    `json:"spec"`
	Status *LVMSharedVolumeGroupStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen=true
type LVMSharedVolumeGroupSpec struct {
	// ActualVGNameOnTheNode is limited to 44 characters: lvmlockd prefixes it
	// with "lvm_" and checks against 64, while sanlock truncates a lockspace
	// name at 48 without a word.
	ActualVGNameOnTheNode string `json:"actualVGNameOnTheNode"`

	// Devices are the LUNs of the group, by WWID. All of them must belong to
	// one geometry class, which LVM decides by "physical or logical block size
	// is 4096", so 512e and 4Kn are the same class and honest 512 is not.
	Devices []LVMSharedVolumeGroupDevice `json:"devices"`

	// Nodes are the members of the group: the agent on a listed node starts the
	// sanlock lockspace and may activate the group's volumes.
	Nodes []string `json:"nodes,omitempty"`

	// MetadataOwner is the single node performing metadata operations, and the
	// only writer of Status. It must be one of Nodes.
	MetadataOwner string `json:"metadataOwner,omitempty"`

	// VolumeCleanup is the floor for how a volume of this group is erased. It
	// is required: capacity carved out of a shared LUN is handed to the next
	// tenant, so "no policy" is not a defensible default.
	VolumeCleanup string `json:"volumeCleanup"`

	// LVM holds what vgcreate is given and what can never be changed after it.
	// The agent runs vgcreate, so the values have to reach it through here.
	LVM *LVMSharedVolumeGroupLVMSpec `json:"lvm,omitempty"`

	// SanlockLVExtend is how much the hidden lease volume grows by when it runs
	// out of slots. Empty means the LVM default.
	SanlockLVExtend string `json:"sanlockLVExtend,omitempty"`

	// PersistentReservations is the pool's intent, mirrored here because this is
	// the resource the node agents act on.
	//
	// `Required` means the group's LUNs are to be held under SCSI-3 persistent
	// reservations, so that a node can be denied access by the array rather than
	// by anything running on it. Reaching that state is an orchestration across
	// every member and a one-way door in the middle of it: after `vgchange
	// --setpersist require` the group answers "Persistent reservation is not
	// started" to everything until `vgchange --persist start` succeeds.
	// +kubebuilder:validation:Optional
	// +kubebuilder:validation:Enum=Disabled;Required
	// +kubebuilder:default=Disabled
	PersistentReservations string `json:"persistentReservations,omitempty"`
}

// +k8s:deepcopy-gen=true
type LVMSharedVolumeGroupLVMSpec struct {
	// PhysicalExtentSize must be a multiple of the unmap granularity of the
	// group's LUNs, or discarding a volume frees nothing however honestly the
	// array reports zeroes afterwards.
	PhysicalExtentSize string `json:"physicalExtentSize,omitempty"`

	// SanlockAlignSize decides the size of the lease area and the ceiling on
	// host_id: 250 hosts for 1Mi, 500 for 2Mi, 1000 for 4Mi, 2000 for 8Mi. LVM
	// does not enforce that ceiling itself, so the allocator does.
	SanlockAlignSize string `json:"sanlockAlignSize,omitempty"`

	MetadataSize string `json:"metadataSize,omitempty"`
}

// +k8s:deepcopy-gen=true
type LVMSharedVolumeGroupDevice struct {
	WWID string `json:"wwid"`
}

// +k8s:deepcopy-gen=true
type LVMSharedVolumeGroupStatus struct {
	Phase              string             `json:"phase,omitempty"`
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	VGUUID             string             `json:"vgUUID,omitempty"`
	VGSize             string             `json:"vgSize,omitempty"`
	VGFree             string             `json:"vgFree,omitempty"`
	ExtentSize         string             `json:"extentSize,omitempty"`
	LeaseAreaSize      string             `json:"leaseAreaSize,omitempty"`
	LogicalVolumeCount int32              `json:"logicalVolumeCount,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`

	// Nodes is what each member says about its own participation in the pool.
	//
	// It is deliberately not a condition. Conditions here are written by one
	// node — the metadata owner — and describe the group; whether a lockspace is
	// running is a fact about a NODE, and putting a per-node fact in a shared
	// array gives a value whose last writer wins and whose reader cannot tell
	// whose answer it got.
	Nodes []LVMSharedVolumeGroupNodeStatus `json:"nodes,omitempty"`
}

// LVMSharedVolumeGroupNodeStatus is one member's account of itself.
type LVMSharedVolumeGroupNodeStatus struct {
	// Name of the node this entry belongs to.
	Name string `json:"name"`

	// LockspaceStarted is true while the node holds a lockspace of this group
	// and can therefore activate its volumes.
	LockspaceStarted bool `json:"lockspaceStarted"`

	// Reason is a short, machine-readable cause of the current state.
	Reason string `json:"reason,omitempty"`

	// Message explains the state to a person, including what — if anything —
	// only a person can do about it.
	Message string `json:"message,omitempty"`

	// Since is when the node last changed what it says here. It does not move
	// while the answer stays the same, so a member re-confirming itself once a
	// minute costs nothing on the API server.
	Since metav1.Time `json:"since,omitempty"`

	// PersistentReservations is what this node can say about the SCSI
	// reservation channel of the pool's LUNs, checked without writing anything
	// to the array.
	//
	// It is reported before anything is switched on, and that is the point:
	// turning reservations on is a one-way door in the middle of its own
	// procedure — the volume group becomes unusable the moment `vgchange
	// --setpersist require` succeeds and stays that way until `--persist start`
	// does too. A pool whose nodes cannot say in advance that the channel works
	// must not be taken through it.
	PersistentReservations *NodePersistentReservations `json:"persistentReservations,omitempty"`
}

// NodePersistentReservations is a node's read-only verdict on the reservation
// channel: whether this node could take part in a pool that requires SCSI
// persistent reservations, and what is missing when it could not.
type NodePersistentReservations struct {
	// Ready is true when every precondition this node can check by reading is
	// met. It says nothing about whether reservations are in use — only that
	// switching them on would not fail here.
	Ready bool `json:"ready"`

	// Reason is a short, machine-readable cause when Ready is false.
	//
	// - `MultipathToolsTooNew`: the version in the lock daemons' image returns a
	//   wrong exit code for `reserve`, and lvmpersist undoes its own
	//   registration in response — which takes the reservation with it.
	// - `NoReservationKey`: the pool's multipath maps have no `reservation_key`,
	//   so every reservation command is refused before it reaches the array.
	// - `ChannelUnreachable`: the reservation channel could not be read at all
	//   from this node, which is what a container without hostNetwork looks like.
	Reason string `json:"reason,omitempty"`

	// Message explains it, including what only a person can do about it.
	// +kubebuilder:validation:MaxLength=4096
	Message string `json:"message,omitempty"`

	// State is where this node stands in the switch to reservations, and it is
	// how the members coordinate: the executor waits for every neighbour to
	// report `Stopped` before it opens the one-way door, and the neighbours wait
	// for `Enabled` before they come back.
	//
	// - `Off`: reservations are not in use here.
	// - `Stopped`: this node has stopped its lockspace and its reservations so
	//   the executor can take the group over. It serves no volumes in this state.
	// - `Enabled`: this node holds a registration with the array and its
	//   lockspace is running under reservations.
	// +kubebuilder:validation:Optional
	// +kubebuilder:validation:Enum=Off;Stopped;Enabled
	State string `json:"state,omitempty"`
}
