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

const (
	PhaseCreated     = "Created"
	PhasePending     = "Pending"
	PhaseCleaning    = "Cleaning"
	PhaseResizing    = "Resizing"
	PhaseFailed      = "Failed"
	PhaseNotReady    = "NotReady"
	PhaseReady       = "Ready"
	PhaseTerminating = "Terminating"

	LLVSNameTag = "storage.deckhouse.io/lvmLogicalVolumeSnapshotName"

	blockDeviceLabelPrefix                = "status.blockdevice.storage.deckhouse.io"
	BlockDeviceTypeLabelKey               = blockDeviceLabelPrefix + "/type"
	BlockDeviceFSTypeLabelKey             = blockDeviceLabelPrefix + "/fstype"
	BlockDevicePVUUIDLabelKey             = blockDeviceLabelPrefix + "/pvuuid"
	BlockDeviceVGUUIDLabelKey             = blockDeviceLabelPrefix + "/vguuid"
	BlockDevicePartUUIDLabelKey           = blockDeviceLabelPrefix + "/partuuid"
	BlockDeviceLVMVolumeGroupNameLabelKey = blockDeviceLabelPrefix + "/lvmvolumegroupname"
	BlockDeviceActualVGNameLabelKey       = blockDeviceLabelPrefix + "/actualvgnameonthenode"
	BlockDeviceWWNLabelKey                = blockDeviceLabelPrefix + "/wwn"
	BlockDeviceSerialLabelKey             = blockDeviceLabelPrefix + "/serial"
	BlockDeviceSizeLabelKey               = blockDeviceLabelPrefix + "/size"
	BlockDeviceModelLabelKey              = blockDeviceLabelPrefix + "/model"
	BlockDeviceRotaLabelKey               = blockDeviceLabelPrefix + "/rota"
	BlockDeviceHotPlugLabelKey            = blockDeviceLabelPrefix + "/hotplug"
	BlockDeviceMachineIDLabelKey          = blockDeviceLabelPrefix + "/machineid"

	// ReplicatedStorageClass VolumeAccess modes
	VolumeAccessLocal           = "Local"
	VolumeAccessEventuallyLocal = "EventuallyLocal"
	VolumeAccessPreferablyLocal = "PreferablyLocal"
	VolumeAccessAny             = "Any"

	// ReplicatedStorageClass Topology modes
	TopologyTransZonal = "TransZonal"
	TopologyZonal      = "Zonal"
	TopologyIgnored    = "Ignored"

	// ReplicatedStoragePool Types
	RSPTypeLVM     = "LVM"     // Thick volumes
	RSPTypeLVMThin = "LVMThin" // Thin volumes

	// Labels for replicated volumes
	LabelReplicatedNode = "storage.deckhouse.io/sds-replicated-volume-node"
)
