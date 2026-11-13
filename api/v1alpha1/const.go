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

	// NodeMissingSinceAnnotation is set by the controller on an LVMVolumeGroup
	// whose node is not in the cluster, to the time (RFC 3339) it first found the
	// node gone, and removed once the node is back. The release of the agent's
	// finalizer from the resources of that node counts its grace period from it,
	// so a restart of the controller does not start the period over.
	NodeMissingSinceAnnotation = "storage.deckhouse.io/node-missing-since"

	blockDeviceLabelPrefix                = "status.blockdevice.storage.deckhouse.io"
	BlockDeviceTypeLabelKey               = blockDeviceLabelPrefix + "/type"
	BlockDeviceFSTypeLabelKey             = blockDeviceLabelPrefix + "/fstype"
	BlockDevicePVUUIDLabelKey             = blockDeviceLabelPrefix + "/pvuuid"
	BlockDeviceVGUUIDLabelKey             = blockDeviceLabelPrefix + "/vguuid"
	BlockDevicePartUUIDLabelKey           = blockDeviceLabelPrefix + "/partuuid"
	BlockDevicePartLabelLabelKey          = blockDeviceLabelPrefix + "/partlabel"
	BlockDeviceLVMVolumeGroupNameLabelKey = blockDeviceLabelPrefix + "/lvmvolumegroupname"
	BlockDeviceActualVGNameLabelKey       = blockDeviceLabelPrefix + "/actualvgnameonthenode"
	BlockDeviceWWNLabelKey                = blockDeviceLabelPrefix + "/wwn"
	BlockDeviceSerialLabelKey             = blockDeviceLabelPrefix + "/serial"
	BlockDeviceSizeLabelKey               = blockDeviceLabelPrefix + "/size"
	BlockDeviceModelLabelKey              = blockDeviceLabelPrefix + "/model"
	BlockDeviceRotaLabelKey               = blockDeviceLabelPrefix + "/rota"
	BlockDeviceHotPlugLabelKey            = blockDeviceLabelPrefix + "/hotplug"
	BlockDeviceMachineIDLabelKey          = blockDeviceLabelPrefix + "/machineid"
	// BlockDeviceSCSITargetLabelKey names the SCSI target the device is reached
	// through; the value is built by SCSITargetLabelValue.
	BlockDeviceSCSITargetLabelKey = blockDeviceLabelPrefix + "/scsitarget"
)
