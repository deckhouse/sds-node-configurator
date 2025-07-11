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

package internal

import (
	"maps"
	"reflect"
	"strconv"

	"github.com/gosimple/slug"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
)

type BlockDeviceCandidate struct {
	NodeName              string
	Consumable            bool
	PVUuid                string
	VGUuid                string
	LVMVolumeGroupName    string
	ActualVGNameOnTheNode string
	Wwn                   string
	Serial                string
	Path                  string
	Size                  resource.Quantity
	Rota                  bool
	Model                 string
	Name                  string
	HotPlug               bool
	KName                 string
	PkName                string
	Type                  string
	FSType                string
	MachineID             string
	PartUUID              string

	SerialInherited string
	WWNInherited    string
}

func isConsumable(device *Device) bool {
	if device.MountPoint != "" {
		return false
	}

	if device.FSType != "" {
		return false
	}

	if device.HotPlug {
		return false
	}

	return true
}

func NewBlockDeviceCandidateByDevice(device *Device, nodeName string, machineID string) BlockDeviceCandidate {
	return BlockDeviceCandidate{
		NodeName:        nodeName,
		Consumable:      isConsumable(device),
		Wwn:             device.Wwn,
		Serial:          device.Serial,
		Path:            device.Name,
		Size:            device.Size,
		Rota:            device.Rota,
		Model:           device.Model,
		HotPlug:         device.HotPlug,
		KName:           device.KName,
		PkName:          device.PkName,
		Type:            device.Type,
		FSType:          device.FSType,
		MachineID:       machineID,
		PartUUID:        device.PartUUID,
		SerialInherited: device.SerialInherited,
		WWNInherited:    device.WWNInherited,
	}
}

func (candidate *BlockDeviceCandidate) asAPIBlockDeviceStatus() v1alpha1.BlockDeviceStatus {
	return v1alpha1.BlockDeviceStatus{
		Type:                  candidate.Type,
		FsType:                candidate.FSType,
		NodeName:              candidate.NodeName,
		Consumable:            candidate.Consumable,
		PVUuid:                candidate.PVUuid,
		VGUuid:                candidate.VGUuid,
		PartUUID:              candidate.PartUUID,
		LVMVolumeGroupName:    candidate.LVMVolumeGroupName,
		ActualVGNameOnTheNode: candidate.ActualVGNameOnTheNode,
		Wwn:                   candidate.GetWWN(),
		Serial:                candidate.GetSerial(),
		Path:                  candidate.Path,
		Size:                  *resource.NewQuantity(candidate.Size.Value(), resource.BinarySI),
		Model:                 candidate.Model,
		Rota:                  candidate.Rota,
		MachineID:             candidate.MachineID,
	}
}

func (candidate BlockDeviceCandidate) AsAPIBlockDevice() v1alpha1.BlockDevice {
	blockDevice := v1alpha1.BlockDevice{
		ObjectMeta: metav1.ObjectMeta{
			Name: candidate.Name,
		},
		Status: candidate.asAPIBlockDeviceStatus(),
	}

	blockDevice.Labels = newBlockDeviceLabels(&blockDevice)

	return blockDevice
}

func (candidate *BlockDeviceCandidate) UpdateAPIBlockDevice(blockDevice *v1alpha1.BlockDevice) {
	blockDevice.Status = candidate.asAPIBlockDeviceStatus()
	blockDevice.Labels = newBlockDeviceLabels(blockDevice)
}

func (candidate *BlockDeviceCandidate) HasBlockDeviceDiff(blockDevice v1alpha1.BlockDevice) bool {
	return candidate.NodeName != blockDevice.Status.NodeName ||
		candidate.Consumable != blockDevice.Status.Consumable ||
		candidate.PVUuid != blockDevice.Status.PVUuid ||
		candidate.VGUuid != blockDevice.Status.VGUuid ||
		candidate.PartUUID != blockDevice.Status.PartUUID ||
		candidate.LVMVolumeGroupName != blockDevice.Status.LVMVolumeGroupName ||
		candidate.ActualVGNameOnTheNode != blockDevice.Status.ActualVGNameOnTheNode ||
		candidate.GetWWN() != blockDevice.Status.Wwn ||
		candidate.GetSerial() != blockDevice.Status.Serial ||
		candidate.Path != blockDevice.Status.Path ||
		candidate.Size.Value() != blockDevice.Status.Size.Value() ||
		candidate.Rota != blockDevice.Status.Rota ||
		candidate.Model != blockDevice.Status.Model ||
		candidate.HotPlug != blockDevice.Status.HotPlug ||
		candidate.Type != blockDevice.Status.Type ||
		candidate.FSType != blockDevice.Status.FsType ||
		candidate.MachineID != blockDevice.Status.MachineID ||
		!reflect.DeepEqual(newBlockDeviceLabels(&blockDevice), blockDevice.Labels)
}

func (candidate *BlockDeviceCandidate) GetSerial() string {
	if candidate.Serial != "" {
		return candidate.Serial
	}
	return candidate.SerialInherited
}

func (candidate *BlockDeviceCandidate) GetWWN() string {
	if candidate.Wwn != "" {
		return candidate.Wwn
	}
	return candidate.WWNInherited
}

// Creates new labels as map, keeping unrelated labels device already has
func newBlockDeviceLabels(blockDevice *v1alpha1.BlockDevice) map[string]string {
	resultItemCount := 16
	if blockDevice.Labels != nil {
		resultItemCount = len(blockDevice.Labels)
	}

	result := make(map[string]string, resultItemCount)

	maps.Copy(result, blockDevice.Labels)

	slug.Lowercase = false
	slug.MaxLength = 63
	slug.EnableSmartTruncate = false
	maps.Copy(result, map[string]string{
		MetadataNameLabelKey:                           slug.Make(blockDevice.ObjectMeta.Name),
		HostNameLabelKey:                               slug.Make(blockDevice.Status.NodeName),
		v1alpha1.BlockDeviceTypeLabelKey:               slug.Make(blockDevice.Status.Type),
		v1alpha1.BlockDeviceFSTypeLabelKey:             slug.Make(blockDevice.Status.FsType),
		v1alpha1.BlockDevicePVUUIDLabelKey:             blockDevice.Status.PVUuid,
		v1alpha1.BlockDeviceVGUUIDLabelKey:             blockDevice.Status.VGUuid,
		v1alpha1.BlockDevicePartUUIDLabelKey:           blockDevice.Status.PartUUID,
		v1alpha1.BlockDeviceLVMVolumeGroupNameLabelKey: slug.Make(blockDevice.Status.LVMVolumeGroupName),
		v1alpha1.BlockDeviceActualVGNameLabelKey:       slug.Make(blockDevice.Status.ActualVGNameOnTheNode),
		v1alpha1.BlockDeviceWWNLabelKey:                slug.Make(blockDevice.Status.Wwn),
		v1alpha1.BlockDeviceSerialLabelKey:             slug.Make(blockDevice.Status.Serial),
		v1alpha1.BlockDeviceSizeLabelKey:               blockDevice.Status.Size.String(),
		v1alpha1.BlockDeviceModelLabelKey:              slug.Make(blockDevice.Status.Model),
		v1alpha1.BlockDeviceRotaLabelKey:               strconv.FormatBool(blockDevice.Status.Rota),
		v1alpha1.BlockDeviceHotPlugLabelKey:            strconv.FormatBool(blockDevice.Status.HotPlug),
		v1alpha1.BlockDeviceMachineIDLabelKey:          slug.Make(blockDevice.Status.MachineID),
	})

	return result
}
