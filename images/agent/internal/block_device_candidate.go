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
		NodeName:   nodeName,
		Consumable: isConsumable(device),
		Wwn:        device.Wwn,
		Serial:     device.Serial,
		Path:       device.Name,
		Size:       device.Size,
		Rota:       device.Rota,
		Model:      device.Model,
		HotPlug:    device.HotPlug,
		KName:      device.KName,
		PkName:     device.PkName,
		Type:       device.Type,
		FSType:     device.FSType,
		MachineID:  machineID,
		PartUUID:   device.PartUUID,
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
		Wwn:                   candidate.Wwn,
		Serial:                candidate.Serial,
		Path:                  candidate.Path,
		Size:                  *resource.NewQuantity(candidate.Size.Value(), resource.BinarySI),
		Model:                 candidate.Model,
		Rota:                  candidate.Rota,
		MachineID:             candidate.MachineID,
	}
}

func (candidate *BlockDeviceCandidate) AsAPIBlockDevice() v1alpha1.BlockDevice {
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
		candidate.Wwn != blockDevice.Status.Wwn ||
		candidate.Serial != blockDevice.Status.Serial ||
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

// Creates new labels as map, keeping unrelated labels device already has
func newBlockDeviceLabels(blockDevice *v1alpha1.BlockDevice) map[string]string {
	result := make(map[string]string, func() int {
		if blockDevice.Labels == nil {
			return 16
		}
		return len(blockDevice.Labels)
	}())

	maps.Copy(result, blockDevice.Labels)

	slug.Lowercase = false
	slug.MaxLength = 63
	slug.EnableSmartTruncate = false
	maps.Copy(result, map[string]string{
		MetadataNameLabelKey:                  slug.Make(blockDevice.ObjectMeta.Name),
		HostNameLabelKey:                      slug.Make(blockDevice.Status.NodeName),
		BlockDeviceTypeLabelKey:               slug.Make(blockDevice.Status.Type),
		BlockDeviceFSTypeLabelKey:             slug.Make(blockDevice.Status.FsType),
		BlockDevicePVUUIDLabelKey:             blockDevice.Status.PVUuid,
		BlockDeviceVGUUIDLabelKey:             blockDevice.Status.VGUuid,
		BlockDevicePartUUIDLabelKey:           blockDevice.Status.PartUUID,
		BlockDeviceLVMVolumeGroupNameLabelKey: slug.Make(blockDevice.Status.LVMVolumeGroupName),
		BlockDeviceActualVGNameLabelKey:       slug.Make(blockDevice.Status.ActualVGNameOnTheNode),
		BlockDeviceWWNLabelKey:                slug.Make(blockDevice.Status.Wwn),
		BlockDeviceSerialLabelKey:             slug.Make(blockDevice.Status.Serial),
		BlockDeviceSizeLabelKey:               blockDevice.Status.Size.String(),
		BlockDeviceModelLabelKey:              slug.Make(blockDevice.Status.Model),
		BlockDeviceRotaLabelKey:               strconv.FormatBool(blockDevice.Status.Rota),
		BlockDeviceHotPlugLabelKey:            strconv.FormatBool(blockDevice.Status.HotPlug),
		BlockDeviceMachineIDLabelKey:          slug.Make(blockDevice.Status.MachineID),
	})

	return result
}
