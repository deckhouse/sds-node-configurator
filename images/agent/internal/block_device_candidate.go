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
	"fmt"
	"maps"
	"reflect"
	"strconv"

	"github.com/gosimple/slug"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/deckhouse/sds-common-lib/conditions"
	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
)

type BlockDeviceCandidate struct {
	NodeName   string
	Consumable bool
	// ConsumableReason and ConsumableMessage explain Consumable. They are
	// published on the Consumable condition; status.consumable carries only the
	// verdict.
	ConsumableReason      string
	ConsumableMessage     string
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

// consumability reports whether the device may back an LVM Physical Volume,
// and why not when it may not.
//
// The verdict used to be a bare bool, so status.consumable said no without
// saying which of the three reasons applied — an operator had to read the
// device's other fields and reconstruct the answer. The reason is published on
// the Consumable condition now.
//
// The order matters and is the one the bool check used: a mounted device is
// reported as mounted even though it also has a filesystem, because unmounting
// is the action that changes the answer.
func consumability(device *Device) (bool, string, string) {
	switch {
	case device.MountPoint != "":
		return false, v1alpha1.ReasonDeviceMounted,
			fmt.Sprintf("the device is mounted at %s", device.MountPoint)
	case device.FSType != "":
		return false, v1alpha1.ReasonDeviceHasFilesystem,
			fmt.Sprintf("the device carries a %s filesystem", device.FSType)
	case device.HotPlug:
		return false, v1alpha1.ReasonDeviceHotPlugged,
			"the device is hot-plugged, and a Volume Group on removable media loses its Physical Volume without notice"
	default:
		return true, v1alpha1.ReasonDeviceAvailable,
			"the device can back an LVM Physical Volume"
	}
}

func isConsumable(device *Device) bool {
	consumable, _, _ := consumability(device)
	return consumable
}

func NewBlockDeviceCandidateByDevice(device *Device, nodeName string, machineID string) BlockDeviceCandidate {
	consumable, reason, message := consumability(device)

	return BlockDeviceCandidate{
		NodeName:          nodeName,
		Consumable:        consumable,
		ConsumableReason:  reason,
		ConsumableMessage: message,
		Wwn:               device.Wwn,
		Serial:            device.Serial,
		Path:              device.Name,
		Size:              device.Size,
		Rota:              device.Rota,
		Model:             device.Model,
		HotPlug:           device.HotPlug,
		KName:             device.KName,
		PkName:            device.PkName,
		Type:              device.Type,
		FSType:            device.FSType,
		MachineID:         machineID,
		PartUUID:          device.PartUUID,
		SerialInherited:   device.SerialInherited,
		WWNInherited:      device.WWNInherited,
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

	candidate.setConsumableCondition(&blockDevice)
	blockDevice.Labels = newBlockDeviceLabels(&blockDevice)

	return blockDevice
}

func (candidate *BlockDeviceCandidate) UpdateAPIBlockDevice(blockDevice *v1alpha1.BlockDevice) {
	// The whole status is replaced, so the conditions have to be carried across
	// deliberately. Dropping them and setting the condition afresh would reset
	// lastTransitionTime on every unrelated change the discoverer picks up — a
	// path rename, a size change — and the timestamp would stop meaning "since
	// when has this device been unusable".
	//
	// Copied rather than re-pointed. The discoverer hands this method a
	// BlockDevice by value, taken from the map it listed from the API — and a
	// struct copy copies the slice header, not the array behind it. Reusing
	// that header would let meta.SetStatusCondition, which edits an existing
	// condition in place, write through into the map. The map is re-listed
	// every pass, so nothing observes it today; it becomes a real defect the
	// moment the write is retried within a pass, because the map would already
	// carry a verdict the API server rejected and the next diff would find
	// nothing to write.
	existing := append([]metav1.Condition(nil), blockDevice.Status.Conditions...)
	blockDevice.Status = candidate.asAPIBlockDeviceStatus()
	blockDevice.Status.Conditions = existing
	candidate.setConsumableCondition(blockDevice)

	blockDevice.Labels = newBlockDeviceLabels(blockDevice)
}

// setConsumableCondition publishes the verdict from consumability.
//
// conditions.Set is what keeps lastTransitionTime honest: it moves only when
// the status flips, not when the reason changes underneath it. A device that
// goes from mounted to carrying a filesystem stays Consumable=False, and the
// timestamp goes on pointing at when it first stopped being usable.
func (candidate *BlockDeviceCandidate) setConsumableCondition(blockDevice *v1alpha1.BlockDevice) {
	status := metav1.ConditionFalse
	if candidate.Consumable {
		status = metav1.ConditionTrue
	}

	conditions.Set(&blockDevice.Status.Conditions, metav1.Condition{
		Type:    v1alpha1.BlockDeviceConditionConsumable,
		Status:  status,
		Reason:  candidate.ConsumableReason,
		Message: conditions.TruncateMessage(candidate.ConsumableMessage),
		// Zero on the create path — the object has no generation until the API
		// server assigns one — and the assigned value on every update after
		// that. BlockDevice has no spec, so it never advances beyond that
		// value; the field is here because a condition that omits it reads as
		// "the writer does not track which revision it looked at", which is not
		// the case.
		ObservedGeneration: blockDevice.Generation,
	})
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
		candidate.consumableConditionDiffers(blockDevice) ||
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

// The slug settings the label values are built under. They are package-global in
// the library, so they are set once here rather than before each call.
//
// Not a tidy-up: newBlockDeviceLabels runs from the block-device discoverer and
// BlockDeviceLabelValue from the LVMVolumeGroup one, those two overlap (see
// scanner.runDiscoveryPass, which retries a pass from a goroutine of its own
// while the main loop keeps starting passes on udev events), and writing a
// package variable from two goroutines while a third reads it is a data race
// under the Go memory model — https://go.dev/ref/mem. Writing them before any
// goroutine exists is not.
func init() {
	slug.Lowercase = false
	slug.MaxLength = 63
	slug.EnableSmartTruncate = false
}

// BlockDeviceLabelValue is how every label value below is derived from the status
// field behind it, and it is exported so a caller that wants to *select*
// BlockDevices by one of them computes the same string the writer wrote.
//
// A second copy of the derivation that forgot one of the slug settings would
// silently select nothing on, say, a node whose name has an upper-case letter —
// and an empty list from a selector looks exactly like "there are no such
// devices".
//
// See Discoverer.bdAPICl in the lvg package for the caller this exists for.
func BlockDeviceLabelValue(statusField string) string {
	return slug.Make(statusField)
}

// Creates new labels as map, keeping unrelated labels device already has
func newBlockDeviceLabels(blockDevice *v1alpha1.BlockDevice) map[string]string {
	resultItemCount := 16
	if blockDevice.Labels != nil {
		resultItemCount = len(blockDevice.Labels)
	}

	result := make(map[string]string, resultItemCount)

	maps.Copy(result, blockDevice.Labels)

	maps.Copy(result, map[string]string{
		MetadataNameLabelKey:                           slug.Make(blockDevice.Name),
		HostNameLabelKey:                               BlockDeviceLabelValue(blockDevice.Status.NodeName),
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

// consumableConditionDiffers reports whether the published Consumable condition
// still says what the candidate would say.
//
// status.consumable alone is not enough to decide: a device that goes from
// mounted to carrying a filesystem stays not-consumable, so every field the
// old check compared is unchanged, and without this the reason on the condition
// would keep naming a mount that is long gone.
func (candidate *BlockDeviceCandidate) consumableConditionDiffers(blockDevice v1alpha1.BlockDevice) bool {
	published := conditions.Get(blockDevice.Status.Conditions, v1alpha1.BlockDeviceConditionConsumable)
	if published == nil {
		return true
	}

	status := metav1.ConditionFalse
	if candidate.Consumable {
		status = metav1.ConditionTrue
	}

	return published.Status != status ||
		published.Reason != candidate.ConsumableReason ||
		published.Message != conditions.TruncateMessage(candidate.ConsumableMessage)
}
