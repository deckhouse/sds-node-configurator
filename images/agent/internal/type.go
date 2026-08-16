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
	"strconv"

	"k8s.io/apimachinery/pkg/api/resource"
)

type LVMVolumeGroupCandidate struct {
	LVMVGName             string
	Finalizers            []string
	ActualVGNameOnTheNode string
	BlockDevicesNames     []string
	SpecThinPools         map[string]resource.Quantity
	Type                  string
	AllocatedSize         resource.Quantity
	Health                string
	Message               string
	StatusThinPools       []LVMVGStatusThinPool
	VGSize                resource.Quantity
	VGFree                resource.Quantity
	VGUUID                string
	ExtentSize            resource.Quantity
	Nodes                 map[string][]LVMVGDevice
	FileDeviceNodes       map[string][]LVMVGFileDevice
	// LVMVGNameGenerated records that LVMVGName was minted rather than read back
	// from the Volume Group's storage.deckhouse.io/lvmVolumeGroupName tag. It is
	// normal for a Volume Group an administrator handed over, and disqualifying
	// for a file-backed one: its backing files are named after the owning
	// LVMVolumeGroup, so under a generated name the agent would not recognise
	// them and would provision a second set alongside.
	//
	// False is the safe default — "the name is the one the node recorded".
	LVMVGNameGenerated bool
	// FileDeviceStateUnknown records that at least one loop PV of this Volume
	// Group could not be classified, so FileDeviceNodes is known to be
	// incomplete. A candidate carrying it must not be written to an
	// LVMVolumeGroup: an entry missing from status.nodes[].fileDevices reads as
	// "never provisioned", which is a claim about the node this candidate cannot
	// make. False is the safe default — "the set is complete".
	FileDeviceStateUnknown bool
	// UnnamedPVs holds the paths of this Volume Group's block-device Physical
	// Volumes that no BlockDevice resource names, sorted. Nodes is therefore known
	// to be incomplete — and when such a PV is the VG's only one, empty.
	//
	// A candidate carrying any must be come back for, and must say so on the
	// resource: the reason it holds is almost always transient, but a PV below
	// BlockDeviceValidSize or excluded by a BlockDeviceFilter never gets a
	// BlockDevice, and the retry has to end somewhere.
	//
	// Unlike FileDeviceStateUnknown it does not by itself make the candidate
	// unpublishable — withholding vgSize, vgFree and thin-pool usage forever would
	// cost more than a missing device entry. What must not be published is a
	// candidate that names no device at all, because status.nodes is overwritten
	// wholesale and an empty one loses the node name itself, and with it the
	// AgentReady condition the controller only sets on an LVMVolumeGroup whose
	// status.nodes names a node.
	//
	// Empty is the safe default — "every PV was named".
	UnnamedPVs []string
}

type LVMVGStatusThinPool struct {
	Name          string
	ActualSize    resource.Quantity
	UsedSize      resource.Quantity
	AllocatedSize resource.Quantity
	Ready         bool
	Message       string
}

type LVMVGDevice struct {
	Path        string
	PVSize      resource.Quantity
	DevSize     resource.Quantity
	PVUUID      string
	BlockDevice string
}

type LVMVGFileDevice struct {
	FilePath   string
	LoopDevice string
	Size       resource.Quantity
	PVUUID     string
}

// LoopBackingFile is what `losetup --output BACK-FILE` says about one loop
// device.
type LoopBackingFile struct {
	// Path is the backing file with the " (deleted)" marker stripped, so
	// ownership matching by basename still works on an unlinked file.
	Path string
	// Deleted is that stripped marker. Keeping it apart from Path is the whole
	// point: cleanup needs the path to recognise the loop as ours, while
	// provisioning needs to know the file is gone so it does not create a
	// second one beside it.
	Deleted bool
}

// LoopDeviceEntry is one line of the node's loop-device table: the device and the
// file behind it. Enumerating both together is what lets the agent tell its own
// file-backed devices from a virtual machine's disk, which is the difference
// between a loop device LVM may read and one it must not touch.
type LoopDeviceEntry struct {
	Device  string
	Backing LoopBackingFile
}

// FilesystemSpace is what one `stat -f` reports about the filesystem holding a
// directory the agent is about to allocate a backing file in.
type FilesystemSpace struct {
	// AvailableBytes is what can still be allocated without dipping into the
	// filesystem's own superuser reserve.
	AvailableBytes int64
	// TotalBytes is the size of the filesystem. It is what makes a reserve
	// expressible as a share rather than as an absolute number, which is the
	// only form that travels between a 30Gi node root and a 4Ti data disk.
	//
	// Zero means "unknown": callers skip the reserve rather than refuse
	// everything. A successful GetFilesystemSpace never returns it (see
	// parseStatfsSpace); it only appears when the measurement did not happen.
	TotalBytes int64
}

type Devices struct {
	BlockDevices []Device `json:"blockdevices"`
}

type Device struct {
	Name       string            `json:"name"`
	MountPoint string            `json:"mountpoint"`
	PartUUID   string            `json:"partuuid"`
	HotPlug    bool              `json:"hotPlug"`
	Model      string            `json:"model"`
	Serial     string            `json:"serial"`
	Size       resource.Quantity `json:"size"`
	Type       string            `json:"type"`
	Wwn        string            `json:"wwn"`
	KName      string            `json:"kname"`
	PkName     string            `json:"pkname"`
	FSType     string            `json:"fstype"`
	Rota       bool              `json:"rota"`

	SerialInherited string
	WWNInherited    string
}

type PVReport struct {
	Report []PV `json:"report"`
}

type PV struct {
	PV []PVData `json:"pv"`
}

type PVData struct {
	PVName string            `json:"pv_name,omitempty"`
	VGName string            `json:"vg_name,omitempty"`
	PVSize resource.Quantity `json:"pv_size,omitempty"`
	PVUsed string            `json:"pv_used,omitempty"`
	PVUuid string            `json:"pv_uuid,omitempty"`
	VGTags string            `json:"vg_tags,omitempty"`
	VGUuid string            `json:"vg_uuid,omitempty"`
}

type VGReport struct {
	Report []VG `json:"report"`
}

type VG struct {
	VG []VGData `json:"vg"`
}

type VGData struct {
	VGAttr       string            `json:"vg_attr"`
	VGFree       resource.Quantity `json:"vg_free"`
	VGName       string            `json:"vg_name"`
	VGShared     string            `json:"vg_shared"`
	VGSize       resource.Quantity `json:"vg_size"`
	VGTags       string            `json:"vg_tags"`
	VGUUID       string            `json:"vg_uuid"`
	VGExtentSize resource.Quantity `json:"vg_extent_size"`
}

type LVReport struct {
	Report []LV `json:"report"`
}

type LV struct {
	LV []LVData `json:"lv"`
}

type LVData struct {
	LVName          string            `json:"lv_name"`
	VGName          string            `json:"vg_name"`
	VGUuid          string            `json:"vg_uuid"`
	LVAttr          string            `json:"lv_attr"`
	LVSize          resource.Quantity `json:"lv_size"`
	PoolName        string            `json:"pool_lv"`
	Origin          string            `json:"origin"`
	DataPercent     string            `json:"data_percent"`
	MetadataPercent string            `json:"metadata_percent"`
	MovePv          string            `json:"move_pv"`
	MirrorLog       string            `json:"mirror_log"`
	CopyPercent     string            `json:"copy_percent"`
	ConvertLv       string            `json:"convert_lv"`
	LvTags          string            `json:"lv_tags"`
	ThinID          string            `json:"thin_id"`
	MetadataLv      string            `json:"metadata_lv"`
	LVDmPath        string            `json:"lv_dm_path"`
}

func (lv LVData) GetUsedSize() (*resource.Quantity, error) {
	var (
		err         error
		dataPercent float64
	)

	if lv.DataPercent == "" {
		dataPercent = 0.0
	} else {
		dataPercent, err = strconv.ParseFloat(lv.DataPercent, 64)
		if err != nil {
			return nil, err
		}
	}

	aproxBytes := float64(lv.LVSize.Value()) * dataPercent * 0.01

	return resource.NewQuantity(int64(aproxBytes), resource.BinarySI), nil
}
