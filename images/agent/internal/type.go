package internal

import "k8s.io/apimachinery/pkg/api/resource"

type BlockDeviceCandidate struct {
	NodeName              string
	Consumable            bool
	PVUuid                string
	VGUuid                string
	LvmVolumeGroupName    string
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
	MachineId             string
	PartUUID              string
}

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
	VGUuid                string
	Nodes                 map[string][]LVMVGDevice
}

type LVMVGStatusThinPool struct {
	Name       string
	ActualSize resource.Quantity
	UsedSize   string
}

type LVMVGDevice struct {
	Path        string
	PVSize      resource.Quantity
	DevSize     resource.Quantity
	PVUuid      string
	BlockDevice string
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
	VGFree   resource.Quantity `json:"vg_free"`
	VGName   string            `json:"vg_name"`
	VGShared string            `json:"vg_shared"`
	VGSize   resource.Quantity `json:"vg_size"`
	VGTags   string            `json:"vg_tags"`
	VGUuid   string            `json:"vg_uuid"`
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
	PoolLv          string            `json:"pool_lv"`
	Origin          string            `json:"origin"`
	DataPercent     string            `json:"data_percent"`
	MetadataPercent string            `json:"metadata_percent"`
	MovePv          string            `json:"move_pv"`
	MirrorLog       string            `json:"mirror_log"`
	CopyPercent     string            `json:"copy_percent"`
	ConvertLv       string            `json:"convert_lv"`
	LvTags          string            `json:"lv_tags"`
}
