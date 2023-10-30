package internal

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
	Size                  string
	Rota                  bool
	Model                 string
	Name                  string
	HotPlug               bool
	KName                 string
	PkName                string
	Type                  string
	FSType                string
	MachineId             string
}

type Devices struct {
	BlockDevices []Device `json:"blockdevices"`
}

type Device struct {
	Name       string `json:"name"`
	MountPoint string `json:"mountpoint"`
	PartUUID   string `json:"partuuid"`
	HotPlug    bool   `json:"hotPlug"`
	Model      string `json:"model"`
	Serial     string `json:"serial"`
	Size       string `json:"size"`
	Type       string `json:"type"`
	Wwn        string `json:"wwn"`
	KName      string `json:"kname"`
	PkName     string `json:"pkname"`
	FSType     string `json:"fstype"`
	Rota       bool   `json:"rota"`
}

type PVReport struct {
	Report []PV `json:"report"`
}

type PV struct {
	PV []PVData `json:"pv"`
}

type PVData struct {
	PVName string `json:"pv_name,omitempty"`
	VGName string `json:"vg_name,omitempty"`
	PVUsed string `json:"pv_used,omitempty"`
	PVUuid string `json:"pv_uuid,omitempty"`
	VGTags string `json:"vg_tags,omitempty"`
	VGUuid string `json:"vg_uuid,omitempty"`
}

type VGReport struct {
	Report []VG `json:"report"`
}

type VG struct {
	VG []VGData `json:"vg"`
}

type VGData struct {
	VGName   string `json:"vg_name"`
	VGUuid   string `json:"vg_uuid"`
	VGTags   string `json:"vg_tags"`
	VGShared string `json:"vg_shared"`
}
