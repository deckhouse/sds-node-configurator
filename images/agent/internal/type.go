package internal

type Candidate struct {
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
	HotPlug    bool   `json:"hotplug"`
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
	Report []Report `json:"report"`
}

type Report struct {
	PV []PV `json:"pv"`
}

type PV struct {
	PVName string `json:"pv_name,omitempty"`
	VGName string `json:"vg_name,omitempty"`
	PVUsed string `json:"pv_used,omitempty"`
	PVUuid string `json:"pv_uuid,omitempty"`
	VGTags string `json:"vg_tags,omitempty"`
	VGUuid string `json:"vg_uuid,omitempty"`
}
