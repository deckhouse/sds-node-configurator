package controller

type Candidate struct {
	NodeName   string
	ID         string
	Path       string
	Size       string
	Model      string
	Name       string
	MountPoint string
	HotPlug    bool
	KName      string
	PkName     string
	FSType     string
}

type CandidateHandler struct {
	Command   []string
	ParseFunc func(nodeName string, out []byte) ([]Candidate, error)
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
}