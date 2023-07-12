package blockdev

import (
	"bytes"
	"github.com/google/uuid"
)

type Candidate struct {
	NodeName   string
	ID         string
	Path       string
	Size       string
	Model      string
	Name       string
	UUID       uuid.UUID
	SkipReason string
}

type CandidateHandler struct {
	Name      string
	Command   []string
	ParseFunc func(nodeName string, out bytes.Buffer) ([]Candidate, error)
}

type Devices struct {
	BlockDevices []Device `json:"blockdevices"`
}

type Device struct {
	Name       string `json:"name"`
	Mountpoint string `json:"mountpoint"`
	Partuuid   string `json:"partuuid"`
	Hotplug    bool   `json:"hotplug"`
	Model      string `json:"model"`
	Serial     string `json:"serial"`
	Size       string `json:"size"`
	Type       string `json:"type"`
	Wwn        string `json:"wwn"`
	Kname      string `json:"kname"`
	Pkname     string `json:"pkname"`
}

//
