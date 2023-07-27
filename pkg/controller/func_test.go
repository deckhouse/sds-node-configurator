package controller

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseJSONlsblkOut(t *testing.T) {
	var devicesIN Devices
	nodeName := "testNode"
	minLenSize := 2
	minLenName := 3

	device1 := Device{
		Name:       "/dev/drbd1039",
		MountPoint: "",
		PartUUID:   "",
		HotPlug:    false,
		Model:      "",
		Serial:     "5G",
		Size:       "",
		Type:       "disk",
		Wwn:        "",
		KName:      "/dev/drbd1039",
		PkName:     "/dev/dm-24",
	}

	device2 := Device{
		Name:       "/dev/loop1",
		MountPoint: "",
		PartUUID:   "",
		HotPlug:    false,
		Model:      "",
		Serial:     "",
		Size:       "79.9M",
		Type:       "loop",
		Wwn:        "",
		KName:      "/dev/loop1",
		PkName:     "",
	}

	device3 := Device{
		Name:       "/dev/sda",
		MountPoint: "",
		PartUUID:   "",
		HotPlug:    false,
		Model:      "CT480BX500SSD1",
		Serial:     "2006E3E96EC0",
		Size:       "447.1G",
		Type:       "disk",
		Wwn:        "0x0000000000000000",
		KName:      "/dev/sda",
		PkName:     "",
	}

	device4 := Device{
		Name:       "/dev/mapper/vg--0-pvc--75619542--0cef--4eea--bd0b--ff07f8ffaf2a_00000",
		MountPoint: "",
		PartUUID:   "",
		HotPlug:    false,
		Model:      "",
		Serial:     "",
		Size:       "5G",
		Type:       "lvm",
		Wwn:        "",
		KName:      "/dev/dm-20",
		PkName:     "/dev/sda",
	}

	device5 := Device{
		Name:       "/dev/mapper/vg--0-pvc--6da6dd02--bde3--4abd--adca--f4d272290ac4_00000",
		MountPoint: "",
		PartUUID:   "",
		HotPlug:    false,
		Model:      "",
		Serial:     "",
		Size:       "5G",
		Type:       "lvm",
		Wwn:        "",
		KName:      "/dev/dm-24",
		PkName:     "/dev/sda",
	}

	device6 := Device{
		Name:       "/dev/vda14",
		MountPoint: "",
		PartUUID:   "",
		HotPlug:    false,
		Model:      "",
		Serial:     "",
		Size:       "4M",
		Type:       "part",
		Wwn:        "",
		KName:      "/dev/vda14",
		PkName:     "/dev/vda",
		FSType:     "",
	}

	devicesIN.BlockDevices = append(devicesIN.BlockDevices, device1, device2, device3, device4, device5, device6)
	buff, _ := json.Marshal(devicesIN)

	candidates, err := parseFreeBlockDev(nodeName, buff)
	if err != nil {
		t.Errorf(err.Error())
	}
	for _, device := range candidates {

		assert.Equal(t, nodeName, device.NodeName, "node name equal")

		if len(device.Name) < minLenName {
			t.Errorf("device name is too short")
		}

		if len(device.FSType) != 0 {
			t.Errorf("device fstype is not null")
		}

		assert.NotContains(t, device.Name, "drbd", "device name contains drbd")
		assert.NotContains(t, device.FSType, "loop", "device name contains loop")

		assert.Equal(t, device.HotPlug, false, "device is plugin")

		if len(device.Size) < minLenSize {
			t.Errorf("device size is empty")
		}

		assert.Equal(t, device.MountPoint, "", "mountpoint is not empty")
	}
}

func TestCreateUniqNameDevice(t *testing.T) {
	nodeName := "testNode"
	can := Candidate{
		NodeName: nodeName,
		ID:       "ZX128ZX128ZX128",
		Path:     "/dev/sda",
		Size:     "4Gb",
		Model:    "HARD-DRIVE",
	}

	deviceName := createUniqNameDevice(can, nodeName)
	assert.Equal(t, "dev-", deviceName[0:4], "device name does not start with dev-")
	assert.Equal(t, len(deviceName[4:]), 40, "device name does not contains sha1 sum")
}
