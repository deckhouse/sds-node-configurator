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

package udev

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// ================== ParseUdevProperties ==================

func TestParseUdevProperties_FullEnv(t *testing.T) {
	env := map[string]string{
		"DEVNAME":            "sda",
		"DEVTYPE":            "disk",
		"MAJOR":              "8",
		"MINOR":              "0",
		"ID_SERIAL_SHORT":    "WD-ABC123",
		"ID_SERIAL":          "WDC_WD10EZEX_WD-ABC123",
		"ID_MODEL":           "WDC_WD10EZEX",
		"ID_WWN":             "0x50014ee2b5e7c5a0",
		"ID_FS_TYPE":         "ext4",
		"ID_PART_ENTRY_UUID": "abcd-1234",
		"DM_NAME":            "",
		"DM_UUID":            "",
		"MD_LEVEL":           "",
	}
	props := ParseUdevProperties(env)

	assert.Equal(t, "/dev/sda", props.DevName)
	assert.Equal(t, "disk", props.DevType)
	assert.Equal(t, 8, props.Major)
	assert.Equal(t, 0, props.Minor)
	assert.Equal(t, "WD-ABC123", props.Serial, "ID_SERIAL_SHORT takes priority")
	assert.Equal(t, "WDC_WD10EZEX", props.Model)
	assert.Equal(t, "0x50014ee2b5e7c5a0", props.WWN)
	assert.Equal(t, "ext4", props.FSType)
	assert.Equal(t, "abcd-1234", props.PartUUID)
}

func TestParseUdevProperties_SerialFallback(t *testing.T) {
	env := map[string]string{
		"DEVNAME":   "/dev/sda",
		"MAJOR":     "8",
		"MINOR":     "0",
		"ID_SERIAL": "WDC_WD10EZEX_WD-ABC123",
	}
	props := ParseUdevProperties(env)
	assert.Equal(t, "WDC_WD10EZEX_WD-ABC123", props.Serial, "Falls back to ID_SERIAL")
}

func TestParseUdevProperties_SerialScsiChainAndNormalize(t *testing.T) {
	env := map[string]string{
		"DEVNAME":           "/dev/sda",
		"MAJOR":             "8",
		"MINOR":             "0",
		"SCSI_IDENT_SERIAL": "  SG3  ID  ",
		"ID_SCSI_SERIAL":    "scsi",
		"ID_SERIAL_SHORT":   "short",
		"ID_SERIAL":         "long",
	}
	props := ParseUdevProperties(env)
	assert.Equal(t, "SG3 ID", props.Serial)
}

func TestParseUdevProperties_WwnPrefersExtension(t *testing.T) {
	env := map[string]string{
		"DEVNAME":               "/dev/sda",
		"MAJOR":                 "8",
		"MINOR":                 "0",
		"ID_WWN":                "0x5000",
		"ID_WWN_WITH_EXTENSION": "0x5000deadbeef",
	}
	props := ParseUdevProperties(env)
	assert.Equal(t, "0x5000deadbeef", props.WWN)
}

func TestParseUdevProperties_ModelFromEnc(t *testing.T) {
	env := map[string]string{
		"DEVNAME":      "/dev/sda",
		"MAJOR":        "8",
		"MINOR":        "0",
		"ID_MODEL":     "IGNORED_WHEN_ENC",
		"ID_MODEL_ENC": `Vendor\x20Disk\x20Name`,
	}
	props := ParseUdevProperties(env)
	assert.Equal(t, "Vendor Disk Name", props.Model)
}

func TestParseUdevProperties_DevNamePrefixAdded(t *testing.T) {
	env := map[string]string{
		"DEVNAME": "sda",
		"MAJOR":   "8",
		"MINOR":   "0",
	}
	props := ParseUdevProperties(env)
	assert.Equal(t, "/dev/sda", props.DevName, "Adds /dev/ prefix when missing")
}

func TestParseUdevProperties_DevNameAlreadyPrefixed(t *testing.T) {
	env := map[string]string{
		"DEVNAME": "/dev/nvme0n1",
		"MAJOR":   "259",
		"MINOR":   "0",
	}
	props := ParseUdevProperties(env)
	assert.Equal(t, "/dev/nvme0n1", props.DevName)
}

func TestParseUdevProperties_EmptyEnv(t *testing.T) {
	props := ParseUdevProperties(map[string]string{})
	assert.Equal(t, "", props.DevName)
	assert.Equal(t, 0, props.Major)
	assert.Equal(t, 0, props.Minor)
	assert.Equal(t, "", props.Serial)
}

func TestParseUdevProperties_InvalidMajorMinor(t *testing.T) {
	env := map[string]string{
		"MAJOR": "abc",
		"MINOR": "xyz",
	}
	props := ParseUdevProperties(env)
	assert.Equal(t, 0, props.Major)
	assert.Equal(t, 0, props.Minor)
}

// ================== ResolveDeviceType ==================

func TestResolveDeviceType(t *testing.T) {
	tests := []struct {
		name     string
		props    UdevProperties
		devName  string
		expected string
	}{
		{"partition before loop name", UdevProperties{DevType: "partition"}, "/dev/loop0p1", "part"},
		{"loop device", UdevProperties{DevType: "disk"}, "/dev/loop0", "loop"},
		{"LVM volume", UdevProperties{DMUUID: "LVM-abc123"}, "/dev/dm-0", "lvm"},
		{"multipath lower", UdevProperties{DMUUID: "mpath-xyz"}, "/dev/dm-1", "mpath"},
		{"multipath upper prefix", UdevProperties{DMUUID: "MPATH-xyz"}, "/dev/dm-1", "mpath"},
		{"crypt dm", UdevProperties{DMUUID: "CRYPT-LUKS2-deadbeef"}, "/dev/dm-2", "crypt"},
		{"dm no uuid", UdevProperties{}, "/dev/dm-9", "dm"},
		{"dm kpartx part prefix", UdevProperties{DMUUID: "part7-00002-abcdef"}, "/dev/dm-3", "part"},
		{"MD RAID1", UdevProperties{MDLevel: "raid1"}, "/dev/md0", "raid1"},
		{"MD RAID5", UdevProperties{MDLevel: "raid5"}, "/dev/md1", "raid5"},
		{"MD level lowercased", UdevProperties{MDLevel: "RAID10"}, "/dev/md2", "raid10"},
		{"MD no level", UdevProperties{DevType: "disk"}, "/dev/md3", "md"},
		{"partition", UdevProperties{DevType: "partition"}, "/dev/sda1", "part"},
		{"plain disk no sysfs", UdevProperties{DevType: "disk"}, "/dev/sda", "disk"},
		{"nvme disk no sysfs", UdevProperties{DevType: "disk"}, "/dev/nvme0n1", "disk"},
		{"CD-ROM sr0 no sysfs", UdevProperties{DevType: "disk"}, "/dev/sr0", "rom"},
		{"CD-ROM sr1 no sysfs", UdevProperties{}, "/dev/sr1", "rom"},
		{"empty props", UdevProperties{}, "/dev/sda", "disk"},
		{"dm uuid starts with dash", UdevProperties{DMUUID: "-something"}, "/dev/dm-5", "dm"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, ResolveDeviceType(tt.props, tt.devName))
		})
	}
}

func TestResolveDeviceType_PartitionFallbackWhenDevTypeMissing(t *testing.T) {
	root := withFakeSysfs(t)
	createPartitionSymlink(t, root, "sda", "sda1")

	assert.Equal(t, "part", ResolveDeviceType(UdevProperties{}, "/dev/sda1"))
}

// ================== ResolveDeviceName ==================

func TestResolveDeviceName(t *testing.T) {
	tests := []struct {
		name     string
		props    UdevProperties
		expected string
	}{
		{"DM device", UdevProperties{DMName: "vg0-lv0", DevName: "/dev/dm-0"}, "/dev/mapper/vg0-lv0"},
		{"regular with prefix", UdevProperties{DevName: "/dev/sda"}, "/dev/sda"},
		{"regular without prefix", UdevProperties{DevName: "sda"}, "/dev/sda"},
		{"empty", UdevProperties{}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, ResolveDeviceName(tt.props))
		})
	}
}

// ================== ResolveKernelName ==================

func TestResolveKernelName(t *testing.T) {
	assert.Equal(t, "/dev/sda", ResolveKernelName("sda"))
	assert.Equal(t, "/dev/sda", ResolveKernelName("/dev/sda"))
	assert.Equal(t, "/dev/nvme0n1", ResolveKernelName("nvme0n1"))
}

// ================== normalizeWhitespace ==================

func TestNormalizeWhitespace(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"", ""},
		{"a", "a"},
		{"  a", "a"},
		{"a  b", "a b"},
		{"a \t b", "a b"},
		{"a b ", "a b"},
		{"  a  b  ", "a b"},
		{"   ", ""},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			assert.Equal(t, tt.want, normalizeWhitespace(tt.in))
		})
	}
}

// ================== unhexmangle ==================

func TestUnhexmangle(t *testing.T) {
	assert.Equal(t, "ST330006 50NS", unhexmangle(`ST330006\x2050NS`))
	assert.Equal(t, "Vendor Disk Name", unhexmangle(`Vendor\x20Disk\x20Name`))
	assert.Equal(t, "no escapes", unhexmangle("no escapes"))
	assert.Equal(t, "", unhexmangle(""))
	assert.Equal(t, `\xZZ`, unhexmangle(`\xZZ`), "invalid hex digits left as-is")
	assert.Equal(t, `\x4`, unhexmangle(`\x4`), "incomplete sequence left as-is")
}

// ================== ensureDevPrefix ==================

func TestEnsureDevPrefix(t *testing.T) {
	assert.Equal(t, "", ensureDevPrefix(""))
	assert.Equal(t, "/dev/sda", ensureDevPrefix("sda"))
	assert.Equal(t, "/dev/sda", ensureDevPrefix("/dev/sda"))
	assert.Equal(t, "/dev/nvme0n1", ensureDevPrefix("nvme0n1"))
}

// ================== DeviceKey ==================

func TestDeviceKey(t *testing.T) {
	assert.Equal(t, "8:0", DeviceKey(8, 0))
	assert.Equal(t, "259:1", DeviceKey(259, 1))
}
