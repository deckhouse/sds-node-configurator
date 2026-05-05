/*
Copyright 2026 Flant JSC

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
	"github.com/stretchr/testify/require"
)

// ================== ParseProperties ==================

func TestParseProperties_FullEnv(t *testing.T) {
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
	props, err := ParseProperties(env)
	require.NoError(t, err)

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

func TestParseProperties_SerialFallback(t *testing.T) {
	env := map[string]string{
		"DEVNAME":   "/dev/sda",
		"MAJOR":     "8",
		"MINOR":     "0",
		"ID_SERIAL": "WDC_WD10EZEX_WD-ABC123",
	}
	props, err := ParseProperties(env)
	require.NoError(t, err)
	assert.Equal(t, "WDC_WD10EZEX_WD-ABC123", props.Serial, "Falls back to ID_SERIAL")
}

func TestParseProperties_SerialScsiChainAndNormalize(t *testing.T) {
	env := map[string]string{
		"DEVNAME":           "/dev/sda",
		"MAJOR":             "8",
		"MINOR":             "0",
		"SCSI_IDENT_SERIAL": "  SG3  ID  ",
		"ID_SCSI_SERIAL":    "scsi",
		"ID_SERIAL_SHORT":   "short",
		"ID_SERIAL":         "long",
	}
	props, err := ParseProperties(env)
	require.NoError(t, err)
	assert.Equal(t, "SG3 ID", props.Serial)
}

func TestParseProperties_WwnPrefersExtension(t *testing.T) {
	env := map[string]string{
		"DEVNAME":               "/dev/sda",
		"MAJOR":                 "8",
		"MINOR":                 "0",
		"ID_WWN":                "0x5000",
		"ID_WWN_WITH_EXTENSION": "0x5000deadbeef",
	}
	props, err := ParseProperties(env)
	require.NoError(t, err)
	assert.Equal(t, "0x5000deadbeef", props.WWN)
}

func TestParseProperties_ModelFromEnc(t *testing.T) {
	env := map[string]string{
		"DEVNAME":      "/dev/sda",
		"MAJOR":        "8",
		"MINOR":        "0",
		"ID_MODEL":     "IGNORED_WHEN_ENC",
		"ID_MODEL_ENC": `Vendor\x20Disk\x20Name`,
	}
	props, err := ParseProperties(env)
	require.NoError(t, err)
	assert.Equal(t, "Vendor Disk Name", props.Model)
}

func TestParseProperties_DevNamePrefixAdded(t *testing.T) {
	env := map[string]string{
		"DEVNAME": "sda",
		"MAJOR":   "8",
		"MINOR":   "0",
	}
	props, err := ParseProperties(env)
	require.NoError(t, err)
	assert.Equal(t, "/dev/sda", props.DevName, "Adds /dev/ prefix when missing")
}

func TestParseProperties_DevNameAlreadyPrefixed(t *testing.T) {
	env := map[string]string{
		"DEVNAME": "/dev/nvme0n1",
		"MAJOR":   "259",
		"MINOR":   "0",
	}
	props, err := ParseProperties(env)
	require.NoError(t, err)
	assert.Equal(t, "/dev/nvme0n1", props.DevName)
}

func TestParseProperties_MissingMajor(t *testing.T) {
	_, err := ParseProperties(map[string]string{"DEVNAME": "/dev/sda"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MAJOR")
}

func TestParseProperties_MissingMinor(t *testing.T) {
	_, err := ParseProperties(map[string]string{"MAJOR": "8"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MINOR")
}

func TestParseProperties_InvalidMajor(t *testing.T) {
	props, err := ParseProperties(map[string]string{
		"MAJOR": "abc", "MINOR": "0",
	})
	require.Error(t, err)
	assert.Equal(t, Properties{}, props)
	assert.Contains(t, err.Error(), "MAJOR")
}

func TestParseProperties_InvalidMinor(t *testing.T) {
	props, err := ParseProperties(map[string]string{
		"MAJOR": "8", "MINOR": "xyz",
	})
	require.Error(t, err)
	assert.Equal(t, Properties{}, props)
	assert.Contains(t, err.Error(), "MINOR")
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
