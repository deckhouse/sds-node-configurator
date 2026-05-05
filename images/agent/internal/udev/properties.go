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
	"fmt"
	"strconv"
	"strings"
)

// Properties is the parsed, normalized subset of a udev environment that
// matches lsblk's field-resolution rules for block device metadata.
type Properties struct {
	DevName  string
	DevType  string
	Major    int
	Minor    int
	Serial   string
	Model    string
	WWN      string
	FSType   string
	PartUUID string
	DMName   string
	DMUUID   string
	MDLevel  string
}

// ParseProperties parses a raw udev environment map into Properties.
// MAJOR and MINOR are required and must be non-negative integers;
// any parse error yields a zero Properties and a wrapped error.
func ParseProperties(env map[string]string) (Properties, error) {
	major, err := strconv.ParseUint(env["MAJOR"], 10, 32)
	if err != nil {
		return Properties{}, fmt.Errorf("parse MAJOR: %w", err)
	}

	minor, err := strconv.ParseUint(env["MINOR"], 10, 32)
	if err != nil {
		return Properties{}, fmt.Errorf("parse MINOR: %w", err)
	}

	return Properties{
		DevName:  ensureDevPrefix(env["DEVNAME"]),
		DevType:  env["DEVTYPE"],
		Major:    int(major),
		Minor:    int(minor),
		Serial:   serialFromUdevEnv(env),
		Model:    modelFromUdevEnv(env),
		WWN:      wwnFromUdevEnv(env),
		FSType:   env["ID_FS_TYPE"],
		PartUUID: env["ID_PART_ENTRY_UUID"],
		DMName:   env["DM_NAME"],
		DMUUID:   env["DM_UUID"],
		MDLevel:  env["MD_LEVEL"],
	}, nil
}

// serialFromUdevEnv follows the lsblk get_properties_by_udev() priority chain:
// SCSI_IDENT_SERIAL -> ID_SCSI_SERIAL -> ID_SERIAL_SHORT -> ID_SERIAL.
func serialFromUdevEnv(env map[string]string) string {
	for _, key := range []string{"SCSI_IDENT_SERIAL", "ID_SCSI_SERIAL", "ID_SERIAL_SHORT", "ID_SERIAL"} {
		if v := env[key]; v != "" {
			return normalizeWhitespace(v)
		}
	}
	return ""
}

// modelFromUdevEnv follows lsblk: ID_MODEL_ENC (unhexmangle + normalize) or ID_MODEL.
func modelFromUdevEnv(env map[string]string) string {
	if enc := env["ID_MODEL_ENC"]; enc != "" {
		return normalizeWhitespace(unhexmangle(enc))
	}
	return normalizeWhitespace(env["ID_MODEL"])
}

// wwnFromUdevEnv follows lsblk: ID_WWN_WITH_EXTENSION takes priority over ID_WWN.
func wwnFromUdevEnv(env map[string]string) string {
	if v := env["ID_WWN_WITH_EXTENSION"]; v != "" {
		return v
	}
	return env["ID_WWN"]
}

// normalizeWhitespace collapses whitespace runs and trims edges,
// approximating util-linux normalize_whitespace() for ASCII inputs.
func normalizeWhitespace(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

// unhexmangle decodes udev \xHH escape sequences,
// matching unhexmangle_to_buffer() in util-linux lib/mangle.c.
func unhexmangle(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); {
		if i+3 < len(s) && s[i] == '\\' && s[i+1] == 'x' {
			if v, err := strconv.ParseUint(s[i+2:i+4], 16, 8); err == nil {
				b.WriteByte(byte(v))
				i += 4
				continue
			}
		}
		b.WriteByte(s[i])
		i++
	}
	return b.String()
}

func ensureDevPrefix(devName string) string {
	if devName == "" || strings.HasPrefix(devName, "/dev/") {
		return devName
	}
	return "/dev/" + devName
}

// DeviceKey returns "major:minor".
func DeviceKey(major, minor int) string {
	return strconv.Itoa(major) + ":" + strconv.Itoa(minor)
}
