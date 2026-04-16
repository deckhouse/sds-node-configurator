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
	"strconv"
	"strings"
)

type UdevProperties struct {
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

func ParseUdevProperties(env map[string]string) UdevProperties {
	major, _ := strconv.Atoi(env["MAJOR"])
	minor, _ := strconv.Atoi(env["MINOR"])

	devName := ensureDevPrefix(env["DEVNAME"])

	return UdevProperties{
		DevName:  devName,
		DevType:  env["DEVTYPE"],
		Major:    major,
		Minor:    minor,
		Serial:   serialFromUdevEnv(env),
		Model:    modelFromUdevEnv(env),
		WWN:      wwnFromUdevEnv(env),
		FSType:   env["ID_FS_TYPE"],
		PartUUID: env["ID_PART_ENTRY_UUID"],
		DMName:   env["DM_NAME"],
		DMUUID:   env["DM_UUID"],
		MDLevel:  env["MD_LEVEL"],
	}
}

// ResolveDeviceType returns the lsblk-style TYPE (util-linux misc-utils/lsblk.c get_type()).
// Order: partition, device-mapper, loop, md, sr prefix, fallback "disk".
func ResolveDeviceType(props UdevProperties, devName string) string {
	bare := SysfsDevName(devName)
	if bare == "" {
		bare = SysfsDevName(props.DevName)
	}

	if props.DevType == "partition" {
		return "part"
	}
	if props.DevType == "" && bare != "" && isPartition(bare) {
		return "part"
	}

	if strings.HasPrefix(bare, "dm-") {
		return dmTypeFromDMUUID(props.DMUUID)
	}

	if strings.HasPrefix(bare, "loop") {
		return "loop"
	}

	if strings.HasPrefix(bare, "md") {
		if lvl := strings.TrimSpace(props.MDLevel); lvl != "" {
			return strings.ToLower(lvl)
		}
		return "md"
	}

	if strings.HasPrefix(bare, "sr") {
		return "rom"
	}
	return "disk"
}

// dmTypeFromDMUUID derives TYPE from dm uuid prefix (substring before first '-', lowercased).
// kpartx uses a "part*" prefix trimmed to "part" (lsblk get_type).
func dmTypeFromDMUUID(dmUUID string) string {
	if dmUUID == "" {
		return "dm"
	}
	prefix := dmUUID
	if i := strings.Index(dmUUID, "-"); i >= 0 {
		prefix = dmUUID[:i]
	}
	if prefix == "" {
		return "dm"
	}
	if len(prefix) >= 4 && strings.EqualFold(prefix[:4], "part") {
		prefix = "part"
	}
	return strings.ToLower(prefix)
}

func ResolveDeviceName(props UdevProperties) string {
	if props.DMName != "" {
		return "/dev/mapper/" + props.DMName
	}
	return ensureDevPrefix(props.DevName)
}

func ResolveKernelName(devName string) string {
	return ensureDevPrefix(devName)
}

// ResolveParentDevice returns PkName: sysfs parent for partitions (first);
// for dm-* / md* the first slave. Partition-first avoids md0p1 -> wrong path.
func ResolveParentDevice(devName string) string {
	devName = SysfsDevName(devName)
	if isPartition(devName) {
		if parent := parentFromSysfs(devName); parent != "" {
			return "/dev/" + parent
		}
	}
	if strings.HasPrefix(devName, "dm-") || strings.HasPrefix(devName, "md") {
		slaves, err := ReadSysfsSlaves(devName)
		if err == nil && len(slaves) > 0 {
			return "/dev/" + slaves[0]
		}
	}
	return ""
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
// matching util-linux normalize_whitespace() used by lsblk after reading udev properties.
func normalizeWhitespace(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

func hexVal(c byte) int {
	switch {
	case c >= '0' && c <= '9':
		return int(c - '0')
	case c >= 'a' && c <= 'f':
		return int(c-'a') + 10
	case c >= 'A' && c <= 'F':
		return int(c-'A') + 10
	default:
		return -1
	}
}

// unhexmangle decodes udev \xHH escape sequences,
// matching unhexmangle_to_buffer() in util-linux lib/mangle.c.
func unhexmangle(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); {
		if i+3 < len(s) && s[i] == '\\' && s[i+1] == 'x' &&
			hexVal(s[i+2]) >= 0 && hexVal(s[i+3]) >= 0 {
			b.WriteByte(byte(hexVal(s[i+2])<<4 | hexVal(s[i+3])))
			i += 4
			continue
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
