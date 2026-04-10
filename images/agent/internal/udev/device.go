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
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Package-level paths; tests may override.
var (
	sysClassBlockPath = "/sys/class/block"
	sysBlockPath      = "/sys/block"
	runUdevDataPath   = "/run/udev/data"
	procSelfMountInfo = "/proc/1/mountinfo"
)

const sectorSize = 512

// UdevProperties holds selected keys from a merged udev uevent/database map.
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

func ReadSysfsSize(devName string) (int64, error) {
	data, err := os.ReadFile(filepath.Join(sysClassBlockPath, devName, "size"))
	if err != nil {
		return 0, fmt.Errorf("reading size for %s: %w", devName, err)
	}
	sectors, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parsing size for %s: %w", devName, err)
	}
	return sectors * sectorSize, nil
}

func ReadSysfsRotational(devName string) (bool, error) {
	devName = resolveParentForPartition(devName)
	data, err := os.ReadFile(filepath.Join(sysClassBlockPath, devName, "queue", "rotational"))
	if err != nil {
		return false, fmt.Errorf("reading rotational for %s: %w", devName, err)
	}
	return strings.TrimSpace(string(data)) == "1", nil
}

func ReadSysfsRemovable(devName string) (bool, error) {
	devName = resolveParentForPartition(devName)
	data, err := os.ReadFile(filepath.Join(sysClassBlockPath, devName, "removable"))
	if err != nil {
		return false, fmt.Errorf("reading removable for %s: %w", devName, err)
	}
	return strings.TrimSpace(string(data)) == "1", nil
}

// ReadSysfsHotplug mirrors util-linux sysfs_blkdev_is_hotpluggable() (lib/sysfs.c):
// walk the device path upward, appending /removable at each level; "removable" -> true,
// "fixed" -> false. This is not the same as the block dev's own "removable" sysfs byte (RM column).
func ReadSysfsHotplug(devName string) (bool, error) {
	devName = resolveParentForPartition(devName)

	symlinkPath := filepath.Join(sysClassBlockPath, devName)
	resolved, err := filepath.EvalSymlinks(symlinkPath)
	if err != nil {
		return false, fmt.Errorf("resolving symlink for %s: %w", devName, err)
	}

	dir := resolved
	for dir != "/" && dir != "." {
		removableFile := filepath.Join(dir, "removable")
		data, err := os.ReadFile(removableFile)
		if err == nil {
			content := strings.TrimSpace(string(data))
			if content == "removable" {
				return true, nil
			}
			if content == "fixed" {
				return false, nil
			}
		}
		dir = filepath.Dir(dir)
	}

	return false, nil
}

func ReadSysfsSlaves(devName string) ([]string, error) {
	dir := filepath.Join(sysBlockPath, devName, "slaves")
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("reading slaves for %s: %w", devName, err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names, nil
}

// resolveParentForPartition returns the parent disk for partitions so
// rotational/removable read the backing device; else devName unchanged.
func resolveParentForPartition(devName string) string {
	if !isPartition(devName) {
		return devName
	}
	if parent := parentFromSysfs(devName); parent != "" {
		return parent
	}
	return devName
}

func ReadUdevDB(major, minor int) (map[string]string, error) {
	path := fmt.Sprintf("%s/b%d:%d", runUdevDataPath, major, minor)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading udev db %s: %w", path, err)
	}
	props := make(map[string]string)
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		after, ok := strings.CutPrefix(line, "E:")
		if !ok {
			continue
		}
		idx := strings.Index(after, "=")
		if idx < 0 {
			continue
		}
		props[after[:idx]] = after[idx+1:]
	}
	return props, nil
}

// MergeEnvWithUdevDB adds /run/udev/data/b<major>:<minor>; event keys win.
func MergeEnvWithUdevDB(env map[string]string) map[string]string {
	majorStr, hasMajor := env["MAJOR"]
	minorStr, hasMinor := env["MINOR"]
	if !hasMajor || !hasMinor {
		return env
	}
	major, err1 := strconv.Atoi(majorStr)
	minor, err2 := strconv.Atoi(minorStr)
	if err1 != nil || err2 != nil {
		return env
	}
	dbProps, err := ReadUdevDB(major, minor)
	if err != nil {
		return env
	}
	merged := make(map[string]string, len(env)+len(dbProps))
	for k, v := range dbProps {
		merged[k] = v
	}
	for k, v := range env {
		merged[k] = v
	}
	return merged
}

func ParseUdevProperties(env map[string]string) UdevProperties {
	major, _ := strconv.Atoi(env["MAJOR"])
	minor, _ := strconv.Atoi(env["MINOR"])

	// Model / serial / WWN follow lsblk get_properties_by_udev() (misc-utils/lsblk-properties.c)
	// so BlockDevice identity matches `lsblk -J` on main (pre-netlink) for the same hardware.
	devName := env["DEVNAME"]
	if devName != "" && !strings.HasPrefix(devName, "/dev/") {
		devName = "/dev/" + devName
	}

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

// scsiTypeName maps sysfs device/type (SCSI peripheral type) to lsblk TYPE strings.
// Matches blkdev_scsi_type_to_name() in util-linux lib/blkdev.c; lsblk lowercases in get_type().
func scsiTypeName(code int) string {
	switch code {
	case 0x00:
		return "disk"
	case 0x01:
		return "tape"
	case 0x02:
		return "printer"
	case 0x03:
		return "processor"
	case 0x04:
		return "worm"
	case 0x05:
		return "rom"
	case 0x06:
		return "scanner"
	case 0x07:
		return "mo-disk"
	case 0x08:
		return "changer"
	case 0x09:
		return "comm"
	case 0x0c:
		return "raid"
	case 0x0d:
		return "enclosure"
	case 0x0e:
		return "rbc"
	case 0x11:
		return "osd"
	case 0x7f:
		return "no-lun"
	default:
		return ""
	}
}

func readScsiTypeFromSysfs(devShort string) (string, bool) {
	if devShort == "" {
		return "", false
	}
	data, err := os.ReadFile(filepath.Join(sysClassBlockPath, devShort, "device", "type"))
	if err != nil {
		return "", false
	}
	code, err := strconv.ParseInt(strings.TrimSpace(string(data)), 0, 32)
	if err != nil {
		return "", false
	}
	name := scsiTypeName(int(code))
	if name == "" {
		return "", false
	}
	return name, true
}

// dmTypeFromDMUUID derives TYPE from dm uuid prefix (same source as sysfs dm/uuid, udev DM_UUID):
// substring before first '-', lowercased; kpartx uses a "part*" prefix trimmed to "part" (lsblk get_type).
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

// ResolveDeviceType returns the lsblk-style TYPE (util-linux misc-utils/lsblk.c get_type()).
// Raw udev DEVTYPE is not enough: order is partition, device-mapper, loop, md, then SCSI device/type or "disk".
// DM uses the DM_UUID prefix before the first '-'. If sysfs device/type is missing, sr* is treated as rom.
func ResolveDeviceType(props UdevProperties, devName string) string {
	bare := SysfsDevName(devName)
	if bare == "" {
		bare = SysfsDevName(props.DevName)
	}

	if props.DevType == "partition" {
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

	if name, ok := readScsiTypeFromSysfs(bare); ok {
		return name
	}
	if strings.HasPrefix(bare, "sr") {
		return "rom"
	}
	return "disk"
}

func ResolveDeviceName(props UdevProperties) string {
	if props.DMName != "" {
		return "/dev/mapper/" + props.DMName
	}
	if strings.HasPrefix(props.DevName, "/dev/") {
		return props.DevName
	}
	if props.DevName != "" {
		return "/dev/" + props.DevName
	}
	return ""
}

func ResolveKernelName(devName string) string {
	if strings.HasPrefix(devName, "/dev/") {
		return devName
	}
	return "/dev/" + devName
}

// ResolveParentDevice returns PkName: sysfs parent for partitions (first);
// for dm-* / md* the first slave. Partition-first avoids md0p1 -> wrong path.
func ResolveParentDevice(devName string) string {
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

func isPartition(devName string) bool {
	partitionFile := filepath.Join(sysClassBlockPath, devName, "partition")
	_, err := os.Stat(partitionFile)
	return err == nil
}

// parentFromSysfs returns the parent block name from /sys/class/block/<part> symlink.
func parentFromSysfs(devName string) string {
	link, err := os.Readlink(filepath.Join(sysClassBlockPath, devName))
	if err != nil {
		return ""
	}
	parent := filepath.Base(filepath.Dir(link))
	if parent == "." || parent == "/" || parent == "block" {
		return ""
	}
	return parent
}

func SysfsDevName(devPath string) string {
	return strings.TrimPrefix(devPath, "/dev/")
}

// DeviceKey returns "major:minor".
func DeviceKey(major, minor int) string {
	return strconv.Itoa(major) + ":" + strconv.Itoa(minor)
}
