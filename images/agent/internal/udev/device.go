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

// Filesystem paths are vars so tests can override them with tmpdir.
var (
	sysClassBlockPath = "/sys/class/block"
	sysBlockPath      = "/sys/block"
	runUdevDataPath   = "/run/udev/data"
	procSelfMountInfo = "/proc/1/mountinfo"
)

const sectorSize = 512

// UdevProperties contains typed fields extracted from a udev env map.
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

// ================== sysfs readers ==================

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

// ReadSysfsHotplug checks whether a block device is hotpluggable by walking
// the sysfs device ancestor chain and looking for a "removable" attribute
// containing "removable" (vs "fixed"). This mirrors lsblk's HOTPLUG column
// (sysfs_blkdev_is_hotpluggable in util-linux), which is different from the
// block device's own "removable" attribute (RM column).
//
// A USB hard drive has removable=0 at the block device level but a parent
// directory (the USB port) is marked "removable", so HOTPLUG=true.
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

// resolveParentForPartition returns the parent disk name for partitions
// so that rotational/removable can be read from the parent.
// For non-partitions returns devName unchanged.
func resolveParentForPartition(devName string) string {
	if !isPartition(devName) {
		return devName
	}
	if parent := parentFromSysfs(devName); parent != "" {
		return parent
	}
	return devName
}

// ================== udev DB ==================

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

// MergeEnvWithUdevDB enriches the env map from a crawler/netlink event
// with additional properties from /run/udev/data/bMAJOR:MINOR.
// Existing env keys take precedence over udev DB keys.
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

// ================== mapping ==================

func ParseUdevProperties(env map[string]string) UdevProperties {
	major, _ := strconv.Atoi(env["MAJOR"])
	minor, _ := strconv.Atoi(env["MINOR"])

	serial := env["ID_SERIAL_SHORT"]
	if serial == "" {
		serial = env["ID_SERIAL"]
	}

	devName := env["DEVNAME"]
	if devName != "" && !strings.HasPrefix(devName, "/dev/") {
		devName = "/dev/" + devName
	}

	return UdevProperties{
		DevName:  devName,
		DevType:  env["DEVTYPE"],
		Major:    major,
		Minor:    minor,
		Serial:   serial,
		Model:    env["ID_MODEL"],
		WWN:      env["ID_WWN"],
		FSType:   env["ID_FS_TYPE"],
		PartUUID: env["ID_PART_ENTRY_UUID"],
		DMName:   env["DM_NAME"],
		DMUUID:   env["DM_UUID"],
		MDLevel:  env["MD_LEVEL"],
	}
}

func ResolveDeviceType(props UdevProperties, devName string) string {
	bare := strings.TrimPrefix(devName, "/dev/")

	if strings.HasPrefix(bare, "loop") {
		return "loop"
	}
	if strings.HasPrefix(bare, "sr") {
		return "rom"
	}
	if strings.HasPrefix(props.DMUUID, "LVM-") {
		return "lvm"
	}
	if strings.HasPrefix(props.DMUUID, "mpath-") {
		return "mpath"
	}
	if props.MDLevel != "" {
		return props.MDLevel
	}
	if props.DevType == "partition" {
		return "part"
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

// ResolveParentDevice determines the parent kernel device name (PkName).
// For partitions: the parent disk. For DM/MD: the first slave device.
// Partition check comes first so that MD partitions (md0p1) resolve to
// their parent array (md0) rather than attempting a slaves lookup.
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

// isPartition checks if the device has a "partition" file in sysfs,
// which is the authoritative way to distinguish partitions from
// whole devices whose names happen to end with digits (loop0, nbd0, sr0).
func isPartition(devName string) bool {
	partitionFile := filepath.Join(sysClassBlockPath, devName, "partition")
	_, err := os.Stat(partitionFile)
	return err == nil
}

// parentFromSysfs reads the sysfs symlink for a partition device to determine
// its parent disk. In sysfs, /sys/class/block/<partition> is a symlink like
// ../../devices/.../block/<parent_disk>/<partition>, so the parent directory
// name in the symlink target is the parent disk name. This works for all
// naming schemes: sda1->sda, nvme0n1p1->nvme0n1, mmcblk0p1->mmcblk0, nbd0p1->nbd0.
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

func readSysfsFile(devName, fileName string) (string, error) {
	data, err := os.ReadFile(filepath.Join(sysClassBlockPath, devName, fileName))
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// DeviceKey builds a "major:minor" key from separate values.
func DeviceKey(major, minor int) string {
	return strconv.Itoa(major) + ":" + strconv.Itoa(minor)
}

// ReadDevMajorMinor reads the dev file from sysfs and returns "major:minor".
func ReadDevMajorMinor(devName string) (string, error) {
	data, err := readSysfsFile(devName, "dev")
	if err != nil {
		return "", fmt.Errorf("reading dev for %s: %w", devName, err)
	}
	return strings.TrimSpace(data), nil
}
