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
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	SysClassBlockPath = "/sys/class/block"
	SysBlockPath      = "/sys/block"

	sectorSize = 512
)

type SysFSDataProvider struct {
	classBlockPath string
	blockPath      string
}

func NewSysFSDataProvider(sysClassBlockPath, sysBlockPath string) *SysFSDataProvider {
	return &SysFSDataProvider{
		classBlockPath: sysClassBlockPath,
		blockPath:      sysBlockPath,
	}
}

// SysfsDevName strips the /dev/ prefix to get the kernel name
// as it appears in /sys/class/block/.
func (s *SysFSDataProvider) SysfsDevName(devPath string) string {
	return strings.TrimPrefix(devPath, "/dev/")
}

func (s *SysFSDataProvider) ReadSysfsSize(devName string) (int64, error) {
	devName = s.SysfsDevName(devName)
	data, err := os.ReadFile(filepath.Join(s.classBlockPath, devName, "size"))
	if err != nil {
		return 0, fmt.Errorf("reading size for %s: %w", devName, err)
	}
	sectors, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parsing size for %s: %w", devName, err)
	}
	return sectors * sectorSize, nil
}

func (s *SysFSDataProvider) ReadSysfsRotational(devName string) (bool, error) {
	devName = s.SysfsDevName(devName)
	devName = s.resolveParentForPartition(devName)
	data, err := os.ReadFile(filepath.Join(s.classBlockPath, devName, "queue", "rotational"))
	if err != nil {
		return false, fmt.Errorf("reading rotational for %s: %w", devName, err)
	}
	return strings.TrimSpace(string(data)) == "1", nil
}

// ReadSysfsHotplug mirrors util-linux sysfs_blkdev_is_hotpluggable() (lib/sysfs.c):
// walk the device path upward, checking for a "removable" file at each level;
// "removable" -> true, "fixed" -> false.
func (s *SysFSDataProvider) ReadSysfsHotplug(devName string) (bool, error) {
	devName = s.SysfsDevName(devName)
	symlinkPath := filepath.Join(s.classBlockPath, devName)
	resolved, err := filepath.EvalSymlinks(symlinkPath)
	if err != nil {
		return false, fmt.Errorf("resolving symlink for %s: %w", devName, err)
	}

	dir := resolved
	for dir != "/" && dir != "." {
		data, err := os.ReadFile(filepath.Join(dir, "removable"))
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

func (s *SysFSDataProvider) ReadSysfsSlaves(devName string) ([]string, error) {
	devName = s.SysfsDevName(devName)
	dir := filepath.Join(s.blockPath, devName, "slaves")
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
// so rotational reads target the backing device.
func (s *SysFSDataProvider) resolveParentForPartition(devName string) string {
	devName = s.SysfsDevName(devName)
	if !s.IsPartition(devName) {
		return devName
	}
	if parent := s.ParentFromSysfs(devName); parent != "" {
		return parent
	}
	return devName
}

func (s *SysFSDataProvider) IsPartition(devName string) bool {
	devName = s.SysfsDevName(devName)
	_, err := os.Stat(filepath.Join(s.classBlockPath, devName, "partition"))
	return err == nil
}

// ParentFromSysfs returns the parent block device name from the
// /sys/class/block/<part> symlink structure.
func (s *SysFSDataProvider) ParentFromSysfs(devName string) string {
	devName = s.SysfsDevName(devName)
	link, err := os.Readlink(filepath.Join(s.classBlockPath, devName))
	if err != nil {
		return ""
	}
	parent := filepath.Base(filepath.Dir(link))
	if parent == "." || parent == "/" || parent == "block" {
		return ""
	}
	return parent
}

// scsiTypeName maps sysfs device/type (SCSI peripheral type) to lsblk TYPE strings.
// Matches blkdev_scsi_type_to_name() in util-linux lib/blkdev.c.
func (s *SysFSDataProvider) scsiTypeName(code int) string {
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

func (s *SysFSDataProvider) ReadScsiTypeFromSysfs(devShort string) (string, bool) {
	if devShort == "" {
		return "", false
	}
	data, err := os.ReadFile(filepath.Join(s.classBlockPath, devShort, "device", "type"))
	if err != nil {
		return "", false
	}
	code, err := strconv.ParseInt(strings.TrimSpace(string(data)), 0, 32)
	if err != nil {
		return "", false
	}
	name := s.scsiTypeName(int(code))
	if name == "" {
		return "", false
	}
	return name, true
}
