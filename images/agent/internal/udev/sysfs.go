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

var (
	sysClassBlockPath = "/sys/class/block"
	sysBlockPath      = "/sys/block"
)

const sectorSize = 512

func ReadSysfsSize(devName string) (int64, error) {
	devName = SysfsDevName(devName)
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
	devName = SysfsDevName(devName)
	devName = resolveParentForPartition(devName)
	data, err := os.ReadFile(filepath.Join(sysClassBlockPath, devName, "queue", "rotational"))
	if err != nil {
		return false, fmt.Errorf("reading rotational for %s: %w", devName, err)
	}
	return strings.TrimSpace(string(data)) == "1", nil
}

// ReadSysfsHotplug mirrors util-linux sysfs_blkdev_is_hotpluggable() (lib/sysfs.c):
// walk the device path upward, checking /removable at each level; "1" -> hotpluggable,
// "0" -> fixed. The kernel writes "1" or "0" into the removable attribute.
func ReadSysfsHotplug(devName string) (bool, error) {
	devName = SysfsDevName(devName)
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
			if content == "1" {
				return true, nil
			}
			if content == "0" {
				return false, nil
			}
		}
		dir = filepath.Dir(dir)
	}

	return false, nil
}

func ReadSysfsSlaves(devName string) ([]string, error) {
	devName = SysfsDevName(devName)
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

func isPartition(devName string) bool {
	devName = SysfsDevName(devName)
	partitionFile := filepath.Join(sysClassBlockPath, devName, "partition")
	_, err := os.Stat(partitionFile)
	return err == nil
}

// parentFromSysfs returns the parent block name from /sys/class/block/<part> symlink.
func parentFromSysfs(devName string) string {
	devName = SysfsDevName(devName)
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

// resolveParentForPartition returns the parent disk for partitions so
// rotational/removable read the backing device; else devName unchanged.
func resolveParentForPartition(devName string) string {
	devName = SysfsDevName(devName)
	if !isPartition(devName) {
		return devName
	}
	if parent := parentFromSysfs(devName); parent != "" {
		return parent
	}
	return devName
}
