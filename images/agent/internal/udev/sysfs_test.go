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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeSysfs wraps a temporary directory that mimics the relevant parts of
// /sys, together with a SysFSDataProvider pointing at it. Tests use this
// instead of mutating global paths.
type fakeSysfs struct {
	root           string
	classBlockPath string
	blockPath      string
	provider       *SysFSDataProvider
}

func newFakeSysfs(t *testing.T) *fakeSysfs {
	t.Helper()
	root := t.TempDir()
	classBlockPath := filepath.Join(root, "sys", "class", "block")
	blockPath := filepath.Join(root, "sys", "block")
	return &fakeSysfs{
		root:           root,
		classBlockPath: classBlockPath,
		blockPath:      blockPath,
		provider:       NewSysFSDataProvider(classBlockPath, blockPath),
	}
}

func (f *fakeSysfs) writeFile(t *testing.T, devName, fileName, content string) {
	t.Helper()
	dir := filepath.Join(f.classBlockPath, devName, filepath.Dir(fileName))
	require.NoError(t, os.MkdirAll(dir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(f.classBlockPath, devName, fileName), []byte(content), 0o644))
}

// createPartitionSymlink creates a fake sysfs partition entry:
// /sys/class/block/<part> -> ../../devices/.../block/<parent>/<part>
// with a "partition" file inside the target directory.
func (f *fakeSysfs) createPartitionSymlink(t *testing.T, parent, part string) {
	t.Helper()
	targetDir := filepath.Join(f.root, "devices", "fake", "block", parent, part)
	require.NoError(t, os.MkdirAll(targetDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(targetDir, "partition"), []byte("1"), 0o644))
	require.NoError(t, os.MkdirAll(f.classBlockPath, 0o755))
	require.NoError(t, os.Symlink(targetDir, filepath.Join(f.classBlockPath, part)))
}

// ================== SysfsDevName ==================

func TestSysfsDevName(t *testing.T) {
	f := newFakeSysfs(t)
	assert.Equal(t, "sda", f.provider.SysfsDevName("/dev/sda"))
	assert.Equal(t, "nvme0n1", f.provider.SysfsDevName("/dev/nvme0n1"))
	assert.Equal(t, "sda", f.provider.SysfsDevName("sda"))
}

// ================== ReadSysfsSize ==================

func TestReadSysfsSize(t *testing.T) {
	f := newFakeSysfs(t)
	f.writeFile(t, "sda", "size", "2097152\n")

	size, err := f.provider.ReadSysfsSize("sda")
	require.NoError(t, err)
	assert.Equal(t, int64(2097152*512), size)
}

func TestReadSysfsSize_WithDevPrefix(t *testing.T) {
	f := newFakeSysfs(t)
	f.writeFile(t, "sda", "size", "8\n")

	size, err := f.provider.ReadSysfsSize("/dev/sda")
	require.NoError(t, err)
	assert.Equal(t, int64(8*512), size)
}

func TestReadSysfsSize_NotExist(t *testing.T) {
	f := newFakeSysfs(t)
	_, err := f.provider.ReadSysfsSize("nonexistent")
	assert.Error(t, err)
}

func TestReadSysfsSize_ParseError(t *testing.T) {
	f := newFakeSysfs(t)
	f.writeFile(t, "sda", "size", "not_a_number\n")

	_, err := f.provider.ReadSysfsSize("sda")
	assert.Error(t, err)
}

// ================== ReadSysfsRotational ==================

func TestReadSysfsRotational_HDD(t *testing.T) {
	f := newFakeSysfs(t)
	f.writeFile(t, "sda", "queue/rotational", "1\n")

	rota, err := f.provider.ReadSysfsRotational("sda")
	require.NoError(t, err)
	assert.True(t, rota)
}

func TestReadSysfsRotational_SSD(t *testing.T) {
	f := newFakeSysfs(t)
	f.writeFile(t, "nvme0n1", "queue/rotational", "0\n")

	rota, err := f.provider.ReadSysfsRotational("nvme0n1")
	require.NoError(t, err)
	assert.False(t, rota)
}

func TestReadSysfsRotational_Partition_ReadsParent(t *testing.T) {
	f := newFakeSysfs(t)
	f.createPartitionSymlink(t, "sda", "sda1")
	f.writeFile(t, "sda", "queue/rotational", "1\n")

	rota, err := f.provider.ReadSysfsRotational("sda1")
	require.NoError(t, err)
	assert.True(t, rota, "partition should read rotational from parent")
}

func TestReadSysfsRotational_WithDevPrefix(t *testing.T) {
	f := newFakeSysfs(t)
	f.writeFile(t, "sda", "queue/rotational", "0\n")

	rota, err := f.provider.ReadSysfsRotational("/dev/sda")
	require.NoError(t, err)
	assert.False(t, rota)
}

// ================== ReadSysfsHotplug ==================

func TestReadSysfsHotplug_USBDisk(t *testing.T) {
	f := newFakeSysfs(t)

	devicesPath := filepath.Join(f.root, "devices", "pci0000:00", "0000:00:14.0", "usb1", "1-1", "1-1:1.0", "host0", "target0:0:0", "0:0:0:0", "block", "sdb")
	usbPortPath := filepath.Join(f.root, "devices", "pci0000:00", "0000:00:14.0", "usb1", "1-1")

	require.NoError(t, os.MkdirAll(devicesPath, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(usbPortPath, "removable"), []byte("removable\n"), 0o644))

	require.NoError(t, os.MkdirAll(f.classBlockPath, 0o755))
	require.NoError(t, os.Symlink(devicesPath, filepath.Join(f.classBlockPath, "sdb")))

	hotplug, err := f.provider.ReadSysfsHotplug("sdb")
	require.NoError(t, err)
	assert.True(t, hotplug, "USB disk should be hotpluggable")
}

func TestReadSysfsHotplug_WithDevPrefix(t *testing.T) {
	f := newFakeSysfs(t)

	devicesPath := filepath.Join(f.root, "devices", "pci0000:00", "0000:00:14.0", "usb1", "1-1", "1-1:1.0", "host0", "target0:0:0", "0:0:0:0", "block", "sdb")
	usbPortPath := filepath.Join(f.root, "devices", "pci0000:00", "0000:00:14.0", "usb1", "1-1")

	require.NoError(t, os.MkdirAll(devicesPath, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(usbPortPath, "removable"), []byte("removable\n"), 0o644))

	require.NoError(t, os.MkdirAll(f.classBlockPath, 0o755))
	require.NoError(t, os.Symlink(devicesPath, filepath.Join(f.classBlockPath, "sdb")))

	hotplug, err := f.provider.ReadSysfsHotplug("/dev/sdb")
	require.NoError(t, err)
	assert.True(t, hotplug)
}

func TestReadSysfsHotplug_SATADisk(t *testing.T) {
	f := newFakeSysfs(t)

	devicesPath := filepath.Join(f.root, "devices", "pci0000:00", "0000:00:1f.2", "ata1", "host0", "target0:0:0", "0:0:0:0", "block", "sda")
	pciPath := filepath.Join(f.root, "devices", "pci0000:00", "0000:00:1f.2")

	require.NoError(t, os.MkdirAll(devicesPath, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(pciPath, "removable"), []byte("fixed\n"), 0o644))

	require.NoError(t, os.MkdirAll(f.classBlockPath, 0o755))
	require.NoError(t, os.Symlink(devicesPath, filepath.Join(f.classBlockPath, "sda")))

	hotplug, err := f.provider.ReadSysfsHotplug("sda")
	require.NoError(t, err)
	assert.False(t, hotplug, "SATA disk should not be hotpluggable")
}

func TestReadSysfsHotplug_NoRemovableFile(t *testing.T) {
	f := newFakeSysfs(t)

	devicesPath := filepath.Join(f.root, "devices", "virtual", "block", "dm-0")
	require.NoError(t, os.MkdirAll(devicesPath, 0o755))

	require.NoError(t, os.MkdirAll(f.classBlockPath, 0o755))
	require.NoError(t, os.Symlink(devicesPath, filepath.Join(f.classBlockPath, "dm-0")))

	hotplug, err := f.provider.ReadSysfsHotplug("dm-0")
	require.NoError(t, err)
	assert.False(t, hotplug, "virtual device with no removable file should not be hotpluggable")
}

func TestReadSysfsHotplug_NotSymlink(t *testing.T) {
	f := newFakeSysfs(t)

	devDir := filepath.Join(f.classBlockPath, "sda")
	require.NoError(t, os.MkdirAll(devDir, 0o755))

	hotplug, err := f.provider.ReadSysfsHotplug("sda")
	require.NoError(t, err)
	assert.False(t, hotplug, "plain directory with no removable ancestors")
}

func TestReadSysfsHotplug_PartitionOnFixedDisk(t *testing.T) {
	f := newFakeSysfs(t)

	devicesPath := filepath.Join(f.root, "devices", "pci0000:00", "0000:00:1f.2", "ata1", "host0", "target0:0:0", "0:0:0:0", "block", "sda")
	pciPath := filepath.Join(f.root, "devices", "pci0000:00", "0000:00:1f.2")

	require.NoError(t, os.MkdirAll(devicesPath, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(pciPath, "removable"), []byte("fixed\n"), 0o644))

	require.NoError(t, os.MkdirAll(f.classBlockPath, 0o755))
	require.NoError(t, os.Symlink(devicesPath, filepath.Join(f.classBlockPath, "sda")))

	sda1DevicesPath := filepath.Join(devicesPath, "sda1")
	require.NoError(t, os.MkdirAll(sda1DevicesPath, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(sda1DevicesPath, "partition"), []byte("1"), 0o644))
	require.NoError(t, os.Symlink(sda1DevicesPath, filepath.Join(f.classBlockPath, "sda1")))

	hotplug, err := f.provider.ReadSysfsHotplug("sda1")
	require.NoError(t, err)
	assert.False(t, hotplug, "partition on fixed SATA disk should not be hotpluggable")
}

func TestReadSysfsHotplug_PartitionOnUSBDisk(t *testing.T) {
	f := newFakeSysfs(t)

	devicesPath := filepath.Join(f.root, "devices", "pci0000:00", "0000:00:14.0", "usb1", "1-1", "1-1:1.0", "host0", "target0:0:0", "0:0:0:0", "block", "sdb")
	usbPortPath := filepath.Join(f.root, "devices", "pci0000:00", "0000:00:14.0", "usb1", "1-1")

	require.NoError(t, os.MkdirAll(devicesPath, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(usbPortPath, "removable"), []byte("removable\n"), 0o644))

	require.NoError(t, os.MkdirAll(f.classBlockPath, 0o755))
	require.NoError(t, os.Symlink(devicesPath, filepath.Join(f.classBlockPath, "sdb")))

	sdb1DevicesPath := filepath.Join(devicesPath, "sdb1")
	require.NoError(t, os.MkdirAll(sdb1DevicesPath, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(sdb1DevicesPath, "partition"), []byte("1"), 0o644))
	require.NoError(t, os.Symlink(sdb1DevicesPath, filepath.Join(f.classBlockPath, "sdb1")))

	hotplug, err := f.provider.ReadSysfsHotplug("sdb1")
	require.NoError(t, err)
	assert.True(t, hotplug, "partition on USB disk should be hotpluggable")
}

// ================== ReadSysfsSlaves ==================

func TestReadSysfsSlaves(t *testing.T) {
	f := newFakeSysfs(t)

	slavesDir := filepath.Join(f.blockPath, "dm-0", "slaves")
	require.NoError(t, os.MkdirAll(slavesDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(slavesDir, "sda"), nil, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(slavesDir, "sdb"), nil, 0o644))

	slaves, err := f.provider.ReadSysfsSlaves("dm-0")
	require.NoError(t, err)
	assert.Len(t, slaves, 2)
	assert.Contains(t, slaves, "sda")
	assert.Contains(t, slaves, "sdb")
}

func TestReadSysfsSlaves_WithDevPrefix(t *testing.T) {
	f := newFakeSysfs(t)

	slavesDir := filepath.Join(f.blockPath, "dm-0", "slaves")
	require.NoError(t, os.MkdirAll(slavesDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(slavesDir, "sda"), nil, 0o644))

	slaves, err := f.provider.ReadSysfsSlaves("/dev/dm-0")
	require.NoError(t, err)
	assert.Equal(t, []string{"sda"}, slaves)
}

func TestReadSysfsSlaves_NoDir(t *testing.T) {
	f := newFakeSysfs(t)
	slaves, err := f.provider.ReadSysfsSlaves("sda")
	require.NoError(t, err)
	assert.Nil(t, slaves)
}

// ================== IsPartition ==================

func TestIsPartition_True(t *testing.T) {
	f := newFakeSysfs(t)
	f.writeFile(t, "sda1", "partition", "1\n")

	assert.True(t, f.provider.IsPartition("sda1"))
}

func TestIsPartition_False(t *testing.T) {
	f := newFakeSysfs(t)
	require.NoError(t, os.MkdirAll(filepath.Join(f.classBlockPath, "sda"), 0o755))

	assert.False(t, f.provider.IsPartition("sda"))
}

// ================== ParentFromSysfs ==================

func TestParentFromSysfs(t *testing.T) {
	tests := []struct {
		name     string
		parent   string
		part     string
		expected string
	}{
		{"sda1 -> sda", "sda", "sda1", "sda"},
		{"sda2 -> sda", "sda", "sda2", "sda"},
		{"nvme0n1p1 -> nvme0n1", "nvme0n1", "nvme0n1p1", "nvme0n1"},
		{"nvme0n1p12 -> nvme0n1", "nvme0n1", "nvme0n1p12", "nvme0n1"},
		{"mmcblk0p1 -> mmcblk0", "mmcblk0", "mmcblk0p1", "mmcblk0"},
		{"nbd0p1 -> nbd0", "nbd0", "nbd0p1", "nbd0"},
		{"vda1 -> vda", "vda", "vda1", "vda"},
		{"loop0p1 -> loop0", "loop0", "loop0p1", "loop0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeSysfs(t)
			f.createPartitionSymlink(t, tt.parent, tt.part)
			assert.Equal(t, tt.expected, f.provider.ParentFromSysfs(tt.part))
		})
	}
}

func TestParentFromSysfs_NotSymlink(t *testing.T) {
	f := newFakeSysfs(t)
	devDir := filepath.Join(f.classBlockPath, "sda")
	require.NoError(t, os.MkdirAll(devDir, 0o755))

	assert.Equal(t, "", f.provider.ParentFromSysfs("sda"))
}

func TestParentFromSysfs_Nonexistent(t *testing.T) {
	f := newFakeSysfs(t)
	assert.Equal(t, "", f.provider.ParentFromSysfs("nonexistent"))
}

// ================== scsiTypeName ==================

func TestScsiTypeName(t *testing.T) {
	f := newFakeSysfs(t)
	assert.Equal(t, "disk", f.provider.scsiTypeName(0x00))
	assert.Equal(t, "rom", f.provider.scsiTypeName(0x05))
	assert.Equal(t, "raid", f.provider.scsiTypeName(0x0c))
	assert.Equal(t, "", f.provider.scsiTypeName(0xFF))
}

// ================== ReadScsiTypeFromSysfs ==================

func TestReadScsiTypeFromSysfs(t *testing.T) {
	f := newFakeSysfs(t)
	f.writeFile(t, "sda", "device/type", "0\n")

	name, ok := f.provider.ReadScsiTypeFromSysfs("sda")
	assert.True(t, ok)
	assert.Equal(t, "disk", name)
}

func TestReadScsiTypeFromSysfs_ROM(t *testing.T) {
	f := newFakeSysfs(t)
	f.writeFile(t, "sdb", "device/type", "5\n")

	name, ok := f.provider.ReadScsiTypeFromSysfs("sdb")
	assert.True(t, ok)
	assert.Equal(t, "rom", name)
}

func TestReadScsiTypeFromSysfs_Hex(t *testing.T) {
	f := newFakeSysfs(t)
	f.writeFile(t, "sdc", "device/type", "0x0c\n")

	name, ok := f.provider.ReadScsiTypeFromSysfs("sdc")
	assert.True(t, ok)
	assert.Equal(t, "raid", name)
}

func TestReadScsiTypeFromSysfs_NoFile(t *testing.T) {
	f := newFakeSysfs(t)
	_, ok := f.provider.ReadScsiTypeFromSysfs("sda")
	assert.False(t, ok)
}

func TestReadScsiTypeFromSysfs_EmptyName(t *testing.T) {
	f := newFakeSysfs(t)
	_, ok := f.provider.ReadScsiTypeFromSysfs("")
	assert.False(t, ok)
}
