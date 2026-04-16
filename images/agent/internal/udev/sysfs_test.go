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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func withFakeSysfs(t *testing.T) string {
	t.Helper()
	root := t.TempDir()

	origSysClassBlock := sysClassBlockPath
	origSysBlock := sysBlockPath
	sysClassBlockPath = filepath.Join(root, "sys", "class", "block")
	sysBlockPath = filepath.Join(root, "sys", "block")
	t.Cleanup(func() {
		sysClassBlockPath = origSysClassBlock
		sysBlockPath = origSysBlock
	})
	return root
}

func writeFakeSysfsFile(t *testing.T, devName, fileName, content string) {
	t.Helper()
	dir := filepath.Join(sysClassBlockPath, devName, filepath.Dir(fileName))
	require.NoError(t, os.MkdirAll(dir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(sysClassBlockPath, devName, fileName), []byte(content), 0o644))
}

func writeFakeSysBlockFile(t *testing.T, devName, relPath, content string) {
	t.Helper()
	full := filepath.Join(sysBlockPath, devName, relPath)
	require.NoError(t, os.MkdirAll(filepath.Dir(full), 0o755))
	require.NoError(t, os.WriteFile(full, []byte(content), 0o644))
}

// createPartitionSymlink creates a fake sysfs partition entry:
// /sys/class/block/<part> -> ../../devices/.../block/<parent>/<part>
// with a "partition" file inside the target directory.
func createPartitionSymlink(t *testing.T, root, parent, part string) {
	t.Helper()
	targetDir := filepath.Join(root, "devices", "fake", "block", parent, part)
	require.NoError(t, os.MkdirAll(targetDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(targetDir, "partition"), []byte("1"), 0o644))
	require.NoError(t, os.MkdirAll(sysClassBlockPath, 0o755))
	require.NoError(t, os.Symlink(targetDir, filepath.Join(sysClassBlockPath, part)))
}

// ================== SysfsDevName ==================

func TestSysfsDevName(t *testing.T) {
	assert.Equal(t, "sda", SysfsDevName("/dev/sda"))
	assert.Equal(t, "nvme0n1", SysfsDevName("/dev/nvme0n1"))
	assert.Equal(t, "sda", SysfsDevName("sda"))
}

// ================== ReadSysfsSize ==================

func TestReadSysfsSize(t *testing.T) {
	withFakeSysfs(t)
	writeFakeSysfsFile(t, "sda", "size", "2097152\n")

	size, err := ReadSysfsSize("sda")
	require.NoError(t, err)
	assert.Equal(t, int64(2097152*512), size)
}

func TestReadSysfsSize_WithDevPrefix(t *testing.T) {
	withFakeSysfs(t)
	writeFakeSysfsFile(t, "sda", "size", "8\n")

	size, err := ReadSysfsSize("/dev/sda")
	require.NoError(t, err)
	assert.Equal(t, int64(8*512), size)
}

func TestReadSysfsSize_NotExist(t *testing.T) {
	withFakeSysfs(t)

	_, err := ReadSysfsSize("nonexistent")
	assert.Error(t, err)
}

func TestReadSysfsSize_ParseError(t *testing.T) {
	withFakeSysfs(t)
	writeFakeSysfsFile(t, "sda", "size", "not_a_number\n")

	_, err := ReadSysfsSize("sda")
	assert.Error(t, err)
}

// ================== ReadSysfsRotational ==================

func TestReadSysfsRotational_HDD(t *testing.T) {
	withFakeSysfs(t)
	writeFakeSysfsFile(t, "sda", "queue/rotational", "1\n")

	rota, err := ReadSysfsRotational("sda")
	require.NoError(t, err)
	assert.True(t, rota)
}

func TestReadSysfsRotational_SSD(t *testing.T) {
	withFakeSysfs(t)
	writeFakeSysfsFile(t, "nvme0n1", "queue/rotational", "0\n")

	rota, err := ReadSysfsRotational("nvme0n1")
	require.NoError(t, err)
	assert.False(t, rota)
}

func TestReadSysfsRotational_Partition_ReadsParent(t *testing.T) {
	root := withFakeSysfs(t)
	createPartitionSymlink(t, root, "sda", "sda1")
	writeFakeSysfsFile(t, "sda", "queue/rotational", "1\n")

	rota, err := ReadSysfsRotational("sda1")
	require.NoError(t, err)
	assert.True(t, rota, "partition should read rotational from parent")
}

func TestReadSysfsRotational_WithDevPrefix(t *testing.T) {
	withFakeSysfs(t)
	writeFakeSysfsFile(t, "sda", "queue/rotational", "0\n")

	rota, err := ReadSysfsRotational("/dev/sda")
	require.NoError(t, err)
	assert.False(t, rota)
}

// ================== ReadSysfsHotplug ==================

func TestReadSysfsHotplug_USBDisk(t *testing.T) {
	root := withFakeSysfs(t)

	devicesPath := filepath.Join(root, "devices", "pci0000:00", "0000:00:14.0", "usb1", "1-1", "1-1:1.0", "host0", "target0:0:0", "0:0:0:0", "block", "sdb")
	usbPortPath := filepath.Join(root, "devices", "pci0000:00", "0000:00:14.0", "usb1", "1-1")

	require.NoError(t, os.MkdirAll(devicesPath, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(usbPortPath, "removable"), []byte("1\n"), 0o644))

	require.NoError(t, os.MkdirAll(sysClassBlockPath, 0o755))
	require.NoError(t, os.Symlink(devicesPath, filepath.Join(sysClassBlockPath, "sdb")))

	hotplug, err := ReadSysfsHotplug("sdb")
	require.NoError(t, err)
	assert.True(t, hotplug, "USB disk should be hotpluggable")
}

func TestReadSysfsHotplug_WithDevPrefix(t *testing.T) {
	root := withFakeSysfs(t)

	devicesPath := filepath.Join(root, "devices", "pci0000:00", "0000:00:14.0", "usb1", "1-1", "1-1:1.0", "host0", "target0:0:0", "0:0:0:0", "block", "sdb")
	usbPortPath := filepath.Join(root, "devices", "pci0000:00", "0000:00:14.0", "usb1", "1-1")

	require.NoError(t, os.MkdirAll(devicesPath, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(usbPortPath, "removable"), []byte("1\n"), 0o644))

	require.NoError(t, os.MkdirAll(sysClassBlockPath, 0o755))
	require.NoError(t, os.Symlink(devicesPath, filepath.Join(sysClassBlockPath, "sdb")))

	hotplug, err := ReadSysfsHotplug("/dev/sdb")
	require.NoError(t, err)
	assert.True(t, hotplug)
}

func TestReadSysfsHotplug_SATADisk(t *testing.T) {
	root := withFakeSysfs(t)

	devicesPath := filepath.Join(root, "devices", "pci0000:00", "0000:00:1f.2", "ata1", "host0", "target0:0:0", "0:0:0:0", "block", "sda")
	pciPath := filepath.Join(root, "devices", "pci0000:00", "0000:00:1f.2")

	require.NoError(t, os.MkdirAll(devicesPath, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(pciPath, "removable"), []byte("0\n"), 0o644))

	require.NoError(t, os.MkdirAll(sysClassBlockPath, 0o755))
	require.NoError(t, os.Symlink(devicesPath, filepath.Join(sysClassBlockPath, "sda")))

	hotplug, err := ReadSysfsHotplug("sda")
	require.NoError(t, err)
	assert.False(t, hotplug, "SATA disk should not be hotpluggable")
}

func TestReadSysfsHotplug_NoRemovableFile(t *testing.T) {
	root := withFakeSysfs(t)

	devicesPath := filepath.Join(root, "devices", "virtual", "block", "dm-0")
	require.NoError(t, os.MkdirAll(devicesPath, 0o755))

	require.NoError(t, os.MkdirAll(sysClassBlockPath, 0o755))
	require.NoError(t, os.Symlink(devicesPath, filepath.Join(sysClassBlockPath, "dm-0")))

	hotplug, err := ReadSysfsHotplug("dm-0")
	require.NoError(t, err)
	assert.False(t, hotplug, "virtual device with no removable file should not be hotpluggable")
}

func TestReadSysfsHotplug_NotSymlink(t *testing.T) {
	withFakeSysfs(t)

	devDir := filepath.Join(sysClassBlockPath, "sda")
	require.NoError(t, os.MkdirAll(devDir, 0o755))

	hotplug, err := ReadSysfsHotplug("sda")
	require.NoError(t, err)
	assert.False(t, hotplug, "plain directory with no removable ancestors")
}

func TestReadSysfsHotplug_PartitionOnFixedDisk(t *testing.T) {
	root := withFakeSysfs(t)

	devicesPath := filepath.Join(root, "devices", "pci0000:00", "0000:00:1f.2", "ata1", "host0", "target0:0:0", "0:0:0:0", "block", "sda")
	pciPath := filepath.Join(root, "devices", "pci0000:00", "0000:00:1f.2")

	require.NoError(t, os.MkdirAll(devicesPath, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(pciPath, "removable"), []byte("0\n"), 0o644))

	require.NoError(t, os.MkdirAll(sysClassBlockPath, 0o755))
	require.NoError(t, os.Symlink(devicesPath, filepath.Join(sysClassBlockPath, "sda")))

	sda1DevicesPath := filepath.Join(devicesPath, "sda1")
	require.NoError(t, os.MkdirAll(sda1DevicesPath, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(sda1DevicesPath, "partition"), []byte("1"), 0o644))
	require.NoError(t, os.Symlink(sda1DevicesPath, filepath.Join(sysClassBlockPath, "sda1")))

	hotplug, err := ReadSysfsHotplug("sda1")
	require.NoError(t, err)
	assert.False(t, hotplug, "partition on fixed SATA disk should not be hotpluggable")
}

func TestReadSysfsHotplug_Partition_ReadsParent(t *testing.T) {
	root := withFakeSysfs(t)

	devicesPath := filepath.Join(root, "devices", "pci0000:00", "0000:00:14.0", "usb1", "1-1", "1-1:1.0", "host0", "target0:0:0", "0:0:0:0", "block", "sdb")
	usbPortPath := filepath.Join(root, "devices", "pci0000:00", "0000:00:14.0", "usb1", "1-1")

	require.NoError(t, os.MkdirAll(devicesPath, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(usbPortPath, "removable"), []byte("1\n"), 0o644))

	require.NoError(t, os.MkdirAll(sysClassBlockPath, 0o755))
	require.NoError(t, os.Symlink(devicesPath, filepath.Join(sysClassBlockPath, "sdb")))

	sdb1DevicesPath := filepath.Join(devicesPath, "sdb1")
	require.NoError(t, os.MkdirAll(sdb1DevicesPath, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(sdb1DevicesPath, "partition"), []byte("1"), 0o644))
	require.NoError(t, os.Symlink(sdb1DevicesPath, filepath.Join(sysClassBlockPath, "sdb1")))

	hotplug, err := ReadSysfsHotplug("sdb1")
	require.NoError(t, err)
	assert.True(t, hotplug, "partition on USB disk should be hotpluggable")
}

// ================== ReadSysfsSlaves ==================

func TestReadSysfsSlaves(t *testing.T) {
	withFakeSysfs(t)

	slavesDir := filepath.Join(sysBlockPath, "dm-0", "slaves")
	require.NoError(t, os.MkdirAll(slavesDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(slavesDir, "sda"), nil, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(slavesDir, "sdb"), nil, 0o644))

	slaves, err := ReadSysfsSlaves("dm-0")
	require.NoError(t, err)
	assert.Len(t, slaves, 2)
	assert.Contains(t, slaves, "sda")
	assert.Contains(t, slaves, "sdb")
}

func TestReadSysfsSlaves_WithDevPrefix(t *testing.T) {
	withFakeSysfs(t)

	slavesDir := filepath.Join(sysBlockPath, "dm-0", "slaves")
	require.NoError(t, os.MkdirAll(slavesDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(slavesDir, "sda"), nil, 0o644))

	slaves, err := ReadSysfsSlaves("/dev/dm-0")
	require.NoError(t, err)
	assert.Equal(t, []string{"sda"}, slaves)
}

func TestReadSysfsSlaves_NoDir(t *testing.T) {
	withFakeSysfs(t)

	slaves, err := ReadSysfsSlaves("sda")
	require.NoError(t, err)
	assert.Nil(t, slaves)
}

// ================== parentFromSysfs ==================

func TestParentFromSysfs(t *testing.T) {
	root := withFakeSysfs(t)

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
		{"mmcblk0p2 -> mmcblk0", "mmcblk0", "mmcblk0p2", "mmcblk0"},
		{"nbd0p1 -> nbd0", "nbd0", "nbd0p1", "nbd0"},
		{"vda1 -> vda", "vda", "vda1", "vda"},
		{"xvda1 -> xvda", "xvda", "xvda1", "xvda"},
		{"loop0p1 -> loop0", "loop0", "loop0p1", "loop0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			createPartitionSymlink(t, root, tt.parent, tt.part)
			assert.Equal(t, tt.expected, parentFromSysfs(tt.part))
		})
	}
}

func TestParentFromSysfs_NotSymlink(t *testing.T) {
	withFakeSysfs(t)

	devDir := filepath.Join(sysClassBlockPath, "sda")
	require.NoError(t, os.MkdirAll(devDir, 0o755))

	assert.Equal(t, "", parentFromSysfs("sda"))
}

func TestParentFromSysfs_Nonexistent(t *testing.T) {
	withFakeSysfs(t)
	assert.Equal(t, "", parentFromSysfs("nonexistent"))
}

// ================== ResolveParentDevice ==================

func TestResolveParentDevice_Partitions(t *testing.T) {
	root := withFakeSysfs(t)

	createPartitionSymlink(t, root, "sda", "sda1")
	createPartitionSymlink(t, root, "nvme0n1", "nvme0n1p1")
	createPartitionSymlink(t, root, "mmcblk0", "mmcblk0p1")
	createPartitionSymlink(t, root, "nbd0", "nbd0p1")

	assert.Equal(t, "/dev/sda", ResolveParentDevice("sda1"))
	assert.Equal(t, "/dev/nvme0n1", ResolveParentDevice("nvme0n1p1"))
	assert.Equal(t, "/dev/mmcblk0", ResolveParentDevice("mmcblk0p1"))
	assert.Equal(t, "/dev/nbd0", ResolveParentDevice("nbd0p1"))
	assert.Equal(t, "/dev/sda", ResolveParentDevice("/dev/sda1"))
	assert.Equal(t, "", ResolveParentDevice("sda"))
}

func TestResolveParentDevice_MDPartition(t *testing.T) {
	root := withFakeSysfs(t)

	createPartitionSymlink(t, root, "md0", "md0p1")

	assert.Equal(t, "/dev/md0", ResolveParentDevice("md0p1"))
}

func TestResolveParentDevice_MDArray(t *testing.T) {
	withFakeSysfs(t)

	slavesDir := filepath.Join(sysBlockPath, "md0", "slaves")
	require.NoError(t, os.MkdirAll(slavesDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(slavesDir, "sda1"), nil, 0o644))

	assert.Equal(t, "/dev/sda1", ResolveParentDevice("md0"))
}

func TestResolveParentDevice_WholeDeviceWithDigits(t *testing.T) {
	withFakeSysfs(t)

	for _, dev := range []string{"loop0", "loop1", "nbd0", "sr0"} {
		devDir := filepath.Join(sysClassBlockPath, dev)
		require.NoError(t, os.MkdirAll(devDir, 0o755))
	}

	assert.Equal(t, "", ResolveParentDevice("loop0"))
	assert.Equal(t, "", ResolveParentDevice("loop1"))
	assert.Equal(t, "", ResolveParentDevice("nbd0"))
	assert.Equal(t, "", ResolveParentDevice("sr0"))
}

func TestResolveParentDevice_DM_WithSlaves(t *testing.T) {
	withFakeSysfs(t)

	slavesDir := filepath.Join(sysBlockPath, "dm-0", "slaves")
	require.NoError(t, os.MkdirAll(slavesDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(slavesDir, "sda"), nil, 0o644))

	assert.Equal(t, "/dev/sda", ResolveParentDevice("dm-0"))
	assert.Equal(t, "/dev/sda", ResolveParentDevice("/dev/dm-0"))
}

func TestResolveParentDevice_DM_NoSlaves(t *testing.T) {
	withFakeSysfs(t)
	assert.Equal(t, "", ResolveParentDevice("dm-0"))
}
