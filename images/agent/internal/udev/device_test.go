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

// withFakeSysfs creates a temporary directory tree mimicking /sys/class/block
// and /sys/block, overrides the package-level path vars, and restores them
// after the test.
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

// withFakeUdevDB creates a temporary directory mimicking /run/udev/data
// and overrides the package-level path var.
func withFakeUdevDB(t *testing.T) string {
	t.Helper()
	root := t.TempDir()

	origRunUdevData := runUdevDataPath
	runUdevDataPath = filepath.Join(root, "run", "udev", "data")
	t.Cleanup(func() {
		runUdevDataPath = origRunUdevData
	})

	require.NoError(t, os.MkdirAll(runUdevDataPath, 0o755))
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

// ================== ParseUdevProperties ==================

func TestParseUdevProperties_FullEnv(t *testing.T) {
	env := map[string]string{
		"DEVNAME":           "sda",
		"DEVTYPE":           "disk",
		"MAJOR":             "8",
		"MINOR":             "0",
		"ID_SERIAL_SHORT":   "WD-ABC123",
		"ID_SERIAL":         "WDC_WD10EZEX_WD-ABC123",
		"ID_MODEL":          "WDC_WD10EZEX",
		"ID_WWN":            "0x50014ee2b5e7c5a0",
		"ID_FS_TYPE":        "ext4",
		"ID_PART_ENTRY_UUID": "abcd-1234",
		"DM_NAME":           "",
		"DM_UUID":           "",
		"MD_LEVEL":          "",
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
		{"loop device", UdevProperties{}, "/dev/loop0", "loop"},
		{"loop with partition suffix", UdevProperties{}, "/dev/loop0p1", "loop"},
		{"LVM volume", UdevProperties{DMUUID: "LVM-abc123"}, "/dev/dm-0", "lvm"},
		{"multipath", UdevProperties{DMUUID: "mpath-xyz"}, "/dev/dm-1", "mpath"},
		{"MD RAID1", UdevProperties{MDLevel: "raid1"}, "/dev/md0", "raid1"},
		{"MD RAID5", UdevProperties{MDLevel: "raid5"}, "/dev/md1", "raid5"},
		{"partition", UdevProperties{DevType: "partition"}, "/dev/sda1", "part"},
		{"plain disk", UdevProperties{DevType: "disk"}, "/dev/sda", "disk"},
		{"nvme disk", UdevProperties{DevType: "disk"}, "/dev/nvme0n1", "disk"},
		{"empty props", UdevProperties{}, "/dev/sda", "disk"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, ResolveDeviceType(tt.props, tt.devName))
		})
	}
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

// ================== parentFromSysfs ==================

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
	assert.Equal(t, "", ResolveParentDevice("sda"))
}

func TestResolveParentDevice_MDPartition(t *testing.T) {
	root := withFakeSysfs(t)

	// md0p1 is a partition on MD array md0
	createPartitionSymlink(t, root, "md0", "md0p1")

	assert.Equal(t, "/dev/md0", ResolveParentDevice("md0p1"))
}

func TestResolveParentDevice_MDArray(t *testing.T) {
	withFakeSysfs(t)

	// md0 is a whole MD array with slaves sda1, sdb1
	slavesDir := filepath.Join(sysBlockPath, "md0", "slaves")
	require.NoError(t, os.MkdirAll(slavesDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(slavesDir, "sda1"), nil, 0o644))

	assert.Equal(t, "/dev/sda1", ResolveParentDevice("md0"))
}

func TestResolveParentDevice_WholeDeviceWithDigits(t *testing.T) {
	withFakeSysfs(t)

	// loop0, nbd0, sr0 have digits in name but no "partition" file in sysfs
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
}

func TestResolveParentDevice_DM_NoSlaves(t *testing.T) {
	withFakeSysfs(t)
	assert.Equal(t, "", ResolveParentDevice("dm-0"))
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

// ================== ReadSysfsRemovable ==================

func TestReadSysfsRemovable_Fixed(t *testing.T) {
	withFakeSysfs(t)
	writeFakeSysfsFile(t, "sda", "removable", "0\n")

	removable, err := ReadSysfsRemovable("sda")
	require.NoError(t, err)
	assert.False(t, removable)
}

func TestReadSysfsRemovable_USB(t *testing.T) {
	withFakeSysfs(t)
	writeFakeSysfsFile(t, "sdb", "removable", "1\n")

	removable, err := ReadSysfsRemovable("sdb")
	require.NoError(t, err)
	assert.True(t, removable)
}

func TestReadSysfsRemovable_Partition_ReadsParent(t *testing.T) {
	root := withFakeSysfs(t)
	createPartitionSymlink(t, root, "sdb", "sdb1")
	writeFakeSysfsFile(t, "sdb", "removable", "1\n")

	removable, err := ReadSysfsRemovable("sdb1")
	require.NoError(t, err)
	assert.True(t, removable, "partition should read removable from parent")
}

// ================== ReadSysfsHotplug ==================

func TestReadSysfsHotplug_USBDisk(t *testing.T) {
	root := withFakeSysfs(t)

	// Simulate a USB disk: /sys/class/block/sdb -> ../../devices/pci.../usb1/.../block/sdb
	// The USB port ancestor has removable=removable, but the block device itself doesn't.
	devicesPath := filepath.Join(root, "devices", "pci0000:00", "0000:00:14.0", "usb1", "1-1", "1-1:1.0", "host0", "target0:0:0", "0:0:0:0", "block", "sdb")
	usbPortPath := filepath.Join(root, "devices", "pci0000:00", "0000:00:14.0", "usb1", "1-1")

	require.NoError(t, os.MkdirAll(devicesPath, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(usbPortPath, "removable"), []byte("removable\n"), 0o644))

	// Create symlink: /sys/class/block/sdb -> devicesPath
	require.NoError(t, os.MkdirAll(sysClassBlockPath, 0o755))
	require.NoError(t, os.Symlink(devicesPath, filepath.Join(sysClassBlockPath, "sdb")))

	hotplug, err := ReadSysfsHotplug("sdb")
	require.NoError(t, err)
	assert.True(t, hotplug, "USB disk should be hotpluggable")
}

func TestReadSysfsHotplug_SATADisk(t *testing.T) {
	root := withFakeSysfs(t)

	// Simulate a SATA disk with "fixed" at the PCI level
	devicesPath := filepath.Join(root, "devices", "pci0000:00", "0000:00:1f.2", "ata1", "host0", "target0:0:0", "0:0:0:0", "block", "sda")
	pciPath := filepath.Join(root, "devices", "pci0000:00", "0000:00:1f.2")

	require.NoError(t, os.MkdirAll(devicesPath, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(pciPath, "removable"), []byte("fixed\n"), 0o644))

	require.NoError(t, os.MkdirAll(sysClassBlockPath, 0o755))
	require.NoError(t, os.Symlink(devicesPath, filepath.Join(sysClassBlockPath, "sda")))

	hotplug, err := ReadSysfsHotplug("sda")
	require.NoError(t, err)
	assert.False(t, hotplug, "SATA disk should not be hotpluggable")
}

func TestReadSysfsHotplug_NoRemovableFile(t *testing.T) {
	root := withFakeSysfs(t)

	// Simulate a device where no ancestor has a "removable" file
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

	// If the entry is a plain directory (not a symlink), EvalSymlinks still works
	devDir := filepath.Join(sysClassBlockPath, "sda")
	require.NoError(t, os.MkdirAll(devDir, 0o755))

	hotplug, err := ReadSysfsHotplug("sda")
	require.NoError(t, err)
	assert.False(t, hotplug, "plain directory with no removable ancestors")
}

func TestReadSysfsHotplug_Partition_ReadsParent(t *testing.T) {
	root := withFakeSysfs(t)

	// Simulate a USB disk: /sys/class/block/sdb -> deep device path with USB ancestor
	devicesPath := filepath.Join(root, "devices", "pci0000:00", "0000:00:14.0", "usb1", "1-1", "1-1:1.0", "host0", "target0:0:0", "0:0:0:0", "block", "sdb")
	usbPortPath := filepath.Join(root, "devices", "pci0000:00", "0000:00:14.0", "usb1", "1-1")

	require.NoError(t, os.MkdirAll(devicesPath, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(usbPortPath, "removable"), []byte("removable\n"), 0o644))

	require.NoError(t, os.MkdirAll(sysClassBlockPath, 0o755))
	require.NoError(t, os.Symlink(devicesPath, filepath.Join(sysClassBlockPath, "sdb")))

	// sdb1 is a partition: symlink points to .../block/sdb/sdb1, with "partition" file
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

func TestReadSysfsSlaves_NoDir(t *testing.T) {
	withFakeSysfs(t)

	slaves, err := ReadSysfsSlaves("sda")
	require.NoError(t, err)
	assert.Nil(t, slaves)
}

// ================== ReadUdevDB ==================

func TestReadUdevDB_ValidFile(t *testing.T) {
	withFakeUdevDB(t)

	content := `N:sda
S:disk/by-id/wwn-0x50014ee2b5e7c5a0
E:DEVNAME=/dev/sda
E:DEVTYPE=disk
E:ID_SERIAL_SHORT=WD-ABC123
E:ID_MODEL=WDC_WD10EZEX
E:ID_WWN=0x50014ee2b5e7c5a0
`
	require.NoError(t, os.WriteFile(filepath.Join(runUdevDataPath, "b8:0"), []byte(content), 0o644))

	props, err := ReadUdevDB(8, 0)
	require.NoError(t, err)

	assert.Equal(t, "/dev/sda", props["DEVNAME"])
	assert.Equal(t, "disk", props["DEVTYPE"])
	assert.Equal(t, "WD-ABC123", props["ID_SERIAL_SHORT"])
	assert.Equal(t, "WDC_WD10EZEX", props["ID_MODEL"])
	assert.Equal(t, "0x50014ee2b5e7c5a0", props["ID_WWN"])
}

func TestReadUdevDB_IgnoresNonELines(t *testing.T) {
	withFakeUdevDB(t)

	content := `N:sda
S:disk/by-id/wwn-0x50014ee2b5e7c5a0
I:12345
E:DEVNAME=/dev/sda
`
	require.NoError(t, os.WriteFile(filepath.Join(runUdevDataPath, "b8:0"), []byte(content), 0o644))

	props, err := ReadUdevDB(8, 0)
	require.NoError(t, err)
	assert.Len(t, props, 1)
	assert.Equal(t, "/dev/sda", props["DEVNAME"])
}

func TestReadUdevDB_FileNotExist(t *testing.T) {
	withFakeUdevDB(t)

	_, err := ReadUdevDB(99, 99)
	assert.Error(t, err)
}

// ================== MergeEnvWithUdevDB ==================

func TestMergeEnvWithUdevDB_EventEnvTakesPriority(t *testing.T) {
	withFakeUdevDB(t)

	dbContent := `E:DEVNAME=/dev/sda
E:ID_MODEL=OldModel
E:ID_WWN=0xold
`
	require.NoError(t, os.WriteFile(filepath.Join(runUdevDataPath, "b8:0"), []byte(dbContent), 0o644))

	env := map[string]string{
		"MAJOR":    "8",
		"MINOR":    "0",
		"DEVNAME":  "/dev/sda",
		"ID_MODEL": "NewModel",
	}

	merged := MergeEnvWithUdevDB(env)

	assert.Equal(t, "NewModel", merged["ID_MODEL"], "event env overrides DB")
	assert.Equal(t, "0xold", merged["ID_WWN"], "DB value preserved when not in event")
	assert.Equal(t, "/dev/sda", merged["DEVNAME"])
}

func TestMergeEnvWithUdevDB_NoMajorMinor(t *testing.T) {
	env := map[string]string{"DEVNAME": "/dev/sda"}
	result := MergeEnvWithUdevDB(env)
	assert.Equal(t, env, result, "returns original env when MAJOR/MINOR missing")
}

func TestMergeEnvWithUdevDB_InvalidMajorMinor(t *testing.T) {
	env := map[string]string{"MAJOR": "abc", "MINOR": "xyz"}
	result := MergeEnvWithUdevDB(env)
	assert.Equal(t, env, result, "returns original env when MAJOR/MINOR not parseable")
}

func TestMergeEnvWithUdevDB_DBNotFound(t *testing.T) {
	withFakeUdevDB(t)

	env := map[string]string{"MAJOR": "99", "MINOR": "99", "DEVNAME": "/dev/sda"}
	result := MergeEnvWithUdevDB(env)
	assert.Equal(t, env, result, "returns original env when DB file missing")
}

// ================== DeviceKey ==================

func TestDeviceKey(t *testing.T) {
	assert.Equal(t, "8:0", DeviceKey(8, 0))
	assert.Equal(t, "259:1", DeviceKey(259, 1))
}
