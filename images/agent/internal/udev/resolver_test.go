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

// ================== dmTypeFromDMUUID ==================

func TestDmTypeFromDMUUID(t *testing.T) {
	tests := []struct {
		name     string
		uuid     string
		expected string
	}{
		{"LVM volume", "LVM-abc123", "lvm"},
		{"multipath lower", "mpath-xyz", "mpath"},
		{"multipath upper", "MPATH-xyz", "mpath"},
		{"crypt dm", "CRYPT-LUKS2-deadbeef", "crypt"},
		{"empty uuid", "", "dm"},
		{"no dash", "LVM", "lvm"},
		{"kpartx part prefix", "part7-00002-abcdef", "part"},
		{"uuid starts with dash", "-something", "dm"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeSysfs(t)
			r := NewResolver(f.provider)
			assert.Equal(t, tt.expected, r.dmTypeFromDMUUID(tt.uuid))
		})
	}
}

// ================== Resolver.DeviceType ==================

func TestResolver_DeviceType(t *testing.T) {
	tests := []struct {
		name     string
		props    Properties
		devName  string
		expected string
	}{
		{"partition before loop name", Properties{DevType: "partition"}, "/dev/loop0p1", "part"},
		{"loop device", Properties{DevType: "disk"}, "/dev/loop0", "loop"},
		{"LVM volume", Properties{DMUUID: "LVM-abc123"}, "/dev/dm-0", "lvm"},
		{"multipath lower", Properties{DMUUID: "mpath-xyz"}, "/dev/dm-1", "mpath"},
		{"multipath upper prefix", Properties{DMUUID: "MPATH-xyz"}, "/dev/dm-1", "mpath"},
		{"crypt dm", Properties{DMUUID: "CRYPT-LUKS2-deadbeef"}, "/dev/dm-2", "crypt"},
		{"dm no uuid", Properties{}, "/dev/dm-9", "dm"},
		{"dm kpartx part prefix", Properties{DMUUID: "part7-00002-abcdef"}, "/dev/dm-3", "part"},
		{"MD RAID1", Properties{MDLevel: "raid1"}, "/dev/md0", "raid1"},
		{"MD RAID5", Properties{MDLevel: "raid5"}, "/dev/md1", "raid5"},
		{"MD level lowercased", Properties{MDLevel: "RAID10"}, "/dev/md2", "raid10"},
		{"MD no level", Properties{DevType: "disk"}, "/dev/md3", "md"},
		{"partition", Properties{DevType: "partition"}, "/dev/sda1", "part"},
		{"plain disk no sysfs", Properties{DevType: "disk"}, "/dev/sda", "disk"},
		{"nvme disk no sysfs", Properties{DevType: "disk"}, "/dev/nvme0n1", "disk"},
		{"CD-ROM sr0 no sysfs", Properties{DevType: "disk"}, "/dev/sr0", "rom"},
		{"CD-ROM sr1 no sysfs", Properties{}, "/dev/sr1", "rom"},
		{"empty props", Properties{}, "/dev/sda", "disk"},
		{"dm uuid starts with dash", Properties{DMUUID: "-something"}, "/dev/dm-5", "dm"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeSysfs(t)
			r := NewResolver(f.provider)
			assert.Equal(t, tt.expected, r.DeviceType(tt.props, tt.devName))
		})
	}
}

func TestResolver_DeviceType_ScsiTypeFromSysfs(t *testing.T) {
	f := newFakeSysfs(t)
	r := NewResolver(f.provider)

	f.writeFile(t, "sda", "device/type", "0\n")
	assert.Equal(t, "disk", r.DeviceType(Properties{DevType: "disk"}, "/dev/sda"))

	f.writeFile(t, "sdb", "device/type", "5\n")
	assert.Equal(t, "rom", r.DeviceType(Properties{DevType: "disk"}, "/dev/sdb"))

	f.writeFile(t, "sdc", "device/type", "0x0c\n")
	assert.Equal(t, "raid", r.DeviceType(Properties{DevType: "disk"}, "/dev/sdc"))
}

func TestResolver_DeviceType_PartitionFallbackWhenDevTypeMissing(t *testing.T) {
	f := newFakeSysfs(t)
	r := NewResolver(f.provider)
	f.createPartitionSymlink(t, "sda", "sda1")

	assert.Equal(t, "part", r.DeviceType(Properties{}, "/dev/sda1"))
}

// ================== Resolver.DeviceName ==================

func TestResolver_DeviceName(t *testing.T) {
	tests := []struct {
		name     string
		props    Properties
		expected string
	}{
		{"DM device", Properties{DMName: "vg0-lv0", DevName: "/dev/dm-0"}, "/dev/mapper/vg0-lv0"},
		{"regular with prefix", Properties{DevName: "/dev/sda"}, "/dev/sda"},
		{"regular without prefix", Properties{DevName: "sda"}, "/dev/sda"},
		{"empty", Properties{}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeSysfs(t)
			r := NewResolver(f.provider)
			assert.Equal(t, tt.expected, r.DeviceName(tt.props))
		})
	}
}

// ================== Resolver.KernelName ==================

func TestResolver_KernelName(t *testing.T) {
	f := newFakeSysfs(t)
	r := NewResolver(f.provider)

	assert.Equal(t, "/dev/sda", r.KernelName("sda"))
	assert.Equal(t, "/dev/sda", r.KernelName("/dev/sda"))
	assert.Equal(t, "/dev/nvme0n1", r.KernelName("nvme0n1"))
}

// ================== Resolver.ParentDevice ==================

func TestResolver_ParentDevice_Partitions(t *testing.T) {
	f := newFakeSysfs(t)
	r := NewResolver(f.provider)

	f.createPartitionSymlink(t, "sda", "sda1")
	f.createPartitionSymlink(t, "nvme0n1", "nvme0n1p1")
	f.createPartitionSymlink(t, "mmcblk0", "mmcblk0p1")
	f.createPartitionSymlink(t, "nbd0", "nbd0p1")

	assert.Equal(t, "/dev/sda", r.ParentDevice("sda1"))
	assert.Equal(t, "/dev/nvme0n1", r.ParentDevice("nvme0n1p1"))
	assert.Equal(t, "/dev/mmcblk0", r.ParentDevice("mmcblk0p1"))
	assert.Equal(t, "/dev/nbd0", r.ParentDevice("nbd0p1"))
	assert.Equal(t, "/dev/sda", r.ParentDevice("/dev/sda1"))
	assert.Equal(t, "", r.ParentDevice("sda"))
}

func TestResolver_ParentDevice_MDPartition(t *testing.T) {
	f := newFakeSysfs(t)
	r := NewResolver(f.provider)
	f.createPartitionSymlink(t, "md0", "md0p1")

	assert.Equal(t, "/dev/md0", r.ParentDevice("md0p1"))
}

func TestResolver_ParentDevice_MDArray(t *testing.T) {
	f := newFakeSysfs(t)
	r := NewResolver(f.provider)

	slavesDir := filepath.Join(f.blockPath, "md0", "slaves")
	require.NoError(t, os.MkdirAll(slavesDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(slavesDir, "sda1"), nil, 0o644))

	assert.Equal(t, "/dev/sda1", r.ParentDevice("md0"))
}

func TestResolver_ParentDevice_WholeDeviceWithDigits(t *testing.T) {
	f := newFakeSysfs(t)
	r := NewResolver(f.provider)

	for _, dev := range []string{"loop0", "loop1", "nbd0", "sr0"} {
		devDir := filepath.Join(f.classBlockPath, dev)
		require.NoError(t, os.MkdirAll(devDir, 0o755))
	}

	assert.Equal(t, "", r.ParentDevice("loop0"))
	assert.Equal(t, "", r.ParentDevice("loop1"))
	assert.Equal(t, "", r.ParentDevice("nbd0"))
	assert.Equal(t, "", r.ParentDevice("sr0"))
}

func TestResolver_ParentDevice_DM_WithSlaves(t *testing.T) {
	f := newFakeSysfs(t)
	r := NewResolver(f.provider)

	slavesDir := filepath.Join(f.blockPath, "dm-0", "slaves")
	require.NoError(t, os.MkdirAll(slavesDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(slavesDir, "sda"), nil, 0o644))

	assert.Equal(t, "/dev/sda", r.ParentDevice("dm-0"))
	assert.Equal(t, "/dev/sda", r.ParentDevice("/dev/dm-0"))
}

func TestResolver_ParentDevice_DM_NoSlaves(t *testing.T) {
	f := newFakeSysfs(t)
	r := NewResolver(f.provider)
	assert.Equal(t, "", r.ParentDevice("dm-0"))
}
