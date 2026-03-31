//go:build linux

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
	"testing"

	"github.com/pilebones/go-udev/crawler"
	"github.com/pilebones/go-udev/netlink"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeEnv(major, minor, devname string) map[string]string {
	return map[string]string{
		"MAJOR":   major,
		"MINOR":   minor,
		"DEVNAME": devname,
	}
}

// ================== HandleEvent ==================

func TestHandleEvent_Add(t *testing.T) {
	dm := NewDeviceMap()
	event := &netlink.UEvent{
		Action: netlink.ADD,
		KObj:   "/devices/pci/block/sda",
		Env:    makeEnv("8", "0", "sda"),
	}
	dm.HandleEvent(event)

	assert.Equal(t, 1, dm.Len())
}

func TestHandleEvent_Change_Updates(t *testing.T) {
	dm := NewDeviceMap()
	dm.HandleEvent(&netlink.UEvent{
		Action: netlink.ADD,
		Env:    makeEnv("8", "0", "sda"),
	})

	env := makeEnv("8", "0", "sda")
	env["ID_FS_TYPE"] = "ext4"
	dm.HandleEvent(&netlink.UEvent{
		Action: netlink.CHANGE,
		Env:    env,
	})

	assert.Equal(t, 1, dm.Len())
	dm.mu.RLock()
	stored := dm.devices["8:0"]
	dm.mu.RUnlock()
	assert.Equal(t, "ext4", stored["ID_FS_TYPE"])
}

func TestHandleEvent_Remove(t *testing.T) {
	dm := NewDeviceMap()
	dm.HandleEvent(&netlink.UEvent{
		Action: netlink.ADD,
		Env:    makeEnv("8", "0", "sda"),
	})
	assert.Equal(t, 1, dm.Len())

	dm.HandleEvent(&netlink.UEvent{
		Action: netlink.REMOVE,
		Env:    makeEnv("8", "0", "sda"),
	})
	assert.Equal(t, 0, dm.Len())
}

func TestHandleEvent_NoMajorMinor_Ignored(t *testing.T) {
	dm := NewDeviceMap()
	dm.HandleEvent(&netlink.UEvent{
		Action: netlink.ADD,
		Env:    map[string]string{"DEVNAME": "sda"},
	})
	assert.Equal(t, 0, dm.Len())
}

func TestHandleEvent_Sequence_AddChangeRemove(t *testing.T) {
	dm := NewDeviceMap()

	dm.HandleEvent(&netlink.UEvent{Action: netlink.ADD, Env: makeEnv("8", "0", "sda")})
	assert.Equal(t, 1, dm.Len())

	dm.HandleEvent(&netlink.UEvent{Action: netlink.ADD, Env: makeEnv("8", "1", "sda1")})
	assert.Equal(t, 2, dm.Len())

	env := makeEnv("8", "0", "sda")
	env["ID_MODEL"] = "SSD"
	dm.HandleEvent(&netlink.UEvent{Action: netlink.CHANGE, Env: env})
	assert.Equal(t, 2, dm.Len())

	dm.HandleEvent(&netlink.UEvent{Action: netlink.REMOVE, Env: makeEnv("8", "1", "sda1")})
	assert.Equal(t, 1, dm.Len())

	dm.HandleEvent(&netlink.UEvent{Action: netlink.REMOVE, Env: makeEnv("8", "0", "sda")})
	assert.Equal(t, 0, dm.Len())
}

func TestHandleEvent_Bind(t *testing.T) {
	dm := NewDeviceMap()
	dm.HandleEvent(&netlink.UEvent{Action: netlink.BIND, Env: makeEnv("8", "0", "sda")})
	assert.Equal(t, 1, dm.Len())
}

func TestHandleEvent_Unbind(t *testing.T) {
	dm := NewDeviceMap()
	dm.HandleEvent(&netlink.UEvent{Action: netlink.ADD, Env: makeEnv("8", "0", "sda")})
	dm.HandleEvent(&netlink.UEvent{Action: netlink.UNBIND, Env: makeEnv("8", "0", "sda")})
	assert.Equal(t, 0, dm.Len())
}

// ================== FillFromCrawler ==================

func TestFillFromCrawler_BulkLoad(t *testing.T) {
	dm := NewDeviceMap()
	devices := []crawler.Device{
		{KObj: "/devices/pci/block/sda", Env: makeEnv("8", "0", "sda")},
		{KObj: "/devices/pci/block/sda/sda1", Env: makeEnv("8", "1", "sda1")},
		{KObj: "/devices/pci/block/sdb", Env: makeEnv("8", "16", "sdb")},
	}
	dm.FillFromCrawler(devices)
	assert.Equal(t, 3, dm.Len())
}

func TestFillFromCrawler_SkipsNoMajorMinor(t *testing.T) {
	dm := NewDeviceMap()
	devices := []crawler.Device{
		{KObj: "/devices/pci/block/sda", Env: makeEnv("8", "0", "sda")},
		{KObj: "/devices/virtual/block/loop0", Env: map[string]string{"DEVNAME": "loop0"}},
	}
	dm.FillFromCrawler(devices)
	assert.Equal(t, 1, dm.Len())
}

func TestFillFromCrawler_Empty(t *testing.T) {
	dm := NewDeviceMap()
	dm.FillFromCrawler(nil)
	assert.Equal(t, 0, dm.Len())
}

// ================== Snapshot ==================

func TestSnapshot_WithFakeSysfs(t *testing.T) {
	root := withFakeSysfs(t)

	fakeMountInfo := `22 1 8:0 / / rw,relatime shared:1 - ext4 /dev/sda rw
`
	mountInfoPath := root + "/mountinfo"
	require.NoError(t, writeFile(mountInfoPath, fakeMountInfo))
	origMountInfo := procSelfMountInfo
	procSelfMountInfo = mountInfoPath
	t.Cleanup(func() { procSelfMountInfo = origMountInfo })

	writeFakeSysfsFile(t, "sda", "size", "2097152\n")
	writeFakeSysfsFile(t, "sda", "queue/rotational", "1\n")
	writeFakeSysfsFile(t, "sda", "removable", "0\n")

	dm := NewDeviceMap()
	dm.HandleEvent(&netlink.UEvent{
		Action: netlink.ADD,
		Env: map[string]string{
			"MAJOR":     "8",
			"MINOR":     "0",
			"DEVNAME":   "sda",
			"DEVTYPE":   "disk",
			"ID_MODEL":  "TestDisk",
			"ID_SERIAL": "SN123",
		},
	})

	devices := dm.Snapshot()
	require.Len(t, devices, 1)

	dev := devices[0]
	assert.Equal(t, "/dev/sda", dev.Name)
	assert.Equal(t, "/dev/sda", dev.KName)
	assert.Equal(t, "disk", dev.Type)
	assert.Equal(t, int64(2097152*512), dev.Size.Value())
	assert.True(t, dev.Rota)
	assert.False(t, dev.HotPlug)
	assert.Equal(t, "TestDisk", dev.Model)
	assert.Equal(t, "SN123", dev.Serial)
	assert.Equal(t, "/", dev.MountPoint)
	assert.Equal(t, "", dev.PkName)
}

func TestSnapshot_SkipsDevicesWithoutSize(t *testing.T) {
	withFakeSysfs(t)

	dm := NewDeviceMap()
	dm.HandleEvent(&netlink.UEvent{
		Action: netlink.ADD,
		Env:    makeEnv("8", "0", "sda"),
	})

	devices := dm.Snapshot()
	assert.Empty(t, devices, "device without sysfs size file should be skipped")
}

func TestSnapshot_Empty(t *testing.T) {
	dm := NewDeviceMap()
	devices := dm.Snapshot()
	assert.Empty(t, devices)
}

func TestSnapshot_DMDevice(t *testing.T) {
	root := withFakeSysfs(t)

	mountInfoPath := root + "/mountinfo"
	require.NoError(t, writeFile(mountInfoPath, ""))
	origMountInfo := procSelfMountInfo
	procSelfMountInfo = mountInfoPath
	t.Cleanup(func() { procSelfMountInfo = origMountInfo })

	writeFakeSysfsFile(t, "dm-0", "size", "4194304\n")
	writeFakeSysfsFile(t, "dm-0", "queue/rotational", "0\n")
	writeFakeSysfsFile(t, "dm-0", "removable", "0\n")

	slavesDir := sysBlockPath + "/dm-0/slaves"
	require.NoError(t, mkdirAll(slavesDir))
	require.NoError(t, writeFile(slavesDir+"/sda", ""))

	dm := NewDeviceMap()
	dm.HandleEvent(&netlink.UEvent{
		Action: netlink.ADD,
		Env: map[string]string{
			"MAJOR":   "253",
			"MINOR":   "0",
			"DEVNAME": "dm-0",
			"DEVTYPE": "disk",
			"DM_NAME": "vg0-lv0",
			"DM_UUID": "LVM-abc123",
		},
	})

	devices := dm.Snapshot()
	require.Len(t, devices, 1)

	dev := devices[0]
	assert.Equal(t, "/dev/mapper/vg0-lv0", dev.Name)
	assert.Equal(t, "/dev/dm-0", dev.KName)
	assert.Equal(t, "lvm", dev.Type)
	assert.Equal(t, "/dev/sda", dev.PkName)
}

func TestSnapshot_Partition(t *testing.T) {
	root := withFakeSysfs(t)

	mountInfoPath := root + "/mountinfo"
	require.NoError(t, writeFile(mountInfoPath, ""))
	origMountInfo := procSelfMountInfo
	procSelfMountInfo = mountInfoPath
	t.Cleanup(func() { procSelfMountInfo = origMountInfo })

	createPartitionSymlink(t, root, "sda", "sda1")
	writeFakeSysfsFile(t, "sda1", "size", "1048576\n")
	writeFakeSysfsFile(t, "sda", "queue/rotational", "0\n")
	writeFakeSysfsFile(t, "sda", "removable", "0\n")

	dm := NewDeviceMap()
	dm.HandleEvent(&netlink.UEvent{
		Action: netlink.ADD,
		Env: map[string]string{
			"MAJOR":              "8",
			"MINOR":              "1",
			"DEVNAME":            "sda1",
			"DEVTYPE":            "partition",
			"ID_PART_ENTRY_UUID": "uuid-1234",
		},
	})

	devices := dm.Snapshot()
	require.Len(t, devices, 1)

	dev := devices[0]
	assert.Equal(t, "/dev/sda1", dev.Name)
	assert.Equal(t, "part", dev.Type)
	assert.Equal(t, "/dev/sda", dev.PkName)
	assert.Equal(t, "uuid-1234", dev.PartUUID)
	assert.False(t, dev.Rota)
}

// ================== helpers ==================

func writeFile(path, content string) error {
	return os.WriteFile(path, []byte(content), 0o644)
}

func mkdirAll(path string) error {
	return os.MkdirAll(path, 0o755)
}
