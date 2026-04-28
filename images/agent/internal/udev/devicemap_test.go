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
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/pilebones/go-udev/crawler"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestDeviceMap builds a DeviceMap wired to the supplied sysfs provider,
// so tests can operate against a fake /sys tree without mutating globals.
// The fakeSysfs helper used to construct the provider lives in sysfs_test.go.
func newTestDeviceMap(sysfs *SysFSDataProvider) *DeviceMap {
	return &DeviceMap{
		devices:           make(map[string]Properties),
		resolver:          NewResolver(sysfs),
		sysFsDataProvider: sysfs,
	}
}

func makeEnv(major, minor, devname string) map[string]string {
	return map[string]string{
		"MAJOR":   major,
		"MINOR":   minor,
		"DEVNAME": devname,
	}
}

// ================== HandleEvent — add/change/remove ==================

func TestHandleEvent_Add(t *testing.T) {
	dm := NewDeviceMap("")
	require.NoError(t, dm.HandleEvent("add", makeEnv("8", "0", "sda")))
	assert.Equal(t, 1, dm.Len())
}

func TestHandleEvent_Change_Updates(t *testing.T) {
	dm := NewDeviceMap("")
	require.NoError(t, dm.HandleEvent("add", makeEnv("8", "0", "sda")))

	env := makeEnv("8", "0", "sda")
	env["ID_FS_TYPE"] = "ext4"
	require.NoError(t, dm.HandleEvent("change", env))

	assert.Equal(t, 1, dm.Len())
	all := dm.All()
	assert.Equal(t, "ext4", all["8:0"].FSType)
}

func TestHandleEvent_Remove(t *testing.T) {
	dm := NewDeviceMap("")
	require.NoError(t, dm.HandleEvent("add", makeEnv("8", "0", "sda")))
	require.NoError(t, dm.HandleEvent("remove", makeEnv("8", "0", "sda")))
	assert.Equal(t, 0, dm.Len())
}

// ================== HandleEvent — bind/unbind/move/online/offline ==================

func TestHandleEvent_Bind(t *testing.T) {
	dm := NewDeviceMap("")
	require.NoError(t, dm.HandleEvent("bind", makeEnv("8", "0", "sda")))
	assert.Equal(t, 1, dm.Len())
}

func TestHandleEvent_Unbind(t *testing.T) {
	dm := NewDeviceMap("")
	require.NoError(t, dm.HandleEvent("add", makeEnv("8", "0", "sda")))
	require.NoError(t, dm.HandleEvent("unbind", makeEnv("8", "0", "sda")))
	assert.Equal(t, 0, dm.Len())
}

func TestHandleEvent_Move(t *testing.T) {
	dm := NewDeviceMap("")
	require.NoError(t, dm.HandleEvent("move", makeEnv("8", "0", "sda")))
	assert.Equal(t, 1, dm.Len())
}

func TestHandleEvent_Online(t *testing.T) {
	dm := NewDeviceMap("")
	require.NoError(t, dm.HandleEvent("online", makeEnv("8", "0", "sda")))
	assert.Equal(t, 1, dm.Len())
}

func TestHandleEvent_Offline(t *testing.T) {
	dm := NewDeviceMap("")
	require.NoError(t, dm.HandleEvent("add", makeEnv("8", "0", "sda")))
	require.NoError(t, dm.HandleEvent("offline", makeEnv("8", "0", "sda")))
	assert.Equal(t, 0, dm.Len())
}

// ================== HandleEvent — error cases ==================

func TestHandleEvent_NoMajorMinor_ReturnsError(t *testing.T) {
	dm := NewDeviceMap("")
	err := dm.HandleEvent("add", map[string]string{"DEVNAME": "sda"})
	require.Error(t, err)
	assert.Equal(t, 0, dm.Len())
}

func TestHandleEvent_InvalidMajorMinor_ReturnsError(t *testing.T) {
	dm := NewDeviceMap("")
	err := dm.HandleEvent("add", map[string]string{
		"MAJOR": "abc", "MINOR": "xyz", "DEVNAME": "sda",
	})
	require.Error(t, err)
	assert.Equal(t, 0, dm.Len())
}

func TestHandleEvent_UnknownAction_ReturnsError(t *testing.T) {
	dm := NewDeviceMap("")
	err := dm.HandleEvent("explode", makeEnv("8", "0", "sda"))
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrUnknownAction))
	assert.Equal(t, 0, dm.Len())
}

// ================== HandleEvent — parsing ==================

func TestHandleEvent_ParsesProperties(t *testing.T) {
	dm := NewDeviceMap("")
	env := map[string]string{
		"MAJOR": "8", "MINOR": "0", "DEVNAME": "sda",
		"DEVTYPE": "disk", "ID_MODEL": "TestDisk",
		"ID_SERIAL_SHORT": "SN123", "ID_WWN": "0x5000",
	}
	require.NoError(t, dm.HandleEvent("add", env))

	all := dm.All()
	props := all["8:0"]
	assert.Equal(t, "/dev/sda", props.DevName)
	assert.Equal(t, "disk", props.DevType)
	assert.Equal(t, 8, props.Major)
	assert.Equal(t, 0, props.Minor)
	assert.Equal(t, "SN123", props.Serial)
	assert.Equal(t, "TestDisk", props.Model)
	assert.Equal(t, "0x5000", props.WWN)
}

func TestHandleEvent_UsesDeviceKeyFromParsedMajorMinor(t *testing.T) {
	dm := NewDeviceMap("")
	require.NoError(t, dm.HandleEvent("add", makeEnv("259", "1", "nvme0n1")))

	all := dm.All()
	_, ok := all["259:1"]
	assert.True(t, ok, "key must use parsed major:minor")
}

// ================== HandleEvent — sequence ==================

func TestHandleEvent_Sequence(t *testing.T) {
	dm := NewDeviceMap("")
	require.NoError(t, dm.HandleEvent("add", makeEnv("8", "0", "sda")))
	require.NoError(t, dm.HandleEvent("add", makeEnv("8", "1", "sda1")))
	assert.Equal(t, 2, dm.Len())

	require.NoError(t, dm.HandleEvent("remove", makeEnv("8", "1", "sda1")))
	assert.Equal(t, 1, dm.Len())

	require.NoError(t, dm.HandleEvent("remove", makeEnv("8", "0", "sda")))
	assert.Equal(t, 0, dm.Len())
}

func TestHandleEvent_RemoveNonexistent_NoError(t *testing.T) {
	dm := NewDeviceMap("")
	require.NoError(t, dm.HandleEvent("remove", makeEnv("8", "0", "sda")))
	assert.Equal(t, 0, dm.Len())
}

// ================== All — copy safety ==================

func TestAll_ReturnsCopy(t *testing.T) {
	dm := NewDeviceMap("")
	require.NoError(t, dm.HandleEvent("add", makeEnv("8", "0", "sda")))

	all := dm.All()
	delete(all, "8:0")
	assert.Equal(t, 1, dm.Len(), "deleting from copy must not affect the map")
}

func TestAll_EmptyMap(t *testing.T) {
	dm := NewDeviceMap("")
	all := dm.All()
	assert.NotNil(t, all)
	assert.Empty(t, all)
}

// ================== FillFromCrawler ==================

func TestFillFromCrawler_ReplacesAll(t *testing.T) {
	dm := NewDeviceMap("/nonexistent")
	require.NoError(t, dm.HandleEvent("add", makeEnv("8", "99", "old")))

	devices := []crawler.Device{
		{KObj: "/devices/pci/block/sda", Env: makeEnv("8", "0", "sda")},
		{KObj: "/devices/pci/block/sdb", Env: makeEnv("8", "16", "sdb")},
	}
	err := dm.FillFromCrawler(context.Background(), devices)
	assert.Equal(t, 2, dm.Len(), "old device replaced by crawler results")

	_, hasOld := dm.All()["8:99"]
	assert.False(t, hasOld, "previous device must be gone after FillFromCrawler")

	assert.NoError(t, err, "missing udev DB files (ErrNotExist) are silently ignored")
}

func TestFillFromCrawler_SkipsBadDevices(t *testing.T) {
	dm := NewDeviceMap("/nonexistent")
	devices := []crawler.Device{
		{KObj: "/devices/block/sda", Env: makeEnv("8", "0", "sda")},
		{KObj: "/devices/block/bad", Env: map[string]string{"DEVNAME": "bad"}},
	}
	err := dm.FillFromCrawler(context.Background(), devices)
	assert.Equal(t, 1, dm.Len())
	assert.Error(t, err, "parse error for bad device expected")
}

func TestFillFromCrawler_NilReturnsError(t *testing.T) {
	dm := NewDeviceMap("/nonexistent")
	require.NoError(t, dm.HandleEvent("add", makeEnv("8", "0", "sda")))
	err := dm.FillFromCrawler(context.Background(), nil)
	assert.Error(t, err, "nil devices must be rejected")
	assert.Equal(t, 1, dm.Len(), "map must not be wiped on nil input")
}

func TestFillFromCrawler_ParsesProperties(t *testing.T) {
	dm := NewDeviceMap("/nonexistent")
	env := map[string]string{
		"MAJOR": "8", "MINOR": "0", "DEVNAME": "sda",
		"DEVTYPE": "disk", "ID_MODEL": "CrawledDisk",
	}
	devices := []crawler.Device{{KObj: "/devices/block/sda", Env: env}}
	_ = dm.FillFromCrawler(context.Background(), devices)

	all := dm.All()
	assert.Equal(t, "CrawledDisk", all["8:0"].Model)
	assert.Equal(t, "/dev/sda", all["8:0"].DevName)
}

func TestFillFromCrawler_WithUdevDB(t *testing.T) {
	dir := newTestUdevDB(t)
	dbContent := "E:ID_SERIAL_SHORT=DB-SERIAL\nE:ID_WWN=0xdeadbeef\n"
	require.NoError(t, os.WriteFile(filepath.Join(dir, "b8:0"), []byte(dbContent), 0o644))

	dm := NewDeviceMap(dir)
	env := makeEnv("8", "0", "sda")
	env["ID_MODEL"] = "TestDisk"
	devices := []crawler.Device{{KObj: "/devices/block/sda", Env: env}}
	err := dm.FillFromCrawler(context.Background(), devices)
	assert.NoError(t, err)

	all := dm.All()
	props := all["8:0"]
	assert.Equal(t, "DB-SERIAL", props.Serial, "serial from udev DB")
	assert.Equal(t, "0xdeadbeef", props.WWN, "wwn from udev DB")
	assert.Equal(t, "TestDisk", props.Model, "model from event env")
}

func TestFillFromCrawler_CancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	dm := NewDeviceMap("/nonexistent")
	devices := []crawler.Device{
		{KObj: "/devices/block/sda", Env: makeEnv("8", "0", "sda")},
	}
	err := dm.FillFromCrawler(ctx, devices)
	assert.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

// ================== Snapshot ==================

func TestSnapshot_BuildsDevices(t *testing.T) {
	f := newFakeSysfs(t)

	devicesPath := filepath.Join(f.root, "devices", "pci0000:00", "0000:00:1f.2", "ata1", "host0", "target0:0:0", "0:0:0:0", "block", "sda")
	pciPath := filepath.Join(f.root, "devices", "pci0000:00", "0000:00:1f.2")
	require.NoError(t, os.MkdirAll(filepath.Join(devicesPath, "queue"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(devicesPath, "size"), []byte("2097152\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(devicesPath, "queue", "rotational"), []byte("1\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(pciPath, "removable"), []byte("fixed\n"), 0o644))
	require.NoError(t, os.MkdirAll(f.classBlockPath, 0o755))
	require.NoError(t, os.Symlink(devicesPath, filepath.Join(f.classBlockPath, "sda")))

	dm := newTestDeviceMap(f.provider)
	env := map[string]string{
		"MAJOR": "8", "MINOR": "0", "DEVNAME": "sda",
		"DEVTYPE": "disk", "ID_MODEL": "TestDisk",
		"ID_SERIAL_SHORT": "SN123", "ID_WWN": "0x5000",
		"ID_FS_TYPE": "ext4",
	}
	require.NoError(t, dm.HandleEvent("add", env))

	mounts := map[string]string{"8:0": "/mnt/data"}
	devices, errs := dm.Snapshot(mounts)
	assert.Empty(t, errs)
	require.Len(t, devices, 1)

	dev := devices[0]
	assert.Equal(t, "/dev/sda", dev.Name)
	assert.Equal(t, "/dev/sda", dev.KName)
	assert.Equal(t, "/mnt/data", dev.MountPoint)
	assert.Equal(t, "disk", dev.Type)
	assert.Equal(t, "TestDisk", dev.Model)
	assert.Equal(t, "SN123", dev.Serial)
	assert.Equal(t, "0x5000", dev.Wwn)
	assert.Equal(t, "ext4", dev.FSType)
	assert.True(t, dev.Rota)
	assert.False(t, dev.HotPlug)
	assert.Equal(t, int64(2097152*512), dev.Size.Value())
}

func TestSnapshot_SkipsDeviceWithNoSize(t *testing.T) {
	f := newFakeSysfs(t)

	dm := newTestDeviceMap(f.provider)
	require.NoError(t, dm.HandleEvent("add", makeEnv("8", "0", "sda")))

	devices, errs := dm.Snapshot(nil)
	assert.Empty(t, devices)
	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "skipping device")
}

func TestSnapshot_SkipsDeviceWithEmptyDevName(t *testing.T) {
	dm := NewDeviceMap("")
	env := map[string]string{
		"MAJOR": "8", "MINOR": "0", "DEVNAME": "",
	}
	require.NoError(t, dm.HandleEvent("add", env))

	devices, errs := dm.Snapshot(nil)
	assert.Empty(t, devices)
	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "no DEVNAME")
}

func TestSnapshot_DMDevice(t *testing.T) {
	f := newFakeSysfs(t)
	f.writeFile(t, "dm-0", "size", "1024\n")
	f.writeFile(t, "dm-0", "queue/rotational", "0\n")

	devDir := filepath.Join(f.classBlockPath, "dm-0")
	require.NoError(t, os.MkdirAll(devDir, 0o755))

	dm := newTestDeviceMap(f.provider)
	env := map[string]string{
		"MAJOR": "253", "MINOR": "0", "DEVNAME": "dm-0",
		"DEVTYPE": "disk", "DM_NAME": "vg0-lv0", "DM_UUID": "LVM-abc123",
	}
	require.NoError(t, dm.HandleEvent("add", env))

	devices, errs := dm.Snapshot(nil)
	assert.Empty(t, errs)
	require.Len(t, devices, 1)

	dev := devices[0]
	assert.Equal(t, "/dev/mapper/vg0-lv0", dev.Name)
	assert.Equal(t, "/dev/dm-0", dev.KName)
	assert.Equal(t, "lvm", dev.Type)
}

func TestSnapshot_EmptyMap(t *testing.T) {
	dm := NewDeviceMap("")
	devices, errs := dm.Snapshot(nil)
	assert.Empty(t, devices)
	assert.Empty(t, errs)
}

func TestSnapshot_NoMountPoint(t *testing.T) {
	f := newFakeSysfs(t)
	f.writeFile(t, "sda", "size", "1024\n")

	devDir := filepath.Join(f.classBlockPath, "sda")
	require.NoError(t, os.MkdirAll(devDir, 0o755))

	dm := newTestDeviceMap(f.provider)
	require.NoError(t, dm.HandleEvent("add", makeEnv("8", "0", "sda")))

	devices, errs := dm.Snapshot(map[string]string{"999:999": "/mnt/other"})
	for _, e := range errs {
		assert.NotContains(t, e.Error(), "skipping")
	}
	require.Len(t, devices, 1)
	assert.Equal(t, "", devices[0].MountPoint)
}

// ================== Concurrency ==================

func TestDeviceMap_ConcurrentAccess(t *testing.T) {
	dm := NewDeviceMap("")

	errs := make([]error, 100)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(minor int) {
			defer wg.Done()
			env := makeEnv("8", fmt.Sprintf("%d", minor), fmt.Sprintf("sd%d", minor))
			errs[minor] = dm.HandleEvent("add", env)
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		assert.NoError(t, err, "goroutine %d returned error", i)
	}
	assert.Equal(t, 100, dm.Len())
}

func TestDeviceMap_ConcurrentReadWrite(t *testing.T) {
	t.Parallel()

	dm := NewDeviceMap("")

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(minor int) {
			defer wg.Done()
			env := makeEnv("8", fmt.Sprintf("%d", minor), fmt.Sprintf("sd%d", minor))
			_ = dm.HandleEvent("add", env)
		}(i)
	}
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = dm.All()
			_ = dm.Len()
		}()
	}
	wg.Wait()
}

func TestDeviceMap_ConcurrentAddRemoveSameKey(t *testing.T) {
	dm := NewDeviceMap("")
	env := makeEnv("8", "0", "sda")

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if i%2 == 0 {
				_ = dm.HandleEvent("add", env)
			} else {
				_ = dm.HandleEvent("remove", env)
			}
		}(i)
	}
	wg.Wait()

	assert.True(t, dm.Len() <= 1, "at most one entry for the key")
}
