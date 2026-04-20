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
