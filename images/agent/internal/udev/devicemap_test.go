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
	"errors"
	"fmt"
	"sync"
	"testing"

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
	dm := NewDeviceMap()
	require.NoError(t, dm.HandleEvent("add", makeEnv("8", "0", "sda")))
	assert.Equal(t, 1, dm.Len())
}

func TestHandleEvent_Change_Updates(t *testing.T) {
	dm := NewDeviceMap()
	require.NoError(t, dm.HandleEvent("add", makeEnv("8", "0", "sda")))

	env := makeEnv("8", "0", "sda")
	env["ID_FS_TYPE"] = "ext4"
	require.NoError(t, dm.HandleEvent("change", env))

	assert.Equal(t, 1, dm.Len())
	all := dm.All()
	assert.Equal(t, "ext4", all["8:0"].FSType)
}

func TestHandleEvent_Remove(t *testing.T) {
	dm := NewDeviceMap()
	require.NoError(t, dm.HandleEvent("add", makeEnv("8", "0", "sda")))
	require.NoError(t, dm.HandleEvent("remove", makeEnv("8", "0", "sda")))
	assert.Equal(t, 0, dm.Len())
}

// ================== HandleEvent — bind/unbind/move/online/offline ==================

func TestHandleEvent_Bind(t *testing.T) {
	dm := NewDeviceMap()
	require.NoError(t, dm.HandleEvent("bind", makeEnv("8", "0", "sda")))
	assert.Equal(t, 1, dm.Len())
}

func TestHandleEvent_Unbind(t *testing.T) {
	dm := NewDeviceMap()
	require.NoError(t, dm.HandleEvent("add", makeEnv("8", "0", "sda")))
	require.NoError(t, dm.HandleEvent("unbind", makeEnv("8", "0", "sda")))
	assert.Equal(t, 0, dm.Len())
}

func TestHandleEvent_Move(t *testing.T) {
	dm := NewDeviceMap()
	require.NoError(t, dm.HandleEvent("move", makeEnv("8", "0", "sda")))
	assert.Equal(t, 1, dm.Len())
}

func TestHandleEvent_Online(t *testing.T) {
	dm := NewDeviceMap()
	require.NoError(t, dm.HandleEvent("online", makeEnv("8", "0", "sda")))
	assert.Equal(t, 1, dm.Len())
}

func TestHandleEvent_Offline(t *testing.T) {
	dm := NewDeviceMap()
	require.NoError(t, dm.HandleEvent("add", makeEnv("8", "0", "sda")))
	require.NoError(t, dm.HandleEvent("offline", makeEnv("8", "0", "sda")))
	assert.Equal(t, 0, dm.Len())
}

// ================== HandleEvent — error cases ==================

func TestHandleEvent_NoMajorMinor_ReturnsError(t *testing.T) {
	dm := NewDeviceMap()
	err := dm.HandleEvent("add", map[string]string{"DEVNAME": "sda"})
	require.Error(t, err)
	assert.Equal(t, 0, dm.Len())
}

func TestHandleEvent_InvalidMajorMinor_ReturnsError(t *testing.T) {
	dm := NewDeviceMap()
	err := dm.HandleEvent("add", map[string]string{
		"MAJOR": "abc", "MINOR": "xyz", "DEVNAME": "sda",
	})
	require.Error(t, err)
	assert.Equal(t, 0, dm.Len())
}

func TestHandleEvent_UnknownAction_ReturnsError(t *testing.T) {
	dm := NewDeviceMap()
	err := dm.HandleEvent("explode", makeEnv("8", "0", "sda"))
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrUnknownAction))
	assert.Equal(t, 0, dm.Len())
}

// ================== HandleEvent — parsing ==================

func TestHandleEvent_ParsesProperties(t *testing.T) {
	dm := NewDeviceMap()
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
	dm := NewDeviceMap()
	require.NoError(t, dm.HandleEvent("add", makeEnv("259", "1", "nvme0n1")))

	all := dm.All()
	_, ok := all["259:1"]
	assert.True(t, ok, "key must use parsed major:minor")
}

// ================== HandleEvent — sequence ==================

func TestHandleEvent_Sequence(t *testing.T) {
	dm := NewDeviceMap()
	require.NoError(t, dm.HandleEvent("add", makeEnv("8", "0", "sda")))
	require.NoError(t, dm.HandleEvent("add", makeEnv("8", "1", "sda1")))
	assert.Equal(t, 2, dm.Len())

	require.NoError(t, dm.HandleEvent("remove", makeEnv("8", "1", "sda1")))
	assert.Equal(t, 1, dm.Len())

	require.NoError(t, dm.HandleEvent("remove", makeEnv("8", "0", "sda")))
	assert.Equal(t, 0, dm.Len())
}

func TestHandleEvent_RemoveNonexistent_NoError(t *testing.T) {
	dm := NewDeviceMap()
	require.NoError(t, dm.HandleEvent("remove", makeEnv("8", "0", "sda")))
	assert.Equal(t, 0, dm.Len())
}

// ================== All — copy safety ==================

func TestAll_ReturnsCopy(t *testing.T) {
	dm := NewDeviceMap()
	require.NoError(t, dm.HandleEvent("add", makeEnv("8", "0", "sda")))

	all := dm.All()
	delete(all, "8:0")
	assert.Equal(t, 1, dm.Len(), "deleting from copy must not affect the map")
}

func TestAll_EmptyMap(t *testing.T) {
	dm := NewDeviceMap()
	all := dm.All()
	assert.NotNil(t, all)
	assert.Empty(t, all)
}

// ================== Concurrency ==================

func TestDeviceMap_ConcurrentAccess(t *testing.T) {
	dm := NewDeviceMap()

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
