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
	"sync"
)

// DeviceMap is a thread-safe in-memory store of block device properties
// keyed by "major:minor". It is populated by netlink events (HandleEvent)
// and provides a consistent snapshot via All().
type DeviceMap struct {
	mu      sync.RWMutex
	devices map[string]UdevProperties
}

func NewDeviceMap() *DeviceMap {
	return &DeviceMap{
		devices: make(map[string]UdevProperties),
	}
}

// HandleEvent processes a single netlink uevent. The action string comes
// directly from the kernel (add, change, remove, bind, unbind, move, online,
// offline). The env map is the raw udev environment from the event.
func (dm *DeviceMap) HandleEvent(action string, env map[string]string) error {
	props, err := ParseUdevProperties(env)
	if err != nil {
		return fmt.Errorf("handle event (action=%s, DEVNAME=%s): %w", action, env["DEVNAME"], err)
	}

	key := DeviceKey(props.Major, props.Minor)

	dm.mu.Lock()
	defer dm.mu.Unlock()

	switch action {
	case "add", "change", "bind", "move", "online":
		dm.devices[key] = props
	case "remove", "unbind", "offline":
		delete(dm.devices, key)
	default:
		return fmt.Errorf("unknown action %q for device %s", action, key)
	}

	return nil
}

// All returns a shallow copy of the device map. Callers may modify the
// returned map without affecting the DeviceMap.
func (dm *DeviceMap) All() map[string]UdevProperties {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	cp := make(map[string]UdevProperties, len(dm.devices))
	for k, v := range dm.devices {
		cp[k] = v
	}
	return cp
}

// Len returns the number of devices currently tracked.
func (dm *DeviceMap) Len() int {
	dm.mu.RLock()
	defer dm.mu.RUnlock()
	return len(dm.devices)
}
