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

	"github.com/pilebones/go-udev/crawler"
)

// ErrUnknownAction is returned by HandleEvent when the uevent action
// is not one of the recognized kernel actions.
var ErrUnknownAction = errors.New("unknown action")

// DeviceMap is a thread-safe in-memory store of block device properties
// keyed by "major:minor". It is populated by netlink events (HandleEvent)
// and provides a consistent snapshot via All().
type DeviceMap struct {
	mu      sync.RWMutex
	devices map[string]Properties
}

// NewDeviceMap returns an empty, ready-to-use DeviceMap.
func NewDeviceMap() *DeviceMap {
	return &DeviceMap{
		devices: make(map[string]Properties),
	}
}

// HandleEvent processes a single netlink uevent. The action string comes
// directly from the kernel (add, change, remove, bind, unbind, move, online,
// offline). The env map is the raw udev environment from the event.
//
// Add-like actions (add, change, bind, move, online) insert or overwrite the
// device entry. Remove-like actions (remove, unbind, offline) delete it.
// This is intentional: we listen on the UdevEvent multicast group, so every
// event carries a fully enriched property set, and a device in the offline or
// unbound state is unavailable for I/O — there is no value in keeping it in
// the map.
func (dm *DeviceMap) HandleEvent(action string, env map[string]string) error {
	props, err := ParseProperties(env)
	if err != nil {
		return fmt.Errorf("handle event (action=%s, DEVNAME=%s): %w", action, env["DEVNAME"], err)
	}

	key := DeviceKey(props.Major, props.Minor)

	switch action {
	case "add", "change", "bind", "move", "online":
		dm.mu.Lock()
		dm.devices[key] = props
		dm.mu.Unlock()
	case "remove", "unbind", "offline":
		dm.mu.Lock()
		delete(dm.devices, key)
		dm.mu.Unlock()
	default:
		return fmt.Errorf("handle event: %w %q for device %s", ErrUnknownAction, action, key)
	}

	return nil
}

// All returns a shallow copy of the device map. Callers may modify the
// returned map without affecting the DeviceMap.
func (dm *DeviceMap) All() map[string]Properties {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	cp := make(map[string]Properties, len(dm.devices))
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

// FillFromCrawler atomically replaces the device map contents with devices
// obtained from a sysfs crawler. Each device's env is enriched with the udev
// database before parsing, because crawler only provides basic kernel uevent
// properties (MAJOR, MINOR, DEVNAME, DEVTYPE) without udev-enriched fields
// (serial, model, wwn). Devices that fail to parse are skipped and their
// errors are collected in the returned slice.
func (dm *DeviceMap) FillFromCrawler(devices []crawler.Device, udevDBPath string) []error {
	newDevices := make(map[string]Properties, len(devices))
	var errs []error
	for _, dev := range devices {
		env, enrichErr := EnrichWithUdevDB(udevDBPath, dev.Env)
		if enrichErr != nil {
			errs = append(errs, fmt.Errorf("crawler device %s: %w", dev.KObj, enrichErr))
		}

		props, err := ParseProperties(env)
		if err != nil {
			errs = append(errs, fmt.Errorf("crawler device %s: %w", dev.KObj, err))
			continue
		}

		key := DeviceKey(props.Major, props.Minor)
		newDevices[key] = props
	}

	dm.mu.Lock()
	defer dm.mu.Unlock()
	dm.devices = newDevices

	return errs
}
