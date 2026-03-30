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
	"sync"

	"github.com/pilebones/go-udev/crawler"
	"github.com/pilebones/go-udev/netlink"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
)

// DeviceMap is an in-memory store of block device udev properties,
// keyed by "major:minor". Updated incrementally by netlink events.
type DeviceMap struct {
	mu      sync.RWMutex
	devices map[string]map[string]string // key = "major:minor", value = merged env map
}

func NewDeviceMap() *DeviceMap {
	return &DeviceMap{
		devices: make(map[string]map[string]string),
	}
}

func devKey(env map[string]string) string {
	return env["MAJOR"] + ":" + env["MINOR"]
}

// HandleEvent processes a netlink UEvent and updates the device map.
func (dm *DeviceMap) HandleEvent(event *netlink.UEvent) {
	env := MergeEnvWithUdevDB(event.Env)

	key := devKey(env)
	if key == ":" {
		return
	}

	dm.mu.Lock()
	defer dm.mu.Unlock()

	switch event.Action {
	case netlink.ADD, netlink.CHANGE, netlink.BIND, netlink.MOVE, netlink.ONLINE:
		dm.devices[key] = env
	case netlink.REMOVE, netlink.UNBIND, netlink.OFFLINE:
		delete(dm.devices, key)
	}
}

// FillFromCrawler replaces the entire device map with devices from crawler results.
// For each device, it merges udev DB properties on top of the uevent env.
// This is safe to call multiple times (e.g. after netlink reconnect) --
// old entries not present in the new crawl are removed.
func (dm *DeviceMap) FillFromCrawler(devices []crawler.Device) {
	newDevices := make(map[string]map[string]string, len(devices))
	for _, dev := range devices {
		env := MergeEnvWithUdevDB(dev.Env)
		key := devKey(env)
		if key == ":" {
			continue
		}
		newDevices[key] = env
	}

	dm.mu.Lock()
	defer dm.mu.Unlock()
	dm.devices = newDevices
}

// Snapshot converts the current device map into a slice of internal.Device,
// reading additional properties from sysfs and mountinfo.
func (dm *DeviceMap) Snapshot() []internal.Device {
	dm.mu.RLock()
	envsCopy := make(map[string]map[string]string, len(dm.devices))
	for k, v := range dm.devices {
		envsCopy[k] = v
	}
	dm.mu.RUnlock()

	mountPoints, _ := ParseMountInfo()
	if mountPoints == nil {
		mountPoints = make(map[string]string)
	}

	result := make([]internal.Device, 0, len(envsCopy))
	for devID, env := range envsCopy {
		props := ParseUdevProperties(env)

		sysName := SysfsDevName(props.DevName)
		if sysName == "" {
			sysName = env["DEVNAME"]
		}
		if sysName == "" {
			continue
		}

		sizeBytes, err := ReadSysfsSize(sysName)
		if err != nil {
			continue
		}

		rota, _ := ReadSysfsRotational(sysName)
		hotplug, _ := ReadSysfsHotplug(sysName)

		devType := ResolveDeviceType(props, props.DevName)
		name := ResolveDeviceName(props)
		kname := ResolveKernelName(sysName)
		pkname := ResolveParentDevice(sysName)
		mountPoint := mountPoints[devID]

		dev := internal.Device{
			Name:       name,
			MountPoint: mountPoint,
			PartUUID:   props.PartUUID,
			HotPlug:    hotplug,
			Model:      props.Model,
			Serial:     props.Serial,
			Size:       *resource.NewQuantity(sizeBytes, resource.BinarySI),
			Type:       devType,
			Wwn:        props.WWN,
			KName:      kname,
			PkName:     pkname,
			FSType:     props.FSType,
			Rota:       rota,
		}
		result = append(result, dev)
	}

	return result
}

// Len returns the number of tracked devices.
func (dm *DeviceMap) Len() int {
	dm.mu.RLock()
	defer dm.mu.RUnlock()
	return len(dm.devices)
}
