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
	"errors"
	"fmt"
	"sync"

	"github.com/pilebones/go-udev/crawler"
	"github.com/pilebones/go-udev/netlink"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/logger"
)

// DeviceMap is an in-memory store of block device udev properties,
// keyed by "major:minor". Updated incrementally by netlink events.
type DeviceMap struct {
	mu      sync.RWMutex
	devices map[string]map[string]string // key = "major:minor", value = merged env map
	log     logger.Logger
}

func NewDeviceMap(log logger.Logger) *DeviceMap {
	return &DeviceMap{
		devices: make(map[string]map[string]string),
		log:     log,
	}
}

func devKey(env map[string]string) (string, error) {
	major, minor := env["MAJOR"], env["MINOR"]
	if major == "" || minor == "" {
		return "", errors.New("[devKey] major or minor version missing")
	}
	return major + ":" + minor, nil
}

// HandleEvent processes a netlink UEvent and updates the device map.
func (dm *DeviceMap) HandleEvent(event *netlink.UEvent) {
	env := MergeEnvWithUdevDB(event.Env)

	key, err := devKey(env)
	if err != nil {
		dm.log.Error(err, "[HandleEvent] devkey for event %v", event)
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
		key, err := devKey(env)
		if err != nil {
			dm.log.Error(err, "[FillFromCrawler] devkey for event %v", dev)
			continue
		}
		newDevices[key] = env
	}

	dm.mu.Lock()
	defer dm.mu.Unlock()
	dm.devices = newDevices
}

// Snapshot converts the current device map into a slice of internal.Device,
// reading additional properties from sysfs. Mount-point enrichment is handled
// separately by the caller (scanner) to decouple mount info failures from
// device discovery.
func (dm *DeviceMap) Snapshot() ([]internal.Device, []string) {
	dm.mu.RLock()
	envsCopy := make(map[string]map[string]string, len(dm.devices))
	for k, v := range dm.devices {
		envsCopy[k] = v
	}
	dm.mu.RUnlock()

	var errs []string
	result := make([]internal.Device, 0, len(envsCopy))
	for devID, env := range envsCopy {
		props := ParseUdevProperties(env)

		sysName := SysfsDevName(props.DevName)
		if sysName == "" {
			sysName = env["DEVNAME"]
		}
		if sysName == "" {
			errs = append(errs, fmt.Sprintf("[Snapshot] skipping device %s: no DEVNAME in env", devID))
			continue
		}

		sizeBytes, err := ReadSysfsSize(sysName)
		if err != nil {
			errs = append(errs, fmt.Sprintf("[Snapshot] skipping device %s (%s): %v", devID, sysName, err))
			continue
		}

		name := ResolveDeviceName(props)
		if name == "" {
			errs = append(errs, fmt.Sprintf("[Snapshot] skipping device %s (%s): resolved name is empty", devID, sysName))
			continue
		}

		rota, rotaErr := ReadSysfsRotational(sysName)
		if rotaErr != nil {
			errs = append(errs, fmt.Sprintf("[Snapshot] device %s (%s): failed to read rotational: %v", devID, sysName, rotaErr))
		}
		hotplug, hotplugErr := ReadSysfsHotplug(sysName)
		if hotplugErr != nil {
			errs = append(errs, fmt.Sprintf("[Snapshot] device %s (%s): failed to read hotplug: %v", devID, sysName, hotplugErr))
		}

		devType := ResolveDeviceType(props, props.DevName)
		kname := ResolveKernelName(sysName)
		pkname := ResolveParentDevice(sysName)

		dev := internal.Device{
			DevID:    devID,
			Name:     name,
			PartUUID: props.PartUUID,
			HotPlug:  hotplug,
			Model:    props.Model,
			Serial:   props.Serial,
			Size:     *resource.NewQuantity(sizeBytes, resource.BinarySI),
			Type:     devType,
			Wwn:      props.WWN,
			KName:    kname,
			PkName:   pkname,
			FSType:   props.FSType,
			Rota:     rota,
		}
		result = append(result, dev)
	}

	return result, errs
}

// Len returns the number of tracked devices.
func (dm *DeviceMap) Len() int {
	dm.mu.RLock()
	defer dm.mu.RUnlock()
	return len(dm.devices)
}
