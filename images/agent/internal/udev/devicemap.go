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
	"sync"

	"github.com/pilebones/go-udev/crawler"
	"github.com/pilebones/go-udev/netlink"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
)

var ErrUnknownAction = errors.New("unknown action")

type DeviceMap struct {
	mu                sync.RWMutex
	devices           map[string]Properties
	udevDBPath        string
	resolver          *Resolver
	sysFsDataProvider *SysFSDataProvider
}

func NewDeviceMap(udevDBPath string) *DeviceMap {
	sysfsProvider := NewSysFSDataProvider(SysClassBlockPath, SysBlockPath)
	return &DeviceMap{
		devices:           make(map[string]Properties),
		udevDBPath:        udevDBPath,
		resolver:          NewResolver(sysfsProvider),
		sysFsDataProvider: sysfsProvider,
	}
}

func (dm *DeviceMap) HandleEvent(action netlink.KObjAction, env map[string]string) error {
	props, err := ParseProperties(env)
	if err != nil {
		return fmt.Errorf("handle event (action=%s, DEVNAME=%s): %w", action, env["DEVNAME"], err)
	}

	key := DeviceKey(props.Major, props.Minor)

	switch action {
	case netlink.ADD, netlink.CHANGE, netlink.BIND, netlink.MOVE, netlink.ONLINE:
		dm.mu.Lock()
		dm.devices[key] = props
		dm.mu.Unlock()
	case netlink.REMOVE, netlink.UNBIND, netlink.OFFLINE:
		dm.mu.Lock()
		delete(dm.devices, key)
		dm.mu.Unlock()
	default:
		return fmt.Errorf("handle event: %w %q for device %s", ErrUnknownAction, action, key)
	}

	return nil
}

func (dm *DeviceMap) All() map[string]Properties {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	cp := make(map[string]Properties, len(dm.devices))
	for k, v := range dm.devices {
		cp[k] = v
	}
	return cp
}

func (dm *DeviceMap) Len() int {
	dm.mu.RLock()
	defer dm.mu.RUnlock()
	return len(dm.devices)
}

func (dm *DeviceMap) FillFromCrawler(ctx context.Context, devices []crawler.Device) error {
	if devices == nil {
		return errors.New("FillFromCrawler: nil devices slice")
	}

	newDevices := make(map[string]Properties, len(devices))
	errs := make([]error, 0, len(devices))

	for _, dev := range devices {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}

		env, enrichErr := EnrichWithUdevDB(dm.udevDBPath, dev.Env)
		if enrichErr != nil && !errors.Is(enrichErr, os.ErrNotExist) {
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

	return errors.Join(errs...)
}

func (dm *DeviceMap) Snapshot(mounts map[string]string) ([]internal.Device, []error) {
	all := dm.All()
	var errs []error
	result := make([]internal.Device, 0, len(all))

	for devID, props := range all {
		sysName := dm.sysFsDataProvider.SysfsDevName(props.DevName)
		if sysName == "" {
			errs = append(errs, fmt.Errorf("skipping device %s: no DEVNAME", devID))
			continue
		}

		sizeBytes, err := dm.sysFsDataProvider.ReadSysfsSize(sysName)
		if err != nil {
			errs = append(errs, fmt.Errorf("skipping device %s (%s): %w", devID, sysName, err))
			continue
		}

		name := dm.resolver.DeviceName(props)
		if name == "" {
			errs = append(errs, fmt.Errorf("skipping device %s (%s): resolved name is empty", devID, sysName))
			continue
		}

		rota, rotaErr := dm.sysFsDataProvider.ReadSysfsRotational(sysName)
		if rotaErr != nil {
			errs = append(errs, fmt.Errorf("device %s (%s): %w", devID, sysName, rotaErr))
		}
		hotplug, hotplugErr := dm.sysFsDataProvider.ReadSysfsHotplug(sysName)
		if hotplugErr != nil {
			errs = append(errs, fmt.Errorf("device %s (%s): %w", devID, sysName, hotplugErr))
		}

		dev := internal.Device{
			Name:       name,
			MountPoint: mounts[devID],
			PartUUID:   props.PartUUID,
			HotPlug:    hotplug,
			Model:      props.Model,
			Serial:     props.Serial,
			Size:       *resource.NewQuantity(sizeBytes, resource.BinarySI),
			Type:       dm.resolver.DeviceType(props, props.DevName),
			Wwn:        props.WWN,
			KName:      dm.resolver.KernelName(sysName),
			PkName:     dm.resolver.ParentDevice(sysName),
			FSType:     props.FSType,
			Rota:       rota,
		}
		result = append(result, dev)
	}

	return result, errs
}
