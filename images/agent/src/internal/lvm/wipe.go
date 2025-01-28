/*
Copyright 2023 Flant JSC

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

package lvm

import (
	"errors"
	"fmt"
)

var ErrNoMetadata = errors.New("metadata not found")

// Returns ranges to wipe in Superblock.DataBlockSize units
func buildWipeMapBySuperblock(superblock Superblock, devices []ThinDeviceId) (map[ThinDeviceId]RangeCover, error) {
	byThinPoolDeviceId := make(map[ThinDeviceId]Device)
	for _, device := range superblock.Devices {
		byThinPoolDeviceId[device.DevId] = device
	}

	covers := make(map[ThinDeviceId]RangeCover)
	for _, deviceId := range devices {
		device, ok := byThinPoolDeviceId[deviceId]
		if !ok {
			return nil, fmt.Errorf("not found metadata for device %d: %w", deviceId, ErrNoMetadata)
		}

		cover := make(RangeCover, len(device.RangeMappings)+len(device.SingleMappings))
		i := 0

		for _, d := range device.RangeMappings {
			cover[i] = Range{Start: d.OriginBegin, Count: d.Length}
			i++
		}

		for _, d := range device.SingleMappings {
			cover[i] = Range{Start: d.OriginBlock, Count: 1}
			i++
		}

		merged, err := cover.Merged()
		if err != nil {
			return nil, fmt.Errorf("can't merge ranges for deviceId %d: %w", deviceId, err)
		}

		covers[device.DevId] = merged
	}

	return covers, nil
}

// Returns ranges to wipe in device sectors
func BuildWipeMap(volumes []ThinVolume) (map[ThinVolume]RangeCover, error) {
	wipeMap := make(map[ThinVolume]RangeCover)
	byThinPool := make(map[ThinPool][]ThinVolume)

	for _, volume := range volumes {
		if volume.ThinPool() != nil {
			byThinPool[volume.ThinPool()] = append(byThinPool[volume.ThinPool()], volume)
		}
	}

	for pool, volumes := range byThinPool {
		superblock, err := pool.DumpThinPoolSuperblock()
		if err != nil {
			return nil, fmt.Errorf("dumping superblock of thin pool %s: %w", pool.FullName(), err)
		}

		deviceIds := make([]ThinDeviceId, len(volumes))
		deviceIdToVolume := make(map[ThinDeviceId]ThinVolume)
		for i, volume := range volumes {
			deviceIds[i] = volume.ThinDeviceId()
			deviceIdToVolume[volume.ThinDeviceId()] = volume
		}

		covers, err := buildWipeMapBySuperblock(superblock, deviceIds)
		if err != nil {
			return nil, fmt.Errorf("building wipe map of thin pool %s: %w", pool.FullName(), err)
		}

		for device, cover := range covers {
			wipeMap[deviceIdToVolume[device]] = cover.Multiplied(superblock.DataBlockSize)
		}
	}
	return wipeMap, nil
}
