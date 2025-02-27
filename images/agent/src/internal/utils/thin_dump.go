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

package utils

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"

	"agent/internal/logger"
)

type (
	LVMTime         = int64
	LVMTransaction  = int64
	LVMThinDeviceID = int64
)

type Superblock struct {
	XMLName       xml.Name       `xml:"superblock"`
	UUID          string         `xml:"uuid,attr"`
	Time          LVMTime        `xml:"time,attr"`
	Transaction   LVMTransaction `xml:"transaction,attr"`
	Flags         int64          `xml:"flags,attr"`
	Version       int32          `xml:"version,attr"`
	DataBlockSize int64          `xml:"data_block_size,attr"`
	NrDataBlocks  int64          `xml:"nr_data_blocks,attr"`
	Devices       []Device       `xml:"device"`
}

type Device struct {
	XMLName        xml.Name        `xml:"device"`
	DevID          LVMThinDeviceID `xml:"dev_id,attr"`
	MappedBlocks   int64           `xml:"mapped_blocks,attr"`
	Transaction    LVMTransaction  `xml:"transaction,attr"`
	CreationTime   LVMTime         `xml:"creation_time,attr"`
	SnapTime       LVMTime         `xml:"snap_time,attr"`
	RangeMappings  []RangeMapping  `xml:"range_mapping"`
	SingleMappings []SingleMapping `xml:"single_mapping"`
}

type RangeMapping struct {
	XMLName     xml.Name `xml:"range_mapping"`
	OriginBegin int64    `xml:"origin_begin,attr"`
	DataBegin   int64    `xml:"data_begin,attr"`
	Length      int64    `xml:"length,attr"`
	Time        LVMTime  `xml:"time,attr"`
}

type SingleMapping struct {
	XMLName     xml.Name `xml:"single_mapping"`
	OriginBlock int64    `xml:"origin_block,attr"`
	DataBlock   int64    `xml:"data_block,attr"`
	Time        LVMTime  `xml:"time,attr"`
}

func ThinDump(ctx context.Context, log logger.Logger, tpool, tmeta string) (superblock Superblock, err error) {
	log.Trace(fmt.Sprintf("[ThinDump] calling for tpool %s tmeta %s", tpool, tmeta))

	var rawOut []byte
	rawOut, err = ThinDumpRaw(ctx, log, tpool, tmeta)
	if err != nil {
		return
	}

	log.Debug("[ThinDump] unmarshaling")
	if err = xml.Unmarshal(rawOut, &superblock); err != nil {
		log.Error(err, "[ThinDump] unmarshaling error")
		err = fmt.Errorf("parsing metadata: %w", err)
		return
	}

	return superblock, nil
}

func ThinVolumeUsedRanges(_ context.Context, log logger.Logger, superblock Superblock, deviceID LVMThinDeviceID) (blockRanges RangeCover, err error) {
	log.Trace(fmt.Sprintf("[ThinVolumeUsedRanges] calling for deviceId %d", deviceID))
	for _, device := range superblock.Devices {
		if device.DevID != deviceID {
			continue
		}

		blockRanges = make(RangeCover, 0, len(device.RangeMappings)+len(device.SingleMappings))

		for _, mapping := range device.RangeMappings {
			blockRanges = append(blockRanges, Range{Start: mapping.DataBegin, Count: mapping.Length})
		}

		for _, mapping := range device.SingleMappings {
			blockRanges = append(blockRanges, Range{Start: mapping.DataBlock, Count: 1})
		}

		blockRanges, err = blockRanges.Merged()
		if err != nil {
			err = fmt.Errorf("finding used ranges: %w", err)
			return
		}

		return blockRanges.Multiplied(superblock.DataBlockSize), nil
	}
	return blockRanges, errors.New("device not found")
}
