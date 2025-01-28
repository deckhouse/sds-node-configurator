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
	"agent/internal/nsenter"
	"bytes"
	"encoding/xml"
	"fmt"
)

type Time uint64
type Transaction uint64

type Superblock struct {
	XMLName       xml.Name    `xml:"superblock"`
	Uuid          string      `xml:"uuid,attr"`
	Time          Time        `xml:"time,attr"`
	Transaction   Transaction `xml:"transaction,attr"`
	Flags         uint64      `xml:"flags,attr"`
	Version       uint32      `xml:"version,attr"`
	DataBlockSize uint64      `xml:"data_block_size,attr"`
	NrDataBlocks  uint64      `xml:"nr_data_blocks,attr"`
	Devices       []Device    `xml:"device"`
}

type Device struct {
	XMLName        xml.Name        `xml:"device"`
	DevId          ThinDeviceId    `xml:"dev_id,attr"`
	MappedBlocks   uint64          `xml:"mapped_blocks,attr"`
	Transaction    Transaction     `xml:"transaction,attr"`
	CreationTime   Time            `xml:"creation_time,attr"`
	SnapTime       Time            `xml:"snap_time,attr"`
	RangeMappings  []RangeMapping  `xml:"range_mapping"`
	SingleMappings []SingleMapping `xml:"single_mapping"`
}

type RangeMapping struct {
	XMLName     xml.Name `xml:"range_mapping"`
	OriginBegin uint64   `xml:"origin_begin,attr"`
	DataBegin   uint64   `xml:"data_begin,attr"`
	Length      uint64   `xml:"length,attr"`
	Time        Time     `xml:"time,attr"`
}

type SingleMapping struct {
	XMLName     xml.Name `xml:"single_mapping"`
	OriginBlock uint64   `xml:"origin_block,attr"`
	DataBlock   uint64   `xml:"data_block,attr"`
	Time        Time     `xml:"time,attr"`
}

func ThinDump(ThinPool ThinPool) (*bytes.Buffer, error) {
	dmPath, err := ThinPool.DeviceMapperPath()

	if err != nil {
		return nil, fmt.Errorf("getting device mapper path: %w", err)
	}

	tpool := dmPath + "-tpool"
	tmeta := dmPath + "_tmeta"

	if err := nsenter.Command("dmsetup", "message", tpool, "0", "reserve_metadata_snap").Run(); err != nil {
		return nil, fmt.Errorf("reserving metadata snapshot: %w", err)
	}
	defer func() {
		if err := nsenter.Command("dmsetup", "message", tpool, "0", "release_metadata_snap").Run(); err != nil {
			panic(fmt.Errorf("releasing metadata snapshot: %w", err))
		}
	}()

	cmd := Command("thin_dump", tmeta, "-m", "-f", "xml")

	var outs bytes.Buffer
	cmd.Stdout = &outs

	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("dumping metadata: %w", err)
	}

	return &outs, nil
}

