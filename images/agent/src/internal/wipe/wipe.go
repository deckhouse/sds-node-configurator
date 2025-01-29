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

package wipe

import (
	"agent/internal/lvm"
	"agent/internal/nsenter"
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"
)

type Method uint8

const (
	WipeNone Method = iota
	WipeDiscard
	WipeOnePassRandom
	WipeThreePass
)

type WipeConfig struct {
	Method Method
}

type Range lvm.Range
type RangeCover lvm.RangeCover

func Wipe(cfg WipeConfig, blockDevice string, sectors *RangeCover) error {
	switch cfg.Method {
	case WipeNone:
		return nil
	case WipeDiscard:
		return WipeBlkiscard(blockDevice, sectors)
	case WipeOnePassRandom:
		return WipeCopy(blockDevice, "/dev/urandom", sectors)
	}
	return errors.ErrUnsupported
}

func WipeCopy(blockDevice string, sourceDevice string, wipeSectors *RangeCover) error {
	var stat syscall.Stat_t
	if err := syscall.Stat(blockDevice, &stat); err != nil {
		return fmt.Errorf("stat call failed: %w", err)
	}

	if stat.Blksize <= 0 {
		return fmt.Errorf("block size %d is invalid", stat.Blksize)
	}

	if wipeSectors == nil {
		wipeSectors = &RangeCover{{Start: 0, Count: stat.Blocks}}
	}

	source, err := os.OpenFile(sourceDevice, syscall.O_RDONLY, os.ModeDevice)
	if err != nil {
		return fmt.Errorf("opening source device %s to wipe: %w", sourceDevice, err)
	}

	out, err := os.OpenFile(blockDevice, 0, os.ModeDevice)
	if err != nil {
		return fmt.Errorf("opening device %s to wipe: %w", blockDevice, err)
	}

	for _, r := range *wipeSectors {
		byteRange := r.Multiplied(stat.Blksize)
		written, err := io.CopyN(
			io.NewOffsetWriter(out, byteRange.Start),
			source,
			r.Count)

		if err != nil {
			return fmt.Errorf("writing range %v of %s: %w", byteRange, blockDevice, err)
		}

		if written != int64(byteRange.Count) {
			return fmt.Errorf("only %d bytes written, expected %d", written, byteRange.Count)
		}
	}

	return nil
}

func WipeBlkiscard(blockDevice string, wipeSectors *RangeCover) error {
	var stat syscall.Stat_t
	if err := syscall.Stat(blockDevice, &stat); err != nil {
		return fmt.Errorf("stat call failed: %w", err)
	}

	if stat.Blksize <= 0 {
		return fmt.Errorf("block size %d is invalid", stat.Blksize)
	}

	if wipeSectors == nil {
		wipeSectors = &RangeCover{{Start: 0, Count: stat.Blocks}}
	}

	// TODO:
}
