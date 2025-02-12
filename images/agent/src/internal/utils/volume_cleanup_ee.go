//go:build !ce

/*
Copyright 2025 Flant JSC
Licensed under the Deckhouse Platform Enterprise Edition (EE) license. See https://github.com/deckhouse/deckhouse/blob/main/ee/LICENSE
*/

package utils

import (
	"agent/internal/logger"
	"context"
	"fmt"
	"io"
	"os"
	"syscall"
	"unsafe"

	commonfeature "github.com/deckhouse/sds-node-configurator/lib/go/common/pkg/feature"
)

func VolumeCleanup(ctx context.Context, log logger.Logger, vgName, lvName, volumeCleanupMethod string) error {
	if !commonfeature.VolumeCleanupEnabled() {
		return fmt.Errorf("Volume cleanup is not supported in your edition.")
	}

	devicePath := fmt.Sprintf("/dev/%s/%s", vgName, lvName)
	randomSource := "/dev/urandom"

	switch volumeCleanupMethod {
	case "Disable":
		return nil
	case "SinglePass":
		return VolumeCleanupCopy(devicePath, randomSource, 1)
	case "ThreePass":
		return VolumeCleanupCopy(devicePath, randomSource, 3)
	case "Discard":
		return VolumeCleanupDiscard(devicePath)
	}

	return fmt.Errorf("Unknown cleanup method %s", volumeCleanupMethod)
}

func _VolumeSize(stat syscall.Stat_t) (int64, error) {
	if stat.Size > 0 {
		return stat.Size, nil
	}

	if stat.Blksize <= 0 {
		return 0, fmt.Errorf("block size %d is invalid", stat.Blksize)
	}
	if stat.Blocks <= 0 {
		return 0, fmt.Errorf("block count %d is invalid", stat.Blocks)
	}

	return stat.Blksize * stat.Blocks, nil
}

func VolumeCleanupCopy(outputPath, inputPath string, passes int) error {
	var stat syscall.Stat_t
	if err := syscall.Stat(outputPath, &stat); err != nil {
		return fmt.Errorf("stat call failed: %w", err)
	}

	bytesToWrite, err := _VolumeSize(stat)
	if err != nil {
		return fmt.Errorf("can't find the size of device: %w", err)
	}

	input, err := os.OpenFile(inputPath, syscall.O_RDONLY, os.ModeDevice)
	if err != nil {
		return fmt.Errorf("opening source device %s to wipe: %w", inputPath, err)
	}

	output, err := os.OpenFile(outputPath, syscall.O_DIRECT, os.ModeDevice)
	if err != nil {
		return fmt.Errorf("opening device %s to wipe: %w", outputPath, err)
	}

	written, err := io.CopyN(
		output,
		input,
		bytesToWrite)

	if err != nil {
		return fmt.Errorf("copying from %s to %s: %w", inputPath, outputPath, err)
	}

	if written != int64(bytesToWrite) {
		return fmt.Errorf("only %d bytes written, expected %d", written, bytesToWrite)
	}

	return nil
}

const (
	BLKDISCARD       = 0x1277
	BLKDISCARDZEROES = 0x127c
	BLKSECDISCARD    = 0x127d
)

type Range struct {
	start, count uint64
}

func VolumeCleanupDiscard(devicePath string) error {
	var stat syscall.Stat_t
	if err := syscall.Stat(devicePath, &stat); err != nil {
		return fmt.Errorf("stat call failed: %w", err)
	}

	deviceSize, err := _VolumeSize(stat)
	if err != nil {
		return fmt.Errorf("can't find the size of device: %w", err)
	}

	device, err := os.OpenFile(devicePath, syscall.O_DIRECT, os.ModeDevice)
	if err != nil {
		return fmt.Errorf("opening device %s to wipe: %w", devicePath, err)
	}

	rng := Range{
		start: 0,
		count: uint64(deviceSize),
	}

	_, _, err = syscall.Syscall(syscall.SYS_IOCTL, uintptr(device.Fd()), BLKDISCARD, uintptr(unsafe.Pointer(&rng)))

	if err != nil {
		return fmt.Errorf("calling ioctl BLKDISCARD: %w", err)
	}

	return nil
}
