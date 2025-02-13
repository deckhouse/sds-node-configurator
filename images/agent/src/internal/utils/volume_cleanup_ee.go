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

	var err error
	closingErrors := []error{}

	switch volumeCleanupMethod {
	case "Disable":
		return nil
	case "SinglePass":
		err = volumeCleanupCopy(ctx, log, &closingErrors, devicePath, randomSource, 1)
		break
	case "ThreePass":
		err = volumeCleanupCopy(ctx, log, &closingErrors, devicePath, randomSource, 3)
		break
	case "Discard":
		err = volumeCleanupDiscard(ctx, log, &closingErrors, devicePath)
		break
	default:
		return fmt.Errorf("unknown cleanup method %s", volumeCleanupMethod)
	}

	if err == nil && len(closingErrors) > 0 {
		err = closingErrors[0]
		closingErrors = closingErrors[1:]
	}

	if len(closingErrors) == 0 {
		return fmt.Errorf("cleaning volume %s: %w", devicePath, err)
	} else {
		return fmt.Errorf("cleaning volume %s: %w, errors while closing files %v", devicePath, err, closingErrors)
	}
}

func volumeSize(stat syscall.Stat_t) (int64, error) {
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

func volumeCleanupCopy(ctx context.Context, log logger.Logger, closingErrors *[]error, outputPath, inputPath string, passes int) error {
	var outputStat syscall.Stat_t
	if err := syscall.Stat(outputPath, &outputStat); err != nil {
		return fmt.Errorf("stat call failed: %w", err)
	}

	close := func(file *os.File) {
		log := log.GetLogger().WithValues("name", file.Name())
		// log.Info("Closing file", "name")
		err := file.Close()
		if err != nil {
			log.Error(err, "While closing file")
			*closingErrors = append(*closingErrors, fmt.Errorf("closing file %s: %w", file.Name(), err))
		}
	}

	input, err := os.OpenFile(inputPath, syscall.O_RDONLY, os.ModeDevice)
	if err != nil {
		return fmt.Errorf("opening source device %s to wipe: %w", inputPath, err)
	}
	defer close(input)

	output, err := os.OpenFile(outputPath, syscall.O_DIRECT, os.ModeDevice)
	if err != nil {
		return fmt.Errorf("opening device %s to wipe: %w", outputPath, err)
	}
	defer close(output)

	bytesToWrite, err := volumeSize(outputStat)
	if err != nil {
		return fmt.Errorf("can't find the size of device %s: %w", outputPath, err)
	}

	for pass := 0; pass < passes; pass++ {
		written, err := io.CopyN(
			io.NewOffsetWriter(output, 0),
			input,
			bytesToWrite)

		if err != nil {
			return fmt.Errorf("copying from %s to %s: %w", inputPath, outputPath, err)
		}

		if written != int64(bytesToWrite) {
			return fmt.Errorf("only %d bytes written, expected %d", written, bytesToWrite)
		}
	}

	return err
}

const (
	BLKDISCARD       = 0x1277
	BLKDISCARDZEROES = 0x127c
	BLKSECDISCARD    = 0x127d
)

type Range struct {
	start, count uint64
}

func volumeCleanupDiscard(ctx context.Context, log logger.Logger, closingErrors *[]error, devicePath string) error {
	var stat syscall.Stat_t
	if err := syscall.Stat(devicePath, &stat); err != nil {
		return fmt.Errorf("stat call failed: %w", err)
	}

	deviceSize, err := volumeSize(stat)
	if err != nil {
		return fmt.Errorf("can't find the size of device: %w", err)
	}

	device, err := os.OpenFile(devicePath, syscall.O_DIRECT, os.ModeDevice)
	if err != nil {
		return fmt.Errorf("opening device %s to wipe: %w", devicePath, err)
	}
	defer func() {
		log.Info("Closing file", device)
		err := device.Close()
		if err != nil {
			*closingErrors = append(*closingErrors, fmt.Errorf("closing file %s: %w", device.Name(), err))
		}
	}()

	rng := Range{
		start: 0,
		count: uint64(deviceSize),
	}

	_, _, err = syscall.Syscall(
		syscall.SYS_IOCTL,
		uintptr(device.Fd()),
		uintptr(BLKDISCARD),
		uintptr(unsafe.Pointer(&rng)))

	if err != nil {
		return fmt.Errorf("calling ioctl BLKDISCARD: %w", err)
	}

	return nil
}
