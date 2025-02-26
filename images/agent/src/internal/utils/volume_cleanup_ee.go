//go:build !ce

/*
Copyright 2025 Flant JSC
Licensed under the Deckhouse Platform Enterprise Edition (EE) license. See https://github.com/deckhouse/deckhouse/blob/main/ee/LICENSE
*/

package utils

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"time"

	"github.com/deckhouse/sds-node-configurator/lib/go/common/pkg/feature"
	"golang.org/x/sys/unix"

	"agent/internal/logger"
)

func VolumeCleanup(ctx context.Context, log logger.Logger, deviceOpener BlockDeviceOpener, vgName string, lvName, volumeCleanup string, usedRanges *RangeCover) error {
	log.Trace(fmt.Sprintf("[VolumeCleanup] cleaning up volume %s in volume group %s using %s", lvName, vgName, volumeCleanup))
	if !feature.VolumeCleanupEnabled() {
		return fmt.Errorf("volume cleanup is not supported in your edition")
	}

	devicePath := filepath.Join("/dev", vgName, lvName)
	randomSource := "/dev/urandom"

	var err error

	switch volumeCleanup {
	case "RandomFillSinglePass":
		err = volumeCleanupOverwrite(ctx, log, deviceOpener, devicePath, randomSource, 1, usedRanges)
	case "RandomFillThreePass":
		err = volumeCleanupOverwrite(ctx, log, deviceOpener, devicePath, randomSource, 3, usedRanges)
	case "Discard":
		err = volumeCleanupDiscard(ctx, log, deviceOpener, devicePath)
	default:
		return fmt.Errorf("unknown cleanup method %s", volumeCleanup)
	}

	if err != nil {
		log.Error(err, fmt.Sprintf("[VolumeCleanup] fail to cleanup volume %s", devicePath))
		return fmt.Errorf("cleaning volume %s: %w", devicePath, err)
	}

	return nil
}

func volumeCleanupOverwrite(_ context.Context, log logger.Logger, deviceOpener BlockDeviceOpener, devicePath, inputPath string, passes int, usedRanges *RangeCover) (err error) {
	log.Trace(fmt.Sprintf("[volumeCleanupOverwrite] overwriting %s by %s in %d passes", devicePath, inputPath, passes))
	closeFile := func(file BlockDevice) {
		log.Trace(fmt.Sprintf("[volumeCleanupOverwrite] closing %s", file.Name()))
		closingErr := file.Close()
		if closingErr != nil {
			log.Error(closingErr, fmt.Sprintf("[volumeCleanupOverwrite] While closing file %s", file.Name()))
			err = errors.Join(err, fmt.Errorf("closing file %s: %w", file.Name(), closingErr))
		}
	}

	input, err := deviceOpener.Open(inputPath, unix.O_RDONLY)
	if err != nil {
		log.Error(err, fmt.Sprintf("[volumeCleanupOverwrite] Opening file %s", inputPath))
		return fmt.Errorf("opening source device %s to wipe: %w", inputPath, err)
	}
	defer closeFile(input)

	output, err := deviceOpener.Open(devicePath, unix.O_DIRECT|unix.O_RDWR)
	if err != nil {
		log.Error(err, fmt.Sprintf("[volumeCleanupOverwrite] Opening file %s", devicePath))
		return fmt.Errorf("opening device %s to wipe: %w", devicePath, err)
	}
	defer closeFile(output)

	if usedRanges == nil {
		size, err := output.Size()
		if err != nil {
			log.Error(err, "[volumeCleanupOverwrite] Finding volume size")
			return fmt.Errorf("can't find the size of device %s: %w", devicePath, err)
		}

		usedRanges = &RangeCover{Range{Start: 0, Count: size}}
	}

	bufferSize := 1024 * 1024 * 4
	buffer := make([]byte, bufferSize)
	for pass := 0; pass < passes; pass++ {
		for _, usedRange := range *usedRanges {
			bytesToWrite := usedRange.Count
			log.Debug(fmt.Sprintf("[volumeCleanupOverwrite] Overwriting %d bytes with offset %d. Pass %d", bytesToWrite, usedRange.Start, pass))
			start := time.Now()
			written, err := io.CopyBuffer(
				io.NewOffsetWriter(output, usedRange.Start),
				io.LimitReader(input, bytesToWrite),
				buffer)
			log.Info(fmt.Sprintf("[volumeCleanupOverwrite] Overwriting is done in %s", time.Since(start).String()))
			if err != nil {
				log.Error(err, fmt.Sprintf("[volumeCleanupOverwrite] copying from %s to %s", inputPath, devicePath))
				return fmt.Errorf("copying from %s to %s: %w", inputPath, devicePath, err)
			}

			if written != bytesToWrite {
				log.Error(err, fmt.Sprintf("[volumeCleanupOverwrite] only %d bytes written, expected %d", written, bytesToWrite))
				return fmt.Errorf("only %d bytes written, expected %d", written, bytesToWrite)
			}
		}
	}

	return nil
}

func volumeCleanupDiscard(_ context.Context, log logger.Logger, deviceOpener BlockDeviceOpener, devicePath string) (err error) {
	log.Trace(fmt.Sprintf("[volumeCleanupDiscard] discarding %s", devicePath))
	device, err := deviceOpener.Open(devicePath, unix.O_RDWR)
	if err != nil {
		log.Error(err, fmt.Sprintf("[volumeCleanupDiscard] Opening device %s", devicePath))
		return fmt.Errorf("opening device %s to wipe: %w", devicePath, err)
	}
	defer func() {
		log.Trace(fmt.Sprintf("Closing file %s", devicePath))
		closingErr := device.Close()
		if closingErr != nil {
			log.Error(closingErr, fmt.Sprintf("[volumeCleanupDiscard] While closing deice %s", devicePath))
			err = errors.Join(err, fmt.Errorf("closing file %s: %w", device.Name(), closingErr))
		}
	}()

	deviceSize, err := device.Size()
	if err != nil {
		log.Error(err, fmt.Sprintf("[volumeCleanupDiscard] can't find the size of device %s", devicePath))
		return fmt.Errorf("can't find the size of device %s: %w", devicePath, err)
	}

	start := time.Now()
	log.Debug(fmt.Sprintf("[volumeCleanupDiscard] Discarding all %d bytes", deviceSize))
	defer func() {
		log.Info(fmt.Sprintf("[volumeCleanupDiscard] Discarding is done in %s", time.Since(start).String()))
	}()

	return device.Discard(0, uint64(deviceSize))
}
