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
	"strconv"
	"time"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"golang.org/x/sys/unix"

	"agent/internal/cache"
	"agent/internal/logger"
)

func VolumeCleanup(ctx context.Context, log logger.Logger, sdsCache *cache.Cache, lv *cache.LVData, volumeCleanup string) (shouldRequeue bool, err error) {
	vgName := lv.Data.VGName
	lvName := lv.Data.LVName

	log.Debug("[deleteLVIfNeeded] finding used blocks")

	usedBlockRanges, err := UsedBlockRangeForThinVolume(ctx, log, sdsCache, lv)
	if err != nil {
		return true, err
	}

	if err := VolumeCleanupWithRangeCover(ctx, log, OsDeviceOpener(), vgName, lvName, volumeCleanup, usedBlockRanges); err != nil {
		return false, err
	}
	return false, nil
}

func VolumeCleanupWithRangeCover(ctx context.Context, log logger.Logger, deviceOpener BlockDeviceOpener, vgName string, lvName, volumeCleanup string, usedBlockRanges *RangeCover) error {
	log.Trace(fmt.Sprintf("[VolumeCleanup] cleaning up volume %s in volume group %s using %s with block ranges %v", lvName, vgName, volumeCleanup, usedBlockRanges))

	devicePath := filepath.Join("/dev", vgName, lvName)
	randomSource := "/dev/urandom"

	var err error

	switch volumeCleanup {
	case v1alpha1.VolumeCleanupRandomFillSinglePass:
		err = volumeCleanupOverwrite(ctx, log, deviceOpener, devicePath, randomSource, 1, usedBlockRanges)
	case v1alpha1.VolumeCleanupRandomFillThreePass:
		err = volumeCleanupOverwrite(ctx, log, deviceOpener, devicePath, randomSource, 3, usedBlockRanges)
	case v1alpha1.VolumeCleanupDiscard:
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

func UsedBlockRangeForThinVolume(ctx context.Context, log logger.Logger, sdsCache *cache.Cache, lv *cache.LVData) (*RangeCover, error) {
	if lv.Data.PoolName == "" {
		return nil, nil
	}

	vgName := lv.Data.VGName
	lvName := lv.Data.LVName

	tpool, poolMetadataMapper, err := sdsCache.FindThinPoolMappers(lv)
	if err != nil {
		err = fmt.Errorf("finding mappers for thin pool %s: %w", lv.Data.PoolName, err)
		log.Error(err, fmt.Sprintf("[UsedBlockRangeForThinVolume] can't find pool for LV %s in VG %s", lvName, vgName))
		return nil, err
	}

	log.Debug(fmt.Sprintf("[UsedBlockRangeForThinVolume] tpool %s tmeta %s", tpool, poolMetadataMapper))
	if lv.Data.ThinID == "" {
		err = fmt.Errorf("missing deviceId for thin volume %s", lvName)
		log.Error(err, fmt.Sprintf("[UsedBlockRangeForThinVolume] can't find pool for LV %s in VG %s", lvName, vgName))
		return nil, err
	}
	superblock, err := ThinDump(ctx, log, tpool, poolMetadataMapper, lv.Data.ThinID)
	if err != nil {
		err = fmt.Errorf("dumping thin pool map: %w", err)
		log.Error(err, fmt.Sprintf("[UsedBlockRangeForThinVolume] can't find pool map for LV %s in VG %s", lvName, vgName))
		return nil, err
	}
	thinID, err := strconv.Atoi(lv.Data.ThinID)
	if err != nil {
		err = fmt.Errorf("deviceId %s is not a number: %w", lv.Data.ThinID, err)
		return nil, err
	}
	log.Debug(fmt.Sprintf("[UsedBlockRangeForThinVolume] ThinID %d", thinID))
	blockRanges, err := ThinVolumeUsedRanges(ctx, log, superblock, LVMThinDeviceID(thinID))
	if err != nil {
		err = fmt.Errorf("finding used ranges for deviceId %d in thin pool %s: %w", thinID, lv.Data.PoolName, err)
		return nil, err
	}
	log.Debug(fmt.Sprintf("[UsedBlockRangeForThinVolume] ranges %v", blockRanges))
	return &blockRanges, nil
}

func volumeCleanupOverwrite(_ context.Context, log logger.Logger, deviceOpener BlockDeviceOpener, devicePath, inputPath string, passes int, usedBlockRanges *RangeCover) (err error) {
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

	var usedByteRanges RangeCover

	if usedBlockRanges == nil {
		size, err := output.Size()
		if err != nil {
			log.Error(err, "[volumeCleanupOverwrite] Finding volume size")
			return fmt.Errorf("can't find the size of device %s: %w", devicePath, err)
		}

		log.Debug(fmt.Sprintf("[volumeCleanupOverwrite] device size is %d. Overwriting whole device.", size))
		usedByteRanges = RangeCover{Range{Start: 0, Count: size}}
	} else {
		blockSize, err := output.BlockSize()
		if err != nil {
			log.Error(err, "[volumeCleanupOverwrite] Finding block size")
			return fmt.Errorf("can't find the block size of device %s: %w", devicePath, err)
		}
		log.Debug(fmt.Sprintf("[volumeCleanupOverwrite] device block size is %d", blockSize))
		usedByteRanges = usedBlockRanges.Multiplied(int64(blockSize))
	}

	log.Debug(fmt.Sprintf("[volumeCleanupOverwrite] overwriting byte ranges %v", usedByteRanges))

	bufferSize := 1024 * 1024 * 4
	buffer := make([]byte, bufferSize)
	for pass := 0; pass < passes; pass++ {
		for _, usedByteRange := range usedByteRanges {
			bytesToWrite := usedByteRange.Count
			log.Debug(fmt.Sprintf("[volumeCleanupOverwrite] Overwriting %d bytes with offset %d. Pass %d", bytesToWrite, usedByteRange.Start, pass))
			start := time.Now()
			written, err := io.CopyBuffer(
				io.NewOffsetWriter(output, usedByteRange.Start),
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
