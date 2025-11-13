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

	"golang.org/x/sys/unix"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/cache"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/logger"
)

func VolumeCleanup(ctx context.Context, log logger.Logger, sdsCache *cache.Cache, lv *cache.LVData, volumeCleanup string) (shouldRequeue bool, err error) {
	log = log.WithName("VolumeCleanup")
	vgName := lv.Data.VGName
	lvName := lv.Data.LVName

	log.Debug("finding used blocks")

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
	log = log.WithName("VolumeCleanupWithRangeCover").WithValues("lvName", lvName, "vgName", vgName, "volumeCleanup", volumeCleanup)
	log.Trace("cleaning up volume in volume group using with block ranges", "usedBlockRanges", usedBlockRanges)

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
		log.Error(err, "fail to cleanup volume", "devicePath", devicePath)
		return fmt.Errorf("cleaning volume %s: %w", devicePath, err)
	}

	return nil
}

func UsedBlockRangeForThinVolume(ctx context.Context, log logger.Logger, sdsCache *cache.Cache, lv *cache.LVData) (*RangeCover, error) {
	log = log.WithName("UsedBlockRangeForThinVolume")
	if lv.Data.PoolName == "" {
		return nil, nil
	}

	vgName := lv.Data.VGName
	lvName := lv.Data.LVName
	log = log.WithValues("lvName", lvName, "vgName", vgName)

	tpool, poolMetadataMapper, err := sdsCache.FindThinPoolMappers(lv)
	if err != nil {
		err = fmt.Errorf("finding mappers for thin pool %s: %w", lv.Data.PoolName, err)
		log.Error(err, "can't find pool for LV in VG")
		return nil, err
	}

	log.Debug("tpool tmeta", "tpool", tpool, "tmeta", poolMetadataMapper)
	if lv.Data.ThinID == "" {
		err = fmt.Errorf("missing deviceId for thin volume %s", lvName)
		log.Error(err, "can't find pool for LV in VG")
		return nil, err
	}
	superblock, err := ThinDump(ctx, log, tpool, poolMetadataMapper, lv.Data.ThinID)
	if err != nil {
		err = fmt.Errorf("dumping thin pool map: %w", err)
		log.Error(err, "can't find pool map for LV in VG")
		return nil, err
	}
	thinID, err := strconv.Atoi(lv.Data.ThinID)
	if err != nil {
		err = fmt.Errorf("deviceId %s is not a number: %w", lv.Data.ThinID, err)
		return nil, err
	}
	log.Debug("ThinID", "thinID", thinID)
	blockRanges, err := ThinVolumeUsedRanges(ctx, log, superblock, LVMThinDeviceID(thinID))
	if err != nil {
		err = fmt.Errorf("finding used ranges for deviceId %d in thin pool %s: %w", thinID, lv.Data.PoolName, err)
		return nil, err
	}
	log.Debug("ranges", "blockRanges", blockRanges)
	return &blockRanges, nil
}

func volumeCleanupOverwrite(_ context.Context, log logger.Logger, deviceOpener BlockDeviceOpener, devicePath, inputPath string, passes int, usedBlockRanges *RangeCover) (err error) {
	log = log.WithName("volumeCleanupOverwrite").WithValues(
		"devicePath", devicePath,
		"inputPath", inputPath,
		"passes", passes)
	log.Trace("overwriting by in passes")

	closeFile := func(file BlockDevice) {
		log.Trace("closing", "fileName", file.Name())
		closingErr := file.Close()
		if closingErr != nil {
			log.Error(closingErr, "While closing file", "fileName", file.Name())
			err = errors.Join(err, fmt.Errorf("closing file %s: %w", file.Name(), closingErr))
		}
	}

	input, err := deviceOpener.Open(inputPath, unix.O_RDONLY)
	if err != nil {
		log.Error(err, "Opening file")
		return fmt.Errorf("opening source device %s to wipe: %w", inputPath, err)
	}
	defer closeFile(input)

	output, err := deviceOpener.Open(devicePath, unix.O_DIRECT|unix.O_RDWR)
	if err != nil {
		log.Error(err, "Opening file")
		return fmt.Errorf("opening device %s to wipe: %w", devicePath, err)
	}
	defer closeFile(output)

	var usedByteRanges RangeCover

	if usedBlockRanges == nil {
		size, err := output.Size()
		if err != nil {
			log.Error(err, "Finding volume size")
			return fmt.Errorf("can't find the size of device %s: %w", devicePath, err)
		}

		log.Debug("device size. Overwriting whole device", "size", size)
		usedByteRanges = RangeCover{Range{Start: 0, Count: size}}
	} else {
		blockSize, err := output.BlockSize()
		if err != nil {
			log.Error(err, "Finding block size")
			return fmt.Errorf("can't find the block size of device %s: %w", devicePath, err)
		}
		log.Debug("device block size", "blockSize", blockSize)
		usedByteRanges = usedBlockRanges.Multiplied(int64(blockSize))
	}

	log.Debug("overwriting byte ranges", "usedByteRanges", usedByteRanges)

	bufferSize := 1024 * 1024 * 4
	buffer := make([]byte, bufferSize)
	for pass := 0; pass < passes; pass++ {
		for _, usedByteRange := range usedByteRanges {
			bytesToWrite := usedByteRange.Count
			log.Debug("Overwriting bytes with offset. Pass",
				"bytesToWrite", bytesToWrite,
				"offset", usedByteRange.Start,
				"pass", pass)
			start := time.Now()
			written, err := io.CopyBuffer(
				io.NewOffsetWriter(output, usedByteRange.Start),
				io.LimitReader(input, bytesToWrite),
				buffer)
			log.Info("Overwriting is done", "duration", time.Since(start))
			if err != nil {
				log.Error(err, "copying from to")
				return fmt.Errorf("copying from %s to %s: %w", inputPath, devicePath, err)
			}

			if written != bytesToWrite {
				log.Error(err, "only bytes written, expected", "written", written, "expected", bytesToWrite)
				return fmt.Errorf("only %d bytes written, expected %d", written, bytesToWrite)
			}
		}
	}

	return nil
}

func volumeCleanupDiscard(_ context.Context, log logger.Logger, deviceOpener BlockDeviceOpener, devicePath string) (err error) {
	log = log.WithName("volumeCleanupDiscard").WithValues("devicePath", devicePath)
	log.Trace("discarding")
	device, err := deviceOpener.Open(devicePath, unix.O_RDWR)
	if err != nil {
		log.Error(err, "Opening device")
		return fmt.Errorf("opening device %s to wipe: %w", devicePath, err)
	}
	defer func() {
		log.Trace("Closing file")
		closingErr := device.Close()
		if closingErr != nil {
			log.Error(closingErr, "While closing device")
			err = errors.Join(err, fmt.Errorf("closing file %s: %w", device.Name(), closingErr))
		}
	}()

	deviceSize, err := device.Size()
	if err != nil {
		log.Error(err, "can't find the size of device")
		return fmt.Errorf("can't find the size of device %s: %w", devicePath, err)
	}

	start := time.Now()
	log.Debug("Discarding all bytes", "deviceSize", deviceSize)
	defer func() {
		log.Info("Discarding is done", "duration", time.Since(start))
	}()

	return device.Discard(0, uint64(deviceSize))
}
