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
	"os"
	"syscall"
	"time"
	"unsafe"

	"github.com/deckhouse/sds-node-configurator/lib/go/common/pkg/feature"

	"agent/internal/logger"
)

func VolumeCleanup(ctx context.Context, log logger.Logger, vgName, lvName, volumeCleanup string) error {
	log.Trace(fmt.Sprintf("[VolumeCleanup] cleaning up volume %s in volume group %s using %s", lvName, vgName, volumeCleanup))
	if !feature.VolumeCleanupEnabled() {
		return fmt.Errorf("volume cleanup is not supported in your edition")
	}

	devicePath := fmt.Sprintf("/dev/%s/%s", vgName, lvName)
	randomSource := "/dev/urandom"

	var err error
	closingErrors := []error{}

	switch volumeCleanup {
	case "RandomFillSinglePass":
		err = volumeCleanupOverwrite(ctx, log, &closingErrors, devicePath, randomSource, 1)
	case "RandomFillThreePass":
		err = volumeCleanupOverwrite(ctx, log, &closingErrors, devicePath, randomSource, 3)
	case "Discard":
		err = volumeCleanupDiscard(ctx, log, &closingErrors, devicePath)
	default:
		return fmt.Errorf("unknown cleanup method %s", volumeCleanup)
	}

	if err != nil && len(closingErrors) > 0 {
		closingErrors = append([]error{err}, closingErrors...)
	}

	if len(closingErrors) > 0 {
		err = errors.Join(closingErrors...)
	}

	if err == nil {
		return nil
	}

	log.Error(err, fmt.Sprintf("[VolumeCleanup] fail to cleanup volume %s", devicePath))
	return fmt.Errorf("cleaning volume %s: %w", devicePath, err)
}

func volumeSize(log logger.Logger, device *os.File) (int64, error) {
	log.Trace(fmt.Sprintf("[volumeSize] finding size of device %v", device))
	var stat syscall.Stat_t
	log.Debug("[volumeSize] Calling fstat")
	if err := syscall.Fstat(int(device.Fd()), &stat); err != nil {
		log.Error(err, "[volumeSize] Calling fstat")
		return 0, fmt.Errorf("fstat call failed: %w", err)
	}

	if stat.Size > 0 {
		log.Debug(fmt.Sprintf("[volumeSize] Size %d is valid.", stat.Size))
		return stat.Size, nil
	}

	if stat.Mode&S_IFMT != S_IFBLK {
		log.Debug(fmt.Sprintf("[volumeSize] Device mode %x", stat.Mode))
		return 0, fmt.Errorf("not a block device, mode: %x", stat.Mode)
	}

	var blockDeviceSize uint64
	_, _, errno := syscall.Syscall(
		syscall.SYS_IOCTL,
		device.Fd(),
		uintptr(BLKGETSIZE64),
		uintptr(unsafe.Pointer(&blockDeviceSize)))
	if errno != 0 {
		err := errors.New(errno.Error())
		log.Error(err, "[volumeSize] calling ioctl BLKGETSIZE64")
		return 0, fmt.Errorf("error calling ioctl BLKGETSIZE64: %w", err)
	}
	log.Debug(fmt.Sprintf("Block device size is %d", blockDeviceSize))
	if blockDeviceSize <= 0 {
		return 0, fmt.Errorf("block size is invalid")
	}

	return int64(blockDeviceSize), nil
}

func volumeCleanupOverwrite(_ context.Context, log logger.Logger, closingErrors *[]error, devicePath, inputPath string, passes int) error {
	log.Trace(fmt.Sprintf("[volumeCleanupOverwrite] overwriting %s by %s in %d passes", devicePath, inputPath, passes))
	closeFile := func(file *os.File) {
		log.Trace(fmt.Sprintf("[volumeCleanupOverwrite] closing %s", file.Name()))
		err := file.Close()
		if err != nil {
			log.Error(err, fmt.Sprintf("[volumeCleanupOverwrite] While closing file %s", file.Name()))
			*closingErrors = append(*closingErrors, fmt.Errorf("closing file %s: %w", file.Name(), err))
		}
	}

	input, err := os.OpenFile(inputPath, syscall.O_RDONLY, os.ModeDevice)
	if err != nil {
		log.Error(err, fmt.Sprintf("[volumeCleanupOverwrite] Opening file %s", inputPath))
		return fmt.Errorf("opening source device %s to wipe: %w", inputPath, err)
	}
	defer closeFile(input)

	output, err := os.OpenFile(devicePath, syscall.O_DIRECT|syscall.O_RDWR, os.ModeDevice)
	if err != nil {
		log.Error(err, fmt.Sprintf("[volumeCleanupOverwrite] Opening file %s", devicePath))
		return fmt.Errorf("opening device %s to wipe: %w", devicePath, err)
	}
	defer closeFile(output)

	bytesToWrite, err := volumeSize(log, output)
	if err != nil {
		log.Error(err, "[volumeCleanupOverwrite] Finding volume size")
		return fmt.Errorf("can't find the size of device %s: %w", devicePath, err)
	}

	bufferSize := 1024 * 1024 * 4
	buffer := make([]byte, bufferSize)
	for pass := 0; pass < passes; pass++ {
		log.Debug(fmt.Sprintf("[volumeCleanupOverwrite] Overwriting %d  bytes. Pass %d", bytesToWrite, pass))
		start := time.Now()
		written, err := io.CopyBuffer(
			io.NewOffsetWriter(output, 0),
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

	return err
}

/* To find these constant run:
gcc -o test -x c - <<EOF
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <linux/fs.h>
#include <stdio.h>

#define PRINT_CONSTANT(name, fmt) printf(#name " = " fmt "\n", name)

int main() {
    PRINT_CONSTANT(S_IFMT, "0x%x");
    PRINT_CONSTANT(S_IFBLK, "0x%x");
    PRINT_CONSTANT(BLKGETSIZE64, "0x%lx");
    PRINT_CONSTANT(BLKSSZGET, "0x%x");
    PRINT_CONSTANT(BLKDISCARD, "0x%x");
    PRINT_CONSTANT(BLKDISCARDZEROES, "0x%x");
    PRINT_CONSTANT(BLKSECDISCARD, "0x%x");
    return 0;
}
EOF
*/

//nolint:revive
const (
	BLKDISCARD       = 0x1277
	BLKDISCARDZEROES = 0x127c
	BLKSECDISCARD    = 0x127d

	BLKGETSIZE64 = 0x80081272
	BLKSSZGET    = 0x1268

	S_IFMT  = 0xf000 /* type of file mask */
	S_IFBLK = 0x6000 /* block special */
)

type Range struct {
	start, count uint64
}

func volumeCleanupDiscard(_ context.Context, log logger.Logger, closingErrors *[]error, devicePath string) error {
	log.Trace(fmt.Sprintf("[volumeCleanupDiscard] discarding %s", devicePath))
	device, err := os.OpenFile(devicePath, syscall.O_RDWR, os.ModeDevice)
	if err != nil {
		log.Error(err, fmt.Sprintf("[volumeCleanupDiscard] Opening device %s", devicePath))
		return fmt.Errorf("opening device %s to wipe: %w", devicePath, err)
	}
	defer func() {
		log.Trace(fmt.Sprintf("Closing file %s", devicePath))
		err := device.Close()
		if err != nil {
			log.Error(err, fmt.Sprintf("[volumeCleanupDiscard] While closing deice %s", devicePath))
			*closingErrors = append(*closingErrors, fmt.Errorf("closing file %s: %w", device.Name(), err))
		}
	}()

	deviceSize, err := volumeSize(log, device)
	if err != nil {
		log.Error(err, fmt.Sprintf("[volumeCleanupDiscard] can't find the size of device %s", devicePath))
		return fmt.Errorf("can't find the size of device %s: %w", devicePath, err)
	}

	rng := Range{
		start: 0,
		count: uint64(deviceSize),
	}

	log.Debug(fmt.Sprintf("[volumeCleanupDiscard] calling BLKDISCARD fd: %d, range %v", device.Fd(), rng))
	start := time.Now()

	_, _, errno := syscall.Syscall(
		syscall.SYS_IOCTL,
		device.Fd(),
		uintptr(BLKDISCARD),
		uintptr(unsafe.Pointer(&rng)))

	log.Info(fmt.Sprintf("[volumeCleanupDiscard] BLKDISCARD is done in %s", time.Since(start).String()))

	if errno != 0 {
		err := errors.New(errno.Error())
		log.Error(err, "[volumeCleanupDiscard] error calling BLKDISCARD")
		return fmt.Errorf("calling ioctl BLKDISCARD: %s", err)
	}

	return nil
}
