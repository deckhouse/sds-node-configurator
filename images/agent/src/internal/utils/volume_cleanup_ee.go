//go:build !ce

/*
Copyright 2025 Flant JSC
Licensed under the Deckhouse Platform Enterprise Edition (EE) license. See https://github.com/deckhouse/deckhouse/blob/main/ee/LICENSE
*/

package utils

import (
	"context"
	"fmt"
	"io"
	"os"
	"syscall"
	"time"
	"unsafe"

	"github.com/deckhouse/sds-node-configurator/lib/go/common/pkg/feature"
	"github.com/go-logr/logr"

	"agent/internal/logger"
)

func VolumeCleanup(ctx context.Context, log logger.Logger, vgName, lvName, volumeCleanup string) error {
	myLog := log.GetLogger().WithValues("vgname", vgName, "lvname", lvName, "method", volumeCleanup)
	if !feature.VolumeCleanupEnabled() {
		return fmt.Errorf("volume cleanup is not supported in your edition")
	}

	devicePath := fmt.Sprintf("/dev/%s/%s", vgName, lvName)
	randomSource := "/dev/urandom"

	var err error
	closingErrors := []error{}

	switch volumeCleanup {
	case "RandomFillSinglePass":
		err = volumeCleanupOverwrite(ctx, myLog, &closingErrors, devicePath, randomSource, 1)
	case "RandomFillThreePass":
		err = volumeCleanupOverwrite(ctx, myLog, &closingErrors, devicePath, randomSource, 3)
	case "Discard":
		err = volumeCleanupDiscard(ctx, myLog, &closingErrors, devicePath)
	default:
		return fmt.Errorf("unknown cleanup method %s", volumeCleanup)
	}

	if err == nil && len(closingErrors) > 0 {
		err = closingErrors[0]
		closingErrors = closingErrors[1:]
	}

	if err == nil {
		return nil
	}

	if len(closingErrors) == 0 {
		return fmt.Errorf("cleaning volume %s: %w", devicePath, err)
	}
	return fmt.Errorf("cleaning volume %s: %w, errors while closing files %v", devicePath, err, closingErrors)
}

func volumeSize(log logr.Logger, device *os.File) (int64, error) {
	log = log.WithValues("device", device.Name())
	var stat syscall.Stat_t
	log.Info("Calling fstat")
	if err := syscall.Fstat(int(device.Fd()), &stat); err != nil {
		log.Error(err, "[volumeSize] Calling fstat")
		return 0, fmt.Errorf("fstat call failed: %w", err)
	}

	if stat.Size > 0 {
		log.Info("[volumeSize] Size is valid.", "size", stat.Size)
		return stat.Size, nil
	}

	if stat.Mode&S_IFMT != S_IFBLK {
		log.Info("[volumeSize] Device mode", "mode", stat.Mode)
		return 0, fmt.Errorf("not a block device, mode: %x", stat.Mode)
	}

	var blockDeviceSize uint64
	_, _, errno := syscall.Syscall(
		syscall.SYS_IOCTL,
		device.Fd(),
		uintptr(BLKGETSIZE64),
		uintptr(unsafe.Pointer(&blockDeviceSize)))
	if errno != 0 {
		return 0, fmt.Errorf("error calling ioctl BLKGETSIZE64: %s", errno.Error())
	}
	log.Info("Block device size", "size", blockDeviceSize)
	if blockDeviceSize <= 0 {
		return 0, fmt.Errorf("block size is invalid")
	}

	return int64(blockDeviceSize), nil
}

func volumeCleanupOverwrite(_ context.Context, log logr.Logger, closingErrors *[]error, devicePath, inputPath string, passes int) error {
	log = log.WithValues("device", devicePath, "input", inputPath, "passes", passes)
	closeFile := func(file *os.File) {
		log := log.WithValues("name", file.Name())
		log.Info("[volumeCleanupOverwrite] Closing")
		err := file.Close()
		if err != nil {
			log.Error(err, "[volumeCleanupOverwrite] While closing")
			*closingErrors = append(*closingErrors, fmt.Errorf("closing file %s: %w", file.Name(), err))
		}
	}

	input, err := os.OpenFile(inputPath, syscall.O_RDONLY, os.ModeDevice)
	if err != nil {
		log.Error(err, "[volumeCleanupOverwrite] Opening file", "file", inputPath)
		return fmt.Errorf("opening source device %s to wipe: %w", inputPath, err)
	}
	defer closeFile(input)

	output, err := os.OpenFile(devicePath, syscall.O_DIRECT|syscall.O_RDWR, os.ModeDevice)
	if err != nil {
		log.Error(err, "[volumeCleanupOverwrite] Opening file", "file", devicePath)
		return fmt.Errorf("opening device %s to wipe: %w", devicePath, err)
	}
	defer closeFile(output)

	bytesToWrite, err := volumeSize(log, output)
	if err != nil {
		log.Error(err, "[volumeCleanupOverwrite] Finding volume size")
		return fmt.Errorf("can't find the size of device %s: %w", devicePath, err)
	}

	for pass := 0; pass < passes; pass++ {
		log.Info("[volumeCleanupOverwrite] Overwriting", "bytes", bytesToWrite, "pass", pass)
		start := time.Now()
		written, err := io.CopyN(
			io.NewOffsetWriter(output, 0),
			input,
			bytesToWrite)
		log.Info("[volumeCleanupOverwrite] Overwriting is done", "duration", time.Since(start).String())
		if err != nil {
			log.Error(err, "While overwriting")
			return fmt.Errorf("copying from %s to %s: %w", inputPath, devicePath, err)
		}

		if written != bytesToWrite {
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

func volumeCleanupDiscard(_ context.Context, log logr.Logger, closingErrors *[]error, devicePath string) error {
	log = log.WithValues("device", devicePath, "device", devicePath)
	device, err := os.OpenFile(devicePath, syscall.O_RDWR, os.ModeDevice)
	if err != nil {
		log.Error(err, "[volumeCleanupDiscard] Opening device")
		return fmt.Errorf("opening device %s to wipe: %w", devicePath, err)
	}
	defer func() {
		log.Info("Closing file")
		err := device.Close()
		if err != nil {
			log.Error(err, "[volumeCleanupDiscard] While closing deice")
			*closingErrors = append(*closingErrors, fmt.Errorf("closing file %s: %w", device.Name(), err))
		}
	}()

	deviceSize, err := volumeSize(log, device)
	if err != nil {
		log.Error(err, "[volumeCleanupDiscard] finding device size")
		return fmt.Errorf("can't find the size of device %s: %w", devicePath, err)
	}

	rng := Range{
		start: 0,
		count: uint64(deviceSize),
	}

	log.Info("[volumeCleanupDiscard] calling BLKDISCARD", "fd", device.Fd(), "range", rng)
	start := time.Now()

	_, _, errno := syscall.Syscall(
		syscall.SYS_IOCTL,
		device.Fd(),
		uintptr(BLKDISCARD),
		uintptr(unsafe.Pointer(&rng)))

	log.Info("[volumeCleanupDiscard] BLKDISCARD is done", "duration", time.Since(start).String(), "errno", errno)

	if errno != 0 {
		err := fmt.Errorf("%s", errno.Error())
		log.Error(err, "[volumeCleanupDiscard] error calling BLKDISCARD")
		return fmt.Errorf("calling ioctl BLKDISCARD: %s", err)
	}

	return nil
}
