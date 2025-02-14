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
	log.Info("Cleaning up volume", "vgname", vgName, "lvname", lvName, "method", volumeCleanupMethod)
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

	if stat.Mode&S_IFMT != S_IFBLK {
		return 0, fmt.Errorf("not a block device, ifmt: %x", stat.Mode&S_IFMT)
	}

	if stat.Blksize <= 0 {
		return 0, fmt.Errorf("block size %d is invalid, stat: %v", stat.Blksize, stat)
	}
	if stat.Blocks <= 0 {
		return 0, fmt.Errorf("block count %d is invalid, stat: %v", stat.Blocks, stat)
	}

	return stat.Blksize * stat.Blocks, nil
}

func volumeCleanupCopy(ctx context.Context, log logger.Logger, closingErrors *[]error, outputPath, inputPath string, passes int) error {

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
	outputStat, err := volumeStat(output)
	if err != nil {
		return fmt.Errorf("can't get stat of %s: %w", outputPath, err)
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

	S_IFMT   = 0x0170000 /* type of file mask */
	S_IFIFO  = 0x0010000 /* named pipe (fifo) */
	S_IFCHR  = 0x0020000 /* character special */
	S_IFDIR  = 0x0040000 /* directory */
	S_IFBLK  = 0x0060000 /* block special */
	S_IFREG  = 0x0100000 /* regular */
	S_IFLNK  = 0x0120000 /* symbolic link */
	S_IFSOCK = 0x0140000 /* socket */
	S_ISUID  = 0x0004000 /* set-user-ID on execution */
	S_ISGID  = 0x0002000 /* set-group-ID on execution */
	S_ISVTX  = 0x0001000 /* save swapped text even after use */
	S_IRWXU  = 0x0000700 /* RWX mask for owner */
	S_IRUSR  = 0x0000400 /* R for owner */
	S_IWUSR  = 0x0000200 /* W for owner */
	S_IXUSR  = 0x0000100 /* X for owner */
	S_IRWXG  = 0x0000070 /* RWX mask for group */
	S_IRGRP  = 0x0000040 /* R for group */
	S_IWGRP  = 0x0000020 /* W for group */
	S_IXGRP  = 0x0000010 /* X for group */
	S_IRWXO  = 0x0000007 /* RWX mask for other */
	S_IROTH  = 0x0000004 /* R for other */
	S_IWOTH  = 0x0000002 /* W for other */
	S_IXOTH  = 0x0000001 /* X for other */
)

type Range struct {
	start, count uint64
}

func volumeStat(device *os.File) (syscall.Stat_t, error) {
	var stat syscall.Stat_t
	if err := syscall.Fstat(int(device.Fd()), &stat); err != nil {
		return stat, fmt.Errorf("fstat call failed: %w", err)
	}
	return stat, nil
}

func volumeCleanupDiscard(ctx context.Context, log logger.Logger, closingErrors *[]error, devicePath string) error {

	device, err := os.OpenFile(devicePath, syscall.O_DIRECT, os.ModeDevice)
	if err != nil {
		return fmt.Errorf("opening device %s to wipe: %w", devicePath, err)
	}

	stat, err := volumeStat(device)
	if err != nil {
		return fmt.Errorf("can't get stat of %s: %w", devicePath, err)
	}

	deviceSize, err := volumeSize(stat)
	if err != nil {
		return fmt.Errorf("can't find the size of device %s: %w", devicePath, err)
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
