package utils

import (
	"errors"
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

type blockDevice struct {
	*os.File
	syscall SysCall
}

type BlockDevice interface {
	Close() error
	WriteAt(p []byte, off int64) (n int, err error)
	Read(p []byte) (n int, err error)
	Fd() uintptr
	Name() string
	Size() (int64, error)
	Discard(start, count uint64) error
}

func OpenBlockDevice(name string, flag int) (BlockDevice, error) {
	return openBlockDeviceWithSyscall(name, flag, DefaultSysCall())
}

func openBlockDeviceWithSyscall(name string, flag int, syscall SysCall) (BlockDevice, error) {
	file, err := os.OpenFile(name, flag, os.ModeDevice)
	if err != nil {
		return nil, fmt.Errorf("opening os file: %w", err)
	}
	return &blockDevice{
		file,
		syscall,
	}, nil
}

func (device *blockDevice) Size() (int64, error) {
	var stat Stat_t
	if err := device.syscall.Fstat(int(device.Fd()), &stat); err != nil {
		return 0, fmt.Errorf("fstat call failed: %w", err)
	}

	if stat.Size > 0 {
		return stat.Size, nil
	}

	if stat.Mode&S_IFMT != S_IFBLK {
		return 0, fmt.Errorf("not a block device, mode: %x", stat.Mode)
	}

	var blockDeviceSize uint64
	_, _, errno := device.syscall.Syscall(
		syscall.SYS_IOCTL,
		device.Fd(),
		uintptr(BLKGETSIZE64),
		uintptr(unsafe.Pointer(&blockDeviceSize)))
	if errno != 0 {
		err := errors.New(errno.Error())
		return 0, fmt.Errorf("error calling ioctl BLKGETSIZE64: %w", err)
	}
	if blockDeviceSize <= 0 {
		return 0, fmt.Errorf("block size is invalid")
	}

	return int64(blockDeviceSize), nil
}

func (device *blockDevice) Discard(start, count uint64) error {
	return device.syscall.Blkdiscard(device.Fd(), start, count)
}
