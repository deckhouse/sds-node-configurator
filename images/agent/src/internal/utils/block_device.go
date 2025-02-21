package utils

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"unsafe"

	"golang.org/x/sys/unix"
)

type blockDevice struct {
	File
	syscall SysCall
}

type Discarder interface {
	Discard(start, count uint64) error
}

type File interface {
	io.Closer
	io.Reader
	io.WriterAt
	io.ReaderAt
	io.Seeker
	Name() string
	Fd() uintptr
}

type BlockDevice interface {
	File
	Discarder

	Size() (int64, error)
}

type BlockDeviceOpener interface {
	Open(name string, flag int) (BlockDevice, error)
}
type blockDeviceOpener struct {
	fileOpener FileOpener
	syscall    SysCall
}

type FileOpener interface {
	Open(name string, flag int, mode fs.FileMode) (File, error)
}

type osFileOpener struct{}

func (osFileOpener) Open(name string, flag int, mode fs.FileMode) (File, error) {
	return os.OpenFile(name, flag, mode)
}

func (opener *blockDeviceOpener) Open(name string, flag int) (BlockDevice, error) {
	file, err := opener.fileOpener.Open(name, flag, os.ModeDevice)
	if err != nil {
		return nil, fmt.Errorf("opening os file: %w", err)
	}
	return &blockDevice{
		file,
		opener.syscall,
	}, nil
}

var defaultBlockDeviceOpener = blockDeviceOpener{
	fileOpener: &osFileOpener{},
	syscall:    OsSysCall(),
}

func OsDeviceOpener() BlockDeviceOpener {
	return &defaultBlockDeviceOpener
}

func NewBlockDeviceOpener(fileOpener FileOpener, syscall SysCall) BlockDeviceOpener {
	return &blockDeviceOpener{
		fileOpener: fileOpener,
		syscall:    syscall,
	}
}

func (device *blockDevice) Size() (int64, error) {
	var stat Stat_t
	err := device.syscall.Fstat(int(device.Fd()), &stat)
	if err != nil {
		return 0, fmt.Errorf("calling fstat: %w", err)
	}
	if stat.Mode&S_IFMT != S_IFBLK {
		return 0, fmt.Errorf("not a block device, mode: %x", stat.Mode)
	}

	var blockDeviceSize uint64
	_, _, errno := device.syscall.Syscall(
		unix.SYS_IOCTL,
		device.Fd(),
		BLKGETSIZE64,
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
