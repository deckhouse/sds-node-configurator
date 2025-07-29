/*
Copyright 2025 Flant JSC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//go:generate go tool mockgen -copyright_file ../../../../hack/boilerplate.txt -write_source_comment -destination=../mock_utils/$GOFILE -source=$GOFILE

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

type blockDevice[TSysCall SysCall] struct {
	File    // TODO: can I make it generic TFile to keep call static?
	syscall TSysCall
}

var _ BlockDevice = &blockDevice[osSyscall]{}

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
	BlockSize() (int, error)
}

type BlockDeviceOpener interface {
	Open(name string, flag int) (BlockDevice, error)
}
type blockDeviceOpener[TFileOpener FileOpener, TSysCall SysCall] struct {
	fileOpener TFileOpener
	syscall    TSysCall
}

type FileOpener interface {
	Open(name string, flag int, mode fs.FileMode) (File, error)
}

type osFileOpener struct{}

func (osFileOpener) Open(name string, flag int, mode fs.FileMode) (File, error) {
	return os.OpenFile(name, flag, mode)
}

func (opener *blockDeviceOpener[TFileOpener, TSysCall]) Open(name string, flag int) (BlockDevice, error) {
	file, err := opener.fileOpener.Open(name, flag, os.ModeDevice)
	if err != nil {
		return nil, fmt.Errorf("opening os file: %w", err)
	}
	return &blockDevice[TSysCall]{
		file,
		opener.syscall,
	}, nil
}

var defaultBlockDeviceOpener = blockDeviceOpener[osFileOpener, osSyscall]{
	fileOpener: osFileOpener{},
	syscall:    OsSysCall(),
}

//nolint:revive
func OsDeviceOpener() *blockDeviceOpener[osFileOpener, osSyscall] {
	return &defaultBlockDeviceOpener
}

//nolint:revive
func NewBlockDeviceOpener[TFileOpener FileOpener, TSysCall SysCall](fileOpener TFileOpener, syscall TSysCall) *blockDeviceOpener[TFileOpener, TSysCall] {
	return &blockDeviceOpener[TFileOpener, TSysCall]{
		fileOpener: fileOpener,
		syscall:    syscall,
	}
}

func (device *blockDevice[TSysCall]) Size() (int64, error) {
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
		unix.BLKGETSIZE64,
		uintptr(unsafe.Pointer(&blockDeviceSize)))
	if errno != 0 {
		err := errors.New(errno.Error())
		return 0, fmt.Errorf("error calling ioctl BLKGETSIZE64: %w", err)
	}
	if blockDeviceSize == 0 {
		return 0, fmt.Errorf("block device size is invalid")
	}

	return int64(blockDeviceSize), nil
}

func (device *blockDevice[TSysCall]) Discard(start, count uint64) error {
	return device.syscall.Blkdiscard(device.Fd(), start, count)
}

func (device *blockDevice[TSysCall]) BlockSize() (blockSize int, err error) {
	_, _, errno := device.syscall.Syscall(
		unix.SYS_IOCTL,
		device.Fd(),
		unix.BLKSSZGET,
		uintptr(unsafe.Pointer(&blockSize)))

	if errno != 0 {
		err = errors.New(errno.Error())
		err = fmt.Errorf("error calling ioctl BLKGETSIZE64: %w", err)
		return
	}
	if blockSize == 0 {
		err = fmt.Errorf("block size is invalid")
		return
	}

	return
}
