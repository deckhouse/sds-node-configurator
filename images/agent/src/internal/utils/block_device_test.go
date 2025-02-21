package utils

import (
	"os"
	"syscall"
	"testing"
	"unsafe"

	"go.uber.org/mock/gomock"
)

func TestBlockDeviceSize(t *testing.T) {
	ctrl := gomock.NewController(t)
	syscall_ := NewMockSysCall(ctrl)
	fileOpener := NewMockFileOpener(ctrl)
	blockDeviceOpener := NewBlockDeviceOpener(fileOpener, syscall_)
	file := NewMockFile(ctrl)

	fileName := "fileName"
	flag := int(0)
	size := int64(1024)
	fd := uintptr(1234)

	file.EXPECT().Fd().AnyTimes().Return(fd)
	syscall_.EXPECT().Fstat(int(fd), gomock.Any()).DoAndReturn(func(fd_ int, stat *Stat_t) error {
		stat.Mode = S_IFBLK
		return nil
	})
	syscall_.EXPECT().Syscall(uintptr(syscall.SYS_IOCTL), fd, BLKGETSIZE64, gomock.Any()).DoAndReturn(func(trap, a1, a2, a3 uintptr) (uintptr, uintptr, Errno) {
		*(*uint64)(unsafe.Pointer(a3)) = uint64(size)
		return 0, 0, 0
	})
	fileOpener.EXPECT().Open(fileName, flag, os.ModeDevice).Return(file, nil)

	device, err := blockDeviceOpener.Open(fileName, 0)
	if err != nil {
		t.Errorf("opening block device: %v", err)
	}
	if device == nil {
		t.Error("nil device returned")
	}

	got, err := device.Size()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if got != size {
		t.Errorf("expected size %d, got %d", size, got)
	}
}

func TestBlockDeviceDiscard(t *testing.T) {
	ctrl := gomock.NewController(t)
	syscall_ := NewMockSysCall(ctrl)
	fileOpener := NewMockFileOpener(ctrl)
	blockDeviceOpener := NewBlockDeviceOpener(fileOpener, syscall_)
	file := NewMockFile(ctrl)

	fileName := "fileName"
	flag := int(0)
	fd := uintptr(1234)
	start := uint64(512)
	count := uint64(512)

	file.EXPECT().Fd().AnyTimes().Return(fd)
	syscall_.EXPECT().Blkdiscard(fd, start, count).Return(nil)
	fileOpener.EXPECT().Open(fileName, flag, os.ModeDevice).Return(file, nil)

	device, err := blockDeviceOpener.Open(fileName, 0)
	if err != nil {
		t.Errorf("opening block device: %v", err)
	}
	if device == nil {
		t.Error("nil device returned")
	}

	err = device.Discard(start, count)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}
