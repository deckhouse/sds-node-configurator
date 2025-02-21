//go:build !ce

/*
Copyright 2025 Flant JSC
Licensed under the Deckhouse Platform Enterprise Edition (EE) license. See https://github.com/deckhouse/deckhouse/blob/main/ee/LICENSE
*/

package utils

import (
	"context"
	"errors"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/deckhouse/sds-node-configurator/lib/go/common/pkg/feature"
	"go.uber.org/mock/gomock"

	"agent/internal/logger"
)

func TestVolumeCleanup_UnknownMethod(t *testing.T) {
	ctrl := gomock.NewController(t)
	opener := NewMockBlockDeviceOpener(ctrl)

	log, err := logger.NewLogger(logger.WarningLevel)
	if err != nil {
		t.Fatalf("creating log: %v", err)
	}
	vgName := "vg"
	lvName := "lv"
	err = VolumeCleanup(context.Background(), log, opener, vgName, lvName, "some")

	if !feature.VolumeCleanupEnabled() {
		expected := "volume cleanup is not supported in your edition"
		got := err.Error()
		if got != expected {
			t.Fatalf("error message expected '%s' got '%s'", expected, got)
		}
		return
	}

	if err == nil {
		t.Fatal("error expected")
	} else if err.Error() != "unknown cleanup method some" {
		t.Fatalf("unexpected error %v", err)
	}
}

func TestVolumeCleanup_Discard(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := NewMockBlockDevice(ctrl)
	opener := NewMockBlockDeviceOpener(ctrl)
	opener.EXPECT().Open("/dev/vg/lv", syscall.O_RDWR).Return(mock, nil)

	deviceSize := 1024
	mock.EXPECT().Size().Return(int64(deviceSize), nil)
	mock.EXPECT().Discard(uint64(0), uint64(deviceSize))
	mock.EXPECT().Close().Return(nil)

	log, err := logger.NewLogger(logger.WarningLevel)
	if err != nil {
		t.Fatalf("creating log: %v", err)
	}
	vgName := "vg"
	lvName := "lv"

	err = VolumeCleanup(context.Background(), log, opener, vgName, lvName, "Discard")

	if !feature.VolumeCleanupEnabled() {
		expected := "volume cleanup is not supported in your edition"
		got := err.Error()
		if got != expected {
			t.Fatalf("error message expected '%s' got '%s'", expected, got)
		}
		return
	}

	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
}

func TestVolumeCleanup_RandomFillSinglePass(t *testing.T) {
	log, err := logger.NewLogger(logger.WarningLevel)
	if err != nil {
		t.Fatalf("creating log: %v", err)
	}
	vgName := "vg"
	lvName := "lv"
	deviceSize := 1024 * 1024 * 50
	bufferSize := 1024 * 1024 * 4
	copyCount := 1 + deviceSize/bufferSize
	expectedWrite := 0

	ctrl := gomock.NewController(t)
	opener := NewMockBlockDeviceOpener(ctrl)

	if feature.VolumeCleanupEnabled() {
		inputName := "/dev/urandom"
		opener.EXPECT().Open(inputName, syscall.O_RDONLY).Return(func() BlockDevice {
			input := NewMockBlockDevice(ctrl)
			input.EXPECT().Read(gomock.Any()).DoAndReturn(func(p []byte) (int, error) {
				if len(p) > bufferSize {
					t.Fatalf("Buffer size should be less then %d, got %d", bufferSize, len(p))
				}
				return len(p), nil
			}).Times(copyCount)
			input.EXPECT().Close().Return(nil)
			input.EXPECT().Name().AnyTimes().Return(inputName)
			return input
		}(), nil)

		deviceName := filepath.Join("/dev", vgName, lvName)
		opener.EXPECT().Open(deviceName, syscall.O_DIRECT|syscall.O_RDWR).Return(func() BlockDevice {
			device := NewMockBlockDevice(ctrl)
			device.EXPECT().Size().Return(int64(deviceSize), nil)
			device.EXPECT().WriteAt(gomock.Any(), gomock.Any()).DoAndReturn(func(p []byte, off int64) (int, error) {
				if int64(expectedWrite) != off {
					t.Fatalf("Expected write offset %d, got %d", expectedWrite, off)
				}
				expectedWrite += bufferSize
				if expectedWrite > deviceSize {
					expectedWrite = 0
					if len(p) > bufferSize {
						t.Fatalf("Buffer size should be less then %d, got %d", bufferSize, len(p))
					}
				} else if len(p) != bufferSize {
					t.Fatalf("Expected buffer size %d, got %d", bufferSize, len(p))
				}
				return len(p), nil
			}).Times(copyCount)
			device.EXPECT().Close().Return(nil)
			device.EXPECT().Name().AnyTimes().Return(deviceName)
			return device
		}(), nil)
	}
	err = VolumeCleanup(context.Background(), log, opener, vgName, lvName, "RandomFillSinglePass")

	if !feature.VolumeCleanupEnabled() {
		expected := "volume cleanup is not supported in your edition"
		got := err.Error()
		if got != expected {
			t.Fatalf("error message expected '%s' got '%s'", expected, got)
		}
		return
	}

	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
}

func TestVolumeCleanup_RandomFillThreePass(t *testing.T) {
	log, err := logger.NewLogger(logger.WarningLevel)
	if err != nil {
		t.Fatalf("creating log: %v", err)
	}
	vgName := "vg"
	lvName := "lv"
	deviceSize := 1024 * 1024 * 50
	bufferSize := 1024 * 1024 * 4
	copyCount := 3 * (1 + deviceSize/bufferSize)
	expectedWrite := 0

	ctrl := gomock.NewController(t)
	opener := NewMockBlockDeviceOpener(ctrl)

	if feature.VolumeCleanupEnabled() {
		inputName := "/dev/urandom"
		opener.EXPECT().Open(inputName, syscall.O_RDONLY).Return(func() BlockDevice {
			input := NewMockBlockDevice(ctrl)
			input.EXPECT().Read(gomock.Any()).DoAndReturn(func(p []byte) (int, error) {
				if len(p) > bufferSize {
					t.Fatalf("Buffer size should be less then %d, got %d", bufferSize, len(p))
				}
				return len(p), nil
			}).Times(copyCount)
			input.EXPECT().Close().Return(nil)
			input.EXPECT().Name().AnyTimes().Return(inputName)
			return input
		}(), nil)

		deviceName := filepath.Join("/dev", vgName, lvName)
		opener.EXPECT().Open(deviceName, syscall.O_DIRECT|syscall.O_RDWR).Return(func() BlockDevice {
			device := NewMockBlockDevice(ctrl)
			device.EXPECT().Size().Return(int64(deviceSize), nil)
			device.EXPECT().WriteAt(gomock.Any(), gomock.Any()).DoAndReturn(func(p []byte, off int64) (int, error) {
				if int64(expectedWrite) != off {
					t.Fatalf("Expected write offset %d, got %d", expectedWrite, off)
				}
				expectedWrite += bufferSize
				if expectedWrite > deviceSize {
					expectedWrite = 0
					if len(p) > bufferSize {
						t.Fatalf("Buffer size should be less then %d, got %d", bufferSize, len(p))
					}
				} else if len(p) != bufferSize {
					t.Fatalf("Expected buffer size %d, got %d", bufferSize, len(p))
				}
				return len(p), nil
			}).Times(copyCount)
			device.EXPECT().Close().Return(nil)
			device.EXPECT().Name().AnyTimes().Return(deviceName)
			return device
		}(), nil)
	}

	err = VolumeCleanup(context.Background(), log, opener, vgName, lvName, "RandomFillThreePass")

	if !feature.VolumeCleanupEnabled() {
		expected := "volume cleanup is not supported in your edition"
		got := err.Error()
		if got != expected {
			t.Fatalf("error message expected '%s' got '%s'", expected, got)
		}
		return
	}

	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
}

func TestVolumeCleanup_RandomFill_ClosingErrors(t *testing.T) {
	log, err := logger.NewLogger(logger.WarningLevel)
	if err != nil {
		t.Fatalf("creating log: %v", err)
	}
	vgName := "vg"
	lvName := "lv"
	deviceSize := 1024

	ctrl := gomock.NewController(t)
	opener := NewMockBlockDeviceOpener(ctrl)

	inputName := "/dev/urandom"
	closingError := errors.New("expected closing error")
	writeError := errors.New("expected writing error")
	if feature.VolumeCleanupEnabled() {
		opener.EXPECT().Open(inputName, syscall.O_RDONLY).Return(func() BlockDevice {
			input := NewMockBlockDevice(ctrl)
			input.EXPECT().Read(gomock.Any()).DoAndReturn(func(p []byte) (int, error) {
				return len(p), nil
			})
			input.EXPECT().Close().Return(closingError)
			input.EXPECT().Name().AnyTimes().Return(inputName)
			return input
		}(), nil)

		deviceName := filepath.Join("/dev", vgName, lvName)
		opener.EXPECT().Open(deviceName, syscall.O_DIRECT|syscall.O_RDWR).Return(func() BlockDevice {
			device := NewMockBlockDevice(ctrl)
			device.EXPECT().Size().Return(int64(deviceSize), nil)
			device.EXPECT().WriteAt(gomock.Any(), gomock.Any()).DoAndReturn(func(_ []byte, _ int64) (int, error) {
				return 0, writeError
			})
			device.EXPECT().Close().Return(closingError)
			device.EXPECT().Name().AnyTimes().Return(deviceName)
			return device
		}(), nil)
	}

	err = VolumeCleanup(context.Background(), log, opener, vgName, lvName, "RandomFillSinglePass")

	if !feature.VolumeCleanupEnabled() {
		expected := "volume cleanup is not supported in your edition"
		got := err.Error()
		if got != expected {
			t.Fatalf("error message expected '%s' got '%s'", expected, got)
		}
		return
	}

	if err == nil {
		t.Fatal("unexpected success")
	}

	if !errors.Is(err, closingError) || !errors.Is(err, writeError) {
		t.Fatalf("expected error to have both (%v, %v), got %v", writeError, closingError, err)
	}
}
