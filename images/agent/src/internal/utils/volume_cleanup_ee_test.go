//go:build !ce

/*
Copyright 2025 Flant JSC
Licensed under the Deckhouse Platform Enterprise Edition (EE) license. See https://github.com/deckhouse/deckhouse/blob/main/ee/LICENSE
*/

package utils_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/deckhouse/sds-node-configurator/lib/go/common/pkg/feature"
	"go.uber.org/mock/gomock"
	"golang.org/x/sys/unix"

	"agent/internal/logger"
	. "agent/internal/mock_utils"
	. "agent/internal/utils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Cleaning up volume", func() {
	var log logger.Logger
	var ctrl *gomock.Controller
	var opener *MockBlockDeviceOpener
	var device *MockBlockDevice
	var err error
	vgName := "vg"
	lvName := "lv"
	var method string
	BeforeEach(func() {
		ctrl = gomock.NewController(GinkgoT())
		opener = NewMockBlockDeviceOpener(ctrl)
		device = NewMockBlockDevice(ctrl)

		log, err = logger.NewLogger(logger.WarningLevel)
		Expect(err).NotTo(HaveOccurred())
	})

	call := func() {
		err = VolumeCleanup(context.Background(), log, opener, vgName, lvName, method)
		if !feature.VolumeCleanupEnabled() {
			Expect(err).To(MatchError("volume cleanup is not supported in your edition"))
		}
	}
	ItSucceed := func() bool {
		return It("succeed", func() {
			call()
			if feature.VolumeCleanupEnabled() {
				Expect(err).ToNot(HaveOccurred())
			}
		})
	}
	When("method is unknown", func() {
		BeforeEach(func() {
			method = "some"
		})
		It("fails", func() {
			call()
			if feature.VolumeCleanupEnabled() {
				Expect(err).To(MatchError(fmt.Sprintf("unknown cleanup method %s", method)))
			}
		})
	})
	When("method is Discard", func() {
		BeforeEach(func() {
			method = "Discard"
			deviceSize := 1024
			if feature.VolumeCleanupEnabled() {
				opener.EXPECT().Open(filepath.Join("/dev", vgName, lvName), unix.O_RDWR).Return(func() BlockDevice {
					device.EXPECT().Size().Return(int64(deviceSize), nil)
					device.EXPECT().Discard(uint64(0), uint64(deviceSize))
					device.EXPECT().Close().Return(nil)
					return device
				}(), nil)
			}
		})
		ItSucceed()
	})
	inputName := "/dev/urandom"
	deviceSize := 1024 * 1024 * 50
	When("method is RandomFill", func() {
		var passCount int
		var copyCount int
		var expectedWrite int
		bufferSize := 1024 * 1024 * 4

		JustBeforeEach(func() {
			expectedWrite = 0
			if feature.VolumeCleanupEnabled() {
				GinkgoWriter.Printf("Setting up expectations for passCount: %d\n", passCount)
				copyCount = passCount * (deviceSize / bufferSize)
				if 0 != deviceSize%bufferSize {
					copyCount += passCount
				}
				GinkgoWriter.Printf("Expected copyCount %d\n", copyCount)
				opener.EXPECT().Open(inputName, unix.O_RDONLY).Return(func() BlockDevice {
					GinkgoWriter.Printf("Opening input device\n")
					input := NewMockBlockDevice(ctrl)
					input.EXPECT().Read(gomock.Any()).DoAndReturn(func(p []byte) (int, error) {
						GinkgoWriter.Printf("Reading %d from input\n", len(p))
						Expect(len(p) <= bufferSize).To(BeTrue())
						return len(p), nil
					}).Times(copyCount)
					input.EXPECT().Close().Return(nil)
					input.EXPECT().Name().AnyTimes().Return(inputName)
					return input
				}(), nil)

				deviceName := filepath.Join("/dev", vgName, lvName)
				opener.EXPECT().Open(deviceName, unix.O_DIRECT|unix.O_RDWR).Return(func() BlockDevice {
					GinkgoWriter.Printf("Opening device\n")
					device := NewMockBlockDevice(ctrl)
					device.EXPECT().Size().Return(int64(deviceSize), nil)
					device.EXPECT().WriteAt(gomock.Any(), gomock.Any()).DoAndReturn(func(p []byte, off int64) (int, error) {
						GinkgoWriter.Printf("Writing to device [%d, %d]\n", off, off+int64(len(p)))
						Expect(off).To(BeEquivalentTo(int64(expectedWrite)))
						expectedWrite += bufferSize
						if expectedWrite >= deviceSize {
							expectedWrite = 0
							Expect(len(p) <= bufferSize).To(BeTrue())
						} else {
							Expect(len(p)).To(BeEquivalentTo(bufferSize))
						}
						return len(p), nil
					}).Times(copyCount)
					device.EXPECT().Close().Return(nil)
					device.EXPECT().Name().AnyTimes().Return(deviceName)
					return device
				}(), nil)
			}
		})
		When("SinglePass", func() {
			BeforeEach(func() {
				method = "RandomFillSinglePass"
				passCount = 1
			})
			ItSucceed()
		})
		When("ThreePass", func() {
			BeforeEach(func() {
				method = "RandomFillThreePass"
				passCount = 3
			})
			ItSucceed()
		})
	})
	When("closing errors happen", func() {
		closingError := errors.New("expected closing error")
		writeError := errors.New("expected writing error")
		JustBeforeEach(func() {
			if feature.VolumeCleanupEnabled() {
				opener.EXPECT().Open(inputName, unix.O_RDONLY).Return(func() BlockDevice {
					input := NewMockBlockDevice(ctrl)
					input.EXPECT().Read(gomock.Any()).DoAndReturn(func(p []byte) (int, error) {
						return len(p), nil
					})
					input.EXPECT().Close().Return(closingError)
					input.EXPECT().Name().AnyTimes().Return(inputName)
					return input
				}(), nil)

				deviceName := filepath.Join("/dev", vgName, lvName)
				opener.EXPECT().Open(deviceName, unix.O_DIRECT|unix.O_RDWR).Return(func() BlockDevice {
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
		})

		It("fails with joined error", func() {
			call()
			Expect(err).To(MatchError(closingError))
			Expect(err).To(MatchError(writeError))
		})
	})
})

func TestVolumeCleanup(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "VolumeCleanup Suite")
}
