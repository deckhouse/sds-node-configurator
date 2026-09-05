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

package utils_test

import (
	"errors"
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"

	. "github.com/deckhouse/sds-node-configurator/images/agent/internal/mock_utils"
	. "github.com/deckhouse/sds-node-configurator/images/agent/internal/utils"
)

var _ = Describe("BlockDevice", func() {
	var ctrl *gomock.Controller
	var sysCall *MockSysCall
	var fileOpener *MockFileOpener
	var blockDeviceOpener BlockDeviceOpener
	var file *MockFile
	var err error
	BeforeEach(func() {
		ctrl = gomock.NewController(GinkgoT())
		sysCall = NewMockSysCall(ctrl)
		fileOpener = NewMockFileOpener(ctrl)
		blockDeviceOpener = NewBlockDeviceOpener(fileOpener, sysCall)
		file = NewMockFile(ctrl)
	})

	fileName := "fileName"
	flag := int(0)
	size := int64(1024)
	fd := uintptr(1234)

	var device BlockDevice

	When("device properly opened", func() {
		BeforeEach(func() {
			file.EXPECT().Fd().AnyTimes().Return(fd)
			fileOpener.EXPECT().Open(fileName, flag, os.ModeDevice).Return(file, nil)
		})
		JustBeforeEach(func() {
			device, err = blockDeviceOpener.Open(fileName, 0)
			Expect(err).NotTo(HaveOccurred())
			Expect(device).NotTo(BeNil())
		})

		It("finds out size", func() {
			sysCall.EXPECT().Fstat(int(fd), gomock.Any()).DoAndReturn(func(_ int, stat *Stat_t) error {
				stat.Mode = S_IFBLK
				return nil
			})
			sysCall.EXPECT().Blkgetsize64(fd).Return(uint64(size), nil)

			got, err := device.Size()
			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(BeEquivalentTo(size))
		})

		It("reports the ioctl error behind an unknown size", func() {
			sysCall.EXPECT().Fstat(int(fd), gomock.Any()).DoAndReturn(func(_ int, stat *Stat_t) error {
				stat.Mode = S_IFBLK
				return nil
			})
			ioctlError := errors.New("calling ioctl BLKGETSIZE64: inappropriate ioctl for device")
			sysCall.EXPECT().Blkgetsize64(fd).Return(uint64(0), ioctlError)

			_, err := device.Size()
			Expect(err).To(MatchError(ioctlError))
		})

		It("rejects a zero size", func() {
			sysCall.EXPECT().Fstat(int(fd), gomock.Any()).DoAndReturn(func(_ int, stat *Stat_t) error {
				stat.Mode = S_IFBLK
				return nil
			})
			sysCall.EXPECT().Blkgetsize64(fd).Return(uint64(0), nil)

			_, err := device.Size()
			Expect(err).To(MatchError("block device size is invalid"))
		})

		It("finds out the block size", func() {
			sysCall.EXPECT().Blksszget(fd).Return(512, nil)

			got, err := device.BlockSize()
			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(Equal(512))
		})

		It("rejects a zero block size", func() {
			sysCall.EXPECT().Blksszget(fd).Return(0, nil)

			_, err := device.BlockSize()
			Expect(err).To(MatchError("block size is invalid"))
		})

		It("issues blkdiscard", func() {
			start := uint64(256)
			count := uint64(512)
			sysCall.EXPECT().Blkdiscard(fd, start, count).Return(nil)

			err = device.Discard(start, count)
			Expect(err).NotTo(HaveOccurred())
		})

		It("closes underlying file", func() {
			closingError := errors.New("Closing error")
			file.EXPECT().Close().Return(closingError)

			err = device.Close()

			Expect(err).To(MatchError(closingError))
		})
	})

	When("underlying open error occurred", func() {
		openError := errors.New("Open file error")
		BeforeEach(func() {
			fileOpener.EXPECT().Open(fileName, flag, os.ModeDevice).Return(nil, openError)
		})
		JustBeforeEach(func() {
			device, err = blockDeviceOpener.Open(fileName, 0)
			Expect(err).To(MatchError(openError))
		})

		It("returns nil device", func() {
			Expect(device).To(BeNil())
		})
	})
})
