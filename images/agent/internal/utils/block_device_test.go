package utils_test

import (
	"errors"
	"os"
	"unsafe"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"
	"golang.org/x/sys/unix"

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
			sysCall.EXPECT().Syscall(uintptr(unix.SYS_IOCTL), fd, BLKGETSIZE64, gomock.Any()).DoAndReturn(func(_, _, _, a3 uintptr) (uintptr, uintptr, Errno) {
				*(*uint64)(unsafe.Pointer(a3)) = uint64(size)
				return 0, 0, 0
			})

			got, err := device.Size()
			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(BeEquivalentTo(size))
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
