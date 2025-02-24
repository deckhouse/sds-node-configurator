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
	"io"
	"path/filepath"

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

	doCall := func() {
		err = VolumeCleanup(context.Background(), log, opener, vgName, lvName, method)
		if !feature.VolumeCleanupEnabled() {
			Expect(err).To(MatchError("volume cleanup is not supported in your edition"))
		}
	}
	ItSucceedOnlyWhenFeatureEnabled := func(text string) bool {
		return It(text, func() {
			doCall()
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
			doCall()
			if feature.VolumeCleanupEnabled() {
				Expect(err).To(MatchError(fmt.Sprintf("unknown cleanup method %s", method)))
			}
		})
	})
	When("method is Discard", func() {
		BeforeEach(func() {
			method = "Discard"
		})

		When("can't open device", func() {
			deviceOpenError := errors.New("can't open device")
			BeforeEach(func() {
				if feature.VolumeCleanupEnabled() {
					opener.EXPECT().Open(filepath.Join("/dev", vgName, lvName), unix.O_RDWR).Return(nil, deviceOpenError)
				}
			})
			It("fails with same error", func() {
				doCall()
				Expect(err).To(MatchError(deviceOpenError))
			})
		})

		When("device opened", func() {
			BeforeEach(func() {
				if feature.VolumeCleanupEnabled() {
					device = NewMockBlockDevice(ctrl)
					name := filepath.Join("/dev", vgName, lvName)
					opener.EXPECT().Open(name, unix.O_RDWR).Return(device, nil)
					device.EXPECT().Name().AnyTimes().Return(name)
				}
			})
			deviceSize := 1024
			When("discard succeed", func() {
				BeforeEach(func() {
					if feature.VolumeCleanupEnabled() {
						device.EXPECT().Size().Return(int64(deviceSize), nil)
						device.EXPECT().Discard(uint64(0), uint64(deviceSize)).Return(nil)
					}
				})
				When("no closing error", func() {
					BeforeEach(func() {
						if feature.VolumeCleanupEnabled() {
							device.EXPECT().Close().Return(nil)
						}
					})
					ItSucceedOnlyWhenFeatureEnabled("calls device discard")
				})
				When("cannot close", func() {
					closingError := errors.New("closing error")
					BeforeEach(func() {
						if feature.VolumeCleanupEnabled() {
							device.EXPECT().Close().Return(closingError)
						}
					})
					It("fails with closing error", func() {
						doCall()
						if feature.VolumeCleanupEnabled() {
							Expect(err).To(MatchError(closingError))
						}
					})
				})
			})
			When("discard fails", func() {
				discardError := errors.New("discard error")
				BeforeEach(func() {
					if feature.VolumeCleanupEnabled() {
						device.EXPECT().Size().Return(int64(deviceSize), nil)
						device.EXPECT().Discard(uint64(0), uint64(deviceSize)).Return(discardError)
					}
				})
				When("no closing error", func() {
					BeforeEach(func() {
						if feature.VolumeCleanupEnabled() {
							device.EXPECT().Close().Return(nil)
						}
					})
					It("fails with matched error", func() {
						doCall()
						if feature.VolumeCleanupEnabled() {
							Expect(err).To(MatchError(discardError))
						}
					})
				})
				When("closing error", func() {
					closingError := errors.New("closing error")
					BeforeEach(func() {
						if feature.VolumeCleanupEnabled() {
							device.EXPECT().Close().Return(closingError)
						}
					})
					It("fails with matched errors", func() {
						doCall()
						if feature.VolumeCleanupEnabled() {
							Expect(err).To(MatchError(discardError))
							Expect(err).To(MatchError(closingError))
						}
					})
				})
			})
		})
	})
	inputName := "/dev/urandom"
	deviceSize := 1024 * 1024 * 50
	When("method is RandomFill", func() {
		var passCount int
		// var expectedReadCount int
		// var expectedWriteCount int
		var expectedWritePosition int
		bufferSize := 1024 * 1024 * 4
		When("input open succeed", func() {
			var input *MockBlockDevice
			BeforeEach(func() {
				input = NewMockBlockDevice(ctrl)
				if feature.VolumeCleanupEnabled() {
					input.EXPECT().Close().Return(nil)
					input.EXPECT().Name().AnyTimes().Return(inputName)
					opener.EXPECT().Open(inputName, unix.O_RDONLY).Return(input, nil)
				}
			})
			deviceName := filepath.Join("/dev", vgName, lvName)
			When("device open succeed", func() {
				BeforeEach(func() {
					device = NewMockBlockDevice(ctrl)
					if feature.VolumeCleanupEnabled() {
						opener.EXPECT().Open(deviceName, unix.O_DIRECT|unix.O_RDWR).Return(device, nil)
						device.EXPECT().Size().Return(int64(deviceSize), nil)
						device.EXPECT().Name().AnyTimes().Return(deviceName)
					}
				})
				When("read succeed", func() {
					var readMissingBytes int
					var totalBytesRead int
					var lastBytesRead int
					JustBeforeEach(func() {
						totalBytesRead = 0
						buffersToReadPerPass := deviceSize / bufferSize
						if 0 != deviceSize%bufferSize {
							buffersToReadPerPass++
						}
						expectedTotalBytesRead := deviceSize * passCount
						readLimit := expectedTotalBytesRead - readMissingBytes

						expectedReadCallCount := buffersToReadPerPass * passCount

						if readLimit < expectedTotalBytesRead {
							// Extra call for EOF
							expectedReadCallCount += 1
						}

						if feature.VolumeCleanupEnabled() {
							input.EXPECT().Read(gomock.Any()).DoAndReturn(func(p []byte) (int, error) {
								GinkgoWriter.Printf("Reading %d from input\n", len(p))
								Expect(len(p) <= bufferSize).To(BeTrue())
								lastBytesRead = min(len(p), readLimit-totalBytesRead)
								totalBytesRead += lastBytesRead
								if lastBytesRead == 0 {
									return 0, io.EOF
								}
								return lastBytesRead, nil
							}).Times(expectedReadCallCount)
						}
					})
					When("write succeed", func() {
						var totalBytesWritten int
						var lastPassBytesWritten int
						JustBeforeEach(func() {
							// expectedWriteCount = expectedReadCount
							expectedWritePosition = 0
							totalBytesWritten = 0
							lastPassBytesWritten = 0
							if feature.VolumeCleanupEnabled() {
								device.EXPECT().WriteAt(gomock.Any(), gomock.Any()).DoAndReturn(func(p []byte, off int64) (int, error) {
									GinkgoWriter.Printf("Writing to device [%d, %d]\n", off, off+int64(len(p)))
									Expect(off).To(BeEquivalentTo(int64(expectedWritePosition)))
									expectedWritePosition += len(p)
									lastPassBytesWritten += len(p)
									totalBytesWritten += len(p)
									if expectedWritePosition >= deviceSize {
										expectedWritePosition = 0

										Expect(lastPassBytesWritten).To(Equal(deviceSize))
										lastPassBytesWritten = 0

										Expect(len(p) <= bufferSize).To(BeTrue())
									} else {
										Expect(len(p)).To(BeEquivalentTo(lastBytesRead))
									}
									return len(p), nil
								}).AnyTimes() // Times(expectedWriteCount)
							}
						})
						When("input has enough bytes to read", func() {
							BeforeEach(func() {
								readMissingBytes = 0
							})
							When("no closing error", func() {
								BeforeEach(func() {
									if feature.VolumeCleanupEnabled() {
										device.EXPECT().Close().Return(nil)
									}
								})
								When("SinglePass", func() {
									BeforeEach(func() {
										method = "RandomFillSinglePass"
										passCount = 1
									})
									ItSucceedOnlyWhenFeatureEnabled("fills the device")
								})
								When("ThreePass", func() {
									BeforeEach(func() {
										method = "RandomFillThreePass"
										passCount = 3
									})
									ItSucceedOnlyWhenFeatureEnabled("fills the device")
								})
							})
						})
						When("input doesn't have enough bytes to read", func() {
							BeforeEach(func() {
								readMissingBytes = 512
							})
							When("no closing error", func() {
								BeforeEach(func() {
									if feature.VolumeCleanupEnabled() {
										device.EXPECT().Close().Return(nil)
									}
								})
								When("SinglePass", func() {
									BeforeEach(func() {
										method = "RandomFillSinglePass"
										passCount = 1
									})
									It("fails", func() {
										doCall()
										if feature.VolumeCleanupEnabled() {
											Expect(err).To(HaveOccurred())
										}
									})
								})
								When("ThreePass", func() {
									BeforeEach(func() {
										method = "RandomFillThreePass"
										passCount = 3
									})
									It("fails", func() {
										doCall()
										if feature.VolumeCleanupEnabled() {
											Expect(err).To(HaveOccurred())
										}
									})
								})
							})
						})
					})
				})
			})
			When("device open failed", func() {
				deviceOpenError := errors.New("input open error")
				BeforeEach(func() {
					if feature.VolumeCleanupEnabled() {
						opener.EXPECT().Open(deviceName, unix.O_DIRECT|unix.O_RDWR).Return(nil, deviceOpenError)
					}
				})
				It("fails with matched error", func() {
					doCall()
					if feature.VolumeCleanupEnabled() {
						Expect(err).To(MatchError(deviceOpenError))
					}
				})
			})
		})
		When("input open failed", func() {
			inputOpenError := errors.New("input open error")
			BeforeEach(func() {
				if feature.VolumeCleanupEnabled() {
					opener.EXPECT().Open(inputName, unix.O_RDONLY).Return(nil, inputOpenError)
				}
			})

			It("fails with matched error", func() {
				doCall()
				if feature.VolumeCleanupEnabled() {
					Expect(err).To(MatchError(inputOpenError))
				}
			})
		})
	})
	When("closing errors happens", func() {
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
			doCall()
			Expect(err).To(MatchError(closingError))
			Expect(err).To(MatchError(writeError))
		})
	})
})
