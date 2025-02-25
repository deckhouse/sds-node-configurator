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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"
	"golang.org/x/sys/unix"

	"agent/internal/cache"
	"agent/internal/logger"
	. "agent/internal/mock_utils"
	. "agent/internal/utils"
)

var _ = Describe("Cleaning up volume", func() {
	var log logger.Logger
	var ctrl *gomock.Controller
	var opener *MockBlockDeviceOpener
	var device *MockBlockDevice
	var err error
	var lv *cache.LVData
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
		err = VolumeCleanup(context.Background(), log, lv, opener, vgName, lvName, method)
		if !feature.VolumeCleanupEnabled() {
			Expect(err).To(MatchError("volume cleanup is not supported in your edition"))
		}
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
				if feature.VolumeCleanupEnabled() {
					Expect(err).To(MatchError(deviceOpenError))
				}
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
					It("calls device discard", func() {
						doCall()
						if feature.VolumeCleanupEnabled() {
							Expect(err).ToNot(HaveOccurred())
						}
					})
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
		var expectedWritePosition int
		bufferSize := 1024 * 1024 * 4
		When("input open succeed", func() {
			var input *MockBlockDevice
			var inputClosingError error
			BeforeEach(func() {
				input = NewMockBlockDevice(ctrl)
				if feature.VolumeCleanupEnabled() {
					input.EXPECT().Name().AnyTimes().Return(inputName)
					opener.EXPECT().Open(inputName, unix.O_RDONLY).DoAndReturn(func(_ string, _ int) (BlockDevice, error) {
						input.EXPECT().Close().Return(inputClosingError)
						return input, nil
					})
				}
			})
			WhenInputClosingErrorVariants := func(f func(), no_error func()) {
				When("no input closing error", func() {
					BeforeEach(func() {
						inputClosingError = nil
					})
					f()
					no_error()
				})
				When("input closing error", func() {
					BeforeEach(func() {
						inputClosingError = errors.New("can't close device")
					})
					f()
					JustAfterEach(func() {
						if feature.VolumeCleanupEnabled() {
							Expect(err).To(MatchError(inputClosingError))
						}
					})
				})
			}
			deviceName := filepath.Join("/dev", vgName, lvName)
			When("device open succeed", func() {
				var deviceClosingError error
				BeforeEach(func() {
					device = NewMockBlockDevice(ctrl)
					if feature.VolumeCleanupEnabled() {
						opener.EXPECT().Open(deviceName, unix.O_DIRECT|unix.O_RDWR).DoAndReturn(func(_ string, _ int) (BlockDevice, error) {
							device.EXPECT().Close().Return(deviceClosingError)
							return device, nil
						})
						device.EXPECT().Size().Return(int64(deviceSize), nil)
						device.EXPECT().Name().AnyTimes().Return(deviceName)
					}
				})
				WhenClosingErrorVariants := func(f func(), no_error func()) {
					When("no device closing error", func() {
						BeforeEach(func() {
							deviceClosingError = nil
						})
						WhenInputClosingErrorVariants(f, no_error)
					})
					When("device closing error", func() {
						BeforeEach(func() {
							deviceClosingError = errors.New("can't close device")
						})
						WhenInputClosingErrorVariants(f, func() {})
						JustAfterEach(func() {
							if feature.VolumeCleanupEnabled() {
								Expect(err).To(MatchError(deviceClosingError))
							}
						})
					})
				}
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
							expectedReadCallCount++
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
								}).AnyTimes()
							}
						})
						When("input has enough bytes to read", func() {
							BeforeEach(func() {
								readMissingBytes = 0
							})

							When("SinglePass", func() {
								BeforeEach(func() {
									method = "RandomFillSinglePass"
									passCount = 1
								})
								WhenClosingErrorVariants(func() {
									It("fails the device", doCall)
								}, func() {
									JustAfterEach(func() {
										if feature.VolumeCleanupEnabled() {
											Expect(err).ToNot(HaveOccurred())
										}
									})
								})
							})
							When("ThreePass", func() {
								BeforeEach(func() {
									method = "RandomFillThreePass"
									passCount = 3
								})
								WhenClosingErrorVariants(func() {
									It("fills the device", doCall)
								}, func() {
									JustAfterEach(func() {
										if feature.VolumeCleanupEnabled() {
											Expect(err).ToNot(HaveOccurred())
										}
									})
								})
							})

						})
						When("input doesn't have enough bytes to read", func() {
							BeforeEach(func() {
								readMissingBytes = 512
							})

							When("SinglePass", func() {
								BeforeEach(func() {
									method = "RandomFillSinglePass"
									passCount = 1
								})

								WhenClosingErrorVariants(func() {
									It("fails", func() {
										doCall()
										if feature.VolumeCleanupEnabled() {
											Expect(err).To(HaveOccurred())
										}
									})
								}, func() {})
							})
							When("ThreePass", func() {
								BeforeEach(func() {
									method = "RandomFillThreePass"
									passCount = 3
								})
								WhenClosingErrorVariants(func() {
									It("fails", func() {
										doCall()
										if feature.VolumeCleanupEnabled() {
											Expect(err).To(HaveOccurred())
										}
									})
								}, func() {})
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
})
