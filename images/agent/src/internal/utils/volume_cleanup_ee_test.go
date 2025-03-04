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
	var blockRange *RangeCover
	vgName := "vg"
	lvName := "lv"
	var method string
	BeforeEach(func() {
		ctrl = gomock.NewController(GinkgoT())
		opener = NewMockBlockDeviceOpener(ctrl)
		device = NewMockBlockDevice(ctrl)
		log = logger.NewLoggerWrap(GinkgoLogr)
	})

	IfVolumeCleanupEnabled := func(foo func()) func() {
		if !feature.VolumeCleanupEnabled() {
			return func() {}
		}
		return foo
	}

	callVolumeCleanup := func() {
		err = VolumeCleanup(context.Background(), log, opener, vgName, lvName, method, blockRange)
		if !feature.VolumeCleanupEnabled() {
			Expect(err).To(MatchError("volume cleanup is not supported in your edition"))
		}
	}

	LengthFormatter := func(e gomock.Matcher) gomock.Matcher {
		return gomock.GotFormatterAdapter(
			gomock.GotFormatterFunc(
				func(i any) string {
					switch ii := i.(type) {
					case []byte:
						return fmt.Sprintf("len %d", len(ii))
					default:
						return "unsupported"
					}
				}),
			e,
		)
	}

	When("method is unknown", func() {
		BeforeEach(func() {
			method = "some"
		})
		It("fails", callVolumeCleanup)
		JustAfterEach(IfVolumeCleanupEnabled(func() {
			Expect(err).To(MatchError(fmt.Sprintf("unknown cleanup method %s", method)))
		}))
	})
	When("method is Discard", func() {
		BeforeEach(func() {
			method = "Discard"
		})

		When("can't open device", func() {
			deviceOpenError := errors.New("can't open device")
			BeforeEach(IfVolumeCleanupEnabled(func() {
				opener.EXPECT().Open(filepath.Join("/dev", vgName, lvName), unix.O_RDWR).Return(nil, deviceOpenError)
			}))
			It("fails with same error", callVolumeCleanup)
			JustAfterEach(IfVolumeCleanupEnabled(func() {
				Expect(err).To(MatchError(deviceOpenError))
			}))
		})

		When("device opened", func() {
			BeforeEach(IfVolumeCleanupEnabled(func() {
				device = NewMockBlockDevice(ctrl)
				name := filepath.Join("/dev", vgName, lvName)
				opener.EXPECT().Open(name, unix.O_RDWR).Return(device, nil)
				device.EXPECT().Name().AnyTimes().Return(name)
			}))
			deviceSize := 1024
			When("discard succeed", func() {
				BeforeEach(IfVolumeCleanupEnabled(func() {
					device.EXPECT().Size().Return(int64(deviceSize), nil)
					device.EXPECT().Discard(uint64(0), uint64(deviceSize)).Return(nil)
				}))
				When("no closing error", func() {
					BeforeEach(IfVolumeCleanupEnabled(func() {
						device.EXPECT().Close().Return(nil)
					}))
					It("calls device discard", callVolumeCleanup)
					JustAfterEach(IfVolumeCleanupEnabled(func() {
						Expect(err).ToNot(HaveOccurred())
					}))
				})
				When("cannot close", func() {
					closingError := errors.New("closing error")
					BeforeEach(IfVolumeCleanupEnabled(func() {
						device.EXPECT().Close().Return(closingError)
					}))
					It("fails with closing error", callVolumeCleanup)
					JustAfterEach(IfVolumeCleanupEnabled(func() {
						Expect(err).To(MatchError(closingError))
					}))
				})
			})
			When("discard fails", func() {
				discardError := errors.New("discard error")
				BeforeEach(IfVolumeCleanupEnabled(func() {
					device.EXPECT().Size().Return(int64(deviceSize), nil)
					device.EXPECT().Discard(uint64(0), uint64(deviceSize)).Return(discardError)
				}))
				When("no closing error", func() {
					BeforeEach(IfVolumeCleanupEnabled(func() {
						device.EXPECT().Close().Return(nil)
					}))
					It("fails with matched error", callVolumeCleanup)
					JustAfterEach(IfVolumeCleanupEnabled(func() {
						Expect(err).To(MatchError(discardError))
					}))
				})
				When("closing error", func() {
					closingError := errors.New("closing error")
					BeforeEach(IfVolumeCleanupEnabled(func() {
						device.EXPECT().Close().Return(closingError)
					}))
					It("fails with matched errors", callVolumeCleanup)
					JustAfterEach(IfVolumeCleanupEnabled(func() {
						Expect(err).To(MatchError(discardError))
						Expect(err).To(MatchError(closingError))
					}))
				})
			})
		})
	})
	inputName := "/dev/urandom"
	DescribeTableSubtree("block sizes",
		Entry("512 bytes", 512),
		Entry("1 Kib", 1024),
		func(deviceBlockSize int) {
			DescribeTableSubtree("block counts",
				Entry("50 Mib", int64(1024*1024*50/deviceBlockSize)),
				Entry("1 Mib", int64(1024*1024*1/deviceBlockSize)),
				Entry("1 Kib", int64(1024)),
				func(deviceBlockCount int64) {
					deviceSize := deviceBlockCount * int64(deviceBlockSize)
					When("method is RandomFill", func() {
						var passCount int
						var expectedByteRangeCover RangeCover
						bufferSize := 1024 * 1024 * 4
						When("input open succeed", func() {
							var input *MockBlockDevice
							var inputClosingError error
							BeforeEach(func() {
								input = NewMockBlockDevice(ctrl)
							})
							JustBeforeEach(IfVolumeCleanupEnabled(func() {
								input.EXPECT().Name().AnyTimes().Return(inputName)
								opener.EXPECT().Open(inputName, unix.O_RDONLY).DoAndReturn(func(_ string, _ int) (BlockDevice, error) {
									input.EXPECT().Close().Return(inputClosingError)
									return input, nil
								})
							}))
							WhenInputClosingErrorVariants := func(f func(noClosingErrors bool)) {
								When("no input closing error", func() {
									BeforeEach(func() {
										inputClosingError = nil
									})
									f(true)
								})
								When("input closing error", func() {
									BeforeEach(func() {
										inputClosingError = errors.New("can't close device")
									})
									f(false)
									JustAfterEach(IfVolumeCleanupEnabled(func() {
										Expect(err).To(MatchError(inputClosingError))
									}))
								})
							}
							deviceName := filepath.Join("/dev", vgName, lvName)
							When("device open succeed", func() {
								var deviceClosingError error
								BeforeEach(func() {
									device = NewMockBlockDevice(ctrl)
								})
								JustBeforeEach(IfVolumeCleanupEnabled(func() {
									opener.EXPECT().Open(deviceName, unix.O_DIRECT|unix.O_RDWR).DoAndReturn(func(_ string, _ int) (BlockDevice, error) {
										device.EXPECT().Close().Return(deviceClosingError)
										return device, nil
									})
									device.EXPECT().Name().AnyTimes().Return(deviceName)
								}))
								WhenClosingErrorVariants := func(f func(noClosingError bool)) {
									When("no device closing error", func() {
										BeforeEach(func() {
											deviceClosingError = nil
										})
										WhenInputClosingErrorVariants(f)
									})
									When("device closing error", func() {
										BeforeEach(func() {
											deviceClosingError = errors.New("can't close device")
										})
										WhenInputClosingErrorVariants(func(_ bool) {
											f(false)
										})
										JustAfterEach(IfVolumeCleanupEnabled(func() {
											Expect(err).To(MatchError(deviceClosingError))
										}))
									})
								}
								When("copying data", func() {
									var readMissingBytes int64
									JustBeforeEach(IfVolumeCleanupEnabled(func() {
										if blockRange == nil {
											expectedByteRangeCover = RangeCover{Range{Start: 0, Count: deviceSize}}
											device.EXPECT().Size().Return(deviceSize, nil)
										} else {
											expectedByteRangeCover = blockRange.Multiplied(int64(deviceBlockSize))
											device.EXPECT().BlockSize().Return(deviceBlockSize, nil)
										}
										bytesToReadPerPass := int64(0)
										for _, r := range expectedByteRangeCover {
											bytesToReadPerPass += r.Count
										}
										expectedTotalBytesRead := bytesToReadPerPass * int64(passCount)
										readLimit := expectedTotalBytesRead - readMissingBytes

										gomock.InOrder(func() (calls []any) {
											for pass := 0; pass < passCount; pass++ {
												for _, byteRange := range expectedByteRangeCover {
													var offset int64 = byteRange.Start
													deviceSizeRemain := deviceSize - byteRange.Start
													for remainInRange := byteRange.Count; remainInRange > 0; {
														var toRead int = int(min(remainInRange, int64(bufferSize)))
														var read int = int(min(readLimit, int64(toRead)))
														var written int = int(min(deviceSizeRemain, int64(read)))

														calls = append(calls, input.EXPECT().Read(LengthFormatter(gomock.Len(toRead))).Return(read, nil))
														if read != 0 {
															calls = append(calls, device.EXPECT().WriteAt(LengthFormatter(gomock.Len(read)), offset).Return(written, nil))
														}

														if read > written {
															return
														}

														if toRead > read {
															calls = append(calls, input.EXPECT().Read(LengthFormatter(gomock.Any())).Return(0, io.EOF))
															return
														}

														deviceSizeRemain -= int64(written)
														readLimit -= int64(read)
														remainInRange -= int64(written)
														offset += int64(written)
													}
												}
											}
											return
										}()...)
									}))
									When("input has enough bytes to read", func() {
										BeforeEach(func() {
											readMissingBytes = 0
										})

										DescribeTableSubtree("methods",
											Entry("SinglePass", "RandomFillSinglePass", 1),
											Entry("ThreePass", "RandomFillThreePass", 3),
											func(methodParam string, passCountParam int) {
												BeforeEach(func() {
													method = methodParam
													passCount = passCountParam
												})
												DescribeTableSubtree("ranges",
													Entry("nil", nil),
													Entry("empty", &RangeCover{}),
													Entry("full", &RangeCover{Range{0, deviceBlockCount}}),
													Entry("first half", &RangeCover{Range{0, deviceBlockCount / 2}}),
													Entry("second half", &RangeCover{Range{deviceBlockCount / 2, deviceBlockCount - deviceBlockCount/2}}),
													Entry("hole inside", &RangeCover{Range{0, deviceBlockCount/2 - 1}, Range{deviceBlockCount/2 + 1, deviceBlockCount/2 - 1}}),
													func(blockRangeParam *RangeCover) {
														BeforeEach(func() {
															blockRange = blockRangeParam
														})
														WhenClosingErrorVariants(func(noClosingError bool) {
															It("fills the device", callVolumeCleanup)
															if noClosingError {
																JustAfterEach(IfVolumeCleanupEnabled(func() {
																	Expect(err).ToNot(HaveOccurred())
																}))
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

											WhenClosingErrorVariants(func(_ bool) {
												It("fails", callVolumeCleanup)
												JustAfterEach(IfVolumeCleanupEnabled(func() {
													Expect(err).To(HaveOccurred())
												}))
											})
										})
										When("ThreePass", func() {
											BeforeEach(func() {
												method = "RandomFillThreePass"
												passCount = 3
											})
											WhenClosingErrorVariants(func(_ bool) {
												It("fails", callVolumeCleanup)
												JustAfterEach(IfVolumeCleanupEnabled(func() {
													Expect(err).To(HaveOccurred())
												}))
											})
										})
									})
								})
							})
							When("device open failed", func() {
								deviceOpenError := errors.New("input open error")
								BeforeEach(IfVolumeCleanupEnabled(func() {
									opener.EXPECT().Open(deviceName, unix.O_DIRECT|unix.O_RDWR).Return(nil, deviceOpenError)
								}))
								It("fails with matched error", callVolumeCleanup)
								JustAfterEach(IfVolumeCleanupEnabled(func() {
									Expect(err).To(MatchError(deviceOpenError))
								}))
							})
						})
						When("input open failed", func() {
							inputOpenError := errors.New("input open error")
							BeforeEach(IfVolumeCleanupEnabled(func() {
								opener.EXPECT().Open(inputName, unix.O_RDONLY).Return(nil, inputOpenError)
							}))

							It("fails with matched error", callVolumeCleanup)
							JustAfterEach(IfVolumeCleanupEnabled(func() {
								Expect(err).To(MatchError(inputOpenError))
							}))
						})
					})
				})
		})
})
