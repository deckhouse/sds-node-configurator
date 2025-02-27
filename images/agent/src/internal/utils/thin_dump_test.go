package utils_test

import (
	"context"
	"encoding/xml"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"agent/internal/logger"
	. "agent/internal/utils"
)

var _ = Describe("Thin mapping", func() {
	var (
		err        error
		log        logger.Logger
		ctx        context.Context
		superblock Superblock
	)

	BeforeEach(func() {
		log = logger.NewLoggerWrap(GinkgoLogr)
		ctx = context.Background()
	})

	var (
		ranges   RangeCover
		deviceID LVMThinDeviceID
	)

	WhenRangeCoverIsFound := func(foo func()) {
		When("range cover is found", func() {
			JustBeforeEach(func() {
				ranges, err = ThinVolumeUsedRanges(ctx, log, superblock, deviceID)
			})

			foo()
		})
	}

	ItFailsWithDeviceNotFound := func() {
		It("fails with device not found", func() {
			Expect(err).To(MatchError("device not found"))
		})
	}

	ItFailsWithSelfOverlappedRanges := func() {
		It("fails", func() {
			Expect(err).To(MatchError("finding used ranges: self overlapped range"))
		})
	}

	When("superblock is empty", func() {
		BeforeEach(func() {
			superblock = Superblock{}
		})

		WhenRangeCoverIsFound(func() {
			DescribeTableSubtree("fails with device not found", func(arg int) {
				BeforeEach(func() {
					deviceID = LVMThinDeviceID(arg)
				})
				ItFailsWithDeviceNotFound()
			},
				Entry("0", 0),
				Entry("1", 1),
				Entry("42", 42))
		})
	})

	WhenRangeCoverIsFound(func() {
		When("superblock has overlapped ranges", func() {
			BeforeEach(func() {
				deviceID = 1
				superblock = Superblock{Devices: []Device{
					{
						DevID: deviceID,
						RangeMappings: []RangeMapping{
							{OriginBegin: 3, Length: 7},
							{OriginBegin: 5, Length: 8},
						},
					},
				}}
			})
			ItFailsWithSelfOverlappedRanges()
		})

		When("superblock has single block within other range", func() {
			BeforeEach(func() {
				deviceID = 1
				superblock = Superblock{Devices: []Device{
					{
						DevID: deviceID,
						RangeMappings: []RangeMapping{
							{OriginBegin: 3, Length: 7},
						},
						SingleMappings: []SingleMapping{
							{OriginBlock: 5},
						},
					},
				}}
			})
			ItFailsWithSelfOverlappedRanges()
		})

		When("superblock has two single block in same point", func() {
			BeforeEach(func() {
				deviceID = 1
				superblock = Superblock{Devices: []Device{
					{
						DevID: deviceID,
						SingleMappings: []SingleMapping{
							{OriginBlock: 5},
							{OriginBlock: 5},
						},
					},
				}}
			})
			ItFailsWithSelfOverlappedRanges()
		})
	})

	When("unmarshaled", func() {
		var (
			raw    []byte
			device Device
		)

		JustBeforeEach(func() {
			superblock = Superblock{}
			err = xml.Unmarshal(raw, &superblock)
			Expect(err).ToNot(HaveOccurred())
			Expect(len(superblock.Devices)).To(BeEquivalentTo(1))
			device = superblock.Devices[0]
		})

		When("parsing real life mapping of 100Mib device", func() {
			BeforeEach(func() {
				raw = []byte(`<superblock uuid="" time="0" transaction="9" flags="0" version="2" data_block_size="128" nr_data_blocks="0">
		  <device dev_id="1" mapped_blocks="1600" transaction="8" creation_time="0" snap_time="0">
			<range_mapping origin_begin="0" data_begin="3044" length="156" time="0"/>
			<range_mapping origin_begin="156" data_begin="0" length="4" time="0"/>
			<single_mapping origin_block="160" data_block="3043" time="0"/>
			<range_mapping origin_begin="161" data_begin="4" length="1439" time="0"/>
		  </device>
		</superblock>
		`)
			})

			JustBeforeEach(func() {
				Expect(err).ToNot(HaveOccurred())
			})

			It("matches 100Mib size", func() {
				Expect(device.MappedBlocks * superblock.DataBlockSize * 512).To(BeEquivalentTo(100 * 1024 * 1024))
			})

			WhenRangeCoverIsFound(func() {
				When("device is in list", func() {
					BeforeEach(func() {
						deviceID = 1
					})

					JustBeforeEach(func() {
						Expect(err).ToNot(HaveOccurred())
					})

					It("single piece range of all the device", func() {
						Expect(len(ranges)).To(BeEquivalentTo(1))
						Expect(ranges[0].Start).To(BeEquivalentTo(0))
						Expect(ranges[0].Count).To(BeEquivalentTo(1024 * 1024 * 100 / 512))
					})
				})

				DescribeTableSubtree("fails with device not found", func(arg int) {
					BeforeEach(func() {
						deviceID = LVMThinDeviceID(arg)
					})
					ItFailsWithDeviceNotFound()
				},
					Entry("0", 0),
					Entry("2", 2),
					Entry("42", 42))
			})
		})
	})
})
