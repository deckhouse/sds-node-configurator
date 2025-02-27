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
		err error
		log logger.Logger
		ctx context.Context
	)

	BeforeEach(func() {
		log = logger.NewLoggerWrap(GinkgoLogr)
		ctx = context.Background()
	})

	When("parsing real life mapping of 100Mib device", func() {
		var (
			raw []byte
		)

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

		When("unmarshaled", func() {
			var superblock Superblock
			var device Device
			BeforeEach(func() {
				err = xml.Unmarshal(raw, &superblock)
				Expect(err).ToNot(HaveOccurred())
				Expect(len(superblock.Devices)).To(BeEquivalentTo(1))
				device = superblock.Devices[0]
			})
			AfterEach(func() {
				superblock = Superblock{}
			})

			It("matches 100Mib size", func() {
				Expect(device.MappedBlocks * superblock.DataBlockSize * 512).To(BeEquivalentTo(100 * 1024 * 1024))
			})

			When("range cover is found", func() {
				var ranges RangeCover
				JustBeforeEach(func() {
					ranges, err = ThinVolumeUsedRanges(ctx, log, superblock, 1)
					Expect(err).ToNot(HaveOccurred())
				})

				It("single piece range of all the device", func() {
					Expect(len(ranges)).To(BeEquivalentTo(1))
					Expect(ranges[0].Start).To(BeEquivalentTo(0))
					Expect(ranges[0].Count).To(BeEquivalentTo(1024 * 1024 * 100 / 512))
				})
			})

		})
	})
})
