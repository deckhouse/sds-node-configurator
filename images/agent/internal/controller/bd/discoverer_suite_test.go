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

package bd_test

import (
	"bytes"
	"context"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/cache"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/controller"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/controller/bd"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/logger"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/monitoring"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/test_utils"
)

var _ = Describe("Storage Controller", func() {
	var ctx context.Context
	var metrics monitoring.Metrics
	var fakeClient client.WithWatch
	var log logger.Logger
	var sdsCache *cache.Cache
	var config bd.DiscovererConfig
	var discoverer *bd.Discoverer

	BeforeEach(func() {
		ctx = context.Background()
		metrics = monitoring.GetMetrics("")
		log = logger.NewLoggerWrap(GinkgoLogr)
		sdsCache = cache.New()
		fakeClient = test_utils.NewFakeClient()
		config.MachineID = "testMachineID"
		config.NodeName = "testNodeName"
		discoverer = bd.NewDiscoverer(fakeClient, log, metrics, sdsCache, config)
	})

	When("new devices appear", func() {
		var (
			newDevices    []internal.Device
			newDevicesErr bytes.Buffer
		)
		BeforeEach(func() {
			newDevices = []internal.Device{
				{
					Name:   "testDeviceName",
					Model:  "very good-model",
					Serial: "testSerial",
					Wwn:    "testWWN",
					Type:   "testType",
					Size:   resource.MustParse("1G"),
				}}
		})

		JustBeforeEach(func() {
			sdsCache.StoreDevices(newDevices, newDevicesErr)
		})

		When("discovered", func() {
			var (
				discoverResult controller.Result
				discoverError  error
			)

			JustBeforeEach(func() {
				discoverResult, discoverError = discoverer.Discover(ctx)
			})

			expectOneDeviceMatchedToNewDevice := func() {
				list := &v1alpha1.BlockDeviceList{}
				Expect(fakeClient.List(ctx, list)).ShouldNot(HaveOccurred())
				Expect(list.Items).Should(HaveLen(1))
				blockDevice := list.Items[0]
				Expect(blockDevice.Status.MachineID).To(Equal(config.MachineID))
				Expect(blockDevice.Status.NodeName).To(Equal(config.NodeName))
				Expect(blockDevice.Status.Consumable).To(BeTrue())
				Expect(blockDevice.Status.Wwn).To(Equal(newDevices[0].Wwn))
				Expect(blockDevice.Status.Serial).To(Equal(newDevices[0].Serial))
				Expect(blockDevice.Status.Size.Value()).To(Equal(newDevices[0].Size.Value()))
				Expect(blockDevice.Status.Rota).To(Equal(newDevices[0].Rota))
				Expect(blockDevice.Status.Model).To(Equal(newDevices[0].Model))
				Expect(blockDevice.Status.Type).To(Equal(newDevices[0].Type))
				Expect(blockDevice.Status.FsType).To(Equal(newDevices[0].FSType))
				Expect(blockDevice.Status.PVUuid).To(BeEmpty())
				Expect(blockDevice.Status.VGUuid).To(BeEmpty())
				Expect(blockDevice.Status.LVMVolumeGroupName).To(BeEmpty())
				Expect(blockDevice.Status.ActualVGNameOnTheNode).To(BeEmpty())
				Expect(blockDevice.Status.Path).To(Equal(newDevices[0].Name))
			}

			It("adds device to api", func() {
				Expect(discoverError).ShouldNot(HaveOccurred())
				Expect(discoverResult.RequeueAfter).Should(BeZero())
				expectOneDeviceMatchedToNewDevice()
			})

			When("device than has changed", func() {
				JustBeforeEach(func() {
					expectOneDeviceMatchedToNewDevice()
					newDevices = []internal.Device{
						{
							Name:   "testDeviceName2",
							Model:  "very good-model2",
							Serial: "testSerial2",
							Wwn:    "testWWN2",
							Type:   "testType2",
							Size:   resource.MustParse("1G"),
						}}
					sdsCache.StoreDevices(newDevices, newDevicesErr)

					result, err := discoverer.Discover(ctx)
					Expect(err).ShouldNot(HaveOccurred())
					Expect(result.RequeueAfter).Should(BeZero())
				})

				It("updates devices", func() {
					expectOneDeviceMatchedToNewDevice()
				})
			})

			When("device deleted", func() {
				JustBeforeEach(func() {
					expectOneDeviceMatchedToNewDevice()
					newDevices = []internal.Device{}
					sdsCache.StoreDevices(newDevices, newDevicesErr)
				})

				It("updates devices", func() {
					result, err := discoverer.Discover(ctx)
					Expect(err).ShouldNot(HaveOccurred())
					Expect(result.RequeueAfter).Should(BeZero())

					list := &v1alpha1.BlockDeviceList{}
					Expect(fakeClient.List(ctx, list)).ShouldNot(HaveOccurred())
					Expect(list.Items).Should(HaveLen(0))
				})
			})
		})
	})
})

func TestController(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Controller Suite")
}
