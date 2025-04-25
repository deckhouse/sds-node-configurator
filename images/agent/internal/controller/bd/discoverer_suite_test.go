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
	"fmt"
	"slices"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/resource"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

	thisNodeAPIDevices := []v1alpha1.BlockDevice{
		{
			ObjectMeta: v1.ObjectMeta{
				Name: "existingName1",
			},
			Status: v1alpha1.BlockDeviceStatus{
				NodeName:  config.NodeName,
				MachineID: config.MachineID,
			},
		},
	}

	otherNodeAPIDevices := []v1alpha1.BlockDevice{
		{
			ObjectMeta: v1.ObjectMeta{
				Name: "existingName2",
			},
			Status: v1alpha1.BlockDeviceStatus{
				NodeName:  "otherNode",
				MachineID: "otherMachineID",
			},
		},
	}

	DescribeTableSubtree("initial devices",
		Entry("no devices", []v1alpha1.BlockDevice{}),
		Entry("only other Node and machineID devices", otherNodeAPIDevices),
		Entry("some devices from the same node", thisNodeAPIDevices),
		Entry("some devices from the same node some from other", append(slices.Clone(thisNodeAPIDevices), otherNodeAPIDevices...)),
		func(intialBlockDevices []v1alpha1.BlockDevice) {
			JustBeforeEach(func() {
				for _, obj := range intialBlockDevices {
					Expect(fakeClient.Create(ctx, &obj)).ShouldNot(HaveOccurred())
				}
			})

			DescribeTableSubtree("initially appears",
				Entry("no devices", []internal.Device{}),
				Entry("one device", []internal.Device{
					{
						Name:   "testDeviceName",
						Model:  "very good-model",
						Serial: "testSerial",
						Wwn:    "testWWN",
						Type:   "testType",
						Size:   resource.MustParse("1G"),
					}}),
				Entry("two devices", []internal.Device{
					{
						Name:   "testDeviceName1",
						Model:  "very good-model1",
						Serial: "testSerial1",
						Wwn:    "testWWN1",
						Type:   "testType1",
						Size:   resource.MustParse("1G"),
					},
					{
						Name:   "testDeviceName2",
						Model:  "very good-model2",
						Serial: "testSerial2",
						Wwn:    "testWWN2",
						Type:   "testType2",
						Size:   resource.MustParse("2G"),
					}}),
				func(internalDevices []internal.Device) {
					JustBeforeEach(func() {
						sdsCache.StoreDevices(internalDevices, bytes.Buffer{})
					})

					When("discovered", func() {
						var (
							discoverResult controller.Result
							discoverError  error
						)

						splitByNode := func(list []v1alpha1.BlockDevice) map[string][]v1alpha1.BlockDevice {
							result := make(map[string][]v1alpha1.BlockDevice)
							for _, item := range list {
								result[item.Status.NodeName] = append(result[item.Status.NodeName], item)
							}
							return result
						}

						splitByMachineID := func(list []v1alpha1.BlockDevice) map[string][]v1alpha1.BlockDevice {
							result := make(map[string][]v1alpha1.BlockDevice)
							for _, item := range list {
								result[item.Status.MachineID] = append(result[item.Status.MachineID], item)
							}
							return result
						}

						JustBeforeEach(func() {
							discoverResult, discoverError = discoverer.Discover(ctx)
						})

						expectAPIDevicesMatchedToInternalDevices := func(internalDevices []internal.Device) {
							list := &v1alpha1.BlockDeviceList{}
							Expect(fakeClient.List(ctx, list)).ShouldNot(HaveOccurred())

							byMachineID := splitByMachineID(list.Items)
							byNode := splitByNode(list.Items)

							Expect(byMachineID[config.MachineID]).Should(HaveLen(len(internalDevices)))
							Expect(byNode)
							mapAPIBlockDevicesByName := make(map[string]v1alpha1.BlockDevice, len(list.Items))
							for _, apiBlockDevice := range byMachineID[config.MachineID] {
								name := apiBlockDevice.Status.Path
								Expect(mapAPIBlockDevicesByName).ShouldNot(ContainElement(name))
								mapAPIBlockDevicesByName[name] = apiBlockDevice
							}
							for _, internalDevice := range internalDevices {
								apiBlockDevice, exists := mapAPIBlockDevicesByName[internalDevice.Name]
								Expect(exists).Should(BeTrue())

								Expect(apiBlockDevice.Status.MachineID).To(Equal(config.MachineID))
								Expect(apiBlockDevice.Status.NodeName).To(Equal(config.NodeName))
								Expect(apiBlockDevice.Status.Consumable).To(BeTrue())
								Expect(apiBlockDevice.Status.Wwn).To(Equal(internalDevice.Wwn))
								Expect(apiBlockDevice.Status.Serial).To(Equal(internalDevice.Serial))
								Expect(apiBlockDevice.Status.Size.Value()).To(Equal(internalDevice.Size.Value()))
								Expect(apiBlockDevice.Status.Rota).To(Equal(internalDevice.Rota))
								Expect(apiBlockDevice.Status.Model).To(Equal(internalDevice.Model))
								Expect(apiBlockDevice.Status.Type).To(Equal(internalDevice.Type))
								Expect(apiBlockDevice.Status.FsType).To(Equal(internalDevice.FSType))
								Expect(apiBlockDevice.Status.PVUuid).To(BeEmpty())
								Expect(apiBlockDevice.Status.VGUuid).To(BeEmpty())
								Expect(apiBlockDevice.Status.LVMVolumeGroupName).To(BeEmpty())
								Expect(apiBlockDevice.Status.ActualVGNameOnTheNode).To(BeEmpty())
							}
						}

						It("adds devices to api", func() {
							Expect(discoverError).ShouldNot(HaveOccurred())
							Expect(discoverResult.RequeueAfter).Should(BeZero())
							expectAPIDevicesMatchedToInternalDevices(internalDevices)
						})

						deviceChangeEntries := []TableEntry{
							Entry("all devices removed", []internal.Device{}),
						}

						if len(internalDevices) > 1 {
							for i := range internalDevices {
								newDevices := slices.Delete(slices.Clone(internalDevices), i, i+1)
								Expect(newDevices).Should(HaveLen(len(internalDevices) - 1))
								deviceChangeEntries = append(deviceChangeEntries, Entry(fmt.Sprintf("device %v is removed", i), newDevices))
							}
						}

						if len(internalDevices) > 0 {
							for i := range internalDevices {
								newDevices := slices.Replace(slices.Clone(internalDevices), i, i+1, internal.Device{
									Name:   "testDeviceNameNew",
									Model:  "very good-modelNew",
									Serial: "testSerialNew",
									Wwn:    "testWWNNew",
									Type:   "testTypeNew",
									Size:   resource.MustParse("10G"),
								})
								deviceChangeEntries = append(deviceChangeEntries, Entry(fmt.Sprintf("device %v is replaced", i), newDevices))
							}
						}

						DescribeTableSubtree("devices has changed",
							deviceChangeEntries,
							func(changedInternalDevices []internal.Device) {
								JustBeforeEach(func() {
									expectAPIDevicesMatchedToInternalDevices(internalDevices)
									sdsCache.StoreDevices(changedInternalDevices, bytes.Buffer{})

									result, err := discoverer.Discover(ctx)
									Expect(err).ShouldNot(HaveOccurred())
									Expect(result.RequeueAfter).Should(BeZero())
								})

								It("updates devices", func() {
									expectAPIDevicesMatchedToInternalDevices(changedInternalDevices)
								})
							})
					})
				})
		})
})

func TestController(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Controller Suite")
}
