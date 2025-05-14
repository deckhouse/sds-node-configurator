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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

var _ = Describe("Discoverer", func() {
	var ctx context.Context
	var metrics monitoring.Metrics
	var fakeClient client.WithWatch
	var log logger.Logger
	var sdsCache *cache.Cache
	var discoverer *bd.Discoverer
	config := bd.DiscovererConfig{
		MachineID: "testMachineID",
		NodeName:  "testNodeName",
	}

	BeforeEach(func() {
		ctx = context.Background()
		metrics = monitoring.GetMetrics("")
		log = logger.NewLoggerWrap(GinkgoLogr)
		sdsCache = cache.New()
		fakeClient = test_utils.NewFakeClient()
		discoverer = bd.NewDiscoverer(fakeClient, log, metrics, sdsCache, config)
	})

	thisNodeConsumableAPIDevices := []v1alpha1.BlockDevice{
		internal.BlockDeviceCandidate{
			NodeName:   config.NodeName,
			MachineID:  config.MachineID,
			Name:       "existingName1",
			Consumable: true,
		}.AsAPIBlockDevice(),
	}

	thisNodeNonConsumableAPIDevices := []v1alpha1.BlockDevice{
		internal.BlockDeviceCandidate{
			NodeName:   config.NodeName,
			MachineID:  config.MachineID,
			Name:       "existingName4",
			Consumable: false,
		}.AsAPIBlockDevice(),
	}
	_ = thisNodeNonConsumableAPIDevices

	otherNodeAPIDevices := []v1alpha1.BlockDevice{
		internal.BlockDeviceCandidate{
			NodeName:  "otherNode",
			MachineID: "MachineID",
			Name:      "existingName2",
		}.AsAPIBlockDevice(),
		internal.BlockDeviceCandidate{
			NodeName:  "otherNode2",
			MachineID: "MachineID2",
			Name:      "existingName3",
		}.AsAPIBlockDevice(),
	}

	DescribeTableSubtree("with initial devices",
		Entry("no devices", []v1alpha1.BlockDevice{}),
		Entry("only other Node and machineID devices", otherNodeAPIDevices),
		Entry("consumable devices from the same node", thisNodeConsumableAPIDevices),
		Entry("consumable devices from the same node some from another", append(slices.Clone(thisNodeConsumableAPIDevices), otherNodeAPIDevices...)),
		// TODO: add proper case for these. We don't remove non-consumable device to keep recourse for tracking and history
		// Entry("non consumable devices from the same node", thisNodeNonConsumableAPIDevices),
		// Entry("devices from the same node", append(slices.Clone(thisNodeNonConsumableAPIDevices), thisNodeConsumableAPIDevices...),

		func(initalBlockDevices []v1alpha1.BlockDevice) {
			JustBeforeEach(func() {
				for _, obj := range initalBlockDevices {
					Expect(fakeClient.Create(ctx, &obj)).ShouldNot(HaveOccurred())
				}
			})

			DescribeTableSubtree("when initially appears",
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

					DescribeTableSubtree("with block device filters",
						func() []TableEntry {
							filterEntries := []TableEntry{
								Entry("no filters", []metav1.LabelSelector{}, internalDevices),
							}

							for i, device := range internalDevices {
								remainingDevices := slices.Delete(slices.Clone(internalDevices), i, i+1)
								filterEntries = append(filterEntries,
									Entry(fmt.Sprintf("device %v filtered by WWN", i), []metav1.LabelSelector{
										{
											MatchExpressions: []metav1.LabelSelectorRequirement{
												{
													Key:      v1alpha1.BlockDeviceWWNLabelKey,
													Operator: metav1.LabelSelectorOpNotIn,
													Values:   []string{device.Wwn},
												},
											},
										},
									}, remainingDevices))
								filterEntries = append(filterEntries,
									Entry(fmt.Sprintf("device %v filtered by Serial", i), []metav1.LabelSelector{
										{
											MatchExpressions: []metav1.LabelSelectorRequirement{
												{
													Key:      v1alpha1.BlockDeviceSerialLabelKey,
													Operator: metav1.LabelSelectorOpNotIn,
													Values:   []string{device.Serial},
												},
											},
										},
									}, remainingDevices))
							}
							return filterEntries
						}(),
						func(selectors []metav1.LabelSelector, remainingInternalDevices []internal.Device) {
							JustBeforeEach(func() {
								for i, selector := range selectors {
									Expect(fakeClient.Create(ctx, &v1alpha1.BlockDeviceFilter{
										ObjectMeta: metav1.ObjectMeta{
											Name: fmt.Sprintf("block-device-filter-%v", i),
										},
										Spec: v1alpha1.BlockDeviceFilterSpec{
											BlockDeviceSelector: &selector,
										},
									})).ShouldNot(HaveOccurred())
								}
							})

							When("discovered", func() {
								var (
									discoverResult           controller.Result
									discoverError            error
									deviceListBeforeDiscover v1alpha1.BlockDeviceList
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
									Expect(fakeClient.List(ctx, &deviceListBeforeDiscover)).ShouldNot(HaveOccurred())
									discoverResult, discoverError = discoverer.Discover(ctx)
								})

								expectAPIDevicesMatchedToInternalDevicesAndUnrelatedDevicesAreNotChanged := func(internalDevices []internal.Device) {
									list := &v1alpha1.BlockDeviceList{}
									Expect(fakeClient.List(ctx, list)).ShouldNot(HaveOccurred())

									byMachineID := splitByMachineID(list.Items)
									byNode := splitByNode(list.Items)

									thisNodeDevices := byNode[config.NodeName]
									thisMachineDevices := byMachineID[config.MachineID]
									Expect(thisNodeDevices).Should(BeEquivalentTo(thisMachineDevices))
									Expect(thisMachineDevices).Should(HaveLen(len(internalDevices)))
									mapAPIBlockDevicesByName := make(map[string]v1alpha1.BlockDevice, len(list.Items))
									for _, apiBlockDevice := range thisMachineDevices {
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

									initialByMachineID := splitByMachineID(deviceListBeforeDiscover.Items)
									for machineID := range byMachineID {
										if machineID == config.MachineID {
											continue
										}
										devices := byMachineID[machineID]
										initialDevices := initialByMachineID[machineID]
										Expect(devices).To(Equal(initialDevices))
									}
								}

								It("adds devices to api", func() {
									Expect(discoverError).ShouldNot(HaveOccurred())
									Expect(discoverResult.RequeueAfter).Should(BeZero())
									expectAPIDevicesMatchedToInternalDevicesAndUnrelatedDevicesAreNotChanged(remainingInternalDevices)
								})

								DescribeTableSubtree("when internal device list has changed",
									func() []TableEntry {
										deviceChangeEntries := []TableEntry{
											Entry("all devices removed", []internal.Device{}),
										}

										for i := range remainingInternalDevices {
											newDevices := slices.Delete(slices.Clone(remainingInternalDevices), i, i+1)
											Expect(newDevices).Should(HaveLen(len(remainingInternalDevices) - 1))
											deviceChangeEntries = append(deviceChangeEntries, Entry(fmt.Sprintf("device %v is removed", i), newDevices))
										}

										for i := range remainingInternalDevices {
											newDevices := slices.Replace(slices.Clone(remainingInternalDevices), i, i+1, internal.Device{
												Name:   "testDeviceNameNew",
												Model:  "very good-modelNew",
												Serial: "testSerialNew",
												Wwn:    "testWWNNew",
												Type:   "testTypeNew",
												Size:   resource.MustParse("10G"),
											})
											deviceChangeEntries = append(deviceChangeEntries, Entry(fmt.Sprintf("device %v is replaced", i), newDevices))
										}

										for i := range remainingInternalDevices {
											newDevices := slices.Clone(remainingInternalDevices)
											newDevices[i].Size = resource.MustParse("3G")
											deviceChangeEntries = append(deviceChangeEntries, Entry(fmt.Sprintf("device %v size has changed", i), newDevices))
										}
										return deviceChangeEntries
									}(),
									func(updatedInternalDevices []internal.Device) {
										JustBeforeEach(func() {
											expectAPIDevicesMatchedToInternalDevicesAndUnrelatedDevicesAreNotChanged(remainingInternalDevices)
											sdsCache.StoreDevices(updatedInternalDevices, bytes.Buffer{})

											result, err := discoverer.Discover(ctx)
											Expect(err).ShouldNot(HaveOccurred())
											Expect(result.RequeueAfter).Should(BeZero())
										})

										It("updates devices", func() {
											expectAPIDevicesMatchedToInternalDevicesAndUnrelatedDevicesAreNotChanged(updatedInternalDevices)
										})
									})

								DescribeTableSubtree("when device filtered out", func() []TableEntry {
									filterChangeEntries := []TableEntry{}
									for i, device := range remainingInternalDevices {
										newRemainingDevices := slices.Delete(slices.Clone(remainingInternalDevices), i, i+1)
										selectorsWithWWN := slices.Clone(selectors)
										selectorsWithWWN = append(selectorsWithWWN, metav1.LabelSelector{
											MatchExpressions: []metav1.LabelSelectorRequirement{
												{
													Key:      v1alpha1.BlockDeviceWWNLabelKey,
													Operator: metav1.LabelSelectorOpNotIn,
													Values:   []string{device.Wwn},
												},
											},
										})
										filterChangeEntries = append(filterChangeEntries, Entry(
											fmt.Sprintf("device %v filtered out by WWN", i), selectorsWithWWN, newRemainingDevices))

										selectorsWithSerial := slices.Clone(selectors)
										selectorsWithSerial = append(selectorsWithSerial, metav1.LabelSelector{
											MatchExpressions: []metav1.LabelSelectorRequirement{
												{
													Key:      v1alpha1.BlockDeviceSerialLabelKey,
													Operator: metav1.LabelSelectorOpNotIn,
													Values:   []string{device.Serial},
												},
											},
										})
										filterChangeEntries = append(filterChangeEntries, Entry(
											fmt.Sprintf("device %v filtered out by Serial", i), selectorsWithSerial, newRemainingDevices))
									}
									return filterChangeEntries
								}(), func(newLabelSelectors []metav1.LabelSelector, newRemainingDevices []internal.Device) {
									JustBeforeEach(func() {
										Expect(discoverError).ShouldNot(HaveOccurred())
										Expect(discoverResult.RequeueAfter).Should(BeZero())
										expectAPIDevicesMatchedToInternalDevicesAndUnrelatedDevicesAreNotChanged(remainingInternalDevices)

										Expect(fakeClient.DeleteAllOf(ctx, &v1alpha1.BlockDeviceFilter{})).ShouldNot(HaveOccurred())
										for i, selector := range newLabelSelectors {
											Expect(fakeClient.Create(ctx, &v1alpha1.BlockDeviceFilter{
												ObjectMeta: metav1.ObjectMeta{
													Name: fmt.Sprintf("block-device-filter-%v", i),
												},
												Spec: v1alpha1.BlockDeviceFilterSpec{
													BlockDeviceSelector: &selector,
												},
											})).ShouldNot(HaveOccurred())
										}

										result, err := discoverer.Discover(ctx)
										Expect(err).ShouldNot(HaveOccurred())
										Expect(result.RequeueAfter).Should(BeZero())
									})

									It("Removes the device", func() {
										expectAPIDevicesMatchedToInternalDevicesAndUnrelatedDevicesAreNotChanged(newRemainingDevices)
									})

									When("filters are returned to previous state", func() {
										JustBeforeEach(func() {
											Expect(fakeClient.DeleteAllOf(ctx, &v1alpha1.BlockDeviceFilter{})).ShouldNot(HaveOccurred())
											for i, selector := range selectors {
												Expect(fakeClient.Create(ctx, &v1alpha1.BlockDeviceFilter{
													ObjectMeta: metav1.ObjectMeta{
														Name: fmt.Sprintf("block-device-filter-%v", i),
													},
													Spec: v1alpha1.BlockDeviceFilterSpec{
														BlockDeviceSelector: &selector,
													},
												})).ShouldNot(HaveOccurred())
											}

											result, err := discoverer.Discover(ctx)
											Expect(err).ShouldNot(HaveOccurred())
											Expect(result.RequeueAfter).Should(BeZero())
										})

										It("device is back", func() {
											expectAPIDevicesMatchedToInternalDevicesAndUnrelatedDevicesAreNotChanged(remainingInternalDevices)
										})
									})
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
