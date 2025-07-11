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
	_ "embed"
	"encoding/json"
	"fmt"
	"slices"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
)

//go:embed testdata/lsblk_mpath.json
var testLsblkMpathOutput []byte

//go:embed testdata/lsblk_mpath_partitioned.json
var testLsblkMpathPartitionedOutput []byte

var _ = Describe("Discoverer", func() {
	withDiscovererCreated(func(vars *DiscoverCreatedVars) {
		thisNodeConsumableAPIDevices := []v1alpha1.BlockDevice{
			internal.BlockDeviceCandidate{
				NodeName:   NodeName,
				MachineID:  MachineID,
				Name:       "existingName1",
				Consumable: true,
			}.AsAPIBlockDevice(),
		}

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
				withDevicesCreated(initalBlockDevices, func() {
					DescribeTableSubtree("when initially appears",
						Entry("no devices", []internal.Device{}, []string{}),
						Entry("one device", []internal.Device{
							{
								Name:   "testDeviceName",
								KName:  "/dev/name",
								Model:  "very good-model",
								Serial: "testSerial",
								Wwn:    "testWWN",
								Type:   "testType",
								Size:   resource.MustParse("1G"),
							}}, []string{}),
						Entry("two devices", []internal.Device{
							{
								Name:   "testDeviceName1",
								KName:  "/dev/name1",
								Model:  "very good-model1",
								Serial: "testSerial1",
								Wwn:    "testWWN1",
								Type:   "testType1",
								Size:   resource.MustParse("1G"),
							},
							{
								Name:   "testDeviceName2",
								KName:  "/dev/name2",
								Model:  "very good-model2",
								Serial: "testSerial2",
								Wwn:    "testWWN2",
								Type:   "testType2",
								Size:   resource.MustParse("2G"),
							}}, []string{}),
						Entry("mpath devices", func() []internal.Device {
							var devices internal.Devices
							err := json.Unmarshal(testLsblkMpathOutput, &devices)
							Expect(err).ShouldNot(HaveOccurred())
							return devices.BlockDevices
						}(), []string{"/dev/sdii", "/dev/sdik", "/dev/sdij", "/dev/sdio"}),
						Entry("mpath partitioned devices", func() []internal.Device {
							var devices internal.Devices
							err := json.Unmarshal(testLsblkMpathPartitionedOutput, &devices)
							Expect(err).ShouldNot(HaveOccurred())
							return devices.BlockDevices
						}(), []string{
							// mpath parts
							"/dev/sdil", "/dev/sdim", "/dev/sdin",
							// has children
							"/dev/dm-74",
							// too small
							"/dev/dm-82", "/dev/dm-83"}),
						func(internalDevices []internal.Device, filteredOutKNames []string) {
							withInternalDevicesCacheUpdated(internalDevices, &vars.sdsCache, func() {
								DescribeTableSubtree("with block device filters",
									func() []TableEntry {
										remainingAfterFilteringDevices := slices.DeleteFunc(slices.Clone(internalDevices), func(device internal.Device) bool {
											return slices.Contains(filteredOutKNames, device.KName)
										})

										filterEntries := []TableEntry{
											Entry("no filters", []metav1.LabelSelector{}, remainingAfterFilteringDevices),
										}

										for i, device := range remainingAfterFilteringDevices {
											remainingDevices := slices.Delete(slices.Clone(remainingAfterFilteringDevices), i, i+1)
											if device.Wwn != "" {
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
													}, slices.DeleteFunc(slices.Clone(remainingDevices), func(d internal.Device) bool {
														return device.Wwn == d.Wwn
													})))
											}
											if device.Serial != "" {
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
													}, slices.DeleteFunc(slices.Clone(remainingDevices), func(d internal.Device) bool {
														return device.Serial == d.Serial
													})))
											}
										}
										return filterEntries
									}(),
									func(selectors []metav1.LabelSelector, remainingInternalDevices []internal.Device) {
										withBlockDeviceFiltersCreated(selectors, func() {
											whenSuccessfullyDiscovered(vars, func() {
												It("adds devices to api", func(ctx SpecContext) {
													expectAPIDevicesMatchedToInternalDevices(ctx, remainingInternalDevices)
												})

												DescribeTableSubtree("when internal device list has changed",
													func() []TableEntry {
														deviceChangeEntries := []TableEntry{
															Entry("all devices removed", []internal.Device{}, []internal.Device{}),
														}

														hasChildren := func(device internal.Device) bool {
															for _, d := range internalDevices {
																if d.PkName == device.KName {
																	return true
																}
															}
															return false
														}

														for i, device := range remainingInternalDevices {
															if hasChildren(device) {
																continue
															}
															newDevices := slices.Delete(slices.Clone(remainingInternalDevices), i, i+1)
															Expect(newDevices).Should(HaveLen(len(remainingInternalDevices) - 1))
															newInternalDevices := slices.DeleteFunc(slices.Clone(internalDevices), func(d internal.Device) bool {
																return device.KName == d.KName
															})
															Expect(newInternalDevices).Should(HaveLen(len(internalDevices) - 1))
															deviceChangeEntries = append(deviceChangeEntries, Entry(fmt.Sprintf("device %v is removed", i), newDevices, newInternalDevices))
														}

														for i, device := range remainingInternalDevices {
															if hasChildren(device) {
																continue
															}
															newDevice := internal.Device{
																Name:   "testDeviceNameNew",
																KName:  "/dev/kname",
																PkName: device.PkName,
																Model:  "very good-modelNew",
																Serial: "testSerialNew",
																Wwn:    "testWWNNew",
																Type:   "testTypeNew",
																Size:   resource.MustParse("10G"),
															}
															internalDevicesIndex := slices.IndexFunc(slices.Clone(internalDevices), func(d internal.Device) bool {
																return device.KName == d.KName
															})
															newInternalDevices := slices.Replace(slices.Clone(internalDevices), internalDevicesIndex, internalDevicesIndex+1, newDevice)
															newDevices := slices.Replace(slices.Clone(remainingInternalDevices), i, i+1, newDevice)
															deviceChangeEntries = append(deviceChangeEntries, Entry(fmt.Sprintf("device %v is replaced", i), newDevices, newInternalDevices))
														}

														for i, device := range remainingInternalDevices {
															newDevices := slices.Clone(remainingInternalDevices)
															newDevices[i].Size = resource.MustParse("3G")
															newInternalDevices := slices.Clone(internalDevices)
															internalDevicesIndex := slices.IndexFunc(newInternalDevices, func(d internal.Device) bool {
																return device.KName == d.KName
															})
															newInternalDevices[internalDevicesIndex].Size = newDevices[i].Size
															deviceChangeEntries = append(deviceChangeEntries, Entry(fmt.Sprintf("device %v size has changed", i), newDevices, newInternalDevices))
														}
														return deviceChangeEntries
													}(),
													func(expectedDevices, updatedInternalDevices []internal.Device) {
														JustBeforeEach(func(ctx SpecContext) {
															expectAPIDevicesMatchedToInternalDevices(ctx, remainingInternalDevices)
														})

														withInternalDevicesCacheUpdated(updatedInternalDevices, &vars.sdsCache, func() {
															whenSuccessfullyDiscovered(vars, func() {
																It("updates devices", func(ctx SpecContext) {
																	expectAPIDevicesMatchedToInternalDevices(ctx, expectedDevices)
																})
															})
														})
													})

												DescribeTableSubtree("when device filtered out", func() []TableEntry {
													filterChangeEntries := []TableEntry{}
													for i, device := range remainingInternalDevices {
														newRemainingDevices := slices.Delete(slices.Clone(remainingInternalDevices), i, i+1)
														if device.Wwn != "" {
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
														}
														if device.Serial != "" {
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
													}
													return filterChangeEntries
												}(), func(newLabelSelectors []metav1.LabelSelector, newRemainingDevices []internal.Device) {
													JustBeforeEach(func(ctx SpecContext) {
														expectAPIDevicesMatchedToInternalDevices(ctx, remainingInternalDevices)
													})

													withBlockDeviceFiltersReplaced(newLabelSelectors, func() {
														whenSuccessfullyDiscovered(vars, func() {
															It("Removes the device", func(ctx SpecContext) {
																expectAPIDevicesMatchedToInternalDevices(ctx, newRemainingDevices)
															})

															When("filters are returned to previous state", func() {
																withBlockDeviceFiltersReplaced(selectors, func() {
																	whenSuccessfullyDiscovered(vars, func() {
																		It("device is back", func(ctx SpecContext) {
																			expectAPIDevicesMatchedToInternalDevices(ctx, remainingInternalDevices)
																		})
																	})
																})
															})
														})
													})
												})
											})
										})
									})
							})
						})
				})
			})
	})
})
