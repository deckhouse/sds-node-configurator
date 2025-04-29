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

package internal_test

import (
	"fmt"
	"reflect"
	"strconv"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
)

var _ = Describe("Block device candidate", func() {
	var candidate internal.BlockDeviceCandidate
	BeforeEach(func() {
		candidate = internal.BlockDeviceCandidate{}
	})

	When("creating candidate by internal block device", func() {
		var internalDevice internal.Device
		var nodeName string
		var machineID string

		BeforeEach(func() {
			internalDevice = internal.Device{}
			nodeName = "node1"
			machineID = "machine1"
		})

		JustBeforeEach(func() {
			candidate = internal.NewBlockDeviceCandidateByDevice(&internalDevice, nodeName, machineID)
		})

		It("has matching values", func() {
			Expect(candidate.NodeName).Should(BeEquivalentTo(nodeName))
			Expect(candidate.MachineID).Should(BeEquivalentTo(machineID))
			Expect(candidate.Wwn).Should(BeEquivalentTo(internalDevice.Wwn))
			Expect(candidate.Serial).Should(BeEquivalentTo(internalDevice.Serial))
			Expect(candidate.Name).Should(BeEquivalentTo(internalDevice.Name))
			Expect(candidate.Size).Should(BeEquivalentTo(internalDevice.Size))
			Expect(candidate.Rota).Should(BeEquivalentTo(internalDevice.Rota))
			Expect(candidate.Model).Should(BeEquivalentTo(internalDevice.Model))
			Expect(candidate.HotPlug).Should(BeEquivalentTo(internalDevice.HotPlug))
			Expect(candidate.KName).Should(BeEquivalentTo(internalDevice.KName))
			Expect(candidate.PkName).Should(BeEquivalentTo(internalDevice.PkName))
			Expect(candidate.Type).Should(BeEquivalentTo(internalDevice.Type))
			Expect(candidate.FSType).Should(BeEquivalentTo(internalDevice.FSType))
			Expect(candidate.PartUUID).Should(BeEquivalentTo(internalDevice.PartUUID))
		})

		When("device is good", func() {
			BeforeEach(func() {
				internalDevice.Name = "goodName"
			})

			It("is consumable", func() {
				Expect(candidate.Consumable).Should(BeTrue())
			})
		})

		DescribeTableSubtree("device is not good",
			Entry("hotplug", internal.Device{
				MountPoint: "",
				HotPlug:    true,
				FSType:     "",
			}),
			Entry("mounted", internal.Device{
				MountPoint: "bad",
				HotPlug:    false,
				FSType:     "",
			}),
			Entry("has file system", internal.Device{
				MountPoint: "",
				HotPlug:    false,
				FSType:     "bad",
			}),
			func(notGoodInternalDevice internal.Device) {
				BeforeEach(func() {
					internalDevice = notGoodInternalDevice
				})
				It("is not consumable", func() {
					Expect(candidate.Consumable).Should(BeFalse())
				})
			})
	})

	When("candidate is test data", func() {
		var blockDevice v1alpha1.BlockDevice

		BeforeEach(func() {
			candidate.Type = "testTYPE"
			candidate.FSType = "testFS"
			candidate.NodeName = "test_node"
			candidate.Consumable = false
			candidate.PVUuid = "testPV"
			candidate.VGUuid = "testVGUID"
			candidate.LVMVolumeGroupName = "testLVGName"
			candidate.ActualVGNameOnTheNode = "testNameOnNode"
			candidate.Wwn = "testWWN"
			candidate.Serial = "testSERIAL"
			candidate.Path = "testPATH"
			candidate.Size = resource.MustParse("1G")
			candidate.Model = "Very good model-1241"
			candidate.MachineID = "testMACHINE"
		})

		hasAllFieldsMatched := func() {
			Expect(blockDevice.Status.Type).Should(BeEquivalentTo(candidate.Type))
			Expect(blockDevice.Status.FsType).Should(BeEquivalentTo(candidate.FSType))
			Expect(blockDevice.Status.NodeName).Should(BeEquivalentTo(candidate.NodeName))
			Expect(blockDevice.Status.Consumable).Should(BeEquivalentTo(candidate.Consumable))
			Expect(blockDevice.Status.PVUuid).Should(BeEquivalentTo(candidate.PVUuid))
			Expect(blockDevice.Status.VGUuid).Should(BeEquivalentTo(candidate.VGUuid))
			Expect(blockDevice.Status.LVMVolumeGroupName).Should(BeEquivalentTo(candidate.LVMVolumeGroupName))
			Expect(blockDevice.Status.ActualVGNameOnTheNode).Should(BeEquivalentTo(candidate.ActualVGNameOnTheNode))
			Expect(blockDevice.Status.Wwn).Should(BeEquivalentTo(candidate.Wwn))
			Expect(blockDevice.Status.Serial).Should(BeEquivalentTo(candidate.Serial))
			Expect(blockDevice.Status.Path).Should(BeEquivalentTo(candidate.Path))
			Expect(blockDevice.Status.Size.Format).Should(BeEquivalentTo(resource.BinarySI))
			Expect(blockDevice.Status.Size.Value()).Should(BeEquivalentTo(candidate.Size.Value()))
			Expect(blockDevice.Status.Model).Should(BeEquivalentTo(candidate.Model))
			Expect(blockDevice.Status.MachineID).Should(BeEquivalentTo(candidate.MachineID))
		}

		doesNotHaveBlockDeviceDifference := func() {
			Expect(candidate.HasBlockDeviceDiff(blockDevice)).Should(BeFalse())
		}

		t := reflect.TypeOf(candidate)
		for i := range t.NumField() {
			field := t.Field(i)
			When(fmt.Sprintf("%s has changed", field.Name), func() {
				BeforeEach(func() {
					value := reflect.ValueOf(&candidate).Elem().Field(i)
					switch field.Type.Kind() {
					case reflect.String:
						value.SetString("new value")
					case reflect.Bool:
						value.SetBool(!value.Bool())
					default:
						if field.Type == reflect.TypeFor[resource.Quantity]() {
							value.Set(reflect.ValueOf(resource.MustParse("32G")))
						} else {
							PanicWith("Unexpected struct")
						}
					}

				})
				It("has change detected", func() {
					Expect(candidate.HasBlockDeviceDiff(blockDevice)).Should(BeTrue())
				})
			})
		}

		When("creating api block device", func() {
			JustBeforeEach(func() {
				blockDevice = candidate.AsAPIBlockDevice()
			})

			It("has correct field values", hasAllFieldsMatched)
			It("does not have block device difference", doesNotHaveBlockDeviceDifference)
		})

		When("updating api block device", func() {
			var expectedLabels map[string]string
			BeforeEach(func() {
				blockDevice.Status.Type = "prevType"
				blockDevice.Status.FsType = "prevFS"
				blockDevice.Status.NodeName = "prevNodeName"
				blockDevice.Status.Consumable = true
				blockDevice.Status.PVUuid = "prevPV"
				blockDevice.Status.VGUuid = "prevVG"
				blockDevice.Status.LVMVolumeGroupName = "prevLVMVGName"
				blockDevice.Status.ActualVGNameOnTheNode = "prevActualNameOnNode"
				blockDevice.Status.Wwn = "prevWWN"
				blockDevice.Status.Serial = "prevSerial"
				blockDevice.Status.Path = "prevPath"
				blockDevice.Status.Size = resource.MustParse("1G")
				blockDevice.Status.Model = "prev Very good model-1241"
				blockDevice.Status.MachineID = "prevMachineId"

				expectedLabels = map[string]string{
					internal.MetadataNameLabelKey:                  blockDevice.ObjectMeta.Name,
					internal.HostNameLabelKey:                      candidate.NodeName,
					internal.BlockDeviceTypeLabelKey:               candidate.Type,
					internal.BlockDeviceFSTypeLabelKey:             candidate.FSType,
					internal.BlockDevicePVUUIDLabelKey:             candidate.PVUuid,
					internal.BlockDeviceVGUUIDLabelKey:             candidate.VGUuid,
					internal.BlockDevicePartUUIDLabelKey:           candidate.PartUUID,
					internal.BlockDeviceLVMVolumeGroupNameLabelKey: candidate.LVMVolumeGroupName,
					internal.BlockDeviceActualVGNameLabelKey:       candidate.ActualVGNameOnTheNode,
					internal.BlockDeviceWWNLabelKey:                candidate.Wwn,
					internal.BlockDeviceSerialLabelKey:             candidate.Serial,
					internal.BlockDeviceSizeLabelKey:               fmt.Sprint(candidate.Size.Value()),
					internal.BlockDeviceModelLabelKey:              "Very-good-model-1241",
					internal.BlockDeviceRotaLabelKey:               strconv.FormatBool(candidate.Rota),
					internal.BlockDeviceHotPlugLabelKey:            strconv.FormatBool(candidate.HotPlug),
					internal.BlockDeviceMachineIDLabelKey:          candidate.MachineID,
				}
			})

			JustBeforeEach(func() {
				Expect(candidate.HasBlockDeviceDiff(blockDevice)).Should(BeTrue())
				candidate.UpdateAPIBlockDevice(&blockDevice)
			})

			It("has labels as expected", func() {
				Expect(blockDevice.Labels).To(BeEquivalentTo(expectedLabels))
			})

			It("does not have block device difference", doesNotHaveBlockDeviceDifference)
			It("has correct field values", hasAllFieldsMatched)

			When("it has extra labels", func() {
				BeforeEach(func() {
					blockDevice.Labels = map[string]string{
						"some-custom-label1": "value1",
						"some-custom-label2": "value2",
					}

					expectedLabels["some-custom-label1"] = "value1"
					expectedLabels["some-custom-label2"] = "value2"
				})

				It("keep extra labels", func() {
					Expect(blockDevice.Labels).To(BeEquivalentTo(expectedLabels))
				})
			})
		})
	})

})
