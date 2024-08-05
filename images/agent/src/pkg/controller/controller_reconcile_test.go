/*
Copyright 2023 Flant JSC

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

package controller_test

import (
	"agent/internal"
	"agent/pkg/controller"
	"agent/pkg/monitoring"
	"context"
	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Storage Controller", func() {

	var (
		ctx         = context.Background()
		testMetrics = monitoring.GetMetrics("")
		deviceName  = "/dev/sda"
		candidate   = internal.BlockDeviceCandidate{
			NodeName:              "test-node",
			Consumable:            true,
			PVUuid:                "123",
			VGUuid:                "123",
			LvmVolumeGroupName:    "testLvm",
			ActualVGNameOnTheNode: "testVG",
			Wwn:                   "WW12345678",
			Serial:                "test",
			Path:                  deviceName,
			Size:                  resource.Quantity{},
			Rota:                  false,
			Model:                 "",
			Name:                  "/dev/sda",
			HotPlug:               false,
			KName:                 "/dev/sda",
			PkName:                "/dev/sda14",
			Type:                  "disk",
			FSType:                "",
			MachineId:             "1234",
		}
	)

	cl := NewFakeClient()

	It("CreateAPIBlockDevice", func() {
		blockDevice, err := controller.CreateAPIBlockDevice(ctx, cl, testMetrics, candidate)
		Expect(err).NotTo(HaveOccurred())
		Expect(blockDevice.Status.NodeName).To(Equal(candidate.NodeName))
		Expect(blockDevice.Status.Consumable).To(Equal(candidate.Consumable))
		Expect(blockDevice.Status.PVUuid).To(Equal(candidate.PVUuid))
		Expect(blockDevice.Status.VGUuid).To(Equal(candidate.VGUuid))
		Expect(blockDevice.Status.LvmVolumeGroupName).To(Equal(candidate.LvmVolumeGroupName))
		Expect(blockDevice.Status.ActualVGNameOnTheNode).To(Equal(candidate.ActualVGNameOnTheNode))
		Expect(blockDevice.Status.Wwn).To(Equal(candidate.Wwn))
		Expect(blockDevice.Status.Serial).To(Equal(candidate.Serial))
		Expect(blockDevice.Status.Path).To(Equal(candidate.Path))
		Expect(blockDevice.Status.Size.Value()).To(Equal(candidate.Size.Value()))
		Expect(blockDevice.Status.Rota).To(Equal(candidate.Rota))
		Expect(blockDevice.Status.Model).To(Equal(candidate.Model))
		Expect(blockDevice.Status.Type).To(Equal(candidate.Type))
		Expect(blockDevice.Status.FsType).To(Equal(candidate.FSType))
		Expect(blockDevice.Status.MachineID).To(Equal(candidate.MachineId))
	})

	It("GetAPIBlockDevices", func() {
		listDevice, err := controller.GetAPIBlockDevices(ctx, cl, testMetrics)
		Expect(err).NotTo(HaveOccurred())
		Expect(listDevice).NotTo(BeNil())
		Expect(len(listDevice)).To(Equal(1))

		testBlockDevice := listDevice[deviceName]
		Expect(testBlockDevice).NotTo(BeNil())
		Expect(testBlockDevice.Status.NodeName).To(Equal(candidate.NodeName))
	})

	It("UpdateAPIBlockDevice", func() {
		newCandidate := internal.BlockDeviceCandidate{
			NodeName:              "test-node",
			Consumable:            false,
			PVUuid:                "123",
			VGUuid:                "123",
			LvmVolumeGroupName:    "updatedField",
			ActualVGNameOnTheNode: "testVG",
			Wwn:                   "WW12345678",
			Serial:                "test",
			Path:                  deviceName,
			Size:                  resource.Quantity{},
			Rota:                  false,
			Model:                 "",
			Name:                  "/dev/sda",
			HotPlug:               false,
			KName:                 "/dev/sda",
			PkName:                "/dev/sda14",
			Type:                  "disk",
			FSType:                "",
			MachineId:             "1234",
		}

		resources, err := controller.GetAPIBlockDevices(ctx, cl, testMetrics)
		Expect(err).NotTo(HaveOccurred())
		Expect(resources).NotTo(BeNil())
		Expect(len(resources)).To(Equal(1))

		oldResource := resources[deviceName]
		Expect(oldResource).NotTo(BeNil())
		Expect(oldResource.Status.NodeName).To(Equal(candidate.NodeName))

		err = controller.UpdateAPIBlockDevice(ctx, cl, testMetrics, oldResource, newCandidate)
		Expect(err).NotTo(HaveOccurred())

		resources, err = controller.GetAPIBlockDevices(ctx, cl, testMetrics)
		Expect(err).NotTo(HaveOccurred())
		Expect(resources).NotTo(BeNil())
		Expect(len(resources)).To(Equal(1))

		newResource := resources[deviceName]
		Expect(newResource).NotTo(BeNil())
		Expect(newResource.Status.NodeName).To(Equal(candidate.NodeName))
		Expect(newResource.Status.Consumable).To(BeFalse())
		Expect(newResource.Status.LvmVolumeGroupName).To(Equal("updatedField"))
	})

	It("DeleteAPIBlockDevice", func() {
		err := controller.DeleteAPIBlockDevice(ctx, cl, testMetrics, &v1alpha1.BlockDevice{
			ObjectMeta: metav1.ObjectMeta{
				Name: deviceName,
			},
		})
		Expect(err).NotTo(HaveOccurred())

		devices, err := controller.GetAPIBlockDevices(context.Background(), cl, testMetrics)
		for name := range devices {
			Expect(name).NotTo(Equal(deviceName))
		}
	})
})
