package controller_test

import (
	"context"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/api/policy/v1beta1"
	"storage-configurator/pkg/controller"
)

var _ = Describe("Storage Controller", func() {

	It("Create Device", func() {
		var (
			ctx        = context.Background()
			deviceName = "/dev/sda"
			candidate  = controller.Candidate{
				NodeName:              "test-node",
				Consumable:            true,
				PVUuid:                "123",
				VGUuid:                "123",
				LvmVolumeGroupName:    "testLvm",
				ActualVGnameOnTheNode: "testVG",
				Wwn:                   "WW12345678",
				Serial:                "test",
				Path:                  deviceName,
				Size:                  "1M",
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

		deviceStatus, err := controller.CreateAPIBlockDevice(ctx, cl, candidate)
		Expect(err).NotTo(HaveOccurred())
		Expect(deviceStatus.NodeName).To(Equal("test-node"))
		Expect(deviceStatus.Consumable).To(Equal(true))
		Expect(deviceStatus.PVUuid).To(Equal("123"))
		Expect(deviceStatus.VGUuid).To(Equal("123"))
		Expect(deviceStatus.LvmVolumeGroupName).To(Equal("testLvm"))
		Expect(deviceStatus.ActualVGnameOnTheNode).To(Equal("testVG"))
		Expect(deviceStatus.Wwn).To(Equal("WW12345678"))
		Expect(deviceStatus.Serial).To(Equal("test"))
		Expect(deviceStatus.Path).To(Equal("/dev/sda"))
		Expect(deviceStatus.Size).To(Equal("1M"))
		Expect(deviceStatus.Rota).To(Equal(false))
		Expect(deviceStatus.Model).To(Equal(""))
		Expect(deviceStatus.Type).To(Equal("disk"))
		Expect(deviceStatus.FsType).To(Equal(v1beta1.FSType("")))
		Expect(deviceStatus.MachineID).To(Equal("1234"))

		listDevice, err := controller.GetAPIBlockDevices(ctx, cl)
		Expect(err).NotTo(HaveOccurred())
		Expect(listDevice).NotTo(BeNil())
		for i := range listDevice {
			Expect(deviceName).To(Equal(i))
		}

		err = controller.DeleteBlockDeviceObject(ctx, cl, deviceName)
		Expect(err).NotTo(HaveOccurred())

		listDevice, err = controller.GetAPIBlockDevices(ctx, cl)
		Expect(err).NotTo(HaveOccurred())
		Expect(listDevice).NotTo(BeNil())

		for i := range listDevice {
			Expect(deviceName).ToNot(Equal(i))
		}
	})
})
