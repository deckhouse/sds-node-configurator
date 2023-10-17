package controller_test

import (
	"context"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/api/policy/v1beta1"
	"storage-configurator/internal"
	"storage-configurator/pkg/controller"
)

var _ = Describe("Storage Controller", func() {

	var (
		ctx        = context.Background()
		deviceName = "/dev/sda"
		candidate  = internal.Candidate{
			NodeName:              "test-node",
			Consumable:            true,
			PVUuid:                "123",
			VGUuid:                "123",
			LvmVolumeGroupName:    "testLvm",
			ActualVGNameOnTheNode: "testVG",
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

	It("CreateAPIBlockDevice", func() {
		deviceStatus, err := controller.CreateAPIBlockDevice(ctx, cl, candidate)
		Expect(err).NotTo(HaveOccurred())
		Expect(deviceStatus.NodeName).To(Equal(candidate.NodeName))
		Expect(deviceStatus.Consumable).To(Equal(candidate.Consumable))
		Expect(deviceStatus.PVUuid).To(Equal(candidate.PVUuid))
		Expect(deviceStatus.VGUuid).To(Equal(candidate.VGUuid))
		Expect(deviceStatus.LvmVolumeGroupName).To(Equal(candidate.LvmVolumeGroupName))
		Expect(deviceStatus.ActualVGNameOnTheNode).To(Equal(candidate.ActualVGNameOnTheNode))
		Expect(deviceStatus.Wwn).To(Equal(candidate.Wwn))
		Expect(deviceStatus.Serial).To(Equal(candidate.Serial))
		Expect(deviceStatus.Path).To(Equal(candidate.Path))
		Expect(deviceStatus.Size).To(Equal(candidate.Size))
		Expect(deviceStatus.Rota).To(Equal(candidate.Rota))
		Expect(deviceStatus.Model).To(Equal(candidate.Model))
		Expect(deviceStatus.Type).To(Equal(candidate.Type))
		Expect(deviceStatus.FsType).To(Equal(v1beta1.FSType(candidate.FSType)))
		Expect(deviceStatus.MachineID).To(Equal(candidate.MachineId))
	})

	It("GetAPIBlockDevices", func() {
		listDevice, err := controller.GetAPIBlockDevices(ctx, cl)
		Expect(err).NotTo(HaveOccurred())
		Expect(listDevice).NotTo(BeNil())
		Expect(len(listDevice)).To(Equal(1))

		apiBd := listDevice[deviceName]
		Expect(apiBd).NotTo(BeNil())
		Expect(apiBd.NodeName).To(Equal(candidate.NodeName))
	})

	It("DeleteAPIBlockDevice", func() {
		err := controller.DeleteAPIBlockDevice(ctx, cl, deviceName)
		Expect(err).NotTo(HaveOccurred())

		devices, err := controller.GetAPIBlockDevices(context.Background(), cl)
		for name, _ := range devices {
			Expect(name).NotTo(Equal(deviceName))
		}
	})
})
