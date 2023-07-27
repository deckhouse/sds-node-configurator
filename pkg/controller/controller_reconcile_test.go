package controller_test

import (
	"context"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"storage-configurator/pkg/controller"
)

var _ = Describe("Storage Controller", func() {

	It("Create Device", func() {
		ctx := context.Background()
		nodeName := "test-node"
		deviceName := "dev-12345"
		nodeUID := "2934650923629083"

		var can = controller.Candidate{
			NodeName:   nodeName,
			ID:         "WW12345678",
			Path:       "/dev/sda",
			Size:       "4G",
			Model:      "",
			Name:       "/dev/sda",
			MountPoint: "",
			HotPlug:    false,
			KName:      "/dev/sda",
			PkName:     "/dev/sda14",
			FSType:     "",
		}

		cl := NewFakeClient()

		//todo Create node?
		//nodeUID, err := kubutils.GetNodeUID(ctx, cl, nodeName)
		//Expect(err).NotTo(HaveOccurred())

		err := controller.CreateBlockDeviceObject(ctx, cl, can, nodeName, deviceName, nodeUID)
		Expect(err).NotTo(HaveOccurred())

		listDevice, err := controller.GetListBlockDevices(ctx, cl)
		Expect(err).NotTo(HaveOccurred())
		Expect(listDevice).NotTo(BeNil())
		for i := range listDevice {
			Expect(deviceName).To(Equal(i))
		}

		err = controller.DeleteBlockDeviceObject(ctx, cl, deviceName)
		Expect(err).NotTo(HaveOccurred())

		listDevice, err = controller.GetListBlockDevices(ctx, cl)
		Expect(err).NotTo(HaveOccurred())
		Expect(listDevice).NotTo(BeNil())

		for i := range listDevice {
			Expect(deviceName).ToNot(Equal(i))
		}
	})
})
