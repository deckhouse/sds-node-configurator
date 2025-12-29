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
	_ "embed"
	"fmt"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
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

type DiscoverCreatedVars struct {
	sdsCache   *cache.Cache
	discoverer *bd.Discoverer
}

var (
	MachineID string
	NodeName  string
	k8client  client.Client
)

func withDiscovererCreated(foo func(vars *DiscoverCreatedVars)) {
	var (
		vars    DiscoverCreatedVars
		log     logger.Logger
		metrics *monitoring.Metrics
		config  bd.DiscovererConfig
	)

	BeforeEach(func() {
		MachineID = "testMachineID"
		NodeName = "testNodeName"
		k8client = nil
	})

	JustBeforeEach(func() {
		config = bd.DiscovererConfig{
			MachineID: MachineID,
			NodeName:  NodeName,
		}
		metrics = monitoring.GetMetrics("")
		log = logger.NewLoggerWrap(GinkgoLogr)
		vars.sdsCache = cache.New()
		k8client = test_utils.NewFakeClient()
		vars.discoverer = bd.NewDiscoverer(k8client, log, metrics, vars.sdsCache, config)
	})

	foo(&vars)
}

func withDevicesCreated(devices []v1alpha1.BlockDevice, foo func()) {
	JustBeforeEach(func(ctx SpecContext) {
		for _, obj := range devices {
			Expect(k8client.Create(ctx, &obj)).ShouldNot(HaveOccurred())
		}
	})

	foo()
}

func withInternalDevicesCacheUpdated(internalDevices []internal.Device, sdsCache **cache.Cache, foo func()) {
	JustBeforeEach(func() {
		(*sdsCache).StoreDevices(internalDevices, bytes.Buffer{})
	})

	foo()
}

func withBlockDeviceFiltersCreated(selectors []metav1.LabelSelector, foo func()) {
	JustBeforeEach(func(ctx SpecContext) {
		for i, selector := range selectors {
			Expect(k8client.Create(ctx, &v1alpha1.BlockDeviceFilter{
				ObjectMeta: metav1.ObjectMeta{
					Name: fmt.Sprintf("block-device-filter-%v", i),
				},
				Spec: v1alpha1.BlockDeviceFilterSpec{
					BlockDeviceSelector: &selector,
				},
			})).ShouldNot(HaveOccurred())
		}
	})

	foo()
}

func withBlockDeviceFiltersReplaced(selectors []metav1.LabelSelector, foo func()) {
	JustBeforeEach(func(ctx SpecContext) {
		Expect(k8client.DeleteAllOf(ctx, &v1alpha1.BlockDeviceFilter{})).ShouldNot(HaveOccurred())
	})

	withBlockDeviceFiltersCreated(selectors, foo)
}

func splitBlockDevicesByNode(list []v1alpha1.BlockDevice) map[string][]v1alpha1.BlockDevice {
	result := make(map[string][]v1alpha1.BlockDevice)
	for _, item := range list {
		result[item.Status.NodeName] = append(result[item.Status.NodeName], item)
	}
	return result
}

func splitBlockDevicesByMachineID(list []v1alpha1.BlockDevice) map[string][]v1alpha1.BlockDevice {
	result := make(map[string][]v1alpha1.BlockDevice)
	for _, item := range list {
		result[item.Status.MachineID] = append(result[item.Status.MachineID], item)
	}
	return result
}

func blockDevicesByName(thisMachineDevices []v1alpha1.BlockDevice) map[string]v1alpha1.BlockDevice {
	mapAPIBlockDevicesByName := make(map[string]v1alpha1.BlockDevice, len(thisMachineDevices))
	for _, apiBlockDevice := range thisMachineDevices {
		name := apiBlockDevice.Status.Path
		Expect(mapAPIBlockDevicesByName).ShouldNot(ContainElement(name))
		mapAPIBlockDevicesByName[name] = apiBlockDevice
	}
	return mapAPIBlockDevicesByName
}

func expectInternalDevicesMatchDevices(internalDevices []internal.Device, thisMachineDevices []v1alpha1.BlockDevice) {
	mapAPIBlockDevicesByName := blockDevicesByName(thisMachineDevices)
	for _, internalDevice := range internalDevices {
		apiBlockDevice, exists := mapAPIBlockDevicesByName[internalDevice.Name]
		Expect(exists).Should(BeTrue())

		Expect(apiBlockDevice.Status.MachineID).To(Equal(MachineID))
		Expect(apiBlockDevice.Status.NodeName).To(Equal(NodeName))
		Expect(apiBlockDevice.Status.Consumable).To(BeTrue())
		// These two will be copied from parent device if empty
		if internalDevice.Wwn != "" {
			Expect(apiBlockDevice.Status.Wwn).To(Equal(internalDevice.Wwn))
		}
		if internalDevice.Serial != "" {
			Expect(apiBlockDevice.Status.Serial).To(Equal(internalDevice.Serial))
		}
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

func whenDiscovered(vars *DiscoverCreatedVars, foo func(discoverResult *controller.Result, discoverError *error)) {
	When("discovered", func() {
		var (
			discoverResult controller.Result
			discoverError  error
		)

		JustBeforeEach(func(ctx SpecContext) {
			By("Getting device list before discover")
			var deviceListBeforeDiscover v1alpha1.BlockDeviceList
			Expect(k8client.List(ctx, &deviceListBeforeDiscover)).ShouldNot(HaveOccurred())

			By("Discover")
			discoverResult, discoverError = vars.discoverer.Discover(ctx)

			By("Getting device list after discover")
			var deviceListAfterDiscover v1alpha1.BlockDeviceList
			Expect(k8client.List(ctx, &deviceListAfterDiscover)).ShouldNot(HaveOccurred())

			By("Checking if other machine devices are not changed")
			expectOtherMachineDevicesAreNotChanged(
				deviceListBeforeDiscover.Items,
				splitBlockDevicesByMachineID(deviceListAfterDiscover.Items),
			)
		})

		foo(&discoverResult, &discoverError)
	})
}

func whenSuccessfullyDiscovered(vars *DiscoverCreatedVars, foo func()) {
	whenDiscovered(vars, func(discoverResult *controller.Result, discoverError *error) {
		JustBeforeEach(func() {
			Expect(*discoverError).ToNot(HaveOccurred())
			Expect(discoverResult.RequeueAfter).Should(BeZero())
		})

		foo()
	})
}

func expectOtherMachineDevicesAreNotChanged(before []v1alpha1.BlockDevice, afterByMachineID map[string][]v1alpha1.BlockDevice) {
	initialByMachineID := splitBlockDevicesByMachineID(before)
	for machineID := range afterByMachineID {
		if machineID == MachineID {
			continue
		}
		devices := afterByMachineID[machineID]
		initialDevices := initialByMachineID[machineID]
		Expect(devices).To(Equal(initialDevices))
	}
}

func expectAPIDevicesMatchedToInternalDevices(ctx context.Context, internalDevices []internal.Device) {
	By("Getting BlockDevice list")
	var list v1alpha1.BlockDeviceList
	Expect(k8client.List(ctx, &list)).ShouldNot(HaveOccurred())

	By("Splitting block devices by MachineID")
	byMachineID := splitBlockDevicesByMachineID(list.Items)
	By("Splitting block devices by Node")

	By("Making sure we have all the device discovered")
	thisMachineDevices := byMachineID[MachineID]
	Expect(thisMachineDevices).Should(HaveLen(len(internalDevices)))

	Expect(splitBlockDevicesByNode(list.Items)[NodeName]).Should(BeEquivalentTo(thisMachineDevices),
		"Devices by machineId and host should be the same")
	expectInternalDevicesMatchDevices(internalDevices, thisMachineDevices)
}

func TestController(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Discoverer Suite")
}
