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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/types"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/utils"
)

//go:embed testdata/case1_lsblk_output.json
var case1LsblkOutput []byte

//go:embed testdata/case1_bd_right_naming.json
var case1BdRightNaming []byte

// Real life use case
var _ = Describe("Regression1", func() {
	withDiscovererCreated(func(vars *DiscoverCreatedVars) {
		internalDevices, err := utils.NewCommands().UnmarshalDevices(case1LsblkOutput)
		Expect(err).ToNot(HaveOccurred())

		var expectedDevice v1alpha1.BlockDevice
		err = json.Unmarshal(case1BdRightNaming, &expectedDevice)
		Expect(err).ShouldNot(HaveOccurred())

		// TODO: Test if LVM data the same. Currently we don't use any LVM data only lsblk
		expectedDevice.Status.PVUuid = ""
		expectedDevice.Status.VGUuid = ""
		expectedDevice.Status.LVMVolumeGroupName = ""
		expectedDevice.Status.ActualVGNameOnTheNode = ""

		BeforeEach(func() {
			MachineID = expectedDevice.Status.MachineID
			NodeName = expectedDevice.Status.NodeName
		})

		withInternalDevicesCacheUpdated(internalDevices, &vars.sdsCache, func() {
			whenSuccessfullyDiscovered(vars, func() {
				It("device count matched", func(ctx SpecContext) {
					var list v1alpha1.BlockDeviceList
					err := k8client.List(ctx, &list)
					Expect(err).ShouldNot(HaveOccurred())
					Expect(list.Items).To(HaveLen(2))
				})

				It("device name and spec matched", func(ctx SpecContext) {
					var device v1alpha1.BlockDevice
					err = k8client.Get(ctx, types.NamespacedName{Name: expectedDevice.Name, Namespace: expectedDevice.Namespace}, &device)
					Expect(err).ShouldNot(HaveOccurred(), "fix code, not test! Names should stay the same across the versions")
					Expect(device.Status).Should(BeEquivalentTo(expectedDevice.Status))
				})
			})
		})
	})
})
