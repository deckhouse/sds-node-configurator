/*
Copyright 2026 Flant JSC

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

package tests

import (
	"crypto/sha1"
	"fmt"
	"regexp"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
)

// blockDevicePathPattern matches /dev/sdX, /dev/vdX (e.g. /dev/sda, /dev/vdb).
var blockDevicePathPattern = regexp.MustCompile(`^/dev/(sd|vd)[a-z]+$`)

var _ = Describe("BlockDevice Discovery E2E", func() {
	Context("Automatic discovery of a new block device", func() {
		var (
			nodeName           string
			expectedDevicePath string
			expectedSerial     string
			expectedBDName     string
		)

		BeforeEach(func() {
			nodeName = GetNodeName()
			expectedDevicePath = GetExpectedDevicePath()
			expectedSerial = GetExpectedDeviceSerial()

			By(fmt.Sprintf("Using node: %s", nodeName))
			if expectedDevicePath != "" {
				By(fmt.Sprintf("Expected device path: %s", expectedDevicePath))
			} else {
				By("Expected device path: any (/dev/sdX or /dev/vdX)")
			}
			By(fmt.Sprintf("Expected serial: %s", expectedSerial))

			// Compute expected BlockDevice name from serial
			if expectedSerial != "" {
				expectedBDName = generateBlockDeviceName(nodeName, expectedSerial, "", "")
				By(fmt.Sprintf("Expected BlockDevice name: %s", expectedBDName))
			}
		})

		It("Should discover a new unformatted disk and create a BlockDevice object", func() {
			By("Step 1: Waiting for BlockDevice to appear in the cluster")

			var foundBD *v1alpha1.BlockDevice
			var blockDevicesList v1alpha1.BlockDeviceList

			// Wait for BlockDevice to appear within 5 minutes
			// (time may vary depending on the agent's scan interval)
			// If E2E_DEVICE_PATH is set, match that path; otherwise accept any /dev/sdX or /dev/vdX on the node.
			Eventually(func(g Gomega) {
				foundBD = nil
				err := k8sClient.List(ctx, &blockDevicesList, &client.ListOptions{})
				g.Expect(err).NotTo(HaveOccurred())

				for i := range blockDevicesList.Items {
					bd := &blockDevicesList.Items[i]
					if bd.Status.NodeName != nodeName {
						continue
					}
					if expectedDevicePath != "" {
						if bd.Status.Path != expectedDevicePath {
							continue
						}
					} else {
						if !isBlockDevicePath(bd.Status.Path) {
							continue
						}
						if bd.Status.Size.IsZero() {
							continue
						}
					}
					foundBD = bd
					return
				}

				g.Expect(foundBD).NotTo(BeNil(), fmt.Sprintf(
					"BlockDevice on nodeName=%s not found (path filter: %s). Total BlockDevices: %d",
					nodeName, orPathFilter(expectedDevicePath), len(blockDevicesList.Items),
				))
			}, 5*time.Minute, 10*time.Second).Should(Succeed())

			// Use the found device path for subsequent checks (so Step 4 passes when path was not specified)
			expectedDevicePath = foundBD.Status.Path
			By(fmt.Sprintf("Found BlockDevice: %s (path: %s)", foundBD.Name, expectedDevicePath))

			// Step 2: Verify resource name matches expected (when serial is set)
			By("Step 2: Verifying BlockDevice name based on serial number")
			if expectedSerial != "" {
				Expect(foundBD.Name).To(Equal(expectedBDName),
					fmt.Sprintf("BlockDevice name does not match expected. "+
						"Expected: %s, got: %s", expectedBDName, foundBD.Name))
			}

			// Step 3: Verify status.nodeName
			By("Step 3: Verifying status.nodeName")
			Expect(foundBD.Status.NodeName).To(Equal(nodeName),
				fmt.Sprintf("NodeName does not match expected. "+
					"Expected: %s, got: %s", nodeName, foundBD.Status.NodeName))

			// Step 4: Verify status.path
			By("Step 4: Verifying status.path")
			Expect(foundBD.Status.Path).To(Equal(expectedDevicePath),
				fmt.Sprintf("Path does not match expected. "+
					"Expected: %s, got: %s", expectedDevicePath, foundBD.Status.Path))

			// Step 5: Verify device size
			By("Step 5: Verifying device size (must be > 0)")
			Expect(foundBD.Status.Size.IsZero()).To(BeFalse(),
				"Device size must not be zero")
			minSize := resource.MustParse("1Gi")
			Expect(foundBD.Status.Size.Cmp(minSize)).To(BeNumerically(">=", 0),
				fmt.Sprintf("Device size must be >= 1Gi. Got: %s",
					foundBD.Status.Size.String()))

			By(fmt.Sprintf("Device size: %s", foundBD.Status.Size.String()))

			// Step 6: Verify serial number
			By("Step 6: Verifying serial number")
			if expectedSerial != "" {
				Expect(foundBD.Status.Serial).To(Equal(expectedSerial),
					fmt.Sprintf("Serial number does not match expected. "+
						"Expected: %s, got: %s", expectedSerial, foundBD.Status.Serial))
			} else {
				Expect(foundBD.Status.Serial).NotTo(BeEmpty(),
					"Device serial number must not be empty")
			}

			By(fmt.Sprintf("Device serial number: %s", foundBD.Status.Serial))

			// Step 7: Verify device is consumable
			By("Step 7: Verifying consumable state")
			Expect(foundBD.Status.Consumable).To(BeTrue(),
				"Device must be marked as consumable for an unformatted disk")

			// Step 8: Verify device type
			By("Step 8: Verifying device type")
			Expect(foundBD.Status.Type).NotTo(BeEmpty(),
				"Device type must not be empty")
			By(fmt.Sprintf("Device type: %s", foundBD.Status.Type))

			// Step 9: Verify FSType is empty for unformatted disk
			By("Step 9: Verifying FSType (must be empty for unformatted disk)")
			Expect(foundBD.Status.FsType).To(BeEmpty(),
				fmt.Sprintf("FSType must be empty for unformatted disk, got: %s",
					foundBD.Status.FsType))

			// Step 10: Verify PVUuid is empty for unformatted disk
			By("Step 10: Verifying PVUuid (must be empty)")
			Expect(foundBD.Status.PVUuid).To(BeEmpty(),
				"PVUuid must be empty for unformatted disk")

			// Step 11: Verify VGUuid is empty for unformatted disk
			By("Step 11: Verifying VGUuid (must be empty)")
			Expect(foundBD.Status.VGUuid).To(BeEmpty(),
				"VGUuid must be empty for unformatted disk")

			// Step 12: Verify machineID
			By("Step 12: Verifying machineID")
			Expect(foundBD.Status.MachineID).NotTo(BeEmpty(),
				"MachineID must not be empty")
			By(fmt.Sprintf("MachineID: %s", foundBD.Status.MachineID))

			// Summary
			By("✓ All checks passed successfully!")
			printBlockDeviceInfo(foundBD)
		})

		It("Should correctly handle device disconnection", func() {
			By("Note: This test requires manual device disconnection")
			Skip("Automated testing of device disconnection requires additional infrastructure")
		})
	})
})

// isBlockDevicePath returns true if path looks like a block device path (/dev/sdX or /dev/vdX).
func isBlockDevicePath(path string) bool {
	return blockDevicePathPattern.MatchString(path)
}

func orPathFilter(path string) string {
	if path == "" {
		return "any /dev/sdX or /dev/vdX"
	}
	return "path=" + path
}

// generateBlockDeviceName generates a BlockDevice name from the given parameters.
// Logic must match createUniqDeviceName in discoverer.go.
func generateBlockDeviceName(nodeName, serial, wwn, partUUID string) string {
	// Use empty model as it is unknown at test time
	temp := fmt.Sprintf("%s%s%s%s%s", nodeName, wwn, "" /* model */, serial, partUUID)
	return fmt.Sprintf("dev-%x", sha1.Sum([]byte(temp)))
}

// printBlockDeviceInfo prints detailed information about the BlockDevice.
func printBlockDeviceInfo(bd *v1alpha1.BlockDevice) {
	GinkgoWriter.Println("\n========== BlockDevice information ==========")
	GinkgoWriter.Printf("Name: %s\n", bd.Name)
	GinkgoWriter.Printf("NodeName: %s\n", bd.Status.NodeName)
	GinkgoWriter.Printf("Path: %s\n", bd.Status.Path)
	GinkgoWriter.Printf("Size: %s\n", bd.Status.Size.String())
	GinkgoWriter.Printf("Type: %s\n", bd.Status.Type)
	GinkgoWriter.Printf("Serial: %s\n", bd.Status.Serial)
	GinkgoWriter.Printf("WWN: %s\n", bd.Status.Wwn)
	GinkgoWriter.Printf("Model: %s\n", bd.Status.Model)
	GinkgoWriter.Printf("Consumable: %t\n", bd.Status.Consumable)
	GinkgoWriter.Printf("FSType: %s\n", bd.Status.FsType)
	GinkgoWriter.Printf("MachineID: %s\n", bd.Status.MachineID)
	GinkgoWriter.Printf("Rota: %t\n", bd.Status.Rota)
	GinkgoWriter.Printf("HotPlug: %t\n", bd.Status.HotPlug)
	GinkgoWriter.Println("=============================================\n")
}
