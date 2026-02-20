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
	"fmt"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
)

	var _ = Describe("BlockDevice Discovery E2E", func() {
	Context("Discovery of a manually added block device", func() {
		var (
			nodeName           string
			expectedDevicePath string
		)

		BeforeEach(func() {
			nodeName = GetNodeName()
			expectedDevicePath = GetExpectedDevicePath()

			if nodeName != "" {
				By(fmt.Sprintf("Filter by node: %s", nodeName))
			} else {
				By("Filter by node: any (node name not required)")
			}
			if expectedDevicePath != "" {
				By(fmt.Sprintf("Expected device path: %s", expectedDevicePath))
			} else {
				By("Expected device path: any block device (path not filtered)")
			}
		})

		It("Should discover a new unformatted disk and create a BlockDevice object", func() {
			By("Expected result: object exists; status.nodeName = Kubernetes node name (kubectl get nodes); status.path correct; size > 0; state Ready (consumable); no errors in conditions")
			By("Step 1: Manually add a new unformatted block device to a node (e.g. attach a volume). The test will then wait for a new BlockDevice to appear.")

			var foundBD *v1alpha1.BlockDevice
			var blockDevicesList v1alpha1.BlockDeviceList

			// Snapshot existing BlockDevices — we will wait for one that is NOT in this set (i.e. newly added).
			err := k8sClient.List(ctx, &blockDevicesList, &client.ListOptions{})
			Expect(err).NotTo(HaveOccurred())
			initialNames := make(map[string]struct{}, len(blockDevicesList.Items))
			for i := range blockDevicesList.Items {
				initialNames[blockDevicesList.Items[i].Name] = struct{}{}
			}
			diag := formatBlockDevicesHint(blockDevicesList.Items, nodeName)
			By(fmt.Sprintf("BlockDevices in cluster before wait: %d. %s", len(blockDevicesList.Items), diag))

			By("Step 2: Waiting for a new BlockDevice to appear in the cluster (up to 5 minutes)")

			// Wait for a BlockDevice that was not present at the start (newly discovered after manual add).
			// If E2E_NODE_NAME is set, only consider that node; if E2E_DEVICE_PATH is set, match that path.
			Eventually(func(g Gomega) {
				foundBD = nil
				err := k8sClient.List(ctx, &blockDevicesList, &client.ListOptions{})
				g.Expect(err).NotTo(HaveOccurred())

				for i := range blockDevicesList.Items {
					bd := &blockDevicesList.Items[i]
					if _, existed := initialNames[bd.Name]; existed {
						continue
					}
					if nodeName != "" && bd.Status.NodeName != nodeName {
						continue
					}
					if expectedDevicePath != "" {
						if bd.Status.Path != expectedDevicePath {
							continue
						}
					} else {
						if bd.Status.Size.IsZero() {
							continue
						}
						if bd.Status.Path == "" || !strings.HasPrefix(bd.Status.Path, "/dev/") {
							continue
						}
					}
					foundBD = bd
					return
				}

				hint := formatBlockDevicesHint(blockDevicesList.Items, nodeName)
				g.Expect(foundBD).NotTo(BeNil(), fmt.Sprintf(
					"No new BlockDevice appeared (node filter: %s, path filter: %s). Total BlockDevices: %d. %s",
					orNodeFilter(nodeName), orPathFilter(expectedDevicePath), len(blockDevicesList.Items), hint,
				))
			}, 5*time.Minute, 10*time.Second).Should(Succeed())

			nodeName = foundBD.Status.NodeName
			expectedDevicePath = foundBD.Status.Path
			By(fmt.Sprintf("Found BlockDevice: %s (node: %s, path: %s)", foundBD.Name, nodeName, expectedDevicePath))

			// Step 3: Verify status.nodeName is a Kubernetes node name (exists in cluster, e.g. kubectl get nodes)
			By("Step 3: Verifying status.nodeName matches a Kubernetes node name")
			var nodeList corev1.NodeList
			err = k8sClient.List(ctx, &nodeList, &client.ListOptions{})
			Expect(err).NotTo(HaveOccurred())
			nodeNames := make(map[string]struct{}, len(nodeList.Items))
			for i := range nodeList.Items {
				nodeNames[nodeList.Items[i].Name] = struct{}{}
			}
			Expect(nodeNames).To(HaveKey(nodeName),
				fmt.Sprintf("status.nodeName=%q is not a node in the cluster (kubectl get nodes). Cluster nodes: %v", nodeName, keysOf(nodeNames)))

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

			// Step 7: Verify Ready state (consumable for unformatted disk)
			By("Step 7: Verifying Ready state (consumable)")
			Expect(foundBD.Status.Consumable).To(BeTrue(),
				"Device must be marked as consumable (Ready) for an unformatted disk")

			// Step 8: Verify conditions have no errors (BlockDevice has no conditions in API; when added, ensure none are error-type)
			By("Step 8: Verifying conditions (no errors)")
			// BlockDeviceStatus has no Conditions field in current API. When conditions are added, check that none have status=False with type indicating error.
			By("BlockDevice API has no conditions field — nothing to check; when present, test should assert no error conditions")

			// Step 9: Verify device type
			By("Step 9: Verifying device type")
			Expect(foundBD.Status.Type).NotTo(BeEmpty(),
				"Device type must not be empty")
			By(fmt.Sprintf("Device type: %s", foundBD.Status.Type))

			// Step 10: Verify FSType is empty for unformatted disk
			By("Step 10: Verifying FSType (must be empty for unformatted disk)")
			Expect(foundBD.Status.FsType).To(BeEmpty(),
				fmt.Sprintf("FSType must be empty for unformatted disk, got: %s",
					foundBD.Status.FsType))

			// Step 11: Verify PVUuid is empty for unformatted disk
			By("Step 11: Verifying PVUuid (must be empty)")
			Expect(foundBD.Status.PVUuid).To(BeEmpty(),
				"PVUuid must be empty for unformatted disk")

			// Step 12: Verify VGUuid is empty for unformatted disk
			By("Step 12: Verifying VGUuid (must be empty)")
			Expect(foundBD.Status.VGUuid).To(BeEmpty(),
				"VGUuid must be empty for unformatted disk")

			// Step 13: Verify machineID
			By("Step 13: Verifying machineID")
			Expect(foundBD.Status.MachineID).NotTo(BeEmpty(),
				"MachineID must not be empty")
			By(fmt.Sprintf("MachineID: %s", foundBD.Status.MachineID))

			// Summary
			By("✓ Expected result verified: object exists; status.nodeName = K8s node name; status.path correct; size > 0; Ready (consumable); conditions (none in API)")
			printBlockDeviceInfo(foundBD)
		})

		It("Should correctly handle device disconnection", func() {
			By("Note: This test requires manual device disconnection")
			// Automated testing of device disconnection would require additional infrastructure (e.g. detach disk from node).
			// Placeholder: verify that BlockDevices list is accessible; full disconnection scenario is manual.
			var list v1alpha1.BlockDeviceList
			err := k8sClient.List(ctx, &list, &client.ListOptions{})
			Expect(err).NotTo(HaveOccurred())
			By(fmt.Sprintf("BlockDevices in cluster: %d (disconnection scenario not automated)", len(list.Items)))
		})
	})
})

func orPathFilter(path string) string {
	if path == "" {
		return "any"
	}
	return "path=" + path
}

func orNodeFilter(node string) string {
	if node == "" {
		return "any"
	}
	return "node=" + node
}

// formatBlockDevicesHint returns a short summary of existing BlockDevices for error messages.
func formatBlockDevicesHint(items []v1alpha1.BlockDevice, expectedNode string) string {
	if len(items) == 0 {
		return "No BlockDevices in cluster."
	}
	var lines []string
	nodesSeen := make(map[string]bool)
	for _, bd := range items {
		n := bd.Status.NodeName
		if n == "" {
			n = "<no nodeName>"
		}
		nodesSeen[n] = true
		path := bd.Status.Path
		if path == "" {
			path = "<no path>"
		}
		size := bd.Status.Size.String()
		lines = append(lines, fmt.Sprintf("%s: nodeName=%s path=%s size=%s", bd.Name, n, path, size))
	}
	hint := "Existing BlockDevices: " + strings.Join(lines, "; ")
	if expectedNode != "" && !nodesSeen[expectedNode] {
		var nodes []string
		for n := range nodesSeen {
			nodes = append(nodes, n)
		}
		hint += ". Expected nodeName=" + expectedNode + " but only found nodes: " + strings.Join(nodes, ", ")
	}
	return hint
}

func keysOf(m map[string]struct{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
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
