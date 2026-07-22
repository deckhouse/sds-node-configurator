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
	"context"
	"fmt"
	"os"
	"time"

	"github.com/deckhouse/sds-node-configurator/e2e/cfg"
	"github.com/deckhouse/sds-node-configurator/e2e/sdsclient"
	"github.com/deckhouse/sds-node-configurator/e2e/tests/utils/consts"
	"github.com/deckhouse/storage-e2e/pkg/e2e"
	"github.com/deckhouse/storage-e2e/pkg/kubernetes"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ = Describe("Block device stability with explicit lifecycle stages", Label("e2e-tests"), Ordered, func() {
	var (
		ctx       context.Context
		conf      *cfg.Config
		cl        *e2e.Cluster
		k8sClient client.Client

		targetNode         string
		diskName           string
		initialBlockDevice kubernetes.BlockDevice

		mpoGVR schema.GroupVersionResource
	)

	BeforeAll(func() {
		By("Preparing shared test context and Kubernetes clients")
		ctx = context.Background()
		conf = cfg.Load()
		Expect(conf).NotTo(BeNil())

		var clErr error
		cl, clErr = e2e.Connect(ctx, e2e.WithTestName("block-device-stable"))
		Expect(clErr).NotTo(HaveOccurred(), "failed to connect to cluster")
		DeferCleanup(func() {
			if err := cl.Close(context.Background()); err != nil {
				GinkgoWriter.Println("Error closing cluster: ", err)
			}
		})

		var k8sErr error
		k8sClient, k8sErr = sdsclient.New(cl.RESTConfig())
		Expect(k8sErr).NotTo(HaveOccurred(), "failed to build controller-runtime client")
		Expect(k8sClient).NotTo(BeNil())

		By("Selecting target node")
		nodeList, nlErr := cl.Clientset().CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		Expect(nlErr).NotTo(HaveOccurred())
		Expect(nodeList.Items).NotTo(BeEmpty())
		targetNode = nodeList.Items[0].Name
		Expect(targetNode).NotTo(BeEmpty(), "need a node for block device stability test")

		diskName = fmt.Sprintf("e2e-bd-stable-%d", time.Now().Unix())
		By("Creating and attaching a virtual disk to the target node: " + diskName)
		disk, createErr := cl.Disks().CreateDisk(ctx, e2e.DiskSpec{
			Name:         diskName,
			Size:         resource.MustParse("5Gi"),
			StorageClass: conf.TestCluster.StorageClass,
		})
		Expect(createErr).NotTo(HaveOccurred())
		Expect(disk).NotTo(BeNil())
		diskName = disk.Name

		Expect(cl.Disks().AttachDisk(ctx, targetNode, diskName)).To(Succeed())

		By("Preparing GVR")
		mpoGVR = schema.GroupVersionResource{
			Group:    "deckhouse.io",
			Version:  "v1alpha2",
			Resource: "modulepulloverrides",
		}

		DeferCleanup(func() {
			if diskName != "" {
				By("Cleaning up virtual disk")
				if detachErr := cl.Disks().DetachDisk(ctx, targetNode, diskName); detachErr != nil {
					GinkgoWriter.Println(detachErr)
				}
				if deleteErr := cl.Disks().DeleteDisk(ctx, diskName); deleteErr != nil {
					GinkgoWriter.Println(deleteErr)
				}
			}
		})
	})

	Context("with disk initially attached to the VM", func() {
		It("has exactly one consumable block device", func() {
			By("Getting consumable block devices on the target node")
			Eventually(func(g Gomega) {
				blockDevices, getBDErr := kubernetes.GetConsumableBlockDevicesByNode(ctx, cl.RESTConfig(), targetNode)
				g.Expect(getBDErr).NotTo(HaveOccurred())
				g.Expect(blockDevices).To(HaveLen(1))
				initialBlockDevice = blockDevices[0]
			}, 5*time.Minute, 2*time.Second).Should(Succeed())
		})

		When("disk is detached from the VM", func() {
			BeforeAll(func() {
				By("Detaching the virtual disk from the node")
				Expect(cl.Disks().DetachDisk(ctx, targetNode, diskName)).To(Succeed())
			})

			It("has zero consumable block devices", func() {
				Eventually(func(g Gomega) {
					blockDevices, getBDErr := kubernetes.GetConsumableBlockDevicesByNode(ctx, cl.RESTConfig(), targetNode)
					g.Expect(getBDErr).NotTo(HaveOccurred())
					g.Expect(blockDevices).To(BeEmpty())
				}, 5*time.Minute, 5*time.Second).Should(Succeed())
			})

			When("disk is reattached to the VM", func() {
				BeforeAll(func() {
					By("Reattaching the virtual disk to the node")
					Expect(cl.Disks().AttachDisk(ctx, targetNode, diskName)).To(Succeed())
				})

				It("has the same consumable block device as before detach", func() {
					blockDevices, getBDErr := kubernetes.GetConsumableBlockDevicesByNode(ctx, cl.RESTConfig(), targetNode)
					Expect(getBDErr).NotTo(HaveOccurred())
					Expect(blockDevices).To(HaveLen(1))
					Expect(blockDevices[0]).To(Equal(initialBlockDevice))
				})

				When("sds-node-configurator-agent pod is restarted on the node", func() {
					BeforeAll(func() {
						restartAt := time.Now()

						err := k8sClient.DeleteAllOf(
							ctx,
							&v1.Pod{},
							client.InNamespace(consts.SdsNodeConfiguratorAgentNamespace),
							client.MatchingLabels{"app": consts.SdsNodeConfiguratorAgentName},
							client.MatchingFields{"spec.nodeName": targetNode},
						)
						Expect(err).NotTo(HaveOccurred())

						Eventually(func(g Gomega) {
							By("Waiting for sds-node-configurator-agent pod to be recreated and become ready")
							var pods v1.PodList
							g.Expect(k8sClient.List(
								ctx,
								&pods,
								client.InNamespace(consts.SdsNodeConfiguratorAgentNamespace),
								client.MatchingLabels{"app": consts.SdsNodeConfiguratorAgentName},
								client.MatchingFields{"spec.nodeName": targetNode},
							)).To(Succeed())

							g.Expect(pods.Items).To(HaveLen(1))
							p := pods.Items[0]

							g.Expect(p.CreationTimestamp.Time.After(restartAt)).To(BeTrue(), "expected a new pod after restart")
							g.Expect(p.DeletionTimestamp).To(BeNil())
							g.Expect(p.Status.Phase).To(Equal(v1.PodRunning))
						}, 5*time.Minute, 5*time.Second).Should(Succeed())
					})

					It("keeps block device state stable after agent restart", func() {
						By("Checking block device visibility on the target node after agent restart")
						blockDevices, getBDErr := kubernetes.GetConsumableBlockDevicesByNode(ctx, cl.RESTConfig(), targetNode)
						Expect(getBDErr).NotTo(HaveOccurred())
						Expect(blockDevices).To(HaveLen(1))
						Expect(blockDevices[0]).To(Equal(initialBlockDevice))
					})
				})

				When("running in pull_request and agent imageTag is switched to main", func() {
					var (
						originalImageTag string
					)

					BeforeAll(func() {
						if os.Getenv("GITHUB_EVENT_NAME") != "pull_request" {
							Skip("PR-only scenario")
						}

						mpo, err := cl.Dynamic().Resource(mpoGVR).Get(ctx, consts.SdsNodeConfiguratorAgentName, metav1.GetOptions{})
						Expect(err).NotTo(HaveOccurred())

						currentImageTag, found, err := unstructured.NestedString(mpo.Object, "spec", "imageTag")
						Expect(err).NotTo(HaveOccurred())
						Expect(found).To(BeTrue(), "expected spec.imageTag to be set")

						originalImageTag = currentImageTag

						err = unstructured.SetNestedField(mpo.Object, conf.ModulesImageTag, "spec", "imageTag")
						Expect(err).NotTo(HaveOccurred())

						_, err = cl.Dynamic().Resource(mpoGVR).Update(ctx, mpo, metav1.UpdateOptions{})
						Expect(err).NotTo(HaveOccurred())

						Expect(kubernetes.WaitForModuleReady(
							ctx,
							cl.RESTConfig(),
							consts.SdsNodeConfiguratorAgentName,
							10*time.Minute,
						)).To(Succeed())
					})

					AfterAll(func() {
						mpo, err := cl.Dynamic().Resource(mpoGVR).Get(ctx, consts.SdsNodeConfiguratorAgentName, metav1.GetOptions{})
						Expect(err).NotTo(HaveOccurred())

						err = unstructured.SetNestedField(mpo.Object, originalImageTag, "spec", "imageTag")
						Expect(err).NotTo(HaveOccurred())

						_, err = cl.Dynamic().Resource(mpoGVR).Update(ctx, mpo, metav1.UpdateOptions{})
						Expect(err).NotTo(HaveOccurred())

						Expect(kubernetes.WaitForModuleReady(
							ctx,
							cl.RESTConfig(),
							consts.SdsNodeConfiguratorAgentName,
							10*time.Minute,
						)).To(Succeed())
					})

					It("has the same consumable block device after agent imageTag switch", func() {
						By("Checking that consumable block device remains unchanged after imageTag switch")
						Expect(initialBlockDevice.Name).NotTo(BeEmpty())
						Eventually(func(g Gomega) {
							blockDevices, getBDErr := kubernetes.GetConsumableBlockDevicesByNode(ctx, cl.RESTConfig(), targetNode)
							g.Expect(getBDErr).NotTo(HaveOccurred())
							g.Expect(blockDevices).To(HaveLen(1))
							g.Expect(blockDevices[0].Name).To(Equal(initialBlockDevice.Name))
						}, 5*time.Minute, 5*time.Second).Should(Succeed())
					})
				})
			})
		})
	})
})
