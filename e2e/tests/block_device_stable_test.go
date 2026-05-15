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
	"os"
	"slices"
	"time"

	"github.com/deckhouse/sds-node-configurator/e2e/cfg"
	"github.com/deckhouse/sds-node-configurator/e2e/tests/utils/consts"
	"github.com/deckhouse/storage-e2e/pkg/cluster"
	"github.com/deckhouse/storage-e2e/pkg/kubernetes"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ = Describe("Block device stability with explicit lifecycle stages", Ordered, func() {
	var (
		ctx       context.Context
		res       *cluster.TestClusterResources
		conf      *cfg.Config
		k8sClient client.Client

		targetVM                    string
		initialBlockDevice          kubernetes.BlockDevice
		virtualDiskAttachmentResult *kubernetes.VirtualDiskAttachmentResult

		mpoGVR schema.GroupVersionResource
		dyn    dynamic.Interface
	)

	BeforeAll(func() {
		By("Preparing shared test context and Kubernetes clients")
		ctx = context.Background()
		res = e2eNestedTestClusterOrNil()
		Expect(res).NotTo(BeNil())
		conf = cfg.Load()
		Expect(conf).NotTo(BeNil())
		ensureE2EK8sClient(res, &k8sClient, ctx)
		Expect(k8sClient).NotTo(BeNil())

		By("Listing virtual machines to select target VM")
		vms, listVmErr := kubernetes.ListVirtualMachineNames(ctx, res.BaseKubeconfig, conf.TestCluster.Namespace)
		Expect(listVmErr).NotTo(HaveOccurred())
		Expect(vms).NotTo(BeEmpty())
		slices.Sort(vms)
		targetVM = vms[0]

		By("Attaching a virtual disk to the target VM")
		attachResult, attachErr := kubernetes.AttachVirtualDiskToVM(ctx, res.BaseKubeconfig,
			kubernetes.VirtualDiskAttachmentConfig{
				VMName:           targetVM,
				Namespace:        conf.TestCluster.Namespace,
				DiskName:         "block-device-stable-readable",
				DiskSize:         "5Gi",
				StorageClassName: conf.TestCluster.StorageClass,
			})
		Expect(attachErr).NotTo(HaveOccurred())
		Expect(attachResult).NotTo(BeNil())

		By("Waiting for virtual disk attachment to become ready")
		attachWaitErr := kubernetes.WaitForVirtualDiskAttached(ctx, res.BaseKubeconfig, conf.TestCluster.Namespace,
			attachResult.AttachmentName, 5*time.Second)
		Expect(attachWaitErr).NotTo(HaveOccurred())

		virtualDiskAttachmentResult = attachResult

		By("Preparing GVR and K8s dynamic clients")
		mpoGVR = schema.GroupVersionResource{
			Group:    "deckhouse.io",
			Version:  "v1alpha2",
			Resource: "modulepulloverrides",
		}
		var dynErr error
		dyn, dynErr = kubernetes.NewDynamicClientWithRetry(ctx, res.Kubeconfig)
		Expect(dynErr).NotTo(HaveOccurred())
	})

	AfterAll(func() {
		if virtualDiskAttachmentResult == nil {
			return
		}

		By("Cleaning up virtual disk attachment and virtual disk")
		deleteVDErr := kubernetes.DetachAndDeleteVirtualDisk(ctx, res.BaseKubeconfig, conf.TestCluster.Namespace,
			virtualDiskAttachmentResult.AttachmentName, virtualDiskAttachmentResult.DiskName)
		if deleteVDErr != nil {
			GinkgoWriter.Println(deleteVDErr)
		}
	})

	Context("with disk initially attached to the VM", func() {
		It("has exactly one consumable block device", func() {
			By("Getting consumable block devices on the target node")
			Eventually(func(g Gomega) {
				blockDevices, getBDErr := kubernetes.GetConsumableBlockDevicesByNode(ctx, res.Kubeconfig, targetVM)
				g.Expect(getBDErr).NotTo(HaveOccurred())
				g.Expect(blockDevices).To(HaveLen(1))
				initialBlockDevice = blockDevices[0]
			}, 5*time.Minute, 2*time.Second).Should(Succeed())
		})

		When("disk is detached from the VM", func() {
			BeforeAll(func() {
				By("Detaching the virtual disk from the VM")
				detachErr := kubernetes.DetachAndDeleteVirtualDisk(ctx, res.BaseKubeconfig, conf.TestCluster.Namespace,
					virtualDiskAttachmentResult.AttachmentName, "")
				Expect(detachErr).NotTo(HaveOccurred())
			})

			It("has zero consumable block devices", func() {
				Eventually(func(g Gomega) {
					blockDevices, getBDErr := kubernetes.GetConsumableBlockDevicesByNode(ctx, res.Kubeconfig, targetVM)
					g.Expect(getBDErr).NotTo(HaveOccurred())
					g.Expect(blockDevices).To(BeEmpty())
				}, 5*time.Minute, 5*time.Second).Should(Succeed())
			})

			When("disk is reattached to the VM", func() {
				BeforeAll(func() {
					By("Reattaching the virtual disk to the VM")
					reattachResult, reattachErr := kubernetes.ReattachVirtualDiskToVM(ctx, res.BaseKubeconfig,
						kubernetes.VirtualDiskReattachmentConfig{
							AttachmentName: virtualDiskAttachmentResult.AttachmentName,
							VMName:         targetVM,
							Namespace:      conf.TestCluster.Namespace,
							DiskName:       virtualDiskAttachmentResult.DiskName,
						})
					Expect(reattachErr).NotTo(HaveOccurred())
					Expect(reattachResult).NotTo(BeNil())

					By("Waiting for virtual disk attachment to become ready")
					waitReattachErr := kubernetes.WaitForVirtualDiskAttached(ctx, res.BaseKubeconfig, conf.TestCluster.Namespace,
						reattachResult.AttachmentName, 5*time.Second)
					Expect(waitReattachErr).NotTo(HaveOccurred())
					virtualDiskAttachmentResult = reattachResult
				})

				It("has the same consumable block device as before detach", func() {
					blockDevices, getBDErr := kubernetes.GetConsumableBlockDevicesByNode(ctx, res.Kubeconfig, targetVM)
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
							client.MatchingFields{"spec.nodeName": targetVM},
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
								client.MatchingFields{"spec.nodeName": targetVM},
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
						blockDevices, getBDErr := kubernetes.GetConsumableBlockDevicesByNode(ctx, res.Kubeconfig, targetVM)
						Expect(getBDErr).NotTo(HaveOccurred())
						Expect(blockDevices).To(HaveLen(1))
						Expect(blockDevices[0]).To(Equal(initialBlockDevice))
					})
				})
			})

			When("running in pull_request and agent is updated to PR version", func() {
				BeforeAll(func() {
					if os.Getenv("GITHUB_EVENT_NAME") != "pull_request" {
						Skip("PR-only scenario")
					}

					mpo, err := dyn.Resource(mpoGVR).Get(ctx, consts.SdsNodeConfiguratorAgentName, metav1.GetOptions{})
					Expect(err).NotTo(HaveOccurred())

					err = unstructured.SetNestedField(mpo.Object, conf.ModulesImageTag, "spec", "imageTag")
					Expect(err).NotTo(HaveOccurred())

					_, err = dyn.Resource(mpoGVR).Update(ctx, mpo, metav1.UpdateOptions{})
					Expect(err).NotTo(HaveOccurred())

					Expect(kubernetes.WaitForModuleReady(ctx, res.Kubeconfig, consts.SdsNodeConfiguratorAgentName, 10*time.Minute)).To(Succeed())
				})

				It("has the same consumable block device", func() {
					By("Checking that consumable BlockDevice name remains unchanged after agent update")
					Expect(initialBlockDevice.Name).NotTo(BeEmpty())
					Eventually(func(g Gomega) {
						blockDevices, getBDErr := kubernetes.GetConsumableBlockDevicesByNode(ctx, res.Kubeconfig, targetVM)
						g.Expect(getBDErr).NotTo(HaveOccurred())
						g.Expect(blockDevices).To(HaveLen(1))
						g.Expect(blockDevices[0].Name).To(Equal(initialBlockDevice.Name))
					}, 5*time.Minute, 5*time.Second).Should(Succeed())
				})

				AfterAll(func() {
					mpo, err := dyn.Resource(mpoGVR).Get(ctx, consts.SdsNodeConfiguratorAgentName, metav1.GetOptions{})
					Expect(err).NotTo(HaveOccurred())

					err = unstructured.SetNestedField(mpo.Object, "main", "spec", "imageTag")
					Expect(err).NotTo(HaveOccurred())

					_, err = dyn.Resource(mpoGVR).Update(ctx, mpo, metav1.UpdateOptions{})
					Expect(err).NotTo(HaveOccurred())
				})
			})
		})
	})
})
