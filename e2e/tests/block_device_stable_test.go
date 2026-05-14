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
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ = Describe("BlockDeviceStable", Ordered, func() {
	var (
		ctx       context.Context
		res       *cluster.TestClusterResources
		conf      *cfg.Config
		k8sClient client.Client

		targetVM                    string
		virtualDiskAttachmentResult *kubernetes.VirtualDiskAttachmentResult
	)

	BeforeAll(func() {
		By("Run before all")
		ctx = context.Background()
		res = e2eNestedTestClusterOrNil()
		Expect(res).NotTo(BeNil())
		conf = cfg.Load()
		Expect(conf).ToNot(BeNil())
		ensureE2EK8sClient(res, &k8sClient, ctx)
		Expect(k8sClient).NotTo(BeNil())

		By("Creating virtualization client")
		virtClient, createVirtClientError := kubernetes.NewVirtualizationClient(ctx, res.BaseKubeconfig)
		Expect(createVirtClientError).NotTo(HaveOccurred())
		Expect(virtClient).NotTo(BeNil())

		By("Listing virtual machines to select target VM")
		vms, listVmErr := kubernetes.ListVirtualMachineNames(ctx, res.BaseKubeconfig, conf.TestCluster.Namespace)
		Expect(listVmErr).NotTo(HaveOccurred())
		slices.Sort(vms)
		targetVM = vms[0]

		By("Creating virtual disk attachment")
		attachResult, attachErr := kubernetes.AttachVirtualDiskToVM(ctx, res.BaseKubeconfig,
			kubernetes.VirtualDiskAttachmentConfig{
				VMName:           targetVM,
				Namespace:        conf.TestCluster.Namespace,
				DiskName:         "block-device-stable",
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

		if os.Getenv("GITHUB_EVENT_NAME") == "pull_request" {
			By("Getting ModulePullOverride spec")
			dyn, err := kubernetes.NewDynamicClientWithRetry(ctx, res.Kubeconfig)
			Expect(err).NotTo(HaveOccurred())

			mpoGVR := schema.GroupVersionResource{
				Group:    "deckhouse.io",
				Version:  "v1alpha2",
				Resource: "modulepulloverrides",
			}

			mpo, err := dyn.Resource(mpoGVR).Get(ctx, "sds-node-configurator", metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred())

			spec, found, err := unstructured.NestedMap(mpo.Object, "spec")
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue())
			GinkgoWriter.Printf("ImageTag: %s", spec["imageTag"])
		}
	})

	AfterAll(func() {
		if virtualDiskAttachmentResult != nil {
			By("Deleting virtual disk attachment and virtual disk")
			deleteVDErr := kubernetes.DetachAndDeleteVirtualDisk(ctx, res.BaseKubeconfig, conf.TestCluster.Namespace,
				virtualDiskAttachmentResult.AttachmentName, virtualDiskAttachmentResult.DiskName)
			if deleteVDErr != nil {
				GinkgoWriter.Println(deleteVDErr)
			}
		}
	})

	Context("Testing block device stability", Ordered, func() {
		var (
			blockDevice kubernetes.BlockDevice
		)
		It("Has consumable block device", func() {
			By("Getting consumable block devices on node - %s")
			blockDevices, getBDErr := kubernetes.GetConsumableBlockDevicesByNode(ctx, res.Kubeconfig, targetVM)
			Expect(getBDErr).NotTo(HaveOccurred())
			Expect(len(blockDevices)).To(BeNumerically(">", 0))
			Expect(len(blockDevices)).To(BeNumerically("<", 2))

			blockDevice = blockDevices[0]
		})

		When("Virtual disk is reattached", func() {
			BeforeAll(func() {
				By("Detaching the block device from the vm")
				detachErr := kubernetes.DetachAndDeleteVirtualDisk(ctx, res.BaseKubeconfig, conf.TestCluster.Namespace,
					virtualDiskAttachmentResult.AttachmentName, "")
				Expect(detachErr).NotTo(HaveOccurred())
			})

			AfterAll(func() {
				By("Reattaching the virtual disk to the vm")
				reattachResult, reattachErr := kubernetes.ReattachVirtualDiskToVM(ctx, res.BaseKubeconfig, kubernetes.VirtualDiskReattachmentConfig{
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

			It("Has not consumable block device", func() {
				Eventually(func(g Gomega) {
					blockDevices, getBDErr := kubernetes.GetConsumableBlockDevicesByNode(ctx, res.Kubeconfig, targetVM)
					g.Expect(getBDErr).NotTo(HaveOccurred())
					g.Expect(len(blockDevices)).To(BeNumerically("==", 0))
				}, time.Minute, 2*time.Second).Should(Succeed())
			})
		})

		When("reattached virtual disk to vm", func() {
			It("Has same consumable block device", func() {
				blockDevices, getBDErr := kubernetes.GetConsumableBlockDevicesByNode(ctx, res.Kubeconfig, targetVM)
				Expect(getBDErr).NotTo(HaveOccurred())
				Expect(len(blockDevices)).To(BeNumerically(">", 0))
				Expect(len(blockDevices)).To(BeNumerically("<", 2))
				newBlockDevice := blockDevices[0]
				Expect(newBlockDevice).To(Equal(blockDevice))
			})
		})

		When("Restarting sds-node-configurator agent", func() {
			BeforeAll(func() {
				restartAt := time.Now()

				err := k8sClient.DeleteAllOf(
					ctx,
					&v1.Pod{},
					client.InNamespace(consts.SdsNodeConfiguratorAgentNamespace),
					client.MatchingLabels{"app": consts.SdsNodeConfiguratorAgentAppLabel},
					client.MatchingFields{"spec.nodeName": targetVM},
				)
				Expect(err).NotTo(HaveOccurred())

				Eventually(func(g Gomega) {
					var pods v1.PodList
					g.Expect(k8sClient.List(
						ctx,
						&pods,
						client.InNamespace(consts.SdsNodeConfiguratorAgentNamespace),
						client.MatchingLabels{"app": consts.SdsNodeConfiguratorAgentAppLabel},
						client.MatchingFields{"spec.nodeName": targetVM},
					)).To(Succeed())

					g.Expect(pods.Items).To(HaveLen(1))
					p := pods.Items[0]

					g.Expect(p.CreationTimestamp.Time.After(restartAt)).To(BeTrue(), "ожидаем новый pod после рестарта")
					g.Expect(p.DeletionTimestamp).To(BeNil())
					g.Expect(p.Status.Phase).To(Equal(v1.PodRunning))
					g.Expect(isPodReady(&p)).To(BeTrue())
				}, 5*time.Minute, 5*time.Second).Should(Succeed())
			})

			It("sdasdas", func() {
				By("Getting sdasdasd on node - %s")
			})
		})
	})
})
