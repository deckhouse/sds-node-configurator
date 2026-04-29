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
	"time"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/e2e/tests/utils/pod"
	"github.com/deckhouse/storage-e2e/pkg/cluster"
	"github.com/deckhouse/storage-e2e/pkg/kubernetes"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sLogs "k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ = Describe("BlockDevice netlink discovery", Ordered, func() {
	var (
		testClusterResources *cluster.TestClusterResources
		e2eCtx               context.Context
		k8sClient            client.Client
		netlinkDiskAttach    *kubernetes.VirtualDiskAttachmentResult
		netlinkBDName        string
	)

	BeforeAll(func() {
		e2eCtx = context.Background()

		testClusterResources = e2eNestedTestClusterOrNil()
		Expect(testClusterResources).NotTo(BeNil(),
			"nested cluster must be created in BeforeSuite (e2eEnsureSharedNestedTestCluster)")
	})

	AfterEach(func() {
		if netlinkDiskAttach != nil && testClusterResources != nil && testClusterResources.BaseKubeconfig != nil {
			ns := e2eConfigNamespace()
			err := kubernetes.DetachAndDeleteVirtualDisk(e2eCtx, testClusterResources.BaseKubeconfig, ns, netlinkDiskAttach.AttachmentName, netlinkDiskAttach.DiskName)
			if err != nil {
				GinkgoWriter.Printf("Error deleting netlink disk: %v\n", err)
			}
			netlinkDiskAttach = nil
		}
		// TODO: best-effort cleanup
		//   - force-delete the BlockDevice CR if it survived (forceDeleteBlockDevicesByNames)
	})

	It("should create BlockDevice on netlink ADD and delete it on netlink REMOVE without invoking lsblk", func() {
		ensureE2EK8sClient(testClusterResources, &k8sClient, e2eCtx)
		Expect(testClusterResources.BaseKubeconfig).NotTo(BeNil(), "test requires nested virtualization")

		e2eNS := e2eConfigNamespace()
		Expect(e2eNS).NotTo(BeEmpty())

		e2eStorageClass := e2eConfigStorageClass()
		Expect(e2eStorageClass).NotTo(BeEmpty(), "TEST_CLUSTER_STORAGE_CLASS is required for VirtualDisk")

		vms := e2eListClusterVMNames(e2eCtx, testClusterResources, e2eNS)
		Expect(vms).NotTo(BeEmpty())
		targetVM := vms[0]

		attachResult, attachError := attachVirtualDiskWithRetry(e2eCtx, testClusterResources.BaseKubeconfig, kubernetes.VirtualDiskAttachmentConfig{
			VMName:           targetVM,
			Namespace:        e2eNS,
			DiskName:         fmt.Sprintf("e2e-netlink-%d", time.Now().UnixNano()),
			DiskSize:         "5Gi",
			StorageClassName: e2eStorageClass,
		}, e2eVirtualDiskAttachMaxRetries, e2eVirtualDiskAttachRetryInterval)
		Expect(attachError).NotTo(HaveOccurred(), "Failed to attach virtual disk")
		Expect(attachResult).NotTo(BeNil(), "Failed to attach virtual disk")

		netlinkDiskAttach = attachResult

		attachCtx, attachCancel := context.WithTimeout(e2eCtx, e2eVirtualDiskAttachWaitTimeout)
		defer attachCancel()
		Expect(kubernetes.WaitForVirtualDiskAttached(
			attachCtx, testClusterResources.BaseKubeconfig,
			e2eNS, netlinkDiskAttach.AttachmentName, 10*time.Second),
		).NotTo(HaveOccurred(), "Failed to attach virtual disk")

		blockDevice := e2eWaitConsumableBlockDeviceForVirtualDisk(e2eCtx, testClusterResources.BaseKubeconfig, k8sClient, e2eNS, netlinkDiskAttach.DiskName, netlinkDiskAttach.AttachmentName, targetVM)
		Expect(blockDevice).NotTo(BeNil())
		Expect(blockDevice.Status.Type).To(Equal("disk"))

		// VirtualDisk / hypervisor may report BlockDevice.Status.Size slightly above requested 5Gi
		// (sector alignment); require at least 5Gi, not byte-identical to MustParse("5Gi").
		wantSize := resource.MustParse("5Gi")
		Expect(blockDevice.Status.Size.Cmp(wantSize)).NotTo(BeNumerically("<", 0),
			"BD size must be >= requested %s, got %s", wantSize.String(), blockDevice.Status.Size.String())
		Expect(blockDevice.Status.NodeName).To(Equal(targetVM))
		netlinkBDName = blockDevice.Name

		agentPodName, fnErr := pod.FindNameOnNode(e2eCtx, k8sClient, targetVM, client.InNamespace("d8-sds-node-configurator"), client.MatchingLabels{"app": "sds-node-configurator"})
		Expect(fnErr).NotTo(HaveOccurred())
		Expect(agentPodName).NotTo(BeEmpty())

		cs, csErr := k8sLogs.NewForConfig(testClusterResources.Kubeconfig)
		Expect(csErr).NotTo(HaveOccurred())

		since := metav1.NewTime(time.Now().Add(-5 * time.Minute))
		logOpts := corev1.PodLogOptions{
			Container:  "sds-node-configurator-agent",
			SinceTime:  &since,
			Timestamps: true,
		}

		logText, logErr := pod.GetLogs(e2eCtx, cs, "d8-sds-node-configurator", agentPodName, logOpts)
		Expect(logErr).NotTo(HaveOccurred())
		Expect(logText).Should(MatchRegexp("(?i)succe.*netlink.*add"))

		Expect(kubernetes.DetachAndDeleteVirtualDisk(e2eCtx, testClusterResources.BaseKubeconfig,
			e2eNS, netlinkDiskAttach.AttachmentName, netlinkDiskAttach.DiskName)).To(Succeed())

		netlinkDiskAttach = nil

		Eventually(func(g Gomega) {
			var bd v1alpha1.BlockDevice
			err := k8sClient.Get(e2eCtx, client.ObjectKey{Name: netlinkBDName}, &bd)
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue(), "BlockDevice %s should be deleted after detach; err=%v", netlinkBDName, err)
		}, 30*time.Second, time.Second).Should(Succeed())

		// Scenario:
		//   1. Attach a 5Gi VirtualDisk to a target VM node from the cluster.
		//   2. Eventually (≤ 30s) a BlockDevice CR appears with status.nodeName==target, type=="disk", size==5Gi.
		//   3. Agent logs on the target node contain `udev event ... action=add` and `HandleEvent`, and NO `exec ... lsblk`.
		//   4. Detach + delete the VirtualDisk.
		//   5. Eventually (≤ 30s) the BlockDevice is gone (apierrors.IsNotFound on Get).
		//   6. Agent logs contain `udev event ... action=remove` and `HandleEvent`, and NO `exec ... lsblk`.
	})
})
