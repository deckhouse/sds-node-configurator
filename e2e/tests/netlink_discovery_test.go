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
	k8sclient "k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	netlinkAgentNamespace = "d8-sds-node-configurator"
	netlinkAgentAppLabel  = "sds-node-configurator"
	netlinkAgentContainer = "sds-node-configurator-agent"

	netlinkDiskSize = "5Gi"

	netlinkAddEventLogPattern    = `(?i)\[HandleEvent\].*udev event.*action=add`
	netlinkRemoveEventLogPattern = `(?i)\[HandleEvent\].*udev event.*action=remove`
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
		if netlinkBDName != "" {
			forceDeleteBlockDevicesByNames(e2eCtx, k8sClient, []string{netlinkBDName})
			netlinkBDName = ""
		}
	})

	// Scenario:
	//   1. Attach a 5Gi VirtualDisk to a target VM node from the cluster.
	//   2. Eventually (≤ 30s) a BlockDevice CR appears with status.nodeName==target, type=="disk", size==5Gi.
	//   3. Agent logs on the target node contain `[HandleEvent] udev event ... action=add`.
	//   4. Detach + delete the VirtualDisk.
	//   5. Eventually (≤ 30s) the BlockDevice is gone (apierrors.IsNotFound on Get).
	//   6. Agent logs contain `[HandleEvent] udev event ... action=remove`.
	It("should create BlockDevice on netlink ADD and delete it on netlink REMOVE", func() {
		ensureE2EK8sClient(testClusterResources, &k8sClient, e2eCtx)
		Expect(testClusterResources.BaseKubeconfig).NotTo(BeNil(), "test requires nested virtualization")

		e2eNS := e2eConfigNamespace()
		Expect(e2eNS).NotTo(BeEmpty())

		e2eStorageClass := e2eConfigStorageClass()
		Expect(e2eStorageClass).NotTo(BeEmpty(), "TEST_CLUSTER_STORAGE_CLASS is required for VirtualDisk")

		vms := e2eListClusterVMNames(e2eCtx, testClusterResources, e2eNS)
		Expect(vms).NotTo(BeEmpty())
		targetVM := vms[0]

		addSince := metav1.NewTime(time.Now())

		attachResult, attachError := attachVirtualDiskWithRetry(e2eCtx, testClusterResources.BaseKubeconfig, kubernetes.VirtualDiskAttachmentConfig{
			VMName:           targetVM,
			Namespace:        e2eNS,
			DiskName:         fmt.Sprintf("e2e-netlink-%d", time.Now().UnixNano()),
			DiskSize:         netlinkDiskSize,
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

		// allow small hypervisor alignment overhead up to 16Mi
		wantSize := resource.MustParse(netlinkDiskSize)
		maxSize := wantSize.DeepCopy()
		maxSize.Add(resource.MustParse("16Mi"))

		Expect(blockDevice.Status.Size.Cmp(wantSize)).NotTo(BeNumerically("<", 0),
			"BD size must be >= requested %s, got %s", wantSize.String(), blockDevice.Status.Size.String())

		Expect(blockDevice.Status.Size.Cmp(maxSize)).NotTo(BeNumerically(">", 0),
			"BD size must be <= requested size + 16Mi (%s), got %s", maxSize.String(), blockDevice.Status.Size.String())
		Expect(blockDevice.Status.NodeName).To(Equal(targetVM))

		netlinkBDName = blockDevice.Name

		agentPodName, fnErr := pod.FindNameOnNode(
			e2eCtx,
			k8sClient,
			targetVM,
			client.InNamespace(netlinkAgentNamespace),
			client.MatchingLabels{"app": netlinkAgentAppLabel},
		)
		Expect(fnErr).NotTo(HaveOccurred())
		Expect(agentPodName).NotTo(BeEmpty())

		cs, csErr := k8sclient.NewForConfig(testClusterResources.Kubeconfig)
		Expect(csErr).NotTo(HaveOccurred())

		logOpts := corev1.PodLogOptions{
			Container:  netlinkAgentContainer,
			SinceTime:  &addSince,
			Timestamps: true,
		}

		logText, logErr := pod.GetLogs(e2eCtx, cs, netlinkAgentNamespace, agentPodName, logOpts)
		Expect(logErr).NotTo(HaveOccurred())
		Expect(logText).Should(MatchRegexp(netlinkAddEventLogPattern))

		removeSince := metav1.NewTime(time.Now())
		Expect(kubernetes.DetachAndDeleteVirtualDisk(e2eCtx, testClusterResources.BaseKubeconfig,
			e2eNS, netlinkDiskAttach.AttachmentName, netlinkDiskAttach.DiskName)).To(Succeed())

		netlinkDiskAttach = nil

		Eventually(func(g Gomega) {
			var bd v1alpha1.BlockDevice
			err := k8sClient.Get(e2eCtx, client.ObjectKey{Name: netlinkBDName}, &bd)
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue(), "BlockDevice %s should be deleted after detach; err=%v", netlinkBDName, err)
		}, 30*time.Second, time.Second).Should(Succeed())

		removeLogOpts := corev1.PodLogOptions{
			Container:  netlinkAgentContainer,
			SinceTime:  &removeSince,
			Timestamps: true,
		}

		removeLogText, removeLogErr := pod.GetLogs(e2eCtx, cs, netlinkAgentNamespace, agentPodName, removeLogOpts)
		Expect(removeLogErr).NotTo(HaveOccurred())
		Expect(removeLogText).Should(MatchRegexp(netlinkRemoveEventLogPattern))
	})
})
