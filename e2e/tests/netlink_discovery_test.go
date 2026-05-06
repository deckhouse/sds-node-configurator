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
	"k8s.io/api/core/v1"
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

var _ = Describe("BlockDevice netlink discovery", Ordered, ContinueOnFailure, func() {
	var (
		testClusterResources *cluster.TestClusterResources
		e2eCtx               context.Context
		k8sClient            client.Client
		cs                   *k8sclient.Clientset

		e2eNs    string
		targetVM string

		netlinkDiskAttach *kubernetes.VirtualDiskAttachmentResult
		blockDevice       *v1alpha1.BlockDevice
		agentPod          *v1.Pod

		addSince    metav1.Time
		removeSince metav1.Time
	)

	BeforeAll(func() {
		e2eCtx = context.Background()

		testClusterResources = e2eNestedTestClusterOrNil()
		Expect(testClusterResources).NotTo(BeNil(),
			"nested cluster must be created in BeforeSuite (e2eEnsureSharedNestedTestCluster)")

		ensureE2EK8sClient(testClusterResources, &k8sClient, e2eCtx)
		Expect(testClusterResources.BaseKubeconfig).NotTo(BeNil(), "test requires nested virtualization")

		e2eNs = e2eConfigNamespace()
		Expect(e2eNs).ToNot(BeEmpty())
		e2eStorageClass := e2eConfigStorageClass()
		Expect(e2eStorageClass).ToNot(BeEmpty(), "TEST_CLUSTER_STORAGE_CLASS is required for VirtualDisk")

		vms := e2eListClusterVMNames(e2eCtx, testClusterResources, e2eNs)
		Expect(vms).NotTo(BeEmpty())
		targetVM = vms[0]

		var csErr error
		cs, csErr = k8sclient.NewForConfig(testClusterResources.Kubeconfig)
		Expect(csErr).NotTo(HaveOccurred())
	})

	AfterAll(func() {
		if netlinkDiskAttach != nil && testClusterResources != nil && testClusterResources.BaseKubeconfig != nil {
			ns := e2eConfigNamespace()
			dErr := kubernetes.DetachAndDeleteVirtualDisk(e2eCtx, testClusterResources.BaseKubeconfig, ns, netlinkDiskAttach.AttachmentName, netlinkDiskAttach.DiskName)
			GinkgoWriter.Printf("Detach error - %v", dErr)
		}
		if blockDevice != nil {
			forceDeleteBlockDevicesByNames(e2eCtx, k8sClient, []string{blockDevice.Name})
		}
	})

	It("attaches VirtualDisk and discovers BlockDevice", func() {
		addSince = metav1.NewTime(time.Now())

		attachResult, attachError := attachVirtualDiskWithRetry(e2eCtx, testClusterResources.BaseKubeconfig, kubernetes.VirtualDiskAttachmentConfig{
			VMName:           targetVM,
			Namespace:        e2eNs,
			DiskName:         fmt.Sprintf("e2e-netlink-%d", time.Now().UnixNano()),
			DiskSize:         netlinkDiskSize,
			StorageClassName: e2eConfigStorageClass(),
		}, e2eVirtualDiskAttachMaxRetries, e2eVirtualDiskAttachRetryInterval)
		Expect(attachError).NotTo(HaveOccurred(), "failed to attach virtual disk")
		Expect(attachResult).NotTo(BeNil())

		netlinkDiskAttach = attachResult

		attachCtx, attachCancel := context.WithTimeout(e2eCtx, e2eVirtualDiskAttachWaitTimeout)
		defer attachCancel()
		Expect(kubernetes.WaitForVirtualDiskAttached(
			attachCtx, testClusterResources.BaseKubeconfig,
			e2eNs, netlinkDiskAttach.AttachmentName, 10*time.Second,
		)).NotTo(HaveOccurred(), "virtual disk did not become attached in time")

		blockDevice = e2eWaitConsumableBlockDeviceForVirtualDisk(
			e2eCtx, testClusterResources.BaseKubeconfig, k8sClient,
			e2eNs, netlinkDiskAttach.DiskName, netlinkDiskAttach.AttachmentName, targetVM,
		)
		Expect(blockDevice).NotTo(BeNil())
	})

	It("creates BlockDevice with expected attributes", func() {
		Expect(blockDevice).NotTo(BeNil(), "blockDevice must be discovered in previous step")
		Expect(blockDevice.Status.Type).To(Equal("disk"))
		Expect(blockDevice.Status.NodeName).To(Equal(targetVM))

		wantSize := resource.MustParse(netlinkDiskSize)
		maxSize := wantSize.DeepCopy()
		maxSize.Add(resource.MustParse("16Mi"))

		Expect(blockDevice.Status.Size.Cmp(wantSize)).NotTo(BeNumerically("<", 0),
			"BD size must be >= requested %s, got %s", wantSize.String(), blockDevice.Status.Size.String())
		Expect(blockDevice.Status.Size.Cmp(maxSize)).NotTo(BeNumerically(">", 0),
			"BD size must be <= requested size + 16Mi (%s), got %s", maxSize.String(), blockDevice.Status.Size.String())
	})

	It("writes udev add event to agent logs", func(ctx SpecContext) {
		Skip("not implemented netlink logs")

		var fnErr error
		agentPod, fnErr = pod.FindRunningPodOnNode(
			e2eCtx, k8sClient, targetVM,
			client.InNamespace(netlinkAgentNamespace),
			client.MatchingLabels{"app": netlinkAgentAppLabel},
		)
		Expect(fnErr).NotTo(HaveOccurred())

		logOpts := v1.PodLogOptions{
			Container:  netlinkAgentContainer,
			SinceTime:  &addSince,
			Timestamps: true,
		}
		Eventually(func(g Gomega) string {
			logText, logErr := pod.GetLogs(ctx, cs, netlinkAgentNamespace, agentPod.Name, logOpts)
			g.Expect(logErr).NotTo(HaveOccurred())
			return logText
		}, time.Minute, 2*time.Second).Should(MatchRegexp(netlinkAddEventLogPattern))
	}, SpecTimeout(2*time.Minute))

	It("removes BlockDevice after VirtualDisk detach", func(ctx SpecContext) {
		Expect(netlinkDiskAttach).NotTo(BeNil(), "disk must be attached in previous step")

		removeSince = metav1.NewTime(time.Now())
		Expect(kubernetes.DetachAndDeleteVirtualDisk(
			e2eCtx, testClusterResources.BaseKubeconfig,
			e2eNs, netlinkDiskAttach.AttachmentName, netlinkDiskAttach.DiskName,
		)).To(Succeed())

		netlinkDiskAttach = nil // prevent AfterAll double-cleanup

		bdName := blockDevice.Name
		Eventually(func(g Gomega) {
			var bd v1alpha1.BlockDevice
			err := k8sClient.Get(ctx, client.ObjectKey{Name: bdName}, &bd)
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue(),
				"BlockDevice %s should be deleted after detach; err=%v", bdName, err)
		}, 30*time.Second, time.Second).Should(Succeed())
	}, SpecTimeout(1*time.Minute))

	It("writes udev remove event to agent logs", func(ctx SpecContext) {
		Skip("not implemented netlink logs")
		logOpts := v1.PodLogOptions{
			Container:  netlinkAgentContainer,
			SinceTime:  &removeSince,
			Timestamps: true,
		}
		Eventually(func(g Gomega) string {
			logText, logErr := pod.GetLogs(ctx, cs, netlinkAgentNamespace, agentPod.Name, logOpts)
			g.Expect(logErr).NotTo(HaveOccurred())
			return logText
		}, time.Minute, 2*time.Second).Should(MatchRegexp(netlinkRemoveEventLogPattern))
	}, SpecTimeout(2*time.Minute))
})
