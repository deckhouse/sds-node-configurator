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
	"strings"
	"time"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/e2e/cfg"
	"github.com/deckhouse/sds-node-configurator/e2e/tests/utils/consts"
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
	netlinkDiskSize        = "5Gi"
	netlinkDiskResizedSize = "8Gi"

	netlinkAddEventLogPattern    = `(?i)\[HandleEvent\].*udev event.*action=add`
	netlinkRemoveEventLogPattern = `(?i)\[HandleEvent\].*udev event.*action=remove`
	netlinkChangeEventLogPattern = `(?i)\[HandleEvent\].*udev event.*action=change`
)

var _ = Describe("BlockDevice netlink discovery", Ordered, ContinueOnFailure, func(ctx context.Context) {
	var (
		testClusterResources *cluster.TestClusterResources
		k8sClient            client.Client
		cs                   *k8sclient.Clientset
		conf                 *cfg.Config

		targetVM string

		netlinkDiskAttach *kubernetes.VirtualDiskAttachmentResult
		blockDevice       *v1alpha1.BlockDevice
		agentPod          *v1.Pod

		addSince    metav1.Time
		removeSince metav1.Time
	)

	BeforeAll(func() {
		testClusterResources = e2eNestedTestClusterOrNil()
		Expect(testClusterResources).NotTo(BeNil(),
			"nested cluster must be created in BeforeSuite (e2eEnsureSharedNestedTestCluster)")

		ensureE2EK8sClient(testClusterResources, &k8sClient, ctx)
		Expect(testClusterResources.BaseKubeconfig).NotTo(BeNil(), "test requires nested virtualization")

		conf = cfg.Load()

		vms, vmListErr := kubernetes.ListVirtualMachineNames(ctx, testClusterResources.BaseKubeconfig, conf.TestCluster.Namespace)
		Expect(vmListErr).NotTo(HaveOccurred())
		Expect(vms).NotTo(BeEmpty())

		for _, vm := range vms {
			if strings.HasPrefix(vm, "bootstrap-node-") {
				continue
			}

			targetVM = vm
		}

		var csErr error
		cs, csErr = kubernetes.NewClientsetWithRetry(ctx, testClusterResources.Kubeconfig)
		Expect(csErr).NotTo(HaveOccurred())
	})

	AfterAll(func() {
		if netlinkDiskAttach != nil && testClusterResources != nil && testClusterResources.BaseKubeconfig != nil {
			dErr := kubernetes.DetachAndDeleteVirtualDisk(ctx, testClusterResources.BaseKubeconfig, conf.TestCluster.Namespace, netlinkDiskAttach.AttachmentName, netlinkDiskAttach.DiskName)
			GinkgoWriter.Printf("Detach error - %v", dErr)
		}
	})

	It("attaches VirtualDisk and discovers BlockDevice", func() {
		addSince = metav1.NewTime(time.Now())

		attachResult, attachError := kubernetes.AttachVirtualDiskToVM(ctx, testClusterResources.BaseKubeconfig, kubernetes.VirtualDiskAttachmentConfig{
			VMName:           targetVM,
			Namespace:        conf.TestCluster.Namespace,
			DiskName:         fmt.Sprintf("e2e-netlink-%d", time.Now().UnixNano()),
			DiskSize:         netlinkDiskSize,
			StorageClassName: conf.TestCluster.StorageClass,
		})
		Expect(attachError).NotTo(HaveOccurred(), "failed to attach virtual disk")
		Expect(attachResult).NotTo(BeNil())

		netlinkDiskAttach = attachResult

		attachCtx, attachCancel := context.WithTimeout(ctx, consts.VirtualDiskAttachWaitTimeout)
		defer attachCancel()
		Expect(kubernetes.WaitForVirtualDiskAttached(
			attachCtx, testClusterResources.BaseKubeconfig,
			conf.TestCluster.Namespace, netlinkDiskAttach.AttachmentName, 10*time.Second,
		)).NotTo(HaveOccurred(), "virtual disk did not become attached in time")

		blockDevice = e2eWaitConsumableBlockDeviceForVirtualDisk(
			ctx, testClusterResources.BaseKubeconfig, k8sClient,
			conf.TestCluster.Namespace, netlinkDiskAttach.DiskName, netlinkDiskAttach.AttachmentName, targetVM,
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
			ctx, k8sClient, targetVM,
			client.InNamespace(consts.SdsNodeConfiguratorAgentNamespace),
			client.MatchingLabels{"app": consts.SdsNodeConfiguratorAgentName},
		)
		Expect(fnErr).NotTo(HaveOccurred())

		logOpts := v1.PodLogOptions{
			Container:  consts.SdsNodeConfiguratorAgentContainer,
			SinceTime:  &addSince,
			Timestamps: true,
		}
		Eventually(func(g Gomega) string {
			logText, logErr := pod.GetLogs(ctx, cs, consts.SdsNodeConfiguratorAgentNamespace, agentPod.Name, logOpts)
			g.Expect(logErr).NotTo(HaveOccurred())
			return logText
		}, time.Minute, 2*time.Second).Should(MatchRegexp(netlinkAddEventLogPattern))
	}, SpecTimeout(2*time.Minute))

	It("removes BlockDevice after VirtualDisk detach", func(ctx SpecContext) {
		Expect(netlinkDiskAttach).NotTo(BeNil(), "disk must be attached in previous step")

		removeSince = metav1.NewTime(time.Now())
		Expect(kubernetes.DetachAndDeleteVirtualDisk(
			ctx, testClusterResources.BaseKubeconfig,
			conf.TestCluster.Namespace, netlinkDiskAttach.AttachmentName, netlinkDiskAttach.DiskName,
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
			Container:  consts.SdsNodeConfiguratorAgentContainer,
			SinceTime:  &removeSince,
			Timestamps: true,
		}
		Eventually(func(g Gomega) string {
			logText, logErr := pod.GetLogs(ctx, cs, consts.SdsNodeConfiguratorAgentNamespace, agentPod.Name, logOpts)
			g.Expect(logErr).NotTo(HaveOccurred())
			return logText
		}, time.Minute, 2*time.Second).Should(MatchRegexp(netlinkRemoveEventLogPattern))
	}, SpecTimeout(2*time.Minute))
})
