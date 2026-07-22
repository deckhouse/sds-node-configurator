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
	"github.com/deckhouse/sds-node-configurator/e2e/cfg"
	"github.com/deckhouse/sds-node-configurator/e2e/framework"
	"github.com/deckhouse/sds-node-configurator/e2e/sdsclient"
	"github.com/deckhouse/sds-node-configurator/e2e/tests/utils/consts"
	"github.com/deckhouse/sds-node-configurator/e2e/tests/utils/pod"
	"github.com/deckhouse/storage-e2e/pkg/e2e"
	"github.com/deckhouse/storage-e2e/pkg/kubernetes"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
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

var _ = Describe("BlockDevice netlink discovery", Label("e2e-tests"), Ordered, ContinueOnFailure, func() {
	var (
		cl         *e2e.Cluster
		k8sClient  client.Client
		cs         *k8sclient.Clientset
		conf       *cfg.Config
		ctx        context.Context
		targetNode string

		diskName    string
		blockDevice *v1alpha1.BlockDevice
		agentPod    *v1.Pod

		addSince    metav1.Time
		changeSince metav1.Time
		removeSince metav1.Time
	)

	BeforeAll(func() {
		ctx = context.Background()

		var clErr error
		cl, clErr = e2e.Connect(ctx, e2e.WithTestName("block-device-netlink-discovery"))
		Expect(clErr).NotTo(HaveOccurred(), "failed to connect to cluster")
		DeferCleanup(func() {
			if err := cl.Close(context.Background()); err != nil {
				GinkgoWriter.Println("Error closing cluster: ", err)
			}
		})

		var k8sErr error
		k8sClient, k8sErr = sdsclient.New(cl.RESTConfig())
		Expect(k8sErr).NotTo(HaveOccurred(), "failed to build controller-runtime client")

		conf = cfg.Load()

		nodeList, nlErr := cl.Clientset().CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		Expect(nlErr).NotTo(HaveOccurred())
		Expect(nodeList.Items).NotTo(BeEmpty())
		targetNode = nodeList.Items[0].Name

		var csErr error
		cs, csErr = k8sclient.NewForConfig(cl.RESTConfig())
		Expect(csErr).NotTo(HaveOccurred())
	})

	AfterAll(func() {
		if diskName != "" {
			dErr := cl.Disks().DetachDisk(ctx, targetNode, diskName)
			GinkgoWriter.Printf("Detach error - %v", dErr)
			if delErr := cl.Disks().DeleteDisk(ctx, diskName); delErr != nil {
				GinkgoWriter.Printf("Delete error - %v", delErr)
			}
		}
	})

	It("attaches VirtualDisk and discovers BlockDevice", func() {
		addSince = metav1.NewTime(time.Now())

		By("Snapshotting consumable BlockDevices before attach")
		before, snapErr := kubernetes.GetConsumableBlockDevicesByNode(ctx, cl.RESTConfig(), targetNode)
		Expect(snapErr).NotTo(HaveOccurred())

		diskName = fmt.Sprintf("e2e-netlink-%d", time.Now().UnixNano())
		disk, createErr := cl.Disks().CreateDisk(ctx, e2e.DiskSpec{
			Name:         diskName,
			Size:         resource.MustParse(netlinkDiskSize),
			StorageClass: conf.TestCluster.StorageClass,
		})
		Expect(createErr).NotTo(HaveOccurred(), "failed to create virtual disk")
		Expect(disk).NotTo(BeNil())
		diskName = disk.Name

		Expect(cl.Disks().AttachDisk(ctx, targetNode, diskName)).To(Succeed(), "failed to attach virtual disk")

		newBD, waitErr := framework.WaitNewConsumableBlockDevice(ctx, cl.RESTConfig(), targetNode, before, 5*time.Minute)
		Expect(waitErr).NotTo(HaveOccurred())

		var bd v1alpha1.BlockDevice
		Expect(k8sClient.Get(ctx, client.ObjectKey{Name: newBD.Name}, &bd)).To(Succeed())
		blockDevice = &bd
		Expect(blockDevice).NotTo(BeNil())
	})

	It("creates BlockDevice with expected attributes", func() {
		Expect(blockDevice).NotTo(BeNil(), "blockDevice must be discovered in previous step")
		Expect(blockDevice.Status.Type).To(Equal("disk"))
		Expect(blockDevice.Status.NodeName).To(Equal(targetNode))

		wantSize := resource.MustParse(netlinkDiskSize)
		maxSize := wantSize.DeepCopy()
		maxSize.Add(resource.MustParse("16Mi"))

		Expect(blockDevice.Status.Size.Cmp(wantSize)).NotTo(BeNumerically("<", 0),
			"BD size must be >= requested %s, got %s", wantSize.String(), blockDevice.Status.Size.String())
		Expect(blockDevice.Status.Size.Cmp(maxSize)).NotTo(BeNumerically(">", 0),
			"BD size must be <= requested size + 16Mi (%s), got %s", maxSize.String(), blockDevice.Status.Size.String())
	})

	It("writes udev add event to agent logs", func() {
		Skip("not implemented netlink logs")

		var fnErr error
		agentPod, fnErr = pod.FindRunningPodOnNode(
			ctx, k8sClient, targetNode,
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
	})

	It("updates BlockDevice after VirtualDisk resize (CHANGE)", func() {
		Expect(diskName).NotTo(BeEmpty(), "disk must be attached in previous step")
		Expect(blockDevice).NotTo(BeNil(), "blockDevice must be discovered in previous step")

		bdName := blockDevice.Name
		bdNode := blockDevice.Status.NodeName
		baselineBDSize := blockDevice.Status.Size.Value()

		changeSince = metav1.NewTime(time.Now())

		By(fmt.Sprintf("Growing VirtualDisk %s from %s to %s", diskName, netlinkDiskSize, netlinkDiskResizedSize))
		Expect(framework.ResizeDisk(
			ctx,
			cl.Dynamic(),
			diskName,
			netlinkDiskResizedSize,
		)).To(Succeed())

		By("Waiting until the same BlockDevice reflects increased size")
		Eventually(func(g Gomega) {
			var bd v1alpha1.BlockDevice
			err := k8sClient.Get(ctx, client.ObjectKey{Name: bdName}, &bd)
			g.Expect(err).NotTo(HaveOccurred())

			g.Expect(bd.Status.Type).To(Equal("disk"))
			g.Expect(bd.Status.NodeName).To(Equal(bdNode), "BlockDevice should stay on the same node after resize")

			g.Expect(bd.Status.Size.Value()).To(BeNumerically(">", baselineBDSize),
				"BlockDevice size should increase after VirtualDisk resize (was %d)", baselineBDSize)

			maxSize := resource.MustParse(netlinkDiskResizedSize)
			maxSize.Add(resource.MustParse("16Mi"))
			g.Expect(bd.Status.Size.Cmp(maxSize)).NotTo(BeNumerically(">", 0),
				"BD size must be <= requested resized size + 16Mi (%s), got %s", maxSize.String(), bd.Status.Size.String())

			blockDevice = &bd
		}, 2*time.Minute, 2*time.Second).Should(Succeed())
	})

	It("writes udev change event to agent logs", func(ctx SpecContext) {
		Skip("not implemented netlink logs")

		var fnErr error
		agentPod, fnErr = pod.FindRunningPodOnNode(
			ctx, k8sClient, targetNode,
			client.InNamespace(consts.SdsNodeConfiguratorAgentNamespace),
			client.MatchingLabels{"app": consts.SdsNodeConfiguratorAgentName},
		)
		Expect(fnErr).NotTo(HaveOccurred())

		logOpts := v1.PodLogOptions{
			Container:  consts.SdsNodeConfiguratorAgentContainer,
			SinceTime:  &changeSince,
			Timestamps: true,
		}
		Eventually(func(g Gomega) string {
			logText, logErr := pod.GetLogs(ctx, cs, consts.SdsNodeConfiguratorAgentNamespace, agentPod.Name, logOpts)
			g.Expect(logErr).NotTo(HaveOccurred())
			return logText
		}, time.Minute, 2*time.Second).Should(MatchRegexp(netlinkChangeEventLogPattern))
	})

	It("removes BlockDevice after VirtualDisk detach", func() {
		Expect(diskName).NotTo(BeEmpty(), "disk must be attached in previous step")

		removeSince = metav1.NewTime(time.Now())
		Expect(cl.Disks().DetachDisk(ctx, targetNode, diskName)).To(Succeed())
		Expect(cl.Disks().DeleteDisk(ctx, diskName)).To(Succeed())

		diskName = "" // prevent AfterAll double-cleanup

		bdName := blockDevice.Name
		Eventually(func(g Gomega) {
			var bd v1alpha1.BlockDevice
			err := k8sClient.Get(ctx, client.ObjectKey{Name: bdName}, &bd)
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue(),
				"BlockDevice %s should be deleted after detach; err=%v", bdName, err)
		}, 30*time.Second, time.Second).Should(Succeed())
	})

	It("writes udev remove event to agent logs", func() {
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
	})
})
