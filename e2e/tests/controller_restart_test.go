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

	"github.com/deckhouse/sds-node-configurator/e2e/cfg"
	"github.com/deckhouse/sds-node-configurator/e2e/framework"
	"github.com/deckhouse/sds-node-configurator/e2e/sdsclient"
	"github.com/deckhouse/storage-e2e/pkg/e2e"
	"github.com/deckhouse/storage-e2e/pkg/kubernetes"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	ctrlrstDiskSize = "2Gi"

	// Not 50%: half of a 2Gi disk rounds to 1Gi in spec while LVM may allocate slightly more
	// bytes (alignment), and VGConfigurationApplied then fails ValidationFailed (requested < actual).
	ctrlrstThinPoolSize            = "60%"
	ctrlrstThinPoolAllocationLimit = "100%"
)

var _ = Describe("Controller restart stability", Label("sds-node-configurator", "controller-restart"), Ordered, func() {
	var (
		ctx  context.Context
		conf *cfg.Config

		cl        *e2e.Cluster
		k8sClient client.Client

		targetNode string
		runID      string

		// Disk created by the spec; detached + deleted in AfterAll.
		managedDisk *e2e.Disk

		// Managed BlockDevice name tracked for AfterEach cleanup (LVGs cleaned by prefix).
		managedBlockDeviceName string
	)

	BeforeAll(func() {
		By("Preparing shared test context and Kubernetes clients")
		ctx = context.Background()
		conf = cfg.Load()

		var clErr error
		cl, clErr = e2e.Connect(ctx, e2e.WithTestName("controller-restart"))
		Expect(clErr).NotTo(HaveOccurred(), "failed to connect to cluster")

		var k8sErr error
		k8sClient, k8sErr = sdsclient.New(cl.RESTConfig())
		Expect(k8sErr).NotTo(HaveOccurred(), "failed to build controller-runtime client")

		By("Listing nodes to select the target node")
		nodeList, nlErr := cl.Clientset().CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		Expect(nlErr).NotTo(HaveOccurred(), "failed to list nodes")
		Expect(nodeList.Items).NotTo(BeEmpty(), "cluster must have at least one node")
		targetNode = nodeList.Items[0].Name

		runID = fmt.Sprintf("%d", time.Now().Unix())
	})

	AfterEach(func() {
		By("Cleaning up e2e LVMVolumeGroups")
		cleanupLVMVolumeGroups(ctx, k8sClient)

		if managedBlockDeviceName != "" {
			By("Force-deleting managed BlockDevice CR")
			forceDeleteBlockDevicesByNames(ctx, k8sClient, []string{managedBlockDeviceName})
			managedBlockDeviceName = ""
		}
	})

	AfterAll(func() {
		defer func() {
			if err := cl.Close(context.Background()); err != nil {
				GinkgoWriter.Println("Error closing cluster: ", err)
			}
		}()

		if managedDisk == nil {
			return
		}

		By("Detaching and deleting the test disk")
		if err := cl.Disks().DetachDisk(ctx, targetNode, managedDisk.Name); err != nil {
			GinkgoWriter.Printf("failed to detach disk %v: %v\n", managedDisk.Name, err)
		}
		if err := cl.Disks().DeleteDisk(ctx, managedDisk.Name); err != nil {
			GinkgoWriter.Printf("failed to delete disk %v: %v\n", managedDisk.Name, err)
		}
	})

	It("Should preserve managed VG and BlockDevice state after controller pod restart", func() {
		By("Сценарий: управляемые VG и BlockDevice → перезапуск pod контроллера → без лишних create/delete, статусы сходятся")

		By("Step 1: attach disk and wait for a new consumable BlockDevice")
		before, bdErr := kubernetes.GetConsumableBlockDevicesByNode(ctx, cl.RESTConfig(), targetNode)
		Expect(bdErr).NotTo(HaveOccurred())

		diskName := fmt.Sprintf("e2e-restart-state-disk-%s", runID)
		By("Creating and attaching a virtual disk: " + diskName)
		disk, createErr := cl.Disks().CreateDisk(ctx, e2e.DiskSpec{
			Name:         diskName,
			Size:         resource.MustParse(ctrlrstDiskSize),
			StorageClass: conf.TestCluster.StorageClass,
		})
		Expect(createErr).NotTo(HaveOccurred(), "failed to create disk")
		managedDisk = disk
		Expect(cl.Disks().AttachDisk(ctx, targetNode, disk.Name)).To(Succeed(), "failed to attach disk")

		By("Waiting for the new consumable BlockDevice on node " + targetNode)
		newBD, waitErr := framework.WaitNewConsumableBlockDevice(ctx, cl.RESTConfig(), targetNode, before, 5*time.Minute)
		Expect(waitErr).NotTo(HaveOccurred())
		managedBlockDeviceName = newBD.Name

		By("Step 1 (continued): create managed LVMVolumeGroup with thin-pool and wait for Ready")
		vgName := "e2e-vg-restart-" + runID
		thinPoolName := "e2e-thin-pool-restart"
		lvgName := lvmVGNamePrefix + "restart-" + runID

		By(fmt.Sprintf("Creating LVMVolumeGroup %s on node %s, VG %s, thin-pool %s %s", lvgName, targetNode, vgName, thinPoolName, ctrlrstThinPoolSize))
		Expect(kubernetes.CreateLVMVolumeGroupWithThinPool(
			ctx, cl.RESTConfig(), lvgName, targetNode, []string{newBD.Name}, vgName,
			[]kubernetes.ThinPoolSpec{{Name: thinPoolName, Size: ctrlrstThinPoolSize, AllocationLimit: ctrlrstThinPoolAllocationLimit}},
		)).To(Succeed())

		By("Waiting for managed LVMVolumeGroup to become Ready before restart")
		Expect(kubernetes.WaitForLVMVolumeGroupReady(ctx, cl.RESTConfig(), lvgName, lvmVolumeGroupReadyTimeout)).
			To(Succeed(), "managed LVMVolumeGroup must be Ready before restart")

		By("Step 2: snapshot BlockDevice/LVMVolumeGroup identity and status on the node")
		snapshot, snapErr := takeManagedStorageSnapshot(ctx, k8sClient, targetNode, newBD.Name, lvgName)
		Expect(snapErr).NotTo(HaveOccurred())
		GinkgoWriter.Printf("    snapshot: %d BlockDevice(s) on node %s; BD=%s LVG=%s phase=%s\n",
			len(snapshot.BlockDeviceNamesOnNode), targetNode, snapshot.BlockDeviceName, snapshot.LVMVolumeGroupName, snapshot.LVMVolumeGroupStatus.Phase)

		By("Step 3: restart sds-node-configurator agent/controller pod on the node")
		Expect(restartAgentOnNode(ctx, cl, targetNode)).To(Succeed(), "agent pod must restart and become ready")

		By("Step 4: verify no recreate/delete churn and statuses converge after controller is back")
		expectManagedStorageStableAfterRestart(ctx, k8sClient, snapshot)
		By("✓ Managed VG and BlockDevice unchanged after controller pod restart")
	})
})
