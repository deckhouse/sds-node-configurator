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
	"math/rand"
	"strings"
	"time"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/e2e/cfg"
	"github.com/deckhouse/sds-node-configurator/e2e/framework"
	"github.com/deckhouse/sds-node-configurator/e2e/sdsclient"
	"github.com/deckhouse/storage-e2e/pkg/e2e"
	"github.com/deckhouse/storage-e2e/pkg/kubernetes"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	bdrecDiskSize       = "5Gi"
	bdrecFakeBDPrefix   = "dev-e2e-fake-manual-"
	bdrecFakeDevicePath = "/dev/e2e-nonexistent-device"
	bdrecReconcileWait  = 5 * time.Minute
	bdrecReconcilePoll  = 10 * time.Second
	bdrecCleanupTimeout = 2 * time.Minute
)

var _ = Describe("BlockDevice reconcile", Label("sds-node-configurator", "block-device"), Ordered, func() {
	var (
		ctx        context.Context
		conf       *cfg.Config
		cl         *e2e.Cluster
		k8sClient  client.Client
		targetNode string

		bdrecCreatedDiskNames []string
		bdrecTrackedBDNames   []string
	)

	BeforeAll(func() {
		ctx = context.Background()
		conf = cfg.Load()

		var clErr error
		cl, clErr = e2e.Connect(ctx, e2e.WithTestName("block-device-reconcile"))
		Expect(clErr).NotTo(HaveOccurred(), "failed to connect to cluster")
		DeferCleanup(func() {
			if err := cl.Close(context.Background()); err != nil {
				GinkgoWriter.Println("Error closing cluster: ", err)
			}
		})

		var k8sErr error
		k8sClient, k8sErr = sdsclient.New(cl.RESTConfig())
		Expect(k8sErr).NotTo(HaveOccurred(), "failed to build controller-runtime client")

		nodeList, nlErr := cl.Clientset().CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		Expect(nlErr).NotTo(HaveOccurred(), "failed to list cluster nodes")
		Expect(nodeList.Items).NotTo(BeEmpty(), "cluster must have at least one node")
		targetNode = nodeList.Items[0].Name
	})

	AfterEach(func() {
		if len(bdrecTrackedBDNames) > 0 {
			forceDeleteBlockDevicesByNames(ctx, k8sClient, bdrecTrackedBDNames)
			bdrecTrackedBDNames = nil
		}
	})

	AfterAll(func() {
		for _, name := range bdrecCreatedDiskNames {
			if detachErr := cl.Disks().DetachDisk(ctx, targetNode, name); detachErr != nil {
				GinkgoWriter.Printf("failed to detach disk %s: %v\n", name, detachErr)
			}
			if deleteErr := cl.Disks().DeleteDisk(ctx, name); deleteErr != nil {
				GinkgoWriter.Printf("failed to delete disk %s: %v\n", name, deleteErr)
			}
		}
		bdrecCreatedDiskNames = nil

		if k8sClient != nil {
			forceDeleteAllNonConsumableBlockDevices(ctx, k8sClient, bdrecCleanupTimeout)
		}
	})

	It("Should delete a BlockDevice after the backing disk disappears", func() {
		By("Recording existing consumable block devices on the target node")
		before, beforeErr := kubernetes.GetConsumableBlockDevicesByNode(ctx, cl.RESTConfig(), targetNode)
		Expect(beforeErr).NotTo(HaveOccurred())

		diskName := fmt.Sprintf("bdrec-missing-disk-%d", time.Now().UnixNano())
		By("Step 1: Attaching a virtual disk to the target node: " + diskName)
		disk, createErr := cl.Disks().CreateDisk(ctx, e2e.DiskSpec{
			Name:         diskName,
			Size:         resource.MustParse(bdrecDiskSize),
			StorageClass: conf.TestCluster.StorageClass,
		})
		Expect(createErr).NotTo(HaveOccurred(), "failed to create disk")
		bdrecCreatedDiskNames = append(bdrecCreatedDiskNames, disk.Name)

		Expect(cl.Disks().AttachDisk(ctx, targetNode, disk.Name)).To(Succeed(), "failed to attach disk")

		By("Step 2: Waiting for the new consumable BlockDevice to appear")
		newBD, waitErr := framework.WaitNewConsumableBlockDevice(ctx, cl.RESTConfig(), targetNode, before, bdrecReconcileWait)
		Expect(waitErr).NotTo(HaveOccurred())
		Expect(newBD.Name).NotTo(BeEmpty())
		By(fmt.Sprintf("Discovered BlockDevice %s on node %s (path=%s, size=%s)",
			newBD.Name, newBD.NodeName, newBD.Path, newBD.Size))

		By("Step 3: Detaching and deleting the virtual disk to simulate device loss")
		Expect(cl.Disks().DetachDisk(ctx, targetNode, disk.Name)).To(Succeed())
		Expect(cl.Disks().DeleteDisk(ctx, disk.Name)).To(Succeed())
		for i, n := range bdrecCreatedDiskNames {
			if n == disk.Name {
				bdrecCreatedDiskNames = append(bdrecCreatedDiskNames[:i], bdrecCreatedDiskNames[i+1:]...)
				break
			}
		}

		By("Step 4: Restarting the agent on the target node to trigger a rescan")
		Expect(restartAgentOnNode(ctx, cl, targetNode)).To(Succeed())

		By("Step 5: Waiting for the BlockDevice to be deleted after device loss")
		Eventually(func(g Gomega) {
			var bd v1alpha1.BlockDevice
			err := k8sClient.Get(ctx, client.ObjectKey{Name: newBD.Name}, &bd)
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue(),
				"BlockDevice %s should be deleted after the backing disk disappears; current err=%v consumable=%t node=%s path=%s",
				newBD.Name, err, bd.Status.Consumable, bd.Status.NodeName, bd.Status.Path)
		}, bdrecReconcileWait, bdrecReconcilePoll).Should(Succeed())
		By(fmt.Sprintf("BlockDevice %s was deleted after the disk disappeared", newBD.Name))
	})

	Context("Manual BlockDevice creation and modification", func() {
		It("Should delete a manually created BlockDevice that does not correspond to a real device", func() {
			fakeBDName := fmt.Sprintf("%s%d", bdrecFakeBDPrefix, rand.Intn(100000))

			By(fmt.Sprintf("Step 1: Creating fake BlockDevice %s with nodeName=%s", fakeBDName, targetNode))
			fakeBD := &v1alpha1.BlockDevice{
				ObjectMeta: metav1.ObjectMeta{
					Name: fakeBDName,
					Labels: map[string]string{
						"kubernetes.io/hostname":      targetNode,
						"kubernetes.io/metadata.name": fakeBDName,
					},
				},
			}
			err := k8sClient.Create(ctx, fakeBD)
			if apierrors.IsForbidden(err) || apierrors.IsInvalid(err) {
				errMsg := strings.ToLower(err.Error())
				isManualProtection := strings.Contains(errMsg, "manual") ||
					strings.Contains(errMsg, "prohibit") ||
					strings.Contains(errMsg, "blockdevice") ||
					strings.Contains(errMsg, "managed by controller")
				Expect(isManualProtection).To(BeTrue(),
					"API rejected BlockDevice creation, but the error does not look like manual-management protection (could be RBAC/schema issue): %v", err)
				By(fmt.Sprintf("API correctly rejected manual BlockDevice creation: %v", err))
				return
			}
			Expect(err).NotTo(HaveOccurred(), "create fake BlockDevice")
			bdrecTrackedBDNames = append(bdrecTrackedBDNames, fakeBDName)

			By("Step 2: Updating fake BlockDevice status (consumable=true, real node, fake path)")
			Expect(k8sClient.Get(ctx, client.ObjectKey{Name: fakeBDName}, fakeBD)).To(Succeed())
			fakeBD.Status = v1alpha1.BlockDeviceStatus{
				NodeName:   targetNode,
				Consumable: true,
				Path:       bdrecFakeDevicePath,
				Size:       resource.MustParse("1Gi"),
				Type:       "disk",
				MachineID:  "e2e-fake-machine-id",
			}
			err = k8sClient.Update(ctx, fakeBD)
			if err != nil {
				err = k8sClient.Status().Update(ctx, fakeBD)
			}
			Expect(err).NotTo(HaveOccurred(), "set status on fake BlockDevice")

			By("Step 3: Restarting the agent on the target node to trigger a rescan")
			Expect(restartAgentOnNode(ctx, cl, targetNode)).To(Succeed())

			By("Step 4: Waiting for the agent to delete the fake BlockDevice (up to 5 minutes)")
			Eventually(func(g Gomega) {
				var bd v1alpha1.BlockDevice
				err := k8sClient.Get(ctx, client.ObjectKey{Name: fakeBDName}, &bd)
				g.Expect(apierrors.IsNotFound(err)).To(BeTrue(),
					"fake BlockDevice %s should be deleted by the agent; current state: err=%v, consumable=%t, nodeName=%s",
					fakeBDName, err, bd.Status.Consumable, bd.Status.NodeName)
			}, bdrecReconcileWait, bdrecReconcilePoll).Should(Succeed())
			By(fmt.Sprintf("Fake BlockDevice %s was deleted by the agent", fakeBDName))
		})

		It("Should revert manual modifications to an existing BlockDevice status", func() {
			By("Step 1: Finding a consumable BlockDevice on the target node")
			consumable, listErr := kubernetes.GetConsumableBlockDevicesByNode(ctx, cl.RESTConfig(), targetNode)
			Expect(listErr).NotTo(HaveOccurred())
			if len(consumable) == 0 {
				Skip("No consumable BlockDevices on the target node to test modification revert")
			}

			var targetBD v1alpha1.BlockDevice
			found := false
			for i := range consumable {
				var bd v1alpha1.BlockDevice
				if err := k8sClient.Get(ctx, client.ObjectKey{Name: consumable[i].Name}, &bd); err != nil {
					continue
				}
				if bd.Status.Path != "" && bd.Status.Size.Value() > 0 && bd.Status.Consumable {
					targetBD = bd
					found = true
					break
				}
			}
			if !found {
				Skip("No consumable BlockDevice with valid path and size found")
			}

			originalSize := targetBD.Status.Size.DeepCopy()
			originalPath := targetBD.Status.Path
			By(fmt.Sprintf("Target BD: %s (node=%s, path=%s, size=%s)",
				targetBD.Name, targetBD.Status.NodeName, originalPath, originalSize.String()))

			DeferCleanup(func() {
				var bd v1alpha1.BlockDevice
				if err := k8sClient.Get(ctx, client.ObjectKey{Name: targetBD.Name}, &bd); err != nil {
					return
				}
				if bd.Status.Size.Equal(originalSize) {
					return
				}
				bd.Status.Size = originalSize
				if err := k8sClient.Update(ctx, &bd); err != nil {
					_ = k8sClient.Status().Update(ctx, &bd)
				}
			})

			By("Step 2: Modifying BlockDevice status.size to a fake value")
			var bdToModify v1alpha1.BlockDevice
			Expect(k8sClient.Get(ctx, client.ObjectKey{Name: targetBD.Name}, &bdToModify)).To(Succeed())
			fakeSize := resource.MustParse("999Ti")
			bdToModify.Status.Size = fakeSize
			err := k8sClient.Status().Update(ctx, &bdToModify)
			if err != nil {
				GinkgoWriter.Printf("    Could not modify BD status (may lack permissions): %v\n", err)
				Skip("Cannot update BlockDevice status: " + err.Error())
			}

			var modified v1alpha1.BlockDevice
			Expect(k8sClient.Get(ctx, client.ObjectKey{Name: targetBD.Name}, &modified)).To(Succeed())
			Expect(modified.Status.Size.Equal(fakeSize)).To(BeTrue(),
				"size should be modified to %s, got %s", fakeSize.String(), modified.Status.Size.String())
			By(fmt.Sprintf("Size temporarily modified to %s", modified.Status.Size.String()))

			By("Step 3: Restarting the agent on the target node to trigger a rescan")
			Expect(restartAgentOnNode(ctx, cl, targetBD.Status.NodeName)).To(Succeed())

			By("Step 4: Waiting for the agent to revert the size to the real value (up to 5 minutes)")
			Eventually(func(g Gomega) {
				var bd v1alpha1.BlockDevice
				g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: targetBD.Name}, &bd)).To(Succeed())
				g.Expect(bd.Status.Size.Equal(originalSize)).To(BeTrue(),
					"agent should have reverted size to original %s; current size=%s",
					originalSize.String(), bd.Status.Size.String())
			}, bdrecReconcileWait, bdrecReconcilePoll).Should(Succeed())

			var reverted v1alpha1.BlockDevice
			Expect(k8sClient.Get(ctx, client.ObjectKey{Name: targetBD.Name}, &reverted)).To(Succeed())
			By(fmt.Sprintf("Agent reverted size: %s (original was %s)", reverted.Status.Size.String(), originalSize.String()))
			Expect(reverted.Status.Size.Equal(originalSize)).To(BeTrue(),
				"size should be restored to exact original value %s, got %s", originalSize.String(), reverted.Status.Size.String())
			Expect(reverted.Status.Path).To(Equal(originalPath), "path should remain unchanged")
		})
	})
})
