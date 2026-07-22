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
	// lvgaiDiscoveryTimeout bounds how long we wait for the agent to auto-import a tagged VG
	// into an LVMVolumeGroup CR (ported from legacy e2eLVMVolumeGroupAutoImportDiscoveryTimeout).
	lvgaiDiscoveryTimeout = 10 * time.Minute
	// lvgaiBlockDeviceVGLinkageTimeout bounds how long we wait for the BD discoverer to link the
	// device to the manually created VG (ported from legacy e2eBlockDeviceVGLinkageTimeout).
	lvgaiBlockDeviceVGLinkageTimeout = 10 * time.Minute
	// lvgaiTag is the enablement tag the controller watches to adopt a pre-existing VG.
	lvgaiTag = "storage.deckhouse.io/enabled=true"
)

var _ = Describe("LVMVolumeGroup auto-import", Label("sds-node-configurator", "lvmvolumegroup"), Ordered, func() {
	var (
		ctx        context.Context
		conf       *cfg.Config
		cl         *e2e.Cluster
		k8sClient  client.Client
		targetNode string

		// per-spec state (also used by AfterEach cleanup)
		autoImportDisk *e2e.Disk
		bdName         string
		devPath        string
		actualVGName   string
		thinPoolName   string
	)

	BeforeAll(func() {
		ctx = context.Background()
		conf = cfg.Load()

		var clErr error
		cl, clErr = e2e.Connect(ctx, e2e.WithTestName("lvmvolumegroup-autoimport"))
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
		Expect(nlErr).NotTo(HaveOccurred(), "failed to list nodes")
		Expect(nodeList.Items).NotTo(BeEmpty(), "cluster must have at least one node")
		targetNode = nodeList.Items[0].Name

		By("LVM auto-import suite: cleanup stale LVMVolumeGroups before test")
		cleanupLVMVolumeGroups(ctx, k8sClient)
	})

	AfterEach(func() {
		if actualVGName != "" {
			By("auto-import cleanup: vgchange --deltag (stop controller tracking)")
			_, _ = framework.NodeExecChecked(ctx, cl, targetNode,
				fmt.Sprintf(`sudo -n vgchange %q --deltag storage.deckhouse.io/enabled=true 2>&1 || true`, actualVGName))

			deadline := time.Now().Add(5 * time.Minute)
			for time.Now().Before(deadline) {
				var list v1alpha1.LVMVolumeGroupList
				_ = k8sClient.List(ctx, &list, &client.ListOptions{})
				gone := true
				for i := range list.Items {
					if list.Items[i].Spec.ActualVGNameOnTheNode == actualVGName {
						gone = false
						break
					}
				}
				if gone {
					break
				}
				time.Sleep(3 * time.Second)
			}

			if thinPoolName != "" {
				By("auto-import cleanup: remove thin LV stack then VG (best effort)")
				prune := framework.RemoveThinPoolStackScript(actualVGName, thinPoolName)
				_, _ = framework.NodeExecChecked(ctx, cl, targetNode, prune)
			}
			_, _ = framework.NodeExecChecked(ctx, cl, targetNode,
				fmt.Sprintf(`sudo -n vgremove -ff %q 2>&1 || true`, actualVGName))
			if devPath != "" {
				_, _ = framework.NodeExecChecked(ctx, cl, targetNode,
					fmt.Sprintf(`sudo -n pvremove -ff %q 2>&1 || true`, devPath))
			}
		}

		if autoImportDisk != nil {
			if detachErr := cl.Disks().DetachDisk(ctx, targetNode, autoImportDisk.Name); detachErr != nil {
				GinkgoWriter.Printf("failed to detach disk %v: %v\n", autoImportDisk.Name, detachErr)
			}
			if deleteErr := cl.Disks().DeleteDisk(ctx, autoImportDisk.Name); deleteErr != nil {
				GinkgoWriter.Printf("failed to delete disk %v: %v\n", autoImportDisk.Name, deleteErr)
			}
			autoImportDisk = nil
		}

		if bdName != "" {
			forceDeleteBlockDevicesByNames(ctx, k8sClient, []string{bdName})
		}
		cleanupLVMVolumeGroups(ctx, k8sClient)

		bdName, devPath, actualVGName, thinPoolName = "", "", "", ""
	})

	It("Should discover LVMVolumeGroup for manually created VG with tag; thin-pool in status; controller management", func() {
		storageClass := conf.TestCluster.StorageClass
		Expect(storageClass).NotTo(BeEmpty(), "storage class must be configured")

		runID := fmt.Sprintf("%d", time.Now().Unix())
		manualVG := fmt.Sprintf("e2e-vgimport-%s", runID)
		thinPoolLV := fmt.Sprintf("e2e-tp-import-%s", runID)
		diskName := fmt.Sprintf("e2e-vgimport-disk-%s", runID)

		By("Recording consumable BlockDevices before attach")
		before, beforeErr := kubernetes.GetConsumableBlockDevicesByNode(ctx, cl.RESTConfig(), targetNode)
		Expect(beforeErr).NotTo(HaveOccurred(), "failed to list consumable block devices")

		By("Attaching a dedicated disk for on-node manual VG + thin-pool: " + diskName)
		disk, createErr := cl.Disks().CreateDisk(ctx, e2e.DiskSpec{
			Name:         diskName,
			Size:         resource.MustParse("3Gi"),
			StorageClass: storageClass,
		})
		Expect(createErr).NotTo(HaveOccurred(), "failed to create disk")
		autoImportDisk = disk
		Expect(cl.Disks().AttachDisk(ctx, targetNode, disk.Name)).To(Succeed(), "failed to attach disk")

		By("Waiting for the new consumable BlockDevice to appear on the node")
		targetBD, bdErr := framework.WaitNewConsumableBlockDevice(ctx, cl.RESTConfig(), targetNode, before, 5*time.Minute)
		Expect(bdErr).NotTo(HaveOccurred(), "new consumable block device should appear after attach")
		devPath = strings.TrimSpace(targetBD.Path)
		Expect(devPath).NotTo(BeEmpty(), "BlockDevice must report device path for LVM on node")
		bdName = targetBD.Name
		actualVGName = manualVG
		thinPoolName = thinPoolLV

		By(fmt.Sprintf("On node %s: pvcreate → vgcreate %s → thin-pool %s → vgchange --addtag %s", targetNode, manualVG, thinPoolLV, lvgaiTag))
		lvmScript := fmt.Sprintf(`set -e
DEV=%q
VG=%q
TP=%q
sudo -n pvcreate -y "$DEV" 2>&1
sudo -n vgcreate "$VG" "$DEV" 2>&1
sudo -n lvcreate -L 700M -T "$VG/$TP" 2>&1
sudo -n vgchange "$VG" --addtag storage.deckhouse.io/enabled=true 2>&1
`, devPath, manualVG, thinPoolLV)
		out, errLvm := framework.NodeExecChecked(ctx, cl, targetNode, lvmScript)
		if out != "" {
			GinkgoWriter.Printf("    on-node LVM script output:\n%s\n", out)
		}
		Expect(errLvm).NotTo(HaveOccurred(), "create VG + thin-pool + tag on node %s", targetNode)

		By("Triggering LVM inventory rescan on node (pvscan + udev) so agent cache can see the new VG")
		framework.TriggerLVMDiscovery(ctx, cl, targetNode)

		By(fmt.Sprintf("Waiting for BlockDevice %s to be linked to VG %s (up to %s)", bdName, manualVG, lvgaiBlockDeviceVGLinkageTimeout))
		Eventually(func(g Gomega) {
			var bd v1alpha1.BlockDevice
			g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: bdName}, &bd)).To(Succeed())
			g.Expect(strings.TrimSpace(bd.Status.ActualVGNameOnTheNode)).To(Equal(manualVG),
				"BlockDevice %s should be linked to VG %s; got %q (consumable=%v path=%s)",
				bdName, manualVG, bd.Status.ActualVGNameOnTheNode, bd.Status.Consumable, bd.Status.Path)
		}, lvgaiBlockDeviceVGLinkageTimeout, 10*time.Second).Should(Succeed())

		By("Restarting sds-node-configurator agent on the node (nested CI often misses udev; same pattern as BD rescan tests)")
		Expect(restartAgentOnNode(ctx, cl, targetNode)).To(Succeed(), "agent restart on node %s", targetNode)

		var lvgName string
		By(fmt.Sprintf("Waiting for agent to create LVMVolumeGroup CR (auto-import) for tagged VG on node (up to %s)", lvgaiDiscoveryTimeout))
		Eventually(func(g Gomega) {
			var list v1alpha1.LVMVolumeGroupList
			g.Expect(k8sClient.List(ctx, &list, &client.ListOptions{})).To(Succeed())
			found := false
			for i := range list.Items {
				if list.Items[i].Spec.ActualVGNameOnTheNode == manualVG {
					lvgName = list.Items[i].Name
					found = true
					break
				}
			}
			g.Expect(found).To(BeTrue(), "expected a LVMVolumeGroup whose spec.actualVGNameOnTheNode is %q (auto-discovered from tagged VG)", manualVG)
		}, lvgaiDiscoveryTimeout, 5*time.Second).Should(Succeed())
		Expect(lvgName).NotTo(BeEmpty())

		By("Waiting for LVMVolumeGroup Ready; thin-pool in status; controller has applied configuration")
		Eventually(func(g Gomega) {
			var cur v1alpha1.LVMVolumeGroup
			g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: lvgName}, &cur)).To(Succeed())
			g.Expect(cur.Status.Phase).To(Equal(v1alpha1.PhaseReady), "LVMVolumeGroup %s (auto-import) should reach Ready; phase=%s", lvgName, cur.Status.Phase)
			g.Expect(len(cur.Status.ThinPools) > 0).To(BeTrue(), "status should list at least one thin-pool; got ThinPools=%v", cur.Status.ThinPools)
			tpoolOK := false
			for i := range cur.Status.ThinPools {
				if strings.TrimSpace(cur.Status.ThinPools[i].Name) == thinPoolLV {
					tpoolOK = true
					break
				}
			}
			if !tpoolOK && len(cur.Status.ThinPools) > 0 {
				GinkgoWriter.Printf("    thin-pool name in status differs from %q: %#v\n", thinPoolLV, cur.Status.ThinPools)
			}
			g.Expect(tpoolOK).To(BeTrue(), "status.thinPools should include thin-pool %q (or align with node LV name); got: %+v", thinPoolLV, cur.Status.ThinPools)
			var cfgCond *metav1.Condition
			for i := range cur.Status.Conditions {
				if cur.Status.Conditions[i].Type == "VGConfigurationApplied" {
					cfgCond = &cur.Status.Conditions[i]
					break
				}
			}
			g.Expect(cfgCond).NotTo(BeNil(), "expected VGConfigurationApplied condition")
			g.Expect(cfgCond.Status).To(Equal(metav1.ConditionTrue), "controller should have applied / reconciled config for imported VG: reason=%s msg=%s", cfgCond.Reason, cfgCond.Message)
		}, lvmVolumeGroupReadyTimeout, 8*time.Second).Should(Succeed())

		By("✓ Auto-import: LVMVolumeGroup exists, Ready, thin-pool in status, VGConfigurationApplied True")
	})
})
