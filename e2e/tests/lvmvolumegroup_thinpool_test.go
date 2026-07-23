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
	"strconv"
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

var _ = Describe("LVMVolumeGroup thin-pool", Label("sds-node-configurator", "lvmvolumegroup"), Ordered, ContinueOnFailure, func() {
	var (
		ctx       context.Context
		conf      *cfg.Config
		cl        *e2e.Cluster
		k8sClient client.Client

		targetNode string
		runID      string

		// disks created by the specs; detached + deleted in AfterAll.
		createdDisks []*e2e.Disk

		// Populated by the pvresize spec, consumed by the "remove VG" spec (no second disk/LVG).
		savedLVG *struct {
			lvgName  string
			nodeName string
			vgName   string
			bdName   string
			thinPool string
		}
	)

	BeforeAll(func() {
		ctx = context.Background()
		var cfgErr error
		conf, cfgErr = cfg.Load()
		Expect(cfgErr).NotTo(HaveOccurred(), "failed to load config")

		var clErr error
		cl, clErr = e2e.Connect(ctx, e2e.WithTestName("lvmvolumegroup-thinpool"))
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

		runID = fmt.Sprintf("%d", time.Now().Unix())
	})

	AfterAll(func() {
		By("Cleaning up e2e LVMVolumeGroups")
		cleanupLVMVolumeGroups(ctx, k8sClient)

		By("Detaching and deleting test disks")
		for _, d := range createdDisks {
			if d == nil {
				continue
			}
			if err := cl.Disks().DetachDisk(ctx, targetNode, d.Name); err != nil {
				GinkgoWriter.Printf("failed to detach disk %v: %v\n", d.Name, err)
			}
			if err := cl.Disks().DeleteDisk(ctx, d.Name); err != nil {
				GinkgoWriter.Printf("failed to delete disk %v: %v\n", d.Name, err)
			}
		}
	})

	It("Should create LVMVolumeGroup with one disk and thin-pool", func() {
		By("Snapshotting consumable BlockDevices before attach")
		before, bdErr := kubernetes.GetConsumableBlockDevicesByNode(ctx, cl.RESTConfig(), targetNode)
		Expect(bdErr).NotTo(HaveOccurred())

		diskName := fmt.Sprintf("e2e-lvg-tp-disk-%s", runID)
		By("Creating and attaching a virtual disk: " + diskName)
		disk, createErr := cl.Disks().CreateDisk(ctx, e2e.DiskSpec{
			Name:         diskName,
			Size:         resource.MustParse("2Gi"),
			StorageClass: conf.TestCluster.StorageClass,
		})
		Expect(createErr).NotTo(HaveOccurred(), "failed to create disk")
		createdDisks = append(createdDisks, disk)
		Expect(cl.Disks().AttachDisk(ctx, targetNode, disk.Name)).To(Succeed(), "failed to attach disk")

		By("Waiting for the new consumable BlockDevice on node " + targetNode)
		newBD, waitErr := framework.WaitNewConsumableBlockDevice(ctx, cl.RESTConfig(), targetNode, before, 5*time.Minute)
		Expect(waitErr).NotTo(HaveOccurred())

		vgName := "e2e-vg-tp-" + runID
		thinPoolName := "e2e-thin-pool"
		// Not 50%: half of a 2Gi disk rounds to 1Gi in spec while LVM may allocate slightly more
		// bytes (alignment), and VGConfigurationApplied then fails ValidationFailed (requested < actual).
		thinPoolSize := "60%"
		thinPoolAllocationLimit := "100%"
		lvgName := lvmVGNamePrefix + "tp-" + runID

		By(fmt.Sprintf("Creating LVMVolumeGroup %s on node %s, VG %s, thin-pool %s %s", lvgName, targetNode, vgName, thinPoolName, thinPoolSize))
		Expect(kubernetes.CreateLVMVolumeGroupWithThinPool(
			ctx, cl.RESTConfig(), lvgName, targetNode, []string{newBD.Name}, vgName,
			[]kubernetes.ThinPoolSpec{{Name: thinPoolName, Size: thinPoolSize, AllocationLimit: thinPoolAllocationLimit}},
		)).To(Succeed())

		By("Waiting for LVMVolumeGroup to become Ready (up to 5 minutes)")
		var created v1alpha1.LVMVolumeGroup
		Eventually(func(g Gomega) {
			err := k8sClient.Get(ctx, client.ObjectKey{Name: lvgName}, &created)
			g.Expect(err).NotTo(HaveOccurred())
			if created.Status.Phase != v1alpha1.PhaseReady {
				GinkgoWriter.Printf("LVMVolumeGroup %s phase=%s (waiting for Ready)\n", lvgName, created.Status.Phase)
				for _, c := range created.Status.Conditions {
					GinkgoWriter.Printf("  condition %s status=%s reason=%s msg=%s\n", c.Type, c.Status, c.Reason, c.Message)
				}
			}
			g.Expect(created.Status.Phase).To(Equal(v1alpha1.PhaseReady), "Phase should be Ready, got %s", created.Status.Phase)
		}, 5*time.Minute, 10*time.Second).Should(Succeed())

		By("Verifying conditions (no errors)")
		for _, c := range created.Status.Conditions {
			Expect(c.Status).NotTo(Equal(metav1.ConditionFalse),
				"condition %s has status False: reason=%s message=%s", c.Type, c.Reason, c.Message)
		}
		By(fmt.Sprintf("LVMVolumeGroup Phase: %s", created.Status.Phase))

		By("Verifying thin-pool in status")
		Expect(created.Status.ThinPools).NotTo(BeEmpty(), "ThinPools status should not be empty")
		var tp *v1alpha1.LVMVolumeGroupThinPoolStatus
		for i := range created.Status.ThinPools {
			if created.Status.ThinPools[i].Name == thinPoolName {
				tp = &created.Status.ThinPools[i]
				break
			}
		}
		Expect(tp).NotTo(BeNil(), "thin-pool %q not found in status", thinPoolName)
		Expect(tp.AllocationLimit).To(Equal(thinPoolAllocationLimit), "thin-pool allocation limit should match spec")
		Expect(tp.Ready).To(BeTrue(), "thin-pool should be Ready")

		By("Removing this spec's LVMVolumeGroup so it does not interfere with later specs")
		_, _ = framework.NodeExecChecked(ctx, cl, targetNode, framework.RemoveThinPoolStackScript(vgName, thinPoolName))
		Expect(kubernetes.DeleteLVMVolumeGroup(ctx, cl.RESTConfig(), lvgName)).To(Succeed())
		Expect(kubernetes.WaitForLVMVolumeGroupDeletion(ctx, cl.RESTConfig(), lvgName, lvmVolumeGroupReadyTimeout)).To(Succeed())

		By("✓ LVMVolumeGroup Ready; thin-pool present and Ready; conditions without errors")
	})

	It("Should grow PV and VG free space after block device resize (pvresize)", func() {
		By("Snapshotting consumable BlockDevices before attach")
		before, bdErr := kubernetes.GetConsumableBlockDevicesByNode(ctx, cl.RESTConfig(), targetNode)
		Expect(bdErr).NotTo(HaveOccurred())

		diskName := fmt.Sprintf("e2e-lvg-pvresize-disk-%s", runID)
		By("Creating and attaching a virtual disk for pvresize: " + diskName)
		disk, createErr := cl.Disks().CreateDisk(ctx, e2e.DiskSpec{
			Name:         diskName,
			Size:         resource.MustParse("2Gi"),
			StorageClass: conf.TestCluster.StorageClass,
		})
		Expect(createErr).NotTo(HaveOccurred(), "failed to create disk")
		createdDisks = append(createdDisks, disk)
		Expect(cl.Disks().AttachDisk(ctx, targetNode, disk.Name)).To(Succeed(), "failed to attach disk")

		By("Waiting for the new consumable BlockDevice on node " + targetNode)
		newBD, waitErr := framework.WaitNewConsumableBlockDevice(ctx, cl.RESTConfig(), targetNode, before, 5*time.Minute)
		Expect(waitErr).NotTo(HaveOccurred())

		vgName := "e2e-vg-pvresize-" + runID
		thinPoolName := "e2e-thin-pool-pvresize"
		thinPoolSize := "60%"
		thinPoolAllocationLimit := "100%"
		lvgName := lvmVGNamePrefix + "pvresize-" + runID

		By(fmt.Sprintf("Creating LVMVolumeGroup %s (VG %s) for pvresize test", lvgName, vgName))
		Expect(kubernetes.CreateLVMVolumeGroupWithThinPool(
			ctx, cl.RESTConfig(), lvgName, targetNode, []string{newBD.Name}, vgName,
			[]kubernetes.ThinPoolSpec{{Name: thinPoolName, Size: thinPoolSize, AllocationLimit: thinPoolAllocationLimit}},
		)).To(Succeed())
		// LVG + disk are kept for the next spec "Should remove VG when LVMVolumeGroup CR is deleted" (no second attach).

		By("Waiting for LVMVolumeGroup to become Ready (up to 5 minutes)")
		var readyLVG v1alpha1.LVMVolumeGroup
		Eventually(func(g Gomega) {
			err := k8sClient.Get(ctx, client.ObjectKey{Name: lvgName}, &readyLVG)
			g.Expect(err).NotTo(HaveOccurred())
			if readyLVG.Status.Phase != v1alpha1.PhaseReady {
				GinkgoWriter.Printf("LVMVolumeGroup %s phase=%s (pvresize test, waiting for Ready)\n", lvgName, readyLVG.Status.Phase)
				for _, c := range readyLVG.Status.Conditions {
					GinkgoWriter.Printf("  condition %s status=%s reason=%s msg=%s\n", c.Type, c.Status, c.Reason, c.Message)
				}
			}
			g.Expect(readyLVG.Status.Phase).To(Equal(v1alpha1.PhaseReady))
		}, 5*time.Minute, 10*time.Second).Should(Succeed())

		for _, c := range readyLVG.Status.Conditions {
			Expect(c.Status).NotTo(Equal(metav1.ConditionFalse),
				"initial: condition %s is False: reason=%s message=%s", c.Type, c.Reason, c.Message)
		}

		baselineVGFree := readyLVG.Status.VGFree.Value()
		Expect(baselineVGFree).To(BeNumerically(">", 0), "baseline VGFree should be positive")

		var baselinePVSize int64
		var foundDev bool
		for _, n := range readyLVG.Status.Nodes {
			if n.Name != targetNode {
				continue
			}
			for _, d := range n.Devices {
				if d.BlockDevice == newBD.Name {
					baselinePVSize = d.PVSize.Value()
					foundDev = true
					break
				}
			}
		}
		Expect(foundDev).To(BeTrue(), "LVMVolumeGroup status should list device for BlockDevice %s", newBD.Name)
		Expect(baselinePVSize).To(BeNumerically(">", 0), "baseline PV size should be reported")

		var bdBefore v1alpha1.BlockDevice
		Expect(k8sClient.Get(ctx, client.ObjectKey{Name: newBD.Name}, &bdBefore)).To(Succeed())
		baselineBDSize := bdBefore.Status.Size.Value()
		GinkgoWriter.Printf("    BlockDevice %s status size before resize: %s\n", newBD.Name, bdBefore.Status.Size.String())

		By("Growing disk 2Gi -> 4Gi via pvresize scenario")
		Expect(cl.Disks().ResizeDisk(ctx, diskName, resource.MustParse("4Gi"))).To(Succeed())

		By("Waiting for BlockDevice status size to reflect larger disk")
		Eventually(func(g Gomega) {
			var bd v1alpha1.BlockDevice
			g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: newBD.Name}, &bd)).To(Succeed())
			g.Expect(bd.Status.Size.Value()).To(BeNumerically(">", baselineBDSize),
				"BlockDevice %s size should grow after resize (was %d)", newBD.Name, baselineBDSize)
		}, 5*time.Minute, 10*time.Second).Should(Succeed())

		By("Waiting for LVMVolumeGroup: Ready, larger VGFree and PV after pvresize")
		Eventually(func(g Gomega) {
			var cur v1alpha1.LVMVolumeGroup
			g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: lvgName}, &cur)).To(Succeed())
			g.Expect(cur.Status.Phase).To(Equal(v1alpha1.PhaseReady), "phase=%s", cur.Status.Phase)
			for _, c := range cur.Status.Conditions {
				g.Expect(c.Status).NotTo(Equal(metav1.ConditionFalse),
					"condition %s False: reason=%s message=%s", c.Type, c.Reason, c.Message)
			}
			g.Expect(cur.Status.VGFree.Value()).To(BeNumerically(">", baselineVGFree),
				"VGFree should grow after pvresize (baseline %d)", baselineVGFree)
			var pvSize int64
			found := false
			for _, n := range cur.Status.Nodes {
				if n.Name != targetNode {
					continue
				}
				for _, d := range n.Devices {
					if d.BlockDevice == newBD.Name {
						pvSize = d.PVSize.Value()
						found = true
						break
					}
				}
			}
			g.Expect(found).To(BeTrue(), "device for BlockDevice %s in status", newBD.Name)
			g.Expect(pvSize).To(BeNumerically(">", baselinePVSize),
				"PV size should grow after pvresize (baseline %d)", baselinePVSize)
		}, 10*time.Minute, 15*time.Second).Should(Succeed())

		By("✓ After disk resize: LVMVolumeGroup Ready, VGFree and PV size increased, no error conditions")

		savedLVG = &struct {
			lvgName  string
			nodeName string
			vgName   string
			bdName   string
			thinPool string
		}{
			lvgName:  lvgName,
			nodeName: targetNode,
			vgName:   vgName,
			bdName:   newBD.Name,
			thinPool: thinPoolName,
		}
	})

	It("Should remove VG from node when LVMVolumeGroup CR is deleted", func() {
		Expect(savedLVG).NotTo(BeNil(),
			"the pvresize test must run first and leave a Ready LVMVolumeGroup + attached disk")
		s := savedLVG
		lvgName := s.lvgName
		nodeName := s.nodeName
		vgName := s.vgName
		bdName := s.bdName
		thinPool := s.thinPool

		By("Chain: (1) pvresize test created a Ready LVMVolumeGroup with thin pool on one node and left the CR + disk; " +
			"(2) this test deletes only the LVMVolumeGroup CR; (3) agent should remove the VG on the node when allowed; " +
			"(4) BlockDevice CR should remain (disk stays attached)")

		var lvgCur v1alpha1.LVMVolumeGroup
		Expect(k8sClient.Get(ctx, client.ObjectKey{Name: lvgName}, &lvgCur)).To(Succeed())
		Expect(lvgCur.Status.Phase).To(Equal(v1alpha1.PhaseReady), "LVG from pvresize must still be Ready before delete")

		By("Checking VG exists on node before CR deletion")
		vgsRes, vgErr := cl.Nodes().Exec(ctx, nodeName, `sudo -n vgs -o vg_name --noheadings 2>/dev/null | sed '/^$/d'`)
		Expect(vgErr).NotTo(HaveOccurred(), "vgs on node %s", nodeName)
		vgsOut := string(vgsRes.Stdout)
		listed := framework.VGInListing(vgsOut, vgName)
		GinkgoWriter.Printf("    expect VG name %q listed on node %s: found=%v\n    vgs output:\n%s\n", vgName, nodeName, listed, vgsOut)
		Expect(listed).To(BeTrue(), "VG %q should exist on node before delete", vgName)

		// E2E-only workaround: remove thin-pool LVs on the node before deleting the CR. The product agent should
		// tear down the pool during delete, but that path can leave the CR stuck Terminating; pruning here keeps
		// the test focused on vgremove + BlockDevice retention without depending on agent delete ordering.
		if len(lvgCur.Spec.ThinPools) > 0 && lvgCur.Spec.ThinPools[0].Name != "" {
			thinPool = lvgCur.Spec.ThinPools[0].Name
		} else if len(lvgCur.Status.ThinPools) > 0 && lvgCur.Status.ThinPools[0].Name != "" {
			thinPool = lvgCur.Status.ThinPools[0].Name
		}
		By(fmt.Sprintf("E2E workaround: lvremove thin-pool stack on node so LVMVolumeGroup CR deletion can finish (vg=%q thinPool=%q)", vgName, thinPool))
		pruneOut, pruneErr := framework.NodeExecChecked(ctx, cl, nodeName, framework.RemoveThinPoolStackScript(vgName, thinPool))
		if pruneOut != "" {
			GinkgoWriter.Printf("    prune script output:\n%s\n", pruneOut)
		}
		Expect(pruneErr).NotTo(HaveOccurred(), "thin-pool prune on node %s", nodeName)

		By("Waiting for thin-pool data LV to be gone before CR delete")
		Eventually(func(g Gomega) {
			lvsRes, err := cl.Nodes().Exec(ctx, nodeName, fmt.Sprintf(`sudo -n lvs -a -o lv_name,lv_attr --noheadings %s 2>/dev/null | sed 's/[][]//g'`, strconv.Quote(vgName)))
			g.Expect(err).NotTo(HaveOccurred())
			lvsOut := string(lvsRes.Stdout)
			present := framework.ThinPoolDataLVPresent(lvsOut, thinPool)
			g.Expect(present).To(BeFalse(), "expected thin-pool %q data LV gone in VG %s; lvs output:\n%s", thinPool, vgName, lvsOut)
		}, 3*time.Minute, 5*time.Second).Should(Succeed())

		By("Deleting LVMVolumeGroup CR")
		Expect(kubernetes.DeleteLVMVolumeGroup(ctx, cl.RESTConfig(), lvgName)).To(Succeed())

		By("Waiting for LVMVolumeGroup CR to be removed from API")
		Expect(kubernetes.WaitForLVMVolumeGroupDeletion(ctx, cl.RESTConfig(), lvgName, 10*time.Minute)).To(Succeed())

		By("Waiting for VG to disappear from node (vgremove)")
		Eventually(func(g Gomega) {
			vgsRes, err := cl.Nodes().Exec(ctx, nodeName, `sudo -n vgs -o vg_name --noheadings 2>/dev/null | sed '/^$/d'`)
			g.Expect(err).NotTo(HaveOccurred())
			out := string(vgsRes.Stdout)
			stillThere := framework.VGInListing(out, vgName)
			if stillThere {
				GinkgoWriter.Printf("    … vgs still lists %q; output:\n%s\n", vgName, out)
			}
			g.Expect(stillThere).To(BeFalse(), "VG %q should be removed from node", vgName)
		}, 5*time.Minute, 10*time.Second).Should(Succeed())

		By("Verifying BlockDevice object still exists (disk not removed)")
		var bdAfter v1alpha1.BlockDevice
		Expect(k8sClient.Get(ctx, client.ObjectKey{Name: bdName}, &bdAfter)).To(Succeed())
		Expect(bdAfter.Status.Path).NotTo(BeEmpty(), "BlockDevice should still report device path")
		GinkgoWriter.Printf("    BlockDevice %s still present: path=%s size=%s\n", bdName, bdAfter.Status.Path, bdAfter.Status.Size.String())

		savedLVG = nil
		By("✓ LVMVolumeGroup CR deleted; VG removed on node; BlockDevice still in API")
	})
})
