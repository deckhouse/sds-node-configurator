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
	lvgextDisk1Size = "2Gi"
	lvgextDisk2Size = "3Gi"

	lvgextShrinkOrigDiskSize  = "4Gi"
	lvgextShrinkSmallDiskSize = "1Gi"

	// lvgextBDLinkageTimeout bounds how long the BD discoverer takes to link a new PV's BlockDevice to the VG.
	lvgextBDLinkageTimeout = 5 * time.Minute
)

// lvgextNodeSafe converts a node name into a DNS-1123-safe fragment for CR names.
func lvgextNodeSafe(n string) string {
	return strings.ReplaceAll(strings.ReplaceAll(n, ".", "-"), "_", "-")
}

// lvgextDevicesOnLVGNode returns the set of BlockDevice CR names listed under status.nodes[node].devices.
func lvgextDevicesOnLVGNode(lvg *v1alpha1.LVMVolumeGroup, nodeName string) map[string]struct{} {
	out := make(map[string]struct{})
	for _, n := range lvg.Status.Nodes {
		if n.Name != nodeName {
			continue
		}
		for _, d := range n.Devices {
			if d.BlockDevice != "" {
				out[d.BlockDevice] = struct{}{}
			}
		}
	}
	return out
}

// lvgextCountDevicesOnLVGNode counts distinct BlockDevices the LVMVolumeGroup status reports for the node.
func lvgextCountDevicesOnLVGNode(lvg *v1alpha1.LVMVolumeGroup, nodeName string) int {
	return len(lvgextDevicesOnLVGNode(lvg, nodeName))
}

// lvgextWaitBlockDeviceLinkedToVG waits until the BlockDevice CR reports it belongs to vgName
// (status.actualVGNameOnTheNode) with PV/VG UUIDs populated by the agent BD discoverer.
func lvgextWaitBlockDeviceLinkedToVG(ctx context.Context, cl client.Client, bdName, vgName string, timeout time.Duration) {
	Eventually(func(g Gomega) {
		var bd v1alpha1.BlockDevice
		g.Expect(cl.Get(ctx, client.ObjectKey{Name: bdName}, &bd)).To(Succeed())
		g.Expect(strings.TrimSpace(bd.Status.ActualVGNameOnTheNode)).To(Equal(vgName),
			"BlockDevice %s should report status.actualVGNameOnTheNode=%q; got %q, pvUuid=%q vgUuid=%q consumable=%v path=%q",
			bdName, vgName, bd.Status.ActualVGNameOnTheNode, bd.Status.PVUuid, bd.Status.VGUuid, bd.Status.Consumable, bd.Status.Path)
		g.Expect(strings.TrimSpace(bd.Status.VGUuid)).NotTo(BeEmpty(), "BlockDevice %s should have status.vgUuid set", bdName)
		g.Expect(strings.TrimSpace(bd.Status.PVUuid)).NotTo(BeEmpty(), "BlockDevice %s should have status.pvUuid set", bdName)
	}, timeout, 5*time.Second).Should(Succeed())
}

// lvgextDeleteLVG best-effort deletes an LVMVolumeGroup CR, stripping finalizers so cleanup never blocks the suite.
func lvgextDeleteLVG(ctx context.Context, cl client.Client, name string) {
	if cl == nil || name == "" {
		return
	}
	lvg := &v1alpha1.LVMVolumeGroup{}
	if err := cl.Get(ctx, client.ObjectKey{Name: name}, lvg); err != nil {
		return
	}
	if len(lvg.Finalizers) > 0 {
		lvg.Finalizers = nil
		_ = cl.Update(ctx, lvg)
	}
	_ = cl.Delete(ctx, lvg)
}

var _ = Describe("LVMVolumeGroup lifecycle with a second disk", Label("sds-node-configurator", "lvmvolumegroup"), Ordered, ContinueOnFailure, func() {
	var (
		ctx        context.Context
		conf       *cfg.Config
		cl         *e2e.Cluster
		k8sClient  client.Client
		targetNode string
	)

	BeforeAll(func() {
		By("Preparing shared test context and Kubernetes clients")
		ctx = context.Background()
		var cfgErr error
		conf, cfgErr = cfg.Load()
		Expect(cfgErr).NotTo(HaveOccurred(), "failed to load config")

		var clErr error
		cl, clErr = e2e.Connect(ctx, e2e.WithTestName("lvmvolumegroup-extend"))
		Expect(clErr).NotTo(HaveOccurred(), "failed to connect to cluster")
		DeferCleanup(func() {
			if err := cl.Close(context.Background()); err != nil {
				GinkgoWriter.Println("Error closing cluster: ", err)
			}
		})

		var k8sErr error
		k8sClient, k8sErr = sdsclient.New(cl.RESTConfig())
		Expect(k8sErr).NotTo(HaveOccurred(), "failed to build controller-runtime client")

		By("Listing nodes to select the target node")
		nodeList, nlErr := cl.Clientset().CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		Expect(nlErr).NotTo(HaveOccurred(), "failed to list nodes")
		Expect(nodeList.Items).NotTo(BeEmpty(), "cluster must have at least one node")
		targetNode = nodeList.Items[0].Name

		Expect(conf.TestCluster.StorageClass).NotTo(BeEmpty(), "TestCluster.StorageClass required")
	})

	AfterAll(func() {
		if k8sClient != nil {
			cleanupLVMVolumeGroups(ctx, k8sClient)
		}
	})

	Context("LVMVolumeGroup extend", func() {
		It("Should vgextend LVMVolumeGroup when a second disk is added to BlockDeviceSelector", func() {
			runID := fmt.Sprintf("%d", time.Now().Unix())
			vgName := fmt.Sprintf("e2e-vg-extend-%s", runID)
			lvgName := fmt.Sprintf("%sextend-%s-%s", lvmVGNamePrefix, runID, lvgextNodeSafe(targetNode))

			By("Step 1: attach the first disk and create an LVMVolumeGroup on a single BlockDevice")
			before1, err := kubernetes.GetConsumableBlockDevicesByNode(ctx, cl.RESTConfig(), targetNode)
			Expect(err).NotTo(HaveOccurred())

			disk1Name := fmt.Sprintf("e2e-lvg-extend-disk1-%s", runID)
			disk1, err := cl.Disks().CreateDisk(ctx, e2e.DiskSpec{
				Name:         disk1Name,
				Size:         resource.MustParse(lvgextDisk1Size),
				StorageClass: conf.TestCluster.StorageClass,
			})
			Expect(err).NotTo(HaveOccurred(), "failed to create first disk")
			DeferCleanup(func() {
				_ = cl.Disks().DetachDisk(ctx, targetNode, disk1.Name)
				_ = cl.Disks().DeleteDisk(ctx, disk1.Name)
			})
			Expect(cl.Disks().AttachDisk(ctx, targetNode, disk1.Name)).To(Succeed(), "failed to attach first disk")

			bd1, err := framework.WaitNewConsumableBlockDevice(ctx, cl.RESTConfig(), targetNode, before1, 5*time.Minute)
			Expect(err).NotTo(HaveOccurred(), "first consumable BlockDevice not discovered")

			Expect(kubernetes.CreateLVMVolumeGroup(ctx, cl.RESTConfig(), lvgName, targetNode, []string{bd1.Name}, vgName)).
				To(Succeed(), "failed to create LVMVolumeGroup")
			DeferCleanup(func() { lvgextDeleteLVG(ctx, k8sClient, lvgName) })

			var readyOneDisk v1alpha1.LVMVolumeGroup
			Eventually(func(g Gomega) {
				g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: lvgName}, &readyOneDisk)).To(Succeed())
				g.Expect(readyOneDisk.Status.Phase).To(Equal(v1alpha1.PhaseReady))
			}, lvmVolumeGroupReadyTimeout, 10*time.Second).Should(Succeed())

			baselineVGFree := readyOneDisk.Status.VGFree.Value()
			baselineVGSize := readyOneDisk.Status.VGSize.Value()
			Expect(baselineVGFree).To(BeNumerically(">", 0))
			Expect(baselineVGSize).To(BeNumerically(">", 0))
			Expect(lvgextCountDevicesOnLVGNode(&readyOneDisk, targetNode)).To(Equal(1),
				"status should list one device before extend")
			GinkgoWriter.Printf("    LVMVolumeGroup with one PV: VGSize=%s VGFree=%s\n",
				readyOneDisk.Status.VGSize.String(), readyOneDisk.Status.VGFree.String())

			By("Step 2: attach the second disk and wait for a new consumable BlockDevice on the same node")
			before2, err := kubernetes.GetConsumableBlockDevicesByNode(ctx, cl.RESTConfig(), targetNode)
			Expect(err).NotTo(HaveOccurred())

			disk2Name := fmt.Sprintf("e2e-lvg-extend-disk2-%s", runID)
			disk2, err := cl.Disks().CreateDisk(ctx, e2e.DiskSpec{
				Name:         disk2Name,
				Size:         resource.MustParse(lvgextDisk2Size),
				StorageClass: conf.TestCluster.StorageClass,
			})
			Expect(err).NotTo(HaveOccurred(), "failed to create second disk")
			DeferCleanup(func() {
				_ = cl.Disks().DetachDisk(ctx, targetNode, disk2.Name)
				_ = cl.Disks().DeleteDisk(ctx, disk2.Name)
			})
			Expect(cl.Disks().AttachDisk(ctx, targetNode, disk2.Name)).To(Succeed(), "failed to attach second disk")

			bd2, err := framework.WaitNewConsumableBlockDevice(ctx, cl.RESTConfig(), targetNode, before2, 5*time.Minute)
			Expect(err).NotTo(HaveOccurred(), "second consumable BlockDevice not discovered")
			Expect(bd2.Name).NotTo(Equal(bd1.Name), "BlockDevice selectors must be distinct")

			By("Step 3: patch LVMVolumeGroup BlockDeviceSelector to include both BlockDevices")
			var cur v1alpha1.LVMVolumeGroup
			Expect(k8sClient.Get(ctx, client.ObjectKey{Name: lvgName}, &cur)).To(Succeed())
			cur.Spec.BlockDeviceSelector = &metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{
					{
						Key:      "kubernetes.io/metadata.name",
						Operator: metav1.LabelSelectorOpIn,
						Values:   []string{bd1.Name, bd2.Name},
					},
				},
			}
			Expect(k8sClient.Update(ctx, &cur)).To(Succeed())

			By("Step 4a: nudge agent/LVM scan after selector patch (vgextend + BD discoverer race on CI)")
			framework.TriggerLVMDiscovery(ctx, cl, targetNode)
			Expect(restartAgentOnNode(ctx, cl, targetNode)).To(Succeed())

			By("Step 4b: wait for two PVs in VG on the node (reconciler vgextend)")
			Eventually(func(g Gomega) {
				pvsRes, errSSH := cl.Nodes().Exec(ctx, targetNode, fmt.Sprintf(`sudo -n pvs -o pv_name --noheadings -S vg_name --select vg_name=%s 2>/dev/null | sed '/^$/d'`, strconv.Quote(vgName)))
				out := string(pvsRes.Stdout)
				if errSSH != nil {
					GinkgoWriter.Printf("    vgextend pvs: err=%v out=%q\n", errSSH, out)
				}
				g.Expect(errSSH).NotTo(HaveOccurred())
				pvCount := framework.CountPVsInVG(out)
				g.Expect(pvCount).To(Equal(2), "VG %q should have 2 PVs; pvs: %q", vgName, strings.TrimSpace(out))
			}, 10*time.Minute, 10*time.Second).Should(Succeed())

			By(fmt.Sprintf("Step 4c: wait for BlockDevice %s linked to VG %s (BD discoverer after vgextend)", bd2.Name, vgName))
			lvgextWaitBlockDeviceLinkedToVG(ctx, k8sClient, bd2.Name, vgName, lvgextBDLinkageTimeout)

			By("Step 4d: wait for LVMVolumeGroup status — Ready, two devices, larger VG size/free")
			Eventually(func(g Gomega) {
				var extended v1alpha1.LVMVolumeGroup
				g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: lvgName}, &extended)).To(Succeed())
				nDev := lvgextCountDevicesOnLVGNode(&extended, targetNode)
				GinkgoWriter.Printf("    vgextend status poll: phase=%s devices=%d vgSize=%s vgFree=%s\n",
					extended.Status.Phase, nDev, extended.Status.VGSize.String(), extended.Status.VGFree.String())
				for _, c := range extended.Status.Conditions {
					if c.Status == metav1.ConditionFalse {
						GinkgoWriter.Printf("    condition %s False: reason=%s msg=%s\n", c.Type, c.Reason, c.Message)
					}
				}
				g.Expect(extended.Status.Phase).To(Equal(v1alpha1.PhaseReady), "phase=%s", extended.Status.Phase)
				for _, c := range extended.Status.Conditions {
					g.Expect(c.Status).NotTo(Equal(metav1.ConditionFalse),
						"condition %s False: reason=%s message=%s", c.Type, c.Reason, c.Message)
				}
				g.Expect(nDev).To(Equal(2), "status.nodes should list two BlockDevices after vgextend")
				g.Expect(extended.Status.VGFree.Value()).To(BeNumerically(">", baselineVGFree),
					"VGFree should grow after adding second PV (baseline %d)", baselineVGFree)
				g.Expect(extended.Status.VGSize.Value()).To(BeNumerically(">", baselineVGSize),
					"VGSize should grow after vgextend (baseline %d)", baselineVGSize)
				devices := lvgextDevicesOnLVGNode(&extended, targetNode)
				g.Expect(devices).To(HaveKey(bd1.Name), "status should include first BlockDevice %s", bd1.Name)
				g.Expect(devices).To(HaveKey(bd2.Name), "status should include second BlockDevice %s", bd2.Name)
			}, lvmVolumeGroupReadyTimeout, 10*time.Second).Should(Succeed())

			By("✓ LVMVolumeGroup extended: two PVs in VG, VGFree/VGSize grew, Phase Ready")
		})
	})

	Context("LVMVolumeGroup size reduction", func() {
		It("Should detect device loss after replacing disk with a smaller one and report VG inconsistency", func() {
			shrinkLVGName := fmt.Sprintf("%sshrink-%s", lvmVGNamePrefix, lvgextNodeSafe(targetNode))
			DeferCleanup(func() { lvgextDeleteLVG(ctx, k8sClient, shrinkLVGName) })

			By(fmt.Sprintf("Step 1: attaching the original disk (%s)", lvgextShrinkOrigDiskSize))
			beforeOrig, err := kubernetes.GetConsumableBlockDevicesByNode(ctx, cl.RESTConfig(), targetNode)
			Expect(err).NotTo(HaveOccurred())

			origDiskName := fmt.Sprintf("e2e-shrink-orig-disk-%d", time.Now().Unix())
			origDisk, err := cl.Disks().CreateDisk(ctx, e2e.DiskSpec{
				Name:         origDiskName,
				Size:         resource.MustParse(lvgextShrinkOrigDiskSize),
				StorageClass: conf.TestCluster.StorageClass,
			})
			Expect(err).NotTo(HaveOccurred(), "failed to create original disk")
			origAttached := true
			DeferCleanup(func() {
				if origAttached {
					_ = cl.Disks().DetachDisk(ctx, targetNode, origDisk.Name)
				}
				_ = cl.Disks().DeleteDisk(ctx, origDisk.Name)
			})
			Expect(cl.Disks().AttachDisk(ctx, targetNode, origDisk.Name)).To(Succeed(), "failed to attach original disk")

			By("Step 2: waiting for BlockDevice discovery")
			targetBD, err := framework.WaitNewConsumableBlockDevice(ctx, cl.RESTConfig(), targetNode, beforeOrig, 5*time.Minute)
			Expect(err).NotTo(HaveOccurred(), "new consumable BlockDevice not discovered")
			GinkgoWriter.Printf("    Found BD %s (size=%s, path=%s)\n",
				targetBD.Name, targetBD.Size, targetBD.Path)

			By("Step 3: creating LVMVolumeGroup on the discovered BlockDevice")
			Expect(kubernetes.CreateLVMVolumeGroup(ctx, cl.RESTConfig(), shrinkLVGName, targetNode, []string{targetBD.Name}, "e2e-shrink-vg")).
				To(Succeed(), "failed to create LVMVolumeGroup")

			By("Waiting for LVMVolumeGroup to become Ready (up to 10 minutes)")
			var origLVG v1alpha1.LVMVolumeGroup
			Eventually(func(g Gomega) {
				g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: shrinkLVGName}, &origLVG)).To(Succeed())
				g.Expect(origLVG.Status.Phase).To(Equal(v1alpha1.PhaseReady), "Phase=%s", origLVG.Status.Phase)
			}, 10*time.Minute, 10*time.Second).Should(Succeed())

			origVGSize := origLVG.Status.VGSize.DeepCopy()
			By(fmt.Sprintf("LVMVolumeGroup Ready: VGSize=%s", origVGSize.String()))

			By("Step 4: detaching and deleting the original disk (simulating device removal)")
			Expect(cl.Disks().DetachDisk(ctx, targetNode, origDisk.Name)).To(Succeed())
			Expect(cl.Disks().DeleteDisk(ctx, origDisk.Name)).To(Succeed())
			origAttached = false

			By(fmt.Sprintf("Step 5: attaching a smaller disk (%s)", lvgextShrinkSmallDiskSize))
			smallDiskName := fmt.Sprintf("e2e-shrink-small-disk-%d", time.Now().Unix())
			smallDisk, err := cl.Disks().CreateDisk(ctx, e2e.DiskSpec{
				Name:         smallDiskName,
				Size:         resource.MustParse(lvgextShrinkSmallDiskSize),
				StorageClass: conf.TestCluster.StorageClass,
			})
			Expect(err).NotTo(HaveOccurred(), "failed to create smaller disk")
			DeferCleanup(func() {
				_ = cl.Disks().DetachDisk(ctx, targetNode, smallDisk.Name)
				_ = cl.Disks().DeleteDisk(ctx, smallDisk.Name)
			})
			Expect(cl.Disks().AttachDisk(ctx, targetNode, smallDisk.Name)).To(Succeed(), "failed to attach smaller disk")

			By("Step 6: waiting for LVMVolumeGroup to leave Ready state (VG lost its backing device)")
			Eventually(func(g Gomega) {
				var current v1alpha1.LVMVolumeGroup
				g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: shrinkLVGName}, &current)).To(Succeed())
				g.Expect(current.Status.Phase).NotTo(Equal(v1alpha1.PhaseReady),
					"Phase should not be Ready after device replacement; Phase=%s VGSize=%s (was %s)",
					current.Status.Phase, current.Status.VGSize.String(), origVGSize.String())
			}, 5*time.Minute, 15*time.Second).Should(Succeed())

			By("Step 7: verifying LVMVolumeGroup conditions contain error information")
			var finalLVG v1alpha1.LVMVolumeGroup
			Expect(k8sClient.Get(ctx, client.ObjectKey{Name: shrinkLVGName}, &finalLVG)).To(Succeed())

			hasErrorCondition := false
			for _, c := range finalLVG.Status.Conditions {
				if c.Status == metav1.ConditionFalse {
					hasErrorCondition = true
					GinkgoWriter.Printf("    Condition %s: status=%s reason=%s message=%s\n",
						c.Type, c.Status, c.Reason, c.Message)
				}
			}
			Expect(hasErrorCondition).To(BeTrue(),
				"LVMVolumeGroup should have at least one condition with status=False indicating device/VG issue")
			Expect(finalLVG.Status.Phase).To(BeElementOf(
				v1alpha1.PhaseNotReady, v1alpha1.PhasePending, v1alpha1.PhaseFailed, ""),
				"Phase should indicate non-ready state, got %s", finalLVG.Status.Phase)
		})
	})
})
