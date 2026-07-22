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

var _ = Describe("LVMVolumeGroup recreate & multiple", Label("sds-node-configurator", "lvmvolumegroup"), Ordered, func() {
	var (
		ctx        context.Context
		conf       *cfg.Config
		cl         *e2e.Cluster
		k8sClient  client.Client
		targetNode string
	)

	// lvgrmNodeSafe sanitizes a node name for use as an LVMVolumeGroup/VG name suffix.
	lvgrmNodeSafe := func(n string) string {
		return strings.ReplaceAll(strings.ReplaceAll(n, ".", "-"), "_", "-")
	}

	// lvgrmDeleteLVG best-effort deletes an LVMVolumeGroup CR and waits for its removal.
	lvgrmDeleteLVG := func(name string) {
		if name == "" {
			return
		}
		if err := kubernetes.DeleteLVMVolumeGroup(ctx, cl.RESTConfig(), name); err != nil {
			GinkgoWriter.Printf("failed to delete LVMVolumeGroup %s: %v\n", name, err)
		}
		if err := kubernetes.WaitForLVMVolumeGroupDeletion(ctx, cl.RESTConfig(), name, lvmVolumeGroupReadyTimeout); err != nil {
			GinkgoWriter.Printf("failed to wait for LVMVolumeGroup %s deletion: %v\n", name, err)
		}
	}

	// lvgrmCleanupDisk best-effort detaches and deletes a provider-managed disk.
	lvgrmCleanupDisk := func(disk *e2e.Disk) {
		if disk == nil {
			return
		}
		if err := cl.Disks().DetachDisk(ctx, targetNode, disk.Name); err != nil {
			GinkgoWriter.Printf("failed to detach disk %s: %v\n", disk.Name, err)
		}
		if err := cl.Disks().DeleteDisk(ctx, disk.Name); err != nil {
			GinkgoWriter.Printf("failed to delete disk %s: %v\n", disk.Name, err)
		}
	}

	BeforeAll(func() {
		ctx = context.Background()
		conf = cfg.Load()

		var clErr error
		cl, clErr = e2e.Connect(ctx, e2e.WithTestName("lvmvolumegroup-recreate-multi"))
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
	})

	AfterAll(func() {
		cleanupLVMVolumeGroups(ctx, k8sClient)
	})

	Context("thin-pool removed manually", func() {
		const (
			lvgrmThinPoolDiskSize  = "2Gi"
			lvgrmThinPoolName      = "e2e-thin-pool-restore"
			lvgrmThinPoolSize      = "60%"
			lvgrmThinPoolAllocatn  = "100%"
			lvgrmThinPoolStackWait = 2 * time.Minute
		)

		var (
			tpDisk    *e2e.Disk
			tpLVGName string
		)

		AfterEach(func() {
			lvgrmDeleteLVG(tpLVGName)
			tpLVGName = ""
			lvgrmCleanupDisk(tpDisk)
			tpDisk = nil
		})

		It("Should recreate thin-pool when the pool LV was removed manually on the node", func() {
			runID := fmt.Sprintf("%d", time.Now().Unix())
			vgName := "e2e-vg-tp-restore-" + runID
			tpLVGName = fmt.Sprintf("e2e-lvg-tp-restore-%s-%s", runID, lvgrmNodeSafe(targetNode))

			storageClass := conf.TestCluster.StorageClass
			Expect(storageClass).NotTo(BeEmpty())

			By("Step 1: create and attach a VirtualDisk, then wait for the consumable BlockDevice")
			before, bdErr := kubernetes.GetConsumableBlockDevicesByNode(ctx, cl.RESTConfig(), targetNode)
			Expect(bdErr).NotTo(HaveOccurred())

			diskName := fmt.Sprintf("e2e-lvg-tp-restore-disk-%s", runID)
			disk, createErr := cl.Disks().CreateDisk(ctx, e2e.DiskSpec{
				Name:         diskName,
				Size:         resource.MustParse(lvgrmThinPoolDiskSize),
				StorageClass: storageClass,
			})
			Expect(createErr).NotTo(HaveOccurred(), "failed to create disk")
			tpDisk = disk
			Expect(cl.Disks().AttachDisk(ctx, targetNode, disk.Name)).To(Succeed(), "failed to attach disk")

			bd, waitErr := framework.WaitNewConsumableBlockDevice(ctx, cl.RESTConfig(), targetNode, before, 5*time.Minute)
			Expect(waitErr).NotTo(HaveOccurred())

			By("Step 1: create LVMVolumeGroup with thin-pool")
			Expect(kubernetes.CreateLVMVolumeGroupWithThinPool(ctx, cl.RESTConfig(), tpLVGName, targetNode,
				[]string{bd.Name}, vgName, []kubernetes.ThinPoolSpec{
					{Name: lvgrmThinPoolName, Size: lvgrmThinPoolSize, AllocationLimit: lvgrmThinPoolAllocatn},
				})).To(Succeed())

			var ready v1alpha1.LVMVolumeGroup
			Eventually(func(g Gomega) {
				g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: tpLVGName}, &ready)).To(Succeed())
				g.Expect(ready.Status.Phase).To(Equal(v1alpha1.PhaseReady))
				var tp *v1alpha1.LVMVolumeGroupThinPoolStatus
				for i := range ready.Status.ThinPools {
					if ready.Status.ThinPools[i].Name == lvgrmThinPoolName {
						tp = &ready.Status.ThinPools[i]
						break
					}
				}
				g.Expect(tp).NotTo(BeNil())
				g.Expect(tp.Ready).To(BeTrue())
			}, lvmVolumeGroupReadyTimeout, 10*time.Second).Should(Succeed())

			lvsRes, errLvs := cl.Nodes().Exec(ctx, targetNode, fmt.Sprintf(`sudo -n lvs -a -o lv_name,lv_attr --noheadings %s 2>/dev/null | sed 's/[][]//g'`, strconv.Quote(vgName)))
			Expect(errLvs).NotTo(HaveOccurred())
			lvsOut := string(lvsRes.Stdout)
			present := framework.ThinPoolDataLVPresent(lvsOut, lvgrmThinPoolName)
			Expect(present).To(BeTrue(), "thin-pool data LV should exist before manual removal (status already Ready); lvs: %q", strings.TrimSpace(lvsOut))

			By("Step 2: manually remove thin-pool stack on the node (pool + metadata segments; VG and PV stay)")
			removeScript := framework.RemoveThinPoolStackScript(vgName, lvgrmThinPoolName)
			outRemove, errRemove := framework.NodeExecChecked(ctx, cl, targetNode, removeScript)
			if outRemove != "" {
				GinkgoWriter.Printf("    thin-pool remove script output:\n%s\n", outRemove)
			}
			Expect(errRemove).NotTo(HaveOccurred(), "manual thin-pool removal on node %s", targetNode)

			Eventually(func(g Gomega) {
				goneRes, errSSH := cl.Nodes().Exec(ctx, targetNode, fmt.Sprintf(`sudo -n lvs -a -o lv_name,lv_attr --noheadings %s 2>/dev/null | sed 's/[][]//g'`, strconv.Quote(vgName)))
				g.Expect(errSSH).NotTo(HaveOccurred())
				out := string(goneRes.Stdout)
				gone := framework.ThinPoolDataLVPresent(out, lvgrmThinPoolName)
				g.Expect(gone).To(BeFalse(), "thin-pool data LV should be gone after manual removal; lvs: %q", strings.TrimSpace(out))
			}, lvgrmThinPoolStackWait, 5*time.Second).Should(Succeed())

			vgsRes, errVgs := cl.Nodes().Exec(ctx, targetNode, `sudo -n vgs -o vg_name --noheadings 2>/dev/null | sed '/^$/d'`)
			Expect(errVgs).NotTo(HaveOccurred())
			vgsOut := string(vgsRes.Stdout)
			listed := framework.VGInListing(vgsOut, vgName)
			Expect(listed).To(BeTrue(), "VG %q must remain after thin-pool removal; vgs: %q", vgName, strings.TrimSpace(vgsOut))

			By("Step 3: nudge agent and wait for controller to recreate thin-pool")
			framework.TriggerLVMDiscovery(ctx, cl, targetNode)
			Expect(restartAgentOnNode(ctx, cl, targetNode)).To(Succeed())

			Eventually(func(g Gomega) {
				var cur v1alpha1.LVMVolumeGroup
				g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: tpLVGName}, &cur)).To(Succeed())
				g.Expect(cur.Status.Phase).To(Equal(v1alpha1.PhaseReady))
				var tp *v1alpha1.LVMVolumeGroupThinPoolStatus
				for i := range cur.Status.ThinPools {
					if cur.Status.ThinPools[i].Name == lvgrmThinPoolName {
						tp = &cur.Status.ThinPools[i]
						break
					}
				}
				g.Expect(tp).NotTo(BeNil(), "status.thinPools should list %q", lvgrmThinPoolName)
				g.Expect(tp.Ready).To(BeTrue(), "recreated thin-pool should be Ready; ThinPools=%+v", cur.Status.ThinPools)
				for _, c := range cur.Status.Conditions {
					if c.Type == "VGConfigurationApplied" {
						g.Expect(c.Status).To(Equal(metav1.ConditionTrue), "VGConfigurationApplied: %s %s", c.Reason, c.Message)
					}
				}
			}, lvmVolumeGroupReadyTimeout, 10*time.Second).Should(Succeed())

			Eventually(func(g Gomega) {
				restoredRes, errSSH := cl.Nodes().Exec(ctx, targetNode, fmt.Sprintf(`sudo -n lvs -a -o lv_name,lv_attr --noheadings %s 2>/dev/null | sed 's/[][]//g'`, strconv.Quote(vgName)))
				g.Expect(errSSH).NotTo(HaveOccurred())
				out := string(restoredRes.Stdout)
				restored := framework.ThinPoolDataLVPresent(out, lvgrmThinPoolName)
				g.Expect(restored).To(BeTrue(), "thin-pool data LV should exist after agent reconcile; lvs: %q", strings.TrimSpace(out))
			}, lvmVolumeGroupReadyTimeout, 10*time.Second).Should(Succeed())

			By("✓ Thin-pool removed manually; agent recreated pool; LVMVolumeGroup Ready")
		})
	})

	Context("Multiple LVMVolumeGroups on one node", func() {
		const (
			lvgrmMultiMinDisks  = 2
			lvgrmMultiMaxDisks  = 5
			lvgrmMultiSizeMinGi = 2
			lvgrmMultiSizeMaxGi = 10
		)

		var (
			multiDisks    []*e2e.Disk
			multiLVGNames []string
		)

		AfterEach(func() {
			for _, name := range multiLVGNames {
				lvgrmDeleteLVG(name)
			}
			multiLVGNames = nil
			for _, disk := range multiDisks {
				lvgrmCleanupDisk(disk)
			}
			multiDisks = nil
		})

		It("Should create independent LVMVolumeGroups on the same node (random 2–5 disks, 2–10 Gi each)", func() {
			runID := fmt.Sprintf("%d", time.Now().Unix())
			nodeSafe := lvgrmNodeSafe(targetNode)

			storageClass := conf.TestCluster.StorageClass
			Expect(storageClass).NotTo(BeEmpty())

			nDisks := lvgrmMultiMinDisks + rand.Intn(lvgrmMultiMaxDisks-lvgrmMultiMinDisks+1) // 2..5 inclusive

			type multiDisk struct {
				diskName string
				diskSize string
				bdName   string
				lvgName  string
				vgName   string
			}
			disks := make([]multiDisk, 0, nDisks)
			for i := 0; i < nDisks; i++ {
				szGi := lvgrmMultiSizeMinGi + rand.Intn(lvgrmMultiSizeMaxGi-lvgrmMultiSizeMinGi+1) // 2..10
				disks = append(disks, multiDisk{
					diskName: fmt.Sprintf("e2e-multi-lvg-d%d-%s-%d", i+1, runID, rand.Intn(100000)),
					diskSize: fmt.Sprintf("%dGi", szGi),
				})
			}

			By(fmt.Sprintf("Attaching %d VirtualDisks to the same node %q (per-disk size 2..10 Gi random)", nDisks, targetNode))
			seen, bdErr := kubernetes.GetConsumableBlockDevicesByNode(ctx, cl.RESTConfig(), targetNode)
			Expect(bdErr).NotTo(HaveOccurred())

			for i := range disks {
				disk, createErr := cl.Disks().CreateDisk(ctx, e2e.DiskSpec{
					Name:         disks[i].diskName,
					Size:         resource.MustParse(disks[i].diskSize),
					StorageClass: storageClass,
				})
				Expect(createErr).NotTo(HaveOccurred(), "create disk %s", disks[i].diskName)
				multiDisks = append(multiDisks, disk)
				Expect(cl.Disks().AttachDisk(ctx, targetNode, disk.Name)).To(Succeed(), "attach disk %s", disks[i].diskName)
				GinkgoWriter.Printf("    disk %d/%d: name=%s size=%s\n", i+1, nDisks, disks[i].diskName, disks[i].diskSize)

				bd, waitErr := framework.WaitNewConsumableBlockDevice(ctx, cl.RESTConfig(), targetNode, seen, 5*time.Minute)
				Expect(waitErr).NotTo(HaveOccurred(), "wait consumable BlockDevice for disk %s", disks[i].diskName)
				disks[i].bdName = bd.Name
				seen = append(seen, bd)
			}

			bdSeen := make(map[string]struct{})
			for i := range disks {
				Expect(bdSeen).NotTo(HaveKey(disks[i].bdName), "expected distinct BlockDevices, duplicate %q", disks[i].bdName)
				bdSeen[disks[i].bdName] = struct{}{}
			}

			for i := range disks {
				idx := i + 1
				disks[i].lvgName = fmt.Sprintf("e2e-lvg-multi-%d-%s-%s", idx, runID, nodeSafe)
				disks[i].vgName = fmt.Sprintf("e2e-vg-multi-%d-%s", idx, runID)
				By(fmt.Sprintf("Creating LVMVolumeGroup %s (VG %s) — BlockDevice %s", disks[i].lvgName, disks[i].vgName, disks[i].bdName))
				Expect(kubernetes.CreateLVMVolumeGroup(ctx, cl.RESTConfig(), disks[i].lvgName, targetNode,
					[]string{disks[i].bdName}, disks[i].vgName)).To(Succeed())
				multiLVGNames = append(multiLVGNames, disks[i].lvgName)
			}

			By(fmt.Sprintf("Waiting for %d LVMVolumeGroup(s) to become Ready (independent VGs on one node)", nDisks))
			Eventually(func(g Gomega) {
				for i := range disks {
					var cur v1alpha1.LVMVolumeGroup
					g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: disks[i].lvgName}, &cur)).To(Succeed())
					g.Expect(cur.Status.Phase).To(Equal(v1alpha1.PhaseReady), "LVMVolumeGroup %s phase", disks[i].lvgName)
				}
			}, lvmVolumeGroupReadyTimeout, 10*time.Second).Should(Succeed())

			readies := make([]v1alpha1.LVMVolumeGroup, len(disks))
			for i := range disks {
				Expect(k8sClient.Get(ctx, client.ObjectKey{Name: disks[i].lvgName}, &readies[i])).To(Succeed())
			}
			vgNameSet := make(map[string]struct{}, len(readies))
			vgUUIDSet := make(map[string]struct{}, len(readies))
			allHaveUUID := true
			for i := range readies {
				vgNameSet[readies[i].Spec.ActualVGNameOnTheNode] = struct{}{}
				u := readies[i].Status.VGUuid
				if u == "" {
					allHaveUUID = false
				} else {
					vgUUIDSet[u] = struct{}{}
				}
			}
			Expect(vgNameSet).To(HaveLen(len(disks)), "each LVMVolumeGroup must have a unique spec VG name on the node")
			if allHaveUUID {
				Expect(vgUUIDSet).To(HaveLen(len(disks)), "when every LVMVolumeGroup reports vgUUID, values must be pairwise distinct")
			}
			for i := range readies {
				for _, c := range readies[i].Status.Conditions {
					Expect(c.Status).NotTo(Equal(metav1.ConditionFalse),
						"LVMVolumeGroup %s: condition %s False — %s: %s", readies[i].Name, c.Type, c.Reason, c.Message)
				}
			}

			for i := range disks {
				vgsRes, errVgs := cl.Nodes().Exec(ctx, targetNode, `sudo -n vgs -o vg_name --noheadings 2>/dev/null | sed '/^$/d'`)
				Expect(errVgs).NotTo(HaveOccurred())
				out := string(vgsRes.Stdout)
				listed := framework.VGInListing(out, disks[i].vgName)
				Expect(listed).To(BeTrue(), "vgs should list %q; output:\n%s", disks[i].vgName, out)
			}
			By("✓ Ready LVMVolumeGroups on one node; all distinct VGs in vgs; no False conditions")
		})
	})
})
