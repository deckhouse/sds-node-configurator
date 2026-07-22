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

var _ = Describe("LVMVolumeGroup validation", Label("sds-node-configurator", "lvmvolumegroup"), Ordered, func() {
	const (
		lvgvalConditionVGConfigurationApplied = "VGConfigurationApplied"
		lvgvalReasonValidationFailed          = "ValidationFailed"
	)

	var (
		ctx        context.Context
		conf       *cfg.Config
		cl         *e2e.Cluster
		k8sClient  client.Client
		targetNode string

		// Per-spec state cleaned in AfterEach.
		createdDisks   []*e2e.Disk
		createdLVGs    []string
		createdBDNames []string
	)

	BeforeAll(func() {
		ctx = context.Background()
		var cfgErr error
		conf, cfgErr = cfg.Load()
		Expect(cfgErr).NotTo(HaveOccurred(), "failed to load config")

		var clErr error
		cl, clErr = e2e.Connect(ctx, e2e.WithTestName("lvmvolumegroup-validation"))
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

	AfterEach(func() {
		By("Cleaning up e2e LVMVolumeGroups")
		cleanupLVMVolumeGroups(ctx, k8sClient)
		createdLVGs = nil

		By("Detaching and deleting validation test disks")
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
		createdDisks = nil

		By("Force-deleting leftover BlockDevice CRs")
		forceDeleteBlockDevicesByNames(ctx, k8sClient, createdBDNames)
		createdBDNames = nil
	})

	// Order: (1) tiny disk — no BlockDevice CR; (2) large disk — intermediate LVG then delete + pvcreate so BD is not
	// consumable; (3) final LVMVolumeGroup selects only that BD (does not touch other BlockDevices on the node).
	It("Should fail LVMVolumeGroup when the only selected BlockDevice is not consumable", func() {
		storageClass := conf.TestCluster.StorageClass
		Expect(storageClass).NotTo(BeEmpty())

		runID := fmt.Sprintf("%d", time.Now().UnixNano())
		smallDiskName := "e2e-lvg-val-s-" + runID
		largeDiskName := "e2e-lvg-val-l-" + runID
		smallSize := fmt.Sprintf("%dMi", 5+rand.Intn(995)) // 5..999 Mi — below agent minimum, expect no BD
		largeSize := fmt.Sprintf("%dGi", 5+rand.Intn(11))  // 5..15 Gi
		midLvgName := lvmVGNamePrefix + "val-mid-" + runID
		midVgName := "e2e-vg-val-mid-" + runID
		finalLvgName := lvmVGNamePrefix + "val-final-" + runID
		finalVgName := "e2e-vg-val-final-" + runID

		By("Step 1: attach small empty disk (no BlockDevice CR expected below minimum size)")
		var beforeList v1alpha1.BlockDeviceList
		Expect(k8sClient.List(ctx, &beforeList, &client.ListOptions{})).To(Succeed())
		beforeNames := make(map[string]struct{}, len(beforeList.Items))
		for i := range beforeList.Items {
			beforeNames[beforeList.Items[i].Name] = struct{}{}
		}

		smallDisk, smallErr := cl.Disks().CreateDisk(ctx, e2e.DiskSpec{
			Name:         smallDiskName,
			Size:         resource.MustParse(smallSize),
			StorageClass: storageClass,
		})
		Expect(smallErr).NotTo(HaveOccurred(), "failed to create small disk")
		createdDisks = append(createdDisks, smallDisk)
		Expect(cl.Disks().AttachDisk(ctx, targetNode, smallDisk.Name)).To(Succeed(), "failed to attach small disk")

		By("Asserting no new BlockDevice CR appears for the sub-minimum disk")
		Consistently(func(g Gomega) {
			var after v1alpha1.BlockDeviceList
			g.Expect(k8sClient.List(ctx, &after, &client.ListOptions{})).To(Succeed())
			var newOnes []string
			for i := range after.Items {
				if _, ok := beforeNames[after.Items[i].Name]; !ok {
					newOnes = append(newOnes, after.Items[i].Name)
				}
			}
			g.Expect(newOnes).To(BeEmpty(),
				"disks below minimum size must not get BlockDevice CRs; new name(s): %v", newOnes)
		}, 3*time.Minute, 15*time.Second).Should(Succeed())

		By("Step 2: attach large disk; intermediate LVM, delete, pvcreate — BD must become not consumable")
		beforeConsumable, bdErr := kubernetes.GetConsumableBlockDevicesByNode(ctx, cl.RESTConfig(), targetNode)
		Expect(bdErr).NotTo(HaveOccurred())

		largeDisk, largeErr := cl.Disks().CreateDisk(ctx, e2e.DiskSpec{
			Name:         largeDiskName,
			Size:         resource.MustParse(largeSize),
			StorageClass: storageClass,
		})
		Expect(largeErr).NotTo(HaveOccurred(), "failed to create large disk")
		createdDisks = append(createdDisks, largeDisk)
		Expect(cl.Disks().AttachDisk(ctx, targetNode, largeDisk.Name)).To(Succeed(), "failed to attach large disk")

		largeBD, waitErr := framework.WaitNewConsumableBlockDevice(ctx, cl.RESTConfig(), targetNode, beforeConsumable, 5*time.Minute)
		Expect(waitErr).NotTo(HaveOccurred())
		createdBDNames = append(createdBDNames, largeBD.Name)
		nodeName := targetNode

		By(fmt.Sprintf("Creating intermediate LVMVolumeGroup %s (VG %s) on %s selecting only %s",
			midLvgName, midVgName, nodeName, largeBD.Name))
		Expect(kubernetes.CreateLVMVolumeGroup(ctx, cl.RESTConfig(), midLvgName, nodeName,
			[]string{largeBD.Name}, midVgName)).To(Succeed())
		createdLVGs = append(createdLVGs, midLvgName)

		Eventually(func(g Gomega) {
			var cur v1alpha1.LVMVolumeGroup
			g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: midLvgName}, &cur)).To(Succeed())
			g.Expect(cur.Status.Phase).To(Equal(v1alpha1.PhaseReady))
		}, lvmVolumeGroupReadyTimeout, 10*time.Second).Should(Succeed())
		By("Intermediate LVMVolumeGroup Ready (before delete)")

		Expect(kubernetes.DeleteLVMVolumeGroup(ctx, cl.RESTConfig(), midLvgName)).To(Succeed())
		Expect(kubernetes.WaitForLVMVolumeGroupDeletion(ctx, cl.RESTConfig(), midLvgName, lvmVolumeGroupReadyTimeout)).To(Succeed())

		var largeBDCR v1alpha1.BlockDevice
		Expect(k8sClient.Get(ctx, client.ObjectKey{Name: largeBD.Name}, &largeBDCR)).To(Succeed())
		largePath := largeBDCR.Status.Path
		Expect(largePath).NotTo(BeEmpty())

		By("pvcreate after agent pvremoved PV on LVG delete (orphan PV → not consumable)")
		_, errPV := framework.NodeExecChecked(ctx, cl, nodeName, fmt.Sprintf("sudo -n pvcreate -y %q 2>&1", largePath))
		Expect(errPV).NotTo(HaveOccurred(), "pvcreate")
		framework.TriggerLVMDiscovery(ctx, cl, nodeName)

		Eventually(func(g Gomega) {
			var bd v1alpha1.BlockDevice
			g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: largeBD.Name}, &bd)).To(Succeed())
			g.Expect(bd.Status.Consumable).To(BeFalse())
		}, 3*time.Minute, 10*time.Second).Should(Succeed())

		By("Step 3: LVMVolumeGroup selecting only this BlockDevice — expect ValidationFailed")
		Expect(kubernetes.CreateLVMVolumeGroup(ctx, cl.RESTConfig(), finalLvgName, nodeName,
			[]string{largeBD.Name}, finalVgName)).To(Succeed())
		createdLVGs = append(createdLVGs, finalLvgName)

		Eventually(func(g Gomega) {
			var cur v1alpha1.LVMVolumeGroup
			g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: finalLvgName}, &cur)).To(Succeed())
			g.Expect(cur.Status.Phase).NotTo(Equal(v1alpha1.PhaseReady))
			var cfg *metav1.Condition
			for i := range cur.Status.Conditions {
				if cur.Status.Conditions[i].Type == lvgvalConditionVGConfigurationApplied {
					cfg = &cur.Status.Conditions[i]
					break
				}
			}
			g.Expect(cfg).NotTo(BeNil())
			g.Expect(cfg.Status).To(Equal(metav1.ConditionFalse))
			g.Expect(cfg.Reason).To(Equal(lvgvalReasonValidationFailed))
			g.Expect(cfg.Message).To(ContainSubstring("not consumable"))
		}, 3*time.Minute, 8*time.Second).Should(Succeed())

		vgsRes, errVgs := cl.Nodes().Exec(ctx, nodeName, `sudo -n vgs -o vg_name --noheadings 2>/dev/null | sed '/^$/d'`)
		Expect(errVgs).NotTo(HaveOccurred())
		vgsOut := string(vgsRes.Stdout)
		listed := framework.VGInListing(vgsOut, finalVgName)
		Expect(listed).To(BeFalse(), "vgs:\n%s", vgsOut)

		By("✓ ValidationFailed on single non-consumable BD; other cluster BlockDevices were not in selector")
	})
})
