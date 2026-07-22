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
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// schedDataDiskSize is the per-node data disk carved into a single LVMVolumeGroup for the consolidation tests.
const schedDataDiskSize = "10Gi"

// schedBDAppearTimeout bounds how long we wait for a freshly attached disk to surface as a consumable BlockDevice.
const schedBDAppearTimeout = 5 * time.Minute

var _ = Describe("Schedule extender", Label("schedule-extender"), Ordered, ContinueOnFailure, func() {
	var (
		ctx  context.Context
		conf *cfg.Config
		cl   *e2e.Cluster

		k8sClient client.Client
		nodeNames []string
		runID     string

		// shared, closure-local state built up across the Ordered specs
		createdLVGs         []*v1alpha1.LVMVolumeGroup
		totalAvailableSpace int64
		storageClassName    string

		createdDiskNames []string
		diskNodeByName   map[string]string
		bdNamesByNode    map[string][]string
	)

	BeforeAll(func() {
		ctx = context.Background()
		conf = cfg.Load()

		var clErr error
		cl, clErr = e2e.Connect(ctx, e2e.WithTestName("schedule-extender"))
		Expect(clErr).NotTo(HaveOccurred(), "failed to connect to cluster")

		var k8sErr error
		k8sClient, k8sErr = sdsclient.New(cl.RESTConfig())
		Expect(k8sErr).NotTo(HaveOccurred(), "failed to build controller-runtime client")

		nodeList, nlErr := cl.Clientset().CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		Expect(nlErr).NotTo(HaveOccurred(), "failed to list nodes")
		Expect(nodeList.Items).NotTo(BeEmpty(), "cluster must have at least one node")
		for i := range nodeList.Items {
			nodeNames = append(nodeNames, nodeList.Items[i].Name)
		}

		runID = fmt.Sprintf("%d", time.Now().Unix())
		diskNodeByName = make(map[string]string)
		bdNamesByNode = make(map[string][]string)
		By(fmt.Sprintf("Generated unique run ID: %s (nodes: %v)", runID, nodeNames))
	})

	////////////////////////////////////
	// ---=== SETUP: CREATE DISKS + LVGs + LSC ===--- //
	////////////////////////////////////

	Context("Setup: Create virtual disks and LVMVolumeGroups", func() {
		It("Should create virtual disks on cluster nodes", func() {
			storageClass := conf.TestCluster.StorageClass
			Expect(storageClass).NotTo(BeEmpty(), "TEST_CLUSTER_STORAGE_CLASS is required for disk creation")

			ensureSchedulerK8sClient(ctx, cl.RESTConfig(), &k8sClient)

			By("Cleaning up existing e2e LVMLogicalVolumes (orphan PVCs)")
			cleanupLVMLogicalVolumes(ctx, k8sClient)

			By("Cleaning up existing e2e LVMVolumeGroups (to release LVM signatures)")
			cleanupLVMVolumeGroups(ctx, k8sClient)

			By("Ensuring no leftover LocalStorageClass from a previous run")
			Expect(ensureLocalStorageClassAbsent(ctx, cl.RESTConfig(), k8sClient, localStorageClassName)).To(Succeed())

			By("Force deleting ALL non-consumable BlockDevices")
			forceDeleteAllNonConsumableBlockDevices(ctx, k8sClient, 2*time.Minute)

			By(fmt.Sprintf("Creating and attaching one data disk (%s) per node across %d nodes", schedDataDiskSize, len(nodeNames)))
			for _, node := range nodeNames {
				nodeSafe := strings.ReplaceAll(strings.ReplaceAll(node, ".", "-"), "_", "-")
				diskName := fmt.Sprintf("e2e-sched-disk-%s-%s", runID, nodeSafe)

				By(fmt.Sprintf("Recording consumable BlockDevices on node %s before attach", node))
				before, beforeErr := kubernetes.GetConsumableBlockDevicesByNode(ctx, cl.RESTConfig(), node)
				Expect(beforeErr).NotTo(HaveOccurred(), "list consumable block devices on node %s", node)

				By(fmt.Sprintf("Creating disk %s and attaching to node %s", diskName, node))
				disk, createErr := cl.Disks().CreateDisk(ctx, e2e.DiskSpec{
					Name:         diskName,
					Size:         resource.MustParse(schedDataDiskSize),
					StorageClass: storageClass,
				})
				Expect(createErr).NotTo(HaveOccurred(), "create disk %s", diskName)
				createdDiskNames = append(createdDiskNames, disk.Name)
				diskNodeByName[disk.Name] = node

				Expect(cl.Disks().AttachDisk(ctx, node, disk.Name)).To(Succeed(), "attach disk %s to node %s", disk.Name, node)

				By(fmt.Sprintf("Waiting for the new consumable BlockDevice on node %s", node))
				newBD, bdErr := framework.WaitNewConsumableBlockDevice(ctx, cl.RESTConfig(), node, before, schedBDAppearTimeout)
				Expect(bdErr).NotTo(HaveOccurred(), "new consumable block device should appear on node %s after attach", node)
				bdNamesByNode[node] = append(bdNamesByNode[node], newBD.Name)
			}

			var totalBDs int
			for _, names := range bdNamesByNode {
				totalBDs += len(names)
			}
			Expect(totalBDs).To(BeNumerically(">", 0), "expected at least one consumable BlockDevice from created disks")

			By("Fetching created BlockDevice CRs and verifying they are consumable")
			var createdBlockDevices []*v1alpha1.BlockDevice
			var notConsumable []string
			for _, names := range bdNamesByNode {
				for _, name := range names {
					var bd v1alpha1.BlockDevice
					Expect(k8sClient.Get(ctx, client.ObjectKey{Name: name}, &bd)).To(Succeed(), "get BlockDevice %s", name)
					bdCopy := bd
					createdBlockDevices = append(createdBlockDevices, &bdCopy)
					if !bd.Status.Consumable {
						notConsumable = append(notConsumable, fmt.Sprintf("%s (fsType=%s, pvUUID=%s)",
							bd.Name, bd.Status.FsType, bd.Status.PVUuid))
					}
				}
			}
			printBlockDevicesSummary(createdBlockDevices)
			Expect(notConsumable).To(BeEmpty(),
				"All BlockDevices from new disks should be consumable, but these are not: %v", notConsumable)
		})

		It("Should create LVMVolumeGroups from BlockDevices (one per node)", func() {
			Expect(bdNamesByNode).NotTo(BeEmpty(), "BlockDevices must be created first")

			By(fmt.Sprintf("Creating LVMVolumeGroups for %d nodes (with unique runID %s)", len(bdNamesByNode), runID))
			for node, bdNames := range bdNamesByNode {
				nodeSafe := strings.ReplaceAll(strings.ReplaceAll(node, ".", "-"), "_", "-")
				lvgName := fmt.Sprintf("%s%s-%s", lvmVGNamePrefix, runID, nodeSafe)
				vgName := fmt.Sprintf("e2e-vg-%s-%s", runID, nodeSafe)

				By(fmt.Sprintf("Creating LVMVolumeGroup %s (VG: %s) on node %s with BlockDevices: %v", lvgName, vgName, node, bdNames))
				Expect(kubernetes.CreateLVMVolumeGroup(ctx, cl.RESTConfig(), lvgName, node, bdNames, vgName)).
					To(Succeed(), "create LVMVolumeGroup %s", lvgName)

				By(fmt.Sprintf("Waiting for LVMVolumeGroup %s to become Ready", lvgName))
				Expect(kubernetes.WaitForLVMVolumeGroupReady(ctx, cl.RESTConfig(), lvgName, lvmVolumeGroupReadyTimeout)).
					To(Succeed(), "LVMVolumeGroup %s should reach Ready", lvgName)

				var lvg v1alpha1.LVMVolumeGroup
				Expect(k8sClient.Get(ctx, client.ObjectKey{Name: lvgName}, &lvg)).To(Succeed(), "get LVMVolumeGroup %s", lvgName)
				lvgCopy := lvg
				createdLVGs = append(createdLVGs, &lvgCopy)
			}

			totalAvailableSpace = getTotalAvailableSpace(ctx, k8sClient, createdLVGs)
			By(fmt.Sprintf("Total available space across all LVMVolumeGroups: %d bytes (%.2f Gi)",
				totalAvailableSpace, float64(totalAvailableSpace)/(1024*1024*1024)))
			Expect(totalAvailableSpace).To(BeNumerically(">", 0),
				"sum(VGFree) across Ready LVMVolumeGroups must be positive")

			printLVGsSummary(ctx, k8sClient, createdLVGs)
		})

		It("Should create LocalStorageClass and wait for StorageClass", func() {
			Expect(createdLVGs).NotTo(BeEmpty(), "LVMVolumeGroups must be created first")

			dynamicClient := cl.Dynamic()

			lvgNames := make([]string, len(createdLVGs))
			for i, lvg := range createdLVGs {
				lvgNames[i] = lvg.Name
			}

			lvmVolumeGroups := make([]interface{}, len(lvgNames))
			for i, lvgName := range lvgNames {
				lvmVolumeGroups[i] = map[string]interface{}{
					"name": lvgName,
				}
			}

			lsc := &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "storage.deckhouse.io/v1alpha1",
					"kind":       "LocalStorageClass",
					"metadata": map[string]interface{}{
						"name": localStorageClassName,
					},
					"spec": map[string]interface{}{
						"lvm": map[string]interface{}{
							"lvmVolumeGroups": lvmVolumeGroups,
							"type":            "Thick",
						},
						"reclaimPolicy":     "Delete",
						"volumeBindingMode": "WaitForFirstConsumer",
					},
				},
			}

			By(fmt.Sprintf("Creating LocalStorageClass %s with LVMVolumeGroups: %v", localStorageClassName, lvgNames))
			Eventually(func(g Gomega) {
				g.Expect(ensureLocalStorageClassAbsent(ctx, cl.RESTConfig(), k8sClient, localStorageClassName)).To(Succeed())
				_, createErr := dynamicClient.Resource(localStorageClassGVR).Create(ctx, lsc.DeepCopy(), metav1.CreateOptions{})
				g.Expect(createErr).NotTo(HaveOccurred(), "create LocalStorageClass")
			}, 5*time.Minute, 10*time.Second).Should(Succeed(), "create LocalStorageClass after prior e2e-local-sc is fully removed")

			By("Waiting for LocalStorageClass to reach Created phase (up to 3 minutes)")
			Eventually(func(g Gomega) {
				lscObj, err := dynamicClient.Resource(localStorageClassGVR).Get(ctx, localStorageClassName, metav1.GetOptions{})
				g.Expect(err).NotTo(HaveOccurred())
				phase, _, _ := unstructured.NestedString(lscObj.Object, "status", "phase")
				g.Expect(phase).To(Equal("Created"), "LocalStorageClass phase should be Created, got %s", phase)
			}, 3*time.Minute, 5*time.Second).Should(Succeed())

			By("Waiting for StorageClass to be created (up to 2 minutes)")
			Eventually(func(g Gomega) {
				var scList storagev1.StorageClassList
				err := k8sClient.List(ctx, &scList, &client.ListOptions{})
				g.Expect(err).NotTo(HaveOccurred())

				for i := range scList.Items {
					sc := &scList.Items[i]
					if sc.Name == localStorageClassName {
						storageClassName = sc.Name
						return
					}
				}
				g.Expect(false).To(BeTrue(), "StorageClass %s not found", localStorageClassName)
			}, 2*time.Minute, 5*time.Second).Should(Succeed())

			By(fmt.Sprintf("StorageClass %s created successfully", storageClassName))
		})
	})

	////////////////////////////////////
	// ---=== SPACE CONSOLIDATION TESTS ===--- //
	////////////////////////////////////

	Context("Scheduler Extender: Space consolidation tests", func() {
		It("Should fill storage with small volumes to maximum capacity", Label("small"), func() {
			Expect(createdLVGs).NotTo(BeEmpty(), "LVMVolumeGroups must be created first")
			Expect(storageClassName).NotTo(BeEmpty(), "StorageClass must be created first")
			Expect(totalAvailableSpace).To(BeNumerically(">", 0),
				"baseline sum(VGFree) must be recorded when LVMVolumeGroups became Ready")

			By("Cleaning up previous test resources")
			schedulerCleanupWorkloadBeforeNextFill(ctx, k8sClient)

			By("Waiting until sum(VGFree) recovers to initial storage budget (same as after LVG Ready)")
			currentAvailable := waitForSchedulerStorageFreedToBaseline(ctx, k8sClient, createdLVGs, totalAvailableSpace)
			By(fmt.Sprintf("Current available space: %.2f Gi (baseline budget %.2f Gi)",
				float64(currentAvailable)/(1024*1024*1024), float64(totalAvailableSpace)/(1024*1024*1024)))

			maxPerLVG := getMaxVGFreeAcrossLVGs(ctx, k8sClient, createdLVGs)
			By(fmt.Sprintf("Max VGFree on one LVMVolumeGroup: %.2f Gi (each PVC must fit a single LVG; sum VGFree can be higher)",
				float64(maxPerLVG)/(1024*1024*1024)))

			preferredUnit := int64(1 * 1024 * 1024 * 1024) // 1Gi
			minVolumeSize := int64(500 * 1024 * 1024)      // 500Mi minimum for remainder
			volumeSizes := schedulerVolumeSizesForConsolidatedFill(currentAvailable, maxPerLVG, preferredUnit, minVolumeSize)
			Expect(volumeSizes).NotTo(BeEmpty(),
				"no schedulable volume plan (max VGFree per LVG vs min remainder)")

			totalPlanned := int64(0)
			for _, s := range volumeSizes {
				totalPlanned += s
			}
			utilization := float64(0)
			if currentAvailable > 0 {
				utilization = float64(totalPlanned) / float64(currentAvailable) * 100
			}

			By(fmt.Sprintf("Planning %d volumes, total %.2f Gi (%.1f%% of sum VGFree); preferred unit %d Mi (capped by max per LVG)",
				len(volumeSizes), float64(totalPlanned)/(1024*1024*1024), utilization, preferredUnit/(1024*1024)))

			successCount, scheduledCount := createPVCsAndPodsWithSizes(ctx, k8sClient, volumeSizes, storageClassName, "small")

			By(fmt.Sprintf("Results: %d/%d PVCs created, %d/%d Pods scheduled", successCount, len(volumeSizes), scheduledCount, successCount))
			Expect(scheduledCount).To(Equal(successCount),
				"All created PVCs must have scheduled Pods")
			Expect(successCount).To(Equal(len(volumeSizes)),
				"All planned PVCs must be created successfully")

			printSchedulingSummary("small volumes", len(volumeSizes), successCount, scheduledCount, preferredUnit)
		})

		It("Should fill storage with medium volumes to maximum capacity", Label("medium"), func() {
			Expect(createdLVGs).NotTo(BeEmpty(), "LVMVolumeGroups must be created first")
			Expect(storageClassName).NotTo(BeEmpty(), "StorageClass must be created first")
			Expect(totalAvailableSpace).To(BeNumerically(">", 0),
				"baseline sum(VGFree) must be recorded when LVMVolumeGroups became Ready")

			By("Cleaning up previous test resources")
			schedulerCleanupWorkloadBeforeNextFill(ctx, k8sClient)

			By("Waiting until sum(VGFree) recovers to initial storage budget (same as after LVG Ready)")
			currentAvailable := waitForSchedulerStorageFreedToBaseline(ctx, k8sClient, createdLVGs, totalAvailableSpace)
			By(fmt.Sprintf("Current available space: %.2f Gi (baseline budget %.2f Gi)",
				float64(currentAvailable)/(1024*1024*1024), float64(totalAvailableSpace)/(1024*1024*1024)))

			maxPerLVG := getMaxVGFreeAcrossLVGs(ctx, k8sClient, createdLVGs)
			By(fmt.Sprintf("Max VGFree on one LVMVolumeGroup: %.2f Gi (each PVC must fit a single LVG; sum VGFree can be higher)",
				float64(maxPerLVG)/(1024*1024*1024)))

			preferredUnit := int64(5 * 1024 * 1024 * 1024) // 5Gi
			minVolumeSize := int64(1 * 1024 * 1024 * 1024) // 1Gi minimum for remainder
			volumeSizes := schedulerVolumeSizesForConsolidatedFill(currentAvailable, maxPerLVG, preferredUnit, minVolumeSize)
			Expect(volumeSizes).NotTo(BeEmpty(),
				"no schedulable volume plan (max VGFree per LVG vs min remainder)")

			totalPlanned := int64(0)
			for _, s := range volumeSizes {
				totalPlanned += s
			}
			utilization := float64(0)
			if currentAvailable > 0 {
				utilization = float64(totalPlanned) / float64(currentAvailable) * 100
			}

			By(fmt.Sprintf("Planning %d volumes, total %.2f Gi (%.1f%% of sum VGFree); preferred unit %d Mi (capped by max per LVG)",
				len(volumeSizes), float64(totalPlanned)/(1024*1024*1024), utilization, preferredUnit/(1024*1024)))

			successCount, scheduledCount := createPVCsAndPodsWithSizes(ctx, k8sClient, volumeSizes, storageClassName, "medium")

			By(fmt.Sprintf("Results: %d/%d PVCs created, %d/%d Pods scheduled", successCount, len(volumeSizes), scheduledCount, successCount))
			Expect(scheduledCount).To(Equal(successCount),
				"All created PVCs must have scheduled Pods")
			Expect(successCount).To(Equal(len(volumeSizes)),
				"All planned PVCs must be created successfully")

			printSchedulingSummary("medium volumes", len(volumeSizes), successCount, scheduledCount, preferredUnit)
		})

		It("Should fill storage with large volumes to maximum capacity", Label("large"), func() {
			Expect(createdLVGs).NotTo(BeEmpty(), "LVMVolumeGroups must be created first")
			Expect(storageClassName).NotTo(BeEmpty(), "StorageClass must be created first")
			Expect(totalAvailableSpace).To(BeNumerically(">", 0),
				"baseline sum(VGFree) must be recorded when LVMVolumeGroups became Ready")

			By("Cleaning up previous test resources")
			schedulerCleanupWorkloadBeforeNextFill(ctx, k8sClient)

			By("Waiting until sum(VGFree) recovers to initial storage budget (same as after LVG Ready)")
			currentAvailable := waitForSchedulerStorageFreedToBaseline(ctx, k8sClient, createdLVGs, totalAvailableSpace)
			By(fmt.Sprintf("Current available space: %.2f Gi (baseline budget %.2f Gi)",
				float64(currentAvailable)/(1024*1024*1024), float64(totalAvailableSpace)/(1024*1024*1024)))

			maxPerLVG := getMaxVGFreeAcrossLVGs(ctx, k8sClient, createdLVGs)
			By(fmt.Sprintf("Max VGFree on one LVMVolumeGroup: %.2f Gi (each PVC must fit a single LVG; sum VGFree can be higher)",
				float64(maxPerLVG)/(1024*1024*1024)))

			preferredUnit := int64(10 * 1024 * 1024 * 1024) // 10Gi
			minVolumeSize := int64(1 * 1024 * 1024 * 1024)  // 1Gi minimum for remainder
			volumeSizes := schedulerVolumeSizesForConsolidatedFill(currentAvailable, maxPerLVG, preferredUnit, minVolumeSize)
			Expect(volumeSizes).NotTo(BeEmpty(),
				"no schedulable volume plan (max VGFree per LVG vs min remainder)")

			totalPlanned := int64(0)
			for _, s := range volumeSizes {
				totalPlanned += s
			}
			utilization := float64(0)
			if currentAvailable > 0 {
				utilization = float64(totalPlanned) / float64(currentAvailable) * 100
			}

			By(fmt.Sprintf("Planning %d volumes, total %.2f Gi (%.1f%% of sum VGFree); preferred unit %d Gi (capped by max per LVG)",
				len(volumeSizes), float64(totalPlanned)/(1024*1024*1024), utilization, preferredUnit/(1024*1024*1024)))

			successCount, scheduledCount := createPVCsAndPodsWithSizes(ctx, k8sClient, volumeSizes, storageClassName, "large")

			By(fmt.Sprintf("Results: %d/%d PVCs created, %d/%d Pods scheduled", successCount, len(volumeSizes), scheduledCount, successCount))
			Expect(scheduledCount).To(Equal(successCount),
				"All created PVCs must have scheduled Pods")
			Expect(successCount).To(Equal(len(volumeSizes)),
				"All planned PVCs must be created successfully")

			printSchedulingSummary("large volumes", len(volumeSizes), successCount, scheduledCount, preferredUnit)
		})
	})

	AfterAll(func() {
		defer func() {
			if cl == nil {
				return
			}
			if err := cl.Close(context.Background()); err != nil {
				GinkgoWriter.Println("Error closing cluster: ", err)
			}
		}()

		cleanupCtx := context.Background()

		By("Schedule extender AfterAll: cleaning up Pods, PVCs, PVs")
		cleanupPodsAndPVCsWithWait(cleanupCtx, k8sClient, suitePodPVCleanupPodTimeout, suitePodPVCleanupPVTimeout)

		By("Schedule extender AfterAll: cleaning up LVMLogicalVolumes and LVMVolumeGroups")
		cleanupLVMLogicalVolumes(cleanupCtx, k8sClient)
		cleanupLVMVolumeGroups(cleanupCtx, k8sClient)

		By("Schedule extender AfterAll: cleaning up LocalStorageClass")
		cleanupLocalStorageClasses(cleanupCtx, cl.RESTConfig())

		By("Schedule extender AfterAll: detaching and deleting created data disks")
		for _, diskName := range createdDiskNames {
			node := diskNodeByName[diskName]
			if detachErr := cl.Disks().DetachDisk(cleanupCtx, node, diskName); detachErr != nil {
				GinkgoWriter.Printf("failed to detach disk %s from node %s: %v\n", diskName, node, detachErr)
			}
			if deleteErr := cl.Disks().DeleteDisk(cleanupCtx, diskName); deleteErr != nil {
				GinkgoWriter.Printf("failed to delete disk %s: %v\n", diskName, deleteErr)
			}
		}

		By("Schedule extender AfterAll: force deleting leftover non-consumable BlockDevices")
		forceDeleteAllNonConsumableBlockDevices(cleanupCtx, k8sClient, 2*time.Minute)
	})
})
