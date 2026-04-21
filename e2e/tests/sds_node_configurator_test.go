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
	"io"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	virtv1alpha2 "github.com/deckhouse/virtualization/api/core/v1alpha2"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/storage-e2e/pkg/cluster"
	"github.com/deckhouse/storage-e2e/pkg/kubernetes"
)

var _ = Describe("sds-node-configurator module e2e", Label("e2e-tests"), Ordered, func() {

	Describe("Common Scheduler Extender", Ordered, func() {
		var (
			testClusterResources *cluster.TestClusterResources
			e2eCtx               context.Context
			k8sClient            client.Client
			dynamicClient        dynamic.Interface
			e2eDiskAttachments   []*kubernetes.VirtualDiskAttachmentResult
			createdBlockDevices  []*v1alpha1.BlockDevice
			createdLVGs          []*v1alpha1.LVMVolumeGroup
			totalAvailableSpace  int64
			e2eStorageClassName  string
			e2eRunID             string
		)

		BeforeAll(func() {
			e2eRunID = fmt.Sprintf("%d", time.Now().Unix())
			By(fmt.Sprintf("Generated unique run ID: %s", e2eRunID))

			By("Outputting environment variables", func() {
				GinkgoWriter.Printf("    📋 Environment variables (without default values):\n")

				maskValue := func(value string, mask bool) string {
					if mask && len(value) > 5 {
						return value[:5] + "***"
					}
					return value
				}

				if e2eConfigDKPLicenseKey() != "" {
					GinkgoWriter.Printf("      E2E_DKP_LICENSE_KEY: %s\n", maskValue(e2eConfigDKPLicenseKey(), true))
				}
				if e2eConfigRegistryDockerCfg() != "" {
					GinkgoWriter.Printf("      E2E_REGISTRY_DOCKER_CFG: %s\n", maskValue(e2eConfigRegistryDockerCfg(), true))
				}
				if e2eConfigTestClusterCreateMode() != "" {
					GinkgoWriter.Printf("      TEST_CLUSTER_CREATE_MODE: %s\n", e2eConfigTestClusterCreateMode())
				}
				if e2eConfigTestClusterCleanup() != "" {
					GinkgoWriter.Printf("      TEST_CLUSTER_CLEANUP: %s\n", e2eConfigTestClusterCleanup())
				}
				if e2eConfigNamespace() != "" {
					GinkgoWriter.Printf("      TEST_CLUSTER_NAMESPACE: %s\n", e2eConfigNamespace())
				}
				if e2eConfigStorageClass() != "" {
					GinkgoWriter.Printf("      TEST_CLUSTER_STORAGE_CLASS: %s\n", e2eConfigStorageClass())
				}
				if e2eConfigSSHHost() != "" {
					GinkgoWriter.Printf("      SSH_HOST: %s\n", e2eConfigSSHHost())
				}
				if e2eConfigSSHUser() != "" {
					GinkgoWriter.Printf("      SSH_USER: %s\n", e2eConfigSSHUser())
				}
				if e2eConfigSSHHost() != "" && e2eConfigSSHUser() != "" {
					GinkgoWriter.Printf("      Base cluster SSH: %s@%s\n", e2eConfigSSHUser(), e2eConfigSSHHost())
				}
				if e2eConfigSSHJumpHost() != "" {
					GinkgoWriter.Printf("      SSH_JUMP_HOST: %s\n", e2eConfigSSHJumpHost())
					if e2eConfigSSHJumpUser() != "" {
						GinkgoWriter.Printf("      SSH_JUMP_USER: %s\n", e2eConfigSSHJumpUser())
					}
					if e2eConfigSSHJumpKeyPath() != "" {
						GinkgoWriter.Printf("      SSH_JUMP_KEY_PATH: %s\n", e2eConfigSSHJumpKeyPath())
					}
				}
				if e2eConfigSSHPassphrase() != "" {
					GinkgoWriter.Printf("      SSH_PASSPHRASE: <set>\n")
				}
				if e2eConfigLogLevel() != "" {
					GinkgoWriter.Printf("      LOG_LEVEL: %s\n", e2eConfigLogLevel())
				}
				if e2eConfigKubeConfigPath() != "" {
					GinkgoWriter.Printf("      KUBE_CONFIG_PATH: %s\n", e2eConfigKubeConfigPath())
				}
			})
			e2eCtx = context.Background()

			By("Binding nested test cluster from BeforeSuite", func() {
				testClusterResources = e2eNestedTestClusterOrNil()
				Expect(testClusterResources).NotTo(BeNil(),
					"nested cluster must be created in BeforeSuite (e2eEnsureSharedNestedTestCluster)")
			})
		})

		AfterAll(func() {
			if testClusterResources != nil {
				ctx, cancel := context.WithTimeout(context.Background(), e2eClusterCleanupTimeout)
				defer cancel()

				ns := e2eConfigNamespace()

				if testClusterResources.BaseKubeconfig != nil {
					GinkgoWriter.Printf("    ▶️ Cleaning up e2e VirtualDisks...\n")
					cleanupE2EVirtualDisks(ctx, testClusterResources.BaseKubeconfig, ns, e2eVirtualDiskPrefix)
				}

				if k8sClient != nil {
					GinkgoWriter.Printf("    ▶️ Cleaning up e2e Pods and PVCs...\n")
					cleanupE2EPodsAndPVCsWithWait(ctx, k8sClient, 2*time.Minute)

					GinkgoWriter.Printf("    ▶️ Cleaning up e2e LocalStorageClasses...\n")
					cleanupE2ELocalStorageClasses(ctx, testClusterResources.Kubeconfig)

					GinkgoWriter.Printf("    ▶️ Cleaning up e2e LVMVolumeGroups...\n")
					cleanupE2ELVMVolumeGroups(ctx, k8sClient)
				}

				// Nested cluster teardown runs in AfterSuite (e2eCleanupNestedTestClusterAfterSuite).
			}
		})

		////////////////////////////////////
		// ---=== SETUP: CREATE VIRTUAL DISKS ===--- //
		////////////////////////////////////

		Context("Setup: Create virtual disks and LVMVolumeGroups", func() {
			const e2eDataDiskSize = "10Gi"

			It("Should create virtual disks on cluster nodes", func() {
				ensureSchedulerE2EK8sClient(testClusterResources, &k8sClient, e2eCtx)

				var clusterVMs []string
				var baseKubeconfig *rest.Config
				ns := e2eConfigNamespace()
				storageClass := e2eConfigStorageClass()
				Expect(storageClass).NotTo(BeEmpty(), "TEST_CLUSTER_STORAGE_CLASS is required for VirtualDisk")

				By("Cleaning up existing e2e LVMLogicalVolumes (orphan PVCs)")
				cleanupE2ELVMLogicalVolumes(e2eCtx, k8sClient)

				By("Cleaning up existing e2e LVMVolumeGroups (to release LVM signatures)")
				cleanupE2ELVMVolumeGroups(e2eCtx, k8sClient)

				By("Ensuring no leftover LocalStorageClass from a previous run")
				Expect(ensureE2ELocalStorageClassAbsent(e2eCtx, testClusterResources.Kubeconfig, k8sClient, e2eLocalStorageClassName)).To(Succeed())

				if testClusterResources.BaseKubeconfig != nil {
					By("Cleaning up e2e VirtualDisks and attachments before tests")
					cleanupE2EVirtualDisks(e2eCtx, testClusterResources.BaseKubeconfig, ns, e2eVirtualDiskPrefix)

					By("Force deleting ALL non-consumable BlockDevices")
					forceDeleteAllNonConsumableBlockDevices(e2eCtx, k8sClient, 2*time.Minute)
				}

				if testClusterResources.BaseKubeconfig == nil || testClusterResources.VMResources == nil {
					if testClusterResources.BaseKubeconfig == nil {
						Skip("VirtualDisk creation requires base cluster kubeconfig (Deckhouse virtualization). " +
							"Set SSH_JUMP_HOST to the base cluster or use TEST_CLUSTER_CREATE_MODE=alwaysCreateNew.")
					}
					By("Listing VirtualMachines on base cluster (jump host)")
					vmNames, listErr := kubernetes.ListVirtualMachineNames(e2eCtx, testClusterResources.BaseKubeconfig, ns)
					Expect(listErr).NotTo(HaveOccurred(), "list VirtualMachines on base cluster")
					Expect(vmNames).NotTo(BeEmpty(), "no VirtualMachines in namespace %s on base cluster", ns)
					clusterVMs = vmNames
					baseKubeconfig = testClusterResources.BaseKubeconfig
				} else {
					clusterVMs = make([]string, 0, len(testClusterResources.VMResources.VMNames))
					for _, name := range testClusterResources.VMResources.VMNames {
						if name != testClusterResources.VMResources.SetupVMName {
							clusterVMs = append(clusterVMs, name)
						}
					}
					Expect(clusterVMs).NotTo(BeEmpty(), "no guest VMs (masters/workers) to attach disk to")
					baseKubeconfig = testClusterResources.BaseKubeconfig
				}

				numNodes := len(clusterVMs)
				parallelism := 1 + rand.Intn(numNodes)
				if parallelism > numNodes {
					parallelism = numNodes
				}
				shuffled := make([]string, numNodes)
				copy(shuffled, clusterVMs)
				rand.Shuffle(numNodes, func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
				targetVMs := shuffled[:parallelism]

				disksPerNode := make([]int, parallelism)
				for i := 0; i < parallelism; i++ {
					disksPerNode[i] = 1 + rand.Intn(5)
				}
				var createPlan []string
				for i, vm := range targetVMs {
					for j := 0; j < disksPerNode[i]; j++ {
						createPlan = append(createPlan, vm)
					}
				}
				totalDisks := len(createPlan)
				By(fmt.Sprintf("Creating %d VirtualDisks in parallel on %d nodes %v (disks per node: %v)", totalDisks, parallelism, targetVMs, disksPerNode))

				var nodeListForDiag corev1.NodeList
				Expect(k8sClient.List(e2eCtx, &nodeListForDiag, &client.ListOptions{})).To(Succeed())
				testClusterNodeNames := make(map[string]struct{}, len(nodeListForDiag.Items))
				for i := range nodeListForDiag.Items {
					testClusterNodeNames[nodeListForDiag.Items[i].Name] = struct{}{}
				}
				for _, vm := range targetVMs {
					Expect(testClusterNodeNames).To(HaveKey(vm),
						"VM %q must be a node in the test cluster. Nodes: %v", vm, keysOf(testClusterNodeNames))
				}

				var mu sync.Mutex
				var wg sync.WaitGroup
				var attachErrs []error
				for idx, vmName := range createPlan {
					wg.Add(1)
					go func(diskIdx int, vm string) {
						defer wg.Done()
						diskName := fmt.Sprintf("%s-%s-%d", e2eVirtualDiskPrefix, e2eRunID, diskIdx)
						att, attachErr := attachVirtualDiskWithRetry(e2eCtx, baseKubeconfig, kubernetes.VirtualDiskAttachmentConfig{
							VMName:           vm,
							Namespace:        ns,
							DiskName:         diskName,
							DiskSize:         e2eDataDiskSize,
							StorageClassName: storageClass,
						}, e2eVirtualDiskAttachMaxRetries, e2eVirtualDiskAttachRetryInterval)
						mu.Lock()
						if attachErr != nil {
							attachErrs = append(attachErrs, fmt.Errorf("VM %s: %w", vm, attachErr))
						} else {
							e2eDiskAttachments = append(e2eDiskAttachments, att)
						}
						mu.Unlock()
					}(idx, vmName)
				}
				wg.Wait()
				Expect(attachErrs).To(BeEmpty(), "all VirtualDisk attaches should succeed: %v", attachErrs)

				sort.Slice(e2eDiskAttachments, func(i, j int) bool {
					ni, _ := strconv.Atoi(strings.TrimPrefix(e2eDiskAttachments[i].DiskName, e2eVirtualDiskPrefix+"-"))
					nj, _ := strconv.Atoi(strings.TrimPrefix(e2eDiskAttachments[j].DiskName, e2eVirtualDiskPrefix+"-"))
					return ni < nj
				})

				attachCtx, cancel := context.WithTimeout(e2eCtx, 5*time.Minute)
				defer cancel()
				for _, att := range e2eDiskAttachments {
					Expect(kubernetes.WaitForVirtualDiskAttached(attachCtx, baseKubeconfig, ns, att.AttachmentName, 10*time.Second)).To(Succeed())
				}
				By("All VirtualDisks attached; fetching their UIDs for BlockDevice matching")

				baseDynClient, err := dynamic.NewForConfig(baseKubeconfig)
				Expect(err).NotTo(HaveOccurred(), "create dynamic client for base cluster")

				expectedSerials := make(map[string]string)
				for _, att := range e2eDiskAttachments {
					vd, err := baseDynClient.Resource(virtualDiskGVR).Namespace(ns).Get(e2eCtx, att.DiskName, metav1.GetOptions{})
					Expect(err).NotTo(HaveOccurred(), "get VirtualDisk %s", att.DiskName)
					vdUID := string(vd.GetUID())
					serial := blockDeviceSerialFromVirtualDiskUID(vdUID)
					expectedSerials[serial] = att.DiskName
					GinkgoWriter.Printf("  VirtualDisk %s (UID=%s) -> expected BD serial: %s\n", att.DiskName, vdUID, serial)
				}

				By(fmt.Sprintf("Waiting for %d BlockDevices with matching serials (up to 3 minutes)", len(expectedSerials)))
				Eventually(func(g Gomega) {
					var list v1alpha1.BlockDeviceList
					g.Expect(k8sClient.List(e2eCtx, &list, &client.ListOptions{})).To(Succeed())
					createdBlockDevices = nil
					foundSerials := make(map[string]bool)
					for i := range list.Items {
						bd := &list.Items[i]
						if _, expected := expectedSerials[bd.Status.Serial]; expected {
							createdBlockDevices = append(createdBlockDevices, bd)
							foundSerials[bd.Status.Serial] = true
						}
					}
					g.Expect(len(createdBlockDevices)).To(Equal(len(expectedSerials)),
						"expected %d BlockDevices matching VirtualDisks, got %d", len(expectedSerials), len(createdBlockDevices))
				}, 3*time.Minute, 10*time.Second).Should(Succeed())

				By(fmt.Sprintf("Found %d BlockDevices corresponding to created VirtualDisks", len(createdBlockDevices)))
				printBlockDevicesSummary(createdBlockDevices)

				By("Verifying all BlockDevices are consumable")
				var notConsumable []string
				for _, bd := range createdBlockDevices {
					if !bd.Status.Consumable {
						notConsumable = append(notConsumable, fmt.Sprintf("%s (fsType=%s, pvUUID=%s)",
							bd.Name, bd.Status.FsType, bd.Status.PVUuid))
					}
				}
				Expect(notConsumable).To(BeEmpty(),
					"All BlockDevices from new VirtualDisks should be consumable, but these are not: %v", notConsumable)
			})

			It("Should create LVMVolumeGroups from BlockDevices (one per node)", func() {
				Expect(createdBlockDevices).NotTo(BeEmpty(), "BlockDevices must be created first")

				bdsByNode := make(map[string][]*v1alpha1.BlockDevice)
				for _, bd := range createdBlockDevices {
					bdsByNode[bd.Status.NodeName] = append(bdsByNode[bd.Status.NodeName], bd)
				}

				By(fmt.Sprintf("Creating LVMVolumeGroups for %d nodes (with unique runID %s)", len(bdsByNode), e2eRunID))

				for nodeName, bds := range bdsByNode {
					nodeNameSafe := strings.ReplaceAll(strings.ReplaceAll(nodeName, ".", "-"), "_", "-")
					lvgName := fmt.Sprintf("%s%s-%s", e2eLVMVGPrefix, e2eRunID, nodeNameSafe)
					vgName := fmt.Sprintf("e2e-vg-%s-%s", e2eRunID, nodeNameSafe)

					bdNames := make([]string, len(bds))
					for i, bd := range bds {
						bdNames[i] = bd.Name
					}

					lvg := &v1alpha1.LVMVolumeGroup{
						ObjectMeta: metav1.ObjectMeta{Name: lvgName},
						Spec: v1alpha1.LVMVolumeGroupSpec{
							ActualVGNameOnTheNode: vgName,
							BlockDeviceSelector: &metav1.LabelSelector{
								MatchExpressions: []metav1.LabelSelectorRequirement{
									{
										Key:      "kubernetes.io/metadata.name",
										Operator: metav1.LabelSelectorOpIn,
										Values:   bdNames,
									},
								},
							},
							Type:  "Local",
							Local: v1alpha1.LVMVolumeGroupLocalSpec{NodeName: nodeName},
						},
					}

					By(fmt.Sprintf("Creating LVMVolumeGroup %s (VG: %s) on node %s with BlockDevices: %v", lvgName, vgName, nodeName, bdNames))
					err := k8sClient.Create(e2eCtx, lvg)
					Expect(err).NotTo(HaveOccurred(), "create LVMVolumeGroup %s", lvgName)
					createdLVGs = append(createdLVGs, lvg)
				}

				By(fmt.Sprintf("Waiting for %d LVMVolumeGroups to become Ready (up to 5 minutes)", len(createdLVGs)))
				Eventually(func(g Gomega) {
					readyCount := 0
					for _, lvg := range createdLVGs {
						var current v1alpha1.LVMVolumeGroup
						err := k8sClient.Get(e2eCtx, client.ObjectKeyFromObject(lvg), &current)
						g.Expect(err).NotTo(HaveOccurred())
						if current.Status.Phase == v1alpha1.PhaseReady {
							readyCount++
						}
					}
					g.Expect(readyCount).To(Equal(len(createdLVGs)),
						"expected %d Ready LVMVolumeGroups, got %d", len(createdLVGs), readyCount)
				}, 5*time.Minute, 10*time.Second).Should(Succeed())

				totalAvailableSpace = getTotalAvailableSpace(e2eCtx, k8sClient, createdLVGs)
				By(fmt.Sprintf("Total available space across all LVMVolumeGroups: %d bytes (%.2f Gi)",
					totalAvailableSpace, float64(totalAvailableSpace)/(1024*1024*1024)))

				printLVGsSummary(e2eCtx, k8sClient, createdLVGs)
			})

			It("Should create LocalStorageClass and wait for StorageClass", func() {
				Expect(createdLVGs).NotTo(BeEmpty(), "LVMVolumeGroups must be created first")
				Expect(testClusterResources).NotTo(BeNil())
				Expect(testClusterResources.Kubeconfig).NotTo(BeNil())

				var err error
				dynamicClient, err = dynamic.NewForConfig(testClusterResources.Kubeconfig)
				Expect(err).NotTo(HaveOccurred(), "create dynamic client")

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
							"name": e2eLocalStorageClassName,
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

				By(fmt.Sprintf("Creating LocalStorageClass %s with LVMVolumeGroups: %v", e2eLocalStorageClassName, lvgNames))
				Eventually(func(g Gomega) {
					g.Expect(ensureE2ELocalStorageClassAbsent(e2eCtx, testClusterResources.Kubeconfig, k8sClient, e2eLocalStorageClassName)).To(Succeed())
					_, createErr := dynamicClient.Resource(localStorageClassGVR).Create(e2eCtx, lsc.DeepCopy(), metav1.CreateOptions{})
					g.Expect(createErr).NotTo(HaveOccurred(), "create LocalStorageClass")
				}, 5*time.Minute, 10*time.Second).Should(Succeed(), "create LocalStorageClass after prior e2e-local-sc is fully removed")

				By("Waiting for LocalStorageClass to reach Created phase (up to 3 minutes)")
				Eventually(func(g Gomega) {
					lscObj, err := dynamicClient.Resource(localStorageClassGVR).Get(e2eCtx, e2eLocalStorageClassName, metav1.GetOptions{})
					g.Expect(err).NotTo(HaveOccurred())
					phase, _, _ := unstructured.NestedString(lscObj.Object, "status", "phase")
					g.Expect(phase).To(Equal("Created"), "LocalStorageClass phase should be Created, got %s", phase)
				}, 3*time.Minute, 5*time.Second).Should(Succeed())

				By("Waiting for StorageClass to be created (up to 2 minutes)")
				Eventually(func(g Gomega) {
					var scList storagev1.StorageClassList
					err := k8sClient.List(e2eCtx, &scList, &client.ListOptions{})
					g.Expect(err).NotTo(HaveOccurred())

					for i := range scList.Items {
						sc := &scList.Items[i]
						if sc.Name == e2eLocalStorageClassName {
							e2eStorageClassName = sc.Name
							return
						}
					}
					g.Expect(false).To(BeTrue(), "StorageClass %s not found", e2eLocalStorageClassName)
				}, 2*time.Minute, 5*time.Second).Should(Succeed())

				By(fmt.Sprintf("StorageClass %s created successfully", e2eStorageClassName))
			})
		})

		////////////////////////////////////
		// ---=== SCHEDULER TESTS ===--- //
		////////////////////////////////////

		Context("Scheduler Extender: Space consolidation tests", func() {
			It("Should fill storage with small volumes to maximum capacity", func() {
				Expect(createdLVGs).NotTo(BeEmpty(), "LVMVolumeGroups must be created first")
				Expect(e2eStorageClassName).NotTo(BeEmpty(), "StorageClass must be created first")

				By("Cleaning up previous test resources")
				cleanupE2EPodsAndPVCsWithWait(e2eCtx, k8sClient, 3*time.Minute)

				currentAvailable := getTotalAvailableSpace(e2eCtx, k8sClient, createdLVGs)
				Expect(currentAvailable).To(BeNumerically(">", 0), "No available space in LVMVolumeGroups")
				By(fmt.Sprintf("Current available space: %.2f Gi", float64(currentAvailable)/(1024*1024*1024)))

				volumeSize := int64(1 * 1024 * 1024 * 1024) // 1Gi
				minVolumeSize := int64(500 * 1024 * 1024)   // 500Mi minimum for remainder

				numVolumes := int(currentAvailable / volumeSize)
				remainder := currentAvailable % volumeSize

				var volumeSizes []int64
				for i := 0; i < numVolumes; i++ {
					volumeSizes = append(volumeSizes, volumeSize)
				}
				if remainder >= minVolumeSize {
					volumeSizes = append(volumeSizes, remainder)
				}

				totalPlanned := int64(0)
				for _, s := range volumeSizes {
					totalPlanned += s
				}
				utilization := float64(totalPlanned) / float64(currentAvailable) * 100

				By(fmt.Sprintf("Planning %d volumes: %d x %dMi + remainder %dMi = %.2f Gi (%.1f%% utilization)",
					len(volumeSizes), numVolumes, volumeSize/(1024*1024), remainder/(1024*1024),
					float64(totalPlanned)/(1024*1024*1024), utilization))

				successCount, scheduledCount := createPVCsAndPodsWithSizes(e2eCtx, k8sClient, volumeSizes, e2eStorageClassName, "small")

				By(fmt.Sprintf("Results: %d/%d PVCs created, %d/%d Pods scheduled", successCount, len(volumeSizes), scheduledCount, successCount))
				Expect(scheduledCount).To(Equal(successCount),
					"All created PVCs must have scheduled Pods")
				Expect(successCount).To(Equal(len(volumeSizes)),
					"All planned PVCs must be created successfully")

				printSchedulingSummary("small volumes", len(volumeSizes), successCount, scheduledCount, volumeSize)
			})

			It("Should fill storage with medium volumes to maximum capacity", func() {
				Expect(createdLVGs).NotTo(BeEmpty(), "LVMVolumeGroups must be created first")
				Expect(e2eStorageClassName).NotTo(BeEmpty(), "StorageClass must be created first")

				By("Cleaning up previous test resources")
				cleanupE2EPodsAndPVCsWithWait(e2eCtx, k8sClient, 3*time.Minute)

				currentAvailable := getTotalAvailableSpace(e2eCtx, k8sClient, createdLVGs)
				Expect(currentAvailable).To(BeNumerically(">", 0), "No available space in LVMVolumeGroups")
				By(fmt.Sprintf("Current available space: %.2f Gi", float64(currentAvailable)/(1024*1024*1024)))

				volumeSize := int64(5 * 1024 * 1024 * 1024)    // 5Gi
				minVolumeSize := int64(1 * 1024 * 1024 * 1024) // 1Gi minimum for remainder

				numVolumes := int(currentAvailable / volumeSize)
				remainder := currentAvailable % volumeSize

				var volumeSizes []int64
				for i := 0; i < numVolumes; i++ {
					volumeSizes = append(volumeSizes, volumeSize)
				}
				if remainder >= minVolumeSize {
					volumeSizes = append(volumeSizes, remainder)
				}

				totalPlanned := int64(0)
				for _, s := range volumeSizes {
					totalPlanned += s
				}
				utilization := float64(totalPlanned) / float64(currentAvailable) * 100

				By(fmt.Sprintf("Planning %d volumes: %d x %dMi + remainder %dMi = %.2f Gi (%.1f%% utilization)",
					len(volumeSizes), numVolumes, volumeSize/(1024*1024), remainder/(1024*1024),
					float64(totalPlanned)/(1024*1024*1024), utilization))

				successCount, scheduledCount := createPVCsAndPodsWithSizes(e2eCtx, k8sClient, volumeSizes, e2eStorageClassName, "medium")

				By(fmt.Sprintf("Results: %d/%d PVCs created, %d/%d Pods scheduled", successCount, len(volumeSizes), scheduledCount, successCount))
				Expect(scheduledCount).To(Equal(successCount),
					"All created PVCs must have scheduled Pods")
				Expect(successCount).To(Equal(len(volumeSizes)),
					"All planned PVCs must be created successfully")

				printSchedulingSummary("medium volumes", len(volumeSizes), successCount, scheduledCount, volumeSize)
			})

			It("Should fill storage with large volumes to maximum capacity", func() {
				Expect(createdLVGs).NotTo(BeEmpty(), "LVMVolumeGroups must be created first")
				Expect(e2eStorageClassName).NotTo(BeEmpty(), "StorageClass must be created first")

				By("Cleaning up previous test resources")
				cleanupE2EPodsAndPVCsWithWait(e2eCtx, k8sClient, 3*time.Minute)

				currentAvailable := getTotalAvailableSpace(e2eCtx, k8sClient, createdLVGs)
				Expect(currentAvailable).To(BeNumerically(">", 0), "No available space in LVMVolumeGroups")
				By(fmt.Sprintf("Current available space: %.2f Gi", float64(currentAvailable)/(1024*1024*1024)))

				volumeSize := int64(10 * 1024 * 1024 * 1024)   // 10Gi
				minVolumeSize := int64(1 * 1024 * 1024 * 1024) // 1Gi minimum for remainder

				numVolumes := int(currentAvailable / volumeSize)
				remainder := currentAvailable % volumeSize

				if numVolumes == 0 && currentAvailable >= minVolumeSize {
					volumeSizes := []int64{currentAvailable}
					By(fmt.Sprintf("Available space < 10Gi, creating single volume of %.2f Gi", float64(currentAvailable)/(1024*1024*1024)))
					successCount, scheduledCount := createPVCsAndPodsWithSizes(e2eCtx, k8sClient, volumeSizes, e2eStorageClassName, "large")
					Expect(scheduledCount).To(Equal(successCount))
					printSchedulingSummary("large volumes", 1, successCount, scheduledCount, currentAvailable)
					return
				}

				var volumeSizes []int64
				for i := 0; i < numVolumes; i++ {
					volumeSizes = append(volumeSizes, volumeSize)
				}
				if remainder >= minVolumeSize {
					volumeSizes = append(volumeSizes, remainder)
				}

				totalPlanned := int64(0)
				for _, s := range volumeSizes {
					totalPlanned += s
				}
				utilization := float64(totalPlanned) / float64(currentAvailable) * 100

				By(fmt.Sprintf("Planning %d volumes: %d x %dGi + remainder %dMi = %.2f Gi (%.1f%% utilization)",
					len(volumeSizes), numVolumes, volumeSize/(1024*1024*1024), remainder/(1024*1024),
					float64(totalPlanned)/(1024*1024*1024), utilization))

				successCount, scheduledCount := createPVCsAndPodsWithSizes(e2eCtx, k8sClient, volumeSizes, e2eStorageClassName, "large")

				By(fmt.Sprintf("Results: %d/%d PVCs created, %d/%d Pods scheduled", successCount, len(volumeSizes), scheduledCount, successCount))
				Expect(scheduledCount).To(Equal(successCount),
					"All created PVCs must have scheduled Pods")
				Expect(successCount).To(Equal(len(volumeSizes)),
					"All planned PVCs must be created successfully")

				printSchedulingSummary("large volumes", len(volumeSizes), successCount, scheduledCount, volumeSize)
			})
		})
	})

	Describe("Sds Node Configurator", Ordered, func() {
		var (
			testClusterResources *cluster.TestClusterResources
			e2eCtx               context.Context
			k8sClient            client.Client
		)

		BeforeAll(func() {
			By("Outputting environment variables", func() {
				GinkgoWriter.Printf("    📋 Environment variables (without default values):\n")

				// Helper function to mask sensitive values
				maskValue := func(value string, mask bool) string {
					if mask && len(value) > 5 {
						return value[:5] + "***"
					}
					return value
				}

				// E2E_DKP_LICENSE_KEY / DKP_LICENSE_KEY - mask first 5 characters
				if e2eConfigDKPLicenseKey() != "" {
					GinkgoWriter.Printf("      E2E_DKP_LICENSE_KEY: %s\n", maskValue(e2eConfigDKPLicenseKey(), true))
				}

				// E2E_REGISTRY_DOCKER_CFG / REGISTRY_DOCKER_CFG - mask first 5 characters
				if e2eConfigRegistryDockerCfg() != "" {
					GinkgoWriter.Printf("      E2E_REGISTRY_DOCKER_CFG: %s\n", maskValue(e2eConfigRegistryDockerCfg(), true))
				}

				// TEST_CLUSTER_CREATE_MODE - no masking
				if e2eConfigTestClusterCreateMode() != "" {
					GinkgoWriter.Printf("      TEST_CLUSTER_CREATE_MODE: %s\n", e2eConfigTestClusterCreateMode())
				}

				// TEST_CLUSTER_CLEANUP - no masking
				if e2eConfigTestClusterCleanup() != "" {
					GinkgoWriter.Printf("      TEST_CLUSTER_CLEANUP: %s\n", e2eConfigTestClusterCleanup())
				}

				// TEST_CLUSTER_NAMESPACE - no masking
				if e2eConfigNamespace() != "" {
					GinkgoWriter.Printf("      TEST_CLUSTER_NAMESPACE: %s\n", e2eConfigNamespace())
				}

				// TEST_CLUSTER_STORAGE_CLASS - no masking
				if e2eConfigStorageClass() != "" {
					GinkgoWriter.Printf("      TEST_CLUSTER_STORAGE_CLASS: %s\n", e2eConfigStorageClass())
				}

				// SSH_HOST (address) and SSH_USER (login): base cluster connection is SSH_USER@SSH_HOST
				if e2eConfigSSHHost() != "" {
					GinkgoWriter.Printf("      SSH_HOST: %s\n", e2eConfigSSHHost())
				}
				if e2eConfigSSHUser() != "" {
					GinkgoWriter.Printf("      SSH_USER: %s\n", e2eConfigSSHUser())
				}
				if e2eConfigSSHHost() != "" && e2eConfigSSHUser() != "" {
					GinkgoWriter.Printf("      Base cluster SSH: %s@%s\n", e2eConfigSSHUser(), e2eConfigSSHHost())
				}
				// storage-e2e UseExistingCluster: jump → SSH_JUMP_USER@SSH_JUMP_HOST, then SSH_USER@SSH_HOST (not SSH_VM_USER).
				GinkgoWriter.Printf("      SSH_VM_USER (worker VM / direct node SSH): %s\n", e2eConfigVMSSHUser())
				if e2eConfigSSHJumpHost() != "" {
					GinkgoWriter.Printf("      (with jump) bastion: SSH_JUMP_USER@SSH_JUMP_HOST (default SSH_USER if SSH_JUMP_USER unset); target nodes: SSH_USER@SSH_HOST\n")
				}

				// SSH_JUMP_* - no masking (for jump host / bastion)
				if e2eConfigSSHJumpHost() != "" {
					GinkgoWriter.Printf("      SSH_JUMP_HOST: %s\n", e2eConfigSSHJumpHost())
					if e2eConfigSSHJumpUser() != "" {
						GinkgoWriter.Printf("      SSH_JUMP_USER: %s\n", e2eConfigSSHJumpUser())
					}
					if e2eConfigSSHJumpKeyPath() != "" {
						GinkgoWriter.Printf("      SSH_JUMP_KEY_PATH: %s\n", e2eConfigSSHJumpKeyPath())
					}
				}

				// SSH_PASSPHRASE - no masking (optional, may be empty)
				if e2eConfigSSHPassphrase() != "" {
					GinkgoWriter.Printf("      SSH_PASSPHRASE: <set>\n")
				}

				// LOG_LEVEL - no masking
				if e2eConfigLogLevel() != "" {
					GinkgoWriter.Printf("      LOG_LEVEL: %s\n", e2eConfigLogLevel())
				}

				// KUBE_CONFIG_PATH - set from path or by CI from E2E_CLUSTER_KUBECONFIG (content written to file)
				if e2eConfigKubeConfigPath() != "" {
					GinkgoWriter.Printf("      KUBE_CONFIG_PATH: %s\n", e2eConfigKubeConfigPath())
				}
				if v := os.Getenv("E2E_NO_CLUSTER_LOCK_RETRY"); v != "" {
					GinkgoWriter.Printf("      E2E_NO_CLUSTER_LOCK_RETRY: %s (disable auto-delete+retry on stale lock)\n", v)
				}
				// E2E_* for BlockDevice discovery (optional)
				if v := os.Getenv("E2E_NODE_NAME"); v != "" {
					GinkgoWriter.Printf("      E2E_NODE_NAME: %s\n", v)
				}
				if v := os.Getenv("E2E_DEVICE_PATH"); v != "" {
					GinkgoWriter.Printf("      E2E_DEVICE_PATH: %s\n", v)
				}
			})
			e2eCtx = context.Background()

			By("Binding nested test cluster from BeforeSuite", func() {
				testClusterResources = e2eNestedTestClusterOrNil()
				Expect(testClusterResources).NotTo(BeNil(),
					"nested cluster must be created in BeforeSuite (e2eEnsureSharedNestedTestCluster)")
			})
		})

		// Nested cluster teardown: AfterSuite (e2eCleanupNestedTestClusterAfterSuite). Do not call CleanupTestCluster from this Describe.

		////////////////////////////////////
		// ---=== TESTS START HERE ===--- //
		////////////////////////////////////

		Context("Discovery of a manually added block device", func() {
			const e2eDataDiskName = "e2e-blockdevice-data-disk"
			const e2eDataDiskSize = "2Gi"

			var (
				nodeName           string
				expectedDevicePath string
				e2eDiskAttachments []*kubernetes.VirtualDiskAttachmentResult // multiple disks for parallel discovery test
			)

			BeforeEach(func() {
				nodeName = getE2ENodeName()
				expectedDevicePath = getE2EDevicePath()
				if nodeName != "" {
					By(fmt.Sprintf("Filter by node: %s", nodeName))
				} else {
					By("Filter by node: any (node name not required)")
				}
				if expectedDevicePath != "" {
					By(fmt.Sprintf("Expected device path: %s", expectedDevicePath))
				} else {
					By("Expected device path: any block device (path not filtered)")
				}
			})

			AfterEach(func() {
				if len(e2eDiskAttachments) == 0 || testClusterResources == nil {
					return
				}
				ns := e2eConfigNamespace()
				kubeconfig := testClusterResources.BaseKubeconfig
				if kubeconfig == nil {
					kubeconfig = testClusterResources.Kubeconfig
				}
				By("Cleaning up VirtualDisks and attachments")
				for _, att := range e2eDiskAttachments {
					if att != nil {
						_ = kubernetes.DetachAndDeleteVirtualDisk(e2eCtx, kubeconfig, ns, att.AttachmentName, att.DiskName)
					}
				}
				e2eDiskAttachments = nil
			})

			It("Should discover a new unformatted disk and create a BlockDevice object", func() {
				ensureE2EK8sClient(testClusterResources, &k8sClient, e2eCtx)
				By("Expected result: multiple disks on different nodes; each BlockDevice exists, consumable, size > 0")

				var clusterVMs []string
				var baseKubeconfig *rest.Config
				ns := e2eConfigNamespace()
				storageClass := e2eConfigStorageClass()
				Expect(storageClass).NotTo(BeEmpty(), "TEST_CLUSTER_STORAGE_CLASS is required for VirtualDisk")

				if testClusterResources.BaseKubeconfig == nil || testClusterResources.VMResources == nil {
					if testClusterResources.BaseKubeconfig == nil {
						Skip("VirtualDisk discovery in alwaysUseExisting requires base cluster kubeconfig (Deckhouse virtualization). " +
							"Set SSH_JUMP_HOST to the base cluster (jump host = base cluster) so the framework can get its kubeconfig, or use TEST_CLUSTER_CREATE_MODE=alwaysCreateNew.")
					}
					By("Step 0: Listing VirtualMachines on base cluster (jump host)")
					vmNames, listErr := kubernetes.ListVirtualMachineNames(e2eCtx, testClusterResources.BaseKubeconfig, ns)
					Expect(listErr).NotTo(HaveOccurred(), "list VirtualMachines on base cluster")
					Expect(vmNames).NotTo(BeEmpty(), "no VirtualMachines in namespace %s on base cluster", ns)
					clusterVMs = vmNames
					baseKubeconfig = testClusterResources.BaseKubeconfig
				} else {
					clusterVMs = make([]string, 0, len(testClusterResources.VMResources.VMNames))
					for _, name := range testClusterResources.VMResources.VMNames {
						if name != testClusterResources.VMResources.SetupVMName {
							clusterVMs = append(clusterVMs, name)
						}
					}
					Expect(clusterVMs).NotTo(BeEmpty(), "no guest VMs (masters/workers) to attach disk to")
					baseKubeconfig = testClusterResources.BaseKubeconfig
				}

				numNodes := len(clusterVMs)
				parallelism := 1 + rand.Intn(numNodes) // [1, numNodes] — сколько нод задействуем
				if parallelism > numNodes {
					parallelism = numNodes
				}
				// Pick N distinct VMs (shuffle and take first N)
				shuffled := make([]string, numNodes)
				copy(shuffled, clusterVMs)
				rand.Shuffle(numNodes, func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
				targetVMs := shuffled[:parallelism]

				// Second random: disks per node, 1..5
				disksPerNode := make([]int, parallelism)
				for i := 0; i < parallelism; i++ {
					disksPerNode[i] = 1 + rand.Intn(5) // [1, 5] дисков на ноду
				}
				var createPlan []string // createPlan[k] = VM name for k-th disk
				for i, vm := range targetVMs {
					for j := 0; j < disksPerNode[i]; j++ {
						createPlan = append(createPlan, vm)
					}
				}
				totalDisks := len(createPlan)
				By(fmt.Sprintf("Step 0: Creating %d VirtualDisks in parallel on %d nodes %v (disks per node: %v, 1..5 per node)", totalDisks, parallelism, targetVMs, disksPerNode))

				// Ensure all target VMs are nodes in the test cluster
				var nodeListForDiag corev1.NodeList
				Expect(k8sClient.List(e2eCtx, &nodeListForDiag, &client.ListOptions{})).To(Succeed())
				testClusterNodeNames := make(map[string]struct{}, len(nodeListForDiag.Items))
				for i := range nodeListForDiag.Items {
					testClusterNodeNames[nodeListForDiag.Items[i].Name] = struct{}{}
				}
				for _, vm := range targetVMs {
					Expect(testClusterNodeNames).To(HaveKey(vm),
						"VM %q must be a node in the test cluster. SSH_HOST must point to the nested cluster. Nodes: %v", vm, keysOf(testClusterNodeNames))
				}

				// Get initial BlockDevices before creating disks
				var blockDevicesList v1alpha1.BlockDeviceList
				Expect(k8sClient.List(e2eCtx, &blockDevicesList, &client.ListOptions{})).To(Succeed())
				initialNames := make(map[string]struct{}, len(blockDevicesList.Items))
				for i := range blockDevicesList.Items {
					initialNames[blockDevicesList.Items[i].Name] = struct{}{}
				}
				By(fmt.Sprintf("BlockDevices in test cluster before create: %d", len(blockDevicesList.Items)))

				// Create totalDisks in parallel (each entry in createPlan = one disk on that VM)
				var mu sync.Mutex
				var wg sync.WaitGroup
				var attachErrs []error
				for idx, vmName := range createPlan {
					wg.Add(1)
					go func(diskIdx int, vm string) {
						defer wg.Done()
						diskName := fmt.Sprintf("%s-%d", e2eDataDiskName, diskIdx)
						att, attachErr := attachVirtualDiskWithRetry(e2eCtx, baseKubeconfig, kubernetes.VirtualDiskAttachmentConfig{
							VMName:           vm,
							Namespace:        ns,
							DiskName:         diskName,
							DiskSize:         e2eDataDiskSize,
							StorageClassName: storageClass,
						}, e2eVirtualDiskAttachMaxRetries, e2eVirtualDiskAttachRetryInterval)
						mu.Lock()
						if attachErr != nil {
							attachErrs = append(attachErrs, fmt.Errorf("VM %s: %w", vm, attachErr))
						} else {
							e2eDiskAttachments = append(e2eDiskAttachments, att)
						}
						mu.Unlock()
					}(idx, vmName)
				}
				wg.Wait()
				Expect(attachErrs).To(BeEmpty(), "all VirtualDisk attaches should succeed: %v", attachErrs)

				// e2eDiskAttachments was filled in goroutine completion order; sort by disk index so expectedDisks[i] matches createPlan[i].
				sort.Slice(e2eDiskAttachments, func(i, j int) bool {
					ni, _ := strconv.Atoi(strings.TrimPrefix(e2eDiskAttachments[i].DiskName, e2eDataDiskName+"-"))
					nj, _ := strconv.Atoi(strings.TrimPrefix(e2eDiskAttachments[j].DiskName, e2eDataDiskName+"-"))
					return ni < nj
				})

				// Build expected (node, VD name) for each created disk — same order as createPlan (disk index 0, 1, ...).
				expectedDisks := make([]expectedDisk, 0, len(e2eDiskAttachments))
				for i, att := range e2eDiskAttachments {
					expectedDisks = append(expectedDisks, expectedDisk{Node: createPlan[i], VDDiskName: att.DiskName})
				}

				// Wait for all attachments to be Attached
				attachCtx, cancel := context.WithTimeout(e2eCtx, 5*time.Minute)
				defer cancel()
				for _, att := range e2eDiskAttachments {
					Expect(kubernetes.WaitForVirtualDiskAttached(attachCtx, baseKubeconfig, ns, att.AttachmentName, 10*time.Second)).To(Succeed())
				}
				// Expected serial: virtualization may use VD.UID or VMBDA.UID (hex MD5); accept either.
				vdGVR := schema.GroupVersionResource{Group: "virtualization.deckhouse.io", Version: "v1alpha2", Resource: "virtualdisks"}
				attGVR := schema.GroupVersionResource{Group: "virtualization.deckhouse.io", Version: "v1alpha2", Resource: "virtualmachineblockdeviceattachments"}
				dynClient, err := dynamic.NewForConfig(baseKubeconfig)
				Expect(err).NotTo(HaveOccurred(), "create dynamic client for base cluster")
				for i := range expectedDisks {
					vdObj, err := dynClient.Resource(vdGVR).Namespace(ns).Get(e2eCtx, e2eDiskAttachments[i].DiskName, metav1.GetOptions{})
					Expect(err).NotTo(HaveOccurred(), "get VirtualDisk %s for UID", e2eDiskAttachments[i].DiskName)
					expectedDisks[i].ExpectedSerialVD = blockDeviceSerialFromVirtualDiskUID(string(vdObj.GetUID()))
					attObj, err := dynClient.Resource(attGVR).Namespace(ns).Get(e2eCtx, e2eDiskAttachments[i].AttachmentName, metav1.GetOptions{})
					Expect(err).NotTo(HaveOccurred(), "get VirtualMachineBlockDeviceAttachment %s for UID", e2eDiskAttachments[i].AttachmentName)
					expectedDisks[i].ExpectedSerialVMBDA = blockDeviceSerialFromVirtualDiskUID(string(attObj.GetUID()))
				}
				By("All VirtualDisks attached; waiting for BlockDevices in test cluster")

				// Step 2: Wait for totalDisks new BlockDevices (multiple per node allowed)
				var foundBDs []*v1alpha1.BlockDevice
				targetVMsSet := make(map[string]struct{}, len(targetVMs))
				for _, v := range targetVMs {
					targetVMsSet[v] = struct{}{}
				}
				By("Step 2: Waiting for new BlockDevices to appear (up to 5 minutes)")
				Eventually(func(g Gomega) {
					var list v1alpha1.BlockDeviceList
					g.Expect(k8sClient.List(e2eCtx, &list, &client.ListOptions{})).To(Succeed())
					foundBDs = nil
					for i := range list.Items {
						bd := &list.Items[i]
						if _, existed := initialNames[bd.Name]; existed {
							continue
						}
						if _, want := targetVMsSet[bd.Status.NodeName]; !want {
							continue
						}
						if bd.Status.Size.IsZero() || bd.Status.Path == "" || !strings.HasPrefix(bd.Status.Path, "/dev/") {
							continue
						}
						foundBDs = append(foundBDs, bd)
					}
					g.Expect(len(foundBDs)).To(Equal(totalDisks),
						"expected %d new BlockDevices, got %d. Total BDs: %d. %s",
						totalDisks, len(foundBDs), len(list.Items), formatBlockDevicesHint(list.Items, ""))
				}, 5*time.Minute, 10*time.Second).Should(Succeed())

				By(fmt.Sprintf("Found %d BlockDevices", len(foundBDs)))

				// Verify expected vs found: same count per node and pair by (node, order)
				expectedByNode := make(map[string]int)
				for _, e := range expectedDisks {
					expectedByNode[e.Node]++
				}
				foundByNode := make(map[string]int)
				for _, bd := range foundBDs {
					foundByNode[bd.Status.NodeName]++
				}
				for node, want := range expectedByNode {
					Expect(foundByNode[node]).To(Equal(want), "node %s: expected %d BlockDevices, got %d", node, want, foundByNode[node])
				}

				// Sort expected by (node, VD name); order foundBDs to match expectedDisks by serial (path order on same node is undefined).
				sort.Slice(expectedDisks, func(i, j int) bool {
					if expectedDisks[i].Node != expectedDisks[j].Node {
						return expectedDisks[i].Node < expectedDisks[j].Node
					}
					return expectedDisks[i].VDDiskName < expectedDisks[j].VDDiskName
				})
				// Match each expectedDisk to the found BD with same node and serial (VD or VMBDA); order foundBDs accordingly.
				used := make([]bool, len(foundBDs))
				orderedFoundBDs := make([]*v1alpha1.BlockDevice, 0, len(expectedDisks))
				for _, exp := range expectedDisks {
					var matched *v1alpha1.BlockDevice
					for j, bd := range foundBDs {
						if used[j] || bd.Status.NodeName != exp.Node {
							continue
						}
						actualSerial := strings.TrimSpace(bd.Status.Serial)
						if actualSerial == exp.ExpectedSerialVD || actualSerial == exp.ExpectedSerialVMBDA {
							matched = bd
							used[j] = true
							break
						}
					}
					Expect(matched).NotTo(BeNil(), "no BlockDevice on node %s with serial matching VD %s (expected VD=%s or VMBDA=%s)",
						exp.Node, exp.VDDiskName, exp.ExpectedSerialVD, exp.ExpectedSerialVMBDA)
					orderedFoundBDs = append(orderedFoundBDs, matched)
				}
				foundBDs = orderedFoundBDs

				// Build and print name/serial check table (expected vs actual for BD name and serial).
				var nameSerialRows []nameSerialCheckRow
				for i := range expectedDisks {
					exp := expectedDisks[i]
					bd := foundBDs[i]
					actualSerial := strings.TrimSpace(bd.Status.Serial)
					expectedBDName := blockDeviceNameFromDiscoveryInput(
						exp.Node,
						bd.Status.Wwn,
						bd.Status.Model,
						bd.Status.Serial,
						bd.Status.PartUUID,
					)
					serialMatch := actualSerial == exp.ExpectedSerialVD || actualSerial == exp.ExpectedSerialVMBDA
					nameSerialRows = append(nameSerialRows, nameSerialCheckRow{
						Node:                bd.Status.NodeName,
						VDName:              exp.VDDiskName,
						BDName:              bd.Name,
						ExpectedSerialVD:    exp.ExpectedSerialVD,
						ExpectedSerialVMBDA: exp.ExpectedSerialVMBDA,
						ActualSerial:        actualSerial,
						SerialMatch:         serialMatch,
						ExpectedBDName:      expectedBDName,
						ActualBDName:        bd.Name,
						NameMatch:           bd.Name == expectedBDName,
					})
				}
				printBlockDeviceNameSerialTable(nameSerialRows)

				for i := range expectedDisks {
					Expect(foundBDs[i].Status.NodeName).To(Equal(expectedDisks[i].Node),
						"BlockDevice %s must be on expected node %s (got %s)", foundBDs[i].Name, expectedDisks[i].Node, foundBDs[i].Status.NodeName)
					// Serial: virtualization may use VD.UID or VMBDA.UID (hex MD5)
					actualSerial := strings.TrimSpace(foundBDs[i].Status.Serial)
					Expect([]string{expectedDisks[i].ExpectedSerialVD, expectedDisks[i].ExpectedSerialVMBDA}).To(ContainElement(actualSerial),
						"BlockDevice serial must match VirtualDisk or VirtualMachineBlockDeviceAttachment UID (MD5): expected one of [%s, %s], got %s",
						expectedDisks[i].ExpectedSerialVD, expectedDisks[i].ExpectedSerialVMBDA, actualSerial)
					// Name: use actual status fields from BD so formula matches what agent hashed (serial may have \n from sysfs; model/wwn from lsblk)
					expectedBDName := blockDeviceNameFromDiscoveryInput(
						expectedDisks[i].Node,
						foundBDs[i].Status.Wwn,
						foundBDs[i].Status.Model,
						foundBDs[i].Status.Serial,
						foundBDs[i].Status.PartUUID,
					)
					Expect(foundBDs[i].Name).To(Equal(expectedBDName),
						"BlockDevice name must match agent formula (dev-SHA1(node+wwn+model+serial+partUUID)): expected %s, got %s", expectedBDName, foundBDs[i].Name)
				}

				// lsblk on each node: connect to node IP the same way we connect to the master (SSH_HOST / jump → node)
				By("Running lsblk on cluster nodes and comparing with BlockDevice status")
				lsblkByNode := make(map[string]map[string]lsblkLine)
				nodesToLsblk := make(map[string]struct{})
				for _, bd := range foundBDs {
					nodesToLsblk[bd.Status.NodeName] = struct{}{}
				}
				sshUser := e2eConfigVMSSHUser()
				for nodeName := range nodesToLsblk {
					lines, err := runLsblkViaDirectSSHWithRetry(e2eCtx, testClusterResources.Kubeconfig, nodeName, sshUser, e2eLsblkSSHMaxRetries, e2eLsblkSSHRetryInterval)
					Expect(err).NotTo(HaveOccurred(), "lsblk on node %s must succeed (SSH to node for discovery verification)", nodeName)
					lsblkByNode[nodeName] = lines
				}

				// Build summary rows (paired expected + found + lsblk)
				var summary []discoveryTableRow
				for i := range foundBDs {
					bd := foundBDs[i]
					exp := expectedDisks[i]
					nodeName := bd.Status.NodeName
					path := bd.Status.Path
					lsblkMap := lsblkByNode[nodeName]
					line, hasLine := lsblkMap[path]
					row := discoveryTableRow{
						Node:     nodeName,
						VDName:   exp.VDDiskName,
						BDName:   bd.Name,
						Path:     path,
						SerialBD: bd.Status.Serial,
						SizeBD:   bd.Status.Size.String(),
					}
					if hasLine {
						row.SerialLsblk = line.Serial
						row.SizeLsblk = line.Size
						row.Match = bd.Status.Serial == line.Serial && bd.Status.Size.Value() == line.SizeBytes
					}
					summary = append(summary, row)
				}
				for _, row := range summary {
					if row.SerialLsblk != "" && !row.Match {
						Expect(row.Match).To(BeTrue(), "BD %s path %s: serial/size must match lsblk (BD serial=%s lsblk serial=%s, BD size=%s lsblk size=%s)",
							row.BDName, row.Path, row.SerialBD, row.SerialLsblk, row.SizeBD, row.SizeLsblk)
					}
				}

				printDiscoveryTable(summary)
			})
		})

		Context("LVMVolumeGroup with one disk and thin-pool", Ordered, func() {
			const e2eLVGDataDiskName = "e2e-lvg-data-disk"
			const e2eLVGDataDiskSize = "2Gi"

			var (
				lvgE2eDiskAttachment    *kubernetes.VirtualDiskAttachmentResult
				targetBD                *v1alpha1.BlockDevice
				lvg                     *v1alpha1.LVMVolumeGroup
				nodeName                string
				thinPoolName            string
				thinPoolAllocationLimit string
			)

			fetchCurrentLVG := func() v1alpha1.LVMVolumeGroup {
				var current v1alpha1.LVMVolumeGroup
				Expect(k8sClient.Get(e2eCtx, client.ObjectKeyFromObject(lvg), &current)).To(Succeed())
				return current
			}

			assertNoFailedConditions := func(current *v1alpha1.LVMVolumeGroup) {
				for _, c := range current.Status.Conditions {
					Expect(c.Status).NotTo(Equal(metav1.ConditionFalse),
						"condition %s has status False: reason=%s message=%s", c.Type, c.Reason, c.Message)
				}
			}

			BeforeEach(func() {
				ensureE2EK8sClient(testClusterResources, &k8sClient, e2eCtx)
				Expect(testClusterResources.BaseKubeconfig).NotTo(BeNil(), "LVMVolumeGroup test requires nested virtualization (base cluster)")

				ns := e2eConfigNamespace()
				var clusterVMs []string
				if testClusterResources.VMResources != nil {
					for _, name := range testClusterResources.VMResources.VMNames {
						if name != testClusterResources.VMResources.SetupVMName {
							clusterVMs = append(clusterVMs, name)
						}
					}
				}
				if len(clusterVMs) == 0 {
					By("VM list not from VMResources (e.g. alwaysUseExisting); listing VirtualMachines on base cluster")
					vmNames, listErr := kubernetes.ListVirtualMachineNames(e2eCtx, testClusterResources.BaseKubeconfig, ns)
					Expect(listErr).NotTo(HaveOccurred(), "list VirtualMachines on base cluster")
					Expect(vmNames).NotTo(BeEmpty(), "no VirtualMachines in namespace %s on base cluster (LVM test needs a VM to attach disk to)", ns)
					clusterVMs = vmNames
				}

				targetVM := clusterVMs[rand.Intn(len(clusterVMs))]
				storageClass := e2eConfigStorageClass()
				Expect(storageClass).NotTo(BeEmpty(), "TEST_CLUSTER_STORAGE_CLASS is required for VirtualDisk")

				var blockDevicesList v1alpha1.BlockDeviceList
				err := k8sClient.List(e2eCtx, &blockDevicesList, &client.ListOptions{})
				Expect(err).NotTo(HaveOccurred())
				initialNames := make(map[string]struct{}, len(blockDevicesList.Items))
				for i := range blockDevicesList.Items {
					initialNames[blockDevicesList.Items[i].Name] = struct{}{}
				}
				By(fmt.Sprintf("BlockDevices before attach: %d", len(initialNames)))

				By("Attaching one VirtualDisk to guest VM " + targetVM + " for LVG")
				var attachErr error
				lvgE2eDiskAttachment, attachErr = attachVirtualDiskWithRetry(e2eCtx, testClusterResources.BaseKubeconfig, kubernetes.VirtualDiskAttachmentConfig{
					VMName:           targetVM,
					Namespace:        ns,
					DiskName:         e2eLVGDataDiskName,
					DiskSize:         e2eLVGDataDiskSize,
					StorageClassName: storageClass,
				}, e2eVirtualDiskAttachMaxRetries, e2eVirtualDiskAttachRetryInterval)
				Expect(attachErr).NotTo(HaveOccurred())

				attachCtx, cancel := context.WithTimeout(e2eCtx, 5*time.Minute)
				defer cancel()
				Expect(kubernetes.WaitForVirtualDiskAttached(attachCtx, testClusterResources.BaseKubeconfig, ns, lvgE2eDiskAttachment.AttachmentName, 10*time.Second)).To(Succeed())
				By("VirtualDisk attached; waiting for BlockDevice in test cluster")

				targetBD = nil
				Eventually(func(g Gomega) {
					var list v1alpha1.BlockDeviceList
					g.Expect(k8sClient.List(e2eCtx, &list, &client.ListOptions{})).To(Succeed())
					targetBD = nil
					for i := range list.Items {
						bd := &list.Items[i]
						if _, existed := initialNames[bd.Name]; existed {
							continue
						}
						if bd.Status.NodeName != targetVM {
							continue
						}
						if !bd.Status.Consumable || bd.Status.Size.IsZero() || bd.Status.Path == "" || !strings.HasPrefix(bd.Status.Path, "/dev/") {
							continue
						}
						targetBD = bd
						return
					}
					g.Expect(targetBD).NotTo(BeNil(), "new consumable BlockDevice on node %s not found yet. %s", targetVM, formatBlockDevicesHint(list.Items, targetVM))
				}, 5*time.Minute, 10*time.Second).Should(Succeed())

				nodeName = targetBD.Status.NodeName
				bdMetaName := targetBD.Labels["kubernetes.io/metadata.name"]
				if bdMetaName == "" {
					bdMetaName = targetBD.Name
				}

				vgName := "e2e-vg"
				thinPoolName = "e2e-thin-pool"
				thinPoolAllocationLimit = "100%"
				thinPoolSize := "60%"
				// Not 50%: half of a 2Gi disk rounds to 1Gi in spec while LVM may allocate slightly more
				// bytes (alignment), and VGConfigurationApplied then fails ValidationFailed (requested < actual).
				lvgName := "e2e-lvg-" + strings.ReplaceAll(strings.ReplaceAll(nodeName, ".", "-"), "_", "-")
				lvg = &v1alpha1.LVMVolumeGroup{
					ObjectMeta: metav1.ObjectMeta{Name: lvgName},
					Spec: v1alpha1.LVMVolumeGroupSpec{
						ActualVGNameOnTheNode: vgName,
						BlockDeviceSelector: &metav1.LabelSelector{
							MatchLabels: map[string]string{
								"kubernetes.io/hostname":      nodeName,
								"kubernetes.io/metadata.name": bdMetaName,
							},
						},
						ThinPools: []v1alpha1.LVMVolumeGroupThinPoolSpec{
							{Name: thinPoolName, Size: thinPoolSize, AllocationLimit: thinPoolAllocationLimit},
						},
						Type:  "Local",
						Local: v1alpha1.LVMVolumeGroupLocalSpec{NodeName: nodeName},
					},
				}

				By(fmt.Sprintf("Creating LVMVolumeGroup %s on node %s, VG %s, thin-pool %s %s", lvg.Name, nodeName, vgName, thinPoolName, thinPoolSize))
				err = k8sClient.Create(e2eCtx, lvg)
				Expect(err).NotTo(HaveOccurred())

				defer func() {
					if lvg == nil {
						return
					}
					var current v1alpha1.LVMVolumeGroup
					if err := k8sClient.Get(e2eCtx, client.ObjectKey{Name: lvg.Name}, &current); err == nil && current.Status.Phase != v1alpha1.PhaseReady {
						GinkgoWriter.Println("\n--- LVMVolumeGroup did not become Ready; current state ---")
						printLVMVolumeGroupInfo(&current)
					}
				}()

				By("Waiting for LVMVolumeGroup to become Ready (up to 5 minutes)")
				Eventually(func(g Gomega) {
					current := fetchCurrentLVG()
					g.Expect(current.Status.Phase).To(Equal(v1alpha1.PhaseReady), "Phase should be Ready, got %s", current.Status.Phase)
				}, 5*time.Minute, 10*time.Second).Should(Succeed())
			})

			AfterEach(func() {
				if lvg != nil {
					By("Cleaning up LVMVolumeGroup")
					_ = k8sClient.Delete(e2eCtx, lvg)
					lvg = nil
				}
				if lvgE2eDiskAttachment == nil || testClusterResources == nil || testClusterResources.BaseKubeconfig == nil {
					return
				}
				ns := e2eConfigNamespace()
				By("Cleaning up LVMVolumeGroup test VirtualDisk and attachment")
				_ = kubernetes.DetachAndDeleteVirtualDisk(e2eCtx, testClusterResources.BaseKubeconfig, ns, lvgE2eDiskAttachment.AttachmentName, lvgE2eDiskAttachment.DiskName)
				lvgE2eDiskAttachment = nil
				targetBD = nil
				nodeName = ""
			})

			It("Should create LVMVolumeGroup with one disk and thin-pool", func() {
				By("Expected result: VG with name + tag storage.deckhouse.io/enabled=true; thin-pool with expected name/size; LVMVolumeGroup Phase Ready; conditions without errors")
				created := fetchCurrentLVG()
				By("Verifying conditions (no errors)")
				assertNoFailedConditions(&created)
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

				By("✓ LVMVolumeGroup Ready; thin-pool present and Ready; conditions without errors")
				printLVMVolumeGroupInfo(&created)
			})

			It("should grow VG after underlying VirtualDisk resize (pvresize)", func() {
				By("Patching VirtualDisk to increase size (simulate block device expansion)")
				ns := e2eConfigNamespace()
				dynClient, err := dynamic.NewForConfig(testClusterResources.BaseKubeconfig)
				Expect(err).NotTo(HaveOccurred())

				vdGVR := schema.GroupVersionResource{Group: "virtualization.deckhouse.io", Version: "v1alpha2", Resource: "virtualdisks"}
				newSize := "3Gi"
				patchBytes := []byte(fmt.Sprintf(`{"spec":{"persistentVolumeClaim":{"size":"%s"}}}`, newSize))

				_, err = dynClient.Resource(vdGVR).Namespace(ns).Patch(e2eCtx, lvgE2eDiskAttachment.DiskName, types.MergePatchType, patchBytes, metav1.PatchOptions{})
				Expect(err).NotTo(HaveOccurred(), "Failed to patch VirtualDisk %s to size %s", lvgE2eDiskAttachment.DiskName, newSize)

				expectedMinBytes := int64(2.5 * 1024 * 1024 * 1024)

				By("Waiting for BlockDevice in test cluster to reflect new size (3Gi)")
				Eventually(func(g Gomega) {
					var bd v1alpha1.BlockDevice
					g.Expect(k8sClient.Get(e2eCtx, client.ObjectKey{Name: targetBD.Name}, &bd)).To(Succeed())
					g.Expect(bd.Status.Size.Value()).To(BeNumerically(">", expectedMinBytes), "BlockDevice size should increase to approximately 3Gi")
				}, 5*time.Minute, 10*time.Second).Should(Succeed())

				By("Waiting for pvs on the node to reflect the new PV size")
				Eventually(func(g Gomega) {
					pvSize, err := getPVSizeViaDirectSSHWithRetry(e2eCtx, testClusterResources.Kubeconfig, nodeName, e2eConfigVMSSHUser(), targetBD.Status.Path, e2eLsblkSSHMaxRetries, e2eLsblkSSHRetryInterval)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(pvSize).To(BeNumerically(">", expectedMinBytes), "pvs should report the resized PV size")
				}, 5*time.Minute, 10*time.Second).Should(Succeed())

				By("Waiting for LVMVolumeGroup to become Ready with updated VGSize")
				Eventually(func(g Gomega) {
					updated := fetchCurrentLVG()
					g.Expect(updated.Status.Phase).To(Equal(v1alpha1.PhaseReady), "Phase should be Ready after resize")
					g.Expect(updated.Status.VGSize.Value()).To(BeNumerically(">", expectedMinBytes), "VGSize should be updated to approximately 3Gi")
					for _, c := range updated.Status.Conditions {
						g.Expect(c.Reason).NotTo(Equal("PVResizeFailed"), "PVResizeFailed condition should not appear after successful resize")
					}
				}, 5*time.Minute, 10*time.Second).Should(Succeed())

				By("Ensuring PVResizeFailed does not appear and pvresize is not retried in a loop")
				Consistently(func(g Gomega) {
					current := fetchCurrentLVG()
					for _, c := range current.Status.Conditions {
						g.Expect(c.Reason).NotTo(Equal("PVResizeFailed"), "PVResizeFailed condition should stay absent after resize converges")
					}
				}, 45*time.Second, 15*time.Second).Should(Succeed())

				resizeCount, err := countResizePVSuccessLogs(e2eCtx, testClusterResources.Kubeconfig, nodeName, targetBD.Status.Path)
				Expect(err).NotTo(HaveOccurred())
				Expect(resizeCount).To(BeNumerically(">=", 1), "expected at least one successful pvresize log entry")

				Consistently(func() int {
					count, logErr := countResizePVSuccessLogs(e2eCtx, testClusterResources.Kubeconfig, nodeName, targetBD.Status.Path)
					Expect(logErr).NotTo(HaveOccurred())
					return count
				}, 45*time.Second, 15*time.Second).Should(Equal(resizeCount), "pvresize invocation count should stay stable after convergence")

				By("✓ PV Resize Test Passed")
			})

			const (
				e2eLVGPVResizeDiskName = "e2e-lvg-pvresize-disk"
				e2eLVGPVResizeDiskSize = "2Gi"
				e2eLVGPVResizeNewSize  = "4Gi"
				// e2eLVGPVResizeThinPoolName must match the pvresize LVMVolumeGroup spec (follow-up delete test prunes this pool on the node).
				e2eLVGPVResizeThinPoolName = "e2e-thin-pool-pvresize"
			)

			It("Should grow PV and VG free space after block device resize (pvresize)", func() {
				ensureE2EK8sClient(testClusterResources, &k8sClient, e2eCtx)
				By("Expected: after VirtualDisk/PVC grow, BlockDevice size increases, agent runs pvresize, LVMVolumeGroup stays Ready, VGFree grows, no False conditions")

				Expect(testClusterResources.BaseKubeconfig).NotTo(BeNil(), "pvresize test requires nested virtualization (base cluster)")
				ns := e2eConfigNamespace()
				By("Guest VM for attach: phase Running only (skip Migrating)")
				clusterVMs := e2eListClusterVMNames(e2eCtx, testClusterResources, ns)

				targetVM := clusterVMs[rand.Intn(len(clusterVMs))]
				storageClass := e2eConfigStorageClass()
				Expect(storageClass).NotTo(BeEmpty(), "TEST_CLUSTER_STORAGE_CLASS is required for VirtualDisk")

				var blockDevicesList v1alpha1.BlockDeviceList
				err := k8sClient.List(e2eCtx, &blockDevicesList, &client.ListOptions{})
				Expect(err).NotTo(HaveOccurred())
				initialNames := make(map[string]struct{}, len(blockDevicesList.Items))
				for i := range blockDevicesList.Items {
					initialNames[blockDevicesList.Items[i].Name] = struct{}{}
				}
				By(fmt.Sprintf("BlockDevices before attach: %d (cluster inventory; agent re-lists disks after cleanup)", len(initialNames)))

				By("Attaching VirtualDisk for pvresize scenario to guest VM " + targetVM)
				var attachErr error
				lvgE2eDiskAttachment, attachErr = attachVirtualDiskWithRetry(e2eCtx, testClusterResources.BaseKubeconfig, kubernetes.VirtualDiskAttachmentConfig{
					VMName:           targetVM,
					Namespace:        ns,
					DiskName:         e2eLVGPVResizeDiskName,
					DiskSize:         e2eLVGPVResizeDiskSize,
					StorageClassName: storageClass,
				}, e2eVirtualDiskAttachMaxRetries, e2eVirtualDiskAttachRetryInterval)
				Expect(attachErr).NotTo(HaveOccurred())

				attachCtx, cancel := context.WithTimeout(e2eCtx, e2eVirtualDiskAttachWaitTimeout)
				defer cancel()
				Expect(kubernetes.WaitForVirtualDiskAttached(attachCtx, testClusterResources.BaseKubeconfig, ns, lvgE2eDiskAttachment.AttachmentName, 10*time.Second)).To(Succeed())
				By("VirtualDisk attached; waiting for BlockDevice with serial matching this VirtualDisk (md5(UID))")
				targetBD := e2eWaitConsumableBlockDeviceForVirtualDisk(e2eCtx, testClusterResources.BaseKubeconfig, k8sClient, ns,
					lvgE2eDiskAttachment.DiskName, lvgE2eDiskAttachment.AttachmentName, targetVM)

				nodeName := targetBD.Status.NodeName
				bdMetaName := targetBD.Labels["kubernetes.io/metadata.name"]
				if bdMetaName == "" {
					bdMetaName = targetBD.Name
				}

				Expect(sdsLvgE2eRunID).NotTo(BeEmpty(), "LVM suite pre-run cleanup must set sdsLvgE2eRunID (BeforeEach)")
				vgName := "e2e-vg-pvresize-" + sdsLvgE2eRunID
				thinPoolName := e2eLVGPVResizeThinPoolName
				thinPoolSize := "60%"
				thinPoolAllocationLimit := "100%"
				lvgName := "e2e-lvg-pvresize-" + sdsLvgE2eRunID + "-" + strings.ReplaceAll(strings.ReplaceAll(nodeName, ".", "-"), "_", "-")
				lvg := &v1alpha1.LVMVolumeGroup{
					ObjectMeta: metav1.ObjectMeta{Name: lvgName},
					Spec: v1alpha1.LVMVolumeGroupSpec{
						ActualVGNameOnTheNode: vgName,
						BlockDeviceSelector: &metav1.LabelSelector{
							MatchLabels: map[string]string{
								"kubernetes.io/hostname":      nodeName,
								"kubernetes.io/metadata.name": bdMetaName,
							},
						},
						ThinPools: []v1alpha1.LVMVolumeGroupThinPoolSpec{
							{Name: thinPoolName, Size: thinPoolSize, AllocationLimit: thinPoolAllocationLimit},
						},
						Type:  "Local",
						Local: v1alpha1.LVMVolumeGroupLocalSpec{NodeName: nodeName},
					},
				}
				By(fmt.Sprintf("Creating LVMVolumeGroup %s (VG %s) for pvresize test", lvgName, vgName))
				err = k8sClient.Create(e2eCtx, lvg)
				Expect(err).NotTo(HaveOccurred())
				// LVG + VirtualDisk are kept for the next test "Should remove VG when LVMVolumeGroup CR is deleted" (no second attach).

				defer func() {
					var current v1alpha1.LVMVolumeGroup
					if err := k8sClient.Get(e2eCtx, client.ObjectKey{Name: lvg.Name}, &current); err == nil && current.Status.Phase != v1alpha1.PhaseReady {
						GinkgoWriter.Println("\n--- LVMVolumeGroup (pvresize test) not Ready; current state ---")
						printLVMVolumeGroupInfo(&current)
					}
				}()

				var readyLVG v1alpha1.LVMVolumeGroup
				Eventually(func(g Gomega) {
					err := k8sClient.Get(e2eCtx, client.ObjectKeyFromObject(lvg), &readyLVG)
					g.Expect(err).NotTo(HaveOccurred())
					if readyLVG.Status.Phase != v1alpha1.PhaseReady {
						GinkgoWriter.Printf("LVMVolumeGroup %s phase=%s (pvresize test, waiting for Ready)\n", lvg.Name, readyLVG.Status.Phase)
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
					if n.Name != nodeName {
						continue
					}
					for _, d := range n.Devices {
						if d.BlockDevice == targetBD.Name {
							baselinePVSize = d.PVSize.Value()
							foundDev = true
							break
						}
					}
				}
				Expect(foundDev).To(BeTrue(), "LVMVolumeGroup status should list device for BlockDevice %s", targetBD.Name)
				Expect(baselinePVSize).To(BeNumerically(">", 0), "baseline PV size should be reported")

				var bdBefore v1alpha1.BlockDevice
				Expect(k8sClient.Get(e2eCtx, client.ObjectKey{Name: targetBD.Name}, &bdBefore)).To(Succeed())
				baselineBDSize := bdBefore.Status.Size.Value()

				By("LVMVolumeGroup before disk resize (baseline for comparison with post-resize output below)")
				GinkgoWriter.Printf("    BlockDevice %s status size before resize: %s\n", targetBD.Name, bdBefore.Status.Size.String())
				printLVMVolumeGroupInfo(&readyLVG)

				By(fmt.Sprintf("Growing VirtualDisk %s: %s -> %s", e2eLVGPVResizeDiskName, e2eLVGPVResizeDiskSize, e2eLVGPVResizeNewSize))
				Expect(e2ePatchVirtualDiskSize(e2eCtx, testClusterResources.BaseKubeconfig, ns, e2eLVGPVResizeDiskName, e2eLVGPVResizeNewSize)).To(Succeed())

				By("Waiting for VirtualDisk to return to Ready after resize")
				virtCl, err := e2eNewVirtClient(testClusterResources.BaseKubeconfig)
				Expect(err).NotTo(HaveOccurred())
				Eventually(func(g Gomega) {
					var vd virtv1alpha2.VirtualDisk
					g.Expect(virtCl.Get(e2eCtx, client.ObjectKey{Namespace: ns, Name: e2eLVGPVResizeDiskName}, &vd)).To(Succeed())
					g.Expect(vd.Status.Phase).To(Equal(virtv1alpha2.DiskReady), "VirtualDisk phase: %s", vd.Status.Phase)
				}, 10*time.Minute, 10*time.Second).Should(Succeed())

				By("Waiting for BlockDevice status size to reflect larger disk")
				Eventually(func(g Gomega) {
					var bd v1alpha1.BlockDevice
					g.Expect(k8sClient.Get(e2eCtx, client.ObjectKey{Name: targetBD.Name}, &bd)).To(Succeed())
					g.Expect(bd.Status.Size.Value()).To(BeNumerically(">", baselineBDSize),
						"BlockDevice %s size should grow after PVC resize (was %d)", targetBD.Name, baselineBDSize)
				}, 5*time.Minute, 10*time.Second).Should(Succeed())

				By("Waiting for LVMVolumeGroup: Ready, larger VGFree and PV after pvresize")
				Eventually(func(g Gomega) {
					var cur v1alpha1.LVMVolumeGroup
					g.Expect(k8sClient.Get(e2eCtx, client.ObjectKeyFromObject(lvg), &cur)).To(Succeed())
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
						if n.Name != nodeName {
							continue
						}
						for _, d := range n.Devices {
							if d.BlockDevice == targetBD.Name {
								pvSize = d.PVSize.Value()
								found = true
								break
							}
						}
					}
					g.Expect(found).To(BeTrue(), "device for BlockDevice %s in status", targetBD.Name)
					g.Expect(pvSize).To(BeNumerically(">", baselinePVSize),
						"PV size should grow after pvresize (baseline %d)", baselinePVSize)
				}, 10*time.Minute, 15*time.Second).Should(Succeed())

				var final v1alpha1.LVMVolumeGroup
				Expect(k8sClient.Get(e2eCtx, client.ObjectKeyFromObject(lvg), &final)).To(Succeed())
				By("✓ After disk resize: LVMVolumeGroup Ready, VGFree and PV size increased, no error conditions")
				printLVMVolumeGroupInfo(&final)

				e2eSavedLVGForVGRemoveTest = &e2eSavedLVGForVGRemoveInfo{
					lvgName:         lvg.Name,
					nodeName:        nodeName,
					vgNameOnNode:    lvg.Spec.ActualVGNameOnTheNode,
					blockDeviceName: targetBD.Name,
				}
				e2eDeferVDCleanupUntilLVGDeleteTest = true
			})

			It("Should remove VG from node when LVMVolumeGroup CR is deleted", func() {
				ensureE2EK8sClient(testClusterResources, &k8sClient, e2eCtx)
				Expect(testClusterResources.BaseKubeconfig).NotTo(BeNil(), "test requires nested virtualization (base cluster)")

				Expect(e2eSavedLVGForVGRemoveTest).NotTo(BeNil(),
					"the pvresize test must run first and leave a Ready LVMVolumeGroup + attached VirtualDisk")

				By("Chain: (1) pvresize test created a Ready LVMVolumeGroup with thin pool on one node and left the CR + VirtualDisk; " +
					"(2) this test deletes only the LVMVolumeGroup CR; (3) agent should remove the VG on the node when allowed; " +
					"(4) BlockDevice CR should remain (disk stays attached)")
				s := e2eSavedLVGForVGRemoveTest
				lvgName := s.lvgName
				nodeName := s.nodeName
				vgName := s.vgNameOnNode
				bdName := s.blockDeviceName

				var lvgCur v1alpha1.LVMVolumeGroup
				Expect(k8sClient.Get(e2eCtx, client.ObjectKey{Name: lvgName}, &lvgCur)).To(Succeed())
				Expect(lvgCur.Status.Phase).To(Equal(v1alpha1.PhaseReady), "LVG from pvresize must still be Ready before delete")
				printLVMVolumeGroupInfo(&lvgCur)

				vmSSH := e2eConfigVMSSHUser()
				vgsCmd := "vgs -o vg_name --noheadings 2>/dev/null || sudo -n vgs -o vg_name --noheadings 2>/dev/null"

				By("Checking VG exists on node before CR deletion")
				GinkgoWriter.Printf("    node=%s sshUser=%s command=%q\n", nodeName, vmSSH, vgsCmd)
				outBefore, errSSH := e2eExecOnTestClusterNodeSSH(e2eCtx, testClusterResources.Kubeconfig, nodeName, vmSSH, vgsCmd)
				Expect(errSSH).NotTo(HaveOccurred(), "vgs on node %s", nodeName)
				if strings.TrimSpace(outBefore) == "" {
					GinkgoWriter.Printf("    vgs stdout: <empty>\n")
				} else {
					GinkgoWriter.Printf("    vgs stdout (raw):\n%s\n", outBefore)
				}
				foundVG := e2eVgNameListedInVgsOutput(outBefore, vgName)
				GinkgoWriter.Printf("    expect VG name %q among vg_name lines: found=%v\n", vgName, foundVG)
				Expect(foundVG).To(BeTrue(),
					"VG %q should exist on node before delete; vgs output above", vgName)

				// E2E-only workaround: remove thin-pool LVs on the node before deleting the CR. The product agent should
				// tear down the pool during delete, but that path can leave the CR stuck Terminating; pruning here keeps
				// the test focused on vgremove + BlockDevice retention without depending on agent delete ordering.
				thinPool := e2eLVGPVResizeThinPoolName
				if len(lvgCur.Spec.ThinPools) > 0 {
					if n := strings.TrimSpace(lvgCur.Spec.ThinPools[0].Name); n != "" {
						thinPool = n
					}
				} else if len(lvgCur.Status.ThinPools) > 0 {
					if n := strings.TrimSpace(lvgCur.Status.ThinPools[0].Name); n != "" {
						thinPool = n
					}
				}
				By("E2E workaround: lvremove thin-pool stack on node so LVMVolumeGroup CR deletion can finish")
				GinkgoWriter.Printf("    vg=%q thinPool=%q\n", vgName, thinPool)
				pruneScript := e2eShellRemoveThinPoolStackForVG(vgName, thinPool)
				outPrune, errPrune := e2eExecOnTestClusterNodeSSH(e2eCtx, testClusterResources.Kubeconfig, nodeName, vmSSH, pruneScript)
				if outPrune != "" {
					GinkgoWriter.Printf("    prune script output:\n%s\n", outPrune)
				}
				Expect(errPrune).NotTo(HaveOccurred(), "thin-pool prune on node %s", nodeName)
				lvsCmd := fmt.Sprintf(`lvs -q --noheadings -o lv_name %q 2>/dev/null || sudo -n lvs -q --noheadings -o lv_name %q 2>/dev/null`, vgName, vgName)
				Eventually(func(g Gomega) {
					out, err := e2eExecOnTestClusterNodeSSH(e2eCtx, testClusterResources.Kubeconfig, nodeName, vmSSH, lvsCmd)
					g.Expect(err).NotTo(HaveOccurred())
					lines := 0
					for _, line := range strings.Split(out, "\n") {
						if strings.TrimSpace(line) != "" {
							lines++
						}
					}
					g.Expect(lines).To(BeZero(), "expected no LVs left in VG %s before CR delete; lvs output:\n%s", vgName, out)
				}, 3*time.Minute, 5*time.Second).Should(Succeed())

				By("Deleting LVMVolumeGroup CR")
				Expect(k8sClient.Delete(e2eCtx, &lvgCur)).To(Succeed())

				By("Waiting for LVMVolumeGroup CR to be removed from API")
				Eventually(func(g Gomega) {
					var cur v1alpha1.LVMVolumeGroup
					err := k8sClient.Get(e2eCtx, client.ObjectKey{Name: lvgName}, &cur)
					if apierrors.IsNotFound(err) {
						GinkgoWriter.Printf("    LVMVolumeGroup %q: Get → NotFound (removed from API)\n", lvgName)
						return
					}
					g.Expect(err).NotTo(HaveOccurred(), "Get LVMVolumeGroup %s", lvgName)
					GinkgoWriter.Printf("    … still in API: phase=%s deletionTimestamp=%v finalizers=%v resourceVersion=%s\n",
						cur.Status.Phase, cur.DeletionTimestamp, cur.Finalizers, cur.ResourceVersion)
					g.Expect(false).To(BeTrue(), "LVMVolumeGroup %q should be removed from API (if this repeats until timeout, check agent logs / finalizers)", lvgName)
				}, 10*time.Minute, 8*time.Second).Should(Succeed())

				By("Waiting for VG to disappear from node (vgremove)")
				Eventually(func(g Gomega) {
					out, err := e2eExecOnTestClusterNodeSSH(e2eCtx, testClusterResources.Kubeconfig, nodeName, vmSSH, vgsCmd)
					g.Expect(err).NotTo(HaveOccurred())
					stillThere := e2eVgNameListedInVgsOutput(out, vgName)
					if stillThere {
						GinkgoWriter.Printf("    … vgs still lists %q; output:\n%s\n", vgName, out)
					} else {
						GinkgoWriter.Printf("    vgs no longer lists %q; output:\n%s\n", vgName, out)
					}
					g.Expect(stillThere).To(BeFalse(),
						"VG %q should be removed from node", vgName)
				}, 5*time.Minute, 10*time.Second).Should(Succeed())

				By("Verifying BlockDevice object still exists (disk not removed)")
				var bdAfter v1alpha1.BlockDevice
				Expect(k8sClient.Get(e2eCtx, client.ObjectKey{Name: bdName}, &bdAfter)).To(Succeed())
				Expect(bdAfter.Status.Path).NotTo(BeEmpty(), "BlockDevice should still report device path")
				GinkgoWriter.Printf("    BlockDevice %s still present: path=%s size=%s\n", bdName, bdAfter.Status.Path, bdAfter.Status.Size.String())

				e2eSavedLVGForVGRemoveTest = nil
				By("✓ LVMVolumeGroup CR deleted; VG removed on node; BlockDevice still in API")
			})
		})

		Context("LVMVolumeGroup extend with second disk", func() {
			const (
				e2eLVGExtendDisk1Name = "e2e-lvg-extend-disk1"
				e2eLVGExtendDisk2Name = "e2e-lvg-extend-disk2"
				e2eLVGExtendDisk1Size = "2Gi"
				e2eLVGExtendDisk2Size = "3Gi"
			)

			var (
				lvgExtendSuiteOnce sync.Once
				lvgExtendRunID     string
				lvgExtendAttaches  []*kubernetes.VirtualDiskAttachmentResult
			)

			BeforeEach(func() {
				lvgExtendSuiteOnce.Do(func() {
					ensureE2EK8sClient(testClusterResources, &k8sClient, e2eCtx)
					lvgExtendRunID = fmt.Sprintf("%d", time.Now().Unix())
					prepCtx, prepCancel := context.WithTimeout(context.Background(), e2eClusterCleanupTimeout)
					defer prepCancel()
					By("LVMVolumeGroup extend suite: cleanup before test")
					cleanupE2EPodsAndPVCsWithWait(prepCtx, k8sClient, e2eSuitePodPVCleanupPodTimeout, e2eSuitePodPVCleanupPVTimeout)
					cleanupE2ELVMLogicalVolumes(prepCtx, k8sClient)
					cleanupE2ELVMVolumeGroups(prepCtx, k8sClient)
					cleanupE2ELocalStorageClasses(prepCtx, testClusterResources.Kubeconfig)
					if testClusterResources.BaseKubeconfig != nil {
						cleanupE2EVirtualDisks(prepCtx, testClusterResources.BaseKubeconfig, e2eConfigNamespace(), e2eSuiteVirtualDiskPrefix)
					}
					forceDeleteAllNonConsumableBlockDevices(prepCtx, k8sClient, 2*time.Minute)
					forceDeleteAllBlockDevices(prepCtx, k8sClient, 3*time.Minute)
				})
			})

			AfterEach(func() {
				if testClusterResources == nil || testClusterResources.BaseKubeconfig == nil {
					return
				}
				ns := e2eConfigNamespace()
				for _, att := range lvgExtendAttaches {
					if att == nil {
						continue
					}
					By("Cleaning up LVMVolumeGroup extend VirtualDisk " + att.DiskName)
					_ = kubernetes.DetachAndDeleteVirtualDisk(e2eCtx, testClusterResources.BaseKubeconfig, ns, att.AttachmentName, att.DiskName)
				}
				lvgExtendAttaches = nil
			})

			It("Should vgextend LVMVolumeGroup when a second disk is added to BlockDeviceSelector", func() {
				ensureE2EK8sClient(testClusterResources, &k8sClient, e2eCtx)
				Expect(testClusterResources.BaseKubeconfig).NotTo(BeNil(), "requires nested virtualization / base cluster")
				Expect(lvgExtendRunID).NotTo(BeEmpty())

				ns := e2eConfigNamespace()
				storageClass := e2eConfigStorageClass()
				Expect(storageClass).NotTo(BeEmpty())
				clusterVMs := e2eListClusterVMNames(e2eCtx, testClusterResources, ns)
				targetVM := clusterVMs[rand.Intn(len(clusterVMs))]

				vgName := fmt.Sprintf("e2e-vg-extend-%s", lvgExtendRunID)
				nodeSafe := func(n string) string {
					return strings.ReplaceAll(strings.ReplaceAll(n, ".", "-"), "_", "-")
				}

				By("Step 1: attach first VirtualDisk and create LVMVolumeGroup on a single BlockDevice")
				att1, err := attachVirtualDiskWithRetry(e2eCtx, testClusterResources.BaseKubeconfig, kubernetes.VirtualDiskAttachmentConfig{
					VMName: targetVM, Namespace: ns, DiskName: e2eLVGExtendDisk1Name,
					DiskSize: e2eLVGExtendDisk1Size, StorageClassName: storageClass,
				}, e2eVirtualDiskAttachMaxRetries, e2eVirtualDiskAttachRetryInterval)
				Expect(err).NotTo(HaveOccurred())
				lvgExtendAttaches = append(lvgExtendAttaches, att1)

				attachCtx1, cancel1 := context.WithTimeout(e2eCtx, e2eVirtualDiskAttachWaitTimeout)
				defer cancel1()
				Expect(kubernetes.WaitForVirtualDiskAttached(attachCtx1, testClusterResources.BaseKubeconfig, ns, att1.AttachmentName, 10*time.Second)).To(Succeed())

				bd1 := e2eWaitConsumableBlockDeviceForVirtualDisk(e2eCtx, testClusterResources.BaseKubeconfig, k8sClient, ns,
					att1.DiskName, att1.AttachmentName, targetVM)
				nodeName := bd1.Status.NodeName
				bd1Meta := bd1.Labels["kubernetes.io/metadata.name"]
				if bd1Meta == "" {
					bd1Meta = bd1.Name
				}

				lvgName := fmt.Sprintf("e2e-lvg-extend-%s-%s", lvgExtendRunID, nodeSafe(nodeName))
				lvg := &v1alpha1.LVMVolumeGroup{
					ObjectMeta: metav1.ObjectMeta{Name: lvgName},
					Spec: v1alpha1.LVMVolumeGroupSpec{
						ActualVGNameOnTheNode: vgName,
						BlockDeviceSelector: &metav1.LabelSelector{
							MatchLabels: map[string]string{
								"kubernetes.io/hostname":      nodeName,
								"kubernetes.io/metadata.name": bd1Meta,
							},
						},
						Type:  "Local",
						Local: v1alpha1.LVMVolumeGroupLocalSpec{NodeName: nodeName},
					},
				}
				Expect(k8sClient.Create(e2eCtx, lvg)).To(Succeed())
				defer func() { _ = k8sClient.Delete(e2eCtx, lvg) }()

				var readyOneDisk v1alpha1.LVMVolumeGroup
				Eventually(func(g Gomega) {
					g.Expect(k8sClient.Get(e2eCtx, client.ObjectKeyFromObject(lvg), &readyOneDisk)).To(Succeed())
					g.Expect(readyOneDisk.Status.Phase).To(Equal(v1alpha1.PhaseReady))
				}, e2eLVMVolumeGroupReadyTimeout, 10*time.Second).Should(Succeed())

				baselineVGFree := readyOneDisk.Status.VGFree.Value()
				baselineVGSize := readyOneDisk.Status.VGSize.Value()
				Expect(baselineVGFree).To(BeNumerically(">", 0))
				Expect(baselineVGSize).To(BeNumerically(">", 0))
				Expect(e2eCountDevicesOnLVGNode(&readyOneDisk, nodeName)).To(Equal(1),
					"status should list one device before extend")
				GinkgoWriter.Printf("    LVMVolumeGroup with one PV: VGSize=%s VGFree=%s\n",
					readyOneDisk.Status.VGSize.String(), readyOneDisk.Status.VGFree.String())

				By("Step 2: attach second VirtualDisk and wait for a new consumable BlockDevice on the same node")
				att2, err := attachVirtualDiskWithRetry(e2eCtx, testClusterResources.BaseKubeconfig, kubernetes.VirtualDiskAttachmentConfig{
					VMName: targetVM, Namespace: ns, DiskName: e2eLVGExtendDisk2Name,
					DiskSize: e2eLVGExtendDisk2Size, StorageClassName: storageClass,
				}, e2eVirtualDiskAttachMaxRetries, e2eVirtualDiskAttachRetryInterval)
				Expect(err).NotTo(HaveOccurred())
				lvgExtendAttaches = append(lvgExtendAttaches, att2)

				attachCtx2, cancel2 := context.WithTimeout(e2eCtx, e2eVirtualDiskAttachWaitTimeout)
				defer cancel2()
				Expect(kubernetes.WaitForVirtualDiskAttached(attachCtx2, testClusterResources.BaseKubeconfig, ns, att2.AttachmentName, 10*time.Second)).To(Succeed())

				bd2 := e2eWaitConsumableBlockDeviceForVirtualDisk(e2eCtx, testClusterResources.BaseKubeconfig, k8sClient, ns,
					att2.DiskName, att2.AttachmentName, targetVM)
				Expect(bd2.Status.NodeName).To(Equal(nodeName), "second disk must land on the same node as the LVMVolumeGroup")
				bd2Meta := bd2.Labels["kubernetes.io/metadata.name"]
				if bd2Meta == "" {
					bd2Meta = bd2.Name
				}
				Expect(bd2Meta).NotTo(Equal(bd1Meta), "BlockDevice selectors must be distinct")

				By("Step 3: patch LVMVolumeGroup BlockDeviceSelector to include both BlockDevices")
				var cur v1alpha1.LVMVolumeGroup
				Expect(k8sClient.Get(e2eCtx, client.ObjectKeyFromObject(lvg), &cur)).To(Succeed())
				cur.Spec.BlockDeviceSelector = &metav1.LabelSelector{
					MatchLabels: map[string]string{
						"kubernetes.io/hostname": nodeName,
					},
					MatchExpressions: []metav1.LabelSelectorRequirement{
						{
							Key:      "kubernetes.io/metadata.name",
							Operator: metav1.LabelSelectorOpIn,
							Values:   []string{bd1Meta, bd2Meta},
						},
					},
				}
				Expect(k8sClient.Update(e2eCtx, &cur)).To(Succeed())

				vmSSH := e2eConfigVMSSHUser()
				By("Step 4a: nudge agent/LVM scan after selector patch (vgextend + BD discoverer race on CI)")
				e2eTriggerLVMDiscoveryOnNode(e2eCtx, testClusterResources.Kubeconfig, nodeName, vmSSH)
				restartSDSNodeConfiguratorAgentOnNode(e2eCtx, k8sClient, nodeName)

				By("Step 4b: wait for two PVs in VG on the node (reconciler vgextend)")
				Eventually(func(g Gomega) {
					pvCount, out, errSSH := e2eCountPVsInVGOnNode(e2eCtx, testClusterResources.Kubeconfig, nodeName, vmSSH, vgName)
					if errSSH != nil {
						GinkgoWriter.Printf("    vgextend pvs: err=%v out=%q\n", errSSH, out)
					}
					g.Expect(errSSH).NotTo(HaveOccurred())
					g.Expect(pvCount).To(Equal(2), "VG %q should have 2 PVs; pvs: %q", vgName, strings.TrimSpace(out))
				}, 10*time.Minute, 10*time.Second).Should(Succeed())

				By(fmt.Sprintf("Step 4c: wait for BlockDevice %s linked to VG %s (BD discoverer after vgextend)", bd2.Name, vgName))
				e2eWaitBlockDeviceLinkedToVG(e2eCtx, k8sClient, bd2.Name, vgName, e2eBlockDeviceVGLinkageTimeout)

				By("Step 4d: wait for LVMVolumeGroup status — Ready, two devices, larger VG size/free")
				Eventually(func(g Gomega) {
					var extended v1alpha1.LVMVolumeGroup
					g.Expect(k8sClient.Get(e2eCtx, client.ObjectKeyFromObject(lvg), &extended)).To(Succeed())
					nDev := e2eCountDevicesOnLVGNode(&extended, nodeName)
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
					devices := e2eDevicesOnLVGNode(&extended, nodeName)
					g.Expect(devices).To(HaveKey(bd1.Name), "status should include first BlockDevice %s", bd1.Name)
					g.Expect(devices).To(HaveKey(bd2.Name), "status should include second BlockDevice %s", bd2.Name)
				}, e2eLVMVolumeGroupReadyTimeout, 10*time.Second).Should(Succeed())

				var final v1alpha1.LVMVolumeGroup
				Expect(k8sClient.Get(e2eCtx, client.ObjectKeyFromObject(lvg), &final)).To(Succeed())
				printLVMVolumeGroupInfo(&final)
				By("✓ LVMVolumeGroup extended: two PVs in VG, VGFree/VGSize grew, Phase Ready")
			})
		})

		// Agent reconcileThinPoolsIfNeeded recreates spec thin-pools missing on the node (lv_attr "t").
		// Manual removal must drop thin volumes, pool data LV, and leftover segments (_tmeta/_tdata) — see e2eShellRemoveThinPoolStackForVG.
		Context("thin-pool removed manually", func() {
			const (
				e2eThinPoolRestoreDiskName = "e2e-lvg-tp-restore-disk"
				e2eThinPoolRestoreDiskSize = "2Gi"
				e2eThinPoolRestorePoolName = "e2e-thin-pool-restore"
				e2eThinPoolRestorePoolSize = "60%"
			)

			var (
				thinPoolRestoreAttach     *kubernetes.VirtualDiskAttachmentResult
				thinPoolRestoreAllocLimit = "100%"
			)

			AfterEach(func() {
				if thinPoolRestoreAttach == nil || testClusterResources == nil || testClusterResources.BaseKubeconfig == nil {
					return
				}
				ns := e2eConfigNamespace()
				By("Cleaning up thin-pool restore VirtualDisk " + thinPoolRestoreAttach.DiskName)
				_ = kubernetes.DetachAndDeleteVirtualDisk(e2eCtx, testClusterResources.BaseKubeconfig, ns,
					thinPoolRestoreAttach.AttachmentName, thinPoolRestoreAttach.DiskName)
				thinPoolRestoreAttach = nil
			})

			It("Should recreate thin-pool when the pool LV was removed manually on the node", func() {
				ensureE2EK8sClient(testClusterResources, &k8sClient, e2eCtx)
				Expect(testClusterResources.BaseKubeconfig).NotTo(BeNil(), "requires nested virtualization")
				thinPoolRestoreRunID := fmt.Sprintf("%d", time.Now().Unix())
				diskName := fmt.Sprintf("%s-%s", e2eThinPoolRestoreDiskName, thinPoolRestoreRunID)

				ns := e2eConfigNamespace()
				storageClass := e2eConfigStorageClass()
				Expect(storageClass).NotTo(BeEmpty())
				if testClusterResources.BaseKubeconfig != nil {
					_ = kubernetes.DetachAndDeleteVirtualDisk(e2eCtx, testClusterResources.BaseKubeconfig, ns,
						diskName+"-attachment", diskName)
					_ = kubernetes.DetachAndDeleteVirtualDisk(e2eCtx, testClusterResources.BaseKubeconfig, ns,
						e2eThinPoolRestoreDiskName+"-attachment", e2eThinPoolRestoreDiskName)
				}
				clusterVMs := e2eListClusterVMNames(e2eCtx, testClusterResources, ns)
				targetVM := clusterVMs[rand.Intn(len(clusterVMs))]
				vmSSH := e2eConfigVMSSHUser()

				vgName := "e2e-vg-tp-restore-" + thinPoolRestoreRunID
				thinPoolName := e2eThinPoolRestorePoolName
				nodeSafe := func(n string) string {
					return strings.ReplaceAll(strings.ReplaceAll(n, ".", "-"), "_", "-")
				}

				By("Step 1: attach VirtualDisk and create LVMVolumeGroup with thin-pool")
				att, err := attachVirtualDiskWithRetry(e2eCtx, testClusterResources.BaseKubeconfig, kubernetes.VirtualDiskAttachmentConfig{
					VMName: targetVM, Namespace: ns, DiskName: diskName,
					DiskSize: e2eThinPoolRestoreDiskSize, StorageClassName: storageClass,
				}, e2eVirtualDiskAttachMaxRetries, e2eVirtualDiskAttachRetryInterval)
				Expect(err).NotTo(HaveOccurred())
				thinPoolRestoreAttach = att

				attachCtx, cancel := context.WithTimeout(e2eCtx, e2eVirtualDiskAttachWaitTimeout)
				defer cancel()
				Expect(kubernetes.WaitForVirtualDiskAttached(attachCtx, testClusterResources.BaseKubeconfig, ns, att.AttachmentName, 10*time.Second)).To(Succeed())

				bd := e2eWaitConsumableBlockDeviceForVirtualDisk(e2eCtx, testClusterResources.BaseKubeconfig, k8sClient, ns,
					att.DiskName, att.AttachmentName, targetVM)
				nodeName := bd.Status.NodeName
				bdMeta := bd.Labels["kubernetes.io/metadata.name"]
				if bdMeta == "" {
					bdMeta = bd.Name
				}

				lvgName := fmt.Sprintf("e2e-lvg-tp-restore-%s-%s", thinPoolRestoreRunID, nodeSafe(nodeName))
				lvg := &v1alpha1.LVMVolumeGroup{
					ObjectMeta: metav1.ObjectMeta{Name: lvgName},
					Spec: v1alpha1.LVMVolumeGroupSpec{
						ActualVGNameOnTheNode: vgName,
						BlockDeviceSelector: &metav1.LabelSelector{
							MatchLabels: map[string]string{
								"kubernetes.io/hostname":      nodeName,
								"kubernetes.io/metadata.name": bdMeta,
							},
						},
						ThinPools: []v1alpha1.LVMVolumeGroupThinPoolSpec{
							{Name: thinPoolName, Size: e2eThinPoolRestorePoolSize, AllocationLimit: thinPoolRestoreAllocLimit},
						},
						Type:  "Local",
						Local: v1alpha1.LVMVolumeGroupLocalSpec{NodeName: nodeName},
					},
				}
				Expect(k8sClient.Create(e2eCtx, lvg)).To(Succeed())
				defer func() { _ = k8sClient.Delete(e2eCtx, lvg) }()

				var ready v1alpha1.LVMVolumeGroup
				Eventually(func(g Gomega) {
					g.Expect(k8sClient.Get(e2eCtx, client.ObjectKeyFromObject(lvg), &ready)).To(Succeed())
					g.Expect(ready.Status.Phase).To(Equal(v1alpha1.PhaseReady))
					var tp *v1alpha1.LVMVolumeGroupThinPoolStatus
					for i := range ready.Status.ThinPools {
						if ready.Status.ThinPools[i].Name == thinPoolName {
							tp = &ready.Status.ThinPools[i]
							break
						}
					}
					g.Expect(tp).NotTo(BeNil())
					g.Expect(tp.Ready).To(BeTrue())
				}, e2eLVMVolumeGroupReadyTimeout, 10*time.Second).Should(Succeed())

				present, lvsOut, errLvs := e2eThinPoolDataLVPresentOnNode(e2eCtx, testClusterResources.Kubeconfig, nodeName, vmSSH, vgName, thinPoolName)
				Expect(errLvs).NotTo(HaveOccurred())
				Expect(present).To(BeTrue(), "thin-pool data LV should exist before manual removal (status already Ready); lvs: %q", strings.TrimSpace(lvsOut))
				printLVMVolumeGroupInfo(&ready)

				By("Step 2: manually remove thin-pool stack on the node (pool + metadata segments; VG and PV stay)")
				removeScript := e2eShellRemoveThinPoolStackForVG(vgName, thinPoolName)
				outRemove, errRemove := e2eExecOnTestClusterNodeSSH(e2eCtx, testClusterResources.Kubeconfig, nodeName, vmSSH, removeScript)
				if outRemove != "" {
					GinkgoWriter.Printf("    thin-pool remove script output:\n%s\n", outRemove)
				}
				Expect(errRemove).NotTo(HaveOccurred(), "manual thin-pool removal on node %s", nodeName)

				Eventually(func(g Gomega) {
					present, out, errSSH := e2eThinPoolDataLVPresentOnNode(e2eCtx, testClusterResources.Kubeconfig, nodeName, vmSSH, vgName, thinPoolName)
					g.Expect(errSSH).NotTo(HaveOccurred())
					g.Expect(present).To(BeFalse(), "thin-pool data LV should be gone after manual removal; lvs: %q", strings.TrimSpace(out))
				}, 2*time.Minute, 5*time.Second).Should(Succeed())

				vgsOut, errVgs := e2eExecOnTestClusterNodeSSH(e2eCtx, testClusterResources.Kubeconfig, nodeName, vmSSH,
					fmt.Sprintf("sudo -n vgs --noheadings -o vg_name %s 2>/dev/null || true", strconv.Quote(vgName)))
				Expect(errVgs).NotTo(HaveOccurred())
				Expect(e2eVgNameListedInVgsOutput(vgsOut, vgName)).To(BeTrue(), "VG %q must remain after thin-pool removal; vgs: %q", vgName, strings.TrimSpace(vgsOut))

				By("Step 3: nudge agent and wait for controller to recreate thin-pool")
				e2eTriggerLVMDiscoveryOnNode(e2eCtx, testClusterResources.Kubeconfig, nodeName, vmSSH)
				restartSDSNodeConfiguratorAgentOnNode(e2eCtx, k8sClient, nodeName)

				Eventually(func(g Gomega) {
					var cur v1alpha1.LVMVolumeGroup
					g.Expect(k8sClient.Get(e2eCtx, client.ObjectKeyFromObject(lvg), &cur)).To(Succeed())
					g.Expect(cur.Status.Phase).To(Equal(v1alpha1.PhaseReady))
					var tp *v1alpha1.LVMVolumeGroupThinPoolStatus
					for i := range cur.Status.ThinPools {
						if cur.Status.ThinPools[i].Name == thinPoolName {
							tp = &cur.Status.ThinPools[i]
							break
						}
					}
					g.Expect(tp).NotTo(BeNil(), "status.thinPools should list %q", thinPoolName)
					g.Expect(tp.Ready).To(BeTrue(), "recreated thin-pool should be Ready; ThinPools=%+v", cur.Status.ThinPools)
					for _, c := range cur.Status.Conditions {
						if c.Type == "VGConfigurationApplied" {
							g.Expect(c.Status).To(Equal(metav1.ConditionTrue), "VGConfigurationApplied: %s %s", c.Reason, c.Message)
						}
					}
				}, e2eLVMVolumeGroupReadyTimeout, 10*time.Second).Should(Succeed())

				Eventually(func(g Gomega) {
					present, out, errSSH := e2eThinPoolDataLVPresentOnNode(e2eCtx, testClusterResources.Kubeconfig, nodeName, vmSSH, vgName, thinPoolName)
					g.Expect(errSSH).NotTo(HaveOccurred())
					g.Expect(present).To(BeTrue(), "thin-pool data LV should exist after agent reconcile; lvs: %q", strings.TrimSpace(out))
				}, e2eLVMVolumeGroupReadyTimeout, 10*time.Second).Should(Succeed())

				var final v1alpha1.LVMVolumeGroup
				Expect(k8sClient.Get(e2eCtx, client.ObjectKeyFromObject(lvg), &final)).To(Succeed())
				printLVMVolumeGroupInfo(&final)
				By("✓ Thin-pool removed manually; agent recreated pool; LVMVolumeGroup Ready")
			})
		})

		Context("Multiple LVMVolumeGroups on one node", func() {
			const (
				e2eMultiLvgMinDisks  = 2
				e2eMultiLvgMaxDisks  = 5
				e2eMultiLvgSizeMinGi = 2
				e2eMultiLvgSizeMaxGi = 10
			)

			var (
				multiLvgAttachments []*kubernetes.VirtualDiskAttachmentResult
				multiLvgRunID       string
				multiLvgSuiteOnce   sync.Once
			)

			BeforeEach(func() {
				multiLvgSuiteOnce.Do(func() {
					ensureE2EK8sClient(testClusterResources, &k8sClient, e2eCtx)
					multiLvgRunID = fmt.Sprintf("%d", time.Now().Unix())
					prepCtx, prepCancel := context.WithTimeout(context.Background(), e2eClusterCleanupTimeout)
					defer prepCancel()
					By("Multiple LVMVolumeGroups on one node: cleanup before test")
					cleanupE2EPodsAndPVCsWithWait(prepCtx, k8sClient, e2eSuitePodPVCleanupPodTimeout, e2eSuitePodPVCleanupPVTimeout)
					cleanupE2ELVMLogicalVolumes(prepCtx, k8sClient)
					cleanupE2ELVMVolumeGroups(prepCtx, k8sClient)
					cleanupE2ELocalStorageClasses(prepCtx, testClusterResources.Kubeconfig)
					if testClusterResources.BaseKubeconfig != nil {
						cleanupE2EVirtualDisks(prepCtx, testClusterResources.BaseKubeconfig, e2eConfigNamespace(), e2eSuiteVirtualDiskPrefix)
					}
					forceDeleteAllNonConsumableBlockDevices(prepCtx, k8sClient, 2*time.Minute)
					forceDeleteAllBlockDevices(prepCtx, k8sClient, 3*time.Minute)
				})
			})

			AfterEach(func() {
				if testClusterResources == nil || testClusterResources.BaseKubeconfig == nil {
					return
				}
				ns := e2eConfigNamespace()
				for _, att := range multiLvgAttachments {
					if att == nil {
						continue
					}
					By("Cleaning up multi-LVG test VirtualDisk " + att.DiskName)
					_ = kubernetes.DetachAndDeleteVirtualDisk(e2eCtx, testClusterResources.BaseKubeconfig, ns, att.AttachmentName, att.DiskName)
				}
				multiLvgAttachments = nil
			})

			It("Should create independent LVMVolumeGroups on the same node (random 2–5 disks, 2–10 Gi each)", func() {
				ensureE2EK8sClient(testClusterResources, &k8sClient, e2eCtx)
				Expect(testClusterResources.BaseKubeconfig).NotTo(BeNil(), "requires nested virtualization")
				Expect(multiLvgRunID).NotTo(BeEmpty(), "BeforeEach must set multiLvgRunID")

				nDisks := e2eMultiLvgMinDisks + rand.Intn(e2eMultiLvgMaxDisks-e2eMultiLvgMinDisks+1) // 2..5 inclusive

				ns := e2eConfigNamespace()
				storageClass := e2eConfigStorageClass()
				Expect(storageClass).NotTo(BeEmpty())
				By("Guest VM for attach: phase Running only")
				clusterVMs := e2eListClusterVMNames(e2eCtx, testClusterResources, ns)
				targetVM := clusterVMs[rand.Intn(len(clusterVMs))]

				type multiDisk struct {
					diskName string
					diskSize string
					att      *kubernetes.VirtualDiskAttachmentResult
					bd       *v1alpha1.BlockDevice
					meta     string
					lvgName  string
					vgName   string
				}
				disks := make([]multiDisk, 0, nDisks)
				for i := 0; i < nDisks; i++ {
					szGi := e2eMultiLvgSizeMinGi + rand.Intn(e2eMultiLvgSizeMaxGi-e2eMultiLvgSizeMinGi+1) // 2..10
					disks = append(disks, multiDisk{
						diskName: fmt.Sprintf("e2e-multi-lvg-d%d-%s-%d", i+1, multiLvgRunID, rand.Intn(100000)),
						diskSize: fmt.Sprintf("%dGi", szGi),
					})
				}
				By(fmt.Sprintf("Attaching %d VirtualDisks to the same VM %q (per-disk size 2..10 Gi random)", nDisks, targetVM))
				for i := range disks {
					att, err := attachVirtualDiskWithRetry(e2eCtx, testClusterResources.BaseKubeconfig, kubernetes.VirtualDiskAttachmentConfig{
						VMName: targetVM, Namespace: ns, DiskName: disks[i].diskName,
						DiskSize: disks[i].diskSize, StorageClassName: storageClass,
					}, e2eVirtualDiskAttachMaxRetries, e2eVirtualDiskAttachRetryInterval)
					Expect(err).NotTo(HaveOccurred(), "attach disk %s", disks[i].diskName)
					GinkgoWriter.Printf("    disk %d/%d: name=%s size=%s\n", i+1, nDisks, disks[i].diskName, disks[i].diskSize)
					disks[i].att = att
					multiLvgAttachments = append(multiLvgAttachments, att)
				}
				for i := range disks {
					attachCtx, cancel := context.WithTimeout(e2eCtx, e2eVirtualDiskAttachWaitTimeout)
					Expect(kubernetes.WaitForVirtualDiskAttached(attachCtx, testClusterResources.BaseKubeconfig, ns, disks[i].att.AttachmentName, 10*time.Second)).To(Succeed())
					cancel()
				}
				nodeName := ""
				metaSeen := make(map[string]struct{})
				for i := range disks {
					bd := e2eWaitConsumableBlockDeviceForVirtualDisk(e2eCtx, testClusterResources.BaseKubeconfig, k8sClient, ns,
						disks[i].att.DiskName, disks[i].att.AttachmentName, targetVM)
					disks[i].bd = bd
					if nodeName == "" {
						nodeName = bd.Status.NodeName
					} else {
						Expect(bd.Status.NodeName).To(Equal(nodeName), "all BlockDevices must be on the same node")
					}
					meta := bd.Labels["kubernetes.io/metadata.name"]
					if meta == "" {
						meta = bd.Name
					}
					Expect(metaSeen).NotTo(HaveKey(meta), "expected distinct BlockDevice selectors, duplicate meta %q", meta)
					metaSeen[meta] = struct{}{}
					disks[i].meta = meta
				}

				nodeSafe := strings.ReplaceAll(strings.ReplaceAll(nodeName, ".", "-"), "_", "-")
				for i := range disks {
					idx := i + 1
					disks[i].lvgName = fmt.Sprintf("e2e-lvg-multi-%d-%s-%s", idx, multiLvgRunID, nodeSafe)
					disks[i].vgName = fmt.Sprintf("e2e-vg-multi-%d-%s", idx, multiLvgRunID)
					lvg := &v1alpha1.LVMVolumeGroup{
						ObjectMeta: metav1.ObjectMeta{Name: disks[i].lvgName},
						Spec: v1alpha1.LVMVolumeGroupSpec{
							ActualVGNameOnTheNode: disks[i].vgName,
							BlockDeviceSelector: &metav1.LabelSelector{
								MatchLabels: map[string]string{
									"kubernetes.io/hostname":      nodeName,
									"kubernetes.io/metadata.name": disks[i].meta,
								},
							},
							Type:  "Local",
							Local: v1alpha1.LVMVolumeGroupLocalSpec{NodeName: nodeName},
						},
					}
					By(fmt.Sprintf("Creating LVMVolumeGroup %s (VG %s) — BlockDevice %s", disks[i].lvgName, disks[i].vgName, disks[i].bd.Name))
					Expect(k8sClient.Create(e2eCtx, lvg)).To(Succeed())
				}
				defer func() {
					for i := len(disks) - 1; i >= 0; i-- {
						_ = client.IgnoreNotFound(k8sClient.Delete(e2eCtx, &v1alpha1.LVMVolumeGroup{ObjectMeta: metav1.ObjectMeta{Name: disks[i].lvgName}}))
					}
				}()

				By(fmt.Sprintf("Waiting for %d LVMVolumeGroup(s) to become Ready (independent VGs on one node)", nDisks))
				Eventually(func(g Gomega) {
					for i := range disks {
						var cur v1alpha1.LVMVolumeGroup
						g.Expect(k8sClient.Get(e2eCtx, client.ObjectKey{Name: disks[i].lvgName}, &cur)).To(Succeed())
						g.Expect(cur.Status.Phase).To(Equal(v1alpha1.PhaseReady), "LVMVolumeGroup %s phase", disks[i].lvgName)
					}
				}, e2eLVMVolumeGroupReadyTimeout, 10*time.Second).Should(Succeed())

				readies := make([]v1alpha1.LVMVolumeGroup, len(disks))
				for i := range disks {
					Expect(k8sClient.Get(e2eCtx, client.ObjectKey{Name: disks[i].lvgName}, &readies[i])).To(Succeed())
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
					printLVMVolumeGroupInfo(&readies[i])
				}

				vmSSH := e2eConfigVMSSHUser()
				vgsCmd := "vgs -o vg_name --noheadings 2>/dev/null || sudo -n vgs -o vg_name --noheadings 2>/dev/null"
				outVgs, errVgs := e2eExecOnTestClusterNodeSSH(e2eCtx, testClusterResources.Kubeconfig, nodeName, vmSSH, vgsCmd)
				Expect(errVgs).NotTo(HaveOccurred())
				for i := range disks {
					Expect(e2eVgNameListedInVgsOutput(outVgs, disks[i].vgName)).To(BeTrue(), "vgs should list %q; output:\n%s", disks[i].vgName, outVgs)
				}
				By("✓ Ready LVMVolumeGroups on one node; all distinct VGs in vgs; no False conditions")
			})
		})

		// Discovery of a pre-existing tagged VG (auto-import): on-node LVM + tag → LVMVolumeGroup CR + thin-pool in status.
		Context("LVMVolumeGroup auto-import (pre-existing VG with tag)", func() {
			var (
				e2eVgImportSuiteOnce     sync.Once
				e2eVgImportRunID         string
				e2eVgImportAttachment    *kubernetes.VirtualDiskAttachmentResult
				e2eVgImportNodeName      string
				e2eVgImportDevicePath    string
				e2eVgImportActualVGName  string
				e2eVgImportThinPoolName  string
				e2eVgImportLVMVolumeName string
			)

			BeforeEach(func() {
				e2eVgImportSuiteOnce.Do(func() {
					ensureE2EK8sClient(testClusterResources, &k8sClient, e2eCtx)
					e2eVgImportRunID = fmt.Sprintf("%d", time.Now().Unix())
					prepCtx, prepCancel := context.WithTimeout(context.Background(), e2eClusterCleanupTimeout)
					defer prepCancel()
					By("LVM auto-import suite: cleanup before test")
					cleanupE2EPodsAndPVCsWithWait(prepCtx, k8sClient, e2eSuitePodPVCleanupPodTimeout, e2eSuitePodPVCleanupPVTimeout)
					cleanupE2ELVMLogicalVolumes(prepCtx, k8sClient)
					cleanupE2ELVMVolumeGroups(prepCtx, k8sClient)
					cleanupE2ELocalStorageClasses(prepCtx, testClusterResources.Kubeconfig)
					if testClusterResources.BaseKubeconfig != nil {
						cleanupE2EVirtualDisks(prepCtx, testClusterResources.BaseKubeconfig, e2eConfigNamespace(), e2eSuiteVirtualDiskPrefix)
					}
					forceDeleteAllNonConsumableBlockDevices(prepCtx, k8sClient, 2*time.Minute)
					forceDeleteAllBlockDevices(prepCtx, k8sClient, 3*time.Minute)
				})
			})

			AfterEach(func() {
				if testClusterResources == nil || testClusterResources.Kubeconfig == nil {
					return
				}
				vmSSH := e2eConfigVMSSHUser()
				if e2eVgImportNodeName != "" && e2eVgImportActualVGName != "" {
					By("auto-import cleanup: vgchange --deltag (stop controller tracking)")
					_, _ = e2eExecOnTestClusterNodeSSH(e2eCtx, testClusterResources.Kubeconfig, e2eVgImportNodeName, vmSSH,
						fmt.Sprintf(`sudo -n vgchange %q --deltag storage.deckhouse.io/enabled=true 2>&1 || true`, e2eVgImportActualVGName))
					deadline := time.Now().Add(5 * time.Minute)
					for time.Now().Before(deadline) {
						var list v1alpha1.LVMVolumeGroupList
						_ = k8sClient.List(e2eCtx, &list, &client.ListOptions{})
						gone := true
						for i := range list.Items {
							if list.Items[i].Spec.ActualVGNameOnTheNode == e2eVgImportActualVGName {
								gone = false
								break
							}
						}
						if gone {
							break
						}
						time.Sleep(3 * time.Second)
					}
					if e2eVgImportLVMVolumeName != "" {
						By("auto-import cleanup: remove thin LV stack then VG (best effort)")
						prune := e2eShellRemoveThinPoolStackForVG(e2eVgImportActualVGName, e2eVgImportThinPoolName)
						_, _ = e2eExecOnTestClusterNodeSSH(e2eCtx, testClusterResources.Kubeconfig, e2eVgImportNodeName, vmSSH, prune)
					}
					_, _ = e2eExecOnTestClusterNodeSSH(e2eCtx, testClusterResources.Kubeconfig, e2eVgImportNodeName, vmSSH,
						fmt.Sprintf(`sudo -n vgremove -ff %q 2>&1 || true`, e2eVgImportActualVGName))
					if e2eVgImportDevicePath != "" {
						_, _ = e2eExecOnTestClusterNodeSSH(e2eCtx, testClusterResources.Kubeconfig, e2eVgImportNodeName, vmSSH,
							fmt.Sprintf(`sudo -n pvremove -ff %q 2>&1 || true`, e2eVgImportDevicePath))
					}
				}
				if testClusterResources.BaseKubeconfig != nil && e2eVgImportAttachment != nil {
					ns := e2eConfigNamespace()
					_ = kubernetes.DetachAndDeleteVirtualDisk(e2eCtx, testClusterResources.BaseKubeconfig, ns,
						e2eVgImportAttachment.AttachmentName, e2eVgImportAttachment.DiskName)
				}
				e2eVgImportAttachment = nil
				e2eVgImportNodeName, e2eVgImportDevicePath, e2eVgImportActualVGName = "", "", ""
				e2eVgImportThinPoolName, e2eVgImportLVMVolumeName = "", ""
			})

			It("Should discover LVMVolumeGroup for manually created VG with tag; thin-pool in status; controller management", func() {
				ensureE2EK8sClient(testClusterResources, &k8sClient, e2eCtx)
				Expect(testClusterResources.BaseKubeconfig).NotTo(BeNil(), "requires nested virtualization / base cluster")
				Expect(e2eVgImportRunID).NotTo(BeEmpty())

				ns := e2eConfigNamespace()
				storageClass := e2eConfigStorageClass()
				Expect(storageClass).NotTo(BeEmpty())
				clusterVMs := e2eListClusterVMNames(e2eCtx, testClusterResources, ns)
				targetVM := clusterVMs[rand.Intn(len(clusterVMs))]

				manualVG := fmt.Sprintf("e2e-vgimport-%s", e2eVgImportRunID)
				thinPoolLV := fmt.Sprintf("e2e-tp-import-%s", e2eVgImportRunID)
				diskName := fmt.Sprintf("e2e-vgimport-disk-%s", e2eVgImportRunID)
				diskSize := "3Gi"

				By("Attaching a dedicated VirtualDisk for on-node manual VG + thin-pool")
				att, err := attachVirtualDiskWithRetry(e2eCtx, testClusterResources.BaseKubeconfig, kubernetes.VirtualDiskAttachmentConfig{
					VMName: targetVM, Namespace: ns, DiskName: diskName,
					DiskSize: diskSize, StorageClassName: storageClass,
				}, e2eVirtualDiskAttachMaxRetries, e2eVirtualDiskAttachRetryInterval)
				Expect(err).NotTo(HaveOccurred())
				e2eVgImportAttachment = att

				attachCtx, cancel := context.WithTimeout(e2eCtx, e2eVirtualDiskAttachWaitTimeout)
				defer cancel()
				Expect(kubernetes.WaitForVirtualDiskAttached(attachCtx, testClusterResources.BaseKubeconfig, ns, att.AttachmentName, 10*time.Second)).To(Succeed())

				targetBD := e2eWaitConsumableBlockDeviceForVirtualDisk(e2eCtx, testClusterResources.BaseKubeconfig, k8sClient, ns,
					att.DiskName, att.AttachmentName, targetVM)
				devPath := strings.TrimSpace(targetBD.Status.Path)
				Expect(devPath).NotTo(BeEmpty(), "BlockDevice must report device path for LVM on node")
				nodeName := targetBD.Status.NodeName
				e2eVgImportNodeName = nodeName
				e2eVgImportDevicePath = devPath
				e2eVgImportActualVGName = manualVG
				e2eVgImportThinPoolName = thinPoolLV
				// lvs(8) / pool LV name: thin pool is usually the name after -T
				e2eVgImportLVMVolumeName = thinPoolLV

				vmSSH := e2eConfigVMSSHUser()
				By(fmt.Sprintf("On node %s: pvcreate → vgcreate %s → thin-pool %s → vgchange --addtag storage.deckhouse.io/enabled=true", nodeName, manualVG, thinPoolLV))
				lvmScript := fmt.Sprintf(`set -e
DEV=%q
VG=%q
TP=%q
sudo -n pvcreate -y "$DEV" 2>&1
sudo -n vgcreate "$VG" "$DEV" 2>&1
sudo -n lvcreate -L 700M -T "$VG/$TP" 2>&1
sudo -n vgchange "$VG" --addtag storage.deckhouse.io/enabled=true 2>&1
`, devPath, manualVG, thinPoolLV)
				out, errLvm := e2eExecOnTestClusterNodeSSH(e2eCtx, testClusterResources.Kubeconfig, nodeName, vmSSH, lvmScript)
				if out != "" {
					GinkgoWriter.Printf("    on-node LVM script output:\n%s\n", out)
				}
				Expect(errLvm).NotTo(HaveOccurred(), "create VG + thin-pool + tag on node %s", nodeName)

				By("Triggering LVM inventory rescan on node (pvscan + udev) so agent cache can see the new VG")
				e2eTriggerLVMDiscoveryOnNode(e2eCtx, testClusterResources.Kubeconfig, nodeName, vmSSH)

				By(fmt.Sprintf("Waiting for BlockDevice %s to be linked to VG %s (up to %s)", targetBD.Name, manualVG, e2eBlockDeviceVGLinkageTimeout))
				e2eWaitBlockDeviceLinkedToVG(e2eCtx, k8sClient, targetBD.Name, manualVG, e2eBlockDeviceVGLinkageTimeout)

				By("Restarting sds-node-configurator agent on the node (nested CI often misses udev; same pattern as BD rescan tests)")
				restartSDSNodeConfiguratorAgentOnNode(e2eCtx, k8sClient, nodeName)

				var lvgName string
				By(fmt.Sprintf("Waiting for agent to create LVMVolumeGroup CR (auto-import) for tagged VG on node (up to %s)", e2eLVMVolumeGroupAutoImportDiscoveryTimeout))
				Eventually(func(g Gomega) {
					var list v1alpha1.LVMVolumeGroupList
					g.Expect(k8sClient.List(e2eCtx, &list, &client.ListOptions{})).To(Succeed())
					found := false
					for i := range list.Items {
						if list.Items[i].Spec.ActualVGNameOnTheNode == manualVG {
							lvgName = list.Items[i].Name
							found = true
							break
						}
					}
					g.Expect(found).To(BeTrue(), "expected a LVMVolumeGroup whose spec.actualVGNameOnTheNode is %q (auto-discovered from tagged VG)", manualVG)
				}, e2eLVMVolumeGroupAutoImportDiscoveryTimeout, 5*time.Second).Should(Succeed())
				Expect(lvgName).NotTo(BeEmpty())

				By("Waiting for LVMVolumeGroup Ready; thin-pool in status; controller has applied configuration")
				Eventually(func(g Gomega) {
					var cur v1alpha1.LVMVolumeGroup
					g.Expect(k8sClient.Get(e2eCtx, client.ObjectKey{Name: lvgName}, &cur)).To(Succeed())
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
					var cfg *metav1.Condition
					for i := range cur.Status.Conditions {
						if cur.Status.Conditions[i].Type == "VGConfigurationApplied" {
							cfg = &cur.Status.Conditions[i]
							break
						}
					}
					g.Expect(cfg).NotTo(BeNil(), "expected VGConfigurationApplied condition")
					g.Expect(cfg.Status).To(Equal(metav1.ConditionTrue), "controller should have applied / reconciled config for imported VG: reason=%s msg=%s", cfg.Reason, cfg.Message)
				}, e2eLVMVolumeGroupReadyTimeout, 8*time.Second).Should(Succeed())

				var final v1alpha1.LVMVolumeGroup
				Expect(k8sClient.Get(e2eCtx, client.ObjectKey{Name: lvgName}, &final)).To(Succeed())
				printLVMVolumeGroupInfo(&final)
				By("✓ Auto-import: LVMVolumeGroup exists, Ready, thin-pool in status, VGConfigurationApplied True")
			})
		})

		Context("Controller pod restart preserves managed VG and BlockDevice state", func() {
			const (
				e2eRestartStateDiskName = "e2e-restart-state-disk"
				e2eRestartStateDiskSize = "2Gi"
			)

			var (
				restartStateAttachment *kubernetes.VirtualDiskAttachmentResult
				restartStateRunID      string
			)

			AfterEach(func() {
				if restartStateAttachment == nil || testClusterResources == nil || testClusterResources.BaseKubeconfig == nil {
					return
				}
				ns := e2eConfigNamespace()
				By("Cleaning up restart-state test VirtualDisk and attachment")
				_ = kubernetes.DetachAndDeleteVirtualDisk(e2eCtx, testClusterResources.BaseKubeconfig, ns,
					restartStateAttachment.AttachmentName, restartStateAttachment.DiskName)
				restartStateAttachment = nil
			})

			It("Should preserve managed VG and BlockDevice state after controller pod restart", func() {
				ensureE2EK8sClient(testClusterResources, &k8sClient, e2eCtx)
				By("Сценарий: управляемые VG и BlockDevice → перезапуск pod контроллера → без лишних create/delete, статусы сходятся")

				Expect(testClusterResources.BaseKubeconfig).NotTo(BeNil(), "restart-state test requires nested virtualization (base cluster)")
				ns := e2eConfigNamespace()
				storageClass := e2eConfigStorageClass()
				Expect(storageClass).NotTo(BeEmpty(), "TEST_CLUSTER_STORAGE_CLASS is required for VirtualDisk")

				restartStateRunID = fmt.Sprintf("%d", time.Now().Unix())
				clusterVMs := e2eListClusterVMNames(e2eCtx, testClusterResources, ns)
				targetVM := clusterVMs[rand.Intn(len(clusterVMs))]

				By("Step 1: attach disk and wait for consumable BlockDevice")
				var attachErr error
				restartStateAttachment, attachErr = attachVirtualDiskWithRetry(e2eCtx, testClusterResources.BaseKubeconfig, kubernetes.VirtualDiskAttachmentConfig{
					VMName:           targetVM,
					Namespace:        ns,
					DiskName:         e2eRestartStateDiskName,
					DiskSize:         e2eRestartStateDiskSize,
					StorageClassName: storageClass,
				}, e2eVirtualDiskAttachMaxRetries, e2eVirtualDiskAttachRetryInterval)
				Expect(attachErr).NotTo(HaveOccurred())

				attachCtx, cancel := context.WithTimeout(e2eCtx, e2eVirtualDiskAttachWaitTimeout)
				defer cancel()
				Expect(kubernetes.WaitForVirtualDiskAttached(attachCtx, testClusterResources.BaseKubeconfig, ns,
					restartStateAttachment.AttachmentName, 10*time.Second)).To(Succeed())

				targetBD := e2eWaitConsumableBlockDeviceForVirtualDisk(e2eCtx, testClusterResources.BaseKubeconfig, k8sClient, ns,
					restartStateAttachment.DiskName, restartStateAttachment.AttachmentName, targetVM)
				nodeName := targetBD.Status.NodeName
				bdMetaName := targetBD.Labels["kubernetes.io/metadata.name"]
				if bdMetaName == "" {
					bdMetaName = targetBD.Name
				}

				By("Step 1 (continued): create managed LVMVolumeGroup and wait for Ready")
				vgName := "e2e-vg-restart-" + restartStateRunID
				thinPoolName := "e2e-thin-pool-restart"
				lvgName := "e2e-lvg-restart-" + restartStateRunID + "-" + strings.ReplaceAll(strings.ReplaceAll(nodeName, ".", "-"), "_", "-")
				lvg := &v1alpha1.LVMVolumeGroup{
					ObjectMeta: metav1.ObjectMeta{Name: lvgName},
					Spec: v1alpha1.LVMVolumeGroupSpec{
						ActualVGNameOnTheNode: vgName,
						BlockDeviceSelector: &metav1.LabelSelector{
							MatchLabels: map[string]string{
								"kubernetes.io/hostname":      nodeName,
								"kubernetes.io/metadata.name": bdMetaName,
							},
						},
						ThinPools: []v1alpha1.LVMVolumeGroupThinPoolSpec{
							{Name: thinPoolName, Size: "60%", AllocationLimit: "100%"},
						},
						Type:  "Local",
						Local: v1alpha1.LVMVolumeGroupLocalSpec{NodeName: nodeName},
					},
				}
				Expect(k8sClient.Create(e2eCtx, lvg)).To(Succeed())
				defer func() { _ = k8sClient.Delete(e2eCtx, lvg) }()

				Eventually(func(g Gomega) {
					var cur v1alpha1.LVMVolumeGroup
					g.Expect(k8sClient.Get(e2eCtx, client.ObjectKey{Name: lvgName}, &cur)).To(Succeed())
					g.Expect(cur.Status.Phase).To(Equal(v1alpha1.PhaseReady), "managed LVMVolumeGroup must be Ready before restart; phase=%s", cur.Status.Phase)
				}, e2eLVMVolumeGroupReadyTimeout, 10*time.Second).Should(Succeed())

				By("Step 2: snapshot BlockDevice/LVMVolumeGroup identity and status on the node")
				snapshot, snapErr := e2eTakeManagedStorageSnapshot(e2eCtx, k8sClient, nodeName, targetBD.Name, lvgName)
				Expect(snapErr).NotTo(HaveOccurred())
				GinkgoWriter.Printf("    snapshot: %d BlockDevice(s) on node %s; BD=%s LVG=%s phase=%s\n",
					len(snapshot.BlockDeviceNamesOnNode), nodeName, snapshot.BlockDeviceName, snapshot.LVMVolumeGroupName, snapshot.LVMVolumeGroupStatus.Phase)

				By("Step 3: restart sds-node-configurator controller pod on the node")
				restartSDSNodeConfiguratorAgentOnNode(e2eCtx, k8sClient, nodeName)

				By("Step 4: verify no recreate/delete churn and statuses converge after controller is back")
				e2eExpectManagedStorageStableAfterRestart(e2eCtx, k8sClient, snapshot)
				By("✓ Managed VG and BlockDevice unchanged after controller pod restart")
			})
		})

		Context("LVMVolumeGroup validation (disk not usable)", func() {
			const (
				lvgConditionVGConfigurationApplied = "VGConfigurationApplied"
				reasonValidationFailed             = "ValidationFailed"
			)

			var validationAttaches []*kubernetes.VirtualDiskAttachmentResult

			AfterEach(func() {
				if testClusterResources == nil || testClusterResources.BaseKubeconfig == nil {
					return
				}
				ns := e2eConfigNamespace()
				for _, att := range validationAttaches {
					if att == nil {
						continue
					}
					By("Cleaning up LVMVolumeGroup validation VirtualDisk " + att.DiskName)
					_ = kubernetes.DetachAndDeleteVirtualDisk(e2eCtx, testClusterResources.BaseKubeconfig, ns, att.AttachmentName, att.DiskName)
				}
				validationAttaches = nil
			})

			// Order: (1) tiny disk — no BlockDevice CR; (2) large disk — intermediate LVG then delete + pvcreate so BD is not consumable;
			// (3) final LVMVolumeGroup selects only that BD (does not touch other BlockDevices on the node).
			It("Should fail LVMVolumeGroup when the only selected BlockDevice is not consumable", func() {
				ensureE2EK8sClient(testClusterResources, &k8sClient, e2eCtx)
				Expect(testClusterResources.BaseKubeconfig).NotTo(BeNil(), "needs nested virtualization")

				ns := e2eConfigNamespace()
				storageClass := e2eConfigStorageClass()
				Expect(storageClass).NotTo(BeEmpty())
				clusterVMs := e2eListClusterVMNames(e2eCtx, testClusterResources, ns)
				targetVM := clusterVMs[rand.Intn(len(clusterVMs))]

				runID := strconv.FormatInt(time.Now().UnixNano(), 10)
				smallDiskName := "e2e-lvg-val-s-" + runID
				largeDiskName := "e2e-lvg-val-l-" + runID
				smallSize := fmt.Sprintf("%dMi", 5+rand.Intn(995)) // 5..999 Mi — below agent minimum, expect no BD
				largeSize := fmt.Sprintf("%dGi", 5+rand.Intn(11))  // 5..15 Gi
				midLvgName := "e2e-lvg-val-mid-" + runID
				midVgName := "e2e-vg-val-mid-" + runID
				finalLvgName := "e2e-lvg-val-final-" + runID
				finalVgName := "e2e-vg-val-final-" + runID

				var beforeList v1alpha1.BlockDeviceList
				Expect(k8sClient.List(e2eCtx, &beforeList, &client.ListOptions{})).To(Succeed())
				beforeNames := make(map[string]struct{}, len(beforeList.Items))
				for i := range beforeList.Items {
					beforeNames[beforeList.Items[i].Name] = struct{}{}
				}

				By("Step 1: attach small empty disk (no BlockDevice CR expected below minimum size)")
				att1, err := attachVirtualDiskWithRetry(e2eCtx, testClusterResources.BaseKubeconfig, kubernetes.VirtualDiskAttachmentConfig{
					VMName: targetVM, Namespace: ns, DiskName: smallDiskName,
					DiskSize: smallSize, StorageClassName: storageClass,
				}, e2eVirtualDiskAttachMaxRetries, e2eVirtualDiskAttachRetryInterval)
				Expect(err).NotTo(HaveOccurred())
				validationAttaches = append(validationAttaches, att1)
				attachCtx1, cancel1 := context.WithTimeout(e2eCtx, e2eVirtualDiskAttachWaitTimeout)
				defer cancel1()
				Expect(kubernetes.WaitForVirtualDiskAttached(attachCtx1, testClusterResources.BaseKubeconfig, ns, att1.AttachmentName, 10*time.Second)).To(Succeed())

				Eventually(func(g Gomega) {
					var after v1alpha1.BlockDeviceList
					g.Expect(k8sClient.List(e2eCtx, &after, &client.ListOptions{})).To(Succeed())
					var newOnes []string
					for i := range after.Items {
						if _, ok := beforeNames[after.Items[i].Name]; !ok {
							newOnes = append(newOnes, after.Items[i].Name)
						}
					}
					g.Expect(newOnes).To(BeEmpty(),
						"disks below minimum size must not get BlockDevice CRs; new name(s): %v", newOnes)
				}, 4*time.Minute, 15*time.Second).Should(Succeed())

				By("Step 2: attach large disk; intermediate LVM, delete, pvcreate — BD must become not consumable")
				att2, err := attachVirtualDiskWithRetry(e2eCtx, testClusterResources.BaseKubeconfig, kubernetes.VirtualDiskAttachmentConfig{
					VMName: targetVM, Namespace: ns, DiskName: largeDiskName,
					DiskSize: largeSize, StorageClassName: storageClass,
				}, e2eVirtualDiskAttachMaxRetries, e2eVirtualDiskAttachRetryInterval)
				Expect(err).NotTo(HaveOccurred())
				validationAttaches = append(validationAttaches, att2)
				attachCtx2, cancel2 := context.WithTimeout(e2eCtx, e2eVirtualDiskAttachWaitTimeout)
				defer cancel2()
				Expect(kubernetes.WaitForVirtualDiskAttached(attachCtx2, testClusterResources.BaseKubeconfig, ns, att2.AttachmentName, 10*time.Second)).To(Succeed())

				largeBD := e2eWaitConsumableBlockDeviceForVirtualDisk(e2eCtx, testClusterResources.BaseKubeconfig, k8sClient, ns,
					att2.DiskName, att2.AttachmentName, targetVM)
				nodeName := largeBD.Status.NodeName
				largeBdMeta := largeBD.Labels["kubernetes.io/metadata.name"]
				if largeBdMeta == "" {
					largeBdMeta = largeBD.Name
				}

				midLvg := &v1alpha1.LVMVolumeGroup{
					ObjectMeta: metav1.ObjectMeta{Name: midLvgName},
					Spec: v1alpha1.LVMVolumeGroupSpec{
						ActualVGNameOnTheNode: midVgName,
						BlockDeviceSelector: &metav1.LabelSelector{
							MatchLabels: map[string]string{
								"kubernetes.io/hostname":      nodeName,
								"kubernetes.io/metadata.name": largeBdMeta,
							},
						},
						Type:  "Local",
						Local: v1alpha1.LVMVolumeGroupLocalSpec{NodeName: nodeName},
					},
				}
				Expect(k8sClient.Create(e2eCtx, midLvg)).To(Succeed())
				defer func() {
					_ = client.IgnoreNotFound(k8sClient.Delete(e2eCtx, &v1alpha1.LVMVolumeGroup{ObjectMeta: metav1.ObjectMeta{Name: midLvgName}}))
				}()

				Eventually(func(g Gomega) {
					var cur v1alpha1.LVMVolumeGroup
					g.Expect(k8sClient.Get(e2eCtx, client.ObjectKey{Name: midLvgName}, &cur)).To(Succeed())
					g.Expect(cur.Status.Phase).To(Equal(v1alpha1.PhaseReady))
				}, e2eLVMVolumeGroupReadyTimeout, 10*time.Second).Should(Succeed())
				var midReady v1alpha1.LVMVolumeGroup
				Expect(k8sClient.Get(e2eCtx, client.ObjectKey{Name: midLvgName}, &midReady)).To(Succeed())
				By("Intermediate LVMVolumeGroup Ready (before delete)")
				printLVMVolumeGroupInfo(&midReady)

				Expect(k8sClient.Delete(e2eCtx, midLvg)).To(Succeed())
				Eventually(func(g Gomega) {
					var cur v1alpha1.LVMVolumeGroup
					err := k8sClient.Get(e2eCtx, client.ObjectKey{Name: midLvgName}, &cur)
					g.Expect(apierrors.IsNotFound(err)).To(BeTrue(), "intermediate LVMVolumeGroup should be removed")
				}, 10*time.Minute, 8*time.Second).Should(Succeed())

				vmSSH := e2eConfigVMSSHUser()
				Expect(k8sClient.Get(e2eCtx, client.ObjectKey{Name: largeBD.Name}, largeBD)).To(Succeed())
				largePath := largeBD.Status.Path
				Expect(largePath).NotTo(BeEmpty())

				By("pvcreate after agent pvremoved PV on LVG delete (orphan PV → not consumable)")
				_, errPV := e2eExecOnTestClusterNodeSSH(e2eCtx, testClusterResources.Kubeconfig, nodeName, vmSSH,
					fmt.Sprintf("sudo -n pvcreate -y %q 2>&1", largePath))
				Expect(errPV).NotTo(HaveOccurred(), "pvcreate")

				Eventually(func(g Gomega) {
					var bd v1alpha1.BlockDevice
					g.Expect(k8sClient.Get(e2eCtx, client.ObjectKey{Name: largeBD.Name}, &bd)).To(Succeed())
					g.Expect(bd.Status.Consumable).To(BeFalse())
				}, 3*time.Minute, 10*time.Second).Should(Succeed())
				var largeBDAfterPV v1alpha1.BlockDevice
				Expect(k8sClient.Get(e2eCtx, client.ObjectKey{Name: largeBD.Name}, &largeBDAfterPV)).To(Succeed())
				printBlockDeviceInfo(&largeBDAfterPV)

				By("Step 3: LVMVolumeGroup selecting only this BlockDevice — expect ValidationFailed")
				e2ePrintBlockDevicesConsumableSummary(e2eCtx, k8sClient, []string{largeBD.Name}, "single BD in selector")

				finalLvg := &v1alpha1.LVMVolumeGroup{
					ObjectMeta: metav1.ObjectMeta{Name: finalLvgName},
					Spec: v1alpha1.LVMVolumeGroupSpec{
						ActualVGNameOnTheNode: finalVgName,
						BlockDeviceSelector: &metav1.LabelSelector{
							MatchLabels: map[string]string{
								"kubernetes.io/hostname":      nodeName,
								"kubernetes.io/metadata.name": largeBdMeta,
							},
						},
						Type:  "Local",
						Local: v1alpha1.LVMVolumeGroupLocalSpec{NodeName: nodeName},
					},
				}
				Expect(k8sClient.Create(e2eCtx, finalLvg)).To(Succeed())
				defer func() {
					_ = client.IgnoreNotFound(k8sClient.Delete(e2eCtx, finalLvg))
				}()

				Eventually(func(g Gomega) {
					var cur v1alpha1.LVMVolumeGroup
					g.Expect(k8sClient.Get(e2eCtx, client.ObjectKey{Name: finalLvgName}, &cur)).To(Succeed())
					g.Expect(cur.Status.Phase).NotTo(Equal(v1alpha1.PhaseReady))
					var cfg *metav1.Condition
					for i := range cur.Status.Conditions {
						if cur.Status.Conditions[i].Type == lvgConditionVGConfigurationApplied {
							cfg = &cur.Status.Conditions[i]
							break
						}
					}
					g.Expect(cfg).NotTo(BeNil())
					g.Expect(cfg.Status).To(Equal(metav1.ConditionFalse))
					g.Expect(cfg.Reason).To(Equal(reasonValidationFailed))
					g.Expect(cfg.Message).To(ContainSubstring("not consumable"))
				}, 3*time.Minute, 8*time.Second).Should(Succeed())

				var finalDump v1alpha1.LVMVolumeGroup
				Expect(k8sClient.Get(e2eCtx, client.ObjectKey{Name: finalLvgName}, &finalDump)).To(Succeed())
				printLVMVolumeGroupInfo(&finalDump)

				vgsCmd := "vgs -o vg_name --noheadings 2>/dev/null || sudo -n vgs -o vg_name --noheadings 2>/dev/null"
				out, errVgs := e2eExecOnTestClusterNodeSSH(e2eCtx, testClusterResources.Kubeconfig, nodeName, vmSSH, vgsCmd)
				Expect(errVgs).NotTo(HaveOccurred())
				Expect(e2eVgNameListedInVgsOutput(out, finalVgName)).To(BeFalse(), "vgs:\n%s", out)

				By("✓ ValidationFailed on single non-consumable BD; other cluster BlockDevices were not in selector")
			})
		})

		///////////////////////////////////////////////////// ---=== TESTS END HERE ===--- /////////////////////////////////////////////////////

	}) // Describe: Sds Node Configurator
}) // Describe: sds-node-configurator module e2e
