/*
Copyright 2025 Flant JSC

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
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/storage-e2e/pkg/cluster"
	"github.com/deckhouse/storage-e2e/pkg/kubernetes"
)

const (
	e2eDefaultNamespace      = "e2e-test-cluster"
	e2eDefaultVMSSHUser      = "cloud"
	e2eClusterCleanupTimeout = 10 * time.Minute
	e2eLVMVGPrefix           = "e2e-lvg-"
	e2eLocalStorageClassName = "e2e-local-sc"
	e2ePVCPrefix             = "e2e-pvc-"
	e2ePodPrefix             = "e2e-pod-"
	e2eVirtualDiskPrefix     = "e2e-scheduler-data-disk"

	e2eVirtualDiskAttachMaxRetries    = 3
	e2eVirtualDiskAttachRetryInterval = 1 * time.Minute
)

var localStorageClassGVR = schema.GroupVersionResource{
	Group:    "storage.deckhouse.io",
	Version:  "v1alpha1",
	Resource: "localstorageclasses",
}

func e2eConfigNamespace() string {
	if v := os.Getenv("TEST_CLUSTER_NAMESPACE"); v != "" {
		return v
	}
	return e2eDefaultNamespace
}
func e2eConfigStorageClass() string       { return os.Getenv("TEST_CLUSTER_STORAGE_CLASS") }
func e2eConfigTestClusterCleanup() string { return os.Getenv("TEST_CLUSTER_CLEANUP") }
func e2eConfigSSHHost() string            { return os.Getenv("SSH_HOST") }
func e2eConfigSSHUser() string            { return os.Getenv("SSH_USER") }
func e2eConfigSSHJumpHost() string        { return os.Getenv("SSH_JUMP_HOST") }
func e2eConfigSSHJumpUser() string        { return os.Getenv("SSH_JUMP_USER") }
func e2eConfigSSHJumpKeyPath() string     { return os.Getenv("SSH_JUMP_KEY_PATH") }
func e2eConfigSSHPassphrase() string      { return os.Getenv("SSH_PASSPHRASE") }
func e2eConfigLogLevel() string           { return os.Getenv("LOG_LEVEL") }
func e2eConfigKubeConfigPath() string     { return os.Getenv("KUBE_CONFIG_PATH") }
func e2eConfigDKPLicenseKey() string {
	if v := os.Getenv("E2E_DKP_LICENSE_KEY"); v != "" {
		return v
	}
	return os.Getenv("DKP_LICENSE_KEY")
}
func e2eConfigRegistryDockerCfg() string {
	if v := os.Getenv("E2E_REGISTRY_DOCKER_CFG"); v != "" {
		return v
	}
	return os.Getenv("REGISTRY_DOCKER_CFG")
}
func e2eConfigTestClusterCreateMode() string { return os.Getenv("TEST_CLUSTER_CREATE_MODE") }
func e2eConfigVMSSHUser() string {
	if v := os.Getenv("SSH_VM_USER"); v != "" {
		return v
	}
	return e2eDefaultVMSSHUser
}

func attachVirtualDiskWithRetry(ctx context.Context, baseKubeconfig *rest.Config, config kubernetes.VirtualDiskAttachmentConfig, maxRetries int, retryInterval time.Duration) (*kubernetes.VirtualDiskAttachmentResult, error) {
	var lastErr error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		att, err := kubernetes.AttachVirtualDiskToVM(ctx, baseKubeconfig, config)
		if err == nil {
			return att, nil
		}
		lastErr = err
		if attempt < maxRetries {
			time.Sleep(retryInterval)
		}
	}
	return nil, lastErr
}

func blockDeviceSerialFromVirtualDiskUID(uid string) string {
	h := md5.Sum([]byte(uid))
	return hex.EncodeToString(h[:])
}

var _ = Describe("Common Scheduler Extender", Ordered, func() {
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

			cleanupEnabled := e2eConfigTestClusterCleanup() == "true" || e2eConfigTestClusterCleanup() == "True"
			if cleanupEnabled {
				GinkgoWriter.Printf("    ▶️ Cleaning up test cluster resources (TEST_CLUSTER_CLEANUP is enabled - all VMs will be removed)...\n")
			} else {
				GinkgoWriter.Printf("    ▶️ Cleaning up test cluster resources (TEST_CLUSTER_CLEANUP is not enabled - only bootstrap node will be removed)...\n")
			}
			err := cluster.CleanupTestCluster(ctx, testClusterResources)
			if err != nil {
				GinkgoWriter.Printf("    ⚠️  Warning: Cleanup errors occurred: %v\n", err)
			} else {
				GinkgoWriter.Printf("    ✅ Test cluster resources cleaned up successfully\n")
			}
		}
	})

	// ---=== TEST CLUSTER IS CREATED AND READY HERE ===--- //
	It("should create test cluster", func() {
		testClusterResources = cluster.CreateOrConnectToTestCluster()
	})

	////////////////////////////////////
	// ---=== SETUP: CREATE VIRTUAL DISKS ===--- //
	////////////////////////////////////

	Context("Setup: Create virtual disks and LVMVolumeGroups", func() {
		const e2eDataDiskSize = "10Gi"

		It("Should create virtual disks on cluster nodes", func() {
			ensureE2EK8sClient(testClusterResources, &k8sClient, e2eCtx, createdLVGs)

			var clusterVMs []string
			var baseKubeconfig *rest.Config
			ns := e2eConfigNamespace()
			storageClass := e2eConfigStorageClass()
			Expect(storageClass).NotTo(BeEmpty(), "TEST_CLUSTER_STORAGE_CLASS is required for VirtualDisk")

			By("Cleaning up existing e2e LVMLogicalVolumes (orphan PVCs)")
			cleanupE2ELVMLogicalVolumes(e2eCtx, k8sClient)
			
			By("Cleaning up existing e2e LVMVolumeGroups (to release LVM signatures)")
			cleanupE2ELVMVolumeGroups(e2eCtx, k8sClient)
			
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

			By(fmt.Sprintf("Creating LocalStorageClass %s with LVMVolumeGroups: %v", e2eLocalStorageClassName, lvgNames))

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

			_, err = dynamicClient.Resource(localStorageClassGVR).Create(e2eCtx, lsc, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred(), "create LocalStorageClass")

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
			minVolumeSize := int64(500 * 1024 * 1024) // 500Mi minimum for remainder
			
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

			volumeSize := int64(5 * 1024 * 1024 * 1024) // 5Gi
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

			volumeSize := int64(10 * 1024 * 1024 * 1024) // 10Gi
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

func ensureE2EK8sClient(resources *cluster.TestClusterResources, k8s *client.Client, ctx context.Context, lvgsToCleanup []*v1alpha1.LVMVolumeGroup) {
	if *k8s != nil {
		return
	}
	Expect(resources).NotTo(BeNil(), "test cluster must be created first")
	Expect(resources.Kubeconfig).NotTo(BeNil())
	err := v1alpha1.AddToScheme(scheme.Scheme)
	Expect(err).NotTo(HaveOccurred())
	var err2 error
	*k8s, err2 = client.New(resources.Kubeconfig, client.Options{Scheme: scheme.Scheme})
	Expect(err2).NotTo(HaveOccurred())
	_, err2 = (*k8s).RESTMapper().RESTMapping(v1alpha1.SchemeGroupVersion.WithKind("BlockDevice").GroupKind())
	Expect(err2).NotTo(HaveOccurred())
	By("Cleaning up existing e2e resources")
	cleanupE2ELocalStorageClasses(ctx, resources.Kubeconfig)
	cleanupE2ELVMVolumeGroups(ctx, *k8s)
	cleanupE2EPVCs(ctx, *k8s)
	cleanupE2EPods(ctx, *k8s)
}

func cleanupE2ELocalStorageClasses(ctx context.Context, kubeconfig *rest.Config) {
	dynClient, err := dynamic.NewForConfig(kubeconfig)
	if err != nil {
		GinkgoWriter.Printf("Failed to create dynamic client for LocalStorageClass cleanup: %v\n", err)
		return
	}

	lscList, err := dynClient.Resource(localStorageClassGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		return
	}

	for _, item := range lscList.Items {
		if strings.HasPrefix(item.GetName(), "e2e-") {
			GinkgoWriter.Printf("Deleting LocalStorageClass %s\n", item.GetName())
			_ = dynClient.Resource(localStorageClassGVR).Delete(ctx, item.GetName(), metav1.DeleteOptions{})
		}
	}
}

var virtualDiskGVR = schema.GroupVersionResource{
	Group:    "virtualization.deckhouse.io",
	Version:  "v1alpha2",
	Resource: "virtualdisks",
}

var vmbdaGVR = schema.GroupVersionResource{
	Group:    "virtualization.deckhouse.io",
	Version:  "v1alpha2",
	Resource: "virtualmachineblockdeviceattachments",
}

func forceDeleteAllNonConsumableBlockDevices(ctx context.Context, cl client.Client, timeout time.Duration) {
	var bdList v1alpha1.BlockDeviceList
	if err := cl.List(ctx, &bdList, &client.ListOptions{}); err != nil {
		GinkgoWriter.Printf("Failed to list BlockDevices: %v\n", err)
		return
	}
	
	var toDelete []*v1alpha1.BlockDevice
	for i := range bdList.Items {
		bd := &bdList.Items[i]
		if !bd.Status.Consumable {
			toDelete = append(toDelete, bd)
		}
	}
	
	if len(toDelete) == 0 {
		GinkgoWriter.Println("No non-consumable BlockDevices found")
		return
	}
	
	GinkgoWriter.Printf("Force deleting %d non-consumable BlockDevices\n", len(toDelete))
	
	for _, bd := range toDelete {
		GinkgoWriter.Printf("  Removing finalizers and deleting BD %s (%s on %s, fsType=%s)\n", 
			bd.Name, bd.Status.Path, bd.Status.NodeName, bd.Status.FsType)
		
		if len(bd.Finalizers) > 0 {
			bdCopy := bd.DeepCopy()
			bdCopy.Finalizers = nil
			if err := cl.Update(ctx, bdCopy); err != nil {
				GinkgoWriter.Printf("    Failed to remove finalizers: %v\n", err)
			}
		}
		
		if err := cl.Delete(ctx, bd); err != nil {
			GinkgoWriter.Printf("    Failed to delete: %v\n", err)
		}
	}
	
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if err := cl.List(ctx, &bdList, &client.ListOptions{}); err != nil {
			time.Sleep(5 * time.Second)
			continue
		}
		
		remaining := 0
		for i := range bdList.Items {
			if !bdList.Items[i].Status.Consumable {
				remaining++
			}
		}
		
		if remaining == 0 {
			GinkgoWriter.Println("All non-consumable BlockDevices deleted")
			return
		}
		
		GinkgoWriter.Printf("Waiting for %d non-consumable BlockDevices to be deleted...\n", remaining)
		time.Sleep(5 * time.Second)
	}
	GinkgoWriter.Println("Warning: timeout waiting for non-consumable BlockDevices deletion")
}

func waitForBlockDevicesConsumable(ctx context.Context, cl client.Client, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var bdList v1alpha1.BlockDeviceList
		if err := cl.List(ctx, &bdList, &client.ListOptions{}); err != nil {
			time.Sleep(5 * time.Second)
			continue
		}
		
		if len(bdList.Items) == 0 {
			GinkgoWriter.Println("No BlockDevices found, nothing to wait for")
			return
		}
		
		allConsumable := true
		var notConsumable []string
		for i := range bdList.Items {
			bd := &bdList.Items[i]
			if !bd.Status.Consumable {
				allConsumable = false
				notConsumable = append(notConsumable, fmt.Sprintf("%s (fsType=%s, pvUUID=%s)", 
					bd.Name, bd.Status.FsType, bd.Status.PVUuid))
			}
		}
		
		if allConsumable {
			GinkgoWriter.Printf("All %d BlockDevices are now consumable\n", len(bdList.Items))
			return
		}
		
		GinkgoWriter.Printf("Waiting for %d BlockDevices to become consumable: %v\n", len(notConsumable), notConsumable)
		time.Sleep(10 * time.Second)
	}
	GinkgoWriter.Println("Warning: timeout waiting for BlockDevices to become consumable")
}

func cleanupE2EBlockDevices(ctx context.Context, cl client.Client, timeout time.Duration) {
	if cl == nil {
		return
	}
	
	var bdList v1alpha1.BlockDeviceList
	if err := cl.List(ctx, &bdList, &client.ListOptions{}); err != nil {
		GinkgoWriter.Printf("Failed to list BlockDevices: %v\n", err)
		return
	}
	
	var toDelete []string
	for i := range bdList.Items {
		bd := &bdList.Items[i]
		if !bd.Status.Consumable {
			toDelete = append(toDelete, bd.Name)
		}
	}
	
	if len(toDelete) == 0 {
		GinkgoWriter.Println("No non-consumable BlockDevices to delete")
		return
	}
	
	GinkgoWriter.Printf("Deleting %d non-consumable BlockDevices: %v\n", len(toDelete), toDelete)
	for _, name := range toDelete {
		bd := &v1alpha1.BlockDevice{}
		bd.Name = name
		if err := cl.Delete(ctx, bd); err != nil {
			GinkgoWriter.Printf("Failed to delete BlockDevice %s: %v\n", name, err)
		}
	}
	
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if err := cl.List(ctx, &bdList, &client.ListOptions{}); err != nil {
			time.Sleep(5 * time.Second)
			continue
		}
		
		remaining := 0
		for i := range bdList.Items {
			if !bdList.Items[i].Status.Consumable {
				remaining++
			}
		}
		
		if remaining == 0 {
			GinkgoWriter.Println("All non-consumable BlockDevices deleted")
			return
		}
		
		GinkgoWriter.Printf("Waiting for BlockDevices deletion: %d remaining\n", remaining)
		time.Sleep(5 * time.Second)
	}
	GinkgoWriter.Println("Warning: timeout waiting for BlockDevices deletion")
}

func cleanupE2EVirtualDisks(ctx context.Context, kubeconfig *rest.Config, namespace, prefix string) {
	dynClient, err := dynamic.NewForConfig(kubeconfig)
	if err != nil {
		GinkgoWriter.Printf("Failed to create dynamic client for VirtualDisk cleanup: %v\n", err)
		return
	}

	vmbdaList, err := dynClient.Resource(vmbdaGVR).Namespace(namespace).List(ctx, metav1.ListOptions{})
	if err == nil {
		for _, item := range vmbdaList.Items {
			if strings.HasPrefix(item.GetName(), prefix) {
				GinkgoWriter.Printf("Deleting VirtualMachineBlockDeviceAttachment %s\n", item.GetName())
				_ = dynClient.Resource(vmbdaGVR).Namespace(namespace).Delete(ctx, item.GetName(), metav1.DeleteOptions{})
			}
		}
	}

	vdList, err := dynClient.Resource(virtualDiskGVR).Namespace(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		GinkgoWriter.Printf("Failed to list VirtualDisks: %v\n", err)
		return
	}

	var toDelete []string
	for _, item := range vdList.Items {
		if strings.HasPrefix(item.GetName(), prefix) {
			toDelete = append(toDelete, item.GetName())
		}
	}

	if len(toDelete) == 0 {
		return
	}

	GinkgoWriter.Printf("Deleting %d VirtualDisks with prefix %s: %v\n", len(toDelete), prefix, toDelete)
	for _, name := range toDelete {
		_ = dynClient.Resource(virtualDiskGVR).Namespace(namespace).Delete(ctx, name, metav1.DeleteOptions{})
	}

	deadline := time.Now().Add(3 * time.Minute)
	for time.Now().Before(deadline) {
		vdList, err = dynClient.Resource(virtualDiskGVR).Namespace(namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			break
		}
		remaining := 0
		for _, item := range vdList.Items {
			if strings.HasPrefix(item.GetName(), prefix) {
				remaining++
			}
		}
		if remaining == 0 {
			GinkgoWriter.Println("All e2e VirtualDisks deleted")
			return
		}
		time.Sleep(5 * time.Second)
	}
	GinkgoWriter.Println("Warning: some e2e VirtualDisks may still exist after cleanup")
}

func cleanupE2ELVMLogicalVolumes(ctx context.Context, cl client.Client) {
	var list v1alpha1.LVMLogicalVolumeList
	err := cl.List(ctx, &list, &client.ListOptions{})
	if err != nil {
		GinkgoWriter.Printf("List LVMLogicalVolumes failed (skip cleanup): %v\n", err)
		return
	}
	
	var toDelete []string
	for i := range list.Items {
		llv := &list.Items[i]
		if strings.HasPrefix(llv.Spec.LVMVolumeGroupName, e2eLVMVGPrefix) {
			toDelete = append(toDelete, llv.Name)
		}
	}
	
	if len(toDelete) == 0 {
		return
	}
	
	GinkgoWriter.Printf("Deleting %d LVMLogicalVolume(s) referencing e2e LVGs\n", len(toDelete))
	for _, name := range toDelete {
		var llv v1alpha1.LVMLogicalVolume
		if err := cl.Get(ctx, client.ObjectKey{Name: name}, &llv); err != nil {
			continue
		}
		
		if len(llv.Finalizers) > 0 {
			llv.Finalizers = nil
			if err := cl.Update(ctx, &llv); err != nil {
				GinkgoWriter.Printf("  Failed to remove finalizers from LLV %s: %v\n", name, err)
			}
		}
		
		if err := cl.Delete(ctx, &llv); err != nil {
			GinkgoWriter.Printf("  Failed to delete LLV %s: %v\n", name, err)
		}
	}
	
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		err := cl.List(ctx, &list, &client.ListOptions{})
		if err != nil {
			break
		}
		remaining := 0
		for i := range list.Items {
			if strings.HasPrefix(list.Items[i].Spec.LVMVolumeGroupName, e2eLVMVGPrefix) {
				remaining++
			}
		}
		if remaining == 0 {
			GinkgoWriter.Println("All e2e LVMLogicalVolumes deleted")
			return
		}
		GinkgoWriter.Printf("Waiting for %d LVMLogicalVolumes to be deleted...\n", remaining)
		time.Sleep(5 * time.Second)
	}
	GinkgoWriter.Println("Warning: timeout waiting for LVMLogicalVolumes deletion")
}

func cleanupE2ELVMVolumeGroups(ctx context.Context, cl client.Client) {
	var list v1alpha1.LVMVolumeGroupList
	err := cl.List(ctx, &list, &client.ListOptions{})
	if err != nil {
		GinkgoWriter.Printf("List LVMVolumeGroups failed (skip cleanup): %v\n", err)
		return
	}
	var toDelete []string
	for i := range list.Items {
		if strings.HasPrefix(list.Items[i].Name, e2eLVMVGPrefix) {
			toDelete = append(toDelete, list.Items[i].Name)
		}
	}
	if len(toDelete) == 0 {
		return
	}
	GinkgoWriter.Printf("Deleting %d LVMVolumeGroup(s): %v\n", len(toDelete), toDelete)
	for _, name := range toDelete {
		lvg := &v1alpha1.LVMVolumeGroup{}
		lvg.Name = name
		_ = cl.Delete(ctx, lvg)
	}
	
	GinkgoWriter.Println("Waiting for sds-node-configurator to cleanup VGs (up to 3 minutes)...")
	deadline := time.Now().Add(3 * time.Minute)
	forceRemoveAfter := time.Now().Add(2 * time.Minute)
	
	for time.Now().Before(deadline) {
		err := cl.List(ctx, &list, &client.ListOptions{})
		if err != nil {
			break
		}
		var remaining []string
		for i := range list.Items {
			if strings.HasPrefix(list.Items[i].Name, e2eLVMVGPrefix) {
				remaining = append(remaining, list.Items[i].Name)
			}
		}
		if len(remaining) == 0 {
			GinkgoWriter.Println("All e2e LVMVolumeGroups removed (VGs cleaned by sds-node-configurator)")
			return
		}
		
		if time.Now().After(forceRemoveAfter) {
			GinkgoWriter.Printf("Force removing %d stuck LVMVolumeGroups\n", len(remaining))
			for _, name := range remaining {
				lvg := &v1alpha1.LVMVolumeGroup{}
				if err := cl.Get(ctx, client.ObjectKey{Name: name}, lvg); err != nil {
					continue
				}
				if len(lvg.Finalizers) > 0 {
					lvg.Finalizers = nil
					_ = cl.Update(ctx, lvg)
				}
			}
		}
		
		GinkgoWriter.Printf("Waiting for %d LVMVolumeGroups to be deleted...\n", len(remaining))
		time.Sleep(10 * time.Second)
	}
}

func cleanupE2EPVCs(ctx context.Context, cl client.Client) {
	var list corev1.PersistentVolumeClaimList
	err := cl.List(ctx, &list, &client.ListOptions{})
	if err != nil {
		return
	}
	for i := range list.Items {
		if strings.HasPrefix(list.Items[i].Name, e2ePVCPrefix) {
			_ = cl.Delete(ctx, &list.Items[i])
		}
	}
}

func cleanupE2EPods(ctx context.Context, cl client.Client) {
	var list corev1.PodList
	err := cl.List(ctx, &list, &client.ListOptions{})
	if err != nil {
		return
	}
	for i := range list.Items {
		if strings.HasPrefix(list.Items[i].Name, e2ePodPrefix) {
			_ = cl.Delete(ctx, &list.Items[i])
		}
	}
}

func cleanupE2EPodsAndPVCsWithWait(ctx context.Context, cl client.Client, timeout time.Duration) {
	cleanupE2EPods(ctx, cl)
	cleanupE2EPVCs(ctx, cl)

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var podList corev1.PodList
		var pvcList corev1.PersistentVolumeClaimList

		podCount := 0
		pvcCount := 0

		if err := cl.List(ctx, &podList, &client.ListOptions{Namespace: "default"}); err == nil {
			for i := range podList.Items {
				if strings.HasPrefix(podList.Items[i].Name, e2ePodPrefix) {
					podCount++
				}
			}
		}

		if err := cl.List(ctx, &pvcList, &client.ListOptions{Namespace: "default"}); err == nil {
			for i := range pvcList.Items {
				if strings.HasPrefix(pvcList.Items[i].Name, e2ePVCPrefix) {
					pvcCount++
				}
			}
		}

		if podCount == 0 && pvcCount == 0 {
			GinkgoWriter.Println("All e2e Pods and PVCs deleted")
			return
		}

		GinkgoWriter.Printf("Waiting for cleanup: %d pods, %d PVCs remaining\n", podCount, pvcCount)
		time.Sleep(5 * time.Second)
	}
	GinkgoWriter.Println("Warning: some e2e Pods/PVCs may still exist after cleanup timeout")
}

func keysOf(m map[string]struct{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func getTotalAvailableSpace(ctx context.Context, cl client.Client, lvgs []*v1alpha1.LVMVolumeGroup) int64 {
	var total int64
	for _, lvg := range lvgs {
		var current v1alpha1.LVMVolumeGroup
		if err := cl.Get(ctx, client.ObjectKeyFromObject(lvg), &current); err != nil {
			continue
		}
		if current.Status.Phase == v1alpha1.PhaseReady {
			total += current.Status.VGFree.Value()
		}
	}
	return total
}

func createPVCsAndPodsWithSizes(ctx context.Context, cl client.Client, volumeSizes []int64, storageClass, sizeLabel string) (successCount, scheduledCount int) {
	for i, volumeSize := range volumeSizes {
		pvcName := fmt.Sprintf("%s%s-%d", e2ePVCPrefix, sizeLabel, i)
		podName := fmt.Sprintf("%s%s-%d", e2ePodPrefix, sizeLabel, i)

		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      pvcName,
				Namespace: "default",
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: *resource.NewQuantity(volumeSize, resource.BinarySI),
					},
				},
				StorageClassName: &storageClass,
			},
		}

		if err := cl.Create(ctx, pvc); err != nil {
			GinkgoWriter.Printf("Failed to create PVC %s: %v\n", pvcName, err)
			continue
		}
		successCount++

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      podName,
				Namespace: "default",
			},
			Spec: corev1.PodSpec{
				Tolerations: []corev1.Toleration{
					{
						Key:      "node-role.kubernetes.io/control-plane",
						Operator: corev1.TolerationOpExists,
						Effect:   corev1.TaintEffectNoSchedule,
					},
					{
						Key:      "node-role.kubernetes.io/master",
						Operator: corev1.TolerationOpExists,
						Effect:   corev1.TaintEffectNoSchedule,
					},
				},
				Containers: []corev1.Container{
					{
						Name:    "test",
						Image:   "busybox",
						Command: []string{"sleep", "3600"},
						VolumeMounts: []corev1.VolumeMount{
							{Name: "data", MountPath: "/data"},
						},
					},
				},
				Volumes: []corev1.Volume{
					{
						Name: "data",
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
								ClaimName: pvcName,
							},
						},
					},
				},
			},
		}

		if err := cl.Create(ctx, pod); err != nil {
			GinkgoWriter.Printf("Failed to create Pod %s: %v\n", podName, err)
			continue
		}
	}

	scheduledCount = waitForPodsScheduled(ctx, cl, sizeLabel, successCount, 5*time.Minute)
	printPVCAndPodStatus(ctx, cl, sizeLabel)
	return successCount, scheduledCount
}

func createPVCsAndPods(ctx context.Context, cl client.Client, numVolumes int, volumeSize int64, storageClass, sizeLabel string) (successCount, scheduledCount int) {
	for i := 0; i < numVolumes; i++ {
		pvcName := fmt.Sprintf("%s%s-%d", e2ePVCPrefix, sizeLabel, i)
		podName := fmt.Sprintf("%s%s-%d", e2ePodPrefix, sizeLabel, i)

		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      pvcName,
				Namespace: "default",
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: *resource.NewQuantity(volumeSize, resource.BinarySI),
					},
				},
				StorageClassName: &storageClass,
			},
		}

		if err := cl.Create(ctx, pvc); err != nil {
			GinkgoWriter.Printf("Failed to create PVC %s: %v\n", pvcName, err)
			continue
		}
		successCount++

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      podName,
				Namespace: "default",
			},
			Spec: corev1.PodSpec{
				Tolerations: []corev1.Toleration{
					{
						Key:      "node-role.kubernetes.io/control-plane",
						Operator: corev1.TolerationOpExists,
						Effect:   corev1.TaintEffectNoSchedule,
					},
					{
						Key:      "node-role.kubernetes.io/master",
						Operator: corev1.TolerationOpExists,
						Effect:   corev1.TaintEffectNoSchedule,
					},
				},
				Containers: []corev1.Container{
					{
						Name:    "test",
						Image:   "busybox",
						Command: []string{"sleep", "3600"},
						VolumeMounts: []corev1.VolumeMount{
							{Name: "data", MountPath: "/data"},
						},
					},
				},
				Volumes: []corev1.Volume{
					{
						Name: "data",
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
								ClaimName: pvcName,
							},
						},
					},
				},
			},
		}

		if err := cl.Create(ctx, pod); err != nil {
			GinkgoWriter.Printf("Failed to create Pod %s: %v\n", podName, err)
			continue
		}
	}

	scheduledCount = waitForPodsScheduled(ctx, cl, sizeLabel, successCount, 5*time.Minute)

	printPVCAndPodStatus(ctx, cl, sizeLabel)

	return successCount, scheduledCount
}

func waitForPodsScheduled(ctx context.Context, cl client.Client, sizeLabel string, expectedCount int, timeout time.Duration) int {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var podList corev1.PodList
		if err := cl.List(ctx, &podList, &client.ListOptions{Namespace: "default"}); err != nil {
			time.Sleep(5 * time.Second)
			continue
		}

		scheduledCount := 0
		for i := range podList.Items {
			pod := &podList.Items[i]
			if !strings.HasPrefix(pod.Name, e2ePodPrefix+sizeLabel) {
				continue
			}
			if pod.Spec.NodeName != "" || pod.Status.Phase == corev1.PodRunning || pod.Status.Phase == corev1.PodSucceeded {
				scheduledCount++
			}
		}

		if scheduledCount >= expectedCount {
			return scheduledCount
		}

		time.Sleep(5 * time.Second)
	}

	var podList corev1.PodList
	_ = cl.List(ctx, &podList, &client.ListOptions{Namespace: "default"})
	scheduledCount := 0
	for i := range podList.Items {
		pod := &podList.Items[i]
		if !strings.HasPrefix(pod.Name, e2ePodPrefix+sizeLabel) {
			continue
		}
		if pod.Spec.NodeName != "" || pod.Status.Phase == corev1.PodRunning || pod.Status.Phase == corev1.PodSucceeded {
			scheduledCount++
		}
	}
	return scheduledCount
}

func printBlockDevicesSummary(bds []*v1alpha1.BlockDevice) {
	GinkgoWriter.Println("\n========== BlockDevices Summary ==========")
	for _, bd := range bds {
		GinkgoWriter.Printf("  %s: node=%s path=%s size=%s\n",
			bd.Name, bd.Status.NodeName, bd.Status.Path, bd.Status.Size.String())
	}
	GinkgoWriter.Println("==========================================\n")
}

func printLVGsSummary(ctx context.Context, cl client.Client, lvgs []*v1alpha1.LVMVolumeGroup) {
	GinkgoWriter.Println("\n========== LVMVolumeGroups Summary ==========")
	for _, lvg := range lvgs {
		var current v1alpha1.LVMVolumeGroup
		if err := cl.Get(ctx, client.ObjectKeyFromObject(lvg), &current); err != nil {
			GinkgoWriter.Printf("  %s: error getting status: %v\n", lvg.Name, err)
			continue
		}
		GinkgoWriter.Printf("  %s: phase=%s vgSize=%s vgFree=%s\n",
			current.Name, current.Status.Phase, current.Status.VGSize.String(), current.Status.VGFree.String())
	}
	GinkgoWriter.Println("=============================================\n")
}

func printSchedulingSummary(testName string, attempted, created, scheduled int, volumeSize int64) {
	GinkgoWriter.Println("\n========== Scheduling Summary ==========")
	GinkgoWriter.Printf("Test: %s\n", testName)
	GinkgoWriter.Printf("Volume size: %d bytes (%.2f Mi)\n", volumeSize, float64(volumeSize)/(1024*1024))
	GinkgoWriter.Printf("Attempted: %d\n", attempted)
	GinkgoWriter.Printf("Created: %d\n", created)
	GinkgoWriter.Printf("Scheduled: %d\n", scheduled)
	if created > 0 {
		GinkgoWriter.Printf("Success rate: %.1f%%\n", float64(scheduled)/float64(created)*100)
	}
	GinkgoWriter.Println("=========================================\n")
}

func printPVCAndPodStatus(ctx context.Context, cl client.Client, sizeLabel string) {
	GinkgoWriter.Println("\n========== PVC Status (first 10) ==========")
	var pvcList corev1.PersistentVolumeClaimList
	if err := cl.List(ctx, &pvcList, &client.ListOptions{Namespace: "default"}); err == nil {
		count := 0
		pendingCount := 0
		boundCount := 0
		for i := range pvcList.Items {
			pvc := &pvcList.Items[i]
			if !strings.HasPrefix(pvc.Name, e2ePVCPrefix+sizeLabel) {
				continue
			}
			if pvc.Status.Phase == corev1.ClaimPending {
				pendingCount++
			} else if pvc.Status.Phase == corev1.ClaimBound {
				boundCount++
			}
			if count < 10 {
				GinkgoWriter.Printf("  %s: phase=%s storageClass=%s\n",
					pvc.Name, pvc.Status.Phase, *pvc.Spec.StorageClassName)
				count++
			}
		}
		GinkgoWriter.Printf("Total: %d Bound, %d Pending\n", boundCount, pendingCount)
	}

	GinkgoWriter.Println("\n========== Pod Status (first 10) ==========")
	var podList corev1.PodList
	if err := cl.List(ctx, &podList, &client.ListOptions{Namespace: "default"}); err == nil {
		count := 0
		scheduledCount := 0
		pendingCount := 0
		for i := range podList.Items {
			pod := &podList.Items[i]
			if !strings.HasPrefix(pod.Name, e2ePodPrefix+sizeLabel) {
				continue
			}
			if pod.Spec.NodeName != "" {
				scheduledCount++
			} else {
				pendingCount++
			}
			if count < 10 {
				reason := ""
				if len(pod.Status.Conditions) > 0 {
					for _, cond := range pod.Status.Conditions {
						if cond.Type == corev1.PodScheduled && cond.Status == corev1.ConditionFalse {
							reason = cond.Reason + ": " + cond.Message
							break
						}
					}
				}
				GinkgoWriter.Printf("  %s: phase=%s node=%s reason=%s\n",
					pod.Name, pod.Status.Phase, pod.Spec.NodeName, reason)
				count++
			}
		}
		GinkgoWriter.Printf("Total: %d Scheduled, %d Pending\n", scheduledCount, pendingCount)
	}
	GinkgoWriter.Println("==========================================\n")
}
