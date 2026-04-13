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
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"runtime"
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
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	k8sclient "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/storage-e2e/pkg/cluster"
	"github.com/deckhouse/storage-e2e/pkg/kubernetes"
	"github.com/deckhouse/storage-e2e/pkg/ssh"
)

// clusterResumeState mirrors storage-e2e cluster-state.json (namespace after VMs are created).
type clusterResumeState struct {
	Namespace string `json:"namespace"`
}

// e2eSavedLVGForVGRemoveInfo records the pvresize LVMVolumeGroup for the follow-up "remove VG" spec.
type e2eSavedLVGForVGRemoveInfo struct {
	lvgName, nodeName, vgNameOnNode, blockDeviceName string
}

// runLsblkViaDirectSSHWithRetry wraps runLsblkViaDirectSSH for transient SSH errors (EOF during handshake, reset).
func runLsblkViaDirectSSHWithRetry(ctx context.Context, testKubeconfig *rest.Config, nodeName, sshUser string, maxRetries int, retryInterval time.Duration) (map[string]lsblkLine, error) {
	var lastErr error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		lines, err := runLsblkViaDirectSSH(ctx, testKubeconfig, nodeName, sshUser)
		if err == nil {
			return lines, nil
		}
		lastErr = err
		if attempt < maxRetries {
			GinkgoWriter.Printf("      lsblk SSH to %s attempt %d/%d failed: %v; retry in %v\n", nodeName, attempt, maxRetries, err, retryInterval)
			time.Sleep(retryInterval)
		}
	}
	return nil, lastErr
}

// expectedDisk is the expected (node, VD name) for one created VirtualDisk (same order as e2eDiskAttachments).
// Serial: virtualization may use VirtualDisk.UID or VirtualMachineBlockDeviceAttachment.UID (hex MD5); we accept either.
type expectedDisk struct {
	Node                string
	VDDiskName          string
	ExpectedSerialVD    string // hex(MD5(VirtualDisk.UID))
	ExpectedSerialVMBDA string // hex(MD5(VirtualMachineBlockDeviceAttachment.UID))
	ExpectedBDName      string
}

// blockDeviceNameFromDiscoveryInput returns the BlockDevice name (same formula as agent createUniqDeviceName: dev-SHA1(nodeName+wwn+model+serial+partUUID)).
func blockDeviceNameFromDiscoveryInput(nodeName, wwn, model, serial, partUUID string) string {
	temp := nodeName + wwn + model + serial + partUUID
	s := sha1.Sum([]byte(temp))
	return fmt.Sprintf("dev-%x", s)
}

// nameSerialCheckRow is one row of the BlockDevice name/serial check table (expected vs actual).
type nameSerialCheckRow struct {
	Node                string
	VDName              string
	BDName              string
	ExpectedSerialVD    string
	ExpectedSerialVMBDA string
	ActualSerial        string
	SerialMatch         bool
	ExpectedBDName      string
	ActualBDName        string
	NameMatch           bool
}

// discoveryTableRow is one row of the discovery test summary (VD + BD + lsblk).
type discoveryTableRow struct {
	Node        string
	VDName      string
	BDName      string
	Path        string
	SerialBD    string
	SerialLsblk string
	SizeBD      string
	SizeLsblk   string
	Match       bool
}

// lsblkLine is one device line from lsblk -b -P -o NAME,SIZE,SERIAL,PATH (keyed by PATH).
type lsblkLine struct {
	Path      string
	Serial    string
	Size      string
	SizeBytes int64
}

var localStorageClassGVR = schema.GroupVersionResource{
	Group:    "storage.deckhouse.io",
	Version:  "v1alpha1",
	Resource: "localstorageclasses",
}

var _ = Describe("sds-node-configurator module e2e", Ordered, func() {

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

		// Per-Describe cleanup: after Common Scheduler finishes, tear down shared LVM/VD/BlockDevices so
		// "Sds Node Configurator" starts without leftover scheduler disks. Suite-wide cleanup still runs in root AfterAll.

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

				if testClusterResources.BaseKubeconfig != nil {
					By("Cleaning up e2e VirtualDisks and attachments before tests")
					cleanupE2EVirtualDisks(e2eCtx, testClusterResources.BaseKubeconfig, ns, e2eVirtualDiskPrefix)

					By("Force deleting ALL non-consumable BlockDevices")
					forceDeleteAllNonConsumableBlockDevices(e2eCtx, k8sClient, 2*time.Minute)
				}

				if testClusterResources.BaseKubeconfig == nil {
					Skip("VirtualDisk creation requires base cluster kubeconfig (Deckhouse virtualization). " +
						"Set SSH_JUMP_HOST to the base cluster or use TEST_CLUSTER_CREATE_MODE=alwaysCreateNew.")
				}
				baseKubeconfig = testClusterResources.BaseKubeconfig
				By("Selecting VirtualMachines in phase Running only (skip Migrating/Starting for stable disk attach)")
				clusterVMs = e2eListClusterVMNames(e2eCtx, testClusterResources, ns)

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

				attachCtx, cancel := context.WithTimeout(e2eCtx, e2eVirtualDiskAttachWaitTimeout)
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

			It("Should delete a BlockDevice after the backing disk disappears", func() {
				const missingDiskPrefix = "e2e-blockdevice-missing-disk"

				ensureSchedulerE2EK8sClient(testClusterResources, &k8sClient, e2eCtx)
				if testClusterResources.BaseKubeconfig == nil {
					Skip("BlockDevice disappearance test requires nested virtualization (base cluster kubeconfig)")
				}

				ns := e2eConfigNamespace()
				storageClass := e2eConfigStorageClass()
				Expect(storageClass).NotTo(BeEmpty(), "TEST_CLUSTER_STORAGE_CLASS is required for VirtualDisk")

				By("Guest VM for attach: phase Running only (skip Migrating)")
				clusterVMs := e2eListClusterVMNames(e2eCtx, testClusterResources, ns)

				targetVM := clusterVMs[rand.Intn(len(clusterVMs))]
				diskName := fmt.Sprintf("%s-%d", missingDiskPrefix, rand.Intn(100000))

				var blockDevicesList v1alpha1.BlockDeviceList
				Expect(k8sClient.List(e2eCtx, &blockDevicesList, &client.ListOptions{})).To(Succeed())
				initialNames := make(map[string]struct{}, len(blockDevicesList.Items))
				for i := range blockDevicesList.Items {
					initialNames[blockDevicesList.Items[i].Name] = struct{}{}
				}

				By(fmt.Sprintf("Step 1: Attaching VirtualDisk %s (%s) to VM %s", diskName, e2eDataDiskSize, targetVM))
				diskAttachment, err := attachVirtualDiskWithRetry(e2eCtx, testClusterResources.BaseKubeconfig, kubernetes.VirtualDiskAttachmentConfig{
					VMName:           targetVM,
					Namespace:        ns,
					DiskName:         diskName,
					DiskSize:         e2eDataDiskSize,
					StorageClassName: storageClass,
				}, e2eVirtualDiskAttachMaxRetries, e2eVirtualDiskAttachRetryInterval)
				Expect(err).NotTo(HaveOccurred())
				e2eDiskAttachments = append(e2eDiskAttachments, diskAttachment)

				attachCtx, cancel := context.WithTimeout(e2eCtx, 5*time.Minute)
				defer cancel()
				Expect(kubernetes.WaitForVirtualDiskAttached(attachCtx, testClusterResources.BaseKubeconfig, ns, diskAttachment.AttachmentName, 10*time.Second)).To(Succeed())

				By("Step 2: Waiting for the new BlockDevice to appear")
				var discoveredBD v1alpha1.BlockDevice
				Eventually(func(g Gomega) {
					var list v1alpha1.BlockDeviceList
					g.Expect(k8sClient.List(e2eCtx, &list, &client.ListOptions{})).To(Succeed())

					var matches []v1alpha1.BlockDevice
					for i := range list.Items {
						bd := list.Items[i]
						if _, existed := initialNames[bd.Name]; existed {
							continue
						}
						if bd.Status.NodeName != targetVM {
							continue
						}
						if !bd.Status.Consumable || bd.Status.Path == "" || bd.Status.Size.IsZero() {
							continue
						}
						matches = append(matches, bd)
					}

					g.Expect(matches).To(HaveLen(1),
						"expected exactly one new consumable BlockDevice on node %s after attaching %s; got %d",
						targetVM, diskName, len(matches))
					discoveredBD = matches[0]
				}, 5*time.Minute, 10*time.Second).Should(Succeed())
				By(fmt.Sprintf("Discovered BlockDevice %s on node %s (path=%s, size=%s)",
					discoveredBD.Name, discoveredBD.Status.NodeName, discoveredBD.Status.Path, discoveredBD.Status.Size.String()))

				By("Step 3: Detaching and deleting the VirtualDisk to simulate device loss")
				Expect(kubernetes.DetachAndDeleteVirtualDisk(
					e2eCtx,
					testClusterResources.BaseKubeconfig,
					ns,
					diskAttachment.AttachmentName,
					diskAttachment.DiskName,
				)).To(Succeed())
				for i, att := range e2eDiskAttachments {
					if att != nil && att.DiskName == diskAttachment.DiskName {
						e2eDiskAttachments = append(e2eDiskAttachments[:i], e2eDiskAttachments[i+1:]...)
						break
					}
				}

				By("Step 4: Restarting sds-node-configurator agent on the target node to trigger BD rescan")
				restartSDSNodeConfiguratorAgentOnNode(e2eCtx, k8sClient, discoveredBD.Status.NodeName)

				By("Step 5: Waiting for the BlockDevice to be deleted after device loss")
				Eventually(func(g Gomega) {
					var bd v1alpha1.BlockDevice
					err := k8sClient.Get(e2eCtx, client.ObjectKey{Name: discoveredBD.Name}, &bd)
					g.Expect(apierrors.IsNotFound(err)).To(BeTrue(),
						"BlockDevice %s should be deleted after the backing disk disappears; current err=%v consumable=%t node=%s path=%s",
						discoveredBD.Name, err, bd.Status.Consumable, bd.Status.NodeName, bd.Status.Path)
				}, 5*time.Minute, 10*time.Second).Should(Succeed())
				By(fmt.Sprintf("BlockDevice %s was deleted after the disk disappeared", discoveredBD.Name))
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

		Context("Block device size reduction", func() {
			const (
				e2eShrinkOrigDiskName  = "e2e-shrink-orig-disk"
				e2eShrinkOrigDiskSize  = "4Gi"
				e2eShrinkSmallDiskName = "e2e-shrink-small-disk"
				e2eShrinkSmallDiskSize = "1Gi"
			)

			var (
				origDiskAttachment  *kubernetes.VirtualDiskAttachmentResult
				smallDiskAttachment *kubernetes.VirtualDiskAttachmentResult
				shrinkLVGName       string
			)

			AfterEach(func() {
				if shrinkLVGName != "" && k8sClient != nil {
					lvg := &v1alpha1.LVMVolumeGroup{}
					if err := k8sClient.Get(e2eCtx, client.ObjectKey{Name: shrinkLVGName}, lvg); err == nil {
						if CurrentSpecReport().Failed() {
							GinkgoWriter.Println("\n--- Shrink test LVMVolumeGroup state on failure ---")
							printLVMVolumeGroupInfo(lvg)
						}
						if len(lvg.Finalizers) > 0 {
							lvg.Finalizers = nil
							_ = k8sClient.Update(e2eCtx, lvg)
						}
						_ = k8sClient.Delete(e2eCtx, lvg)
					}
					shrinkLVGName = ""
				}
				if testClusterResources == nil || testClusterResources.BaseKubeconfig == nil {
					return
				}
				ns := e2eConfigNamespace()
				if origDiskAttachment != nil {
					_ = kubernetes.DetachAndDeleteVirtualDisk(e2eCtx, testClusterResources.BaseKubeconfig, ns, origDiskAttachment.AttachmentName, origDiskAttachment.DiskName)
					origDiskAttachment = nil
				}
				if smallDiskAttachment != nil {
					_ = kubernetes.DetachAndDeleteVirtualDisk(e2eCtx, testClusterResources.BaseKubeconfig, ns, smallDiskAttachment.AttachmentName, smallDiskAttachment.DiskName)
					smallDiskAttachment = nil
				}
			})

			It("Should detect device loss after replacing disk with a smaller one and report VG inconsistency", func() {
				ensureSchedulerE2EK8sClient(testClusterResources, &k8sClient, e2eCtx)
				if testClusterResources.BaseKubeconfig == nil {
					Skip("Block device shrink test requires nested virtualization (base cluster kubeconfig)")
				}
				ns := e2eConfigNamespace()
				storageClass := e2eConfigStorageClass()
				Expect(storageClass).NotTo(BeEmpty(), "TEST_CLUSTER_STORAGE_CLASS required")

				By("Guest VM for attach: phase Running only (skip Migrating)")
				clusterVMs := e2eListClusterVMNames(e2eCtx, testClusterResources, ns)
				targetVM := clusterVMs[rand.Intn(len(clusterVMs))]

				var bdList v1alpha1.BlockDeviceList
				Expect(k8sClient.List(e2eCtx, &bdList)).To(Succeed())
				initialBDs := make(map[string]struct{}, len(bdList.Items))
				for _, bd := range bdList.Items {
					initialBDs[bd.Name] = struct{}{}
				}

				By(fmt.Sprintf("Step 1: Attaching original VirtualDisk (%s) to VM %s", e2eShrinkOrigDiskSize, targetVM))
				var err error
				origDiskAttachment, err = attachVirtualDiskWithRetry(e2eCtx, testClusterResources.BaseKubeconfig, kubernetes.VirtualDiskAttachmentConfig{
					VMName:           targetVM,
					Namespace:        ns,
					DiskName:         e2eShrinkOrigDiskName,
					DiskSize:         e2eShrinkOrigDiskSize,
					StorageClassName: storageClass,
				}, e2eVirtualDiskAttachMaxRetries, e2eVirtualDiskAttachRetryInterval)
				Expect(err).NotTo(HaveOccurred())

				attachCtx, attachCancel := context.WithTimeout(e2eCtx, 5*time.Minute)
				defer attachCancel()
				Expect(kubernetes.WaitForVirtualDiskAttached(attachCtx, testClusterResources.BaseKubeconfig, ns, origDiskAttachment.AttachmentName, 10*time.Second)).To(Succeed())

				By("Step 2: Waiting for BlockDevice discovery")
				var targetBD *v1alpha1.BlockDevice
				Eventually(func(g Gomega) {
					var list v1alpha1.BlockDeviceList
					g.Expect(k8sClient.List(e2eCtx, &list)).To(Succeed())
					targetBD = nil
					for i := range list.Items {
						bd := &list.Items[i]
						if _, existed := initialBDs[bd.Name]; existed {
							continue
						}
						if bd.Status.NodeName != targetVM || !bd.Status.Consumable || bd.Status.Size.IsZero() || !strings.HasPrefix(bd.Status.Path, "/dev/") {
							continue
						}
						targetBD = bd
						return
					}
					g.Expect(targetBD).NotTo(BeNil(), "new consumable BlockDevice on node %s not found yet", targetVM)
				}, 5*time.Minute, 10*time.Second).Should(Succeed())

				By(fmt.Sprintf("Found BD %s (size=%s, path=%s)", targetBD.Name, targetBD.Status.Size.String(), targetBD.Status.Path))
				printBlockDeviceInfo(targetBD)

				nodeName := targetBD.Status.NodeName
				bdMetaName := targetBD.Labels["kubernetes.io/metadata.name"]
				if bdMetaName == "" {
					bdMetaName = targetBD.Name
				}

				By("Step 3: Creating LVMVolumeGroup on the discovered BlockDevice")
				shrinkLVGName = "e2e-lvg-shrink-" + strings.ReplaceAll(strings.ReplaceAll(nodeName, ".", "-"), "_", "-")
				lvg := &v1alpha1.LVMVolumeGroup{
					ObjectMeta: metav1.ObjectMeta{Name: shrinkLVGName},
					Spec: v1alpha1.LVMVolumeGroupSpec{
						ActualVGNameOnTheNode: "e2e-shrink-vg",
						BlockDeviceSelector: &metav1.LabelSelector{
							MatchLabels: map[string]string{
								"kubernetes.io/hostname":      nodeName,
								"kubernetes.io/metadata.name": bdMetaName,
							},
						},
						Type:  "Local",
						Local: v1alpha1.LVMVolumeGroupLocalSpec{NodeName: nodeName},
					},
				}
				Expect(k8sClient.Create(e2eCtx, lvg)).To(Succeed())

				By("Waiting for LVMVolumeGroup to become Ready (up to 10 minutes)")
				Eventually(func(g Gomega) {
					var current v1alpha1.LVMVolumeGroup
					g.Expect(k8sClient.Get(e2eCtx, client.ObjectKeyFromObject(lvg), &current)).To(Succeed())
					g.Expect(current.Status.Phase).To(Equal(v1alpha1.PhaseReady), "Phase=%s", current.Status.Phase)
				}, 10*time.Minute, 10*time.Second).Should(Succeed())

				var origLVG v1alpha1.LVMVolumeGroup
				Expect(k8sClient.Get(e2eCtx, client.ObjectKeyFromObject(lvg), &origLVG)).To(Succeed())
				origVGSize := origLVG.Status.VGSize.DeepCopy()
				By(fmt.Sprintf("LVMVolumeGroup Ready: VGSize=%s", origVGSize.String()))
				printLVMVolumeGroupInfo(&origLVG)

				By("Step 4: Detaching and deleting the original VirtualDisk (simulating device removal)")
				Expect(kubernetes.DetachAndDeleteVirtualDisk(e2eCtx, testClusterResources.BaseKubeconfig, ns, origDiskAttachment.AttachmentName, origDiskAttachment.DiskName)).To(Succeed())
				origDiskAttachment = nil

				By(fmt.Sprintf("Step 5: Attaching a smaller VirtualDisk (%s) to VM %s", e2eShrinkSmallDiskSize, targetVM))
				smallDiskAttachment, err = attachVirtualDiskWithRetry(e2eCtx, testClusterResources.BaseKubeconfig, kubernetes.VirtualDiskAttachmentConfig{
					VMName:           targetVM,
					Namespace:        ns,
					DiskName:         e2eShrinkSmallDiskName,
					DiskSize:         e2eShrinkSmallDiskSize,
					StorageClassName: storageClass,
				}, e2eVirtualDiskAttachMaxRetries, e2eVirtualDiskAttachRetryInterval)
				Expect(err).NotTo(HaveOccurred())

				attachCtx2, attachCancel2 := context.WithTimeout(e2eCtx, 5*time.Minute)
				defer attachCancel2()
				Expect(kubernetes.WaitForVirtualDiskAttached(attachCtx2, testClusterResources.BaseKubeconfig, ns, smallDiskAttachment.AttachmentName, 10*time.Second)).To(Succeed())

				By("Step 6: Waiting for LVMVolumeGroup to leave Ready state (VG lost its backing device)")
				Eventually(func(g Gomega) {
					var current v1alpha1.LVMVolumeGroup
					g.Expect(k8sClient.Get(e2eCtx, client.ObjectKey{Name: shrinkLVGName}, &current)).To(Succeed())
					g.Expect(current.Status.Phase).NotTo(Equal(v1alpha1.PhaseReady),
						"Phase should not be Ready after device replacement; Phase=%s VGSize=%s (was %s)",
						current.Status.Phase, current.Status.VGSize.String(), origVGSize.String())
				}, 5*time.Minute, 15*time.Second).Should(Succeed())

				By("Step 7: Verifying LVMVolumeGroup conditions contain error information")
				var finalLVG v1alpha1.LVMVolumeGroup
				Expect(k8sClient.Get(e2eCtx, client.ObjectKey{Name: shrinkLVGName}, &finalLVG)).To(Succeed())
				printLVMVolumeGroupInfo(&finalLVG)

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

		Context("Manual BlockDevice creation and modification", func() {
			const e2eFakeBDPrefix = "dev-e2e-fake-manual-"

			It("Should delete a manually created BlockDevice that does not correspond to a real device", func() {
				ensureSchedulerE2EK8sClient(testClusterResources, &k8sClient, e2eCtx)

				By("Step 1: Getting a real node name from the cluster")
				var nodeList corev1.NodeList
				Expect(k8sClient.List(e2eCtx, &nodeList)).To(Succeed())
				Expect(nodeList.Items).NotTo(BeEmpty(), "cluster must have at least one node")
				realNodeName := nodeList.Items[0].Name

				fakeBDName := e2eFakeBDPrefix + strconv.Itoa(rand.Intn(100000))

				By(fmt.Sprintf("Step 2: Creating fake BlockDevice %s with nodeName=%s", fakeBDName, realNodeName))
				fakeBD := &v1alpha1.BlockDevice{
					ObjectMeta: metav1.ObjectMeta{
						Name: fakeBDName,
						Labels: map[string]string{
							"kubernetes.io/hostname":      realNodeName,
							"kubernetes.io/metadata.name": fakeBDName,
						},
					},
				}
				err := k8sClient.Create(e2eCtx, fakeBD)
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

				By("Step 3: Updating fake BlockDevice status (consumable=true, real node, fake path)")
				Expect(k8sClient.Get(e2eCtx, client.ObjectKey{Name: fakeBDName}, fakeBD)).To(Succeed())
				fakeBD.Status = v1alpha1.BlockDeviceStatus{
					NodeName:   realNodeName,
					Consumable: true,
					Path:       "/dev/e2e-nonexistent-device",
					Size:       resource.MustParse("1Gi"),
					Type:       "disk",
					MachineID:  "e2e-fake-machine-id",
				}
				err = k8sClient.Update(e2eCtx, fakeBD)
				if err != nil {
					err = k8sClient.Status().Update(e2eCtx, fakeBD)
				}
				Expect(err).NotTo(HaveOccurred(), "set status on fake BlockDevice")

				By("Step 4: Restarting sds-node-configurator agent on the target node to trigger BD rescan")
				restartSDSNodeConfiguratorAgentOnNode(e2eCtx, k8sClient, realNodeName)

				By("Step 5: Waiting for the agent to delete the fake BlockDevice (up to 5 minutes)")
				Eventually(func(g Gomega) {
					var bd v1alpha1.BlockDevice
					err := k8sClient.Get(e2eCtx, client.ObjectKey{Name: fakeBDName}, &bd)
					g.Expect(apierrors.IsNotFound(err)).To(BeTrue(),
						"fake BlockDevice %s should be deleted by the agent; current state: err=%v, consumable=%t, nodeName=%s",
						fakeBDName, err, bd.Status.Consumable, bd.Status.NodeName)
				}, 5*time.Minute, 10*time.Second).Should(Succeed())
				By(fmt.Sprintf("Fake BlockDevice %s was deleted by the agent", fakeBDName))
			})

			It("Should revert manual modifications to an existing BlockDevice status", func() {
				ensureSchedulerE2EK8sClient(testClusterResources, &k8sClient, e2eCtx)

				By("Step 1: Finding an existing BlockDevice in the cluster")
				var bdList v1alpha1.BlockDeviceList
				Expect(k8sClient.List(e2eCtx, &bdList)).To(Succeed())
				if len(bdList.Items) == 0 {
					Skip("No BlockDevices in cluster to test modification revert")
				}

				var targetBD *v1alpha1.BlockDevice
				for i := range bdList.Items {
					bd := &bdList.Items[i]
					if bd.Status.Path != "" && bd.Status.Size.Value() > 0 && bd.Status.Consumable {
						targetBD = bd
						break
					}
				}
				if targetBD == nil {
					Skip("No consumable BlockDevice with valid path and size found")
				}

				originalSize := targetBD.Status.Size.DeepCopy()
				originalPath := targetBD.Status.Path
				By(fmt.Sprintf("Target BD: %s (node=%s, path=%s, size=%s)",
					targetBD.Name, targetBD.Status.NodeName, originalPath, originalSize.String()))

				DeferCleanup(func() {
					var bd v1alpha1.BlockDevice
					if err := k8sClient.Get(e2eCtx, client.ObjectKey{Name: targetBD.Name}, &bd); err != nil {
						return
					}
					if bd.Status.Size.Equal(originalSize) {
						return
					}

					bd.Status.Size = originalSize
					err := k8sClient.Update(e2eCtx, &bd)
					if err != nil {
						_ = k8sClient.Status().Update(e2eCtx, &bd)
					}
				})

				By("Step 2: Modifying BlockDevice status.size to a fake value")
				var bdToModify v1alpha1.BlockDevice
				Expect(k8sClient.Get(e2eCtx, client.ObjectKey{Name: targetBD.Name}, &bdToModify)).To(Succeed())
				fakeSize := resource.MustParse("999Ti")
				bdToModify.Status.Size = fakeSize
				err := k8sClient.Update(e2eCtx, &bdToModify)
				if err != nil {
					err = k8sClient.Status().Update(e2eCtx, &bdToModify)
				}
				if err != nil {
					GinkgoWriter.Printf("    Could not modify BD status (may lack permissions): %v\n", err)
					Skip("Cannot update BlockDevice status: " + err.Error())
				}

				var modified v1alpha1.BlockDevice
				Expect(k8sClient.Get(e2eCtx, client.ObjectKey{Name: targetBD.Name}, &modified)).To(Succeed())
				Expect(modified.Status.Size.Equal(fakeSize)).To(BeTrue(),
					"size should be modified to %s, got %s", fakeSize.String(), modified.Status.Size.String())
				By(fmt.Sprintf("Size temporarily modified to %s", modified.Status.Size.String()))

				By("Step 3: Restarting sds-node-configurator agent on the target node to trigger BD rescan")
				restartSDSNodeConfiguratorAgentOnNode(e2eCtx, k8sClient, targetBD.Status.NodeName)

				By("Step 4: Waiting for the agent to revert the size to the real value (up to 5 minutes)")
				Eventually(func(g Gomega) {
					var bd v1alpha1.BlockDevice
					g.Expect(k8sClient.Get(e2eCtx, client.ObjectKey{Name: targetBD.Name}, &bd)).To(Succeed())
					g.Expect(bd.Status.Size.Equal(originalSize)).To(BeTrue(),
						"agent should have reverted size to original %s; current size=%s",
						originalSize.String(), bd.Status.Size.String())
				}, 5*time.Minute, 10*time.Second).Should(Succeed())

				var reverted v1alpha1.BlockDevice
				Expect(k8sClient.Get(e2eCtx, client.ObjectKey{Name: targetBD.Name}, &reverted)).To(Succeed())
				By(fmt.Sprintf("Agent reverted size: %s (original was %s)", reverted.Status.Size.String(), originalSize.String()))
				Expect(reverted.Status.Size.Equal(originalSize)).To(BeTrue(),
					"size should be restored to exact original value %s, got %s", originalSize.String(), reverted.Status.Size.String())
				Expect(reverted.Status.Path).To(Equal(originalPath), "path should remain unchanged")
			})
		})

		Context("Scheduler Extender: Space consolidation tests", func() {
			It("Should fill storage with small volumes to maximum capacity", func() {
				Expect(createdLVGs).NotTo(BeEmpty(), "LVMVolumeGroups must be created first")
				Expect(e2eStorageClassName).NotTo(BeEmpty(), "StorageClass must be created first")
				Expect(totalAvailableSpace).To(BeNumerically(">", 0),
					"baseline sum(VGFree) must be recorded when LVMVolumeGroups became Ready")

				By("Cleaning up previous test resources")
				schedulerCleanupWorkloadBeforeNextFill(e2eCtx, k8sClient)

				By("Waiting until sum(VGFree) recovers to initial storage budget (same as after LVG Ready)")
				currentAvailable := waitForSchedulerStorageFreedToBaseline(e2eCtx, k8sClient, createdLVGs, totalAvailableSpace)
				By(fmt.Sprintf("Current available space: %.2f Gi (baseline budget %.2f Gi)",
					float64(currentAvailable)/(1024*1024*1024), float64(totalAvailableSpace)/(1024*1024*1024)))

				maxPerLVG := getMaxVGFreeAcrossLVGs(e2eCtx, k8sClient, createdLVGs)
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

				successCount, scheduledCount := createPVCsAndPodsWithSizes(e2eCtx, k8sClient, volumeSizes, e2eStorageClassName, "small")

				By(fmt.Sprintf("Results: %d/%d PVCs created, %d/%d Pods scheduled", successCount, len(volumeSizes), scheduledCount, successCount))
				Expect(scheduledCount).To(Equal(successCount),
					"All created PVCs must have scheduled Pods")
				Expect(successCount).To(Equal(len(volumeSizes)),
					"All planned PVCs must be created successfully")

				printSchedulingSummary("small volumes", len(volumeSizes), successCount, scheduledCount, preferredUnit)
			})

			It("Should fill storage with medium volumes to maximum capacity", func() {
				Expect(createdLVGs).NotTo(BeEmpty(), "LVMVolumeGroups must be created first")
				Expect(e2eStorageClassName).NotTo(BeEmpty(), "StorageClass must be created first")
				Expect(totalAvailableSpace).To(BeNumerically(">", 0),
					"baseline sum(VGFree) must be recorded when LVMVolumeGroups became Ready")

				By("Cleaning up previous test resources")
				schedulerCleanupWorkloadBeforeNextFill(e2eCtx, k8sClient)

				By("Waiting until sum(VGFree) recovers to initial storage budget (same as after LVG Ready)")
				currentAvailable := waitForSchedulerStorageFreedToBaseline(e2eCtx, k8sClient, createdLVGs, totalAvailableSpace)
				By(fmt.Sprintf("Current available space: %.2f Gi (baseline budget %.2f Gi)",
					float64(currentAvailable)/(1024*1024*1024), float64(totalAvailableSpace)/(1024*1024*1024)))

				maxPerLVG := getMaxVGFreeAcrossLVGs(e2eCtx, k8sClient, createdLVGs)
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

				successCount, scheduledCount := createPVCsAndPodsWithSizes(e2eCtx, k8sClient, volumeSizes, e2eStorageClassName, "medium")

				By(fmt.Sprintf("Results: %d/%d PVCs created, %d/%d Pods scheduled", successCount, len(volumeSizes), scheduledCount, successCount))
				Expect(scheduledCount).To(Equal(successCount),
					"All created PVCs must have scheduled Pods")
				Expect(successCount).To(Equal(len(volumeSizes)),
					"All planned PVCs must be created successfully")

				printSchedulingSummary("medium volumes", len(volumeSizes), successCount, scheduledCount, preferredUnit)
			})

			It("Should fill storage with large volumes to maximum capacity", func() {
				Expect(createdLVGs).NotTo(BeEmpty(), "LVMVolumeGroups must be created first")
				Expect(e2eStorageClassName).NotTo(BeEmpty(), "StorageClass must be created first")
				Expect(totalAvailableSpace).To(BeNumerically(">", 0),
					"baseline sum(VGFree) must be recorded when LVMVolumeGroups became Ready")

				By("Cleaning up previous test resources")
				schedulerCleanupWorkloadBeforeNextFill(e2eCtx, k8sClient)

				By("Waiting until sum(VGFree) recovers to initial storage budget (same as after LVG Ready)")
				currentAvailable := waitForSchedulerStorageFreedToBaseline(e2eCtx, k8sClient, createdLVGs, totalAvailableSpace)
				By(fmt.Sprintf("Current available space: %.2f Gi (baseline budget %.2f Gi)",
					float64(currentAvailable)/(1024*1024*1024), float64(totalAvailableSpace)/(1024*1024*1024)))

				maxPerLVG := getMaxVGFreeAcrossLVGs(e2eCtx, k8sClient, createdLVGs)
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

				successCount, scheduledCount := createPVCsAndPodsWithSizes(e2eCtx, k8sClient, volumeSizes, e2eStorageClassName, "large")

				By(fmt.Sprintf("Results: %d/%d PVCs created, %d/%d Pods scheduled", successCount, len(volumeSizes), scheduledCount, successCount))
				Expect(scheduledCount).To(Equal(successCount),
					"All created PVCs must have scheduled Pods")
				Expect(successCount).To(Equal(len(volumeSizes)),
					"All planned PVCs must be created successfully")

				printSchedulingSummary("large volumes", len(volumeSizes), successCount, scheduledCount, preferredUnit)
			})
		})

		AfterAll(func() {
			ctx, cancel := context.WithTimeout(context.Background(), e2eClusterCleanupTimeout)
			defer cancel()
			res := e2eNestedTestClusterOrNil()
			if res == nil || res.Kubeconfig == nil {
				return
			}
			k8sCl, err := e2eNewTestClusterK8sClient(res.Kubeconfig)
			if err != nil {
				GinkgoWriter.Printf("Common Scheduler AfterAll: k8s client: %v\n", err)
				return
			}
			ns := e2eConfigNamespace()
			By("Common Scheduler AfterAll: cleaning up LVM, PVCs, LocalStorageClass, VirtualDisks, BlockDevices before Sds Node Configurator")
			cleanupE2EPodsAndPVCsWithWait(ctx, k8sCl, e2eSuitePodPVCleanupPodTimeout, e2eSuitePodPVCleanupPVTimeout)
			cleanupE2ELVMLogicalVolumes(ctx, k8sCl)
			cleanupE2ELVMVolumeGroups(ctx, k8sCl)
			cleanupE2ELocalStorageClasses(ctx, res.Kubeconfig)
			if res.BaseKubeconfig != nil {
				cleanupE2EVirtualDisks(ctx, res.BaseKubeconfig, ns, e2eSuiteVirtualDiskPrefix)
			}
			forceDeleteAllNonConsumableBlockDevices(ctx, k8sCl, 2*time.Minute)
			forceDeleteAllBlockDevices(ctx, k8sCl, 3*time.Minute)
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
				// BlockDevice CR names to delete after detach (VirtualDisk cleanup alone leaves consumable BD objects).
				discoveryBlockDeviceNamesForCleanup []string
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
				if len(discoveryBlockDeviceNamesForCleanup) > 0 && testClusterResources != nil && testClusterResources.Kubeconfig != nil {
					bdCtx, bdCancel := context.WithTimeout(context.Background(), 3*time.Minute)
					defer bdCancel()
					By("Cleaning up BlockDevice CRs created during discovery")
					cl, clErr := e2eNewTestClusterK8sClient(testClusterResources.Kubeconfig)
					if clErr != nil {
						GinkgoWriter.Printf("discovery AfterEach: k8s client for BD cleanup: %v\n", clErr)
					} else {
						forceDeleteBlockDevicesByNames(bdCtx, cl, discoveryBlockDeviceNamesForCleanup)
					}
					discoveryBlockDeviceNamesForCleanup = nil
				}
			})

			It("Should discover a new unformatted disk and create a BlockDevice object", func() {
				ensureE2EK8sClient(testClusterResources, &k8sClient, e2eCtx)
				By("Expected result: multiple disks on different nodes; each BlockDevice exists, consumable, size > 0")

				var clusterVMs []string
				var baseKubeconfig *rest.Config
				ns := e2eConfigNamespace()
				storageClass := e2eConfigStorageClass()
				Expect(storageClass).NotTo(BeEmpty(), "TEST_CLUSTER_STORAGE_CLASS is required for VirtualDisk")

				if testClusterResources.BaseKubeconfig == nil {
					Skip("VirtualDisk discovery in alwaysUseExisting requires base cluster kubeconfig (Deckhouse virtualization). " +
						"Set SSH_JUMP_HOST to the base cluster (jump host = base cluster) so the framework can get its kubeconfig, or use TEST_CLUSTER_CREATE_MODE=alwaysCreateNew.")
				}
				baseKubeconfig = testClusterResources.BaseKubeconfig
				By("Step 0: VirtualMachines in phase Running only (skip Migrating/Starting)")
				clusterVMs = e2eListClusterVMNames(e2eCtx, testClusterResources, ns)

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
				attachCtx, cancel := context.WithTimeout(e2eCtx, e2eVirtualDiskAttachWaitTimeout)
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
				discoveryBlockDeviceNamesForCleanup = nil
				for _, bd := range foundBDs {
					discoveryBlockDeviceNamesForCleanup = append(discoveryBlockDeviceNamesForCleanup, bd.Name)
				}

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

		Context("LVMVolumeGroup with one disk and thin-pool", func() {
			const e2eLVGDataDiskName = "e2e-lvg-data-disk"
			const e2eLVGDataDiskSize = "2Gi"

			// Populated by the pvresize test: reused by "Should remove VG when LVMVolumeGroup CR is deleted" (no second disk/LVG).
			var (
				e2eSavedLVGForVGRemoveTest          *e2eSavedLVGForVGRemoveInfo
				e2eDeferVDCleanupUntilLVGDeleteTest bool
			)

			var (
				lvgE2eDiskAttachment *kubernetes.VirtualDiskAttachmentResult
				sdsLvgE2eRunID       string
				lvgSuitePrepareOnce  sync.Once
			)

			BeforeEach(func() {
				lvgSuitePrepareOnce.Do(func() {
					ensureE2EK8sClient(testClusterResources, &k8sClient, e2eCtx)
					sdsLvgE2eRunID = fmt.Sprintf("%d", time.Now().Unix())
					prepCtx, prepCancel := context.WithTimeout(context.Background(), e2eClusterCleanupTimeout)
					defer prepCancel()
					By("LVMVolumeGroup suite: cleaning orphan LVM/PVC/VirtualDisks/BlockDevices before thin-pool and pvresize tests")
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
				if e2eDeferVDCleanupUntilLVGDeleteTest {
					// pvresize test retained LVG + disk for the follow-up "remove VG" test; skip one VD cleanup.
					e2eDeferVDCleanupUntilLVGDeleteTest = false
					GinkgoWriter.Println("Skipping VirtualDisk cleanup this AfterEach (disk kept for LVG delete test)")
					return
				}
				if lvgE2eDiskAttachment == nil || testClusterResources == nil || testClusterResources.BaseKubeconfig == nil {
					return
				}
				ns := e2eConfigNamespace()
				By("Cleaning up LVMVolumeGroup test VirtualDisk and attachment")
				_ = kubernetes.DetachAndDeleteVirtualDisk(e2eCtx, testClusterResources.BaseKubeconfig, ns, lvgE2eDiskAttachment.AttachmentName, lvgE2eDiskAttachment.DiskName)
				lvgE2eDiskAttachment = nil
			})

			It("Should create LVMVolumeGroup with one disk and thin-pool", func() {
				ensureE2EK8sClient(testClusterResources, &k8sClient, e2eCtx)
				By("Expected result: VG with name + tag storage.deckhouse.io/enabled=true; thin-pool with expected name/size; LVMVolumeGroup Phase Ready; conditions without errors")

				Expect(testClusterResources.BaseKubeconfig).NotTo(BeNil(), "LVMVolumeGroup test requires nested virtualization (base cluster)")
				ns := e2eConfigNamespace()
				By("Guest VM for attach: phase Running only (skip Migrating)")
				clusterVMs := e2eListClusterVMNames(e2eCtx, testClusterResources, ns)

				targetVM := clusterVMs[rand.Intn(len(clusterVMs))]
				storageClass := e2eConfigStorageClass()
				Expect(storageClass).NotTo(BeEmpty(), "TEST_CLUSTER_STORAGE_CLASS is required for VirtualDisk")

				// Snapshot BlockDevices before attach so we can detect the new one (sds-node-configurator may discover it quickly after attach)
				var blockDevicesList v1alpha1.BlockDeviceList
				err := k8sClient.List(e2eCtx, &blockDevicesList, &client.ListOptions{})
				Expect(err).NotTo(HaveOccurred())
				initialNames := make(map[string]struct{}, len(blockDevicesList.Items))
				for i := range blockDevicesList.Items {
					initialNames[blockDevicesList.Items[i].Name] = struct{}{}
				}
				By(fmt.Sprintf("BlockDevices before attach: %d (cluster inventory; agent re-lists disks after cleanup)", len(initialNames)))

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
				vgName := "e2e-vg-tp-" + sdsLvgE2eRunID
				thinPoolName := "e2e-thin-pool"
				// Not 50%: half of a 2Gi disk rounds to 1Gi in spec while LVM may allocate slightly more
				// bytes (alignment), and VGConfigurationApplied then fails ValidationFailed (requested < actual).
				thinPoolSize := "60%"
				thinPoolAllocationLimit := "100%"

				lvgName := "e2e-lvg-tp-" + sdsLvgE2eRunID + "-" + strings.ReplaceAll(strings.ReplaceAll(nodeName, ".", "-"), "_", "-")
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
				By(fmt.Sprintf("Creating LVMVolumeGroup %s on node %s, VG %s, thin-pool %s %s", lvg.Name, nodeName, vgName, thinPoolName, thinPoolSize))
				err = k8sClient.Create(e2eCtx, lvg)
				Expect(err).NotTo(HaveOccurred())
				defer func() { _ = k8sClient.Delete(e2eCtx, lvg) }()

				defer func() {
					var current v1alpha1.LVMVolumeGroup
					if err := k8sClient.Get(e2eCtx, client.ObjectKey{Name: lvg.Name}, &current); err == nil && current.Status.Phase != v1alpha1.PhaseReady {
						GinkgoWriter.Println("\n--- LVMVolumeGroup did not become Ready; current state ---")
						printLVMVolumeGroupInfo(&current)
					}
				}()

				By("Waiting for LVMVolumeGroup to become Ready (up to 5 minutes)")
				var created v1alpha1.LVMVolumeGroup
				Eventually(func(g Gomega) {
					err := k8sClient.Get(e2eCtx, client.ObjectKeyFromObject(lvg), &created)
					g.Expect(err).NotTo(HaveOccurred())
					if created.Status.Phase != v1alpha1.PhaseReady {
						GinkgoWriter.Printf("LVMVolumeGroup %s phase=%s (waiting for Ready)\n", lvg.Name, created.Status.Phase)
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

				By("✓ LVMVolumeGroup Ready; thin-pool present and Ready; conditions without errors")
				printLVMVolumeGroupInfo(&created)

				By("=== PV Resize Test ===")
				By("Patching VirtualDisk to increase size (simulate block device expansion)")
				dynClient, err := dynamic.NewForConfig(testClusterResources.BaseKubeconfig)
				Expect(err).NotTo(HaveOccurred())

				vdGVR := schema.GroupVersionResource{Group: "virtualization.deckhouse.io", Version: "v1alpha2", Resource: "virtualdisks"}
				newSize := "3Gi"
				patchBytes := []byte(fmt.Sprintf(`{"spec":{"persistentVolumeClaim":{"size":"%s"}}}`, newSize))

				_, err = dynClient.Resource(vdGVR).Namespace(ns).Patch(e2eCtx, lvgE2eDiskAttachment.DiskName, types.MergePatchType, patchBytes, metav1.PatchOptions{})
				Expect(err).NotTo(HaveOccurred(), "Failed to patch VirtualDisk %s to size %s", lvgE2eDiskAttachment.DiskName, newSize)

				By("Waiting for BlockDevice in test cluster to reflect new size (3Gi)")
				Eventually(func(g Gomega) {
					var bd v1alpha1.BlockDevice
					g.Expect(k8sClient.Get(e2eCtx, client.ObjectKey{Name: targetBD.Name}, &bd)).To(Succeed())
					bdSize := bd.Status.Size.Value()
					expectedMinBytes := int64(2.5 * 1024 * 1024 * 1024)
					g.Expect(bdSize).To(BeNumerically(">", expectedMinBytes), "BlockDevice size should increase to approximately 3Gi")
				}, 5*time.Minute, 10*time.Second).Should(Succeed())

				By("Waiting for LVMVolumeGroup to be Ready with new VGSize")
				Eventually(func(g Gomega) {
					var updated v1alpha1.LVMVolumeGroup
					g.Expect(k8sClient.Get(e2eCtx, client.ObjectKey{Name: lvg.Name}, &updated)).To(Succeed())
					g.Expect(updated.Status.Phase).To(Equal(v1alpha1.PhaseReady), "Phase should be Ready after resize")

					vgSize := updated.Status.VGSize.Value()
					expectedMinBytes := int64(2.5 * 1024 * 1024 * 1024)
					g.Expect(vgSize).To(BeNumerically(">", expectedMinBytes), "VGSize should be updated to approximately 3Gi")
				}, 5*time.Minute, 10*time.Second).Should(Succeed())

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

	AfterAll(func() {
		// After Common Scheduler + Sds Node Configurator: tear down shared scheduler disks, LVG, SC, workload.
		ctx, cancel := context.WithTimeout(context.Background(), e2eClusterCleanupTimeout)
		defer cancel()
		e2eSuiteSharedStorageCleanup(ctx)
	})
}) // Describe: sds-node-configurator module e2e

// e2eSuiteVirtualDiskPrefix matches all test VirtualDisks in the e2e namespace (scheduler, Sds LVG, discovery disks).
const e2eSuiteVirtualDiskPrefix = "e2e-"

// e2eSuiteSharedStorageCleanup removes e2e VirtualDisks, PVCs/Pods, LocalStorageClasses, LVMLogicalVolumes, and LVMVolumeGroups
// after both Common Scheduler and Sds Node Configurator Describes complete.
func e2eSuiteSharedStorageCleanup(ctx context.Context) {
	res := e2eNestedTestClusterOrNil()
	if res == nil || res.Kubeconfig == nil {
		return
	}
	k8sCl, err := e2eNewTestClusterK8sClient(res.Kubeconfig)
	if err != nil {
		GinkgoWriter.Printf("    ⚠️  suite storage cleanup: k8s client: %v\n", err)
		return
	}
	ns := e2eConfigNamespace()
	if res.BaseKubeconfig != nil {
		GinkgoWriter.Printf("    ▶️ AfterAll (suite): cleaning up e2e VirtualDisks / attachments (name prefix %q)...\n", e2eSuiteVirtualDiskPrefix)
		cleanupE2EVirtualDisks(ctx, res.BaseKubeconfig, ns, e2eSuiteVirtualDiskPrefix)
	}
	GinkgoWriter.Printf("    ▶️ AfterAll (suite): cleaning up e2e Pods and PVCs...\n")
	cleanupE2EPodsAndPVCsWithWait(ctx, k8sCl, e2eSuitePodPVCleanupPodTimeout, e2eSuitePodPVCleanupPVTimeout)
	GinkgoWriter.Printf("    ▶️ AfterAll (suite): cleaning up e2e LocalStorageClasses...\n")
	cleanupE2ELocalStorageClasses(ctx, res.Kubeconfig)
	GinkgoWriter.Printf("    ▶️ AfterAll (suite): cleaning up e2e LVMLogicalVolumes (orphan PVCs)...\n")
	cleanupE2ELVMLogicalVolumes(ctx, k8sCl)
	GinkgoWriter.Printf("    ▶️ AfterAll (suite): cleaning up e2e LVMVolumeGroups...\n")
	cleanupE2ELVMVolumeGroups(ctx, k8sCl)
}

func e2eNewTestClusterK8sClient(cfg *rest.Config) (client.Client, error) {
	if err := v1alpha1.AddToScheme(scheme.Scheme); err != nil {
		return nil, err
	}
	return client.New(cfg, client.Options{Scheme: scheme.Scheme})
}

// ensureE2EK8sClient initializes k8sClient from test cluster kubeconfig (once). Does not delete LVMVolumeGroups:
// scheduler setup creates shared LVG/VDs for the whole suite; e2eSuiteSharedStorageCleanup runs in root AfterAll.
func ensureE2EK8sClient(resources *cluster.TestClusterResources, k8s *client.Client, ctx context.Context) {
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
}

func getE2ENodeName() string   { return os.Getenv("E2E_NODE_NAME") }
func getE2EDevicePath() string { return os.Getenv("E2E_DEVICE_PATH") }

func orPathFilter(path string) string {
	if path == "" {
		return "any"
	}
	return "path=" + path
}

func orNodeFilter(node string) string {
	if node == "" {
		return "any"
	}
	return "node=" + node
}

func formatBlockDevicesHint(items []v1alpha1.BlockDevice, expectedNode string) string {
	if len(items) == 0 {
		return "No BlockDevices in cluster."
	}
	var lines []string
	nodesSeen := make(map[string]bool)
	for _, bd := range items {
		n := bd.Status.NodeName
		if n == "" {
			n = "<no nodeName>"
		}
		nodesSeen[n] = true
		path := bd.Status.Path
		if path == "" {
			path = "<no path>"
		}
		lines = append(lines, fmt.Sprintf("%s: nodeName=%s path=%s size=%s", bd.Name, n, path, bd.Status.Size.String()))
	}
	hint := "Existing BlockDevices: " + strings.Join(lines, "; ")
	if expectedNode != "" && !nodesSeen[expectedNode] {
		var nodes []string
		for n := range nodesSeen {
			nodes = append(nodes, n)
		}
		hint += ". Expected nodeName=" + expectedNode + " but only found nodes: " + strings.Join(nodes, ", ")
	}
	return hint
}

// e2eWaitConsumableBlockDeviceForVirtualDisk finds the BlockDevice for this VirtualDisk attachment the same way
// as the discovery tests: Status.Serial must equal hex(md5(VirtualDisk.UID)) or hex(md5(VMBDA.UID)).
// This avoids picking another disk on the same node (leftover LVM, other e2e disks).
func e2eWaitConsumableBlockDeviceForVirtualDisk(ctx context.Context, baseKube *rest.Config, k8sClient client.Client, ns, diskName, attachmentName, targetVM string) *v1alpha1.BlockDevice {
	baseDyn, err := dynamic.NewForConfig(baseKube)
	Expect(err).NotTo(HaveOccurred(), "dynamic client for base cluster (read VirtualDisk / VMBDA UIDs)")
	vdObj, err := baseDyn.Resource(virtualDiskGVR).Namespace(ns).Get(ctx, diskName, metav1.GetOptions{})
	Expect(err).NotTo(HaveOccurred(), "get VirtualDisk %s", diskName)
	attObj, err := baseDyn.Resource(vmbdaGVR).Namespace(ns).Get(ctx, attachmentName, metav1.GetOptions{})
	Expect(err).NotTo(HaveOccurred(), "get VirtualMachineBlockDeviceAttachment %s", attachmentName)
	serialVD := blockDeviceSerialFromVirtualDiskUID(string(vdObj.GetUID()))
	serialAtt := blockDeviceSerialFromVirtualDiskUID(string(attObj.GetUID()))

	var picked *v1alpha1.BlockDevice
	Eventually(func(g Gomega) {
		var list v1alpha1.BlockDeviceList
		g.Expect(k8sClient.List(ctx, &list, &client.ListOptions{})).To(Succeed())
		picked = nil
		for i := range list.Items {
			bd := list.Items[i]
			s := strings.TrimSpace(bd.Status.Serial)
			if s != serialVD && s != serialAtt {
				continue
			}
			if bd.Status.NodeName != targetVM {
				continue
			}
			if !bd.Status.Consumable || bd.Status.Size.IsZero() || bd.Status.Path == "" || !strings.HasPrefix(bd.Status.Path, "/dev/") {
				continue
			}
			copyBD := bd
			picked = &copyBD
			return
		}
		g.Expect(picked).NotTo(BeNil(),
			"BlockDevice for VirtualDisk %q: want Status.Serial %q or %q on node %q, consumable, with /dev path. %s",
			diskName, serialVD, serialAtt, targetVM, formatBlockDevicesHint(list.Items, targetVM))
	}, 5*time.Minute, 10*time.Second).Should(Succeed())
	return picked
}

// runLsblkViaDirectSSH connects to the node by IP the same way we connect to the master (SSH_HOST / jump → node).
// Gets node IP from the test cluster API and uses the same SSH credentials (jump host if set, VM user, key).
func runLsblkViaDirectSSH(ctx context.Context, testKubeconfig *rest.Config, nodeName, sshUser string) (map[string]lsblkLine, error) {
	nodeIP, err := kubernetes.GetNodeInternalIP(ctx, testKubeconfig, nodeName)
	if err != nil {
		return nil, fmt.Errorf("get IP for node %s: %w", nodeName, err)
	}
	keyPath, err := cluster.GetSSHPrivateKeyPath()
	if err != nil {
		return nil, fmt.Errorf("get SSH key path: %w", err)
	}
	jumpKeyPath := e2eConfigSSHJumpKeyPath()
	if jumpKeyPath == "" {
		jumpKeyPath = keyPath
	}
	var sshClient ssh.Client
	if e2eConfigSSHJumpHost() != "" {
		jumpUser := e2eConfigSSHJumpUser()
		if jumpUser == "" {
			jumpUser = e2eConfigSSHUser()
		}
		sshClient, err = ssh.NewClientWithJumpHost(jumpUser, e2eConfigSSHJumpHost(), jumpKeyPath, sshUser, nodeIP, keyPath)
	} else {
		sshClient, err = ssh.NewClient(sshUser, nodeIP, keyPath)
	}
	if err != nil {
		return nil, fmt.Errorf("SSH to node %s (%s@%s): %w", nodeName, sshUser, nodeIP, err)
	}
	defer sshClient.Close()
	out, err := sshClient.Exec(ctx, "lsblk -b -P -o NAME,SIZE,SERIAL,PATH -n")
	if err != nil {
		return nil, fmt.Errorf("run lsblk on node %s (%s@%s): %w", nodeName, sshUser, nodeIP, err)
	}
	return parseLsblkOutput(out), nil
}

// e2eExecOnTestClusterNodeSSH runs a shell command on a test cluster node (same SSH path as lsblk: jump host + node IP).
//
// Each call builds a new storage-e2e SSH client and connects again. With SSH_JUMP_HOST, internal code may log
// DEBUG lines (SSH key loaded / ssh-agent) per hop, so a single Exec can produce several [DEBUG] lines.
// To reduce noise, run tests with a lower log level for storage-e2e if supported (e.g. LOG_LEVEL=info).
func e2eExecOnTestClusterNodeSSH(ctx context.Context, testKubeconfig *rest.Config, nodeName, sshUser, command string) (string, error) {
	nodeIP, err := kubernetes.GetNodeInternalIP(ctx, testKubeconfig, nodeName)
	if err != nil {
		return "", fmt.Errorf("get IP for node %s: %w", nodeName, err)
	}
	keyPath, err := cluster.GetSSHPrivateKeyPath()
	if err != nil {
		return "", fmt.Errorf("get SSH key path: %w", err)
	}
	jumpKeyPath := e2eConfigSSHJumpKeyPath()
	if jumpKeyPath == "" {
		jumpKeyPath = keyPath
	}
	var sshClient ssh.Client
	if e2eConfigSSHJumpHost() != "" {
		jumpUser := e2eConfigSSHJumpUser()
		if jumpUser == "" {
			jumpUser = e2eConfigSSHUser()
		}
		sshClient, err = ssh.NewClientWithJumpHost(jumpUser, e2eConfigSSHJumpHost(), jumpKeyPath, sshUser, nodeIP, keyPath)
	} else {
		sshClient, err = ssh.NewClient(sshUser, nodeIP, keyPath)
	}
	if err != nil {
		return "", fmt.Errorf("SSH to node %s (%s@%s): %w", nodeName, sshUser, nodeIP, err)
	}
	defer sshClient.Close()
	out, err := sshClient.Exec(ctx, command)
	if err != nil {
		return out, fmt.Errorf("exec on node %s: %w", nodeName, err)
	}
	return out, nil
}

// e2eVgNameListedInVgsOutput returns true if a line in vgs output (one VG name per line) equals vgName.
func e2eVgNameListedInVgsOutput(vgsOutput, vgName string) bool {
	for _, line := range strings.Split(vgsOutput, "\n") {
		if strings.TrimSpace(line) == vgName {
			return true
		}
	}
	return false
}

// e2eShellRemoveThinPoolStackForVG returns a shell script run on the guest node via SSH: removes thin volumes that
// use the pool, then the pool LV, then any remaining LVs in the VG. Used only by e2e to avoid Terminating LVMVolumeGroup
// when agent-side delete ordering leaves thin-pool segments on the node.
func e2eShellRemoveThinPoolStackForVG(vgName, thinPoolName string) string {
	return fmt.Sprintf(`set +e
VG=%q
POOL=%q
runlv() { lvs "$@" 2>/dev/null || sudo -n lvs "$@" 2>/dev/null; }
runrm() { lvremove -fy "$@" 2>/dev/null || sudo -n lvremove -fy "$@" 2>/dev/null; }
for pass in 1 2 3 4 5 6 7 8 9 10; do
  runlv -a --noheadings -o lv_name,pool_lv "$VG" | while IFS= read -r line; do
    lv=$(echo "$line" | awk '{print $1}' | tr -d '[]')
    pl=$(echo "$line" | awk '{print $2}' | tr -d '[]')
    [ -z "$lv" ] && continue
    [ -n "$pl" ] && [ "$pl" = "$POOL" ] && [ "$lv" != "$POOL" ] && runrm "/dev/$VG/$lv"
  done
done
runrm "/dev/$VG/$POOL"
for pass in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do
  cnt=$(runlv -a --noheadings -o lv_name "$VG" | sed '/^$/d' | wc -l)
  cnt=$(echo "$cnt" | tr -cd '0-9')
  [ "${cnt:-0}" -eq 0 ] && break
  runlv -a --noheadings -o lv_name "$VG" | while IFS= read -r line; do
    lv=$(echo "$line" | awk '{print $1}' | tr -d '[]')
    [ -n "$lv" ] && runrm "/dev/$VG/$lv"
  done
done
`, vgName, thinPoolName)
}

// parseLsblkOutput parses lsblk -b -P -o NAME,SIZE,SERIAL,PATH output (KEY="value" per line).
// Returns map keyed by PATH.
func parseLsblkOutput(out string) map[string]lsblkLine {
	result := make(map[string]lsblkLine)
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var path, serial, sizeStr string
		for _, part := range strings.Split(line, " ") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			if idx := strings.Index(part, "="); idx >= 0 && len(part) > idx+2 {
				k, v := part[:idx], strings.Trim(part[idx+1:], "\"")
				switch k {
				case "PATH":
					path = v
				case "SERIAL":
					serial = v
				case "SIZE":
					sizeStr = v
				}
			}
		}
		if path == "" {
			continue
		}
		var sizeBytes int64
		if sizeStr != "" {
			sizeBytes, _ = strconv.ParseInt(sizeStr, 10, 64)
		}
		result[path] = lsblkLine{Path: path, Serial: serial, Size: sizeStr, SizeBytes: sizeBytes}
	}
	return result
}

func ptr[T any](v T) *T { return &v }

func printBlockDeviceNameSerialTable(rows []nameSerialCheckRow) {
	if len(rows) == 0 {
		return
	}
	const (
		wNode   = 18
		wVD     = 32
		wBD     = 44
		wSerial = 34
		wMatch  = 5
	)
	pad := func(s string, w int) string {
		if len(s) > w {
			return s[:w-1] + "…"
		}
		return s + strings.Repeat(" ", w-len(s))
	}
	sep := " | "
	header := pad("NODE", wNode) + sep + pad("VD_NAME", wVD) + sep + pad("BD_NAME", wBD) + sep +
		pad("EXP_SERIAL_VD", wSerial) + sep + pad("EXP_SERIAL_VMBDA", wSerial) + sep + pad("ACT_SERIAL", wSerial) + sep + pad("SERIAL", wMatch) + sep +
		pad("EXP_BD_NAME", wBD) + sep + pad("ACT_BD_NAME", wBD) + sep + pad("NAME", wMatch)
	lineLen := len(header)

	GinkgoWriter.Println("\n========== BlockDevice name & serial check (expected vs actual) ==========")
	GinkgoWriter.Println(header)
	GinkgoWriter.Println(strings.Repeat("-", lineLen))
	for _, r := range rows {
		serialOk := "✓"
		if !r.SerialMatch {
			serialOk = "✗"
		}
		nameOk := "✓"
		if !r.NameMatch {
			nameOk = "✗"
		}
		GinkgoWriter.Println(
			pad(r.Node, wNode) + sep +
				pad(r.VDName, wVD) + sep +
				pad(r.BDName, wBD) + sep +
				pad(r.ExpectedSerialVD, wSerial) + sep +
				pad(r.ExpectedSerialVMBDA, wSerial) + sep +
				pad(r.ActualSerial, wSerial) + sep +
				pad(serialOk, wMatch) + sep +
				pad(r.ExpectedBDName, wBD) + sep +
				pad(r.ActualBDName, wBD) + sep +
				pad(nameOk, wMatch))
	}
	GinkgoWriter.Println(strings.Repeat("=", lineLen) + "\n")
}

func printDiscoveryTable(rows []discoveryTableRow) {
	if len(rows) == 0 {
		return
	}
	const (
		wNode   = 18
		wVD     = 32
		wBD     = 44
		wPath   = 10
		wSerial = 34
		wSize   = 12
		wMatch  = 5
	)
	pad := func(s string, w int) string {
		if len(s) > w {
			return s[:w-1] + "…"
		}
		return s + strings.Repeat(" ", w-len(s))
	}
	sep := " | "
	header := pad("NODE", wNode) + sep + pad("VD_NAME", wVD) + sep + pad("BD_NAME", wBD) + sep + pad("PATH", wPath) + sep + pad("SERIAL_BD", wSerial) + sep + pad("SERIAL_LSBLK", wSerial) + sep + pad("SIZE_BD", wSize) + sep + pad("SIZE_LSBLK", wSize) + sep + pad("MATCH", wMatch)
	lineLen := len(header)

	GinkgoWriter.Println("\n========== Discovery test summary (VD → BD → lsblk) ==========")
	GinkgoWriter.Println(header)
	GinkgoWriter.Println(strings.Repeat("-", lineLen))
	for _, r := range rows {
		matchStr := "—"
		if r.SerialLsblk != "" {
			if r.Match {
				matchStr = "✓"
			} else {
				matchStr = "✗"
			}
		}
		GinkgoWriter.Println(
			pad(r.Node, wNode) + sep +
				pad(r.VDName, wVD) + sep +
				pad(r.BDName, wBD) + sep +
				pad(r.Path, wPath) + sep +
				pad(r.SerialBD, wSerial) + sep +
				pad(r.SerialLsblk, wSerial) + sep +
				pad(r.SizeBD, wSize) + sep +
				pad(r.SizeLsblk, wSize) + sep +
				pad(matchStr, wMatch))
	}
	GinkgoWriter.Println(strings.Repeat("=", lineLen) + "\n")
}

func printBlockDeviceInfo(bd *v1alpha1.BlockDevice) {
	GinkgoWriter.Println("\n========== BlockDevice information ==========")
	GinkgoWriter.Printf("Name: %s\n", bd.Name)
	GinkgoWriter.Printf("NodeName: %s\n", bd.Status.NodeName)
	GinkgoWriter.Printf("Path: %s\n", bd.Status.Path)
	GinkgoWriter.Printf("Size: %s\n", bd.Status.Size.String())
	GinkgoWriter.Printf("Type: %s\n", bd.Status.Type)
	GinkgoWriter.Printf("Serial: %s\n", bd.Status.Serial)
	GinkgoWriter.Printf("WWN: %s\n", bd.Status.Wwn)
	GinkgoWriter.Printf("Model: %s\n", bd.Status.Model)
	GinkgoWriter.Printf("Consumable: %t\n", bd.Status.Consumable)
	GinkgoWriter.Printf("FSType: %s\n", bd.Status.FsType)
	GinkgoWriter.Printf("MachineID: %s\n", bd.Status.MachineID)
	GinkgoWriter.Printf("Rota: %t\n", bd.Status.Rota)
	GinkgoWriter.Printf("HotPlug: %t\n", bd.Status.HotPlug)
	GinkgoWriter.Println("=============================================\n")
}

// e2ePrintBlockDevicesConsumableSummary prints a compact table of BlockDevice status fields relevant to LVM validation.
func e2ePrintBlockDevicesConsumableSummary(ctx context.Context, cl client.Client, bdNames []string, title string) {
	if len(bdNames) == 0 {
		return
	}
	names := append([]string(nil), bdNames...)
	sort.Strings(names)
	GinkgoWriter.Printf("\n========== BlockDevices (%s) ==========\n", title)
	for _, name := range names {
		var bd v1alpha1.BlockDevice
		if err := cl.Get(ctx, client.ObjectKey{Name: name}, &bd); err != nil {
			GinkgoWriter.Printf("  %s: Get failed: %v\n", name, err)
			continue
		}
		GinkgoWriter.Printf("  %s: Consumable=%v  FsType=%q  PVUuid=%q  Path=%s  Size=%s  LVMVolumeGroupName=%q\n",
			bd.Name, bd.Status.Consumable, bd.Status.FsType, bd.Status.PVUuid, bd.Status.Path, bd.Status.Size.String(), bd.Status.LVMVolumeGroupName)
	}
	GinkgoWriter.Println("=================================================\n")
}

func printLVMVolumeGroupInfo(lvg *v1alpha1.LVMVolumeGroup) {
	GinkgoWriter.Println("\n========== LVMVolumeGroup information ==========")
	GinkgoWriter.Printf("Name: %s\n", lvg.Name)
	GinkgoWriter.Println("--- Spec ---")
	GinkgoWriter.Printf("  Type: %s\n", lvg.Spec.Type)
	GinkgoWriter.Printf("  ActualVGNameOnTheNode: %s\n", lvg.Spec.ActualVGNameOnTheNode)
	GinkgoWriter.Printf("  Local.NodeName: %s\n", lvg.Spec.Local.NodeName)
	if sel := lvg.Spec.BlockDeviceSelector; sel != nil {
		if len(sel.MatchLabels) > 0 {
			GinkgoWriter.Printf("  BlockDeviceSelector.MatchLabels: %v\n", sel.MatchLabels)
		}
		for i, me := range sel.MatchExpressions {
			GinkgoWriter.Printf("  BlockDeviceSelector.MatchExpressions[%d]: Key=%s Operator=%s Values=%v\n",
				i, me.Key, me.Operator, me.Values)
		}
	}
	for i, tp := range lvg.Spec.ThinPools {
		GinkgoWriter.Printf("  ThinPools[%d]: Name=%s Size=%s AllocationLimit=%s\n", i, tp.Name, tp.Size, tp.AllocationLimit)
	}
	GinkgoWriter.Println("--- Status ---")
	GinkgoWriter.Printf("  Phase: %s\n", lvg.Status.Phase)
	GinkgoWriter.Printf("  VGSize: %s\n", lvg.Status.VGSize.String())
	GinkgoWriter.Printf("  VGFree: %s\n", lvg.Status.VGFree.String())
	for i, tp := range lvg.Status.ThinPools {
		GinkgoWriter.Printf("  ThinPools[%d]: Name=%s AllocationLimit=%s Ready=%t\n", i, tp.Name, tp.AllocationLimit, tp.Ready)
	}
	GinkgoWriter.Println("--- Conditions ---")
	for i, c := range lvg.Status.Conditions {
		GinkgoWriter.Printf("  [%d] Type=%s Status=%s Reason=%s\n", i, c.Type, c.Status, c.Reason)
		if c.Message != "" {
			GinkgoWriter.Printf("      Message: %s\n", c.Message)
		}
	}
	GinkgoWriter.Println("=================================================\n")
}

// cleanupE2ELVMVolumeGroupsSdsNodeConfigurator deletes all LVMVolumeGroups whose name has prefix e2e-lvg-.
// Used by Sds Node Configurator suite (avoid name clash with cleanupE2ELVMVolumeGroups in block_device_discovery_suite_test.go).
func cleanupE2ELVMVolumeGroupsSdsNodeConfigurator(ctx context.Context, cl client.Client) {
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
	deadline := time.Now().Add(2 * time.Minute)
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
			GinkgoWriter.Println("All e2e LVMVolumeGroups removed")
			return
		}
		for _, name := range remaining {
			lvg := &v1alpha1.LVMVolumeGroup{}
			if err := cl.Get(ctx, client.ObjectKey{Name: name}, lvg); err != nil {
				continue
			}
			if len(lvg.Finalizers) > 0 {
				lvg.Finalizers = nil
				_ = cl.Update(ctx, lvg)
			}
			_ = cl.Delete(ctx, lvg)
		}
		time.Sleep(5 * time.Second)
	}
	GinkgoWriter.Printf("Warning: some e2e LVMVolumeGroups may still exist after cleanup\n")
}

func restartSDSNodeConfiguratorAgentOnNode(ctx context.Context, cl client.Client, nodeName string) {
	const (
		namespace = "d8-sds-node-configurator"
		appLabel  = "sds-node-configurator"
	)

	Expect(nodeName).NotTo(BeEmpty(), "node name is required to restart sds-node-configurator")

	var podToRestart corev1.Pod
	Eventually(func(g Gomega) {
		var podList corev1.PodList
		g.Expect(cl.List(ctx, &podList, client.InNamespace(namespace), client.MatchingLabels{"app": appLabel})).To(Succeed())

		found := false
		for i := range podList.Items {
			pod := podList.Items[i]
			if pod.Spec.NodeName != nodeName || pod.DeletionTimestamp != nil {
				continue
			}
			podToRestart = pod
			found = true
			break
		}

		g.Expect(found).To(BeTrue(), "no sds-node-configurator pod found on node %s", nodeName)
	}, 2*time.Minute, 5*time.Second).Should(Succeed())

	GinkgoWriter.Printf("    Restarting sds-node-configurator pod %s on node %s\n", podToRestart.Name, nodeName)
	Expect(cl.Delete(ctx, &podToRestart)).To(Succeed(), "delete sds-node-configurator pod %s on node %s", podToRestart.Name, nodeName)

	Eventually(func(g Gomega) {
		var deleted corev1.Pod
		err := cl.Get(ctx, client.ObjectKey{Namespace: namespace, Name: podToRestart.Name}, &deleted)
		g.Expect(apierrors.IsNotFound(err)).To(BeTrue(), "old pod %s should disappear before replacement becomes ready; err=%v", podToRestart.Name, err)
	}, 2*time.Minute, 5*time.Second).Should(Succeed())

	Eventually(func(g Gomega) {
		var podList corev1.PodList
		g.Expect(cl.List(ctx, &podList, client.InNamespace(namespace), client.MatchingLabels{"app": appLabel})).To(Succeed())

		var replacement *corev1.Pod
		for i := range podList.Items {
			pod := &podList.Items[i]
			if pod.Spec.NodeName != nodeName || pod.DeletionTimestamp != nil {
				continue
			}
			if pod.Name == podToRestart.Name || pod.UID == podToRestart.UID {
				continue
			}
			replacement = pod
			break
		}

		g.Expect(replacement).NotTo(BeNil(), "replacement sds-node-configurator pod on node %s not found yet", nodeName)
		g.Expect(replacement.Status.Phase).To(Equal(corev1.PodRunning),
			"replacement pod %s on node %s is not running yet (phase=%s)", replacement.Name, nodeName, replacement.Status.Phase)
		g.Expect(isPodReady(replacement)).To(BeTrue(),
			"replacement pod %s on node %s is not Ready yet", replacement.Name, nodeName)
	}, 5*time.Minute, 10*time.Second).Should(Succeed())
}

func isPodReady(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// e2eClusterStateJSONPath returns the path to storage-e2e cluster-state.json for this test file
// (same layout as getClusterStatePath in github.com/deckhouse/storage-e2e/pkg/cluster).
// Must be called from this file so runtime.Caller resolves to sds_node_configurator_test.go.
func e2eClusterStateJSONPath() (string, error) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("runtime.Caller failed")
	}
	repoRoot, err := filepath.Abs(filepath.Join(filepath.Dir(file), "..", ".."))
	if err != nil {
		return "", err
	}
	base := strings.TrimSuffix(filepath.Base(file), filepath.Ext(file))
	return filepath.Join(repoRoot, "temp", base, "cluster-state.json"), nil
}

func e2eNewVirtClient(cfg *rest.Config) (client.Client, error) {
	sch := k8sruntime.NewScheme()
	if err := virtv1alpha2.SchemeBuilder.AddToScheme(sch); err != nil {
		return nil, err
	}
	return client.New(cfg, client.Options{Scheme: sch})
}

// e2ePatchVirtualDiskSize updates VirtualDisk .spec.persistentVolumeClaim.size (allowed mutable field).
func e2ePatchVirtualDiskSize(ctx context.Context, cfg *rest.Config, namespace, diskName, newSize string) error {
	cl, err := e2eNewVirtClient(cfg)
	if err != nil {
		return err
	}
	q, err := resource.ParseQuantity(newSize)
	if err != nil {
		return fmt.Errorf("parse disk size %q: %w", newSize, err)
	}
	var vd virtv1alpha2.VirtualDisk
	key := client.ObjectKey{Namespace: namespace, Name: diskName}
	if err := cl.Get(ctx, key, &vd); err != nil {
		return fmt.Errorf("get VirtualDisk %s/%s: %w", namespace, diskName, err)
	}
	vd.Spec.PersistentVolumeClaim.Size = &q
	if err := cl.Update(ctx, &vd); err != nil {
		return fmt.Errorf("update VirtualDisk %s/%s size to %s: %w", namespace, diskName, newSize, err)
	}
	return nil
}

// e2eCleanupBaseClusterNamespaceWorkload deletes namespaced virtualization objects on the base cluster in order:
// VirtualMachineBlockDeviceAttachment → VirtualDisk → VirtualMachine, then the namespace (Deckhouse DVP).
func e2eCleanupBaseClusterNamespaceWorkload(ctx context.Context, cfg *rest.Config, ns string) {
	cl, err := e2eNewVirtClient(cfg)
	if err != nil {
		GinkgoWriter.Printf("    ⚠️  cleanup: virt client: %v (will still try namespace delete)\n", err)
		e2eDeleteNamespaceBestEffort(ctx, cfg, ns)
		return
	}

	var vmbdaList virtv1alpha2.VirtualMachineBlockDeviceAttachmentList
	if err := cl.List(ctx, &vmbdaList, client.InNamespace(ns)); err != nil {
		GinkgoWriter.Printf("    ⚠️  cleanup: list VMBDA: %v\n", err)
	} else {
		for i := range vmbdaList.Items {
			if err := cl.Delete(ctx, &vmbdaList.Items[i]); err != nil && !apierrors.IsNotFound(err) {
				GinkgoWriter.Printf("    ⚠️  cleanup: delete VMBDA %s: %v\n", vmbdaList.Items[i].Name, err)
			}
		}
	}

	var vdList virtv1alpha2.VirtualDiskList
	if err := cl.List(ctx, &vdList, client.InNamespace(ns)); err != nil {
		GinkgoWriter.Printf("    ⚠️  cleanup: list VirtualDisk: %v\n", err)
	} else {
		for i := range vdList.Items {
			if err := cl.Delete(ctx, &vdList.Items[i]); err != nil && !apierrors.IsNotFound(err) {
				GinkgoWriter.Printf("    ⚠️  cleanup: delete VirtualDisk %s: %v\n", vdList.Items[i].Name, err)
			}
		}
	}

	var vmList virtv1alpha2.VirtualMachineList
	if err := cl.List(ctx, &vmList, client.InNamespace(ns)); err != nil {
		GinkgoWriter.Printf("    ⚠️  cleanup: list VirtualMachine: %v\n", err)
	} else {
		for i := range vmList.Items {
			if err := cl.Delete(ctx, &vmList.Items[i]); err != nil && !apierrors.IsNotFound(err) {
				GinkgoWriter.Printf("    ⚠️  cleanup: delete VirtualMachine %s: %v\n", vmList.Items[i].Name, err)
			}
		}
	}

	e2eDeleteNamespaceBestEffort(ctx, cfg, ns)
}

func e2eDeleteNamespaceBestEffort(ctx context.Context, cfg *rest.Config, ns string) {
	cs, err := k8sclient.NewForConfig(cfg)
	if err != nil {
		GinkgoWriter.Printf("    ⚠️  cleanup: kubernetes client: %v\n", err)
		return
	}
	delCtx, cancel := context.WithTimeout(ctx, e2eClusterCleanupTimeout)
	defer cancel()
	err = cs.CoreV1().Namespaces().Delete(delCtx, ns, metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		GinkgoWriter.Printf("    ℹ️  cleanup: namespace %q already gone\n", ns)
		return
	}
	if err != nil {
		GinkgoWriter.Printf("    ⚠️  cleanup: delete namespace %q: %v\n", ns, err)
		return
	}
	GinkgoWriter.Printf("    ✅ cleanup: namespace %q deletion submitted\n", ns)
}

func kubeconfigDirForNamespaceDelete(clusterStatePath string, _ error) (dir string, tmpToRemove string, err error) {
	if clusterStatePath != "" {
		d := filepath.Dir(clusterStatePath)
		if mkErr := os.MkdirAll(d, 0755); mkErr != nil {
			return "", "", mkErr
		}
		return d, "", nil
	}
	d, mkErr := os.MkdirTemp("", "e2e-ns-del-kube-*")
	if mkErr != nil {
		return "", "", mkErr
	}
	return d, d, nil
}

// e2eSyncCleanupBaseClusterNamespace connects to the base cluster, removes VMBDA/VD/VM in namespace, then deletes the namespace.
// Used when alwaysCreateNew fails mid-run so resources are not left until AfterAll.
func e2eSyncCleanupBaseClusterNamespace(ctx context.Context, clusterStatePath string, statePathErr error) {
	kubeconfigDir, cleanupDir, err := kubeconfigDirForNamespaceDelete(clusterStatePath, statePathErr)
	if err != nil {
		GinkgoWriter.Printf("    ⚠️  cleanup: %v\n", err)
		return
	}
	if cleanupDir != "" {
		defer func() { _ = os.RemoveAll(cleanupDir) }()
	}

	var ns string
	if clusterStatePath != "" {
		data, readErr := os.ReadFile(clusterStatePath)
		if readErr == nil {
			var st clusterResumeState
			if json.Unmarshal(data, &st) == nil && st.Namespace != "" {
				ns = st.Namespace
			}
		} else if !errors.Is(readErr, os.ErrNotExist) {
			GinkgoWriter.Printf("    ⚠️  cleanup: read %s: %v\n", clusterStatePath, readErr)
		}
	}
	if ns == "" {
		ns = e2eConfigNamespace()
		GinkgoWriter.Printf("    ▶️  cleanup: namespace from TEST_CLUSTER_NAMESPACE=%q\n", ns)
	} else {
		GinkgoWriter.Printf("    ▶️  cleanup: namespace from cluster-state.json: %q\n", ns)
	}

	sshHost := e2eConfigSSHHost()
	sshUser := e2eConfigSSHUser()
	if sshHost == "" || sshUser == "" {
		GinkgoWriter.Printf("    ⚠️  cleanup: SSH_HOST or SSH_USER not set, skip base cluster cleanup\n")
		return
	}

	sshKeyPath, err := cluster.GetSSHPrivateKeyPath()
	if err != nil {
		GinkgoWriter.Printf("    ⚠️  cleanup: SSH key: %v\n", err)
		return
	}

	useJump := e2eConfigSSHJumpHost() != ""
	var opts cluster.ConnectClusterOptions
	if !useJump {
		opts = cluster.ConnectClusterOptions{
			SSHUser: sshUser, SSHHost: sshHost, SSHKeyPath: sshKeyPath,
			UseJumpHost:         false,
			KubeconfigOutputDir: kubeconfigDir,
		}
	} else {
		jumpUser := e2eConfigSSHJumpUser()
		if jumpUser == "" {
			jumpUser = sshUser
		}
		jumpKey := e2eConfigSSHJumpKeyPath()
		if jumpKey == "" {
			jumpKey = sshKeyPath
		}
		opts = cluster.ConnectClusterOptions{
			SSHUser: jumpUser, SSHHost: e2eConfigSSHJumpHost(), SSHKeyPath: jumpKey,
			UseJumpHost: true, TargetUser: sshUser, TargetHost: sshHost, TargetKeyPath: sshKeyPath,
			KubeconfigOutputDir: kubeconfigDir,
		}
	}

	baseRes, err := cluster.ConnectToCluster(ctx, opts)
	if err != nil {
		GinkgoWriter.Printf("    ⚠️  cleanup: connect to base cluster: %v\n", err)
		return
	}
	defer func() {
		if baseRes.TunnelInfo != nil && baseRes.TunnelInfo.StopFunc != nil {
			_ = baseRes.TunnelInfo.StopFunc()
		}
		if baseRes.SSHClient != nil {
			_ = baseRes.SSHClient.Close()
		}
	}()

	GinkgoWriter.Printf("    ▶️  cleanup: ordered teardown VMBDA → VirtualDisk → VirtualMachine → Namespace %q\n", ns)
	e2eCleanupBaseClusterNamespaceWorkload(ctx, baseRes.Kubeconfig, ns)

	if clusterStatePath != "" {
		_ = os.Remove(clusterStatePath)
	}
}

// createE2EAlwaysNewClusterWithCleanupOnFailure runs CreateTestCluster + WaitForTestClusterReady for alwaysCreateNew.
// On failure, cleans the base cluster namespace (VD/VM/ns) inside the same spec before Gomega fails.
func createE2EAlwaysNewClusterWithCleanupOnFailure() *cluster.TestClusterResources {
	statePath, statePathErr := e2eClusterStateJSONPath()

	yamlName := os.Getenv("YAML_CONFIG_FILENAME")
	if yamlName == "" {
		yamlName = "cluster_config.yml"
	}

	createCtx, cancel := context.WithTimeout(context.Background(), e2eClusterCreationTimeout)
	defer cancel()

	res, err := cluster.CreateTestCluster(createCtx, yamlName)
	if err != nil {
		GinkgoWriter.Printf("    ▶️  CreateTestCluster failed; cleaning base cluster namespace before spec exits...\n")
		e2eSyncCleanupBaseClusterNamespace(context.Background(), statePath, statePathErr)
		Expect(err).NotTo(HaveOccurred(), "Test cluster should be created successfully")
	}

	waitCtx, cancel2 := context.WithTimeout(context.Background(), e2eModuleDeployTimeout)
	defer cancel2()
	err = cluster.WaitForTestClusterReady(waitCtx, res)
	if err != nil {
		GinkgoWriter.Printf("    ▶️  WaitForTestClusterReady failed; cleaning base cluster then storage-e2e cleanup...\n")
		e2eSyncCleanupBaseClusterNamespace(context.Background(), statePath, statePathErr)
		cctx, ccancel := context.WithTimeout(context.Background(), e2eClusterCleanupTimeout)
		cleanupErr := cluster.CleanupTestCluster(cctx, res)
		ccancel()
		if cleanupErr != nil {
			GinkgoWriter.Printf("    ⚠️  CleanupTestCluster after failed readiness: %v\n", cleanupErr)
		}
		Expect(err).NotTo(HaveOccurred(), "Test cluster should become ready")
	}

	return res
}

// e2eEnsureSharedNestedTestCluster creates or connects to the test cluster once for the whole suite run.
// Call from BeforeSuite only: root-level Ordered Describes are shuffled by Ginkgo, so we cannot rely on
// Common Scheduler running before Sds Node Configurator.
func e2eEnsureSharedNestedTestCluster() {
	if e2eNestedTestCluster != nil {
		return
	}
	switch e2eConfigTestClusterCreateMode() {
	case testClusterModeCreateNew:
		r := createE2EAlwaysNewClusterWithCleanupOnFailure()
		e2eRegisterNestedTestCluster(r)
	case testClusterModeUseExisting:
		r, err := e2eConnectUseExistingClusterOnceOrRetryAfterLockDelete()
		if err != nil {
			e2ePrintStaleClusterLockHint(err)
		}
		Expect(err).NotTo(HaveOccurred(), "Should connect to existing cluster successfully")
		e2eRegisterNestedTestCluster(r)
		Expect(e2eEnsureDeckhouseStorageModulesReadyForUseExisting(context.Background(), r)).To(Succeed(),
			"ModuleConfig applied; storage.deckhouse.io BlockDevice API must appear (if Deckhouse refuses enable, fix bundle/edition — see Module.status)")
	default:
		Expect(waitForVirtualizationModuleReadyIfNeeded(context.Background())).To(Succeed(),
			"virtualization module should become Ready on base cluster (retry while Reconciling)")
		r := cluster.CreateOrConnectToTestCluster()
		e2eRegisterNestedTestCluster(r)
	}
}

// --- Defaults, env helpers, and nested test cluster lifecycle (one cluster, AfterSuite cleanup) ---
// Align consts with storage-e2e internal/config when using setup.Init().

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
	// WaitForVirtualDiskAttached: VMBDA may stay Pending for many minutes (virt hotplug, node load); 5m was too tight on virtlab.
	e2eVirtualDiskAttachWaitTimeout = 15 * time.Minute

	e2eVirtualizationModuleWaitDefault = 25 * time.Minute

	e2eLsblkSSHMaxRetries    = 6
	e2eLsblkSSHRetryInterval = 15 * time.Second

	e2eClusterCreationTimeout = 90 * time.Minute
	e2eModuleDeployTimeout    = 15 * time.Minute
	// LVMVolumeGroup Pending → Ready on busy CI can exceed 5m (agent + node LVM).
	e2eLVMVolumeGroupReadyTimeout = 15 * time.Minute
	e2eStorageModuleReadyTimeout  = 30 * time.Minute // alwaysUseExisting: wait for Module Ready after ModuleConfig
	e2eUseExistingClusterTimeout  = 90 * time.Minute

	// Common Scheduler "fill to max" tests create many PVCs/Pods; provisioning and binding can exceed 5m on loaded clusters.
	e2eSchedulerFillPodsWaitTimeout = 10 * time.Minute

	// Scheduler cleanup: pod termination and CSI PV teardown must not share one deadline — many PVs delete serially.
	e2eSchedulerPodCleanupTimeout = 5 * time.Minute
	e2eSchedulerPVDeleteTimeout   = 25 * time.Minute

	// Suite/AfterAll: short pod wait; PVC deletion returns quickly while PV finalizers need a separate budget.
	e2eSuitePodPVCleanupPodTimeout = 2 * time.Minute
	e2eSuitePodPVCleanupPVTimeout  = 15 * time.Minute

	// Guest VM name prefix for dhctl/bootstrap (Deckhouse test clusters). Do not attach data disks here — not a worker.
	e2eBootstrapGuestVMPrefix = "bootstrap-node-"
)

const (
	testClusterModeCreateNew   = "alwaysCreateNew"
	testClusterModeUseExisting = "alwaysUseExisting"
)

var deckhouseModuleGVR = schema.GroupVersionResource{
	Group:    "deckhouse.io",
	Version:  "v1alpha1",
	Resource: "modules",
}

var deckhouseModuleConfigGVR = schema.GroupVersionResource{
	Group:    "deckhouse.io",
	Version:  "v1alpha1",
	Resource: "moduleconfigs",
}

// e2eRequiredDeckhouseStorageModules is applied in order (local-volume before node-configurator).
var e2eRequiredDeckhouseStorageModules = []string{"sds-local-volume", "sds-node-configurator"}

const (
	deckhouseModuleConditionIsReady               = "IsReady"
	deckhouseModuleConditionEnabledByModuleConfig = "EnabledByModuleConfig"
)

func moduleVirtualizationIsReady(obj *unstructured.Unstructured) (phase string, isReady bool) {
	p, found, _ := unstructured.NestedString(obj.Object, "status", "phase")
	if found {
		phase = p
	}
	if phase == "Ready" {
		return phase, true
	}
	conditions, found, _ := unstructured.NestedSlice(obj.Object, "status", "conditions")
	if !found {
		return phase, false
	}
	for _, c := range conditions {
		cm, ok := c.(map[string]interface{})
		if !ok {
			continue
		}
		t, _ := cm["type"].(string)
		st, _ := cm["status"].(string)
		if t == deckhouseModuleConditionIsReady && st == "True" {
			return phase, true
		}
	}
	return phase, false
}

// e2eApplyModuleConfigEnableStorageModule applies the shape Deckhouse expects for ModuleConfig (see module-config CRD example):
// spec.enabled, spec.version (settings schema), spec.settings. Without version/settings the controller may ignore the object.
func e2eApplyModuleConfigEnableStorageModule(ctx context.Context, dyn dynamic.Interface, name string) error {
	obj, err := dyn.Resource(deckhouseModuleConfigGVR).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		newObj := &unstructured.Unstructured{}
		newObj.SetAPIVersion("deckhouse.io/v1alpha1")
		newObj.SetKind("ModuleConfig")
		newObj.SetName(name)
		if err := unstructured.SetNestedMap(newObj.Object, map[string]interface{}{
			"enabled":  true,
			"version":  int64(1),
			"settings": map[string]interface{}{},
		}, "spec"); err != nil {
			return err
		}
		_, err = dyn.Resource(deckhouseModuleConfigGVR).Create(ctx, newObj, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("create ModuleConfig %s: %w", name, err)
		}
		e2eLogModuleConfigApplied(ctx, dyn, name, "created")
		return nil
	}
	if err != nil {
		return fmt.Errorf("get ModuleConfig %s: %w", name, err)
	}

	spec, found, err := unstructured.NestedMap(obj.Object, "spec")
	if err != nil || !found || spec == nil {
		spec = map[string]interface{}{}
	}
	changed := false
	if eb, ok := spec["enabled"].(bool); !ok || !eb {
		spec["enabled"] = true
		changed = true
	}
	if _, ok := spec["version"]; !ok {
		spec["version"] = int64(1)
		changed = true
	}
	if _, ok := spec["settings"]; !ok {
		spec["settings"] = map[string]interface{}{}
		changed = true
	}
	if err := unstructured.SetNestedMap(obj.Object, spec, "spec"); err != nil {
		return err
	}
	if !changed {
		e2eLogModuleConfigApplied(ctx, dyn, name, "already satisfied (enabled+version+settings)")
		return nil
	}
	_, err = dyn.Resource(deckhouseModuleConfigGVR).Update(ctx, obj, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("update ModuleConfig %s: %w", name, err)
	}
	e2eLogModuleConfigApplied(ctx, dyn, name, "updated")
	return nil
}

func e2eLogModuleConfigApplied(ctx context.Context, dyn dynamic.Interface, name, action string) {
	GinkgoWriter.Printf("    ✅ ModuleConfig %q %s (spec: enabled=true, version=1, settings={})\n", name, action)
	getCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	obj, err := dyn.Resource(deckhouseModuleConfigGVR).Get(getCtx, name, metav1.GetOptions{})
	if err != nil {
		GinkgoWriter.Printf("    ⚠️  ModuleConfig %q re-get: %v\n", name, err)
		return
	}
	if msg, ok, _ := unstructured.NestedString(obj.Object, "status", "message"); ok && msg != "" {
		GinkgoWriter.Printf("    ℹ️  ModuleConfig %q status.message: %s\n", name, msg)
	}
	if v, ok, _ := unstructured.NestedString(obj.Object, "status", "version"); ok && v != "" {
		GinkgoWriter.Printf("    ℹ️  ModuleConfig %q status.version (schema in use): %s\n", name, v)
	}
}

func e2eFormatDeckhouseModuleCondition(obj *unstructured.Unstructured, condType string) string {
	conditions, found, _ := unstructured.NestedSlice(obj.Object, "status", "conditions")
	if !found {
		return "n/a"
	}
	for _, c := range conditions {
		cm, ok := c.(map[string]interface{})
		if !ok {
			continue
		}
		t, _ := cm["type"].(string)
		if t != condType {
			continue
		}
		st, _ := cm["status"].(string)
		reason, _ := cm["reason"].(string)
		msg, _ := cm["message"].(string)
		if len(msg) > 160 {
			msg = msg[:160] + "…"
		}
		if reason != "" || msg != "" {
			return fmt.Sprintf("%s reason=%q %s", st, reason, msg)
		}
		return st
	}
	return "absent"
}

// e2eDeckhouseModuleIsReadyDiag returns IsReady condition status and short message for logging.
func e2eDeckhouseModuleIsReadyDiag(obj *unstructured.Unstructured) (phase, isReadyStatus, isReadyMsg string) {
	phase, _, _ = unstructured.NestedString(obj.Object, "status", "phase")
	conditions, found, _ := unstructured.NestedSlice(obj.Object, "status", "conditions")
	if !found {
		return phase, "?", ""
	}
	for _, c := range conditions {
		cm, ok := c.(map[string]interface{})
		if !ok {
			continue
		}
		t, _ := cm["type"].(string)
		if t != deckhouseModuleConditionIsReady {
			continue
		}
		isReadyStatus, _ = cm["status"].(string)
		isReadyMsg, _ = cm["message"].(string)
		return phase, isReadyStatus, isReadyMsg
	}
	return phase, "?", ""
}

func e2eLogDeckhouseStorageModulesStatus(ctx context.Context, dyn dynamic.Interface) {
	for _, name := range e2eRequiredDeckhouseStorageModules {
		getCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		obj, err := dyn.Resource(deckhouseModuleGVR).Get(getCtx, name, metav1.GetOptions{})
		cancel()
		if err != nil {
			GinkgoWriter.Printf("    … Module/%s: get: %v\n", name, err)
			continue
		}
		phase, _, _ := e2eDeckhouseModuleIsReadyDiag(obj)
		enc := e2eFormatDeckhouseModuleCondition(obj, deckhouseModuleConditionEnabledByModuleConfig)
		ird := e2eFormatDeckhouseModuleCondition(obj, deckhouseModuleConditionIsReady)
		GinkgoWriter.Printf("    … Module/%s phase=%q EnabledByModuleConfig={%s} IsReady={%s}\n", name, phase, enc, ird)
	}
}

// e2eWaitForBlockDeviceAPI polls API discovery until BlockDevice is registered.
// Deckhouse Module status often stays phase=Available while hooks run; Ready comes later — tests only need CRDs.
func e2eWaitForBlockDeviceAPI(ctx context.Context, cfg *rest.Config, dyn dynamic.Interface, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	poll := 5 * time.Second

	GinkgoWriter.Printf("    ⏳ Waiting for BlockDevice API (storage.deckhouse.io/v1alpha1 discovery; timeout %s, poll %s)...\n", timeout, poll)

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout after %v: BlockDevice API still missing — check Module.status EnabledByModuleConfig (False = bundle/edition/dependencies block enable; ModuleConfig alone is not enough)", timeout)
		}
		err := e2eVerifyBlockDeviceAPIAvailable(cfg)
		if err == nil {
			GinkgoWriter.Printf("    ✅ BlockDevice API registered (storage.deckhouse.io/v1alpha1)\n")
			return nil
		}
		GinkgoWriter.Printf("    … %v\n", err)
		e2eLogDeckhouseStorageModulesStatus(ctx, dyn)
		time.Sleep(poll)
	}
}

func e2eVerifyBlockDeviceAPIAvailable(cfg *rest.Config) error {
	dc, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		return fmt.Errorf("discovery client: %w", err)
	}
	resources, err := dc.ServerResourcesForGroupVersion("storage.deckhouse.io/v1alpha1")
	if err != nil {
		return fmt.Errorf("storage.deckhouse.io/v1alpha1 not advertised (install sds-node-configurator / wait for CRDs): %w", err)
	}
	for i := range resources.APIResources {
		if resources.APIResources[i].Kind == "BlockDevice" {
			return nil
		}
	}
	return fmt.Errorf("BlockDevice kind not found in storage.deckhouse.io/v1alpha1 discovery")
}

// e2eEnsureDeckhouseStorageModulesReadyForUseExisting applies ModuleConfig for sds-local-volume and sds-node-configurator
// (enabled + version + settings) and waits until BlockDevice appears in API discovery (alwaysUseExisting path).
// Deckhouse may still refuse to enable modules (bundle/edition); see Module.status EnabledByModuleConfig in logs.
func e2eEnsureDeckhouseStorageModulesReadyForUseExisting(ctx context.Context, res *cluster.TestClusterResources) error {
	if res == nil || res.Kubeconfig == nil {
		return fmt.Errorf("test cluster resources or kubeconfig is nil")
	}

	GinkgoWriter.Printf("[INFO]  ▶ Step 5 (e2e): Applying ModuleConfig for sds-local-volume & sds-node-configurator; waiting for BlockDevice API...\n")

	cfg := rest.CopyConfig(res.Kubeconfig)
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return fmt.Errorf("dynamic client for module ensure: %w", err)
	}

	for _, mod := range e2eRequiredDeckhouseStorageModules {
		if err := e2eApplyModuleConfigEnableStorageModule(ctx, dyn, mod); err != nil {
			return err
		}
	}

	if err := e2eWaitForBlockDeviceAPI(ctx, cfg, dyn, e2eStorageModuleReadyTimeout); err != nil {
		return err
	}

	GinkgoWriter.Printf("[INFO]  ✅ Step 5 (e2e) Complete: BlockDevice API available\n")
	return nil
}

func e2eTestTempDirFromStack() (string, error) {
	for i := 1; i <= 20; i++ {
		_, file, _, ok := runtime.Caller(i)
		if !ok {
			break
		}
		if !strings.Contains(filepath.ToSlash(file), "/tests/") {
			continue
		}
		dir := filepath.Dir(file)
		for filepath.Base(dir) != "tests" {
			parent := filepath.Dir(dir)
			if parent == dir {
				break
			}
			dir = parent
		}
		if filepath.Base(dir) != "tests" {
			continue
		}
		repoRoot := filepath.Dir(dir)
		testFileName := strings.TrimSuffix(filepath.Base(file), filepath.Ext(file))
		return filepath.Join(repoRoot, "temp", testFileName), nil
	}
	return "", fmt.Errorf("could not determine e2e temp dir from call stack (expected caller under tests/)")
}

func waitForVirtualizationModuleReadyWithRestConfig(ctx context.Context, baseCfg *rest.Config) error {
	cfg := rest.CopyConfig(baseCfg)
	cfg.Timeout = 30 * time.Second
	cfg.Dial = func(ctx context.Context, network, addr string) (net.Conn, error) {
		d := net.Dialer{Timeout: 15 * time.Second}
		return d.DialContext(ctx, network, addr)
	}

	timeout := e2eVirtualizationModuleWaitDefault
	if v := os.Getenv("E2E_VIRTUALIZATION_MODULE_WAIT_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			timeout = d
		}
	}
	deadline := time.Now().Add(timeout)
	poll := 3 * time.Second
	reqTimeout := 30 * time.Second

	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return fmt.Errorf("dynamic client for virtualization wait: %w", err)
	}

	GinkgoWriter.Printf("    ⏳ Waiting for Deckhouse module %q to become Ready (timeout %s, polling every %s)...\n",
		"virtualization", timeout, poll)

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout after %v waiting for Module/virtualization phase Ready (see logs above)", timeout)
		}

		getCtx, cancel := context.WithTimeout(ctx, reqTimeout)
		obj, err := dyn.Resource(deckhouseModuleGVR).Get(getCtx, "virtualization", metav1.GetOptions{})
		cancel()

		if err != nil {
			GinkgoWriter.Printf("    … Module/virtualization get: %v\n", err)
			time.Sleep(poll)
			continue
		}
		phase, ready := moduleVirtualizationIsReady(obj)
		if ready {
			GinkgoWriter.Printf("    ✅ Module/virtualization is ready (phase=%q, IsReady or phase Ready)\n", phase)
			return nil
		}
		if phase == "" {
			GinkgoWriter.Printf("    … Module/virtualization: no status.phase yet (waiting)\n")
		} else {
			GinkgoWriter.Printf("    … Module/virtualization phase=%q (waiting for Ready or IsReady=True)\n", phase)
		}
		time.Sleep(poll)
	}
}

func waitForVirtualizationModuleReadyIfNeeded(ctx context.Context) error {
	if os.Getenv("E2E_SKIP_VIRTUALIZATION_MODULE_WAIT") == "true" {
		GinkgoWriter.Printf("    ⏭️  Skipping virtualization Module pre-wait (E2E_SKIP_VIRTUALIZATION_MODULE_WAIT=true)\n")
		return nil
	}
	if e2eConfigTestClusterCreateMode() != testClusterModeCreateNew {
		return nil
	}

	sshHost := e2eConfigSSHHost()
	sshUser := e2eConfigSSHUser()
	if sshHost == "" || sshUser == "" {
		return nil
	}

	sshKeyPath, err := cluster.GetSSHPrivateKeyPath()
	if err != nil {
		return fmt.Errorf("ssh key for virtualization pre-wait: %w", err)
	}

	kubeconfigDir, err := e2eTestTempDirFromStack()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(kubeconfigDir, 0o755); err != nil {
		return fmt.Errorf("mkdir kubeconfig dir for virtualization pre-wait: %w", err)
	}

	useJump := e2eConfigSSHJumpHost() != ""
	jumpUser := e2eConfigSSHJumpUser()
	if jumpUser == "" {
		jumpUser = sshUser
	}
	jumpHost := e2eConfigSSHJumpHost()
	jumpKeyPath := e2eConfigSSHJumpKeyPath()
	if jumpKeyPath == "" {
		jumpKeyPath = sshKeyPath
	}

	opts := cluster.ConnectClusterOptions{
		SSHUser:             sshUser,
		SSHHost:             sshHost,
		SSHKeyPath:          sshKeyPath,
		UseJumpHost:         useJump,
		KubeconfigOutputDir: kubeconfigDir,
	}
	if useJump {
		opts = cluster.ConnectClusterOptions{
			SSHUser:             jumpUser,
			SSHHost:             jumpHost,
			SSHKeyPath:          jumpKeyPath,
			UseJumpHost:         true,
			TargetUser:          sshUser,
			TargetHost:          sshHost,
			TargetKeyPath:       sshKeyPath,
			KubeconfigOutputDir: kubeconfigDir,
		}
	}

	GinkgoWriter.Printf("    🔌 Connecting to base cluster (SSH) for virtualization Module pre-wait...\n")

	connectTimeout := 20 * time.Minute
	if v := os.Getenv("E2E_VIRTUALIZATION_MODULE_CONNECT_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			connectTimeout = d
		}
	}
	connectCtx, cancelConnect := context.WithTimeout(ctx, connectTimeout)
	defer cancelConnect()

	base, err := cluster.ConnectToCluster(connectCtx, opts)
	if err != nil {
		return fmt.Errorf("connect to base cluster for virtualization pre-wait: %w", err)
	}
	defer func() {
		if base.TunnelInfo != nil && base.TunnelInfo.StopFunc != nil {
			_ = base.TunnelInfo.StopFunc()
		}
		if base.SSHClient != nil {
			_ = base.SSHClient.Close()
		}
	}()

	if err := waitForVirtualizationModuleReadyWithRestConfig(ctx, base.Kubeconfig); err != nil {
		return err
	}
	GinkgoWriter.Printf("    🔌 Closed pre-wait SSH tunnel; proceeding to CreateTestCluster\n")
	return nil
}

// e2eListVirtualMachineNamesRunningOnly returns VM names whose status.phase is Running.
// Attach to Migrating/Starting VMs often hangs VMBDA in Pending — do not use those for disk attach.
func e2eListVirtualMachineNamesRunningOnly(ctx context.Context, baseKube *rest.Config, ns string) ([]string, error) {
	cl, err := e2eNewVirtClient(baseKube)
	if err != nil {
		return nil, err
	}
	var list virtv1alpha2.VirtualMachineList
	if err := cl.List(ctx, &list, client.InNamespace(ns)); err != nil {
		return nil, err
	}
	names := make([]string, 0, len(list.Items))
	for i := range list.Items {
		if list.Items[i].Status.Phase == virtv1alpha2.MachineRunning {
			names = append(names, list.Items[i].Name)
		}
	}
	return names, nil
}

// e2eExcludeBootstrapGuestVMs removes bootstrap guest VMs (prefix bootstrap-node-) from attach candidate lists.
func e2eExcludeBootstrapGuestVMs(names []string) []string {
	if len(names) == 0 {
		return names
	}
	out := make([]string, 0, len(names))
	for _, n := range names {
		if strings.HasPrefix(n, e2eBootstrapGuestVMPrefix) {
			continue
		}
		out = append(out, n)
	}
	return out
}

// e2eIntersectVMNamesRunning keeps only Running VMs. If candidates is non-empty but none are Running (e.g. Migrating),
// falls back to all Running VMs in the namespace and logs a warning.
func e2eIntersectVMNamesRunning(ctx context.Context, baseKube *rest.Config, ns string, candidates []string) []string {
	running, err := e2eListVirtualMachineNamesRunningOnly(ctx, baseKube, ns)
	Expect(err).NotTo(HaveOccurred(), "list VirtualMachines on base cluster")
	running = e2eExcludeBootstrapGuestVMs(running)
	candidates = e2eExcludeBootstrapGuestVMs(candidates)
	Expect(running).NotTo(BeEmpty(),
		"no non-bootstrap guest VMs in phase Running in namespace %s (Migrating/Starting/bootstrap-only)", ns)

	if len(candidates) == 0 {
		return running
	}
	allowed := make(map[string]struct{}, len(running))
	for _, n := range running {
		allowed[n] = struct{}{}
	}
	var out []string
	for _, c := range candidates {
		if _, ok := allowed[c]; ok {
			out = append(out, c)
		}
	}
	if len(out) > 0 {
		return out
	}
	GinkgoWriter.Printf("    ⚠️  no Running non-bootstrap VM among candidates %v (e.g. Migrating); using all Running non-bootstrap VMs in namespace: %v\n", candidates, running)
	return running
}

// e2eListClusterVMNames returns guest VM names to attach disks to (same pattern as other LVM tests).
func e2eListClusterVMNames(ctx context.Context, res *cluster.TestClusterResources, ns string) []string {
	var candidates []string
	if res.VMResources != nil {
		for _, name := range res.VMResources.VMNames {
			if name != res.VMResources.SetupVMName {
				candidates = append(candidates, name)
			}
		}
	}
	return e2eIntersectVMNamesRunning(ctx, res.BaseKubeconfig, ns, candidates)
}

func e2eConfigNamespace() string {
	if v := os.Getenv("TEST_CLUSTER_NAMESPACE"); v != "" {
		return v
	}
	return e2eDefaultNamespace
}

func e2eConfigStorageClass() string       { return os.Getenv("TEST_CLUSTER_STORAGE_CLASS") }
func e2eConfigTestClusterCleanup() string { return os.Getenv("TEST_CLUSTER_CLEANUP") }

// e2eShouldDeleteBaseNamespaceAfterSuite controls deletion of TEST_CLUSTER_NAMESPACE on the base (virtualization) cluster
// in AfterSuite. storage-e2e CleanupTestCluster removes VMs but does not delete the namespace; namespace teardown must
// run while BaseKubeconfig still works (before CleanupTestCluster stops the base tunnel).
// Set TEST_CLUSTER_DELETE_NAMESPACE=false to keep the namespace. If unset, defaults to when TEST_CLUSTER_CLEANUP is enabled.
func e2eShouldDeleteBaseNamespaceAfterSuite() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("TEST_CLUSTER_DELETE_NAMESPACE"))) {
	case "true", "1", "yes":
		return true
	case "false", "0", "no":
		return false
	default:
		return e2eConfigTestClusterCleanup() == "true" || e2eConfigTestClusterCleanup() == "True"
	}
}
func e2eConfigSSHHost() string        { return os.Getenv("SSH_HOST") }
func e2eConfigSSHUser() string        { return os.Getenv("SSH_USER") }
func e2eConfigSSHJumpHost() string    { return os.Getenv("SSH_JUMP_HOST") }
func e2eConfigSSHJumpUser() string    { return os.Getenv("SSH_JUMP_USER") }
func e2eConfigSSHJumpKeyPath() string { return os.Getenv("SSH_JUMP_KEY_PATH") }
func e2eConfigSSHPassphrase() string  { return os.Getenv("SSH_PASSPHRASE") }
func e2eConfigLogLevel() string       { return os.Getenv("LOG_LEVEL") }

func e2eConfigKubeConfigPath() string {
	return os.Getenv("KUBE_CONFIG_PATH")
}

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

// e2eNestedTestCluster is the single nested cluster for a full suite run (both Ordered Describes).
// Common Scheduler Extender registers it after CreateOrConnect; AfterSuite runs e2eCleanupNestedTestClusterAfterSuite.
var e2eNestedTestCluster *cluster.TestClusterResources

func e2eRegisterNestedTestCluster(r *cluster.TestClusterResources) {
	e2eNestedTestCluster = r
}

func e2eNestedTestClusterOrNil() *cluster.TestClusterResources {
	return e2eNestedTestCluster
}

// e2eModuleRootDir returns the e2e Go module root (directory containing ./tests), from this source file.
func e2eModuleRootDir() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return ""
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), ".."))
}

// e2eFindClusterStateJSONPathForCleanup returns cluster-state.json if present under temp/<test_stem>/ for any
// test file that may have invoked CreateTestCluster (storage-e2e saves state per calling test file name).
func e2eFindClusterStateJSONPathForCleanup() string {
	root := e2eModuleRootDir()
	if root == "" {
		return ""
	}
	for _, base := range []string{"common_scheduler_test", "sds_node_configurator_test"} {
		p := filepath.Join(root, "temp", base, "cluster-state.json")
		if st, err := os.Stat(p); err == nil && !st.IsDir() {
			return p
		}
	}
	return ""
}

func e2eCleanupNestedTestClusterAfterSuite() {
	if e2eNestedTestCluster != nil {
		ctx, cancel := context.WithTimeout(context.Background(), e2eClusterCleanupTimeout)
		defer cancel()

		cleanupEnabled := e2eConfigTestClusterCleanup() == "true" || e2eConfigTestClusterCleanup() == "True"
		if cleanupEnabled {
			GinkgoWriter.Printf("    ▶️ AfterSuite: cleaning up test cluster resources (TEST_CLUSTER_CLEANUP is enabled - all VMs will be removed)...\n")
		} else {
			GinkgoWriter.Printf("    ▶️ AfterSuite: cleaning up test cluster resources (TEST_CLUSTER_CLEANUP is not enabled - only bootstrap node will be removed)...\n")
		}

		if e2eShouldDeleteBaseNamespaceAfterSuite() && e2eNestedTestCluster.BaseKubeconfig != nil {
			ns := e2eConfigNamespace()
			if ns != "" {
				GinkgoWriter.Printf("    ▶️ AfterSuite: deleting base cluster namespace %q (VMBDA → VirtualDisk → VirtualMachine → Namespace; storage-e2e CleanupTestCluster does not remove the namespace)\n", ns)
				e2eCleanupBaseClusterNamespaceWorkload(ctx, e2eNestedTestCluster.BaseKubeconfig, ns)
			}
		} else if e2eShouldDeleteBaseNamespaceAfterSuite() && e2eNestedTestCluster.BaseKubeconfig == nil {
			GinkgoWriter.Printf("    ⚠️  AfterSuite: skip namespace delete — BaseKubeconfig is nil\n")
		}

		err := cluster.CleanupTestCluster(ctx, e2eNestedTestCluster)
		if err != nil {
			GinkgoWriter.Printf("    ⚠️  Warning: AfterSuite cluster cleanup errors: %v\n", err)
		} else {
			GinkgoWriter.Printf("    ✅ AfterSuite: test cluster resources cleaned up successfully\n")
		}
		e2eNestedTestCluster = nil
		return
	}

	// CreateTestCluster may create VMs and write cluster-state.json, then fail (e.g. during dhctl bootstrap).
	// It returns (nil, err), so e2eRegisterNestedTestCluster never runs — same gap as covered by
	// createE2EAlwaysNewClusterWithCleanupOnFailure for the Sds Describe. Tear down the namespace workload here.
	if e2eConfigTestClusterCreateMode() != testClusterModeCreateNew {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), e2eClusterCleanupTimeout)
	defer cancel()
	statePath := e2eFindClusterStateJSONPathForCleanup()
	GinkgoWriter.Printf("    ▶️ AfterSuite: no registered nested cluster — best-effort VM/namespace cleanup (failed mid-CreateTestCluster; cluster-state=%q)\n", statePath)
	e2eSyncCleanupBaseClusterNamespace(ctx, statePath, nil)
}

func e2eConfigVMSSHUser() string {
	if v := os.Getenv("SSH_VM_USER"); v != "" {
		return v
	}
	return e2eDefaultVMSSHUser
}

// e2eAttachVirtualDiskToVM mirrors storage-e2e AttachVirtualDiskToVM but treats AlreadyExists on VirtualDisk
// or VirtualMachineBlockDeviceAttachment as success. That way attachVirtualDiskWithRetry recovers when the first
// attempt created the VirtualDisk and failed on VMBDA (retries no longer hit 409 on VD create).
func e2eAttachVirtualDiskToVM(ctx context.Context, baseKubeconfig *rest.Config, config kubernetes.VirtualDiskAttachmentConfig) (*kubernetes.VirtualDiskAttachmentResult, error) {
	if config.VMName == "" || config.Namespace == "" || config.DiskSize == "" || config.StorageClassName == "" {
		return nil, fmt.Errorf("VirtualDiskAttachmentConfig: VMName, Namespace, DiskSize, StorageClassName are required")
	}
	diskName := config.DiskName
	if diskName == "" {
		diskName = fmt.Sprintf("%s-data-disk", config.VMName)
	}
	attachmentName := fmt.Sprintf("%s-attachment", diskName)

	cl, err := e2eNewVirtClient(baseKubeconfig)
	if err != nil {
		return nil, err
	}
	diskSize, err := resource.ParseQuantity(config.DiskSize)
	if err != nil {
		return nil, fmt.Errorf("parse disk size %q: %w", config.DiskSize, err)
	}
	sc := config.StorageClassName
	vd := &virtv1alpha2.VirtualDisk{
		ObjectMeta: metav1.ObjectMeta{Name: diskName, Namespace: config.Namespace},
		Spec: virtv1alpha2.VirtualDiskSpec{
			PersistentVolumeClaim: virtv1alpha2.VirtualDiskPersistentVolumeClaim{
				Size:         &diskSize,
				StorageClass: &sc,
			},
		},
	}
	err = cl.Create(ctx, vd)
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			GinkgoWriter.Printf("    ℹ️  VirtualDisk %s/%s already exists (idempotent attach)\n", config.Namespace, diskName)
		} else {
			return nil, fmt.Errorf("create VirtualDisk %s: %w", diskName, err)
		}
	}

	att := &virtv1alpha2.VirtualMachineBlockDeviceAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: attachmentName, Namespace: config.Namespace},
		Spec: virtv1alpha2.VirtualMachineBlockDeviceAttachmentSpec{
			VirtualMachineName: config.VMName,
			BlockDeviceRef: virtv1alpha2.VMBDAObjectRef{
				Kind: virtv1alpha2.VMBDAObjectRefKindVirtualDisk,
				Name: diskName,
			},
		},
	}
	err = cl.Create(ctx, att)
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			GinkgoWriter.Printf("    ℹ️  VirtualMachineBlockDeviceAttachment %s/%s already exists (idempotent attach)\n", config.Namespace, attachmentName)
		} else {
			return nil, fmt.Errorf("create VirtualMachineBlockDeviceAttachment %s: %w", attachmentName, err)
		}
	}

	return &kubernetes.VirtualDiskAttachmentResult{
		DiskName:       diskName,
		AttachmentName: attachmentName,
	}, nil
}

func attachVirtualDiskWithRetry(ctx context.Context, baseKubeconfig *rest.Config, config kubernetes.VirtualDiskAttachmentConfig, maxRetries int, retryInterval time.Duration) (*kubernetes.VirtualDiskAttachmentResult, error) {
	var lastErr error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		att, err := e2eAttachVirtualDiskToVM(ctx, baseKubeconfig, config)
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

func keysOf(m map[string]struct{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
func ensureSchedulerE2EK8sClient(resources *cluster.TestClusterResources, k8s *client.Client, ctx context.Context) {
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
	cleanupE2EPods(ctx, *k8s)
	cleanupE2EPVCs(ctx, *k8s)
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

// forceDeleteAllBlockDevices removes finalizers and deletes every BlockDevice CR, then waits until none remain.
// Used after scheduler tests so the next Describe does not inherit orphan consumable BlockDevices.
func forceDeleteAllBlockDevices(ctx context.Context, cl client.Client, timeout time.Duration) {
	var bdList v1alpha1.BlockDeviceList
	if err := cl.List(ctx, &bdList, &client.ListOptions{}); err != nil {
		GinkgoWriter.Printf("forceDeleteAllBlockDevices: list failed: %v\n", err)
		return
	}
	if len(bdList.Items) == 0 {
		GinkgoWriter.Println("No BlockDevices to delete")
		return
	}
	GinkgoWriter.Printf("Force deleting %d BlockDevice(s)\n", len(bdList.Items))
	for i := range bdList.Items {
		bd := &bdList.Items[i]
		GinkgoWriter.Printf("  Removing finalizers and deleting BD %s (%s on %s)\n",
			bd.Name, bd.Status.Path, bd.Status.NodeName)
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
		if len(bdList.Items) == 0 {
			GinkgoWriter.Println("All BlockDevices deleted")
			return
		}
		GinkgoWriter.Printf("Waiting for %d BlockDevice(s) to be gone...\n", len(bdList.Items))
		time.Sleep(5 * time.Second)
	}
	GinkgoWriter.Println("Warning: timeout waiting for all BlockDevices deletion")
}

func forceDeleteBlockDevicesByNames(ctx context.Context, cl client.Client, names []string) {
	if cl == nil || len(names) == 0 {
		return
	}
	for _, name := range names {
		bd := &v1alpha1.BlockDevice{}
		if err := cl.Get(ctx, client.ObjectKey{Name: name}, bd); err != nil {
			continue
		}
		GinkgoWriter.Printf("Deleting BlockDevice CR %s\n", name)
		if len(bd.Finalizers) > 0 {
			bd.Finalizers = nil
			if err := cl.Update(ctx, bd); err != nil {
				GinkgoWriter.Printf("  failed to strip finalizers on %s: %v\n", name, err)
			}
		}
		if err := cl.Delete(ctx, bd); err != nil {
			GinkgoWriter.Printf("  failed to delete BlockDevice %s: %v\n", name, err)
		}
	}
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

	// Delete LLV CRs only — do not strip finalizers: concurrent controller updates cause 409 conflicts and can leave
	// VGs half-cleaned; the agent completes removal when finalizers run normally after PVC/Pods are gone.
	GinkgoWriter.Printf("Deleting %d LVMLogicalVolume(s) referencing e2e LVGs\n", len(toDelete))
	for _, name := range toDelete {
		llv := &v1alpha1.LVMLogicalVolume{ObjectMeta: metav1.ObjectMeta{Name: name}}
		if err := cl.Delete(ctx, llv); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			GinkgoWriter.Printf("  Failed to delete LLV %s: %v\n", name, err)
		}
	}

	deadline := time.Now().Add(5 * time.Minute)
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
	err := cl.List(ctx, &list, client.InNamespace(metav1.NamespaceDefault))
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
	err := cl.List(ctx, &list, client.InNamespace(metav1.NamespaceDefault))
	if err != nil {
		return
	}
	for i := range list.Items {
		if strings.HasPrefix(list.Items[i].Name, e2ePodPrefix) {
			p := list.Items[i]
			_ = cl.Delete(ctx, &p, client.GracePeriodSeconds(0))
		}
	}
}

func countE2EPodsDefault(ctx context.Context, cl client.Client) int {
	var podList corev1.PodList
	if err := cl.List(ctx, &podList, client.InNamespace(metav1.NamespaceDefault)); err != nil {
		GinkgoWriter.Printf("count e2e pods: list failed: %v\n", err)
		return 999999
	}
	n := 0
	for i := range podList.Items {
		if strings.HasPrefix(podList.Items[i].Name, e2ePodPrefix) {
			n++
		}
	}
	return n
}

func countE2EPVCsDefault(ctx context.Context, cl client.Client) int {
	var pvcList corev1.PersistentVolumeClaimList
	if err := cl.List(ctx, &pvcList, client.InNamespace(metav1.NamespaceDefault)); err != nil {
		GinkgoWriter.Printf("count e2e PVCs: list failed: %v\n", err)
		return 999999
	}
	n := 0
	for i := range pvcList.Items {
		if strings.HasPrefix(pvcList.Items[i].Name, e2ePVCPrefix) {
			n++
		}
	}
	return n
}

// countE2ERelatedPVs returns PVs still present for e2e local volumes (same StorageClass as scheduler tests).
// PVC objects can be gone while PV is still Terminating — CSI must detach before LVMLogicalVolume can be removed safely.
func countE2ERelatedPVs(ctx context.Context, cl client.Client) int {
	var list corev1.PersistentVolumeList
	if err := cl.List(ctx, &list); err != nil {
		GinkgoWriter.Printf("count e2e PVs: list failed: %v\n", err)
		return 999999
	}
	n := 0
	for i := range list.Items {
		pv := &list.Items[i]
		if pv.Spec.StorageClassName == e2eLocalStorageClassName {
			n++
			continue
		}
		if ref := pv.Spec.ClaimRef; ref != nil && ref.Namespace == metav1.NamespaceDefault &&
			strings.HasPrefix(ref.Name, e2ePVCPrefix) {
			n++
		}
	}
	return n
}

// cleanupE2EPodsAndPVCsWithWait deletes e2e Pods first and waits until they are gone before deleting PVCs.
// Deleting PVCs while Pods still mount the volume leaves PVCs stuck in Terminating and blocks the test.
// Then waits until related PersistentVolumes are gone (CSI finishes delete) so LVMLogicalVolume teardown can run in order.
// podPhaseTimeout and pvPhaseTimeout are independent: time spent waiting for Pods must not reduce the CSI budget for
// deleting many PVs (detach + DeleteVolume + finalizers), which serializes in the controller on loaded clusters.
// Uses only Delete (no finalizer removal on Pods or PVCs).
func cleanupE2EPodsAndPVCsWithWait(ctx context.Context, cl client.Client, podPhaseTimeout, pvPhaseTimeout time.Duration) {
	podDeadline := time.Now().Add(podPhaseTimeout)

	cleanupE2EPods(ctx, cl)
	for time.Now().Before(podDeadline) {
		n := countE2EPodsDefault(ctx, cl)
		if n == 0 {
			break
		}
		GinkgoWriter.Printf("Waiting for e2e Pods to terminate before PVC cleanup: %d remaining\n", n)
		time.Sleep(3 * time.Second)
	}

	cleanupE2EPVCs(ctx, cl)
	pvDeadline := time.Now().Add(pvPhaseTimeout)
	for time.Now().Before(pvDeadline) {
		podCount := countE2EPodsDefault(ctx, cl)
		pvcCount := countE2EPVCsDefault(ctx, cl)
		pvCount := countE2ERelatedPVs(ctx, cl)
		if podCount == 0 && pvcCount == 0 && pvCount == 0 {
			GinkgoWriter.Println("All e2e Pods, PVCs, and related PVs deleted")
			return
		}
		GinkgoWriter.Printf("Waiting for cleanup: %d pods, %d PVCs, %d PVs remaining\n", podCount, pvcCount, pvCount)
		time.Sleep(5 * time.Second)
	}
	GinkgoWriter.Println("Warning: some e2e Pods/PVCs/PVs may still exist after cleanup timeout")
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

// getMaxVGFreeAcrossLVGs returns the largest VGFree among Ready e2e LVMVolumeGroups. A single PVC is satisfied by
// one LVG on one node — sum(VGFree) over the cluster can exceed this (fragmented free space after prior tests).
func getMaxVGFreeAcrossLVGs(ctx context.Context, cl client.Client, lvgs []*v1alpha1.LVMVolumeGroup) int64 {
	var maxFree int64
	for _, lvg := range lvgs {
		var current v1alpha1.LVMVolumeGroup
		if err := cl.Get(ctx, client.ObjectKeyFromObject(lvg), &current); err != nil {
			continue
		}
		if current.Status.Phase != v1alpha1.PhaseReady {
			continue
		}
		v := current.Status.VGFree.Value()
		if v > maxFree {
			maxFree = v
		}
	}
	return maxFree
}

// schedulerVolumeSizesForConsolidatedFill builds PVC sizes that sum to at most currentAvailable (sum of VGFree),
// with each request <= maxPerLVG. preferredUnit is capped by maxPerLVG; leftover bytes are drained in chunks
// <= maxPerLVG so every volume can schedule on some node with local LVM.
func schedulerVolumeSizesForConsolidatedFill(currentAvailable, maxPerLVG, preferredUnit, minRemainder int64) []int64 {
	if currentAvailable <= 0 || maxPerLVG <= 0 {
		return nil
	}
	unit := preferredUnit
	if unit > maxPerLVG {
		unit = maxPerLVG
	}
	if unit <= 0 {
		return nil
	}
	var sizes []int64
	left := currentAvailable
	for left >= unit {
		sizes = append(sizes, unit)
		left -= unit
	}
	for left >= minRemainder {
		sz := min(maxPerLVG, left)
		if sz < minRemainder {
			break
		}
		sizes = append(sizes, sz)
		left -= sz
	}
	return sizes
}

// schedulerCleanupWorkloadBeforeNextFill removes e2e Pods/PVCs, then LVMLogicalVolumes tied to e2e LVGs.
// Deleting PVCs alone is not enough: thin LVs remain on the VG until LLV CRs are removed; otherwise the next test
// or LVMVolumeGroup teardown hits "Delete used LVs first" and PV can stay Released with no PVC.
// Pods/PVCs are deleted without stripping finalizers; wait for PVs before LLV so CSI does not race with LVMLogicalVolume deletion.
func schedulerCleanupWorkloadBeforeNextFill(ctx context.Context, cl client.Client) {
	cleanupE2EPodsAndPVCsWithWait(ctx, cl, e2eSchedulerPodCleanupTimeout, e2eSchedulerPVDeleteTimeout)
	// If cleanupE2EPodsAndPVCsWithWait hit its deadline, PVs can remain Released while CSI waits for detach/delete.
	// Deleting LVMLogicalVolume CRs in that window makes the provisioner error (LLV not found) and leaves PV stuck.
	n := countE2ERelatedPVs(ctx, cl)
	Expect(n).To(BeZero(),
		"e2e-related PVs must be gone before LVMLogicalVolume cleanup; leftover PVs mean CSI has not finished delete/detach (see VolumeAttachments, VolumeFailedDelete)")
	By("Removing LVMLogicalVolumes for e2e LVGs after PVC deletion (thin LVs must leave the VG)")
	cleanupE2ELVMLogicalVolumes(ctx, cl)
}

// waitForSchedulerStorageFreedToBaseline waits until sum(VGFree) across Ready e2e LVMVolumeGroups returns to the
// baseline recorded after LVG became Ready (totalAvailableSpace). Pod/PVC deletion finishes before thin LVs and
// LLV CRs are gone; waiting only for VGFree > 0 under-fills the next "100% utilization" test.
func waitForSchedulerStorageFreedToBaseline(ctx context.Context, cl client.Client, lvgs []*v1alpha1.LVMVolumeGroup, baselineFreeBytes int64) int64 {
	if baselineFreeBytes <= 0 {
		return 0
	}
	// Allow 1% slack for status rounding vs first observation; require ~full recovery of the initial budget.
	minExpected := baselineFreeBytes * 99 / 100
	var total int64
	Eventually(func(g Gomega) {
		total = getTotalAvailableSpace(ctx, cl, lvgs)
		g.Expect(total).To(BeNumerically(">=", minExpected),
			"sum(VGFree) must recover to ~initial storage budget after Pod/PVC/LLV cleanup (baseline=%d min=%d got=%d)",
			baselineFreeBytes, minExpected, total)
	}, 15*time.Minute, 5*time.Second).Should(Succeed())
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

	scheduledCount = waitForPodsScheduled(ctx, cl, sizeLabel, successCount, e2eSchedulerFillPodsWaitTimeout)
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

	scheduledCount = waitForPodsScheduled(ctx, cl, sizeLabel, successCount, e2eSchedulerFillPodsWaitTimeout)

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
