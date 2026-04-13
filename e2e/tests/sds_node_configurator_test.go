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
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	virtv1alpha2 "github.com/deckhouse/virtualization/api/core/v1alpha2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	k8sclient "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/storage-e2e/pkg/cluster"
	"github.com/deckhouse/storage-e2e/pkg/kubernetes"
	"github.com/deckhouse/storage-e2e/pkg/ssh"
)

// e2e config defaults (must match storage-e2e internal/config when using setup.Init())
const (
	e2eDefaultNamespace      = "e2e-test-cluster"
	e2eDefaultVMSSHUser      = "cloud"
	e2eClusterCleanupTimeout     = 10 * time.Minute
	e2eUseExistingClusterTimeout = 90 * time.Minute // storage-e2e ClusterCreationTimeout (connect + lock + health)
	e2eLVMVGPrefix               = "e2e-lvg-"

	e2eVirtualDiskAttachMaxRetries    = 3
	e2eVirtualDiskAttachRetryInterval = 1 * time.Minute

	// Direct SSH to nodes for lsblk can hit transient "handshake failed: EOF" (sshd/network) after heavy I/O.
	e2eLsblkSSHMaxRetries   = 6
	e2eLsblkSSHRetryInterval = 15 * time.Second

	// alwaysCreateNew: aligned with github.com/deckhouse/storage-e2e/internal/config
	e2eClusterCreationTimeout = 90 * time.Minute
	e2eModuleDeployTimeout    = 15 * time.Minute
)

// clusterResumeState mirrors storage-e2e cluster-state.json (namespace after VMs are created).
type clusterResumeState struct {
	Namespace string `json:"namespace"`
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
func e2eConfigKubeConfigPath() string {
	// KUBE_CONFIG_PATH is set by CI from E2E_CLUSTER_KUBECONFIG (written to file) or directly as path
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
func e2eConfigVMSSHUser() string {
	if v := os.Getenv("SSH_VM_USER"); v != "" {
		return v
	}
	return e2eDefaultVMSSHUser
}

// attachVirtualDiskWithRetry calls AttachVirtualDiskToVM up to maxRetries times with retryInterval between attempts.
// Tolerates transient webhook/network errors (e.g. "operation not permitted", webhook dial failures).
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

// blockDeviceSerialFromVirtualDiskUID returns the BlockDevice serial (hex-encoded MD5 of UID).
// Virtualization uses VirtualDisk.UID: see kvbuilder.GenerateSerialFromObject(vd) in deckhouse/virtualization.
func blockDeviceSerialFromVirtualDiskUID(uid string) string {
	h := md5.Sum([]byte(uid))
	return hex.EncodeToString(h[:])
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

var _ = Describe("Sds Node Configurator", Ordered, func() {
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
	})

	AfterAll(func() {
		// Cleanup test cluster resources
		// Note: Bootstrap node (setup VM) is always removed.
		// Test cluster VMs (masters and workers) are only removed if TEST_CLUSTER_CLEANUP='true' or 'True'
		if testClusterResources != nil {
			ctx, cancel := context.WithTimeout(context.Background(), e2eClusterCleanupTimeout)
			defer cancel()

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
	// alwaysCreateNew: CreateTestCluster + WaitForTestClusterReady; on failure we clean base cluster (see createE2EAlwaysNewClusterWithCleanupOnFailure).

	It("should create test cluster", func() {
		if e2eConfigTestClusterCreateMode() == "alwaysCreateNew" {
			testClusterResources = createE2EAlwaysNewClusterWithCleanupOnFailure()
			return
		}
		if e2eConfigTestClusterCreateMode() == "alwaysUseExisting" {
			By("Connecting to existing cluster", func() {
				GinkgoWriter.Printf("    ▶️ Connecting to existing cluster (mode: alwaysUseExisting)\n")
				var err error
				testClusterResources, err = e2eConnectUseExistingClusterOnceOrRetryAfterLockDelete()
				if err != nil {
					GinkgoWriter.Printf("    ❌ Failed to connect to existing cluster: %v\n", err)
					e2ePrintStaleClusterLockHint(err)
				}
				Expect(err).NotTo(HaveOccurred(), "Should connect to existing cluster successfully")
				GinkgoWriter.Printf("    ✅ Connected to existing cluster successfully (cluster lock acquired)\n")
			})
			return
		}
		testClusterResources = cluster.CreateOrConnectToTestCluster()

		if nestedKubeconfigPath := os.Getenv("NESTED_KUBE_CONFIG_PATH"); nestedKubeconfigPath != "" {
			By(fmt.Sprintf("Using nested cluster kubeconfig: %s (base cluster becomes VirtualDisk manager)", nestedKubeconfigPath))
			testClusterResources.BaseKubeconfig = testClusterResources.Kubeconfig
			nestedConfig, err := clientcmd.BuildConfigFromFlags("", nestedKubeconfigPath)
			Expect(err).NotTo(HaveOccurred(), "load nested cluster kubeconfig from %s", nestedKubeconfigPath)
			testClusterResources.Kubeconfig = nestedConfig
		}
	}) // should create test cluster

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

		It("Should delete a BlockDevice after the backing disk disappears", func() {
			ensureE2EK8sClient(testClusterResources, &k8sClient, e2eCtx)
			if testClusterResources.BaseKubeconfig == nil {
				Skip("BlockDevice disappearance test requires nested virtualization (base cluster kubeconfig)")
			}

			ns := e2eConfigNamespace()
			storageClass := e2eConfigStorageClass()
			Expect(storageClass).NotTo(BeEmpty(), "TEST_CLUSTER_STORAGE_CLASS is required for VirtualDisk")

			var clusterVMs []string
			if testClusterResources.VMResources != nil {
				for _, name := range testClusterResources.VMResources.VMNames {
					if name != testClusterResources.VMResources.SetupVMName {
						clusterVMs = append(clusterVMs, name)
					}
				}
			}
			if len(clusterVMs) == 0 {
				vmNames, listErr := kubernetes.ListVirtualMachineNames(e2eCtx, testClusterResources.BaseKubeconfig, ns)
				Expect(listErr).NotTo(HaveOccurred(), "list VirtualMachines on base cluster")
				Expect(vmNames).NotTo(BeEmpty(), "no VirtualMachines in namespace %s on base cluster", ns)
				clusterVMs = vmNames
			}

			targetVM := clusterVMs[rand.Intn(len(clusterVMs))]
			diskName := fmt.Sprintf("%s-missing-%d", e2eDataDiskName, rand.Intn(100000))

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
			e2eDiskAttachments = nil

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
	})

	Context("LVMVolumeGroup with one disk and thin-pool", func() {
		const e2eLVGDataDiskName = "e2e-lvg-data-disk"
		const e2eLVGDataDiskSize = "2Gi"

		var lvgE2eDiskAttachment *kubernetes.VirtualDiskAttachmentResult

		AfterEach(func() {
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

			// Snapshot BlockDevices before attach so we can detect the new one (sds-node-configurator may discover it quickly after attach)
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

			var targetBD *v1alpha1.BlockDevice
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

			nodeName := targetBD.Status.NodeName

			bdMetaName := targetBD.Labels["kubernetes.io/metadata.name"]
			if bdMetaName == "" {
				bdMetaName = targetBD.Name
			}

			vgName := "e2e-vg"
			thinPoolName := "e2e-thin-pool"
			// Not 50%: half of a 2Gi disk rounds to 1Gi in spec while LVM may allocate slightly more
			// bytes (alignment), and VGConfigurationApplied then fails ValidationFailed (requested < actual).
			thinPoolSize := "60%"
			thinPoolAllocationLimit := "100%"

			lvgName := "e2e-lvg-" + strings.ReplaceAll(strings.ReplaceAll(nodeName, ".", "-"), "_", "-")
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
		})
	})

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
					if len(lvg.Finalizers) > 0 {
						lvg.Finalizers = nil
						_ = k8sClient.Update(e2eCtx, lvg)
					}
					_ = k8sClient.Delete(e2eCtx, lvg)
				}
				shrinkLVGName = ""
			}
			if testClusterResources == nil {
				return
			}
			ns := e2eConfigNamespace()
			kubeconfig := testClusterResources.BaseKubeconfig
			if kubeconfig == nil {
				kubeconfig = testClusterResources.Kubeconfig
			}
			if origDiskAttachment != nil {
				_ = kubernetes.DetachAndDeleteVirtualDisk(e2eCtx, kubeconfig, ns, origDiskAttachment.AttachmentName, origDiskAttachment.DiskName)
				origDiskAttachment = nil
			}
			if smallDiskAttachment != nil {
				_ = kubernetes.DetachAndDeleteVirtualDisk(e2eCtx, kubeconfig, ns, smallDiskAttachment.AttachmentName, smallDiskAttachment.DiskName)
				smallDiskAttachment = nil
			}
		})

		It("Should detect device loss after replacing disk with a smaller one and report VG inconsistency", func() {
			ensureE2EK8sClient(testClusterResources, &k8sClient, e2eCtx)
			if testClusterResources.BaseKubeconfig == nil {
				Skip("Block device shrink test requires nested virtualization (base cluster kubeconfig)")
			}
			ns := e2eConfigNamespace()
			storageClass := e2eConfigStorageClass()
			Expect(storageClass).NotTo(BeEmpty(), "TEST_CLUSTER_STORAGE_CLASS required")

			var clusterVMs []string
			if testClusterResources.VMResources != nil {
				for _, name := range testClusterResources.VMResources.VMNames {
					if name != testClusterResources.VMResources.SetupVMName {
						clusterVMs = append(clusterVMs, name)
					}
				}
			}
			if len(clusterVMs) == 0 {
				vmNames, listErr := kubernetes.ListVirtualMachineNames(e2eCtx, testClusterResources.BaseKubeconfig, ns)
				Expect(listErr).NotTo(HaveOccurred(), "list VirtualMachines on base cluster")
				Expect(vmNames).NotTo(BeEmpty(), "no VirtualMachines in namespace %s", ns)
				clusterVMs = vmNames
			}
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

			By("Waiting for LVMVolumeGroup to become Ready")
			Eventually(func(g Gomega) {
				var current v1alpha1.LVMVolumeGroup
				g.Expect(k8sClient.Get(e2eCtx, client.ObjectKeyFromObject(lvg), &current)).To(Succeed())
				g.Expect(current.Status.Phase).To(Equal(v1alpha1.PhaseReady), "Phase=%s", current.Status.Phase)
			}, 5*time.Minute, 10*time.Second).Should(Succeed())

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
			ensureE2EK8sClient(testClusterResources, &k8sClient, e2eCtx)

			By("Step 1: Getting a real node name from the cluster")
			var nodeList corev1.NodeList
			Expect(k8sClient.List(e2eCtx, &nodeList)).To(Succeed())
			Expect(nodeList.Items).NotTo(BeEmpty(), "cluster must have at least one node")
			realNodeName := nodeList.Items[0].Name

			fakeBDName := e2eFakeBDPrefix + fmt.Sprintf("%d", rand.Intn(100000))

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

			By("Step 6: Verifying that an event or condition was created about manual management prohibition")
			var eventList corev1.EventList
			Expect(k8sClient.List(e2eCtx, &eventList)).To(Succeed())
			foundProhibitionEvent := false
			for _, ev := range eventList.Items {
				if ev.InvolvedObject.Name == fakeBDName && ev.InvolvedObject.Kind == "BlockDevice" {
					GinkgoWriter.Printf("    Event on fake BD: reason=%s message=%s\n", ev.Reason, ev.Message)
					if strings.Contains(strings.ToLower(ev.Reason+ev.Message), "manual") ||
						strings.Contains(strings.ToLower(ev.Reason+ev.Message), "prohibit") ||
						strings.Contains(strings.ToLower(ev.Reason+ev.Message), "rejected") ||
						strings.Contains(strings.ToLower(ev.Reason+ev.Message), "deleted") {
						foundProhibitionEvent = true
					}
				}
			}
			Expect(foundProhibitionEvent).To(BeTrue(),
				"controller should create an Event on the BlockDevice about manual management prohibition when deleting a manually created object")
		})

		It("Should revert manual modifications to an existing BlockDevice status", func() {
			ensureE2EK8sClient(testClusterResources, &k8sClient, e2eCtx)

			By("Step 1: Finding an existing BlockDevice in the cluster")
			var bdList v1alpha1.BlockDeviceList
			Expect(k8sClient.List(e2eCtx, &bdList)).To(Succeed())
			if len(bdList.Items) == 0 {
				Skip("No BlockDevices in cluster to test modification revert")
			}

			var targetBD *v1alpha1.BlockDevice
			for i := range bdList.Items {
				bd := &bdList.Items[i]
				if bd.Status.Path != "" && bd.Status.Size.Value() > 0 {
					targetBD = bd
					break
				}
			}
			if targetBD == nil {
				Skip("No BlockDevice with valid path and size found")
			}

			originalSize := targetBD.Status.Size.DeepCopy()
			originalPath := targetBD.Status.Path
			By(fmt.Sprintf("Target BD: %s (node=%s, path=%s, size=%s)",
				targetBD.Name, targetBD.Status.NodeName, originalPath, originalSize.String()))

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

			By("Step 5: Verifying that an event or condition was created about manual modification revert")
			var eventList corev1.EventList
			Expect(k8sClient.List(e2eCtx, &eventList)).To(Succeed())
			foundRevertEvent := false
			for _, ev := range eventList.Items {
				if ev.InvolvedObject.Name == targetBD.Name && ev.InvolvedObject.Kind == "BlockDevice" {
					GinkgoWriter.Printf("    Event on BD %s: reason=%s message=%s\n", targetBD.Name, ev.Reason, ev.Message)
					if strings.Contains(strings.ToLower(ev.Reason+ev.Message), "manual") ||
						strings.Contains(strings.ToLower(ev.Reason+ev.Message), "revert") ||
						strings.Contains(strings.ToLower(ev.Reason+ev.Message), "prohibit") ||
						strings.Contains(strings.ToLower(ev.Reason+ev.Message), "overwritten") {
						foundRevertEvent = true
					}
				}
			}
			Expect(foundRevertEvent).To(BeTrue(),
				"controller should create an Event on the BlockDevice about manual modification being prohibited/reverted")
		})
	})

	///////////////////////////////////////////////////// ---=== TESTS END HERE ===--- /////////////////////////////////////////////////////

}) // Describe: Sds Node Configurator

// ensureE2EK8sClient initializes k8sClient from test cluster kubeconfig (once) and cleans up e2e LVMVolumeGroups.
// Must be called from inside Describe so resources/k8sClient/e2eCtx are in scope.
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
	By("Cleaning up existing e2e LVMVolumeGroups (prefix " + e2eLVMVGPrefix + ")")
	cleanupE2ELVMVolumeGroupsSdsNodeConfigurator(ctx, *k8s)
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

func keysOf(m map[string]struct{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
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

func printLVMVolumeGroupInfo(lvg *v1alpha1.LVMVolumeGroup) {
	GinkgoWriter.Println("\n========== LVMVolumeGroup information ==========")
	GinkgoWriter.Printf("Name: %s\n", lvg.Name)
	GinkgoWriter.Println("--- Spec ---")
	GinkgoWriter.Printf("  Type: %s\n", lvg.Spec.Type)
	GinkgoWriter.Printf("  ActualVGNameOnTheNode: %s\n", lvg.Spec.ActualVGNameOnTheNode)
	GinkgoWriter.Printf("  Local.NodeName: %s\n", lvg.Spec.Local.NodeName)
	if lvg.Spec.BlockDeviceSelector != nil && len(lvg.Spec.BlockDeviceSelector.MatchLabels) > 0 {
		GinkgoWriter.Printf("  BlockDeviceSelector.MatchLabels: %v\n", lvg.Spec.BlockDeviceSelector.MatchLabels)
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
