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
	"os"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/deckhouse/storage-e2e/pkg/cluster"
)

// clusterResumeState mirrors storage-e2e cluster-state.json (namespace after VMs are created).
type clusterResumeState struct {
	Namespace string `json:"namespace"`
}

// e2eSavedLVGForVGRemoveInfo records the pvresize LVMVolumeGroup for the follow-up "remove VG" spec.

// e2eSavedLVGForVGRemoveInfo records the pvresize LVMVolumeGroup for the follow-up "remove VG" spec.
type e2eSavedLVGForVGRemoveInfo struct {
	lvgName, nodeName, vgNameOnNode, blockDeviceName string
}

// runLsblkViaDirectSSHWithRetry wraps runLsblkViaDirectSSH for transient SSH errors (EOF during handshake, reset).

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

// e2eSuiteVirtualDiskPrefix matches all test VirtualDisks in the e2e namespace (scheduler, Sds LVG, discovery disks).

// e2eSuiteVirtualDiskPrefix matches all test VirtualDisks in the e2e namespace (scheduler, Sds LVG, discovery disks).
const e2eSuiteVirtualDiskPrefix = "e2e-"

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

// e2eRequiredDeckhouseStorageModules is applied in order (local-volume before node-configurator).
var e2eRequiredDeckhouseStorageModules = []string{"sds-local-volume", "sds-node-configurator"}

const (
	deckhouseModuleConditionIsReady               = "IsReady"
	deckhouseModuleConditionEnabledByModuleConfig = "EnabledByModuleConfig"
)

func e2eConfigNamespace() string {
	if v := os.Getenv("TEST_CLUSTER_NAMESPACE"); v != "" {
		return v
	}
	return e2eDefaultNamespace
}

func e2eConfigStorageClass() string { return os.Getenv("TEST_CLUSTER_STORAGE_CLASS") }

func e2eConfigTestClusterCleanup() string { return os.Getenv("TEST_CLUSTER_CLEANUP") }

// e2eShouldDeleteBaseNamespaceAfterSuite controls deletion of TEST_CLUSTER_NAMESPACE on the base (virtualization) cluster
// in AfterSuite. storage-e2e CleanupTestCluster removes VMs but does not delete the namespace; namespace teardown must
// run while BaseKubeconfig still works (before CleanupTestCluster stops the base tunnel).
// Set TEST_CLUSTER_DELETE_NAMESPACE=false to keep the namespace. If unset, defaults to when TEST_CLUSTER_CLEANUP is enabled.

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

func e2eConfigSSHHost() string { return os.Getenv("SSH_HOST") }

func e2eConfigSSHUser() string { return os.Getenv("SSH_USER") }

func e2eConfigSSHJumpHost() string { return os.Getenv("SSH_JUMP_HOST") }

func e2eConfigSSHJumpUser() string { return os.Getenv("SSH_JUMP_USER") }

func e2eConfigSSHJumpKeyPath() string { return os.Getenv("SSH_JUMP_KEY_PATH") }

func e2eConfigSSHPassphrase() string { return os.Getenv("SSH_PASSPHRASE") }

func e2eConfigLogLevel() string { return os.Getenv("LOG_LEVEL") }

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

// e2eNestedTestCluster is the single nested cluster for a full suite run (both Ordered Describes).
// Common Scheduler Extender registers it after CreateOrConnect; AfterSuite runs e2eCleanupNestedTestClusterAfterSuite.
var e2eNestedTestCluster *cluster.TestClusterResources

func keysOf(m map[string]struct{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
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
