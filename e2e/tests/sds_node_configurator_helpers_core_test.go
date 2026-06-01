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
	"strconv"
	"strings"
	"testing"
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
	// Auto-import: BD discoverer must link PV to BlockDevice before LVG discoverer can create a CR.
	e2eBlockDeviceVGLinkageTimeout = 10 * time.Minute
	// Tagged VG auto-import: LVMVolumeGroup CR after BD linkage + agent rescan (nested CI may miss udev).
	e2eLVMVolumeGroupAutoImportDiscoveryTimeout = 15 * time.Minute
	e2eStorageModuleReadyTimeout  = 30 * time.Minute // alwaysUseExisting: wait for Module Ready after ModuleConfig
	e2eUseExistingClusterTimeout  = 90 * time.Minute

	// Common Scheduler "fill to max" tests create many PVCs/Pods; provisioning and binding can exceed 5m on loaded clusters.
	e2eSchedulerFillPodsWaitTimeout = 10 * time.Minute

	// Scheduler cleanup: pod termination and CSI PV teardown must not share one deadline — many PVs delete serially.
	e2eSchedulerPodCleanupTimeout = 5 * time.Minute
	e2eSchedulerPVDeleteTimeout   = 25 * time.Minute
	// Many fill-test Pods (busybox + RWO volume) can stay Terminating until volumes detach; force-finalize when stuck.
	e2ePodCleanupStuckWithoutProgress = 90 * time.Second
	e2ePodCleanupPollInterval           = 3 * time.Second
	e2ePodCleanupLogStuckInterval       = 30 * time.Second

	// Suite/AfterAll: short pod wait; PVC deletion returns quickly while PV finalizers need a separate budget.
	e2eSuitePodPVCleanupPodTimeout = 2 * time.Minute
	e2eSuitePodPVCleanupPVTimeout  = 15 * time.Minute

	// Full smoke suite (BeforeSuite cluster + scheduler + module e2e) exceeds 60m on CI.
	e2eTestTimeoutDefaultLocal = 90 * time.Minute
	e2eTestTimeoutDefaultCI    = 3*time.Hour + 30*time.Minute
	e2eMinCIGoTestTimeout       = e2eTestTimeoutDefaultCI // go test -timeout must be ~this on CI (see TestSdsNodeConfigurator)
	e2eCIGoTestTimeoutDetectMin = 2 * time.Hour           // fail only below this (catches 60m org defaults, not 3h30m−ε)

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

// Ginkgo label filters (see sds_node_configurator_suite_test.go).
const (
	e2eGinkgoLabelE2ETests   = "e2e-tests"
	e2eGinkgoLabelStressTest = "stress-test"
	e2eGinkgoLabelFilterEnv  = "E2E_GINKGO_LABEL_FILTER"
)

// e2eGinkgoLabelFilter is applied when go test is run without -ginkgo.label-filter.
// Default smoke excludes stress-test; set E2E_GINKGO_LABEL_FILTER=all to run every spec.
func e2eGinkgoLabelFilter() string {
	v := strings.TrimSpace(os.Getenv(e2eGinkgoLabelFilterEnv))
	switch strings.ToLower(v) {
	case "", "default", "smoke":
		return e2eGinkgoLabelE2ETests
	case "all", "*", "!", "none":
		return ""
	default:
		return v
	}
}

// e2eTestSuiteTimeout is the Ginkgo suite / go test -timeout budget for TestSdsNodeConfigurator.
// e2eAssertCIGoTestTimeout fails fast when CI runs go test with -timeout 60m (default org setting).
// Ginkgo suite timeout cannot extend the go test process alarm — both must be >= e2eMinCIGoTestTimeout.
func e2eAssertCIGoTestTimeout(t *testing.T) {
	t.Helper()
	if os.Getenv("CI") == "" {
		return
	}
	deadline, ok := t.Deadline()
	if !ok {
		t.Logf("CI: go test deadline unknown; invoke with -timeout at least %v", e2eMinCIGoTestTimeout)
		return
	}
	remaining := time.Until(deadline)
	// go test sets deadline slightly below -timeout (startup); Round for stable log output.
	if remaining.Round(time.Second) < e2eCIGoTestTimeoutDetectMin {
		t.Fatalf("go test -timeout too short for CI smoke (%v remaining, need >= %v). "+
			"Use go test -timeout 3h30m (workflow must not pass a shorter value to the -timeout flag).",
			remaining.Round(time.Second), e2eMinCIGoTestTimeout)
	}
}

func e2eTestSuiteTimeout() time.Duration {
	var d time.Duration
	if v := strings.TrimSpace(os.Getenv("E2E_TEST_TIMEOUT")); v != "" {
		if parsed, err := time.ParseDuration(v); err == nil && parsed > 0 {
			d = parsed
		}
	}
	if d == 0 {
		if os.Getenv("CI") != "" {
			d = e2eTestTimeoutDefaultCI
		} else {
			d = e2eTestTimeoutDefaultLocal
		}
	}
	if os.Getenv("CI") != "" && d < e2eMinCIGoTestTimeout {
		return e2eMinCIGoTestTimeout
	}
	return d
}

// Stress e2e: many independent LVMVolumeGroups (1 PV = 1 VG) on one node.
const (
	e2eStressMaxVGTargetEnv       = "E2E_STRESS_MAX_VG_TARGET"
	e2eStressMaxVGDiskSizeEnv     = "E2E_STRESS_MAX_VG_DISK_SIZE"
	e2eStressMaxVGBatchSizeEnv    = "E2E_STRESS_MAX_VG_BATCH_SIZE"
	e2eStressMaxVGStrictEnv       = "E2E_STRESS_MAX_VG_STRICT"
	e2eStressMaxVGMinReadyEnv          = "E2E_STRESS_MAX_VG_MIN_READY"
	e2eStressMaxVMBlockDevicesEnv      = "E2E_STRESS_MAX_VM_BLOCK_DEVICES"
	e2eStressMaxVGDefaultTarget        = 15
	e2eStressMaxVMBlockDevicesDefault  = 15 // Deckhouse virt: max 16 VMBDAs per VM; reserve one for boot/system disks
	e2eStressMaxVGDefaultBatch    = 5
	e2eStressMaxVGDefaultDiskSize = "1Gi"
	e2eStressMaxVGNamePrefix      = "e2e-stress-vg-"
	e2eStressMaxLVGNamePrefix     = "e2e-lvg-stress-"
)

func e2eStressMaxVMBlockDevices() int {
	return e2eEnvIntPositive(e2eStressMaxVMBlockDevicesEnv, e2eStressMaxVMBlockDevicesDefault)
}

func e2eStressMaxVGTarget() int {
	target := e2eEnvIntPositive(e2eStressMaxVGTargetEnv, e2eStressMaxVGDefaultTarget)
	maxVM := e2eStressMaxVMBlockDevices()
	if target > maxVM {
		return maxVM
	}
	return target
}

func e2eStressMaxVGBatchSize() int {
	b := e2eEnvIntPositive(e2eStressMaxVGBatchSizeEnv, e2eStressMaxVGDefaultBatch)
	if t := e2eStressMaxVGTarget(); b > t {
		return t
	}
	return b
}

func e2eStressMaxVGDiskSize() string {
	if v := strings.TrimSpace(os.Getenv(e2eStressMaxVGDiskSizeEnv)); v != "" {
		return v
	}
	return e2eStressMaxVGDefaultDiskSize
}

func e2eStressMaxVGStrict() bool { return e2eEnvBool(e2eStressMaxVGStrictEnv) }

func e2eStressMaxVGMinReady(target int) int {
	if v := strings.TrimSpace(os.Getenv(e2eStressMaxVGMinReadyEnv)); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	if e2eStressMaxVGStrict() {
		return target
	}
	return 1
}

func e2eEnvBool(name string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(name))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func e2eEnvIntPositive(name string, defaultVal int) int {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return defaultVal
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return defaultVal
	}
	return n
}

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
