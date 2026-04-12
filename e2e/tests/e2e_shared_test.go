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
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"

	"github.com/deckhouse/storage-e2e/pkg/cluster"
	"github.com/deckhouse/storage-e2e/pkg/kubernetes"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

// Defaults (align with storage-e2e internal/config when using setup.Init()).
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

	// Pre-wait before cluster.CreateTestCluster (after SSH+tunnel): storage-e2e step 3 uses a short Get with no Reconciling retry.
	e2eVirtualizationModuleWaitDefault = 25 * time.Minute

	// Direct SSH to nodes for lsblk can hit transient "handshake failed: EOF" (sshd/network) after heavy I/O.
	e2eLsblkSSHMaxRetries    = 6
	e2eLsblkSSHRetryInterval = 15 * time.Second

	// alwaysCreateNew / alwaysUseExisting timeouts (sds suite cluster helpers).
	e2eClusterCreationTimeout      = 90 * time.Minute
	e2eModuleDeployTimeout         = 15 * time.Minute
	e2eUseExistingClusterTimeout   = 90 * time.Minute // storage-e2e ClusterCreationTimeout (connect + lock + health)
)

const testClusterModeCreateNew = "alwaysCreateNew"

var deckhouseModuleGVR = schema.GroupVersionResource{
	Group:    "deckhouse.io",
	Version:  "v1alpha1",
	Resource: "modules",
}

// deckhouseModuleConditionIsReady matches ModuleConditionIsReady in deckhouse.io API.
const deckhouseModuleConditionIsReady = "IsReady"

// moduleVirtualizationIsReady returns true if status.phase is Ready or condition IsReady is True.
// Some clusters keep phase as Reconciling while IsReady becomes True first.
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

// e2eTestTempDirFromStack returns e2e/temp/<test-file-name>/ (same layout as storage-e2e CreateTestCluster kubeconfig dir).
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

// waitForVirtualizationModuleReadyWithRestConfig polls Module/virtualization until Ready (phase or IsReady).
// cfg must come from an active API connection (e.g. after SSH tunnel from cluster.ConnectToCluster).
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

// waitForVirtualizationModuleReadyIfNeeded polls Module/virtualization before CreateTestCluster so storage-e2e step 3
// (short timeout, phase-only) does not fail while the module is still Reconciling.
//
// KUBE_CONFIG_PATH alone is not sufficient: CI kubeconfig points at 127.0.0.1:<tunnel-port>, but the SSH tunnel only
// exists after cluster.ConnectToCluster inside CreateTestCluster. We open the same SSH+tunnel here, wait, then close.
// No-op if TEST_CLUSTER_CREATE_MODE is not alwaysCreateNew or E2E_SKIP_VIRTUALIZATION_MODULE_WAIT=true.
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
	// KUBE_CONFIG_PATH is set by CI from E2E_CLUSTER_KUBECONFIG (written to file) or directly as path.
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

// e2eSharedTestClusterResources is the single nested test cluster for a full TestE2E run (both Ordered Describes).
// The first suite that calls CreateOrConnectToTestCluster registers it here; the second reuses it.
// CleanupTestCluster runs once in AfterSuite — intermediate AfterAll blocks must not destroy VMs.
var e2eSharedTestClusterResources *cluster.TestClusterResources

func e2eSetSharedTestClusterResources(r *cluster.TestClusterResources) {
	e2eSharedTestClusterResources = r
}

func e2eTakeSharedTestClusterResourcesIfReady() *cluster.TestClusterResources {
	return e2eSharedTestClusterResources
}

// e2eCleanupSharedTestClusterAfterSuite tears down nested cluster resources (respects TEST_CLUSTER_CLEANUP).
func e2eCleanupSharedTestClusterAfterSuite() {
	if e2eSharedTestClusterResources == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), e2eClusterCleanupTimeout)
	defer cancel()

	cleanupEnabled := e2eConfigTestClusterCleanup() == "true" || e2eConfigTestClusterCleanup() == "True"
	if cleanupEnabled {
		GinkgoWriter.Printf("    ▶️ AfterSuite: cleaning up test cluster resources (TEST_CLUSTER_CLEANUP is enabled - all VMs will be removed)...\n")
	} else {
		GinkgoWriter.Printf("    ▶️ AfterSuite: cleaning up test cluster resources (TEST_CLUSTER_CLEANUP is not enabled - only bootstrap node will be removed)...\n")
	}
	err := cluster.CleanupTestCluster(ctx, e2eSharedTestClusterResources)
	if err != nil {
		GinkgoWriter.Printf("    ⚠️  Warning: AfterSuite cluster cleanup errors: %v\n", err)
	} else {
		GinkgoWriter.Printf("    ✅ AfterSuite: test cluster resources cleaned up successfully\n")
	}
	e2eSharedTestClusterResources = nil
}

func e2eConfigVMSSHUser() string {
	if v := os.Getenv("SSH_VM_USER"); v != "" {
		return v
	}
	return e2eDefaultVMSSHUser
}

// e2ePrintStaleClusterLockHint logs guidance when alwaysUseExisting fails on the cluster lock.
func e2ePrintStaleClusterLockHint(err error) {
	if err == nil {
		return
	}
	GinkgoWriter.Printf("    Hint: if the lock is stale, run once with TEST_CLUSTER_FORCE_LOCK_RELEASE=true or delete ConfigMap %s/%s. (%v)\n",
		cluster.ClusterLockNamespace, cluster.ClusterLockConfigMapName, err)
}

// e2eConnectUseExistingClusterOnceOrRetryAfterLockDelete connects via storage-e2e UseExistingCluster (SSH, lock, health checks).
func e2eConnectUseExistingClusterOnceOrRetryAfterLockDelete() (*cluster.TestClusterResources, error) {
	ctx, cancel := context.WithTimeout(context.Background(), e2eUseExistingClusterTimeout)
	defer cancel()
	return cluster.UseExistingCluster(ctx)
}

// attachVirtualDiskWithRetry calls AttachVirtualDiskToVM up to maxRetries times with retryInterval between attempts.
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

// blockDeviceSerialFromVirtualDiskUID returns the BlockDevice serial (hex-encoded MD5 of UID).
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
