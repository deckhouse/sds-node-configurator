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
	"time"

	. "github.com/onsi/ginkgo/v2"

	"github.com/deckhouse/storage-e2e/pkg/kubernetes"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
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

	// Pre-wait before cluster.CreateTestCluster: storage-e2e step 3 uses a 10s timeout with no retry;
	// Deckhouse Module "virtualization" often stays in Reconciling longer than that.
	e2eVirtualizationModuleWaitDefault = 25 * time.Minute
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

// waitForVirtualizationModuleReadyIfNeeded polls Module/virtualization until status.phase is Ready.
// storage-e2e step 3 uses a single short Get; we wait while phase is Reconciling etc.
// Does not use storage-e2e kubernetes.WaitForModuleReady: that helper never applies the deadline when GetModule errors (infinite loop).
// No-op if TEST_CLUSTER_CREATE_MODE is not alwaysCreateNew, KUBE_CONFIG_PATH is unset, or E2E_SKIP_VIRTUALIZATION_MODULE_WAIT=true.
func waitForVirtualizationModuleReadyIfNeeded(ctx context.Context) error {
	if os.Getenv("E2E_SKIP_VIRTUALIZATION_MODULE_WAIT") == "true" {
		GinkgoWriter.Printf("    ⏭️  Skipping virtualization Module pre-wait (E2E_SKIP_VIRTUALIZATION_MODULE_WAIT=true)\n")
		return nil
	}
	if e2eConfigTestClusterCreateMode() != testClusterModeCreateNew {
		return nil
	}
	path := e2eConfigKubeConfigPath()
	if path == "" {
		return nil
	}
	cfg, err := clientcmd.BuildConfigFromFlags("", path)
	if err != nil {
		return fmt.Errorf("load kubeconfig for virtualization wait: %w", err)
	}
	// Avoid hanging forever on a stuck TCP dial / TLS handshake (rest.Timeout alone may not cap dial).
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

func e2eConfigVMSSHUser() string {
	if v := os.Getenv("SSH_VM_USER"); v != "" {
		return v
	}
	return e2eDefaultVMSSHUser
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
