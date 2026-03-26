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
	"os"
	"time"

	"github.com/deckhouse/storage-e2e/pkg/kubernetes"
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
)

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
