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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/deckhouse/storage-e2e/pkg/cluster"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sclient "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	. "github.com/onsi/ginkgo/v2"
)

func e2eExpandKubeConfigPath() string {
	p := strings.TrimSpace(e2eConfigKubeConfigPath())
	if p == "" {
		return ""
	}
	if strings.HasPrefix(p, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return p
		}
		return filepath.Join(home, p[2:])
	}
	return p
}

func e2eNoClusterLockRetry() bool {
	v := os.Getenv("E2E_NO_CLUSTER_LOCK_RETRY")
	return v == "true" || v == "True" || v == "1"
}

func e2eIsClusterLockDenied(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "already locked") || strings.Contains(s, "failed to acquire cluster lock")
}

// e2eTryDeleteClusterLockViaKubeconfig deletes storage-e2e lock ConfigMap using KUBE_CONFIG_PATH (test cluster API).
// Fails when kubeconfig points at localhost without a tunnel (typical admin.conf); use e2eForceReleaseClusterLockViaSSH then.
func e2eTryDeleteClusterLockViaKubeconfig(ctx context.Context) error {
	path := e2eExpandKubeConfigPath()
	if path == "" {
		return fmt.Errorf("KUBE_CONFIG_PATH is empty")
	}
	restCfg, err := clientcmd.BuildConfigFromFlags("", path)
	if err != nil {
		return fmt.Errorf("load kubeconfig %s: %w", path, err)
	}
	cs, err := k8sclient.NewForConfig(restCfg)
	if err != nil {
		return fmt.Errorf("kubernetes client: %w", err)
	}
	err = cs.CoreV1().ConfigMaps(cluster.ClusterLockNamespace).Delete(ctx, cluster.ClusterLockConfigMapName, metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

// e2eForceReleaseClusterLockViaSSH opens the same SSH + API tunnel as UseExistingCluster (step 1) and deletes the lock ConfigMap.
// Use when KUBE_CONFIG_PATH uses 127.0.0.1 and nothing is listening until storage-e2e establishes the tunnel.
func e2eForceReleaseClusterLockViaSSH(ctx context.Context) error {
	sshHost := e2eConfigSSHHost()
	sshUser := e2eConfigSSHUser()
	if sshHost == "" || sshUser == "" {
		return fmt.Errorf("SSH_HOST and SSH_USER are required for SSH lock release")
	}
	sshKeyPath, err := cluster.GetSSHPrivateKeyPath()
	if err != nil {
		return err
	}
	tmpDir, err := os.MkdirTemp("", "e2e-lock-release-kube-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	var opts cluster.ConnectClusterOptions
	if e2eConfigSSHJumpHost() != "" {
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
			KubeconfigOutputDir: tmpDir,
		}
	} else {
		opts = cluster.ConnectClusterOptions{
			SSHUser: sshUser, SSHHost: sshHost, SSHKeyPath: sshKeyPath,
			UseJumpHost: false, KubeconfigOutputDir: tmpDir,
		}
	}

	res, err := cluster.ConnectToCluster(ctx, opts)
	if err != nil {
		return fmt.Errorf("SSH connect for lock release: %w", err)
	}
	defer func() {
		if res.TunnelInfo != nil && res.TunnelInfo.StopFunc != nil {
			_ = res.TunnelInfo.StopFunc()
		}
		if res.SSHClient != nil {
			_ = res.SSHClient.Close()
		}
	}()

	if err := cluster.ForceReleaseClusterLock(ctx, res.Kubeconfig); err != nil {
		return fmt.Errorf("ForceReleaseClusterLock: %w", err)
	}
	return nil
}

// e2eConnectUseExistingClusterOnceOrRetryAfterLockDelete calls cluster.UseExistingCluster; on stale lock error it deletes
// default/e2e-cluster-lock via KUBE_CONFIG_PATH and retries once. Set E2E_NO_CLUSTER_LOCK_RETRY=true to disable.
func e2eConnectUseExistingClusterOnceOrRetryAfterLockDelete() (*cluster.TestClusterResources, error) {
	ctx, cancel := context.WithTimeout(context.Background(), e2eUseExistingClusterTimeout)
	defer cancel()
	res, err := cluster.UseExistingCluster(ctx)
	if err == nil {
		return res, nil
	}
	if !e2eIsClusterLockDenied(err) || e2eNoClusterLockRetry() {
		return nil, err
	}
	GinkgoWriter.Printf("    ▶️  Cluster lock present; clearing lock and retrying once...\n")
	lockCtx, lockCancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer lockCancel()

	var clearErr error
	if e2eExpandKubeConfigPath() != "" {
		clearErr = e2eTryDeleteClusterLockViaKubeconfig(lockCtx)
		if clearErr != nil {
			GinkgoWriter.Printf("    ℹ️  Lock delete via KUBE_CONFIG_PATH failed (%v); if server is 127.0.0.1, tunnel was not up — trying SSH...\n", clearErr)
		}
	} else {
		clearErr = fmt.Errorf("KUBE_CONFIG_PATH empty")
		GinkgoWriter.Printf("    ℹ️  %v; trying SSH to clear lock...\n", clearErr)
	}
	if clearErr != nil {
		if sshErr := e2eForceReleaseClusterLockViaSSH(lockCtx); sshErr != nil {
			GinkgoWriter.Printf("    ⚠️  Could not clear cluster lock (file: %v; SSH: %v)\n", clearErr, sshErr)
			return nil, err
		}
	}
	GinkgoWriter.Printf("    ▶️  Retrying connection after lock cleared...\n")
	ctx2, cancel2 := context.WithTimeout(context.Background(), e2eUseExistingClusterTimeout)
	defer cancel2()
	return cluster.UseExistingCluster(ctx2)
}

func e2ePrintStaleClusterLockHint(err error) {
	if err == nil {
		return
	}
	if !e2eIsClusterLockDenied(err) {
		return
	}
	kb := e2eExpandKubeConfigPath()
	GinkgoWriter.Printf("\n    --- stale cluster lock (storage-e2e ConfigMap %s/%s on the test cluster API) ---\n",
		cluster.ClusterLockNamespace, cluster.ClusterLockConfigMapName)
	GinkgoWriter.Printf("    Retry uses KUBE_CONFIG_PATH if the API is reachable; otherwise SSH+tunnel (same as test connect).\n")
	GinkgoWriter.Printf("    Manual: export TEST_CLUSTER_FORCE_LOCK_RELEASE=true before go test, or kubectl delete configmap %s -n %s\n",
		cluster.ClusterLockConfigMapName, cluster.ClusterLockNamespace)
	if kb != "" {
		GinkgoWriter.Printf("    Example: kubectl --kubeconfig=%q delete configmap %s -n %s\n",
			kb, cluster.ClusterLockConfigMapName, cluster.ClusterLockNamespace)
	}
	GinkgoWriter.Printf("    Disable auto-retry: E2E_NO_CLUSTER_LOCK_RETRY=true\n\n")
}
