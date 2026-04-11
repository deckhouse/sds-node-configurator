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
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/deckhouse/storage-e2e/pkg/cluster"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// Timeouts aligned with github.com/deckhouse/storage-e2e/internal/config (ClusterCreationTimeout, ModuleDeployTimeout).
const (
	e2eClusterCreationTimeout = 90 * time.Minute
	e2eModuleDeployTimeout    = 15 * time.Minute
)

// clusterResumeState mirrors storage-e2e pkg/cluster resumeState (cluster-state.json after step 6).
type clusterResumeState struct {
	Namespace string `json:"namespace"`
}

// createE2EAlwaysNewClusterWithSyncCleanupOnFailure runs the same steps as storage-e2e CreateOrConnectToTestCluster
// for alwaysCreateNew, but on failure deletes the test namespace on the base cluster (VMs + namespaced resources)
// and closes connections before Gomega fails the spec — so resources are not left to AfterAll.
func createE2EAlwaysNewClusterWithSyncCleanupOnFailure() *cluster.TestClusterResources {
	statePath, statePathErr := e2eClusterStateJSONPath()

	yamlName := os.Getenv("YAML_CONFIG_FILENAME")
	if yamlName == "" {
		yamlName = "cluster_config.yml"
	}

	createCtx, cancel := context.WithTimeout(context.Background(), e2eClusterCreationTimeout)
	defer cancel()

	res, err := cluster.CreateTestCluster(createCtx, yamlName)
	if err != nil {
		GinkgoWriter.Printf("    ▶️  CreateTestCluster failed; deleting test namespace on base cluster before the spec exits...\n")
		syncDeleteE2ETestNamespaceOnBaseCluster(context.Background(), statePath, statePathErr)
		Expect(err).NotTo(HaveOccurred(), "Test cluster should be created successfully")
	}

	waitCtx, cancel2 := context.WithTimeout(context.Background(), e2eModuleDeployTimeout)
	defer cancel2()
	err = cluster.WaitForTestClusterReady(waitCtx, res)
	if err != nil {
		GinkgoWriter.Printf("    ▶️  WaitForTestClusterReady failed; deleting namespace and running storage-e2e cleanup before the spec exits...\n")
		syncDeleteE2ETestNamespaceOnBaseCluster(context.Background(), statePath, statePathErr)
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

// syncDeleteE2ETestNamespaceOnBaseCluster connects to the base (Deckhouse) cluster and deletes the e2e workload namespace,
// which removes VirtualMachines and other namespaced objects created for the test cluster.
func syncDeleteE2ETestNamespaceOnBaseCluster(ctx context.Context, clusterStatePath string, statePathErr error) {
	kubeconfigDir, cleanupDir, err := kubeconfigDirForNamespaceDelete(clusterStatePath, statePathErr)
	if err != nil {
		GinkgoWriter.Printf("    ⚠️  Namespace delete: %v\n", err)
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
			GinkgoWriter.Printf("    ⚠️  Namespace delete: read %s: %v\n", clusterStatePath, readErr)
		}
	}
	if ns == "" {
		ns = e2eConfigNamespace()
		GinkgoWriter.Printf("    ▶️  Namespace delete: using TEST_CLUSTER_NAMESPACE=%q\n", ns)
	} else {
		GinkgoWriter.Printf("    ▶️  Namespace delete: target namespace %q (from cluster-state.json when present)\n", ns)
	}

	sshHost := e2eConfigSSHHost()
	sshUser := e2eConfigSSHUser()
	if sshHost == "" || sshUser == "" {
		GinkgoWriter.Printf("    ⚠️  Namespace delete: SSH_HOST or SSH_USER not set, skip\n")
		return
	}

	sshKeyPath, err := cluster.GetSSHPrivateKeyPath()
	if err != nil {
		GinkgoWriter.Printf("    ⚠️  Namespace delete: SSH key: %v\n", err)
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
		GinkgoWriter.Printf("    ⚠️  Namespace delete: connect to base cluster: %v\n", err)
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

	cs, err := kubernetes.NewForConfig(baseRes.Kubeconfig)
	if err != nil {
		GinkgoWriter.Printf("    ⚠️  Namespace delete: kubernetes client: %v\n", err)
		return
	}

	delCtx, cancel := context.WithTimeout(ctx, e2eClusterCleanupTimeout)
	defer cancel()

	err = cs.CoreV1().Namespaces().Delete(delCtx, ns, metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		GinkgoWriter.Printf("    ℹ️  Namespace delete: %q already absent\n", ns)
		if clusterStatePath != "" {
			_ = os.Remove(clusterStatePath)
		}
		return
	}
	if err != nil {
		GinkgoWriter.Printf("    ⚠️  Namespace delete: %q: %v\n", ns, err)
		return
	}
	GinkgoWriter.Printf("    ✅ Namespace delete: removal of %q submitted (VMs and namespaced resources)\n", ns)

	if clusterStatePath != "" {
		_ = os.Remove(clusterStatePath)
	}
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
