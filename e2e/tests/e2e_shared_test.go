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
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	. "github.com/onsi/ginkgo/v2"

	"github.com/deckhouse/storage-e2e/pkg/cluster"
)

const testClusterModeCreateNew = "alwaysCreateNew"

// e2eOrphanNestedClusterState matches storage-e2e resumeState (cluster-state.json after step 6).
type e2eOrphanNestedClusterState struct {
	FirstMasterIP string   `json:"first_master_ip"`
	Namespace     string   `json:"namespace"`
	VMNames       []string `json:"vm_names"`
	SetupVMName   string   `json:"setup_vm_name"`
}

func e2eRepoRootFromE2EModule() (string, error) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("runtime.Caller failed")
	}
	// .../e2e/tests/<this file>.go -> repo root (parent of e2e/)
	e2eMod := filepath.Dir(filepath.Dir(file))
	return filepath.Abs(filepath.Join(e2eMod, ".."))
}

func e2eNestedClusterStateJSONPath(testFileBase string) (string, error) {
	root, err := e2eRepoRootFromE2EModule()
	if err != nil {
		return "", err
	}
	return filepath.Join(root, "temp", testFileBase, "cluster-state.json"), nil
}

// e2eTryCleanupOrphanNestedClusterFromStateFile removes nested test-cluster VMs when CreateTestCluster failed
// after VMs were created (cluster-state.json exists) but before *TestClusterResources were returned — AfterAll
// then skips CleanupTestCluster because testClusterResources is still nil.
func e2eTryCleanupOrphanNestedClusterFromStateFile(ctx context.Context, testFileBase string) {
	if e2eConfigTestClusterCreateMode() != testClusterModeCreateNew {
		return
	}
	statePath, err := e2eNestedClusterStateJSONPath(testFileBase)
	if err != nil {
		GinkgoWriter.Printf("    ⚠️  Orphan nested-cluster cleanup: state path: %v\n", err)
		return
	}
	data, err := os.ReadFile(statePath)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		GinkgoWriter.Printf("    ⚠️  Orphan nested-cluster cleanup: read %s: %v\n", statePath, err)
		return
	}
	var st e2eOrphanNestedClusterState
	if err := json.Unmarshal(data, &st); err != nil {
		GinkgoWriter.Printf("    ⚠️  Orphan nested-cluster cleanup: parse %s: %v\n", statePath, err)
		return
	}
	if st.Namespace == "" || len(st.VMNames) == 0 {
		GinkgoWriter.Printf("    ⚠️  Orphan nested-cluster cleanup: invalid state in %s (missing namespace or vm_names)\n", statePath)
		return
	}

	sshKeyPath, err := cluster.GetSSHPrivateKeyPath()
	if err != nil {
		GinkgoWriter.Printf("    ⚠️  Orphan nested-cluster cleanup: SSH key: %v\n", err)
		return
	}
	sshHost := e2eConfigSSHHost()
	sshUser := e2eConfigSSHUser()
	if sshHost == "" || sshUser == "" {
		GinkgoWriter.Printf("    ⚠️  Orphan nested-cluster cleanup: SSH_HOST / SSH_USER not set\n")
		return
	}
	useJumpHost := e2eConfigSSHJumpHost() != ""
	kubeconfigDir := filepath.Dir(statePath)
	baseConnectOpts := cluster.ConnectClusterOptions{
		SSHUser: sshUser, SSHHost: sshHost, SSHKeyPath: sshKeyPath,
		UseJumpHost: useJumpHost, KubeconfigOutputDir: kubeconfigDir,
	}
	if useJumpHost {
		jumpUser := e2eConfigSSHJumpUser()
		if jumpUser == "" {
			jumpUser = sshUser
		}
		jumpHost := e2eConfigSSHJumpHost()
		jumpKeyPath := e2eConfigSSHJumpKeyPath()
		if jumpKeyPath == "" {
			jumpKeyPath = sshKeyPath
		}
		baseConnectOpts = cluster.ConnectClusterOptions{
			SSHUser: jumpUser, SSHHost: jumpHost, SSHKeyPath: jumpKeyPath,
			UseJumpHost: true, TargetUser: sshUser, TargetHost: sshHost, TargetKeyPath: sshKeyPath,
			KubeconfigOutputDir: kubeconfigDir,
		}
	}

	GinkgoWriter.Printf("    ▶️  Orphan nested-cluster cleanup: connecting to base cluster to remove VMs in %s (state %s)...\n", st.Namespace, statePath)
	baseRes, err := cluster.ConnectToCluster(ctx, baseConnectOpts)
	if err != nil {
		GinkgoWriter.Printf("    ⚠️  Orphan nested-cluster cleanup: ConnectToCluster: %v\n", err)
		return
	}

	cleanupRes := &cluster.TestClusterResources{
		BaseClusterClient:  baseRes.SSHClient,
		BaseKubeconfig:     baseRes.Kubeconfig,
		BaseKubeconfigPath: baseRes.KubeconfigPath,
		BaseTunnelInfo:     baseRes.TunnelInfo,
		VMResources: &cluster.VMResources{
			Namespace:   st.Namespace,
			VMNames:     st.VMNames,
			SetupVMName: st.SetupVMName,
		},
	}
	if err := cluster.CleanupTestCluster(ctx, cleanupRes); err != nil {
		GinkgoWriter.Printf("    ⚠️  Orphan nested-cluster cleanup: CleanupTestCluster: %v\n", err)
		return
	}
	GinkgoWriter.Printf("    ✅ Orphan nested-cluster cleanup finished (removed VMs for %s)\n", st.Namespace)
	_ = os.Remove(statePath)
}
