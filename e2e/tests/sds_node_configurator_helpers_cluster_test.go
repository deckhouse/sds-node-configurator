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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	e2ecfg "github.com/deckhouse/sds-node-configurator/e2e/cfg"
	virtv1alpha2 "github.com/deckhouse/virtualization/api/core/v1alpha2"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sclient "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/storage-e2e/pkg/cluster"
)

// clusterResumeState mirrors storage-e2e cluster-state.json (namespace after VMs are created).
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
	cfg := e2ecfg.Load()
	if ns == "" {
		ns = e2eConfigNamespace()
		GinkgoWriter.Printf("    ▶️  cleanup: namespace from TEST_CLUSTER_NAMESPACE=%q\n", ns)
	} else {
		GinkgoWriter.Printf("    ▶️  cleanup: namespace from cluster-state.json: %q\n", ns)
	}

	var opts cluster.ConnectClusterOptions
	if cfg.SSH.Jump.Host != "" {
		opts = cluster.ConnectClusterOptions{
			SSHUser: cfg.SSH.User, SSHHost: cfg.SSH.Host, SSHKeyPath: cfg.SSH.PrivateKey,
			UseJumpHost:         false,
			KubeconfigOutputDir: kubeconfigDir,
		}
	} else {
		opts = cluster.ConnectClusterOptions{
			SSHUser: cfg.SSH.User, SSHHost: cfg.SSH.Jump.Host, SSHKeyPath: cfg.SSH.Jump.PrivateKeyPath,
			UseJumpHost: true, TargetUser: cfg.SSH.User, TargetHost: cfg.SSH.Host, TargetKeyPath: cfg.SSH.PrivateKey,
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

func e2eRegisterNestedTestCluster(r *cluster.TestClusterResources) {
	e2eNestedTestCluster = r
}

func e2eNestedTestClusterOrNil() *cluster.TestClusterResources {
	return e2eNestedTestCluster
}

// e2eModuleRootDir returns the e2e Go module root (directory containing ./tests), from this source file.

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
