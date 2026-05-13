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
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/deckhouse/sds-node-configurator/e2e/cfg"
	virtv1alpha2 "github.com/deckhouse/virtualization/api/core/v1alpha2"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/storage-e2e/pkg/cluster"
	"github.com/deckhouse/storage-e2e/pkg/kubernetes"
)

// clusterResumeState mirrors storage-e2e cluster-state.json (namespace after VMs are created).
// e2eWaitConsumableBlockDeviceForVirtualDisk finds the BlockDevice for this VirtualDisk attachment the same way
// as the discovery tests: Status.Serial must equal hex(md5(VirtualDisk.UID)) or hex(md5(VMBDA.UID)).
// This avoids picking another disk on the same node (leftover LVM, other e2e disks).
func e2eWaitConsumableBlockDeviceForVirtualDisk(ctx context.Context, baseKube *rest.Config, k8sClient client.Client, ns, diskName, attachmentName, targetVM string) *v1alpha1.BlockDevice {
	baseDyn, err := dynamic.NewForConfig(baseKube)
	Expect(err).NotTo(HaveOccurred(), "dynamic client for base cluster (read VirtualDisk / VMBDA UIDs)")
	vdObj, err := baseDyn.Resource(virtualDiskGVR).Namespace(ns).Get(ctx, diskName, metav1.GetOptions{})
	Expect(err).NotTo(HaveOccurred(), "get VirtualDisk %s", diskName)
	attObj, err := baseDyn.Resource(vmbdaGVR).Namespace(ns).Get(ctx, attachmentName, metav1.GetOptions{})
	Expect(err).NotTo(HaveOccurred(), "get VirtualMachineBlockDeviceAttachment %s", attachmentName)
	serialVD := blockDeviceSerialFromVirtualDiskUID(string(vdObj.GetUID()))
	serialAtt := blockDeviceSerialFromVirtualDiskUID(string(attObj.GetUID()))

	var picked *v1alpha1.BlockDevice
	Eventually(func(g Gomega) {
		var list v1alpha1.BlockDeviceList
		g.Expect(k8sClient.List(ctx, &list, &client.ListOptions{})).To(Succeed())
		picked = nil
		for i := range list.Items {
			bd := list.Items[i]
			s := strings.TrimSpace(bd.Status.Serial)
			if s != serialVD && s != serialAtt {
				continue
			}
			if bd.Status.NodeName != targetVM {
				continue
			}
			if !bd.Status.Consumable || bd.Status.Size.IsZero() || bd.Status.Path == "" || !strings.HasPrefix(bd.Status.Path, "/dev/") {
				continue
			}
			copyBD := bd
			picked = &copyBD
			return
		}
		g.Expect(picked).NotTo(BeNil(),
			"BlockDevice for VirtualDisk %q: want Status.Serial %q or %q on node %q, consumable, with /dev path. %s",
			diskName, serialVD, serialAtt, targetVM, formatBlockDevicesHint(list.Items, targetVM))
	}, 5*time.Minute, 10*time.Second).Should(Succeed())
	return picked
}

// runLsblkViaDirectSSH connects to the node by IP the same way we connect to the master (SSH_HOST / jump → node).
// Gets node IP from the test cluster API and uses the same SSH credentials (jump host if set, VM user, key).

func e2eNewVirtClient(cfg *rest.Config) (client.Client, error) {
	sch := k8sruntime.NewScheme()
	if err := virtv1alpha2.SchemeBuilder.AddToScheme(sch); err != nil {
		return nil, err
	}
	return client.New(cfg, client.Options{Scheme: sch})
}

// e2ePatchVirtualDiskSize updates VirtualDisk .spec.persistentVolumeClaim.size (allowed mutable field).

// e2ePatchVirtualDiskSize updates VirtualDisk .spec.persistentVolumeClaim.size (allowed mutable field).
func e2ePatchVirtualDiskSize(ctx context.Context, cfg *rest.Config, namespace, diskName, newSize string) error {
	cl, err := e2eNewVirtClient(cfg)
	if err != nil {
		return err
	}
	q, err := resource.ParseQuantity(newSize)
	if err != nil {
		return fmt.Errorf("parse disk size %q: %w", newSize, err)
	}
	var vd virtv1alpha2.VirtualDisk
	key := client.ObjectKey{Namespace: namespace, Name: diskName}
	if err := cl.Get(ctx, key, &vd); err != nil {
		return fmt.Errorf("get VirtualDisk %s/%s: %w", namespace, diskName, err)
	}
	vd.Spec.PersistentVolumeClaim.Size = &q
	if err := cl.Update(ctx, &vd); err != nil {
		return fmt.Errorf("update VirtualDisk %s/%s size to %s: %w", namespace, diskName, newSize, err)
	}
	return nil
}

// e2eCleanupBaseClusterNamespaceWorkload deletes namespaced virtualization objects on the base cluster in order:
// VirtualMachineBlockDeviceAttachment → VirtualDisk → VirtualMachine, then the namespace (Deckhouse DVP).

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

// e2eApplyModuleConfigEnableStorageModule applies the shape Deckhouse expects for ModuleConfig (see module-config CRD example):
// spec.enabled, spec.version (settings schema), spec.settings. Without version/settings the controller may ignore the object.

// e2eApplyModuleConfigEnableStorageModule applies the shape Deckhouse expects for ModuleConfig (see module-config CRD example):
// spec.enabled, spec.version (settings schema), spec.settings. Without version/settings the controller may ignore the object.
func e2eApplyModuleConfigEnableStorageModule(ctx context.Context, dyn dynamic.Interface, name string) error {
	obj, err := dyn.Resource(deckhouseModuleConfigGVR).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		newObj := &unstructured.Unstructured{}
		newObj.SetAPIVersion("deckhouse.io/v1alpha1")
		newObj.SetKind("ModuleConfig")
		newObj.SetName(name)
		if err := unstructured.SetNestedMap(newObj.Object, map[string]interface{}{
			"enabled":  true,
			"version":  int64(1),
			"settings": map[string]interface{}{},
		}, "spec"); err != nil {
			return err
		}
		_, err = dyn.Resource(deckhouseModuleConfigGVR).Create(ctx, newObj, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("create ModuleConfig %s: %w", name, err)
		}
		e2eLogModuleConfigApplied(ctx, dyn, name, "created")
		return nil
	}
	if err != nil {
		return fmt.Errorf("get ModuleConfig %s: %w", name, err)
	}

	spec, found, err := unstructured.NestedMap(obj.Object, "spec")
	if err != nil || !found || spec == nil {
		spec = map[string]interface{}{}
	}
	changed := false
	if eb, ok := spec["enabled"].(bool); !ok || !eb {
		spec["enabled"] = true
		changed = true
	}
	if _, ok := spec["version"]; !ok {
		spec["version"] = int64(1)
		changed = true
	}
	if _, ok := spec["settings"]; !ok {
		spec["settings"] = map[string]interface{}{}
		changed = true
	}
	if err := unstructured.SetNestedMap(obj.Object, spec, "spec"); err != nil {
		return err
	}
	if !changed {
		e2eLogModuleConfigApplied(ctx, dyn, name, "already satisfied (enabled+version+settings)")
		return nil
	}
	_, err = dyn.Resource(deckhouseModuleConfigGVR).Update(ctx, obj, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("update ModuleConfig %s: %w", name, err)
	}
	e2eLogModuleConfigApplied(ctx, dyn, name, "updated")
	return nil
}

func e2eLogModuleConfigApplied(ctx context.Context, dyn dynamic.Interface, name, action string) {
	GinkgoWriter.Printf("    ✅ ModuleConfig %q %s (spec: enabled=true, version=1, settings={})\n", name, action)
	getCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	obj, err := dyn.Resource(deckhouseModuleConfigGVR).Get(getCtx, name, metav1.GetOptions{})
	if err != nil {
		GinkgoWriter.Printf("    ⚠️  ModuleConfig %q re-get: %v\n", name, err)
		return
	}
	if msg, ok, _ := unstructured.NestedString(obj.Object, "status", "message"); ok && msg != "" {
		GinkgoWriter.Printf("    ℹ️  ModuleConfig %q status.message: %s\n", name, msg)
	}
	if v, ok, _ := unstructured.NestedString(obj.Object, "status", "version"); ok && v != "" {
		GinkgoWriter.Printf("    ℹ️  ModuleConfig %q status.version (schema in use): %s\n", name, v)
	}
}

func e2eFormatDeckhouseModuleCondition(obj *unstructured.Unstructured, condType string) string {
	conditions, found, _ := unstructured.NestedSlice(obj.Object, "status", "conditions")
	if !found {
		return "n/a"
	}
	for _, c := range conditions {
		cm, ok := c.(map[string]interface{})
		if !ok {
			continue
		}
		t, _ := cm["type"].(string)
		if t != condType {
			continue
		}
		st, _ := cm["status"].(string)
		reason, _ := cm["reason"].(string)
		msg, _ := cm["message"].(string)
		if len(msg) > 160 {
			msg = msg[:160] + "…"
		}
		if reason != "" || msg != "" {
			return fmt.Sprintf("%s reason=%q %s", st, reason, msg)
		}
		return st
	}
	return "absent"
}

// e2eDeckhouseModuleIsReadyDiag returns IsReady condition status and short message for logging.

// e2eDeckhouseModuleIsReadyDiag returns IsReady condition status and short message for logging.
func e2eDeckhouseModuleIsReadyDiag(obj *unstructured.Unstructured) (phase, isReadyStatus, isReadyMsg string) {
	phase, _, _ = unstructured.NestedString(obj.Object, "status", "phase")
	conditions, found, _ := unstructured.NestedSlice(obj.Object, "status", "conditions")
	if !found {
		return phase, "?", ""
	}
	for _, c := range conditions {
		cm, ok := c.(map[string]interface{})
		if !ok {
			continue
		}
		t, _ := cm["type"].(string)
		if t != deckhouseModuleConditionIsReady {
			continue
		}
		isReadyStatus, _ = cm["status"].(string)
		isReadyMsg, _ = cm["message"].(string)
		return phase, isReadyStatus, isReadyMsg
	}
	return phase, "?", ""
}

func e2eLogDeckhouseStorageModulesStatus(ctx context.Context, dyn dynamic.Interface) {
	for _, name := range e2eRequiredDeckhouseStorageModules {
		getCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		obj, err := dyn.Resource(deckhouseModuleGVR).Get(getCtx, name, metav1.GetOptions{})
		cancel()
		if err != nil {
			GinkgoWriter.Printf("    … Module/%s: get: %v\n", name, err)
			continue
		}
		phase, _, _ := e2eDeckhouseModuleIsReadyDiag(obj)
		enc := e2eFormatDeckhouseModuleCondition(obj, deckhouseModuleConditionEnabledByModuleConfig)
		ird := e2eFormatDeckhouseModuleCondition(obj, deckhouseModuleConditionIsReady)
		GinkgoWriter.Printf("    … Module/%s phase=%q EnabledByModuleConfig={%s} IsReady={%s}\n", name, phase, enc, ird)
	}
}

// e2eWaitForBlockDeviceAPI polls API discovery until BlockDevice is registered.
// Deckhouse Module status often stays phase=Available while hooks run; Ready comes later — tests only need CRDs.

// e2eWaitForBlockDeviceAPI polls API discovery until BlockDevice is registered.
// Deckhouse Module status often stays phase=Available while hooks run; Ready comes later — tests only need CRDs.
func e2eWaitForBlockDeviceAPI(ctx context.Context, cfg *rest.Config, dyn dynamic.Interface, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	poll := 5 * time.Second

	GinkgoWriter.Printf("    ⏳ Waiting for BlockDevice API (storage.deckhouse.io/v1alpha1 discovery; timeout %s, poll %s)...\n", timeout, poll)

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout after %v: BlockDevice API still missing — check Module.status EnabledByModuleConfig (False = bundle/edition/dependencies block enable; ModuleConfig alone is not enough)", timeout)
		}
		err := e2eVerifyBlockDeviceAPIAvailable(cfg)
		if err == nil {
			GinkgoWriter.Printf("    ✅ BlockDevice API registered (storage.deckhouse.io/v1alpha1)\n")
			return nil
		}
		GinkgoWriter.Printf("    … %v\n", err)
		e2eLogDeckhouseStorageModulesStatus(ctx, dyn)
		time.Sleep(poll)
	}
}

func e2eVerifyBlockDeviceAPIAvailable(cfg *rest.Config) error {
	dc, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		return fmt.Errorf("discovery client: %w", err)
	}
	resources, err := dc.ServerResourcesForGroupVersion("storage.deckhouse.io/v1alpha1")
	if err != nil {
		return fmt.Errorf("storage.deckhouse.io/v1alpha1 not advertised (install sds-node-configurator / wait for CRDs): %w", err)
	}
	for i := range resources.APIResources {
		if resources.APIResources[i].Kind == "BlockDevice" {
			return nil
		}
	}
	return fmt.Errorf("BlockDevice kind not found in storage.deckhouse.io/v1alpha1 discovery")
}

// e2eEnsureDeckhouseStorageModulesReadyForUseExisting applies ModuleConfig for sds-local-volume and sds-node-configurator
// (enabled + version + settings) and waits until BlockDevice appears in API discovery (alwaysUseExisting path).
// Deckhouse may still refuse to enable modules (bundle/edition); see Module.status EnabledByModuleConfig in logs.

// e2eEnsureDeckhouseStorageModulesReadyForUseExisting applies ModuleConfig for sds-local-volume and sds-node-configurator
// (enabled + version + settings) and waits until BlockDevice appears in API discovery (alwaysUseExisting path).
// Deckhouse may still refuse to enable modules (bundle/edition); see Module.status EnabledByModuleConfig in logs.
func e2eEnsureDeckhouseStorageModulesReadyForUseExisting(ctx context.Context, res *cluster.TestClusterResources) error {
	if res == nil || res.Kubeconfig == nil {
		return fmt.Errorf("test cluster resources or kubeconfig is nil")
	}

	GinkgoWriter.Printf("[INFO]  ▶ Step 5 (e2e): Applying ModuleConfig for sds-local-volume & sds-node-configurator; waiting for BlockDevice API...\n")

	cfg := rest.CopyConfig(res.Kubeconfig)
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return fmt.Errorf("dynamic client for module ensure: %w", err)
	}

	for _, mod := range e2eRequiredDeckhouseStorageModules {
		if err := e2eApplyModuleConfigEnableStorageModule(ctx, dyn, mod); err != nil {
			return err
		}
	}

	if err := e2eWaitForBlockDeviceAPI(ctx, cfg, dyn, e2eStorageModuleReadyTimeout); err != nil {
		return err
	}

	GinkgoWriter.Printf("[INFO]  ✅ Step 5 (e2e) Complete: BlockDevice API available\n")
	return nil
}

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

func waitForVirtualizationModuleReadyIfNeeded(ctx context.Context) error {
	if os.Getenv("E2E_SKIP_VIRTUALIZATION_MODULE_WAIT") == "true" {
		GinkgoWriter.Printf("    ⏭️  Skipping virtualization Module pre-wait (E2E_SKIP_VIRTUALIZATION_MODULE_WAIT=true)\n")
		return nil
	}
	if e2eConfigTestClusterCreateMode() != testClusterModeCreateNew {
		return nil
	}

	e2eCfg := cfg.Load()

	kubeconfigDir, err := e2eTestTempDirFromStack()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(kubeconfigDir, 0o755); err != nil {
		return fmt.Errorf("mkdir kubeconfig dir for virtualization pre-wait: %w", err)
	}

	var opts cluster.ConnectClusterOptions

	if e2eCfg.SSH.Jump.Host != "" {
		opts = cluster.ConnectClusterOptions{
			SSHUser: e2eCfg.SSH.Jump.User, SSHHost: e2eCfg.SSH.Jump.Host, SSHKeyPath: e2eCfg.SSH.Jump.PrivateKeyPath,
			UseJumpHost: true, TargetUser: e2eCfg.SSH.User, TargetHost: e2eCfg.SSH.Host, TargetKeyPath: e2eCfg.SSH.PrivateKey,
			KubeconfigOutputDir: kubeconfigDir,
		}
	} else {
		opts = cluster.ConnectClusterOptions{
			SSHUser: e2eCfg.SSH.User, SSHHost: e2eCfg.SSH.Host, SSHKeyPath: e2eCfg.SSH.PrivateKey,
			UseJumpHost: false, KubeconfigOutputDir: kubeconfigDir,
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

// e2eListVirtualMachineNamesRunningOnly returns VM names whose status.phase is Running.
// Attach to Migrating/Starting VMs often hangs VMBDA in Pending — do not use those for disk attach.

// e2eListVirtualMachineNamesRunningOnly returns VM names whose status.phase is Running.
// Attach to Migrating/Starting VMs often hangs VMBDA in Pending — do not use those for disk attach.
func e2eListVirtualMachineNamesRunningOnly(ctx context.Context, baseKube *rest.Config, ns string) ([]string, error) {
	cl, err := e2eNewVirtClient(baseKube)
	if err != nil {
		return nil, err
	}
	var list virtv1alpha2.VirtualMachineList
	if err := cl.List(ctx, &list, client.InNamespace(ns)); err != nil {
		return nil, err
	}
	names := make([]string, 0, len(list.Items))
	for i := range list.Items {
		if list.Items[i].Status.Phase == virtv1alpha2.MachineRunning {
			names = append(names, list.Items[i].Name)
		}
	}
	return names, nil
}

// e2eExcludeBootstrapGuestVMs removes bootstrap guest VMs (prefix bootstrap-node-) from attach candidate lists.

// e2eExcludeBootstrapGuestVMs removes bootstrap guest VMs (prefix bootstrap-node-) from attach candidate lists.
func e2eExcludeBootstrapGuestVMs(names []string) []string {
	if len(names) == 0 {
		return names
	}
	out := make([]string, 0, len(names))
	for _, n := range names {
		if strings.HasPrefix(n, e2eBootstrapGuestVMPrefix) {
			continue
		}
		out = append(out, n)
	}
	return out
}

// e2eIntersectVMNamesRunning keeps only Running VMs. If candidates is non-empty but none are Running (e.g. Migrating),
// falls back to all Running VMs in the namespace and logs a warning.

// e2eIntersectVMNamesRunning keeps only Running VMs. If candidates is non-empty but none are Running (e.g. Migrating),
// falls back to all Running VMs in the namespace and logs a warning.
func e2eIntersectVMNamesRunning(ctx context.Context, baseKube *rest.Config, ns string, candidates []string) []string {
	running, err := e2eListVirtualMachineNamesRunningOnly(ctx, baseKube, ns)
	Expect(err).NotTo(HaveOccurred(), "list VirtualMachines on base cluster")
	running = e2eExcludeBootstrapGuestVMs(running)
	candidates = e2eExcludeBootstrapGuestVMs(candidates)
	Expect(running).NotTo(BeEmpty(),
		"no non-bootstrap guest VMs in phase Running in namespace %s (Migrating/Starting/bootstrap-only)", ns)

	if len(candidates) == 0 {
		return running
	}
	allowed := make(map[string]struct{}, len(running))
	for _, n := range running {
		allowed[n] = struct{}{}
	}
	var out []string
	for _, c := range candidates {
		if _, ok := allowed[c]; ok {
			out = append(out, c)
		}
	}
	if len(out) > 0 {
		return out
	}
	GinkgoWriter.Printf("    ⚠️  no Running non-bootstrap VM among candidates %v (e.g. Migrating); using all Running non-bootstrap VMs in namespace: %v\n", candidates, running)
	return running
}

// e2eListClusterVMNames returns guest VM names to attach disks to (same pattern as other LVM tests).

// e2eListClusterVMNames returns guest VM names to attach disks to (same pattern as other LVM tests).
func e2eListClusterVMNames(ctx context.Context, res *cluster.TestClusterResources, ns string) []string {
	var candidates []string
	if res.VMResources != nil {
		for _, name := range res.VMResources.VMNames {
			if name != res.VMResources.SetupVMName {
				candidates = append(candidates, name)
			}
		}
	}
	return e2eIntersectVMNamesRunning(ctx, res.BaseKubeconfig, ns, candidates)
}

func e2eConfigVMSSHUser() string {
	if v := os.Getenv("SSH_VM_USER"); v != "" {
		return v
	}
	return e2eDefaultVMSSHUser
}

// e2eAttachVirtualDiskToVM mirrors storage-e2e AttachVirtualDiskToVM but treats AlreadyExists on VirtualDisk
// or VirtualMachineBlockDeviceAttachment as success. That way attachVirtualDiskWithRetry recovers when the first
// attempt created the VirtualDisk and failed on VMBDA (retries no longer hit 409 on VD create).

// e2eAttachVirtualDiskToVM mirrors storage-e2e AttachVirtualDiskToVM but treats AlreadyExists on VirtualDisk
// or VirtualMachineBlockDeviceAttachment as success. That way attachVirtualDiskWithRetry recovers when the first
// attempt created the VirtualDisk and failed on VMBDA (retries no longer hit 409 on VD create).
func e2eAttachVirtualDiskToVM(ctx context.Context, baseKubeconfig *rest.Config, config kubernetes.VirtualDiskAttachmentConfig) (*kubernetes.VirtualDiskAttachmentResult, error) {
	if config.VMName == "" || config.Namespace == "" || config.DiskSize == "" || config.StorageClassName == "" {
		return nil, fmt.Errorf("VirtualDiskAttachmentConfig: VMName, Namespace, DiskSize, StorageClassName are required")
	}
	diskName := config.DiskName
	if diskName == "" {
		diskName = fmt.Sprintf("%s-data-disk", config.VMName)
	}
	attachmentName := fmt.Sprintf("%s-attachment", diskName)

	cl, err := e2eNewVirtClient(baseKubeconfig)
	if err != nil {
		return nil, err
	}
	diskSize, err := resource.ParseQuantity(config.DiskSize)
	if err != nil {
		return nil, fmt.Errorf("parse disk size %q: %w", config.DiskSize, err)
	}
	sc := config.StorageClassName
	vd := &virtv1alpha2.VirtualDisk{
		ObjectMeta: metav1.ObjectMeta{Name: diskName, Namespace: config.Namespace},
		Spec: virtv1alpha2.VirtualDiskSpec{
			PersistentVolumeClaim: virtv1alpha2.VirtualDiskPersistentVolumeClaim{
				Size:         &diskSize,
				StorageClass: &sc,
			},
		},
	}
	err = cl.Create(ctx, vd)
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			GinkgoWriter.Printf("    ℹ️  VirtualDisk %s/%s already exists (idempotent attach)\n", config.Namespace, diskName)
		} else {
			return nil, fmt.Errorf("create VirtualDisk %s: %w", diskName, err)
		}
	}

	att := &virtv1alpha2.VirtualMachineBlockDeviceAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: attachmentName, Namespace: config.Namespace},
		Spec: virtv1alpha2.VirtualMachineBlockDeviceAttachmentSpec{
			VirtualMachineName: config.VMName,
			BlockDeviceRef: virtv1alpha2.VMBDAObjectRef{
				Kind: virtv1alpha2.VMBDAObjectRefKindVirtualDisk,
				Name: diskName,
			},
		},
	}
	err = cl.Create(ctx, att)
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			GinkgoWriter.Printf("    ℹ️  VirtualMachineBlockDeviceAttachment %s/%s already exists (idempotent attach)\n", config.Namespace, attachmentName)
		} else {
			return nil, fmt.Errorf("create VirtualMachineBlockDeviceAttachment %s: %w", attachmentName, err)
		}
	}

	return &kubernetes.VirtualDiskAttachmentResult{
		DiskName:       diskName,
		AttachmentName: attachmentName,
	}, nil
}

func attachVirtualDiskWithRetry(ctx context.Context, baseKubeconfig *rest.Config, config kubernetes.VirtualDiskAttachmentConfig, maxRetries int, retryInterval time.Duration) (*kubernetes.VirtualDiskAttachmentResult, error) {
	var lastErr error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		att, err := e2eAttachVirtualDiskToVM(ctx, baseKubeconfig, config)
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
