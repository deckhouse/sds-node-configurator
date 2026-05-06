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
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/storage-e2e/pkg/cluster"
)

// clusterResumeState mirrors storage-e2e cluster-state.json (namespace after VMs are created).
func printLVMVolumeGroupInfo(lvg *v1alpha1.LVMVolumeGroup) {
	GinkgoWriter.Println("\n========== LVMVolumeGroup information ==========")
	GinkgoWriter.Printf("Name: %s\n", lvg.Name)
	GinkgoWriter.Println("--- Spec ---")
	GinkgoWriter.Printf("  Type: %s\n", lvg.Spec.Type)
	GinkgoWriter.Printf("  ActualVGNameOnTheNode: %s\n", lvg.Spec.ActualVGNameOnTheNode)
	GinkgoWriter.Printf("  Local.NodeName: %s\n", lvg.Spec.Local.NodeName)
	if sel := lvg.Spec.BlockDeviceSelector; sel != nil {
		if len(sel.MatchLabels) > 0 {
			GinkgoWriter.Printf("  BlockDeviceSelector.MatchLabels: %v\n", sel.MatchLabels)
		}
		for i, me := range sel.MatchExpressions {
			GinkgoWriter.Printf("  BlockDeviceSelector.MatchExpressions[%d]: Key=%s Operator=%s Values=%v\n",
				i, me.Key, me.Operator, me.Values)
		}
	}
	for i, tp := range lvg.Spec.ThinPools {
		GinkgoWriter.Printf("  ThinPools[%d]: Name=%s Size=%s AllocationLimit=%s\n", i, tp.Name, tp.Size, tp.AllocationLimit)
	}
	GinkgoWriter.Println("--- Status ---")
	GinkgoWriter.Printf("  Phase: %s\n", lvg.Status.Phase)
	GinkgoWriter.Printf("  VGSize: %s\n", lvg.Status.VGSize.String())
	GinkgoWriter.Printf("  VGFree: %s\n", lvg.Status.VGFree.String())
	for i, tp := range lvg.Status.ThinPools {
		GinkgoWriter.Printf("  ThinPools[%d]: Name=%s AllocationLimit=%s Ready=%t\n", i, tp.Name, tp.AllocationLimit, tp.Ready)
	}
	GinkgoWriter.Println("--- Conditions ---")
	for i, c := range lvg.Status.Conditions {
		GinkgoWriter.Printf("  [%d] Type=%s Status=%s Reason=%s\n", i, c.Type, c.Status, c.Reason)
		if c.Message != "" {
			GinkgoWriter.Printf("      Message: %s\n", c.Message)
		}
	}
	GinkgoWriter.Println("=================================================\n")
}

func restartSDSNodeConfiguratorAgentOnNode(ctx context.Context, cl client.Client, nodeName string) {
	const (
		namespace = "d8-sds-node-configurator"
		appLabel  = "sds-node-configurator"
	)

	Expect(nodeName).NotTo(BeEmpty(), "node name is required to restart sds-node-configurator")

	var podToRestart corev1.Pod
	Eventually(func(g Gomega) {
		var podList corev1.PodList
		g.Expect(cl.List(ctx, &podList, client.InNamespace(namespace), client.MatchingLabels{"app": appLabel})).To(Succeed())

		found := false
		for i := range podList.Items {
			pod := podList.Items[i]
			if pod.Spec.NodeName != nodeName || pod.DeletionTimestamp != nil {
				continue
			}
			podToRestart = pod
			found = true
			break
		}

		g.Expect(found).To(BeTrue(), "no sds-node-configurator pod found on node %s", nodeName)
	}, 2*time.Minute, 5*time.Second).Should(Succeed())

	GinkgoWriter.Printf("    Restarting sds-node-configurator pod %s on node %s\n", podToRestart.Name, nodeName)
	Expect(cl.Delete(ctx, &podToRestart)).To(Succeed(), "delete sds-node-configurator pod %s on node %s", podToRestart.Name, nodeName)

	Eventually(func(g Gomega) {
		var deleted corev1.Pod
		err := cl.Get(ctx, client.ObjectKey{Namespace: namespace, Name: podToRestart.Name}, &deleted)
		g.Expect(apierrors.IsNotFound(err)).To(BeTrue(), "old pod %s should disappear before replacement becomes ready; err=%v", podToRestart.Name, err)
	}, 2*time.Minute, 5*time.Second).Should(Succeed())

	Eventually(func(g Gomega) {
		var podList corev1.PodList
		g.Expect(cl.List(ctx, &podList, client.InNamespace(namespace), client.MatchingLabels{"app": appLabel})).To(Succeed())

		var replacement *corev1.Pod
		for i := range podList.Items {
			pod := &podList.Items[i]
			if pod.Spec.NodeName != nodeName || pod.DeletionTimestamp != nil {
				continue
			}
			if pod.Name == podToRestart.Name || pod.UID == podToRestart.UID {
				continue
			}
			replacement = pod
			break
		}

		g.Expect(replacement).NotTo(BeNil(), "replacement sds-node-configurator pod on node %s not found yet", nodeName)
		g.Expect(replacement.Status.Phase).To(Equal(corev1.PodRunning),
			"replacement pod %s on node %s is not running yet (phase=%s)", replacement.Name, nodeName, replacement.Status.Phase)
		g.Expect(isPodReady(replacement)).To(BeTrue(),
			"replacement pod %s on node %s is not Ready yet", replacement.Name, nodeName)
	}, 5*time.Minute, 10*time.Second).Should(Succeed())
}

func isPodReady(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// e2eClusterStateJSONPath returns the path to storage-e2e cluster-state.json for this test file
// (same layout as getClusterStatePath in github.com/deckhouse/storage-e2e/pkg/cluster).
// Must be called from this file so runtime.Caller resolves to sds_node_configurator_test.go.

func ensureSchedulerE2EK8sClient(resources *cluster.TestClusterResources, k8s *client.Client, ctx context.Context) {
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
	By("Cleaning up existing e2e resources")
	cleanupE2ELocalStorageClasses(ctx, resources.Kubeconfig)
	cleanupE2ELVMVolumeGroups(ctx, *k8s)
	cleanupE2EPods(ctx, *k8s)
	cleanupE2EPVCs(ctx, *k8s)
}

func cleanupE2ELocalStorageClasses(ctx context.Context, kubeconfig *rest.Config) {
	dynClient, err := dynamic.NewForConfig(kubeconfig)
	if err != nil {
		GinkgoWriter.Printf("Failed to create dynamic client for LocalStorageClass cleanup: %v\n", err)
		return
	}

	lscList, err := dynClient.Resource(localStorageClassGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		return
	}

	for _, item := range lscList.Items {
		if strings.HasPrefix(item.GetName(), "e2e-") {
			GinkgoWriter.Printf("Deleting LocalStorageClass %s\n", item.GetName())
			_ = dynClient.Resource(localStorageClassGVR).Delete(ctx, item.GetName(), metav1.DeleteOptions{})
		}
	}
}

func forceDeleteAllNonConsumableBlockDevices(ctx context.Context, cl client.Client, timeout time.Duration) {
	var bdList v1alpha1.BlockDeviceList
	if err := cl.List(ctx, &bdList, &client.ListOptions{}); err != nil {
		GinkgoWriter.Printf("Failed to list BlockDevices: %v\n", err)
		return
	}

	var toDelete []*v1alpha1.BlockDevice
	for i := range bdList.Items {
		bd := &bdList.Items[i]
		if !bd.Status.Consumable {
			toDelete = append(toDelete, bd)
		}
	}

	if len(toDelete) == 0 {
		GinkgoWriter.Println("No non-consumable BlockDevices found")
		return
	}

	GinkgoWriter.Printf("Force deleting %d non-consumable BlockDevices\n", len(toDelete))

	for _, bd := range toDelete {
		GinkgoWriter.Printf("  Removing finalizers and deleting BD %s (%s on %s, fsType=%s)\n",
			bd.Name, bd.Status.Path, bd.Status.NodeName, bd.Status.FsType)

		if len(bd.Finalizers) > 0 {
			bdCopy := bd.DeepCopy()
			bdCopy.Finalizers = nil
			if err := cl.Update(ctx, bdCopy); err != nil {
				GinkgoWriter.Printf("    Failed to remove finalizers: %v\n", err)
			}
		}

		if err := cl.Delete(ctx, bd); err != nil {
			GinkgoWriter.Printf("    Failed to delete: %v\n", err)
		}
	}

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if err := cl.List(ctx, &bdList, &client.ListOptions{}); err != nil {
			time.Sleep(5 * time.Second)
			continue
		}

		remaining := 0
		for i := range bdList.Items {
			if !bdList.Items[i].Status.Consumable {
				remaining++
			}
		}

		if remaining == 0 {
			GinkgoWriter.Println("All non-consumable BlockDevices deleted")
			return
		}

		GinkgoWriter.Printf("Waiting for %d non-consumable BlockDevices to be deleted...\n", remaining)
		time.Sleep(5 * time.Second)
	}
	GinkgoWriter.Println("Warning: timeout waiting for non-consumable BlockDevices deletion")
}

// forceDeleteAllBlockDevices removes finalizers and deletes every BlockDevice CR, then waits until none remain.
// Used after scheduler tests so the next Describe does not inherit orphan consumable BlockDevices.

// forceDeleteAllBlockDevices removes finalizers and deletes every BlockDevice CR, then waits until none remain.
// Used after scheduler tests so the next Describe does not inherit orphan consumable BlockDevices.
func forceDeleteAllBlockDevices(ctx context.Context, cl client.Client, timeout time.Duration) {
	var bdList v1alpha1.BlockDeviceList
	if err := cl.List(ctx, &bdList, &client.ListOptions{}); err != nil {
		GinkgoWriter.Printf("forceDeleteAllBlockDevices: list failed: %v\n", err)
		return
	}
	if len(bdList.Items) == 0 {
		GinkgoWriter.Println("No BlockDevices to delete")
		return
	}
	GinkgoWriter.Printf("Force deleting %d BlockDevice(s)\n", len(bdList.Items))
	for i := range bdList.Items {
		bd := &bdList.Items[i]
		GinkgoWriter.Printf("  Removing finalizers and deleting BD %s (%s on %s)\n",
			bd.Name, bd.Status.Path, bd.Status.NodeName)
		if len(bd.Finalizers) > 0 {
			bdCopy := bd.DeepCopy()
			bdCopy.Finalizers = nil
			if err := cl.Update(ctx, bdCopy); err != nil {
				GinkgoWriter.Printf("    Failed to remove finalizers: %v\n", err)
			}
		}
		if err := cl.Delete(ctx, bd); err != nil {
			GinkgoWriter.Printf("    Failed to delete: %v\n", err)
		}
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if err := cl.List(ctx, &bdList, &client.ListOptions{}); err != nil {
			time.Sleep(5 * time.Second)
			continue
		}
		if len(bdList.Items) == 0 {
			GinkgoWriter.Println("All BlockDevices deleted")
			return
		}
		GinkgoWriter.Printf("Waiting for %d BlockDevice(s) to be gone...\n", len(bdList.Items))
		time.Sleep(5 * time.Second)
	}
	GinkgoWriter.Println("Warning: timeout waiting for all BlockDevices deletion")
}

func forceDeleteBlockDevicesByNames(ctx context.Context, cl client.Client, names []string) {
	if cl == nil || len(names) == 0 {
		return
	}
	for _, name := range names {
		bd := &v1alpha1.BlockDevice{}
		if err := cl.Get(ctx, client.ObjectKey{Name: name}, bd); err != nil {
			continue
		}
		GinkgoWriter.Printf("Deleting BlockDevice CR %s\n", name)
		if len(bd.Finalizers) > 0 {
			bd.Finalizers = nil
			if err := cl.Update(ctx, bd); err != nil {
				GinkgoWriter.Printf("  failed to strip finalizers on %s: %v\n", name, err)
			}
		}
		if err := cl.Delete(ctx, bd); err != nil {
			GinkgoWriter.Printf("  failed to delete BlockDevice %s: %v\n", name, err)
		}
	}
}

func cleanupE2EVirtualDisks(ctx context.Context, kubeconfig *rest.Config, namespace, prefix string) {
	dynClient, err := dynamic.NewForConfig(kubeconfig)
	if err != nil {
		GinkgoWriter.Printf("Failed to create dynamic client for VirtualDisk cleanup: %v\n", err)
		return
	}

	vmbdaList, err := dynClient.Resource(vmbdaGVR).Namespace(namespace).List(ctx, metav1.ListOptions{})
	if err == nil {
		for _, item := range vmbdaList.Items {
			if strings.HasPrefix(item.GetName(), prefix) {
				GinkgoWriter.Printf("Deleting VirtualMachineBlockDeviceAttachment %s\n", item.GetName())
				_ = dynClient.Resource(vmbdaGVR).Namespace(namespace).Delete(ctx, item.GetName(), metav1.DeleteOptions{})
			}
		}
	}

	vdList, err := dynClient.Resource(virtualDiskGVR).Namespace(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		GinkgoWriter.Printf("Failed to list VirtualDisks: %v\n", err)
		return
	}

	var toDelete []string
	for _, item := range vdList.Items {
		if strings.HasPrefix(item.GetName(), prefix) {
			toDelete = append(toDelete, item.GetName())
		}
	}

	if len(toDelete) == 0 {
		return
	}

	GinkgoWriter.Printf("Deleting %d VirtualDisks with prefix %s: %v\n", len(toDelete), prefix, toDelete)
	for _, name := range toDelete {
		_ = dynClient.Resource(virtualDiskGVR).Namespace(namespace).Delete(ctx, name, metav1.DeleteOptions{})
	}

	deadline := time.Now().Add(3 * time.Minute)
	for time.Now().Before(deadline) {
		vdList, err = dynClient.Resource(virtualDiskGVR).Namespace(namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			break
		}
		remaining := 0
		for _, item := range vdList.Items {
			if strings.HasPrefix(item.GetName(), prefix) {
				remaining++
			}
		}
		if remaining == 0 {
			GinkgoWriter.Println("All e2e VirtualDisks deleted")
			return
		}
		time.Sleep(5 * time.Second)
	}
	GinkgoWriter.Println("Warning: some e2e VirtualDisks may still exist after cleanup")
}

func cleanupE2ELVMLogicalVolumes(ctx context.Context, cl client.Client) {
	var list v1alpha1.LVMLogicalVolumeList
	err := cl.List(ctx, &list, &client.ListOptions{})
	if err != nil {
		GinkgoWriter.Printf("List LVMLogicalVolumes failed (skip cleanup): %v\n", err)
		return
	}

	var toDelete []string
	for i := range list.Items {
		llv := &list.Items[i]
		if strings.HasPrefix(llv.Spec.LVMVolumeGroupName, e2eLVMVGPrefix) {
			toDelete = append(toDelete, llv.Name)
		}
	}

	if len(toDelete) == 0 {
		return
	}

	// Delete LLV CRs only — do not strip finalizers: concurrent controller updates cause 409 conflicts and can leave
	// VGs half-cleaned; the agent completes removal when finalizers run normally after PVC/Pods are gone.
	GinkgoWriter.Printf("Deleting %d LVMLogicalVolume(s) referencing e2e LVGs\n", len(toDelete))
	for _, name := range toDelete {
		llv := &v1alpha1.LVMLogicalVolume{ObjectMeta: metav1.ObjectMeta{Name: name}}
		if err := cl.Delete(ctx, llv); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			GinkgoWriter.Printf("  Failed to delete LLV %s: %v\n", name, err)
		}
	}

	deadline := time.Now().Add(5 * time.Minute)
	for time.Now().Before(deadline) {
		err := cl.List(ctx, &list, &client.ListOptions{})
		if err != nil {
			break
		}
		remaining := 0
		for i := range list.Items {
			if strings.HasPrefix(list.Items[i].Spec.LVMVolumeGroupName, e2eLVMVGPrefix) {
				remaining++
			}
		}
		if remaining == 0 {
			GinkgoWriter.Println("All e2e LVMLogicalVolumes deleted")
			return
		}
		GinkgoWriter.Printf("Waiting for %d LVMLogicalVolumes to be deleted...\n", remaining)
		time.Sleep(5 * time.Second)
	}
	GinkgoWriter.Println("Warning: timeout waiting for LVMLogicalVolumes deletion")
}

func cleanupE2ELVMVolumeGroups(ctx context.Context, cl client.Client) {
	var list v1alpha1.LVMVolumeGroupList
	err := cl.List(ctx, &list, &client.ListOptions{})
	if err != nil {
		GinkgoWriter.Printf("List LVMVolumeGroups failed (skip cleanup): %v\n", err)
		return
	}
	var toDelete []string
	for i := range list.Items {
		if strings.HasPrefix(list.Items[i].Name, e2eLVMVGPrefix) {
			toDelete = append(toDelete, list.Items[i].Name)
		}
	}
	if len(toDelete) == 0 {
		return
	}
	GinkgoWriter.Printf("Deleting %d LVMVolumeGroup(s): %v\n", len(toDelete), toDelete)
	for _, name := range toDelete {
		lvg := &v1alpha1.LVMVolumeGroup{}
		lvg.Name = name
		_ = cl.Delete(ctx, lvg)
	}

	GinkgoWriter.Println("Waiting for sds-node-configurator to cleanup VGs (up to 3 minutes)...")
	deadline := time.Now().Add(3 * time.Minute)
	forceRemoveAfter := time.Now().Add(2 * time.Minute)

	for time.Now().Before(deadline) {
		err := cl.List(ctx, &list, &client.ListOptions{})
		if err != nil {
			break
		}
		var remaining []string
		for i := range list.Items {
			if strings.HasPrefix(list.Items[i].Name, e2eLVMVGPrefix) {
				remaining = append(remaining, list.Items[i].Name)
			}
		}
		if len(remaining) == 0 {
			GinkgoWriter.Println("All e2e LVMVolumeGroups removed (VGs cleaned by sds-node-configurator)")
			return
		}

		if time.Now().After(forceRemoveAfter) {
			GinkgoWriter.Printf("Force removing %d stuck LVMVolumeGroups\n", len(remaining))
			for _, name := range remaining {
				lvg := &v1alpha1.LVMVolumeGroup{}
				if err := cl.Get(ctx, client.ObjectKey{Name: name}, lvg); err != nil {
					continue
				}
				if len(lvg.Finalizers) > 0 {
					lvg.Finalizers = nil
					_ = cl.Update(ctx, lvg)
				}
			}
		}

		GinkgoWriter.Printf("Waiting for %d LVMVolumeGroups to be deleted...\n", len(remaining))
		time.Sleep(10 * time.Second)
	}
}

func cleanupE2EPVCs(ctx context.Context, cl client.Client) {
	var list corev1.PersistentVolumeClaimList
	err := cl.List(ctx, &list, client.InNamespace(metav1.NamespaceDefault))
	if err != nil {
		return
	}
	for i := range list.Items {
		if strings.HasPrefix(list.Items[i].Name, e2ePVCPrefix) {
			_ = cl.Delete(ctx, &list.Items[i])
		}
	}
}

func cleanupE2EPods(ctx context.Context, cl client.Client) {
	var list corev1.PodList
	err := cl.List(ctx, &list, client.InNamespace(metav1.NamespaceDefault))
	if err != nil {
		return
	}
	for i := range list.Items {
		if strings.HasPrefix(list.Items[i].Name, e2ePodPrefix) {
			p := list.Items[i]
			_ = cl.Delete(ctx, &p, client.GracePeriodSeconds(0))
		}
	}
}

func countE2EPodsDefault(ctx context.Context, cl client.Client) int {
	var podList corev1.PodList
	if err := cl.List(ctx, &podList, client.InNamespace(metav1.NamespaceDefault)); err != nil {
		GinkgoWriter.Printf("count e2e pods: list failed: %v\n", err)
		return 999999
	}
	n := 0
	for i := range podList.Items {
		if strings.HasPrefix(podList.Items[i].Name, e2ePodPrefix) {
			n++
		}
	}
	return n
}

func countE2EPVCsDefault(ctx context.Context, cl client.Client) int {
	var pvcList corev1.PersistentVolumeClaimList
	if err := cl.List(ctx, &pvcList, client.InNamespace(metav1.NamespaceDefault)); err != nil {
		GinkgoWriter.Printf("count e2e PVCs: list failed: %v\n", err)
		return 999999
	}
	n := 0
	for i := range pvcList.Items {
		if strings.HasPrefix(pvcList.Items[i].Name, e2ePVCPrefix) {
			n++
		}
	}
	return n
}

// countE2ERelatedPVs returns PVs still present for e2e local volumes (same StorageClass as scheduler tests).
// PVC objects can be gone while PV is still Terminating — CSI must detach before LVMLogicalVolume can be removed safely.

// countE2ERelatedPVs returns PVs still present for e2e local volumes (same StorageClass as scheduler tests).
// PVC objects can be gone while PV is still Terminating — CSI must detach before LVMLogicalVolume can be removed safely.
func countE2ERelatedPVs(ctx context.Context, cl client.Client) int {
	var list corev1.PersistentVolumeList
	if err := cl.List(ctx, &list); err != nil {
		GinkgoWriter.Printf("count e2e PVs: list failed: %v\n", err)
		return 999999
	}
	n := 0
	for i := range list.Items {
		pv := &list.Items[i]
		if pv.Spec.StorageClassName == e2eLocalStorageClassName {
			n++
			continue
		}
		if ref := pv.Spec.ClaimRef; ref != nil && ref.Namespace == metav1.NamespaceDefault &&
			strings.HasPrefix(ref.Name, e2ePVCPrefix) {
			n++
		}
	}
	return n
}

// cleanupE2EPodsAndPVCsWithWait deletes e2e Pods first and waits until they are gone before deleting PVCs.
// Deleting PVCs while Pods still mount the volume leaves PVCs stuck in Terminating and blocks the test.
// Then waits until related PersistentVolumes are gone (CSI finishes delete) so LVMLogicalVolume teardown can run in order.
// podPhaseTimeout and pvPhaseTimeout are independent: time spent waiting for Pods must not reduce the CSI budget for
// deleting many PVs (detach + DeleteVolume + finalizers), which serializes in the controller on loaded clusters.
// Uses only Delete (no finalizer removal on Pods or PVCs).

// cleanupE2EPodsAndPVCsWithWait deletes e2e Pods first and waits until they are gone before deleting PVCs.
// Deleting PVCs while Pods still mount the volume leaves PVCs stuck in Terminating and blocks the test.
// Then waits until related PersistentVolumes are gone (CSI finishes delete) so LVMLogicalVolume teardown can run in order.
// podPhaseTimeout and pvPhaseTimeout are independent: time spent waiting for Pods must not reduce the CSI budget for
// deleting many PVs (detach + DeleteVolume + finalizers), which serializes in the controller on loaded clusters.
// Uses only Delete (no finalizer removal on Pods or PVCs).
func cleanupE2EPodsAndPVCsWithWait(ctx context.Context, cl client.Client, podPhaseTimeout, pvPhaseTimeout time.Duration) {
	podDeadline := time.Now().Add(podPhaseTimeout)

	cleanupE2EPods(ctx, cl)
	for time.Now().Before(podDeadline) {
		n := countE2EPodsDefault(ctx, cl)
		if n == 0 {
			break
		}
		GinkgoWriter.Printf("Waiting for e2e Pods to terminate before PVC cleanup: %d remaining\n", n)
		time.Sleep(3 * time.Second)
	}

	cleanupE2EPVCs(ctx, cl)
	pvDeadline := time.Now().Add(pvPhaseTimeout)
	for time.Now().Before(pvDeadline) {
		podCount := countE2EPodsDefault(ctx, cl)
		pvcCount := countE2EPVCsDefault(ctx, cl)
		pvCount := countE2ERelatedPVs(ctx, cl)
		if podCount == 0 && pvcCount == 0 && pvCount == 0 {
			GinkgoWriter.Println("All e2e Pods, PVCs, and related PVs deleted")
			return
		}
		GinkgoWriter.Printf("Waiting for cleanup: %d pods, %d PVCs, %d PVs remaining\n", podCount, pvcCount, pvCount)
		time.Sleep(5 * time.Second)
	}
	GinkgoWriter.Println("Warning: some e2e Pods/PVCs/PVs may still exist after cleanup timeout")
}

func getTotalAvailableSpace(ctx context.Context, cl client.Client, lvgs []*v1alpha1.LVMVolumeGroup) int64 {
	var total int64
	for _, lvg := range lvgs {
		var current v1alpha1.LVMVolumeGroup
		if err := cl.Get(ctx, client.ObjectKeyFromObject(lvg), &current); err != nil {
			continue
		}
		if current.Status.Phase == v1alpha1.PhaseReady {
			total += current.Status.VGFree.Value()
		}
	}
	return total
}

// getMaxVGFreeAcrossLVGs returns the largest VGFree among Ready e2e LVMVolumeGroups. A single PVC is satisfied by
// one LVG on one node — sum(VGFree) over the cluster can exceed this (fragmented free space after prior tests).

// getMaxVGFreeAcrossLVGs returns the largest VGFree among Ready e2e LVMVolumeGroups. A single PVC is satisfied by
// one LVG on one node — sum(VGFree) over the cluster can exceed this (fragmented free space after prior tests).
func getMaxVGFreeAcrossLVGs(ctx context.Context, cl client.Client, lvgs []*v1alpha1.LVMVolumeGroup) int64 {
	var maxFree int64
	for _, lvg := range lvgs {
		var current v1alpha1.LVMVolumeGroup
		if err := cl.Get(ctx, client.ObjectKeyFromObject(lvg), &current); err != nil {
			continue
		}
		if current.Status.Phase != v1alpha1.PhaseReady {
			continue
		}
		v := current.Status.VGFree.Value()
		if v > maxFree {
			maxFree = v
		}
	}
	return maxFree
}

// schedulerVolumeSizesForConsolidatedFill builds PVC sizes that sum to at most currentAvailable (sum of VGFree),
// with each request <= maxPerLVG. preferredUnit is capped by maxPerLVG; leftover bytes are drained in chunks
// <= maxPerLVG so every volume can schedule on some node with local LVM.

// schedulerVolumeSizesForConsolidatedFill builds PVC sizes that sum to at most currentAvailable (sum of VGFree),
// with each request <= maxPerLVG. preferredUnit is capped by maxPerLVG; leftover bytes are drained in chunks
// <= maxPerLVG so every volume can schedule on some node with local LVM.
func schedulerVolumeSizesForConsolidatedFill(currentAvailable, maxPerLVG, preferredUnit, minRemainder int64) []int64 {
	if currentAvailable <= 0 || maxPerLVG <= 0 {
		return nil
	}
	unit := preferredUnit
	if unit > maxPerLVG {
		unit = maxPerLVG
	}
	if unit <= 0 {
		return nil
	}
	var sizes []int64
	left := currentAvailable
	for left >= unit {
		sizes = append(sizes, unit)
		left -= unit
	}
	for left >= minRemainder {
		sz := min(maxPerLVG, left)
		if sz < minRemainder {
			break
		}
		sizes = append(sizes, sz)
		left -= sz
	}
	return sizes
}

// schedulerCleanupWorkloadBeforeNextFill removes e2e Pods/PVCs, then LVMLogicalVolumes tied to e2e LVGs.
// Deleting PVCs alone is not enough: thin LVs remain on the VG until LLV CRs are removed; otherwise the next test
// or LVMVolumeGroup teardown hits "Delete used LVs first" and PV can stay Released with no PVC.
// Pods/PVCs are deleted without stripping finalizers; wait for PVs before LLV so CSI does not race with LVMLogicalVolume deletion.

// schedulerCleanupWorkloadBeforeNextFill removes e2e Pods/PVCs, then LVMLogicalVolumes tied to e2e LVGs.
// Deleting PVCs alone is not enough: thin LVs remain on the VG until LLV CRs are removed; otherwise the next test
// or LVMVolumeGroup teardown hits "Delete used LVs first" and PV can stay Released with no PVC.
// Pods/PVCs are deleted without stripping finalizers; wait for PVs before LLV so CSI does not race with LVMLogicalVolume deletion.
func schedulerCleanupWorkloadBeforeNextFill(ctx context.Context, cl client.Client) {
	cleanupE2EPodsAndPVCsWithWait(ctx, cl, e2eSchedulerPodCleanupTimeout, e2eSchedulerPVDeleteTimeout)
	// If cleanupE2EPodsAndPVCsWithWait hit its deadline, PVs can remain Released while CSI waits for detach/delete.
	// Deleting LVMLogicalVolume CRs in that window makes the provisioner error (LLV not found) and leaves PV stuck.
	n := countE2ERelatedPVs(ctx, cl)
	Expect(n).To(BeZero(),
		"e2e-related PVs must be gone before LVMLogicalVolume cleanup; leftover PVs mean CSI has not finished delete/detach (see VolumeAttachments, VolumeFailedDelete)")
	By("Removing LVMLogicalVolumes for e2e LVGs after PVC deletion (thin LVs must leave the VG)")
	cleanupE2ELVMLogicalVolumes(ctx, cl)
}

// waitForSchedulerStorageFreedToBaseline waits until sum(VGFree) across Ready e2e LVMVolumeGroups returns to the
// baseline recorded after LVG became Ready (totalAvailableSpace). Pod/PVC deletion finishes before thin LVs and
// LLV CRs are gone; waiting only for VGFree > 0 under-fills the next "100% utilization" test.

// waitForSchedulerStorageFreedToBaseline waits until sum(VGFree) across Ready e2e LVMVolumeGroups returns to the
// baseline recorded after LVG became Ready (totalAvailableSpace). Pod/PVC deletion finishes before thin LVs and
// LLV CRs are gone; waiting only for VGFree > 0 under-fills the next "100% utilization" test.
func waitForSchedulerStorageFreedToBaseline(ctx context.Context, cl client.Client, lvgs []*v1alpha1.LVMVolumeGroup, baselineFreeBytes int64) int64 {
	if baselineFreeBytes <= 0 {
		return 0
	}
	// Allow 1% slack for status rounding vs first observation; require ~full recovery of the initial budget.
	minExpected := baselineFreeBytes * 99 / 100
	var total int64
	Eventually(func(g Gomega) {
		total = getTotalAvailableSpace(ctx, cl, lvgs)
		g.Expect(total).To(BeNumerically(">=", minExpected),
			"sum(VGFree) must recover to ~initial storage budget after Pod/PVC/LLV cleanup (baseline=%d min=%d got=%d)",
			baselineFreeBytes, minExpected, total)
	}, 15*time.Minute, 5*time.Second).Should(Succeed())
	return total
}

func createPVCsAndPodsWithSizes(ctx context.Context, cl client.Client, volumeSizes []int64, storageClass, sizeLabel string) (successCount, scheduledCount int) {
	for i, volumeSize := range volumeSizes {
		pvcName := fmt.Sprintf("%s%s-%d", e2ePVCPrefix, sizeLabel, i)
		podName := fmt.Sprintf("%s%s-%d", e2ePodPrefix, sizeLabel, i)

		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      pvcName,
				Namespace: "default",
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: *resource.NewQuantity(volumeSize, resource.BinarySI),
					},
				},
				StorageClassName: &storageClass,
			},
		}

		if err := cl.Create(ctx, pvc); err != nil {
			GinkgoWriter.Printf("Failed to create PVC %s: %v\n", pvcName, err)
			continue
		}
		successCount++

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      podName,
				Namespace: "default",
			},
			Spec: corev1.PodSpec{
				Tolerations: []corev1.Toleration{
					{
						Key:      "node-role.kubernetes.io/control-plane",
						Operator: corev1.TolerationOpExists,
						Effect:   corev1.TaintEffectNoSchedule,
					},
					{
						Key:      "node-role.kubernetes.io/master",
						Operator: corev1.TolerationOpExists,
						Effect:   corev1.TaintEffectNoSchedule,
					},
				},
				Containers: []corev1.Container{
					{
						Name:    "test",
						Image:   "busybox",
						Command: []string{"sleep", "3600"},
						VolumeMounts: []corev1.VolumeMount{
							{Name: "data", MountPath: "/data"},
						},
					},
				},
				Volumes: []corev1.Volume{
					{
						Name: "data",
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
								ClaimName: pvcName,
							},
						},
					},
				},
			},
		}

		if err := cl.Create(ctx, pod); err != nil {
			GinkgoWriter.Printf("Failed to create Pod %s: %v\n", podName, err)
			continue
		}
	}

	scheduledCount = waitForPodsScheduled(ctx, cl, sizeLabel, successCount, e2eSchedulerFillPodsWaitTimeout)
	printPVCAndPodStatus(ctx, cl, sizeLabel)
	return successCount, scheduledCount
}

func waitForPodsScheduled(ctx context.Context, cl client.Client, sizeLabel string, expectedCount int, timeout time.Duration) int {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var podList corev1.PodList
		if err := cl.List(ctx, &podList, &client.ListOptions{Namespace: "default"}); err != nil {
			time.Sleep(5 * time.Second)
			continue
		}

		scheduledCount := 0
		for i := range podList.Items {
			pod := &podList.Items[i]
			if !strings.HasPrefix(pod.Name, e2ePodPrefix+sizeLabel) {
				continue
			}
			if pod.Spec.NodeName != "" || pod.Status.Phase == corev1.PodRunning || pod.Status.Phase == corev1.PodSucceeded {
				scheduledCount++
			}
		}

		if scheduledCount >= expectedCount {
			return scheduledCount
		}

		time.Sleep(5 * time.Second)
	}

	var podList corev1.PodList
	_ = cl.List(ctx, &podList, &client.ListOptions{Namespace: "default"})
	scheduledCount := 0
	for i := range podList.Items {
		pod := &podList.Items[i]
		if !strings.HasPrefix(pod.Name, e2ePodPrefix+sizeLabel) {
			continue
		}
		if pod.Spec.NodeName != "" || pod.Status.Phase == corev1.PodRunning || pod.Status.Phase == corev1.PodSucceeded {
			scheduledCount++
		}
	}
	return scheduledCount
}

func printBlockDevicesSummary(bds []*v1alpha1.BlockDevice) {
	GinkgoWriter.Println("\n========== BlockDevices Summary ==========")
	for _, bd := range bds {
		GinkgoWriter.Printf("  %s: node=%s path=%s size=%s\n",
			bd.Name, bd.Status.NodeName, bd.Status.Path, bd.Status.Size.String())
	}
	GinkgoWriter.Println("==========================================\n")
}

func printLVGsSummary(ctx context.Context, cl client.Client, lvgs []*v1alpha1.LVMVolumeGroup) {
	GinkgoWriter.Println("\n========== LVMVolumeGroups Summary ==========")
	for _, lvg := range lvgs {
		var current v1alpha1.LVMVolumeGroup
		if err := cl.Get(ctx, client.ObjectKeyFromObject(lvg), &current); err != nil {
			GinkgoWriter.Printf("  %s: error getting status: %v\n", lvg.Name, err)
			continue
		}
		GinkgoWriter.Printf("  %s: phase=%s vgSize=%s vgFree=%s\n",
			current.Name, current.Status.Phase, current.Status.VGSize.String(), current.Status.VGFree.String())
	}
	GinkgoWriter.Println("=============================================\n")
}

func printSchedulingSummary(testName string, attempted, created, scheduled int, volumeSize int64) {
	GinkgoWriter.Println("\n========== Scheduling Summary ==========")
	GinkgoWriter.Printf("Test: %s\n", testName)
	GinkgoWriter.Printf("Volume size: %d bytes (%.2f Mi)\n", volumeSize, float64(volumeSize)/(1024*1024))
	GinkgoWriter.Printf("Attempted: %d\n", attempted)
	GinkgoWriter.Printf("Created: %d\n", created)
	GinkgoWriter.Printf("Scheduled: %d\n", scheduled)
	if created > 0 {
		GinkgoWriter.Printf("Success rate: %.1f%%\n", float64(scheduled)/float64(created)*100)
	}
	GinkgoWriter.Println("=========================================\n")
}

func printPVCAndPodStatus(ctx context.Context, cl client.Client, sizeLabel string) {
	GinkgoWriter.Println("\n========== PVC Status (first 10) ==========")
	var pvcList corev1.PersistentVolumeClaimList
	if err := cl.List(ctx, &pvcList, &client.ListOptions{Namespace: "default"}); err == nil {
		count := 0
		pendingCount := 0
		boundCount := 0
		for i := range pvcList.Items {
			pvc := &pvcList.Items[i]
			if !strings.HasPrefix(pvc.Name, e2ePVCPrefix+sizeLabel) {
				continue
			}
			if pvc.Status.Phase == corev1.ClaimPending {
				pendingCount++
			} else if pvc.Status.Phase == corev1.ClaimBound {
				boundCount++
			}
			if count < 10 {
				GinkgoWriter.Printf("  %s: phase=%s storageClass=%s\n",
					pvc.Name, pvc.Status.Phase, *pvc.Spec.StorageClassName)
				count++
			}
		}
		GinkgoWriter.Printf("Total: %d Bound, %d Pending\n", boundCount, pendingCount)
	}

	GinkgoWriter.Println("\n========== Pod Status (first 10) ==========")
	var podList corev1.PodList
	if err := cl.List(ctx, &podList, &client.ListOptions{Namespace: "default"}); err == nil {
		count := 0
		scheduledCount := 0
		pendingCount := 0
		for i := range podList.Items {
			pod := &podList.Items[i]
			if !strings.HasPrefix(pod.Name, e2ePodPrefix+sizeLabel) {
				continue
			}
			if pod.Spec.NodeName != "" {
				scheduledCount++
			} else {
				pendingCount++
			}
			if count < 10 {
				reason := ""
				if len(pod.Status.Conditions) > 0 {
					for _, cond := range pod.Status.Conditions {
						if cond.Type == corev1.PodScheduled && cond.Status == corev1.ConditionFalse {
							reason = cond.Reason + ": " + cond.Message
							break
						}
					}
				}
				GinkgoWriter.Printf("  %s: phase=%s node=%s reason=%s\n",
					pod.Name, pod.Status.Phase, pod.Spec.NodeName, reason)
				count++
			}
		}
		GinkgoWriter.Printf("Total: %d Scheduled, %d Pending\n", scheduledCount, pendingCount)
	}
	GinkgoWriter.Println("==========================================\n")
}
