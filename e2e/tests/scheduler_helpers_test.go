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
	"os"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/e2e/framework"
)

// localStorageClassGVR identifies the storage.deckhouse.io LocalStorageClass resource (cluster-scoped).
var localStorageClassGVR = schema.GroupVersionResource{
	Group:    "storage.deckhouse.io",
	Version:  "v1alpha1",
	Resource: "localstorageclasses",
}

const (
	// localStorageClassName is the LocalStorageClass (and derived StorageClass) used by the scheduler-extender
	// consolidation tests.
	localStorageClassName = "e2e-local-sc"
	// pvcNamePrefix / podNamePrefix prefix every PVC/Pod the scheduler fill tests create in the default namespace.
	pvcNamePrefix = "e2e-pvc-"
	podNamePrefix = "e2e-pod-"

	// schedulerFillPodsWaitTimeout: fill tests create many PVCs/Pods; provisioning and binding can exceed 5m on
	// loaded clusters.
	schedulerFillPodsWaitTimeout = 10 * time.Minute

	// Scheduler cleanup: pod termination and CSI PV teardown must not share one deadline — many PVs delete serially.
	schedulerPodCleanupTimeout = 5 * time.Minute
	schedulerPVDeleteTimeout   = 25 * time.Minute

	// Many fill-test Pods (busybox + RWO volume) can stay Terminating until volumes detach; force-finalize when stuck.
	podCleanupStuckWithoutProgress = 90 * time.Second
	podCleanupPollInterval         = 3 * time.Second
	podCleanupLogStuckInterval     = 30 * time.Second

	// Teardown: short pod wait; PVC deletion returns quickly while PV finalizers need a separate budget.
	suitePodPVCleanupPodTimeout = 2 * time.Minute
	suitePodPVCleanupPVTimeout  = 15 * time.Minute
)

// ---=== LocalStorageClass lifecycle ===--- //

// ensureSchedulerK8sClient lazily builds a controller-runtime client (backed by the SDK REST config) with the
// sds-node-configurator scheme registered, and clears leftover e2e resources on first build.
func ensureSchedulerK8sClient(ctx context.Context, cfg *rest.Config, k8s *client.Client) {
	if *k8s != nil {
		return
	}
	Expect(cfg).NotTo(BeNil(), "REST config must be available")
	Expect(v1alpha1.AddToScheme(scheme.Scheme)).To(Succeed())
	var err error
	*k8s, err = client.New(cfg, client.Options{Scheme: scheme.Scheme})
	Expect(err).NotTo(HaveOccurred())
	_, err = (*k8s).RESTMapper().RESTMapping(v1alpha1.SchemeGroupVersion.WithKind("BlockDevice").GroupKind())
	Expect(err).NotTo(HaveOccurred())
	By("Cleaning up existing e2e resources")
	cleanupLocalStorageClasses(ctx, cfg)
	cleanupLVMVolumeGroups(ctx, *k8s)
	cleanupPods(ctx, *k8s)
	cleanupPVCs(ctx, *k8s)
}

func waitForLocalStorageClassDeleted(ctx context.Context, dynClient dynamic.Interface, name string, timeout time.Duration) error {
	err := framework.Poll(ctx, 2*time.Second, timeout, func(ctx context.Context) (bool, error) {
		_, getErr := dynClient.Resource(localStorageClassGVR).Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(getErr) {
			return true, nil
		}
		return false, getErr
	})
	if err != nil {
		return fmt.Errorf("timeout waiting for LocalStorageClass %s to be deleted", name)
	}
	return nil
}

func forceDeleteLocalStorageClass(ctx context.Context, dynClient dynamic.Interface, name string) {
	obj, err := dynClient.Resource(localStorageClassGVR).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return
	}
	if err != nil {
		GinkgoWriter.Printf("force delete LSC %s: get: %v\n", name, err)
		return
	}
	obj.SetFinalizers(nil)
	if _, err := dynClient.Resource(localStorageClassGVR).Update(ctx, obj, metav1.UpdateOptions{}); err != nil {
		GinkgoWriter.Printf("force delete LSC %s: clear finalizers: %v\n", name, err)
	}
	propagation := metav1.DeletePropagationForeground
	_ = dynClient.Resource(localStorageClassGVR).Delete(ctx, name, metav1.DeleteOptions{
		PropagationPolicy: &propagation,
	})
}

// ensureLocalStorageClassAbsent deletes e2e-local-sc (and matching StorageClass) and waits until gone.
// Needed when a prior run left LocalStorageClass in Terminating ("already exists" on recreate).
func ensureLocalStorageClassAbsent(ctx context.Context, kubeconfig *rest.Config, k8sCl client.Client, name string) error {
	dynClient, err := dynamic.NewForConfig(kubeconfig)
	if err != nil {
		return fmt.Errorf("dynamic client: %w", err)
	}

	propagation := metav1.DeletePropagationForeground
	deleteOpts := metav1.DeleteOptions{PropagationPolicy: &propagation}

	obj, getErr := dynClient.Resource(localStorageClassGVR).Get(ctx, name, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(getErr):
		// gone
	case getErr != nil:
		return fmt.Errorf("get LocalStorageClass %s: %w", name, getErr)
	default:
		if obj.GetDeletionTimestamp() == nil {
			GinkgoWriter.Printf("Deleting LocalStorageClass %s before recreate\n", name)
			_ = dynClient.Resource(localStorageClassGVR).Delete(ctx, name, deleteOpts)
		} else {
			GinkgoWriter.Printf("Waiting for LocalStorageClass %s to finish deleting\n", name)
		}
	}

	if err := waitForLocalStorageClassDeleted(ctx, dynClient, name, 5*time.Minute); err != nil {
		GinkgoWriter.Printf("ensure LSC absent: %v; force deleting %s\n", err, name)
		forceDeleteLocalStorageClass(ctx, dynClient, name)
		if err2 := waitForLocalStorageClassDeleted(ctx, dynClient, name, 2*time.Minute); err2 != nil {
			return fmt.Errorf("LocalStorageClass %s still present after force delete: %w", name, err2)
		}
	}

	if k8sCl != nil {
		var sc storagev1.StorageClass
		if err := k8sCl.Get(ctx, client.ObjectKey{Name: name}, &sc); err == nil {
			GinkgoWriter.Printf("Deleting leftover StorageClass %s\n", name)
			_ = client.IgnoreNotFound(k8sCl.Delete(ctx, &sc))
		} else if !apierrors.IsNotFound(err) {
			return fmt.Errorf("get StorageClass %s: %w", name, err)
		}
	}
	return nil
}

func cleanupLocalStorageClasses(ctx context.Context, kubeconfig *rest.Config) {
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
		if !strings.HasPrefix(item.GetName(), "e2e-") {
			continue
		}
		name := item.GetName()
		GinkgoWriter.Printf("Deleting LocalStorageClass %s\n", name)
		_ = dynClient.Resource(localStorageClassGVR).Delete(ctx, name, metav1.DeleteOptions{})
		if err := waitForLocalStorageClassDeleted(ctx, dynClient, name, 3*time.Minute); err != nil {
			GinkgoWriter.Printf("  %v; force deleting\n", err)
			forceDeleteLocalStorageClass(ctx, dynClient, name)
			_ = waitForLocalStorageClassDeleted(ctx, dynClient, name, time.Minute)
		}
	}
}

// ---=== Pod / PVC / PV fill + cleanup ===--- //

func cleanupPVCs(ctx context.Context, cl client.Client) {
	var list corev1.PersistentVolumeClaimList
	err := cl.List(ctx, &list, client.InNamespace(metav1.NamespaceDefault))
	if err != nil {
		return
	}
	for i := range list.Items {
		if strings.HasPrefix(list.Items[i].Name, pvcNamePrefix) {
			_ = cl.Delete(ctx, &list.Items[i])
		}
	}
}

func isSchedulerPodName(name string) bool {
	return strings.HasPrefix(name, podNamePrefix)
}

// cleanupPods issues Delete (grace 0, foreground) for every e2e-pod-* in default. Safe to call repeatedly.
func cleanupPods(ctx context.Context, cl client.Client) int {
	var list corev1.PodList
	if err := cl.List(ctx, &list, client.InNamespace(metav1.NamespaceDefault)); err != nil {
		GinkgoWriter.Printf("list e2e pods for delete: %v\n", err)
		return 0
	}
	foreground := client.PropagationPolicy(metav1.DeletePropagationForeground)
	requested := 0
	for i := range list.Items {
		if !isSchedulerPodName(list.Items[i].Name) {
			continue
		}
		p := list.Items[i]
		err := cl.Delete(ctx, &p, client.GracePeriodSeconds(0), foreground)
		if err != nil && !apierrors.IsNotFound(err) {
			GinkgoWriter.Printf("delete pod %s (phase=%s): %v\n", p.Name, p.Status.Phase, err)
			continue
		}
		requested++
	}
	return requested
}

// forceFinalizeStuckPods removes finalizers from e2e Pods already in deletion (volume unmount can block for minutes).
func forceFinalizeStuckPods(ctx context.Context, cl client.Client) int {
	var list corev1.PodList
	if err := cl.List(ctx, &list, client.InNamespace(metav1.NamespaceDefault)); err != nil {
		GinkgoWriter.Printf("list e2e pods for force finalize: %v\n", err)
		return 0
	}
	finalized := 0
	for i := range list.Items {
		if !isSchedulerPodName(list.Items[i].Name) {
			continue
		}
		p := list.Items[i]
		if p.DeletionTimestamp == nil {
			continue
		}
		var fresh corev1.Pod
		if err := cl.Get(ctx, client.ObjectKeyFromObject(&p), &fresh); err != nil {
			if !apierrors.IsNotFound(err) {
				GinkgoWriter.Printf("get pod %s for force finalize: %v\n", p.Name, err)
			}
			continue
		}
		if len(fresh.Finalizers) == 0 {
			continue
		}
		fresh.Finalizers = nil
		if err := cl.Update(ctx, &fresh); err != nil && !apierrors.IsNotFound(err) {
			GinkgoWriter.Printf("strip finalizers on pod %s: %v\n", fresh.Name, err)
			continue
		}
		deletingFor := time.Since(fresh.DeletionTimestamp.Time).Round(time.Second)
		GinkgoWriter.Printf("Force-finalized stuck pod %s (phase=%s, deletingFor=%s)\n", fresh.Name, fresh.Status.Phase, deletingFor)
		finalized++
	}
	return finalized
}

func logSampleStuckPods(ctx context.Context, cl client.Client, max int) {
	var list corev1.PodList
	if err := cl.List(ctx, &list, client.InNamespace(metav1.NamespaceDefault)); err != nil {
		return
	}
	logged := 0
	for i := range list.Items {
		p := list.Items[i]
		if !isSchedulerPodName(p.Name) {
			continue
		}
		deletingFor := "—"
		if p.DeletionTimestamp != nil {
			deletingFor = time.Since(p.DeletionTimestamp.Time).Round(time.Second).String()
		}
		GinkgoWriter.Printf("  e2e pod %s phase=%s node=%s deletingFor=%s finalizers=%v\n",
			p.Name, p.Status.Phase, p.Spec.NodeName, deletingFor, p.Finalizers)
		logged++
		if logged >= max {
			break
		}
	}
}

// waitPodsGone retries Delete on each poll and force-finalizes Pods stuck in Terminating without progress.
func waitPodsGone(ctx context.Context, cl client.Client, timeout time.Duration) {
	lastCount := -1
	lastProgress := time.Now()
	lastDetailLog := time.Time{}

	gone := framework.Poll(ctx, podCleanupPollInterval, timeout, func(ctx context.Context) (bool, error) {
		cleanupPods(ctx, cl)

		n := countPodsDefault(ctx, cl)
		if n == 0 {
			return true, nil
		}
		if n < lastCount {
			lastProgress = time.Now()
		}
		lastCount = n

		if time.Since(lastProgress) >= podCleanupStuckWithoutProgress {
			if forceFinalizeStuckPods(ctx, cl) > 0 {
				lastProgress = time.Now()
			}
		}

		if time.Since(lastDetailLog) >= podCleanupLogStuckInterval {
			GinkgoWriter.Printf("Waiting for e2e Pods to terminate before PVC cleanup: %d remaining (sample):\n", n)
			logSampleStuckPods(ctx, cl, 5)
			lastDetailLog = time.Now()
		} else {
			GinkgoWriter.Printf("Waiting for e2e Pods to terminate before PVC cleanup: %d remaining\n", n)
		}
		return false, nil
	}) == nil
	if gone {
		return
	}

	if n := countPodsDefault(ctx, cl); n > 0 {
		GinkgoWriter.Printf("Pod cleanup deadline reached with %d e2e Pods left; forcing removal\n", n)
		cleanupPods(ctx, cl)
		forceFinalizeStuckPods(ctx, cl)
		_ = framework.Poll(ctx, 2*time.Second, 30*time.Second, func(ctx context.Context) (bool, error) {
			return countPodsDefault(ctx, cl) == 0, nil
		})
		if n := countPodsDefault(ctx, cl); n > 0 {
			GinkgoWriter.Printf("Warning: %d e2e Pods still present after force cleanup:\n", n)
			logSampleStuckPods(ctx, cl, 10)
		}
	}
}

func countPodsDefault(ctx context.Context, cl client.Client) int {
	var podList corev1.PodList
	if err := cl.List(ctx, &podList, client.InNamespace(metav1.NamespaceDefault)); err != nil {
		GinkgoWriter.Printf("count e2e pods: list failed: %v\n", err)
		return 999999
	}
	n := 0
	for i := range podList.Items {
		if strings.HasPrefix(podList.Items[i].Name, podNamePrefix) {
			n++
		}
	}
	return n
}

func countPVCsDefault(ctx context.Context, cl client.Client) int {
	var pvcList corev1.PersistentVolumeClaimList
	if err := cl.List(ctx, &pvcList, client.InNamespace(metav1.NamespaceDefault)); err != nil {
		GinkgoWriter.Printf("count e2e PVCs: list failed: %v\n", err)
		return 999999
	}
	n := 0
	for i := range pvcList.Items {
		if strings.HasPrefix(pvcList.Items[i].Name, pvcNamePrefix) {
			n++
		}
	}
	return n
}

func isRelatedPV(pv *corev1.PersistentVolume) bool {
	if pv.Spec.StorageClassName == localStorageClassName {
		return true
	}
	if ref := pv.Spec.ClaimRef; ref != nil && ref.Namespace == metav1.NamespaceDefault &&
		strings.HasPrefix(ref.Name, pvcNamePrefix) {
		return true
	}
	return false
}

// countRelatedPVs returns PVs still present for e2e local volumes (same StorageClass as scheduler tests).
// PVC objects can be gone while PV is still Terminating — CSI must detach before LVMLogicalVolume can be removed safely.
func countRelatedPVs(ctx context.Context, cl client.Client) int {
	var list corev1.PersistentVolumeList
	if err := cl.List(ctx, &list); err != nil {
		GinkgoWriter.Printf("count e2e PVs: list failed: %v\n", err)
		return 999999
	}
	n := 0
	for i := range list.Items {
		if isRelatedPV(&list.Items[i]) {
			n++
		}
	}
	return n
}

func schedulerPVDeleteTimeoutDuration() time.Duration {
	if os.Getenv("CI") != "" {
		return 40 * time.Minute
	}
	return schedulerPVDeleteTimeout
}

func logSampleStuckPVs(ctx context.Context, cl client.Client, max int) {
	var list corev1.PersistentVolumeList
	if err := cl.List(ctx, &list); err != nil {
		return
	}
	logged := 0
	for i := range list.Items {
		pv := &list.Items[i]
		if !isRelatedPV(pv) {
			continue
		}
		deletingFor := "—"
		if pv.DeletionTimestamp != nil {
			deletingFor = time.Since(pv.DeletionTimestamp.Time).Round(time.Second).String()
		}
		GinkgoWriter.Printf("  e2e PV %s phase=%s claim=%s deletingFor=%s finalizers=%v\n",
			pv.Name, pv.Status.Phase, pv.Spec.ClaimRef, deletingFor, pv.Finalizers)
		logged++
		if logged >= max {
			break
		}
	}
}

// forceFinalizeStuckPVs issues Delete on lingering PVs and strips finalizers when CSI delete stalls.
func forceFinalizeStuckPVs(ctx context.Context, cl client.Client) int {
	var list corev1.PersistentVolumeList
	if err := cl.List(ctx, &list); err != nil {
		GinkgoWriter.Printf("list e2e PVs for force finalize: %v\n", err)
		return 0
	}
	finalized := 0
	for i := range list.Items {
		pv := &list.Items[i]
		if !isRelatedPV(pv) {
			continue
		}
		if pv.DeletionTimestamp == nil {
			if err := cl.Delete(ctx, pv); err != nil && !apierrors.IsNotFound(err) {
				GinkgoWriter.Printf("delete PV %s: %v\n", pv.Name, err)
			}
			continue
		}
		var fresh corev1.PersistentVolume
		if err := cl.Get(ctx, client.ObjectKeyFromObject(pv), &fresh); err != nil {
			if !apierrors.IsNotFound(err) {
				GinkgoWriter.Printf("get PV %s for force finalize: %v\n", pv.Name, err)
			}
			continue
		}
		if len(fresh.Finalizers) == 0 {
			continue
		}
		fresh.Finalizers = nil
		if err := cl.Update(ctx, &fresh); err != nil && !apierrors.IsNotFound(err) {
			GinkgoWriter.Printf("strip finalizers on PV %s: %v\n", fresh.Name, err)
			continue
		}
		deletingFor := time.Since(fresh.DeletionTimestamp.Time).Round(time.Second)
		GinkgoWriter.Printf("Force-finalized stuck PV %s (phase=%s, deletingFor=%s)\n", fresh.Name, fresh.Status.Phase, deletingFor)
		finalized++
	}
	return finalized
}

// cleanupPodsAndPVCsWithWait deletes e2e Pods first and waits until they are gone before deleting PVCs.
// Deleting PVCs while Pods still mount the volume leaves PVCs stuck in Terminating and blocks the test.
// Then waits until related PersistentVolumes are gone (CSI finishes delete) so LVMLogicalVolume teardown can run in order.
func cleanupPodsAndPVCsWithWait(ctx context.Context, cl client.Client, podPhaseTimeout, pvPhaseTimeout time.Duration) {
	waitPodsGone(ctx, cl, podPhaseTimeout)

	cleanupPVCs(ctx, cl)
	lastPVCount := -1
	lastPVProgress := time.Now()
	lastPVDetailLog := time.Time{}
	done := framework.Poll(ctx, 5*time.Second, pvPhaseTimeout, func(ctx context.Context) (bool, error) {
		if countPodsDefault(ctx, cl) > 0 {
			cleanupPods(ctx, cl)
		}
		if countPVCsDefault(ctx, cl) > 0 {
			cleanupPVCs(ctx, cl)
		}
		podCount := countPodsDefault(ctx, cl)
		pvcCount := countPVCsDefault(ctx, cl)
		pvCount := countRelatedPVs(ctx, cl)
		if podCount == 0 && pvcCount == 0 && pvCount == 0 {
			GinkgoWriter.Println("All e2e Pods, PVCs, and related PVs deleted")
			return true, nil
		}
		if pvCount < lastPVCount {
			lastPVProgress = time.Now()
		}
		lastPVCount = pvCount
		if pvCount > 0 && time.Since(lastPVProgress) >= 3*time.Minute {
			if forceFinalizeStuckPVs(ctx, cl) > 0 {
				lastPVProgress = time.Now()
			}
		}
		if pvCount > 0 && time.Since(lastPVDetailLog) >= podCleanupLogStuckInterval {
			GinkgoWriter.Printf("Waiting for cleanup: %d pods, %d PVCs, %d PVs remaining (sample PVs):\n", podCount, pvcCount, pvCount)
			logSampleStuckPVs(ctx, cl, 5)
			lastPVDetailLog = time.Now()
		} else {
			GinkgoWriter.Printf("Waiting for cleanup: %d pods, %d PVCs, %d PVs remaining\n", podCount, pvcCount, pvCount)
		}
		return false, nil
	}) == nil
	if done {
		return
	}
	if n := countRelatedPVs(ctx, cl); n > 0 {
		GinkgoWriter.Printf("PV cleanup deadline reached with %d e2e PVs left; forcing removal\n", n)
		forceFinalizeStuckPVs(ctx, cl)
		logSampleStuckPVs(ctx, cl, 10)
	}
	GinkgoWriter.Println("Warning: some e2e Pods/PVCs/PVs may still exist after cleanup timeout")
}

// ---=== Capacity / fill planning ===--- //

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
func schedulerCleanupWorkloadBeforeNextFill(ctx context.Context, cl client.Client) {
	cleanupPodsAndPVCsWithWait(ctx, cl, schedulerPodCleanupTimeout, schedulerPVDeleteTimeoutDuration())
	// If cleanupPodsAndPVCsWithWait hit its deadline, PVs can remain Released while CSI waits for detach/delete.
	// Deleting LVMLogicalVolume CRs in that window makes the provisioner error (LLV not found) and leaves PV stuck.
	n := countRelatedPVs(ctx, cl)
	Expect(n).To(BeZero(),
		"e2e-related PVs must be gone before LVMLogicalVolume cleanup; leftover PVs mean CSI has not finished delete/detach (see VolumeAttachments, VolumeFailedDelete)")
	By("Removing LVMLogicalVolumes for e2e LVGs after PVC deletion (thin LVs must leave the VG)")
	cleanupLVMLogicalVolumes(ctx, cl)
}

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
		pvcName := fmt.Sprintf("%s%s-%d", pvcNamePrefix, sizeLabel, i)
		podName := fmt.Sprintf("%s%s-%d", podNamePrefix, sizeLabel, i)

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

	scheduledCount = waitForPodsScheduled(ctx, cl, sizeLabel, successCount, schedulerFillPodsWaitTimeout)
	printPVCAndPodStatus(ctx, cl, sizeLabel)
	return successCount, scheduledCount
}

func waitForPodsScheduled(ctx context.Context, cl client.Client, sizeLabel string, expectedCount int, timeout time.Duration) int {
	_ = framework.Poll(ctx, 5*time.Second, timeout, func(ctx context.Context) (bool, error) {
		var podList corev1.PodList
		if err := cl.List(ctx, &podList, &client.ListOptions{Namespace: "default"}); err != nil {
			return false, err
		}

		scheduledCount := 0
		for i := range podList.Items {
			pod := &podList.Items[i]
			if !strings.HasPrefix(pod.Name, podNamePrefix+sizeLabel) {
				continue
			}
			if pod.Spec.NodeName != "" || pod.Status.Phase == corev1.PodRunning || pod.Status.Phase == corev1.PodSucceeded {
				scheduledCount++
			}
		}

		return scheduledCount >= expectedCount, nil
	})

	var podList corev1.PodList
	_ = cl.List(ctx, &podList, &client.ListOptions{Namespace: "default"})
	scheduledCount := 0
	for i := range podList.Items {
		pod := &podList.Items[i]
		if !strings.HasPrefix(pod.Name, podNamePrefix+sizeLabel) {
			continue
		}
		if pod.Spec.NodeName != "" || pod.Status.Phase == corev1.PodRunning || pod.Status.Phase == corev1.PodSucceeded {
			scheduledCount++
		}
	}
	return scheduledCount
}

// ---=== Printers ===--- //

func printBlockDevicesSummary(bds []*v1alpha1.BlockDevice) {
	GinkgoWriter.Println("\n========== BlockDevices Summary ==========")
	for _, bd := range bds {
		GinkgoWriter.Printf("  %s: node=%s path=%s size=%s\n",
			bd.Name, bd.Status.NodeName, bd.Status.Path, bd.Status.Size.String())
	}
	GinkgoWriter.Println("==========================================")
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
	GinkgoWriter.Println("=============================================")
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
	GinkgoWriter.Println("=========================================")
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
			if !strings.HasPrefix(pvc.Name, pvcNamePrefix+sizeLabel) {
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
			if !strings.HasPrefix(pod.Name, podNamePrefix+sizeLabel) {
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
	GinkgoWriter.Println("==========================================")
}
