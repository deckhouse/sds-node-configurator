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
	"slices"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/e2e/framework"
	"github.com/deckhouse/storage-e2e/pkg/e2e"
	"github.com/deckhouse/storage-e2e/pkg/kubernetes"
)

// localStorageClassGVR identifies the storage.deckhouse.io LocalStorageClass resource (cluster-scoped).
var localStorageClassGVR = schema.GroupVersionResource{
	Group:    "storage.deckhouse.io",
	Version:  "v1alpha1",
	Resource: "localstorageclasses",
}

const (
	// localStorageClassName is the single Thick WaitForFirstConsumer LocalStorageClass (and the
	// derived StorageClass) shared by all four specs. It is also how teardown recognizes our PVs.
	localStorageClassName = "e2e-local-sc"

	// Capacity layout. One node gets a big disk, every other node a small one, so "which node fits"
	// is decided by the fixture instead of by runtime VGFree arithmetic.
	schedBigDiskSize   = "8Gi"
	schedSmallDiskSize = "2Gi"

	// schedSteerPVCSize fits the big node only. It must stay above VGFree(small) (~2044Mi) and at or
	// below VGFree(big)/2 (~4094Mi): the extender subtracts a PVC's own 60s reservation on a repeat
	// filter call for that PVC, which the annotation spec always triggers (launcher, then attachment).
	schedSteerPVCSize = "3Gi"
	// schedBlockPVCSize fits nowhere — 2.5x the biggest VG.
	schedBlockPVCSize = "20Gi"

	// Every object these specs create in the default namespace carries one of these prefixes;
	// teardown is prefix-driven.
	schedPVCPrefix = "e2e-sched-pvc-"
	schedPodPrefix = "e2e-sched-pod-"

	schedExtenderNamespace      = "d8-sds-node-configurator"
	schedExtenderDeploymentName = "sds-common-scheduler-extender"

	// Timeouts. Poll interval is shared; each phase gets its own budget so a slow disk attach cannot
	// eat the budget of a stuck PV.
	schedPollInterval           = 5 * time.Second
	schedBDAppearTimeout        = 5 * time.Minute
	schedBDCleanupTimeout       = 2 * time.Minute
	schedExtenderRolloutTimeout = 5 * time.Minute
	schedLSCCreatedTimeout      = 3 * time.Minute
	schedSCAppearTimeout        = 2 * time.Minute
	schedLSCDeleteTimeout       = 5 * time.Minute
	schedPodScheduledTimeout    = 3 * time.Minute
	schedPVCBoundTimeout        = 5 * time.Minute
	schedPodRunningTimeout      = 5 * time.Minute
	schedPodDeleteTimeout       = 3 * time.Minute
	schedPVCDeleteTimeout       = 3 * time.Minute
	schedPVDeleteTimeout        = 5 * time.Minute
)

// schedNodeDisk records one node's fixture: the data disk and the LVMVolumeGroup built on it.
// AfterAll needs the disk name to detach and delete it.
type schedNodeDisk struct {
	node     string
	diskName string
	lvgName  string
}

// ---=== Fixture ===--- //

// schedPickNodes assigns the capacity roles. The big node is the alphabetically first node without a
// control-plane role label; if the cluster has none, the alphabetically first node overall. The master
// is deliberately not made big: our Pods tolerate control-plane NoSchedule but not NoExecute, so on a
// stand with a NoExecute master the steer specs would aim a Pod at a node that will not admit it.
func schedPickNodes(ctx context.Context, k8s client.Client) (bigNode string, smallNodes []string) {
	var nodeList corev1.NodeList
	Expect(k8s.List(ctx, &nodeList)).To(Succeed(), "list nodes")
	Expect(nodeList.Items).NotTo(BeEmpty(), "cluster must have at least one node")

	var all, workers []string
	for i := range nodeList.Items {
		node := &nodeList.Items[i]
		all = append(all, node.Name)
		_, isControlPlane := node.Labels["node-role.kubernetes.io/control-plane"]
		_, isMaster := node.Labels["node-role.kubernetes.io/master"]
		if !isControlPlane && !isMaster {
			workers = append(workers, node.Name)
		}
	}
	slices.Sort(all)
	slices.Sort(workers)

	bigNode = all[0]
	if len(workers) > 0 {
		bigNode = workers[0]
	}
	for _, name := range all {
		if name != bigNode {
			smallNodes = append(smallNodes, name)
		}
	}
	return bigNode, smallNodes
}

// attachDiskWithLVG creates a data disk of the given size, attaches it to the node, waits for the new
// consumable BlockDevice and builds a Ready LVMVolumeGroup on it.
func attachDiskWithLVG(
	ctx context.Context,
	cl *e2e.Cluster,
	storageClass, runID, node, size string,
) schedNodeDisk {
	nodeSafe := strings.ReplaceAll(strings.ReplaceAll(node, ".", "-"), "_", "-")
	diskName := fmt.Sprintf("e2e-sched-disk-%s-%s", runID, nodeSafe)

	before, listErr := kubernetes.GetConsumableBlockDevicesByNode(ctx, cl.RESTConfig(), node)
	Expect(listErr).NotTo(HaveOccurred(), "list consumable BlockDevices on node %s", node)

	By(fmt.Sprintf("Creating disk %s (%s) and attaching it to node %s", diskName, size, node))
	disk, createErr := cl.Disks().CreateDisk(ctx, e2e.DiskSpec{
		Name:         diskName,
		Size:         resource.MustParse(size),
		StorageClass: storageClass,
	})
	Expect(createErr).NotTo(HaveOccurred(), "create disk %s", diskName)
	Expect(cl.Disks().AttachDisk(ctx, node, disk.Name)).
		To(Succeed(), "attach disk %s to node %s", disk.Name, node)

	newBD, bdErr := framework.WaitNewConsumableBlockDevice(ctx, cl.RESTConfig(), node, before, schedBDAppearTimeout)
	Expect(bdErr).NotTo(HaveOccurred(), "a new consumable BlockDevice must appear on node %s after attach", node)

	lvgName := fmt.Sprintf("%s%s-%s", lvmVGNamePrefix, runID, nodeSafe)
	vgName := fmt.Sprintf("e2e-vg-%s-%s", runID, nodeSafe)
	By(fmt.Sprintf("Creating LVMVolumeGroup %s (VG %s) on node %s from BlockDevice %s", lvgName, vgName, node, newBD.Name))
	Expect(kubernetes.CreateLVMVolumeGroup(ctx, cl.RESTConfig(), lvgName, node, []string{newBD.Name}, vgName)).
		To(Succeed(), "create LVMVolumeGroup %s", lvgName)
	Expect(kubernetes.WaitForLVMVolumeGroupReady(ctx, cl.RESTConfig(), lvgName, lvmVolumeGroupReadyTimeout)).
		To(Succeed(), "LVMVolumeGroup %s must reach Ready", lvgName)

	return schedNodeDisk{node: node, diskName: disk.Name, lvgName: lvgName}
}

// createLocalStorageClass creates one Thick WaitForFirstConsumer LocalStorageClass over every LVG and
// waits until its controller has produced the derived StorageClass.
func createLocalStorageClass(ctx context.Context, cl *e2e.Cluster, k8s client.Client, lvgNames []string) string {
	lvmVolumeGroups := make([]any, 0, len(lvgNames))
	for _, name := range lvgNames {
		lvmVolumeGroups = append(lvmVolumeGroups, map[string]any{"name": name})
	}

	lsc := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "storage.deckhouse.io/v1alpha1",
		"kind":       "LocalStorageClass",
		"metadata":   map[string]any{"name": localStorageClassName},
		"spec": map[string]any{
			"lvm": map[string]any{
				"lvmVolumeGroups": lvmVolumeGroups,
				"type":            "Thick",
			},
			"reclaimPolicy":     "Delete",
			"volumeBindingMode": "WaitForFirstConsumer",
		},
	}}

	dynClient := cl.Dynamic()
	By(fmt.Sprintf("Creating LocalStorageClass %s over LVMVolumeGroups %v", localStorageClassName, lvgNames))
	_, createErr := dynClient.Resource(localStorageClassGVR).Create(ctx, lsc, metav1.CreateOptions{})
	Expect(createErr).NotTo(HaveOccurred(), "create LocalStorageClass %s", localStorageClassName)

	Eventually(func(g Gomega) {
		obj, err := dynClient.Resource(localStorageClassGVR).Get(ctx, localStorageClassName, metav1.GetOptions{})
		g.Expect(err).NotTo(HaveOccurred())
		phase, _, _ := unstructured.NestedString(obj.Object, "status", "phase")
		g.Expect(phase).To(Equal("Created"), "LocalStorageClass phase")
	}, schedLSCCreatedTimeout, schedPollInterval).Should(Succeed())

	Eventually(func(g Gomega) {
		var sc storagev1.StorageClass
		g.Expect(k8s.Get(ctx, client.ObjectKey{Name: localStorageClassName}, &sc)).To(Succeed())
	}, schedSCAppearTimeout, schedPollInterval).Should(Succeed(), "derived StorageClass %s must appear", localStorageClassName)

	return localStorageClassName
}

// waitExtenderRolledOut blocks until the extender Deployment is fully rolled out on the image under
// test. Two reasons, both invisible in the specs themselves: the extender is registered with
// ignorable: true, so a restart mid-spec turns into a timeout kube-scheduler silently ignores; and a
// surge Pod means two independent in-memory reservation caches for one PVC.
func waitExtenderRolledOut(ctx context.Context, k8s client.Client) {
	By(fmt.Sprintf("Waiting for Deployment %s/%s to be fully rolled out", schedExtenderNamespace, schedExtenderDeploymentName))
	Eventually(func(g Gomega) {
		var dep appsv1.Deployment
		g.Expect(k8s.Get(ctx, client.ObjectKey{
			Namespace: schedExtenderNamespace,
			Name:      schedExtenderDeploymentName,
		}, &dep)).To(Succeed())

		wanted := int32(1)
		if dep.Spec.Replicas != nil {
			wanted = *dep.Spec.Replicas
		}
		g.Expect(dep.Status.ObservedGeneration).To(BeNumerically(">=", dep.Generation),
			"controller has not observed the current spec yet")
		g.Expect(dep.Status.UpdatedReplicas).To(Equal(wanted), "updated replicas")
		g.Expect(dep.Status.ReadyReplicas).To(Equal(wanted), "ready replicas")
		g.Expect(dep.Status.Replicas).To(Equal(wanted), "surge Pod present: two reservation caches for one PVC")
		g.Expect(dep.Status.UnavailableReplicas).To(BeZero(), "unavailable replicas")
	}, schedExtenderRolloutTimeout, schedPollInterval).Should(Succeed())
}

// ---=== Teardown ===--- //

// cleanupLocalStorageClasses deletes every e2e LocalStorageClass and waits until it is gone. It never
// strips the *.deckhouse.io finalizer: the deny-deckhouse-finalizers VAP forbids it, and the LSC is
// removed by its own controller once no consumer is left.
func cleanupLocalStorageClasses(ctx context.Context, cl *e2e.Cluster) {
	dynClient := cl.Dynamic()

	list, listErr := dynClient.Resource(localStorageClassGVR).List(ctx, metav1.ListOptions{})
	if apierrors.IsNotFound(listErr) {
		return // CRD absent: sds-local-volume is not installed, nothing of ours can exist
	}
	Expect(listErr).NotTo(HaveOccurred(), "list LocalStorageClasses")

	for i := range list.Items {
		name := list.Items[i].GetName()
		if !strings.HasPrefix(name, "e2e-") {
			continue
		}
		By(fmt.Sprintf("Deleting LocalStorageClass %s", name))
		if delErr := dynClient.Resource(localStorageClassGVR).Delete(ctx, name, metav1.DeleteOptions{}); delErr != nil {
			Expect(apierrors.IsNotFound(delErr)).To(BeTrue(), "delete LocalStorageClass %s: %v", name, delErr)
		}
	}

	Eventually(func(g Gomega) {
		current, err := dynClient.Resource(localStorageClassGVR).List(ctx, metav1.ListOptions{})
		g.Expect(err).NotTo(HaveOccurred())
		var left []string
		for i := range current.Items {
			if name := current.Items[i].GetName(); strings.HasPrefix(name, "e2e-") {
				left = append(left, name)
			}
		}
		g.Expect(left).To(BeEmpty(), "LocalStorageClasses still present: %v", left)
	}, schedLSCDeleteTimeout, schedPollInterval).Should(Succeed(),
		"an LSC stuck in Terminating means its controller could not finalize it — check whether an LVG was deleted first")
}

// deleteWorkload removes every Pod and PVC these specs create and waits until the PersistentVolumes
// behind them are gone. Prefix-driven, so it also clears leftovers from a crashed previous run.
// Nothing is force-finalized: a Pod stuck Terminating means the volume never unmounted and a PV stuck
// Terminating means CSI never deleted the LV — the bugs this suite exists to surface.
func deleteWorkload(ctx context.Context, k8s client.Client) {
	inDefault := client.InNamespace(metav1.NamespaceDefault)

	// Pods first: deleting a PVC while a Pod still mounts it leaves the PVC in Terminating.
	var pods corev1.PodList
	Expect(k8s.List(ctx, &pods, inDefault)).To(Succeed(), "list Pods")
	for i := range pods.Items {
		if !strings.HasPrefix(pods.Items[i].Name, schedPodPrefix) {
			continue
		}
		By(fmt.Sprintf("Deleting Pod %s", pods.Items[i].Name))
		Expect(client.IgnoreNotFound(k8s.Delete(ctx, &pods.Items[i], client.GracePeriodSeconds(0)))).
			To(Succeed(), "delete Pod %s", pods.Items[i].Name)
	}
	Eventually(func(g Gomega) {
		var current corev1.PodList
		g.Expect(k8s.List(ctx, &current, inDefault)).To(Succeed())
		var left []string
		for i := range current.Items {
			pod := &current.Items[i]
			if strings.HasPrefix(pod.Name, schedPodPrefix) {
				left = append(left, fmt.Sprintf("%s(phase=%s,finalizers=%v)", pod.Name, pod.Status.Phase, pod.Finalizers))
			}
		}
		g.Expect(left).To(BeEmpty(), "Pods still present: %v", left)
	}, schedPodDeleteTimeout, schedPollInterval).Should(Succeed())

	var pvcs corev1.PersistentVolumeClaimList
	Expect(k8s.List(ctx, &pvcs, inDefault)).To(Succeed(), "list PVCs")
	for i := range pvcs.Items {
		if !strings.HasPrefix(pvcs.Items[i].Name, schedPVCPrefix) {
			continue
		}
		By(fmt.Sprintf("Deleting PVC %s", pvcs.Items[i].Name))
		Expect(client.IgnoreNotFound(k8s.Delete(ctx, &pvcs.Items[i]))).
			To(Succeed(), "delete PVC %s", pvcs.Items[i].Name)
	}
	Eventually(func(g Gomega) {
		var current corev1.PersistentVolumeClaimList
		g.Expect(k8s.List(ctx, &current, inDefault)).To(Succeed())
		var left []string
		for i := range current.Items {
			pvc := &current.Items[i]
			if strings.HasPrefix(pvc.Name, schedPVCPrefix) {
				left = append(left, fmt.Sprintf("%s(phase=%s,finalizers=%v)", pvc.Name, pvc.Status.Phase, pvc.Finalizers))
			}
		}
		g.Expect(left).To(BeEmpty(), "PVCs still present: %v", left)
	}, schedPVCDeleteTimeout, schedPollInterval).Should(Succeed())

	// PVs outlive their PVCs while CSI detaches and removes the LV.
	Eventually(func(g Gomega) {
		var pvs corev1.PersistentVolumeList
		g.Expect(k8s.List(ctx, &pvs)).To(Succeed())
		var left []string
		for i := range pvs.Items {
			pv := &pvs.Items[i]
			if pv.Spec.StorageClassName == localStorageClassName {
				left = append(left, fmt.Sprintf("%s(phase=%s,finalizers=%v)", pv.Name, pv.Status.Phase, pv.Finalizers))
			}
		}
		g.Expect(left).To(BeEmpty(), "PersistentVolumes still present: %v", left)
	}, schedPVDeleteTimeout, schedPollInterval).Should(Succeed(),
		"a PV stuck here means CSI did not detach or delete the volume — check VolumeAttachments and VolumeFailedDelete events")
}

// ---=== Diagnostics ===--- //

// dumpLVGs prints the capacity layout as the cluster sees it. The only printer in the suite; called
// on failure so a red spec carries the numbers that explain it.
func dumpLVGs(ctx context.Context, k8s client.Client) {
	var list v1alpha1.LVMVolumeGroupList
	if err := k8s.List(ctx, &list); err != nil {
		GinkgoWriter.Printf("dumpLVGs: list failed: %v\n", err)
		return
	}
	for i := range list.Items {
		lvg := &list.Items[i]
		if !strings.HasPrefix(lvg.Name, lvmVGNamePrefix) {
			continue
		}
		GinkgoWriter.Printf("  LVG %s node=%s phase=%s vgSize=%s vgFree=%s\n",
			lvg.Name, lvg.Spec.Local.NodeName, lvg.Status.Phase,
			lvg.Status.VGSize.String(), lvg.Status.VGFree.String())
	}
}
