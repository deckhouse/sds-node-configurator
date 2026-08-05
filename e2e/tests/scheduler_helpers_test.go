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

	// podExtraPVCsAnnotation mirrors consts.PodExtraPVCsAnnotation from the extender module. The e2e
	// module does not import that module, so the key is duplicated here on purpose: a divergence fails
	// the annotation specs on the stand instead of failing to compile.
	podExtraPVCsAnnotation = "scheduler.deckhouse.io/extra-pvcs"

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

// ---=== Workload ===--- //

// podOpts covers all four Pod shapes the specs need: one that mounts a volume, a launcher that only
// advertises a PVC through the annotation, an attachment Pod pinned to the launcher's node, and a
// control Pod with none of the above.
type podOpts struct {
	mountPVC      string // mounted through spec.volumes; empty means the Pod has no volumes
	annotationPVC string // advertised only via the extra-pvcs annotation
	node          string // pin to this node with a hostname nodeSelector
}

func createPVC(ctx context.Context, k8s client.Client, name, storageClass, size string) {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: metav1.NamespaceDefault},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(size)},
			},
			StorageClassName: &storageClass,
		},
	}
	By(fmt.Sprintf("Creating PVC %s (%s, storageClass %s)", name, size, storageClass))
	Expect(k8s.Create(ctx, pvc)).To(Succeed(), "create PVC %s", name)
}

func createPod(ctx context.Context, k8s client.Client, name string, opts podOpts) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: metav1.NamespaceDefault},
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
			Containers: []corev1.Container{{
				Name:    "test",
				Image:   "busybox",
				Command: []string{"sleep", "3600"},
				// Tiny requests: these specs must be decided by storage capacity, never by CPU or memory.
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("10m"),
						corev1.ResourceMemory: resource.MustParse("16Mi"),
					},
				},
			}},
		},
	}

	if opts.mountPVC != "" {
		pod.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{Name: "data", MountPath: "/data"}}
		pod.Spec.Volumes = []corev1.Volume{{
			Name: "data",
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: opts.mountPVC},
			},
		}}
	}
	if opts.annotationPVC != "" {
		pod.Annotations = map[string]string{podExtraPVCsAnnotation: opts.annotationPVC}
	}
	if opts.node != "" {
		pod.Spec.NodeSelector = map[string]string{corev1.LabelHostname: opts.node}
	}

	By(fmt.Sprintf("Creating Pod %s (mountPVC=%q annotationPVC=%q node=%q)",
		name, opts.mountPVC, opts.annotationPVC, opts.node))
	Expect(k8s.Create(ctx, pod)).To(Succeed(), "create Pod %s", name)
}

// waitPodScheduled returns the node kube-scheduler placed the Pod on.
func waitPodScheduled(ctx context.Context, k8s client.Client, name string) string {
	var node string
	Eventually(func(g Gomega) {
		var pod corev1.Pod
		g.Expect(k8s.Get(ctx, client.ObjectKey{Namespace: metav1.NamespaceDefault, Name: name}, &pod)).To(Succeed())
		g.Expect(pod.Spec.NodeName).NotTo(BeEmpty(),
			"Pod %s is still unscheduled:\n%s", name, schedulingFailureText(ctx, k8s, &pod))
		node = pod.Spec.NodeName
	}, schedPodScheduledTimeout, schedPollInterval).Should(Succeed())
	By(fmt.Sprintf("Pod %s scheduled on node %s", name, node))
	return node
}

// waitPVCBoundAndPodRunning is the mechanical half of the steer assertion. Checking the node name
// alone is weak: without the extender every node is equal and kube-scheduler draws lots, so on three
// nodes the spec would pass by chance one time in three. A wrong node cannot produce the volume — the
// requested LV does not fit a small VG — so the PVC stays Pending and the Pod never runs.
func waitPVCBoundAndPodRunning(ctx context.Context, k8s client.Client, pvcName, podName string) {
	Eventually(func(g Gomega) {
		var pvc corev1.PersistentVolumeClaim
		g.Expect(k8s.Get(ctx, client.ObjectKey{Namespace: metav1.NamespaceDefault, Name: pvcName}, &pvc)).To(Succeed())
		g.Expect(pvc.Status.Phase).To(Equal(corev1.ClaimBound), "PVC %s phase", pvcName)
	}, schedPVCBoundTimeout, schedPollInterval).Should(Succeed(),
		"PVC %s must bind: a %s volume is only creatable on the big node", pvcName, schedSteerPVCSize)

	Eventually(func(g Gomega) {
		var pod corev1.Pod
		g.Expect(k8s.Get(ctx, client.ObjectKey{Namespace: metav1.NamespaceDefault, Name: podName}, &pod)).To(Succeed())
		g.Expect(pod.Status.Phase).To(Equal(corev1.PodRunning), "Pod %s phase", podName)
	}, schedPodRunningTimeout, schedPollInterval).Should(Succeed(),
		"Pod %s must reach Running — the volume was placed where it can actually be provisioned", podName)
}

// schedulingFailureText collects everything the cluster says about why a Pod is not scheduled: the
// PodScheduled condition plus every event on the Pod. Used both in failure messages and by the
// rejection assertion, because kube-scheduler surfaces extender reasons in either place.
func schedulingFailureText(ctx context.Context, k8s client.Client, pod *corev1.Pod) string {
	var sb strings.Builder
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodScheduled {
			fmt.Fprintf(&sb, "condition PodScheduled=%s %s: %s\n", cond.Status, cond.Reason, cond.Message)
		}
	}

	var events corev1.EventList
	if err := k8s.List(ctx, &events, client.InNamespace(pod.Namespace)); err != nil {
		fmt.Fprintf(&sb, "list events: %v\n", err)
		return sb.String()
	}
	for i := range events.Items {
		ev := &events.Items[i]
		if ev.InvolvedObject.Kind != "Pod" || ev.InvolvedObject.Name != pod.Name {
			continue
		}
		fmt.Fprintf(&sb, "event %s: %s\n", ev.Reason, ev.Message)
	}
	return sb.String()
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
