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
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/e2e/framework"
	"github.com/deckhouse/sds-node-configurator/e2e/tests/utils/consts"
	"github.com/deckhouse/storage-e2e/pkg/e2e"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// fdBaseDir must match the fileDevicesDirectory module default
	// (openapi/config-values.yaml) and DefaultFileDevicesDirectory in the agent
	// (images/agent/internal/config/config.go). Backing files are placed here so
	// the agent's base-directory allowlist accepts them.
	fdBaseDir = "/opt/deckhouse/sds/file-devices"

	fdConditionVGConfigurationApplied = "VGConfigurationApplied"
	fdReasonValidationFailed          = "ValidationFailed"
	// fdReasonFileDeviceDrift is what a removed-but-still-provisioned entry is
	// reported under. It is deliberately not ValidationFailed: the spec is
	// well-formed and everything that could be applied has been, so the two have
	// to be distinguishable for anyone alerting on them.
	fdReasonFileDeviceDrift = "FileDeviceDrift"
	// fdReasonFileDeviceNotApplied is what an entry the node could not bring up is
	// reported under — no room for the backing file, losetup refusing, a backing
	// file unlinked while its loop is still attached. Like the drift reason it is
	// in the conditions watcher's acceptableReasons, so the Volume Group keeps
	// working while it is set.
	fdReasonFileDeviceNotApplied = "FileDeviceNotApplied"
	// fdReasonCacheStale means the node has a Volume Group the agent's LVM cache
	// does not know about, so the reconcile has nothing to work from. It is the
	// reason a foreign loop-backed Volume Group used to be misreported as: taken
	// for ours, it made create refuse ("the VG is there") while update found
	// nothing cached, and the LVMVolumeGroup sat here forever pointing the operator
	// at a cache that was not the problem.
	fdReasonCacheStale = "CacheStale"
	// fdReasonVGCreationFailed is what a genuine vgcreate failure surfaces as.
	fdReasonVGCreationFailed = "VGCreationFailed"

	// fdManagedTag marks a Volume Group as this module's. It is NOT proof of
	// ownership for a loop-backed one — an image of a node disk the module used to
	// manage carries it too, which is why the agent additionally requires a
	// backing file it named itself (utils.ClassifyLoopVGs).
	fdManagedTag = "storage.deckhouse.io/enabled=true"
	// fdLegacyTagPrefix is the tag ReTag migrates to fdManagedTag. A guest running
	// LINSTOR inside a file-backed disk carries one, and until ownership was decided
	// by the backing file that was enough to have its Volume Group adopted.
	fdLegacyTagPrefix = "linstor-"

	// fdCleanupTimeout bounds node-side teardown (loop detach + backing file removal)
	// after an LVMVolumeGroup is deleted.
	fdCleanupTimeout = 5 * time.Minute

	// fdLVGReadyTimeout bounds a file-backed LVMVolumeGroup reaching Ready. Far
	// shorter than the shared lvmVolumeGroupReadyTimeout, which is sized for
	// attaching and scanning a real disk: here the agent only has to fallocate a
	// couple of GiB, losetup it and pvcreate/vgcreate, which is seconds when it
	// works. Keeping the block-device figure meant a wedged LVG burnt 15 minutes,
	// and a handful of them pushed the suite past its 3h30m budget so no results
	// came out at all. Failing in five minutes reports the same defect sooner.
	fdLVGReadyTimeout = 5 * time.Minute

	// fdRejectionTimeout bounds how long a rejected spec may take to surface on
	// VGConfigurationApplied. Validation runs before any node work, so this is short.
	fdRejectionTimeout = 3 * time.Minute

	// fdLVMConfig mirrors internal.LVMGlobalFilter + internal.LVMArchiveRetention,
	// the --config the agent passes to every LVM command.
	//
	// It is mandatory for anything touching a file-backed VG: the module's
	// NodeGroupConfiguration writes global_filter = ["r|^/dev/loop[0-9]+|"] into
	// /etc/lvm/lvm.conf, so host-wide LVM rejects loop devices outright —
	// "Cannot use /dev/loop0: device is rejected by filter config". Only the
	// agent sees loop-backed VGs, because it overrides that filter per command.
	// A plain vgs/pvs/lvs here would report a working VG as missing, and a plain
	// lvremove would silently fail to tear a thin-pool down.
	fdLVMConfig = `devices/global_filter=["r|^/dev/rbd|","r|^/dev/drbd|","r|^/dev/nbd|"] backup/retain_min=10 backup/retain_days=7`

	// fdLVMCfg is fdLVMConfig ready to paste directly into a shell command line.
	// Only for that: assigning it to a variable and expanding it unquoted splits
	// the setting into fragments. Pass fdLVMConfig where a raw value is wanted.
	//
	// Always invoke LVM through the `lvm` wrapper — `sudo -n lvm vgs`, never
	// `sudo -n vgs`. The individual binaries live in /sbin and are absent from
	// sudo's secure_path on some distributions (Debian among them), where the bare
	// form dies with "sudo: vgs: command not found" and the caller sees empty
	// output rather than an error. The agent invokes lvm.static the same way.
	fdLVMCfg = `--config '` + fdLVMConfig + `'`

	// fdFileDeviceSize is the default backing-file size for specs that need a
	// thin-pool. Kept small on purpose: the worker's root filesystem holds every
	// backing file the suite allocates, and a run that leaks a few 4Gi files fills
	// a 20Gi disk — later specs then sit Pending with vgSize=0 for their whole
	// timeout, which is how one leak turns into half an hour of red.
	fdFileDeviceSize = "2Gi"

	// fdThinPoolSize is an absolute, extent-aligned size rather than a percentage.
	// A percentage of a file-backed VG rarely lands on a 4MiB extent boundary, LVM
	// rounds the pool up, and the agent then refuses its own result:
	// "Requested Spec.ThinPool ... size 2574463795 is less than actual one 2579496960".
	fdThinPoolSize = "1Gi"

	// fdThinLVSize leaves room in the pool for a snapshot. The snapshot reconciler
	// requires free pool space of at least the source LV's size, so a 1Gi LV in a
	// 1Gi pool can never be snapshotted: "Not enough space available in thin pool
	// ...: need at least 1073741824, got 512Mi".
	fdThinLVSize = "256Mi"

	// fdNoSuchNode is a node name no cluster has. An LVMVolumeGroup pointing at it
	// is admitted (CEL does not check the node) but never provisioned, which is what
	// the admission-only specs want.
	fdNoSuchNode = "e2e-no-such-node"

	// fdMaxLVGNameLen bounds a generated LVMVolumeGroup name. The agent copies the
	// resource name into the kubernetes.io/metadata.name label, and a label value may
	// not exceed 63 characters, so a longer name makes every reconcile fail admission
	// ("metadata.labels: Invalid value: ...: must be no more than 63 characters") and
	// the group never leaves Pending. Node names differ per provider and the Commander
	// ones are long enough to cross the limit on their own, so build names through
	// fdLVGName rather than concatenating by hand.
	fdMaxLVGNameLen = 63
)

// fdNodeSafe converts a node name into a DNS-1123-safe fragment for CR names.
func fdNodeSafe(n string) string {
	return strings.ReplaceAll(strings.ReplaceAll(n, ".", "-"), "_", "-")
}

// fdLVGName builds the name of a node-scoped LVMVolumeGroup, keeping it within
// fdMaxLVGNameLen. The node fragment is what makes the name unique per node, so it is
// trimmed from the left when the name would be too long: the tail carries the distro
// and the ordinal, which is exactly what tells two nodes apart.
func fdLVGName(tag, runID, node string) string {
	head := lvmVGNamePrefix + tag + "-" + runID + "-"
	budget := fdMaxLVGNameLen - len(head)
	if budget < 1 {
		return strings.TrimRight(head[:fdMaxLVGNameLen], "-")
	}

	fragment := fdNodeSafe(node)
	if len(fragment) > budget {
		fragment = strings.TrimLeft(fragment[len(fragment)-budget:], "-")
	}

	return head + fragment
}

// fdNodeWithReadyAgent returns a node that runs a Ready sds-node-configurator agent pod.
// File-backed LVMVolumeGroups need no dedicated disk, so any node with a healthy agent can
// host the test VG; worker nodes are preferred so a control-plane taint never blocks the
// VirtualDisk attach done by the mixed block+file spec.
func fdNodeWithReadyAgent(ctx context.Context, cl *e2e.Cluster) (string, error) {
	pods, err := cl.Clientset().CoreV1().Pods(consts.SdsNodeConfiguratorAgentNamespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app=" + consts.SdsNodeConfiguratorAgentName,
	})
	if err != nil {
		return "", fmt.Errorf("list agent pods: %w", err)
	}

	ready := make(map[string]struct{}, len(pods.Items))
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.Spec.NodeName == "" || p.DeletionTimestamp != nil || p.Status.Phase != corev1.PodRunning || !isPodReady(p) {
			continue
		}
		ready[p.Spec.NodeName] = struct{}{}
	}
	if len(ready) == 0 {
		return "", fmt.Errorf("no Ready %s agent pod found in namespace %s",
			consts.SdsNodeConfiguratorAgentName, consts.SdsNodeConfiguratorAgentNamespace)
	}

	nodes, err := cl.Clientset().CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", fmt.Errorf("list nodes: %w", err)
	}
	var fallback string
	for i := range nodes.Items {
		n := &nodes.Items[i]
		if _, ok := ready[n.Name]; !ok {
			continue
		}
		if _, isControlPlane := n.Labels["node-role.kubernetes.io/control-plane"]; !isControlPlane {
			return n.Name, nil
		}
		if fallback == "" {
			fallback = n.Name
		}
	}
	if fallback == "" {
		return "", fmt.Errorf("agent pods are Ready but none of their nodes is in the node list")
	}
	return fallback, nil
}

// fdNewLVG builds a file-backed LVMVolumeGroup. With bdSelector nil the VG has no
// blockDeviceSelector at all, which is the spec.fileDevices-only case.
func fdNewLVG(
	node, lvgName, vgName string,
	bdSelector *metav1.LabelSelector,
	fileDevices []v1alpha1.LVMVolumeGroupFileDeviceSpec,
	thinPools []v1alpha1.LVMVolumeGroupThinPoolSpec,
) *v1alpha1.LVMVolumeGroup {
	return &v1alpha1.LVMVolumeGroup{
		ObjectMeta: metav1.ObjectMeta{Name: lvgName},
		Spec: v1alpha1.LVMVolumeGroupSpec{
			ActualVGNameOnTheNode: vgName,
			Type:                  "Local",
			Local:                 v1alpha1.LVMVolumeGroupLocalSpec{NodeName: node},
			BlockDeviceSelector:   bdSelector,
			FileDevices:           fileDevices,
			ThinPools:             thinPools,
		},
	}
}

// fdFileDevicesForNode returns the file-device status entries the LVG reports for node
// (nil if the node is absent or has no file devices yet).
func fdFileDevicesForNode(lvg *v1alpha1.LVMVolumeGroup, node string) []v1alpha1.LVMVolumeGroupFileDevice {
	for i := range lvg.Status.Nodes {
		if lvg.Status.Nodes[i].Name == node {
			return lvg.Status.Nodes[i].FileDevices
		}
	}
	return nil
}

// fdCountDevicesOnNode counts the block devices (non file-backed PVs) the LVG reports for node.
func fdCountDevicesOnNode(lvg *v1alpha1.LVMVolumeGroup, node string) int {
	for i := range lvg.Status.Nodes {
		if lvg.Status.Nodes[i].Name == node {
			return len(lvg.Status.Nodes[i].Devices)
		}
	}
	return 0
}

// fdNodePathExists reports whether path exists on the node. Checked as root: backing files
// live under root-owned /opt/deckhouse/sds.
func fdNodePathExists(ctx context.Context, cl *e2e.Cluster, node, path string) (bool, error) {
	out, err := framework.NodeExecChecked(ctx, cl, node,
		fmt.Sprintf(`sudo -n test -e %s && echo EXISTS || echo MISSING`, strconv.Quote(path)))
	if err != nil {
		return false, err
	}
	return strings.Contains(out, "EXISTS"), nil
}

// fdLoopBoundToFile reports whether any loop device is currently attached to path on the node.
// The raw `losetup -j` output is returned for failure messages.
func fdLoopBoundToFile(ctx context.Context, cl *e2e.Cluster, node, path string) (bool, string, error) {
	out, err := framework.NodeExecChecked(ctx, cl, node,
		fmt.Sprintf(`sudo -n losetup -j %s 2>/dev/null || true`, strconv.Quote(path)))
	if err != nil {
		return false, out, err
	}
	return strings.TrimSpace(out) != "", out, nil
}

// fdVGListedOnNode reports whether vgName is visible to vgs on the node.
func fdVGListedOnNode(ctx context.Context, cl *e2e.Cluster, node, vgName string) (bool, string, error) {
	out, err := framework.NodeExecChecked(ctx, cl, node,
		fmt.Sprintf(`sudo -n lvm vgs %s -o vg_name --noheadings 2>/dev/null || true`, fdLVMCfg))
	if err != nil {
		return false, out, err
	}
	return framework.VGInListing(out, vgName), out, nil
}

// fdPVNamesInVGOnNode returns the pv_name of every PV that belongs to vgName on the node.
// A mixed block+file VG must report at least one /dev/loop* PV (the backing file) and at
// least one non-loop PV (the block device).
func fdPVNamesInVGOnNode(ctx context.Context, cl *e2e.Cluster, node, vgName string) ([]string, string, error) {
	out, err := framework.NodeExecChecked(ctx, cl, node,
		fmt.Sprintf(`sudo -n lvm pvs %s --noheadings -o pv_name -S vg_name=%s 2>/dev/null || true`, fdLVMCfg, strconv.Quote(vgName)))
	if err != nil {
		return nil, out, err
	}
	return framework.PVNamesInListing(out), out, nil
}

// fdThinPoolDataLVPresentOnNode reports whether the thin-pool data LV for vgName/thinPoolName
// exists on the node.
func fdThinPoolDataLVPresentOnNode(ctx context.Context, cl *e2e.Cluster, node, vgName, thinPoolName string) (bool, string, error) {
	out, err := framework.NodeExecChecked(ctx, cl, node,
		fmt.Sprintf(`sudo -n lvm lvs %s -a -o lv_name,lv_attr --noheadings %s 2>/dev/null || true`, fdLVMCfg, strconv.Quote(vgName)))
	if err != nil {
		return false, out, err
	}
	return framework.ThinPoolDataLVPresent(out, thinPoolName), out, nil
}

// fdPrintLVG dumps the parts of an LVMVolumeGroup a file-backed failure needs: phase,
// sizes, conditions and both device lists.
func fdPrintLVG(lvg *v1alpha1.LVMVolumeGroup) {
	GinkgoWriter.Printf("LVMVolumeGroup %s: phase=%s vgSize=%s vgFree=%s\n",
		lvg.Name, lvg.Status.Phase, lvg.Status.VGSize.String(), lvg.Status.VGFree.String())
	for _, c := range lvg.Status.Conditions {
		GinkgoWriter.Printf("  condition %s status=%s reason=%s msg=%s\n", c.Type, c.Status, c.Reason, c.Message)
	}
	for _, n := range lvg.Status.Nodes {
		for _, d := range n.Devices {
			GinkgoWriter.Printf("  node %s device %s path=%s pvSize=%s\n", n.Name, d.BlockDevice, d.Path, d.PVSize.String())
		}
		for _, fd := range n.FileDevices {
			GinkgoWriter.Printf("  node %s fileDevice file=%s loop=%s size=%s pvUUID=%s\n",
				n.Name, fd.FilePath, fd.LoopDevice, fd.Size.String(), fd.PVUuid)
		}
	}
}

// fdCreateLVGAndWaitReady creates lvg and waits until it reports Phase Ready, dumping
// conditions while it waits. Returns the refreshed object.
func fdCreateLVGAndWaitReady(ctx context.Context, cluster *e2e.Cluster, cl client.Client, lvg *v1alpha1.LVMVolumeGroup) *v1alpha1.LVMVolumeGroup {
	GinkgoHelper()
	Expect(cl.Create(ctx, lvg)).To(Succeed())
	// An LVMVolumeGroup that never leaves Pending shows VGConfigurationApplied=True,
	// vgSize=0 and no VGReady — which explains nothing. The agent log and the node's
	// free space do, so dump them if this spec ends up failing.
	node := lvg.Spec.Local.NodeName
	DeferCleanup(func() {
		if CurrentSpecReport().Failed() && cluster != nil {
			fdDumpAgentDiagnostics(ctx, cluster, node)
		}
	})
	var created v1alpha1.LVMVolumeGroup
	Eventually(func(g Gomega) {
		g.Expect(cl.Get(ctx, client.ObjectKeyFromObject(lvg), &created)).To(Succeed())
		if created.Status.Phase != v1alpha1.PhaseReady {
			fdPrintLVG(&created)
		}
		g.Expect(created.Status.Phase).To(Equal(v1alpha1.PhaseReady), "Phase should be Ready, got %s", created.Status.Phase)
	}, fdLVGReadyTimeout, 10*time.Second).Should(Succeed())
	return &created
}

// fdDumpAgentDiagnostics prints the node's free space and the tail of the agent log.
// An LVMVolumeGroup stuck at VGConfigurationApplied=True with vgSize=0 and no VGReady
// says nothing about why; the agent log names the command that failed.
func fdDumpAgentDiagnostics(ctx context.Context, cl *e2e.Cluster, node string) {
	fdLogFreeSpace(ctx, cl, node, "at failure")

	pods, err := cl.Clientset().CoreV1().Pods(consts.SdsNodeConfiguratorAgentNamespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app=" + consts.SdsNodeConfiguratorAgentName,
		FieldSelector: "spec.nodeName=" + node,
	})
	if err != nil {
		GinkgoWriter.Printf("list agent pods on %s: %v\n", node, err)
		return
	}
	fdDumpNodeLVMState(ctx, cl, node)

	// 80 lines only ever showed the failing command, never the scan cycle that
	// preceded it. The reconcile decision that matters — create vs update — is
	// several hundred lines earlier at DEBUG.
	tail := int64(400)
	for i := range pods.Items {
		name := pods.Items[i].Name
		raw, err := cl.Clientset().CoreV1().Pods(consts.SdsNodeConfiguratorAgentNamespace).
			GetLogs(name, &corev1.PodLogOptions{
				Container: consts.SdsNodeConfiguratorAgentContainer,
				TailLines: &tail,
			}).DoRaw(ctx)
		if err != nil {
			GinkgoWriter.Printf("logs of %s: %v\n", name, err)
			continue
		}
		GinkgoWriter.Printf("--- tail of %s ---\n%s\n", name, string(raw))
	}
}

// fdExpectNoFalseConditions fails if any status condition is False.
func fdExpectNoFalseConditions(lvg *v1alpha1.LVMVolumeGroup) {
	GinkgoHelper()
	for _, c := range lvg.Status.Conditions {
		Expect(c.Status).NotTo(Equal(metav1.ConditionFalse),
			"condition %s has status False: reason=%s message=%s", c.Type, c.Reason, c.Message)
	}
}

// fdDeleteLVGAndWaitGone deletes the LVMVolumeGroup and waits until its CR is gone. The
// finalizer is only cleared after the agent detaches the loop devices and removes the
// backing files, so a successful delete also confirms node-side cleanup ran — finalizers
// are deliberately not stripped here.
func fdDeleteLVGAndWaitGone(ctx context.Context, cl client.Client, name string) {
	GinkgoHelper()
	Expect(client.IgnoreNotFound(
		cl.Delete(ctx, &v1alpha1.LVMVolumeGroup{ObjectMeta: metav1.ObjectMeta{Name: name}}),
	)).To(Succeed())
	Eventually(func(g Gomega) {
		var cur v1alpha1.LVMVolumeGroup
		err := cl.Get(ctx, client.ObjectKey{Name: name}, &cur)
		if err == nil {
			fdPrintLVG(&cur)
		}
		g.Expect(apierrors.IsNotFound(err)).To(BeTrue(), "LVMVolumeGroup %s should be deleted", name)
	}, fdCleanupTimeout, 8*time.Second).Should(Succeed())
}

// fdExpectBackingFileGone waits until the backing file is removed and asserts the loop
// device is detached — the node-side half of a file-backed LVG delete.
func fdExpectBackingFileGone(ctx context.Context, cl *e2e.Cluster, node, filePath string) {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		stillThere, err := fdNodePathExists(ctx, cl, node, filePath)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(stillThere).To(BeFalse(), "backing file %s should be removed after delete", filePath)
	}, 3*time.Minute, 10*time.Second).Should(Succeed())

	stillBound, loopOut, err := fdLoopBoundToFile(ctx, cl, node, filePath)
	Expect(err).NotTo(HaveOccurred())
	Expect(stillBound).To(BeFalse(), "loop device should be detached from %s after delete; losetup:\n%s", filePath, loopOut)
}

// ---=== Node-side loop / LVM inspection ===--- //

// fdLoopsForFile returns every loop device currently attached to path. More than one means
// the agent leaked a duplicate attachment, which silently doubles the VG.
func fdLoopsForFile(ctx context.Context, cl *e2e.Cluster, node, path string) ([]string, string, error) {
	out, err := framework.NodeExecChecked(ctx, cl, node,
		fmt.Sprintf(`sudo -n losetup -j %s 2>/dev/null || true`, strconv.Quote(path)))
	if err != nil {
		return nil, out, err
	}
	var loops []string
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		// `losetup -j` prints "/dev/loop3: 0 /path/to/file".
		if name, _, found := strings.Cut(line, ":"); found {
			loops = append(loops, strings.TrimSpace(name))
		}
	}
	return loops, out, nil
}

// fdDirectIOOnLoop reports the DIO flag of a loop device. The agent requests direct
// I/O right after attaching so the page cache does not hold a second copy of every
// LVM write.
//
// It is requested as a separate, best-effort call: the kernel refuses direct I/O
// outright on a backing filesystem without an ->direct_IO implementation, and
// buffered I/O is a performance regression rather than a reason to leave storage
// unprovisioned. On this stand the backing file lives on the node's root
// filesystem, which supports it — so DIO=0 here means the request failed and is
// worth looking into, not that it was never made.
func fdDirectIOOnLoop(ctx context.Context, cl *e2e.Cluster, node, loop string) (bool, string, error) {
	out, err := framework.NodeExecChecked(ctx, cl, node,
		fmt.Sprintf(`sudo -n losetup -l -n -O DIO %s 2>/dev/null || true`, strconv.Quote(loop)))
	if err != nil {
		return false, out, err
	}
	return strings.TrimSpace(out) == "1", out, nil
}

// fdVGTagsOnNode returns the tags of vgName. Managed VGs carry both
// storage.deckhouse.io/enabled=true and storage.deckhouse.io/lvmVolumeGroupName=<lvg>;
// the latter is what gates loop-PV ownership.
func fdVGTagsOnNode(ctx context.Context, cl *e2e.Cluster, node, vgName string) (string, error) {
	out, err := framework.NodeExecChecked(ctx, cl, node,
		fmt.Sprintf(`sudo -n lvm vgs %s -o vg_tags --noheadings %s 2>/dev/null || true`, fdLVMCfg, strconv.Quote(vgName)))
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(out), nil
}

// fdBackingFilesForLVG narrows fdManagedBackingFilesInDir to the files owned by one
// LVMVolumeGroup. Leak assertions must be scoped this way: several file-devices Describes
// share the base directory and Ginkgo randomises the order of top-level containers, so a
// count over the whole directory would be order-dependent.
func fdBackingFilesForLVG(ctx context.Context, cl *e2e.Cluster, node, lvgName string) ([]string, error) {
	all, err := fdManagedBackingFilesInDir(ctx, cl, node, fdBaseDir)
	if err != nil {
		return nil, err
	}
	var mine []string
	for _, f := range all {
		if strings.HasPrefix(filepath.Base(f), internalFileDevicePrefix+lvgName+internalFileDeviceSeparator) {
			mine = append(mine, f)
		}
	}
	return mine, nil
}

// fdBackingFileName renders the backing file the agent creates for one
// spec.fileDevices entry, mirroring utils.BuildFileDevicePath: the e2e module does not
// import the agent's internal package.
func fdBackingFileName(lvgName, entryName string) string {
	return internalFileDevicePrefix + lvgName + internalFileDeviceSeparator + entryName + internalFileDeviceSuffix
}

// These mirror internal.FileDevicePrefix / the separator / internal.FileDeviceImageSuffix
// in the agent: "<dir>/sds-<lvgName>.<entryName>.img".
const (
	internalFileDevicePrefix    = "sds-"
	internalFileDeviceSeparator = "."
	internalFileDeviceSuffix    = ".img"
)

// fdManagedBackingFilesInDir lists the agent-owned backing files (sds-*.img) present in dir.
func fdManagedBackingFilesInDir(ctx context.Context, cl *e2e.Cluster, node, dir string) ([]string, error) {
	out, err := framework.NodeExecChecked(ctx, cl, node,
		fmt.Sprintf(`sudo -n sh -c 'ls -1 %s/sds-*.img 2>/dev/null' || true`, dir))
	if err != nil {
		return nil, err
	}
	var files []string
	for _, line := range strings.Split(out, "\n") {
		if f := strings.TrimSpace(line); f != "" {
			files = append(files, f)
		}
	}
	return files, nil
}

// fdDetachLoopAndDeactivateVG simulates the node-side state after a reboot: the VG is
// deactivated and the loop mapping is torn down, so the backing file is on disk but no
// loop device points at it. Only then does the agent's startup reattach path do real work.
func fdDetachLoopAndDeactivateVG(ctx context.Context, cl *e2e.Cluster, node, vgName, loop string) {
	GinkgoHelper()
	script := fmt.Sprintf(`set -e
sudo -n lvm vgchange %s -an %s 2>&1 || true
sudo -n losetup -d %s 2>&1`, fdLVMCfg, strconv.Quote(vgName), strconv.Quote(loop))
	out, err := framework.NodeExecChecked(ctx, cl, node, script)
	Expect(err).NotTo(HaveOccurred(), "failed to detach %s / deactivate %s:\n%s", loop, vgName, out)
}

// fdRunThinPoolTeardown removes a thin-pool stack and logs the result. Discarding
// it hides the one failure that matters: if the LVs survive, the LVMVolumeGroup
// hangs in Terminating and the spec's cleanup burns its whole timeout.
func fdRunThinPoolTeardown(ctx context.Context, cl *e2e.Cluster, node, script string) {
	out, err := framework.NodeExecChecked(ctx, cl, node, script)
	if err != nil {
		GinkgoWriter.Printf("thin-pool teardown on %s reported: %v\n%s\n", node, err, out)
	}
}

// fdSweepLeakedBackingFiles detaches and removes every agent-owned backing file left
// in the base dir. Ordinarily the agent does this on delete, but a spec whose
// LVMVolumeGroup had to be force-removed (finalizer stripped) leaves the file behind,
// and those accumulate until the node's filesystem is full — after which every later
// spec sits Pending with vgSize=0. Runs in AfterAll, and logs what it found: a
// non-empty sweep means some spec's own cleanup did not do its job.
func fdSweepLeakedBackingFiles(ctx context.Context, cl *e2e.Cluster, node string) {
	files, err := fdManagedBackingFilesInDir(ctx, cl, node, fdBaseDir)
	if err != nil {
		GinkgoWriter.Printf("leaked-file sweep on %s: %v\n", node, err)
		return
	}
	if len(files) == 0 {
		return
	}
	GinkgoWriter.Printf("leaked backing files on %s: %v\n", node, files)
	for _, f := range files {
		script := fmt.Sprintf(`set +e
for l in $(sudo -n losetup -j %s 2>/dev/null | cut -d: -f1); do sudo -n losetup -d "$l"; done
sudo -n rm -f %s
exit 0`, strconv.Quote(f), strconv.Quote(f))
		if out, err := framework.NodeExecChecked(ctx, cl, node, script); err != nil {
			GinkgoWriter.Printf("  removing %s: %v\n%s\n", f, err, out)
		}
	}
}

// fdDumpNodeLVMState prints what LVM on the node actually sees, using the agent's
// own --config. When the agent re-runs create for a VG that already exists, the
// question is whether the node has the VG while the agent's cache does not — this
// is the node half of that comparison.
func fdDumpNodeLVMState(ctx context.Context, cl *e2e.Cluster, node string) {
	for _, c := range []struct{ what, cmd string }{
		{"vgs", `sudo -n lvm vgs ` + fdLVMCfg + ` -o vg_name,vg_uuid,vg_size,vg_tags --noheadings 2>&1 || true`},
		{"pvs", `sudo -n lvm pvs ` + fdLVMCfg + ` -o pv_name,vg_name,vg_uuid --noheadings 2>&1 || true`},
		{"lvs", `sudo -n lvm lvs ` + fdLVMCfg + ` -a -o lv_name,vg_name,lv_attr --noheadings 2>&1 || true`},
		{"losetup", `sudo -n losetup -a 2>&1 || true`},
	} {
		out, err := framework.NodeExecChecked(ctx, cl, node, c.cmd)
		if err != nil {
			GinkgoWriter.Printf("node %s %s: %v\n", node, c.what, err)
			continue
		}
		GinkgoWriter.Printf("--- node %s: %s ---\n%s\n", node, c.what, out)
	}
}

// fdLogFreeSpace records how much room the base dir has left, so a Pending
// LVMVolumeGroup can be told apart from a full disk without guessing.
func fdLogFreeSpace(ctx context.Context, cl *e2e.Cluster, node, when string) {
	avail, err := fdNodeAvailableBytes(ctx, cl, node, fdBaseDir)
	if err != nil {
		GinkgoWriter.Printf("free space in %s on %s (%s): %v\n", fdBaseDir, node, when, err)
		return
	}
	GinkgoWriter.Printf("free space in %s on %s (%s): %.2f GiB\n",
		fdBaseDir, node, when, float64(avail)/(1<<30))
}

// ---=== Foreign (unmanaged) loop devices ===--- //

// fdCreateDiskOrSkip creates a disk, skipping the spec when the cluster provider
// has no disk management at all.
//
// Commander hands out a cluster, not the infrastructure under it, so every disk
// operation there fails with ErrDisksUnsupported. CI already keeps disk-dependent
// specs off that provider with the needs-disks label; this turns a filter that
// was forgotten into a clear skip instead of a confusing failure.
func fdCreateDiskOrSkip(ctx context.Context, cl *e2e.Cluster, spec e2e.DiskSpec) (*e2e.Disk, error) {
	GinkgoHelper()
	disk, err := cl.Disks().CreateDisk(ctx, spec)
	if errors.Is(err, e2e.ErrDisksUnsupported) {
		Skip(fmt.Sprintf("cluster provider %q cannot attach block devices; this spec needs one", cl.ProviderName()))
	}
	return disk, err
}

// fdCreateAdoptableLoopVG builds, directly on the node, exactly what the agent itself
// would have left behind: a backing file under the managed name, a loop device, a PV and
// a Volume Group carrying both the "managed" tag and the owning LVMVolumeGroup's name.
// No LVMVolumeGroup resource is created — that is the point.
//
// This is how the import path is exercised. The alternative, creating a real
// LVMVolumeGroup and then deleting the resource out from under it, cannot be made
// reliable: stripping the finalizer and deleting is a race against the agent, which
// re-adds the finalizer on every reconcile, and losing that race means the agent runs
// its delete flow and tears down the very VG the spec needs to survive.
func fdCreateAdoptableLoopVG(ctx context.Context, cl *e2e.Cluster, node, dir, lvgName, entryName, vgName, size string) string {
	GinkgoHelper()
	filePath := dir + "/" + fdBackingFileName(lvgName, entryName)
	script := fmt.Sprintf(`set -e
sudo -n mkdir -p %s
sudo -n fallocate -l %s %s
LOOP=$(sudo -n losetup --find --nooverlap --show %s)
sudo -n lvm pvcreate %s -y -ff "$LOOP" >/dev/null
sudo -n lvm vgcreate %s --addtag %s --addtag %s %s "$LOOP" >/dev/null
echo "$LOOP"`,
		strconv.Quote(dir), strconv.Quote(size), strconv.Quote(filePath), strconv.Quote(filePath),
		fdLVMCfg, fdLVMCfg,
		strconv.Quote("storage.deckhouse.io/enabled=true"),
		strconv.Quote("storage.deckhouse.io/lvmVolumeGroupName="+lvgName),
		strconv.Quote(vgName))

	out, err := framework.NodeExecChecked(ctx, cl, node, script)
	Expect(err).NotTo(HaveOccurred(), "failed to create an adoptable loop VG %s:\n%s", vgName, out)

	// The agent's LVM cache is filled from udev events, so a Volume Group created
	// behind its back stays invisible until something makes it rescan. The
	// block-device auto-import spec nudges it the same way after its manual vgcreate.
	framework.TriggerLVMDiscovery(ctx, cl, node)

	GinkgoWriter.Printf("Adoptable loop VG %s created on %s: %s\n", vgName, node, filePath)
	return filePath
}

// fdForeignLoopVG is an unmanaged loop-backed Volume Group created directly on the node,
// standing in for nested LVM inside a guest VM. The agent must never adopt it.
type fdForeignLoopVG struct {
	Node     string
	Dir      string
	FilePath string
	VGName   string
	// Loop is empty while the image is detached — the state fdStageForeignLoopVGImage
	// leaves it in, and the one fdDetachForeignLoopVG returns it to.
	Loop string
	// VGUUID is set only for a staged image, and says the VG name is not a safe
	// handle: a staged image deliberately duplicates a managed Volume Group's name,
	// so anything addressing it by name is free to hit the module's Volume Group
	// instead. Teardown and assertions go by UUID whenever this is set.
	VGUUID string
}

// fdCreateForeignLoopVG builds a loop-backed VG on the node without any deckhouse tag.
// basename lets a caller mimic the agent's own naming (sds-<lvg>.<entry>.img) to prove
// ownership is decided by the LVG name inside the basename, not by the pattern alone.
func fdCreateForeignLoopVG(ctx context.Context, cl *e2e.Cluster, node, dir, basename, vgName string) *fdForeignLoopVG {
	GinkgoHelper()
	return fdCreateTaggedForeignLoopVG(ctx, cl, node, dir, basename, vgName, nil)
}

// fdCreateTaggedForeignLoopVG is fdCreateForeignLoopVG with LVM tags of the caller's
// choosing, so a spec can build the case the tag-only ownership check got wrong: a
// Volume Group that carries the module's own tags and is still not the module's,
// because its backing file is one this agent never named.
//
// That is not a contrived shape. It is what an operator produces with
// `losetup -f /backup/node2-root.img` while restoring a node, or what a nested
// cluster on a rawfile-backed volume looks like from the host — and a guest running
// LINSTOR supplies the legacy tag instead.
func fdCreateTaggedForeignLoopVG(ctx context.Context, cl *e2e.Cluster, node, dir, basename, vgName string, tags []string) *fdForeignLoopVG {
	GinkgoHelper()
	filePath := dir + "/" + basename

	tagArgs := ""
	for _, tag := range tags {
		tagArgs += " --addtag " + strconv.Quote(tag)
	}

	script := fmt.Sprintf(`set -e
sudo -n mkdir -p %s
sudo -n fallocate -l 1G %s
LOOP=$(sudo -n losetup --find --show %s)
sudo -n lvm pvcreate %s -y -ff "$LOOP" >/dev/null
sudo -n lvm vgcreate %s%s %s "$LOOP" >/dev/null
echo "$LOOP"`, strconv.Quote(dir), strconv.Quote(filePath), strconv.Quote(filePath),
		fdLVMCfg, fdLVMCfg, tagArgs, strconv.Quote(vgName))

	out, err := framework.NodeExecChecked(ctx, cl, node, script)
	Expect(err).NotTo(HaveOccurred(), "failed to create foreign loop VG %s:\n%s", vgName, out)

	loop := strings.TrimSpace(out)
	// The script echoes the loop path last; tolerate leading LVM chatter.
	if lines := strings.Split(loop, "\n"); len(lines) > 0 {
		loop = strings.TrimSpace(lines[len(lines)-1])
	}
	Expect(loop).To(HavePrefix("/dev/loop"), "expected a loop device path, got %q (full output:\n%s)", loop, out)

	GinkgoWriter.Printf("Foreign loop VG %s created on %s: %s -> %s (tags: %v)\n", vgName, node, filePath, loop, tags)
	return &fdForeignLoopVG{Node: node, Dir: dir, FilePath: filePath, VGName: vgName, Loop: loop}
}

// fdStageForeignLoopVGImage builds a foreign loop VG and then detaches its loop,
// leaving the Volume Group on the image alone.
//
// It is the only way to get two Volume Groups of one name onto a node: `vgcreate`
// refuses a name that is already taken ("A volume group called X already exists"),
// so the duplicate cannot be built after the managed Volume Group exists. Staging
// it first, letting the agent create its own, and plugging the image back in is
// also exactly how the situation arises in production — `losetup -f
// /backup/node2-root.img` while restoring a node.
func fdStageForeignLoopVGImage(ctx context.Context, cl *e2e.Cluster, node, dir, basename, vgName string, tags []string) *fdForeignLoopVG {
	GinkgoHelper()
	f := fdCreateTaggedForeignLoopVG(ctx, cl, node, dir, basename, vgName, tags)

	out, err := framework.NodeExecChecked(ctx, cl, node,
		fmt.Sprintf(`sudo -n lvm vgs %s --noheadings -o vg_uuid %s 2>/dev/null`, fdLVMCfg, strconv.Quote(vgName)))
	Expect(err).NotTo(HaveOccurred(), "failed to read the UUID of the staged VG %s:\n%s", vgName, out)
	f.VGUUID = strings.TrimSpace(out)
	Expect(f.VGUUID).NotTo(BeEmpty(), "the staged VG %s reported no UUID:\n%s", vgName, out)

	fdDetachForeignLoopVG(ctx, cl, f)
	GinkgoWriter.Printf("Staged foreign loop VG %s (VG_UUID=%s) on %s: %s, now detached\n", vgName, f.VGUUID, node, f.FilePath)
	return f
}

// fdAttachForeignLoopVGImage plugs a staged image back in, which is the moment the
// node starts carrying two Volume Groups of the same name.
func fdAttachForeignLoopVGImage(ctx context.Context, cl *e2e.Cluster, f *fdForeignLoopVG) {
	GinkgoHelper()
	out, err := framework.NodeExecChecked(ctx, cl, f.Node,
		fmt.Sprintf(`sudo -n losetup --find --show %s`, strconv.Quote(f.FilePath)))
	Expect(err).NotTo(HaveOccurred(), "failed to attach the staged image %s:\n%s", f.FilePath, out)

	loop := strings.TrimSpace(out)
	if lines := strings.Split(loop, "\n"); len(lines) > 0 {
		loop = strings.TrimSpace(lines[len(lines)-1])
	}
	Expect(loop).To(HavePrefix("/dev/loop"), "expected a loop device path, got %q (full output:\n%s)", loop, out)

	f.Loop = loop
	GinkgoWriter.Printf("Attached the staged image %s on %s: %s (VG %s, VG_UUID=%s)\n", f.FilePath, f.Node, loop, f.VGName, f.VGUUID)
}

// fdDetachForeignLoopVG takes the image back out without removing it.
//
// A spec that duplicated a managed VG name has to call this before its
// LVMVolumeGroup is torn down: the agent's own cleanup addresses its Volume Group
// by name, and while the duplicate is attached that name is ambiguous.
func fdDetachForeignLoopVG(ctx context.Context, cl *e2e.Cluster, f *fdForeignLoopVG) {
	GinkgoHelper()
	if f == nil || f.Loop == "" {
		return
	}
	out, err := framework.NodeExecChecked(ctx, cl, f.Node,
		fmt.Sprintf(`sudo -n losetup -d %s`, strconv.Quote(f.Loop)))
	Expect(err).NotTo(HaveOccurred(), "failed to detach %s from %s:\n%s", f.Loop, f.FilePath, out)
	f.Loop = ""
}

// fdCreateForeignLoopVGWithLV is fdCreateTaggedForeignLoopVG plus a thick logical
// volume, left deactivated. It is what an activation spec needs: whether the agent
// touched somebody else's Volume Group is only visible as its logical volumes
// appearing on the host.
func fdCreateForeignLoopVGWithLV(ctx context.Context, cl *e2e.Cluster, node, dir, basename, vgName, lvName string, tags []string) *fdForeignLoopVG {
	GinkgoHelper()
	f := fdCreateTaggedForeignLoopVG(ctx, cl, node, dir, basename, vgName, tags)

	script := fmt.Sprintf(`set -e
sudo -n lvm lvcreate %s -y -L 128M -n %s %s >/dev/null
sudo -n lvm lvchange %s -an %s/%s >/dev/null`,
		fdLVMCfg, strconv.Quote(lvName), strconv.Quote(vgName),
		fdLVMCfg, vgName, lvName)
	out, err := framework.NodeExecChecked(ctx, cl, node, script)
	Expect(err).NotTo(HaveOccurred(), "failed to create a deactivated LV %s/%s:\n%s", vgName, lvName, out)

	active, lvsOut, err := fdLVActiveOnNode(ctx, cl, node, vgName, lvName)
	Expect(err).NotTo(HaveOccurred())
	Expect(active).To(BeFalse(), "the LV must start out inactive, otherwise the spec proves nothing; lvs:\n%s", lvsOut)

	return f
}

// fdLVActiveOnNode reports whether a logical volume is active, read from the fifth
// character of lv_attr — the same field utils.IsLVActive parses.
func fdLVActiveOnNode(ctx context.Context, cl *e2e.Cluster, node, vgName, lvName string) (bool, string, error) {
	out, err := framework.NodeExecChecked(ctx, cl, node,
		fmt.Sprintf(`sudo -n lvm lvs %s --noheadings -o lv_attr %s/%s 2>/dev/null || true`,
			fdLVMCfg, vgName, lvName))
	if err != nil {
		return false, out, err
	}
	attr := strings.TrimSpace(out)
	if len(attr) < 5 {
		return false, out, nil
	}
	return attr[4] == 'a', out, nil
}

// fdVGHasTagOnNode reports whether vgName carries an exact tag.
func fdVGHasTagOnNode(ctx context.Context, cl *e2e.Cluster, node, vgName, tag string) (bool, string, error) {
	tags, err := fdVGTagsOnNode(ctx, cl, node, vgName)
	if err != nil {
		return false, tags, err
	}
	for _, t := range strings.Split(tags, ",") {
		if strings.TrimSpace(t) == tag {
			return true, tags, nil
		}
	}
	return false, tags, nil
}

// fdUnlinkBackingFileOnNode removes a backing file while its loop device stays
// attached — the state losetup spells "<path> (deleted)".
//
// It is an ordinary operator mistake: the file looks like a plain file, and the FAQ
// documents `fstrim` precisely because people reach for `rm` instead. The Physical
// Volume stays live on the unlinked inode, and `losetup -j <path>` — which matches by
// inode — then reports nothing, so the entry looks unprovisioned.
func fdUnlinkBackingFileOnNode(ctx context.Context, cl *e2e.Cluster, node, filePath string) {
	GinkgoHelper()
	out, err := framework.NodeExecChecked(ctx, cl, node,
		fmt.Sprintf(`set -e
sudo -n rm -f %s
# Prove the state the spec is about: the loop still reads from the unlinked inode.
sudo -n losetup -a | grep -F %s || { echo "no loop reports the deleted file"; exit 1; }`,
			strconv.Quote(filePath), strconv.Quote(filePath)))
	Expect(err).NotTo(HaveOccurred(), "failed to unlink %s while its loop is attached:\n%s", filePath, out)
	GinkgoWriter.Printf("Unlinked %s on %s; losetup still reports it:\n%s\n", filePath, node, out)
}

// fdLoopsReportingBasename returns every loop device whose backing file has the given
// basename, read from `losetup -a`.
//
// It exists because fdLoopsForFile cannot answer this question: `losetup -j <path>`
// matches by inode, so an unlinked backing file yields nothing at all — and "no loop"
// is exactly the wrong answer when the point is to count how many loops are reading
// from that name, one of them possibly through a deleted inode.
func fdLoopsReportingBasename(ctx context.Context, cl *e2e.Cluster, node, basename string) ([]string, string, error) {
	out, err := framework.NodeExecChecked(ctx, cl, node,
		fmt.Sprintf(`sudo -n losetup -a 2>/dev/null | grep -F %s || true`, strconv.Quote(basename)))
	if err != nil {
		return nil, out, err
	}
	var loops []string
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		// `losetup -a` prints "/dev/loop3: [2049]:131 (/path/to/file)" and appends
		// " (deleted)" when the inode is gone.
		if name, _, found := strings.Cut(line, ":"); found {
			loops = append(loops, strings.TrimSpace(name))
		}
	}
	return loops, out, nil
}

// fdTriggerLVGReconcile nudges the LVMVolumeGroup reconciler.
//
// The scanner only re-runs the BlockDevice and LVMVolumeGroup *discoverers*; the
// reconciler is driven by events on the resource. A change made directly on the node
// therefore produces no reconcile of its own, which is what this label is for — the
// agent removes it again on sight.
func fdTriggerLVGReconcile(ctx context.Context, cl client.Client, lvgName string) {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		var cur v1alpha1.LVMVolumeGroup
		g.Expect(cl.Get(ctx, client.ObjectKey{Name: lvgName}, &cur)).To(Succeed())
		if cur.Labels == nil {
			cur.Labels = map[string]string{}
		}
		cur.Labels["storage.deckhouse.io/update-trigger"] = "true"
		g.Expect(cl.Update(ctx, &cur)).To(Succeed())
	}, time.Minute, 5*time.Second).Should(Succeed())
}

// fdSymlinkedDirOnNode creates target/ and a symlink to it beside target, both inside
// the managed base directory, and returns the symlink path.
//
// A spec.fileDevices[].directory going through a symlink is the natural way to point
// the default location at a data disk. It is also where the two spellings of one path
// part company: status.nodes[].fileDevices[].FilePath comes from losetup, which
// resolves symlink components, while the spec keeps the literal directory.
func fdSymlinkedDirOnNode(ctx context.Context, cl *e2e.Cluster, node, target, link string) {
	GinkgoHelper()
	out, err := framework.NodeExecChecked(ctx, cl, node,
		fmt.Sprintf(`set -e
sudo -n mkdir -p %s
sudo -n ln -sfn %s %s
test -d %s`, strconv.Quote(target), strconv.Quote(target), strconv.Quote(link), strconv.Quote(link)))
	Expect(err).NotTo(HaveOccurred(), "failed to prepare the symlinked directory %s -> %s:\n%s", link, target, out)
}

// fdRemoveSymlinkedDirOnNode undoes fdSymlinkedDirOnNode. Best effort: it runs in
// cleanup, where a half-created directory must not fail the spec.
func fdRemoveSymlinkedDirOnNode(ctx context.Context, cl *e2e.Cluster, node, target, link string) {
	script := fmt.Sprintf(`set +e
sudo -n rm -f %s
sudo -n rm -rf %s
exit 0`, strconv.Quote(link), strconv.Quote(target))
	if out, err := framework.NodeExecChecked(ctx, cl, node, script); err != nil {
		GinkgoWriter.Printf("symlinked dir teardown reported: %v\n%s\n", err, out)
	}
}

// fdDestroyForeignLoopVG tears the foreign VG down. Best effort: it runs in cleanup where a
// half-created VG must not fail the spec.
func fdDestroyForeignLoopVG(ctx context.Context, cl *e2e.Cluster, f *fdForeignLoopVG) {
	if f == nil {
		return
	}
	// A staged image duplicates a managed Volume Group's name, so `vgremove <name>`
	// is free to take the module's Volume Group instead. Nothing is lost by leaving
	// it out: the Volume Group's only Physical Volume is inside the image, so
	// detaching the loop and removing the file takes it with them.
	removeVG := ""
	if f.VGUUID == "" {
		removeVG = fmt.Sprintf(`sudo -n lvm vgremove %s -fy %s >/dev/null 2>&1
sudo -n lvm pvremove %s -fy %s >/dev/null 2>&1`,
			fdLVMCfg, strconv.Quote(f.VGName), fdLVMCfg, strconv.Quote(f.Loop))
	}
	detach := ""
	if f.Loop != "" {
		detach = fmt.Sprintf(`sudo -n losetup -d %s >/dev/null 2>&1`, strconv.Quote(f.Loop))
	}
	script := fmt.Sprintf(`set +e
%s
%s
sudo -n rm -f %s
exit 0`, removeVG, detach, strconv.Quote(f.FilePath))
	if out, err := framework.NodeExecChecked(ctx, cl, f.Node, script); err != nil {
		GinkgoWriter.Printf("foreign loop VG %s teardown reported: %v\n%s\n", f.VGName, err, out)
	}
}

// fdExpectForeignLoopVGIntact asserts the foreign VG, its loop and its backing file all
// survived whatever the agent did — the cleanup path must never touch what it does not own.
func fdExpectForeignLoopVGIntact(ctx context.Context, cl *e2e.Cluster, f *fdForeignLoopVG) {
	GinkgoHelper()
	exists, err := fdNodePathExists(ctx, cl, f.Node, f.FilePath)
	Expect(err).NotTo(HaveOccurred())
	Expect(exists).To(BeTrue(), "foreign backing file %s must not be removed by the agent", f.FilePath)

	bound, loopOut, err := fdLoopBoundToFile(ctx, cl, f.Node, f.FilePath)
	Expect(err).NotTo(HaveOccurred())
	Expect(bound).To(BeTrue(), "foreign loop for %s must stay attached; losetup:\n%s", f.FilePath, loopOut)

	// By UUID whenever there is one: a staged image shares its name with the
	// managed Volume Group, and a name match would be satisfied by the module's
	// own Volume Group even if the foreign one had been wiped.
	if f.VGUUID != "" {
		listed, vgsOut, err := fdVGUUIDListedOnNode(ctx, cl, f.Node, f.VGUUID)
		Expect(err).NotTo(HaveOccurred())
		Expect(listed).To(BeTrue(), "foreign VG %s (VG_UUID=%s) must stay on the node; vgs:\n%s", f.VGName, f.VGUUID, vgsOut)
		return
	}

	listed, vgsOut, err := fdVGListedOnNode(ctx, cl, f.Node, f.VGName)
	Expect(err).NotTo(HaveOccurred())
	Expect(listed).To(BeTrue(), "foreign VG %s must stay on the node; vgs:\n%s", f.VGName, vgsOut)
}

// fdVGUUIDListedOnNode reports whether a Volume Group with this UUID is on the
// node. It is the only unambiguous question to ask once a staged image has
// duplicated a Volume Group name.
func fdVGUUIDListedOnNode(ctx context.Context, cl *e2e.Cluster, node, vgUUID string) (bool, string, error) {
	out, err := framework.NodeExecChecked(ctx, cl, node,
		fmt.Sprintf(`sudo -n lvm vgs %s -o vg_uuid --noheadings 2>/dev/null || true`, fdLVMCfg))
	if err != nil {
		return false, out, err
	}
	for _, line := range strings.Split(out, "\n") {
		if strings.TrimSpace(line) == vgUUID {
			return true, out, nil
		}
	}
	return false, out, nil
}

// fdForceDeleteLVG deletes an LVMVolumeGroup without waiting for node-side cleanup,
// stripping the finalizer if one is present. For LVGs that were never provisioned —
// no agent owns them, so nothing would ever clear the finalizer on its own.
func fdForceDeleteLVG(ctx context.Context, cl client.Client, name string) {
	var lvg v1alpha1.LVMVolumeGroup
	if err := cl.Get(ctx, client.ObjectKey{Name: name}, &lvg); err != nil {
		return
	}
	if len(lvg.Finalizers) > 0 {
		lvg.Finalizers = nil
		if err := cl.Update(ctx, &lvg); err != nil {
			GinkgoWriter.Printf("strip finalizers on %s: %v\n", name, err)
		}
	}
	if err := client.IgnoreNotFound(cl.Delete(ctx, &lvg)); err != nil {
		GinkgoWriter.Printf("delete %s: %v\n", name, err)
	}
}

// ---=== Negative-path assertions ===--- //

// fdWaitVGConfigurationApplied waits for VGConfigurationApplied=False with reason
// ValidationFailed and a message fragment. Every agent-side rejection of a fileDevices entry
// surfaces this way.
func fdWaitVGConfigurationApplied(ctx context.Context, cl client.Client, lvgName, msgFragment string) {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		var cur v1alpha1.LVMVolumeGroup
		g.Expect(cl.Get(ctx, client.ObjectKey{Name: lvgName}, &cur)).To(Succeed())
		g.Expect(cur.Status.Phase).NotTo(Equal(v1alpha1.PhaseReady))
	}, fdRejectionTimeout, 5*time.Second).Should(Succeed())

	fdWaitVGConfigurationRejected(ctx, cl, lvgName, fdReasonValidationFailed, msgFragment)
}

// fdWaitVGConfigurationRejected is fdWaitVGConfigurationApplied without the phase
// requirement, for a Volume Group that is expected to keep working.
//
// The module treats VGConfigurationApplied=False with reason ValidationFailed or
// FileDeviceDrift as an acceptable state: the conditions watcher lists both in
// acceptableReasons (images/controller/pkg/controller/lvg_conditions_watcher.go), so
// neither drags the aggregate Ready condition or the phase down with it. A live Volume
// Group whose spec carries one unusable entry, or one entry fewer than the node has
// Physical Volumes, therefore stays Ready — which is the property these specs are here
// to assert, so requiring the phase to leave Ready would contradict it.
//
// wantReason is explicit rather than assumed: the whole point of giving drift its own
// reason is that an alert can tell "the spec is malformed" from "the spec and the node
// disagree", and a helper that accepts either would not notice the two being conflated
// again.
//
// The phase check still belongs in fdWaitVGConfigurationApplied: there the
// LVMVolumeGroup was rejected before anything was provisioned, so it is not Ready for
// the independent reason that its Volume Group does not exist.
func fdWaitVGConfigurationRejected(ctx context.Context, cl client.Client, lvgName, wantReason, msgFragment string) {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		var cur v1alpha1.LVMVolumeGroup
		g.Expect(cl.Get(ctx, client.ObjectKey{Name: lvgName}, &cur)).To(Succeed())

		var applied *metav1.Condition
		for i := range cur.Status.Conditions {
			if cur.Status.Conditions[i].Type == fdConditionVGConfigurationApplied {
				applied = &cur.Status.Conditions[i]
				break
			}
		}
		g.Expect(applied).NotTo(BeNil(), "VGConfigurationApplied condition should be present")
		g.Expect(applied.Status).To(Equal(metav1.ConditionFalse))
		g.Expect(applied.Reason).To(Equal(wantReason))
		g.Expect(applied.Message).To(ContainSubstring(msgFragment),
			"unexpected message on VGConfigurationApplied: %s", applied.Message)
	}, fdRejectionTimeout, 8*time.Second).Should(Succeed())
}

// fdWaitVGConfigurationOK waits for VGConfigurationApplied to come back True.
//
// This, not the phase, is what says a previously reported problem is gone. Because
// ValidationFailed is an acceptable reason for the conditions watcher, an LVMVolumeGroup
// carrying that condition stays Ready — so waiting on the phase would return instantly
// and prove nothing about the recovery.
func fdWaitVGConfigurationOK(ctx context.Context, cl client.Client, lvgName string) {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		var cur v1alpha1.LVMVolumeGroup
		g.Expect(cl.Get(ctx, client.ObjectKey{Name: lvgName}, &cur)).To(Succeed())
		fdPrintLVG(&cur)

		var applied *metav1.Condition
		for i := range cur.Status.Conditions {
			if cur.Status.Conditions[i].Type == fdConditionVGConfigurationApplied {
				applied = &cur.Status.Conditions[i]
				break
			}
		}
		g.Expect(applied).NotTo(BeNil(), "VGConfigurationApplied condition should be present")
		g.Expect(applied.Status).To(Equal(metav1.ConditionTrue),
			"VGConfigurationApplied is still False: %s", applied.Message)
		g.Expect(cur.Status.Phase).To(Equal(v1alpha1.PhaseReady))
	}, fdLVGReadyTimeout, 10*time.Second).Should(Succeed())
}

// fdExpectNothingProvisioned asserts a rejected LVMVolumeGroup left no VG on the node and no
// backing file of its own in dir.
func fdExpectNothingProvisioned(ctx context.Context, cl *e2e.Cluster, node, lvgName, vgName string) {
	GinkgoHelper()
	listed, vgsOut, err := fdVGListedOnNode(ctx, cl, node, vgName)
	Expect(err).NotTo(HaveOccurred())
	Expect(listed).To(BeFalse(), "VG %s must not be created for a rejected spec; vgs:\n%s", vgName, vgsOut)

	files, err := fdBackingFilesForLVG(ctx, cl, node, lvgName)
	Expect(err).NotTo(HaveOccurred())
	Expect(files).To(BeEmpty(), "a rejected spec must leave no backing file for %s in %s, found: %v", lvgName, fdBaseDir, files)
}

// fdNodeAvailableBytes returns the free space statfs reports for dir on the node — the same
// number the agent's free-space guard compares against.
func fdNodeAvailableBytes(ctx context.Context, cl *e2e.Cluster, node, dir string) (int64, error) {
	// Fall back to the nearest existing parent: the base dir is created on demand
	// by the agent, so before the first spec it does not exist yet and df prints
	// nothing — which used to surface as a parse error instead of a number.
	out, err := framework.NodeExecChecked(ctx, cl, node,
		fmt.Sprintf(`d=%s; while [ ! -d "$d" ] && [ "$d" != / ]; do d=$(dirname "$d"); done; sudo -n df -B1 --output=avail "$d" 2>/dev/null | tail -n1 || true`,
			strconv.Quote(dir)))
	if err != nil {
		return 0, err
	}
	avail, convErr := strconv.ParseInt(strings.TrimSpace(out), 10, 64)
	if convErr != nil {
		return 0, fmt.Errorf("parse available bytes from %q: %w", out, convErr)
	}
	return avail, nil
}
