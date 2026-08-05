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
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/e2e/cfg"
	"github.com/deckhouse/sds-node-configurator/e2e/framework"
	"github.com/deckhouse/sds-node-configurator/e2e/sdsclient"
	"github.com/deckhouse/sds-node-configurator/e2e/tests/utils/consts"
	"github.com/deckhouse/sds-node-configurator/e2e/tests/utils/pod"
	"github.com/deckhouse/storage-e2e/pkg/e2e"
	"github.com/deckhouse/storage-e2e/pkg/kubernetes"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sclient "k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	hostPIDMountPath = "/mnt/e2e-test"
	hostPIDDiskSize  = "5Gi"

	hostPIDDiscoveryWait   = 5 * time.Minute
	hostPIDStateChangeWait = 2 * time.Minute
	hostPIDPollInterval    = time.Second
	// hostPIDNamespaceProbeWindow spans two default scanner rescan cycles
	// (BlockDeviceScanInterval defaults to 5s). Environment precondition only —
	// not the primary assertion that the agent consumed host mountinfo.
	hostPIDNamespaceProbeWindow = 12 * time.Second

	hostPIDNetlinkEnvName     = "ENABLE_NETLINK_BLOCK_DEVICE_DISCOVERY"
	hostPIDNodeCleanupTimeout = 3 * time.Minute
)

// Regression coverage for PR #220: with hostPID the agent must treat a host
// mount (visible only via /proc/1/mountinfo) as making a BlockDevice
// non-consumable. FSType is wiped while the FS stays mounted so MountPoint is
// the only remaining reason for Consumable=false — reverting #220 (reading
// /proc/self/mountinfo) makes the primary assertion go red.
//
// Requires netlink discovery (cluster_config*.yml sets
// enableNetlinkBlockDeviceDiscovery); the lsblk path cannot see host mounts.
// Mountinfo is read over SSH on the node (not via an ephemeral busybox reader):
// /proc/<pid>/mountinfo is rendered relative to the inspected process.
var _ = Describe("BlockDevice host mountinfo", Label("sds-node-configurator", "block-device", "host-pid"), Ordered, func() {
	var (
		ctx        context.Context
		conf       *cfg.Config
		cl         *e2e.Cluster
		k8sClient  client.Client
		clientset  *k8sclient.Clientset
		targetNode string

		testStarted metav1.Time
		diskName    string
		bdName      string
		devicePath  string
		agentPod    *v1.Pod
	)

	BeforeAll(func() {
		ctx = context.Background()

		var cfgErr error
		conf, cfgErr = cfg.Load()
		Expect(cfgErr).NotTo(HaveOccurred(), "failed to load config")

		var clErr error
		cl, clErr = e2e.Connect(ctx, e2e.WithTestName("block-device-host-mountinfo"))
		Expect(clErr).NotTo(HaveOccurred(), "failed to connect to cluster")
		DeferCleanup(func() {
			if err := cl.Close(context.Background()); err != nil {
				GinkgoWriter.Println("Error closing cluster:", err)
			}
		})

		var clientErr error
		k8sClient, clientErr = sdsclient.New(cl.RESTConfig())
		Expect(clientErr).NotTo(HaveOccurred(), "failed to build controller-runtime client")
		clientset, clientErr = k8sclient.NewForConfig(cl.RESTConfig())
		Expect(clientErr).NotTo(HaveOccurred(), "failed to build Kubernetes clientset")

		nodes, listErr := cl.Clientset().CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		Expect(listErr).NotTo(HaveOccurred(), "failed to list nodes")
		Expect(nodes.Items).NotTo(BeEmpty(), "cluster must have at least one node")
		targetNode = nodes.Items[0].Name

		By("Checking netlink discovery and hostPID on the agent DaemonSet")
		ds, dsErr := clientset.AppsV1().DaemonSets(consts.SdsNodeConfiguratorAgentNamespace).Get(
			ctx, consts.SdsNodeConfiguratorAgentName, metav1.GetOptions{},
		)
		Expect(dsErr).NotTo(HaveOccurred())
		Expect(ds.Spec.Template.Spec.HostPID).To(BeTrue(),
			"agent DaemonSet must have hostPID=true for /proc/1/mountinfo to be the host mount table")
		if !daemonSetEnvIsTrue(ds, consts.SdsNodeConfiguratorAgentContainer, hostPIDNetlinkEnvName) {
			Skip(fmt.Sprintf(
				"DaemonSet env %s is not true; enable enableNetlinkBlockDeviceDiscovery in ModuleConfig "+
					"(e2e/tests/cluster_config*.yml) — this spec needs the netlink /proc/1/mountinfo path",
				hostPIDNetlinkEnvName,
			))
		}
	})

	AfterEach(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), hostPIDNodeCleanupTimeout)
		defer cancel()

		if CurrentSpecReport().Failed() && cl != nil {
			collectHostPIDFailureArtifacts(cleanupCtx, cl, k8sClient, clientset, targetNode, testStarted)
		}

		if cl != nil && targetNode != "" {
			cleanupScript := fmt.Sprintf(
				`if mountpoint -q %s; then sudo -n umount %s; fi
sudo -n rmdir %s 2>/dev/null || true`,
				shellQuote(hostPIDMountPath), shellQuote(hostPIDMountPath), shellQuote(hostPIDMountPath),
			)
			if devicePath != "" {
				cleanupScript += fmt.Sprintf(
					"\nsudo -n wipefs -a -f %s >/dev/null 2>&1 || true",
					shellQuote(devicePath),
				)
			}
			if out, err := framework.NodeExecChecked(cleanupCtx, cl, targetNode, cleanupScript); err != nil {
				GinkgoWriter.Printf("hostPID cleanup on node failed: %v; output=%q\n", err, out)
			}
		}

		// Detach first so the agent drops the BlockDevice when the disk disappears.
		// Do not strip finalizers — they are owned by the agent and cannot be cleared
		// from the test client as a reliable cleanup path.
		if cl != nil && diskName != "" {
			if err := cl.Disks().DetachDisk(cleanupCtx, targetNode, diskName); err != nil {
				GinkgoWriter.Printf("failed to detach disk %s: %v\n", diskName, err)
			}
			if err := cl.Disks().DeleteDisk(cleanupCtx, diskName); err != nil {
				GinkgoWriter.Printf("failed to delete disk %s: %v\n", diskName, err)
			}
		}

		if k8sClient != nil && bdName != "" {
			waitBlockDeviceGone(cleanupCtx, k8sClient, bdName)
		}

		diskName = ""
		bdName = ""
		devicePath = ""
		agentPod = nil
	})

	It("marks a host-mounted device non-consumable via /proc/1/mountinfo", func() {
		testStarted = metav1.NewTime(time.Now())

		By("Snapshotting consumable BlockDevices before attaching a disk")
		before, beforeErr := kubernetes.GetConsumableBlockDevicesByNode(ctx, cl.RESTConfig(), targetNode)
		Expect(beforeErr).NotTo(HaveOccurred())

		diskName = fmt.Sprintf("e2e-host-pid-%d", time.Now().UnixNano())
		disk, createErr := cl.Disks().CreateDisk(ctx, e2e.DiskSpec{
			Name:         diskName,
			Size:         resource.MustParse(hostPIDDiskSize),
			StorageClass: conf.TestCluster.StorageClass,
		})
		Expect(createErr).NotTo(HaveOccurred(), "failed to create virtual disk")
		Expect(disk).NotTo(BeNil())
		diskName = disk.Name

		By("Attaching the disk and waiting for its consumable BlockDevice")
		Expect(cl.Disks().AttachDisk(ctx, targetNode, diskName)).To(Succeed(), "failed to attach virtual disk")
		discovered, waitErr := framework.WaitNewConsumableBlockDevice(
			ctx, cl.RESTConfig(), targetNode, before, hostPIDDiscoveryWait,
		)
		Expect(waitErr).NotTo(HaveOccurred())
		bdName = discovered.Name

		var bd v1alpha1.BlockDevice
		Expect(k8sClient.Get(ctx, client.ObjectKey{Name: bdName}, &bd)).To(Succeed())
		Expect(bd.Status.Consumable).To(BeTrue())
		Expect(bd.Status.Path).NotTo(BeEmpty())
		Expect(bd.Status.Path).To(HavePrefix("/dev/"),
			"resolved BlockDevice %s path %q is not a /dev/ device; refusing to mkfs", bdName, bd.Status.Path)
		// VirtualDisk capacity often rounds up a few MiB above the PVC request
		// (here: 5Gi → 5245816Ki). Match netlink_discovery_test: accept
		// [requested, requested+16Mi].
		wantSize := resource.MustParse(hostPIDDiskSize)
		maxSize := wantSize.DeepCopy()
		maxSize.Add(resource.MustParse("16Mi"))
		Expect(bd.Status.Size.Cmp(wantSize)).NotTo(BeNumerically("<", 0),
			"resolved BlockDevice %s size must be >= requested %s (got %s); refusing to mkfs %s",
			bdName, hostPIDDiskSize, bd.Status.Size.String(), bd.Status.Path)
		Expect(bd.Status.Size.Cmp(maxSize)).NotTo(BeNumerically(">", 0),
			"resolved BlockDevice %s size must be <= requested %s + 16Mi (got %s); refusing to mkfs %s",
			bdName, hostPIDDiskSize, bd.Status.Size.String(), bd.Status.Path)
		devicePath = bd.Status.Path

		var podErr error
		agentPod, podErr = pod.FindRunningPodOnNode(
			ctx,
			k8sClient,
			targetNode,
			client.InNamespace(consts.SdsNodeConfiguratorAgentNamespace),
			client.MatchingLabels{"app": consts.SdsNodeConfiguratorAgentName},
		)
		Expect(podErr).NotTo(HaveOccurred(), "failed to find the agent pod on node %s", targetNode)

		By("Resolving the agent container init PID via crictl on the node")
		agentPID, pidErr := findAgentPIDViaCrictl(ctx, cl, targetNode, agentPod)
		Expect(pidErr).NotTo(HaveOccurred())

		By("Mounting ext4 then wiping the on-disk signature while still mounted (FSType empty; mount remains)")
		// SSH login shell reports only the last exit code — set -eu so a failed
		// mkfs/mount cannot be masked by a successful printf at the end.
		setupScript := fmt.Sprintf(
			`set -eu
if mountpoint -q %s; then echo "mount path is already in use" >&2; exit 1; fi
sudo -n mkfs.ext4 -F %s
sudo -n mkdir -p %s
sudo -n mount %s %s
sudo -n wipefs -a -f %s
sudo -n udevadm trigger --action=change -- %s
sudo -n udevadm settle
printf '%%s %%s\n' "$(stat -c %%t %s)" "$(stat -c %%T %s)"`,
			shellQuote(hostPIDMountPath),
			shellQuote(devicePath),
			shellQuote(hostPIDMountPath),
			shellQuote(devicePath),
			shellQuote(hostPIDMountPath),
			shellQuote(devicePath),
			shellQuote(devicePath),
			shellQuote(devicePath),
			shellQuote(devicePath),
		)
		setupOut, setupErr := framework.NodeExecChecked(ctx, cl, targetNode, setupScript)
		Expect(setupErr).NotTo(HaveOccurred(), "failed to mount+wipe setup: %s", setupOut)
		deviceID, parseErr := parseHexDeviceID(setupOut)
		Expect(parseErr).NotTo(HaveOccurred(), "failed to determine major:minor for %s; output=%q", devicePath, setupOut)

		By("Primary: BlockDevice stays present and is non-consumable solely because of the host mount")
		Eventually(func(g Gomega) {
			var current v1alpha1.BlockDevice
			g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: bdName}, &current)).To(Succeed())
			g.Expect(current.Status.FsType).To(BeEmpty(),
				"precondition: on-disk signature must be wiped so FSType is not the reason for non-consumable")
			g.Expect(current.Status.HotPlug).To(BeFalse(),
				"precondition: HotPlug must be false so the host mount is the only remaining reason for non-consumable")
			g.Expect(current.Status.Consumable).To(BeFalse(),
				"device is mounted on the host; an agent reading /proc/self/mountinfo would miss it and keep Consumable=true")
		}, hostPIDStateChangeWait, hostPIDPollInterval).Should(Succeed())

		By("Environment precondition: host mount is in /proc/1/mountinfo but not in the agent mount ns")
		Consistently(func(g Gomega) {
			agentMI, agentErr := readNodeProcMountInfo(ctx, cl, targetNode, agentPID)
			g.Expect(agentErr).NotTo(HaveOccurred())
			g.Expect(mountInfoContains(agentMI, deviceID, hostPIDMountPath)).To(BeFalse(),
				"agent mount namespace (pid %s) must not contain %s at %s; mountinfo:\n%s",
				agentPID, deviceID, hostPIDMountPath, agentMI)
		}, hostPIDNamespaceProbeWindow, hostPIDPollInterval).Should(Succeed())

		hostMI, hostErr := readNodeProcMountInfo(ctx, cl, targetNode, "1")
		Expect(hostErr).NotTo(HaveOccurred())
		Expect(mountInfoContains(hostMI, deviceID, hostPIDMountPath)).To(BeTrue(),
			"/proc/1/mountinfo does not contain device %s mounted at %s; mountinfo:\n%s",
			deviceID, hostPIDMountPath, hostMI)

		By("After umount the same BlockDevice becomes consumable again")
		// umount itself does not emit a block uevent; the agent only rescans mountinfo
		// on udev activity (or one-shot idle timer). Trigger the target device only.
		// ext4 commits its in-memory superblock on umount (ext4_put_super), which
		// restores the magic wipefs removed while mounted — re-wipe so FSType stays
		// empty and MountPoint is provably the only reason Consumable flipped.
		teardownScript := fmt.Sprintf(
			`set -eu
sudo -n umount %s
if mountpoint -q %s; then echo "mount still present after umount" >&2; exit 1; fi
sudo -n wipefs -a -f %s
sudo -n udevadm trigger --action=change -- %s
sudo -n udevadm settle`,
			shellQuote(hostPIDMountPath),
			shellQuote(hostPIDMountPath),
			shellQuote(devicePath),
			shellQuote(devicePath),
		)
		teardownOut, teardownErr := framework.NodeExecChecked(ctx, cl, targetNode, teardownScript)
		Expect(teardownErr).NotTo(HaveOccurred(), "failed to umount the test device: %s", teardownOut)

		By("Confirming the host mount is gone from /proc/1/mountinfo")
		Eventually(func(g Gomega) {
			mi, err := readNodeProcMountInfo(ctx, cl, targetNode, "1")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(mountInfoContains(mi, deviceID, hostPIDMountPath)).To(BeFalse(),
				"host mountinfo still has %s at %s after umount:\n%s", deviceID, hostPIDMountPath, mi)
		}, hostPIDStateChangeWait, hostPIDPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			var current v1alpha1.BlockDevice
			g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: bdName}, &current)).To(Succeed())
			g.Expect(current.Status.FsType).To(BeEmpty(),
				"precondition: FSType must stay empty after umount+wipefs so MountPoint was the reason Consumable flipped")
			g.Expect(current.Status.Consumable).To(BeTrue(),
				"BlockDevice %s did not become consumable after umount (fsType=%s)",
				bdName, current.Status.FsType)
		}, hostPIDStateChangeWait, hostPIDPollInterval).Should(Succeed())
	})
})

func daemonSetEnvIsTrue(ds *appsv1.DaemonSet, containerName, envName string) bool {
	for i := range ds.Spec.Template.Spec.Containers {
		c := &ds.Spec.Template.Spec.Containers[i]
		if c.Name != containerName {
			continue
		}
		for _, e := range c.Env {
			if e.Name == envName {
				return strings.EqualFold(strings.TrimSpace(e.Value), "true")
			}
		}
	}
	return false
}

// waitBlockDeviceGone waits for the agent to remove the CR after disk detach.
// Best-effort only: never strips finalizers from the test client.
func waitBlockDeviceGone(ctx context.Context, cl client.Client, name string) {
	deadline := time.Now().Add(hostPIDStateChangeWait)
	for time.Now().Before(deadline) {
		var bd v1alpha1.BlockDevice
		err := cl.Get(ctx, client.ObjectKey{Name: name}, &bd)
		if apierrors.IsNotFound(err) {
			return
		}
		if err != nil {
			GinkgoWriter.Printf("wait BlockDevice %s gone: get failed: %v\n", name, err)
			return
		}
		select {
		case <-ctx.Done():
			GinkgoWriter.Printf("wait BlockDevice %s gone: %v (still present)\n", name, ctx.Err())
			return
		case <-time.After(hostPIDPollInterval):
		}
	}
	GinkgoWriter.Printf("BlockDevice %s still present after detach within %s\n", name, hostPIDStateChangeWait)
}

func readNodeProcMountInfo(ctx context.Context, cl *e2e.Cluster, node, pid string) (string, error) {
	return framework.NodeExecChecked(ctx, cl, node, fmt.Sprintf("cat /proc/%s/mountinfo", shellQuote(pid)))
}

// findAgentPIDViaCrictl returns the container init PID in the host PID namespace.
// Prefer crictl over a cgroup scan: unambiguous and not confused by nsenter -m helpers.
func findAgentPIDViaCrictl(ctx context.Context, cl *e2e.Cluster, node string, agentPod *v1.Pod) (string, error) {
	containerID, err := agentContainerID(agentPod, consts.SdsNodeConfiguratorAgentContainer)
	if err != nil {
		return "", err
	}
	cmd := fmt.Sprintf(
		`set -eu
sudo -n crictl inspect --output go-template --template '{{.info.pid}}' %s`,
		shellQuote(containerID),
	)
	out, err := framework.NodeExecChecked(ctx, cl, node, cmd)
	if err != nil {
		return "", fmt.Errorf("crictl inspect pid for container %s: %w (output=%q)", containerID, err, out)
	}
	pid := strings.TrimSpace(out)
	if _, err := strconv.Atoi(pid); err != nil {
		return "", fmt.Errorf("invalid PID %q from crictl: %w", pid, err)
	}
	return pid, nil
}

func agentContainerID(pod *v1.Pod, containerName string) (string, error) {
	for i := range pod.Status.ContainerStatuses {
		st := &pod.Status.ContainerStatuses[i]
		if st.Name != containerName {
			continue
		}
		id := strings.TrimSpace(st.ContainerID)
		if id == "" {
			return "", fmt.Errorf("container %s has empty ContainerID", containerName)
		}
		if _, rest, ok := strings.Cut(id, "://"); ok {
			id = rest
		}
		if id == "" {
			return "", fmt.Errorf("container %s has empty ID after runtime prefix", containerName)
		}
		return id, nil
	}
	return "", fmt.Errorf("container %s not found in pod %s status", containerName, pod.Name)
}

// shellQuote returns a POSIX single-quoted string safe for interpolation into
// shell scripts run via NodeExecChecked (SSH login shell on the node).
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'"'"'`) + "'"
}

func parseHexDeviceID(output string) (string, error) {
	trimmed := strings.TrimSpace(output)
	if trimmed == "" {
		return "", fmt.Errorf("empty stat output")
	}
	lines := strings.Split(trimmed, "\n")
	fields := strings.Fields(lines[len(lines)-1])
	if len(fields) != 2 {
		return "", fmt.Errorf("expected two hexadecimal fields, got %q", lines[len(lines)-1])
	}
	major, err := strconv.ParseUint(fields[0], 16, 32)
	if err != nil {
		return "", fmt.Errorf("parse major %q: %w", fields[0], err)
	}
	minor, err := strconv.ParseUint(fields[1], 16, 32)
	if err != nil {
		return "", fmt.Errorf("parse minor %q: %w", fields[1], err)
	}
	return fmt.Sprintf("%d:%d", major, minor), nil
}

func mountInfoContains(output, deviceID, mountPath string) bool {
	for _, line := range strings.Split(output, "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 5 && fields[2] == deviceID && fields[4] == mountPath {
			return true
		}
	}
	return false
}

func collectHostPIDFailureArtifacts(
	ctx context.Context,
	cl *e2e.Cluster,
	k8sClient client.Client,
	clientset *k8sclient.Clientset,
	targetNode string,
	since metav1.Time,
) {
	if k8sClient != nil {
		var bds v1alpha1.BlockDeviceList
		if err := k8sClient.List(ctx, &bds); err != nil {
			GinkgoWriter.Printf("failed to collect BlockDevices: %v\n", err)
		} else {
			addJSONReportEntry("blockdevices.json", bds)
		}

		var lvgs v1alpha1.LVMVolumeGroupList
		if err := k8sClient.List(ctx, &lvgs); err != nil {
			GinkgoWriter.Printf("failed to collect LVMVolumeGroups: %v\n", err)
		} else {
			addJSONReportEntry("lvmvolumegroups.json", lvgs)
		}
	}

	events, err := cl.Clientset().CoreV1().Events(consts.SdsNodeConfiguratorAgentNamespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		GinkgoWriter.Printf("failed to collect events: %v\n", err)
	} else {
		addJSONReportEntry("events.json", events)
	}

	if targetNode == "" || k8sClient == nil || clientset == nil {
		return
	}
	agentPod, err := pod.FindRunningPodOnNode(
		ctx,
		k8sClient,
		targetNode,
		client.InNamespace(consts.SdsNodeConfiguratorAgentNamespace),
		client.MatchingLabels{"app": consts.SdsNodeConfiguratorAgentName},
	)
	if err != nil {
		GinkgoWriter.Printf("failed to find agent pod for log collection: %v\n", err)
		return
	}
	logText, err := pod.GetLogs(ctx, clientset, consts.SdsNodeConfiguratorAgentNamespace, agentPod.Name, v1.PodLogOptions{
		Container: consts.SdsNodeConfiguratorAgentContainer,
		SinceTime: &since,
	})
	if err != nil {
		GinkgoWriter.Printf("failed to collect agent logs: %v\n", err)
		return
	}
	AddReportEntry("agent.log", logText)
}

func addJSONReportEntry(name string, value any) {
	data, err := json.MarshalIndent(value, "", " ")
	if err != nil {
		GinkgoWriter.Printf("failed to marshal %s: %v\n", name, err)
		return
	}
	AddReportEntry(name, string(data))
}
