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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8sclient "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	hostPIDMountPath = "/mnt/e2e-test"
	hostPIDDiskSize  = "5Gi"

	hostPIDDiscoveryWait   = 5 * time.Minute
	hostPIDStateChangeWait = 2 * time.Minute
	hostPIDPollInterval    = time.Second
	// hostPIDNamespaceProbeWindow is longer than one scanner rescan cycle and
	// is used only as a hostPID environment precondition (not the primary
	// assertion that the agent consumed host mountinfo).
	hostPIDNamespaceProbeWindow = 5 * time.Second

	hostPIDNetlinkEnvName       = "ENABLE_NETLINK_BLOCK_DEVICE_DISCOVERY"
	hostPIDNetlinkSetting       = "enableNetlinkBlockDeviceDiscovery"
	hostPIDDistrolessSessionTTL = 15 * time.Minute
	hostPIDModuleReadyTimeout   = 10 * time.Minute
)

var moduleConfigGVR = schema.GroupVersionResource{
	Group:    "deckhouse.io",
	Version:  "v1alpha1",
	Resource: "moduleconfigs",
}

// Regression coverage for PR #220: with hostPID the agent must treat a host
// mount (visible only via /proc/1/mountinfo) as making a BlockDevice
// non-consumable. FSType is wiped while the FS stays mounted so MountPoint is
// the only remaining reason for Consumable=false — reverting #220 (reading
// /proc/self/mountinfo) makes the primary assertion go red.
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
		reader      *kubernetes.DistrolessReader

		// netlinkPrevious holds the ModuleConfig setting before this spec
		// toggled it; nil means the key was absent.
		netlinkPrevious *bool
		netlinkPatched  bool
	)

	BeforeAll(func() {
		ctx = context.Background()

		var cfgErr error
		conf, cfgErr = cfg.Load()
		Expect(cfgErr).NotTo(HaveOccurred(), "failed to load config")
		Expect(conf.DebugImage).NotTo(BeEmpty(),
			"E2E_DEBUG_IMAGE must be set to a busybox-like image reachable from the cluster (prefer a registry/mirror digest pin)")

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

		By("Enabling netlink block-device discovery for this spec only (ModuleConfig)")
		enabled, netlinkErr := agentDaemonSetNetlinkEnabled(ctx, clientset)
		Expect(netlinkErr).NotTo(HaveOccurred())
		if !enabled {
			var patchErr error
			netlinkPrevious, patchErr = patchModuleConfigNetlinkDiscovery(ctx, cl, true)
			Expect(patchErr).NotTo(HaveOccurred())
			netlinkPatched = true

			Expect(kubernetes.WaitForModuleReady(
				ctx, cl.RESTConfig(), consts.SdsNodeConfiguratorAgentName, hostPIDModuleReadyTimeout,
			)).To(Succeed())
			waitAgentDaemonSetNetlinkEnv(ctx, clientset, true)
		}

		DeferCleanup(func() {
			if !netlinkPatched {
				return
			}
			restoreCtx, cancel := context.WithTimeout(context.Background(), hostPIDModuleReadyTimeout+time.Minute)
			defer cancel()

			want := false
			if netlinkPrevious != nil {
				want = *netlinkPrevious
			}
			By(fmt.Sprintf("Restoring ModuleConfig %s=%v", hostPIDNetlinkSetting, want))
			if _, err := patchModuleConfigNetlinkDiscovery(restoreCtx, cl, want); err != nil {
				GinkgoWriter.Printf("failed to restore %s: %v\n", hostPIDNetlinkSetting, err)
				return
			}
			if err := kubernetes.WaitForModuleReady(
				restoreCtx, cl.RESTConfig(), consts.SdsNodeConfiguratorAgentName, hostPIDModuleReadyTimeout,
			); err != nil {
				GinkgoWriter.Printf("module not ready after restoring %s: %v\n", hostPIDNetlinkSetting, err)
				return
			}
			if err := waitAgentDaemonSetNetlinkEnvErr(restoreCtx, clientset, want); err != nil {
				GinkgoWriter.Printf("DaemonSet env not restored for %s: %v\n", hostPIDNetlinkEnvName, err)
			}
		})

		enabled, netlinkErr = agentDaemonSetNetlinkEnabled(ctx, clientset)
		Expect(netlinkErr).NotTo(HaveOccurred())
		Expect(enabled).To(BeTrue(),
			"DaemonSet env %s must be true; this spec patches ModuleConfig.%s for its duration",
			hostPIDNetlinkEnvName, hostPIDNetlinkSetting)
	})

	AfterEach(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
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

		// Detach before force-deleting the CR so the agent cannot recreate it
		// from an still-attached, again-consumable disk.
		if cl != nil && diskName != "" {
			if err := cl.Disks().DetachDisk(cleanupCtx, targetNode, diskName); err != nil {
				GinkgoWriter.Printf("failed to detach disk %s: %v\n", diskName, err)
			}
			if err := cl.Disks().DeleteDisk(cleanupCtx, diskName); err != nil {
				GinkgoWriter.Printf("failed to delete disk %s: %v\n", diskName, err)
			}
		}

		if k8sClient != nil && bdName != "" {
			forceDeleteBlockDevicesByNames(cleanupCtx, k8sClient, []string{bdName})
		}

		diskName = ""
		bdName = ""
		devicePath = ""
		agentPod = nil
		reader = nil
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
		wantSize := resource.MustParse(hostPIDDiskSize)
		Expect(bd.Status.Size.Equal(wantSize)).To(BeTrue(),
			"resolved BlockDevice %s is not the %s disk just attached (got %s); refusing to mkfs %s",
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

		By("Opening a distroless reader (ephemeral busybox) against the agent container")
		var readerErr error
		reader, readerErr = kubernetes.OpenDistrolessReader(
			ctx,
			cl.RESTConfig(),
			consts.SdsNodeConfiguratorAgentNamespace,
			agentPod.Name,
			consts.SdsNodeConfiguratorAgentContainer,
			kubernetes.ReadFileOptions{
				DebugImage: conf.DebugImage,
				SessionTTL: hostPIDDistrolessSessionTTL,
			},
		)
		Expect(readerErr).NotTo(HaveOccurred(), "failed to open distroless reader with image %s", conf.DebugImage)

		agentPID, pidErr := findAgentPID(
			ctx,
			cl.RESTConfig(),
			consts.SdsNodeConfiguratorAgentNamespace,
			reader,
			string(agentPod.UID),
		)
		Expect(pidErr).NotTo(HaveOccurred(), "failed to resolve agent PID in the shared host PID namespace")

		By("Mounting ext4 then wiping the on-disk signature while still mounted (FSType empty; mount remains)")
		setupScript := fmt.Sprintf(
			`if mountpoint -q %s; then echo "mount path is already in use" >&2; exit 1; fi
sudo -n mkfs.ext4 -F %s
sudo -n mkdir -p %s
sudo -n mount %s %s
sudo -n wipefs -a -f %s
sudo -n udevadm trigger --action=change "$(udevadm info --query=path --name=%s)"
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

		// targetContainerName shares the agent's PID and IPC namespaces, but
		// not its mount namespace. /proc/self/mountinfo is therefore the
		// ephemeral container's view. Resolve the agent PID in the host PID
		// namespace (already shared) and read /proc/<pid>/mountinfo.
		By("Resolving the agent PID in the host PID namespace")
		agentPID, pidErr := findAgentPIDInHostNamespace(
			ctx, cl.RESTConfig(), consts.SdsNodeConfiguratorAgentNamespace, reader, agentPod,
		)
		Expect(pidErr).NotTo(HaveOccurred())

		By("Environment precondition: hostPID exposes the mount in /proc/1/mountinfo but not in the agent mount ns")
		Consistently(func(g Gomega) {
			agentMI, agentErr := readPIDMountInfo(
				ctx, cl.RESTConfig(), consts.SdsNodeConfiguratorAgentNamespace, reader, agentPID,
			)
			g.Expect(agentErr).NotTo(HaveOccurred())
			g.Expect(mountInfoContains(agentMI, deviceID, hostPIDMountPath)).To(BeFalse(),
				"agent mount namespace (pid %s) must not contain %s at %s; mountinfo:\n%s",
				agentPID, deviceID, hostPIDMountPath, agentMI)
		}, hostPIDNamespaceProbeWindow, hostPIDPollInterval).Should(Succeed())

		hostMI, hostErr := readPIDMountInfo(
			ctx, cl.RESTConfig(), consts.SdsNodeConfiguratorAgentNamespace, reader, "1",
		)
		Expect(hostErr).NotTo(HaveOccurred())
		Expect(mountInfoContains(hostMI, deviceID, hostPIDMountPath)).To(BeTrue(),
			"/proc/1/mountinfo does not contain device %s mounted at %s; mountinfo:\n%s",
			deviceID, hostPIDMountPath, hostMI)

		By("After umount the same BlockDevice becomes consumable again")
		teardownScript := fmt.Sprintf(
			`sudo -n umount %s
sudo -n udevadm trigger --action=change "$(udevadm info --query=path --name=%s)"
sudo -n udevadm settle`,
			shellQuote(hostPIDMountPath),
			shellQuote(devicePath),
		)
		teardownOut, teardownErr := framework.NodeExecChecked(ctx, cl, targetNode, teardownScript)
		Expect(teardownErr).NotTo(HaveOccurred(), "failed to umount the test device: %s", teardownOut)

		Eventually(func(g Gomega) {
			var current v1alpha1.BlockDevice
			g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: bdName}, &current)).To(Succeed())
			g.Expect(current.Status.Consumable).To(BeTrue(),
				"BlockDevice %s did not become consumable after umount (fsType=%s)",
				bdName, current.Status.FsType)
		}, hostPIDStateChangeWait, hostPIDPollInterval).Should(Succeed())
	})
})

func agentDaemonSetNetlinkEnabled(ctx context.Context, clientset *k8sclient.Clientset) (bool, error) {
	ds, err := clientset.AppsV1().DaemonSets(consts.SdsNodeConfiguratorAgentNamespace).Get(
		ctx, consts.SdsNodeConfiguratorAgentName, metav1.GetOptions{},
	)
	if err != nil {
		return false, err
	}
	return daemonSetEnvIsTrue(ds, consts.SdsNodeConfiguratorAgentContainer, hostPIDNetlinkEnvName), nil
}

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

func waitAgentDaemonSetNetlinkEnv(ctx context.Context, clientset *k8sclient.Clientset, want bool) {
	GinkgoHelper()
	Expect(waitAgentDaemonSetNetlinkEnvErr(ctx, clientset, want)).To(Succeed())
}

func waitAgentDaemonSetNetlinkEnvErr(ctx context.Context, clientset *k8sclient.Clientset, want bool) error {
	var last error
	deadline := time.Now().Add(hostPIDModuleReadyTimeout)
	for time.Now().Before(deadline) {
		ds, err := clientset.AppsV1().DaemonSets(consts.SdsNodeConfiguratorAgentNamespace).Get(
			ctx, consts.SdsNodeConfiguratorAgentName, metav1.GetOptions{},
		)
		if err != nil {
			last = err
		} else if daemonSetEnvIsTrue(ds, consts.SdsNodeConfiguratorAgentContainer, hostPIDNetlinkEnvName) != want {
			last = fmt.Errorf("DaemonSet env %s want %v", hostPIDNetlinkEnvName, want)
		} else if ds.Status.ObservedGeneration < ds.Generation {
			last = fmt.Errorf("DaemonSet generation not observed (%d < %d)", ds.Status.ObservedGeneration, ds.Generation)
		} else if ds.Status.DesiredNumberScheduled == 0 {
			last = fmt.Errorf("DaemonSet DesiredNumberScheduled is 0")
		} else if ds.Status.NumberReady != ds.Status.DesiredNumberScheduled {
			last = fmt.Errorf("DaemonSet NumberReady %d != Desired %d", ds.Status.NumberReady, ds.Status.DesiredNumberScheduled)
		} else if ds.Status.UpdatedNumberScheduled != ds.Status.DesiredNumberScheduled {
			last = fmt.Errorf("DaemonSet UpdatedNumberScheduled %d != Desired %d", ds.Status.UpdatedNumberScheduled, ds.Status.DesiredNumberScheduled)
		} else {
			return nil
		}
		select {
		case <-ctx.Done():
			if last == nil {
				last = ctx.Err()
			}
			return last
		case <-time.After(5 * time.Second):
		}
	}
	if last == nil {
		last = fmt.Errorf("timed out waiting for DaemonSet env %s=%v", hostPIDNetlinkEnvName, want)
	}
	return last
}

// patchModuleConfigNetlinkDiscovery sets spec.settings.enableNetlinkBlockDeviceDiscovery
// and returns the previous value (nil if the key was absent).
func patchModuleConfigNetlinkDiscovery(ctx context.Context, cl *e2e.Cluster, enabled bool) (*bool, error) {
	mc, err := cl.Dynamic().Resource(moduleConfigGVR).Get(ctx, consts.SdsNodeConfiguratorAgentName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get ModuleConfig %s: %w", consts.SdsNodeConfiguratorAgentName, err)
	}

	settings, found, err := unstructured.NestedMap(mc.Object, "spec", "settings")
	if err != nil {
		return nil, fmt.Errorf("read spec.settings: %w", err)
	}
	if !found || settings == nil {
		settings = map[string]interface{}{}
	}

	var previous *bool
	if raw, ok := settings[hostPIDNetlinkSetting]; ok {
		switch v := raw.(type) {
		case bool:
			previous = &v
		case string:
			b := strings.EqualFold(strings.TrimSpace(v), "true")
			previous = &b
		}
	}

	settings[hostPIDNetlinkSetting] = enabled
	if err := unstructured.SetNestedMap(mc.Object, settings, "spec", "settings"); err != nil {
		return nil, fmt.Errorf("set spec.settings: %w", err)
	}

	if _, err := cl.Dynamic().Resource(moduleConfigGVR).Update(ctx, mc, metav1.UpdateOptions{}); err != nil {
		return nil, fmt.Errorf("update ModuleConfig %s: %w", consts.SdsNodeConfiguratorAgentName, err)
	}
	return previous, nil
}

func readPIDMountInfo(
	ctx context.Context,
	restCfg *rest.Config,
	namespace string,
	reader *kubernetes.DistrolessReader,
	pid string,
) (string, error) {
	path := fmt.Sprintf("/proc/%s/mountinfo", pid)
	stdout, stderr, err := kubernetes.ExecInPod(
		ctx, restCfg, namespace, reader.PodName(), reader.EphemeralName(),
		[]string{"cat", path},
	)
	if err != nil {
		return stdout, fmt.Errorf("read %s: %w (stderr=%q)", path, err, stderr)
	}
	return stdout, nil
}

// findAgentPIDInHostNamespace locates the agent container's PID in the host
// PID namespace (shared by the ephemeral reader when the agent has hostPID).
func findAgentPIDInHostNamespace(
	ctx context.Context,
	restCfg *rest.Config,
	namespace string,
	reader *kubernetes.DistrolessReader,
	agentPod *v1.Pod,
) (string, error) {
	containerID, err := agentContainerID(agentPod, consts.SdsNodeConfiguratorAgentContainer)
	if err != nil {
		return "", err
	}
	cmd := fmt.Sprintf(
		`for p in /proc/[0-9]*; do grep -qs %s "$p/cgroup" && { echo "${p#/proc/}"; break; }; done`,
		shellQuote(containerID),
	)
	stdout, stderr, err := kubernetes.ExecInPod(
		ctx, restCfg, namespace, reader.PodName(), reader.EphemeralName(),
		[]string{"sh", "-c", cmd},
	)
	if err != nil {
		return "", fmt.Errorf("scan /proc for container %s: %w (stderr=%q)", containerID, err, stderr)
	}
	pid := strings.TrimSpace(stdout)
	if pid == "" {
		return "", fmt.Errorf("no host PID found whose cgroup mentions container %s", containerID)
	}
	if _, err := strconv.Atoi(pid); err != nil {
		return "", fmt.Errorf("invalid PID %q from cgroup scan: %w", pid, err)
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
// shell scripts run via NodeExecChecked / sh -ec.
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
	GinkgoHelper()

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
