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
	"bytes"
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
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sclient "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	hostPIDMountPath       = "/mnt/e2e-test"
	hostPIDDiskSize        = "5Gi"
	hostPIDDiscoveryWait   = 5 * time.Minute
	hostPIDStateChangeWait = 30 * time.Second
	hostPIDPollInterval    = time.Second
)

var _ = Describe("BlockDevice host mount namespace", Label("sds-node-configurator", "block-device", "host-pid"), func() {
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
	)

	BeforeEach(func() {
		ctx = context.Background()
		testStarted = metav1.NewTime(time.Now())

		var cfgErr error
		conf, cfgErr = cfg.Load()
		Expect(cfgErr).NotTo(HaveOccurred(), "failed to load config")

		var clErr error
		cl, clErr = e2e.Connect(ctx, e2e.WithTestName("block-device-host-pid"))
		Expect(clErr).NotTo(HaveOccurred(), "failed to connect to cluster")

		var clientErr error
		k8sClient, clientErr = sdsclient.New(cl.RESTConfig())
		Expect(clientErr).NotTo(HaveOccurred(), "failed to build controller-runtime client")
		clientset, clientErr = k8sclient.NewForConfig(cl.RESTConfig())
		Expect(clientErr).NotTo(HaveOccurred(), "failed to build Kubernetes clientset")

		nodes, listErr := cl.Clientset().CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		Expect(listErr).NotTo(HaveOccurred(), "failed to list nodes")
		Expect(nodes.Items).NotTo(BeEmpty(), "cluster must have at least one node")
		targetNode = nodes.Items[0].Name
	})

	AfterEach(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		if CurrentSpecReport().Failed() && cl != nil {
			collectHostPIDFailureArtifacts(cleanupCtx, cl, k8sClient, targetNode, testStarted)
		}

		if cl != nil && targetNode != "" {
			cleanupScript := fmt.Sprintf(
				`if mountpoint -q %s; then sudo -n umount %s; fi
sudo -n rmdir %s 2>/dev/null || true`,
				strconv.Quote(hostPIDMountPath), strconv.Quote(hostPIDMountPath), strconv.Quote(hostPIDMountPath),
			)
			if devicePath != "" {
				cleanupScript += fmt.Sprintf(
					"\nsudo -n wipefs -a %s >/dev/null 2>&1 || true",
					strconv.Quote(devicePath),
				)
			}
			if out, err := framework.NodeExecChecked(cleanupCtx, cl, targetNode, cleanupScript); err != nil {
				GinkgoWriter.Printf("hostPID cleanup on node failed: %v; output=%q\n", err, out)
			}
		}

		if cl != nil && diskName != "" {
			if err := cl.Disks().DetachDisk(cleanupCtx, targetNode, diskName); err != nil {
				GinkgoWriter.Printf("failed to detach disk %s: %v\n", diskName, err)
			}
			if err := cl.Disks().DeleteDisk(cleanupCtx, diskName); err != nil {
				GinkgoWriter.Printf("failed to delete disk %s: %v\n", diskName, err)
			}
		}
		if cl != nil {
			if err := cl.Close(cleanupCtx); err != nil {
				GinkgoWriter.Printf("failed to close cluster connection: %v\n", err)
			}
		}
	})

	It("reads host mounts from PID 1 rather than the agent mount namespace", func() {
		By("Step 1: snapshotting consumable BlockDevices before attaching a disk")
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

		By("Step 2: attaching the disk and waiting for its consumable BlockDevice")
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
		devicePath = bd.Status.Path

		agentPod, podErr := pod.FindRunningPodOnNode(
			ctx,
			k8sClient,
			targetNode,
			client.InNamespace(consts.SdsNodeConfiguratorAgentNamespace),
			client.MatchingLabels{"app": consts.SdsNodeConfiguratorAgentName},
		)
		Expect(podErr).NotTo(HaveOccurred(), "failed to find the agent pod on node %s", targetNode)

		By("Step 3: creating ext4 and mounting the device on the host")
		setupScript := fmt.Sprintf(
			`if mountpoint -q %s; then echo "mount path is already in use" >&2; exit 1; fi
sudo -n mkfs.ext4 -F %s
sudo -n mkdir -p %s
sudo -n mount %s %s
printf '%%s %%s\n' "$(stat -c %%t %s)" "$(stat -c %%T %s)"`,
			strconv.Quote(hostPIDMountPath),
			strconv.Quote(devicePath),
			strconv.Quote(hostPIDMountPath),
			strconv.Quote(devicePath),
			strconv.Quote(hostPIDMountPath),
			strconv.Quote(devicePath),
			strconv.Quote(devicePath),
		)
		setupOut, setupErr := framework.NodeExecChecked(ctx, cl, targetNode, setupScript)
		Expect(setupErr).NotTo(HaveOccurred(), "failed to create and mount ext4: %s", setupOut)
		deviceID, parseErr := parseHexDeviceID(setupOut)
		Expect(parseErr).NotTo(HaveOccurred(), "failed to determine major:minor for %s; output=%q", devicePath, setupOut)

		By("Step 4: waiting for the agent to remove the ext4 BlockDevice from consumable candidates")
		Eventually(func(g Gomega) {
			var current v1alpha1.BlockDevice
			err := k8sClient.Get(ctx, client.ObjectKey{Name: bdName}, &current)
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue(),
				"ext4 BlockDevice %s should be removed from candidates; current error=%v, consumable=%t, fsType=%s",
				bdName, err, current.Status.Consumable, current.Status.FsType)
		}, hostPIDStateChangeWait, hostPIDPollInterval).Should(Succeed())

		By("Step 5: proving the host mount is absent from the agent's own mount namespace")
		Consistently(func(g Gomega) {
			stdout, stderr, execErr := execInAgentPod(
				ctx, cl.RESTConfig(), agentPod.Name,
				[]string{"sh", "-ec", fmt.Sprintf("! grep -F -- %s /proc/self/mountinfo", strconv.Quote(hostPIDMountPath))},
			)
			g.Expect(execErr).NotTo(HaveOccurred(), "stdout=%q stderr=%q", stdout, stderr)
			g.Expect(strings.TrimSpace(stdout)).To(BeEmpty())
		}, 5*time.Second, hostPIDPollInterval).Should(Succeed())

		By("Step 5: proving PID 1 mountinfo contains the target device and mount point")
		hostMountInfo, stderr, execErr := execInAgentPod(
			ctx, cl.RESTConfig(), agentPod.Name,
			[]string{"grep", "-F", "--", hostPIDMountPath, "/proc/1/mountinfo"},
		)
		Expect(execErr).NotTo(HaveOccurred(), "stdout=%q stderr=%q", hostMountInfo, stderr)
		Expect(mountInfoContains(hostMountInfo, deviceID, hostPIDMountPath)).To(BeTrue(),
			"/proc/1/mountinfo does not contain device %s mounted at %s; matching lines:\n%s",
			deviceID, hostPIDMountPath, hostMountInfo)

		By("Step 6: unmounting, removing the ext4 signature, and waiting for the consumable BlockDevice to reappear")
		teardownScript := fmt.Sprintf(
			`sudo -n umount %s
sudo -n wipefs -a %s
sudo -n udevadm trigger --action=change "$(udevadm info --query=path --name=%s)"
sudo -n udevadm settle`,
			strconv.Quote(hostPIDMountPath),
			strconv.Quote(devicePath),
			strconv.Quote(devicePath),
		)
		teardownOut, teardownErr := framework.NodeExecChecked(ctx, cl, targetNode, teardownScript)
		Expect(teardownErr).NotTo(HaveOccurred(), "failed to unmount and wipe the test device: %s", teardownOut)

		Eventually(func(g Gomega) {
			var current v1alpha1.BlockDevice
			g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: bdName}, &current)).To(Succeed())
			g.Expect(current.Status.Consumable).To(BeTrue(),
				"BlockDevice %s did not reappear as consumable after unmount and wipefs (fsType=%s)",
				bdName, current.Status.FsType)
		}, hostPIDStateChangeWait, hostPIDPollInterval).Should(Succeed())

		By("Verifying the agent did not invoke lsblk during the scenario")
		logText, logErr := pod.GetLogs(ctx, clientset, consts.SdsNodeConfiguratorAgentNamespace, agentPod.Name, v1.PodLogOptions{
			Container: consts.SdsNodeConfiguratorAgentContainer,
			SinceTime: &testStarted,
		})
		Expect(logErr).NotTo(HaveOccurred(), "failed to read agent logs")
		Expect(logText).NotTo(MatchRegexp(`(?i)exec.*lsblk`))
	})
})

func execInAgentPod(ctx context.Context, config *rest.Config, podName string, command []string) (string, string, error) {
	clientset, err := k8sclient.NewForConfig(config)
	if err != nil {
		return "", "", fmt.Errorf("create Kubernetes clientset: %w", err)
	}
	req := clientset.CoreV1().RESTClient().Post().
		Namespace(consts.SdsNodeConfiguratorAgentNamespace).
		Resource("pods").
		Name(podName).
		SubResource("exec").
		VersionedParams(&v1.PodExecOptions{
			Container: consts.SdsNodeConfiguratorAgentContainer,
			Command:   command,
			Stdout:    true,
			Stderr:    true,
		}, scheme.ParameterCodec)

	executor, err := remotecommand.NewSPDYExecutor(config, "POST", req.URL())
	if err != nil {
		return "", "", fmt.Errorf("create pod executor: %w", err)
	}

	var stdout, stderr bytes.Buffer
	err = executor.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: &stdout,
		Stderr: &stderr,
	})
	if err != nil {
		return stdout.String(), stderr.String(), fmt.Errorf("exec %q in pod %s: %w", command, podName, err)
	}
	return stdout.String(), stderr.String(), nil
}

func parseHexDeviceID(output string) (string, error) {
	lines := strings.Split(strings.TrimSpace(output), "\n")
	if len(lines) == 0 {
		return "", fmt.Errorf("empty stat output")
	}
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
	targetNode string,
	since metav1.Time,
) {
	GinkgoHelper()

	if k8sClient != nil {
		var bds v1alpha1.BlockDeviceList
		if err := k8sClient.List(ctx, &bds); err != nil {
			GinkgoWriter.Printf("failed to collect BlockDevices: %v\n", err)
		} else {
			addJSONReportEntry("blockdevices.yaml", bds)
		}

		var lvgs v1alpha1.LVMVolumeGroupList
		if err := k8sClient.List(ctx, &lvgs); err != nil {
			GinkgoWriter.Printf("failed to collect LVMVolumeGroups: %v\n", err)
		} else {
			addJSONReportEntry("lvmvolumegroups.yaml", lvgs)
		}
	}

	events, err := cl.Clientset().CoreV1().Events("").List(ctx, metav1.ListOptions{})
	if err != nil {
		GinkgoWriter.Printf("failed to collect events: %v\n", err)
	} else {
		addJSONReportEntry("events.yaml", events)
	}

	if targetNode == "" || k8sClient == nil {
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
	clientset, err := k8sclient.NewForConfig(cl.RESTConfig())
	if err != nil {
		GinkgoWriter.Printf("failed to create clientset for agent logs: %v\n", err)
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
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		GinkgoWriter.Printf("failed to marshal %s: %v\n", name, err)
		return
	}
	AddReportEntry(name, string(data))
}
