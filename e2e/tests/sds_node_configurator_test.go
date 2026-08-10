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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sclient "k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	devicemapDiskSize = "5Gi"

	devicemapDiscoveryWait = 5 * time.Minute
	devicemapPollInterval  = 5 * time.Second
	devicemapCleanupWait   = 3 * time.Minute

	devicemapNetlinkEnvName = "ENABLE_NETLINK_BLOCK_DEVICE_DISCOVERY"
	// Matches images/agent/internal/scanner/scanner.go startup log when
	// NetlinkBlockDeviceDiscovery is enabled.
	devicemapInitialCrawlLogPattern = `\[RunScanner\] initial crawl found \d+ block devices`
)

// Focused coverage for the uncovered DeviceMap slice: a VirtualDisk attached while
// the agent is down must become a BlockDevice after the agent restarts. Netlink add
// for that attach is lost while no Running agent pod exists; recovery relies on the
// post-restart discovery path (startup crawler fills DeviceMap, then the first cache
// fill/reconcile creates the CR). This does not claim that no later netlink event
// can also touch the same device after the new pod is up.
//
// Broader BD/LVG stability after a plain agent restart is already covered by
// block_device_stable_test.go / controller_restart_test.go — kept here only as a
// light baseline-name check.
var _ = Describe("DeviceMap recovery after agent downtime attach",
	Label("sds-node-configurator", "block-device", "devicemap-crawler"), Ordered, func() {
		var (
			ctx        context.Context
			conf       *cfg.Config
			cl         *e2e.Cluster
			k8sClient  client.Client
			clientset  *k8sclient.Clientset
			targetNode string

			testStarted metav1.Time
			diskName    string
			newBDName   string
		)

		BeforeAll(func() {
			ctx = context.Background()

			var cfgErr error
			conf, cfgErr = cfg.Load()
			Expect(cfgErr).NotTo(HaveOccurred(), "failed to load config")

			var clErr error
			cl, clErr = e2e.Connect(ctx, e2e.WithTestName("devicemap-crawler-restart"))
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

			By("Checking that netlink block-device discovery is enabled on the agent DaemonSet")
			ds, dsErr := clientset.AppsV1().DaemonSets(consts.SdsNodeConfiguratorAgentNamespace).Get(
				ctx, consts.SdsNodeConfiguratorAgentName, metav1.GetOptions{},
			)
			Expect(dsErr).NotTo(HaveOccurred())
			if !daemonSetEnvIsTrue(ds, consts.SdsNodeConfiguratorAgentContainer, devicemapNetlinkEnvName) {
				Skip(fmt.Sprintf(
					"DaemonSet env %s is not true; enable enableNetlinkBlockDeviceDiscovery in ModuleConfig "+
						"(e2e/tests/cluster_config*.yml) — this spec needs the DeviceMap initial crawler path",
					devicemapNetlinkEnvName,
				))
			}
		})

		AfterEach(func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), devicemapCleanupWait)
			defer cancel()

			// On failure, attach kubectl logs / events / BD+LVG dumps to the Ginkgo report
			// before teardown so CI artifacts still show the cluster state that failed.
			if CurrentSpecReport().Failed() && cl != nil {
				collectDeviceMapFailureArtifacts(cleanupCtx, cl, k8sClient, clientset, targetNode, testStarted)
			}

			// Detach first so the agent drops the BlockDevice when the disk disappears.
			// Do not strip finalizers — they are owned by the agent.
			if cl != nil && diskName != "" {
				if err := cl.Disks().DetachDisk(cleanupCtx, targetNode, diskName); err != nil {
					GinkgoWriter.Printf("failed to detach disk %s: %v\n", diskName, err)
				}
				if err := cl.Disks().DeleteDisk(cleanupCtx, diskName); err != nil {
					GinkgoWriter.Printf("failed to delete disk %s: %v\n", diskName, err)
				}
			}

			diskName = ""
			newBDName = ""
		})

		It("discovers a BlockDevice attached while the agent was down after agent restart", func() {
			testStarted = metav1.NewTime(time.Now())

			By("Snapshotting consumable BlockDevices already present on the target node")
			baseline, baselineErr := kubernetes.GetConsumableBlockDevicesByNode(ctx, cl.RESTConfig(), targetNode)
			Expect(baselineErr).NotTo(HaveOccurred())
			baselineNames := make(map[string]struct{}, len(baseline))
			for _, bd := range baseline {
				baselineNames[bd.Name] = struct{}{}
			}

			diskName = fmt.Sprintf("e2e-devicemap-crawl-%d", time.Now().UnixNano())
			By("Creating a VirtualDisk that will be attached only while the agent is down: " + diskName)
			disk, createErr := cl.Disks().CreateDisk(ctx, e2e.DiskSpec{
				Name:         diskName,
				Size:         resource.MustParse(devicemapDiskSize),
				StorageClass: conf.TestCluster.StorageClass,
			})
			Expect(createErr).NotTo(HaveOccurred(), "failed to create virtual disk")
			Expect(disk).NotTo(BeNil())
			diskName = disk.Name

			By("Holding the agent down, attaching the disk, then waiting for the agent to become Ready")
			Expect(runWithAgentDownOnNode(ctx, cl, targetNode, func(attachCtx context.Context) error {
				return cl.Disks().AttachDisk(attachCtx, targetNode, diskName)
			})).To(Succeed())

			By("Waiting for the new consumable BlockDevice discovered after the agent restart")
			discovered, waitErr := framework.WaitNewConsumableBlockDevice(
				ctx, cl.RESTConfig(), targetNode, baseline, devicemapDiscoveryWait,
			)
			Expect(waitErr).NotTo(HaveOccurred())
			newBDName = discovered.Name
			Expect(newBDName).NotTo(BeEmpty())

			By("Asserting the new BlockDevice has expected status fields")
			var bd v1alpha1.BlockDevice
			Expect(k8sClient.Get(ctx, client.ObjectKey{Name: newBDName}, &bd)).To(Succeed())
			Expect(bd.Status.NodeName).To(Equal(targetNode))
			Expect(bd.Status.Consumable).To(BeTrue())
			Expect(bd.Status.Type).To(Equal("disk"))
			wantSize := resource.MustParse(devicemapDiskSize)
			maxSize := wantSize.DeepCopy()
			maxSize.Add(resource.MustParse("16Mi"))
			Expect(bd.Status.Size.Cmp(wantSize)).NotTo(BeNumerically("<", 0),
				"BD size must be >= requested %s, got %s", wantSize.String(), bd.Status.Size.String())
			Expect(bd.Status.Size.Cmp(maxSize)).NotTo(BeNumerically(">", 0),
				"BD size must be <= requested + 16Mi (%s), got %s", maxSize.String(), bd.Status.Size.String())

			By("Light baseline check: pre-existing consumable BlockDevices remain present")
			Eventually(func(g Gomega) {
				current, err := kubernetes.GetConsumableBlockDevicesByNode(ctx, cl.RESTConfig(), targetNode)
				g.Expect(err).NotTo(HaveOccurred())
				have := make(map[string]struct{}, len(current))
				for _, cur := range current {
					have[cur.Name] = struct{}{}
				}
				for name := range baselineNames {
					g.Expect(have).To(HaveKey(name), "baseline BlockDevice %s disappeared after downtime recovery", name)
				}
				g.Expect(have).To(HaveKey(newBDName))
			}, devicemapDiscoveryWait, devicemapPollInterval).Should(Succeed())

			By("Asserting agent logs show the initial crawler scan on restart")
			agentPod, podErr := pod.FindRunningPodOnNode(
				ctx,
				k8sClient,
				targetNode,
				client.InNamespace(consts.SdsNodeConfiguratorAgentNamespace),
				client.MatchingLabels{"app": consts.SdsNodeConfiguratorAgentName},
			)
			Expect(podErr).NotTo(HaveOccurred(), "failed to find Ready agent pod on node %s", targetNode)

			Eventually(func(g Gomega) {
				logText, logErr := pod.GetLogs(ctx, clientset, consts.SdsNodeConfiguratorAgentNamespace, agentPod.Name, v1.PodLogOptions{
					Container: consts.SdsNodeConfiguratorAgentContainer,
					SinceTime: &testStarted,
				})
				g.Expect(logErr).NotTo(HaveOccurred())
				g.Expect(logText).To(MatchRegexp(devicemapInitialCrawlLogPattern),
					"agent must run initial crawler scan when rebuilding DeviceMap after restart")
			}, 2*time.Minute, 2*time.Second).Should(Succeed())
		})
	})

func collectDeviceMapFailureArtifacts(
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

	if cl != nil {
		events, err := cl.Clientset().CoreV1().Events(consts.SdsNodeConfiguratorAgentNamespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			GinkgoWriter.Printf("failed to collect events: %v\n", err)
		} else {
			addJSONReportEntry("events.json", events)
		}
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

