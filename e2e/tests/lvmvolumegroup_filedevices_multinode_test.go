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
	"github.com/deckhouse/sds-node-configurator/e2e/sdsclient"
	"github.com/deckhouse/sds-node-configurator/e2e/tests/utils/consts"
	"github.com/deckhouse/storage-e2e/pkg/e2e"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// fdSetReadyTimeout bounds an LVMVolumeGroupSet fanning out to one LVMVolumeGroup per node,
// each of which has to fallocate and pvcreate its own backing file.
const fdSetReadyTimeout = 10 * time.Minute

// Cluster-wide behaviour of spec.fileDevices: the LVMVolumeGroupSet template path, which is
// how the feature is actually rolled out to a fleet, and the controller's own restart.
var _ = Describe("LVMVolumeGroup file-backed devices across nodes",
	Label("sds-node-configurator", "lvmvolumegroup", "file-devices"), Ordered, ContinueOnFailure, func() {
		var (
			ctx       context.Context
			cl        *e2e.Cluster
			k8sClient client.Client
			runID     string

			agentNodes []string
		)

		BeforeAll(func() {
			By("Preparing shared test context and Kubernetes clients")
			ctx = context.Background()

			var clErr error
			cl, clErr = e2e.Connect(ctx, e2e.WithTestName("lvmvolumegroup-filedevices-multinode"))
			Expect(clErr).NotTo(HaveOccurred(), "failed to connect to cluster")
			DeferCleanup(func() {
				if err := cl.Close(context.Background()); err != nil {
					GinkgoWriter.Println("Error closing cluster: ", err)
				}
			})

			var k8sErr error
			k8sClient, k8sErr = sdsclient.New(cl.RESTConfig())
			Expect(k8sErr).NotTo(HaveOccurred(), "failed to build controller-runtime client")

			By("Listing worker nodes that run a Ready agent")
			var nodesErr error
			agentNodes, nodesErr = fdWorkerNodesWithReadyAgent(ctx, cl)
			Expect(nodesErr).NotTo(HaveOccurred())
			Expect(agentNodes).NotTo(BeEmpty(), "at least one node with a Ready agent is required")
			GinkgoWriter.Printf("Nodes eligible for the LVMVolumeGroupSet: %v\n", agentNodes)

			runID = fmt.Sprintf("%d", time.Now().Unix())
		})

		AfterAll(func() {
			if k8sClient != nil {
				cleanupLVMVolumeGroups(ctx, k8sClient)
			}
			for _, n := range agentNodes {
				if cl != nil {
					fdSweepLeakedBackingFiles(ctx, cl, n)
					fdLogFreeSpace(ctx, cl, n, "after the specs")
				}
			}
		})

		It("Should fan an LVMVolumeGroupSet with fileDevices out to one LVMVolumeGroup per node", func() {
			// Prefixed with lvmVGNamePrefix so the LVMVolumeGroups the set generates
			// ("<setName>-<N>") are also caught by the suite-wide cleanupLVMVolumeGroups
			// sweep, not only by this spec's label-scoped cleanup.
			setName := lvmVGNamePrefix + "set-fd-" + runID
			vgName := "e2e-vg-fdset-" + runID
			setLabel := "e2e-fdset-" + runID

			By(fmt.Sprintf("Creating LVMVolumeGroupSet %s targeting %d node(s)", setName, len(agentNodes)))
			set := &v1alpha1.LVMVolumeGroupSet{
				ObjectMeta: metav1.ObjectMeta{Name: setName},
				Spec: v1alpha1.LVMVolumeGroupSetSpec{
					Strategy: "PerNode",
					NodeSelector: &metav1.LabelSelector{
						MatchExpressions: []metav1.LabelSelectorRequirement{{
							Key:      "kubernetes.io/hostname",
							Operator: metav1.LabelSelectorOpIn,
							Values:   agentNodes,
						}},
					},
					LVGTemplate: v1alpha1.LVMVolumeGroupTemplate{
						Metadata:              v1alpha1.LVMVolumeGroupTemplateMeta{Labels: map[string]string{"e2e-set": setLabel}},
						ActualVGNameOnTheNode: vgName,
						Type:                  "Local",
						FileDevices: []v1alpha1.LVMVolumeGroupFileDeviceSpec{
							{Name: "d1g", Directory: fdBaseDir, Size: resource.MustParse("1Gi")},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, set)).To(Succeed())
			DeferCleanup(func() {
				By("Deleting the LVMVolumeGroupSet and its generated LVMVolumeGroups")
				_ = client.IgnoreNotFound(k8sClient.Delete(ctx, &v1alpha1.LVMVolumeGroupSet{
					ObjectMeta: metav1.ObjectMeta{Name: setName},
				}))
				fdDeleteLVGsByLabelAndWaitGone(ctx, k8sClient, "e2e-set", setLabel, fdSetReadyTimeout)
			})

			By("Waiting for one Ready LVMVolumeGroup per selected node, each with its own backing file")
			var generated []v1alpha1.LVMVolumeGroup
			Eventually(func(g Gomega) {
				var list v1alpha1.LVMVolumeGroupList
				g.Expect(k8sClient.List(ctx, &list, client.MatchingLabels{"e2e-set": setLabel})).To(Succeed())
				g.Expect(list.Items).To(HaveLen(len(agentNodes)),
					"expected one LVMVolumeGroup per node (%d), got %d", len(agentNodes), len(list.Items))

				for i := range list.Items {
					lvg := &list.Items[i]
					if lvg.Status.Phase != v1alpha1.PhaseReady {
						fdPrintLVG(lvg)
					}
					g.Expect(lvg.Status.Phase).To(Equal(v1alpha1.PhaseReady),
						"LVMVolumeGroup %s should be Ready, got %s", lvg.Name, lvg.Status.Phase)
					g.Expect(lvg.Spec.FileDevices).To(HaveLen(1), "the template's fileDevices should be propagated")
				}
				generated = list.Items
			}, fdSetReadyTimeout, 15*time.Second).Should(Succeed())

			By("Verifying each generated LVMVolumeGroup landed on a distinct node with its own file")
			seenNodes := map[string]struct{}{}
			seenFiles := map[string]struct{}{}
			for i := range generated {
				lvg := &generated[i]
				node := lvg.Spec.Local.NodeName
				Expect(node).NotTo(BeEmpty(), "LVMVolumeGroup %s has no node", lvg.Name)
				Expect(seenNodes).NotTo(HaveKey(node), "two LVMVolumeGroups target node %s", node)
				seenNodes[node] = struct{}{}

				fds := fdFileDevicesForNode(lvg, node)
				Expect(fds).To(HaveLen(1), "LVMVolumeGroup %s should report one file device on %s", lvg.Name, node)
				Expect(fds[0].FilePath).To(ContainSubstring(lvg.Name),
					"each node's backing file must be named after its own LVMVolumeGroup")
				Expect(fds[0].LoopDevice).To(HavePrefix("/dev/loop"))
				Expect(seenFiles).NotTo(HaveKey(fds[0].FilePath), "two LVMVolumeGroups share backing file %s", fds[0].FilePath)
				seenFiles[fds[0].FilePath] = struct{}{}

				By(fmt.Sprintf("Verifying VG %s exists on node %s", vgName, node))
				listed, vgsOut, err := fdVGListedOnNode(ctx, cl, node, vgName)
				Expect(err).NotTo(HaveOccurred())
				Expect(listed).To(BeTrue(), "VG %s should exist on node %s; vgs:\n%s", vgName, node, vgsOut)
			}

			// Growing a fleet, first way: append an entry to the template. Every node
			// must end up with a second backing file.
			By("Appending a second fileDevices entry to the set template")
			vgSizeBefore := map[string]int64{}
			for i := range generated {
				vgSizeBefore[generated[i].Name] = generated[i].Status.VGSize.Value()
			}

			Eventually(func(g Gomega) {
				var cur v1alpha1.LVMVolumeGroupSet
				g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: setName}, &cur)).To(Succeed())
				cur.Spec.LVGTemplate.FileDevices = append(cur.Spec.LVGTemplate.FileDevices,
					v1alpha1.LVMVolumeGroupFileDeviceSpec{Name: "d2g", Directory: fdBaseDir, Size: resource.MustParse("2Gi")})
				g.Expect(k8sClient.Update(ctx, &cur)).To(Succeed())
			}, 2*time.Minute, 5*time.Second).Should(Succeed())

			By("Waiting for every node's LVMVolumeGroup to grow a second file device")
			Eventually(func(g Gomega) {
				var list v1alpha1.LVMVolumeGroupList
				g.Expect(k8sClient.List(ctx, &list, client.MatchingLabels{"e2e-set": setLabel})).To(Succeed())
				g.Expect(list.Items).To(HaveLen(len(agentNodes)),
					"the update must not fan out extra LVMVolumeGroups")

				for i := range list.Items {
					lvg := &list.Items[i]
					node := lvg.Spec.Local.NodeName
					if lvg.Status.Phase != v1alpha1.PhaseReady {
						fdPrintLVG(lvg)
					}
					g.Expect(lvg.Spec.FileDevices).To(HaveLen(2), "the appended entry should reach %s", lvg.Name)
					g.Expect(lvg.Status.Phase).To(Equal(v1alpha1.PhaseReady))
					g.Expect(fdFileDevicesForNode(lvg, node)).To(HaveLen(2),
						"node %s should report two file devices", node)
					g.Expect(lvg.Status.VGSize.Value()).To(BeNumerically(">", vgSizeBefore[lvg.Name]),
						"VG %s should grow after the template was extended", lvg.Name)
				}
			}, fdSetReadyTimeout, 15*time.Second).Should(Succeed())

			// Growing a fleet, second way: raise an existing entry's size. The set
			// copies the template into every child, so one edit runs the in-place
			// grow path (file, loop device, PV) on every node at once. No node may
			// gain a device or a file from it.
			By("Raising an existing entry's size in the set template")
			sizeBefore := map[string]int64{}
			var beforeGrow v1alpha1.LVMVolumeGroupList
			Expect(k8sClient.List(ctx, &beforeGrow, client.MatchingLabels{"e2e-set": setLabel})).To(Succeed())
			for i := range beforeGrow.Items {
				lvg := &beforeGrow.Items[i]
				sizeBefore[lvg.Name] = lvg.Status.VGSize.Value()
			}

			Eventually(func(g Gomega) {
				var cur v1alpha1.LVMVolumeGroupSet
				g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: setName}, &cur)).To(Succeed())
				for i := range cur.Spec.LVGTemplate.FileDevices {
					if cur.Spec.LVGTemplate.FileDevices[i].Name == "d1g" {
						cur.Spec.LVGTemplate.FileDevices[i].Size = resource.MustParse("3Gi")
					}
				}
				g.Expect(k8sClient.Update(ctx, &cur)).To(Succeed())
			}, 2*time.Minute, 5*time.Second).Should(Succeed(),
				"raising the size of an existing template entry must be accepted")

			By("Waiting for every node's VG to grow through the devices it already had")
			Eventually(func(g Gomega) {
				var list v1alpha1.LVMVolumeGroupList
				g.Expect(k8sClient.List(ctx, &list, client.MatchingLabels{"e2e-set": setLabel})).To(Succeed())
				g.Expect(list.Items).To(HaveLen(len(agentNodes)),
					"the resize must not fan out extra LVMVolumeGroups")

				for i := range list.Items {
					lvg := &list.Items[i]
					node := lvg.Spec.Local.NodeName
					if lvg.Status.Phase != v1alpha1.PhaseReady {
						fdPrintLVG(lvg)
					}
					g.Expect(lvg.Status.Phase).To(Equal(v1alpha1.PhaseReady))
					g.Expect(fdFileDevicesForNode(lvg, node)).To(HaveLen(2),
						"growth must not add a third file device on %s", node)
					g.Expect(lvg.Status.VGSize.Value()).To(BeNumerically(">", sizeBefore[lvg.Name]),
						"VG %s should grow after the entry was enlarged", lvg.Name)

					files, err := fdBackingFilesForLVG(ctx, cl, node, lvg.Name)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(files).To(HaveLen(2),
						"growth must not leave an extra backing file on %s: %v", node, files)
				}
			}, fdSetReadyTimeout, 15*time.Second).Should(Succeed())

			By("✓ LVMVolumeGroupSet fanned out, then grew every node's VG both by adding and by enlarging an entry")
		})

		// The block-device equivalent lives in controller_restart_test.go; a file-backed VG
		// must be just as boring across a controller restart — in particular
		// status.nodes[].fileDevices must not be rewritten with a new loop or file.
		It("Should keep file-backed status stable across an agent/controller restart", func() {
			targetNode := agentNodes[0]
			lvgName := lvmVGNamePrefix + "fdrst-" + runID
			vgName := "e2e-vg-fdrst-" + runID

			By(fmt.Sprintf("Creating file-backed LVMVolumeGroup %s on node %s", lvgName, targetNode))
			lvg := fdNewLVG(targetNode, lvgName, vgName, nil,
				[]v1alpha1.LVMVolumeGroupFileDeviceSpec{{Name: "d1g", Directory: fdBaseDir, Size: resource.MustParse("1Gi")}}, nil)
			DeferCleanup(func() { fdDeleteLVGAndWaitGone(ctx, k8sClient, lvgName) })
			created := fdCreateLVGAndWaitReady(ctx, cl, k8sClient, lvg)

			fdsBefore := fdFileDevicesForNode(created, targetNode)
			Expect(fdsBefore).To(HaveLen(1))
			before := fdsBefore[0]
			uidBefore := created.UID
			vgUUIDBefore := created.Status.VGUuid

			By("Restarting the sds-node-configurator pod on the node")
			Expect(restartAgentOnNode(ctx, cl, targetNode)).To(Succeed())

			// A fresh agent re-runs discovery and the resource dips out of Ready
			// while it does. The dip is expected; what must not change is the VG's
			// identity or its file device. Let it settle before asserting stability,
			// the same way the reattach spec does.
			By("Waiting for the LVMVolumeGroup to settle after the restart")
			Eventually(func(g Gomega) {
				var cur v1alpha1.LVMVolumeGroup
				g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: lvgName}, &cur)).To(Succeed())
				g.Expect(cur.Status.Phase).To(Equal(v1alpha1.PhaseReady),
					"the VG should return to Ready after the restart, got %s", cur.Status.Phase)
			}, fdLVGReadyTimeout, 10*time.Second).Should(Succeed())

			By("Verifying identity and file-device status do not churn")
			Consistently(func(g Gomega) {
				var cur v1alpha1.LVMVolumeGroup
				g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: lvgName}, &cur)).To(Succeed())
				g.Expect(cur.UID).To(Equal(uidBefore), "the LVMVolumeGroup must not be recreated")
				g.Expect(cur.Status.Phase).To(Equal(v1alpha1.PhaseReady))
				g.Expect(cur.Status.VGUuid).To(Equal(vgUUIDBefore), "the VG must not be recreated")

				fds := fdFileDevicesForNode(&cur, targetNode)
				g.Expect(fds).To(HaveLen(1))
				g.Expect(fds[0].FilePath).To(Equal(before.FilePath), "backing file path must be stable")
				g.Expect(fds[0].LoopDevice).To(Equal(before.LoopDevice), "loop device must be stable")
				g.Expect(fds[0].PVUuid).To(Equal(before.PVUuid), "the file-backed PV must not be recreated")
			}, 2*time.Minute, 15*time.Second).Should(Succeed())

			By("✓ File-backed VG identity and status unchanged after a restart")
		})
	})

// fdWorkerNodesWithReadyAgent returns every worker node running a Ready agent pod, in node
// listing order. Control-plane nodes are excluded so the set fans out over the same nodes a
// real workload would use.
func fdWorkerNodesWithReadyAgent(ctx context.Context, cl *e2e.Cluster) ([]string, error) {
	pods, err := cl.Clientset().CoreV1().Pods(consts.SdsNodeConfiguratorAgentNamespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app=" + consts.SdsNodeConfiguratorAgentName,
	})
	if err != nil {
		return nil, fmt.Errorf("list agent pods: %w", err)
	}
	ready := make(map[string]struct{}, len(pods.Items))
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.Spec.NodeName == "" || p.DeletionTimestamp != nil || !isPodReady(p) {
			continue
		}
		ready[p.Spec.NodeName] = struct{}{}
	}

	nodes, err := cl.Clientset().CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list nodes: %w", err)
	}
	var workers, fallback []string
	for i := range nodes.Items {
		n := &nodes.Items[i]
		if _, ok := ready[n.Name]; !ok {
			continue
		}
		if _, isControlPlane := n.Labels["node-role.kubernetes.io/control-plane"]; isControlPlane {
			fallback = append(fallback, n.Name)
			continue
		}
		workers = append(workers, n.Name)
	}
	if len(workers) > 0 {
		return workers, nil
	}
	return fallback, nil
}

// fdDeleteLVGsByLabelAndWaitGone deletes every LVMVolumeGroup carrying the label and waits
// until none remain, so each node's loop device and backing file are torn down.
func fdDeleteLVGsByLabelAndWaitGone(ctx context.Context, cl client.Client, key, value string, timeout time.Duration) {
	var list v1alpha1.LVMVolumeGroupList
	if err := cl.List(ctx, &list, client.MatchingLabels{key: value}); err != nil {
		GinkgoWriter.Printf("list LVMVolumeGroups by %s=%s: %v\n", key, value, err)
		return
	}
	for i := range list.Items {
		name := list.Items[i].Name
		if err := client.IgnoreNotFound(cl.Delete(ctx, &list.Items[i])); err != nil {
			GinkgoWriter.Printf("delete LVMVolumeGroup %s: %v\n", name, err)
		}
	}

	Eventually(func(g Gomega) {
		var cur v1alpha1.LVMVolumeGroupList
		g.Expect(cl.List(ctx, &cur, client.MatchingLabels{key: value})).To(Succeed())
		for i := range cur.Items {
			fdPrintLVG(&cur.Items[i])
		}
		g.Expect(cur.Items).To(BeEmpty(), "LVMVolumeGroups labelled %s=%s should be gone", key, value)
	}, timeout, 10*time.Second).Should(Succeed())
}
