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
	"strconv"
	"strings"
	"time"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/e2e/framework"
	"github.com/deckhouse/sds-node-configurator/e2e/sdsclient"
	"github.com/deckhouse/storage-e2e/pkg/e2e"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-node-configurator/e2e/tests/utils/consts"
)

const (
	// fdRebootBackTimeout bounds the full return: kubelet Ready plus a Ready
	// agent pod on the node again.
	fdRebootBackTimeout = 15 * time.Minute
)

// A reboot is the event the whole reattach path exists for, and it is the one
// thing the simulated variant (losetup -d + vgchange -an) cannot reproduce:
// only a real boot exercises the agent's startup ordering against udev and
// systemd, with every loop mapping genuinely gone from the kernel.
//
// It is also the most disruptive spec in the suite, so it carries its own
// label: exclude it with GINKGO_LABEL_FILTER='!node-reboot' if a run must not
// take a worker down.
var _ = Describe("LVMVolumeGroup file-backed devices across a node reboot",
	Label("sds-node-configurator", "lvmvolumegroup", "file-devices", "node-reboot"), Ordered, ContinueOnFailure, func() {
		var (
			ctx        context.Context
			cl         *e2e.Cluster
			k8sClient  client.Client
			targetNode string
			runID      string
		)

		BeforeAll(func() {
			By("Preparing shared test context and Kubernetes clients")
			ctx = context.Background()

			var clErr error
			cl, clErr = e2e.Connect(ctx, e2e.WithTestName("lvmvolumegroup-filedevices-reboot"))
			Expect(clErr).NotTo(HaveOccurred(), "failed to connect to cluster")
			DeferCleanup(func() {
				if err := cl.Close(context.Background()); err != nil {
					GinkgoWriter.Println("Error closing cluster: ", err)
				}
			})

			var k8sErr error
			k8sClient, k8sErr = sdsclient.New(cl.RESTConfig())
			Expect(k8sErr).NotTo(HaveOccurred(), "failed to build controller-runtime client")

			By("Selecting a worker node with a Ready sds-node-configurator agent")
			workers, nodesErr := fdWorkerNodesWithReadyAgent(ctx, cl)
			Expect(nodesErr).NotTo(HaveOccurred())
			Expect(workers).NotTo(BeEmpty(), "a worker node with a Ready agent is required")
			// Never reboot a control-plane node: losing the apiserver mid-suite
			// would fail every later spec for an unrelated reason.
			targetNode = workers[0]

			runID = fmt.Sprintf("%d", time.Now().Unix())
			fdLogFreeSpace(ctx, cl, targetNode, "before the specs")
		})

		AfterAll(func() {
			if k8sClient != nil {
				cleanupLVMVolumeGroups(ctx, k8sClient)
			}
			if cl != nil {
				fdSweepLeakedBackingFiles(ctx, cl, targetNode)
				fdLogFreeSpace(ctx, cl, targetNode, "after the specs")
			}
		})

		It("Should reattach file devices and reactivate the VG after the node reboots", func() {
			lvgName := lvmVGNamePrefix + "fdboot-" + runID
			vgName := "e2e-vg-fdboot-" + runID
			thinPoolName := "e2e-thin-fdboot"

			By(fmt.Sprintf("Creating a file-backed LVMVolumeGroup %s with a thin-pool on node %s", lvgName, targetNode))
			lvg := fdNewLVG(targetNode, lvgName, vgName, nil,
				[]v1alpha1.LVMVolumeGroupFileDeviceSpec{{Name: "d0", Directory: fdBaseDir, Size: resource.MustParse(fdFileDeviceSize)}},
				[]v1alpha1.LVMVolumeGroupThinPoolSpec{{Name: thinPoolName, Size: fdThinPoolSize, AllocationLimit: "100%"}})
			DeferCleanup(func() {
				fdRunThinPoolTeardown(ctx, cl, targetNode,
					framework.RemoveThinPoolStackScriptWithLVMConfig(vgName, thinPoolName, fdLVMConfig))
				fdDeleteLVGAndWaitGone(ctx, k8sClient, lvgName)
			})
			created := fdCreateLVGAndWaitReady(ctx, cl, k8sClient, lvg)
			fdExpectNoFalseConditions(created)

			fds := fdFileDevicesForNode(created, targetNode)
			Expect(fds).To(HaveLen(1))
			filePath := fds[0].FilePath
			vgUUIDBefore := created.Status.VGUuid

			By("Recording the node's boot id so the reboot can be confirmed, not assumed")
			bootIDBefore := fdNodeBootID(ctx, cl, targetNode)
			Expect(bootIDBefore).NotTo(BeEmpty())

			By(fmt.Sprintf("Rebooting node %s", targetNode))
			fdRebootNode(ctx, cl, targetNode)

			By("Waiting for the node to come back Ready with a fresh boot id")
			fdWaitNodeRebooted(ctx, cl, targetNode, bootIDBefore)

			By("Waiting for the sds-node-configurator agent to be Ready on the node again")
			fdWaitAgentReadyOnNode(ctx, cl, targetNode, fdRebootBackTimeout)

			By("Verifying the backing file survived and was reattached to a loop")
			exists, err := fdNodePathExists(ctx, cl, targetNode, filePath)
			Expect(err).NotTo(HaveOccurred())
			Expect(exists).To(BeTrue(), "the backing file must survive a reboot")

			Eventually(func(g Gomega) {
				loops, loopOut, err := fdLoopsForFile(ctx, cl, targetNode, filePath)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(loops).To(HaveLen(1),
					"exactly one loop must be reattached to %s after boot; losetup:\n%s", filePath, loopOut)
			}, fdRebootBackTimeout, 15*time.Second).Should(Succeed())

			By("Verifying the VG is active again with the same identity")
			var recovered v1alpha1.LVMVolumeGroup
			Eventually(func(g Gomega) {
				g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: lvgName}, &recovered)).To(Succeed())
				fdPrintLVG(&recovered)
				g.Expect(recovered.Status.Phase).To(Equal(v1alpha1.PhaseReady), "the VG should be Ready again after boot")
				g.Expect(recovered.Status.VGUuid).To(Equal(vgUUIDBefore), "the VG must be the same one, not recreated")
				fdsAfter := fdFileDevicesForNode(&recovered, targetNode)
				g.Expect(fdsAfter).To(HaveLen(1))
				g.Expect(fdsAfter[0].FilePath).To(Equal(filePath), "the deterministic backing file must be reused")
				g.Expect(fdsAfter[0].LoopDevice).To(HavePrefix("/dev/loop"))
			}, fdRebootBackTimeout, 15*time.Second).Should(Succeed())
			fdExpectNoFalseConditions(&recovered)

			By("Verifying the thin-pool on the file-backed VG is usable again")
			present, lvsOut, err := fdThinPoolDataLVPresentOnNode(ctx, cl, targetNode, vgName, thinPoolName)
			Expect(err).NotTo(HaveOccurred())
			Expect(present).To(BeTrue(), "thin-pool data LV should be back after boot; lvs:\n%s", lvsOut)

			By("Verifying no orphan backing file was allocated during recovery")
			files, err := fdBackingFilesForLVG(ctx, cl, targetNode, lvgName)
			Expect(err).NotTo(HaveOccurred())
			Expect(files).To(ConsistOf(filePath), "boot recovery must reuse the file, not allocate a new one")

			By("✓ File-backed VG survived a real node reboot: file reused, one loop, VG active, thin-pool intact")
		})

		// The agent relies on `losetup --nooverlap` to make attaching idempotent:
		// without it the startup reattach racing the reconciler's provision step
		// binds a second minor to the same backing file and the VG silently
		// doubles. --nooverlap needs util-linux >= 2.29, so this asserts the node
		// image actually honours it rather than trusting the flag is accepted.
		It("Should refuse to bind a second loop to the same backing file", func() {
			probeFile := fdBaseDir + "/e2e-nooverlap-probe-" + runID + ".img"

			DeferCleanup(func() {
				_, _ = framework.NodeExecChecked(ctx, cl, targetNode, fmt.Sprintf(`set +e
for l in $(sudo -n losetup -j %s 2>/dev/null | cut -d: -f1); do sudo -n losetup -d "$l"; done
sudo -n rm -f %s
exit 0`, strconv.Quote(probeFile), strconv.Quote(probeFile)))
			})

			// --direct-io is deliberately left out: it is a separate, best-effort
			// call in the agent (the kernel refuses it on a backing filesystem
			// without O_DIRECT support), and folding it in here would make this
			// spec fail for a reason that has nothing to do with --nooverlap.
			By("Allocating a probe file and attaching it twice with the agent's own attach flags")
			script := fmt.Sprintf(`set -e
sudo -n mkdir -p %s
sudo -n fallocate -l 16M %s
FIRST=$(sudo -n losetup --find --nooverlap --show %s)
SECOND=$(sudo -n losetup --find --nooverlap --show %s)
echo "first=$FIRST second=$SECOND"`,
				strconv.Quote(fdBaseDir), strconv.Quote(probeFile),
				strconv.Quote(probeFile), strconv.Quote(probeFile))

			out, err := framework.NodeExecChecked(ctx, cl, targetNode, script)
			Expect(err).NotTo(HaveOccurred(), "losetup --nooverlap should be supported by the node image:\n%s", out)

			var first, second string
			for _, field := range strings.Fields(strings.TrimSpace(out)) {
				if v, ok := strings.CutPrefix(field, "first="); ok {
					first = v
				}
				if v, ok := strings.CutPrefix(field, "second="); ok {
					second = v
				}
			}
			Expect(first).To(HavePrefix("/dev/loop"), "unexpected losetup output:\n%s", out)
			Expect(second).To(Equal(first),
				"--nooverlap must return the existing loop instead of binding a second minor; got %q then %q", first, second)

			loops, loopOut, err := fdLoopsForFile(ctx, cl, targetNode, probeFile)
			Expect(err).NotTo(HaveOccurred())
			Expect(loops).To(HaveLen(1), "the probe file must be bound to exactly one loop; losetup:\n%s", loopOut)

			By("✓ losetup --nooverlap is honoured on this node: a backing file cannot gain a second loop")
		})
	})

// ---=== Node reboot helpers ===--- //

// fdNodeBootID reads /proc/sys/kernel/random/boot_id, which the kernel
// regenerates on every boot. Comparing it is the only way to prove the node
// actually rebooted rather than merely blipping NotReady.
func fdNodeBootID(ctx context.Context, cl *e2e.Cluster, node string) string {
	GinkgoHelper()
	out, err := framework.NodeExecChecked(ctx, cl, node, "cat /proc/sys/kernel/random/boot_id")
	Expect(err).NotTo(HaveOccurred(), "failed to read boot id: %s", out)
	return strings.TrimSpace(out)
}

// fdRebootNode issues the reboot. The command is expected to die with the
// connection, so its error is deliberately ignored — fdWaitNodeRebooted is
// what decides whether the reboot happened.
func fdRebootNode(ctx context.Context, cl *e2e.Cluster, node string) {
	// `--no-wall` keeps the shutdown quiet; `&` plus a short delay lets the
	// command return before the transport drops.
	out, err := framework.NodeExecChecked(ctx, cl, node,
		"sudo -n sh -c 'nohup sh -c \"sleep 2; systemctl reboot --no-wall || reboot\" >/dev/null 2>&1 &' ; echo issued")
	GinkgoWriter.Printf("reboot command returned err=%v out=%q\n", err, strings.TrimSpace(out))
}

// fdWaitNodeRebooted waits until the node reports a different boot id and is
// Ready again. It tolerates the exec failing while the node is down.
func fdWaitNodeRebooted(ctx context.Context, cl *e2e.Cluster, node, bootIDBefore string) {
	GinkgoHelper()

	// No "wait for the node to go down" step: the SSH transport reconnects
	// transparently, so that poll never observed the outage and simply burned its
	// full timeout before the real check ran. The boot id is authoritative — it
	// only changes on an actual boot — so wait for that directly.
	By("Waiting for a new boot id and a Ready kubelet")
	Eventually(func(g Gomega) {
		out, err := framework.NodeExecChecked(ctx, cl, node, "cat /proc/sys/kernel/random/boot_id")
		g.Expect(err).NotTo(HaveOccurred(), "node is not reachable yet")
		g.Expect(strings.TrimSpace(out)).NotTo(Equal(bootIDBefore), "boot id unchanged: the node did not reboot")

		n, err := cl.Clientset().CoreV1().Nodes().Get(ctx, node, metav1.GetOptions{})
		g.Expect(err).NotTo(HaveOccurred())
		var ready bool
		for _, c := range n.Status.Conditions {
			if c.Type == corev1.NodeReady && c.Status == corev1.ConditionTrue {
				ready = true
			}
		}
		g.Expect(ready).To(BeTrue(), "node %s is back but not Ready yet", node)
	}, fdRebootBackTimeout, 15*time.Second).Should(Succeed())
}

// fdWaitAgentReadyOnNode waits until a Running, Ready agent pod exists on node.
func fdWaitAgentReadyOnNode(ctx context.Context, cl *e2e.Cluster, node string, timeout time.Duration) {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		pods, err := cl.Clientset().CoreV1().Pods(consts.SdsNodeConfiguratorAgentNamespace).List(ctx, metav1.ListOptions{
			LabelSelector: "app=" + consts.SdsNodeConfiguratorAgentName,
			FieldSelector: "spec.nodeName=" + node,
		})
		g.Expect(err).NotTo(HaveOccurred())

		var ready bool
		for i := range pods.Items {
			p := &pods.Items[i]
			if p.DeletionTimestamp == nil && p.Status.Phase == corev1.PodRunning && isPodReady(p) {
				ready = true
			}
		}
		g.Expect(ready).To(BeTrue(), "no Ready agent pod on node %s yet", node)
	}, timeout, 10*time.Second).Should(Succeed())
}
