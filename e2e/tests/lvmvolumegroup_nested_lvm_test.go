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
	"github.com/deckhouse/sds-node-configurator/e2e/cfg"
	"github.com/deckhouse/sds-node-configurator/e2e/framework"
	"github.com/deckhouse/sds-node-configurator/e2e/sdsclient"
	"github.com/deckhouse/sds-node-configurator/e2e/tests/utils/consts"
	"github.com/deckhouse/storage-e2e/pkg/e2e"
	"github.com/deckhouse/storage-e2e/pkg/kubernetes"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Nested LVM is the normal state of a hypervisor, not an edge case: a block-mode
// PersistentVolume handed to a virtual machine appears on the host as /dev/loopN,
// and whatever the guest does inside it — its own Volume Groups, named however the
// guest likes — is visible to the agent's LVM commands. The agent overrides the
// module's own NodeGroupConfiguration filter (global_filter = ["r|^/dev/loop[0-9]+|"]
// in /etc/lvm/lvm.conf) on every command, so it, and only it, sees them.
//
// Two ways that took a node's own storage offline on a 20-node cluster, both
// reproduced here, and neither of them involving the guest's Volume Group being
// adopted (lvmvolumegroup_filedevices_foreign_test.go covers adoption):
//
//   - a guest name colliding with the node's own Volume Group name;
//   - two GUEST Volume Groups colliding with EACH OTHER, which is the one nobody
//     expects: nothing about it concerns this node's storage, and it still marked
//     the node's own vg NonOperational on six nodes out of twenty.
//
// The first spec deliberately puts the node's own Volume Group on a REAL DISK: that
// is the shape every node has, the shape the incident happened on, and the one no
// existing spec pairs with a foreign loop VG. The others use a file-backed Volume
// Group as the bystander, because a second and third create/attach/delete of a real
// disk on one node inside a single Ordered container is where this suite's
// BlockDevice linkage stops settling in time — and a spec that reports its own
// fixture is worse than no spec.
const (
	// nlvmArchiveDir is where lvm keeps the metadata history whose size it warns
	// about on STDOUT, in the middle of its own JSON report.
	nlvmArchiveDir = "/etc/lvm/archive"

	// nlvmArchiveFiles is how many archive entries the advisory spec plants. lvm
	// starts printing "Consider pruning <vg> VG archive with more then N MiB in N
	// files" somewhere above 8192 entries — the node this was taken from tripped it
	// at 8929 files and 12 MiB, so the count and not the size is what matters. The
	// spec verifies the advisory actually appeared and skips itself if this node's
	// lvm does not print one, rather than passing on an unreproduced precondition.
	nlvmArchiveFiles = 9000

	// nlvmDiskSize only has to be large enough for a Volume Group to be created on.
	nlvmDiskSize = "1Gi"

	// nlvmQuietFor is how long the LVMVolumeGroup has to stay healthy for the spec
	// to accept that the foreign Volume Groups are not affecting it. It spans
	// several scanner cycles, which is where the damage used to appear: the scan is
	// what re-reads lvm's stderr and re-attributes it.
	nlvmQuietFor = 2 * time.Minute
)

var _ = Describe("LVMVolumeGroup with nested guest LVM on the node",
	Label("sds-node-configurator", "lvmvolumegroup", "nested-lvm"), Ordered, ContinueOnFailure, func() {
		var (
			ctx        context.Context
			conf       *cfg.Config
			cl         *e2e.Cluster
			k8sClient  client.Client
			targetNode string
			runID      string
			guestDir   string
		)

		BeforeAll(func() {
			By("Preparing shared test context and Kubernetes clients")
			ctx = context.Background()

			var cfgErr error
			conf, cfgErr = cfg.Load()
			Expect(cfgErr).NotTo(HaveOccurred(), "failed to load config")

			var clErr error
			cl, clErr = e2e.Connect(ctx, e2e.WithTestName("lvmvolumegroup-nested-lvm"))
			Expect(clErr).NotTo(HaveOccurred(), "failed to connect to cluster")
			DeferCleanup(func() {
				if err := cl.Close(context.Background()); err != nil {
					GinkgoWriter.Println("Error closing cluster: ", err)
				}
			})

			var k8sErr error
			k8sClient, k8sErr = sdsclient.New(cl.RESTConfig())
			Expect(k8sErr).NotTo(HaveOccurred(), "failed to build controller-runtime client")

			By("Selecting a node with a Ready sds-node-configurator agent")
			var nodeErr error
			targetNode, nodeErr = fdNodeWithReadyAgent(ctx, cl)
			Expect(nodeErr).NotTo(HaveOccurred())

			Expect(conf.TestCluster.StorageClass).NotTo(BeEmpty(), "TestCluster.StorageClass required")

			runID = fmt.Sprintf("%d", time.Now().Unix())
			// Outside the agent's base directory: a guest's disk has no reason to live
			// under /opt/deckhouse/sds, and putting it there would test ownership
			// (which the foreign-loop specs already do) instead of visibility.
			guestDir = "/var/tmp/e2e-nested-lvm-" + runID
		})

		AfterAll(func() {
			if k8sClient != nil {
				cleanupLVMVolumeGroups(ctx, k8sClient)
			}
		})

		// The incident's first class: four nodes of twenty in
		// VGConfigurationApplied=False/CacheEmpty. The guest's Volume Group answering to
		// the same name as the node's own is what made lvm re-archive metadata on every
		// read, which is what eventually produced the advisory that broke the report.
		It("Should keep a disk-backed LVMVolumeGroup Ready while a guest VG answers to the same name", func() {
			vgName := "e2e-vg-nested-dup-" + runID
			lvgName := fdLVGName("nesteddup", runID, targetNode)

			// Staged before ours exists, because vgcreate refuses a name already taken
			// on the node. In production the guest gets there first just as easily — it
			// picks its VG name without asking the host.
			By(fmt.Sprintf("Staging a guest disk carrying a VG also named %s, untagged and detached", vgName))
			guest := fdStageForeignLoopVGImage(ctx, cl, targetNode, guestDir,
				"guest-dup-"+runID+".img", vgName, nil)
			DeferCleanup(func() { fdDestroyForeignLoopVG(ctx, cl, guest) })

			By(fmt.Sprintf("Creating our own LVMVolumeGroup %s on a real disk, VG %s", lvgName, vgName))
			ourLVG := nlvmCreateDiskBackedLVG(ctx, cl, k8sClient, conf, targetNode, runID, "dup", lvgName, vgName)
			Expect(ourLVG.Status.Phase).To(Equal(v1alpha1.PhaseReady))

			By("Plugging the guest disk in, so two Volume Groups now answer to the name")
			fdAttachForeignLoopVGImage(ctx, cl, guest)
			DeferCleanup(func() { fdDetachForeignLoopVG(ctx, cl, guest) })
			nlvmExpectVGNameCount(ctx, cl, targetNode, vgName, 2)

			logsFrom := time.Now()

			By("Verifying our LVMVolumeGroup stays Ready and the scan keeps working")
			Consistently(func(g Gomega) {
				var cur v1alpha1.LVMVolumeGroup
				g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: lvgName}, &cur)).To(Succeed())
				g.Expect(cur.Status.Phase).To(Equal(v1alpha1.PhaseReady),
					"a guest's Volume Group must not take the node's own storage offline; conditions: %s",
					describeLVGStatus(&cur))
				g.Expect(cur.Status.VGSize.Value()).To(BeNumerically(">", 0),
					"status stopped being refreshed, which is what a stalled cache looks like")
			}, nlvmQuietFor, 15*time.Second).Should(Succeed())

			By("Verifying the agent never failed to parse an LVM report")
			nlvmExpectNoReportParseFailures(ctx, cl, targetNode, logsFrom)

			By("Detaching the guest disk before teardown, so our VG owns its name again")
			fdDetachForeignLoopVG(ctx, cl, guest)

			By("✓ A guest VG of the same name neither stopped the scan nor took the LVMVolumeGroup down")
		})

		// The incident's second class, and the reason this file exists: six nodes of
		// twenty in VGReady=False/ScanFailed over a name collision they had no part in.
		// lvm prints the duplicate-name warning on EVERY invocation, whatever object the
		// command asked about, and the discoverer recorded whatever stderr came back as
		// the queried Volume Group's health.
		It("Should ignore a name collision between two guest VGs that has nothing to do with ours", func() {
			guestVG := "e2e-guest-shared-name-" + runID
			vgName := "e2e-vg-nested-bystander-" + runID
			lvgName := fdLVGName("nestedby", runID, targetNode)

			By(fmt.Sprintf("Staging two guest disks that both carry a VG named %s", guestVG))
			// Sequentially, and each detached before the next is built: two Volume Groups
			// of one name cannot be created while both are visible.
			guestA := fdStageForeignLoopVGImage(ctx, cl, targetNode, guestDir,
				"guest-a-"+runID+".img", guestVG, nil)
			DeferCleanup(func() { fdDestroyForeignLoopVG(ctx, cl, guestA) })
			guestB := fdStageForeignLoopVGImage(ctx, cl, targetNode, guestDir,
				"guest-b-"+runID+".img", guestVG, nil)
			DeferCleanup(func() { fdDestroyForeignLoopVG(ctx, cl, guestB) })
			Expect(guestA.VGUUID).NotTo(Equal(guestB.VGUUID), "the two guest VGs must be distinct")

			// File-backed rather than disk-backed, unlike the spec above, and for a
			// reason that has nothing to do with the subject: what this spec needs is a
			// healthy bystander LVMVolumeGroup, and a third create/attach/delete of a
			// real disk on the same node within one Ordered container is where this
			// suite's BlockDevice linkage stops settling in time — the resource sits
			// Pending with an empty status.nodes and the spec reports the fixture rather
			// than the collision it is about. The bystander is if anything a better one
			// this way: it lives on a loop device of ours, so it also shows the agent
			// still serving its own loop-backed storage while two guest loop VGs collide.
			By(fmt.Sprintf("Creating our own file-backed LVMVolumeGroup %s (VG %s) — a name unrelated to the guests'", lvgName, vgName))
			lvg := fdNewLVG(targetNode, lvgName, vgName, nil,
				[]v1alpha1.LVMVolumeGroupFileDeviceSpec{{Name: "d1g", Directory: fdBaseDir, Size: resource.MustParse("1Gi")}}, nil)
			DeferCleanup(func() { fdDeleteLVGAndWaitGone(ctx, k8sClient, lvgName) })
			ourLVG := fdCreateLVGAndWaitReady(ctx, cl, k8sClient, lvg)
			fdExpectNoFalseConditions(ourLVG)

			By("Plugging both guest disks in, so lvm warns about the collision on every command")
			fdAttachForeignLoopVGImage(ctx, cl, guestA)
			DeferCleanup(func() { fdDetachForeignLoopVG(ctx, cl, guestA) })
			fdAttachForeignLoopVGImage(ctx, cl, guestB)
			DeferCleanup(func() { fdDetachForeignLoopVG(ctx, cl, guestB) })
			nlvmExpectVGNameCount(ctx, cl, targetNode, guestVG, 2)

			// Without the warning on stderr the spec proves nothing, so check that lvm
			// really does print it for a command about an unrelated object.
			By("Verifying lvm reports the collision even when asked about our own VG")
			stderrText := nlvmVGSStderrOnNode(ctx, cl, targetNode, vgName)
			Expect(stderrText).To(ContainSubstring("is used by VGs"),
				"lvm did not warn about the guest collision while querying %s, so this node cannot reproduce the case", vgName)

			By("Verifying our LVMVolumeGroup never picks up somebody else's collision")
			Consistently(func(g Gomega) {
				var cur v1alpha1.LVMVolumeGroup
				g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: lvgName}, &cur)).To(Succeed())
				g.Expect(cur.Status.Phase).To(Equal(v1alpha1.PhaseReady),
					"a collision between two guest VGs must not reach our LVMVolumeGroup; conditions: %s",
					describeLVGStatus(&cur))
				for i := range cur.Status.Conditions {
					c := &cur.Status.Conditions[i]
					g.Expect(c.Message).NotTo(ContainSubstring("is used by VGs"),
						"condition %s carries lvm's node-wide duplicate warning, which is not about this Volume Group: %s",
						c.Type, c.Message)
					g.Expect(c.Message).NotTo(ContainSubstring("Fix duplicate VG names"),
						"condition %s carries lvm's node-wide advice, which is not about this Volume Group: %s",
						c.Type, c.Message)
					if c.Type == "VGReady" {
						g.Expect(c.Reason).NotTo(Equal("ScanFailed"),
							"the scan of our Volume Group was reported as failed over a foreign name collision: %s", c.Message)
					}
				}
			}, nlvmQuietFor, 15*time.Second).Should(Succeed())

			By("✓ A collision between two guest VGs left the node's own LVMVolumeGroup alone")
		})

		// The advisory itself: lvm prints part of its diagnostics on stdout, and
		// --reportformat json does not make an exception. One such line landing inside
		// the report is what stopped fillTheCache on four nodes for hours.
		It("Should keep filling the cache while lvm prints archive advisories into its JSON report", func() {
			vgName := "e2e-vg-nested-advisory-" + runID
			lvgName := fdLVGName("nestedadv", runID, targetNode)

			// The advisory is printed while lvm archives, and a READ command archives
			// only when it finds metadata to repair — which is what a duplicate name
			// gives it. Both halves are needed: on the nodes that broke, the archive had
			// grown large *because* the duplicate made every scan write to it.
			By(fmt.Sprintf("Staging a guest disk carrying a VG also named %s, detached for now", vgName))
			guest := fdStageForeignLoopVGImage(ctx, cl, targetNode, guestDir,
				"guest-adv-"+runID+".img", vgName, nil)
			DeferCleanup(func() { fdDestroyForeignLoopVG(ctx, cl, guest) })

			By(fmt.Sprintf("Creating our own file-backed LVMVolumeGroup %s (VG %s)", lvgName, vgName))
			lvg := fdNewLVG(targetNode, lvgName, vgName, nil,
				[]v1alpha1.LVMVolumeGroupFileDeviceSpec{{Name: "d1g", Directory: fdBaseDir, Size: resource.MustParse("1Gi")}}, nil)
			DeferCleanup(func() { fdDeleteLVGAndWaitGone(ctx, k8sClient, lvgName) })
			ourLVG := fdCreateLVGAndWaitReady(ctx, cl, k8sClient, lvg)
			fdExpectNoFalseConditions(ourLVG)

			By(fmt.Sprintf("Planting %d archive entries for %s so lvm starts advising to prune them", nlvmArchiveFiles, vgName))
			planted := nlvmPlantArchiveEntries(ctx, cl, targetNode, vgName, nlvmArchiveFiles)
			DeferCleanup(func() { nlvmRemovePlantedArchiveEntries(ctx, cl, targetNode, vgName) })
			Expect(planted).To(BeNumerically(">=", nlvmArchiveFiles),
				"the archive was not filled, so the advisory cannot appear")

			By("Plugging the guest disk in, so a plain vgs has metadata to archive")
			fdAttachForeignLoopVGImage(ctx, cl, guest)
			DeferCleanup(func() { fdDetachForeignLoopVG(ctx, cl, guest) })
			nlvmExpectVGNameCount(ctx, cl, targetNode, vgName, 2)

			By("Checking whether this node's lvm actually mixes the advisory into its report")
			stdout := nlvmVGSStdoutOnNode(ctx, cl, targetNode)
			if !nlvmHasNonJSONLine(stdout) {
				Skip(fmt.Sprintf("this lvm build does not mix advisories into the JSON report even with %d archive entries; "+
					"the parser is covered by the unit test TestReportSurvivesLVMAdvisoriesOnStdout instead.\nstdout head:\n%s",
					nlvmArchiveFiles, nlvmHead(stdout, 400)))
			}
			GinkgoWriter.Printf("lvm mixes non-JSON lines into its report on this node:\n%s\n", nlvmNonJSONLines(stdout))

			logsFrom := time.Now()

			By("Verifying the agent parses the report anyway and keeps the LVMVolumeGroup Ready")
			Consistently(func(g Gomega) {
				var cur v1alpha1.LVMVolumeGroup
				g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: lvgName}, &cur)).To(Succeed())
				g.Expect(cur.Status.Phase).To(Equal(v1alpha1.PhaseReady),
					"an advisory line inside the report must not take the Volume Group offline; conditions: %s",
					describeLVGStatus(&cur))
				for i := range cur.Status.Conditions {
					c := &cur.Status.Conditions[i]
					g.Expect(c.Reason).NotTo(Equal("CacheEmpty"),
						"the cache stopped filling, which is the outage this spec is about: %s", c.Message)
				}
			}, nlvmQuietFor, 15*time.Second).Should(Succeed())

			By("Verifying the agent logged no report parse failure")
			nlvmExpectNoReportParseFailures(ctx, cl, targetNode, logsFrom)

			By("Detaching the guest disk before teardown, so our VG owns its name again")
			fdDetachForeignLoopVG(ctx, cl, guest)

			By("✓ An advisory inside the JSON report neither stopped the scan nor emptied the cache")
		})

		// The pair that pins the filter down from both sides on ONE node, and the
		// reason it is a single spec: rejecting every loop device would satisfy the
		// second half and break the first, and accepting every loop device does the
		// opposite. Only a filter that admits the agent's own loops by name passes.
		//
		// "The agent does not see it" is asserted where it is observable — through the
		// code paths that run over everything the agent does see. ReTag rewrites the
		// tags of every Volume Group it finds and ActivateAllManagedVGs activates their
		// Logical Volumes, so a guest Volume Group coming back untouched from an agent
		// restart is the strongest available statement that it was never in view. The
		// filter string itself is pinned in the unit test
		// TestLVMFilterAlwaysRejectsUnownedLoopDevices — the e2e module cannot import
		// the agent's constant, and asserting on a copy of it would only test the copy.
		It("Should serve its own file-backed devices while leaving a guest's loop LVM untouched", func() {
			lvgName := fdLVGName("nestedboth", runID, targetNode)
			vgName := "e2e-vg-nested-both-" + runID
			guestVG := "e2e-guest-untouched-" + runID
			const guestLV = "guest-data"

			By(fmt.Sprintf("Creating a guest loop VG %s with a deactivated LV, tagged as the module's", guestVG))
			// Tagged, because the tag is what ReTag and the activation path gate on: an
			// untagged Volume Group would be left alone even by an agent that sees it,
			// and the spec would prove nothing about visibility.
			guest := fdCreateForeignLoopVGWithLV(ctx, cl, targetNode, guestDir,
				"guest-untouched-"+runID+".img", guestVG, guestLV, []string{fdManagedTag})
			DeferCleanup(func() { fdDestroyForeignLoopVG(ctx, cl, guest) })

			By(fmt.Sprintf("Creating our own file-backed LVMVolumeGroup %s (VG %s) on the same node", lvgName, vgName))
			lvg := fdNewLVG(targetNode, lvgName, vgName, nil,
				[]v1alpha1.LVMVolumeGroupFileDeviceSpec{{Name: "d1g", Directory: fdBaseDir, Size: resource.MustParse("1Gi")}}, nil)
			DeferCleanup(func() { fdDeleteLVGAndWaitGone(ctx, k8sClient, lvgName) })
			created := fdCreateLVGAndWaitReady(ctx, cl, k8sClient, lvg)
			fdExpectNoFalseConditions(created)

			By("Verifying our own loop-backed device is exactly the one in status")
			fds := fdFileDevicesForNode(created, targetNode)
			Expect(fds).To(HaveLen(1), "the guest's loop must not be counted as ours")
			Expect(fds[0].LoopDevice).To(HavePrefix("/dev/loop"),
				"a file-backed PV is a loop device; if the agent could not use it, the LVG would never have gone Ready")
			Expect(fds[0].LoopDevice).NotTo(Equal(guest.Loop))

			By("Restarting the agent so ReTag and ActivateAllManagedVGs run over everything it can see")
			Expect(restartAgentOnNode(ctx, cl, targetNode)).To(Succeed())

			By("Waiting for our LVMVolumeGroup to come back Ready after the restart")
			Eventually(func(g Gomega) {
				var cur v1alpha1.LVMVolumeGroup
				g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: lvgName}, &cur)).To(Succeed())
				g.Expect(cur.Status.Phase).To(Equal(v1alpha1.PhaseReady),
					"the agent must still be able to use its own loop devices after a restart; conditions: %s",
					describeLVGStatus(&cur))
				g.Expect(fdFileDevicesForNode(&cur, targetNode)).To(HaveLen(1))
			}, fdLVGReadyTimeout, 5*time.Second).Should(Succeed())

			By("Verifying the guest's Volume Group was neither re-tagged, activated, nor adopted")
			Consistently(func(g Gomega) {
				active, lvsOut, err := fdLVActiveOnNode(ctx, cl, targetNode, guestVG, guestLV)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(active).To(BeFalse(),
					"the guest's logical volume was activated on the host, so the agent is reading LVM it must not see; lvs:\n%s", lvsOut)

				var lvgs v1alpha1.LVMVolumeGroupList
				g.Expect(k8sClient.List(ctx, &lvgs)).To(Succeed())
				for i := range lvgs.Items {
					g.Expect(lvgs.Items[i].Spec.ActualVGNameOnTheNode).NotTo(Equal(guestVG),
						"LVMVolumeGroup %s adopted the guest's VG", lvgs.Items[i].Name)
				}
			}, nlvmQuietFor, 15*time.Second).Should(Succeed())
			fdExpectForeignLoopVGIntact(ctx, cl, guest)

			By("✓ Own file-backed devices served, guest LVM on the same node untouched")
		})
	})

// nlvmCreateDiskBackedLVG attaches a fresh disk to the node and builds an
// LVMVolumeGroup on the BlockDevice it turns into, waiting for Ready.
//
// A disk-backed group rather than a file-backed one: that is the shape every node
// in production has, and the shape whose interaction with a guest's loop-backed
// Volume Group nothing covered.
func nlvmCreateDiskBackedLVG(
	ctx context.Context,
	cl *e2e.Cluster,
	k8sClient client.Client,
	conf *cfg.Config,
	node, runID, tag, lvgName, vgName string,
) *v1alpha1.LVMVolumeGroup {
	GinkgoHelper()

	before, err := kubernetes.GetConsumableBlockDevicesByNode(ctx, cl.RESTConfig(), node)
	Expect(err).NotTo(HaveOccurred())

	diskName := fmt.Sprintf("e2e-nested-lvm-%s-%s", tag, runID)
	disk, err := cl.Disks().CreateDisk(ctx, e2e.DiskSpec{
		Name:         diskName,
		Size:         resource.MustParse(nlvmDiskSize),
		StorageClass: conf.TestCluster.StorageClass,
	})
	Expect(err).NotTo(HaveOccurred(), "failed to create the disk for %s", lvgName)
	DeferCleanup(func() {
		_ = cl.Disks().DetachDisk(ctx, node, disk.Name)
		_ = cl.Disks().DeleteDisk(ctx, disk.Name)
	})
	Expect(cl.Disks().AttachDisk(ctx, node, disk.Name)).To(Succeed(), "failed to attach the disk for %s", lvgName)

	bd, err := framework.WaitNewConsumableBlockDevice(ctx, cl.RESTConfig(), node, before, 5*time.Minute)
	Expect(err).NotTo(HaveOccurred(), "no consumable BlockDevice appeared for the disk of %s", lvgName)

	Expect(kubernetes.CreateLVMVolumeGroup(ctx, cl.RESTConfig(), lvgName, node, []string{bd.Name}, vgName)).
		To(Succeed(), "failed to create LVMVolumeGroup %s", lvgName)
	DeferCleanup(func() { lvgextDeleteLVG(ctx, k8sClient, lvgName) })

	var ready v1alpha1.LVMVolumeGroup
	Eventually(func(g Gomega) {
		g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: lvgName}, &ready)).To(Succeed())
		g.Expect(ready.Status.Phase).To(Equal(v1alpha1.PhaseReady),
			"LVMVolumeGroup %s did not become Ready; conditions: %s", lvgName, describeLVGStatus(&ready))
	}, lvmVolumeGroupReadyTimeout, 10*time.Second).Should(Succeed())

	return &ready
}

// nlvmExpectVGNameCount asserts how many Volume Groups on the node answer to a
// name. It is the precondition check of every spec here: without the duplicate
// actually present, they all pass for the wrong reason.
func nlvmExpectVGNameCount(ctx context.Context, cl *e2e.Cluster, node, vgName string, want int) {
	GinkgoHelper()
	out, err := framework.NodeExecChecked(ctx, cl, node,
		fmt.Sprintf(`sudo -n lvm vgs %s --noheadings -o vg_name,vg_uuid 2>/dev/null | awk '$1 == %s' | wc -l`,
			fdLVMCfg, strconv.Quote(vgName)))
	Expect(err).NotTo(HaveOccurred(), "failed to count VGs named %s on %s:\n%s", vgName, node, out)

	got, convErr := strconv.Atoi(strings.TrimSpace(nlvmLastLine(out)))
	Expect(convErr).NotTo(HaveOccurred(), "unexpected output counting VGs named %s:\n%s", vgName, out)
	Expect(got).To(Equal(want), "expected %d Volume Groups named %s on %s, got %d", want, vgName, node, got)
}

// nlvmVGSStderrOnNode returns what lvm prints on stderr for a report about one
// specific Volume Group — the stream the discoverer used to attribute wholesale to
// whatever object it had asked about.
func nlvmVGSStderrOnNode(ctx context.Context, cl *e2e.Cluster, node, vgName string) string {
	GinkgoHelper()
	out, err := framework.NodeExecChecked(ctx, cl, node,
		fmt.Sprintf(`sudo -n lvm vgs %s -o vg_name,vg_uuid --reportformat json %s 2>&1 >/dev/null || true`,
			fdLVMCfg, strconv.Quote(vgName)))
	Expect(err).NotTo(HaveOccurred(), "failed to read lvm's stderr for %s on %s:\n%s", vgName, node, out)
	return out
}

// nlvmVGSStdoutOnNode runs the agent's own VG listing and returns stdout only,
// which is where lvm's advisories end up alongside the JSON.
func nlvmVGSStdoutOnNode(ctx context.Context, cl *e2e.Cluster, node string) string {
	GinkgoHelper()
	out, err := framework.NodeExecChecked(ctx, cl, node,
		fmt.Sprintf(`sudo -n lvm vgs %s -o +uuid,tags,shared,vg_attr,vg_extent_size --units B --nosuffix --reportformat json 2>/dev/null || true`,
			fdLVMCfg))
	Expect(err).NotTo(HaveOccurred(), "failed to read lvm's stdout on %s:\n%s", node, out)
	return out
}

// nlvmPlantArchiveEntries fills /etc/lvm/archive with copies of the Volume Group's
// metadata and returns how many entries the directory holds afterwards.
//
// The source is /etc/lvm/backup/<vg> — the current metadata, which vgcreate always
// writes — and not an existing entry in the archive itself: a Volume Group created a
// minute ago has had no metadata change to archive yet, so requiring one there is how
// this helper used to fail with "no archive entry to copy" before it had planted
// anything. Real metadata rather than empty files, because lvm reads this directory
// to decide whether to advise pruning.
//
// find + xargs rather than a glob: the directory is about to hold five figures of
// files, where `ls "$VG"_*.vg` dies with "Argument list too long".
func nlvmPlantArchiveEntries(ctx context.Context, cl *e2e.Cluster, node, vgName string, count int) int {
	GinkgoHelper()

	script := fmt.Sprintf(`set -e
DIR=%s
VG=%s
SRC=/etc/lvm/backup/"$VG"
if ! sudo -n test -f "$SRC"; then
  SRC=$(sudo -n find "$DIR" -maxdepth 1 -name "$VG"_'*.vg' -print -quit 2>/dev/null)
fi
if [ -z "$SRC" ]; then
  echo "no metadata for $VG to copy: neither /etc/lvm/backup/$VG nor an archive entry exists" >&2
  exit 1
fi
sudo -n mkdir -p "$DIR"
sudo -n sh -c 'seq 1 '%d' | xargs -P 8 -I{} cp "'"$SRC"'" "'"$DIR"'/'"$VG"'_9{}-0000000000.vg"'
sudo -n find "$DIR" -maxdepth 1 -name "$VG"_'*.vg' | wc -l`,
		strconv.Quote(nlvmArchiveDir), strconv.Quote(vgName), count)

	out, err := framework.NodeExecChecked(ctx, cl, node, script)
	Expect(err).NotTo(HaveOccurred(), "failed to plant archive entries for %s on %s:\n%s", vgName, node, out)

	planted, convErr := strconv.Atoi(strings.TrimSpace(nlvmLastLine(out)))
	Expect(convErr).NotTo(HaveOccurred(), "unexpected output planting archive entries:\n%s", out)
	GinkgoWriter.Printf("Archive of %s on %s now holds %d entries\n", vgName, node, planted)
	return planted
}

// nlvmRemovePlantedArchiveEntries removes only the entries this suite planted. The
// pattern is the one nlvmPlantArchiveEntries writes, so lvm's own history survives.
func nlvmRemovePlantedArchiveEntries(ctx context.Context, cl *e2e.Cluster, node, vgName string) {
	out, err := framework.NodeExecChecked(ctx, cl, node,
		fmt.Sprintf(`sudo -n find %s -maxdepth 1 -name %s -delete; sudo -n find %s -maxdepth 1 -type f | wc -l`,
			strconv.Quote(nlvmArchiveDir), strconv.Quote(vgName+"_9*-0000000000.vg"), strconv.Quote(nlvmArchiveDir)))
	if err != nil {
		GinkgoWriter.Printf("failed to remove planted archive entries for %s on %s: %v\n%s\n", vgName, node, err, out)
		return
	}
	GinkgoWriter.Printf("Archive on %s holds %s entries after cleanup\n", node, strings.TrimSpace(nlvmLastLine(out)))
}

// nlvmExpectNoReportParseFailures fails if the agent could not parse an LVM report
// while the spec was running.
//
// This is the log line the outage produced hundreds of times a minute, and it is
// worth asserting on directly: the LVMVolumeGroup phase only shows the consequence
// once the cache has been empty long enough for a reconcile to notice.
func nlvmExpectNoReportParseFailures(ctx context.Context, cl *e2e.Cluster, node string, since time.Time) {
	GinkgoHelper()

	pods, err := cl.Clientset().CoreV1().Pods(consts.SdsNodeConfiguratorAgentNamespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app=" + consts.SdsNodeConfiguratorAgentName,
		FieldSelector: "spec.nodeName=" + node,
	})
	Expect(err).NotTo(HaveOccurred(), "failed to list agent pods on %s", node)
	Expect(pods.Items).NotTo(BeEmpty(), "no agent pod on %s", node)

	sinceTime := metav1.NewTime(since)
	for i := range pods.Items {
		name := pods.Items[i].Name
		raw, logErr := cl.Clientset().CoreV1().Pods(consts.SdsNodeConfiguratorAgentNamespace).
			GetLogs(name, &corev1.PodLogOptions{
				Container: consts.SdsNodeConfiguratorAgentContainer,
				SinceTime: &sinceTime,
			}).DoRaw(ctx)
		Expect(logErr).NotTo(HaveOccurred(), "failed to read the log of %s", name)

		logs := string(raw)
		for _, marker := range []string{
			// json.Unmarshal on something that is not JSON — the advisory landing in
			// the report.
			"looking for beginning of value",
			// GetAllVGs/GetAllPVs/GetAllLVs giving up, whatever the reason.
			"unable to GetAllVGs",
			"unable to GetAllPVs",
			"unable to GetAllLVs",
		} {
			Expect(logs).NotTo(ContainSubstring(marker),
				"the agent on %s failed to read the node's LVM state (%q); log:\n%s", node, marker, nlvmTail(logs, 60))
		}
	}
}

// nlvmHasNonJSONLine reports whether lvm mixed a non-JSON line into its report.
// Every line of an lvm JSON report starts with a structural character or a quoted
// key, so a line starting with a letter is one of lvm's own advisories.
func nlvmHasNonJSONLine(out string) bool {
	return nlvmNonJSONLines(out) != ""
}

func nlvmNonJSONLines(out string) string {
	var found []string
	for _, line := range strings.Split(out, "\n") {
		trimmed := strings.TrimLeft(line, " \t")
		if trimmed == "" {
			continue
		}
		if c := trimmed[0]; (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') {
			found = append(found, line)
		}
	}
	return strings.Join(found, "\n")
}

func nlvmLastLine(out string) string {
	lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
	return lines[len(lines)-1]
}

func nlvmHead(out string, n int) string {
	if len(out) <= n {
		return out
	}
	return out[:n] + "..."
}

func nlvmTail(out string, lines int) string {
	split := strings.Split(strings.TrimRight(out, "\n"), "\n")
	if len(split) <= lines {
		return out
	}
	return strings.Join(split[len(split)-lines:], "\n")
}
