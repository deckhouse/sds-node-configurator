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
	"path/filepath"
	"strconv"
	"time"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/e2e/framework"
	"github.com/deckhouse/sds-node-configurator/e2e/sdsclient"
	"github.com/deckhouse/storage-e2e/pkg/e2e"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// States a node can be left in that the happy path never produces, and where getting
// the recovery wrong costs storage rather than a condition: a backing file removed
// while its loop is still attached, a Physical Volume left behind by an interrupted
// create, a directory reached through a symlink.
var _ = Describe("LVMVolumeGroup file-backed devices recovery from node-side states",
	Label("sds-node-configurator", "lvmvolumegroup", "file-devices"), Ordered, ContinueOnFailure, func() {
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
			cl, clErr = e2e.Connect(ctx, e2e.WithTestName("lvmvolumegroup-filedevices-recovery"))
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

		// `losetup -j <path>` matches by inode, so an unlinked backing file looks
		// unattached while its loop carries on serving a live Physical Volume. Taken for
		// "this entry has nothing on the node", provisioning creates a fresh file at the
		// same path — a different inode, which `--nooverlap` cannot match — and vgextend
		// then puts a second Physical Volume of the same size into the Volume Group.
		It("Should refuse to re-provision an entry whose backing file was unlinked, and not double the VG", func() {
			lvgName := fdLVGName("fdunlink", runID, targetNode)
			vgName := "e2e-vg-fdunlink-" + runID

			By(fmt.Sprintf("Creating a file-backed LVMVolumeGroup %s (VG %s)", lvgName, vgName))
			lvg := fdNewLVG(targetNode, lvgName, vgName, nil,
				[]v1alpha1.LVMVolumeGroupFileDeviceSpec{{Name: "d1g", Directory: fdBaseDir, Size: resource.MustParse("1Gi")}}, nil)
			deleted := false
			DeferCleanup(func() {
				if !deleted {
					fdDeleteLVGAndWaitGone(ctx, k8sClient, lvgName)
				}
			})
			created := fdCreateLVGAndWaitReady(ctx, cl, k8sClient, lvg)
			fdExpectNoFalseConditions(created)

			fds := fdFileDevicesForNode(created, targetNode)
			Expect(fds).To(HaveLen(1))
			backingFile, loop := fds[0].FilePath, fds[0].LoopDevice
			Expect(loop).To(HavePrefix("/dev/loop"))

			pvsBefore, pvsOut, err := fdPVNamesInVGOnNode(ctx, cl, targetNode, vgName)
			Expect(err).NotTo(HaveOccurred())
			Expect(pvsBefore).To(HaveLen(1), "the VG must start with exactly one PV; pvs:\n%s", pvsOut)

			By(fmt.Sprintf("Removing %s on the node while %s stays attached to it", backingFile, loop))
			fdUnlinkBackingFileOnNode(ctx, cl, targetNode, backingFile)

			// Nothing on the node produces an event for the reconciler, and the
			// discoverer sees no status change either — the deleted marker is stripped
			// before the comparison — so the round has to be asked for.
			By("Nudging the reconciler so the state is observed")
			fdTriggerLVGReconcile(ctx, k8sClient, lvgName)

			By("Verifying the agent reports the entry instead of re-provisioning it")
			fdWaitVGConfigurationRejected(ctx, k8sClient, lvgName, fdReasonFileDeviceNotApplied, "unlinked")

			// The report is the whole point: FileDeviceNotApplied is in the conditions
			// watcher's acceptableReasons, so the Physical Volume keeps serving whatever
			// is on it while an operator decides what to do.
			By("Verifying the Volume Group keeps working and does not gain a second PV")
			Consistently(func(g Gomega) {
				var cur v1alpha1.LVMVolumeGroup
				g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: lvgName}, &cur)).To(Succeed())
				g.Expect(cur.Status.Phase).To(Equal(v1alpha1.PhaseReady),
					"an entry that cannot be brought up must not take the Volume Group out of service")

				pvNames, out, pvErr := fdPVNamesInVGOnNode(ctx, cl, targetNode, vgName)
				g.Expect(pvErr).NotTo(HaveOccurred())
				g.Expect(pvNames).To(HaveLen(1),
					"the VG doubled: a second backing file was created at the same path; pvs:\n%s", out)

				// Same statement from the other side, and the one that actually counts
				// the doubling: `losetup -j` matches by inode and would report nothing
				// for the deleted file, so the basename is what has to be counted —
				// the old loop reads it through a deleted inode, a re-provisioned one
				// would read it through a fresh inode, and both name it.
				loops, losetupOut, loopErr := fdLoopsReportingBasename(ctx, cl, targetNode, filepath.Base(backingFile))
				g.Expect(loopErr).NotTo(HaveOccurred())
				g.Expect(loops).To(HaveLen(1),
					"a second loop appeared for %s; losetup -a:\n%s", backingFile, losetupOut)
			}, 2*time.Minute, 15*time.Second).Should(Succeed())

			By("Verifying the message says how to get out of it")
			var cur v1alpha1.LVMVolumeGroup
			Expect(k8sClient.Get(ctx, client.ObjectKey{Name: lvgName}, &cur)).To(Succeed())
			for i := range cur.Status.Conditions {
				if cur.Status.Conditions[i].Type == fdConditionVGConfigurationApplied {
					Expect(cur.Status.Conditions[i].Message).To(ContainSubstring("pvmove"),
						"the operator has to be told the recovery, since the file cannot come back on its own")
				}
			}

			// Deleting the resource must still take the loop down. Resolving the loop
			// from the file finds nothing now, so without the fallback to the loop
			// recorded in status the minor would outlive the only record of it.
			By("Deleting the LVMVolumeGroup and verifying the stranded loop is detached")
			fdDeleteLVGAndWaitGone(ctx, k8sClient, lvgName)
			deleted = true

			Eventually(func(g Gomega) {
				out, execErr := framework.NodeExecChecked(ctx, cl, targetNode,
					fmt.Sprintf(`sudo -n losetup -a | grep -F %s || true`, strconv.Quote(backingFile)))
				g.Expect(execErr).NotTo(HaveOccurred())
				g.Expect(out).To(BeEmpty(), "the loop for the unlinked %s was left on the node:\n%s", backingFile, out)
			}, fdCleanupTimeout, 10*time.Second).Should(Succeed())

			By("✓ An unlinked backing file is reported, never re-provisioned, and its loop is collected on delete")
		})

		// spec.fileDevices[].directory reached through a symlink is the natural way to
		// point the default location at a data disk. It is also where the two spellings of
		// one path part company: status carries the path losetup reported, with symlink
		// components resolved, while the spec keeps the literal directory. Counting them
		// as two devices inflated the VG size used for thin-pool validation, and walking
		// them as two cleanup targets tried to remove the same file twice.
		It("Should handle a fileDevices directory reached through a symlink", func() {
			realDir := fdBaseDir + "/e2e-real-" + runID
			linkDir := fdBaseDir + "/e2e-link-" + runID

			By(fmt.Sprintf("Preparing %s as a symlink to %s on the node", linkDir, realDir))
			fdSymlinkedDirOnNode(ctx, cl, targetNode, realDir, linkDir)
			DeferCleanup(func() { fdRemoveSymlinkedDirOnNode(ctx, cl, targetNode, realDir, linkDir) })

			lvgName := fdLVGName("fdsymlink", runID, targetNode)
			vgName := "e2e-vg-fdsymlink-" + runID

			By(fmt.Sprintf("Creating LVMVolumeGroup %s with directory %s and a thin pool over it", lvgName, linkDir))
			lvg := fdNewLVG(targetNode, lvgName, vgName, nil,
				[]v1alpha1.LVMVolumeGroupFileDeviceSpec{{Name: "d1", Directory: linkDir, Size: resource.MustParse(fdFileDeviceSize)}},
				[]v1alpha1.LVMVolumeGroupThinPoolSpec{{Name: "tp", Size: fdThinPoolSize, AllocationLimit: "100%"}})
			deleted := false
			DeferCleanup(func() {
				if !deleted {
					// The thin pool has to come down first, or the delete waits on it.
					fdRunThinPoolTeardown(ctx, cl, targetNode,
						framework.RemoveThinPoolStackScriptWithLVMConfig(vgName, "tp", fdLVMConfig))
					fdDeleteLVGAndWaitGone(ctx, k8sClient, lvgName)
				}
			})
			created := fdCreateLVGAndWaitReady(ctx, cl, k8sClient, lvg)
			fdExpectNoFalseConditions(created)
			fdPrintLVG(created)

			By("Verifying exactly one file device is reported, under its resolved path")
			fds := fdFileDevicesForNode(created, targetNode)
			Expect(fds).To(HaveLen(1),
				"the same file counted twice — once per spelling of its directory: %+v", fds)
			backingFile := fds[0].FilePath
			Expect(filepath.Base(backingFile)).To(Equal(fdBackingFileName(lvgName, "d1")))

			By("Verifying the VG spans exactly one PV")
			pvNames, pvsOut, err := fdPVNamesInVGOnNode(ctx, cl, targetNode, vgName)
			Expect(err).NotTo(HaveOccurred())
			Expect(pvNames).To(HaveLen(1), "one entry must yield one PV; pvs:\n%s", pvsOut)

			By("Verifying the thin pool was created — its size is validated against the VG size")
			present, lvsOut, err := fdThinPoolDataLVPresentOnNode(ctx, cl, targetNode, vgName, "tp")
			Expect(err).NotTo(HaveOccurred())
			Expect(present).To(BeTrue(), "thin pool tp missing; lvs:\n%s", lvsOut)

			// The condition has to stay applied across several reconciles: counting the
			// device twice inflated the VG size on every pass, which showed up as the
			// thin-pool validation flapping rather than as an outright failure.
			By("Verifying the configuration stays applied instead of flapping")
			Consistently(func(g Gomega) {
				var cur v1alpha1.LVMVolumeGroup
				g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: lvgName}, &cur)).To(Succeed())
				g.Expect(cur.Status.Phase).To(Equal(v1alpha1.PhaseReady))
				g.Expect(fdFileDevicesForNode(&cur, targetNode)).To(HaveLen(1))
			}, 90*time.Second, 15*time.Second).Should(Succeed())

			By("Deleting the LVMVolumeGroup and verifying the backing file is gone through either spelling")
			fdRunThinPoolTeardown(ctx, cl, targetNode,
				framework.RemoveThinPoolStackScriptWithLVMConfig(vgName, "tp", fdLVMConfig))
			fdDeleteLVGAndWaitGone(ctx, k8sClient, lvgName)
			deleted = true

			fdExpectBackingFileGone(ctx, cl, targetNode, backingFile)
			fdExpectBackingFileGone(ctx, cl, targetNode, linkDir+"/"+fdBackingFileName(lvgName, "d1"))

			By("✓ A symlinked directory yields one device, one PV and a clean teardown")
		})

		// An interrupted create leaves a loop that is already a Physical Volume and no
		// Volume Group membership — and no udev event, so the agent's cache is the one
		// place that PV is missing. Handing it to pvcreate again fails "already a PV",
		// which the caller reports as VGExtendFailed: a fatal reason, so a Volume Group
		// that was serving every volume it had went NotReady with nothing to bring it back.
		It("Should extend a VG with a loop that is already a PV", func() {
			lvgName := fdLVGName("fdprepv", runID, targetNode)
			vgName := "e2e-vg-fdprepv-" + runID

			By(fmt.Sprintf("Creating a file-backed LVMVolumeGroup %s with one entry", lvgName))
			lvg := fdNewLVG(targetNode, lvgName, vgName, nil,
				[]v1alpha1.LVMVolumeGroupFileDeviceSpec{{Name: "d1", Directory: fdBaseDir, Size: resource.MustParse("1Gi")}}, nil)
			deleted := false
			DeferCleanup(func() {
				if !deleted {
					fdDeleteLVGAndWaitGone(ctx, k8sClient, lvgName)
				}
			})
			created := fdCreateLVGAndWaitReady(ctx, cl, k8sClient, lvg)
			fdExpectNoFalseConditions(created)

			// Exactly what a create interrupted between pvcreate and vgextend leaves: the
			// backing file the agent would build for entry d2, attached, pvcreated, and
			// in no Volume Group. Built under the agent's own naming so it recognises it
			// as its own and reuses the loop instead of allocating a second one.
			secondFile := fdBaseDir + "/" + fdBackingFileName(lvgName, "d2")
			By(fmt.Sprintf("Leaving a half-provisioned PV behind for entry d2 (%s)", secondFile))
			out, err := framework.NodeExecChecked(ctx, cl, targetNode, fmt.Sprintf(`set -e
sudo -n fallocate -l 1G %s
LOOP=$(sudo -n losetup --find --nooverlap --show %s)
sudo -n lvm pvcreate %s -y -ff "$LOOP" >/dev/null
echo "$LOOP"`, strconv.Quote(secondFile), strconv.Quote(secondFile), fdLVMCfg))
			Expect(err).NotTo(HaveOccurred(), "failed to stage a half-provisioned PV:\n%s", out)

			By("Appending entry d2 to the spec")
			Eventually(func(g Gomega) {
				var cur v1alpha1.LVMVolumeGroup
				g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: lvgName}, &cur)).To(Succeed())
				cur.Spec.FileDevices = append(cur.Spec.FileDevices,
					v1alpha1.LVMVolumeGroupFileDeviceSpec{Name: "d2", Directory: fdBaseDir, Size: resource.MustParse("1Gi")})
				g.Expect(k8sClient.Update(ctx, &cur)).To(Succeed())
			}, time.Minute, 5*time.Second).Should(Succeed())

			By("Verifying the agent skips the redundant pvcreate and extends the VG")
			fdWaitVGConfigurationOK(ctx, k8sClient, lvgName)

			Eventually(func(g Gomega) {
				pvNames, pvsOut, pvErr := fdPVNamesInVGOnNode(ctx, cl, targetNode, vgName)
				g.Expect(pvErr).NotTo(HaveOccurred())
				g.Expect(pvNames).To(HaveLen(2),
					"the staged PV must have joined the VG rather than wedging the reconcile; pvs:\n%s", pvsOut)
			}, fdLVGReadyTimeout, 10*time.Second).Should(Succeed())

			// status.nodes[].fileDevices is written by the discoverer, not by the
			// reconciler that ran vgextend, so it lags the node by up to a scan
			// interval. The PV check above passes the moment vgextend returns, which
			// is why this has to be an Eventually rather than a read taken right
			// after it — a one-shot Get here caught the status from before the
			// entry joined and failed on a Volume Group that was already correct.
			By("Verifying the second file device is reported in the status")
			var cur v1alpha1.LVMVolumeGroup
			Eventually(func(g Gomega) {
				g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: lvgName}, &cur)).To(Succeed())
				fdPrintLVG(&cur)
				g.Expect(fdFileDevicesForNode(&cur, targetNode)).To(HaveLen(2))
			}, fdLVGReadyTimeout, 10*time.Second).Should(Succeed())

			// Checked after the status has settled: a second loop would be allocated
			// by a later provisioning round, so the longer this waits the more of a
			// chance the duplicate has to appear.
			By("Verifying the staged backing file was reused, not duplicated")
			loops, losetupOut, err := fdLoopsForFile(ctx, cl, targetNode, secondFile)
			Expect(err).NotTo(HaveOccurred())
			Expect(loops).To(HaveLen(1), "the agent allocated a second loop for %s; losetup:\n%s", secondFile, losetupOut)

			By("Deleting the LVMVolumeGroup and verifying both backing files are gone")
			fdDeleteLVGAndWaitGone(ctx, k8sClient, lvgName)
			deleted = true
			fdExpectBackingFileGone(ctx, cl, targetNode, secondFile)

			By("✓ A loop that was already a PV joins the VG instead of wedging it")
		})
	})
