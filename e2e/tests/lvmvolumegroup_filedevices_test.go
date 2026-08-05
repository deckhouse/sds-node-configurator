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
	"strings"
	"time"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/e2e/cfg"
	"github.com/deckhouse/sds-node-configurator/e2e/framework"
	"github.com/deckhouse/sds-node-configurator/e2e/sdsclient"
	"github.com/deckhouse/storage-e2e/pkg/e2e"
	"github.com/deckhouse/storage-e2e/pkg/kubernetes"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// LVMVolumeGroups backed by regular files instead of block devices (spec.fileDevices).
// Except for the mixed block+file spec, none of these need a spare disk: the agent
// fallocates a backing file under the module's fileDevicesDirectory, attaches it as a
// loop device and pvcreates that loop.
var _ = Describe("LVMVolumeGroup on file-backed devices",
	Label("sds-node-configurator", "lvmvolumegroup", "file-devices"), Ordered, ContinueOnFailure, func() {
		var (
			ctx        context.Context
			conf       *cfg.Config
			cl         *e2e.Cluster
			k8sClient  client.Client
			targetNode string
			runID      string
		)

		BeforeAll(func() {
			By("Preparing shared test context and Kubernetes clients")
			ctx = context.Background()

			var cfgErr error
			conf, cfgErr = cfg.Load()
			Expect(cfgErr).NotTo(HaveOccurred(), "failed to load config")

			var clErr error
			cl, clErr = e2e.Connect(ctx, e2e.WithTestName("lvmvolumegroup-filedevices"))
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
			GinkgoWriter.Printf("File-backed specs run on node %s (base dir %s)\n", targetNode, fdBaseDir)

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

		It("Should create a file-backed LVMVolumeGroup and remove the backing file on delete", func() {
			lvgName := fdLVGName("fd", runID, targetNode)
			vgName := "e2e-vg-fd-" + runID

			By(fmt.Sprintf("Creating file-backed LVMVolumeGroup %s on node %s (dir %s)", lvgName, targetNode, fdBaseDir))
			lvg := fdNewLVG(targetNode, lvgName, vgName, nil,
				[]v1alpha1.LVMVolumeGroupFileDeviceSpec{{Name: "d1g", Directory: fdBaseDir, Size: resource.MustParse("1Gi")}}, nil)

			deleted := false
			DeferCleanup(func() {
				if !deleted {
					fdDeleteLVGAndWaitGone(ctx, k8sClient, lvgName)
				}
			})
			created := fdCreateLVGAndWaitReady(ctx, cl, k8sClient, lvg)

			By("Verifying conditions (no errors)")
			fdExpectNoFalseConditions(created)

			By("Verifying status.nodes[].fileDevices")
			fds := fdFileDevicesForNode(created, targetNode)
			Expect(fds).To(HaveLen(1), "expected exactly one file device in status for node %s", targetNode)
			fd := fds[0]
			Expect(fd.FilePath).To(HavePrefix(fdBaseDir+"/"), "backing file must live under the base dir")
			Expect(fd.LoopDevice).To(HavePrefix("/dev/loop"), "loop device path")
			Expect(fd.PVUuid).NotTo(BeEmpty(), "file device should be a PV")
			fdPrintLVG(created)

			By("Verifying backing file, loop attachment and VG exist on the node")
			exists, err := fdNodePathExists(ctx, cl, targetNode, fd.FilePath)
			Expect(err).NotTo(HaveOccurred())
			Expect(exists).To(BeTrue(), "backing file %s should exist on node", fd.FilePath)

			loops, loopOut, err := fdLoopsForFile(ctx, cl, targetNode, fd.FilePath)
			Expect(err).NotTo(HaveOccurred())
			Expect(loops).To(HaveLen(1), "exactly one loop device should be attached to %s; losetup:\n%s", fd.FilePath, loopOut)
			Expect(loops[0]).To(Equal(fd.LoopDevice), "status.loopDevice should match the actual attachment")

			vgListed, vgsOut, err := fdVGListedOnNode(ctx, cl, targetNode, vgName)
			Expect(err).NotTo(HaveOccurred())
			Expect(vgListed).To(BeTrue(), "VG %s should be visible on node; vgs:\n%s", vgName, vgsOut)

			By("Verifying direct I/O was enabled on the loop device")
			dio, dioOut, err := fdDirectIOOnLoop(ctx, cl, targetNode, fd.LoopDevice)
			Expect(err).NotTo(HaveOccurred())
			Expect(dio).To(BeTrue(),
				"loop %s should have DIO=1: the agent requests direct I/O as a separate best-effort step after attaching, "+
					"and the backing file is on the node's root filesystem, which supports it; losetup -O DIO: %q", fd.LoopDevice, dioOut)

			By("Verifying the VG carries the ownership tag the loop-PV discovery relies on")
			tags, err := fdVGTagsOnNode(ctx, cl, targetNode, vgName)
			Expect(err).NotTo(HaveOccurred())
			Expect(tags).To(ContainSubstring("storage.deckhouse.io/lvmVolumeGroupName="+lvgName),
				"VG %s should be tagged with its LVMVolumeGroup name; vg_tags=%q", vgName, tags)
			Expect(tags).To(ContainSubstring("storage.deckhouse.io/enabled=true"),
				"VG %s should be tagged as managed; vg_tags=%q", vgName, tags)

			By("Deleting the LVMVolumeGroup and verifying node-side cleanup")
			fdDeleteLVGAndWaitGone(ctx, k8sClient, lvgName)
			deleted = true
			fdExpectBackingFileGone(ctx, cl, targetNode, fd.FilePath)

			By("✓ File-backed VG created (file+loop+PV+VG), then fully cleaned up on delete")
		})

		It("Should create a thin-pool on a file-backed device", func() {
			lvgName := fdLVGName("fdtp", runID, targetNode)
			vgName := "e2e-vg-fdtp-" + runID
			thinPoolName := "e2e-thin-fd"

			By(fmt.Sprintf("Creating file-backed LVMVolumeGroup %s with thin-pool %s on node %s", lvgName, thinPoolName, targetNode))
			lvg := fdNewLVG(targetNode, lvgName, vgName, nil,
				[]v1alpha1.LVMVolumeGroupFileDeviceSpec{{Name: "d0", Directory: fdBaseDir, Size: resource.MustParse(fdFileDeviceSize)}},
				[]v1alpha1.LVMVolumeGroupThinPoolSpec{{Name: thinPoolName, Size: fdThinPoolSize, AllocationLimit: "100%"}})

			DeferCleanup(func() {
				fdRunThinPoolTeardown(ctx, cl, targetNode, framework.RemoveThinPoolStackScriptWithLVMConfig(vgName, thinPoolName, fdLVMConfig))
				fdDeleteLVGAndWaitGone(ctx, k8sClient, lvgName)
			})
			created := fdCreateLVGAndWaitReady(ctx, cl, k8sClient, lvg)
			fdExpectNoFalseConditions(created)
			fdPrintLVG(created)

			By("Verifying the thin-pool is Ready in status")
			var tp *v1alpha1.LVMVolumeGroupThinPoolStatus
			for i := range created.Status.ThinPools {
				if created.Status.ThinPools[i].Name == thinPoolName {
					tp = &created.Status.ThinPools[i]
					break
				}
			}
			Expect(tp).NotTo(BeNil(), "thin-pool %q not found in status", thinPoolName)
			Expect(tp.Ready).To(BeTrue(), "thin-pool should be Ready: %s", tp.Message)

			By("Verifying the thin-pool data LV exists on the node")
			present, lvsOut, err := fdThinPoolDataLVPresentOnNode(ctx, cl, targetNode, vgName, thinPoolName)
			Expect(err).NotTo(HaveOccurred())
			Expect(present).To(BeTrue(), "thin-pool data LV for %s/%s should exist on node; lvs:\n%s", vgName, thinPoolName, lvsOut)

			By("✓ Thin-pool created on a file-backed VG and reported Ready")
		})

		It("Should extend a file-backed VG by adding a fileDevices entry", func() {
			lvgName := fdLVGName("fdext", runID, targetNode)
			vgName := "e2e-vg-fdext-" + runID

			By(fmt.Sprintf("Creating file-backed LVMVolumeGroup %s with one file device on node %s", lvgName, targetNode))
			lvg := fdNewLVG(targetNode, lvgName, vgName, nil,
				[]v1alpha1.LVMVolumeGroupFileDeviceSpec{{Name: "d1g", Directory: fdBaseDir, Size: resource.MustParse("1Gi")}}, nil)

			DeferCleanup(func() { fdDeleteLVGAndWaitGone(ctx, k8sClient, lvgName) })
			created := fdCreateLVGAndWaitReady(ctx, cl, k8sClient, lvg)
			Expect(fdFileDevicesForNode(created, targetNode)).To(HaveLen(1))
			vgSizeBefore := created.Status.VGSize.Value()

			By("Appending a second fileDevices entry under a new name")
			Eventually(func(g Gomega) {
				var cur v1alpha1.LVMVolumeGroup
				g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: lvgName}, &cur)).To(Succeed())
				// The name is what distinguishes the entries, so the same size in the
				// same directory would work just as well — see the spec below.
				cur.Spec.FileDevices = append(cur.Spec.FileDevices,
					v1alpha1.LVMVolumeGroupFileDeviceSpec{Name: "d2g", Directory: fdBaseDir, Size: resource.MustParse("2Gi")})
				g.Expect(k8sClient.Update(ctx, &cur)).To(Succeed())
			}, 1*time.Minute, 5*time.Second).Should(Succeed())

			By("Waiting for the VG to grow with the second file device")
			var extended v1alpha1.LVMVolumeGroup
			Eventually(func(g Gomega) {
				g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: lvgName}, &extended)).To(Succeed())
				fdPrintLVG(&extended)
				g.Expect(extended.Status.Phase).To(Equal(v1alpha1.PhaseReady), "phase should return to Ready after extend")
				g.Expect(fdFileDevicesForNode(&extended, targetNode)).To(HaveLen(2), "status should list two file devices")
				g.Expect(extended.Status.VGSize.Value()).To(BeNumerically(">", vgSizeBefore),
					"VG size should grow after extend (baseline %d)", vgSizeBefore)
			}, fdLVGReadyTimeout, 10*time.Second).Should(Succeed())
			fdExpectNoFalseConditions(&extended)

			By("Verifying the VG spans two loop PVs on the node")
			pvNames, pvsOut, err := fdPVNamesInVGOnNode(ctx, cl, targetNode, vgName)
			Expect(err).NotTo(HaveOccurred())
			Expect(pvNames).To(HaveLen(2), "extended VG should have 2 PVs; pvs:\n%s", pvsOut)
			for _, pv := range pvNames {
				Expect(pv).To(HavePrefix("/dev/loop"), "every PV of a file-only VG should be a loop device; pvs:\n%s", pvsOut)
			}

			By("✓ File-backed VG extended by adding a new fileDevices entry")
		})

		// Growth in equal steps is the obvious thing to ask for and it used to be
		// impossible: with the entry keyed by (directory, size), a second 1Gi entry in
		// the same directory mapped to the same backing file and was rejected as a
		// collision. Keying by name is what makes this expressible.
		It("Should provision two equally-sized file devices in the same directory", func() {
			lvgName := fdLVGName("fdsame", runID, targetNode)
			vgName := "e2e-vg-fdsame-" + runID

			By(fmt.Sprintf("Creating LVMVolumeGroup %s with two 1Gi entries in %s", lvgName, fdBaseDir))
			lvg := fdNewLVG(targetNode, lvgName, vgName, nil,
				[]v1alpha1.LVMVolumeGroupFileDeviceSpec{
					{Name: "part-a", Directory: fdBaseDir, Size: resource.MustParse("1Gi")},
					{Name: "part-b", Directory: fdBaseDir, Size: resource.MustParse("1Gi")},
				}, nil)
			DeferCleanup(func() { fdDeleteLVGAndWaitGone(ctx, k8sClient, lvgName) })

			created := fdCreateLVGAndWaitReady(ctx, cl, k8sClient, lvg)
			fdExpectNoFalseConditions(created)
			fdPrintLVG(created)

			By("Verifying both entries produced their own backing file, loop and PV")
			fds := fdFileDevicesForNode(created, targetNode)
			Expect(fds).To(HaveLen(2), "both equally-sized entries must be provisioned")

			names := make([]string, 0, len(fds))
			paths := make(map[string]struct{}, len(fds))
			loops := make(map[string]struct{}, len(fds))
			for _, fd := range fds {
				names = append(names, fd.Name)
				paths[fd.FilePath] = struct{}{}
				loops[fd.LoopDevice] = struct{}{}
			}
			Expect(names).To(ConsistOf("part-a", "part-b"), "status must attribute each device to its spec entry")
			Expect(paths).To(HaveLen(2), "the two entries must not share a backing file: %v", paths)
			Expect(loops).To(HaveLen(2), "the two entries must not share a loop device: %v", loops)

			pvNames, pvsOut, err := fdPVNamesInVGOnNode(ctx, cl, targetNode, vgName)
			Expect(err).NotTo(HaveOccurred())
			Expect(pvNames).To(HaveLen(2), "the VG should span two loop PVs; pvs:\n%s", pvsOut)

			By("✓ Two equally-sized file devices coexist in one directory")
		})

		// Growing an entry in place, as opposed to appending a new one. The whole
		// sequence — fallocate, losetup -c, pvresize — runs on a live device, so
		// what has to be true afterwards is not just "the VG is bigger" but "it is
		// bigger through the same file, the same loop and the same PV".
		It("Should grow a file device in place when its size is raised", func() {
			lvgName := fdLVGName("fdgrow", runID, targetNode)
			vgName := "e2e-vg-fdgrow-" + runID

			By(fmt.Sprintf("Creating a 1Gi file-backed LVMVolumeGroup %s on node %s", lvgName, targetNode))
			lvg := fdNewLVG(targetNode, lvgName, vgName, nil,
				[]v1alpha1.LVMVolumeGroupFileDeviceSpec{
					{Name: "data-0", Directory: fdBaseDir, Size: resource.MustParse("1Gi")},
				}, nil)
			DeferCleanup(func() { fdDeleteLVGAndWaitGone(ctx, k8sClient, lvgName) })

			created := fdCreateLVGAndWaitReady(ctx, cl, k8sClient, lvg)
			before := fdFileDevicesForNode(created, targetNode)[0]
			vgSizeBefore := created.Status.VGSize.Value()

			By("Raising the entry size from 1Gi to 3Gi")
			Eventually(func(g Gomega) {
				var cur v1alpha1.LVMVolumeGroup
				g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: lvgName}, &cur)).To(Succeed())
				cur.Spec.FileDevices[0].Size = resource.MustParse("3Gi")
				g.Expect(k8sClient.Update(ctx, &cur)).To(Succeed())
			}, 1*time.Minute, 5*time.Second).Should(Succeed(),
				"raising the size of an existing entry must be accepted")

			By("Waiting for the VG to grow through the same device")
			var grown v1alpha1.LVMVolumeGroup
			Eventually(func(g Gomega) {
				g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: lvgName}, &grown)).To(Succeed())
				fdPrintLVG(&grown)
				g.Expect(grown.Status.Phase).To(Equal(v1alpha1.PhaseReady))

				fds := fdFileDevicesForNode(&grown, targetNode)
				g.Expect(fds).To(HaveLen(1), "growth must not add a second file device")
				g.Expect(fds[0].Size.Value()).To(BeNumerically(">", before.Size.Value()),
					"the PV should have grown (was %s)", before.Size.String())
				g.Expect(grown.Status.VGSize.Value()).To(BeNumerically(">", vgSizeBefore))
			}, fdLVGReadyTimeout, 10*time.Second).Should(Succeed())
			fdExpectNoFalseConditions(&grown)

			By("Verifying it is the same backing file, loop device and PV")
			after := fdFileDevicesForNode(&grown, targetNode)[0]
			Expect(after.FilePath).To(Equal(before.FilePath), "the backing file must not be renamed by a resize")
			Expect(after.LoopDevice).To(Equal(before.LoopDevice), "the loop device must be refreshed, not re-attached")
			Expect(after.PVUuid).To(Equal(before.PVUuid), "the PV must be resized, not recreated")

			files, err := fdBackingFilesForLVG(ctx, cl, targetNode, lvgName)
			Expect(err).NotTo(HaveOccurred())
			Expect(files).To(HaveLen(1), "growth must not leave a second backing file: %v", files)

			pvNames, pvsOut, err := fdPVNamesInVGOnNode(ctx, cl, targetNode, vgName)
			Expect(err).NotTo(HaveOccurred())
			Expect(pvNames).To(HaveLen(1), "the VG must still span exactly one PV; pvs:\n%s", pvsOut)

			By("Verifying the reconcile settles instead of growing forever")
			// A PV is always slightly smaller than its backing file, so a naive
			// size comparison would make every reconcile try to grow again.
			Consistently(func(g Gomega) {
				var stable v1alpha1.LVMVolumeGroup
				g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: lvgName}, &stable)).To(Succeed())
				g.Expect(stable.Status.Phase).To(Equal(v1alpha1.PhaseReady))
			}, 1*time.Minute, 10*time.Second).Should(Succeed())

			By("✓ File device grown in place: same file, same loop, same PV, larger VG")
		})

		// The reason to grow in place at all: the capacity has to reach a thin pool
		// that is sized as a share of the VG.
		It("Should extend a 100% thin pool after a file device grows", func() {
			lvgName := fdLVGName("fdgrowtp", runID, targetNode)
			vgName := "e2e-vg-fdgrowtp-" + runID
			const poolName = "e2e-thin-fdgrow"

			By(fmt.Sprintf("Creating a 2Gi file-backed LVMVolumeGroup %s with a 100%% thin pool", lvgName))
			lvg := fdNewLVG(targetNode, lvgName, vgName, nil,
				[]v1alpha1.LVMVolumeGroupFileDeviceSpec{
					{Name: "data-0", Directory: fdBaseDir, Size: resource.MustParse("2Gi")},
				},
				[]v1alpha1.LVMVolumeGroupThinPoolSpec{{Name: poolName, Size: "100%", AllocationLimit: "150%"}})
			DeferCleanup(func() {
				fdRunThinPoolTeardown(ctx, cl, targetNode,
					framework.RemoveThinPoolStackScriptWithLVMConfig(vgName, poolName, fdLVMConfig))
				fdDeleteLVGAndWaitGone(ctx, k8sClient, lvgName)
			})

			created := fdCreateLVGAndWaitReady(ctx, cl, k8sClient, lvg)
			var poolSizeBefore int64
			for _, tp := range created.Status.ThinPools {
				if tp.Name == poolName {
					poolSizeBefore = tp.ActualSize.Value()
				}
			}
			Expect(poolSizeBefore).To(BeNumerically(">", 0), "the thin pool should be created")

			By("Raising the entry size from 2Gi to 4Gi")
			Eventually(func(g Gomega) {
				var cur v1alpha1.LVMVolumeGroup
				g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: lvgName}, &cur)).To(Succeed())
				cur.Spec.FileDevices[0].Size = resource.MustParse("4Gi")
				g.Expect(k8sClient.Update(ctx, &cur)).To(Succeed())
			}, 1*time.Minute, 5*time.Second).Should(Succeed())

			By("Waiting for the thin pool to follow the VG")
			var grown v1alpha1.LVMVolumeGroup
			Eventually(func(g Gomega) {
				g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: lvgName}, &grown)).To(Succeed())
				fdPrintLVG(&grown)
				g.Expect(grown.Status.Phase).To(Equal(v1alpha1.PhaseReady))

				var poolSizeAfter int64
				for _, tp := range grown.Status.ThinPools {
					if tp.Name == poolName {
						poolSizeAfter = tp.ActualSize.Value()
					}
				}
				g.Expect(poolSizeAfter).To(BeNumerically(">", poolSizeBefore),
					"the 100%% thin pool should grow with the VG (was %d)", poolSizeBefore)
			}, fdLVGReadyTimeout, 10*time.Second).Should(Succeed())
			fdExpectNoFalseConditions(&grown)

			By("✓ A 100% thin pool follows an in-place file device growth")
		})

		// A tagged Volume Group whose LVMVolumeGroup resource is gone is re-imported
		// by the discoverer. Block-backed VGs have always had this; a file-backed one
		// used to be excluded silently, because the import path both left
		// spec.fileDevices empty and bailed out on a node with no BlockDevices —
		// the permanent state of a VG built entirely on files. Recovering the spec is
		// possible only because the backing file's name says which LVMVolumeGroup and
		// which entry it belongs to.
		It("Should import a file-backed VG that has no LVMVolumeGroup resource", func() {
			lvgName := fdLVGName("fdimp", runID, targetNode)
			vgName := "e2e-vg-fdimp-" + runID

			By(fmt.Sprintf("Building a tagged file-backed VG %s on node %s with no resource behind it", vgName, targetNode))
			backingFile := fdCreateAdoptableLoopVG(ctx, cl, targetNode, fdBaseDir, lvgName, "data-0", vgName, "1G")
			DeferCleanup(func() { fdDeleteLVGAndWaitGone(ctx, k8sClient, lvgName) })

			By("Waiting for the discoverer to import it")
			var imported v1alpha1.LVMVolumeGroup
			Eventually(func(g Gomega) {
				g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: lvgName}, &imported)).To(Succeed())
				g.Expect(imported.Spec.FileDevices).To(HaveLen(1), "spec.fileDevices must be rebuilt from the node")
				g.Expect(imported.Status.Phase).To(Equal(v1alpha1.PhaseReady))
			}, fdLVGReadyTimeout, 10*time.Second).Should(Succeed())
			fdPrintLVG(&imported)

			By("Verifying the rebuilt entry describes what is actually on the node")
			entry := imported.Spec.FileDevices[0]
			Expect(entry.Name).To(Equal("data-0"), "the entry name is recovered from the backing file name")
			Expect(entry.Directory).To(Equal(fdBaseDir))
			wantSize := resource.MustParse("1Gi")
			Expect(entry.Size.Value()).To(Equal(wantSize.Value()),
				"the PV size must round back up to a whole GiB")

			By("Verifying the existing backing file was adopted, not duplicated")
			fds := fdFileDevicesForNode(&imported, targetNode)
			Expect(fds).To(HaveLen(1))
			Expect(fds[0].FilePath).To(Equal(backingFile))

			files, err := fdBackingFilesForLVG(ctx, cl, targetNode, lvgName)
			Expect(err).NotTo(HaveOccurred())
			Expect(files).To(HaveLen(1), "the import must reuse the existing file, not provision a second: %v", files)

			pvNames, pvsOut, err := fdPVNamesInVGOnNode(ctx, cl, targetNode, vgName)
			Expect(err).NotTo(HaveOccurred())
			Expect(pvNames).To(HaveLen(1), "the VG must still span exactly one PV; pvs:\n%s", pvsOut)

			By("✓ File-backed VG imported with its spec rebuilt from the node")
		})

		// The anti-wedge guarantee, end to end. Under the previous contract this
		// sequence was terminal: an entry the agent rejects made the whole
		// LVMVolumeGroup invalid, and the apiserver refused to let it be removed
		// again, so a healthy Volume Group with data on it could only be repaired by
		// deleting it. Every step below has to work on a live, in-use LVG.
		It("Should stay manageable when an unusable fileDevices entry is added", func() {
			lvgName := fdLVGName("fdwedge", runID, targetNode)
			vgName := "e2e-vg-fdwedge-" + runID
			// The entry has to be one the APISERVER ACCEPTS and the AGENT REJECTS,
			// which is the only shape that exercises the anti-wedge path at all: an
			// entry rejected at admission never reaches a node and can never wedge
			// anything. A sub-1Gi `size` used to be that shape and no longer is — the
			// CRD now carries a per-item CEL rule enforcing the 1Gi minimum, so the
			// apiserver refuses the update outright. `directory` has no such rule
			// (only maxLength), and confinement to the module's base directory is
			// enforced by the agent alone, so an out-of-base path is the shape left.
			// See the sibling spec "Should reject a fileDevices directory outside the
			// allowed base dir", which asserts the same rejection on the create path.
			badDir := "/tmp/e2e-fdwedge-" + runID

			By(fmt.Sprintf("Creating a healthy file-backed LVMVolumeGroup %s on node %s", lvgName, targetNode))
			lvg := fdNewLVG(targetNode, lvgName, vgName, nil,
				[]v1alpha1.LVMVolumeGroupFileDeviceSpec{
					{Name: "keep", Directory: fdBaseDir, Size: resource.MustParse("1Gi")},
				}, nil)
			DeferCleanup(func() { fdDeleteLVGAndWaitGone(ctx, k8sClient, lvgName) })

			created := fdCreateLVGAndWaitReady(ctx, cl, k8sClient, lvg)
			vgSizeBefore := created.Status.VGSize.Value()

			By(fmt.Sprintf("Adding an entry the agent will reject (directory %s, outside the base dir)", badDir))
			Eventually(func(g Gomega) {
				var cur v1alpha1.LVMVolumeGroup
				g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: lvgName}, &cur)).To(Succeed())
				cur.Spec.FileDevices = append(cur.Spec.FileDevices,
					v1alpha1.LVMVolumeGroupFileDeviceSpec{Name: "typo", Directory: badDir, Size: resource.MustParse("1Gi")})
				g.Expect(k8sClient.Update(ctx, &cur)).To(Succeed())
			}, 1*time.Minute, 5*time.Second).Should(Succeed(),
				"an out-of-base directory must be accepted at admission; only the agent may reject it")

			By("Expecting the rejection to be reported on VGConfigurationApplied")
			fdWaitVGConfigurationRejected(ctx, k8sClient, lvgName, fdReasonValidationFailed, "or a subdirectory of it")

			By("Verifying the bad entry provisioned nothing and the healthy one is untouched")
			var wedged v1alpha1.LVMVolumeGroup
			Expect(k8sClient.Get(ctx, client.ObjectKey{Name: lvgName}, &wedged)).To(Succeed())
			fdPrintLVG(&wedged)
			Expect(fdFileDevicesForNode(&wedged, targetNode)).To(HaveLen(1),
				"the rejected entry must not appear in status")
			Expect(wedged.Status.VGSize.Value()).To(Equal(vgSizeBefore),
				"the VG must not grow by a rejected entry")

			files, err := fdBackingFilesForLVG(ctx, cl, targetNode, lvgName)
			Expect(err).NotTo(HaveOccurred())
			Expect(files).To(HaveLen(1), "no backing file may be created for a rejected entry: %v", files)

			// The point of the confinement check: a mistyped directory must not be
			// created, let alone filled, outside the module's own subtree.
			badDirExists, err := fdNodePathExists(ctx, cl, targetNode, badDir)
			Expect(err).NotTo(HaveOccurred())
			Expect(badDirExists).To(BeFalse(),
				"the out-of-base directory %s must not be created on the node", badDir)

			pvNames, pvsOut, err := fdPVNamesInVGOnNode(ctx, cl, targetNode, vgName)
			Expect(err).NotTo(HaveOccurred())
			Expect(pvNames).To(HaveLen(1), "the VG must still span exactly one PV; pvs:\n%s", pvsOut)

			By("Removing the bad entry — the step the previous contract made impossible")
			Eventually(func(g Gomega) {
				var cur v1alpha1.LVMVolumeGroup
				g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: lvgName}, &cur)).To(Succeed())
				kept := make([]v1alpha1.LVMVolumeGroupFileDeviceSpec, 0, 1)
				for _, e := range cur.Spec.FileDevices {
					if e.Name != "typo" {
						kept = append(kept, e)
					}
				}
				cur.Spec.FileDevices = kept
				g.Expect(k8sClient.Update(ctx, &cur)).To(Succeed())
			}, 1*time.Minute, 5*time.Second).Should(Succeed(),
				"a rejected entry must be removable from a live LVMVolumeGroup")

			By("Expecting the LVMVolumeGroup to recover")
			fdWaitVGConfigurationOK(ctx, k8sClient, lvgName)
			var recovered v1alpha1.LVMVolumeGroup
			Expect(k8sClient.Get(ctx, client.ObjectKey{Name: lvgName}, &recovered)).To(Succeed())
			fdExpectNoFalseConditions(&recovered)

			By("✓ A rejected entry is reported, provisions nothing, and can be removed again")
		})

		// Removing an entry that backs a live PV is drift, not an instruction: the
		// module never shrinks a Volume Group. The apiserver allows the edit — that is
		// what keeps an unprovisioned entry removable — so the agent has to be the one
		// that refuses to act, says why, and leaves the PV in place.
		It("Should report drift and keep the PV when a provisioned entry is removed", func() {
			lvgName := fdLVGName("fddrift", runID, targetNode)
			vgName := "e2e-vg-fddrift-" + runID

			By(fmt.Sprintf("Creating LVMVolumeGroup %s with two file devices on node %s", lvgName, targetNode))
			lvg := fdNewLVG(targetNode, lvgName, vgName, nil,
				[]v1alpha1.LVMVolumeGroupFileDeviceSpec{
					{Name: "keep", Directory: fdBaseDir, Size: resource.MustParse("1Gi")},
					{Name: "dropped", Directory: fdBaseDir, Size: resource.MustParse("1Gi")},
				}, nil)
			DeferCleanup(func() { fdDeleteLVGAndWaitGone(ctx, k8sClient, lvgName) })

			created := fdCreateLVGAndWaitReady(ctx, cl, k8sClient, lvg)
			Expect(fdFileDevicesForNode(created, targetNode)).To(HaveLen(2))

			By("Removing the provisioned 'dropped' entry from the spec")
			Eventually(func(g Gomega) {
				var cur v1alpha1.LVMVolumeGroup
				g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: lvgName}, &cur)).To(Succeed())
				kept := make([]v1alpha1.LVMVolumeGroupFileDeviceSpec, 0, 1)
				for _, fd := range cur.Spec.FileDevices {
					if fd.Name != "dropped" {
						kept = append(kept, fd)
					}
				}
				cur.Spec.FileDevices = kept
				g.Expect(k8sClient.Update(ctx, &cur)).To(Succeed())
			}, 1*time.Minute, 5*time.Second).Should(Succeed(),
				"the apiserver must accept the removal; refusing it is what wedges an LVG")

			// Under its own reason, not ValidationFailed: the spec is well-formed and
			// the agent applied everything it could, so an alert on a malformed
			// LVMVolumeGroup must not fire on this.
			By("Expecting the drift to surface on VGConfigurationApplied under the drift reason")
			fdWaitVGConfigurationRejected(ctx, k8sClient, lvgName, fdReasonFileDeviceDrift, "were removed from the spec")

			By("Verifying the PV is still part of the VG and its backing file still exists")
			pvNames, pvsOut, err := fdPVNamesInVGOnNode(ctx, cl, targetNode, vgName)
			Expect(err).NotTo(HaveOccurred())
			Expect(pvNames).To(HaveLen(2), "the orphaned PV must NOT be removed from the VG; pvs:\n%s", pvsOut)

			droppedFile := fdBaseDir + "/" + fdBackingFileName(lvgName, "dropped")
			exists, err := fdNodePathExists(ctx, cl, targetNode, droppedFile)
			Expect(err).NotTo(HaveOccurred())
			Expect(exists).To(BeTrue(), "the backing file of a live PV must never be deleted: %s", droppedFile)

			// Reporting drift must not take the node's storage out of service: the
			// Volume Group still serves every volume on it, and only an operator can
			// decide what happens to the orphaned Physical Volume. This holds because
			// the conditions watcher lists the drift reason in acceptableReasons.
			By("Verifying the Volume Group keeps working while the drift is reported")
			var drifted v1alpha1.LVMVolumeGroup
			Expect(k8sClient.Get(ctx, client.ObjectKey{Name: lvgName}, &drifted)).To(Succeed())
			Expect(drifted.Status.Phase).To(Equal(v1alpha1.PhaseReady),
				"drift must not drag the LVMVolumeGroup out of Ready; phase=%s", drifted.Status.Phase)

			By("Restoring the entry and expecting the LVG to go green again")
			Eventually(func(g Gomega) {
				var cur v1alpha1.LVMVolumeGroup
				g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: lvgName}, &cur)).To(Succeed())
				cur.Spec.FileDevices = append(cur.Spec.FileDevices,
					v1alpha1.LVMVolumeGroupFileDeviceSpec{Name: "dropped", Directory: fdBaseDir, Size: resource.MustParse("1Gi")})
				g.Expect(k8sClient.Update(ctx, &cur)).To(Succeed())
			}, 1*time.Minute, 5*time.Second).Should(Succeed())

			fdWaitVGConfigurationOK(ctx, k8sClient, lvgName)
			var restored v1alpha1.LVMVolumeGroup
			Expect(k8sClient.Get(ctx, client.ObjectKey{Name: lvgName}, &restored)).To(Succeed())
			Expect(fdFileDevicesForNode(&restored, targetNode)).To(HaveLen(2))
			fdExpectNoFalseConditions(&restored)

			By("✓ Removing a provisioned entry reports drift, keeps the PV and is reversible")
		})

		// Two phases, because a plain agent restart does NOT exercise the reattach code:
		// loop devices are created in PID 1's mount namespace and survive the pod, so
		// ReattachFileDevices finds them already attached and returns early. Only after the
		// loop is really gone — as it is after a node reboot — does the path do any work.
		It("Should reattach file devices after the loop mapping is lost", func() {
			lvgName := fdLVGName("fdreat", runID, targetNode)
			vgName := "e2e-vg-fdreat-" + runID

			By(fmt.Sprintf("Creating file-backed LVMVolumeGroup %s on node %s", lvgName, targetNode))
			lvg := fdNewLVG(targetNode, lvgName, vgName, nil,
				[]v1alpha1.LVMVolumeGroupFileDeviceSpec{{Name: "d1g", Directory: fdBaseDir, Size: resource.MustParse("1Gi")}}, nil)

			DeferCleanup(func() { fdDeleteLVGAndWaitGone(ctx, k8sClient, lvgName) })
			created := fdCreateLVGAndWaitReady(ctx, cl, k8sClient, lvg)
			fdsBefore := fdFileDevicesForNode(created, targetNode)
			Expect(fdsBefore).To(HaveLen(1))
			filePath := fdsBefore[0].FilePath
			loopBefore := fdsBefore[0].LoopDevice

			By("Phase 1: plain agent restart — the mapping survives, so nothing should churn")
			Expect(restartAgentOnNode(ctx, cl, targetNode)).To(Succeed())
			// A fresh agent re-runs discovery, and the resource dips through NotReady
			// while it does. That dip is expected; what must not happen is the loop or
			// the backing file changing. Let it settle first, then assert stability.
			Eventually(func(g Gomega) {
				var cur v1alpha1.LVMVolumeGroup
				g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: lvgName}, &cur)).To(Succeed())
				g.Expect(cur.Status.Phase).To(Equal(v1alpha1.PhaseReady),
					"the VG should settle back to Ready after the agent restart, got %s", cur.Status.Phase)
			}, fdLVGReadyTimeout, 10*time.Second).Should(Succeed())
			Consistently(func(g Gomega) {
				var cur v1alpha1.LVMVolumeGroup
				g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: lvgName}, &cur)).To(Succeed())
				g.Expect(cur.Status.Phase).To(Equal(v1alpha1.PhaseReady), "phase should stay Ready across a plain restart")
				fds := fdFileDevicesForNode(&cur, targetNode)
				g.Expect(fds).To(HaveLen(1))
				g.Expect(fds[0].FilePath).To(Equal(filePath), "backing file path should be unchanged")
				g.Expect(fds[0].LoopDevice).To(Equal(loopBefore), "a surviving loop must be reused, not re-created")
			}, 90*time.Second, 15*time.Second).Should(Succeed())

			loops, loopOut, err := fdLoopsForFile(ctx, cl, targetNode, filePath)
			Expect(err).NotTo(HaveOccurred())
			Expect(loops).To(HaveLen(1), "a plain restart must not attach a second loop to %s; losetup:\n%s", filePath, loopOut)

			By("Phase 2: dropping the loop mapping and deactivating the VG (reboot simulation)")
			fdDetachLoopAndDeactivateVG(ctx, cl, targetNode, vgName, loopBefore)

			gone, _, err := fdLoopBoundToFile(ctx, cl, targetNode, filePath)
			Expect(err).NotTo(HaveOccurred())
			Expect(gone).To(BeFalse(), "precondition: the loop for %s must really be detached before the restart", filePath)

			By("Restarting the agent so ReattachFileDevices has to re-establish the mapping")
			Expect(restartAgentOnNode(ctx, cl, targetNode)).To(Succeed())

			By("Verifying the file device is reattached and the VG converges back to Ready")
			var recovered v1alpha1.LVMVolumeGroup
			Eventually(func(g Gomega) {
				g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: lvgName}, &recovered)).To(Succeed())
				fdPrintLVG(&recovered)
				g.Expect(recovered.Status.Phase).To(Equal(v1alpha1.PhaseReady), "phase should return to Ready after reattach")
				fds := fdFileDevicesForNode(&recovered, targetNode)
				g.Expect(fds).To(HaveLen(1), "file device should be reported again after reattach")
				g.Expect(fds[0].FilePath).To(Equal(filePath), "the deterministic backing file must be reused, not a new one")
				g.Expect(fds[0].LoopDevice).To(HavePrefix("/dev/loop"), "a loop device should be attached again")
			}, fdLVGReadyTimeout, 10*time.Second).Should(Succeed())
			fdExpectNoFalseConditions(&recovered)

			By("Verifying exactly one loop is attached and the VG is active on the node")
			loopsAfter, loopOutAfter, err := fdLoopsForFile(ctx, cl, targetNode, filePath)
			Expect(err).NotTo(HaveOccurred())
			Expect(loopsAfter).To(HaveLen(1), "reattach must not leave two loops on %s; losetup:\n%s", filePath, loopOutAfter)

			vgListed, vgsOut, err := fdVGListedOnNode(ctx, cl, targetNode, vgName)
			Expect(err).NotTo(HaveOccurred())
			Expect(vgListed).To(BeTrue(), "VG %s should be visible again after reattach; vgs:\n%s", vgName, vgsOut)

			By("Verifying no orphan backing file was created alongside the reused one")
			files, err := fdBackingFilesForLVG(ctx, cl, targetNode, lvgName)
			Expect(err).NotTo(HaveOccurred())
			Expect(files).To(ConsistOf(filePath), "reattach must reuse the one backing file, found: %v", files)

			By("✓ File device reattached after a real loss of the loop mapping; no duplicate loop or file")
		})

		// Guards the failure mode called out in rollbackProvisionedFileDevices: one backing
		// file attached to two loops (one shown "(deleted)"), which silently doubles the VG.
		It("Should stay idempotent across repeated reconciles without leaking loops or files", func() {
			lvgName := fdLVGName("fdidem", runID, targetNode)
			vgName := "e2e-vg-fdidem-" + runID

			By(fmt.Sprintf("Creating file-backed LVMVolumeGroup %s on node %s", lvgName, targetNode))
			lvg := fdNewLVG(targetNode, lvgName, vgName, nil,
				[]v1alpha1.LVMVolumeGroupFileDeviceSpec{{Name: "d1g", Directory: fdBaseDir, Size: resource.MustParse("1Gi")}}, nil)

			DeferCleanup(func() { fdDeleteLVGAndWaitGone(ctx, k8sClient, lvgName) })
			created := fdCreateLVGAndWaitReady(ctx, cl, k8sClient, lvg)
			fds := fdFileDevicesForNode(created, targetNode)
			Expect(fds).To(HaveLen(1))
			filePath := fds[0].FilePath
			vgSizeBaseline := created.Status.VGSize.Value()
			Expect(vgSizeBaseline).To(BeNumerically(">", 0))

			By("Forcing three more reconciles (label touch + agent restart)")
			for i := range 3 {
				Eventually(func(g Gomega) {
					var cur v1alpha1.LVMVolumeGroup
					g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: lvgName}, &cur)).To(Succeed())
					if cur.Labels == nil {
						cur.Labels = map[string]string{}
					}
					cur.Labels["e2e.storage.deckhouse.io/reconcile-nudge"] = fmt.Sprintf("%d", i)
					g.Expect(k8sClient.Update(ctx, &cur)).To(Succeed())
				}, 1*time.Minute, 5*time.Second).Should(Succeed())

				Expect(restartAgentOnNode(ctx, cl, targetNode)).To(Succeed())
			}

			By("Verifying the VG did not grow and still reports a single file device")
			var settled v1alpha1.LVMVolumeGroup
			Eventually(func(g Gomega) {
				g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: lvgName}, &settled)).To(Succeed())
				g.Expect(settled.Status.Phase).To(Equal(v1alpha1.PhaseReady))
				g.Expect(fdFileDevicesForNode(&settled, targetNode)).To(HaveLen(1),
					"repeated reconciles must not add a second file device")
				g.Expect(settled.Status.VGSize.Value()).To(Equal(vgSizeBaseline),
					"VG size must not drift across reconciles (baseline %d)", vgSizeBaseline)
			}, fdLVGReadyTimeout, 10*time.Second).Should(Succeed())
			fdExpectNoFalseConditions(&settled)
			fdPrintLVG(&settled)

			By("Verifying the node has exactly one loop, one PV and one backing file")
			loops, loopOut, err := fdLoopsForFile(ctx, cl, targetNode, filePath)
			Expect(err).NotTo(HaveOccurred())
			Expect(loops).To(HaveLen(1), "backing file %s must be attached to exactly one loop; losetup:\n%s", filePath, loopOut)

			pvNames, pvsOut, err := fdPVNamesInVGOnNode(ctx, cl, targetNode, vgName)
			Expect(err).NotTo(HaveOccurred())
			Expect(pvNames).To(HaveLen(1), "VG %s must have exactly one PV; pvs:\n%s", vgName, pvsOut)

			files, err := fdBackingFilesForLVG(ctx, cl, targetNode, lvgName)
			Expect(err).NotTo(HaveOccurred())
			Expect(files).To(ConsistOf(filePath), "the LVG must own exactly one backing file, found: %v", files)

			By("✓ Repeated reconciles are idempotent: one file, one loop, one PV, stable VG size")
		})

		// Unlike the other file-backed scenarios, a mixed VG needs a real BlockDevice,
		// so this spec attaches a disk to the node.
		// The only file-devices spec that needs a real disk, hence the needs-disks
		// label: it cannot run on a provider that hands out a cluster without the
		// infrastructure under it (Commander), so CI keeps it on the dvp job.
		It("Should create a mixed block + file-backed LVMVolumeGroup on one node", Label("needs-disks"), func() {
			if conf.TestCluster.StorageClass == "" {
				Skip("mixed block+file spec needs a storage class for the VirtualDisk (E2E_DVP_BASE_CLUSTER_STORAGE_CLASS)")
			}

			lvgName := fdLVGName("fdmix", runID, targetNode)
			vgName := "e2e-vg-fdmix-" + runID

			By("Snapshotting consumable BlockDevices before attach")
			before, err := kubernetes.GetConsumableBlockDevicesByNode(ctx, cl.RESTConfig(), targetNode)
			Expect(err).NotTo(HaveOccurred())

			diskName := "e2e-lvg-fdmix-disk-" + runID
			By("Creating and attaching a virtual disk: " + diskName)
			disk, err := fdCreateDiskOrSkip(ctx, cl, e2e.DiskSpec{
				Name:         diskName,
				Size:         resource.MustParse("2Gi"),
				StorageClass: conf.TestCluster.StorageClass,
			})
			Expect(err).NotTo(HaveOccurred(), "failed to create disk")
			DeferCleanup(func() {
				_ = cl.Disks().DetachDisk(ctx, targetNode, disk.Name)
				_ = cl.Disks().DeleteDisk(ctx, disk.Name)
			})
			Expect(cl.Disks().AttachDisk(ctx, targetNode, disk.Name)).To(Succeed(), "failed to attach disk")

			By("Waiting for the new consumable BlockDevice on node " + targetNode)
			newBD, err := framework.WaitNewConsumableBlockDevice(ctx, cl.RESTConfig(), targetNode, before, 5*time.Minute)
			Expect(err).NotTo(HaveOccurred())

			By(fmt.Sprintf("Creating mixed LVMVolumeGroup %s on node %s (block %s + one file device)", lvgName, targetNode, newBD.Name))
			selector := &metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{
					{
						Key:      "kubernetes.io/metadata.name",
						Operator: metav1.LabelSelectorOpIn,
						Values:   []string{newBD.Name},
					},
				},
			}
			lvg := fdNewLVG(targetNode, lvgName, vgName, selector,
				[]v1alpha1.LVMVolumeGroupFileDeviceSpec{{Name: "d1g", Directory: fdBaseDir, Size: resource.MustParse("1Gi")}}, nil)

			deleted := false
			DeferCleanup(func() {
				if !deleted {
					fdDeleteLVGAndWaitGone(ctx, k8sClient, lvgName)
				}
			})
			created := fdCreateLVGAndWaitReady(ctx, cl, k8sClient, lvg)
			fdExpectNoFalseConditions(created)
			fdPrintLVG(created)

			By("Verifying the VG reports both a block device and a file device in status")
			// Polled, not read off the snapshot taken the moment the VG went Ready.
			// The two halves of status.nodes[] are filled from different sources: the
			// file device as soon as the loop PV is discovered, the block device only
			// once a BlockDevice resource exists for that PV — until then the
			// discoverer skips it ("no BlockDevice resource is yet configured for PV
			// ..., retry on the next iteration"). Ready therefore does not imply both
			// are present yet.
			// The block half of the status depends on the PV being matched to a
			// BlockDevice resource, which in turn depends on udev having reported the
			// disk. Nudge the node's LVM/udev state before polling so a missed event
			// does not read as a missing device.
			framework.TriggerLVMDiscovery(ctx, cl, targetNode)

			var mixed v1alpha1.LVMVolumeGroup
			Eventually(func(g Gomega) {
				g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: lvgName}, &mixed)).To(Succeed())
				fds := fdFileDevicesForNode(&mixed, targetNode)
				g.Expect(fds).To(HaveLen(1), "the loop-backed PV should be reported as one file device")
				g.Expect(fds[0].LoopDevice).To(HavePrefix("/dev/loop"))
				g.Expect(fdCountDevicesOnNode(&mixed, targetNode)).To(BeNumerically(">=", 1),
					"the block PV should be reported under status.nodes[].devices")
			}, fdLVGReadyTimeout, 10*time.Second).Should(Succeed())
			fdPrintLVG(&mixed)
			fd := fdFileDevicesForNode(&mixed, targetNode)[0]

			By("Verifying the VG spans two PVs on the node: one /dev/loop* and one block device")
			pvNames, pvsOut, err := fdPVNamesInVGOnNode(ctx, cl, targetNode, vgName)
			Expect(err).NotTo(HaveOccurred())
			Expect(len(pvNames)).To(BeNumerically(">=", 2), "mixed VG should have >=2 PVs; pvs:\n%s", pvsOut)
			var loopPVs, blockPVs int
			for _, pv := range pvNames {
				if strings.HasPrefix(pv, "/dev/loop") {
					loopPVs++
				} else {
					blockPVs++
				}
			}
			Expect(loopPVs).To(BeNumerically(">=", 1), "expected a loop PV in the mixed VG; pvs:\n%s", pvsOut)
			Expect(blockPVs).To(BeNumerically(">=", 1), "expected a block PV in the mixed VG; pvs:\n%s", pvsOut)

			By("Deleting the mixed LVMVolumeGroup and verifying the backing file is cleaned up")
			fdDeleteLVGAndWaitGone(ctx, k8sClient, lvgName)
			deleted = true
			fdExpectBackingFileGone(ctx, cl, targetNode, fd.FilePath)

			By("✓ Mixed block+file VG created (block PV + loop PV), backing file cleaned up on delete")
		})
	})
