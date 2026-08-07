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
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// fdLocalStorageClassName is distinct from the scheduler suite's e2e-local-sc so the two
	// Describes never fight over the same LocalStorageClass; both share the "e2e-" prefix
	// that cleanupLocalStorageClasses sweeps.
	fdLocalStorageClassName = "e2e-local-sc-filedevices"

	fdWorkloadTimeout = 10 * time.Minute
)

// The point of spec.fileDevices is that a node with no spare disk still gets full-featured
// LVM. These specs follow that promise all the way to a workload: a PersistentVolume carved
// out of a file-backed VG, data written and read back, and a thin snapshot taken on a pool
// that lives on a loop device.
var _ = Describe("LVMVolumeGroup file-backed devices consumers",
	Label("sds-node-configurator", "lvmvolumegroup", "file-devices"), Ordered, ContinueOnFailure, func() {
		var (
			ctx        context.Context
			cl         *e2e.Cluster
			k8sClient  client.Client
			targetNode string
			runID      string

			thinPoolLVGName string
			thinPoolVGName  string
		)

		const thinPoolName = "e2e-thin-fd-consumer"

		BeforeAll(func() {
			By("Preparing shared test context and Kubernetes clients")
			ctx = context.Background()

			var clErr error
			cl, clErr = e2e.Connect(ctx, e2e.WithTestName("lvmvolumegroup-filedevices-consumers"))
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
			By("Cleaning up workload, LocalStorageClass and LVMVolumeGroups")
			if k8sClient != nil {
				fdCleanupWorkload(ctx, k8sClient)
				cleanupLVMLogicalVolumes(ctx, k8sClient)
			}
			if cl != nil {
				cleanupLocalStorageClasses(ctx, cl.RESTConfig())
			}
			if k8sClient != nil {
				cleanupLVMVolumeGroups(ctx, k8sClient)
			}
			if cl != nil {
				fdSweepLeakedBackingFiles(ctx, cl, targetNode)
				fdLogFreeSpace(ctx, cl, targetNode, "after the specs")
			}
		})

		// A Thick LocalStorageClass exercises the whole chain: scheduler-extender picks the
		// node from the file-backed LVG's free space, sds-local-volume carves an LV out of
		// the loop-backed PV, and the kubelet mounts it.
		It("Should serve a PersistentVolume from a file-backed LVMVolumeGroup", func() {
			lvgName := lvmVGNamePrefix + "fdpv-" + runID
			vgName := "e2e-vg-fdpv-" + runID

			By(fmt.Sprintf("Creating file-backed LVMVolumeGroup %s (4Gi) on node %s", lvgName, targetNode))
			lvg := fdNewLVG(targetNode, lvgName, vgName, nil,
				[]v1alpha1.LVMVolumeGroupFileDeviceSpec{{Name: "d0", Directory: fdBaseDir, Size: resource.MustParse(fdFileDeviceSize)}}, nil)
			DeferCleanup(func() { fdDeleteLVGAndWaitGone(ctx, k8sClient, lvgName) })
			created := fdCreateLVGAndWaitReady(ctx, cl, k8sClient, lvg)
			fdExpectNoFalseConditions(created)
			fdPrintLVG(created)

			By("Creating a Thick LocalStorageClass over that LVMVolumeGroup")
			fdCreateLocalStorageClass(ctx, cl, k8sClient, fdLocalStorageClassName, "Thick", []string{lvgName})
			DeferCleanup(func() {
				fdCleanupWorkload(ctx, k8sClient)
				cleanupLocalStorageClasses(ctx, cl.RESTConfig())
			})

			pvcName := pvcNamePrefix + "fd-" + runID
			marker := "file-devices-" + runID

			By("Running a writer Pod that puts a marker on the volume")
			fdCreatePVC(ctx, k8sClient, pvcName, fdLocalStorageClassName, "1Gi")
			writer := podNamePrefix + "fd-writer-" + runID
			fdRunPodToCompletion(ctx, k8sClient, writer, pvcName, targetNode,
				fmt.Sprintf("echo %s > /data/marker && sync", strconv.Quote(marker)))

			By("Verifying the PVC bound to a PersistentVolume on the file-backed VG")
			var pvc corev1.PersistentVolumeClaim
			Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: metav1.NamespaceDefault, Name: pvcName}, &pvc)).To(Succeed())
			Expect(pvc.Status.Phase).To(Equal(corev1.ClaimBound), "PVC should be Bound")
			Expect(pvc.Spec.VolumeName).NotTo(BeEmpty())

			By("Verifying an LVMLogicalVolume was created on our LVMVolumeGroup")
			Eventually(func(g Gomega) {
				var llvs v1alpha1.LVMLogicalVolumeList
				g.Expect(k8sClient.List(ctx, &llvs)).To(Succeed())
				var found bool
				for i := range llvs.Items {
					if llvs.Items[i].Spec.LVMVolumeGroupName == lvgName {
						found = true
						// Status is a pointer and is absent until the agent writes it.
						g.Expect(llvs.Items[i].Status).NotTo(BeNil(), "LVMLogicalVolume %s has no status yet", llvs.Items[i].Name)
						g.Expect(llvs.Items[i].Status.Phase).To(Equal(v1alpha1.PhaseCreated),
							"LVMLogicalVolume %s should be Created, reason=%s", llvs.Items[i].Name, llvs.Items[i].Status.Reason)
					}
				}
				g.Expect(found).To(BeTrue(), "no LVMLogicalVolume references LVMVolumeGroup %s", lvgName)
			}, fdWorkloadTimeout, 10*time.Second).Should(Succeed())

			By("Running a reader Pod that must find the same marker")
			reader := podNamePrefix + "fd-reader-" + runID
			fdRunPodToCompletion(ctx, k8sClient, reader, pvcName, targetNode,
				fmt.Sprintf("test \"$(cat /data/marker)\" = %s", strconv.Quote(marker)))

			By("✓ Data written to a PV on a file-backed VG survived a Pod restart and read back intact")
		})

		// The claim behind in-place growth is that it is online: `losetup -c` makes
		// the loop driver re-read the size of a device that stays attached, with
		// filesystems and logical volumes live on top of it. Nothing short of
		// growing a Volume Group that is actually carrying a mounted volume, and
		// then reading the data back, actually tests that claim.
		It("Should grow a file-backed VG that is carrying a mounted volume", func() {
			lvgName := lvmVGNamePrefix + "fdgrowlive-" + runID
			vgName := "e2e-vg-fdgrowlive-" + runID

			By(fmt.Sprintf("Creating a 2Gi file-backed LVMVolumeGroup %s on node %s", lvgName, targetNode))
			lvg := fdNewLVG(targetNode, lvgName, vgName, nil,
				[]v1alpha1.LVMVolumeGroupFileDeviceSpec{
					{Name: "data-0", Directory: fdBaseDir, Size: resource.MustParse("2Gi")},
				}, nil)
			DeferCleanup(func() { fdDeleteLVGAndWaitGone(ctx, k8sClient, lvgName) })
			created := fdCreateLVGAndWaitReady(ctx, cl, k8sClient, lvg)
			before := fdFileDevicesForNode(created, targetNode)[0]

			By("Creating a Thick LocalStorageClass and a volume on it")
			fdCreateLocalStorageClass(ctx, cl, k8sClient, fdLocalStorageClassName, "Thick", []string{lvgName})
			DeferCleanup(func() {
				fdCleanupWorkload(ctx, k8sClient)
				cleanupLocalStorageClasses(ctx, cl.RESTConfig())
			})

			pvcName := pvcNamePrefix + "fdgrow-" + runID
			marker := "grown-under-load-" + runID
			fdCreatePVC(ctx, k8sClient, pvcName, fdLocalStorageClassName, "1Gi")

			By("Writing a marker to the volume before the growth")
			fdRunPodToCompletion(ctx, k8sClient, podNamePrefix+"fdgrow-pre-"+runID, pvcName, targetNode,
				fmt.Sprintf("echo %s > /data/marker && sync", strconv.Quote(marker)))

			By("Growing the backing file from 2Gi to 4Gi while the volume exists")
			Eventually(func(g Gomega) {
				var cur v1alpha1.LVMVolumeGroup
				g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: lvgName}, &cur)).To(Succeed())
				cur.Spec.FileDevices[0].Size = resource.MustParse("4Gi")
				g.Expect(k8sClient.Update(ctx, &cur)).To(Succeed())
			}, 1*time.Minute, 5*time.Second).Should(Succeed())

			var grown v1alpha1.LVMVolumeGroup
			Eventually(func(g Gomega) {
				g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: lvgName}, &grown)).To(Succeed())
				fdPrintLVG(&grown)
				g.Expect(grown.Status.Phase).To(Equal(v1alpha1.PhaseReady))
				fds := fdFileDevicesForNode(&grown, targetNode)
				g.Expect(fds).To(HaveLen(1))
				g.Expect(fds[0].Size.Value()).To(BeNumerically(">", before.Size.Value()))
			}, fdLVGReadyTimeout, 10*time.Second).Should(Succeed())
			fdExpectNoFalseConditions(&grown)

			By("Verifying the volume was carried through the growth, not re-created")
			after := fdFileDevicesForNode(&grown, targetNode)[0]
			Expect(after.LoopDevice).To(Equal(before.LoopDevice), "the loop device must stay attached across the growth")
			Expect(after.PVUuid).To(Equal(before.PVUuid), "the PV must be resized, not recreated under the volume")

			By("Reading the marker back from the same volume")
			fdRunPodToCompletion(ctx, k8sClient, podNamePrefix+"fdgrow-post-"+runID, pvcName, targetNode,
				fmt.Sprintf("test \"$(cat /data/marker)\" = %s", strconv.Quote(marker)))

			By("Verifying the freed capacity is usable: a second volume fits where it did not before")
			secondPVC := pvcNamePrefix + "fdgrow2-" + runID
			fdCreatePVC(ctx, k8sClient, secondPVC, fdLocalStorageClassName, "1Gi")
			fdRunPodToCompletion(ctx, k8sClient, podNamePrefix+"fdgrow-second-"+runID, secondPVC, targetNode,
				"dd if=/dev/zero of=/data/fill bs=1M count=64 && sync")

			By("✓ File device grown under a live volume: data intact, new capacity usable")
		})

		It("Should create a thin LVMLogicalVolume on a file-backed thin pool", func() {
			thinPoolLVGName = lvmVGNamePrefix + "fdthin-" + runID
			thinPoolVGName = "e2e-vg-fdthin-" + runID

			By(fmt.Sprintf("Creating file-backed LVMVolumeGroup %s (4Gi) with thin-pool %s", thinPoolLVGName, thinPoolName))
			lvg := fdNewLVG(targetNode, thinPoolLVGName, thinPoolVGName, nil,
				[]v1alpha1.LVMVolumeGroupFileDeviceSpec{{Name: "d0", Directory: fdBaseDir, Size: resource.MustParse(fdFileDeviceSize)}},
				[]v1alpha1.LVMVolumeGroupThinPoolSpec{{Name: thinPoolName, Size: fdThinPoolSize, AllocationLimit: "150%"}})
			DeferCleanup(func() {
				fdRunThinPoolTeardown(ctx, cl, targetNode,
					framework.RemoveThinPoolStackScriptWithLVMConfig(thinPoolVGName, thinPoolName, fdLVMConfig))
				fdDeleteLVGAndWaitGone(ctx, k8sClient, thinPoolLVGName)
			})
			created := fdCreateLVGAndWaitReady(ctx, cl, k8sClient, lvg)
			fdExpectNoFalseConditions(created)

			llvName := "e2e-llv-fdthin-" + runID
			lvOnNode := "e2e-lv-fdthin-" + runID

			By(fmt.Sprintf("Creating thin LVMLogicalVolume %s in pool %s", llvName, thinPoolName))
			llv := &v1alpha1.LVMLogicalVolume{
				ObjectMeta: metav1.ObjectMeta{Name: llvName},
				Spec: v1alpha1.LVMLogicalVolumeSpec{
					ActualLVNameOnTheNode: lvOnNode,
					Type:                  "Thin",
					Size:                  fdThinLVSize,
					LVMVolumeGroupName:    thinPoolLVGName,
					Thin:                  &v1alpha1.LVMLogicalVolumeThinSpec{PoolName: thinPoolName},
				},
			}
			Expect(k8sClient.Create(ctx, llv)).To(Succeed())
			DeferCleanup(func() {
				_ = client.IgnoreNotFound(k8sClient.Delete(ctx, &v1alpha1.LVMLogicalVolume{
					ObjectMeta: metav1.ObjectMeta{Name: llvName},
				}))
			})

			By("Waiting for the LVMLogicalVolume to reach Created")
			Eventually(func(g Gomega) {
				var cur v1alpha1.LVMLogicalVolume
				g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: llvName}, &cur)).To(Succeed())
				g.Expect(cur.Status).NotTo(BeNil(), "LVMLogicalVolume %s has no status yet", llvName)
				g.Expect(cur.Status.Phase).To(Equal(v1alpha1.PhaseCreated),
					"LVMLogicalVolume phase=%s reason=%s", cur.Status.Phase, cur.Status.Reason)
			}, fdWorkloadTimeout, 10*time.Second).Should(Succeed())

			By("Verifying the thin LV exists on the node inside the loop-backed VG")
			out, err := framework.NodeExecChecked(ctx, cl, targetNode,
				fmt.Sprintf(`sudo -n lvm lvs %s -a -o lv_name,pool_lv --noheadings %s 2>/dev/null || true`, fdLVMCfg, strconv.Quote(thinPoolVGName)))
			Expect(err).NotTo(HaveOccurred())
			Expect(out).To(ContainSubstring(lvOnNode), "thin LV %s should exist in VG %s; lvs:\n%s", lvOnNode, thinPoolVGName, out)

			// The snapshot lives in the same spec on purpose: the thin pool and the LV are
			// torn down by this spec's DeferCleanup, so a separate Ordered spec would find
			// nothing left to snapshot.
			//
			// The R&D motivation for file-backed VGs is snapshots on a node with no spare
			// disk. LVMLogicalVolumeSnapshot is edition-gated (CE builds have it compiled
			// out), so this asserts the layer the module actually enables regardless of
			// edition: an LVM thin snapshot on a pool whose PV is a loop device.
			snapName := "e2e-snap-fdthin-" + runID
			srcMount := "/mnt/e2e-fdthin-src-" + runID
			snapMount := "/mnt/e2e-fdthin-snap-" + runID

			By("Formatting the thin LV and writing a marker into it before snapshotting")
			DeferCleanup(func() {
				_, _ = framework.NodeExecChecked(ctx, cl, targetNode, fmt.Sprintf(`set +e
sudo -n umount %s >/dev/null 2>&1
sudo -n umount %s >/dev/null 2>&1
sudo -n lvm lvremove %s -fy %s/%s >/dev/null 2>&1
sudo -n rmdir %s %s >/dev/null 2>&1
exit 0`,
					snapMount, srcMount,
					fdLVMCfg, thinPoolVGName, snapName,
					snapMount, srcMount))
			})

			writeScript := fmt.Sprintf(`set -e
sudo -n lvm lvchange %s -ay -K %s/%s
sudo -n mkfs.ext4 -q -F /dev/%s/%s
sudo -n mkdir -p %s
sudo -n mount /dev/%s/%s %s
echo %s | sudo -n tee %s/marker >/dev/null
sudo -n sync
sudo -n umount %s`,
				fdLVMCfg, thinPoolVGName, lvOnNode,
				thinPoolVGName, lvOnNode,
				srcMount,
				thinPoolVGName, lvOnNode, srcMount,
				strconv.Quote(runID), srcMount,
				srcMount)
			out, err = framework.NodeExecChecked(ctx, cl, targetNode, writeScript)
			Expect(err).NotTo(HaveOccurred(), "failed to prepare the thin LV:\n%s", out)

			By(fmt.Sprintf("Creating thin snapshot %s of %s/%s", snapName, thinPoolVGName, lvOnNode))
			snapOut, err := framework.NodeExecChecked(ctx, cl, targetNode,
				fmt.Sprintf(`sudo -n lvm lvcreate %s -s -kn -n %s %s/%s 2>&1`, fdLVMCfg, snapName, thinPoolVGName, lvOnNode))
			Expect(err).NotTo(HaveOccurred(), "lvcreate -s failed on a loop-backed thin pool:\n%s", snapOut)

			By("Verifying the snapshot carries the marker written before it was taken")
			readScript := fmt.Sprintf(`set -e
sudo -n lvm lvchange %s -ay -K %s/%s
sudo -n mkdir -p %s
sudo -n mount -o ro /dev/%s/%s %s
cat %s/marker
sudo -n umount %s`,
				fdLVMCfg, thinPoolVGName, snapName,
				snapMount,
				thinPoolVGName, snapName, snapMount,
				snapMount,
				snapMount)
			readOut, err := framework.NodeExecChecked(ctx, cl, targetNode, readScript)
			Expect(err).NotTo(HaveOccurred(), "failed to read back the snapshot:\n%s", readOut)
			Expect(strings.TrimSpace(readOut)).To(ContainSubstring(runID),
				"snapshot should contain the pre-snapshot marker; got:\n%s", readOut)

			By("Verifying the LVMVolumeGroup stays Ready with the snapshot present")
			Eventually(func(g Gomega) {
				var cur v1alpha1.LVMVolumeGroup
				g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: thinPoolLVGName}, &cur)).To(Succeed())
				g.Expect(cur.Status.Phase).To(Equal(v1alpha1.PhaseReady),
					"LVG should stay Ready while a thin snapshot exists")
			}, 5*time.Minute, 10*time.Second).Should(Succeed())

			By("✓ Thin LVMLogicalVolume provisioned and snapshotted on a loop-backed thin pool")
		})

		// The module's own snapshot API on top of a loop-backed pool. The
		// reconciler behind LVMLogicalVolumeSnapshot is compiled out of CE
		// builds (cmd/llvs_ee.go is `//go:build !ce`) while the CRD ships in
		// every edition, so a CR created there would simply never get a status.
		// The spec detects that and skips instead of failing an edition that
		// legitimately does not have the feature.
		It("Should snapshot a thin LVMLogicalVolume on a file-backed pool through the module API", func() {
			lvgName := lvmVGNamePrefix + "fdllvs-" + runID
			vgName := "e2e-vg-fdllvs-" + runID
			const poolName = "e2e-thin-fdllvs"

			By(fmt.Sprintf("Creating file-backed LVMVolumeGroup %s with thin-pool %s", lvgName, poolName))
			lvg := fdNewLVG(targetNode, lvgName, vgName, nil,
				[]v1alpha1.LVMVolumeGroupFileDeviceSpec{{Name: "d0", Directory: fdBaseDir, Size: resource.MustParse(fdFileDeviceSize)}},
				[]v1alpha1.LVMVolumeGroupThinPoolSpec{{Name: poolName, Size: fdThinPoolSize, AllocationLimit: "150%"}})
			DeferCleanup(func() {
				fdRunThinPoolTeardown(ctx, cl, targetNode,
					framework.RemoveThinPoolStackScriptWithLVMConfig(vgName, poolName, fdLVMConfig))
				fdDeleteLVGAndWaitGone(ctx, k8sClient, lvgName)
			})
			fdExpectNoFalseConditions(fdCreateLVGAndWaitReady(ctx, cl, k8sClient, lvg))

			llvName := "e2e-llv-fdllvs-" + runID
			By(fmt.Sprintf("Creating the source thin LVMLogicalVolume %s", llvName))
			llv := &v1alpha1.LVMLogicalVolume{
				ObjectMeta: metav1.ObjectMeta{Name: llvName},
				Spec: v1alpha1.LVMLogicalVolumeSpec{
					ActualLVNameOnTheNode: "e2e-lv-fdllvs-" + runID,
					Type:                  "Thin",
					Size:                  fdThinLVSize,
					LVMVolumeGroupName:    lvgName,
					Thin:                  &v1alpha1.LVMLogicalVolumeThinSpec{PoolName: poolName},
				},
			}
			Expect(k8sClient.Create(ctx, llv)).To(Succeed())
			DeferCleanup(func() {
				_ = client.IgnoreNotFound(k8sClient.Delete(ctx, &v1alpha1.LVMLogicalVolume{
					ObjectMeta: metav1.ObjectMeta{Name: llvName},
				}))
			})
			Eventually(func(g Gomega) {
				var cur v1alpha1.LVMLogicalVolume
				g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: llvName}, &cur)).To(Succeed())
				g.Expect(cur.Status).NotTo(BeNil(), "source LVMLogicalVolume %s has no status yet", llvName)
				g.Expect(cur.Status.Phase).To(Equal(v1alpha1.PhaseCreated),
					"source LVMLogicalVolume phase=%s reason=%s", cur.Status.Phase, cur.Status.Reason)
			}, fdWorkloadTimeout, 10*time.Second).Should(Succeed())

			llvsName := "e2e-llvs-fdllvs-" + runID
			By(fmt.Sprintf("Creating LVMLogicalVolumeSnapshot %s", llvsName))
			llvs := &v1alpha1.LVMLogicalVolumeSnapshot{
				ObjectMeta: metav1.ObjectMeta{Name: llvsName},
				Spec: v1alpha1.LVMLogicalVolumeSnapshotSpec{
					ActualSnapshotNameOnTheNode: "e2e-snap-fdllvs-" + runID,
					LVMLogicalVolumeName:        llvName,
				},
			}
			if err := k8sClient.Create(ctx, llvs); err != nil {
				if meta.IsNoMatchError(err) || apierrors.IsNotFound(err) {
					Skip("LVMLogicalVolumeSnapshot is not served by this cluster: " + err.Error())
				}
				Expect(err).NotTo(HaveOccurred())
			}
			DeferCleanup(func() {
				_ = client.IgnoreNotFound(k8sClient.Delete(ctx, &v1alpha1.LVMLogicalVolumeSnapshot{
					ObjectMeta: metav1.ObjectMeta{Name: llvsName},
				}))
			})

			By("Waiting for the snapshot to be reconciled")
			var phase, reason string
			_ = framework.Poll(ctx, 10*time.Second, fdWorkloadTimeout, func(ctx context.Context) (bool, error) {
				var cur v1alpha1.LVMLogicalVolumeSnapshot
				if err := k8sClient.Get(ctx, client.ObjectKey{Name: llvsName}, &cur); err != nil {
					return false, err
				}
				if cur.Status == nil {
					// No status at all is exactly the CE signal the caller checks for.
					return false, nil
				}
				phase, reason = cur.Status.Phase, cur.Status.Reason
				return phase != "", nil
			})

			if phase == "" {
				// No controller ever touched the object: this is a CE build,
				// where the snapshot reconciler is not compiled in.
				Skip("LVMLogicalVolumeSnapshot was never reconciled — snapshots are disabled in this edition")
			}

			Eventually(func(g Gomega) {
				var cur v1alpha1.LVMLogicalVolumeSnapshot
				g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: llvsName}, &cur)).To(Succeed())
				g.Expect(cur.Status).NotTo(BeNil(), "LVMLogicalVolumeSnapshot %s has no status yet", llvsName)
				g.Expect(cur.Status.Phase).To(Equal(v1alpha1.PhaseCreated),
					"snapshot phase=%s reason=%s", cur.Status.Phase, cur.Status.Reason)
				g.Expect(cur.Status.ActualVGNameOnTheNode).To(Equal(vgName))
				g.Expect(cur.Status.NodeName).To(Equal(targetNode))
			}, fdWorkloadTimeout, 10*time.Second).Should(Succeed(), "last seen phase=%s reason=%s", phase, reason)

			By("Verifying the snapshot LV exists on the node inside the loop-backed VG")
			out, err := framework.NodeExecChecked(ctx, cl, targetNode,
				fmt.Sprintf(`sudo -n lvm lvs %s -a -o lv_name --noheadings %s 2>/dev/null || true`, fdLVMCfg, strconv.Quote(vgName)))
			Expect(err).NotTo(HaveOccurred())
			Expect(out).To(ContainSubstring(llvs.Spec.ActualSnapshotNameOnTheNode),
				"snapshot LV should exist in VG %s; lvs:\n%s", vgName, out)

			By("✓ LVMLogicalVolumeSnapshot created on a thin pool backed by a loop device")
		})
	})

// ---=== Workload helpers ===--- //

// fdCreateLocalStorageClass creates a LocalStorageClass over the given LVMVolumeGroups and
// waits for both it and its derived StorageClass to appear.
func fdCreateLocalStorageClass(ctx context.Context, cl *e2e.Cluster, k8sClient client.Client, name, lvmType string, lvgNames []string) {
	GinkgoHelper()
	dynamicClient := cl.Dynamic()

	lvmVolumeGroups := make([]interface{}, len(lvgNames))
	for i, lvgName := range lvgNames {
		lvmVolumeGroups[i] = map[string]interface{}{"name": lvgName}
	}

	lsc := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "storage.deckhouse.io/v1alpha1",
			"kind":       "LocalStorageClass",
			"metadata":   map[string]interface{}{"name": name},
			"spec": map[string]interface{}{
				"lvm": map[string]interface{}{
					"lvmVolumeGroups": lvmVolumeGroups,
					"type":            lvmType,
				},
				"reclaimPolicy":     "Delete",
				"volumeBindingMode": "WaitForFirstConsumer",
			},
		},
	}

	Eventually(func(g Gomega) {
		g.Expect(ensureLocalStorageClassAbsent(ctx, cl.RESTConfig(), k8sClient, name)).To(Succeed())
		_, createErr := dynamicClient.Resource(localStorageClassGVR).Create(ctx, lsc.DeepCopy(), metav1.CreateOptions{})
		g.Expect(createErr).NotTo(HaveOccurred(), "create LocalStorageClass %s", name)
	}, 5*time.Minute, 10*time.Second).Should(Succeed())

	By("Waiting for the LocalStorageClass to reach Created and its StorageClass to appear")
	Eventually(func(g Gomega) {
		lscObj, err := dynamicClient.Resource(localStorageClassGVR).Get(ctx, name, metav1.GetOptions{})
		g.Expect(err).NotTo(HaveOccurred())
		phase, _, _ := unstructured.NestedString(lscObj.Object, "status", "phase")
		g.Expect(phase).To(Equal("Created"), "LocalStorageClass phase should be Created, got %s", phase)

		var sc storagev1.StorageClass
		g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: name}, &sc)).To(Succeed())
	}, 5*time.Minute, 10*time.Second).Should(Succeed())
}

// fdCreatePVC creates a WaitForFirstConsumer PVC in the default namespace.
func fdCreatePVC(ctx context.Context, cl client.Client, name, storageClass, size string) {
	GinkgoHelper()
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: metav1.NamespaceDefault},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(size)},
			},
			StorageClassName: &storageClass,
		},
	}
	Expect(cl.Create(ctx, pvc)).To(Succeed(), "create PVC %s", name)
	DeferCleanup(func() {
		_ = client.IgnoreNotFound(cl.Delete(ctx, &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: metav1.NamespaceDefault},
		}))
	})
}

// fdRunPodToCompletion runs a one-shot Pod with the PVC mounted at /data and requires it to
// reach Succeeded — so the shell command itself is the assertion.
//
// The Pod goes through the scheduler on purpose: the PVC binds WaitForFirstConsumer, so the
// volume is provisioned only once kube-scheduler picks a node and stamps
// volume.kubernetes.io/selected-node on the claim. spec.nodeName would bypass the scheduler
// and leave the Pod in ContainerCreating forever.
//
// Pinning uses nodeAffinity matchFields on metadata.name rather than a
// kubernetes.io/hostname nodeSelector: that label is the OS hostname, which on this cluster
// does not equal the Kubernetes node name, so the selector matched nothing and the Pod sat
// Unschedulable ("2 node(s) didn't match Pod's node affinity/selector").
func fdRunPodToCompletion(ctx context.Context, cl client.Client, podName, pvcName, node, script string) {
	GinkgoHelper()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: podName, Namespace: metav1.NamespaceDefault},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Affinity: &corev1.Affinity{
				NodeAffinity: &corev1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
						NodeSelectorTerms: []corev1.NodeSelectorTerm{{
							MatchFields: []corev1.NodeSelectorRequirement{{
								Key:      "metadata.name",
								Operator: corev1.NodeSelectorOpIn,
								Values:   []string{node},
							}},
						}},
					},
				},
			},
			Tolerations: []corev1.Toleration{
				{Key: "node-role.kubernetes.io/control-plane", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule},
				{Key: "node-role.kubernetes.io/master", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule},
			},
			Containers: []corev1.Container{{
				Name:         "worker",
				Image:        "busybox",
				Command:      []string{"sh", "-c", script},
				VolumeMounts: []corev1.VolumeMount{{Name: "data", MountPath: "/data"}},
			}},
			Volumes: []corev1.Volume{{
				Name: "data",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvcName},
				},
			}},
		},
	}
	Expect(cl.Create(ctx, pod)).To(Succeed(), "create Pod %s", podName)
	DeferCleanup(func() {
		_ = client.IgnoreNotFound(cl.Delete(ctx, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: podName, Namespace: metav1.NamespaceDefault},
		}, client.GracePeriodSeconds(0)))
	})
	// Registered after the delete above, so it runs before it: the Pod and its
	// events must still exist for the dump to say anything.
	DeferCleanup(func() {
		if CurrentSpecReport().Failed() {
			fdDumpWorkloadDiagnostics(ctx, cl, podName, pvcName)
		}
	})

	Eventually(func(g Gomega) {
		var cur corev1.Pod
		g.Expect(cl.Get(ctx, client.ObjectKey{Namespace: metav1.NamespaceDefault, Name: podName}, &cur)).To(Succeed())
		if cur.Status.Phase == corev1.PodFailed {
			// A failed one-shot Pod means the in-Pod assertion itself failed; stop polling.
			StopTrying(fmt.Sprintf("Pod %s failed: %s", podName, fdPodTerminationSummary(&cur))).Now()
		}
		g.Expect(cur.Status.Phase).To(Equal(corev1.PodSucceeded),
			"Pod %s phase=%s: %s", podName, cur.Status.Phase, fdPodTerminationSummary(&cur))
	}, fdWorkloadTimeout, 10*time.Second).Should(Succeed())
}

// fdPodTerminationSummary renders container exit codes and reasons for a failure message.
func fdPodTerminationSummary(pod *corev1.Pod) string {
	var parts []string
	for _, cs := range pod.Status.ContainerStatuses {
		switch {
		case cs.State.Terminated != nil:
			parts = append(parts, fmt.Sprintf("%s terminated exit=%d reason=%s",
				cs.Name, cs.State.Terminated.ExitCode, cs.State.Terminated.Reason))
		case cs.State.Waiting != nil:
			parts = append(parts, fmt.Sprintf("%s waiting reason=%s msg=%s",
				cs.Name, cs.State.Waiting.Reason, cs.State.Waiting.Message))
		}
	}
	// A Pod that never starts has no container status at all, which is precisely
	// the case worth explaining: scheduling conditions carry the reason.
	for _, c := range pod.Status.Conditions {
		if c.Status != corev1.ConditionTrue {
			parts = append(parts, fmt.Sprintf("condition %s=%s reason=%s msg=%s",
				c.Type, c.Status, c.Reason, c.Message))
		}
	}
	if pod.Spec.NodeName == "" {
		parts = append(parts, "not scheduled to any node")
	}
	if len(parts) == 0 {
		return "no container status yet"
	}
	return strings.Join(parts, "; ")
}

// fdDumpWorkloadDiagnostics prints why a Pod is stuck. A Pending Pod on a
// WaitForFirstConsumer claim can be blocked by the scheduler (extender veto,
// nodeSelector), by provisioning (no PV yet) or by the mount — and the Pod's own
// status shows none of that. Events and the claim's state do.
func fdDumpWorkloadDiagnostics(ctx context.Context, cl client.Client, podName, pvcName string) {
	var pod corev1.Pod
	if err := cl.Get(ctx, client.ObjectKey{Namespace: metav1.NamespaceDefault, Name: podName}, &pod); err == nil {
		GinkgoWriter.Printf("Pod %s: phase=%s node=%q\n", pod.Name, pod.Status.Phase, pod.Spec.NodeName)
		for _, c := range pod.Status.Conditions {
			GinkgoWriter.Printf("  condition %s=%s reason=%s msg=%s\n", c.Type, c.Status, c.Reason, c.Message)
		}
	} else {
		GinkgoWriter.Printf("Pod %s: %v\n", podName, err)
	}

	var pvc corev1.PersistentVolumeClaim
	if err := cl.Get(ctx, client.ObjectKey{Namespace: metav1.NamespaceDefault, Name: pvcName}, &pvc); err == nil {
		GinkgoWriter.Printf("PVC %s: phase=%s volume=%q sc=%v selectedNode=%q\n",
			pvc.Name, pvc.Status.Phase, pvc.Spec.VolumeName, pvc.Spec.StorageClassName,
			pvc.Annotations["volume.kubernetes.io/selected-node"])
	} else {
		GinkgoWriter.Printf("PVC %s: %v\n", pvcName, err)
	}

	for _, name := range []string{podName, pvcName} {
		var events corev1.EventList
		if err := cl.List(ctx, &events, client.InNamespace(metav1.NamespaceDefault),
			client.MatchingFields{"involvedObject.name": name}); err != nil {
			GinkgoWriter.Printf("events for %s: %v\n", name, err)
			continue
		}
		for i := range events.Items {
			e := &events.Items[i]
			GinkgoWriter.Printf("  event %s/%s %s: %s\n", name, e.Type, e.Reason, e.Message)
		}
	}
}

// fdCleanupWorkload removes the Pods and PVCs these specs create and waits for the PVs to go,
// so the next spec's LVMVolumeGroup delete is not blocked by a live logical volume.
func fdCleanupWorkload(ctx context.Context, cl client.Client) {
	var pods corev1.PodList
	if err := cl.List(ctx, &pods, client.InNamespace(metav1.NamespaceDefault)); err == nil {
		for i := range pods.Items {
			if strings.HasPrefix(pods.Items[i].Name, podNamePrefix+"fd-") {
				_ = client.IgnoreNotFound(cl.Delete(ctx, &pods.Items[i], client.GracePeriodSeconds(0)))
			}
		}
	}

	var pvcs corev1.PersistentVolumeClaimList
	if err := cl.List(ctx, &pvcs, client.InNamespace(metav1.NamespaceDefault)); err == nil {
		for i := range pvcs.Items {
			if strings.HasPrefix(pvcs.Items[i].Name, pvcNamePrefix+"fd-") {
				_ = client.IgnoreNotFound(cl.Delete(ctx, &pvcs.Items[i]))
			}
		}
	}

	_ = framework.Poll(ctx, 5*time.Second, 5*time.Minute, func(ctx context.Context) (bool, error) {
		var list corev1.PersistentVolumeClaimList
		if err := cl.List(ctx, &list, client.InNamespace(metav1.NamespaceDefault)); err != nil {
			if apierrors.IsNotFound(err) {
				return true, nil
			}
			return false, err
		}
		for i := range list.Items {
			if strings.HasPrefix(list.Items[i].Name, pvcNamePrefix+"fd-") {
				return false, fmt.Errorf("PVC %s still present", list.Items[i].Name)
			}
		}
		return true, nil
	})
}
