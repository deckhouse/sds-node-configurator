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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-node-configurator/e2e/cfg"
	"github.com/deckhouse/sds-node-configurator/e2e/sdsclient"
	"github.com/deckhouse/storage-e2e/pkg/e2e"
)

// The four specs share one capacity layout and start from the same fixture, so they live in one
// Ordered container with a single BeforeAll. Unique labels are what makes them individually runnable;
// separate files would need registrar functions plus a state accessor, because Ginkgo builds the spec
// tree before any BeforeAll runs. ContinueOnFailure is deliberately absent: a broken fixture must stop
// the container instead of producing four cascading failures with unrelated messages.
var _ = Describe("Schedule extender", Label("schedule-extender"), Ordered, func() {
	var (
		ctx       context.Context
		conf      *cfg.Config
		cl        *e2e.Cluster
		k8sClient client.Client

		bigNode          string
		storageClassName string
		nodeDisks        []schedNodeDisk
	)

	BeforeAll(func() {
		ctx = context.Background()

		var cfgErr error
		conf, cfgErr = cfg.Load()
		Expect(cfgErr).NotTo(HaveOccurred(), "failed to load config")

		storageClass := conf.TestCluster.StorageClass
		Expect(storageClass).NotTo(BeEmpty(), "E2E_DVP_BASE_CLUSTER_STORAGE_CLASS is required to create data disks")

		var clErr error
		cl, clErr = e2e.Connect(ctx, e2e.WithTestName("schedule-extender"))
		Expect(clErr).NotTo(HaveOccurred(), "failed to connect to cluster")
		DeferCleanup(func() {
			if err := cl.Close(context.Background()); err != nil {
				GinkgoWriter.Println("Error closing cluster: ", err)
			}
		})

		var k8sErr error
		k8sClient, k8sErr = sdsclient.New(cl.RESTConfig())
		Expect(k8sErr).NotTo(HaveOccurred(), "failed to build controller-runtime client")

		waitExtenderRolledOut(ctx, k8sClient)

		By("Checking that no objects from an earlier run are left behind")
		assertNoLeftoversFromPreviousRun(ctx, cl, k8sClient)

		runID := fmt.Sprintf("%d", time.Now().Unix())
		var smallNodes []string
		bigNode, smallNodes = schedPickNodes(ctx, k8sClient)
		By(fmt.Sprintf("Capacity layout (runID %s): %s gets %s, %v get %s",
			runID, bigNode, schedBigDiskSize, smallNodes, schedSmallDiskSize))

		nodeDisks = append(nodeDisks, attachDiskWithLVG(ctx, cl, storageClass, runID, bigNode, schedBigDiskSize))
		for _, node := range smallNodes {
			nodeDisks = append(nodeDisks, attachDiskWithLVG(ctx, cl, storageClass, runID, node, schedSmallDiskSize))
		}

		lvgNames := make([]string, 0, len(nodeDisks))
		for _, disk := range nodeDisks {
			lvgNames = append(lvgNames, disk.lvgName)
		}
		storageClassName = createLocalStorageClass(ctx, cl, k8sClient, lvgNames)
		By(fmt.Sprintf("StorageClass %s ready", storageClassName))

		// Fixture invariant, not an extender behavior: if the provisioner quantizes volumes, this must
		// fail here with the observed VGFree numbers, not surface later as "the extender must steer".
		assertCapacityInvariant(ctx, k8sClient, bigNode, nodeDisks)
		dumpLVGs(ctx, k8sClient)
	})

	AfterEach(func() {
		if k8sClient == nil {
			return // BeforeAll failed before the client existed; let that failure be the one that shows
		}
		if CurrentSpecReport().Failed() {
			dumpLVGs(ctx, k8sClient)
		}
		deleteWorkload(ctx, k8sClient)
	})

	It("steers a Pod with a spec.volumes PVC to the only node that fits", Label("sched-steer-spec"), func() {
		const (
			pvcName = schedPVCPrefix + "steer-spec"
			podName = schedPodPrefix + "steer-spec"
		)

		createPVC(ctx, k8sClient, pvcName, storageClassName, schedSteerPVCSize)
		createPod(ctx, k8sClient, podName, podOpts{mountPVC: pvcName})

		Expect(waitPodScheduled(ctx, k8sClient, podName)).To(Equal(bigNode),
			"the extender must steer the Pod to %s: %s does not fit a %s VG on any other node",
			bigNode, schedSteerPVCSize, schedSmallDiskSize)

		waitPVCBoundAndPodRunning(ctx, k8sClient, pvcName, podName)
	})

	It("steers a launcher Pod whose PVC comes only from the annotation", Label("sched-steer-annotation"), func() {
		const (
			pvcName        = schedPVCPrefix + "steer-annotation"
			launcherName   = schedPodPrefix + "steer-annotation-launcher"
			attachmentName = schedPodPrefix + "steer-annotation-attachment"
		)

		// The PVC has to exist before the launcher is filtered: a PVC named only in the annotation and
		// missing at that moment is skipped as a stale hint, and the Pod would be placed anywhere.
		createPVC(ctx, k8sClient, pvcName, storageClassName, schedSteerPVCSize)
		createPod(ctx, k8sClient, launcherName, podOpts{annotationPVC: pvcName})

		launcherNode := waitPodScheduled(ctx, k8sClient, launcherName)
		Expect(launcherNode).To(Equal(bigNode),
			"the extender must read the PVC from the %s annotation and steer the launcher to %s",
			podExtraPVCsAnnotation, bigNode)

		// Nothing mounts the PVC yet, so it is still Pending: the attachment Pod is its first consumer
		// and is pinned to wherever the launcher landed. If the launcher went to a small node, this Pod
		// is pinned there too, the volume cannot be created and the spec goes red.
		createPod(ctx, k8sClient, attachmentName, podOpts{mountPVC: pvcName, node: launcherNode})
		waitPVCBoundAndPodRunning(ctx, k8sClient, pvcName, attachmentName)
	})

	It("blocks a Pod whose spec.volumes PVC fits nowhere", Label("sched-block-spec"), func() {
		const (
			pvcName     = schedPVCPrefix + "block-spec"
			podName     = schedPodPrefix + "block-spec"
			controlName = schedPodPrefix + "block-spec-control"
		)

		createPVC(ctx, k8sClient, pvcName, storageClassName, schedBlockPVCSize)
		createPod(ctx, k8sClient, podName, podOpts{mountPVC: pvcName})
		expectPodRejectedByExtender(ctx, k8sClient, podName)

		// A/B control. Pending on its own proves nothing: the Pod could be stuck for an unrelated
		// reason. The same Pod without our PVC must schedule. The two legs do not interfere: a full
		// hard reject leaves no filtered node, so no reservation is created, and a Pod with no managed
		// PVC short-circuits the filter into a no-op.
		createPod(ctx, k8sClient, controlName, podOpts{})
		Expect(waitPodScheduled(ctx, k8sClient, controlName)).NotTo(BeEmpty(),
			"a Pod without our PVC must schedule — otherwise the Pending above is not about storage")
	})

	It("blocks a Pod whose annotation PVC fits nowhere", Label("sched-block-annotation"), func() {
		const (
			pvcName     = schedPVCPrefix + "block-annotation"
			podName     = schedPodPrefix + "block-annotation"
			controlName = schedPodPrefix + "block-annotation-control"
		)

		createPVC(ctx, k8sClient, pvcName, storageClassName, schedBlockPVCSize)
		createPod(ctx, k8sClient, podName, podOpts{annotationPVC: pvcName})

		// Strict on purpose, with no in-tree fallback reason: kube-scheduler's VolumeBinding does not
		// know about this PVC at all, so the only component that can reject every node is the extender.
		expectPodRejectedByExtender(ctx, k8sClient, podName)

		// A/B control: the same Pod without the annotation must schedule.
		createPod(ctx, k8sClient, controlName, podOpts{})
		Expect(waitPodScheduled(ctx, k8sClient, controlName)).NotTo(BeEmpty(),
			"a Pod without the annotation must schedule — otherwise the Pending above is not about storage")
	})

	AfterAll(func() {
		if k8sClient == nil || cl == nil {
			return // BeforeAll failed before the cluster connection existed; nothing here to tear down
		}

		cleanupCtx := context.Background()

		// AfterEach's deleteWorkload can itself fail (e.g. its 5-minute PV wait times out) and AfterAll
		// still runs. Deleting the LVMVolumeGroups out from under a PV that has not finished deleting
		// leaves csi-provisioner retrying DeleteVolume against a VG that no longer exists — a retry that
		// can never succeed, wedging the PV's finalizer and every later run's BeforeAll cleanup forever.
		// Force-finalizing is not the answer (that is the bug class this suite exists to catch), so
		// instead: refuse the whole destructive sequence and fail loudly.
		if left, err := leftoverPVs(cleanupCtx, k8sClient); err != nil {
			Fail(fmt.Sprintf("AfterAll: failed to list PersistentVolumes before teardown: %v", err))
		} else if len(left) > 0 {
			GinkgoWriter.Println("AfterAll: PersistentVolumes below are still present; skipping LocalStorageClass/LVMVolumeGroup/disk teardown")
			for i := range left {
				pv := &left[i]
				GinkgoWriter.Printf("  PV %s phase=%s finalizers=%v\n", pv.Name, pv.Status.Phase, pv.Finalizers)
			}
			Fail(fmt.Sprintf(
				"%d PersistentVolume(s) still bound to StorageClass %s must finish deleting before the "+
					"LocalStorageClass and LVMVolumeGroups can be removed safely", len(left), localStorageClassName))
		}

		// The LocalStorageClass must go before its LVMVolumeGroups: the sds-local-volume validating
		// webhook rejects every LSC update — including the controller's own finalizer removal — once a
		// referenced LVG is gone, which deadlocks the LSC in Terminating forever.
		By("AfterAll: deleting the LocalStorageClass")
		cleanupLocalStorageClasses(cleanupCtx, cl, k8sClient)

		By("AfterAll: deleting LVMLogicalVolumes and LVMVolumeGroups")
		cleanupLVMLogicalVolumes(cleanupCtx, k8sClient)
		cleanupLVMVolumeGroups(cleanupCtx, k8sClient)

		By("AfterAll: detaching and deleting data disks")
		for _, disk := range nodeDisks {
			if detachErr := cl.Disks().DetachDisk(cleanupCtx, disk.node, disk.diskName); detachErr != nil {
				GinkgoWriter.Printf("failed to detach disk %s from node %s: %v\n", disk.diskName, disk.node, detachErr)
			}
			if deleteErr := cl.Disks().DeleteDisk(cleanupCtx, disk.diskName); deleteErr != nil {
				GinkgoWriter.Printf("failed to delete disk %s: %v\n", disk.diskName, deleteErr)
			}
		}

		// No BlockDevice cleanup here on purpose: once the disks above are detached, the devices are
		// gone from the nodes and the agent's discoverer removes their BlockDevice CRs itself
		// (removeDeprecatedAPIDevices). Deleting them from the test would only race that reconcile.
	})
})
