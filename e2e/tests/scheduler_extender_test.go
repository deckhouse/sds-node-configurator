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

		By("Clearing leftovers from a previous run")
		deleteWorkload(ctx, k8sClient)
		cleanupLocalStorageClasses(ctx, cl)
		cleanupLVMLogicalVolumes(ctx, k8sClient)
		cleanupLVMVolumeGroups(ctx, k8sClient)
		forceDeleteAllNonConsumableBlockDevices(ctx, k8sClient, schedBDCleanupTimeout)

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
		dumpLVGs(ctx, k8sClient)
	})

	AfterEach(func() {
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

	AfterAll(func() {
		cleanupCtx := context.Background()

		// The LocalStorageClass must go before its LVMVolumeGroups: the sds-local-volume validating
		// webhook rejects every LSC update — including the controller's own finalizer removal — once a
		// referenced LVG is gone, which deadlocks the LSC in Terminating forever.
		By("AfterAll: deleting the LocalStorageClass")
		cleanupLocalStorageClasses(cleanupCtx, cl)

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

		By("AfterAll: force deleting leftover non-consumable BlockDevices")
		forceDeleteAllNonConsumableBlockDevices(cleanupCtx, k8sClient, schedBDCleanupTimeout)
	})
})
