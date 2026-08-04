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

// The origin is deliberately not a round number of extents' worth of MiB in the
// request: 35Mi rounds up to 36Mi on a 4Mi extent, which is the drift the CSI
// side used to leave in Spec.Size. The clone then asks for a strictly larger
// size, because `lvcreate -s` produces an LV of the origin size and the agent
// has to extend it before reporting Created.
const (
	llvCloneOriginRequest = "35Mi"
	llvCloneOriginActual  = "36Mi"
	llvCloneTargetRequest = "52Mi"

	llvCloneReadyTimeout = 5 * time.Minute
	llvClonePollInterval = 5 * time.Second
)

var _ = Describe("LVMLogicalVolume clone", Label("sds-node-configurator", "lvmlogicalvolume"), Ordered, ContinueOnFailure, func() {
	var (
		ctx       context.Context
		conf      *cfg.Config
		cl        *e2e.Cluster
		k8sClient client.Client

		targetNode string
		runID      string

		createdDisks []*e2e.Disk

		lvgName      string
		vgName       string
		thinPoolName string
	)

	BeforeAll(func() {
		ctx = context.Background()

		var cfgErr error
		conf, cfgErr = cfg.Load()
		Expect(cfgErr).NotTo(HaveOccurred(), "failed to load config")

		var clErr error
		cl, clErr = e2e.Connect(ctx, e2e.WithTestName("lvmlogicalvolume-clone"))
		Expect(clErr).NotTo(HaveOccurred(), "failed to connect to cluster")
		DeferCleanup(func() {
			if err := cl.Close(context.Background()); err != nil {
				GinkgoWriter.Println("Error closing cluster: ", err)
			}
		})

		var k8sErr error
		k8sClient, k8sErr = sdsclient.New(cl.RESTConfig())
		Expect(k8sErr).NotTo(HaveOccurred(), "failed to build controller-runtime client")

		nodeList, nlErr := cl.Clientset().CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		Expect(nlErr).NotTo(HaveOccurred(), "failed to list nodes")
		Expect(nodeList.Items).NotTo(BeEmpty(), "cluster must have at least one node")
		targetNode = nodeList.Items[0].Name

		runID = fmt.Sprintf("%d", time.Now().Unix())

		By("Snapshotting consumable BlockDevices before attach")
		before, bdErr := kubernetes.GetConsumableBlockDevicesByNode(ctx, cl.RESTConfig(), targetNode)
		Expect(bdErr).NotTo(HaveOccurred())

		diskName := fmt.Sprintf("e2e-llv-clone-disk-%s", runID)
		By("Creating and attaching a virtual disk: " + diskName)
		disk, createErr := cl.Disks().CreateDisk(ctx, e2e.DiskSpec{
			Name:         diskName,
			Size:         resource.MustParse("2Gi"),
			StorageClass: conf.TestCluster.StorageClass,
		})
		Expect(createErr).NotTo(HaveOccurred(), "failed to create disk")
		createdDisks = append(createdDisks, disk)
		Expect(cl.Disks().AttachDisk(ctx, targetNode, disk.Name)).To(Succeed(), "failed to attach disk")

		By("Waiting for the new consumable BlockDevice on node " + targetNode)
		newBD, waitErr := framework.WaitNewConsumableBlockDevice(ctx, cl.RESTConfig(), targetNode, before, 5*time.Minute)
		Expect(waitErr).NotTo(HaveOccurred())

		vgName = "e2e-vg-llv-clone-" + runID
		thinPoolName = "e2e-thin-pool-clone"
		lvgName = lvmVGNamePrefix + "llv-clone-" + runID

		By(fmt.Sprintf("Creating LVMVolumeGroup %s on node %s, VG %s, thin-pool %s", lvgName, targetNode, vgName, thinPoolName))
		Expect(kubernetes.CreateLVMVolumeGroupWithThinPool(
			ctx, cl.RESTConfig(), lvgName, targetNode, []string{newBD.Name}, vgName,
			[]kubernetes.ThinPoolSpec{{Name: thinPoolName, Size: "60%", AllocationLimit: "150%"}},
		)).To(Succeed())

		By("Waiting for LVMVolumeGroup to become Ready")
		Eventually(func(g Gomega) {
			var lvg v1alpha1.LVMVolumeGroup
			g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: lvgName}, &lvg)).To(Succeed())
			// The conditions go into the failure message, not just the phase. A bare
			// "got Pending" after a 15-minute wait says nothing about which condition
			// is missing or False, and this wait is in a BeforeAll — so when it times
			// out the whole suite reports one undiagnosable failure and the log holds
			// no other trace of it.
			g.Expect(lvg.Status.Phase).To(Equal(v1alpha1.PhaseReady),
				"Phase should be Ready, got %s; %s", lvg.Status.Phase, describeLVGStatus(&lvg))
		}, lvmVolumeGroupReadyTimeout, 10*time.Second).Should(Succeed())
	})

	AfterAll(func() {
		By("Cleaning up e2e LVMLogicalVolumes")
		cleanupLVMLogicalVolumes(ctx, k8sClient)

		// Before the LVMVolumeGroups, not after: the agent refuses to delete a
		// Volume Group that still has logical volumes (getLVForVG counts the
		// module's own thin pool), so deleting the resource first ends in
		// cleanupLVMVolumeGroups force-removing the finalizer and leaving the Volume
		// Group, its thin pool and its Physical Volume on the node — after which the
		// disk is detached from underneath them and every later spec on this node
		// works against the debris.
		By("Removing the thin-pool stack on the node")
		_, _ = framework.NodeExecChecked(ctx, cl, targetNode, framework.RemoveThinPoolStackScript(vgName, thinPoolName))

		By("Cleaning up e2e LVMVolumeGroups")
		cleanupLVMVolumeGroups(ctx, k8sClient)

		By("Detaching and deleting test disks")
		for _, d := range createdDisks {
			if d == nil {
				continue
			}
			if err := cl.Disks().DetachDisk(ctx, targetNode, d.Name); err != nil {
				GinkgoWriter.Printf("failed to detach disk %v: %v\n", d.Name, err)
			}
			if err := cl.Disks().DeleteDisk(ctx, d.Name); err != nil {
				GinkgoWriter.Printf("failed to delete disk %v: %v\n", d.Name, err)
			}
		}
	})

	// Regression test for the restore-from-snapshot hang: a thin LV created from a
	// source via `lvcreate -s` inherits the origin size, so a clone requested at a
	// larger size used to stay at the origin size forever. The CSI controller waits
	// for Status.ActualSize to reach the request before returning from CreateVolume,
	// so the PVC stayed Pending indefinitely.
	It("Should extend a cloned LVMLogicalVolume to the requested size", func() {
		originName := "e2e-llv-clone-origin-" + runID
		originLVName := "e2e-lv-clone-origin-" + runID

		By(fmt.Sprintf("Creating the origin LVMLogicalVolume %s with size %s", originName, llvCloneOriginRequest))
		origin := &v1alpha1.LVMLogicalVolume{
			ObjectMeta: metav1.ObjectMeta{Name: originName},
			Spec: v1alpha1.LVMLogicalVolumeSpec{
				ActualLVNameOnTheNode: originLVName,
				Type:                  "Thin",
				Size:                  llvCloneOriginRequest,
				LVMVolumeGroupName:    lvgName,
				Thin:                  &v1alpha1.LVMLogicalVolumeThinSpec{PoolName: thinPoolName},
			},
		}
		Expect(k8sClient.Create(ctx, origin)).To(Succeed())

		By("Waiting for the origin to become Created")
		originActual := waitLLVCreated(ctx, k8sClient, originName)

		// The node provisions whole extents, so a 35Mi request lands on a 36Mi LV.
		By(fmt.Sprintf("Verifying the origin was rounded up to the extent boundary (%s)", llvCloneOriginActual))
		expectedOrigin := resource.MustParse(llvCloneOriginActual)
		Expect(originActual.Value()).To(Equal(expectedOrigin.Value()),
			"origin ActualSize should be the extent-aligned %s, got %s", llvCloneOriginActual, originActual.String())

		cloneName := "e2e-llv-clone-target-" + runID
		cloneLVName := "e2e-lv-clone-target-" + runID

		By(fmt.Sprintf("Creating the clone %s from %s with a larger size %s", cloneName, originName, llvCloneTargetRequest))
		clone := &v1alpha1.LVMLogicalVolume{
			ObjectMeta: metav1.ObjectMeta{Name: cloneName},
			Spec: v1alpha1.LVMLogicalVolumeSpec{
				ActualLVNameOnTheNode: cloneLVName,
				Type:                  "Thin",
				Size:                  llvCloneTargetRequest,
				LVMVolumeGroupName:    lvgName,
				Thin:                  &v1alpha1.LVMLogicalVolumeThinSpec{PoolName: thinPoolName},
				Source: &v1alpha1.LVMLogicalVolumeSource{
					Kind: "LVMLogicalVolume",
					Name: originName,
				},
			},
		}
		Expect(k8sClient.Create(ctx, clone)).To(Succeed())

		By("Waiting for the clone to become Created")
		cloneActual := waitLLVCreated(ctx, k8sClient, cloneName)

		By(fmt.Sprintf("Verifying the clone reached the requested size %s and not the origin size %s", llvCloneTargetRequest, llvCloneOriginActual))
		expectedClone := resource.MustParse(llvCloneTargetRequest)
		Expect(cloneActual.Value()).To(BeNumerically(">=", expectedClone.Value()),
			"clone ActualSize %s is below the requested %s: the LV was not extended after `lvcreate -s`",
			cloneActual.String(), llvCloneTargetRequest)

		By("Verifying the size LVM reports on the node matches the status")
		out, execErr := framework.NodeExecChecked(ctx, cl, targetNode,
			fmt.Sprintf("lvs --noheadings --units b --nosuffix -o lv_size /dev/%s/%s 2>/dev/null || sudo -n lvs --noheadings --units b --nosuffix -o lv_size /dev/%s/%s",
				vgName, cloneLVName, vgName, cloneLVName))
		Expect(execErr).NotTo(HaveOccurred(), "failed to read the LV size from the node")
		Expect(strings.TrimSpace(out)).To(Equal(fmt.Sprintf("%d", cloneActual.Value())),
			"LVM reports a different size than Status.ActualSize")

		By("✓ the cloned LVMLogicalVolume was extended from the origin size to the requested size")
	})
})

// waitLLVCreated blocks until the LVMLogicalVolume reaches the Created phase and
// returns the ActualSize the agent published, failing the spec on Failed.
func waitLLVCreated(ctx context.Context, cl client.Client, name string) resource.Quantity {
	GinkgoHelper()

	var llv v1alpha1.LVMLogicalVolume
	Eventually(func(g Gomega) {
		g.Expect(cl.Get(ctx, client.ObjectKey{Name: name}, &llv)).To(Succeed())
		g.Expect(llv.Status).NotTo(BeNil(), "LVMLogicalVolume %s has no status yet", name)
		g.Expect(llv.Status.Phase).NotTo(Equal(v1alpha1.PhaseFailed),
			"LVMLogicalVolume %s failed: %s", name, llv.Status.Reason)
		g.Expect(llv.Status.Phase).To(Equal(v1alpha1.PhaseCreated),
			"LVMLogicalVolume %s phase=%s (waiting for Created)", name, llv.Status.Phase)
	}, llvCloneReadyTimeout, llvClonePollInterval).Should(Succeed())

	return llv.Status.ActualSize
}
