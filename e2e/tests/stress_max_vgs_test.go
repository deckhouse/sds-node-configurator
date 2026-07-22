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

// --- Stress env knobs (ported from the legacy suite, `stress`-prefixed) ---
// Many independent LVMVolumeGroups (1 disk → 1 BlockDevice → 1 VG) on a single node.

const (
	// stressAttachTimeout bounds a single AttachDisk call so a VM that has hit its
	// block-device attachment limit surfaces as an error instead of hanging the ramp.
	stressAttachTimeout = 15 * time.Minute
	// stressBDDiscoveryTimeout bounds waiting for the agent to publish a new consumable BlockDevice.
	stressBDDiscoveryTimeout = 5 * time.Minute
	// stressCleanupTimeout bounds each best-effort teardown operation.
	stressCleanupTimeout = 10 * time.Minute
)

// stressVGSlot is one independent VG (one Disk → BlockDevice → LVMVolumeGroup) on a single node.
type stressVGSlot struct {
	index    int
	diskName string
	disk     *e2e.Disk
	attached bool
	bdName   string
	lvgName  string
	vgName   string
	ready    bool
}

// stressBatchReadyTimeout scales with batch size: many concurrent LVM ops on one node need headroom.
func stressBatchReadyTimeout(batchLen int) time.Duration {
	if batchLen <= 0 {
		return lvmVolumeGroupReadyTimeout
	}
	t := time.Duration(batchLen) * 4 * time.Minute
	if t < lvmVolumeGroupReadyTimeout {
		return lvmVolumeGroupReadyTimeout
	}
	const maxBatch = 3 * time.Hour
	if t > maxBatch {
		return maxBatch
	}
	return t
}

// stressNodeSafe converts a node name into a DNS-1123-safe fragment for CR names.
func stressNodeSafe(n string) string {
	return strings.ReplaceAll(strings.ReplaceAll(n, ".", "-"), "_", "-")
}

// stressAttachErrIndicatesLimit reports whether an AttachDisk/CreateDisk error looks like the
// provider's VM block-device attachment limit (vs a transient failure). The DVP DiskManager does
// not return a typed admission error — a full VM surfaces as a Failed VMBDA phase or an attach
// timeout — so we match on error text and otherwise treat any repeated attach failure as the stop
// condition (logged by the caller).
func stressAttachErrIndicatesLimit(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	for _, kw := range []string{
		"limit", "maximum", "too many", "exceed", "no more", "capacity",
		"admission", "denied", "forbidden", "hotplug", "phase failed",
	} {
		if strings.Contains(s, kw) {
			return true
		}
	}
	return false
}

// stressWaitLVGBatchReady polls until every LVMVolumeGroup in [from,to) is Ready or timeout expires.
// Returns false on timeout (probe mode stops the ramp without failing the spec).
func stressWaitLVGBatchReady(ctx context.Context, cl client.Client, slots []stressVGSlot, from, to int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	poll := 15 * time.Second
	for {
		allReady := true
		for i := from; i < to; i++ {
			var cur v1alpha1.LVMVolumeGroup
			if err := cl.Get(ctx, client.ObjectKey{Name: slots[i].lvgName}, &cur); err != nil {
				allReady = false
				break
			}
			if cur.Status.Phase != v1alpha1.PhaseReady {
				allReady = false
				break
			}
		}
		if allReady {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(poll)
	}
}

// stressMarkSlotsReady marks slots in [from,to) whose LVMVolumeGroup is already Ready and returns the count.
func stressMarkSlotsReady(ctx context.Context, cl client.Client, slots []stressVGSlot, from, to int) int {
	n := 0
	for i := from; i < to; i++ {
		var cur v1alpha1.LVMVolumeGroup
		if err := cl.Get(ctx, client.ObjectKey{Name: slots[i].lvgName}, &cur); err != nil {
			continue
		}
		if cur.Status.Phase == v1alpha1.PhaseReady {
			slots[i].ready = true
			n++
		}
	}
	return n
}

// stressCountLinesOnNode runs cmd on the node (one item per line) and returns the non-empty line count.
func stressCountLinesOnNode(ctx context.Context, cl *e2e.Cluster, node, cmd string) (int, string, error) {
	out, err := framework.NodeExecChecked(ctx, cl, node, cmd)
	if err != nil {
		return 0, out, err
	}
	count := 0
	for _, line := range strings.Split(out, "\n") {
		if strings.TrimSpace(line) != "" {
			count++
		}
	}
	return count, out, nil
}

// stressCountVGsOnNode returns how many volume groups `vgs` lists on the node.
func stressCountVGsOnNode(ctx context.Context, cl *e2e.Cluster, node string) (int, string, error) {
	return stressCountLinesOnNode(ctx, cl, node, `sudo -n vgs -o vg_name --noheadings 2>/dev/null | sed '/^$/d'`)
}

// stressCountPVsOnNode returns how many physical volumes `pvs` lists on the node.
func stressCountPVsOnNode(ctx context.Context, cl *e2e.Cluster, node string) (int, string, error) {
	return stressCountLinesOnNode(ctx, cl, node, `sudo -n pvs -o pv_name --noheadings 2>/dev/null | sed '/^$/d'`)
}

func stressPrintReport(nodeName string, target, ready, batchSize, maxVMBlockDevices int, strict, stoppedEarly, virtLimitReached bool, slots []stressVGSlot, vgCount, pvCount int) {
	GinkgoWriter.Printf("\n========== Stress: max independent VGs per node — report ==========\n")
	GinkgoWriter.Printf("  node: %s\n", nodeName)
	GinkgoWriter.Printf("  target LVMVolumeGroups: %d (batch size %d; capped by E2E_STRESS_MAX_VM_BLOCK_DEVICES=%d)\n",
		target, batchSize, maxVMBlockDevices)
	GinkgoWriter.Printf("  Ready LVMVolumeGroups: %d\n", ready)
	GinkgoWriter.Printf("  strict mode (all target must be Ready): %v\n", strict)
	if stoppedEarly {
		if virtLimitReached {
			GinkgoWriter.Println("  stopped early: Deckhouse virtualization VM block-device limit (16 attachments per VM).")
		} else {
			GinkgoWriter.Println("  stopped early: attach failure or batch LVMVolumeGroup Ready timeout.")
		}
	}
	GinkgoWriter.Printf("  on-node vgs count (all VGs): %d\n", vgCount)
	GinkgoWriter.Printf("  on-node pvs count (all PVs): %d\n", pvCount)
	for _, s := range slots {
		phase := "<not created>"
		if s.lvgName != "" {
			phase = "Pending/missing"
		}
		if s.ready {
			phase = "Ready"
		}
		bd := "-"
		if s.bdName != "" {
			bd = s.bdName
		}
		GinkgoWriter.Printf("  [%02d] disk=%s lvg=%s vg=%s bd=%s phase=%s\n",
			s.index, s.diskName, s.lvgName, s.vgName, bd, phase)
	}
	GinkgoWriter.Println("  LVM2 has no documented hard cap on VG count; on a single VM the Deckhouse virt controller")
	GinkgoWriter.Println("  caps block-device attachments at 16 (leave headroom for boot disks — default stress target 15).")
	GinkgoWriter.Println("  Beyond that, practical limits depend on metadata size, udev, and agent throughput.")
	GinkgoWriter.Println("====================================================================")
}

// Label stress-test: excluded from default smoke; run via E2E_GINKGO_LABEL_FILTER=stress-test.
var _ = Describe("Stress: maximum independent LVMVolumeGroups per node", Label("sds-node-configurator", "stress-test"), Ordered, func() {
	var (
		ctx       context.Context
		conf      *cfg.Config
		stressCfg *cfg.Stress
		cl        *e2e.Cluster
		k8s       client.Client
		nodeName  string
		nodeSafe  string
		runID     string

		slots         []stressVGSlot
		attachedDisks []*e2e.Disk
		createdBDs    []string
	)

	BeforeAll(func() {
		By("Preparing shared test context and Kubernetes clients")
		ctx = context.Background()
		conf = cfg.Load()

		var stressErr error
		stressCfg, stressErr = cfg.LoadStress()
		Expect(stressErr).NotTo(HaveOccurred(), "invalid E2E_STRESS_* config")

		var clErr error
		cl, clErr = e2e.Connect(ctx, e2e.WithTestName("stress-max-vgs"))
		Expect(clErr).NotTo(HaveOccurred(), "failed to connect to cluster")
		DeferCleanup(func() {
			if err := cl.Close(context.Background()); err != nil {
				GinkgoWriter.Println("Error closing cluster: ", err)
			}
		})

		var k8sErr error
		k8s, k8sErr = sdsclient.New(cl.RESTConfig())
		Expect(k8sErr).NotTo(HaveOccurred(), "failed to build controller-runtime client")

		By("Listing nodes to select the single target node")
		nodeList, nlErr := cl.Clientset().CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		Expect(nlErr).NotTo(HaveOccurred(), "failed to list nodes")
		Expect(nodeList.Items).NotTo(BeEmpty(), "cluster must have at least one node")
		nodeName = nodeList.Items[0].Name
		nodeSafe = stressNodeSafe(nodeName)

		Expect(conf.TestCluster.StorageClass).NotTo(BeEmpty(), "TestCluster.StorageClass required")

		runID = fmt.Sprintf("%d", time.Now().Unix())
	})

	AfterAll(func() {
		// 1) Delete e2e LVMVolumeGroups first so the agent removes the on-node VGs before disks detach.
		if k8s != nil {
			cleanupLVMVolumeGroups(ctx, k8s)
			forceDeleteBlockDevicesByNames(ctx, k8s, createdBDs)
		}

		// 2) Detach and delete every disk this spec attached.
		for i := len(attachedDisks) - 1; i >= 0; i-- {
			d := attachedDisks[i]
			if d == nil {
				continue
			}
			detachCtx, cancel := context.WithTimeout(context.Background(), stressCleanupTimeout)
			if err := cl.Disks().DetachDisk(detachCtx, nodeName, d.Name); err != nil {
				GinkgoWriter.Printf("cleanup: detach disk %s: %v\n", d.Name, err)
			}
			cancel()

			deleteCtx, cancelDel := context.WithTimeout(context.Background(), stressCleanupTimeout)
			if err := cl.Disks().DeleteDisk(deleteCtx, d.Name); err != nil {
				GinkgoWriter.Printf("cleanup: delete disk %s: %v\n", d.Name, err)
			}
			cancelDel()
		}
	})

	It("Should ramp independent LVMVolumeGroups on one node until target or the attachment limit", func() {
		target := stressCfg.Target
		batchSize := stressCfg.BatchSize
		diskSize := stressCfg.DiskSize
		strict := stressCfg.Strict
		minReady := stressCfg.MinReady

		readyTotal := 0
		batchNum := 0
		stoppedEarly := false
		virtLimitReached := false

		By(fmt.Sprintf("Stress ramp: target=%d batch=%d diskSize=%s node=%q (max VM block devices=%d)",
			target, batchSize, diskSize, nodeName, stressCfg.MaxVMBlockDevices))

		for batchStart := 0; batchStart < target && !stoppedEarly; batchStart += batchSize {
			batchEnd := batchStart + batchSize
			if batchEnd > target {
				batchEnd = target
			}
			batchNum++

			By(fmt.Sprintf("Batch %d: disks/LVGs [%d..%d)", batchNum, batchStart, batchEnd))

			batchFrom := len(slots)
			for i := batchStart; i < batchEnd && !stoppedEarly; i++ {
				idx := i + 1
				slot := stressVGSlot{
					index:    idx,
					diskName: fmt.Sprintf("e2e-stress-vg-d%d-%s", idx, runID),
					vgName:   fmt.Sprintf("e2e-vg-stress-%d-%s", idx, runID),
					// lvgName MUST start with lvmVGNamePrefix so cleanupLVMVolumeGroups reaps it.
					lvgName: fmt.Sprintf("%sstress-%d-%s-%s", lvmVGNamePrefix, idx, runID, nodeSafe),
				}

				before, listErr := kubernetes.GetConsumableBlockDevicesByNode(ctx, cl.RESTConfig(), nodeName)
				Expect(listErr).NotTo(HaveOccurred(), "list consumable BlockDevices before disk %s", slot.diskName)

				disk, createErr := cl.Disks().CreateDisk(ctx, e2e.DiskSpec{
					Name:         slot.diskName,
					Size:         resource.MustParse(diskSize),
					StorageClass: conf.TestCluster.StorageClass,
				})
				if createErr != nil {
					GinkgoWriter.Printf("    create disk %s failed: %v — stopping ramp (%d Ready so far)\n",
						slot.diskName, createErr, readyTotal)
					stoppedEarly = true
					virtLimitReached = stressAttachErrIndicatesLimit(createErr)
					break
				}
				slot.disk = disk
				attachedDisks = append(attachedDisks, disk)

				attachCtx, cancelAttach := context.WithTimeout(ctx, stressAttachTimeout)
				attachErr := cl.Disks().AttachDisk(attachCtx, nodeName, disk.Name)
				cancelAttach()
				if attachErr != nil {
					virtLimitReached = stressAttachErrIndicatesLimit(attachErr)
					GinkgoWriter.Printf("    attach disk %s failed (limit=%v): %v — stopping ramp (%d Ready so far)\n",
						disk.Name, virtLimitReached, attachErr, readyTotal)
					stoppedEarly = true
					break
				}
				slot.attached = true

				bd, bdErr := framework.WaitNewConsumableBlockDevice(ctx, cl.RESTConfig(), nodeName, before, stressBDDiscoveryTimeout)
				if bdErr != nil {
					GinkgoWriter.Printf("    new consumable BlockDevice for disk %s not discovered: %v — stopping ramp\n",
						disk.Name, bdErr)
					stoppedEarly = true
					break
				}
				slot.bdName = bd.Name
				createdBDs = append(createdBDs, bd.Name)

				Expect(kubernetes.CreateLVMVolumeGroup(ctx, cl.RESTConfig(), slot.lvgName, nodeName, []string{bd.Name}, slot.vgName)).
					To(Succeed(), "create LVMVolumeGroup %s (batch %d)", slot.lvgName, batchNum)

				slots = append(slots, slot)
			}

			batchTo := len(slots)
			createdInBatch := batchTo - batchFrom
			if createdInBatch == 0 {
				break
			}

			batchTimeout := stressBatchReadyTimeout(createdInBatch)
			if !stressWaitLVGBatchReady(ctx, k8s, slots, batchFrom, batchTo, batchTimeout) {
				GinkgoWriter.Printf("    batch %d: not all %d LVMVolumeGroups Ready within %v — stopping ramp\n",
					batchNum, createdInBatch, batchTimeout)
				readyTotal += stressMarkSlotsReady(ctx, k8s, slots, batchFrom, batchTo)
				stoppedEarly = true
				break
			}
			for i := batchFrom; i < batchTo; i++ {
				slots[i].ready = true
			}
			readyTotal += createdInBatch

			if vgN, _, err := stressCountVGsOnNode(ctx, cl, nodeName); err == nil {
				GinkgoWriter.Printf("    batch %d OK: cumulative Ready=%d; on-node vgs=%d\n", batchNum, readyTotal, vgN)
			}
		}

		vgCount, vgOut, errVG := stressCountVGsOnNode(ctx, cl, nodeName)
		if errVG != nil {
			GinkgoWriter.Printf("    vgs count on node failed: %v (output %q)\n", errVG, vgOut)
		}
		pvCount, pvOut, errPV := stressCountPVsOnNode(ctx, cl, nodeName)
		if errPV != nil {
			GinkgoWriter.Printf("    pvs count on node failed: %v (output %q)\n", errPV, pvOut)
		}

		stressPrintReport(nodeName, target, readyTotal, batchSize, stressCfg.MaxVMBlockDevices, strict, stoppedEarly, virtLimitReached, slots, vgCount, pvCount)

		if strict {
			Expect(readyTotal).To(Equal(target),
				"strict mode: expected %d Ready LVMVolumeGroups on %s; got %d (stopped early=%v)",
				target, nodeName, readyTotal, stoppedEarly)
		} else {
			Expect(readyTotal).To(BeNumerically(">=", minReady),
				"probe mode: expected at least %d Ready LVMVolumeGroups; got %d (target %d, stopped early=%v)",
				minReady, readyTotal, target, stoppedEarly)
		}

		if readyTotal > 0 {
			for i := range slots {
				if !slots[i].ready {
					continue
				}
				vgsRes, err := cl.Nodes().Exec(ctx, nodeName, `sudo -n vgs -o vg_name --noheadings 2>/dev/null | sed '/^$/d'`)
				out := string(vgsRes.Stdout)
				Expect(err).NotTo(HaveOccurred(), "vgs listing on node %s failed: %q", nodeName, out)
				listed := framework.VGInListing(out, slots[i].vgName)
				Expect(listed).To(BeTrue(), "vgs should list stress VG %q on node %s", slots[i].vgName, nodeName)
			}
		}

		By(fmt.Sprintf("✓ Stress ramp finished: %d/%d LVMVolumeGroups Ready on node %s", readyTotal, target, nodeName))
	})
})
