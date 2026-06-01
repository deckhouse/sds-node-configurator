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
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/storage-e2e/pkg/cluster"
	"github.com/deckhouse/storage-e2e/pkg/kubernetes"
)

// stressVGSlot is one independent VG (one VirtualDisk → BlockDevice → LVMVolumeGroup) on a single node.
type stressVGSlot struct {
	index    int
	diskName string
	att      *kubernetes.VirtualDiskAttachmentResult
	bd       *v1alpha1.BlockDevice
	meta     string
	lvgName  string
	vgName   string
	ready    bool
}

// e2eStressBatchReadyTimeout scales with batch size: many concurrent LVM ops on one node need headroom.
func e2eStressBatchReadyTimeout(batchLen int) time.Duration {
	if batchLen <= 0 {
		return e2eLVMVolumeGroupReadyTimeout
	}
	t := time.Duration(batchLen) * 4 * time.Minute
	if t < e2eLVMVolumeGroupReadyTimeout {
		return e2eLVMVolumeGroupReadyTimeout
	}
	const maxBatch = 3 * time.Hour
	if t > maxBatch {
		return maxBatch
	}
	return t
}

func e2eStressPrintReport(nodeName string, target, ready, batchSize int, strict, stoppedEarly, virtLimitReached bool, slots []stressVGSlot, vgCount, pvCount int) {
	GinkgoWriter.Printf("\n========== Stress: max independent VGs per node — report ==========\n")
	GinkgoWriter.Printf("  node: %s\n", nodeName)
	GinkgoWriter.Printf("  target LVMVolumeGroups: %d (batch size %d; capped by E2E_STRESS_MAX_VM_BLOCK_DEVICES=%d)\n",
		target, batchSize, e2eStressMaxVMBlockDevices())
	GinkgoWriter.Printf("  Ready LVMVolumeGroups: %d\n", ready)
	GinkgoWriter.Printf("  strict mode (all target must be Ready): %v\n", strict)
	if stoppedEarly {
		if virtLimitReached {
			GinkgoWriter.Println("  stopped early: Deckhouse virtualization VM block-device limit (16 attachments per VM).")
		} else {
			GinkgoWriter.Println("  stopped early: batch LVMVolumeGroup Ready timeout or no further attach progress.")
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
		GinkgoWriter.Printf("  [%02d] disk=%s lvg=%s vg=%s bd=%s phase=%s\n",
			s.index, s.diskName, s.lvgName, s.vgName, stressBDName(s), phase)
	}
	GinkgoWriter.Println("  LVM2 has no documented hard cap on VG count; on a single VM the Deckhouse virt controller")
	GinkgoWriter.Println("  caps block-device attachments at 16 (leave headroom for boot disks — default stress target 15).")
	GinkgoWriter.Println("  Beyond that, practical limits depend on metadata size, udev, and agent throughput.")
	GinkgoWriter.Println("====================================================================\n")
}

func stressBDName(s stressVGSlot) string {
	if s.bd == nil {
		return "-"
	}
	return s.bd.Name
}

// e2eWaitStressLVGBatchReady polls until every LVMVolumeGroup in [from,to) is Ready or timeout expires.
// Returns false on timeout (probe mode stops the ramp without failing the spec).
func e2eWaitStressLVGBatchReady(ctx context.Context, cl client.Client, slots []stressVGSlot, from, to int, timeout time.Duration) bool {
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

func e2eMarkStressSlotsReady(ctx context.Context, cl client.Client, slots []stressVGSlot, from, to int) int {
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

// Label stress-test: excluded from default smoke (suite label filter e2e-tests). Run via make test-stress or E2E_GINKGO_LABEL_FILTER=stress-test.
var _ = Describe("Stress: maximum independent LVMVolumeGroups per node", Label(e2eGinkgoLabelStressTest), Ordered, func() {
	var (
		e2eCtx      context.Context
		res         *cluster.TestClusterResources
		k8sClient   client.Client
		stressRunID string
		stressOnce  sync.Once
		slots       []stressVGSlot
		nodeName    string
		attachments []*kubernetes.VirtualDiskAttachmentResult
	)

	BeforeAll(func() {
		e2eCtx = context.Background()
		res = e2eNestedTestClusterOrNil()
		Expect(res).NotTo(BeNil(), "nested test cluster must exist (BeforeSuite)")
		Expect(res.BaseKubeconfig).NotTo(BeNil(), "stress test requires nested virtualization (base cluster kubeconfig)")
		ensureE2EK8sClient(res, &k8sClient, e2eCtx)
		stressRunID = fmt.Sprintf("%d", time.Now().Unix())
	})

	BeforeEach(func() {
		stressOnce.Do(func() {
			prepCtx, prepCancel := context.WithTimeout(context.Background(), e2eClusterCleanupTimeout)
			defer prepCancel()
			By("Stress max-VGs: cleanup before test")
			cleanupE2EPodsAndPVCsWithWait(prepCtx, k8sClient, e2eSuitePodPVCleanupPodTimeout, e2eSuitePodPVCleanupPVTimeout)
			cleanupE2ELVMLogicalVolumes(prepCtx, k8sClient)
			cleanupE2ELVMVolumeGroups(prepCtx, k8sClient)
			cleanupE2ELocalStorageClasses(prepCtx, res.Kubeconfig)
			cleanupE2EVirtualDisks(prepCtx, res.BaseKubeconfig, e2eConfigNamespace(), e2eSuiteVirtualDiskPrefix)
			forceDeleteAllNonConsumableBlockDevices(prepCtx, k8sClient, 2*time.Minute)
			forceDeleteAllBlockDevices(prepCtx, k8sClient, 3*time.Minute)
		})
	})

	AfterEach(func() {
		if res == nil || res.BaseKubeconfig == nil {
			return
		}
		ns := e2eConfigNamespace()
		for _, att := range attachments {
			if att == nil {
				continue
			}
			_ = kubernetes.DetachAndDeleteVirtualDisk(e2eCtx, res.BaseKubeconfig, ns, att.AttachmentName, att.DiskName)
		}
		attachments = nil
		for i := len(slots) - 1; i >= 0; i-- {
			if slots[i].lvgName == "" {
				continue
			}
			_ = client.IgnoreNotFound(k8sClient.Delete(e2eCtx, &v1alpha1.LVMVolumeGroup{ObjectMeta: metav1.ObjectMeta{Name: slots[i].lvgName}}))
		}
	})

	It("Should ramp independent LVMVolumeGroups on one node until target or first failing batch", func() {
		target := e2eStressMaxVGTarget()
		batchSize := e2eStressMaxVGBatchSize()
		diskSize := e2eStressMaxVGDiskSize()
		strict := e2eStressMaxVGStrict()
		minReady := e2eStressMaxVGMinReady(target)

		ns := e2eConfigNamespace()
		storageClass := e2eConfigStorageClass()
		Expect(storageClass).NotTo(BeEmpty())

		clusterVMs := e2eListClusterVMNames(e2eCtx, res, ns)
		Expect(clusterVMs).NotTo(BeEmpty())
		targetVM := clusterVMs[0]
		nodeSafe := ""

		slots = make([]stressVGSlot, 0, target)
		readyTotal := 0
		batchNum := 0
		stoppedEarly := false
		virtLimitReached := false

		By(fmt.Sprintf("Stress ramp: target=%d batch=%d diskSize=%s VM=%q (max VM block devices=%d)",
			target, batchSize, diskSize, targetVM, e2eStressMaxVMBlockDevices()))

		for batchStart := 0; batchStart < target && !stoppedEarly; batchStart += batchSize {
			batchEnd := batchStart + batchSize
			if batchEnd > target {
				batchEnd = target
			}
			curBatch := batchEnd - batchStart
			batchNum++

			By(fmt.Sprintf("Batch %d: disks/LVGs [%d..%d) (%d slots)", batchNum, batchStart, batchEnd, curBatch))

			for i := batchStart; i < batchEnd; i++ {
				idx := i + 1
				slot := stressVGSlot{
					index:    idx,
					diskName: fmt.Sprintf("%sd%d-%s", e2eStressMaxVGNamePrefix, idx, stressRunID),
					vgName:   fmt.Sprintf("e2e-vg-stress-%d-%s", idx, stressRunID),
				}
				slots = append(slots, slot)
			}

			batchEndAttached := batchEnd
			for i := batchStart; i < batchEnd; i++ {
				att, err := attachVirtualDiskWithRetry(e2eCtx, res.BaseKubeconfig, kubernetes.VirtualDiskAttachmentConfig{
					VMName: targetVM, Namespace: ns, DiskName: slots[i].diskName,
					DiskSize: diskSize, StorageClassName: storageClass,
				}, e2eVirtualDiskAttachMaxRetries, e2eVirtualDiskAttachRetryInterval)
				if e2eVirtVMBlockDeviceLimitReached(err) {
					GinkgoWriter.Printf("    VM %q block-device attachment limit reached at disk %s — stopping ramp (%d Ready so far)\n",
						targetVM, slots[i].diskName, readyTotal)
					stoppedEarly = true
					virtLimitReached = true
					slots = slots[:i]
					batchEndAttached = i
					break
				}
				Expect(err).NotTo(HaveOccurred(), "attach disk %s", slots[i].diskName)
				slots[i].att = att
				attachments = append(attachments, att)
			}
			if batchEndAttached == batchStart {
				break
			}
			batchEnd = batchEndAttached
			curBatch = batchEnd - batchStart

			for i := batchStart; i < batchEnd; i++ {
				attachCtx, cancel := context.WithTimeout(e2eCtx, e2eVirtualDiskAttachWaitTimeout)
				Expect(kubernetes.WaitForVirtualDiskAttached(attachCtx, res.BaseKubeconfig, ns, slots[i].att.AttachmentName, 10*time.Second)).To(Succeed())
				cancel()
			}

			metaSeen := make(map[string]struct{})
			for i := batchStart; i < batchEnd; i++ {
				bd := e2eWaitConsumableBlockDeviceForVirtualDisk(e2eCtx, res.BaseKubeconfig, k8sClient, ns,
					slots[i].att.DiskName, slots[i].att.AttachmentName, targetVM)
				slots[i].bd = bd
				if nodeName == "" {
					nodeName = bd.Status.NodeName
					nodeSafe = strings.ReplaceAll(strings.ReplaceAll(nodeName, ".", "-"), "_", "-")
				} else {
					Expect(bd.Status.NodeName).To(Equal(nodeName), "all BlockDevices must be on the same node")
				}
				meta := bd.Labels["kubernetes.io/metadata.name"]
				if meta == "" {
					meta = bd.Name
				}
				Expect(metaSeen).NotTo(HaveKey(meta), "duplicate BlockDevice selector meta %q", meta)
				metaSeen[meta] = struct{}{}
				slots[i].meta = meta
				slots[i].lvgName = fmt.Sprintf("%s%d-%s-%s", e2eStressMaxLVGNamePrefix, slots[i].index, stressRunID, nodeSafe)
			}

			for i := batchStart; i < batchEnd; i++ {
				lvg := &v1alpha1.LVMVolumeGroup{
					ObjectMeta: metav1.ObjectMeta{Name: slots[i].lvgName},
					Spec: v1alpha1.LVMVolumeGroupSpec{
						ActualVGNameOnTheNode: slots[i].vgName,
						BlockDeviceSelector: &metav1.LabelSelector{
							MatchLabels: map[string]string{
								"kubernetes.io/hostname":      nodeName,
								"kubernetes.io/metadata.name": slots[i].meta,
							},
						},
						Type:  "Local",
						Local: v1alpha1.LVMVolumeGroupLocalSpec{NodeName: nodeName},
					},
				}
				Expect(k8sClient.Create(e2eCtx, lvg)).To(Succeed(),
					"create LVMVolumeGroup %s (batch %d)", slots[i].lvgName, batchNum)
			}

			batchTimeout := e2eStressBatchReadyTimeout(curBatch)
			if !e2eWaitStressLVGBatchReady(e2eCtx, k8sClient, slots, batchStart, batchEnd, batchTimeout) {
				GinkgoWriter.Printf("    batch %d: not all LVMVolumeGroups Ready within %v — stopping ramp\n", batchNum, batchTimeout)
				stoppedEarly = true
				readyTotal += e2eMarkStressSlotsReady(e2eCtx, k8sClient, slots, batchStart, batchEnd)
				break
			}
			for i := batchStart; i < batchEnd; i++ {
				slots[i].ready = true
			}
			readyTotal += curBatch

			vmSSH := e2eConfigVMSSHUser()
			if vgN, _, err := e2eCountVGsOnNode(e2eCtx, res.Kubeconfig, nodeName, vmSSH); err == nil {
				if pvN, _, errPV := e2eCountPVsOnNode(e2eCtx, res.Kubeconfig, nodeName, vmSSH); errPV == nil {
					GinkgoWriter.Printf("    batch %d OK: cumulative Ready=%d; on-node vgs=%d pvs=%d\n", batchNum, readyTotal, vgN, pvN)
				}
			}
			if virtLimitReached {
				break
			}
		}

		vmSSH := e2eConfigVMSSHUser()
		vgCount, vgOut, errVG := e2eCountVGsOnNode(e2eCtx, res.Kubeconfig, nodeName, vmSSH)
		if errVG != nil {
			GinkgoWriter.Printf("    vgs count on node failed: %v (output %q)\n", errVG, vgOut)
		}
		pvCount, pvOut, errPV := e2eCountPVsOnNode(e2eCtx, res.Kubeconfig, nodeName, vmSSH)
		if errPV != nil {
			GinkgoWriter.Printf("    pvs count on node failed: %v (output %q)\n", errPV, pvOut)
		}

		e2eStressPrintReport(nodeName, target, readyTotal, batchSize, strict, stoppedEarly, virtLimitReached, slots, vgCount, pvCount)

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
			vgsCmd := "vgs -o vg_name --noheadings 2>/dev/null || sudo -n vgs -o vg_name --noheadings 2>/dev/null"
			outVgs, errVgs := e2eExecOnTestClusterNodeSSH(e2eCtx, res.Kubeconfig, nodeName, vmSSH, vgsCmd)
			Expect(errVgs).NotTo(HaveOccurred())
			for i := range slots {
				if !slots[i].ready {
					continue
				}
				Expect(e2eVgNameListedInVgsOutput(outVgs, slots[i].vgName)).To(BeTrue(),
					"vgs should list stress VG %q", slots[i].vgName)
			}
		}

		By(fmt.Sprintf("✓ Stress ramp finished: %d/%d LVMVolumeGroups Ready on node %s", readyTotal, target, nodeName))
	})
})
