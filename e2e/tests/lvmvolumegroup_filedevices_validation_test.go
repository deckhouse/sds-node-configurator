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

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/e2e/sdsclient"
	"github.com/deckhouse/storage-e2e/pkg/e2e"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Rejections of a bad spec.fileDevices, split into two layers:
//
//   - the CRD's CEL rules, enforced by the apiserver at admission — these never reach a
//     node and cannot be covered by the agent's unit tests, so an apiserver is the only
//     place they can be exercised at all;
//   - the agent's own validation, which surfaces as VGConfigurationApplied=False and must
//     additionally leave nothing behind on the node.
var _ = Describe("LVMVolumeGroup file-backed devices validation",
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
			cl, clErr = e2e.Connect(ctx, e2e.WithTestName("lvmvolumegroup-filedevices-validation"))
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
		})

		AfterAll(func() {
			if k8sClient != nil {
				cleanupLVMVolumeGroups(ctx, k8sClient)
			}
		})

		// ---=== CRD CEL rules (apiserver admission) ===--- //

		Context("CRD validation rules", func() {
			It("Should reject an LVMVolumeGroup with neither blockDeviceSelector nor fileDevices", func() {
				lvgName := lvmVGNamePrefix + "fdcel-empty-" + runID
				lvg := fdNewLVG(fdNoSuchNode, lvgName, "e2e-vg-fdcel-empty-"+runID, nil, nil, nil)

				By("Creating an LVMVolumeGroup with no device source at all")
				err := k8sClient.Create(ctx, lvg)
				if err == nil {
					DeferCleanup(func() { fdForceDeleteLVG(ctx, k8sClient, lvgName) })
				}
				Expect(err).To(HaveOccurred(), "the apiserver should reject an LVG with no device source")
				Expect(err.Error()).To(ContainSubstring("At least one of 'blockDeviceSelector' or 'fileDevices'"),
					"unexpected admission error: %v", err)

				By("✓ CEL rule rejects an LVMVolumeGroup without a device source")
			})

			It("Should reject editing the directory of an existing fileDevices entry", func() {
				lvgName := lvmVGNamePrefix + "fdcel-dir-" + runID
				fdExpectImmutableFileDeviceEdit(ctx, k8sClient, lvgName, "e2e-vg-fdcel-dir-"+runID,
					"'directory' is immutable",
					func(lvg *v1alpha1.LVMVolumeGroup) {
						lvg.Spec.FileDevices[0].Directory = fdBaseDir + "/moved"
					})

				By("✓ CEL rule rejects editing fileDevices[0].directory")
			})

			// `size` may grow but never shrink. This is not a policy preference:
			// growth is `fallocate -l` to a larger size, and the same command to a
			// smaller one truncates the file — under a live Physical Volume that
			// destroys data. The agent refuses it too, but admission is where it
			// should stop.
			It("Should reject shrinking an existing fileDevices entry", func() {
				lvgName := lvmVGNamePrefix + "fdcel-shrink-" + runID
				fdExpectImmutableFileDeviceEdit(ctx, k8sClient, lvgName, "e2e-vg-fdcel-shrink-"+runID,
					"'size' can only be increased",
					func(lvg *v1alpha1.LVMVolumeGroup) {
						lvg.Spec.FileDevices[0].Size = resource.MustParse("512Mi")
					})

				By("✓ CEL rule rejects shrinking fileDevices[0].size")
			})

			// The units must be compared as quantities, not as strings: 1024Mi is
			// the same size as 1Gi, and 512Mi is smaller than it despite the larger
			// number in front.
			It("Should reject a shrink written in a different unit", func() {
				lvgName := lvmVGNamePrefix + "fdcel-shrinku-" + runID
				fdExpectImmutableFileDeviceEdit(ctx, k8sClient, lvgName, "e2e-vg-fdcel-shrinku-"+runID,
					"'size' can only be increased",
					func(lvg *v1alpha1.LVMVolumeGroup) {
						// The entry is created at 1Gi; 900Mi is less, in another unit.
						lvg.Spec.FileDevices[0].Size = resource.MustParse("900Mi")
					})

				By("✓ CEL rule compares sizes as quantities, not as strings")
			})

			It("Should allow raising the size of an existing fileDevices entry", func() {
				lvgName := lvmVGNamePrefix + "fdcel-grow-" + runID
				vgName := "e2e-vg-fdcel-grow-" + runID

				lvg := fdNewLVG(fdNoSuchNode, lvgName, vgName, nil,
					[]v1alpha1.LVMVolumeGroupFileDeviceSpec{
						{Name: "data-0", Directory: fdBaseDir, Size: resource.MustParse("1Gi")},
					}, nil)
				Expect(k8sClient.Create(ctx, lvg)).To(Succeed())
				DeferCleanup(func() { fdForceDeleteLVG(ctx, k8sClient, lvgName) })

				Eventually(func(g Gomega) error {
					var cur v1alpha1.LVMVolumeGroup
					g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: lvgName}, &cur)).To(Succeed())
					cur.Spec.FileDevices[0].Size = resource.MustParse("2Gi")
					return k8sClient.Update(ctx, &cur)
				}, 1*time.Minute, 5*time.Second).Should(Succeed(), "growth must be accepted at admission")

				By("✓ CEL rule accepts raising fileDevices[0].size")
			})

			// The counterpart to the rejections above, and the reason the rule is
			// scoped to (directory, size) of a surviving name rather than "nothing may
			// ever be removed": an entry the agent refused to provision has to be
			// removable, or one typo wedges a live LVMVolumeGroup for good — the spec
			// cannot be fixed and recreating the resource means destroying the data.
			It("Should allow removing a fileDevices entry that was never provisioned", func() {
				lvgName := lvmVGNamePrefix + "fdcel-drop-" + runID
				vgName := "e2e-vg-fdcel-drop-" + runID

				By("Creating an LVMVolumeGroup with a valid entry and one the agent will reject")
				lvg := fdNewLVG(fdNoSuchNode, lvgName, vgName, nil,
					[]v1alpha1.LVMVolumeGroupFileDeviceSpec{
						{Name: "keep", Directory: fdBaseDir, Size: resource.MustParse("1Gi")},
						{Name: "typo", Directory: fdBaseDir, Size: resource.MustParse("1024Mi")},
					}, nil)
				Expect(k8sClient.Create(ctx, lvg)).To(Succeed())
				DeferCleanup(func() { fdForceDeleteLVG(ctx, k8sClient, lvgName) })

				By("Dropping the unprovisioned entry")
				Eventually(func(g Gomega) error {
					var cur v1alpha1.LVMVolumeGroup
					g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: lvgName}, &cur)).To(Succeed())
					cur.Spec.FileDevices = []v1alpha1.LVMVolumeGroupFileDeviceSpec{cur.Spec.FileDevices[0]}
					return k8sClient.Update(ctx, &cur)
				}, 1*time.Minute, 5*time.Second).Should(Succeed(),
					"an entry absent from status must be removable")

				var after v1alpha1.LVMVolumeGroup
				Expect(k8sClient.Get(ctx, client.ObjectKey{Name: lvgName}, &after)).To(Succeed())
				Expect(after.Spec.FileDevices).To(HaveLen(1))
				Expect(after.Spec.FileDevices[0].Name).To(Equal("keep"))

				By("✓ An unprovisioned fileDevices entry can be removed")
			})
		})

		// ---=== Agent-side validation (VGConfigurationApplied) ===--- //

		Context("Agent validation", func() {
			It("Should reject a fileDevices directory outside the allowed base dir", func() {
				lvgName := lvmVGNamePrefix + "fdbad-" + runID
				vgName := "e2e-vg-fdbad-" + runID
				badDir := "/tmp/e2e-filedevices-" + runID

				By(fmt.Sprintf("Creating LVMVolumeGroup %s with an out-of-base directory %s", lvgName, badDir))
				lvg := fdNewLVG(targetNode, lvgName, vgName, nil,
					[]v1alpha1.LVMVolumeGroupFileDeviceSpec{{Name: "d1g", Directory: badDir, Size: resource.MustParse("1Gi")}}, nil)
				Expect(k8sClient.Create(ctx, lvg)).To(Succeed())
				DeferCleanup(func() { fdDeleteLVGAndWaitGone(ctx, k8sClient, lvgName) })

				fdWaitVGConfigurationApplied(ctx, k8sClient, lvgName,
					"or a subdirectory of it")

				By("Verifying no VG and no backing directory were created on the node")
				vgListed, vgsOut, err := fdVGListedOnNode(ctx, cl, targetNode, vgName)
				Expect(err).NotTo(HaveOccurred())
				Expect(vgListed).To(BeFalse(), "VG %s must not be created for a rejected spec; vgs:\n%s", vgName, vgsOut)

				dirExists, err := fdNodePathExists(ctx, cl, targetNode, badDir)
				Expect(err).NotTo(HaveOccurred())
				Expect(dirExists).To(BeFalse(), "the out-of-base directory %s must not be created", badDir)

				By("✓ Out-of-base fileDevices directory rejected; nothing provisioned on the node")
			})

			// "1G" is 10^9 bytes, below the 1Gi (2^30) minimum — the classic slip. Two
			// independent CRD rules catch it before it reaches a node: the `size`
			// pattern accepts binary units only, and the per-item CEL rule enforces the
			// minimum.
			It("Should reject a decimal fileDevices size at admission", func() {
				lvgName := lvmVGNamePrefix + "fddec-" + runID
				vgName := "e2e-vg-fddec-" + runID

				By("Creating LVMVolumeGroup with size 1G (decimal)")
				lvg := fdNewLVG(fdNoSuchNode, lvgName, vgName, nil,
					[]v1alpha1.LVMVolumeGroupFileDeviceSpec{{Name: "decimal", Directory: fdBaseDir, Size: resource.MustParse("1G")}}, nil)
				err := k8sClient.Create(ctx, lvg)
				if err == nil {
					DeferCleanup(func() { fdForceDeleteLVG(ctx, k8sClient, lvgName) })
				}
				Expect(err).To(HaveOccurred(), "the apiserver should reject a decimal size")
				Expect(err.Error()).To(ContainSubstring("spec.fileDevices"),
					"unexpected admission error: %v", err)

				By("✓ Decimal fileDevices size rejected at admission")
			})

			// A binary unit below the minimum passes the `size` pattern, so the CRD
			// carries a per-item CEL rule for it — the pattern and the minimum are two
			// separate rules and only the second one catches "512Mi". Admission is the
			// right place for it: `size` cannot be lowered afterwards, so an entry
			// accepted below the minimum is one the agent would refuse forever.
			//
			// The agent keeps its own minimum check, but it is no longer reachable
			// through the API — only through an object the discoverer imported from a
			// node — so it is asserted by unit tests (validateFileDevice) rather than
			// here. Asserting it here instead is what made this spec fail once the CEL
			// rule landed: it waited for a condition on a resource the apiserver had
			// refused to create.
			It("Should reject a fileDevices size below the 1Gi minimum at admission", func() {
				lvgName := lvmVGNamePrefix + "fdsmall-" + runID
				vgName := "e2e-vg-fdsmall-" + runID

				By("Creating LVMVolumeGroup with size 512Mi (below the 1Gi minimum)")
				lvg := fdNewLVG(fdNoSuchNode, lvgName, vgName, nil,
					[]v1alpha1.LVMVolumeGroupFileDeviceSpec{{Name: "toosmall", Directory: fdBaseDir, Size: resource.MustParse("512Mi")}}, nil)
				err := k8sClient.Create(ctx, lvg)
				if err == nil {
					DeferCleanup(func() { fdForceDeleteLVG(ctx, k8sClient, lvgName) })
				}
				Expect(err).To(HaveOccurred(), "the apiserver should reject a sub-1Gi size")
				Expect(err.Error()).To(ContainSubstring("spec.fileDevices"),
					"unexpected admission error: %v", err)
				Expect(err.Error()).To(ContainSubstring("at least 1Gi"),
					"the rejection must name the minimum, not some other rule: %v", err)

				By("✓ Sub-minimum fileDevices size rejected at admission")
			})

			// Two identical entries are rejected by the apiserver, not by the agent: the
			// CRD declares fileDevices as a set, so admission reports
			// `name` is the list-map key, so two entries sharing it are rejected as
			// spec.fileDevices[1]: Duplicate value before anything reaches a node. The
			// agent's own collision check stays as defence in depth.
			It("Should reject two fileDevices entries with the same name at admission", func() {
				lvgName := lvmVGNamePrefix + "fdcollide-" + runID
				vgName := "e2e-vg-fdcollide-" + runID

				By("Creating LVMVolumeGroup with two identical fileDevices entries")
				lvg := fdNewLVG(fdNoSuchNode, lvgName, vgName, nil,
					[]v1alpha1.LVMVolumeGroupFileDeviceSpec{
						{Name: "d1g", Directory: fdBaseDir, Size: resource.MustParse("1Gi")},
						{Name: "d1g", Directory: fdBaseDir, Size: resource.MustParse("1Gi")},
					}, nil)
				err := k8sClient.Create(ctx, lvg)
				if err == nil {
					DeferCleanup(func() { fdForceDeleteLVG(ctx, k8sClient, lvgName) })
				}
				Expect(err).To(HaveOccurred(), "the apiserver should reject duplicate fileDevices entries")
				Expect(err.Error()).To(ContainSubstring("Duplicate value"),
					"unexpected admission error: %v", err)

				By("✓ Duplicate fileDevices entries rejected at admission")
			})

			It("Should reject a relative fileDevices directory", func() {
				lvgName := lvmVGNamePrefix + "fdrel-" + runID
				vgName := "e2e-vg-fdrel-" + runID

				By("Creating LVMVolumeGroup with a relative directory")
				lvg := fdNewLVG(targetNode, lvgName, vgName, nil,
					[]v1alpha1.LVMVolumeGroupFileDeviceSpec{{Name: "d1g", Directory: "opt/deckhouse/sds/file-devices", Size: resource.MustParse("1Gi")}}, nil)
				err := k8sClient.Create(ctx, lvg)
				if err != nil {
					// A CRD pattern may already reject a relative path at admission; either
					// layer catching it is a pass, but say which one did.
					By(fmt.Sprintf("Rejected by the apiserver before reaching the agent: %v", err))
					By("✓ Relative fileDevices directory rejected at admission")
					return
				}
				DeferCleanup(func() { fdDeleteLVGAndWaitGone(ctx, k8sClient, lvgName) })

				fdWaitVGConfigurationApplied(ctx, k8sClient, lvgName,
					"must be an absolute path")

				fdExpectNothingProvisioned(ctx, cl, targetNode, lvgName, vgName)

				By("✓ Relative fileDevices directory rejected by the agent")
			})

			// `fallocate -l` preallocates the full size, so without the statfs guard one
			// oversized entry fills the node filesystem and trips kubelet DiskPressure —
			// a node-level outage rather than a condition error. This is also the only
			// cheap way to drive the provisioning rollback path from e2e.
			It("Should refuse a backing file larger than the node's free space and roll back cleanly", func() {
				lvgName := lvmVGNamePrefix + "fdspace-" + runID
				vgName := "e2e-vg-fdspace-" + runID

				By("Reading the free space statfs reports for the base dir")
				avail, err := fdNodeAvailableBytes(ctx, cl, targetNode, fdBaseDir)
				Expect(err).NotTo(HaveOccurred())
				Expect(avail).To(BeNumerically(">", 0), "expected positive free space under %s", fdBaseDir)

				// Round up to the next whole GiB past the free space so the request is
				// unambiguously too large even if the node frees a little in the meantime.
				const gib = int64(1) << 30
				requested := ((avail / gib) + 4) * gib
				GinkgoWriter.Printf("free under %s: %d bytes; requesting %d bytes\n", fdBaseDir, avail, requested)

				By(fmt.Sprintf("Creating LVMVolumeGroup asking for %d bytes", requested))
				lvg := fdNewLVG(targetNode, lvgName, vgName, nil,
					[]v1alpha1.LVMVolumeGroupFileDeviceSpec{
						{Name: "toobig", Directory: fdBaseDir, Size: *resource.NewQuantity(requested, resource.BinarySI)},
					}, nil)
				Expect(k8sClient.Create(ctx, lvg)).To(Succeed())
				DeferCleanup(func() { fdDeleteLVGAndWaitGone(ctx, k8sClient, lvgName) })

				By("Expecting the VG to stay unconfigured with a free-space error")
				Eventually(func(g Gomega) {
					var cur v1alpha1.LVMVolumeGroup
					g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: lvgName}, &cur)).To(Succeed())
					fdPrintLVG(&cur)
					g.Expect(cur.Status.Phase).NotTo(Equal(v1alpha1.PhaseReady))

					var applied *metav1.Condition
					for i := range cur.Status.Conditions {
						if cur.Status.Conditions[i].Type == fdConditionVGConfigurationApplied {
							applied = &cur.Status.Conditions[i]
							break
						}
					}
					g.Expect(applied).NotTo(BeNil(), "VGConfigurationApplied condition should be present")
					g.Expect(applied.Status).To(Equal(metav1.ConditionFalse))
					g.Expect(applied.Message).To(ContainSubstring("not enough free space"),
						"unexpected message: %s", applied.Message)
				}, 5*time.Minute, 10*time.Second).Should(Succeed())

				By("Verifying the guard fired before fallocate: no VG and no backing file on the node")
				fdExpectNothingProvisioned(ctx, cl, targetNode, lvgName, vgName)

				By("✓ Oversized backing file refused by the free-space guard; nothing left behind")
			})
		})
	})

// fdExpectImmutableFileDeviceEdit creates a file-only LVMVolumeGroup, applies mutate to its
// spec and asserts the apiserver rejects the update with wantMessage.
//
// wantMessage is a parameter because the two invariants are two separate CEL rules
// on fileDevices[] with their own messages — an edit to `directory` and a shrink of
// `size` must each be reported for what it is.
//
// The LVG targets fdNoSuchNode so no agent ever claims it: CEL runs at admission and needs
// no node, while a schedulable LVG would be provisioned for real — three backing files and
// loops allocated and torn down just to exercise an API rule, racing the other specs'
// cleanup (one such race surfaced as pvcreate exiting 5 on /dev/loop0).
func fdExpectImmutableFileDeviceEdit(
	ctx context.Context,
	cl client.Client,
	lvgName, vgName, wantMessage string,
	mutate func(*v1alpha1.LVMVolumeGroup),
) {
	GinkgoHelper()
	lvg := fdNewLVG(fdNoSuchNode, lvgName, vgName, nil,
		[]v1alpha1.LVMVolumeGroupFileDeviceSpec{{Name: "frozen", Directory: fdBaseDir, Size: resource.MustParse("1Gi")}}, nil)
	Expect(cl.Create(ctx, lvg)).To(Succeed())
	DeferCleanup(func() { fdForceDeleteLVG(ctx, cl, lvgName) })

	By("Attempting the forbidden edit")
	var updateErr error
	Eventually(func(g Gomega) {
		var cur v1alpha1.LVMVolumeGroup
		g.Expect(cl.Get(ctx, client.ObjectKey{Name: lvgName}, &cur)).To(Succeed())
		mutate(&cur)
		updateErr = cl.Update(ctx, &cur)
		// Retry only on an optimistic-concurrency conflict; an admission denial is the
		// result we are after and must not be retried away.
		g.Expect(apierrors.IsConflict(updateErr)).To(BeFalse(), "resource version conflict, retrying")
	}, 1*time.Minute, 5*time.Second).Should(Succeed())

	Expect(updateErr).To(HaveOccurred(), "the apiserver should reject the edit")
	// Matches one of the per-entry CEL rule messages in crds/lvmvolumegroup.yaml.
	Expect(updateErr.Error()).To(ContainSubstring(wantMessage),
		"unexpected admission error: %v", updateErr)
}
