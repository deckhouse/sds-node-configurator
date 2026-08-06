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
	"github.com/deckhouse/sds-node-configurator/e2e/sdsclient"
	"github.com/deckhouse/storage-e2e/pkg/e2e"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Supporting spec.fileDevices meant the agent had to stop rejecting loop devices wholesale
// in its LVM scan (see utils.FilterForeignPVs / FilterForeignLoopPVs). These specs pin down
// the replacement boundary: a loop-backed VG the agent did not create must never be adopted,
// counted, or cleaned up — including one whose backing file mimics the agent's own naming.
var _ = Describe("LVMVolumeGroup file-backed devices foreign-loop isolation",
	Label("sds-node-configurator", "lvmvolumegroup", "file-devices"), Ordered, ContinueOnFailure, func() {
		var (
			ctx        context.Context
			cl         *e2e.Cluster
			k8sClient  client.Client
			targetNode string
			runID      string
			foreignDir string
		)

		BeforeAll(func() {
			By("Preparing shared test context and Kubernetes clients")
			ctx = context.Background()

			var clErr error
			cl, clErr = e2e.Connect(ctx, e2e.WithTestName("lvmvolumegroup-filedevices-foreign"))
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
			// Outside the managed base dir: this stands in for nested LVM in a guest VM,
			// which has no reason to live under /opt/deckhouse/sds.
			foreignDir = "/var/tmp/e2e-foreign-loop-" + runID
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

		It("Should not adopt an unmanaged loop-backed VG on the node", func() {
			foreignVG := "e2e-foreign-vg-" + runID

			By(fmt.Sprintf("Creating an untagged loop-backed VG %s in %s directly on the node", foreignVG, foreignDir))
			foreign := fdCreateForeignLoopVG(ctx, cl, targetNode, foreignDir, "foreign-"+runID+".img", foreignVG)
			DeferCleanup(func() { fdDestroyForeignLoopVG(ctx, cl, foreign) })

			By("Restarting the agent so a full discovery pass runs against the foreign VG")
			Expect(restartAgentOnNode(ctx, cl, targetNode)).To(Succeed())

			By("Verifying the agent never turns the foreign loop PV into a BlockDevice or LVMVolumeGroup")
			Consistently(func(g Gomega) {
				var lvgs v1alpha1.LVMVolumeGroupList
				g.Expect(k8sClient.List(ctx, &lvgs)).To(Succeed())
				for i := range lvgs.Items {
					g.Expect(lvgs.Items[i].Spec.ActualVGNameOnTheNode).NotTo(Equal(foreignVG),
						"LVMVolumeGroup %s adopted the foreign VG %s", lvgs.Items[i].Name, foreignVG)
				}

				var bds v1alpha1.BlockDeviceList
				g.Expect(k8sClient.List(ctx, &bds)).To(Succeed())
				for i := range bds.Items {
					bd := &bds.Items[i]
					g.Expect(bd.Status.ActualVGNameOnTheNode).NotTo(Equal(foreignVG),
						"BlockDevice %s was linked to the foreign VG %s", bd.Name, foreignVG)
					g.Expect(bd.Status.Path).NotTo(Equal(foreign.Loop),
						"BlockDevice %s was created for the foreign loop %s", bd.Name, foreign.Loop)
				}
			}, 2*time.Minute, 15*time.Second).Should(Succeed())

			By("Verifying the foreign VG is untouched on the node")
			fdExpectForeignLoopVGIntact(ctx, cl, foreign)

			By("✓ Unmanaged loop-backed VG neither adopted nor disturbed")
		})

		// Ownership is decided by the LVG name embedded in the basename, not by the
		// sds-<...>.img pattern alone — otherwise anyone could park a file in the base dir
		// and have the agent delete it.
		It("Should not claim a foreign backing file that mimics the managed naming", func() {
			otherLVGName := "e2e-lvg-not-mine-" + runID
			mimicBasename := fdBackingFileName(otherLVGName, "d1")
			foreignVG := "e2e-foreign-mimic-vg-" + runID

			By(fmt.Sprintf("Placing a mimicking backing file %s inside the managed base dir", mimicBasename))
			foreign := fdCreateForeignLoopVG(ctx, cl, targetNode, fdBaseDir, mimicBasename, foreignVG)
			DeferCleanup(func() { fdDestroyForeignLoopVG(ctx, cl, foreign) })

			lvgName := lvmVGNamePrefix + "fdmimic-" + runID
			vgName := "e2e-vg-fdmimic-" + runID

			By(fmt.Sprintf("Creating our own file-backed LVMVolumeGroup %s alongside it", lvgName))
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
			fdPrintLVG(created)

			By("Verifying our LVG reports only its own backing file")
			fds := fdFileDevicesForNode(created, targetNode)
			Expect(fds).To(HaveLen(1), "the mimicking file must not be counted as ours")
			ourFile := fds[0].FilePath
			Expect(ourFile).NotTo(Equal(foreign.FilePath))
			Expect(ourFile).To(ContainSubstring(lvgName), "our backing file must carry our own LVG name")

			By("Verifying our VG spans exactly one PV — the foreign loop is not in it")
			pvNames, pvsOut, err := fdPVNamesInVGOnNode(ctx, cl, targetNode, vgName)
			Expect(err).NotTo(HaveOccurred())
			Expect(pvNames).To(HaveLen(1), "VG %s must not absorb the foreign loop PV; pvs:\n%s", vgName, pvsOut)
			Expect(pvNames).NotTo(ContainElement(foreign.Loop))

			By("Deleting our LVMVolumeGroup and verifying the foreign file survives the cleanup")
			fdDeleteLVGAndWaitGone(ctx, k8sClient, lvgName)
			deleted = true
			fdExpectBackingFileGone(ctx, cl, targetNode, ourFile)
			fdExpectForeignLoopVGIntact(ctx, cl, foreign)

			By("✓ Mimicking backing file neither claimed nor removed; only our own file was cleaned up")
		})

		// The blast radius of a wrong owner check is worst here: cleanup runs `rm` and
		// `losetup -d` on paths taken from status, so a foreign file in the same directory
		// must come through a full create/delete cycle untouched.
		It("Should leave a foreign loop in the base dir untouched when deleting a managed LVG", func() {
			foreignVG := "e2e-foreign-samedir-vg-" + runID

			By("Creating a foreign loop VG whose backing file sits in the managed base dir")
			foreign := fdCreateForeignLoopVG(ctx, cl, targetNode, fdBaseDir,
				"e2e-foreign-samedir-"+runID+".img", foreignVG)
			DeferCleanup(func() { fdDestroyForeignLoopVG(ctx, cl, foreign) })

			lvgName := lvmVGNamePrefix + "fdsamedir-" + runID
			vgName := "e2e-vg-fdsamedir-" + runID

			By(fmt.Sprintf("Creating managed LVMVolumeGroup %s in the same directory", lvgName))
			lvg := fdNewLVG(targetNode, lvgName, vgName, nil,
				[]v1alpha1.LVMVolumeGroupFileDeviceSpec{{Name: "d1g", Directory: fdBaseDir, Size: resource.MustParse("1Gi")}}, nil)
			deleted := false
			DeferCleanup(func() {
				if !deleted {
					fdDeleteLVGAndWaitGone(ctx, k8sClient, lvgName)
				}
			})
			created := fdCreateLVGAndWaitReady(ctx, cl, k8sClient, lvg)
			ourFile := fdFileDevicesForNode(created, targetNode)[0].FilePath

			By("Deleting the managed LVMVolumeGroup")
			fdDeleteLVGAndWaitGone(ctx, k8sClient, lvgName)
			deleted = true

			By("Verifying only our backing file is gone")
			fdExpectBackingFileGone(ctx, cl, targetNode, ourFile)
			fdExpectForeignLoopVGIntact(ctx, cl, foreign)

			files, err := fdManagedBackingFilesInDir(ctx, cl, targetNode, fdBaseDir)
			Expect(err).NotTo(HaveOccurred())
			for _, f := range files {
				Expect(strings.TrimSpace(f)).NotTo(Equal(ourFile), "our backing file should be removed")
			}

			By("✓ Cleanup removed only the agent-owned backing file")
		})

		// Everything above builds an UNtagged foreign Volume Group, and that was the
		// only shape the loop filter used to look at: a tagged one was kept
		// unconditionally. The specs below build the tagged shape, which is both more
		// likely and more dangerous — an image of a node disk this module used to
		// manage carries storage.deckhouse.io/enabled=true, so `losetup -f` during a
		// restore is enough to produce it.

		It("Should not let a tagged foreign loop VG take a same-named managed LVG offline", func() {
			lvgName := fdLVGName("fdtagged", runID, targetNode)
			vgName := "e2e-vg-fdtagged-" + runID

			// The image has to be built before the managed Volume Group exists.
			// `vgcreate` refuses a name that is already taken on the node, so a
			// duplicate cannot be manufactured afterwards — it only ever arrives on a
			// disk that was created elsewhere and plugged in, which is what staging
			// and re-attaching the image reproduces.
			By(fmt.Sprintf("Staging an image of another node's disk: a detached loop VG also named %s, tagged as the module's", vgName))
			foreign := fdStageForeignLoopVGImage(ctx, cl, targetNode, foreignDir,
				"e2e-foreign-tagged-"+runID+".img", vgName,
				[]string{fdManagedTag, "storage.deckhouse.io/lvmVolumeGroupName=e2e-lvg-on-another-node"})
			DeferCleanup(func() { fdDestroyForeignLoopVG(ctx, cl, foreign) })

			By(fmt.Sprintf("Creating a healthy file-backed LVMVolumeGroup %s (VG %s)", lvgName, vgName))
			lvg := fdNewLVG(targetNode, lvgName, vgName, nil,
				[]v1alpha1.LVMVolumeGroupFileDeviceSpec{{Name: "d1g", Directory: fdBaseDir, Size: resource.MustParse("1Gi")}}, nil)
			DeferCleanup(func() { fdDeleteLVGAndWaitGone(ctx, k8sClient, lvgName) })
			created := fdCreateLVGAndWaitReady(ctx, cl, k8sClient, lvg)
			fdExpectNoFalseConditions(created)

			// A duplicate VG name is what turns "the cache kept a foreign Volume Group"
			// into an outage: findDuplicateVGNames runs over every cached VG and takes
			// the managed LVMVolumeGroup to VGReady=False. The tag alone used to be
			// enough to keep this one in the cache.
			By(fmt.Sprintf("Attaching the image, so two Volume Groups now answer to %s", vgName))
			fdAttachForeignLoopVGImage(ctx, cl, foreign)

			By("Restarting the agent so a full discovery pass sees both Volume Groups")
			Expect(restartAgentOnNode(ctx, cl, targetNode)).To(Succeed())

			// Settle before asserting that it stays settled. restartAgentOnNode waits
			// for the new POD to be Ready, but AgentReady is a condition the
			// CONTROLLER writes on the resource, and it lags: it goes False/PodNotReady
			// while the pod is being deleted, the conditions watcher then takes Ready —
			// and with it the phase — down to NotReady, and both come back only after
			// the controller has seen the new pod. Consistently fails on its FIRST
			// sample, so without this wait the spec asserts the phase a couple of
			// seconds into that window and reports the restart it performed itself as
			// the foreign Volume Group dragging ours down.
			By("Waiting for the LVMVolumeGroup to come back Ready after the restart")
			Eventually(func(g Gomega) {
				var cur v1alpha1.LVMVolumeGroup
				g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: lvgName}, &cur)).To(Succeed())
				g.Expect(cur.Status.Phase).To(Equal(v1alpha1.PhaseReady),
					"the LVMVolumeGroup did not recover after the agent restart; conditions: %+v", cur.Status.Conditions)
			}, 3*time.Minute, 5*time.Second).Should(Succeed())

			By("Verifying our LVMVolumeGroup stays Ready and keeps reporting exactly its own device")
			Consistently(func(g Gomega) {
				var cur v1alpha1.LVMVolumeGroup
				g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: lvgName}, &cur)).To(Succeed())
				g.Expect(cur.Status.Phase).To(Equal(v1alpha1.PhaseReady),
					"the foreign Volume Group must not drag ours down; conditions: %+v", cur.Status.Conditions)
				for i := range cur.Status.Conditions {
					c := &cur.Status.Conditions[i]
					// duplicateVGMessage, the outage this spec is about: the foreign
					// Volume Group reaching the cache is what makes findDuplicateVGNames
					// declare the name ambiguous and refuse to reconcile ours.
					g.Expect(c.Message).NotTo(ContainSubstring("Multiple LVM VGs share the name"),
						"condition %s reported the foreign VG as a duplicate of ours: %s", c.Type, c.Message)
				}
				g.Expect(fdFileDevicesForNode(&cur, targetNode)).To(HaveLen(1),
					"the foreign loop must not be counted as one of our file devices")
			}, 2*time.Minute, 15*time.Second).Should(Succeed())

			By("Verifying no LVMVolumeGroup was created for the image and the image is untouched")
			var lvgs v1alpha1.LVMVolumeGroupList
			Expect(k8sClient.List(ctx, &lvgs)).To(Succeed())
			for i := range lvgs.Items {
				Expect(lvgs.Items[i].Name).NotTo(Equal("e2e-lvg-on-another-node"),
					"the tag named an LVMVolumeGroup that does not exist here; it must not be created from the image")
			}
			fdExpectForeignLoopVGIntact(ctx, cl, foreign)

			// Before teardown, because the agent removes its Volume Group by name and
			// that name is ambiguous for as long as the image is plugged in.
			By("Detaching the image so the managed Volume Group owns its name again")
			fdDetachForeignLoopVG(ctx, cl, foreign)

			By("✓ A tagged foreign loop VG neither adopted nor allowed to collide")
		})

		// The Critical case: ReTag's write is what makes a Volume Group the module's,
		// because it replaces the legacy tag with storage.deckhouse.io/enabled=true and
		// the discoverer adopts it afterwards. Its only gate is that legacy tag, which a
		// guest running LINSTOR inside a file-backed disk carries too — and once the tag
		// is replaced there is nothing left to identify the guest's Volume Group.
		It("Should not re-tag a foreign loop VG carrying the legacy LINSTOR tag", func() {
			foreignVG := "e2e-foreign-linstor-vg-" + runID
			legacyTag := fdLegacyTagPrefix + "e2e-guest-" + runID

			By(fmt.Sprintf("Creating a loop VG %s tagged %s, standing in for a nested LINSTOR cluster", foreignVG, legacyTag))
			foreign := fdCreateTaggedForeignLoopVG(ctx, cl, targetNode, foreignDir,
				"e2e-foreign-linstor-"+runID+".img", foreignVG, []string{legacyTag})
			DeferCleanup(func() { fdDestroyForeignLoopVG(ctx, cl, foreign) })

			By("Restarting the agent so ReTag runs at startup")
			Expect(restartAgentOnNode(ctx, cl, targetNode)).To(Succeed())

			By("Verifying the guest's tags are exactly as they were")
			Consistently(func(g Gomega) {
				managed, tags, err := fdVGHasTagOnNode(ctx, cl, targetNode, foreignVG, fdManagedTag)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(managed).To(BeFalse(),
					"the agent adopted a Volume Group it does not own by tagging it managed; tags: %q", tags)

				legacy, tags, err := fdVGHasTagOnNode(ctx, cl, targetNode, foreignVG, legacyTag)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(legacy).To(BeTrue(),
					"the legacy tag was removed, which is what identified the guest's Volume Group; tags: %q", tags)
			}, 2*time.Minute, 15*time.Second).Should(Succeed())

			By("Verifying no LVMVolumeGroup was created for it")
			var lvgs v1alpha1.LVMVolumeGroupList
			Expect(k8sClient.List(ctx, &lvgs)).To(Succeed())
			for i := range lvgs.Items {
				Expect(lvgs.Items[i].Spec.ActualVGNameOnTheNode).NotTo(Equal(foreignVG),
					"LVMVolumeGroup %s adopted the guest's VG %s", lvgs.Items[i].Name, foreignVG)
			}
			fdExpectForeignLoopVGIntact(ctx, cl, foreign)

			By("✓ Legacy-tagged foreign loop VG left alone by ReTag")
		})

		// `vgchange -ay` over a running guest's Volume Group gives the same extents two
		// writers. Whether the agent did it is only visible as the guest's logical
		// volumes appearing on the host, so the spec deactivates one and watches it.
		It("Should not activate the logical volumes of a tagged foreign loop VG", func() {
			foreignVG := "e2e-foreign-active-vg-" + runID
			const foreignLV = "guest-data"

			By(fmt.Sprintf("Creating a tagged loop VG %s with a deactivated LV %s", foreignVG, foreignLV))
			foreign := fdCreateForeignLoopVGWithLV(ctx, cl, targetNode, foreignDir,
				"e2e-foreign-active-"+runID+".img", foreignVG, foreignLV, []string{fdManagedTag})
			DeferCleanup(func() { fdDestroyForeignLoopVG(ctx, cl, foreign) })

			By("Restarting the agent so ActivateAllManagedVGs runs at startup")
			Expect(restartAgentOnNode(ctx, cl, targetNode)).To(Succeed())

			// EnsureVGActivation runs on every cache fill, not only at startup, so the
			// window has to stay open for a while: a udev burst is what used to bring
			// the guest's volumes back up after an operator deactivated them by hand.
			By("Verifying the LV stays inactive across discovery and cache fills")
			Consistently(func(g Gomega) {
				active, lvsOut, err := fdLVActiveOnNode(ctx, cl, targetNode, foreignVG, foreignLV)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(active).To(BeFalse(),
					"the agent activated a logical volume it does not own; lvs:\n%s", lvsOut)
			}, 2*time.Minute, 10*time.Second).Should(Succeed())

			By("✓ Foreign loop VG not activated on the host")
		})

		// Taking a foreign Volume Group for ours has no way out: create is refused
		// ("the VG is there"), update finds nothing in the cache, and the resource sits
		// in CacheStale forever while the condition tells the operator to wait for a
		// cache that is not the problem.
		//
		// What the agent does INSTEAD of that has changed, and this spec tracks the
		// change. While loop devices were visible wholesale, the name genuinely
		// collided in lvm's view and vgcreate had to fail for the real reason
		// (VGCreationFailed). Now that the filter only admits the loops the agent owns
		// (internal.LVMGlobalFilterAcceptingLoops), a Volume Group inside somebody
		// else's disk is not in that view at all — so there is no collision to report
		// and the managed Volume Group is simply created. That is the point of the
		// filter: a guest's VG must not decide what this node can do with its own
		// storage. What must still hold is that the agent never parks in CacheStale
		// over it, and never touches the guest's image.
		It("Should create its own VG despite a foreign loop VG of the same name, and never park in CacheStale", func() {
			vgName := "e2e-vg-fdcollide-" + runID
			lvgName := fdLVGName("fdcollide", runID, targetNode)

			By(fmt.Sprintf("Attaching a tagged foreign loop VG named %s before any LVMVolumeGroup exists", vgName))
			foreign := fdCreateTaggedForeignLoopVG(ctx, cl, targetNode, foreignDir,
				"e2e-foreign-collide-"+runID+".img", vgName, []string{fdManagedTag})
			DeferCleanup(func() { fdDestroyForeignLoopVG(ctx, cl, foreign) })

			By(fmt.Sprintf("Creating an LVMVolumeGroup %s that wants the very same VG name", lvgName))
			lvg := fdNewLVG(targetNode, lvgName, vgName, nil,
				[]v1alpha1.LVMVolumeGroupFileDeviceSpec{{Name: "d1g", Directory: fdBaseDir, Size: resource.MustParse("1Gi")}}, nil)
			Expect(k8sClient.Create(ctx, lvg)).To(Succeed())
			// A normal delete now, not a forced one: the create succeeds here, so there
			// is a real Volume Group and a real backing file to clean up, and dropping
			// the resource by force would leak both onto the node for the rest of the
			// suite. fdForceDeleteLVG stays the tool for resources that never got that far.
			DeferCleanup(func() { fdDeleteLVGAndWaitGone(ctx, k8sClient, lvgName) })

			By("Verifying the agent applies the configuration instead of parking in CacheStale")
			Eventually(func(g Gomega) {
				var cur v1alpha1.LVMVolumeGroup
				g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: lvgName}, &cur)).To(Succeed())

				var applied *metav1.Condition
				for i := range cur.Status.Conditions {
					if cur.Status.Conditions[i].Type == fdConditionVGConfigurationApplied {
						applied = &cur.Status.Conditions[i]
						break
					}
				}
				g.Expect(applied).NotTo(BeNil(), "the agent must say something about this LVMVolumeGroup")
				g.Expect(applied.Reason).NotTo(Equal(fdReasonCacheStale),
					"the foreign loop VG was taken for ours; message: %s", applied.Message)
				g.Expect(applied.Status).To(Equal(metav1.ConditionTrue),
					"a Volume Group inside a guest's disk is not in the agent's view, so there is nothing to stop the create; reason %s, message: %s",
					applied.Reason, applied.Message)
				g.Expect(fdFileDevicesForNode(&cur, targetNode)).To(HaveLen(1),
					"the foreign loop must not be counted as one of our file devices")
			}, fdLVGReadyTimeout, 10*time.Second).Should(Succeed())

			By("Verifying the VG the agent created is its own and not the guest's")
			var created v1alpha1.LVMVolumeGroup
			Expect(k8sClient.Get(ctx, client.ObjectKey{Name: lvgName}, &created)).To(Succeed())
			Expect(created.Status.VGUuid).NotTo(BeEmpty(), "the managed VG must be reported in status")
			Expect(created.Status.VGUuid).NotTo(Equal(foreign.VGUUID),
				"the agent adopted the guest's Volume Group instead of creating its own")

			By("Verifying the image is untouched")
			fdExpectForeignLoopVGIntact(ctx, cl, foreign)

			By("Detaching the image before teardown, so the managed VG owns its name for the delete")
			fdDetachForeignLoopVG(ctx, cl, foreign)

			By("✓ The guest's Volume Group neither blocked the create nor was taken for ours")
		})
	})
