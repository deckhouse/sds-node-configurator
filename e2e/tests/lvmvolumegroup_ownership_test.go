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
	"github.com/deckhouse/sds-node-configurator/e2e/cfg"
	"github.com/deckhouse/sds-node-configurator/e2e/framework"
	"github.com/deckhouse/sds-node-configurator/e2e/sdsclient"
	"github.com/deckhouse/sds-node-configurator/e2e/tests/utils/consts"
	"github.com/deckhouse/storage-e2e/pkg/e2e"
	"github.com/deckhouse/storage-e2e/pkg/kubernetes"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Who owns a Volume Group, and what follows from the answer.
//
// A Volume Group's name is not a handle: the same name can belong to a guest's LVM
// inside a disk this node can see, to a LUN presented to several hosts, or to a
// resource somebody deleted a year ago. Everything here pins behaviour that used to
// depend on the name alone, and each spec corresponds to something that happened on
// a live cluster rather than to a hypothesis:
//
//   - a stale LVMVolumeGroup over a Volume Group tagged for another resource must go
//     away without taking the storage with it. Four hundred such resources pointed at
//     one live Volume Group; the first delete would have removed it.
//   - a resource whose blockDeviceSelector matches nothing must still be deletable.
//     A hundred and fifty-five were stuck in Terminating for good, because validation
//     ran before the delete path and a cluster policy forbids stripping finalizers.
//   - a Volume Group whose owner tag names an existing LVMVolumeGroup must not be
//     imported again. Doing so under a generated name created a resource per scan —
//     ninety in four seconds, about nine hundred over a day.
//
// The Volume Groups here live on real disks, not on loop devices: with the loop rule
// back in internal.LVMGlobalFilter a loop-backed foreign VG is invisible to the
// agent, so it could not exercise any of this.
const (
	// ownDiskSize only has to be big enough to carry a Volume Group.
	ownDiskSize = "1Gi"

	// ownQuietFor is how long "nothing new appeared" has to hold. The import loop
	// this pins produced tens of resources per minute, so a couple of minutes is
	// several orders of magnitude of headroom.
	ownQuietFor = 2 * time.Minute

	// ownForeignOwnerName is the LVMVolumeGroup name written into the tag of a
	// Volume Group nobody in this cluster owns. It must not exist as a resource:
	// that is what makes the VG "tagged for somebody else" rather than importable.
	ownForeignOwnerPrefix = "lvg-owned-elsewhere-"
)

var _ = Describe("LVMVolumeGroup ownership of a Volume Group",
	Label("sds-node-configurator", "lvmvolumegroup", "nested-lvm"), Ordered, ContinueOnFailure, func() {
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
			cl, clErr = e2e.Connect(ctx, e2e.WithTestName("lvmvolumegroup-ownership"))
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

			Expect(conf.TestCluster.StorageClass).NotTo(BeEmpty(), "TestCluster.StorageClass required")
			runID = fmt.Sprintf("%d", time.Now().Unix())
		})

		AfterAll(func() {
			if k8sClient != nil {
				cleanupLVMVolumeGroups(ctx, k8sClient)
			}
		})

		// Two fixes at once, because the resource that has to be deleted safely is the
		// same resource that could not be deleted at all: it points at a Volume Group
		// tagged for another LVMVolumeGroup (so its removal must not touch the VG) and
		// its blockDeviceSelector matches nothing (so validation must not block the
		// delete). That combination is not contrived — it is the state hundreds of
		// resources were found in.
		It("Should delete a stale resource without removing the Volume Group it points at", func() {
			vgName := "e2e-vg-owned-" + runID
			owner := ownForeignOwnerPrefix + runID
			staleName := fdLVGName("stale", runID, targetNode)

			By(fmt.Sprintf("Creating VG %s on a real disk, tagged as owned by %s — a resource that does not exist here", vgName, owner))
			vg := ownCreateTaggedVGOnDisk(ctx, cl, conf, targetNode, runID, "owned", vgName, owner)

			By(fmt.Sprintf("Creating a stale LVMVolumeGroup %s that points at it, with a selector matching nothing", staleName))
			stale := ownNewStaleLVG(staleName, targetNode, vgName)
			Expect(k8sClient.Create(ctx, stale)).To(Succeed())
			staleGone := false
			DeferCleanup(func() {
				if !staleGone {
					fdForceDeleteLVG(ctx, k8sClient, staleName)
				}
			})

			// The finalizer is what makes the delete meaningful: without it the resource
			// would vanish without the agent ever seeing it, and the spec would prove
			// nothing about the delete path.
			By("Waiting for the agent to take the resource (finalizer added)")
			Eventually(func(g Gomega) {
				var cur v1alpha1.LVMVolumeGroup
				g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: staleName}, &cur)).To(Succeed())
				g.Expect(cur.Finalizers).To(ContainElement(ownFinalizer),
					"the agent has not picked this resource up yet; conditions: %s", describeLVGStatus(&cur))
			}, fdRejectionTimeout, 5*time.Second).Should(Succeed())

			By("Deleting the stale resource")
			Expect(k8sClient.Delete(ctx, &v1alpha1.LVMVolumeGroup{
				ObjectMeta: metav1.ObjectMeta{Name: staleName},
			})).To(Succeed())

			By("Verifying it goes away instead of sitting in Terminating")
			Eventually(func(g Gomega) {
				var cur v1alpha1.LVMVolumeGroup
				err := k8sClient.Get(ctx, client.ObjectKey{Name: staleName}, &cur)
				if err == nil {
					g.Expect(cur.Finalizers).NotTo(ContainElement(ownFinalizer),
						"still holding the finalizer — validation is gating the delete path again; conditions: %s",
						describeLVGStatus(&cur))
					return
				}
				g.Expect(client.IgnoreNotFound(err)).To(Succeed())
			}, fdCleanupTimeout, 5*time.Second).Should(Succeed())
			staleGone = true

			By("Verifying the Volume Group is still on the node, with its identity and tags intact")
			ownExpectVGIntact(ctx, cl, targetNode, vg, owner)

			By("✓ The stale resource is gone and the Volume Group it named is untouched")
		})

		// The import loop. It needs a Volume Group whose owner tag names an
		// LVMVolumeGroup that exists, and the cheapest honest way to get one is a
		// second Volume Group carrying the same tag: one gets imported, the other then
		// finds its name taken. On a hypervisor the same state arrives through shared
		// storage instead — a LUN presented to two hosts — which no e2e provider here
		// can reproduce.
		It("Should not create a resource per scan for a Volume Group that already has one", func() {
			ownerVGName := "e2e-vg-owner-" + runID
			twinVGName := "e2e-vg-twin-" + runID
			ownerLVG := fdLVGName("owner", runID, targetNode)

			By(fmt.Sprintf("Creating VG %s tagged for %s and letting the agent import it", ownerVGName, ownerLVG))
			ownerVG := ownCreateTaggedVGOnDisk(ctx, cl, conf, targetNode, runID, "owner", ownerVGName, ownerLVG)
			Eventually(func(g Gomega) {
				var cur v1alpha1.LVMVolumeGroup
				g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: ownerLVG}, &cur)).To(Succeed())
			}, lvmVolumeGroupReadyTimeout, 10*time.Second).Should(Succeed(), "the tagged VG was never imported, so the rest of the spec has no owner to collide with")
			DeferCleanup(func() { fdForceDeleteLVG(ctx, k8sClient, ownerLVG) })

			By(fmt.Sprintf("Creating a second VG %s carrying the very same owner tag", twinVGName))
			twinVG := ownCreateTaggedVGOnDisk(ctx, cl, conf, targetNode, runID, "twin", twinVGName, ownerLVG)

			before := ownCountLVGs(ctx, k8sClient)
			GinkgoWriter.Printf("LVMVolumeGroups before the quiet window: %d\n", before)

			By(fmt.Sprintf("Verifying no new LVMVolumeGroup appears for %s over %s", twinVGName, ownQuietFor))
			Consistently(func(g Gomega) {
				g.Expect(ownCountLVGs(ctx, k8sClient)).To(BeNumerically("<=", before),
					"a resource is being created per scan for a Volume Group that already has an owner — the import loop is back")

				var lvgs v1alpha1.LVMVolumeGroupList
				g.Expect(k8sClient.List(ctx, &lvgs)).To(Succeed())
				for i := range lvgs.Items {
					g.Expect(lvgs.Items[i].Spec.ActualVGNameOnTheNode).NotTo(Equal(twinVGName),
						"LVMVolumeGroup %s was created for a VG whose tag names %s", lvgs.Items[i].Name, ownerLVG)
				}
			}, ownQuietFor, 15*time.Second).Should(Succeed())

			By("Verifying the agent says why it refused, naming the resource that owns the name")
			logs := ownAgentLog(ctx, cl, targetNode)
			Expect(logs).To(ContainSubstring("not importing VG "+twinVGName),
				"the refusal has to be in the log: silence here is what made the loop invisible for a day")
			Expect(logs).To(ContainSubstring(ownerLVG))

			By("Verifying both Volume Groups are intact")
			ownExpectVGIntact(ctx, cl, targetNode, ownerVG, ownerLVG)
			ownExpectVGIntact(ctx, cl, targetNode, twinVG, ownerLVG)

			By("✓ A Volume Group that already has an LVMVolumeGroup is refused, not re-imported")
		})
	})

// ownFinalizer is the finalizer the agent puts on every LVMVolumeGroup it manages.
// Its presence means the agent has seen the resource; its absence after a delete
// means the delete path ran to the end.
const ownFinalizer = "storage.deckhouse.io/sds-node-configurator"

// ownVG is a Volume Group this suite created on the node by hand, with the identity
// needed to prove afterwards that it is the same one.
type ownVG struct {
	Name string
	UUID string
	Disk string
}

// ownCreateTaggedVGOnDisk attaches a disk and builds a Volume Group on it directly on
// the node, tagged as managed and owned by ownerLVG.
//
// By hand rather than through an LVMVolumeGroup: the point of every spec here is a
// Volume Group whose owner is somebody other than the resource under test, and the
// agent will not create that for us.
func ownCreateTaggedVGOnDisk(
	ctx context.Context,
	cl *e2e.Cluster,
	conf *cfg.Config,
	node, runID, tag, vgName, ownerLVG string,
) ownVG {
	GinkgoHelper()

	diskName := fmt.Sprintf("e2e-own-%s-%s", tag, runID)
	disk, err := fdCreateDiskOrSkip(ctx, cl, e2e.DiskSpec{
		Name:         diskName,
		Size:         resource.MustParse(ownDiskSize),
		StorageClass: conf.TestCluster.StorageClass,
	})
	Expect(err).NotTo(HaveOccurred(), "failed to create the disk for %s", vgName)
	DeferCleanup(func() {
		_ = cl.Disks().DetachDisk(ctx, node, disk.Name)
		_ = cl.Disks().DeleteDisk(ctx, disk.Name)
	})

	// The device path comes from the BlockDevice the discoverer publishes for the
	// fresh disk, which is how every other spec here finds one: it is the same answer
	// lsblk would give and it also proves the disk really arrived on this node.
	before, err := kubernetes.GetConsumableBlockDevicesByNode(ctx, cl.RESTConfig(), node)
	Expect(err).NotTo(HaveOccurred(), "failed to list consumable BlockDevices on %s", node)

	Expect(cl.Disks().AttachDisk(ctx, node, disk.Name)).To(Succeed(), "failed to attach the disk for %s", vgName)

	bd, err := framework.WaitNewConsumableBlockDevice(ctx, cl.RESTConfig(), node, before, 5*time.Minute)
	Expect(err).NotTo(HaveOccurred(), "no consumable BlockDevice appeared for the disk of %s", vgName)
	devPath := bd.Path
	Expect(devPath).NotTo(BeEmpty(), "BlockDevice %s reports no device path", bd.Name)
	GinkgoWriter.Printf("Disk %s appeared on %s as %s (BlockDevice %s)\n", disk.Name, node, devPath, bd.Name)

	script := fmt.Sprintf(`set -e
sudo -n lvm pvcreate %s -y -ff %s >/dev/null
sudo -n lvm vgcreate %s --addtag %s --addtag %s=%s %s %s >/dev/null
sudo -n lvm vgs %s -o vg_uuid --noheadings %s`,
		fdLVMCfg, strconv.Quote(devPath),
		fdLVMCfg, strconv.Quote(fdManagedTag), strconv.Quote(ownerTagKey), strconv.Quote(ownerLVG),
		strconv.Quote(vgName), strconv.Quote(devPath),
		fdLVMCfg, strconv.Quote(vgName))
	out, err := framework.NodeExecChecked(ctx, cl, node, script)
	Expect(err).NotTo(HaveOccurred(), "failed to create VG %s on %s:\n%s", vgName, devPath, out)

	uuid := strings.TrimSpace(ownLastLine(out))
	Expect(uuid).NotTo(BeEmpty(), "VG %s reported no UUID:\n%s", vgName, out)
	DeferCleanup(func() {
		script := fmt.Sprintf(`sudo -n lvm vgremove %s -y %s >/dev/null 2>&1; sudo -n lvm pvremove %s -y -ff %s >/dev/null 2>&1; true`,
			fdLVMCfg, strconv.Quote(vgName), fdLVMCfg, strconv.Quote(devPath))
		if out, err := framework.NodeExecChecked(ctx, cl, node, script); err != nil {
			GinkgoWriter.Printf("teardown of VG %s: %v\n%s\n", vgName, err, out)
		}
	})

	GinkgoWriter.Printf("VG %s (VG_UUID=%s) created on %s:%s, tagged owner=%s\n", vgName, uuid, node, devPath, ownerLVG)
	return ownVG{Name: vgName, UUID: uuid, Disk: devPath}
}

// ownerTagKey mirrors internal.LVMVolumeGroupTag. The e2e module cannot import the
// agent's constants, so a divergence here shows up as a spec that stops reproducing
// its own precondition — which is why every spec asserts the tag it just wrote.
const ownerTagKey = "storage.deckhouse.io/lvmVolumeGroupName"

// ownNewStaleLVG builds the resource hundreds were found in: it names a Volume Group
// that exists but belongs to somebody else, and its selector matches no BlockDevice.
func ownNewStaleLVG(name, node, vgName string) *v1alpha1.LVMVolumeGroup {
	return &v1alpha1.LVMVolumeGroup{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1alpha1.LVMVolumeGroupSpec{
			Type:                  "Local",
			ActualVGNameOnTheNode: vgName,
			Local:                 v1alpha1.LVMVolumeGroupLocalSpec{NodeName: node},
			BlockDeviceSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"kubernetes.io/metadata.name": "dev-that-does-not-exist"},
			},
		},
	}
}

// ownExpectVGIntact fails unless the Volume Group is still on the node with the same
// UUID and still tagged for the same owner. Both halves matter: the UUID says it was
// not removed and recreated, the tag says nothing re-tagged it as ours.
func ownExpectVGIntact(ctx context.Context, cl *e2e.Cluster, node string, vg ownVG, ownerLVG string) {
	GinkgoHelper()

	out, err := framework.NodeExecChecked(ctx, cl, node,
		fmt.Sprintf(`sudo -n lvm vgs %s -o vg_name,vg_uuid,vg_tags --noheadings %s 2>&1`,
			fdLVMCfg, strconv.Quote(vg.Name)))
	Expect(err).NotTo(HaveOccurred(), "VG %s is gone from %s:\n%s", vg.Name, node, out)
	Expect(out).To(ContainSubstring(vg.UUID),
		"VG %s no longer has VG_UUID=%s, so it was removed and something else took the name:\n%s", vg.Name, vg.UUID, out)
	Expect(out).To(ContainSubstring(ownerTagKey+"="+ownerLVG),
		"VG %s lost or changed its owner tag:\n%s", vg.Name, out)
}

func ownCountLVGs(ctx context.Context, cl client.Client) int {
	GinkgoHelper()
	var lvgs v1alpha1.LVMVolumeGroupList
	Expect(cl.List(ctx, &lvgs)).To(Succeed())
	return len(lvgs.Items)
}

func ownAgentLog(ctx context.Context, cl *e2e.Cluster, node string) string {
	GinkgoHelper()
	pods, err := cl.Clientset().CoreV1().Pods(consts.SdsNodeConfiguratorAgentNamespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app=" + consts.SdsNodeConfiguratorAgentName,
		FieldSelector: "spec.nodeName=" + node,
	})
	Expect(err).NotTo(HaveOccurred(), "failed to list agent pods on %s", node)
	Expect(pods.Items).NotTo(BeEmpty(), "no agent pod on %s", node)

	tail := int64(4000)
	raw, err := cl.Clientset().CoreV1().Pods(consts.SdsNodeConfiguratorAgentNamespace).
		GetLogs(pods.Items[0].Name, &corev1.PodLogOptions{
			Container: consts.SdsNodeConfiguratorAgentContainer,
			TailLines: &tail,
		}).DoRaw(ctx)
	Expect(err).NotTo(HaveOccurred(), "failed to read the log of %s", pods.Items[0].Name)
	return string(raw)
}

func ownLastLine(out string) string {
	lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
	return lines[len(lines)-1]
}
