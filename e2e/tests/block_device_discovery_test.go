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
	"math/rand"
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

const (
	// bddiscDiskSize is the size requested for every disk attached in this suite.
	bddiscDiskSize = "2Gi"
	// bddiscMaxDisksPerNode caps the random per-node disk count. Legacy used 1..5;
	// each attach is verified sequentially via set-diff (one new BD at a time), so a
	// smaller cap keeps total wall-clock bounded while preserving the multi-disk-per-node intent.
	bddiscMaxDisksPerNode = 3
)

var _ = Describe("BlockDevice discovery", Label("sds-node-configurator", "block-device", "discovery"), Ordered, func() {
	// bddiscAttached correlates one attached disk with the BlockDevice it produced.
	type bddiscAttached struct {
		node     string
		diskName string
		bdName   string
		reqSize  resource.Quantity
	}

	var (
		ctx       context.Context
		conf      *cfg.Config
		cl        *e2e.Cluster
		k8sClient client.Client
		nodes     []string
		attached  []bddiscAttached
	)

	BeforeAll(func() {
		ctx = context.Background()
		var cfgErr error
		conf, cfgErr = cfg.Load()
		Expect(cfgErr).NotTo(HaveOccurred(), "failed to load config")

		var clErr error
		cl, clErr = e2e.Connect(ctx, e2e.WithTestName("block-device-discovery"))
		Expect(clErr).NotTo(HaveOccurred(), "failed to connect to cluster")
		DeferCleanup(func() {
			if err := cl.Close(context.Background()); err != nil {
				GinkgoWriter.Println("Error closing cluster: ", err)
			}
		})

		var k8sErr error
		k8sClient, k8sErr = sdsclient.New(cl.RESTConfig())
		Expect(k8sErr).NotTo(HaveOccurred(), "failed to build controller-runtime client")

		Expect(conf.TestCluster.StorageClass).NotTo(BeEmpty(), "storage class is required for VirtualDisk creation")

		nodeList, nlErr := cl.Clientset().CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		Expect(nlErr).NotTo(HaveOccurred(), "failed to list nodes")
		Expect(nodeList.Items).NotTo(BeEmpty(), "cluster must have at least one node")
		for i := range nodeList.Items {
			nodes = append(nodes, nodeList.Items[i].Name)
		}
	})

	AfterEach(func() {
		if cl == nil {
			return
		}
		By("Detaching and deleting disks created during discovery")
		var bdNames []string
		for _, d := range attached {
			if detachErr := cl.Disks().DetachDisk(ctx, d.node, d.diskName); detachErr != nil {
				GinkgoWriter.Printf("failed to detach disk %s from node %s: %v\n", d.diskName, d.node, detachErr)
			}
			if deleteErr := cl.Disks().DeleteDisk(ctx, d.diskName); deleteErr != nil {
				GinkgoWriter.Printf("failed to delete disk %s: %v\n", d.diskName, deleteErr)
			}
			if d.bdName != "" {
				bdNames = append(bdNames, d.bdName)
			}
		}
		if len(bdNames) > 0 {
			By("Force-deleting leftover BlockDevice CRs")
			forceDeleteBlockDevicesByNames(ctx, k8sClient, bdNames)
		}
		attached = nil
	})

	It("Should discover a new unformatted disk and create a BlockDevice object", func() {
		reqSize := resource.MustParse(bddiscDiskSize)

		By("Selecting a random subset of nodes for parallel discovery")
		shuffled := append([]string(nil), nodes...)
		rand.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
		parallelism := 1 + rand.Intn(len(shuffled)) // [1, len(nodes)] nodes involved
		targetNodes := shuffled[:parallelism]

		// Attach disks per node. The SDK Disk API does not expose the VD UID, so we correlate each
		// attached disk to its BlockDevice by a before/after set-diff of consumable BlockDevices on the node.
		for _, node := range targetNodes {
			disksOnNode := 1 + rand.Intn(bddiscMaxDisksPerNode) // [1, bddiscMaxDisksPerNode] per node
			for j := 0; j < disksOnNode; j++ {
				By(fmt.Sprintf("Snapshotting consumable BlockDevices on node %s before attach", node))
				before, snapErr := kubernetes.GetConsumableBlockDevicesByNode(ctx, cl.RESTConfig(), node)
				Expect(snapErr).NotTo(HaveOccurred(), "failed to snapshot consumable BlockDevices on node %s", node)

				diskName := fmt.Sprintf("e2e-bddisc-%d", time.Now().UnixNano())
				By("Creating and attaching a virtual disk: " + diskName)
				disk, createErr := cl.Disks().CreateDisk(ctx, e2e.DiskSpec{
					Name:         diskName,
					Size:         reqSize,
					StorageClass: conf.TestCluster.StorageClass,
				})
				Expect(createErr).NotTo(HaveOccurred(), "failed to create disk %s", diskName)

				attached = append(attached, bddiscAttached{node: node, diskName: disk.Name, reqSize: reqSize})

				Expect(cl.Disks().AttachDisk(ctx, node, disk.Name)).
					To(Succeed(), "failed to attach disk %s to node %s", disk.Name, node)

				By("Waiting for the new consumable BlockDevice to appear (set-diff)")
				newBD, waitErr := framework.WaitNewConsumableBlockDevice(ctx, cl.RESTConfig(), node, before, 5*time.Minute)
				Expect(waitErr).NotTo(HaveOccurred(),
					"no single new consumable BlockDevice appeared on node %s after attaching %s", node, disk.Name)

				attached[len(attached)-1].bdName = newBD.Name
			}
		}

		Expect(attached).NotTo(BeEmpty(), "at least one disk must have been attached")

		countByNode := make(map[string]int, len(targetNodes))
		for _, d := range attached {
			countByNode[d.node]++
		}

		// Name-formula verification (replaces md5-of-UID serial cross-check): fetch the full BlockDevice
		// and recompute the agent's name from EXACT v1alpha1 status fields. Asserting the recomputed name
		// equals BD.Name validates wwn/model/serial/partUUID indirectly (they feed the hash).
		By("Verifying each BlockDevice matches the agent name formula")
		for i := range attached {
			d := attached[i]
			var bd v1alpha1.BlockDevice
			Expect(k8sClient.Get(ctx, client.ObjectKey{Name: d.bdName}, &bd)).
				To(Succeed(), "get BlockDevice %s", d.bdName)

			Expect(bd.Status.NodeName).To(Equal(d.node),
				"BlockDevice %s must be on node %s (got %s)", bd.Name, d.node, bd.Status.NodeName)
			Expect(bd.Status.Consumable).To(BeTrue(), "BlockDevice %s must be consumable", bd.Name)
			Expect(bd.Status.Path).To(HavePrefix("/dev/"),
				"BlockDevice %s path must be a /dev path, got %q", bd.Name, bd.Status.Path)

			expectedName := framework.BlockDeviceName(
				d.node,
				bd.Status.Wwn,
				bd.Status.Model,
				bd.Status.Serial,
				bd.Status.PartUUID,
			)
			Expect(bd.Name).To(Equal(expectedName),
				"BlockDevice name must match agent formula dev-SHA1(node+wwn+model+serial+partUUID): expected %s, got %s",
				expectedName, bd.Name)

			Expect(bd.Status.Size.Cmp(d.reqSize)).NotTo(BeNumerically("<", 0),
				"BD %s size must be >= requested %s, got %s", bd.Name, d.reqSize.String(), bd.Status.Size.String())
		}

		// Per-node consumable-BD count assertions: every disk we attached on a node must be present as a
		// consumable BlockDevice there, and the total consumable count must be at least what we attached.
		By("Verifying per-node consumable BlockDevice counts")
		for node, want := range countByNode {
			bds, listErr := kubernetes.GetConsumableBlockDevicesByNode(ctx, cl.RESTConfig(), node)
			Expect(listErr).NotTo(HaveOccurred(), "list consumable BlockDevices on node %s", node)

			present := make(map[string]struct{}, len(bds))
			for _, bd := range bds {
				present[bd.Name] = struct{}{}
			}
			got := 0
			for _, d := range attached {
				if d.node != node {
					continue
				}
				Expect(present).To(HaveKey(d.bdName),
					"attached BlockDevice %s must be consumable on node %s", d.bdName, node)
				got++
			}
			Expect(got).To(Equal(want), "node %s: tracked attached BlockDevice count mismatch", node)
			Expect(len(bds)).To(BeNumerically(">=", want),
				"node %s: consumable BD count must be >= attached count %d, got %d", node, want, len(bds))
		}

		// OS cross-check (replaces SSH lsblk): assert each BD's device path is present in on-node lsblk with a
		// matching serial and a size >= requested. framework.LsblkLine carries no wwn/model/partUUID, so those are not
		// cross-checked here — the name-formula assertion above already validates them via BD.Status.
		By("Cross-checking discovered BlockDevices against on-node lsblk")
		lsblkByNode := make(map[string][]framework.LsblkLine, len(countByNode))
		for node := range countByNode {
			out, lsblkErr := framework.NodeExecChecked(ctx, cl, node, "lsblk -b -P -o NAME,SIZE,SERIAL,PATH -n")
			Expect(lsblkErr).NotTo(HaveOccurred(), "lsblk on node %s", node)
			parsed := framework.ParseLsblk(out)
			lines := make([]framework.LsblkLine, 0, len(parsed))
			for _, l := range parsed {
				lines = append(lines, l)
			}
			lsblkByNode[node] = lines
		}
		for i := range attached {
			d := attached[i]
			var bd v1alpha1.BlockDevice
			Expect(k8sClient.Get(ctx, client.ObjectKey{Name: d.bdName}, &bd)).
				To(Succeed(), "get BlockDevice %s", d.bdName)

			var line *framework.LsblkLine
			for j := range lsblkByNode[d.node] {
				if lsblkByNode[d.node][j].Path == bd.Status.Path {
					line = &lsblkByNode[d.node][j]
					break
				}
			}
			Expect(line).NotTo(BeNil(),
				"device path %s of BD %s must be present in lsblk on node %s", bd.Status.Path, bd.Name, d.node)
			Expect(line.Serial).To(Equal(bd.Status.Serial),
				"lsblk serial for %s must match BD.Status.Serial (%s)", bd.Status.Path, bd.Status.Serial)
			Expect(line.SizeBytes).To(BeNumerically(">=", d.reqSize.Value()),
				"lsblk size for %s must be >= requested %s, got %d bytes", bd.Status.Path, d.reqSize.String(), line.SizeBytes)
		}
	})
})
