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

// Device-type matrix: discovery + LVG lifecycle for supported types (disk,
// mpath, opened crypt/LUKS), filtering of unsupported types (loop, closed
// LUKS), and an optional mix of supported devices in one LVG.
// Label device-types is exclusive. Local Makefile smoke excludes it; in CI run
// via PR label e2e/label:device-types (or make test-device-types locally):
//
//	make test-device-types
//	go test ... -ginkgo.label-filter=device-types
//	PR label e2e/label:device-types
var _ = Describe("Block device types matrix",
	Label("sds-node-configurator", "device-types"),
	Ordered, ContinueOnFailure, func() {

		var (
			ctx        context.Context
			conf       *cfg.Config
			cl         *e2e.Cluster
			k8sClient  client.Client
			targetNode string
		)

		BeforeAll(func() {
			ctx = context.Background()
			var cfgErr error
			conf, cfgErr = cfg.Load()
			Expect(cfgErr).NotTo(HaveOccurred(), "failed to load config")

			var clErr error
			cl, clErr = e2e.Connect(ctx, e2e.WithTestName("block-device-types"))
			Expect(clErr).NotTo(HaveOccurred(), "failed to connect to cluster")
			DeferCleanup(func() {
				if err := cl.Close(context.Background()); err != nil {
					GinkgoWriter.Println("Error closing cluster:", err)
				}
			})

			var k8sErr error
			k8sClient, k8sErr = sdsclient.New(cl.RESTConfig())
			Expect(k8sErr).NotTo(HaveOccurred(), "failed to build controller-runtime client")

			nodeList, nlErr := cl.Clientset().CoreV1().Nodes().List(ctx, metav1.ListOptions{})
			Expect(nlErr).NotTo(HaveOccurred(), "failed to list nodes")
			Expect(nodeList.Items).NotTo(BeEmpty(), "cluster must have at least one node")
			targetNode = nodeList.Items[0].Name
			Expect(conf.TestCluster.StorageClass).NotTo(BeEmpty(), "TestCluster.StorageClass required")
		})

		AfterAll(func() {
			if k8sClient != nil {
				cleanupLVMVolumeGroups(ctx, k8sClient)
			}
		})

		// attachPlainDisk creates a VirtualDisk, attaches it, and waits for a
		// new consumable BlockDevice. Disk ops are hard-capped so a stuck
		// VirtualDisk/Attachment cannot burn the whole CI job.
		attachPlainDisk := func(runID, tag string) (disk *e2e.Disk, bd kubernetes.BlockDevice, path string) {
			GinkgoHelper()
			before, err := kubernetes.GetConsumableBlockDevicesByNode(ctx, cl.RESTConfig(), targetNode)
			Expect(err).NotTo(HaveOccurred())

			diskName := fmt.Sprintf("e2e-bdtypes-%s-%s", tag, runID)
			diskCtx, diskCancel := context.WithTimeout(ctx, bdtypesDiskOpTimeout)
			defer diskCancel()

			By(fmt.Sprintf("CreateDisk %s (timeout %s)", diskName, bdtypesDiskOpTimeout))
			disk, err = cl.Disks().CreateDisk(diskCtx, e2e.DiskSpec{
				Name:         diskName,
				Size:         resource.MustParse(bdtypesDiskSize),
				StorageClass: conf.TestCluster.StorageClass,
			})
			Expect(err).NotTo(HaveOccurred(), "failed to create disk %s", diskName)
			DeferCleanup(func() {
				cctx, cancel := context.WithTimeout(context.Background(), bdtypesDiskOpTimeout)
				defer cancel()
				_ = cl.Disks().DetachDisk(cctx, targetNode, disk.Name)
				_ = cl.Disks().DeleteDisk(cctx, disk.Name)
			})

			By(fmt.Sprintf("AttachDisk %s → %s (timeout %s)", disk.Name, targetNode, bdtypesDiskOpTimeout))
			attachCtx, attachCancel := context.WithTimeout(ctx, bdtypesDiskOpTimeout)
			defer attachCancel()
			Expect(cl.Disks().AttachDisk(attachCtx, targetNode, disk.Name)).
				To(Succeed(), "failed to attach disk %s (timed out or error)", disk.Name)

			bd, err = framework.WaitNewConsumableBlockDevice(ctx, cl.RESTConfig(), targetNode, before, bdtypesDiscoveryTimeout)
			Expect(err).NotTo(HaveOccurred(), "consumable BlockDevice not discovered for %s", disk.Name)
			DeferCleanup(func() { forceDeleteBlockDevicesByNames(ctx, k8sClient, []string{bd.Name}) })

			var bdCR v1alpha1.BlockDevice
			Expect(k8sClient.Get(ctx, client.ObjectKey{Name: bd.Name}, &bdCR)).To(Succeed())
			path = bdCR.Status.Path
			Expect(path).NotTo(BeEmpty(), "BlockDevice %s has empty status.path", bd.Name)
			return disk, bd, path
		}

		createReadyLVG := func(runID, tag string, bdNames []string) (lvgName, vgName string) {
			GinkgoHelper()
			vgName = fmt.Sprintf("e2e-vg-bdtypes-%s-%s", tag, runID)
			lvgName = fmt.Sprintf("%sbdtypes-%s-%s-%s", lvmVGNamePrefix, tag, runID, bdtypesNodeSafe(targetNode))
			Expect(kubernetes.CreateLVMVolumeGroup(ctx, cl.RESTConfig(), lvgName, targetNode, bdNames, vgName)).
				To(Succeed(), "failed to create LVMVolumeGroup %s", lvgName)
			DeferCleanup(func() { bdtypesDeleteLVG(ctx, k8sClient, lvgName) })

			Eventually(func(g Gomega) {
				var cur v1alpha1.LVMVolumeGroup
				g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: lvgName}, &cur)).To(Succeed())
				if cur.Status.Phase != v1alpha1.PhaseReady {
					var condSummary []string
					for _, c := range cur.Status.Conditions {
						condSummary = append(condSummary,
							fmt.Sprintf("%s=%s reason=%s msg=%q", c.Type, c.Status, c.Reason, c.Message))
					}
					g.Expect(cur.Status.Phase).To(Equal(v1alpha1.PhaseReady),
						"LVMVolumeGroup %s stuck phase=%s conditions=%v selectedBDs=%v",
						lvgName, cur.Status.Phase, condSummary, bdNames)
				}
			}, bdtypesLVGReadyTimeout, 10*time.Second).Should(Succeed())
			return lvgName, vgName
		}

		DescribeTable("supported device types: discover consumable BD and create Ready LVMVolumeGroup",
			func(tag, expectedType string, transform func(runID, backingPath string) (mapperPath string)) {
				runID := fmt.Sprintf("%d", time.Now().UnixNano())

				By(fmt.Sprintf("[%s] attach plain disk and wait for initial discovery", tag))
				_, plainBD, backingPath := attachPlainDisk(runID, tag)

				bdName := plainBD.Name
				bdPath := backingPath

				if transform != nil {
					By(fmt.Sprintf("[%s] transform backing device into %s", tag, expectedType))
					before, err := kubernetes.GetConsumableBlockDevicesByNode(ctx, cl.RESTConfig(), targetNode)
					Expect(err).NotTo(HaveOccurred())

					mapperPath := transform(runID, backingPath)
					framework.TriggerLVMDiscovery(ctx, cl, targetNode)

					By(fmt.Sprintf("[%s] wait for consumable BlockDevice type=%s path=%s", tag, expectedType, mapperPath))
					newBD, waitErr := framework.WaitNewConsumableBlockDevice(
						ctx, cl.RESTConfig(), targetNode, before, bdtypesDiscoveryTimeout)
					Expect(waitErr).NotTo(HaveOccurred(),
						"expected new consumable BlockDevice for transformed %s device", expectedType)
					DeferCleanup(func() { forceDeleteBlockDevicesByNames(ctx, k8sClient, []string{newBD.Name}) })

					var bdCR v1alpha1.BlockDevice
					Expect(k8sClient.Get(ctx, client.ObjectKey{Name: newBD.Name}, &bdCR)).To(Succeed())
					Expect(bdCR.Status.Type).To(Equal(expectedType),
						"discovered BlockDevice type mismatch for %s", newBD.Name)
					Expect(bdCR.Status.Path).To(Equal(mapperPath))
					Expect(bdCR.Status.Consumable).To(BeTrue())
					// createUniqDeviceName hashes NodeName+Wwn+Model+Serial+PartUUID
					// (not Type/Path). Transformed devices must not collide with the
					// backing disk CR — otherwise LVG selects the wrong object.
					Expect(newBD.Name).NotTo(Equal(plainBD.Name),
						"transformed %s BlockDevice must not reuse backing disk BD name %s",
						expectedType, plainBD.Name)
					bdName = newBD.Name
					bdPath = mapperPath
				} else {
					By(fmt.Sprintf("[%s] assert discovered BlockDevice type=%s", tag, expectedType))
					var bdCR v1alpha1.BlockDevice
					Expect(k8sClient.Get(ctx, client.ObjectKey{Name: bdName}, &bdCR)).To(Succeed())
					Expect(bdCR.Status.Type).To(Equal(expectedType))
					Expect(bdCR.Status.Consumable).To(BeTrue())
				}

				By(fmt.Sprintf("[%s] assert BlockDevice %s is stable and selectable by kubernetes.io/metadata.name", tag, bdName))
				bdtypesAssertBDSelectable(ctx, k8sClient, bdName)

				By(fmt.Sprintf("[%s] create LVMVolumeGroup on BlockDevice %s (%s)", tag, bdName, bdPath))
				lvgName, vgName := createReadyLVG(runID, tag, []string{bdName})

				By(fmt.Sprintf("[%s] confirm BlockDevice linked to VG %s", tag, vgName))
				lvgextWaitBlockDeviceLinkedToVG(ctx, k8sClient, bdName, vgName, lvgextBDLinkageTimeout)

				var ready v1alpha1.LVMVolumeGroup
				Expect(k8sClient.Get(ctx, client.ObjectKey{Name: lvgName}, &ready)).To(Succeed())
				Expect(lvgextCountDevicesOnLVGNode(&ready, targetNode)).To(Equal(1))
				GinkgoWriter.Printf("    [%s] LVG Ready: BD=%s type=%s path=%s VGSize=%s\n",
					tag, bdName, expectedType, bdPath, ready.Status.VGSize.String())
			},

			Entry("disk", "disk", "disk", nil),

			Entry("mpath (multipathd)", "mpath", "mpath",
				func(_runID, backingPath string) string {
					m := bdtypesCreateMpath(ctx, cl, targetNode, backingPath)
					DeferCleanup(func() { bdtypesRemoveMpath(ctx, cl, targetNode, m) })
					return m.MapperPath
				}),

			Entry("crypt (opened LUKS)", "crypt", "crypt",
				func(runID, backingPath string) string {
					mapperName := fmt.Sprintf("e2e-luks-%s", runID)
					mapper := bdtypesOpenLUKS(ctx, cl, targetNode, backingPath, mapperName)
					DeferCleanup(func() { bdtypesCloseLUKS(ctx, cl, targetNode, mapperName, backingPath) })
					return mapper
				}),
		)

		DescribeTable("unsupported device types are filtered from BlockDevice discovery",
			func(tag string, setup func(runID string)) {
				runID := fmt.Sprintf("%d", time.Now().UnixNano())
				By(fmt.Sprintf("[%s] set up unsupported device and assert it is filtered", tag))
				setup(runID)
				GinkgoWriter.Printf("    [%s] filtered OK\n", tag)
			},

			Entry("loop", "loop",
				func(runID string) {
					var list v1alpha1.BlockDeviceList
					Expect(k8sClient.List(ctx, &list)).To(Succeed())
					beforeNames := make(map[string]struct{}, len(list.Items))
					for i := range list.Items {
						beforeNames[list.Items[i].Name] = struct{}{}
					}

					loopPath, filePath := bdtypesCreateLoop(ctx, cl, targetNode, runID)
					DeferCleanup(func() { bdtypesRemoveLoop(ctx, cl, targetNode, loopPath, filePath) })
					framework.TriggerLVMDiscovery(ctx, cl, targetNode)

					By(fmt.Sprintf("assert no BlockDevice CR for loop path %s", loopPath))
					bdtypesAssertNoBDForPath(ctx, k8sClient, targetNode, loopPath)
					bdtypesAssertNoConsumableOfType(ctx, k8sClient, targetNode, "loop", beforeNames)
				}),

			Entry("closed LUKS (crypto_LUKS fstype)", "closed-luks",
				func(runID string) {
					_, plainBD, backingPath := attachPlainDisk(runID, "closed-luks")

					By("luksFormat without open — backing disk gets crypto_LUKS and must leave consumable set")
					bdtypesFormatClosedLUKS(ctx, cl, targetNode, backingPath)
					DeferCleanup(func() {
						_, _ = framework.NodeExecChecked(ctx, cl, targetNode,
							fmt.Sprintf("sudo -n wipefs -a -f %s >/dev/null 2>&1 || true", shellQuote(backingPath)))
					})
					framework.TriggerLVMDiscovery(ctx, cl, targetNode)

					By("plain BlockDevice must become non-consumable or disappear")
					Eventually(func(g Gomega) {
						var bd v1alpha1.BlockDevice
						err := k8sClient.Get(ctx, client.ObjectKey{Name: plainBD.Name}, &bd)
						if client.IgnoreNotFound(err) != nil {
							g.Expect(err).NotTo(HaveOccurred())
						}
						if err != nil {
							return // deleted — acceptable (filtered out)
						}
						g.Expect(bd.Status.Consumable).To(BeFalse(),
							"closed LUKS device must not stay consumable; fsType=%q", bd.Status.FsType)
					}, bdtypesFilterWaitTimeout, bdtypesPollInterval).Should(Succeed())

					By("no new consumable BlockDevice may appear for the closed-LUKS path")
					Consistently(func(g Gomega) {
						consumable, err := kubernetes.GetConsumableBlockDevicesByNode(ctx, cl.RESTConfig(), targetNode)
						g.Expect(err).NotTo(HaveOccurred())
						for _, bd := range consumable {
							g.Expect(bd.Name).NotTo(Equal(plainBD.Name),
								"closed LUKS BlockDevice %s must not be consumable", plainBD.Name)
							var cr v1alpha1.BlockDevice
							if getErr := k8sClient.Get(ctx, client.ObjectKey{Name: bd.Name}, &cr); getErr != nil {
								continue
							}
							g.Expect(cr.Status.Path).NotTo(Equal(backingPath),
								"path %s must not reappear as consumable after luksFormat", backingPath)
						}
					}, bdtypesFilterStableTimeout, bdtypesPollInterval).Should(Succeed())
				}),
		)

		// Mix uses disk+crypt (both already proven Ready in the table above).
		// disk+mpath is intentionally avoided here: mpath setup is environment-
		// sensitive (multipathd/WWID) and is covered by the dedicated mpath Entry.
		It("can mix supported device types (disk + crypt) in one LVMVolumeGroup", func() {
			runID := fmt.Sprintf("%d", time.Now().UnixNano())

			By("Attach first plain disk (type=disk)")
			_, diskBD, _ := attachPlainDisk(runID, "mix-disk")
			var diskCR v1alpha1.BlockDevice
			Expect(k8sClient.Get(ctx, client.ObjectKey{Name: diskBD.Name}, &diskCR)).To(Succeed())
			Expect(diskCR.Status.Type).To(Equal("disk"))

			By("Attach second disk and open it as LUKS crypt")
			_, plainCryptBD, backingPath := attachPlainDisk(runID, "mix-crypt")
			before, err := kubernetes.GetConsumableBlockDevicesByNode(ctx, cl.RESTConfig(), targetNode)
			Expect(err).NotTo(HaveOccurred())

			mapperName := fmt.Sprintf("e2e-luks-mix-%s", runID)
			mapperPath := bdtypesOpenLUKS(ctx, cl, targetNode, backingPath, mapperName)
			DeferCleanup(func() { bdtypesCloseLUKS(ctx, cl, targetNode, mapperName, backingPath) })
			framework.TriggerLVMDiscovery(ctx, cl, targetNode)

			cryptBD, waitErr := framework.WaitNewConsumableBlockDevice(
				ctx, cl.RESTConfig(), targetNode, before, bdtypesDiscoveryTimeout)
			Expect(waitErr).NotTo(HaveOccurred())
			DeferCleanup(func() { forceDeleteBlockDevicesByNames(ctx, k8sClient, []string{cryptBD.Name}) })

			var cryptCR v1alpha1.BlockDevice
			Expect(k8sClient.Get(ctx, client.ObjectKey{Name: cryptBD.Name}, &cryptCR)).To(Succeed())
			Expect(cryptCR.Status.Type).To(Equal("crypt"))
			Expect(cryptCR.Status.Path).To(Equal(mapperPath))
			// Same identity-hash contract as the transformed-device table entries.
			Expect(cryptBD.Name).NotTo(Equal(plainCryptBD.Name),
				"crypt BlockDevice must not reuse backing disk BD name %s", plainCryptBD.Name)
			Expect(cryptBD.Name).NotTo(Equal(diskBD.Name),
				"crypt BlockDevice must not collide with the other disk BD name %s", diskBD.Name)
			bdtypesAssertBDSelectable(ctx, k8sClient, cryptBD.Name)
			bdtypesAssertBDSelectable(ctx, k8sClient, diskBD.Name)

			By("Create LVMVolumeGroup selecting both disk and crypt BlockDevices")
			lvgName, vgName := createReadyLVG(runID, "mix", []string{diskBD.Name, cryptBD.Name})

			lvgextWaitBlockDeviceLinkedToVG(ctx, k8sClient, diskBD.Name, vgName, lvgextBDLinkageTimeout)
			lvgextWaitBlockDeviceLinkedToVG(ctx, k8sClient, cryptBD.Name, vgName, lvgextBDLinkageTimeout)

			var ready v1alpha1.LVMVolumeGroup
			Expect(k8sClient.Get(ctx, client.ObjectKey{Name: lvgName}, &ready)).To(Succeed())
			Expect(lvgextCountDevicesOnLVGNode(&ready, targetNode)).To(Equal(2),
				"mixed LVG should list both BlockDevices")
			devices := lvgextDevicesOnLVGNode(&ready, targetNode)
			Expect(devices).To(HaveKey(diskBD.Name))
			Expect(devices).To(HaveKey(cryptBD.Name))
			GinkgoWriter.Printf("    mix LVG Ready: disk=%s crypt=%s VGSize=%s\n",
				diskBD.Name, cryptBD.Name, ready.Status.VGSize.String())
		})
	})
