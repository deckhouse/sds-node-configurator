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

package internal_test

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/deckhouse/sds-common-lib/conditions"
	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
)

// status.consumable said no without saying which of the three reasons applied,
// and an operator had to read the device's other fields and reconstruct it.
var _ = Describe("The Consumable condition", func() {
	publish := func(device internal.Device) *metav1.Condition {
		candidate := internal.NewBlockDeviceCandidateByDevice(&device, "node-1", "machine-1")
		bd := candidate.AsAPIBlockDevice()
		return conditions.Get(bd.Status.Conditions, v1alpha1.BlockDeviceConditionConsumable)
	}

	It("names the reason a device cannot be used", func() {
		for _, tc := range []struct {
			what   string
			device internal.Device
			status metav1.ConditionStatus
			reason string
		}{
			{"an empty device", internal.Device{}, metav1.ConditionTrue, v1alpha1.ReasonDeviceAvailable},
			{"a mounted device", internal.Device{MountPoint: "/var/lib/x"}, metav1.ConditionFalse, v1alpha1.ReasonDeviceMounted},
			{"a formatted device", internal.Device{FSType: "ext4"}, metav1.ConditionFalse, v1alpha1.ReasonDeviceHasFilesystem},
			{"a hot-plugged device", internal.Device{HotPlug: true}, metav1.ConditionFalse, v1alpha1.ReasonDeviceHotPlugged},
			// A partition table with no partitions in it: the kernel shows the
			// disk as empty, and pvcreate still refuses it.
			{"a device with an empty partition table", internal.Device{PartTableType: "gpt"}, metav1.ConditionFalse, v1alpha1.ReasonDeviceHasPartitionTable},
			// The common case, and not a problem: a device already serving as a
			// Physical Volume. Ready would have called this unhealthy.
			{"a Physical Volume", internal.Device{FSType: "LVM2_member"}, metav1.ConditionFalse, v1alpha1.ReasonDeviceHasFilesystem},
		} {
			cond := publish(tc.device)

			Expect(cond).ShouldNot(BeNil(), tc.what)
			Expect(cond.Status).Should(Equal(tc.status), tc.what)
			Expect(cond.Reason).Should(Equal(tc.reason), tc.what)
			Expect(cond.Message).ShouldNot(BeEmpty(), tc.what+" needs a message; the CRD requires one")
		}
	})

	// What a Volume Group creation that failed after pvcreate leaves behind.
	// It kept the LVMVolumeGroup from being created again, since the retry
	// found its device not consumable.
	It("offers a Physical Volume of no Volume Group", func() {
		for _, tc := range []struct {
			what       string
			device     internal.Device
			consumable bool
			reason     string
		}{
			{"a Physical Volume", internal.Device{FSType: "LVM2_member"}, true, v1alpha1.ReasonDeviceOrphanPhysicalVolume},
			{"a hot-plugged Physical Volume", internal.Device{FSType: "LVM2_member", HotPlug: true}, false, v1alpha1.ReasonDeviceHasFilesystem},
			{"a Physical Volume with a partition table", internal.Device{FSType: "LVM2_member", PartTableType: "gpt"}, false, v1alpha1.ReasonDeviceHasFilesystem},
			{"a mounted device", internal.Device{FSType: "LVM2_member", MountPoint: "/var/lib/x"}, false, v1alpha1.ReasonDeviceMounted},
			{"a formatted device", internal.Device{FSType: "ext4"}, false, v1alpha1.ReasonDeviceHasFilesystem},
		} {
			candidate := internal.NewBlockDeviceCandidateByDevice(&tc.device, "node-1", "machine-1")
			candidate.MarkOrphanPhysicalVolume(&tc.device)
			cond := conditions.Get(candidate.AsAPIBlockDevice().Status.Conditions, v1alpha1.BlockDeviceConditionConsumable)

			Expect(candidate.Consumable).Should(Equal(tc.consumable), tc.what)
			Expect(cond).ShouldNot(BeNil(), tc.what)
			Expect(cond.Reason).Should(Equal(tc.reason), tc.what)
			Expect(cond.Message).ShouldNot(BeEmpty(), tc.what)
		}
	})

	// A mounted device also has a filesystem. Unmounting is the action that
	// changes the answer, so that is what the reason has to name.
	It("reports a mounted device as mounted, not as formatted", func() {
		cond := publish(internal.Device{MountPoint: "/var/lib/x", FSType: "ext4"})

		Expect(cond.Reason).Should(Equal(v1alpha1.ReasonDeviceMounted))
	})

	// Wiping the table cannot make a hot-plugged device usable, so the reason
	// has to name the hot plug.
	It("reports a hot-plugged device with a partition table as hot-plugged", func() {
		cond := publish(internal.Device{HotPlug: true, PartTableType: "gpt"})

		Expect(cond.Reason).Should(Equal(v1alpha1.ReasonDeviceHotPlugged))
	})

	It("agrees with status.consumable", func() {
		for _, device := range []internal.Device{
			{},
			{MountPoint: "/mnt"},
			{FSType: "xfs"},
			{HotPlug: true},
			{PartTableType: "dos"},
		} {
			candidate := internal.NewBlockDeviceCandidateByDevice(&device, "node-1", "machine-1")
			bd := candidate.AsAPIBlockDevice()
			cond := conditions.Get(bd.Status.Conditions, v1alpha1.BlockDeviceConditionConsumable)

			Expect(cond.Status == metav1.ConditionTrue).Should(Equal(bd.Status.Consumable),
				"the condition and status.consumable must not disagree")
		}
	})

	// The whole status is replaced on every update, so the conditions have to be
	// carried across deliberately. Without that, lastTransitionTime would reset
	// on every unrelated change the discoverer picks up and would stop meaning
	// "since when has this device been unusable".
	It("keeps lastTransitionTime across an unrelated change", func() {
		device := internal.Device{FSType: "ext4", Name: "/dev/sda"}
		candidate := internal.NewBlockDeviceCandidateByDevice(&device, "node-1", "machine-1")
		bd := candidate.AsAPIBlockDevice()

		was := metav1.NewTime(time.Now().Add(-time.Hour))
		bd.Status.Conditions[0].LastTransitionTime = was

		renamed := internal.Device{FSType: "ext4", Name: "/dev/sdb"}
		next := internal.NewBlockDeviceCandidateByDevice(&renamed, "node-1", "machine-1")
		next.UpdateAPIBlockDevice(&bd)

		cond := conditions.Get(bd.Status.Conditions, v1alpha1.BlockDeviceConditionConsumable)
		Expect(cond.LastTransitionTime).Should(Equal(was),
			"the device did not become usable or unusable, so the timestamp must not move")
	})

	// The discoverer hands UpdateAPIBlockDevice a BlockDevice by value, taken
	// from the map it listed from the API. A struct copy copies the slice
	// header, not the array behind it, and meta.SetStatusCondition edits an
	// existing condition in place — so without copying the conditions the
	// update would write through into the caller's map.
	It("does not write through into the caller's copy", func() {
		mounted := internal.Device{MountPoint: "/mnt", FSType: "ext4"}
		bd := internal.NewBlockDeviceCandidateByDevice(&mounted, "node-1", "machine-1").AsAPIBlockDevice()
		bd.Name = "dev"
		listed := map[string]v1alpha1.BlockDevice{"dev": bd}

		byValue := listed["dev"]
		unmounted := internal.Device{FSType: "ext4"}
		next := internal.NewBlockDeviceCandidateByDevice(&unmounted, "node-1", "machine-1")
		next.UpdateAPIBlockDevice(&byValue)

		Expect(conditions.Get(listed["dev"].Status.Conditions, v1alpha1.BlockDeviceConditionConsumable).Reason).
			Should(Equal(v1alpha1.ReasonDeviceMounted),
				"the listed device must keep the verdict the API server holds")
		Expect(conditions.Get(byValue.Status.Conditions, v1alpha1.BlockDeviceConditionConsumable).Reason).
			Should(Equal(v1alpha1.ReasonDeviceHasFilesystem))
	})

	// status.consumable stays false across this change, and so does every other
	// field the old diff compared — without consulting the condition the
	// discoverer would go on publishing a reason naming a mount that is gone.
	It("makes the discoverer write when only the reason changed", func() {
		mounted := internal.Device{MountPoint: "/mnt", FSType: "ext4"}
		bd := internal.NewBlockDeviceCandidateByDevice(&mounted, "node-1", "machine-1").AsAPIBlockDevice()

		unmounted := internal.Device{FSType: "ext4"}
		next := internal.NewBlockDeviceCandidateByDevice(&unmounted, "node-1", "machine-1")

		Expect(next.HasBlockDeviceDiff(bd)).Should(BeTrue())
	})
})
