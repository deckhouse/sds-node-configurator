/*
Copyright 2025 Flant JSC

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

package utils

import (
	"fmt"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
)

func IsPercentSize(size string) bool {
	return strings.Contains(size, "%")
}

func NewEnabledTags(key string, value string) []string {
	return []string{internal.LVMTags[0], fmt.Sprintf("%s=%s", key, value)}
}

// HasManagedTag reports whether an LVM tag list contains the exact
// storage.deckhouse.io/enabled=true tag that marks a Volume Group as this
// module's.
//
// The comparison is against whole comma-separated elements rather than a
// substring of the joined list. LVM's tag charset allows a tag to merely
// contain that text (`x-storage.deckhouse.io/enabled=true` is a legal tag), and
// the answer decides both whether a Volume Group is adopted and — in
// FilterForeignLoopPVs — whether its loop PVs are allowed into the cache at all.
func HasManagedTag(tags string) bool {
	for _, tag := range strings.Split(tags, ",") {
		if strings.TrimSpace(tag) == internal.LVMTags[0] {
			return true
		}
	}
	return false
}

// ReadValueFromTags reports whether the tag list marks the object as this
// module's and, if so, the value of the key=value tag named by key ("" when the
// list carries no such tag).
//
// Both halves of a tag are parsed rather than assumed. LVM's tag charset is far
// wider than what the agent itself writes — an administrator can hand a Volume
// Group over with any tag `vgchange --addtag` accepts, and that includes a bare
// `<key>` with no `=` at all. Splitting on "=" and taking element [1] panicked on
// exactly that, taking the whole DaemonSet into CrashLoopBackOff, and
// spec.fileDevices widened the set of Volume Groups this is asked about: dropping
// `loop` from LVMGlobalFilter made Volume Groups the agent never wrote tags for
// visible to lvm.static.
//
// The key must match a whole tag name, not a prefix of one, so a tag that merely
// starts with the same text is not read as the tag being asked for. The value is
// everything after the first "=", since LVM allows one inside a tag.
func ReadValueFromTags(tags string, key string) (bool, string) {
	if !HasManagedTag(tags) {
		return false, ""
	}

	for _, tag := range strings.Split(tags, ",") {
		name, value, ok := strings.Cut(strings.TrimSpace(tag), "=")
		if !ok || name != key {
			continue
		}
		return true, value
	}

	return true, ""
}

func GetRequestedSizeFromString(size string, targetSpace resource.Quantity) (resource.Quantity, error) {
	if IsPercentSize(size) {
		strPercent := strings.Split(size, "%")[0]
		percent, err := strconv.Atoi(strPercent)
		if err != nil {
			return resource.Quantity{}, err
		}
		lvSize := targetSpace.Value() * int64(percent) / 100
		return *resource.NewQuantity(lvSize, resource.BinarySI), nil
	}
	return resource.ParseQuantity(size)
}

func GetThinPoolAvailableSpace(actualSize, allocatedSize resource.Quantity, allocationLimit string) (resource.Quantity, error) {
	totalSize, err := GetThinPoolSpaceWithAllocationLimit(actualSize, allocationLimit)
	if err != nil {
		return resource.Quantity{}, err
	}

	return *resource.NewQuantity(totalSize.Value()-allocatedSize.Value(), resource.BinarySI), nil
}

func GetThinPoolSpaceWithAllocationLimit(actualSize resource.Quantity, allocationLimit string) (resource.Quantity, error) {
	limits := strings.Split(allocationLimit, "%")
	percent, err := strconv.Atoi(limits[0])
	if err != nil {
		return resource.Quantity{}, err
	}

	factor := float64(percent)
	factor /= 100

	return *resource.NewQuantity(int64(float64(actualSize.Value())*factor), resource.BinarySI), nil
}

func GetLLVRequestedSize(llv *v1alpha1.LVMLogicalVolume, lvg *v1alpha1.LVMVolumeGroup) (resource.Quantity, error) {
	switch llv.Spec.Type {
	case internal.Thick:
		return GetRequestedSizeFromString(llv.Spec.Size, lvg.Status.VGSize)
	case internal.Thin:
		for _, tp := range lvg.Status.ThinPools {
			if tp.Name == llv.Spec.Thin.PoolName {
				totalSize, err := GetThinPoolSpaceWithAllocationLimit(tp.ActualSize, tp.AllocationLimit)
				if err != nil {
					return resource.Quantity{}, err
				}

				return GetRequestedSizeFromString(llv.Spec.Size, totalSize)
			}
		}
	}

	return resource.Quantity{}, nil
}

func LVGBelongsToNode(lvg *v1alpha1.LVMVolumeGroup, nodeName string) bool {
	var belongs bool
	for _, node := range lvg.Status.Nodes {
		if node.Name == nodeName {
			belongs = true
		}
	}

	return belongs
}

func GetFreeLVGSpaceForLLV(lvg *v1alpha1.LVMVolumeGroup, llv *v1alpha1.LVMLogicalVolume) resource.Quantity {
	switch llv.Spec.Type {
	case internal.Thick:
		return lvg.Status.VGFree
	case internal.Thin:
		for _, tp := range lvg.Status.ThinPools {
			if tp.Name == llv.Spec.Thin.PoolName {
				return tp.AvailableSpace
			}
		}
	}

	return resource.Quantity{}
}
