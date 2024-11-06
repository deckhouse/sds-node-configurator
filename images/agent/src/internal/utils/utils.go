package utils

import (
	"agent/internal"
	"strconv"
	"strings"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func IsPercentSize(size string) bool {
	return strings.Contains(size, "%")
}

func CheckTag(tags string) (bool, string) {
	if !strings.Contains(tags, internal.LVMTags[0]) {
		return false, ""
	}

	splitTags := strings.Split(tags, ",")
	for _, tag := range splitTags {
		if strings.HasPrefix(tag, "storage.deckhouse.io/lvmVolumeGroupName") {
			kv := strings.Split(tag, "=")
			return true, kv[1]
		}
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
