package lvg

import (
	"agent/internal"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/api/resource"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func isApplied(lvg *v1alpha1.LVMVolumeGroup) bool {
	for _, c := range lvg.Status.Conditions {
		if c.Type == internal.TypeVGConfigurationApplied && c.Status == v1.ConditionTrue {
			return true
		}
	}

	return false
}

func isThinPool(lv internal.LVData) bool {
	return string(lv.LVAttr[0]) == "t"
}

func getVGAllocatedSize(vg internal.VGData) resource.Quantity {
	allocatedSize := vg.VGSize
	allocatedSize.Sub(vg.VGFree)
	return allocatedSize
}
