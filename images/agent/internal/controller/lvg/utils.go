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

package lvg

import (
	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
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
