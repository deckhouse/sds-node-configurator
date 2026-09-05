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

	"k8s.io/apimachinery/pkg/api/resource"
)

func AlignSizeToExtent(size, extentSize resource.Quantity) (resource.Quantity, error) {
	sizeBytes := size.Value()
	extentBytes := extentSize.Value()
	if extentBytes <= 0 {
		return resource.Quantity{}, fmt.Errorf("extent size must be positive, got %d", extentBytes)
	}
	aligned := ((sizeBytes + extentBytes - 1) / extentBytes) * extentBytes
	return *resource.NewQuantity(aligned, resource.BinarySI), nil
}

// alignSizeToExtentDown aligns a size to the extent boundary by flooring: the
// largest whole number of extents that still fits into the size. Callers reach
// it through AlignRequestedSizeToExtent, which decides the direction from the
// spelling of the request.
func alignSizeToExtentDown(size, extentSize resource.Quantity) (resource.Quantity, error) {
	extentBytes := extentSize.Value()
	if extentBytes <= 0 {
		return resource.Quantity{}, fmt.Errorf("extent size must be positive, got %d", extentBytes)
	}
	floored := (size.Value() / extentBytes) * extentBytes
	return *resource.NewQuantity(floored, resource.BinarySI), nil
}

// AlignRequestedSizeToExtent aligns a requested size the way the spelling of
// the request demands.
//
// An absolute size ("10Gi") is rounded UP: that is the real number of extents
// LVM will consume, so a request that does not fit must still be rejected.
//
// A percentage ("100%", "50%FREE") is rounded DOWN. The percentage is computed
// against a capacity — VG size, or a thin pool's size — that LVM reports as the
// raw device sum and is not itself extent-aligned. Rounding such a request up
// lands one extent past the capacity it was derived from, and the caller then
// concludes that 100% of a thing does not fit into that thing: the create-time
// check rejects the volume, and the extender tries to grow an LV that already
// owns every free extent and fails with "not enough space". Flooring is always
// safe, because a percentage of a capacity can never legitimately exceed it,
// and it also makes split percentages ("50%"+"50%") sum to at most the whole.
func AlignRequestedSizeToExtent(specSize string, requested, extentSize resource.Quantity) (resource.Quantity, error) {
	if IsPercentSize(specSize) {
		return alignSizeToExtentDown(requested, extentSize)
	}
	return AlignSizeToExtent(requested, extentSize)
}
