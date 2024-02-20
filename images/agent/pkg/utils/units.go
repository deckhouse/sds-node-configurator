/*
Copyright 2023 Flant JSC

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
	"math"

	"k8s.io/apimachinery/pkg/api/resource"
)

func BytesToQuantity(size int64) string {
	tmp := resource.NewQuantity(size, resource.BinarySI)
	return tmp.String()
}

func QuantityToBytes(quantity string) (int64, error) {
	b, err := resource.ParseQuantity(quantity)
	if err != nil {
		return 0, err
	}
	return b.Value(), nil
}

func AreSizesEqualWithinDelta(leftSize, rightSize, allowedDelta resource.Quantity) bool {
	leftSizeFloat := float64(leftSize.Value())
	rightSizeFloat := float64(rightSize.Value())

	return math.Abs(leftSizeFloat-rightSizeFloat) < float64(allowedDelta.Value())
}
