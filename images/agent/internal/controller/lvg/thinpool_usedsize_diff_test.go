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

package lvg

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
)

const gi = int64(1024 * 1024 * 1024)

func usedSizeLVG(t *testing.T, actual, used int64) (v1alpha1.LVMVolumeGroup, internal.LVMVolumeGroupCandidate) {
	t.Helper()
	tps := []internal.LVMVGStatusThinPool{{
		Name:          "tp",
		ActualSize:    *resource.NewQuantity(actual, resource.BinarySI),
		UsedSize:      *resource.NewQuantity(used, resource.BinarySI),
		AllocatedSize: *resource.NewQuantity(actual, resource.BinarySI),
		Ready:         true,
	}}
	candidate := internal.LVMVolumeGroupCandidate{
		AllocatedSize:   *resource.NewQuantity(actual, resource.BinarySI),
		VGSize:          *resource.NewQuantity(2*actual, resource.BinarySI),
		VGFree:          *resource.NewQuantity(actual, resource.BinarySI),
		StatusThinPools: tps,
	}
	statusTPs, err := convertStatusThinPools(v1alpha1.LVMVolumeGroup{}, tps)
	assert.NoError(t, err)
	lvg := v1alpha1.LVMVolumeGroup{Status: v1alpha1.LVMVolumeGroupStatus{
		AllocatedSize: candidate.AllocatedSize,
		VGSize:        candidate.VGSize,
		VGFree:        candidate.VGFree,
		ThinPools:     statusTPs,
	}}
	return lvg, candidate
}

func TestHasLVMVolumeGroupDiff_ThinPoolUsedSize(t *testing.T) {
	const actual = 10 * gi
	const onePercent = (actual + 99) / 100
	const published = 70866960

	cases := []struct {
		name string
		from int64
		to   int64
		diff bool
	}{
		{name: "unchanged", from: published, to: published, diff: false},
		{name: "below_one_percent_of_the_pool", from: published, to: published + onePercent - 1, diff: false},
		{name: "one_percent_of_the_pool", from: published, to: published + onePercent, diff: true},
		{name: "a_gibibyte_written_into_a_10Gi_pool", from: published, to: 1212222013, diff: true},
		{name: "freed_by_one_percent", from: 2 * gi, to: 2*gi - onePercent, diff: true},
		{name: "freed_by_less_than_one_percent", from: 2 * gi, to: 2*gi - onePercent + 1, diff: false},
		{name: "the_pool_got_full", from: published, to: actual, diff: true},
		{name: "a_near_full_pool_reaching_full", from: actual - 1024*1024, to: actual, diff: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lvg, candidate := usedSizeLVG(t, actual, tc.from)
			candidate.StatusThinPools[0].UsedSize = *resource.NewQuantity(tc.to, resource.BinarySI)
			assert.Equal(t, tc.diff, hasLVMVolumeGroupDiff(logger.NewNop(), lvg, candidate))
		})
	}

	t.Run("a_small_change_goes_out_with_any_other_change", func(t *testing.T) {
		lvg, candidate := usedSizeLVG(t, actual, published)
		candidate.StatusThinPools[0].UsedSize = *resource.NewQuantity(published+1024*1024, resource.BinarySI)
		candidate.StatusThinPools[0].AllocatedSize = *resource.NewQuantity(actual+gi, resource.BinarySI)
		assert.True(t, hasLVMVolumeGroupDiff(logger.NewNop(), lvg, candidate))
	})
}
