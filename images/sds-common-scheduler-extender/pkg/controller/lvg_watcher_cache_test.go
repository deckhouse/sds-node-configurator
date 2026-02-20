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

package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	snc "github.com/deckhouse/sds-node-configurator/api/v1alpha1"
)

func TestLVGWatcherCache(t *testing.T) {
	t.Run("shouldReconcileLVG", func(t *testing.T) {
		t.Run("deletion_timestamp_not_nil_returns_false", func(t *testing.T) {
			lvg := &snc.LVMVolumeGroup{}
			lvg.DeletionTimestamp = &v1.Time{}

			assert.False(t, shouldReconcileLVG(&snc.LVMVolumeGroup{}, lvg))
		})

		t.Run("allocated_size_and_status_thin_pools_equal_returns_false", func(t *testing.T) {
			size := resource.MustParse("1G")
			thinPools := []snc.LVMVolumeGroupThinPoolStatus{
				{
					Name:       "thin",
					ActualSize: resource.MustParse("1G"),
				},
			}
			oldLvg := &snc.LVMVolumeGroup{
				ObjectMeta: v1.ObjectMeta{
					Name: "first",
				},
				Status: snc.LVMVolumeGroupStatus{
					AllocatedSize: size,
					ThinPools:     thinPools,
				},
			}
			newLvg := &snc.LVMVolumeGroup{
				ObjectMeta: v1.ObjectMeta{
					Name: "first",
				},
				Status: snc.LVMVolumeGroupStatus{
					AllocatedSize: size,
					ThinPools:     thinPools,
				},
			}

			assert.False(t, shouldReconcileLVG(oldLvg, newLvg))
		})

		t.Run("allocated_size_not_equal_returns_true", func(t *testing.T) {
			thinPools := []snc.LVMVolumeGroupThinPoolStatus{
				{
					Name:       "thin",
					ActualSize: resource.MustParse("1G"),
				},
			}
			oldLvg := &snc.LVMVolumeGroup{
				ObjectMeta: v1.ObjectMeta{
					Name: "first",
				},
				Status: snc.LVMVolumeGroupStatus{
					AllocatedSize: resource.MustParse("1G"),
					ThinPools:     thinPools,
				},
			}
			newLvg := &snc.LVMVolumeGroup{
				ObjectMeta: v1.ObjectMeta{
					Name: "first",
				},
				Status: snc.LVMVolumeGroupStatus{
					AllocatedSize: resource.MustParse("2G"),
					ThinPools:     thinPools,
				},
			}

			assert.True(t, shouldReconcileLVG(oldLvg, newLvg))
		})

		t.Run("status_thin_pools_not_equal_returns_false", func(t *testing.T) {
			size := resource.MustParse("1G")
			oldLvg := &snc.LVMVolumeGroup{
				ObjectMeta: v1.ObjectMeta{
					Name: "first",
				},
				Status: snc.LVMVolumeGroupStatus{
					AllocatedSize: size,
					ThinPools: []snc.LVMVolumeGroupThinPoolStatus{
						{
							Name:       "thin",
							ActualSize: resource.MustParse("1G"),
						},
					},
				},
			}
			newLvg := &snc.LVMVolumeGroup{
				ObjectMeta: v1.ObjectMeta{
					Name: "first",
				},
				Status: snc.LVMVolumeGroupStatus{
					AllocatedSize: size,
					ThinPools: []snc.LVMVolumeGroupThinPoolStatus{
						{
							Name:       "thin",
							ActualSize: resource.MustParse("2G"),
						},
					},
				},
			}

			assert.True(t, shouldReconcileLVG(oldLvg, newLvg))
		})
	})
}
