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

package cache

import (
	"bytes"
	"testing"

	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/stretchr/testify/assert"
)

func TestCache(t *testing.T) {
	t.Run("general_functionality", func(t *testing.T) {
		sdsCache := New()
		devices := []internal.Device{
			{
				Name: "test-1",
			},
			{
				Name: "test-2",
			},
			{
				Name: "test-3",
			},
		}

		pvs := []internal.PVData{
			{
				PVName: "pv-1",
			},
			{
				PVName: "pv-2",
			},
			{
				PVName: "pv-3",
			},
		}

		vgs := []internal.VGData{
			{
				VGName: "vg-1",
			},
			{
				VGName: "vg-2",
			},
			{
				VGName: "vg-3",
			},
		}

		lvs := []internal.LVData{
			{
				LVName: "lv-1",
			},
			{
				LVName: "lv-2",
			},
			{
				LVName: "lv-3",
			},
		}

		sdsCache.StoreDevices(devices, bytes.Buffer{})
		sdsCache.StorePVs(pvs, bytes.Buffer{})
		sdsCache.StoreVGs(vgs, bytes.Buffer{})
		sdsCache.StoreLVs(lvs, bytes.Buffer{})

		actualDev, _ := sdsCache.GetDevices()
		actualPVs, _ := sdsCache.GetPVs()
		actualVGs, _ := sdsCache.GetVGs()
		actualLVs, _ := sdsCache.GetLVs()
		assert.ElementsMatch(t, devices, actualDev)
		assert.ElementsMatch(t, pvs, actualPVs)
		assert.ElementsMatch(t, vgs, actualVGs)
		assert.ElementsMatch(t, lvs, actualLVs)
	})

	t.Run("StoreLVs_LV_is_empty_and_exist_is_true_do_not_delete_it", func(t *testing.T) {
		const (
			key = "some-vg/some-lv"
		)
		cache := New()
		cache.AddLV("some-vg", "some-lv")
		cache.StoreLVs([]internal.LVData{}, bytes.Buffer{})

		_, exist := cache.lvs[key]
		assert.True(t, exist)
	})

	t.Run("StoreLVs_LV_is_empty_and_exist_is_false_delete_it", func(t *testing.T) {
		const (
			key = "some-vg/some-lv"
		)
		cache := New()
		cache.lvs = map[string]*LVData{
			key: {Exist: false},
		}

		cache.StoreLVs([]internal.LVData{}, bytes.Buffer{})

		_, exist := cache.lvs[key]
		assert.False(t, exist)
	})

	t.Run("StoreLVs_LV_is_not_empty_and_exist_is_true_do_not_delete_it", func(t *testing.T) {
		const (
			key = "some-vg/some-lv"
		)
		lv := internal.LVData{
			LVName: "some-lv",
			VGName: "some-vg",
		}
		cache := New()
		cache.lvs = map[string]*LVData{
			key: {
				Exist: true,
				Data:  lv},
		}

		cache.StoreLVs([]internal.LVData{lv}, bytes.Buffer{})

		_, exist := cache.lvs[key]
		assert.True(t, exist)
	})

	t.Run("StoreLVs_LV_is_not_empty_and_exist_is_true_delete_it_due_to_not_on_the_node", func(t *testing.T) {
		const (
			key = "some-vg/some-lv"
		)
		lv := internal.LVData{
			LVName: "some-lv",
			VGName: "some-vg",
		}
		cache := New()
		cache.lvs = map[string]*LVData{
			key: {
				Exist: true,
				Data:  lv},
		}

		cache.StoreLVs([]internal.LVData{}, bytes.Buffer{})

		_, exist := cache.lvs[key]
		assert.False(t, exist)
	})
}

func BenchmarkCache(b *testing.B) {
	sdsCache := New()
	devices := []internal.Device{
		{
			Name: "test-1",
		},
		{
			Name: "test-2",
		},
		{
			Name: "test-3",
		},
	}

	pvs := []internal.PVData{
		{
			PVName: "pv-1",
		},
		{
			PVName: "pv-2",
		},
		{
			PVName: "pv-3",
		},
	}

	vgs := []internal.VGData{
		{
			VGName: "vg-1",
		},
		{
			VGName: "vg-2",
		},
		{
			VGName: "vg-3",
		},
	}

	lvs := []internal.LVData{
		{
			LVName: "lv-1",
		},
		{
			LVName: "lv-2",
		},
		{
			LVName: "lv-3",
		},
	}

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			sdsCache.StoreDevices(devices, bytes.Buffer{})
			sdsCache.StorePVs(pvs, bytes.Buffer{})
			sdsCache.StoreVGs(vgs, bytes.Buffer{})
			sdsCache.StoreLVs(lvs, bytes.Buffer{})

			sdsCache.GetDevices()
			sdsCache.GetPVs()
			sdsCache.GetVGs()
			sdsCache.GetLVs()
		}
	})
}
