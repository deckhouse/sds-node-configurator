package cache

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"testing"

	"agent/internal"
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
