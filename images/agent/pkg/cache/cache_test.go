package cache

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"sds-node-configurator/internal"
	"testing"
)

func TestCache(t *testing.T) {
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
