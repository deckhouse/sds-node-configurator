package cache

import (
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

	sdsCache.StoreDevices(devices)
	sdsCache.StorePVs(pvs)
	sdsCache.StoreVGs(vgs)
	sdsCache.StoreLVs(lvs)

	assert.ElementsMatch(t, devices, sdsCache.GetDevices())
	assert.ElementsMatch(t, pvs, sdsCache.GetPVs())
	assert.ElementsMatch(t, vgs, sdsCache.GetVGs())
	assert.ElementsMatch(t, lvs, sdsCache.GetLVs())
}
