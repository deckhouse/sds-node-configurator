package cache

import (
	"fmt"
	"sds-node-configurator/internal"
	"sds-node-configurator/pkg/logger"
)

type Cache struct {
	devices []internal.Device
	pvs     []internal.PVData
	vgs     []internal.VGData
	lvs     []internal.LVData
}

func New() *Cache {
	return &Cache{}
}

func (c *Cache) StoreDevices(devices []internal.Device) {
	c.devices = devices
}

func (c *Cache) GetDevices() []internal.Device {
	dst := make([]internal.Device, len(c.devices))
	copy(dst, c.devices)

	return dst
}

func (c *Cache) StorePVs(pvs []internal.PVData) {
	c.pvs = pvs
}

func (c *Cache) GetPVs() []internal.PVData {
	dst := make([]internal.PVData, len(c.pvs))
	copy(dst, c.pvs)

	return dst
}

func (c *Cache) StoreVGs(vgs []internal.VGData) {
	c.vgs = vgs
}

func (c *Cache) GetVGs() []internal.VGData {
	dst := make([]internal.VGData, len(c.vgs))
	copy(dst, c.vgs)

	return dst
}

func (c *Cache) StoreLVs(lvs []internal.LVData) {
	c.lvs = lvs
}

func (c *Cache) GetLVs() []internal.LVData {
	dst := make([]internal.LVData, len(c.lvs))
	copy(dst, c.lvs)

	return dst
}

func (c *Cache) PrintTheCache(log logger.Logger) {
	log.Cache("*****************CACHE BEGIN*****************")
	log.Cache("[Devices BEGIN]")
	for _, d := range c.devices {
		log.Cache(fmt.Sprintf("     Device Name: %s, size: %s, fsType: %s, serial: %s, wwn: %s", d.Name, d.Size.String(), d.FSType, d.Serial, d.Wwn))
	}
	log.Cache("[Devices ENDS]")
	log.Cache("[PVs BEGIN]")
	for _, pv := range c.pvs {
		log.Cache(fmt.Sprintf("     PV Name: %s, VG Name: %s, size: %s, vgTags: %s", pv.PVName, pv.VGName, pv.PVSize.String(), pv.VGTags))
	}
	log.Cache("[PVs ENDS]")
	log.Cache("[VGs BEGIN]")
	for _, vg := range c.vgs {
		log.Cache(fmt.Sprintf("     VG Name: %s, size: %s, free: %s, vgTags: %s", vg.VGName, vg.VGSize.String(), vg.VGFree.String(), vg.VGTags))
	}
	log.Cache("[VGs ENDS]")
	log.Cache("[LVs BEGIN]")
	for _, lv := range c.lvs {
		log.Cache(fmt.Sprintf("     LV Name: %s, VG name: %s, size: %s, tags: %s, attr: %s, pool: %s", lv.LVName, lv.VGName, lv.LVSize.String(), lv.LvTags, lv.LVAttr, lv.PoolLv))
	}
	log.Cache("[LVs ENDS]")
	log.Cache("*****************CACHE ENDS*****************")
}
