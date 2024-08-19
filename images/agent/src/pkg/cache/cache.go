package cache

import (
	"bytes"
	"fmt"

	"agent/internal"
	"agent/pkg/logger"
)

type Cache struct {
	devices    []internal.Device
	deviceErrs bytes.Buffer
	pvs        []internal.PVData
	pvsErrs    bytes.Buffer
	vgs        []internal.VGData
	vgsErrs    bytes.Buffer
	lvs        []internal.LVData
	lvsErrs    bytes.Buffer
}

func New() *Cache {
	return &Cache{}
}

func (c *Cache) StoreDevices(devices []internal.Device, stdErr bytes.Buffer) {
	c.devices = devices
	c.deviceErrs = stdErr
}

func (c *Cache) GetDevices() ([]internal.Device, bytes.Buffer) {
	dst := make([]internal.Device, len(c.devices))
	copy(dst, c.devices)

	return dst, c.deviceErrs
}

func (c *Cache) StorePVs(pvs []internal.PVData, stdErr bytes.Buffer) {
	c.pvs = pvs
	c.pvsErrs = stdErr
}

func (c *Cache) GetPVs() ([]internal.PVData, bytes.Buffer) {
	dst := make([]internal.PVData, len(c.pvs))
	copy(dst, c.pvs)

	return dst, c.pvsErrs
}

func (c *Cache) StoreVGs(vgs []internal.VGData, stdErr bytes.Buffer) {
	c.vgs = vgs
	c.vgsErrs = stdErr
}

func (c *Cache) GetVGs() ([]internal.VGData, bytes.Buffer) {
	dst := make([]internal.VGData, len(c.vgs))
	copy(dst, c.vgs)

	return dst, c.vgsErrs
}

func (c *Cache) StoreLVs(lvs []internal.LVData, stdErr bytes.Buffer) {
	c.lvs = lvs
	c.lvsErrs = stdErr
}

func (c *Cache) GetLVs() ([]internal.LVData, bytes.Buffer) {
	dst := make([]internal.LVData, len(c.lvs))
	copy(dst, c.lvs)

	return dst, c.lvsErrs
}

func (c *Cache) FindLV(vgName, lvName string) *internal.LVData {
	for _, lv := range c.lvs {
		if lv.VGName == vgName && lv.LVName == lvName {
			return &lv
		}
	}

	return nil
}

func (c *Cache) FindVG(vgName string) *internal.VGData {
	for _, vg := range c.vgs {
		if vg.VGName == vgName {
			return &vg
		}
	}

	return nil
}

func (c *Cache) PrintTheCache(log logger.Logger) {
	log.Cache("*****************CACHE BEGIN*****************")
	log.Cache("[Devices BEGIN]")
	for _, d := range c.devices {
		log.Cache(fmt.Sprintf("     Device Name: %s, size: %s, fsType: %s, serial: %s, wwn: %s", d.Name, d.Size.String(), d.FSType, d.Serial, d.Wwn))
	}
	log.Cache("[ERRS]")
	log.Cache(c.deviceErrs.String())
	log.Cache("[Devices ENDS]")
	log.Cache("[PVs BEGIN]")
	for _, pv := range c.pvs {
		log.Cache(fmt.Sprintf("     PV Name: %s, VG Name: %s, size: %s, vgTags: %s", pv.PVName, pv.VGName, pv.PVSize.String(), pv.VGTags))
	}
	log.Cache("[ERRS]")
	log.Cache(c.pvsErrs.String())
	log.Cache("[PVs ENDS]")
	log.Cache("[VGs BEGIN]")
	for _, vg := range c.vgs {
		log.Cache(fmt.Sprintf("     VG Name: %s, size: %s, free: %s, vgTags: %s", vg.VGName, vg.VGSize.String(), vg.VGFree.String(), vg.VGTags))
	}
	log.Cache("[ERRS]")
	log.Cache(c.vgsErrs.String())
	log.Cache("[VGs ENDS]")
	log.Cache("[LVs BEGIN]")
	for _, lv := range c.lvs {
		log.Cache(fmt.Sprintf("     LV Name: %s, VG name: %s, size: %s, tags: %s, attr: %s, pool: %s", lv.LVName, lv.VGName, lv.LVSize.String(), lv.LvTags, lv.LVAttr, lv.PoolName))
	}
	log.Cache("[ERRS]")
	log.Cache(c.lvsErrs.String())
	log.Cache("[LVs ENDS]")
	log.Cache("*****************CACHE ENDS*****************")
}
