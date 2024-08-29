package cache

import (
	"bytes"
	"fmt"
	"sync"

	"agent/internal"
	"agent/pkg/logger"
)

const (
	lvcount = 50
)

type Cache struct {
	m          sync.RWMutex
	devices    []internal.Device
	deviceErrs bytes.Buffer
	pvs        []internal.PVData
	pvsErrs    bytes.Buffer
	vgs        []internal.VGData
	vgsErrs    bytes.Buffer
	lvs        map[string]*LVData
	lvsErrs    bytes.Buffer
}

type LVData struct {
	Data  internal.LVData
	Exist bool
}

func New() *Cache {
	return &Cache{
		lvs: make(map[string]*LVData, lvcount),
	}
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
	lvsOnNode := make(map[string]internal.LVData, len(lvs))
	for _, lv := range lvs {
		lvsOnNode[c.configureLVKey(lv.VGName, lv.LVName)] = lv
	}

	c.m.Lock()
	defer c.m.Unlock()

	for _, lv := range lvsOnNode {
		k := c.configureLVKey(lv.VGName, lv.LVName)
		if cachedLV, exist := c.lvs[k]; !exist || cachedLV.Exist {
			c.lvs[k] = &LVData{
				Data:  lv,
				Exist: true,
			}
		}
	}

	for key, lv := range c.lvs {
		if lv.Exist {
			continue
		}

		if _, exist := lvsOnNode[key]; !exist {
			delete(c.lvs, key)
		}
	}

	c.lvsErrs = stdErr
}

func (c *Cache) GetLVs() ([]internal.LVData, bytes.Buffer) {
	dst := make([]internal.LVData, 0, len(c.lvs))

	c.m.RLock()
	defer c.m.RUnlock()
	for _, lv := range c.lvs {
		dst = append(dst, lv.Data)
	}

	return dst, c.lvsErrs
}

func (c *Cache) FindLV(vgName, lvName string) *LVData {
	c.m.RLock()
	defer c.m.RUnlock()
	return c.lvs[c.configureLVKey(vgName, lvName)]
}

func (c *Cache) AddLV(vgName, lvName string) {
	c.m.Lock()
	defer c.m.Unlock()
	c.lvs[c.configureLVKey(vgName, lvName)] = &LVData{
		Data:  internal.LVData{VGName: vgName, LVName: lvName},
		Exist: true,
	}
}

func (c *Cache) MarkLVAsRemoved(vgName, lvName string) {
	c.m.Lock()
	defer c.m.Unlock()

	c.lvs[c.configureLVKey(vgName, lvName)].Exist = false
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
	lvs, _ := c.GetLVs()
	for _, lv := range lvs {
		log.Cache(fmt.Sprintf("     Data Name: %s, VG name: %s, size: %s, tags: %s, attr: %s, pool: %s", lv.LVName, lv.VGName, lv.LVSize.String(), lv.LvTags, lv.LVAttr, lv.PoolName))
	}
	log.Cache("[ERRS]")
	log.Cache(c.lvsErrs.String())
	log.Cache("[LVs ENDS]")
	log.Cache("*****************CACHE ENDS*****************")
}

func (c *Cache) configureLVKey(vgName, lvName string) string {
	return fmt.Sprintf("%s/%s", vgName, lvName)
}
