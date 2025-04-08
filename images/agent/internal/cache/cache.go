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
	"fmt"
	"reflect"
	"sync"

	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/logger"
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

	for k, lv := range lvsOnNode {
		if cachedLV, exist := c.lvs[k]; !exist || cachedLV.Exist {
			c.lvs[k] = &LVData{
				Data:  lv,
				Exist: true,
			}
		}
	}

	for key, lv := range c.lvs {
		if lv.Exist && reflect.ValueOf(lv.Data).IsZero() {
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
		Data:  internal.LVData{},
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

func (c *Cache) FindThinPoolMappers(thinLvData *LVData) (tpool, poolMetadataMapper string, err error) {
	if thinLvData.Data.PoolName == "" {
		return "", "", fmt.Errorf("pool name is empty")
	}

	c.m.RLock()
	defer c.m.RUnlock()

	poolLv := c.lvs[c.configureLVKey(thinLvData.Data.VGName, thinLvData.Data.PoolName)]
	if poolLv == nil {
		return "", "", fmt.Errorf("can't find pool %s", thinLvData.Data.PoolName)
	}

	tpool = fmt.Sprintf("%s-tpool", poolLv.Data.LVDmPath)

	if poolLv.Data.MetadataLv == "" {
		return "", "", fmt.Errorf("metadata name is empty for pool %s", thinLvData.Data.PoolName)
	}

	metaLv := c.lvs[c.configureLVKey(thinLvData.Data.VGName, poolLv.Data.MetadataLv)]
	if metaLv == nil {
		return "", "", fmt.Errorf("can't find metadata %s", poolLv.Data.MetadataLv)
	}

	return tpool, metaLv.Data.LVDmPath, nil
}

func (c *Cache) PrintTheCache(log logger.Logger) {
	log.Trace("*****************CACHE BEGIN*****************")
	log.Trace("[Devices BEGIN]")
	for _, d := range c.devices {
		log.Trace(fmt.Sprintf("     Device Name: %s, size: %s, fsType: %s, serial: %s, wwn: %s", d.Name, d.Size.String(), d.FSType, d.Serial, d.Wwn))
	}
	log.Trace("[ERRS]")
	log.Trace(c.deviceErrs.String())
	log.Trace("[Devices ENDS]")
	log.Trace("[PVs BEGIN]")
	for _, pv := range c.pvs {
		log.Trace(fmt.Sprintf("     PV Name: %s, VG Name: %s, size: %s, vgTags: %s", pv.PVName, pv.VGName, pv.PVSize.String(), pv.VGTags))
	}
	log.Trace("[ERRS]")
	log.Trace(c.pvsErrs.String())
	log.Trace("[PVs ENDS]")
	log.Trace("[VGs BEGIN]")
	for _, vg := range c.vgs {
		log.Trace(fmt.Sprintf("     VG Name: %s, size: %s, free: %s, vgTags: %s", vg.VGName, vg.VGSize.String(), vg.VGFree.String(), vg.VGTags))
	}
	log.Trace("[ERRS]")
	log.Trace(c.vgsErrs.String())
	log.Trace("[VGs ENDS]")
	log.Trace("[LVs BEGIN]")

	for key, lv := range c.lvs {
		lvData := lv.Data
		log.Trace(fmt.Sprintf("     Key: %s, Exist: %t, Data Name: %s, VG name: %s, size: %s, tags: %s, attr: %s, pool: %s", key, lv.Exist, lvData.LVName, lvData.VGName, lvData.LVSize.String(), lvData.LvTags, lvData.LVAttr, lvData.PoolName))
	}

	log.Trace("[ERRS]")
	log.Trace(c.lvsErrs.String())
	log.Trace("[LVs ENDS]")
	log.Trace("*****************CACHE ENDS*****************")
}

func (c *Cache) configureLVKey(vgName, lvName string) string {
	return fmt.Sprintf("%s/%s", vgName, lvName)
}
