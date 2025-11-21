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
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"

	snc "github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/consts"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/logger"
)

const (
	DefaultPVCExpiredDurationSec = 30

	pvcPerLVGCount         = 150
	lvgsPerPVCCount        = 5
	lvgsPerNodeCount       = 5
	SelectedNodeAnnotation = "volume.kubernetes.io/selected-node"
)

type Cache struct {
	mtx             sync.RWMutex
	lvgByName       map[string]*lvgEntry // map[lvgName]*lvgEntry
	log             logger.Logger
	expiredDuration time.Duration
}

type lvgEntry struct {
	lvg        *snc.LVMVolumeGroup
	thickByPVC map[string]*pvcEntry      // map[pvcKey]*pvcEntry
	thinByPool map[string]*thinPoolEntry // map[thinPoolName]*thinPoolEntry
}

type thinPoolEntry struct {
	pvcs map[string]*pvcEntry // map[pvcKey]*pvcEntry
}

type pvcEntry struct {
	pvc          *corev1.PersistentVolumeClaim
	selectedNode string
}

// NewCache initialize new cache.
func NewCache(logger logger.Logger, pvcExpDurSec int) *Cache {
	ch := &Cache{
		lvgByName:       make(map[string]*lvgEntry),
		log:             logger,
		expiredDuration: time.Duration(pvcExpDurSec) * time.Second,
	}

	go func() {
		timer := time.NewTimer(ch.expiredDuration)

		for range timer.C {
			ch.clearBoundExpiredPVC()
			timer.Reset(ch.expiredDuration)
		}
	}()

	return ch
}

func (c *Cache) clearBoundExpiredPVC() {
	c.log.Debug("[clearBoundExpiredPVC] starts to clear expired PVC")
	// Take a snapshot of all PVCs under read lock to avoid holding write lock during removals
	var snapshot []*corev1.PersistentVolumeClaim
	c.mtx.RLock()
	for _, lvg := range c.lvgByName {
		for _, pe := range lvg.thickByPVC {
			snapshot = append(snapshot, pe.pvc)
		}
		for _, tp := range lvg.thinByPool {
			for _, pe := range tp.pvcs {
				snapshot = append(snapshot, pe.pvc)
			}
		}
	}
	c.mtx.RUnlock()

	for _, pvc := range snapshot {
		if pvc.Status.Phase != corev1.ClaimBound {
			c.log.Trace(fmt.Sprintf("[clearBoundExpiredPVC] PVC %s is not in a Bound state", pvc.Name))
			continue
		}
		if time.Since(pvc.CreationTimestamp.Time) > c.expiredDuration {
			c.log.Warning(fmt.Sprintf("[clearBoundExpiredPVC] PVC %s is in a Bound state and expired, remove it from the cache", pvc.Name))
			c.RemovePVCFromTheCache(pvc)
		} else {
			c.log.Trace(fmt.Sprintf("[clearBoundExpiredPVC] PVC %s is in a Bound state but not expired yet.", pvc.Name))
		}
	}
	c.log.Debug("[clearBoundExpiredPVC] finished the expired PVC clearing")
}

// AddLVG adds selected LVMVolumeGroup resource to the cache. If it is already stored, does nothing.
func (c *Cache) AddLVG(lvg *snc.LVMVolumeGroup) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	if _, exists := c.lvgByName[lvg.Name]; exists {
		c.log.Debug(fmt.Sprintf("[AddLVG] the LVMVolumeGroup %s has been already added to the cache", lvg.Name))
		return
	}

	c.lvgByName[lvg.Name] = &lvgEntry{
		lvg:        lvg,
		thickByPVC: make(map[string]*pvcEntry),
		thinByPool: make(map[string]*thinPoolEntry),
	}

	c.log.Trace(fmt.Sprintf("[AddLVG] the LVMVolumeGroup %s nodes: %v", lvg.Name, lvg.Status.Nodes))
}

// UpdateLVG updated selected LVMVolumeGroup resource in the cache. If such LVMVolumeGroup is not stored, returns an error.
func (c *Cache) UpdateLVG(lvg *snc.LVMVolumeGroup) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	entry, found := c.lvgByName[lvg.Name]
	if !found {
		return fmt.Errorf("the LVMVolumeGroup %s was not found in the cache", lvg.Name)
	}
	entry.lvg = lvg
	c.log.Trace(fmt.Sprintf("[UpdateLVG] the LVMVolumeGroup %s nodes: %v", lvg.Name, lvg.Status.Nodes))
	return nil
}

// TryGetLVG returns selected LVMVolumeGroup resource if it is stored in the cache, otherwise returns nil.
func (c *Cache) TryGetLVG(name string) *snc.LVMVolumeGroup {
	c.mtx.RLock()
	defer c.mtx.RUnlock()
	entry, found := c.lvgByName[name]
	if !found || entry == nil {
		c.log.Debug(fmt.Sprintf("[TryGetLVG] the LVMVolumeGroup %s was not found in the cache. Return nil", name))
		return nil
	}

	return entry.lvg
}

// GetLVGNamesByNodeName returns LVMVolumeGroups resources names stored in the cache for the selected node. If none of them exist, returns empty slice.
func (c *Cache) GetLVGNamesByNodeName(nodeName string) []string {
	c.mtx.RLock()
	defer c.mtx.RUnlock()

	var result []string
	for name, entry := range c.lvgByName {
		if lvgHasNode(entry.lvg, nodeName) {
			result = append(result, name)
		}
	}
	if len(result) == 0 {
		c.log.Debug(fmt.Sprintf("[GetLVGNamesByNodeName] no LVMVolumeGroup was found in the cache for the node %s. Return empty slice", nodeName))
	}
	return result
}

// GetAllLVG returns all the LVMVolumeGroups resources stored in the cache.
func (c *Cache) GetAllLVG() map[string]*snc.LVMVolumeGroup {
	c.mtx.RLock()
	defer c.mtx.RUnlock()
	result := make(map[string]*snc.LVMVolumeGroup, len(c.lvgByName))
	for name, entry := range c.lvgByName {
		if entry.lvg == nil {
			c.log.Error(fmt.Errorf("LVMVolumeGroup %s is not initialized", name), "[GetAllLVG] an error occurs while iterating the LVMVolumeGroups")
			continue
		}
		result[name] = entry.lvg
	}
	return result
}

// GetLVGThickReservedSpace returns a sum of reserved space by every thick PVC in the selected LVMVolumeGroup resource. If such LVMVolumeGroup resource is not stored, returns an error.
func (c *Cache) GetLVGThickReservedSpace(lvgName string) (int64, error) {
	c.mtx.RLock()
	defer c.mtx.RUnlock()
	entry, found := c.lvgByName[lvgName]
	if !found || entry == nil {
		c.log.Debug(fmt.Sprintf("[GetLVGThickReservedSpace] the LVMVolumeGroup %s was not found in the cache. Returns 0", lvgName))
		return 0, nil
	}

	var space int64
	for _, pe := range entry.thickByPVC {
		space += pe.pvc.Spec.Resources.Requests.Storage().Value()
	}

	return space, nil
}

// GetLVGThinReservedSpace returns a sum of reserved space by every thin PVC in the selected LVMVolumeGroup resource. If such LVMVolumeGroup resource is not stored, returns an error.
func (c *Cache) GetLVGThinReservedSpace(lvgName string, thinPoolName string) (int64, error) {
	c.mtx.RLock()
	defer c.mtx.RUnlock()
	entry, found := c.lvgByName[lvgName]
	if !found || entry == nil {
		c.log.Debug(fmt.Sprintf("[GetLVGThinReservedSpace] the LVMVolumeGroup %s was not found in the cache. Returns 0", lvgName))
		return 0, nil
	}

	tp, found := entry.thinByPool[thinPoolName]
	if !found || tp == nil {
		c.log.Debug(fmt.Sprintf("[GetLVGThinReservedSpace] the Thin pool %s of the LVMVolumeGroup %s was not found in the cache. Returns 0", lvgName, thinPoolName))
		return 0, nil
	}

	var space int64
	for _, pe := range tp.pvcs {
		space += pe.pvc.Spec.Resources.Requests.Storage().Value()
	}

	return space, nil
}

// DeleteLVG deletes selected LVMVolumeGroup resource from the cache.
func (c *Cache) DeleteLVG(lvgName string) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	delete(c.lvgByName, lvgName)
}

// AddThickPVC adds selected PVC to selected LVMVolumeGroup resource. If the LVMVolumeGroup resource is not stored, returns an error.
// If selected PVC is already stored in the cache, does nothing.
func (c *Cache) AddThickPVC(lvgName string, pvc *corev1.PersistentVolumeClaim) error {
	if pvc.Status.Phase == corev1.ClaimBound {
		c.log.Warning(fmt.Sprintf("[AddThickPVC] PVC %s/%s has status phase BOUND. It will not be added to the cache", pvc.Namespace, pvc.Name))
		return nil
	}

	pvcKey := configurePVCKey(pvc)

	c.mtx.Lock()
	defer c.mtx.Unlock()

	entry, found := c.lvgByName[lvgName]
	if !found || entry == nil {
		err := fmt.Errorf("the LVMVolumeGroup %s was not found in the cache", lvgName)
		c.log.Error(err, fmt.Sprintf("[AddThickPVC] an error occurred while trying to add PVC %s to the cache", pvcKey))
		return err
	}

	// this case might be triggered if the extender recovers after fail and finds some pending thickPVCs with selected nodes
	c.log.Trace(fmt.Sprintf("[AddThickPVC] PVC %s/%s annotations: %v", pvc.Namespace, pvc.Name, pvc.Annotations))

	shouldAdd, err := c.shouldAddPVC(pvc, entry, pvcKey, lvgName, "")
	if err != nil {
		return err
	}

	if !shouldAdd {
		c.log.Debug(fmt.Sprintf("[AddThickPVC] PVC %s should not be added", pvcKey))
		return nil
	}

	c.log.Debug(fmt.Sprintf("[AddThickPVC] new PVC %s cache will be added to the LVMVolumeGroup %s", pvcKey, lvgName))
	c.addNewThickPVC(entry, pvc)

	return nil
}

func (c *Cache) shouldAddPVC(pvc *corev1.PersistentVolumeClaim, entry *lvgEntry, pvcKey, lvgName, thinPoolName string) (bool, error) {
	if pvc.Annotations[SelectedNodeAnnotation] != "" {
		c.log.Debug(fmt.Sprintf("[shouldAddPVC] PVC %s/%s has selected node anotation, selected node: %s", pvc.Namespace, pvc.Name, pvc.Annotations[SelectedNodeAnnotation]))

		if !lvgHasNode(entry.lvg, pvc.Annotations[SelectedNodeAnnotation]) {
			c.log.Debug(fmt.Sprintf("[shouldAddPVC] LVMVolumeGroup %s does not belong to PVC %s/%s selected node %s. It will be skipped", lvgName, pvc.Namespace, pvc.Name, pvc.Annotations[SelectedNodeAnnotation]))
			return false, nil
		}

		c.log.Debug(fmt.Sprintf("[shouldAddPVC] LVMVolumeGroup %s belongs to PVC %s/%s selected node %s", lvgName, pvc.Namespace, pvc.Name, pvc.Annotations[SelectedNodeAnnotation]))

		// if pvc is thick
		if _, found := entry.thickByPVC[pvcKey]; found && thinPoolName == "" {
			c.log.Debug(fmt.Sprintf("[shouldAddPVC] PVC %s was found in the cache of the LVMVolumeGroup %s", pvcKey, lvgName))
			return false, nil
		}

		// if pvc is thin
		if thinPoolName != "" {
			tp, found := entry.thinByPool[thinPoolName]
			if !found || tp == nil {
				c.log.Debug(fmt.Sprintf("[shouldAddPVC] Thin pool %s was not found in the cache, PVC %s should be added", thinPoolName, pvcKey))
				return true, nil
			}

			if _, found = tp.pvcs[pvcKey]; found {
				c.log.Debug(fmt.Sprintf("[shouldAddPVC] PVC %s was found in the Thin pool %s cache of the LVMVolumeGroup %s. No need to add", pvcKey, thinPoolName, lvgName))
				return false, nil
			}
		}
	}

	return true, nil
}

func (c *Cache) AddThinPVC(lvgName, thinPoolName string, pvc *corev1.PersistentVolumeClaim) error {
	if pvc.Status.Phase == corev1.ClaimBound {
		c.log.Warning(fmt.Sprintf("[AddThinPVC] PVC %s/%s has status phase BOUND. It will not be added to the cache", pvc.Namespace, pvc.Name))
		return nil
	}

	pvcKey := configurePVCKey(pvc)

	c.mtx.Lock()
	defer c.mtx.Unlock()

	entry, found := c.lvgByName[lvgName]
	if !found || entry == nil {
		err := fmt.Errorf("the LVMVolumeGroup %s was not found in the cache", lvgName)
		c.log.Error(err, fmt.Sprintf("[AddThinPVC] an error occurred while trying to add PVC %s to the cache", pvcKey))
		return err
	}

	thinPoolBelongs := lvgHasThinPool(entry.lvg, thinPoolName)
	if !thinPoolBelongs {
		err := fmt.Errorf("thin pool %s was not found in the LVMVolumeGroup %s", thinPoolName, lvgName)
		c.log.Error(err, fmt.Sprintf("[AddThinPVC] unable to add Thin pool %s of the LVMVolumeGroup %s for the PVC %s", thinPoolName, lvgName, pvcKey))
		return err
	}

	// this case might be triggered if the extender recovers after fail and finds some pending thin PVCs with selected nodes
	c.log.Trace(fmt.Sprintf("[AddThinPVC] PVC %s/%s annotations: %v", pvc.Namespace, pvc.Name, pvc.Annotations))
	shouldAdd, err := c.shouldAddPVC(pvc, entry, pvcKey, lvgName, thinPoolName)
	if err != nil {
		return err
	}

	if !shouldAdd {
		c.log.Debug(fmt.Sprintf("[AddThinPVC] PVC %s should not be added", pvcKey))
		return nil
	}

	c.log.Debug(fmt.Sprintf("[AddThinPVC] new PVC %s cache will be added to the LVMVolumeGroup %s", pvcKey, lvgName))
	err = c.addNewThinPVC(entry, pvc, thinPoolName)
	if err != nil {
		c.log.Error(err, fmt.Sprintf("[AddThinPVC] unable to add PVC %s to Thin Pool %s of the LVMVolumeGroup %s", pvcKey, thinPoolName, lvgName))
		return err
	}

	return nil
}

func (c *Cache) addNewThickPVC(lvgCh *lvgEntry, pvc *corev1.PersistentVolumeClaim) {
	pvcKey := configurePVCKey(pvc)
	lvgCh.thickByPVC[pvcKey] = &pvcEntry{pvc: pvc, selectedNode: pvc.Annotations[SelectedNodeAnnotation]}
}

func (c *Cache) addNewThinPVC(lvgCh *lvgEntry, pvc *corev1.PersistentVolumeClaim, thinPoolName string) error {
	pvcKey := configurePVCKey(pvc)

	err := c.addThinPoolIfNotExists(lvgCh, thinPoolName)
	if err != nil {
		c.log.Error(err, fmt.Sprintf("[addNewThinPVC] unable to add Thin pool %s in the LVMVolumeGroup %s cache for PVC %s", thinPoolName, lvgCh.lvg.Name, pvc.Name))
		return err
	}

	thinPoolCh := lvgCh.thinByPool[thinPoolName]
	if thinPoolCh == nil {
		err = fmt.Errorf("thin pool %s not found", thinPoolName)
		c.log.Error(err, fmt.Sprintf("[addNewThinPVC] unable to add Thin PVC %s to the cache", pvcKey))
		return err
	}

	thinPoolCh.pvcs[pvcKey] = &pvcEntry{pvc: pvc, selectedNode: pvc.Annotations[SelectedNodeAnnotation]}
	c.log.Debug(fmt.Sprintf("[addNewThinPVC] THIN PVC %s was added to the cache to Thin Pool %s", pvcKey, thinPoolName))
	return nil
}

// UpdateThickPVC updates selected PVC in selected LVMVolumeGroup resource. If no such PVC is stored in the cache, adds it.
func (c *Cache) UpdateThickPVC(lvgName string, pvc *corev1.PersistentVolumeClaim) error {
	pvcKey := configurePVCKey(pvc)

	c.mtx.Lock()
	defer c.mtx.Unlock()

	entry, found := c.lvgByName[lvgName]
	if !found || entry == nil {
		return fmt.Errorf("the LVMVolumeGroup %s was not found in the cache", lvgName)
	}

	pvcCh, found := entry.thickByPVC[pvcKey]
	if !found || pvcCh == nil {
		c.log.Warning(fmt.Sprintf("[UpdateThickPVC] PVC %s was not found in the cache for the LVMVolumeGroup %s. It will be added", pvcKey, lvgName))
		c.addNewThickPVC(entry, pvc)
		return nil
	}

	pvcCh.pvc = pvc
	pvcCh.selectedNode = pvc.Annotations[SelectedNodeAnnotation]
	c.log.Debug(fmt.Sprintf("[UpdateThickPVC] successfully updated PVC %s with selected node %s in the cache for LVMVolumeGroup %s", pvcKey, pvc.Annotations[SelectedNodeAnnotation], lvgName))

	return nil
}

func (c *Cache) UpdateThinPVC(lvgName, thinPoolName string, pvc *corev1.PersistentVolumeClaim) error {
	pvcKey := configurePVCKey(pvc)

	c.mtx.Lock()
	defer c.mtx.Unlock()

	entry, found := c.lvgByName[lvgName]
	if !found || entry == nil {
		return fmt.Errorf("the LVMVolumeGroup %s was not found in the cache", lvgName)
	}

	thinPoolCh, found := entry.thinByPool[thinPoolName]
	if !found || thinPoolCh == nil {
		c.log.Debug(fmt.Sprintf("[UpdateThinPVC] Thin Pool %s was not found in the LVMVolumeGroup %s, add it.", thinPoolName, lvgName))
		err := c.addThinPoolIfNotExists(entry, thinPoolName)
		if err != nil {
			return err
		}
		thinPoolCh = entry.thinByPool[thinPoolName]
	}

	pvcCh, found := thinPoolCh.pvcs[pvcKey]
	if !found || pvcCh == nil {
		c.log.Warning(fmt.Sprintf("[UpdateThinPVC] Thin PVC %s was not found in Thin pool %s in the cache for the LVMVolumeGroup %s. It will be added", pvcKey, thinPoolName, lvgName))
		err := c.addNewThinPVC(entry, pvc, thinPoolName)
		if err != nil {
			c.log.Error(err, fmt.Sprintf("[UpdateThinPVC] an error occurred while trying to update the PVC %s", pvcKey))
			return err
		}
		return nil
	}

	pvcCh.pvc = pvc
	pvcCh.selectedNode = pvc.Annotations[SelectedNodeAnnotation]
	c.log.Debug(fmt.Sprintf("[UpdateThinPVC] successfully updated THIN PVC %s with selected node %s in the cache for LVMVolumeGroup %s", pvcKey, pvc.Annotations[SelectedNodeAnnotation], lvgName))

	return nil
}

func (c *Cache) addThinPoolIfNotExists(lvgCh *lvgEntry, thinPoolName string) error {
	if len(thinPoolName) == 0 {
		err := errors.New("no thin pool name specified")
		c.log.Error(err, fmt.Sprintf("[addThinPoolIfNotExists] unable to add thin pool in the LVMVolumeGroup %s", lvgCh.lvg.Name))
		return err
	}

	if _, found := lvgCh.thinByPool[thinPoolName]; found {
		c.log.Debug(fmt.Sprintf("[addThinPoolIfNotExists] Thin pool %s is already created in the LVMVolumeGroup %s. No need to add a new one", thinPoolName, lvgCh.lvg.Name))
		return nil
	}

	lvgCh.thinByPool[thinPoolName] = &thinPoolEntry{
		pvcs: make(map[string]*pvcEntry),
	}
	return nil
}

// GetAllPVCForLVG returns slice of PVC belonging to selected LVMVolumeGroup resource. If such LVMVolumeGroup is not stored in the cache, returns an error.
func (c *Cache) GetAllPVCForLVG(lvgName string) ([]*corev1.PersistentVolumeClaim, error) {
	c.mtx.RLock()
	defer c.mtx.RUnlock()

	lvgCh, found := c.lvgByName[lvgName]
	if !found || lvgCh == nil {
		err := fmt.Errorf("cache was not found for the LVMVolumeGroup %s", lvgName)
		c.log.Error(err, fmt.Sprintf("[GetAllPVCForLVG] an error occurred while trying to get all PVC for the LVMVolumeGroup %s", lvgName))
		return nil, err
	}

	result := make([]*corev1.PersistentVolumeClaim, 0, pvcPerLVGCount)
	// collect Thick PVC for the LVG
	for _, pe := range lvgCh.thickByPVC {
		result = append(result, pe.pvc)
	}

	// collect Thin PVC for the LVG
	for _, tp := range lvgCh.thinByPool {
		for _, pe := range tp.pvcs {
			result = append(result, pe.pvc)
		}
	}

	return result, nil
}

// GetAllThickPVCLVG returns slice of PVC belonging to selected LVMVolumeGroup resource. If such LVMVolumeGroup is not stored in the cache, returns an error.
func (c *Cache) GetAllThickPVCLVG(lvgName string) ([]*corev1.PersistentVolumeClaim, error) {
	c.mtx.RLock()
	defer c.mtx.RUnlock()

	lvgCh, found := c.lvgByName[lvgName]
	if !found || lvgCh == nil {
		err := fmt.Errorf("cache was not found for the LVMVolumeGroup %s", lvgName)
		c.log.Error(err, fmt.Sprintf("[GetAllPVCForLVG] an error occurred while trying to get all PVC for the LVMVolumeGroup %s", lvgName))
		return nil, err
	}

	result := make([]*corev1.PersistentVolumeClaim, 0, pvcPerLVGCount)
	// collect Thick PVC for the LVG
	for _, pe := range lvgCh.thickByPVC {
		result = append(result, pe.pvc)
	}

	return result, nil
}

// GetAllPVCFromLVGThinPool returns slice of PVC belonging to selected LVMVolumeGroup resource. If such LVMVolumeGroup is not stored in the cache, returns an error.
func (c *Cache) GetAllPVCFromLVGThinPool(lvgName, thinPoolName string) ([]*corev1.PersistentVolumeClaim, error) {
	c.mtx.RLock()
	defer c.mtx.RUnlock()

	lvgCh, found := c.lvgByName[lvgName]
	if !found || lvgCh == nil {
		err := fmt.Errorf("cache was not found for the LVMVolumeGroup %s", lvgName)
		c.log.Error(err, fmt.Sprintf("[GetAllPVCFromLVGThinPool] an error occurred while trying to get all PVC for the LVMVolumeGroup %s", lvgName))
		return nil, err
	}

	thinPoolCh, found := lvgCh.thinByPool[thinPoolName]
	if !found || thinPoolCh == nil {
		c.log.Debug(fmt.Sprintf("[GetAllPVCFromLVGThinPool] no Thin pool %s in the LVMVolumeGroup %s was found. Returns nil slice", thinPoolName, lvgName))
		return nil, nil
	}

	result := make([]*corev1.PersistentVolumeClaim, 0, pvcPerLVGCount)
	for _, pe := range thinPoolCh.pvcs {
		result = append(result, pe.pvc)
	}

	return result, nil
}

// GetLVGNamesForPVC returns a slice of LVMVolumeGroup resources names, where selected PVC has been stored in. If no such LVMVolumeGroup found, returns empty slice.
func (c *Cache) GetLVGNamesForPVC(pvc *corev1.PersistentVolumeClaim) []string {
	pvcKey := configurePVCKey(pvc)
	c.mtx.RLock()
	defer c.mtx.RUnlock()
	var result []string
	for lvgName, entry := range c.lvgByName {
		if _, ok := entry.thickByPVC[pvcKey]; ok {
			result = append(result, lvgName)
			continue
		}
		for _, tp := range entry.thinByPool {
			if _, ok := tp.pvcs[pvcKey]; ok {
				result = append(result, lvgName)
				break
			}
		}
	}
	if len(result) == 0 {
		c.log.Warning(fmt.Sprintf("[GetLVGNamesForPVC] no cached LVMVolumeGroups were found for PVC %s", pvcKey))
		return nil
	}
	return result
}

// CheckIsPVCStored checks if selected PVC has been already stored in the cache.
func (c *Cache) CheckIsPVCStored(pvc *corev1.PersistentVolumeClaim) bool {
	pvcKey := configurePVCKey(pvc)
	c.mtx.RLock()
	defer c.mtx.RUnlock()
	for _, entry := range c.lvgByName {
		if _, ok := entry.thickByPVC[pvcKey]; ok {
			return true
		}
		for _, tp := range entry.thinByPool {
			if _, ok := tp.pvcs[pvcKey]; ok {
				return true
			}
		}
	}
	return false
}

// RemoveSpaceReservationForPVCWithSelectedNode removes space reservation for selected PVC for every LVMVolumeGroup resource, which is not bound to the PVC selected node.
func (c *Cache) RemoveSpaceReservationForPVCWithSelectedNode(pvc *corev1.PersistentVolumeClaim, deviceType string) error {
	pvcKey := configurePVCKey(pvc)
	// the LVG which is used to store PVC
	selectedLVGName := ""

	c.mtx.Lock()
	defer c.mtx.Unlock()

	// Build list of LVG names for this PVC on the fly
	lvgNamesForPVC := make([]string, 0, lvgsPerPVCCount)
	for lvgName, entry := range c.lvgByName {
		switch deviceType {
		case consts.Thin:
			for _, tp := range entry.thinByPool {
				if _, ok := tp.pvcs[pvcKey]; ok {
					lvgNamesForPVC = append(lvgNamesForPVC, lvgName)
					break
				}
			}
		case consts.Thick:
			if _, ok := entry.thickByPVC[pvcKey]; ok {
				lvgNamesForPVC = append(lvgNamesForPVC, lvgName)
			}
		}
	}
	if len(lvgNamesForPVC) == 0 {
		c.log.Debug(fmt.Sprintf("[RemoveSpaceReservationForPVCWithSelectedNode] cache for PVC %s has been already removed", pvcKey))
		return nil
	}

	for _, lvgName := range lvgNamesForPVC {
		entry, found := c.lvgByName[lvgName]
		if !found || entry == nil {
			err := fmt.Errorf("no cache found for the LVMVolumeGroup %s", lvgName)
			c.log.Error(err, fmt.Sprintf("[RemoveSpaceReservationForPVCWithSelectedNode] an error occurred while trying to remove space reservation for PVC %s", pvcKey))
			return err
		}

		switch deviceType {
		case consts.Thin:
			for thinPoolName, thinPoolCh := range entry.thinByPool {
				pvcCh, found := thinPoolCh.pvcs[pvcKey]
				if !found {
					c.log.Debug(fmt.Sprintf("[RemoveSpaceReservationForPVCWithSelectedNode] PVC %s space reservation in the LVMVolumeGroup %s has been already removed", pvcKey, lvgName))
					continue
				}

				selectedNode := pvcCh.selectedNode
				if selectedNode == "" {
					delete(thinPoolCh.pvcs, pvcKey)
					c.log.Debug(fmt.Sprintf("[RemoveSpaceReservationForPVCWithSelectedNode] removed space reservation for PVC %s in the Thin pool %s of the LVMVolumeGroup %s due the PVC got selected to the node %s", pvcKey, thinPoolName, lvgName, pvc.Annotations[SelectedNodeAnnotation]))
				} else {
					selectedLVGName = lvgName
					c.log.Debug(fmt.Sprintf("[RemoveSpaceReservationForPVCWithSelectedNode] PVC %s got selected to the node %s. It should not be revomed from the LVMVolumeGroup %s", pvcKey, pvc.Annotations[SelectedNodeAnnotation], lvgName))
				}
			}
		case consts.Thick:
			pvcCh, found := entry.thickByPVC[pvcKey]
			if !found {
				c.log.Debug(fmt.Sprintf("[RemoveSpaceReservationForPVCWithSelectedNode] PVC %s space reservation in the LVMVolumeGroup %s has been already removed", pvcKey, lvgName))
				continue
			}

			selectedNode := pvcCh.selectedNode
			if selectedNode == "" {
				delete(entry.thickByPVC, pvcKey)
				c.log.Debug(fmt.Sprintf("[RemoveSpaceReservationForPVCWithSelectedNode] removed space reservation for PVC %s in the LVMVolumeGroup %s due the PVC got selected to the node %s", pvcKey, lvgName, pvc.Annotations[SelectedNodeAnnotation]))
			} else {
				selectedLVGName = lvgName
				c.log.Debug(fmt.Sprintf("[RemoveSpaceReservationForPVCWithSelectedNode] PVC %s got selected to the node %s. It should not be revomed from the LVMVolumeGroup %s", pvcKey, pvc.Annotations[SelectedNodeAnnotation], lvgName))
			}
		}
	}
	c.log.Debug(fmt.Sprintf("[RemoveSpaceReservationForPVCWithSelectedNode] PVC %s space reservation has been removed from LVMVolumeGroup cache", pvcKey))

	c.log.Debug(fmt.Sprintf("[RemoveSpaceReservationForPVCWithSelectedNode] cache for PVC %s will be wiped from unused LVMVolumeGroups", pvcKey))
	cleared := make([]string, 0, len(lvgNamesForPVC))
	for _, lvgName := range lvgNamesForPVC {
		if lvgName == selectedLVGName {
			c.log.Debug(fmt.Sprintf("[RemoveSpaceReservationForPVCWithSelectedNode] the LVMVolumeGroup %s will be saved for PVC %s cache as used", lvgName, pvcKey))
			cleared = append(cleared, lvgName)
		} else {
			c.log.Debug(fmt.Sprintf("[RemoveSpaceReservationForPVCWithSelectedNode] the LVMVolumeGroup %s will be removed from PVC %s cache as not used", lvgName, pvcKey))
		}
	}
	c.log.Trace(fmt.Sprintf("[RemoveSpaceReservationForPVCWithSelectedNode] cleared LVMVolumeGroups for PVC %s: %v", pvcKey, cleared))

	return nil
}

// RemovePVCFromTheCache completely removes selected PVC in the cache.
func (c *Cache) RemovePVCFromTheCache(pvc *corev1.PersistentVolumeClaim) {
	pvcKey := configurePVCKey(pvc)

	c.log.Debug(fmt.Sprintf("[RemovePVCFromTheCache] run full cache wipe for PVC %s", pvcKey))
	c.mtx.Lock()
	defer c.mtx.Unlock()
	for _, entry := range c.lvgByName {
		delete(entry.thickByPVC, pvcKey)
		for _, tp := range entry.thinByPool {
			delete(tp.pvcs, pvcKey)
		}
	}
}

// FindLVGForPVCBySelectedNode finds a suitable LVMVolumeGroup resource's name for selected PVC based on selected node. If no such LVMVolumeGroup found, returns empty string.
func (c *Cache) FindLVGForPVCBySelectedNode(pvc *corev1.PersistentVolumeClaim, nodeName string) string {
	pvcKey := configurePVCKey(pvc)

	c.mtx.RLock()
	defer c.mtx.RUnlock()

	// Build a list of LVGs that contain the PVC
	var lvgsForPVC []string
	for lvgName, entry := range c.lvgByName {
		if _, ok := entry.thickByPVC[pvcKey]; ok {
			lvgsForPVC = append(lvgsForPVC, lvgName)
			continue
		}
		for _, tp := range entry.thinByPool {
			if _, ok := tp.pvcs[pvcKey]; ok {
				lvgsForPVC = append(lvgsForPVC, lvgName)
				break
			}
		}
	}
	if len(lvgsForPVC) == 0 {
		c.log.Debug(fmt.Sprintf("[FindLVGForPVCBySelectedNode] no LVMVolumeGroups were found in the cache for PVC %s. Returns empty string", pvcKey))
		return ""
	}

	// Build a set of LVGs that belong to node
	nodeLVGs := make(map[string]struct{}, lvgsPerNodeCount)
	for lvgName, entry := range c.lvgByName {
		if lvgHasNode(entry.lvg, nodeName) {
			nodeLVGs[lvgName] = struct{}{}
		}
	}
	if len(nodeLVGs) == 0 {
		c.log.Debug(fmt.Sprintf("[FindLVGForPVCBySelectedNode] no LVMVolumeGroups were found in the cache for the node %s. Returns empty string", nodeName))
		return ""
	}

	var targetLVG string
	for _, lvgName := range lvgsForPVC {
		if _, ok := nodeLVGs[lvgName]; ok {
			targetLVG = lvgName
			break
		}
	}

	if targetLVG == "" {
		c.log.Debug(fmt.Sprintf("[FindLVGForPVCBySelectedNode] no LVMVolumeGroup was found for PVC %s. Returns empty string", pvcKey))
	}

	return targetLVG
}

// PrintTheCacheLog prints the logs with cache state.
func (c *Cache) PrintTheCacheLog() {
	c.log.Cache("*******************CACHE BEGIN*******************")
	c.log.Cache("[LVMVolumeGroups BEGIN]")
	c.mtx.RLock()
	for lvgName, lvgCh := range c.lvgByName {
		c.log.Cache(fmt.Sprintf("[%s]", lvgName))

		for pvcName, pvcCh := range lvgCh.thickByPVC {
			c.log.Cache(fmt.Sprintf("      THICK PVC %s, selected node: %s", pvcName, pvcCh.selectedNode))
		}

		for thinPoolName, thinPoolCh := range lvgCh.thinByPool {
			for pvcName, pvcCh := range thinPoolCh.pvcs {
				c.log.Cache(fmt.Sprintf("      THIN POOL %s PVC %s, selected node: %s", thinPoolName, pvcName, pvcCh.selectedNode))
			}
		}
	}
	c.mtx.RUnlock()
	c.log.Cache("[LVMVolumeGroups ENDS]")

	c.log.Cache("[PVC and LVG BEGINS]")
	// Build pvc -> lvgs mapping for printing purposes
	c.mtx.RLock()
	pvcToLvgs := make(map[string][]string)
	for lvgName, entry := range c.lvgByName {
		for pvcKey := range entry.thickByPVC {
			pvcToLvgs[pvcKey] = append(pvcToLvgs[pvcKey], lvgName)
		}
		for _, tp := range entry.thinByPool {
			for pvcKey := range tp.pvcs {
				// deduplicate on append
				if !containsString(pvcToLvgs[pvcKey], lvgName) {
					pvcToLvgs[pvcKey] = append(pvcToLvgs[pvcKey], lvgName)
				}
			}
		}
	}
	for pvcName, lvgs := range pvcToLvgs {
		c.log.Cache(fmt.Sprintf("[PVC: %s]", pvcName))
		for _, lvgName := range lvgs {
			c.log.Cache(fmt.Sprintf("      LVMVolumeGroup: %s", lvgName))
		}
	}
	c.mtx.RUnlock()
	c.log.Cache("[PVC and LVG ENDS]")

	c.log.Cache("[Node and LVG BEGINS]")
	// Build node -> lvgs mapping
	c.mtx.RLock()
	nodeToLvgs := make(map[string][]string)
	for lvgName, entry := range c.lvgByName {
		for _, n := range entry.lvg.Status.Nodes {
			if !containsString(nodeToLvgs[n.Name], lvgName) {
				nodeToLvgs[n.Name] = append(nodeToLvgs[n.Name], lvgName)
			}
		}
	}
	for nodeName, lvgs := range nodeToLvgs {
		c.log.Cache(fmt.Sprintf("[Node: %s]", nodeName))
		for _, lvgName := range lvgs {
			c.log.Cache(fmt.Sprintf("      LVMVolumeGroup name: %s", lvgName))
		}
	}
	c.mtx.RUnlock()
	c.log.Cache("[Node and LVG ENDS]")
	c.log.Cache("*******************CACHE END*******************")
}

func configurePVCKey(pvc *corev1.PersistentVolumeClaim) string {
	return fmt.Sprintf("%s/%s", pvc.Namespace, pvc.Name)
}

func lvgHasNode(lvg *snc.LVMVolumeGroup, nodeName string) bool {
	for _, n := range lvg.Status.Nodes {
		if n.Name == nodeName {
			return true
		}
	}
	return false
}

func lvgHasThinPool(lvg *snc.LVMVolumeGroup, thinPoolName string) bool {
	for _, tp := range lvg.Status.ThinPools {
		if tp.Name == thinPoolName {
			return true
		}
	}
	return false
}

func containsString(slice []string, val string) bool {
	for _, s := range slice {
		if strings.EqualFold(s, val) {
			return true
		}
	}
	return false
}
