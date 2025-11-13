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
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	slices2 "k8s.io/utils/strings/slices"

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
	lvgs            sync.Map // map[string]*lvgCache
	pvcLVGs         sync.Map // map[string][]string
	nodeLVGs        sync.Map // map[string][]string
	log             logger.Logger
	expiredDuration time.Duration
}

type lvgCache struct {
	lvg       *snc.LVMVolumeGroup
	thickPVCs sync.Map // map[string]*pvcCache
	thinPools sync.Map // map[string]*thinPoolCache
}

type thinPoolCache struct {
	pvcs sync.Map // map[string]*pvcCache
}

type pvcCache struct {
	pvc          *v1.PersistentVolumeClaim
	selectedNode string
}

// NewCache initialize new cache.
func NewCache(logger logger.Logger, pvcExpDurSec int) *Cache {
	ch := &Cache{
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
	c.lvgs.Range(func(lvgName, _ any) bool {
		pvcs, err := c.GetAllPVCForLVG(lvgName.(string))
		if err != nil {
			c.log.Error(err, fmt.Sprintf("[clearBoundExpiredPVC] unable to get PVCs for the LVMVolumeGroup %s", lvgName.(string)))
			return false
		}

		for _, pvc := range pvcs {
			if pvc.Status.Phase != v1.ClaimBound {
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

		return true
	})
	c.log.Debug("[clearBoundExpiredPVC] finished the expired PVC clearing")
}

// AddLVG adds selected LVMVolumeGroup resource to the cache. If it is already stored, does nothing.
func (c *Cache) AddLVG(lvg *snc.LVMVolumeGroup) {
	_, loaded := c.lvgs.LoadOrStore(lvg.Name, &lvgCache{
		lvg: lvg,
	})
	if loaded {
		c.log.Debug(fmt.Sprintf("[AddLVG] the LVMVolumeGroup %s has been already added to the cache", lvg.Name))
		return
	}

	c.log.Trace(fmt.Sprintf("[AddLVG] the LVMVolumeGroup %s nodes: %v", lvg.Name, lvg.Status.Nodes))
	for _, node := range lvg.Status.Nodes {
		lvgsOnTheNode, _ := c.nodeLVGs.Load(node.Name)
		if lvgsOnTheNode == nil {
			lvgsOnTheNode = make([]string, 0, lvgsPerNodeCount)
		}

		lvgsOnTheNode = append(lvgsOnTheNode.([]string), lvg.Name)
		c.log.Debug(fmt.Sprintf("[AddLVG] the LVMVolumeGroup %s has been added to the node %s", lvg.Name, node.Name))
		c.nodeLVGs.Store(node.Name, lvgsOnTheNode)
	}
}

// UpdateLVG updated selected LVMVolumeGroup resource in the cache. If such LVMVolumeGroup is not stored, returns an error.
func (c *Cache) UpdateLVG(lvg *snc.LVMVolumeGroup) error {
	lvgCh, found := c.lvgs.Load(lvg.Name)
	if !found {
		return fmt.Errorf("the LVMVolumeGroup %s was not found in the lvgCh", lvg.Name)
	}

	lvgCh.(*lvgCache).lvg = lvg

	c.log.Trace(fmt.Sprintf("[UpdateLVG] the LVMVolumeGroup %s nodes: %v", lvg.Name, lvg.Status.Nodes))
	for _, node := range lvg.Status.Nodes {
		lvgsOnTheNode, _ := c.nodeLVGs.Load(node.Name)
		if lvgsOnTheNode == nil {
			lvgsOnTheNode = make([]string, 0, lvgsPerNodeCount)
		}

		if !slices2.Contains(lvgsOnTheNode.([]string), lvg.Name) {
			lvgsOnTheNode = append(lvgsOnTheNode.([]string), lvg.Name)
			c.log.Debug(fmt.Sprintf("[UpdateLVG] the LVMVolumeGroup %s has been added to the node %s", lvg.Name, node.Name))
			c.nodeLVGs.Store(node.Name, lvgsOnTheNode)
		} else {
			c.log.Debug(fmt.Sprintf("[UpdateLVG] the LVMVolumeGroup %s has been already added to the node %s", lvg.Name, node.Name))
		}
	}

	return nil
}

// TryGetLVG returns selected LVMVolumeGroup resource if it is stored in the cache, otherwise returns nil.
func (c *Cache) TryGetLVG(name string) *snc.LVMVolumeGroup {
	lvgCh, found := c.lvgs.Load(name)
	if !found {
		c.log.Debug(fmt.Sprintf("[TryGetLVG] the LVMVolumeGroup %s was not found in the cache. Return nil", name))
		return nil
	}

	return lvgCh.(*lvgCache).lvg
}

// GetLVGNamesByNodeName returns LVMVolumeGroups resources names stored in the cache for the selected node. If none of them exist, returns empty slice.
func (c *Cache) GetLVGNamesByNodeName(nodeName string) []string {
	lvgs, found := c.nodeLVGs.Load(nodeName)
	if !found {
		c.log.Debug(fmt.Sprintf("[GetLVGNamesByNodeName] no LVMVolumeGroup was found in the cache for the node %s. Return empty slice", nodeName))
		return []string{}
	}

	return lvgs.([]string)
}

// GetAllLVG returns all the LVMVolumeGroups resources stored in the cache.
func (c *Cache) GetAllLVG() map[string]*snc.LVMVolumeGroup {
	lvgs := make(map[string]*snc.LVMVolumeGroup)
	c.lvgs.Range(func(lvgName, lvgCh any) bool {
		if lvgCh.(*lvgCache).lvg == nil {
			c.log.Error(fmt.Errorf("LVMVolumeGroup %s is not initialized", lvgName), "[GetAllLVG] an error occurs while iterating the LVMVolumeGroups")
			return true
		}

		lvgs[lvgName.(string)] = lvgCh.(*lvgCache).lvg
		return true
	})

	return lvgs
}

// GetLVGThickReservedSpace returns a sum of reserved space by every thick PVC in the selected LVMVolumeGroup resource. If such LVMVolumeGroup resource is not stored, returns an error.
func (c *Cache) GetLVGThickReservedSpace(lvgName string) (int64, error) {
	lvg, found := c.lvgs.Load(lvgName)
	if !found {
		c.log.Debug(fmt.Sprintf("[GetLVGThickReservedSpace] the LVMVolumeGroup %s was not found in the cache. Returns 0", lvgName))
		return 0, nil
	}

	var space int64
	lvg.(*lvgCache).thickPVCs.Range(func(_, pvcCh any) bool {
		space += pvcCh.(*pvcCache).pvc.Spec.Resources.Requests.Storage().Value()
		return true
	})

	return space, nil
}

// GetLVGThinReservedSpace returns a sum of reserved space by every thin PVC in the selected LVMVolumeGroup resource. If such LVMVolumeGroup resource is not stored, returns an error.
func (c *Cache) GetLVGThinReservedSpace(lvgName string, thinPoolName string) (int64, error) {
	lvgCh, found := c.lvgs.Load(lvgName)
	if !found {
		c.log.Debug(fmt.Sprintf("[GetLVGThinReservedSpace] the LVMVolumeGroup %s was not found in the cache. Returns 0", lvgName))
		return 0, nil
	}

	thinPool, found := lvgCh.(*lvgCache).thinPools.Load(thinPoolName)
	if !found {
		c.log.Debug(fmt.Sprintf("[GetLVGThinReservedSpace] the Thin pool %s of the LVMVolumeGroup %s was not found in the cache. Returns 0", lvgName, thinPoolName))
		return 0, nil
	}

	var space int64
	thinPool.(*thinPoolCache).pvcs.Range(func(_, pvcCh any) bool {
		space += pvcCh.(*pvcCache).pvc.Spec.Resources.Requests.Storage().Value()
		return true
	})

	return space, nil
}

// DeleteLVG deletes selected LVMVolumeGroup resource from the cache.
func (c *Cache) DeleteLVG(lvgName string) {
	c.lvgs.Delete(lvgName)

	c.nodeLVGs.Range(func(_, lvgNames any) bool {
		for i, lvg := range lvgNames.([]string) {
			if lvg == lvgName {
				//nolint:gocritic,ineffassign
				lvgNames = append(lvgNames.([]string)[:i], lvgNames.([]string)[i+1:]...)
				return false
			}
		}

		return true
	})

	c.pvcLVGs.Range(func(_, lvgNames any) bool {
		for i, lvg := range lvgNames.([]string) {
			if lvg == lvgName {
				//nolint:gocritic,ineffassign
				lvgNames = append(lvgNames.([]string)[:i], lvgNames.([]string)[i+1:]...)
				return false
			}
		}

		return true
	})
}

// AddThickPVC adds selected PVC to selected LVMVolumeGroup resource. If the LVMVolumeGroup resource is not stored, returns an error.
// If selected PVC is already stored in the cache, does nothing.
func (c *Cache) AddThickPVC(lvgName string, pvc *v1.PersistentVolumeClaim) error {
	if pvc.Status.Phase == v1.ClaimBound {
		c.log.Warning(fmt.Sprintf("[AddThickPVC] PVC %s/%s has status phase BOUND. It will not be added to the cache", pvc.Namespace, pvc.Name))
		return nil
	}

	pvcKey := configurePVCKey(pvc)

	lvgCh, found := c.lvgs.Load(lvgName)
	if !found {
		err := fmt.Errorf("the LVMVolumeGroup %s was not found in the cache", lvgName)
		c.log.Error(err, fmt.Sprintf("[AddThickPVC] an error occurred while trying to add PVC %s to the cache", pvcKey))
		return err
	}

	// this case might be triggered if the extender recovers after fail and finds some pending thickPVCs with selected nodes
	c.log.Trace(fmt.Sprintf("[AddThickPVC] PVC %s/%s annotations: %v", pvc.Namespace, pvc.Name, pvc.Annotations))

	shouldAdd, err := c.shouldAddPVC(pvc, lvgCh.(*lvgCache), pvcKey, lvgName, "")
	if err != nil {
		return err
	}

	if !shouldAdd {
		c.log.Debug(fmt.Sprintf("[AddThickPVC] PVC %s should not be added", pvcKey))
		return nil
	}

	c.log.Debug(fmt.Sprintf("[AddThickPVC] new PVC %s cache will be added to the LVMVolumeGroup %s", pvcKey, lvgName))
	c.addNewThickPVC(lvgCh.(*lvgCache), pvc)

	return nil
}

func (c *Cache) shouldAddPVC(pvc *v1.PersistentVolumeClaim, lvgCh *lvgCache, pvcKey, lvgName, thinPoolName string) (bool, error) {
	if pvc.Annotations[SelectedNodeAnnotation] != "" {
		c.log.Debug(fmt.Sprintf("[shouldAddPVC] PVC %s/%s has selected node anotation, selected node: %s", pvc.Namespace, pvc.Name, pvc.Annotations[SelectedNodeAnnotation]))

		lvgsOnTheNode, found := c.nodeLVGs.Load(pvc.Annotations[SelectedNodeAnnotation])
		if !found {
			err := fmt.Errorf("no LVMVolumeGroups found for the node %s", pvc.Annotations[SelectedNodeAnnotation])
			c.log.Error(err, fmt.Sprintf("[shouldAddPVC] an error occurred while trying to add PVC %s to the cache", pvcKey))
			return false, err
		}

		if !slices2.Contains(lvgsOnTheNode.([]string), lvgName) {
			c.log.Debug(fmt.Sprintf("[shouldAddPVC] LVMVolumeGroup %s does not belong to PVC %s/%s selected node %s. It will be skipped", lvgName, pvc.Namespace, pvc.Name, pvc.Annotations[SelectedNodeAnnotation]))
			return false, nil
		}

		c.log.Debug(fmt.Sprintf("[shouldAddPVC] LVMVolumeGroup %s belongs to PVC %s/%s selected node %s", lvgName, pvc.Namespace, pvc.Name, pvc.Annotations[SelectedNodeAnnotation]))

		// if pvc is thick
		_, found = lvgCh.thickPVCs.Load(pvcKey)
		if found {
			c.log.Debug(fmt.Sprintf("[shouldAddPVC] PVC %s was found in the cache of the LVMVolumeGroup %s", pvcKey, lvgName))
			return false, nil
		}

		// if pvc is thin
		if thinPoolName != "" {
			thinPoolCh, found := lvgCh.thinPools.Load(thinPoolName)
			if !found {
				c.log.Debug(fmt.Sprintf("[shouldAddPVC] Thin pool %s was not found in the cache, PVC %s should be added", thinPoolName, pvcKey))
				return true, nil
			}

			if _, found = thinPoolCh.(*thinPoolCache).pvcs.Load(pvcKey); found {
				c.log.Debug(fmt.Sprintf("[shouldAddPVC] PVC %s was found in the Thin pool %s cache of the LVMVolumeGroup %s. No need to add", pvcKey, thinPoolName, lvgName))
				return false, nil
			}
		}
	}

	return true, nil
}

func (c *Cache) AddThinPVC(lvgName, thinPoolName string, pvc *v1.PersistentVolumeClaim) error {
	if pvc.Status.Phase == v1.ClaimBound {
		c.log.Warning(fmt.Sprintf("[AddThinPVC] PVC %s/%s has status phase BOUND. It will not be added to the cache", pvc.Namespace, pvc.Name))
		return nil
	}

	pvcKey := configurePVCKey(pvc)

	lvgCh, found := c.lvgs.Load(lvgName)
	if !found {
		err := fmt.Errorf("the LVMVolumeGroup %s was not found in the cache", lvgName)
		c.log.Error(err, fmt.Sprintf("[AddThinPVC] an error occurred while trying to add PVC %s to the cache", pvcKey))
		return err
	}

	thinPoolBelongs := c.checkIfThinPoolBelongsToLVG(lvgCh.(*lvgCache), thinPoolName)
	if !thinPoolBelongs {
		err := fmt.Errorf("thin pool %s was not found in the LVMVolumeGroup %s", thinPoolName, lvgName)
		c.log.Error(err, fmt.Sprintf("[AddThinPVC] unable to add Thin pool %s of the LVMVolumeGroup %s for the PVC %s", thinPoolName, lvgName, pvcKey))
		return err
	}

	// this case might be triggered if the extender recovers after fail and finds some pending thin PVCs with selected nodes
	c.log.Trace(fmt.Sprintf("[AddThinPVC] PVC %s/%s annotations: %v", pvc.Namespace, pvc.Name, pvc.Annotations))
	shouldAdd, err := c.shouldAddPVC(pvc, lvgCh.(*lvgCache), pvcKey, lvgName, thinPoolName)
	if err != nil {
		return err
	}

	if !shouldAdd {
		c.log.Debug(fmt.Sprintf("[AddThinPVC] PVC %s should not be added", pvcKey))
		return nil
	}

	c.log.Debug(fmt.Sprintf("[AddThinPVC] new PVC %s cache will be added to the LVMVolumeGroup %s", pvcKey, lvgName))
	err = c.addNewThinPVC(lvgCh.(*lvgCache), pvc, thinPoolName)
	if err != nil {
		c.log.Error(err, fmt.Sprintf("[AddThinPVC] unable to add PVC %s to Thin Pool %s of the LVMVolumeGroup %s", pvcKey, thinPoolName, lvgName))
		return err
	}

	return nil
}

func (c *Cache) checkIfThinPoolBelongsToLVG(lvgCh *lvgCache, thinPoolName string) bool {
	for _, tp := range lvgCh.lvg.Status.ThinPools {
		if tp.Name == thinPoolName {
			return true
		}
	}

	return false
}

func (c *Cache) addNewThickPVC(lvgCh *lvgCache, pvc *v1.PersistentVolumeClaim) {
	pvcKey := configurePVCKey(pvc)
	lvgCh.thickPVCs.Store(pvcKey, &pvcCache{pvc: pvc, selectedNode: pvc.Annotations[SelectedNodeAnnotation]})

	c.addLVGToPVC(lvgCh.lvg.Name, pvcKey)
}

func (c *Cache) addNewThinPVC(lvgCh *lvgCache, pvc *v1.PersistentVolumeClaim, thinPoolName string) error {
	pvcKey := configurePVCKey(pvc)

	err := c.addThinPoolIfNotExists(lvgCh, thinPoolName)
	if err != nil {
		c.log.Error(err, fmt.Sprintf("[addNewThinPVC] unable to add Thin pool %s in the LVMVolumeGroup %s cache for PVC %s", thinPoolName, lvgCh.lvg.Name, pvc.Name))
		return err
	}

	thinPoolCh, found := lvgCh.thinPools.Load(thinPoolName)
	if !found {
		err = fmt.Errorf("thin pool %s not found", thinPoolName)
		c.log.Error(err, fmt.Sprintf("[addNewThinPVC] unable to add Thin PVC %s to the cache", pvcKey))
		return err
	}

	thinPoolCh.(*thinPoolCache).pvcs.Store(pvcKey, &pvcCache{pvc: pvc, selectedNode: pvc.Annotations[SelectedNodeAnnotation]})
	c.log.Debug(fmt.Sprintf("[addNewThinPVC] THIN PVC %s was added to the cache to Thin Pool %s", pvcKey, thinPoolName))

	c.addLVGToPVC(lvgCh.lvg.Name, pvcKey)
	return nil
}

func (c *Cache) addLVGToPVC(lvgName, pvcKey string) {
	lvgsForPVC, found := c.pvcLVGs.Load(pvcKey)
	if !found || lvgsForPVC == nil {
		lvgsForPVC = make([]string, 0, lvgsPerPVCCount)
	}

	c.log.Trace(fmt.Sprintf("[addLVGToPVC] LVMVolumeGroups from the cache for PVC %s before append: %v", pvcKey, lvgsForPVC))
	lvgsForPVC = append(lvgsForPVC.([]string), lvgName)
	c.log.Trace(fmt.Sprintf("[addLVGToPVC] LVMVolumeGroups from the cache for PVC %s after append: %v", pvcKey, lvgsForPVC))
	c.pvcLVGs.Store(pvcKey, lvgsForPVC)
}

// UpdateThickPVC updates selected PVC in selected LVMVolumeGroup resource. If no such PVC is stored in the cache, adds it.
func (c *Cache) UpdateThickPVC(lvgName string, pvc *v1.PersistentVolumeClaim) error {
	pvcKey := configurePVCKey(pvc)

	lvgCh, found := c.lvgs.Load(lvgName)
	if !found {
		return fmt.Errorf("the LVMVolumeGroup %s was not found in the cache", lvgName)
	}

	pvcCh, found := lvgCh.(*lvgCache).thickPVCs.Load(pvcKey)
	if !found {
		c.log.Warning(fmt.Sprintf("[UpdateThickPVC] PVC %s was not found in the cache for the LVMVolumeGroup %s. It will be added", pvcKey, lvgName))
		err := c.AddThickPVC(lvgName, pvc)
		if err != nil {
			c.log.Error(err, fmt.Sprintf("[UpdateThickPVC] an error occurred while trying to update the PVC %s", pvcKey))
			return err
		}
		return nil
	}

	pvcCh.(*pvcCache).pvc = pvc
	pvcCh.(*pvcCache).selectedNode = pvc.Annotations[SelectedNodeAnnotation]
	c.log.Debug(fmt.Sprintf("[UpdateThickPVC] successfully updated PVC %s with selected node %s in the cache for LVMVolumeGroup %s", pvcKey, pvc.Annotations[SelectedNodeAnnotation], lvgName))

	return nil
}

func (c *Cache) UpdateThinPVC(lvgName, thinPoolName string, pvc *v1.PersistentVolumeClaim) error {
	pvcKey := configurePVCKey(pvc)

	lvgCh, found := c.lvgs.Load(lvgName)
	if !found {
		return fmt.Errorf("the LVMVolumeGroup %s was not found in the cache", lvgName)
	}

	thinPoolCh, found := lvgCh.(*lvgCache).thinPools.Load(thinPoolName)
	if !found {
		c.log.Debug(fmt.Sprintf("[UpdateThinPVC] Thin Pool %s was not found in the LVMVolumeGroup %s, add it.", thinPoolName, lvgName))
		err := c.addThinPoolIfNotExists(lvgCh.(*lvgCache), thinPoolName)
		if err != nil {
			return err
		}
		thinPoolCh, _ = lvgCh.(*lvgCache).thinPools.Load(thinPoolName)
	}

	pvcCh, found := thinPoolCh.(*thinPoolCache).pvcs.Load(pvcKey)
	if !found {
		c.log.Warning(fmt.Sprintf("[UpdateThinPVC] Thin PVC %s was not found in Thin pool %s in the cache for the LVMVolumeGroup %s. It will be added", pvcKey, thinPoolName, lvgName))
		err := c.addNewThinPVC(lvgCh.(*lvgCache), pvc, thinPoolName)
		if err != nil {
			c.log.Error(err, fmt.Sprintf("[UpdateThinPVC] an error occurred while trying to update the PVC %s", pvcKey))
			return err
		}
		return nil
	}

	pvcCh.(*pvcCache).pvc = pvc
	pvcCh.(*pvcCache).selectedNode = pvc.Annotations[SelectedNodeAnnotation]
	c.log.Debug(fmt.Sprintf("[UpdateThinPVC] successfully updated THIN PVC %s with selected node %s in the cache for LVMVolumeGroup %s", pvcKey, pvc.Annotations[SelectedNodeAnnotation], lvgName))

	return nil
}

func (c *Cache) addThinPoolIfNotExists(lvgCh *lvgCache, thinPoolName string) error {
	if len(thinPoolName) == 0 {
		err := errors.New("no thin pool name specified")
		c.log.Error(err, fmt.Sprintf("[addThinPoolIfNotExists] unable to add thin pool in the LVMVolumeGroup %s", lvgCh.lvg.Name))
		return err
	}

	_, found := lvgCh.thinPools.Load(thinPoolName)
	if found {
		c.log.Debug(fmt.Sprintf("[addThinPoolIfNotExists] Thin pool %s is already created in the LVMVolumeGroup %s. No need to add a new one", thinPoolName, lvgCh.lvg.Name))
		return nil
	}

	lvgCh.thinPools.Store(thinPoolName, &thinPoolCache{})
	return nil
}

// GetAllPVCForLVG returns slice of PVC belonging to selected LVMVolumeGroup resource. If such LVMVolumeGroup is not stored in the cache, returns an error.
func (c *Cache) GetAllPVCForLVG(lvgName string) ([]*v1.PersistentVolumeClaim, error) {
	lvgCh, found := c.lvgs.Load(lvgName)
	if !found {
		err := fmt.Errorf("cache was not found for the LVMVolumeGroup %s", lvgName)
		c.log.Error(err, fmt.Sprintf("[GetAllPVCForLVG] an error occurred while trying to get all PVC for the LVMVolumeGroup %s", lvgName))
		return nil, err
	}

	// TODO: fix this to struct size field after refactoring
	size := 0
	lvgCh.(*lvgCache).thickPVCs.Range(func(_, _ any) bool {
		size++
		return true
	})
	lvgCh.(*lvgCache).thinPools.Range(func(_, tpCh any) bool {
		tpCh.(*thinPoolCache).pvcs.Range(func(_, _ any) bool {
			size++
			return true
		})
		return true
	})

	result := make([]*v1.PersistentVolumeClaim, 0, size)
	// collect Thick PVC for the LVG
	lvgCh.(*lvgCache).thickPVCs.Range(func(_, pvcCh any) bool {
		result = append(result, pvcCh.(*pvcCache).pvc)
		return true
	})

	// collect Thin PVC for the LVG
	lvgCh.(*lvgCache).thinPools.Range(func(_, tpCh any) bool {
		tpCh.(*thinPoolCache).pvcs.Range(func(_, pvcCh any) bool {
			result = append(result, pvcCh.(*pvcCache).pvc)
			return true
		})
		return true
	})

	return result, nil
}

// GetAllThickPVCLVG returns slice of PVC belonging to selected LVMVolumeGroup resource. If such LVMVolumeGroup is not stored in the cache, returns an error.
func (c *Cache) GetAllThickPVCLVG(lvgName string) ([]*v1.PersistentVolumeClaim, error) {
	lvgCh, found := c.lvgs.Load(lvgName)
	if !found {
		err := fmt.Errorf("cache was not found for the LVMVolumeGroup %s", lvgName)
		c.log.Error(err, fmt.Sprintf("[GetAllPVCForLVG] an error occurred while trying to get all PVC for the LVMVolumeGroup %s", lvgName))
		return nil, err
	}

	result := make([]*v1.PersistentVolumeClaim, 0, pvcPerLVGCount)
	// collect Thick PVC for the LVG
	lvgCh.(*lvgCache).thickPVCs.Range(func(_, pvcCh any) bool {
		result = append(result, pvcCh.(*pvcCache).pvc)
		return true
	})

	return result, nil
}

// GetAllPVCFromLVGThinPool returns slice of PVC belonging to selected LVMVolumeGroup resource. If such LVMVolumeGroup is not stored in the cache, returns an error.
func (c *Cache) GetAllPVCFromLVGThinPool(lvgName, thinPoolName string) ([]*v1.PersistentVolumeClaim, error) {
	lvgCh, found := c.lvgs.Load(lvgName)
	if !found {
		err := fmt.Errorf("cache was not found for the LVMVolumeGroup %s", lvgName)
		c.log.Error(err, fmt.Sprintf("[GetAllPVCFromLVGThinPool] an error occurred while trying to get all PVC for the LVMVolumeGroup %s", lvgName))
		return nil, err
	}

	thinPoolCh, found := lvgCh.(*lvgCache).thinPools.Load(thinPoolName)
	if !found || thinPoolCh == nil {
		c.log.Debug(fmt.Sprintf("[GetAllPVCFromLVGThinPool] no Thin pool %s in the LVMVolumeGroup %s was found. Returns nil slice", thinPoolName, lvgName))
		return nil, nil
	}

	result := make([]*v1.PersistentVolumeClaim, 0, pvcPerLVGCount)
	thinPoolCh.(*thinPoolCache).pvcs.Range(func(_, pvcCh any) bool {
		result = append(result, pvcCh.(*pvcCache).pvc)
		return true
	})

	return result, nil
}

// GetLVGNamesForPVC returns a slice of LVMVolumeGroup resources names, where selected PVC has been stored in. If no such LVMVolumeGroup found, returns empty slice.
func (c *Cache) GetLVGNamesForPVC(pvc *v1.PersistentVolumeClaim) []string {
	pvcKey := configurePVCKey(pvc)
	lvgNames, found := c.pvcLVGs.Load(pvcKey)
	if !found {
		c.log.Warning(fmt.Sprintf("[GetLVGNamesForPVC] no cached LVMVolumeGroups were found for PVC %s", pvcKey))
		return nil
	}

	return lvgNames.([]string)
}

// CheckIsPVCStored checks if selected PVC has been already stored in the cache.
func (c *Cache) CheckIsPVCStored(pvc *v1.PersistentVolumeClaim) bool {
	pvcKey := configurePVCKey(pvc)
	if _, found := c.pvcLVGs.Load(pvcKey); found {
		return true
	}

	return false
}

// RemoveSpaceReservationForPVCWithSelectedNode removes space reservation for selected PVC for every LVMVolumeGroup resource, which is not bound to the PVC selected node.
func (c *Cache) RemoveSpaceReservationForPVCWithSelectedNode(pvc *v1.PersistentVolumeClaim, deviceType string) error {
	pvcKey := configurePVCKey(pvc)
	// the LVG which is used to store PVC
	selectedLVGName := ""

	lvgNamesForPVC, found := c.pvcLVGs.Load(pvcKey)
	if !found {
		c.log.Debug(fmt.Sprintf("[RemoveSpaceReservationForPVCWithSelectedNode] cache for PVC %s has been already removed", pvcKey))
		return nil
	}

	for _, lvgName := range lvgNamesForPVC.([]string) {
		lvgCh, found := c.lvgs.Load(lvgName)
		if !found || lvgCh == nil {
			err := fmt.Errorf("no cache found for the LVMVolumeGroup %s", lvgName)
			c.log.Error(err, fmt.Sprintf("[RemoveSpaceReservationForPVCWithSelectedNode] an error occurred while trying to remove space reservation for PVC %s", pvcKey))
			return err
		}

		switch deviceType {
		case consts.Thin:
			lvgCh.(*lvgCache).thinPools.Range(func(thinPoolName, thinPoolCh any) bool {
				pvcCh, found := thinPoolCh.(*thinPoolCache).pvcs.Load(pvcKey)
				if !found {
					c.log.Debug(fmt.Sprintf("[RemoveSpaceReservationForPVCWithSelectedNode] PVC %s space reservation in the LVMVolumeGroup %s has been already removed", pvcKey, lvgName))
					return true
				}

				selectedNode := pvcCh.(*pvcCache).selectedNode
				if selectedNode == "" {
					thinPoolCh.(*thinPoolCache).pvcs.Delete(pvcKey)
					c.log.Debug(fmt.Sprintf("[RemoveSpaceReservationForPVCWithSelectedNode] removed space reservation for PVC %s in the Thin pool %s of the LVMVolumeGroup %s due the PVC got selected to the node %s", pvcKey, thinPoolName.(string), lvgName, pvc.Annotations[SelectedNodeAnnotation]))
				} else {
					selectedLVGName = lvgName
					c.log.Debug(fmt.Sprintf("[RemoveSpaceReservationForPVCWithSelectedNode] PVC %s got selected to the node %s. It should not be revomed from the LVMVolumeGroup %s", pvcKey, pvc.Annotations[SelectedNodeAnnotation], lvgName))
				}

				return true
			})
		case consts.Thick:
			pvcCh, found := lvgCh.(*lvgCache).thickPVCs.Load(pvcKey)
			if !found {
				c.log.Debug(fmt.Sprintf("[RemoveSpaceReservationForPVCWithSelectedNode] PVC %s space reservation in the LVMVolumeGroup %s has been already removed", pvcKey, lvgName))
				continue
			}

			selectedNode := pvcCh.(*pvcCache).selectedNode
			if selectedNode == "" {
				lvgCh.(*lvgCache).thickPVCs.Delete(pvcKey)
				c.log.Debug(fmt.Sprintf("[RemoveSpaceReservationForPVCWithSelectedNode] removed space reservation for PVC %s in the LVMVolumeGroup %s due the PVC got selected to the node %s", pvcKey, lvgName, pvc.Annotations[SelectedNodeAnnotation]))
			} else {
				selectedLVGName = lvgName
				c.log.Debug(fmt.Sprintf("[RemoveSpaceReservationForPVCWithSelectedNode] PVC %s got selected to the node %s. It should not be revomed from the LVMVolumeGroup %s", pvcKey, pvc.Annotations[SelectedNodeAnnotation], lvgName))
			}
		}
	}
	c.log.Debug(fmt.Sprintf("[RemoveSpaceReservationForPVCWithSelectedNode] PVC %s space reservation has been removed from LVMVolumeGroup cache", pvcKey))

	c.log.Debug(fmt.Sprintf("[RemoveSpaceReservationForPVCWithSelectedNode] cache for PVC %s will be wiped from unused LVMVolumeGroups", pvcKey))
	cleared := make([]string, 0, len(lvgNamesForPVC.([]string)))
	for _, lvgName := range lvgNamesForPVC.([]string) {
		if lvgName == selectedLVGName {
			c.log.Debug(fmt.Sprintf("[RemoveSpaceReservationForPVCWithSelectedNode] the LVMVolumeGroup %s will be saved for PVC %s cache as used", lvgName, pvcKey))
			cleared = append(cleared, lvgName)
		} else {
			c.log.Debug(fmt.Sprintf("[RemoveSpaceReservationForPVCWithSelectedNode] the LVMVolumeGroup %s will be removed from PVC %s cache as not used", lvgName, pvcKey))
		}
	}
	c.log.Trace(fmt.Sprintf("[RemoveSpaceReservationForPVCWithSelectedNode] cleared LVMVolumeGroups for PVC %s: %v", pvcKey, cleared))
	c.pvcLVGs.Store(pvcKey, cleared)

	return nil
}

// RemovePVCFromTheCache completely removes selected PVC in the cache.
func (c *Cache) RemovePVCFromTheCache(pvc *v1.PersistentVolumeClaim) {
	pvcKey := configurePVCKey(pvc)

	c.log.Debug(fmt.Sprintf("[RemovePVCFromTheCache] run full cache wipe for PVC %s", pvcKey))
	lvgSlice, ok := c.pvcLVGs.Load(pvcKey)
	if ok {
		for _, lvgName := range lvgSlice.([]string) {
			lvgCh, found := c.lvgs.Load(lvgName)
			if found {
				lvgCh.(*lvgCache).thickPVCs.Delete(pvcKey)
				lvgCh.(*lvgCache).thinPools.Range(func(_, tpCh any) bool {
					tpCh.(*thinPoolCache).pvcs.Delete(pvcKey)
					return true
				})
			}
		}
	}

	c.pvcLVGs.Delete(pvcKey)
}

// FindLVGForPVCBySelectedNode finds a suitable LVMVolumeGroup resource's name for selected PVC based on selected node. If no such LVMVolumeGroup found, returns empty string.
func (c *Cache) FindLVGForPVCBySelectedNode(pvc *v1.PersistentVolumeClaim, nodeName string) string {
	pvcKey := configurePVCKey(pvc)

	lvgsForPVC, found := c.pvcLVGs.Load(pvcKey)
	if !found {
		c.log.Debug(fmt.Sprintf("[FindLVGForPVCBySelectedNode] no LVMVolumeGroups were found in the cache for PVC %s. Returns empty string", pvcKey))
		return ""
	}

	lvgsOnTheNode, found := c.nodeLVGs.Load(nodeName)
	if !found {
		c.log.Debug(fmt.Sprintf("[FindLVGForPVCBySelectedNode] no LVMVolumeGroups were found in the cache for the node %s. Returns empty string", nodeName))
		return ""
	}

	var targetLVG string
	for _, lvgName := range lvgsForPVC.([]string) {
		if slices2.Contains(lvgsOnTheNode.([]string), lvgName) {
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
	c.lvgs.Range(func(lvgName, lvgCh any) bool {
		c.log.Cache(fmt.Sprintf("[%s]", lvgName))

		lvgCh.(*lvgCache).thickPVCs.Range(func(pvcName, pvcCh any) bool {
			c.log.Cache(fmt.Sprintf("      THICK PVC %s, selected node: %s", pvcName, pvcCh.(*pvcCache).selectedNode))
			return true
		})

		lvgCh.(*lvgCache).thinPools.Range(func(thinPoolName, thinPoolCh any) bool {
			thinPoolCh.(*thinPoolCache).pvcs.Range(func(pvcName, pvcCh any) bool {
				c.log.Cache(fmt.Sprintf("      THIN POOL %s PVC %s, selected node: %s", thinPoolName, pvcName, pvcCh.(*pvcCache).selectedNode))
				return true
			})

			return true
		})

		return true
	})

	c.log.Cache("[LVMVolumeGroups ENDS]")
	c.log.Cache("[PVC and LVG BEGINS]")
	c.pvcLVGs.Range(func(pvcName, lvgs any) bool {
		c.log.Cache(fmt.Sprintf("[PVC: %s]", pvcName))

		for _, lvgName := range lvgs.([]string) {
			c.log.Cache(fmt.Sprintf("      LVMVolumeGroup: %s", lvgName))
		}

		return true
	})

	c.log.Cache("[PVC and LVG ENDS]")
	c.log.Cache("[Node and LVG BEGINS]")
	c.nodeLVGs.Range(func(nodeName, lvgs any) bool {
		c.log.Cache(fmt.Sprintf("[Node: %s]", nodeName))

		for _, lvgName := range lvgs.([]string) {
			c.log.Cache(fmt.Sprintf("      LVMVolumeGroup name: %s", lvgName))
		}

		return true
	})
	c.log.Cache("[Node and LVG ENDS]")
	c.log.Cache("*******************CACHE END*******************")
}

func configurePVCKey(pvc *v1.PersistentVolumeClaim) string {
	return fmt.Sprintf("%s/%s", pvc.Namespace, pvc.Name)
}
