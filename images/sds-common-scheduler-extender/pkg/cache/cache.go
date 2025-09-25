/*
Copyright YEAR Flant JSC

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
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	slices2 "k8s.io/utils/strings/slices"

	snc "github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/consts"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/logger"
)

const (
	SelectedNodeAnnotation = "volume.kubernetes.io/selected-node"
	lvgsPerPVCCount        = 5
	lvgsPerNodeCount       = 5
)

type Cache struct {
	storage *Storage
	log     *logger.Logger
}

type Storage struct {
	Lvgs     map[string]*LvgCache `json:"lvgs"`
	PvcLVGs  map[string][]string  `json:"pvc_lvgs"`
	NodeLVGs map[string][]string  `json:"node_lvgs"`
	// ReplicaLVGs stores mapping replicaKey -> LVG names where reservations were created
	ReplicaLVGs map[string][]string `json:"replica_lvgs"`
}

type LvgCache struct {
	Lvg       *snc.LVMVolumeGroup             `json:"lvg"`
	ThickPVCs map[string]*pvcCache            `json:"thick_pvcs"`
	ThinPools map[string]map[string]*pvcCache `json:"thin_pools"`
	// ThickReplicas holds replica reservations for thick devices: replicaKey -> reservation
	ThickReplicas map[string]*replicaCache `json:"thick_replicas"`
	// ThinReplicaPools holds replica reservations for thin devices: thinPoolName -> (replicaKey -> reservation)
	ThinReplicaPools map[string]map[string]*replicaCache `json:"thin_replica_pools"`
}

type pvcCache struct {
	PVC          *corev1.PersistentVolumeClaim `json:"pvc"`
	SelectedNode string                        `json:"selected_node"`
	Provisioner  string                        `json:"provisioner"`
}

type replicaCache struct {
	Key       string `json:"key"`
	Requested int64  `json:"requested_bytes"`
}

func NewCache(log *logger.Logger) *Cache {
	return &Cache{
		storage: &Storage{
			Lvgs:        make(map[string]*LvgCache),
			PvcLVGs:     make(map[string][]string),
			NodeLVGs:    make(map[string][]string),
			ReplicaLVGs: make(map[string][]string),
		},
		log: log,
	}
}

func (c *Cache) String() string {
	bytes, err := json.Marshal(c)
	if err != nil {
		c.log.Error(err, "failed to marshal cache. returning empty string")
		return ""
	}
	return string(bytes)
}

func (c *Cache) clearBoundExpiredPVC(pvcTTL time.Duration) int {
	deletedPVCs := 0

	for lvgName := range c.storage.Lvgs {
		pvcs, err := c.GetAllPVCForLVG(lvgName)
		if err != nil {
			c.log.Error(err, fmt.Sprintf("[clearBoundExpiredPVC] unable to get PVCs for the LVMVolumeGroup %s", lvgName))
			continue
		}

		for _, pvc := range pvcs {
			if pvc.Status.Phase != v1.ClaimBound {
				c.log.Trace(fmt.Sprintf("[clearBoundExpiredPVC] PVC %s is not in a Bound state", pvc.Name))
				continue
			}

			if time.Since(pvc.CreationTimestamp.Time) > pvcTTL {
				c.log.Warning(fmt.Sprintf("[clearBoundExpiredPVC] PVC %s is in a Bound state and expired, remove it from the cache", pvc.Name))
				c.RemovePVCFromTheCache(pvc)
				deletedPVCs++
			} else {
				c.log.Trace(fmt.Sprintf("[clearBoundExpiredPVC] PVC %s is in a Bound state but not expired yet.", pvc.Name))
			}
		}
	}

	c.log.Debug("[clearBoundExpiredPVC] finished the expired PVC clearing")
	return deletedPVCs
}

func (c *Cache) GetAllPVCForLVG(lvgName string) ([]*v1.PersistentVolumeClaim, error) {
	lvgCh, found := c.storage.Lvgs[lvgName]
	if !found {
		err := fmt.Errorf("cache was not found for the LVMVolumeGroup %s", lvgName)
		c.log.Error(err, fmt.Sprintf("[GetAllPVCForLVG] an error occurred while trying to get all PVC for the LVMVolumeGroup %s", lvgName))
		return nil, err
	}

	size := len(lvgCh.ThickPVCs)
	for _, pvcMap := range lvgCh.ThinPools {
		size += len(pvcMap)
	}

	result := make([]*v1.PersistentVolumeClaim, 0, size)
	for _, pvcCh := range lvgCh.ThickPVCs {
		result = append(result, pvcCh.PVC)
	}

	for _, pvcMap := range lvgCh.ThinPools {
		for _, pvcCh := range pvcMap {
			result = append(result, pvcCh.PVC)
		}
	}

	return result, nil
}

func (c *Cache) GetAllLVG() map[string]*snc.LVMVolumeGroup {
	lvgs := make(map[string]*snc.LVMVolumeGroup)
	for lvgName, lvgCh := range c.storage.Lvgs {
		if lvgCh.Lvg == nil {
			c.log.Error(fmt.Errorf("LVMVolumeGroup %s is not initialized", lvgName), "[GetAllLVG] an error occurs while iterating the LVMVolumeGroups")
			continue
		}

		lvgs[lvgName] = lvgCh.Lvg
	}

	return lvgs
}

func (c *Cache) GetLVGThickReservedSpace(lvgName string) (int64, error) {
	lvg, found := c.storage.Lvgs[lvgName]
	if !found {
		c.log.Debug(fmt.Sprintf("[GetLVGThickReservedSpace] the LVMVolumeGroup %s was not found in the cache. Returns 0", lvgName))
		return 0, nil
	}

	var space int64
	for _, pvcCh := range lvg.ThickPVCs {
		space += pvcCh.PVC.Spec.Resources.Requests.Storage().Value()
	}

	for _, r := range lvg.ThickReplicas {
		space += r.Requested
	}

	return space, nil
}

func (c *Cache) GetLVGThinReservedSpace(lvgName string, thinPoolName string) (int64, error) {
	lvgCh, found := c.storage.Lvgs[lvgName]
	if !found {
		c.log.Debug(fmt.Sprintf("[GetLVGThinReservedSpace] the LVMVolumeGroup %s was not found in the cache. Returns 0", lvgName))
		return 0, nil
	}

	var space int64
	if pvcMap, found := lvgCh.ThinPools[thinPoolName]; found {
		for _, pvcCh := range pvcMap {
			space += pvcCh.PVC.Spec.Resources.Requests.Storage().Value()
		}
	} else {
		c.log.Debug(fmt.Sprintf("[GetLVGThinReservedSpace] the Thin pool %s of the LVMVolumeGroup %s was not found in the PVC cache. Continue with replica reservations only", thinPoolName, lvgName))
	}

	if rMap, ok := lvgCh.ThinReplicaPools[thinPoolName]; ok {
		for _, r := range rMap {
			space += r.Requested
		}
	}

	return space, nil
}

func (c *Cache) RemovePVCFromTheCache(pvc *v1.PersistentVolumeClaim) {
	pvcKey := configurePVCKey(pvc)

	c.log.Debug(fmt.Sprintf("[RemovePVCFromTheCache] run full cache wipe for PVC %s", pvcKey))
	lvgSlice, ok := c.storage.PvcLVGs[pvcKey]
	if ok {
		for _, lvgName := range lvgSlice {
			lvgCh, found := c.storage.Lvgs[lvgName]
			if found {
				delete(lvgCh.ThickPVCs, pvcKey)
				for _, pvcMap := range lvgCh.ThinPools {
					delete(pvcMap, pvcKey)
				}
			}
		}
	}

	delete(c.storage.PvcLVGs, pvcKey)
}

func (c *Cache) GetLVGNamesForPVC(pvc *v1.PersistentVolumeClaim) []string {
	pvcKey := configurePVCKey(pvc)
	lvgNames, found := c.storage.PvcLVGs[pvcKey]
	if !found {
		c.log.Warning(fmt.Sprintf("[GetLVGNamesForPVC] no cached LVMVolumeGroups were found for PVC %s", pvcKey))
		return nil
	}

	return lvgNames
}

func (c *Cache) GetLVGNamesByNodeName(nodeName string) []string {
	lvgs, found := c.storage.NodeLVGs[nodeName]
	if !found {
		c.log.Debug(fmt.Sprintf("[GetLVGNamesByNodeName] no LVMVolumeGroup was found in the cache for the node %s. Return empty slice", nodeName))
		return []string{}
	}

	return lvgs
}

func (c *Cache) UpdateThickPVC(lvgName string, pvc *v1.PersistentVolumeClaim, provisioner string) error {
	pvcKey := configurePVCKey(pvc)

	lvgCh, found := c.storage.Lvgs[lvgName]
	if !found {
		return fmt.Errorf("the LVMVolumeGroup %s was not found in the cache", lvgName)
	}

	pvcCh, found := lvgCh.ThickPVCs[pvcKey]
	if !found {
		c.log.Warning(fmt.Sprintf("[UpdateThickPVC] PVC %s was not found in the cache for the LVMVolumeGroup %s. It will be added", pvcKey, lvgName))
		err := c.AddThickPVC(lvgName, pvc, provisioner)
		if err != nil {
			c.log.Error(err, fmt.Sprintf("[UpdateThickPVC] an error occurred while trying to update the PVC %s", pvcKey))
			return err
		}
		return nil
	}

	pvcCh.PVC = pvc
	pvcCh.SelectedNode = pvc.Annotations[SelectedNodeAnnotation]
	pvcCh.Provisioner = provisioner
	c.log.Debug(fmt.Sprintf("[UpdateThickPVC] successfully updated PVC %s with selected node %s in the cache for LVMVolumeGroup %s", pvcKey, pvc.Annotations[SelectedNodeAnnotation], lvgName))
	return nil
}

func (c *Cache) AddThickPVC(lvgName string, pvc *v1.PersistentVolumeClaim, provisioner string) error {
	if pvc.Status.Phase == v1.ClaimBound {
		c.log.Warning(fmt.Sprintf("[AddThickPVC] PVC %s/%s has status phase BOUND. It will not be added to the cache", pvc.Namespace, pvc.Name))
		return nil
	}

	pvcKey := configurePVCKey(pvc)

	lvgCh, found := c.storage.Lvgs[lvgName]
	if !found {
		err := fmt.Errorf("the LVMVolumeGroup %s was not found in the cache", lvgName)
		c.log.Error(err, fmt.Sprintf("[AddThickPVC] an error occurred while trying to add PVC %s to the cache", pvcKey))
		return err
	}

	c.log.Trace(fmt.Sprintf("[AddThickPVC] PVC %s/%s annotations: %v", pvc.Namespace, pvc.Name, pvc.Annotations))

	shouldAdd, err := c.shouldAddPVC(pvc, lvgCh, pvcKey, lvgName, "")
	if err != nil {
		return err
	}

	if !shouldAdd {
		c.log.Debug(fmt.Sprintf("[AddThickPVC] PVC %s should not be added", pvcKey))
		return nil
	}

	c.log.Debug(fmt.Sprintf("[AddThickPVC] new PVC %s cache will be added to the LVMVolumeGroup %s", pvcKey, lvgName))
	c.addNewThickPVC(lvgCh, pvc, provisioner)

	return nil
}

func (c *Cache) addNewThickPVC(lvgCh *LvgCache, pvc *v1.PersistentVolumeClaim, provisioner string) {
	pvcKey := configurePVCKey(pvc)
	lvgCh.ThickPVCs[pvcKey] = &pvcCache{
		PVC:          pvc,
		SelectedNode: pvc.Annotations[SelectedNodeAnnotation],
		Provisioner:  provisioner,
	}

	c.AddLVGToPVC(lvgCh.Lvg.Name, pvcKey)
}

func (c *Cache) AddLVGToPVC(lvgName, pvcKey string) {
	// TODO protect from duplicates
	lvgsForPVC, found := c.storage.PvcLVGs[pvcKey]
	if !found || lvgsForPVC == nil {
		lvgsForPVC = make([]string, 0, lvgsPerPVCCount)
	}

	c.log.Trace(fmt.Sprintf("[addLVGToPVC] LVMVolumeGroups from the cache for PVC %s before append: %v", pvcKey, lvgsForPVC))
	lvgsForPVC = append(lvgsForPVC, lvgName)
	c.log.Trace(fmt.Sprintf("[addLVGToPVC] LVMVolumeGroups from the cache for PVC %s after append: %v", pvcKey, lvgsForPVC))
	c.storage.PvcLVGs[pvcKey] = lvgsForPVC
}

// ReserveThickReplica stores a thick replica reservation for given LVG and node
func (c *Cache) ReserveThickReplica(lvgName string, replicaKey string, requestedBytes int64) error {
	lvgCh, found := c.storage.Lvgs[lvgName]
	if !found {
		return fmt.Errorf("the LVMVolumeGroup %s was not found in the cache", lvgName)
	}

	if lvgCh.ThickReplicas == nil {
		lvgCh.ThickReplicas = make(map[string]*replicaCache)
	}
	lvgCh.ThickReplicas[replicaKey] = &replicaCache{Key: replicaKey, Requested: requestedBytes}

	// index LVG by replica key for fast cleanup
	c.addLVGToReplica(replicaKey, lvgName)
	c.log.Debug(fmt.Sprintf("[ReserveThickReplica] reserved %d bytes in LVG %s for replica %s", requestedBytes, lvgName, replicaKey))
	return nil
}

// ReserveThinReplica stores a thin replica reservation for given LVG, thin pool and node
func (c *Cache) ReserveThinReplica(lvgName, thinPoolName, replicaKey string, requestedBytes int64) error {
	lvgCh, found := c.storage.Lvgs[lvgName]
	if !found {
		return fmt.Errorf("the LVMVolumeGroup %s was not found in the cache", lvgName)
	}

	if lvgCh.ThinReplicaPools == nil {
		lvgCh.ThinReplicaPools = make(map[string]map[string]*replicaCache)
	}
	if _, ok := lvgCh.ThinReplicaPools[thinPoolName]; !ok {
		lvgCh.ThinReplicaPools[thinPoolName] = make(map[string]*replicaCache)
	}
	lvgCh.ThinReplicaPools[thinPoolName][replicaKey] = &replicaCache{Key: replicaKey, Requested: requestedBytes}

	// index LVG by replica key for fast cleanup
	c.addLVGToReplica(replicaKey, lvgName)
	c.log.Debug(fmt.Sprintf("[ReserveThinReplica] reserved %d bytes in LVG %s thin pool %s for replica %s", requestedBytes, lvgName, thinPoolName, replicaKey))
	return nil
}

// RemoveReplicaReservations removes reservations for a replica based on provided flags
func (c *Cache) RemoveReplicaReservations(replicaKey string, selectedThickLVG string, selectedThinLVG string, selectedThinPool string, clearNonSelected, clearSelected bool) {
	lvgs, found := c.storage.ReplicaLVGs[replicaKey]
	if !found {
		c.log.Debug(fmt.Sprintf("[RemoveReplicaReservations] no reservations found for replica %s", replicaKey))
		return
	}

	keptLVGs := make([]string, 0, len(lvgs))
	for _, lvgName := range lvgs {
		lvgCh, ok := c.storage.Lvgs[lvgName]
		if !ok {
			continue
		}

		// thick: keep only selectedThickLVG when clearNonSelected; remove selected when clearSelected
		if _, ok := lvgCh.ThickReplicas[replicaKey]; ok {
			if clearNonSelected {
				if lvgName != selectedThickLVG {
					delete(lvgCh.ThickReplicas, replicaKey)
					c.log.Debug(fmt.Sprintf("[RemoveReplicaReservations] removed thick reservation for replica %s from LVG %s (non-selected)", replicaKey, lvgName))
				} else {
					keptLVGs = append(keptLVGs, lvgName)
				}
			} else if clearSelected && lvgName == selectedThickLVG {
				delete(lvgCh.ThickReplicas, replicaKey)
				c.log.Debug(fmt.Sprintf("[RemoveReplicaReservations] removed thick reservation for replica %s from LVG %s (selected)", replicaKey, lvgName))
			} else {
				keptLVGs = append(keptLVGs, lvgName)
			}
		}

		// thin
		for thinPoolName, rMap := range lvgCh.ThinReplicaPools {
			if _, ok := rMap[replicaKey]; ok {
				if clearNonSelected {
					if !(lvgName == selectedThinLVG && thinPoolName == selectedThinPool) {
						delete(rMap, replicaKey)
						c.log.Debug(fmt.Sprintf("[RemoveReplicaReservations] removed thin reservation for replica %s from LVG %s pool %s (non-selected)", replicaKey, lvgName, thinPoolName))
					} else {
						keptLVGs = append(keptLVGs, lvgName)
					}
				} else if clearSelected && lvgName == selectedThinLVG && thinPoolName == selectedThinPool {
					delete(rMap, replicaKey)
					c.log.Debug(fmt.Sprintf("[RemoveReplicaReservations] removed thin reservation for replica %s from LVG %s pool %s (selected)", replicaKey, lvgName, thinPoolName))
				} else {
					keptLVGs = append(keptLVGs, lvgName)
				}
			}
		}
	}

	// update index
	if len(keptLVGs) == 0 {
		delete(c.storage.ReplicaLVGs, replicaKey)
	} else {
		c.storage.ReplicaLVGs[replicaKey] = keptLVGs
	}
}

func (c *Cache) addLVGToReplica(replicaKey, lvgName string) {
	l := c.storage.ReplicaLVGs[replicaKey]
	if l == nil {
		l = make([]string, 0, 3)
	}
	if !slices.Contains(l, lvgName) {
		l = append(l, lvgName)
	}
	c.storage.ReplicaLVGs[replicaKey] = l
}

func (c *Cache) shouldAddPVC(pvc *v1.PersistentVolumeClaim, lvgCh *LvgCache, pvcKey, lvgName, thinPoolName string) (bool, error) {
	if pvc.Annotations[SelectedNodeAnnotation] != "" {
		c.log.Debug(fmt.Sprintf("[shouldAddPVC] PVC %s/%s has selected node annotation, selected node: %s", pvc.Namespace, pvc.Name, pvc.Annotations[SelectedNodeAnnotation]))

		lvgsOnTheNode, found := c.storage.NodeLVGs[pvc.Annotations[SelectedNodeAnnotation]]
		if !found {
			err := fmt.Errorf("no LVMVolumeGroups found for the node %s", pvc.Annotations[SelectedNodeAnnotation])
			c.log.Error(err, fmt.Sprintf("[shouldAddPVC] an error occurred while trying to add PVC %s to the cache", pvcKey))
			return false, err
		}

		if !slices2.Contains(lvgsOnTheNode, lvgName) {
			c.log.Debug(fmt.Sprintf("[shouldAddPVC] LVMVolumeGroup %s does not belong to PVC %s/%s selected node %s. It will be skipped", lvgName, pvc.Namespace, pvc.Name, pvc.Annotations[SelectedNodeAnnotation]))
			return false, nil
		}

		c.log.Debug(fmt.Sprintf("[shouldAddPVC] LVMVolumeGroup %s belongs to PVC %s/%s selected node %s", lvgName, pvc.Namespace, pvc.Name, pvc.Annotations[SelectedNodeAnnotation]))

		// if pvc is thick
		if _, found := lvgCh.ThickPVCs[pvcKey]; found {
			c.log.Debug(fmt.Sprintf("[shouldAddPVC] PVC %s was found in the cache of the LVMVolumeGroup %s", pvcKey, lvgName))
			return false, nil
		}

		// if pvc is thin
		if thinPoolName != "" {
			if thinPool, found := lvgCh.ThinPools[thinPoolName]; found {
				if _, found := thinPool[pvcKey]; found {
					c.log.Debug(fmt.Sprintf("[shouldAddPVC] PVC %s was found in the Thin pool %s cache of the LVMVolumeGroup %s. No need to add", pvcKey, thinPoolName, lvgName))
					return false, nil
				}
			} else {
				c.log.Debug(fmt.Sprintf("[shouldAddPVC] Thin pool %s was not found in the cache, PVC %s should be added", thinPoolName, pvcKey))
				return true, nil
			}
		}
	}

	return true, nil
}

func (c *Cache) UpdateThinPVC(lvgName, thinPoolName string, pvc *v1.PersistentVolumeClaim, provisioner string) error {
	pvcKey := configurePVCKey(pvc)
	lvgCh, found := c.storage.Lvgs[lvgName]
	if !found {
		return fmt.Errorf("the LVMVolumeGroup %s was not found in the cache", lvgName)
	}

	if _, found := lvgCh.ThinPools[thinPoolName]; !found {
		c.log.Debug(fmt.Sprintf("[UpdateThinPVC] Thin Pool %s was not found in the LVMVolumeGroup %s, add it.", thinPoolName, lvgName))
		err := c.addThinPoolIfNotExists(lvgCh, thinPoolName)
		if err != nil {
			return err
		}
	}

	if _, found := lvgCh.ThinPools[thinPoolName][pvcKey]; !found {
		c.log.Warning(fmt.Sprintf("[UpdateThinPVC] Thin PVC %s was not found in Thin pool %s in the cache for the LVMVolumeGroup %s. It will be added", pvcKey, thinPoolName, lvgName))
		err := c.addNewThinPVC(lvgCh, pvc, thinPoolName, provisioner)
		if err != nil {
			c.log.Error(err, fmt.Sprintf("[UpdateThinPVC] an error occurred while trying to update the PVC %s", pvcKey))
			return err
		}
		return nil
	}

	lvgCh.ThinPools[thinPoolName][pvcKey].PVC = pvc
	lvgCh.ThinPools[thinPoolName][pvcKey].SelectedNode = pvc.Annotations[SelectedNodeAnnotation]
	lvgCh.ThinPools[thinPoolName][pvcKey].Provisioner = provisioner
	c.log.Debug(fmt.Sprintf("[UpdateThinPVC] successfully updated THIN PVC %s with selected node %s in the cache for LVMVolumeGroup %s", pvcKey, pvc.Annotations[SelectedNodeAnnotation], lvgName))

	return nil
}

func (c *Cache) addThinPoolIfNotExists(lvgCh *LvgCache, thinPoolName string) error {
	if thinPoolName == "" {
		err := errors.New("no thin pool name specified")
		c.log.Error(err, fmt.Sprintf("[addThinPoolIfNotExists] unable to add thin pool in the LVMVolumeGroup %s", lvgCh.Lvg.Name))
		return err
	}

	if _, found := lvgCh.ThinPools[thinPoolName]; found {
		c.log.Debug(fmt.Sprintf("[addThinPoolIfNotExists] Thin pool %s is already created in the LVMVolumeGroup %s. No need to add a new one", thinPoolName, lvgCh.Lvg.Name))
		return nil
	}

	lvgCh.ThinPools[thinPoolName] = make(map[string]*pvcCache)
	return nil
}

func (c *Cache) addNewThinPVC(lvgCh *LvgCache, pvc *v1.PersistentVolumeClaim, thinPoolName string, provisioner string) error {
	pvcKey := configurePVCKey(pvc)
	err := c.addThinPoolIfNotExists(lvgCh, thinPoolName)
	if err != nil {
		c.log.Error(err, fmt.Sprintf("[addNewThinPVC] unable to add Thin pool %s in the LVMVolumeGroup %s cache for PVC %s", thinPoolName, lvgCh.Lvg.Name, pvc.Name))
		return err
	}

	if _, found := lvgCh.ThinPools[thinPoolName]; !found {
		err = fmt.Errorf("thin pool %s not found", thinPoolName)
		c.log.Error(err, fmt.Sprintf("[addNewThinPVC] unable to add Thin PVC %s to the cache", pvcKey))
		return err
	}

	lvgCh.ThinPools[thinPoolName][pvcKey] = &pvcCache{
		PVC:          pvc,
		SelectedNode: pvc.Annotations[SelectedNodeAnnotation],
		Provisioner:  provisioner,
	}
	c.log.Debug(fmt.Sprintf("[addNewThinPVC] THIN PVC %s was added to the cache to Thin Pool %s", pvcKey, thinPoolName))

	c.AddLVGToPVC(lvgCh.Lvg.Name, pvcKey)
	return nil
}

func (c *Cache) RemoveSpaceReservationForPVCWithSelectedNode(pvc *v1.PersistentVolumeClaim, deviceType string) error {
	pvcKey := configurePVCKey(pvc)
	// the LVG which is used to store PVC
	selectedLVGsNames := make([]string, 0, 3)

	lvgNamesForPVC, found := c.storage.PvcLVGs[pvcKey]
	if !found {
		c.log.Debug(fmt.Sprintf("[RemoveSpaceReservationForPVCWithSelectedNode] cache for PVC %s has been already removed", pvcKey))
		return nil
	}

	for _, lvgName := range lvgNamesForPVC {
		lvgCh, found := c.storage.Lvgs[lvgName]
		if !found || lvgCh == nil {
			err := fmt.Errorf("no cache found for the LVMVolumeGroup %s", lvgName)
			c.log.Error(err, fmt.Sprintf("[RemoveSpaceReservationForPVCWithSelectedNode] an error occurred while trying to remove space reservation for PVC %s", pvcKey))
			return err
		}

		switch deviceType {
		case consts.Thin:
			for thinPoolName, thinPool := range lvgCh.ThinPools {
				if pvcCh, found := thinPool[pvcKey]; found {
					selectedNode := pvcCh.SelectedNode
					if selectedNode == "" {
						delete(lvgCh.ThinPools[thinPoolName], pvcKey)
						c.log.Debug(fmt.Sprintf("[RemoveSpaceReservationForPVCWithSelectedNode] removed space reservation for PVC %s in the Thin pool %s of the LVMVolumeGroup %s due the PVC was selected to the node %s", pvcKey, thinPoolName, lvgName, pvc.Annotations[SelectedNodeAnnotation]))
					} else {
						// TODO найти все лвг, хрянящие копии тома
						selectedLVGsNames = append(selectedLVGsNames, lvgName)
						c.log.Debug(fmt.Sprintf("[RemoveSpaceReservationForPVCWithSelectedNode] PVC %s was selected to the node %s. It should not be removed from the LVMVolumeGroup %s", pvcKey, pvc.Annotations[SelectedNodeAnnotation], lvgName))
					}
				} else {
					c.log.Debug(fmt.Sprintf("[RemoveSpaceReservationForPVCWithSelectedNode] PVC %s space reservation in the LVMVolumeGroup %s has been already removed", pvcKey, lvgName))
				}
			}
		case consts.Thick:
			if pvcCh, found := lvgCh.ThickPVCs[pvcKey]; found {
				selectedNode := pvcCh.SelectedNode
				if selectedNode == "" {
					delete(lvgCh.ThickPVCs, pvcKey)
					c.log.Debug(fmt.Sprintf("[RemoveSpaceReservationForPVCWithSelectedNode] removed space reservation for PVC %s in the LVMVolumeGroup %s due the PVC was selected to the node %s", pvcKey, lvgName, pvc.Annotations[SelectedNodeAnnotation]))
				} else {
					// TODO найти все лвг, хрянящие копии тома
					selectedLVGsNames = append(selectedLVGsNames, lvgName)
					c.log.Debug(fmt.Sprintf("[RemoveSpaceReservationForPVCWithSelectedNode] PVC %s was selected to the node %s. It should not be removed from the LVMVolumeGroup %s", pvcKey, pvc.Annotations[SelectedNodeAnnotation], lvgName))
				}
			} else {
				c.log.Debug(fmt.Sprintf("[RemoveSpaceReservationForPVCWithSelectedNode] PVC %s space reservation in the LVMVolumeGroup %s has been already removed", pvcKey, lvgName))
			}
		}
	}
	c.log.Debug(fmt.Sprintf("[RemoveSpaceReservationForPVCWithSelectedNode] PVC %s space reservation has been removed from LVMVolumeGroup cache", pvcKey))

	c.log.Debug(fmt.Sprintf("[RemoveSpaceReservationForPVCWithSelectedNode] cache for PVC %s will be wiped from unused LVMVolumeGroups", pvcKey))
	cleared := make([]string, 0, len(lvgNamesForPVC))

	for _, lvgName := range lvgNamesForPVC {
		if slices.Contains(selectedLVGsNames, lvgName) {
			c.log.Debug(fmt.Sprintf("[RemoveSpaceReservationForPVCWithSelectedNode] the LVMVolumeGroup %s will be saved for PVC %s cache as used", lvgName, pvcKey))
			cleared = append(cleared, lvgName)
		} else {
			c.log.Debug(fmt.Sprintf("[RemoveSpaceReservationForPVCWithSelectedNode] the LVMVolumeGroup %s will be removed from PVC %s cache as not used", lvgName, pvcKey))
		}
	}

	c.log.Trace(fmt.Sprintf("[RemoveSpaceReservationForPVCWithSelectedNode] cleared LVMVolumeGroups for PVC %s: %v", pvcKey, cleared))
	c.storage.PvcLVGs[pvcKey] = cleared
	return nil
}

func (c *Cache) TryGetLVG(name string) *snc.LVMVolumeGroup {
	lvgCh, found := c.storage.Lvgs[name]
	if !found {
		c.log.Debug(fmt.Sprintf("[TryGetLVG] the LVMVolumeGroup %s was not found in the cache. Return nil", name))
		return nil
	}

	return lvgCh.Lvg
}

func (c *Cache) UpdateLVG(lvg *snc.LVMVolumeGroup) error {
	lvgCh, found := c.storage.Lvgs[lvg.Name]
	if !found {
		return fmt.Errorf("the LVMVolumeGroup %s was not found in the lvgCh", lvg.Name)
	}

	lvgCh.Lvg = lvg

	c.log.Trace(fmt.Sprintf("[UpdateLVG] the LVMVolumeGroup %s nodes: %v", lvg.Name, lvg.Status.Nodes))
	for _, node := range lvg.Status.Nodes {
		lvgsOnTheNode, found := c.storage.NodeLVGs[node.Name]
		if !found {
			lvgsOnTheNode = make([]string, 0, lvgsPerNodeCount)
		}

		if !slices2.Contains(lvgsOnTheNode, lvg.Name) {
			lvgsOnTheNode = append(lvgsOnTheNode, lvg.Name)
			c.log.Debug(fmt.Sprintf("[UpdateLVG] the LVMVolumeGroup %s has been added to the node %s", lvg.Name, node.Name))
			c.storage.NodeLVGs[node.Name] = lvgsOnTheNode
		} else {
			c.log.Debug(fmt.Sprintf("[UpdateLVG] the LVMVolumeGroup %s has been already added to the node %s", lvg.Name, node.Name))
		}
	}
	return nil
}

func (c *Cache) AddLVG(lvg *snc.LVMVolumeGroup) {
	if _, found := c.storage.Lvgs[lvg.Name]; found {
		c.log.Debug(fmt.Sprintf("[AddLVG] the LVMVolumeGroup %s has been already added to the cache", lvg.Name))
		return
	}

	c.storage.Lvgs[lvg.Name] = &LvgCache{
		Lvg:       lvg,
		ThickPVCs: make(map[string]*pvcCache),
		ThinPools: make(map[string]map[string]*pvcCache),
	}

	c.log.Trace(fmt.Sprintf("[AddLVG] the LVMVolumeGroup %s nodes: %v", lvg.Name, lvg.Status.Nodes))
	for _, node := range lvg.Status.Nodes {
		lvgsOnTheNode, found := c.storage.NodeLVGs[node.Name]
		if !found {
			lvgsOnTheNode = make([]string, 0, lvgsPerNodeCount)
		}

		lvgsOnTheNode = append(lvgsOnTheNode, lvg.Name)
		c.log.Debug(fmt.Sprintf("[AddLVG] the LVMVolumeGroup %s has been added to the node %s", lvg.Name, node.Name))
		c.storage.NodeLVGs[node.Name] = lvgsOnTheNode
	}
}

func (c *Cache) DeleteLVG(lvgName string) {
	delete(c.storage.Lvgs, lvgName)

	for nodeName, lvgNames := range c.storage.NodeLVGs {
		for i, lvg := range lvgNames {
			if lvg == lvgName {
				c.storage.NodeLVGs[nodeName] = append(lvgNames[:i], lvgNames[i+1:]...)
				break
			}
		}
	}

	for pvcKey, lvgNames := range c.storage.PvcLVGs {
		for i, lvg := range lvgNames {
			if lvg == lvgName {
				c.storage.PvcLVGs[pvcKey] = append(lvgNames[:i], lvgNames[i+1:]...)
				break
			}
		}
	}
}

func configurePVCKey(pvc *v1.PersistentVolumeClaim) string {
	return fmt.Sprintf("%s/%s", pvc.Namespace, pvc.Name)
}
