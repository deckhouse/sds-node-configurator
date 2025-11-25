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

package scheduler

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/strings/slices"

	snc "github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/cache"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/consts"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/logger"
)

func (s *scheduler) filter(w http.ResponseWriter, r *http.Request) {
	servingLog := s.log.WithName("filter")

	servingLog.Debug("starts the serving the request")

	var inputData ExtenderArgs
	reader := http.MaxBytesReader(w, r.Body, 10<<20) // 10MB
	err := json.NewDecoder(reader).Decode(&inputData)
	if err != nil {
		servingLog.Error(err, "unable to decode a request")
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	servingLog.Trace(fmt.Sprintf("input data: %+v", inputData))

	if inputData.Pod == nil {
		servingLog.Error(errors.New("no pod in the request"), "unable to get a Pod from the request")
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	servingLog = servingLog.WithValues("Pod", fmt.Sprintf("%s/%s", inputData.Pod.Namespace, inputData.Pod.Name))

	nodeNames, err := getNodeNames(inputData)
	if err != nil {
		servingLog.Error(err, "unable to get node names from the request")
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	servingLog.Trace(fmt.Sprintf("NodeNames from the request: %+v", nodeNames))

	targetProvisioners := []string{consts.SdsLocalVolumeProvisioner, consts.SdsReplicatedVolumeProvisioner}
	managedPVCs, err := getManagedPVCsFromPod(s.ctx, s.client, servingLog, inputData.Pod, targetProvisioners)
	if err != nil {
		servingLog.Error(err, "unable to get managed PVCs from the Pod")
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	if len(managedPVCs) == 0 {
		servingLog.Debug("Pod uses PVCs which are not managed by our modules. Return the same nodes")
		if err := writeNodeNamesResponse(w, servingLog, nodeNames); err != nil {
			servingLog.Error(err, "unable to write node names response")
			http.Error(w, "internal server error", http.StatusInternalServerError)
		}
		return
	}
	for _, pvc := range managedPVCs {
		servingLog.Trace(fmt.Sprintf("managed PVC: %s", pvc.Name))

		// this might happen when the extender-scheduler recovers after failure, populates the cache with PVC-watcher controller and then
		// the kube scheduler post a request to schedule the pod with the PVC.
		if s.cache.CheckIsPVCStored(pvc) {
			servingLog.Debug(fmt.Sprintf("PVC %s/%s has been already stored in the cache. Old state will be removed from the cache", pvc.Namespace, pvc.Name))
			s.cache.RemovePVCFromTheCache(pvc)
		} else {
			servingLog.Debug(fmt.Sprintf("PVC %s/%s was not found in the scheduler cache", pvc.Namespace, pvc.Name))
		}
	}

	scUsedByPVCs, err := getStorageClassesUsedByPVCs(s.ctx, s.client, managedPVCs)
	if err != nil {
		servingLog.Error(err, "unable to get StorageClasses from the PVC")
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	for _, sc := range scUsedByPVCs {
		servingLog.Trace(fmt.Sprintf("Pod uses StorageClass: %s", sc.Name))
	}
	if len(scUsedByPVCs) != len(managedPVCs) {
		servingLog.Error(errors.New("number of StorageClasses does not match the number of PVCs"), "unable to get StorageClasses from the PVC")
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	servingLog.Debug("starts to extract PVC requested sizes")
	pvcRequests, err := extractRequestedSize(s.ctx, s.client, servingLog, managedPVCs, scUsedByPVCs)
	if err != nil {
		servingLog.Error(err, "unable to extract request size")
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	if len(pvcRequests) == 0 {
		servingLog.Debug("No PVC requests found. Return the same nodes")
		if err := writeNodeNamesResponse(w, servingLog, nodeNames); err != nil {
			servingLog.Error(err, "unable to write node names response")
			http.Error(w, "internal server error", http.StatusInternalServerError)
		}
		return
	}
	servingLog.Trace(fmt.Sprintf("PVC requests: %+v", pvcRequests))
	servingLog.Debug("successfully extracted the PVC requested sizes")

	servingLog.Debug("starts to filter the nodes from the request")
	filteredNodes, err := filterNodes(servingLog, s.cache, &nodeNames, inputData.Pod, managedPVCs, scUsedByPVCs, pvcRequests)
	if err != nil {
		servingLog.Error(err, "unable to filter the nodes")
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	servingLog.Debug("successfully filtered the nodes from the request")

	servingLog.Debug("starts to populate the cache")
	servingLog.Cache("cache before the PVC reservation")
	s.cache.PrintTheCacheLog()
	err = populateCache(servingLog, filteredNodes.NodeNames, inputData.Pod, s.cache, managedPVCs, scUsedByPVCs)
	if err != nil {
		servingLog.Error(err, "unable to populate cache")
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	servingLog.Debug("successfully populated the cache")
	servingLog.Cache("cache after the PVC reservation")
	s.cache.PrintTheCacheLog()

	w.Header().Set("content-type", "application/json")
	err = json.NewEncoder(w).Encode(filteredNodes)
	if err != nil {
		servingLog.Error(err, "unable to encode a response for a Pod")
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	servingLog.Debug("ends the serving the request")
}

func writeNodeNamesResponse(w http.ResponseWriter, log logger.Logger, nodeNames []string) error {
	filteredNodes := &ExtenderFilterResult{
		NodeNames: &nodeNames,
	}
	log.Trace(fmt.Sprintf("filtered nodes: %+v", filteredNodes))

	w.Header().Set("content-type", "application/json")
	if err := json.NewEncoder(w).Encode(filteredNodes); err != nil {
		return err
	}
	return nil
}

func filterNodes(
	log logger.Logger,
	schedulerCache *cache.Cache,
	nodeNames *[]string,
	pod *corev1.Pod,
	managedPVCs map[string]*corev1.PersistentVolumeClaim,
	scUsedByPVCs map[string]*storagev1.StorageClass,
	pvcRequests map[string]PVCRequest,
) (*ExtenderFilterResult, error) {
	allLVGs := schedulerCache.GetAllLVG()
	for _, lvg := range allLVGs {
		log.Trace(fmt.Sprintf("[filterNodes] LVMVolumeGroup %s in the cache", lvg.Name))
	}

	// TODO: This place is for the future feature to separate LVMVolumeGroups by provisioners

	log.Debug("[filterNodes] starts to get LVMVolumeGroups for each StorageClasses")
	scLVGs, err := GetLVGsFromStorageClasses(scUsedByPVCs)
	if err != nil {
		return nil, err
	}
	log.Debug("[filterNodes] successfully got LVMVolumeGroups for each StorageClasses")
	for scName, lvmVolumeGroups := range scLVGs {
		for _, lvg := range lvmVolumeGroups {
			log.Trace(fmt.Sprintf("[filterNodes] LVMVolumeGroup %s belongs to StorageClass %s", lvg.Name, scName))
		}
	}

	// list of LVMVolumeGroups which are used by the PVCs
	usedLVGs := RemoveUnusedLVGs(allLVGs, scLVGs)
	for _, lvg := range usedLVGs {
		log.Trace(fmt.Sprintf("[filterNodes] the LVMVolumeGroup %s is actually used. VG size: %s, allocatedSize: %s", lvg.Name, lvg.Status.VGSize.String(), lvg.Status.AllocatedSize.String()))
	}

	// get the free space for the Thick LVMVolumeGroups
	lvgsThickFree := getLVGThickFreeSpaces(usedLVGs)
	log.Trace(fmt.Sprintf("[filterNodes] LVMVolumeGroups Thick FreeSpace: %+v", lvgsThickFree))
	for lvgName, freeSpace := range lvgsThickFree {
		log.Trace(fmt.Sprintf("[filterNodes] current LVMVolumeGroup %s Thick free space %s", lvgName, resource.NewQuantity(freeSpace, resource.BinarySI)))
		reservedSpace, err := schedulerCache.GetLVGThickReservedSpace(lvgName)
		if err != nil {
			log.Error(err, fmt.Sprintf("[filterNodes] unable to count cache reserved space for the LVMVolumeGroup %s", lvgName))
			continue
		}
		log.Trace(fmt.Sprintf("[filterNodes] current LVMVolumeGroup %s reserved PVC space %s", lvgName, resource.NewQuantity(reservedSpace, resource.BinarySI)))
		lvgsThickFree[lvgName] -= reservedSpace
	}
	log.Trace(fmt.Sprintf("[filterNodes] LVMVolumeGroups Thick FreeSpace with reserved PVC: %+v", lvgsThickFree))

	// get the free space for the Thin LVMVolumeGroups
	lvgsThinFree := getLVGThinFreeSpaces(usedLVGs)
	log.Trace(fmt.Sprintf("[filterNodes] LVMVolumeGroups Thin FreeSpace: %+v", lvgsThinFree))
	for lvgName, thinPools := range lvgsThinFree {
		for tpName, freeSpace := range thinPools {
			log.Trace(fmt.Sprintf("[filterNodes] current LVMVolumeGroup %s Thin Pool %s free space %s", lvgName, tpName, resource.NewQuantity(freeSpace, resource.BinarySI)))
			reservedSpace, err := schedulerCache.GetLVGThinReservedSpace(lvgName, tpName)
			if err != nil {
				log.Error(err, fmt.Sprintf("[filterNodes] unable to count cache reserved space for the Thin pool %s of the LVMVolumeGroup %s", tpName, lvgName))
				continue
			}
			log.Trace(fmt.Sprintf("[filterNodes] current LVMVolumeGroup %s Thin pool %s reserved PVC space %s", lvgName, tpName, resource.NewQuantity(reservedSpace, resource.BinarySI)))
			lvgsThinFree[lvgName][tpName] -= reservedSpace
		}
	}
	log.Trace(fmt.Sprintf("[filterNodes] LVMVolumeGroups Thin FreeSpace with reserved PVC: %+v", lvgsThinFree))

	// get the LVMVolumeGroups by node name
	// these are the nodes which might store every PVC from the Pod
	nodeLVGs := LVMVolumeGroupsByNodeName(usedLVGs)
	for n, ls := range nodeLVGs {
		for _, l := range ls {
			log.Trace(fmt.Sprintf("[filterNodes] the LVMVolumeGroup %s belongs to node %s", l.Name, n))
		}
	}

	result := &ExtenderFilterResult{
		NodeNames:   &[]string{},
		FailedNodes: FailedNodesMap{},
	}

	thickMapMtx := &sync.RWMutex{}
	thinMapMtx := &sync.RWMutex{}
	failedNodesMapMtx := &sync.Mutex{}

	wg := &sync.WaitGroup{}
	wg.Add(len(*nodeNames))
	errs := make(chan error, len(*nodeNames)*len(managedPVCs))

	for i, nodeName := range *nodeNames {
		go func(i int, nodeName string) {
			log.Trace(fmt.Sprintf("[filterNodes] gourutine %d starts the work with node %s", i, nodeName))
			defer func() {
				log.Trace(fmt.Sprintf("[filterNodes] gourutine %d ends the work with node %s", i, nodeName))
				wg.Done()
			}()

			// we get all LVMVolumeGroups from the node-applicant
			lvgsFromNode, exists := nodeLVGs[nodeName]
			if !exists {
				log.Debug(fmt.Sprintf("[filterNodes] node %s does not have any LVMVolumeGroups from used StorageClasses", nodeName))
				failedNodesMapMtx.Lock()
				result.FailedNodes[nodeName] = fmt.Sprintf("node %s does not have any LVMVolumeGroups from used StorageClasses", nodeName)
				failedNodesMapMtx.Unlock()
				return
			}
			hasEnoughSpace := true

			// now we iterate all over the PVCs to see if we can place all of them on the node (does the node have enough space)
			for _, pvc := range managedPVCs {
				pvcReq := pvcRequests[pvc.Name]

				// we get LVGs which might be used by the PVC
				lvgsFromSC := scLVGs[*pvc.Spec.StorageClassName]

				// we get the specific LVG which the PVC can use on the node as we support only one specified LVG in the Storage Class on each node
				commonLVG := findMatchedLVG(lvgsFromNode, lvgsFromSC)
				if commonLVG == nil {
					err = fmt.Errorf("unable to match Storage Class's LVMVolumeGroup with the node's one, Storage Class: %s, node: %s", *pvc.Spec.StorageClassName, nodeName)
					errs <- err
					return
				}
				log.Trace(fmt.Sprintf("[scoreNodes] LVMVolumeGroup %s is common for storage class %s and node %s", commonLVG.Name, *pvc.Spec.StorageClassName, nodeName))

				// see what kind of space does the PVC need
				switch pvcReq.DeviceType {
				case consts.Thick:
					thickMapMtx.RLock()
					freeSpace := lvgsThickFree[commonLVG.Name]
					thickMapMtx.RUnlock()

					log.Trace(fmt.Sprintf("[filterNodes] LVMVolumeGroup %s Thick free space: %s, PVC requested space: %s", commonLVG.Name, resource.NewQuantity(freeSpace, resource.BinarySI), resource.NewQuantity(pvcReq.RequestedSize, resource.BinarySI)))
					if freeSpace < pvcReq.RequestedSize {
						hasEnoughSpace = false
						break
					}

					thickMapMtx.Lock()
					lvgsThickFree[commonLVG.Name] -= pvcReq.RequestedSize
					thickMapMtx.Unlock()
				case consts.Thin:
					lvg := allLVGs[commonLVG.Name]

					// we try to find specific ThinPool which the PVC can use in the LVMVolumeGroup
					targetThinPool := findMatchedThinPool(lvg.Status.ThinPools, commonLVG.Thin.PoolName)
					if targetThinPool == nil {
						err = fmt.Errorf("unable to match Storage Class's ThinPools with the node's one, Storage Class: %s; node: %s; lvg Thin pools: %+v; Thin.poolName from StorageClass: %s", *pvc.Spec.StorageClassName, nodeName, lvg.Status.ThinPools, commonLVG.Thin.PoolName)
						errs <- err
						return
					}

					thinMapMtx.RLock()
					freeSpace := lvgsThinFree[lvg.Name][targetThinPool.Name]
					thinMapMtx.RUnlock()

					log.Trace(fmt.Sprintf("[filterNodes] LVMVolumeGroup %s Thin Pool %s free space: %s, PVC requested space: %s", lvg.Name, targetThinPool.Name, resource.NewQuantity(freeSpace, resource.BinarySI), resource.NewQuantity(pvcReq.RequestedSize, resource.BinarySI)))

					if freeSpace < pvcReq.RequestedSize {
						hasEnoughSpace = false
						break
					}

					thinMapMtx.Lock()
					lvgsThinFree[lvg.Name][targetThinPool.Name] -= pvcReq.RequestedSize
					thinMapMtx.Unlock()
				}

				if !hasEnoughSpace {
					// we break as if only one PVC can't get enough space, the node does not fit
					break
				}
			}

			if !hasEnoughSpace {
				failedNodesMapMtx.Lock()
				result.FailedNodes[nodeName] = "not enough space"
				failedNodesMapMtx.Unlock()
				return
			}

			*result.NodeNames = append(*result.NodeNames, nodeName)
		}(i, nodeName)
	}
	wg.Wait()
	log.Debug("[filterNodes] goroutines work is done")
	if len(errs) != 0 {
		for err = range errs {
			log.Error(err, "[filterNodes] an error occurs while filtering the nodes")
		}
	}
	close(errs)
	if err != nil {
		log.Error(err, fmt.Sprintf("[filterNodes] unable to filter nodes for the Pod, last error: %s", err.Error()))
		return nil, err
	}

	for _, nodeName := range *result.NodeNames {
		log.Trace(fmt.Sprintf("[filterNodes] for a Pod there is a suitable node: %s", nodeName))
	}

	for node, reason := range result.FailedNodes {
		log.Trace(fmt.Sprintf("[filterNodes] for a Pod there is a failed node: %s, reason: %s", node, reason))
	}

	return result, nil
}

func populateCache(log logger.Logger, nodeNames *[]string, pod *corev1.Pod, schedulerCache *cache.Cache, pvcs map[string]*corev1.PersistentVolumeClaim, scs map[string]*storagev1.StorageClass) error {
	for _, nodeName := range *nodeNames {
		for _, volume := range pod.Spec.Volumes {
			if volume.PersistentVolumeClaim != nil {
				log.Debug(fmt.Sprintf("[populateCache] reconcile the PVC %s for Pod %s/%s on node %s", volume.PersistentVolumeClaim.ClaimName, pod.Namespace, pod.Name, nodeName))
				lvgNamesForTheNode := schedulerCache.GetLVGNamesByNodeName(nodeName)
				log.Trace(fmt.Sprintf("[populateCache] LVMVolumeGroups from cache for the node %s: %v", nodeName, lvgNamesForTheNode))
				pvc := pvcs[volume.PersistentVolumeClaim.ClaimName]
				sc := scs[*pvc.Spec.StorageClassName]

				lvgsForPVC, err := ExtractLVGsFromSC(sc)
				if err != nil {
					return err
				}

				switch sc.Parameters[consts.LvmTypeParamKey] {
				case consts.Thick:
					log.Debug(fmt.Sprintf("[populateCache] Storage Class %s has device type Thick, so the cache will be populated by PVC space requests", sc.Name))
					log.Trace(fmt.Sprintf("[populateCache] LVMVolumeGroups from Storage Class %s for PVC %s/%s: %+v", sc.Name, pvc.Namespace, pvc.Name, lvgsForPVC))
					for _, lvg := range lvgsForPVC {
						if slices.Contains(lvgNamesForTheNode, lvg.Name) {
							log.Trace(fmt.Sprintf("[populateCache] PVC %s/%s will reserve space in LVMVolumeGroup %s cache", pvc.Namespace, pvc.Name, lvg.Name))
							err = schedulerCache.AddThickPVC(lvg.Name, pvc)
							if err != nil {
								return err
							}
						}
					}
				case consts.Thin:
					log.Debug(fmt.Sprintf("[populateCache] Storage Class %s has device type Thin, so the cache will be populated by PVC space requests", sc.Name))
					log.Trace(fmt.Sprintf("[populateCache] LVMVolumeGroups from Storage Class %s for PVC %s/%s: %+v", sc.Name, pvc.Namespace, pvc.Name, lvgsForPVC))
					for _, lvg := range lvgsForPVC {
						if slices.Contains(lvgNamesForTheNode, lvg.Name) {
							log.Trace(fmt.Sprintf("[populateCache] PVC %s/%s will reserve space in LVMVolumeGroup %s Thin Pool %s cache", pvc.Namespace, pvc.Name, lvg.Name, lvg.Thin.PoolName))
							err = schedulerCache.AddThinPVC(lvg.Name, lvg.Thin.PoolName, pvc)
							if err != nil {
								return err
							}
						}
					}
				}
			}
		}
	}

	return nil
}

// Params:
// lvgs - all LVMVolumeGroups in the cache;
//
// Return: map[lvgName]map[string]int64
// Example:
//
//	{
//	  "vg0": {
//	    "tp0": 100,
//	    "tp1": 200,
//	  },
//	}
//
// Description:
// This function returns a map of ThinPools free spaces for each LVMVolumeGroup.
//
// .status.thinPools[].availableSpace is the free space of the ThinPool.
func getLVGThinFreeSpaces(lvgs map[string]*snc.LVMVolumeGroup) map[string]map[string]int64 {
	result := make(map[string]map[string]int64, len(lvgs))

	for _, lvg := range lvgs {
		if result[lvg.Name] == nil {
			result[lvg.Name] = make(map[string]int64, len(lvg.Status.ThinPools))
		}

		for _, tp := range lvg.Status.ThinPools {
			result[lvg.Name][tp.Name] = tp.AvailableSpace.Value()
		}
	}

	return result
}

// Params:
// lvgs - all LVMVolumeGroups in the cache;
//
// Return: map[lvgName]int64
// Example:
//
//	{
//	  "vg0": 100,
//	  "vg1": 200,
//	}
//
// Description:
// This function returns a map of Thick free spaces for each LVMVolumeGroup.
//
// .status.VGFree is the free space of the LVMVolumeGroup.
func getLVGThickFreeSpaces(lvgs map[string]*snc.LVMVolumeGroup) map[string]int64 {
	result := make(map[string]int64, len(lvgs))

	for _, lvg := range lvgs {
		result[lvg.Name] = lvg.Status.VGFree.Value()
	}

	return result
}

// Params:
// thinPools - ThinPools of the LVMVolumeGroup;
// name - name of the ThinPool to find;
//
// Return: *snc.LVMVolumeGroupThinPoolStatus
// Example:
//
//	{
//	  "name": "tp0",
//	  "availableSpace": 100,
//	}
func findMatchedThinPool(thinPools []snc.LVMVolumeGroupThinPoolStatus, name string) *snc.LVMVolumeGroupThinPoolStatus {
	for _, tp := range thinPools {
		if tp.Name == name {
			return &tp
		}
	}

	return nil
}

// Params:
// nodeLVGs - LVMVolumeGroups on the node;
// scLVGs - LVMVolumeGroups for the Storage Class;
//
// Return: *LVMVolumeGroup
// Example:
//
//	{
//	  "name": "vg0",
//	  "status": {
//	    "nodes": ["node1", "node2"],
//	  },
//	}
func findMatchedLVG(nodeLVGs []*snc.LVMVolumeGroup, scLVGs LVMVolumeGroups) *LVMVolumeGroup {
	nodeLVGNames := make(map[string]struct{}, len(nodeLVGs))
	for _, lvg := range nodeLVGs {
		nodeLVGNames[lvg.Name] = struct{}{}
	}

	for _, lvg := range scLVGs {
		if _, match := nodeLVGNames[lvg.Name]; match {
			return &lvg
		}
	}

	return nil
}
