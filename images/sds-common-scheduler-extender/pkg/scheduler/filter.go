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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/strings/slices"
	"sigs.k8s.io/controller-runtime/pkg/client"

	snc "github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/cache"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/consts"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/logger"
)

func (s *scheduler) filter(w http.ResponseWriter, r *http.Request) {
	servingLog := logger.WithTraceIDLogger(r.Context(), s.log).WithName("filter")

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

	managedPVCs, err := getManagedPVCsFromPod(s.ctx, s.client, servingLog, inputData.Pod, s.targetProvisioners)
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

	// Check if there are replicated PVCs that require node information
	var nodes map[string]*corev1.Node
	if hasReplicatedPVCs(managedPVCs, scUsedByPVCs) {
		servingLog.Debug("Pod has replicated PVCs, fetching node information")
		nodes, err = getNodes(s.ctx, s.client, nodeNames)
		if err != nil {
			servingLog.Error(err, "unable to get nodes")
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
	}

	servingLog.Debug("starts to filter the nodes from the request")
	filteredNodes, err := filterNodes(servingLog, s.ctx, s.client, s.cache, &nodeNames, nodes, inputData.Pod, managedPVCs, scUsedByPVCs, pvcRequests)
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

	// Log response body at DEBUG level
	responseJSON, err := json.Marshal(filteredNodes)
	if err != nil {
		servingLog.Error(err, "unable to marshal response")
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	servingLog.Debug(fmt.Sprintf("response: %s", string(responseJSON)))

	w.Header().Set("content-type", "application/json")
	_, err = w.Write(responseJSON)
	if err != nil {
		servingLog.Error(err, "unable to write response")
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

	// Log response body at DEBUG level
	responseJSON, err := json.Marshal(filteredNodes)
	if err != nil {
		return err
	}
	log.Debug(fmt.Sprintf("response: %s", string(responseJSON)))

	w.Header().Set("content-type", "application/json")
	if _, err := w.Write(responseJSON); err != nil {
		return err
	}
	return nil
}

func filterNodes(
	log logger.Logger,
	ctx context.Context,
	cl client.Client,
	schedulerCache *cache.Cache,
	nodeNames *[]string,
	nodes map[string]*corev1.Node,
	pod *corev1.Pod,
	managedPVCs map[string]*corev1.PersistentVolumeClaim,
	scUsedByPVCs map[string]*storagev1.StorageClass,
	pvcRequests map[string]PVCRequest,
) (*ExtenderFilterResult, error) {
	allLVGs := schedulerCache.GetAllLVG()
	for _, lvg := range allLVGs {
		log.Trace(fmt.Sprintf("[filterNodes] LVMVolumeGroup %s in the cache", lvg.Name))
	}

	// Separate PVCs by provisioner
	localPVCs := filterPVCsByProvisioner(managedPVCs, scUsedByPVCs, consts.SdsLocalVolumeProvisioner)
	replicatedPVCs := filterPVCsByProvisioner(managedPVCs, scUsedByPVCs, consts.SdsReplicatedVolumeProvisioner)

	log.Debug(fmt.Sprintf("[filterNodes] local PVCs count: %d, replicated PVCs count: %d", len(localPVCs), len(replicatedPVCs)))

	// Get LVGs from StorageClasses only for local PVCs
	var scLVGs map[string]LVMVolumeGroups
	var usedLVGs map[string]*snc.LVMVolumeGroup
	var nodeLVGs map[string][]*snc.LVMVolumeGroup
	var err error

	if len(localPVCs) > 0 {
		log.Debug("[filterNodes] starts to get LVMVolumeGroups for local StorageClasses")
		localSCs := make(map[string]*storagev1.StorageClass)
		for _, pvc := range localPVCs {
			if pvc.Spec.StorageClassName != nil {
				if sc, exists := scUsedByPVCs[*pvc.Spec.StorageClassName]; exists {
					localSCs[sc.Name] = sc
				}
			}
		}

		scLVGs, err = GetLVGsFromStorageClasses(localSCs)
		if err != nil {
			return nil, err
		}
		log.Debug("[filterNodes] successfully got LVMVolumeGroups for local StorageClasses")
		for scName, lvmVolumeGroups := range scLVGs {
			for _, lvg := range lvmVolumeGroups {
				log.Trace(fmt.Sprintf("[filterNodes] LVMVolumeGroup %s belongs to StorageClass %s", lvg.Name, scName))
			}
		}

		// list of LVMVolumeGroups which are used by the local PVCs
		usedLVGs = RemoveUnusedLVGs(allLVGs, scLVGs)
		for _, lvg := range usedLVGs {
			log.Trace(fmt.Sprintf("[filterNodes] the LVMVolumeGroup %s is actually used. VG size: %s, allocatedSize: %s", lvg.Name, lvg.Status.VGSize.String(), lvg.Status.AllocatedSize.String()))
		}

		// get the LVMVolumeGroups by node name
		nodeLVGs = LVMVolumeGroupsByNodeName(usedLVGs)
		for n, ls := range nodeLVGs {
			for _, l := range ls {
				log.Trace(fmt.Sprintf("[filterNodes] the LVMVolumeGroup %s belongs to node %s", l.Name, n))
			}
		}
	}

	result := &ExtenderFilterResult{
		NodeNames:   &[]string{},
		FailedNodes: FailedNodesMap{},
	}

	failedNodesMapMtx := &sync.Mutex{}
	resultNodesMtx := &sync.Mutex{}

	wg := &sync.WaitGroup{}
	wg.Add(len(*nodeNames))
	errs := make(chan error, len(*nodeNames)*len(managedPVCs))

	for i, nodeName := range *nodeNames {
		go func(i int, nodeName string) {
			log.Trace(fmt.Sprintf("[filterNodes] goroutine %d starts the work with node %s", i, nodeName))
			defer func() {
				log.Trace(fmt.Sprintf("[filterNodes] goroutine %d ends the work with node %s", i, nodeName))
				wg.Done()
			}()

			var failReasons []string

			// === Filter for LOCAL PVCs ===
			if len(localPVCs) > 0 {
				ok, reason := filterNodeForLocalPVCs(log, schedulerCache, allLVGs, nodeName, nodeLVGs, localPVCs, scLVGs, pvcRequests)
				if !ok && reason != "" {
					failReasons = append(failReasons, fmt.Sprintf("[local] %s", reason))
				}
			}

			// === Filter for REPLICATED PVCs ===
			if len(replicatedPVCs) > 0 {
				node := nodes[nodeName]
				if node == nil {
					failReasons = append(failReasons, fmt.Sprintf("[replicated] node %s not found", nodeName))
				} else {
					ok, reason := filterNodeForReplicatedPVCs(log, ctx, cl, schedulerCache, nodeName, node, replicatedPVCs, scUsedByPVCs, pvcRequests)
					if !ok && reason != "" {
						failReasons = append(failReasons, fmt.Sprintf("[replicated] %s", reason))
					}
				}
			}

			if len(failReasons) > 0 {
				failedNodesMapMtx.Lock()
				result.FailedNodes[nodeName] = strings.Join(failReasons, "; ")
				failedNodesMapMtx.Unlock()
				return
			}

			resultNodesMtx.Lock()
			*result.NodeNames = append(*result.NodeNames, nodeName)
			resultNodesMtx.Unlock()
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

// filterNodeForLocalPVCs filters node for local PVCs
func filterNodeForLocalPVCs(
	log logger.Logger,
	schedulerCache *cache.Cache,
	allLVGs map[string]*snc.LVMVolumeGroup,
	nodeName string,
	nodeLVGs map[string][]*snc.LVMVolumeGroup,
	localPVCs map[string]*corev1.PersistentVolumeClaim,
	scLVGs map[string]LVMVolumeGroups,
	pvcRequests map[string]PVCRequest,
) (bool, string) {
	// if the node does not have any LVMVolumeGroups from used StorageClasses, then this node is not suitable
	lvgsFromNode, exists := nodeLVGs[nodeName]
	if !exists {
		log.Debug(fmt.Sprintf("[filterNodeForLocalPVCs] node %s does not have any LVMVolumeGroups from used StorageClasses", nodeName))
		return false, fmt.Sprintf("node %s does not have any LVMVolumeGroups from used StorageClasses", nodeName)
	}

	// now we iterate all over the PVCs to see if we can place all of them on the node
	for _, pvc := range localPVCs {
		pvcReq := pvcRequests[pvc.Name]

		// we get LVGs which might be used by the PVC
		lvgsFromSC := scLVGs[*pvc.Spec.StorageClassName]

		// we get the specific LVG which the PVC can use on the node
		commonLVG := findMatchedLVG(lvgsFromNode, lvgsFromSC)
		if commonLVG == nil {
			return false, fmt.Sprintf("unable to match Storage Class's LVMVolumeGroup with the node's one, Storage Class: %s, node: %s", *pvc.Spec.StorageClassName, nodeName)
		}
		log.Trace(fmt.Sprintf("[filterNodeForLocalPVCs] LVMVolumeGroup %s is common for storage class %s and node %s", commonLVG.Name, *pvc.Spec.StorageClassName, nodeName))

		// Use common function to check available space in LVG
		lvg := allLVGs[commonLVG.Name]
		hasSpace, err := checkLVGHasSpace(schedulerCache, lvg, pvcReq.DeviceType, commonLVG.Thin.PoolName, pvcReq.RequestedSize)
		if err != nil {
			log.Error(err, fmt.Sprintf("[filterNodeForLocalPVCs] unable to check space for LVG %s", commonLVG.Name))
			return false, fmt.Sprintf("error checking space for LVG %s: %v", commonLVG.Name, err)
		}

		if !hasSpace {
			log.Trace(fmt.Sprintf("[filterNodeForLocalPVCs] LVMVolumeGroup %s does not have enough space for PVC %s (requested: %s)", commonLVG.Name, pvc.Name, resource.NewQuantity(pvcReq.RequestedSize, resource.BinarySI)))
			return false, fmt.Sprintf("LVMVolumeGroup %s does not have enough space for PVC %s", commonLVG.Name, pvc.Name)
		}
	}

	return true, ""
}

func populateCache(
	log logger.Logger,
	filteredNodeNames *[]string,
	pod *corev1.Pod,
	schedulerCache *cache.Cache,
	managedPVCS map[string]*corev1.PersistentVolumeClaim,
	scUsedByPVCs map[string]*storagev1.StorageClass,
) error {
	for _, nodeName := range *filteredNodeNames {
		for _, volume := range pod.Spec.Volumes {
			if volume.PersistentVolumeClaim != nil {
				pvc := managedPVCS[volume.PersistentVolumeClaim.ClaimName]
				if pvc == nil {
					continue
				}

				sc := scUsedByPVCs[*pvc.Spec.StorageClassName]

				// Only cache local PVCs, replicated PVCs use different mechanism
				if sc.Provisioner != consts.SdsLocalVolumeProvisioner {
					log.Debug(fmt.Sprintf("[populateCache] PVC %s uses provisioner %s, skipping cache population", pvc.Name, sc.Provisioner))
					continue
				}

				log.Debug(fmt.Sprintf("[populateCache] reconcile the PVC %s on node %s", volume.PersistentVolumeClaim.ClaimName, nodeName))
				lvgNamesForTheNode := schedulerCache.GetLVGNamesByNodeName(nodeName)
				log.Trace(fmt.Sprintf("[populateCache] LVMVolumeGroups from cache for the node %s: %v", nodeName, lvgNamesForTheNode))

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
