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

	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/cache"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/logger"
)

func (s *scheduler) prioritize(w http.ResponseWriter, r *http.Request) {
	servingLog := s.log.WithName("prioritize")

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
		if err := writeNodeScoresResponse(w, servingLog, nodeNames, 0); err != nil {
			servingLog.Error(err, "unable to write node names response")
			http.Error(w, "internal server error", http.StatusInternalServerError)
		}
		return
	}
	for _, pvc := range managedPVCs {
		servingLog.Trace(fmt.Sprintf("managed PVC: %s", pvc.Name))
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
		servingLog.Debug("No PVC requests found. Return the same nodes with 0 score")
		if err := writeNodeScoresResponse(w, servingLog, nodeNames, 0); err != nil {
			servingLog.Error(err, "unable to write node scores response")
			http.Error(w, "internal server error", http.StatusInternalServerError)
		}
		return
	}
	servingLog.Trace(fmt.Sprintf("PVC requests: %+v", pvcRequests))
	servingLog.Debug("successfully extracted the PVC requested sizes")

	servingLog.Debug("starts to score the nodes for Pod")
	scoredNodes, err := scoreNodes(servingLog, s.cache, &nodeNames, managedPVCs, scUsedByPVCs, pvcRequests, s.defaultDivisor)
	if err != nil {
		servingLog.Error(err, "unable to score nodes")
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	servingLog.Debug("successfully scored the nodes for Pod")

	// Log response body at DEBUG level
	responseJSON, err := json.Marshal(scoredNodes)
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

func writeNodeScoresResponse(w http.ResponseWriter, log logger.Logger, nodeNames []string, score int) error {
	scores := make([]HostPriority, 0, len(nodeNames))
	for _, nodeName := range nodeNames {
		scores = append(scores, HostPriority{
			Host:  nodeName,
			Score: score,
		})
	}
	log.Trace(fmt.Sprintf("node scores: %+v", scores))

	// Log response body at DEBUG level
	responseJSON, err := json.Marshal(scores)
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

func scoreNodes(
	log logger.Logger,
	schedulerCache *cache.Cache,
	nodeNames *[]string,
	managedPVCs map[string]*corev1.PersistentVolumeClaim,
	scUsedByPVCs map[string]*storagev1.StorageClass,
	pvcRequests map[string]PVCRequest,
	divisor float64,
) ([]HostPriority, error) {
	allLVGs := schedulerCache.GetAllLVG()
	for _, lvg := range allLVGs {
		log.Trace(fmt.Sprintf("[scoreNodes] LVMVolumeGroup %s in the cache", lvg.Name))
	}

	log.Debug("[scoreNodes] starts to get LVMVolumeGroups for Storage Classes")
	scLVGs, err := GetLVGsFromStorageClasses(scUsedByPVCs)
	if err != nil {
		return nil, err
	}
	log.Debug("[scoreNodes] successfully got LVMVolumeGroups for Storage Classes")
	for scName, lvmVolumeGroups := range scLVGs {
		for _, lvg := range lvmVolumeGroups {
			log.Trace(fmt.Sprintf("[scoreNodes] LVMVolumeGroup %s belongs to Storage Class %s", lvg.Name, scName))
		}
	}

	usedLVGs := RemoveUnusedLVGs(allLVGs, scLVGs)
	for lvgName := range usedLVGs {
		log.Trace(fmt.Sprintf("[scoreNodes] used LVMVolumeGroup %s", lvgName))
	}

	nodeLVGs := LVMVolumeGroupsByNodeName(usedLVGs)
	for n, ls := range nodeLVGs {
		for _, l := range ls {
			log.Trace(fmt.Sprintf("[scoreNodes] the LVMVolumeGroup %s belongs to node %s", l.Name, n))
		}
	}

	result := make([]HostPriority, 0, len(*nodeNames))
	wg := &sync.WaitGroup{}
	wg.Add(len(*nodeNames))
	errs := make(chan error, len(managedPVCs)*len(*nodeNames))

	for i, nodeName := range *nodeNames {
		go func(i int, nodeName string) {
			log.Trace(fmt.Sprintf("[scoreNodes] gourutine %d starts the work for the node %s", i, nodeName))
			defer func() {
				log.Trace(fmt.Sprintf("[scoreNodes] gourutine %d ends the work for the node %s", i, nodeName))
				wg.Done()
			}()

			lvgsFromNode := nodeLVGs[nodeName]
			var totalFreeSpaceLeft int64
			for _, pvc := range managedPVCs {
				pvcReq := pvcRequests[pvc.Name]
				lvgsFromSC := scLVGs[*pvc.Spec.StorageClassName]
				commonLVG := findMatchedLVG(lvgsFromNode, lvgsFromSC)
				if commonLVG == nil {
					err = fmt.Errorf("unable to match Storage Class's LVMVolumeGroup with the node's one, Storage Class: %s, node: %s", *pvc.Spec.StorageClassName, nodeName)
					errs <- err
					return
				}
				log.Trace(fmt.Sprintf("[scoreNodes] LVMVolumeGroup %s is common for storage class %s and node %s", commonLVG.Name, *pvc.Spec.StorageClassName, nodeName))

				// Use common function to get available space in LVG
				lvg := allLVGs[commonLVG.Name]
				spaceInfo, err := getLVGAvailableSpace(schedulerCache, lvg, pvcReq.DeviceType, commonLVG.Thin.PoolName)
				if err != nil {
					log.Error(err, fmt.Sprintf("[scoreNodes] unable to get available space for LVG %s", lvg.Name))
					errs <- err
					return
				}

				log.Trace(fmt.Sprintf("[scoreNodes] LVMVolumeGroup %s available space: %s, total size: %s", lvg.Name, resource.NewQuantity(spaceInfo.AvailableSpace, resource.BinarySI), resource.NewQuantity(spaceInfo.TotalSize, resource.BinarySI)))
				totalFreeSpaceLeft += getFreeSpaceLeftPercent(spaceInfo.AvailableSpace, pvcReq.RequestedSize, spaceInfo.TotalSize)
			}

			averageFreeSpace := totalFreeSpaceLeft / int64(len(managedPVCs))
			log.Trace(fmt.Sprintf("[scoreNodes] average free space left for the node: %s", nodeName))
			score := getNodeScore(averageFreeSpace, divisor)
			log.Trace(fmt.Sprintf("[scoreNodes] node %s has score %d with average free space left (after all PVC bounded), percent %d", nodeName, score, averageFreeSpace))

			result = append(result, HostPriority{
				Host:  nodeName,
				Score: score,
			})
		}(i, nodeName)
	}
	wg.Wait()
	log.Debug("[scoreNodes] goroutines work is done")
	if len(errs) != 0 {
		for err = range errs {
			log.Error(err, "[scoreNodes] an error occurs while scoring the nodes")
		}
	}
	close(errs)
	if err != nil {
		return nil, err
	}

	log.Trace("[scoreNodes] final result")
	for _, n := range result {
		log.Trace(fmt.Sprintf("[scoreNodes] host: %s", n.Host))
		log.Trace(fmt.Sprintf("[scoreNodes] score: %d", n.Score))
	}

	return result, nil
}
