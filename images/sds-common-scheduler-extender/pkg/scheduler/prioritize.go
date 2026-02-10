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
	"sync"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/cache"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/consts"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/logger"
)

func (s *scheduler) prioritize(w http.ResponseWriter, r *http.Request) {
	servingLog := logger.WithTraceIDLogger(r.Context(), s.log).WithName("prioritize")

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

	scUsedByPVCs, err := getStorageClassesUsedByPVCs(s.ctx, s.client, managedPVCs)
	if err != nil {
		servingLog.Error(err, "unable to get StorageClasses from the PVC")
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
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
	servingLog.Debug("successfully extracted the PVC requested sizes")

	servingLog.Debug("starts to score the nodes for Pod")
	replicaLocations := make(map[string][]string) // TODO: retrieve from DRBD/Linstor

	scoredNodes, err := scoreNodes(s.ctx, servingLog, s.client, s.cache, &nodeNames, managedPVCs, scUsedByPVCs, pvcRequests, replicaLocations, s.defaultDivisor)
	if err != nil {
		servingLog.Error(err, "unable to score nodes")
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	servingLog.Debug("successfully scored the nodes for Pod")

	// Narrow reservations to the final node list (prioritize may receive fewer nodes than filter)
	narrowReservationsToFinalNodes(s.ctx, servingLog, s.client, s.cache, nodeNames, managedPVCs, scUsedByPVCs, pvcRequests)

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
	ctx context.Context,
	log logger.Logger,
	cl client.Client,
	schedulerCache *cache.Cache,
	nodeNames *[]string,
	managedPVCs map[string]*corev1.PersistentVolumeClaim,
	scUsedByPVCs map[string]*storagev1.StorageClass,
	pvcRequests map[string]PVCRequest,
	replicaLocations map[string][]string,
	divisor float64,
) ([]HostPriority, error) {
	// Separate PVCs by provisioner
	localPVCs := filterPVCsByProvisioner(managedPVCs, scUsedByPVCs, consts.SdsLocalVolumeProvisioner)
	replicatedPVCs := filterPVCsByProvisioner(managedPVCs, scUsedByPVCs, consts.SdsReplicatedVolumeProvisioner)

	log.Debug(fmt.Sprintf("[scoreNodes] local PVCs count: %d, replicated PVCs count: %d", len(localPVCs), len(replicatedPVCs)))

	// Get LVGs from StorageClasses only for local PVCs
	var scLVGs map[string]SCLVMVolumeGroups
	var err error

	if len(localPVCs) > 0 {
		log.Debug("[scoreNodes] starts to get LVMVolumeGroups for local Storage Classes")
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
	}

	result := make([]HostPriority, 0, len(*nodeNames))
	resultMtx := &sync.Mutex{}
	wg := &sync.WaitGroup{}
	wg.Add(len(*nodeNames))
	errs := make(chan error, len(managedPVCs)*len(*nodeNames))

	for i, nodeName := range *nodeNames {
		go func(i int, nodeName string) {
			log.Trace(fmt.Sprintf("[scoreNodes] goroutine %d starts the work for the node %s", i, nodeName))
			defer func() {
				log.Trace(fmt.Sprintf("[scoreNodes] goroutine %d ends the work for the node %s", i, nodeName))
				wg.Done()
			}()

			var totalScore int64
			pvcCount := 0

			// === Score LOCAL PVCs ===
			if len(localPVCs) > 0 {
				// Get LVGs on this node via field indexer
				nodeLVGs, err := getLVGsOnNode(ctx, cl, nodeName)
				if err != nil {
					log.Error(err, fmt.Sprintf("[scoreNodes] unable to get LVGs for node %s", nodeName))
				} else {
					for _, pvc := range localPVCs {
						pvcReq := pvcRequests[pvc.Name]
						lvgsFromSC := scLVGs[*pvc.Spec.StorageClassName]
						commonLVG := findMatchedSCLVG(nodeLVGs, lvgsFromSC)
						if commonLVG == nil {
							log.Debug(fmt.Sprintf("[scoreNodes] unable to match local LVG for SC %s on node %s, scoring 0", *pvc.Spec.StorageClassName, nodeName))
							pvcCount++
							continue
						}

						key := storagePoolKeyFromSCLVG(*commonLVG, pvcReq.DeviceType)
						spaceInfo, err := getAvailableSpace(ctx, cl, schedulerCache, key)
						if err != nil {
							log.Error(err, fmt.Sprintf("[scoreNodes] unable to get available space for %s", key.String()))
							pvcCount++
							continue
						}

						log.Trace(fmt.Sprintf("[scoreNodes] pool %s available space: %s, total size: %s", key.String(), resource.NewQuantity(spaceInfo.AvailableSpace, resource.BinarySI), resource.NewQuantity(spaceInfo.TotalSize, resource.BinarySI)))
						totalScore += getFreeSpaceLeftPercent(spaceInfo.AvailableSpace, pvcReq.RequestedSize, spaceInfo.TotalSize)
						pvcCount++
					}
				}
			}

			// === Score REPLICATED PVCs ===
			if len(replicatedPVCs) > 0 {
				for _, pvc := range replicatedPVCs {
					pvcReq := pvcRequests[pvc.Name]
					sc := scUsedByPVCs[*pvc.Spec.StorageClassName]

					rsc, err := getReplicatedStorageClass(ctx, cl, sc.Name)
					if err != nil {
						log.Error(err, fmt.Sprintf("[scoreNodes] unable to get RSC for SC %s", sc.Name))
						pvcCount++
						continue
					}

					rsp, err := getReplicatedStoragePool(ctx, cl, rsc.Spec.StoragePool)
					if err != nil {
						log.Error(err, fmt.Sprintf("[scoreNodes] unable to get RSP %s", rsc.Spec.StoragePool))
						pvcCount++
						continue
					}

					volumeAccess := rsc.Spec.VolumeAccess
					if volumeAccess == "" {
						volumeAccess = consts.VolumeAccessPreferablyLocal
					}

					if pvc.Status.Phase == corev1.ClaimBound &&
						volumeAccess != consts.VolumeAccessLocal &&
						volumeAccess != consts.VolumeAccessEventuallyLocal {
						hasLVGAndSpace, _ := checkNodeHasLVGWithSpaceForReplicated(ctx, log, cl, schedulerCache, nodeName, rsp, pvcReq.RequestedSize)
						if !hasLVGAndSpace {
							log.Debug(fmt.Sprintf("[scoreNodes] node %s has no LVG/space for replicated PVC %s, scoring 0", nodeName, pvc.Name))
							pvcCount++
							continue
						}
					}

					pvcScore := calculateReplicatedPVCScore(ctx, log, cl, schedulerCache, nodeName, rsp, pvcReq, divisor)
					totalScore += pvcScore
					pvcCount++
				}
			}

			// Calculate replica bonus
			replicaBonus := calculateReplicaBonus(nodeName, managedPVCs, replicaLocations)
			totalScore += replicaBonus

			var averageScore int64
			if pvcCount > 0 {
				averageScore = totalScore / int64(pvcCount)
			}
			score := getNodeScore(averageScore, divisor)
			log.Trace(fmt.Sprintf("[scoreNodes] node %s has final score %d (avg: %d, total: %d, pvcs: %d, replicaBonus: %d)", nodeName, score, averageScore, totalScore, pvcCount, replicaBonus))

			resultMtx.Lock()
			result = append(result, HostPriority{
				Host:  nodeName,
				Score: score,
			})
			resultMtx.Unlock()
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

	return result, nil
}

// narrowReservationsToFinalNodes narrows reservations for each PVC to only include
// StoragePoolKeys from the final node list. This releases reserved space on nodes
// that were filtered out by kube's own filters or other extenders between filter and prioritize.
func narrowReservationsToFinalNodes(
	ctx context.Context,
	log logger.Logger,
	cl client.Client,
	schedulerCache *cache.Cache,
	finalNodeNames []string,
	managedPVCs map[string]*corev1.PersistentVolumeClaim,
	scUsedByPVCs map[string]*storagev1.StorageClass,
	pvcRequests map[string]PVCRequest,
) {
	for _, pvc := range managedPVCs {
		sc := scUsedByPVCs[*pvc.Spec.StorageClassName]

		// Only narrow local PVCs (replicated use a different mechanism)
		if sc.Provisioner != consts.SdsLocalVolumeProvisioner {
			continue
		}

		pvcReq, exists := pvcRequests[pvc.Name]
		if !exists {
			continue
		}

		lvgsFromSC, err := ExtractLVGsFromSC(sc)
		if err != nil {
			log.Error(err, fmt.Sprintf("[narrowReservationsToFinalNodes] unable to extract LVGs from SC %s", sc.Name))
			continue
		}

		// Collect StoragePoolKeys from the final node list
		var keepPools []cache.StoragePoolKey
		for _, nodeName := range finalNodeNames {
			nodeLVGs, err := getLVGsOnNode(ctx, cl, nodeName)
			if err != nil {
				log.Error(err, fmt.Sprintf("[narrowReservationsToFinalNodes] unable to get LVGs for node %s", nodeName))
				continue
			}

			commonLVG := findMatchedSCLVG(nodeLVGs, lvgsFromSC)
			if commonLVG == nil {
				continue
			}

			key := storagePoolKeyFromSCLVG(*commonLVG, pvcReq.DeviceType)
			keepPools = append(keepPools, key)
		}

		if len(keepPools) > 0 {
			pvcKey := pvc.Namespace + "/" + pvc.Name
			ok := schedulerCache.NarrowReservation(pvcKey, keepPools, defaultReservationTTL)
			if ok {
				log.Debug(fmt.Sprintf("[narrowReservationsToFinalNodes] narrowed reservation %s to %d pools", pvcKey, len(keepPools)))
			}
		}
	}
}
