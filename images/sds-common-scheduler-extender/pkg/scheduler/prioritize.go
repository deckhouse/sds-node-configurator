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
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/cache"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/consts"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/tracing"
	srv "github.com/deckhouse/sds-replicated-volume/api/v1alpha1"
)

func (s *scheduler) prioritize(w http.ResponseWriter, r *http.Request) {
	servingLog := tracing.WithTraceIDLogger(r.Context(), s.log)

	servingLog.Debug("starts the serving the request")

	ctx, cancel := context.WithTimeout(r.Context(), 4*time.Second)
	defer cancel()

	var inputData ExtenderArgs
	reader := http.MaxBytesReader(w, r.Body, 10<<20) // 10MB
	err := json.NewDecoder(reader).Decode(&inputData)
	if err != nil {
		servingLog.Error("unable to decode a request", logger.Err(err))
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	servingLog.Trace(fmt.Sprintf("input data: %+v", inputData))

	if inputData.Pod == nil {
		servingLog.Error("unable to get a Pod from the request", logger.Err(errors.New("no pod in the request")))
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	servingLog = servingLog.With("Pod", fmt.Sprintf("%s/%s", inputData.Pod.Namespace, inputData.Pod.Name), "dryRun", inputData.DryRun)
	if inputData.DryRun {
		s.prioritizeDryRunRequestCount.Add(1)
		tracing.MarkDryRun(w)
	}

	nodeNames, err := getNodeNames(inputData)
	if err != nil {
		servingLog.Error("unable to get node names from the request", logger.Err(err))
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	servingLog.Trace(fmt.Sprintf("NodeNames from the request: %+v", nodeNames))

	managedPVCs, hintOnlyPVCs, err := getManagedPVCsFromPod(ctx, s.client, servingLog, inputData.Pod, s.targetProvisioners)
	if err != nil {
		servingLog.Error("unable to get managed PVCs from the Pod", logger.Err(err))
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	if len(managedPVCs) == 0 {
		servingLog.Debug("Pod uses PVCs which are not managed by our modules. Return the same nodes")
		if err := writeNodeScoresResponse(w, servingLog, nodeNames, 0); err != nil {
			servingLog.Error("unable to write node names response", logger.Err(err))
			http.Error(w, "internal server error", http.StatusInternalServerError)
		}
		return
	}

	scUsedByPVCs, err := getStorageClassesUsedByPVCs(ctx, s.client, managedPVCs)
	if err != nil {
		servingLog.Error("unable to get StorageClasses from the PVC", logger.Err(err))
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	// Drop managed PVCs whose StorageClass is missing or unset. See filter.go
	// for the rationale: the upstream kube-scheduler does not require the SC
	// object for bound PVCs, and refusing to score for the whole pod when one
	// PVC's SC is gone would break statically provisioned PVs and SC migrations.
	if dropped := dropPVCsWithMissingSC(managedPVCs, scUsedByPVCs); len(dropped) > 0 {
		allKeys, pendingKeys := formatDroppedPVCsForLog(dropped)
		servingLog.Warn(fmt.Sprintf("dropping PVCs without an existing StorageClass from prioritize decision: %v", allKeys))
		if len(pendingKeys) > 0 {
			servingLog.Warn(fmt.Sprintf("Pending PVCs reference a missing StorageClass and will never be dynamically provisioned (the Pod will likely get stuck in ContainerCreating with FailedMount): %v", pendingKeys))
		}
	}
	if len(managedPVCs) == 0 {
		servingLog.Debug("After filtering out PVCs with missing StorageClass, no managed PVCs left. Return the same nodes with 0 score")
		if err := writeNodeScoresResponse(w, servingLog, nodeNames, 0); err != nil {
			servingLog.Error("unable to write node scores response", logger.Err(err))
			http.Error(w, "internal server error", http.StatusInternalServerError)
		}
		return
	}

	servingLog.Debug("starts to extract PVC requested sizes")
	pvcRequests, droppedHints, err := extractRequestedSize(ctx, s.client, servingLog, managedPVCs, scUsedByPVCs, hintOnlyPVCs)
	if err != nil {
		servingLog.Error("unable to extract request size", logger.Err(err))
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	// Mirror of filter: an unresolvable annotation-only PVC is dropped rather
	// than allowed to fail the request, so scoring stops accounting for it.
	if len(droppedHints) > 0 {
		for _, name := range droppedHints {
			delete(managedPVCs, name)
		}
		servingLog.Warn(fmt.Sprintf("dropping unresolvable PVCs listed only in the %s annotation: %v", consts.PodExtraPVCsAnnotation, droppedHints))
	}
	if len(managedPVCs) == 0 {
		servingLog.Debug("After dropping unresolvable annotation PVCs, no managed PVCs left. Return the same nodes with 0 score")
		if err := writeNodeScoresResponse(w, servingLog, nodeNames, 0); err != nil {
			servingLog.Error("unable to write node scores response", logger.Err(err))
			http.Error(w, "internal server error", http.StatusInternalServerError)
		}
		return
	}

	// Mirror of filter: do not short-circuit when len(pvcRequests) == 0. Scoring
	// always runs so the response is consistent with the filter contract (the
	// filter never returns "all nodes" for a local PVC; therefore prioritize
	// must score whatever the scheduler actually passed us).
	servingLog.Debug("successfully extracted the PVC requested sizes")

	servingLog.Debug("starts to score the nodes for Pod")

	replicatedPVCs := filterPVCsByProvisioner(managedPVCs, scUsedByPVCs, consts.SdsReplicatedVolumeProvisioner)
	replicaLocations := buildReplicaLocations(ctx, servingLog, s.client, replicatedPVCs)

	scoredNodes, err := scoreNodes(ctx, servingLog, s.client, s.cache, &nodeNames, managedPVCs, scUsedByPVCs, pvcRequests, replicaLocations, s.defaultDivisor)
	if err != nil {
		servingLog.Error("unable to score nodes", logger.Err(err))
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	servingLog.Debug("successfully scored the nodes for Pod")

	// Narrow reservations to the final node list (prioritize may receive fewer nodes than filter).
	// A dry run leaves them as they are, TTL included.
	if !inputData.DryRun {
		narrowReservationsToFinalNodes(ctx, servingLog, s.client, s.cache, nodeNames, managedPVCs, scUsedByPVCs, pvcRequests)
	}

	// Log response body at DEBUG level
	responseJSON, err := json.Marshal(scoredNodes)
	if err != nil {
		servingLog.Error("unable to marshal response", logger.Err(err))
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	servingLog.Debug(fmt.Sprintf("response: %s", string(responseJSON)))

	w.Header().Set("content-type", "application/json")
	_, err = w.Write(responseJSON)
	if err != nil {
		servingLog.Error("unable to write response", logger.Err(err))
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

	log.Debug(fmt.Sprintf("local PVCs count: %d, replicated PVCs count: %d", len(localPVCs), len(replicatedPVCs)))

	// Get LVGs from StorageClasses only for local PVCs
	var scLVGs map[string]SCLVMVolumeGroups
	var err error

	if len(localPVCs) > 0 {
		log.Debug("starts to get LVMVolumeGroups for local Storage Classes")
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

	// The Pod's own local PVCs are accounted from the request, not from the cache: a real
	// filter reserves them on every candidate pool right before prioritize, a dry run
	// reserves nothing, and both must score the same. This holds for every pool, the
	// pools of an RSP included: an LVG can serve a local StorageClass and an RSP at once.
	podReservationIDs := reservationIDs(localPVCs)

	result := make([]HostPriority, 0, len(*nodeNames))
	resultMtx := &sync.Mutex{}
	wg := &sync.WaitGroup{}
	wg.Add(len(*nodeNames))
	errs := make(chan error, len(managedPVCs)*len(*nodeNames))

	for i, nodeName := range *nodeNames {
		go func(i int, nodeName string) {
			log := log

			log.Trace(fmt.Sprintf("goroutine %d starts the work for the node %s", i, nodeName))
			defer func() {
				log.Trace(fmt.Sprintf("goroutine %d ends the work for the node %s", i, nodeName))
				wg.Done()
			}()

			var totalPercent int64
			pvcCount := 0
			replicaHits := 0
			replicaTotalPVCs := 0
			// claims are the PVCs scored by the free space of their pool on this node,
			// local and replicated alike; they are scored once all of them are known.
			var claims []poolClaim

			// === Score LOCAL PVCs ===
			if len(localPVCs) > 0 {
				// Get LVGs on this node via field indexer
				nodeLVGs, err := getLVGsOnNode(ctx, cl, nodeName)
				if err != nil {
					log.Error(fmt.Sprintf("unable to get LVGs for node %s", nodeName), logger.Err(err))
				} else {
					for _, pvc := range localPVCs {
						commonLVG := findMatchedSCLVG(nodeLVGs, scLVGs[*pvc.Spec.StorageClassName])
						if commonLVG == nil {
							log.Debug(fmt.Sprintf("unable to match local LVG for SC %s on node %s, scoring 0", *pvc.Spec.StorageClassName, nodeName))
							pvcCount++
							continue
						}
						pvcReq := pvcRequests[pvc.Name]
						claims = append(claims, poolClaim{pvc: pvc.Name, key: storagePoolKeyFromSCLVG(*commonLVG, pvcReq.DeviceType), size: pvcReq.RequestedSize})
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
						log.Error(fmt.Sprintf("unable to get RSC for SC %s", sc.Name), logger.Err(err))
						pvcCount++
						continue
					}

					rspName := rscStoragePoolName(rsc)
					rsp, err := getReplicatedStoragePool(ctx, cl, rspName)
					if err != nil {
						log.Error(fmt.Sprintf("unable to get RSP %s", rspName), logger.Err(err))
						pvcCount++
						continue
					}

					volumeAccess := rsc.Spec.VolumeAccess
					if volumeAccess == "" {
						volumeAccess = srv.VolumeAccessPreferablyLocal
					}

					// Use Spec.VolumeName, not Status.Phase, to detect binding:
					// Status.Phase is updated by kube-controller-manager separately
					// and lags behind binding, so a freshly bound (or freshly created)
					// PVC may temporarily show an out-of-date phase here. See the
					// rationale on extractRequestedSize.
					//
					// For Local, PreferablyLocal and EventuallyLocal a bound PVC with a diskful
					// replica on the node counts in the replica score only and stays out of the
					// space average; the growth delta of an expanded PVC is not charged to the
					// replica node, as before. Counting it as 0% in the average made a node with
					// a replica of one of several PVCs lose to a node without replicas.
					if pvc.Spec.VolumeName != "" {
						switch volumeAccess {
						case srv.VolumeAccessLocal:
							replicaTotalPVCs++
							if nodeHasReplicaForPVC(nodeName, pvc.Name, replicaLocations) {
								replicaHits++
							} else {
								pvcCount++
							}
							continue

						case srv.VolumeAccessPreferablyLocal, srv.VolumeAccessEventuallyLocal:
							replicaTotalPVCs++
							if nodeHasReplicaForPVC(nodeName, pvc.Name, replicaLocations) {
								replicaHits++
								continue
							}

						case srv.VolumeAccessAny:
							// no replica consideration
						}
					}

					key, found := replicatedPoolKey(ctx, cl, nodeName, rsp)
					if !found {
						pvcCount++
						continue
					}
					claims = append(claims, poolClaim{pvc: pvc.Name, key: key, size: pvcReq.RequestedSize})
				}
			}

			totalPercent += scoreClaims(ctx, log, cl, schedulerCache, claims, podReservationIDs)
			pvcCount += len(claims)

			var averagePercent int64
			if pvcCount > 0 {
				averagePercent = totalPercent / int64(pvcCount)
			}
			baseScore := getNodeScore(averagePercent, divisor)
			score := baseScore
			if replicaTotalPVCs > 0 && replicaHits > 0 {
				replicaScore := 10 * replicaHits / replicaTotalPVCs
				if replicaScore > score {
					score = replicaScore
				}
			}
			log.Trace(fmt.Sprintf("node %s has final score %d (base: %d, total percent: %d, pvcs: %d, replicaHits: %d/%d)", nodeName, score, baseScore, totalPercent, pvcCount, replicaHits, replicaTotalPVCs))

			resultMtx.Lock()
			result = append(result, HostPriority{
				Host:  nodeName,
				Score: score,
			})
			resultMtx.Unlock()
		}(i, nodeName)
	}
	wg.Wait()
	log.Debug("goroutines work is done")
	if len(errs) != 0 {
		for err = range errs {
			log.Error("an error occurs while scoring the nodes", logger.Err(err))
		}
	}
	close(errs)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// scoreClaims sums, over the claims, the percent of the pool left free once the whole Pod is
// placed: the other claims of the Pod on the same pool count too. A pool whose space cannot be
// read adds 0.
func scoreClaims(
	ctx context.Context,
	log logger.Logger,
	cl client.Client,
	schedulerCache *cache.Cache,
	claims []poolClaim,
	podReservationIDs []string,
) int64 {
	podClaims := make(map[cache.StoragePoolKey]int64, len(claims))
	for _, c := range claims {
		podClaims[c.key] += c.size
	}

	// The space of a pool is the same for every claim on it.
	spaces := make(map[cache.StoragePoolKey]*LVGSpaceInfo, len(podClaims))
	var total int64
	for _, c := range claims {
		spaceInfo, read := spaces[c.key]
		if !read {
			info, err := getAvailableSpace(ctx, cl, schedulerCache, c.key, podReservationIDs...)
			if err != nil {
				log.Error(fmt.Sprintf("unable to get available space for %s", c.key.String()), logger.Err(err))
			} else {
				spaceInfo = &info
				log.Trace(fmt.Sprintf("pool %s available space: %s, total size: %s", c.key.String(), resource.NewQuantity(info.AvailableSpace, resource.BinarySI), resource.NewQuantity(info.TotalSize, resource.BinarySI)))
			}
			spaces[c.key] = spaceInfo
		}
		if spaceInfo == nil {
			continue
		}
		others := podClaims[c.key] - c.size
		total += getFreeSpaceLeftPercent(spaceInfo.AvailableSpace-others, c.size, spaceInfo.TotalSize)
	}
	return total
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
			log.Error(fmt.Sprintf("unable to extract LVGs from SC %s", sc.Name), logger.Err(err))
			continue
		}

		// Collect StoragePoolKeys from the final node list
		var keepPools []cache.StoragePoolKey
		for _, nodeName := range finalNodeNames {
			nodeLVGs, err := getLVGsOnNode(ctx, cl, nodeName)
			if err != nil {
				log.Error(fmt.Sprintf("unable to get LVGs for node %s", nodeName), logger.Err(err))
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
				log.Debug(fmt.Sprintf("narrowed reservation %s to %d pools", pvcKey, len(keepPools)))
			}
		}
	}
}
