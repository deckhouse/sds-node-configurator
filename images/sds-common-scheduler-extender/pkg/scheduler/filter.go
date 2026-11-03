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
	"slices"
	"strings"
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
)

const (
	defaultReservationTTL = 60 * time.Second
)

func (s *scheduler) filter(w http.ResponseWriter, r *http.Request) {
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
		s.filterDryRunRequestCount.Add(1)
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
		writeFailAllNodesResponse(w, servingLog, nodeNames, fmt.Sprintf("unable to get managed PVCs: %s", err))
		return
	}
	if len(managedPVCs) == 0 {
		servingLog.Debug("Pod uses PVCs which are not managed by our modules. Return the same nodes")
		if err := writeNodeNamesResponse(w, servingLog, nodeNames); err != nil {
			servingLog.Error("unable to write node names response", logger.Err(err))
			http.Error(w, "internal server error", http.StatusInternalServerError)
		}
		return
	}

	scUsedByPVCs, err := getStorageClassesUsedByPVCs(ctx, s.client, managedPVCs)
	if err != nil {
		servingLog.Error("unable to get StorageClasses from the PVC", logger.Err(err))
		writeFailAllNodesResponse(w, servingLog, nodeNames, fmt.Sprintf("unable to get StorageClasses: %s", err))
		return
	}

	// Drop managed PVCs whose StorageClass is missing or unset. Such PVCs are
	// valid in Kubernetes (e.g. statically provisioned PVs, PVs that survived
	// the deletion of their StorageClass) and the upstream kube-scheduler does
	// not require the SC object for already-bound PVCs. The extender has no
	// actionable scheduling decision to make for them, so we skip these PVCs
	// instead of failing the whole pod-scheduling request.
	if dropped := dropPVCsWithMissingSC(managedPVCs, scUsedByPVCs); len(dropped) > 0 {
		allKeys, pendingKeys := formatDroppedPVCsForLog(dropped)
		servingLog.Warn(fmt.Sprintf("dropping PVCs without an existing StorageClass from scheduling decision: %v", allKeys))
		if len(pendingKeys) > 0 {
			servingLog.Warn(fmt.Sprintf("Pending PVCs reference a missing StorageClass and will never be dynamically provisioned (the Pod will likely get stuck in ContainerCreating with FailedMount): %v", pendingKeys))
		}
	}
	if len(managedPVCs) == 0 {
		servingLog.Debug("After filtering out PVCs with missing StorageClass, no managed PVCs left. Return the same nodes")
		if err := writeNodeNamesResponse(w, servingLog, nodeNames); err != nil {
			servingLog.Error("unable to write node names response", logger.Err(err))
			http.Error(w, "internal server error", http.StatusInternalServerError)
		}
		return
	}

	servingLog.Debug("starts to extract PVC requested sizes")
	pvcRequests, droppedHints, err := extractRequestedSize(ctx, s.client, servingLog, managedPVCs, scUsedByPVCs, hintOnlyPVCs)
	if err != nil {
		servingLog.Error("unable to extract request size", logger.Err(err))
		writeFailAllNodesResponse(w, servingLog, nodeNames, fmt.Sprintf("unable to extract request size: %s", err))
		return
	}

	// A PVC named only in the annotation that the extender cannot resolve is a
	// hint that failed to materialize, not a requirement. Drop it so neither the
	// node filter nor the reservation step keeps accounting for it; the Pod then
	// degrades to its pre-annotation behavior instead of losing every node.
	if len(droppedHints) > 0 {
		for _, name := range droppedHints {
			delete(managedPVCs, name)
		}
		servingLog.Warn(fmt.Sprintf("dropping unresolvable PVCs listed only in the %s annotation: %v", consts.PodExtraPVCsAnnotation, droppedHints))
	}
	if len(managedPVCs) == 0 {
		servingLog.Debug("After dropping unresolvable annotation PVCs, no managed PVCs left. Return the same nodes")
		if err := writeNodeNamesResponse(w, servingLog, nodeNames); err != nil {
			servingLog.Error("unable to write node names response", logger.Err(err))
			http.Error(w, "internal server error", http.StatusInternalServerError)
		}
		return
	}

	// NB: we intentionally do not short-circuit here when len(pvcRequests) == 0.
	// For local PVCs, returning all incoming nodes would be unsafe: kube-scheduler
	// could pick a node without a matching LVMVolumeGroup and the CSI provisioner
	// would then fail with "error during SelectLVG". The invariant we enforce is
	// that whenever managedPVCs contains at least one local PVC, the response must
	// be a subset of nodes that have an LVG matching every local PVC's StorageClass.
	// filterNodes / filterNodeForLocalPVCs enforces this even if pvcRequests is
	// somehow empty for a PVC (LVG-match check via findMatchedSCLVG always runs;
	// the space check trivially passes when RequestedSize == 0).

	// Check if there are replicated PVCs that require node information
	var nodes map[string]*corev1.Node
	if hasReplicatedPVCs(managedPVCs, scUsedByPVCs) {
		servingLog.Debug("Pod has replicated PVCs, fetching node information")
		nodes, err = getNodes(ctx, s.client, nodeNames)
		if err != nil {
			servingLog.Error("unable to get nodes", logger.Err(err))
			writeFailAllNodesResponse(w, servingLog, nodeNames, fmt.Sprintf("unable to get nodes: %s", err))
			return
		}
	}

	servingLog.Debug("starts to filter the nodes from the request")
	filteredNodes, err := filterNodes(ctx, servingLog, s.client, s.cache, &nodeNames, nodes, managedPVCs, scUsedByPVCs, pvcRequests)
	if err != nil {
		servingLog.Error("unable to filter the nodes", logger.Err(err))
		writeFailAllNodesResponse(w, servingLog, nodeNames, fmt.Sprintf("unable to filter nodes: %s", err))
		return
	}
	servingLog.Debug("successfully filtered the nodes from the request")

	// Create reservations for each PVC across all filtered nodes. A dry run
	// only reads them: a simulated copy shares the PVC keys of the real Pod, and
	// writing would replace the real Pod's reservation and reset its TTL.
	if inputData.DryRun {
		servingLog.Debug("dry run: reservations are not created")
	} else {
		servingLog.Debug("starts to create reservations")
		err = createReservations(ctx, servingLog, s.client, s.cache, filteredNodes.NodeNames, managedPVCs, scUsedByPVCs, pvcRequests)
		if err != nil {
			servingLog.Error("unable to create reservations", logger.Err(err))
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		servingLog.Debug("successfully created reservations")
	}

	// Log response body at DEBUG level
	responseJSON, err := json.Marshal(filteredNodes)
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

// writeFailAllNodesResponse returns HTTP 200 with an empty node list and all nodes in FailedNodes.
// Unlike http.Error (which returns 500 and gets silently ignored due to failurePolicy: Ignore),
// this approach makes the scheduler see "no suitable nodes" → Pod stays Pending.
func writeFailAllNodesResponse(w http.ResponseWriter, log logger.Logger, nodeNames []string, reason string) {
	failedNodes := make(FailedNodesMap, len(nodeNames))
	for _, name := range nodeNames {
		failedNodes[name] = reason
	}
	emptyList := []string{}
	result := &ExtenderFilterResult{
		NodeNames:   &emptyList,
		FailedNodes: failedNodes,
	}

	responseJSON, err := json.Marshal(result)
	if err != nil {
		log.Error("unable to marshal fail-all response, falling back to 500", logger.Err(err))
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	log.Debug(fmt.Sprintf("fail-all response: %s", string(responseJSON)))

	w.Header().Set("content-type", "application/json")
	if _, err := w.Write(responseJSON); err != nil {
		log.Error("unable to write fail-all response", logger.Err(err))
	}
}

func writeNodeNamesResponse(w http.ResponseWriter, log logger.Logger, nodeNames []string) error {
	filteredNodes := &ExtenderFilterResult{
		NodeNames: &nodeNames,
	}

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
	ctx context.Context,
	log logger.Logger,
	cl client.Client,
	schedulerCache *cache.Cache,
	nodeNames *[]string,
	nodes map[string]*corev1.Node,
	managedPVCs map[string]*corev1.PersistentVolumeClaim,
	scUsedByPVCs map[string]*storagev1.StorageClass,
	pvcRequests map[string]PVCRequest,
) (*ExtenderFilterResult, error) {
	// Separate PVCs by provisioner
	localPVCs := filterPVCsByProvisioner(managedPVCs, scUsedByPVCs, consts.SdsLocalVolumeProvisioner)
	replicatedPVCs := filterPVCsByProvisioner(managedPVCs, scUsedByPVCs, consts.SdsReplicatedVolumeProvisioner)

	log.Debug(fmt.Sprintf("local PVCs count: %d, replicated PVCs count: %d", len(localPVCs), len(replicatedPVCs)))

	// Get LVGs from StorageClasses only for local PVCs
	var scLVGs map[string]SCLVMVolumeGroups
	var err error

	if len(localPVCs) > 0 {
		log.Debug("starts to get LVMVolumeGroups for local StorageClasses")
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

	podReservationIDs := reservationIDs(localPVCs)

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
			log.Trace(fmt.Sprintf("goroutine %d starts the work with node %s", i, nodeName))
			defer func() {
				log.Trace(fmt.Sprintf("goroutine %d ends the work with node %s", i, nodeName))
				wg.Done()
			}()

			var failReasons []string
			var claims []poolClaim

			// === Filter for LOCAL PVCs ===
			if len(localPVCs) > 0 {
				ok, reason, localClaims := filterNodeForLocalPVCs(ctx, log, cl, nodeName, localPVCs, scLVGs, pvcRequests)
				if !ok && reason != "" {
					failReasons = append(failReasons, fmt.Sprintf("[local] %s", reason))
				}
				claims = append(claims, localClaims...)
			}

			// === Filter for REPLICATED PVCs ===
			if len(replicatedPVCs) > 0 {
				node := nodes[nodeName]
				if node == nil {
					failReasons = append(failReasons, fmt.Sprintf("[replicated] node %s not found", nodeName))
				} else {
					ok, reason, replicatedClaims := filterNodeForReplicatedPVCs(ctx, log, cl, nodeName, node, replicatedPVCs, scUsedByPVCs, pvcRequests)
					if !ok && reason != "" {
						failReasons = append(failReasons, fmt.Sprintf("[replicated] %s", reason))
					}
					claims = append(claims, replicatedClaims...)
				}
			}

			// === Space of the whole Pod in each pool ===
			if ok, reason := checkPodClaims(ctx, log, cl, schedulerCache, claims, podReservationIDs); !ok {
				failReasons = append(failReasons, fmt.Sprintf("[space] %s", reason))
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
	log.Debug("goroutines work is done")
	if len(errs) != 0 {
		for err = range errs {
			log.Error("an error occurs while filtering the nodes", logger.Err(err))
		}
	}
	close(errs)
	if err != nil {
		log.Error(fmt.Sprintf("unable to filter nodes for the Pod, last error: %s", err.Error()), logger.Err(err))
		return nil, err
	}

	return result, nil
}

// filterNodeForLocalPVCs filters node for local PVCs using client.Client for LVG lookups and
// returns the space the PVCs ask of the node's pools; checkPodClaims checks it.
func filterNodeForLocalPVCs(
	ctx context.Context,
	log logger.Logger,
	cl client.Client,
	nodeName string,
	localPVCs map[string]*corev1.PersistentVolumeClaim,
	scLVGs map[string]SCLVMVolumeGroups,
	pvcRequests map[string]PVCRequest,
) (bool, string, []poolClaim) {
	// Get all LVGs on this node via field indexer
	nodeLVGs, err := getLVGsOnNode(ctx, cl, nodeName)
	if err != nil {
		log.Error(fmt.Sprintf("unable to get LVGs for node %s", nodeName), logger.Err(err))
		return false, fmt.Sprintf("unable to get LVGs for node %s: %v", nodeName, err), nil
	}

	if len(nodeLVGs) == 0 {
		log.Debug(fmt.Sprintf("node %s does not have any LVMVolumeGroups", nodeName))
		return false, fmt.Sprintf("node %s does not have any LVMVolumeGroups", nodeName), nil
	}

	claims := make([]poolClaim, 0, len(localPVCs))
	for _, pvc := range localPVCs {
		// Find the matching LVG between node and SC
		commonLVG := findMatchedSCLVG(nodeLVGs, scLVGs[*pvc.Spec.StorageClassName])
		if commonLVG == nil {
			return false, fmt.Sprintf("unable to match Storage Class's LVMVolumeGroup with the node's one, Storage Class: %s, node: %s", *pvc.Spec.StorageClassName, nodeName), nil
		}
		pvcReq := pvcRequests[pvc.Name]
		// A bound PVC that does not grow takes no new space: its volume already
		// exists in this pool and is part of the allocated space the check
		// counts. Claiming it anyway rejects a node whose pool is full on paper
		// for a Pod that only mounts what is already there.
		if pvcReq.RequestedSize == 0 {
			continue
		}
		claims = append(claims, poolClaim{pvc: pvc.Name, key: storagePoolKeyFromSCLVG(*commonLVG, pvcReq.DeviceType), size: pvcReq.RequestedSize})
	}

	return true, "", claims
}

// checkPodClaims checks that every pool holds all the Pod's PVCs placed in it, local and
// replicated alike. The Pod's own local reservations are left out: the request is about to
// replace them. Every other reservation counts, a replicated PVC's own one included: it is
// made under ns/name while sds-replicated-volume places a diskful replica and lives until
// Bound or its TTL. Claims are checked in PVC name order, so the reason is stable.
func checkPodClaims(
	ctx context.Context,
	log logger.Logger,
	cl client.Client,
	schedulerCache *cache.Cache,
	claims []poolClaim,
	podReservationIDs []string,
) (bool, string) {
	claims = slices.Clone(claims)
	slices.SortFunc(claims, func(a, b poolClaim) int { return strings.Compare(a.pvc, b.pvc) })

	podClaims := make(map[cache.StoragePoolKey]int64, len(claims))
	for _, c := range claims {
		podClaims[c.key] += c.size
	}

	checked := make(map[cache.StoragePoolKey]bool, len(podClaims))
	for _, c := range claims {
		if checked[c.key] {
			continue
		}
		checked[c.key] = true

		hasSpace, err := checkPoolHasSpace(ctx, cl, schedulerCache, c.key, podClaims[c.key], podReservationIDs...)
		if err != nil {
			log.Error(fmt.Sprintf("unable to check space for %s", c.key.String()), logger.Err(err))
			return false, fmt.Sprintf("error checking space for LVG %s: %v", c.key.LVGName, err)
		}
		if hasSpace {
			continue
		}

		log.Trace(fmt.Sprintf("%s does not have enough space for the Pod's PVCs (requested: %s)", c.key.String(), resource.NewQuantity(podClaims[c.key], resource.BinarySI)))
		if podClaims[c.key] > c.size {
			return false, fmt.Sprintf("LVMVolumeGroup %s does not have enough space for PVC %s and the Pod's other PVCs in it", c.key.LVGName, c.pvc)
		}
		return false, fmt.Sprintf("LVMVolumeGroup %s does not have enough space for PVC %s", c.key.LVGName, c.pvc)
	}

	return true, ""
}

// reservationIDs returns the reservation IDs of the PVCs, ns/name each.
func reservationIDs(pvcs map[string]*corev1.PersistentVolumeClaim) []string {
	ids := make([]string, 0, len(pvcs))
	for _, pvc := range pvcs {
		ids = append(ids, pvc.Namespace+"/"+pvc.Name)
	}
	return ids
}

// createReservations creates reservations for each PVC across all filtered nodes.
//
// The reservation is deliberately spread over every surviving node and narrowed
// later — by narrowReservationsToFinalNodes in prioritize, and down to a single
// node by the PVC watcher once VolumeBinding stamps selected-node on the PVC.
// That last step never happens for a PVC known only from PodExtraPVCsAnnotation:
// the Pod does not mount it, so no binding cycle touches it and the reservation
// stays spread across all candidate nodes until the TTL expires.
func createReservations(
	ctx context.Context,
	log logger.Logger,
	cl client.Client,
	schedulerCache *cache.Cache,
	filteredNodeNames *[]string,
	managedPVCs map[string]*corev1.PersistentVolumeClaim,
	scUsedByPVCs map[string]*storagev1.StorageClass,
	pvcRequests map[string]PVCRequest,
) error {
	if filteredNodeNames == nil || len(*filteredNodeNames) == 0 {
		return nil
	}

	for _, pvc := range managedPVCs {
		sc := scUsedByPVCs[*pvc.Spec.StorageClassName]

		// Only create reservations for local PVCs; replicated PVCs use a different mechanism
		if sc.Provisioner != consts.SdsLocalVolumeProvisioner {
			log.Debug(fmt.Sprintf("PVC %s uses provisioner %s, skipping", pvc.Name, sc.Provisioner))
			continue
		}

		pvcReq, exists := pvcRequests[pvc.Name]
		if !exists {
			continue
		}

		lvgsFromSC, err := ExtractLVGsFromSC(sc)
		if err != nil {
			return err
		}

		// Collect StoragePoolKeys across all filtered nodes
		var poolKeys []cache.StoragePoolKey
		for _, nodeName := range *filteredNodeNames {
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
			poolKeys = append(poolKeys, key)
			log.Trace(fmt.Sprintf("PVC %s/%s will reserve space in %s (node %s)", pvc.Namespace, pvc.Name, key.String(), nodeName))
		}

		if len(poolKeys) > 0 {
			pvcKey := pvc.Namespace + "/" + pvc.Name
			schedulerCache.AddReservation(pvcKey, defaultReservationTTL, pvcReq.RequestedSize, poolKeys)
			log.Debug(fmt.Sprintf("reservation created for PVC %s: size=%d, pools=%d", pvcKey, pvcReq.RequestedSize, len(poolKeys)))
		}
	}

	return nil
}
