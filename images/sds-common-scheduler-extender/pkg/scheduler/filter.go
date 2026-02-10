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
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/cache"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/consts"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/logger"
)

const (
	defaultReservationTTL = 60 * time.Second
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

	// Remove old reservations for these PVCs (idempotent -- AddReservation replaces anyway)
	for _, pvc := range managedPVCs {
		pvcKey := pvc.Namespace + "/" + pvc.Name
		if s.cache.HasReservation(pvcKey) {
			servingLog.Debug(fmt.Sprintf("PVC %s has existing reservation, it will be replaced", pvcKey))
		}
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
		servingLog.Debug("No PVC requests found. Return the same nodes")
		if err := writeNodeNamesResponse(w, servingLog, nodeNames); err != nil {
			servingLog.Error(err, "unable to write node names response")
			http.Error(w, "internal server error", http.StatusInternalServerError)
		}
		return
	}

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

	// Create reservations for each PVC across all filtered nodes
	servingLog.Debug("starts to create reservations")
	err = createReservations(servingLog, s.ctx, s.client, s.cache, filteredNodes.NodeNames, managedPVCs, scUsedByPVCs, pvcRequests)
	if err != nil {
		servingLog.Error(err, "unable to create reservations")
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	servingLog.Debug("successfully created reservations")

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
	// Separate PVCs by provisioner
	localPVCs := filterPVCsByProvisioner(managedPVCs, scUsedByPVCs, consts.SdsLocalVolumeProvisioner)
	replicatedPVCs := filterPVCsByProvisioner(managedPVCs, scUsedByPVCs, consts.SdsReplicatedVolumeProvisioner)

	log.Debug(fmt.Sprintf("[filterNodes] local PVCs count: %d, replicated PVCs count: %d", len(localPVCs), len(replicatedPVCs)))

	// Get LVGs from StorageClasses only for local PVCs
	var scLVGs map[string]SCLVMVolumeGroups
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
				ok, reason := filterNodeForLocalPVCs(log, ctx, cl, schedulerCache, nodeName, localPVCs, scLVGs, pvcRequests)
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

	return result, nil
}

// filterNodeForLocalPVCs filters node for local PVCs using client.Client for LVG lookups.
func filterNodeForLocalPVCs(
	log logger.Logger,
	ctx context.Context,
	cl client.Client,
	schedulerCache *cache.Cache,
	nodeName string,
	localPVCs map[string]*corev1.PersistentVolumeClaim,
	scLVGs map[string]SCLVMVolumeGroups,
	pvcRequests map[string]PVCRequest,
) (bool, string) {
	// Get all LVGs on this node via field indexer
	nodeLVGs, err := getLVGsOnNode(ctx, cl, nodeName)
	if err != nil {
		log.Error(err, fmt.Sprintf("[filterNodeForLocalPVCs] unable to get LVGs for node %s", nodeName))
		return false, fmt.Sprintf("unable to get LVGs for node %s: %v", nodeName, err)
	}

	if len(nodeLVGs) == 0 {
		log.Debug(fmt.Sprintf("[filterNodeForLocalPVCs] node %s does not have any LVMVolumeGroups", nodeName))
		return false, fmt.Sprintf("node %s does not have any LVMVolumeGroups", nodeName)
	}

	for _, pvc := range localPVCs {
		pvcReq := pvcRequests[pvc.Name]
		lvgsFromSC := scLVGs[*pvc.Spec.StorageClassName]

		// Find the matching LVG between node and SC
		commonLVG := findMatchedSCLVG(nodeLVGs, lvgsFromSC)
		if commonLVG == nil {
			return false, fmt.Sprintf("unable to match Storage Class's LVMVolumeGroup with the node's one, Storage Class: %s, node: %s", *pvc.Spec.StorageClassName, nodeName)
		}

		// Build StoragePoolKey and check space
		key := storagePoolKeyFromSCLVG(*commonLVG, pvcReq.DeviceType)
		hasSpace, err := checkPoolHasSpace(ctx, cl, schedulerCache, key, pvcReq.RequestedSize)
		if err != nil {
			log.Error(err, fmt.Sprintf("[filterNodeForLocalPVCs] unable to check space for %s", key.String()))
			return false, fmt.Sprintf("error checking space for LVG %s: %v", commonLVG.Name, err)
		}

		if !hasSpace {
			log.Trace(fmt.Sprintf("[filterNodeForLocalPVCs] %s does not have enough space for PVC %s (requested: %s)", key.String(), pvc.Name, resource.NewQuantity(pvcReq.RequestedSize, resource.BinarySI)))
			return false, fmt.Sprintf("LVMVolumeGroup %s does not have enough space for PVC %s", commonLVG.Name, pvc.Name)
		}
	}

	return true, ""
}

// createReservations creates reservations for each PVC across all filtered nodes.
func createReservations(
	log logger.Logger,
	ctx context.Context,
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
			log.Debug(fmt.Sprintf("[createReservations] PVC %s uses provisioner %s, skipping", pvc.Name, sc.Provisioner))
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
				log.Error(err, fmt.Sprintf("[createReservations] unable to get LVGs for node %s", nodeName))
				continue
			}

			commonLVG := findMatchedSCLVG(nodeLVGs, lvgsFromSC)
			if commonLVG == nil {
				continue
			}

			key := storagePoolKeyFromSCLVG(*commonLVG, pvcReq.DeviceType)
			poolKeys = append(poolKeys, key)
			log.Trace(fmt.Sprintf("[createReservations] PVC %s/%s will reserve space in %s (node %s)", pvc.Namespace, pvc.Name, key.String(), nodeName))
		}

		if len(poolKeys) > 0 {
			pvcKey := pvc.Namespace + "/" + pvc.Name
			schedulerCache.AddReservation(pvcKey, defaultReservationTTL, pvcReq.RequestedSize, poolKeys)
			log.Debug(fmt.Sprintf("[createReservations] reservation created for PVC %s: size=%d, pools=%d", pvcKey, pvcReq.RequestedSize, len(poolKeys)))
		}
	}

	return nil
}
