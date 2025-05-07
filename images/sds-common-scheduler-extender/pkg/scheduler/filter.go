package scheduler

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/consts"
	corev1 "k8s.io/api/core/v1"
)

// filter handles HTTP requests for node filtering.
func (s *scheduler) filter(w http.ResponseWriter, r *http.Request) {
	s.log.Debug("[filter] starts serving")

	var inputData ExtenderArgs
	reader := http.MaxBytesReader(w, r.Body, 10<<20)
	if err := json.NewDecoder(reader).Decode(&inputData); err != nil {
		s.log.Error(err, "[filter] unable to decode request")
		httpError(w, "unable to decode request", http.StatusBadRequest)
		return
	}

	s.log.Trace(fmt.Sprintf("[filter] input data: %+v", inputData))
	if inputData.Pod == nil {
		s.log.Error(errors.New("no pod in request"), "[filter] no pod provided")
		httpError(w, "no pod in request", http.StatusBadRequest)
		return
	}

	result, err := s.processFilterRequest(inputData)
	if err != nil {
		s.log.Error(err, "[filter] filtering failed")
		httpError(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("content-type", "application/json")
	if err := json.NewEncoder(w).Encode(result); err != nil {
		s.log.Error(err, "[filter] unable to encode response")
		httpError(w, "internal error", http.StatusInternalServerError)
		return
	}

	s.log.Debug(fmt.Sprintf("[filter] completed serving for Pod %s/%s", inputData.Pod.Namespace, inputData.Pod.Name))
}

// collectFilterInput gathers all necessary data for filtering.
func (s *scheduler) collectFilterInput(pod *corev1.Pod, nodeNames []string) (*FilterInput, error) {
	pvcs, err := getUsedPVC(s.ctx, s.client, s.log, pod)
	if err != nil {
		return nil, fmt.Errorf("unable to get PVCs for Pod %s/%s: %w", pod.Name, pod.Namespace, err)
	}
	if len(pvcs) == 0 {
		return nil, errors.New("no PVCs found for Pod")
	}

	scs, err := getStorageClassesUsedByPVCs(s.ctx, s.client, pvcs)
	if err != nil {
		return nil, fmt.Errorf("unable to get StorageClasses: %w", err)
	}

	managedPVCs := filterNotManagedPVC(s.log, pvcs, scs)
	if len(managedPVCs) == 0 {
		s.log.Warning(fmt.Sprintf("[filter] Pod %s/%s uses unmanaged PVCs", pod.Namespace, pod.Name))
		return nil, errors.New("no managed PVCs found")
	}

	pvMap, err := getPersistentVolumes(s.ctx, s.client)
	if err != nil {
		return nil, fmt.Errorf("unable to get PersistentVolumes: %w", err)
	}

	pvcRequests, err := extractRequestedSize(s.log, managedPVCs, scs, pvMap)
	if err != nil {
		return nil, fmt.Errorf("unable to extract PVC request sizes: %w", err)
	}

	replicatedSCs, err := filterOnlyReplicaredSC(s.ctx, s.client, scs)
	if err != nil {
		return nil, fmt.Errorf("unable to filter replicated StorageClasses: %w", err)
	}

	drbdResourceMap, err := getDRBDResourceMap(s.ctx, s.client)
	if err != nil {
		return nil, fmt.Errorf("unable to get DRBD resource map: %w", err)
	}

	drbdNodesMap, err := getDRBDNodesMap(s.ctx, s.client)
	if err != nil {
		return nil, fmt.Errorf("unable to get DRBD nodes map: %w", err)
	}

	return &FilterInput{
		Pod:             pod,
		NodeNames:       nodeNames,
		PVCs:            managedPVCs,
		StorageClasses:  scs,
		PVCRequests:     pvcRequests,
		ReplicatedSCs:   replicatedSCs,
		DRBDResourceMap: drbdResourceMap,
		DRBDNodesMap:    drbdNodesMap,
	}, nil
}

// processFilterRequest processes the filtering logic for a given request.
func (s *scheduler) processFilterRequest(inputData ExtenderArgs) (*ExtenderFilterResult, error) {
	nodeNames, err := getNodeNames(inputData)
	if err != nil {
		return nil, fmt.Errorf("unable to get node names: %w", err)
	}

	s.log.Debug(fmt.Sprintf("[filter] filtering for Pod %s/%s", inputData.Pod.Namespace, inputData.Pod.Name))
	s.log.Trace(fmt.Sprintf("[filter] Pod: %+v, Nodes: %+v", inputData.Pod, nodeNames))

	shouldProcess, _, err := shouldProcessPod(s.ctx, s.client, s.log, inputData.Pod, consts.SdsReplicatedVolumeProvisioner)
	if err != nil {
		return nil, fmt.Errorf("unable to check if Pod should be processed: %w", err)
	}

	if !shouldProcess {
		s.log.Debug(fmt.Sprintf("[filter] Pod %s/%s should not be processed", inputData.Pod.Namespace, inputData.Pod.Name))
		return &ExtenderFilterResult{NodeNames: &nodeNames}, nil
	}

	input, err := s.collectFilterInput(inputData.Pod, nodeNames)
	if err != nil {
		return nil, err
	}

	return filterNodes(s, input)
}

// httpError writes an HTTP error response.
func httpError(w http.ResponseWriter, msg string, statusCode int) {
	http.Error(w, msg, statusCode)
}
