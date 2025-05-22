package scheduler

import (
	"encoding/json"
	"errors"
	"fmt"

	srv "github.com/deckhouse/sds-replicated-volume/api/v1alpha1"

	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
)

func (s *scheduler) Prioritize(inputData ExtenderArgs) ([]HostPriority, error) {
	nodeNames, err := getNodeNames(inputData, s.log)
	if err != nil {
		return nil, fmt.Errorf("unable to get node names: %w", err)
	}

	s.log.Debug(fmt.Sprintf("[prioritize] prioritizing for Pod %s/%s", inputData.Pod.Namespace, inputData.Pod.Name))
	s.log.Trace(fmt.Sprintf("[prioritize] Pod: %+v, Nodes: %+v", inputData.Pod, nodeNames))

	input, err := s.collectPrioritizeInput(inputData.Pod, nodeNames)
	if err != nil {
		return nil, err
	}

	return s.scoreNodes(input)
}

// collectPrioritizeInput gathers all necessary data for prioritization.
func (s *scheduler) collectPrioritizeInput(pod *v1.Pod, nodeNames []string) (*PrioritizeInput, error) {
	pvcs, err := getPodRelatedPVCs(s.ctx, s.client, s.log, pod)
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

	replicatedPVCs, localPVCs := filterPVCsByProvisioner(s.log, pvcs, scs)
	if len(replicatedPVCs) == 0 || len(localPVCs) == 0 {
		s.log.Warning(fmt.Sprintf("[filter] Pod %s/%s uses unmanaged PVCs. replicatedPVCs length %d, localPVCs length %d", pod.Namespace, pod.Name, len(replicatedPVCs), len(localPVCs)))
		return nil, errors.New("no managed PVCs found")
	}

	replicatedAndLocalPVCs := make(map[string]*corev1.PersistentVolumeClaim, len(replicatedPVCs)+len(localPVCs))
	for name, pvc := range replicatedPVCs {
		replicatedAndLocalPVCs[name] = pvc
	}
	for name, pvc := range replicatedPVCs {
		localPVCs[name] = pvc
	}

	pvMap, err := getPersistentVolumes(s.ctx, s.client, s.log)
	if err != nil {
		return nil, fmt.Errorf("unable to get PersistentVolumes: %w", err)
	}

	pvcRequests, err := extractRequestedSize(s.log, replicatedAndLocalPVCs, scs, pvMap)
	if err != nil {
		return nil, fmt.Errorf("unable to extract PVC request sizes: %w", err)
	}

	storagePoolList := &srv.ReplicatedStoragePoolList{}
	if err := s.client.List(s.ctx, storagePoolList); err != nil {
		return nil, fmt.Errorf("unable to list replicated storage pools: %w", err)
	}
	storagePoolMap := make(map[string]*srv.ReplicatedStoragePool, len(storagePoolList.Items))
	for _, storagePool := range storagePoolList.Items {
		storagePoolMap[storagePool.Name] = &storagePool
	}

	res := &PrioritizeInput{
		Pod:                     pod,
		NodeNames:               nodeNames,
		ReplicatedProvisionPVCs: replicatedPVCs,
		LocalProvisionPVCs:      localPVCs,
		StorageClasses:          scs,
		PVCRequests:             pvcRequests,
		StoragePoolMap:          storagePoolMap,
		DefaultDivisor:          s.defaultDivisor,
	}
	b, _ := json.MarshalIndent(res, "", "  ")
	s.log.Trace(fmt.Sprintf("[collectPrioritizeInput] PrioritizeInput: %+v", string(b)))
	return res, nil
}

// scoreNodes prioritizes nodes based on storage criteria.
func (s *scheduler) scoreNodes(input *PrioritizeInput) ([]HostPriority, error) {
	s.log.Debug("[scoreNodes] prioritizing nodes", "nodes", input.NodeNames)

	lvgInfo, err := collectLVGScoreInfo(s, input.StorageClasses)
	if err != nil {
		return nil, fmt.Errorf("unable to collect LVG info: %w", err)
	}

	return scoreNodesParallel(s, input, lvgInfo)
}
