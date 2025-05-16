package scheduler

import (
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"
)

// Filter processes the filtering logic for a given request.
func (s *scheduler) Filter(inputData ExtenderArgs) (*ExtenderFilterResult, error) {
	nodeNames, err := getNodeNames(inputData)
	if err != nil {
		return nil, fmt.Errorf("unable to get node names: %w", err)
	}

	s.log.Debug(fmt.Sprintf("[filter] filtering for Pod %s/%s", inputData.Pod.Namespace, inputData.Pod.Name))
	s.log.Trace(fmt.Sprintf("[filter] Pod: %+v, Nodes: %+v", inputData.Pod, nodeNames))

	input, err := s.collectFilterInput(inputData.Pod, nodeNames)
	if err != nil {
		return nil, err
	}

	return filterNodes(s, input)
}

// collectFilterInput gathers all necessary data for filtering.
func (s *scheduler) collectFilterInput(pod *corev1.Pod, nodeNames []string) (*FilterInput, error) {
	podRelatedPVCs, err := getPodRelatedPVCs(s.ctx, s.client, s.log, pod)
	if err != nil {
		return nil, fmt.Errorf("unable to get PVCs for Pod %s/%s: %w", pod.Name, pod.Namespace, err)
	}
	if len(podRelatedPVCs) == 0 {
		return nil, errors.New("no PVCs found for Pod")
	}

	scsUsedByPodPVCs, err := getStorageClassesUsedByPVCs(s.ctx, s.client, podRelatedPVCs)
	if err != nil {
		return nil, fmt.Errorf("unable to get StorageClasses: %w", err)
	}

	replicatedProvisionPVCs := filterPVCsByProvisioner(s.log, podRelatedPVCs, scsUsedByPodPVCs)
	if len(replicatedProvisionPVCs) == 0 {
		s.log.Warning(fmt.Sprintf("[filter] Pod %s/%s uses unmanaged PVCs", pod.Namespace, pod.Name))
		return nil, errors.New("no managed PVCs found")
	}

	pvMap, err := getPersistentVolumes(s.ctx, s.client)
	if err != nil {
		return nil, fmt.Errorf("unable to get PersistentVolumes: %w", err)
	}

	pvcSizeRequests, err := extractRequestedSize(s.log, replicatedProvisionPVCs, scsUsedByPodPVCs, pvMap)
	if err != nil {
		return nil, fmt.Errorf("unable to extract PVC request sizes: %w", err)
	}

	replicatedSCSUsedByPodPVCs, err := getRSCByCS(s.ctx, s.client, scsUsedByPodPVCs)
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
		Pod:                        pod,
		NodeNames:                  nodeNames,
		ReplicatedProvisionPVCs:    replicatedProvisionPVCs,
		SCSUsedByPodPVCs:           scsUsedByPodPVCs,
		PVCSizeRequests:            pvcSizeRequests,
		ReplicatedSCSUsedByPodPVCs: replicatedSCSUsedByPodPVCs,
		DRBDResourceMap:            drbdResourceMap,
		DRBDNodesMap:               drbdNodesMap,
	}, nil
}
