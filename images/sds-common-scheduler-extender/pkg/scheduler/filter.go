package scheduler

import (
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"
)

// Filter processes the filtering logic for a given request.
func (s *scheduler) Filter(inputData ExtenderArgs) (*ExtenderFilterResult, error) {
	nodeNames, err := getNodeNames(inputData, s.log)
	if err != nil {
		return nil, fmt.Errorf("unable to get node names: %w", err)
	}

	s.log.Debug(fmt.Sprintf("[filter] filtering for Pod %s/%s", inputData.Pod.Namespace, inputData.Pod.Name))
	s.log.Trace(fmt.Sprintf("[filter] Pod: %+v, Nodes: %+v", inputData.Pod, nodeNames))

	input, err := s.collectFilterInput(inputData.Pod, nodeNames)
	if err != nil {
		return nil, err
	}

	return filterNodes(s, input, s.log)
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

	replicatedPVCs, localPVCs := filterPVCsByProvisioner(s.log, podRelatedPVCs, scsUsedByPodPVCs)
	if len(replicatedPVCs) == 0 || len(localPVCs) == 0 {
		s.log.Warning(fmt.Sprintf("[filter] Pod %s/%s uses unmanaged PVCs. replicatedPVCs length %d, localPVCs length %d", pod.Namespace, pod.Name, len(replicatedPVCs), len(localPVCs)))
		return nil, errors.New("no managed PVCs found")
	}

	pvMap, err := getPersistentVolumes(s.ctx, s.client, s.log)
	if err != nil {
		return nil, fmt.Errorf("unable to get PersistentVolumes: %w", err)
	}

	replicatedAndLocalPVCs := make(map[string]*corev1.PersistentVolumeClaim, len(replicatedPVCs)+len(localPVCs))
	for name, pvc := range replicatedPVCs {
		replicatedAndLocalPVCs[name] = pvc
	}
	for name, pvc := range replicatedPVCs {
		localPVCs[name] = pvc
	}

	pvcSizeRequests, err := extractRequestedSize(s.log, replicatedAndLocalPVCs, scsUsedByPodPVCs, pvMap)
	if err != nil {
		return nil, fmt.Errorf("unable to extract PVC request sizes: %w", err)
	}

	replicatedSCSUsedByPodPVCs, localSCSUsedByPodPVCs, err := getRSCByCS(s.ctx, s.client, scsUsedByPodPVCs, s.log)
	if err != nil {
		return nil, fmt.Errorf("unable to filter replicated StorageClasses: %w", err)
	}

	drbdResourceMap, err := getDRBDResourceMap(s.ctx, s.client)
	if err != nil {
		return nil, fmt.Errorf("unable to get DRBD resource map: %w", err)
	}

	drbdNodesMap, err := getDRBDNodesMap(s.ctx, s.client, s.log)
	if err != nil {
		return nil, fmt.Errorf("unable to get DRBD nodes map: %w", err)
	}

	return &FilterInput{
		Pod:                        pod,
		NodeNames:                  nodeNames,
		ReplicatedProvisionPVCs:    replicatedPVCs,
		LocalProvisionPVCs:         localPVCs,
		SCSUsedByPodPVCs:           scsUsedByPodPVCs,
		PVCSizeRequests:            pvcSizeRequests,
		ReplicatedSCSUsedByPodPVCs: replicatedSCSUsedByPodPVCs,
		LocalSCSUsedByPodPVCs:      localSCSUsedByPodPVCs,
		DRBDResourceMap:            drbdResourceMap,
		DRBDNodesMap:               drbdNodesMap,
	}, nil
}
