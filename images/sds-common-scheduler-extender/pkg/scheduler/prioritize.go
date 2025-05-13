package scheduler

import (
	"errors"
	"fmt"

	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/consts"
	srv "github.com/deckhouse/sds-replicated-volume/api/v1alpha1"

	v1 "k8s.io/api/core/v1"
)

func (s *scheduler) Prioritize(inputData ExtenderArgs) ([]HostPriority, error) {
	nodeNames, err := getNodeNames(inputData)
	if err != nil {
		return nil, fmt.Errorf("unable to get node names: %w", err)
	}

	s.log.Debug(fmt.Sprintf("[prioritize] prioritizing for Pod %s/%s", inputData.Pod.Namespace, inputData.Pod.Name))
	s.log.Trace(fmt.Sprintf("[prioritize] Pod: %+v, Nodes: %+v", inputData.Pod, nodeNames))

	shouldProcess, _, err := shouldProcessPod(s.ctx, s.client, nil, s.log, inputData.Pod, consts.SdsReplicatedVolumeProvisioner)
	if err != nil {
		return nil, fmt.Errorf("unable to check if Pod should be processed: %w", err)
	}

	if !shouldProcess {
		s.log.Debug(fmt.Sprintf("[prioritize] Pod %s/%s should not be processed", inputData.Pod.Namespace, inputData.Pod.Name))
		scores := make([]HostPriority, 0, len(nodeNames))
		for _, nodeName := range nodeNames {
			scores = append(scores, HostPriority{
				Host:  nodeName,
				Score: 0,
			})
		}
		return scores, nil
	}

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

	managedPVCs := filterPVCsByProvisioner(s.log, pvcs, scs)
	if len(managedPVCs) == 0 {
		s.log.Warning(fmt.Sprintf("[prioritize] Pod %s/%s uses unmanaged PVCs", pod.Namespace, pod.Name))
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

	storagePoolList := &srv.ReplicatedStoragePoolList{}
	if err := s.client.List(s.ctx, storagePoolList); err != nil {
		return nil, fmt.Errorf("unable to list replicated storage pools: %w", err)
	}
	storagePoolMap := make(map[string]*srv.ReplicatedStoragePool, len(storagePoolList.Items))
	for _, storagePool := range storagePoolList.Items {
		storagePoolMap[storagePool.Name] = &storagePool
	}

	return &PrioritizeInput{
		Pod:            pod,
		NodeNames:      nodeNames,
		PVCs:           managedPVCs,
		StorageClasses: scs,
		PVCRequests:    pvcRequests,
		StoragePoolMap: storagePoolMap,
		DefaultDivisor: s.defaultDivisor,
	}, nil
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
