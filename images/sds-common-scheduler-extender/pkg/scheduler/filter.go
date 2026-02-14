/*
Copyright YEAR Flant JSC

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
	"errors"
	"fmt"
	"sync"

	corev1 "k8s.io/api/core/v1"

	snc "github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/logger"
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

	return s.filterNodes(input, s.log)
}

// collectFilterInput gathers all necessary data for filtering.
func (s *scheduler) collectFilterInput(pod *corev1.Pod, nodeNames []string) (*FilterInput, error) {
	podRelatedPVCs, err := getPodRelatedPVCs(s.ctx, s.client, s.log, pod)
	if err != nil {
		return nil, fmt.Errorf("unable to get PVCs for Pod %s/%s: %w", pod.Name, pod.Namespace, err)
	}
	if len(podRelatedPVCs) == 0 {
		s.log.Debug(fmt.Sprintf("[filter] no PVCs found for Pod %s/%s", pod.Namespace, pod.Name))
		// TODO: not error
		return nil, err
	}

	scsUsedByPodPVCs, err := getStorageClassesUsedByPVCs(s.ctx, s.client, podRelatedPVCs)
	if err != nil {
		return nil, fmt.Errorf("unable to get StorageClasses: %w", err)
	}

	replicatedPVCs, localPVCs := filterPVCsByProvisioner(s.log, podRelatedPVCs, scsUsedByPodPVCs)
	if len(replicatedPVCs) == 0 && len(localPVCs) == 0 {
		s.log.Warning(fmt.Sprintf("[filter] Pod %s/%s uses unmanaged PVCs. replicatedPVCs length %d, localPVCs length %d", pod.Namespace, pod.Name, len(replicatedPVCs), len(localPVCs)))
		// TODO: not error
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
	for name, pvc := range localPVCs {
		replicatedAndLocalPVCs[name] = pvc
	}

	pvcSizeRequests, err := extractRequestedSize(s.log, replicatedAndLocalPVCs, scsUsedByPodPVCs, pvMap)
	if err != nil {
		return nil, fmt.Errorf("unable to extract PVC request sizes: %w", err)
	}

	replicatedSCSUsedByPodPVCs, localSCSUsedByPodPVCs, err := getRSCByCS(s.ctx, s.client, scsUsedByPodPVCs, s.log)
	if err != nil {
		return nil, fmt.Errorf("unable to filter replicated StorageClasses: %w", err)
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
		DRBDNodesMap:               drbdNodesMap,
	}, nil
}

func (s *scheduler) filterNodes(input *FilterInput, log *logger.Logger) (*ExtenderFilterResult, error) {
	log.Debug("[filterNodes] filtering nodes", "nodes", input.NodeNames)

	lvgInfo, err := collectLVGInfo(s, input.SCSUsedByPodPVCs)
	if err != nil {
		log.Error(err, "[filterNodes] unable to collect LVG info")
		return nil, fmt.Errorf("unable to collect LVG info: %w", err)
	}

	result, err := s.filterNodesParallel(input, lvgInfo)
	if err != nil {
		log.Error(err, "[filterNodes] failed to filter nodes")
		return nil, err
	}

	log.Trace("[filterNodes]", "filtered nodes result", result)
	return result, nil
}

func (s *scheduler) filterNodesParallel(input *FilterInput, lvgInfo *LVGInfo) (*ExtenderFilterResult, error) {
	commonNodes, err := getSharedNodesByStorageClasses(input.SCSUsedByPodPVCs, lvgInfo.NodeToLVGs)
	if err != nil {
		s.log.Error(err, "[filterNodesParallel] failed to find any shared nodes")
		return nil, fmt.Errorf("unable to get common nodes: %w", err)
	}

	result := &ExtenderFilterResult{
		NodeNames:   &[]string{},
		FailedNodes: map[string]string{},
	}
	resCh := make(chan ResultWithError, len(input.NodeNames))
	var wg sync.WaitGroup
	wg.Add(len(input.NodeNames))

	for _, nodeName := range input.NodeNames {
		go func(nodeName string) {
			defer wg.Done()

			// TODO: uncomment this when we will have a way to filter out PVCs by Storage class
			//srvErr := s.filterSingleNodeSRV(nodeName, input, lvgInfo, commonNodes, s.log)
			var srvErr error
			slvErr := s.filterSingleNodeSLV(nodeName, input, lvgInfo, commonNodes, s.log)

			if srvErr == nil && slvErr == nil {
				s.log.Debug(fmt.Sprintf("[filterNodesParallel] node %s is ok to schedule a pod to", nodeName))
				resCh <- ResultWithError{NodeName: nodeName}
				return
			}
			// TODO improve this part of the code later
			//nodeErr := errors.Join(srvErr, slvErr)
			nodeErr := slvErr
			s.log.Debug(fmt.Sprintf("[filterNodesParallel] node %s is bad to schedule a pod to. Reason: %s", nodeName, nodeErr.Error()))
			resCh <- ResultWithError{NodeName: nodeName, Err: nodeErr}
		}(nodeName)
	}

	go func() {
		wg.Wait()
		close(resCh)
	}()

	for r := range resCh {
		if r.Err == nil {
			*result.NodeNames = append(*result.NodeNames, r.NodeName)
		} else {
			result.FailedNodes[r.NodeName] = r.Err.Error()
		}
	}

	s.log.Debug("[filterNodes] filtered nodes", "nodes", result.NodeNames)
	return result, nil
}

func (s *scheduler) filterSingleNodeSLV(nodeName string, filterInput *FilterInput, lvgInfo *LVGInfo, commonNodes map[string][]*snc.LVMVolumeGroup, log *logger.Logger) error {
	log.Debug("[filterSingleNodeSLV] checking node", "node", nodeName)

	nodeLvgs := commonNodes[nodeName]

	hasEnoughSpace := true
	for _, pvc := range filterInput.LocalProvisionPVCs {
		lvgsFromSC := lvgInfo.SCLVGs[*pvc.Spec.StorageClassName]
		sharedLVG := findSharedLVG(nodeLvgs, lvgsFromSC)
		lvgs := s.cacheMgr.GetAllLVG()

		hasEnoughSpace = nodeHasEnoughSpace(filterInput.PVCSizeRequests, lvgInfo.ThickFreeSpaces, lvgInfo.ThinFreeSpaces, sharedLVG, pvc, lvgs, s.log)
		if !hasEnoughSpace {
			return fmt.Errorf("[filterSingleNodeSLV] node %s has not enough space", nodeName)
		}
	}
	return nil
}

func (s *scheduler) filterSingleNodeSRV(nodeName string, filterInput *FilterInput, lvgInfo *LVGInfo, commonNodes map[string][]*snc.LVMVolumeGroup, log *logger.Logger) error {
	log.Debug("[filterSingleNodeSRV] filtering node", "node", nodeName)

	nodeLvgs := commonNodes[nodeName]
	for _, pvc := range filterInput.ReplicatedProvisionPVCs {
		log.Debug("[filterSingleNodeSRV] processing PVC", "pvc", pvc.Name, "node", nodeName)
		lvgsFromSC := lvgInfo.SCLVGs[*pvc.Spec.StorageClassName]
		pvcRSC := filterInput.ReplicatedSCSUsedByPodPVCs[*pvc.Spec.StorageClassName]
		sharedLVG := findSharedLVG(nodeLvgs, lvgsFromSC)

		lvgs := s.cacheMgr.GetAllLVG()
		hasEnoughSpace := nodeHasEnoughSpace(filterInput.PVCSizeRequests, lvgInfo.ThickFreeSpaces, lvgInfo.ThinFreeSpaces, sharedLVG, pvc, lvgs, s.log)

		switch pvcRSC.Spec.VolumeAccess {
		case "Local":
			if pvc.Spec.VolumeName == "" {
				if sharedLVG == nil {
					return fmt.Errorf("[filterSingleNodeSRV] node %s does not contain LVGs from storage class %s", nodeName, pvcRSC.Name)
				}
				if !hasEnoughSpace {
					return fmt.Errorf("[filterSingleNodeSRV] node does not have enough space in LVG %s for PVC %s/%s", sharedLVG.Name, pvc.Namespace, pvc.Name)
				}
			}

		case "EventuallyLocal":
			if pvc.Spec.VolumeName == "" {
				if sharedLVG == nil {
					return fmt.Errorf("[filterSingleNodeSRV] node %s does not contain LVGs from storage class %s", nodeName, pvcRSC.Name)
				}
				if !hasEnoughSpace {
					return fmt.Errorf("[filterSingleNodeSRV] node does not have enough space in LVG %s for PVC %s/%s", sharedLVG.Name, pvc.Namespace, pvc.Name)
				}
			} else if sharedLVG == nil || !hasEnoughSpace {
				return fmt.Errorf("[filterSingleNodeSRV] node %s does not meet EventuallyLocal criteria for PVC %s", nodeName, pvc.Name)
			}

		case "PreferablyLocal":
			if pvc.Spec.VolumeName == "" && !hasEnoughSpace {
				return fmt.Errorf("[filterSingleNodeSRV] node does not have enough space in LVG %s for PVC %s/%s", sharedLVG.Name, pvc.Namespace, pvc.Name)
			}
		}
	}

	if !isDrbdNode(nodeName, filterInput.DRBDNodesMap) {
		return fmt.Errorf("[filterSingleNodeSRV] node %s is not a DRBD node", nodeName)
	}
	if !isOkNode(nodeName) {
		return fmt.Errorf("[filterSingleNodeSRV] node %s is offline", nodeName)
	}

	log.Debug("[filterSingleNodeSRV] node is ok", "node", nodeName)
	return nil
}
