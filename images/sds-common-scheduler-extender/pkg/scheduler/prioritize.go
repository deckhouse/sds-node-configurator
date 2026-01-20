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
	v1 "k8s.io/api/core/v1"

	srv "github.com/deckhouse/sds-replicated-volume/api/v1alpha1"
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
	if len(replicatedPVCs) == 0 && len(localPVCs) == 0 {
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
	// b, _ := json.MarshalIndent(res, "", "  ")
	// s.log.Trace(fmt.Sprintf("[collectPrioritizeInput] PrioritizeInput: %+v", string(b)))
	return res, nil
}

// scoreNodes prioritizes nodes based on storage criteria.
func (s *scheduler) scoreNodes(input *PrioritizeInput) ([]HostPriority, error) {
	s.log.Debug("[scoreNodes] prioritizing nodes", "nodes", input.NodeNames)

	lvgInfo, err := collectLVGScoreInfo(s, input.StorageClasses)
	if err != nil {
		return nil, fmt.Errorf("unable to collect LVG info: %w", err)
	}

	return s.scoreNodesParallel(input, lvgInfo)
}

func (s *scheduler) scoreNodesParallel(input *PrioritizeInput, lvgInfo *LVGScoreInfo) ([]HostPriority, error) {
	result := make([]HostPriority, 0, len(input.NodeNames))
	resultCh := make(chan HostPriority, len(input.NodeNames))
	var wg sync.WaitGroup
	wg.Add(len(input.NodeNames))

	for _, nodeName := range input.NodeNames {
		go func(nodeName string) {
			defer wg.Done()
			score := s.scoreSingleNode(input, lvgInfo, nodeName)
			resultCh <- HostPriority{Host: nodeName, Score: score}
		}(nodeName)
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	for score := range resultCh {
		result = append(result, score)
	}

	s.log.Debug("[scoreNodes] scored nodes", "results", result)
	return result, nil
}

func (s *scheduler) scoreSingleNode(input *PrioritizeInput, lvgInfo *LVGScoreInfo, nodeName string) int {
	s.log.Debug(fmt.Sprintf("[scoreSingleNode] scoring node %s", nodeName))

	lvgsFromNode := lvgInfo.NodeToLVGs[nodeName]
	s.log.Trace(fmt.Sprintf("[scoreSingleNode] LVMVolumeGroups from node %s: %+v", nodeName, lvgsFromNode))
	var totalFreeSpaceLeftPercent int64
	nodeScore := 0
	processedPVCsCount := int64(0)

	// 1) Handle replicated PVCs: must check DRBD replica peers
	for _, pvc := range input.ReplicatedProvisionPVCs {
		s.log.Info(fmt.Sprintf("[scoreSingleNode] replicated pvc: %+v", pvc))

		pvcReq := input.PVCRequests[pvc.Name]
		s.log.Trace(fmt.Sprintf("[scoreSingleNode] pvc %s size request: %+v", pvc.Name, pvcReq))

		lvgsFromSC := lvgInfo.SCLVGs[*pvc.Spec.StorageClassName]
		s.log.Trace(fmt.Sprintf("[scoreSingleNode] LVMVolumeGroups %+v from SC: %s", lvgsFromSC, *pvc.Spec.StorageClassName))
		commonLVG := findMatchedLVGs(lvgsFromNode, lvgsFromSC)
		s.log.Trace(fmt.Sprintf("[scoreSingleNode] Common LVMVolumeGroup %+v of node %s and SC %s", commonLVG, nodeName, *pvc.Spec.StorageClassName))

		if commonLVG == nil {
			s.log.Warning(fmt.Sprintf("[scoreSingleNode] unable to match Storage Class's LVMVolumeGroup with node %s for Storage Class %s", nodeName, *pvc.Spec.StorageClassName))
			continue
		}

		nodeScore += 10
		lvg := lvgInfo.LVGs[commonLVG.Name]
		s.log.Trace(fmt.Sprintf("[scoreSingleNode] LVMVolumeGroup %s data: %+v", lvg.Name, lvg))

		freeSpace, err := calculateFreeSpace(lvg, s.cacheMgr, &pvcReq, commonLVG, s.log, pvc, nodeName)
		if err != nil {
			s.log.Error(err, fmt.Sprintf("[scoreSingleNode] unable to calculate free space for LVMVolumeGroup %s, PVC: %s, node: %s", lvg.Name, pvc.Name, nodeName))
			continue
		}
		s.log.Trace(fmt.Sprintf("[scoreSingleNode] LVMVolumeGroup %s freeSpace: %s", lvg.Name, freeSpace.String()))
		s.log.Trace(fmt.Sprintf("[scoreSingleNode] LVMVolumeGroup %s total size: %s", lvg.Name, lvg.Status.VGSize.String()))
		totalFreeSpaceLeftPercent += getFreeSpaceLeftAsPercent(freeSpace.Value(), pvcReq.RequestedSize, lvg.Status.VGSize.Value())
		processedPVCsCount++
		s.log.Trace(fmt.Sprintf("[scoreSingleNode] totalFreeSpaceLeftPercent: %d", totalFreeSpaceLeftPercent))
	}

	// 2) Handle local PVCs: no DRBD checks
	for _, pvc := range input.LocalProvisionPVCs {
		s.log.Info(fmt.Sprintf("[scoreSingleNode] local pvc: %+v", pvc))

		pvcReq := input.PVCRequests[pvc.Name]
		s.log.Trace(fmt.Sprintf("[scoreSingleNode] pvc %s size request: %+v", pvc.Name, pvcReq))

		lvgsFromSC := lvgInfo.SCLVGs[*pvc.Spec.StorageClassName]
		s.log.Trace(fmt.Sprintf("[scoreSingleNode] LVMVolumeGroups %+v from SC: %s", lvgsFromSC, *pvc.Spec.StorageClassName))
		commonLVG := findMatchedLVGs(lvgsFromNode, lvgsFromSC)
		s.log.Trace(fmt.Sprintf("[scoreSingleNode] Common LVMVolumeGroup %+v of node %s and SC %s", commonLVG, nodeName, *pvc.Spec.StorageClassName))

		if commonLVG == nil {
			s.log.Warning(fmt.Sprintf("[scoreSingleNode] unable to match Storage Class's LVMVolumeGroup with node %s for Storage Class %s", nodeName, *pvc.Spec.StorageClassName))
			continue
		}

		nodeScore += 10
		lvg := lvgInfo.LVGs[commonLVG.Name]
		s.log.Trace(fmt.Sprintf("[scoreSingleNode] LVMVolumeGroup %s data: %+v", lvg.Name, lvg))

		freeSpace, err := calculateFreeSpace(lvg, s.cacheMgr, &pvcReq, commonLVG, s.log, pvc, nodeName)
		if err != nil {
			s.log.Error(err, fmt.Sprintf("[scoreSingleNode] unable to calculate free space for LVMVolumeGroup %s, PVC: %s, node: %s", lvg.Name, pvc.Name, nodeName))
			continue
		}
		s.log.Trace(fmt.Sprintf("[scoreSingleNode] LVMVolumeGroup %s freeSpace: %s", lvg.Name, freeSpace.String()))
		s.log.Trace(fmt.Sprintf("[scoreSingleNode] LVMVolumeGroup %s total size: %s", lvg.Name, lvg.Status.VGSize.String()))
		totalFreeSpaceLeftPercent += getFreeSpaceLeftAsPercent(freeSpace.Value(), pvcReq.RequestedSize, lvg.Status.VGSize.Value())
		processedPVCsCount++
		s.log.Trace(fmt.Sprintf("[scoreSingleNode] totalFreeSpaceLeftPercent: %d", totalFreeSpaceLeftPercent))
	}

	averageFreeSpace := int64(0)
	if processedPVCsCount > 0 {
		averageFreeSpace = totalFreeSpaceLeftPercent / processedPVCsCount
	}
	s.log.Trace(fmt.Sprintf("[scoreNodes] average free space left for node %s: %d%%", nodeName, averageFreeSpace))

	nodeScore += getNodeScore(averageFreeSpace, 1/input.DefaultDivisor)
	s.log.Trace(fmt.Sprintf("[scoreNodes] node %s has score %d with average free space left %d%%", nodeName, nodeScore, averageFreeSpace))

	return nodeScore
}
