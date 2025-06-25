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
	"errors"
	"fmt"
	"sync"

	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/cache"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/consts"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/logger"
	srv "github.com/deckhouse/sds-replicated-volume/api/v1alpha1"
	srv2 "github.com/deckhouse/sds-replicated-volume/api/v1alpha2"

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

	return s.scoreNodesParallel(input)
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

	scsUsedByPodPVCs, err := getStorageClassesUsedByPVCs(s.ctx, s.client, pvcs)
	if err != nil {
		return nil, fmt.Errorf("unable to get StorageClasses: %w", err)
	}

	replicatedPVCs, localPVCs := filterPVCsByProvisioner(s.log, pvcs, scsUsedByPodPVCs)
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

	pvcRequests, err := extractRequestedSize(s.log, replicatedAndLocalPVCs, scsUsedByPodPVCs, pvMap)
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

	drbdReplicaList, err := getDRBDReplicaList(s.ctx, s.client)
	if err != nil {
		return nil, fmt.Errorf("unable to list DRBD replicas: %w", err)
	}

	drbdReplicaMap := make(map[string]*srv2.DRBDResourceReplica, len(drbdReplicaList.Items))
	for _, replica := range drbdReplicaList.Items {
		drbdReplicaMap[replica.Name] = &replica
	}

	lvgInfo, err := collectLVGScoreInfo(s, scsUsedByPodPVCs)
	if err != nil {
		return nil, fmt.Errorf("unable to collect LVG info: %w", err)
	}

	replicatedSCSUsedByPodPVCs, localSCSUsedByPodPVCs, err := getRSCByCS(s.ctx, s.client, scsUsedByPodPVCs, s.log)
	if err != nil {
		return nil, fmt.Errorf("unable to filter replicated StorageClasses: %w", err)
	}

	res := &PrioritizeInput{
		Pod:                        pod,
		NodeNames:                  nodeNames,
		ReplicatedSCSUsedByPodPVCs: replicatedSCSUsedByPodPVCs,
		LocalSCSUsedByPodPVCs:      localSCSUsedByPodPVCs,
		ReplicatedProvisionPVCs:    replicatedPVCs,
		ReplicatedAndLocalPVC:      replicatedAndLocalPVCs,
		LVGScoringInfo:             lvgInfo,
		LocalProvisionPVCs:         localPVCs,
		SCSUsedByPodPVCs:           scsUsedByPodPVCs,
		PVCRequests:                pvcRequests,
		StoragePoolMap:             storagePoolMap,
		DefaultDivisor:             s.defaultDivisor,
		DRBDResourceReplicaMap:     drbdReplicaMap,
	}

	return res, nil
}

func scoreNodeForNotBoundLocalVolumePVC(nodeName string, input *PrioritizeInput, pvc *corev1.PersistentVolumeClaim, schedulerCache *cache.CacheManager, log *logger.Logger) int {
	score := 0
	lvgsFromNode := input.LVGScoringInfo.NodeToLVGs[nodeName]
	lvgsFromSC := input.LVGScoringInfo.SCLVGs[*pvc.Spec.StorageClassName]

	sharedLVG := findMatchedLVGs(lvgsFromNode, lvgsFromSC)

	lvg := input.LVGScoringInfo.LVGs[sharedLVG.Name]
	pvcReq := input.PVCRequests[pvc.Name]
	freeSpace, err := calculateFreeSpace(lvg, schedulerCache, &pvcReq, sharedLVG, log, pvc, nodeName)
	if err != nil {
		log.Error(err, fmt.Sprintf("[scoreNodeForNotBoundReplicatedVolumePVC] unable to calculate free space for LVMVolumeGroup %s, PVC: %s, node: %s", lvg.Name, pvc.Name, nodeName))
		return score
	}

	totalFreeSpaceLeftPercent := getFreeSpaceLeftAsPercent(freeSpace.Value(), pvcReq.RequestedSize, lvg.Status.VGSize.Value())

	averageFreeSpace := int64(0)
	if len(input.ReplicatedAndLocalPVC) > 0 {
		averageFreeSpace = totalFreeSpaceLeftPercent / int64(len(input.ReplicatedAndLocalPVC))
	}
	score += getNodeScore(averageFreeSpace, 1/input.DefaultDivisor)

	return score
}

func scoreNodeForNotBoundReplicatedVolumePVC(nodeName string, input *PrioritizeInput, pvc *corev1.PersistentVolumeClaim, schedulerCache *cache.CacheManager, log *logger.Logger) int {
	score := 0
	pvcRSC := input.ReplicatedSCSUsedByPodPVCs[*pvc.Spec.StorageClassName]
	lvgsFromNode := input.LVGScoringInfo.NodeToLVGs[nodeName]
	lvgsFromSC := input.LVGScoringInfo.SCLVGs[*pvc.Spec.StorageClassName]

	sharedLVG := findMatchedLVGs(lvgsFromNode, lvgsFromSC)

	switch pvcRSC.Spec.VolumeAccess {
	case "Local", "EventuallyLocal", "PreferablyLocal", "Any":
		lvg := input.LVGScoringInfo.LVGs[sharedLVG.Name]
		pvcReq := input.PVCRequests[pvc.Name]
		freeSpace, err := calculateFreeSpace(lvg, schedulerCache, &pvcReq, sharedLVG, log, pvc, nodeName)
		if err != nil {
			log.Error(err, fmt.Sprintf("[scoreNodeForNotBoundReplicatedVolumePVC] unable to calculate free space for LVMVolumeGroup %s, PVC: %s, node: %s", lvg.Name, pvc.Name, nodeName))
			return score
		}

		totalFreeSpaceLeftPercent := getFreeSpaceLeftAsPercent(freeSpace.Value(), pvcReq.RequestedSize, lvg.Status.VGSize.Value())

		averageFreeSpace := int64(0)
		if len(input.ReplicatedAndLocalPVC) > 0 {
			averageFreeSpace = totalFreeSpaceLeftPercent / int64(len(input.ReplicatedAndLocalPVC))
		}
		score += getNodeScore(averageFreeSpace, 1/input.DefaultDivisor)
	}

	return score
}

func scoreNodeForBoundReplicatedVolumePVC(nodeName string, input *PrioritizeInput, pvc *corev1.PersistentVolumeClaim, schedulerCache *cache.CacheManager, log *logger.Logger) int {
	score := 0
	pvcRSC := input.ReplicatedSCSUsedByPodPVCs[*pvc.Spec.StorageClassName]
	lvgsFromNode := input.LVGScoringInfo.NodeToLVGs[nodeName]
	lvgsFromSC := input.LVGScoringInfo.SCLVGs[*pvc.Spec.StorageClassName]

	sharedLVG := findMatchedLVGs(lvgsFromNode, lvgsFromSC)

	if sharedLVG != nil {
		log.Info(fmt.Sprintf("[scoreNodeForBoundReplicatedVolumePVC] node %s contains a volume replica and gets +100 score points", nodeName))
		score += 100
	}

	switch pvcRSC.Spec.VolumeAccess {
	case "Local", "EventuallyLocal", "PreferablyLocal", "Any":
		lvg := input.LVGScoringInfo.LVGs[sharedLVG.Name]
		pvcReq := input.PVCRequests[pvc.Name]
		freeSpace, err := calculateFreeSpace(lvg, schedulerCache, &pvcReq, sharedLVG, log, pvc, nodeName)
		if err != nil {
			log.Error(err, fmt.Sprintf("[scoreNodeForBoundReplicatedVolumePVC] unable to calculate free space for LVMVolumeGroup %s, PVC: %s, node: %s", lvg.Name, pvc.Name, nodeName))
			return score
		}

		totalFreeSpaceLeftPercent := getFreeSpaceLeftAsPercent(freeSpace.Value(), pvcReq.RequestedSize, lvg.Status.VGSize.Value())

		averageFreeSpace := int64(0)
		if len(input.ReplicatedAndLocalPVC) > 0 {
			averageFreeSpace = totalFreeSpaceLeftPercent / int64(len(input.ReplicatedAndLocalPVC))
		}
		score += getNodeScore(averageFreeSpace, 1/input.DefaultDivisor)
	}

	return score
}

func (s *scheduler) scoreNodesParallel(input *PrioritizeInput) ([]HostPriority, error) {
	result := make([]HostPriority, 0, len(input.NodeNames))
	resultCh := make(chan HostPriority, len(input.NodeNames))
	var wg sync.WaitGroup
	wg.Add(len(input.NodeNames))

	for _, nodeName := range input.NodeNames {
		go func(nodeName string) {
			defer wg.Done()
			score := s.scoreSingleNode(input, nodeName)
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

func (s *scheduler) scoreSingleNode(input *PrioritizeInput, nodeName string) int {
	s.log.Debug(fmt.Sprintf("[scoreSingleNode] scoring node %s", nodeName))

	score := 0
	for _, pvc := range input.ReplicatedAndLocalPVC {
		sc := input.SCSUsedByPodPVCs[*pvc.Spec.StorageClassName]

		if sc.Provisioner == consts.SdsReplicatedVolumeProvisioner {
			if pvc.Spec.VolumeName != "" {
				score = scoreNodeForBoundReplicatedVolumePVC(nodeName, input, pvc, s.cacheMgr, s.log)
			} else {
				score = scoreNodeForNotBoundReplicatedVolumePVC(nodeName, input, pvc, s.cacheMgr, s.log)
			}
		}

		if sc.Provisioner == consts.SdsLocalVolumeProvisioner {
			if pvc.Spec.VolumeName != "" {

			} else {
				score = scoreNodeForNotBoundLocalVolumePVC(nodeName, input, pvc, s.cacheMgr, s.log)
			}
		}
	}

	s.log.Info(fmt.Sprintf("[scoreSingleNode] node %s score %d", nodeName, score))
	return score
}
