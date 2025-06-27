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

	snc "github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/consts"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/logger"
	srv2 "github.com/deckhouse/sds-replicated-volume/api/v1alpha2"
	corev1 "k8s.io/api/core/v1"
)

// Filter processes the filtering logic for a given request.
func (s *scheduler) Filter(inputData ExtenderArgs) (*ExtenderFilterResult, error) {
	nodeNames, err := getNodeNames(inputData, s.log)
	if err != nil {
		return nil, fmt.Errorf("unable to get node names: %w", err)
	}

	s.log.Debug(fmt.Sprintf("[filter] filtering Nodes: %+v for Pod %s/%s", nodeNames, inputData.Pod.Namespace, inputData.Pod.Name))

	input, err := s.collectFilterInput(inputData.Pod, nodeNames)
	if err != nil {
		return nil, err
	}

	return s.filterNodesParallel(input)
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

	pvMap, err := getPersistentVolumes(s.ctx, s.client, s.log)
	if err != nil {
		return nil, fmt.Errorf("unable to get PersistentVolumes: %w", err)
	}

	pvcSizeRequests, err := extractRequestedSize(s.log, podRelatedPVCs, scsUsedByPodPVCs, pvMap)
	if err != nil {
		return nil, fmt.Errorf("unable to extract PVC request sizes: %w", err)
	}

	replicatedSCSUsedByPodPVCs, localSCSUsedByPodPVCs, err := getRSCByCS(s.ctx, s.client, scsUsedByPodPVCs, s.log)
	if err != nil {
		return nil, fmt.Errorf("unable to filter replicated StorageClasses: %w", err)
	}

	drbdReplicaList, err := getDRBDReplicaList(s.ctx, s.client)
	if err != nil {
		return nil, fmt.Errorf("unable to list DRBD replicas: %w", err)
	}

	drbdReplicaMap := make(map[string]*srv2.DRBDResourceReplica, len(drbdReplicaList.Items))
	for _, replica := range drbdReplicaList.Items {
		drbdReplicaMap[replica.Name] = &replica
	}

	drbdNodesMap, err := getDRBDNodesMap(s.ctx, s.client, s.log)
	if err != nil {
		return nil, fmt.Errorf("unable to get DRBD nodes map: %w", err)
	}

	lvgInfo, err := collectLVGInfo(s, scsUsedByPodPVCs)
	if err != nil {
		return nil, fmt.Errorf("unable to collect LVG info: %w", err)
	}

	return &FilterInput{
		Pod:                        pod,
		NodeNames:                  nodeNames,
		PodRelatedPVCs:             podRelatedPVCs,
		SCSUsedByPodPVCs:           scsUsedByPodPVCs,
		PVCSizeRequests:            pvcSizeRequests,
		ReplicatedSCSUsedByPodPVCs: replicatedSCSUsedByPodPVCs,
		LocalSCSUsedByPodPVCs:      localSCSUsedByPodPVCs,
		DRBDNodesMap:               drbdNodesMap,
		LVGFilteringInfo:           lvgInfo,
	}, nil
}

func (s *scheduler) filterNodesParallel(input *FilterInput) (*ExtenderFilterResult, error) {
	sharedNodes, err := getSharedNodesByStorageClasses(input.SCSUsedByPodPVCs, input.LVGFilteringInfo.NodeToLVGs)
	if err != nil {
		s.log.Error(err, "[filterNodesParallel] failed to find any shared nodes")
		return nil, fmt.Errorf("unable to get shared nodes: %w", err)
	}
	cachedLVGs := s.cacheMgr.GetAllLVG()

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

			nodeIsOk := false
			for _, pvc := range input.PodRelatedPVCs {
				s.log.Debug("[filterNodesParallel] filtering node %s, pvc %s/%s", nodeName, pvc.Namespace, pvc.Name)
				sc := input.SCSUsedByPodPVCs[*pvc.Spec.StorageClassName]

				if sc.Provisioner == consts.SdsReplicatedVolumeProvisioner {
					if pvc.Spec.VolumeName != "" {
						nodeIsOk = filterNodeForBoundReplicatedVolumePVC(nodeName, pvc, input, sharedNodes, cachedLVGs, s.log)
					} else {
						nodeIsOk = filterNodeForNotBoundReplicatedVolumePVC(nodeName, pvc, input, cachedLVGs, sharedNodes, s.log)
					}
				}
				if sc.Provisioner == consts.SdsLocalVolumeProvisioner {
					if pvc.Spec.VolumeName != "" {
						nodeIsOk = filterNodeForBoundLocalVolumePVC(nodeName, pvc, input, sharedNodes, s.log)
					} else {
						nodeIsOk = filterNodeForNotBoundLocalVolumePVC(nodeName, pvc, input, sharedNodes, cachedLVGs, s.log)
					}
				}
			}

			if nodeIsOk {
				s.log.Debug(fmt.Sprintf("[filterNodesParallel] node %s is ok to schedule a pod to", nodeName))
				resCh <- ResultWithError{NodeName: nodeName}
				return
			}

			s.log.Debug(fmt.Sprintf("[filterNodesParallel] node %s is not ok to schedule a pod to", nodeName))
			// TODO make filtering methods return an error and put error text here
			resCh <- ResultWithError{NodeName: nodeName, Err: errors.New("node not ok")}
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

	s.log.Debug(fmt.Sprintf("[filterNodes] filtered nodes: %v", result.NodeNames))
	return result, nil
}

func filterNodeForBoundLocalVolumePVC(nodeName string, pvc *corev1.PersistentVolumeClaim, filterInput *FilterInput, sharedNodes map[string][]*snc.LVMVolumeGroup, log *logger.Logger) bool {
	log.Debug("[filterNodeForBoundLocalVolumePVC] filter node %s, pvc %s/%s", nodeName, pvc.Namespace, pvc.Name)
	nodeLvgs := sharedNodes[nodeName]
	lvgsFromSC := filterInput.LVGFilteringInfo.SCLVGs[*pvc.Spec.StorageClassName]
	sharedLVG := findSharedLVG(nodeLvgs, lvgsFromSC)

	if sharedLVG == nil {
		log.Info(fmt.Sprintf("[filterNodeForBoundLocalVolumePVC] node %s is not ok, it does not have any LVG from PVC's Storage Class", nodeName))
		return false
	}

	log.Info(fmt.Sprintf("[filterNodeForBoundLocalVolumePVC] node %s is ok", nodeName))
	return true
}

func filterNodeForNotBoundLocalVolumePVC(nodeName string, pvc *corev1.PersistentVolumeClaim, filterInput *FilterInput, sharedNodes map[string][]*snc.LVMVolumeGroup, cachedLVGs map[string]*snc.LVMVolumeGroup, log *logger.Logger) bool {
	log.Debug("[filterNodeForNotBoundLocalVolumePVC] filter node %s, pvc %s/%s", nodeName, pvc.Namespace, pvc.Name)
	nodeLvgs := sharedNodes[nodeName]
	lvgsFromSC := filterInput.LVGFilteringInfo.SCLVGs[*pvc.Spec.StorageClassName]
	sharedLVG := findSharedLVG(nodeLvgs, lvgsFromSC)
	hasEnoughSpace := nodeHasEnoughSpace(filterInput.PVCSizeRequests, filterInput.LVGFilteringInfo, sharedLVG, pvc, cachedLVGs, log)

	if sharedLVG == nil || !hasEnoughSpace {
		log.Info(fmt.Sprintf("[filterNodeForNotBoundLocalVolumePVC] node %s is not ok, has enough disk space: %t or does not have any LVG from PVC's Storage Class", nodeName, hasEnoughSpace))
		return false
	}

	log.Info(fmt.Sprintf("[filterNodeForNotBoundLocalVolumePVC] node %s is ok", nodeName))
	return true
}

func checkIfNodeContainsDiskfulReplica(nodeName string, drbdReplica *srv2.DRBDResourceReplica) bool {
	for peerName, peer := range drbdReplica.Spec.Peers {
		if peerName != nodeName {
			continue
		}
		if !peer.Diskless {
			return true
		}
	}
	return false
}

func filterNodeForBoundReplicatedVolumePVC(nodeName string, pvc *corev1.PersistentVolumeClaim, filterInput *FilterInput, sharedNodes map[string][]*snc.LVMVolumeGroup, cachedLVGs map[string]*snc.LVMVolumeGroup, log *logger.Logger) bool {
	log.Debug("[filterNodeForBoundReplicatedVolumePVC] filter node %s, pvc %s/%s", nodeName, pvc.Namespace, pvc.Name)
	nodeIsOk := false
	replica, found := filterInput.DRBDResourceReplicaMap[pvc.Spec.VolumeName]
	if found {

	}

	pvcRSC := filterInput.ReplicatedSCSUsedByPodPVCs[*pvc.Spec.StorageClassName]

	switch pvcRSC.Spec.VolumeAccess {
	case "Local":
		isOk := checkIfNodeContainsDiskfulReplica(nodeName, replica)
		log.Info(fmt.Sprintf("[filterNodeForBoundReplicatedVolumePVC] volume access: %s, nodeName: %s, node contains diskful replica status: %t", pvcRSC.Spec.VolumeAccess, nodeName, isOk))
		nodeIsOk = isOk
	case "EventuallyLocal":
		nodeLvgs := sharedNodes[nodeName]
		lvgsFromSC := filterInput.LVGFilteringInfo.SCLVGs[*pvc.Spec.StorageClassName]
		sharedLVG := findSharedLVG(nodeLvgs, lvgsFromSC)

		if sharedLVG == nil {
			log.Info(fmt.Sprintf("[filterNodeForBoundReplicatedVolumePVC] volume access: %s,  node %s does not contain LVGs from storage class %s", pvcRSC.Spec.VolumeAccess, nodeName, *pvc.Spec.StorageClassName))
			nodeIsOk = false
		}

		hasEnoughSpace := nodeHasEnoughSpace(filterInput.PVCSizeRequests, filterInput.LVGFilteringInfo, sharedLVG, pvc, cachedLVGs, log)
		containsDiskfulReplica := checkIfNodeContainsDiskfulReplica(nodeName, replica)
		if !hasEnoughSpace && containsDiskfulReplica {
			log.Info(fmt.Sprintf("[filterNodeForBoundReplicatedVolumePVC] node %s is not ok: volume access: %s, enough disk space: %t, contains diskful replica: %t", nodeName, pvcRSC.Spec.VolumeAccess, hasEnoughSpace, containsDiskfulReplica))
			nodeIsOk = false
		}

		nodeIsOk = true
	case "PreferablyLocal", "Any":
		_, isOk := filterInput.DRBDNodesMap[nodeName]
		log.Info(fmt.Sprintf("[filterNodeForBoundReplicatedVolumePVC] volume access: %s, node is ok status: %t", pvcRSC.Spec.VolumeAccess, isOk))
		nodeIsOk = isOk
	}

	log.Info(fmt.Sprintf("[filterNodeForBoundReplicatedVolumePVC] node filter status: %t", nodeIsOk))
	return nodeIsOk
}

func filterNodeForNotBoundReplicatedVolumePVC(nodeName string, pvc *corev1.PersistentVolumeClaim, filterInput *FilterInput, cachedLVGs map[string]*snc.LVMVolumeGroup, sharedNodes map[string][]*snc.LVMVolumeGroup, log *logger.Logger) bool {
	log.Debug("[filterNodeForNotBoundReplicatedVolumePVC] filter node %s, pvc %s/%s", nodeName, pvc.Namespace, pvc.Name)
	nodeIsOk := false
	pvcRSC := filterInput.ReplicatedSCSUsedByPodPVCs[*pvc.Spec.StorageClassName]

	switch pvcRSC.Spec.VolumeAccess {
	case "Local", "EventuallyLocal":
		nodeLvgs := sharedNodes[nodeName]
		lvgsFromSC := filterInput.LVGFilteringInfo.SCLVGs[*pvc.Spec.StorageClassName]
		sharedLVG := findSharedLVG(nodeLvgs, lvgsFromSC)
		hasEnoughSpace := nodeHasEnoughSpace(filterInput.PVCSizeRequests, filterInput.LVGFilteringInfo, sharedLVG, pvc, cachedLVGs, log)

		if sharedLVG == nil || hasEnoughSpace {
			log.Info(fmt.Sprintf("[checkRSCVolumeAccessForUnBoundPVC] volume access: %s, node is ok status: %t", pvcRSC.Spec.VolumeAccess, false))
			nodeIsOk = false
		}

	case "PreferablyLocal", "Any":
		_, ok := filterInput.DRBDNodesMap[nodeName]
		log.Info(fmt.Sprintf("[checkRSCVolumeAccessForUnBoundPVC] volume access: %s, node is ok status: %t", pvcRSC.Spec.VolumeAccess, ok))
		nodeIsOk = ok
	}

	log.Info(fmt.Sprintf("[filterNodeForNotBoundReplicatedVolumePVC] node filter status: %t", nodeIsOk))
	return nodeIsOk
}
