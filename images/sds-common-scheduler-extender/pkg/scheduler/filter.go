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

	s.log.Debug(fmt.Sprintf("[filter] filtering for Pod %s/%s", inputData.Pod.Namespace, inputData.Pod.Name))
	s.log.Trace(fmt.Sprintf("[filter] Pod: %+v, Nodes: %+v", inputData.Pod, nodeNames))

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

	replicatedPVCs, localPVCs := filterPVCsByProvisioner(s.log, podRelatedPVCs, scsUsedByPodPVCs)
	if len(replicatedPVCs) == 0 && len(localPVCs) == 0 {
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
		ReplicatedProvisionPVCs:    replicatedPVCs,
		LocalProvisionPVCs:         localPVCs,
		ReplicatedAndLocalPVC:      replicatedAndLocalPVCs,
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
		return nil, fmt.Errorf("unable to get common nodes: %w", err)
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
			for _, pvc := range input.ReplicatedAndLocalPVC {
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
			// TODO specify error text
			resCh <- ResultWithError{NodeName: nodeName, Err: errors.New("node not ok")}

			// for _, pvc := range input.ReplicatedProvisionPVCs {
			// 	nodeIsOk := false
			// 	if pvc.Spec.VolumeName != "" {
			// 		nodeIsOk = checkRSCVolumeAccessForBoundPVC(nodeName, pvc, input, sharedNodes, cachedLVGs, s.log)
			// 	} else {
			// 		nodeIsOk = checkRSCVolumeAccessForUnBoundPVC(nodeName, pvc, input, cachedLVGs, sharedNodes, s.log)
			// 	}

			// 	if !nodeIsOk {
			// 		s.log.Debug(fmt.Sprintf("[filterNodesParallel] node %s is ok to schedule a pod to", nodeName))
			// 		// TODO specify error text
			// 		resCh <- ResultWithError{NodeName: nodeName, Err: errors.New("node not ok")}
			// 		return
			// 	}
			// }

			// srvErr := s.filterSingleNodeSRV(nodeName, input, input.LVGInfo, sharedNodes, s.log)
			// slvErr := s.filterSingleNodeSLV(nodeName, input, input.LVGInfo, sharedNodes, s.log)

			// if srvErr == nil && slvErr == nil {
			// 	s.log.Debug(fmt.Sprintf("[filterNodesParallel] node %s is ok to schedule a pod to", nodeName))
			// 	resCh <- ResultWithError{NodeName: nodeName}
			// 	return
			// }
			// // TODO improve this part of the code later
			// errMessages := []string{srvErr.Error(), slvErr.Error()}
			// nodeErr := fmt.Errorf(strings.Join(errMessages, ", "))
			// s.log.Debug(fmt.Sprintf("[filterNodesParallel] node %s is bad to schedule a pod to. Reason: %s", nodeName, nodeErr.Error()))
			// resCh <- ResultWithError{NodeName: nodeName, Err: nodeErr}
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

func filterNodeForBoundLocalVolumePVC(nodeName string, pvc *corev1.PersistentVolumeClaim, filterInput *FilterInput, sharedNodes map[string][]*snc.LVMVolumeGroup, log *logger.Logger) bool {
	nodeLvgs := sharedNodes[nodeName]
	lvgsFromSC := filterInput.LVGFilteringInfo.SCLVGs[*pvc.Spec.StorageClassName]
	sharedLVG := findSharedLVG(nodeLvgs, lvgsFromSC)

	if sharedLVG == nil {
		log.Info("[filterNodeForBoundLocalVolumePVC] node %s is not ok, it does not have any LVG from PVC's Storage Class", nodeName)
		return false
	}

	log.Info("[filterNodeForBoundLocalVolumePVC] node %s is ok", nodeName)
	return true
}

func filterNodeForNotBoundLocalVolumePVC(nodeName string, pvc *corev1.PersistentVolumeClaim, filterInput *FilterInput, sharedNodes map[string][]*snc.LVMVolumeGroup, cachedLVGs map[string]*snc.LVMVolumeGroup, log *logger.Logger) bool {
	nodeLvgs := sharedNodes[nodeName]
	lvgsFromSC := filterInput.LVGFilteringInfo.SCLVGs[*pvc.Spec.StorageClassName]
	sharedLVG := findSharedLVG(nodeLvgs, lvgsFromSC)
	hasEnoughSpace := nodeHasEnoughSpace(filterInput.PVCSizeRequests, filterInput.LVGFilteringInfo, sharedLVG, pvc, cachedLVGs, log)

	if sharedLVG == nil || !hasEnoughSpace {
		log.Info("[filterNodeForNotBoundLocalVolumePVC] node %s is not ok, has enough disk space: %b or does not have any LVG from PVC's Storage Class", nodeName, hasEnoughSpace)
		return false
	}

	return true
}

// func (s *scheduler) filterSingleNodeSLV(nodeName string, filterInput *FilterInput, lvgInfo *LVGInfo, sharedNodes map[string][]*snc.LVMVolumeGroup, log *logger.Logger) bool {
// 	log.Debug("[filterSingleNodeSLV] filtering node", "node", nodeName)

// 	nodeLvgs := sharedNodes[nodeName]

// 	hasEnoughSpace := true
// 	for _, pvc := range filterInput.LocalProvisionPVCs {
// 		lvgsFromSC := lvgInfo.SCLVGs[*pvc.Spec.StorageClassName]
// 		sharedLVG := findSharedLVG(nodeLvgs, lvgsFromSC)
// 		lvgs := s.cacheMgr.GetAllLVG()
// 		hasEnoughSpace = nodeHasEnoughSpace(filterInput.PVCSizeRequests, lvgInfo, sharedLVG, pvc, lvgs, s.log)

// 		if pvc.Spec.VolumeName == "" {
// 			if sharedLVG == nil {
// 				return fmt.Errorf("[filterSingleNodeSLV] node %s does not contain LVGs from storage class", nodeName)
// 			}
// 			if !hasEnoughSpace {
// 				return fmt.Errorf("[filterSingleNodeSLV] node does not have enough space in LVG %s for PVC %s/%s", nodeName, pvc.Namespace, pvc.Name)
// 			}
// 			continue
// 		} else {
// 			if sharedLVG == nil {
// 				return fmt.Errorf("[filterSingleNodeSLV] node %s does not contain LVGs from storage class", nodeName)
// 			}
// 			continue
// 		}
// 	}
// 	return nil
// }

// func checkBoundPVC(nodeName string, pvc *corev1.PersistentVolumeClaim, filterInput *FilterInput) error {
// 	replica, found := filterInput.DRBDResourceReplicaMap[pvc.Spec.VolumeName]
// 	if !found {
// 		return fmt.Errorf("[filterSingleNodeSRV] pvc %s/%s does not have any diskful replicas", pvc.Namespace, pvc.Name)
// 	}

// 	hasDiskfulVolumes := false
// 	for _, peer := range replica.Spec.Peers {
// 		if peer.Diskless {
// 			continue
// 		}
// 		hasDiskfulVolumes = true
// 		break
// 	}
// 	if !hasDiskfulVolumes {
// 		return fmt.Errorf("[filterSingleNodeSRV] pvc %s/%s does not have any diskful replicas", pvc.Namespace, pvc.Name)
// 	}

// 	return nil
// }

func checkIfNodeContainsDiskfulReplica(nodeName string, drbdReplica *srv2.DRBDResourceReplica) bool {
	for _, peer := range drbdReplica.Spec.Peers {
		if peer.NodeName != nodeName {
			continue
		}
		if !peer.Diskless {
			return true
		}
	}
	return false
}

func filterNodeForBoundReplicatedVolumePVC(nodeName string, pvc *corev1.PersistentVolumeClaim, filterInput *FilterInput, sharedNodes map[string][]*snc.LVMVolumeGroup, cachedLVGs map[string]*snc.LVMVolumeGroup, log *logger.Logger) bool {
	nodeIsOk := false
	replica, found := filterInput.DRBDResourceReplicaMap[pvc.Spec.VolumeName]
	if found {

	}

	pvcRSC := filterInput.ReplicatedSCSUsedByPodPVCs[*pvc.Spec.StorageClassName]

	switch pvcRSC.Spec.VolumeAccess {
	case "Local":
		isOk := checkIfNodeContainsDiskfulReplica(nodeName, replica)
		log.Info("[filterNodeForBoundReplicatedVolumePVC] volume access: %s, nodeName: %s, node contains diskful replica status: %b", pvcRSC.Spec.VolumeAccess, nodeName, isOk)
		nodeIsOk = isOk
	case "EventuallyLocal":
		nodeLvgs := sharedNodes[nodeName]
		lvgsFromSC := filterInput.LVGFilteringInfo.SCLVGs[*pvc.Spec.StorageClassName]
		sharedLVG := findSharedLVG(nodeLvgs, lvgsFromSC)

		if sharedLVG == nil {
			log.Info("[filterNodeForBoundReplicatedVolumePVC] volume access: %s,  node %s does not contain LVGs from storage class %s", pvcRSC.Spec.VolumeAccess, nodeName, *pvc.Spec.StorageClassName)
			nodeIsOk = false
		}

		hasEnoughSpace := nodeHasEnoughSpace(filterInput.PVCSizeRequests, filterInput.LVGFilteringInfo, sharedLVG, pvc, cachedLVGs, log)
		containsDiskfulReplica := checkIfNodeContainsDiskfulReplica(nodeName, replica)
		if !hasEnoughSpace && containsDiskfulReplica {
			log.Info("[filterNodeForBoundReplicatedVolumePVC] node %s is not ok: volume access: %s, enough disk space: %b, contains diskful replica: %b", nodeName, pvcRSC.Spec.VolumeAccess, hasEnoughSpace, containsDiskfulReplica)
			nodeIsOk = false
		}

		nodeIsOk = true
	case "PreferablyLocal", "Any":
		_, isOk := filterInput.DRBDNodesMap[nodeName]
		log.Info("[filterNodeForBoundReplicatedVolumePVC] volume access: %s, node is ok status: %b", pvcRSC.Spec.VolumeAccess, isOk)
		nodeIsOk = isOk
	}

	return nodeIsOk
}

func filterNodeForNotBoundReplicatedVolumePVC(nodeName string, pvc *corev1.PersistentVolumeClaim, filterInput *FilterInput, cachedLVGs map[string]*snc.LVMVolumeGroup, sharedNodes map[string][]*snc.LVMVolumeGroup, log *logger.Logger) bool {
	nodeIsOk := false
	pvcRSC := filterInput.ReplicatedSCSUsedByPodPVCs[*pvc.Spec.StorageClassName]

	switch pvcRSC.Spec.VolumeAccess {
	case "Local", "EventuallyLocal":
		nodeLvgs := sharedNodes[nodeName]
		lvgsFromSC := filterInput.LVGFilteringInfo.SCLVGs[*pvc.Spec.StorageClassName]
		sharedLVG := findSharedLVG(nodeLvgs, lvgsFromSC)
		hasEnoughSpace := nodeHasEnoughSpace(filterInput.PVCSizeRequests, filterInput.LVGFilteringInfo, sharedLVG, pvc, cachedLVGs, log)

		if sharedLVG == nil || hasEnoughSpace {
			log.Info("[checkRSCVolumeAccessForUnBoundPVC] volume access: %s, node is ok status: %b", pvcRSC.Spec.VolumeAccess, false)
			nodeIsOk = false
		}

	case "PreferablyLocal", "Any":
		_, ok := filterInput.DRBDNodesMap[nodeName]
		log.Info("[checkRSCVolumeAccessForUnBoundPVC] volume access: %s, node is ok status: %b", pvcRSC.Spec.VolumeAccess, ok)
		nodeIsOk = ok
	}

	return nodeIsOk
}

// func (s *scheduler) filterSingleNodeSRV(nodeName string, filterInput *FilterInput, lvgInfo *LVGInfo, sharedNodes map[string][]*snc.LVMVolumeGroup, log *logger.Logger) error {
// log.Debug("[filterSingleNodeSRV] filtering node", "node", nodeName)

// nodeLvgs := sharedNodes[nodeName]
// for _, pvc := range filterInput.ReplicatedProvisionPVCs {

// log.Debug("[filterSingleNodeSRV] processing PVC", "pvc", pvc.Name, "node", nodeName)

// if pvc.Spec.VolumeName != "" {
// 	replica, found := filterInput.DRBDResourceReplicaMap[pvc.Spec.VolumeName]

// 	if !found {
// 		return fmt.Errorf("[filterSingleNodeSRV] pvc %s/%s does not have any diskful replicas", pvc.Namespace, pvc.Name)
// 	}

// 	hasDiskfulVolumes := false
// 	for _, peer := range replica.Spec.Peers {
// 		if peer.Diskless {
// 			continue
// 		}
// 		hasDiskfulVolumes = true
// 		break
// 	}

// 	if !hasDiskfulVolumes {
// 		return fmt.Errorf("[filterSingleNodeSRV] pvc %s/%s does not have any diskful replicas", pvc.Namespace, pvc.Name)
// 	}
// }

// isNodeDiskless := true
// lvgsFromSC := lvgInfo.SCLVGs[*pvc.Spec.StorageClassName]
// pvcRSC := filterInput.ReplicatedSCSUsedByPodPVCs[*pvc.Spec.StorageClassName]
// sharedLVG := findSharedLVG(nodeLvgs, lvgsFromSC)

// lvgs := s.cacheMgr.GetAllLVG()
// hasEnoughSpace := nodeHasEnoughSpace(filterInput.PVCSizeRequests, lvgInfo, sharedLVG, pvc, lvgs, s.log)

// switch pvcRSC.Spec.VolumeAccess {
// case "Local":
// 	if pvc.Spec.VolumeName == "" {
// 		if sharedLVG == nil {
// 			return fmt.Errorf("[filterSingleNodeSRV] node %s does not contain LVGs from storage class %s", nodeName, pvcRSC.Name)
// 		}
// 		if !hasEnoughSpace {
// 			return fmt.Errorf("[filterSingleNodeSRV] node does not have enough space in LVG %s for PVC %s/%s", sharedLVG.Name, pvc.Namespace, pvc.Name)
// 		}
// 	} else if !isNodeDiskless {
// 		return fmt.Errorf("[filterSingleNodeSRV] node %s is not diskful for PV %s", nodeName, pvc.Spec.VolumeName)
// 	}

// case "EventuallyLocal":
// 	if pvc.Spec.VolumeName == "" {
// 		if sharedLVG == nil {
// 			return fmt.Errorf("[filterSingleNodeSRV] node %s does not contain LVGs from storage class %s", nodeName, pvcRSC.Name)
// 		}
// 		if !hasEnoughSpace {
// 			return fmt.Errorf("[filterSingleNodeSRV] node does not have enough space in LVG %s for PVC %s/%s", sharedLVG.Name, pvc.Namespace, pvc.Name)
// 		}
// 	} else if isNodeDiskless {
// 		log.Trace("[filterSingleNodeSRV]", "node is diskful for EventuallyLocal PVC", "node", nodeName, "pvc", pvc.Name)
// 		return nil
// 	} else if sharedLVG == nil || !hasEnoughSpace {
// 		return fmt.Errorf("[filterSingleNodeSRV] node %s does not meet EventuallyLocal criteria for PVC %s", nodeName, pvc.Name)
// 	}

// case "PreferablyLocal":
// 	if pvc.Spec.VolumeName == "" && !hasEnoughSpace {
// 		return fmt.Errorf("[filterSingleNodeSRV] node does not have enough space in LVG %s for PVC %s/%s", sharedLVG.Name, pvc.Namespace, pvc.Name)
// 	}
// }
// }

// if !isDrbdNode(nodeName, filterInput.DRBDNodesMap) {
// 	return fmt.Errorf("[filterSingleNodeSRV] node %s is not a DRBD node", nodeName)
// }
// if !isOkNode(nodeName) {
// 	return fmt.Errorf("[filterSingleNodeSRV] node %s is offline", nodeName)
// }

// log.Debug("[filterSingleNodeSRV] node is ok", "node", nodeName)
// return nil
// }
