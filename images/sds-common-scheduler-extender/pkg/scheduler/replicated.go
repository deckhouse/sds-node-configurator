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
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	snc "github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/cache"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/consts"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/logger"
)

// getReplicatedStorageClass retrieves RSC by SC name (they are equal)
func getReplicatedStorageClass(ctx context.Context, cl client.Client, scName string) (*snc.ReplicatedStorageClass, error) {
	rsc := &snc.ReplicatedStorageClass{}
	err := cl.Get(ctx, client.ObjectKey{Name: scName}, rsc)
	if err != nil {
		return nil, fmt.Errorf("unable to get ReplicatedStorageClass %s: %w", scName, err)
	}
	return rsc, nil
}

// getReplicatedStoragePool retrieves RSP by name
func getReplicatedStoragePool(ctx context.Context, cl client.Client, rspName string) (*snc.ReplicatedStoragePool, error) {
	rsp := &snc.ReplicatedStoragePool{}
	err := cl.Get(ctx, client.ObjectKey{Name: rspName}, rsp)
	if err != nil {
		return nil, fmt.Errorf("unable to get ReplicatedStoragePool %s: %w", rspName, err)
	}
	return rsp, nil
}

// getDeviceTypeFromRSP returns device type (Thick/Thin) from RSP
func getDeviceTypeFromRSP(rsp *snc.ReplicatedStoragePool) string {
	switch rsp.Spec.Type {
	case consts.RSPTypeLVM:
		return consts.Thick
	case consts.RSPTypeLVMThin:
		return consts.Thin
	default:
		return consts.Thick
	}
}

// requiresLVGCheck returns true if volumeAccess requires LVG and space checks
func requiresLVGCheck(volumeAccess string) bool {
	return volumeAccess == consts.VolumeAccessLocal ||
		volumeAccess == consts.VolumeAccessEventuallyLocal
}

// isNodeInZones checks if the node is in one of the specified zones
func isNodeInZones(node *corev1.Node, zones []string) bool {
	if len(zones) == 0 {
		return true
	}
	nodeZone := node.Labels["topology.kubernetes.io/zone"]
	for _, z := range zones {
		if z == nodeZone {
			return true
		}
	}
	return false
}

// hasReplicatedNodeLabel checks if node has the sds-replicated-volume-node label
func hasReplicatedNodeLabel(node *corev1.Node) bool {
	_, exists := node.Labels[consts.LabelReplicatedNode]
	return exists
}

// findLVGForNodeInRSP finds LVG from RSP that belongs to the node.
// Uses client.Client to fetch LVG data from the informer cache.
func findLVGForNodeInRSP(
	ctx context.Context,
	cl client.Client,
	nodeName string,
	rsp *snc.ReplicatedStoragePool,
) (*snc.ReplicatedStoragePoolLVG, bool) {
	for i := range rsp.Spec.LvmVolumeGroups {
		lvgRef := &rsp.Spec.LvmVolumeGroups[i]
		lvg := &snc.LVMVolumeGroup{}
		if err := cl.Get(ctx, client.ObjectKey{Name: lvgRef.Name}, lvg); err != nil {
			continue
		}
		if ok, _ := isLVGSchedulable(lvg); !ok {
			continue
		}
		if lvgHasNode(lvg, nodeName) {
			return lvgRef, true
		}
	}
	return nil, false
}

// checkNodeHasLVGFromRSP checks if node has any LVG from RSP
func checkNodeHasLVGFromRSP(
	ctx context.Context,
	cl client.Client,
	nodeName string,
	rsp *snc.ReplicatedStoragePool,
) bool {
	_, found := findLVGForNodeInRSP(ctx, cl, nodeName, rsp)
	return found
}

// storagePoolKeyFromRSPLVG creates a StoragePoolKey from an RSP LVG reference and device type.
func storagePoolKeyFromRSPLVG(lvgRef *snc.ReplicatedStoragePoolLVG, deviceType string) cache.StoragePoolKey {
	key := cache.StoragePoolKey{LVGName: lvgRef.Name}
	if deviceType == consts.Thin {
		key.ThinPoolName = lvgRef.ThinPoolName
	}
	return key
}

// checkNodeHasLVGWithSpaceForReplicated checks if node has LVG with enough space for replicated PVC.
// Uses client.Client for LVG lookups and reservation cache for reserved space.
func checkNodeHasLVGWithSpaceForReplicated(
	ctx context.Context,
	log logger.Logger,
	cl client.Client,
	reservationCache *cache.Cache,
	nodeName string,
	rsp *snc.ReplicatedStoragePool,
	requestedSize int64,
) (bool, string) {
	deviceType := getDeviceTypeFromRSP(rsp)

	lvgRef, found := findLVGForNodeInRSP(ctx, cl, nodeName, rsp)
	if !found {
		return false, fmt.Sprintf("no LVG from RSP %s found on node %s", rsp.Name, nodeName)
	}

	key := storagePoolKeyFromRSPLVG(lvgRef, deviceType)
	hasSpace, err := checkPoolHasSpace(ctx, cl, reservationCache, key, requestedSize)
	if err != nil {
		log.Error(err, fmt.Sprintf("unable to check space for pool %s", key.String()))
		return false, fmt.Sprintf("error checking space for LVG %s: %v", lvgRef.Name, err)
	}

	if !hasSpace {
		return false, fmt.Sprintf("LVG %s on node %s does not have enough space", lvgRef.Name, nodeName)
	}

	return true, ""
}

// filterNodesByVolumeReplicas is a stub for filtering by volume replicas
// TODO: Implement replica-based filtering for Local volumeAccess with Bound PVC.
func filterNodesByVolumeReplicas(
	log logger.Logger,
	nodeNames []string,
	pvc *corev1.PersistentVolumeClaim,
) []string {
	log.Debug(fmt.Sprintf("[filterNodesByVolumeReplicas] TODO: implement replica-based filtering for PVC %s/%s", pvc.Namespace, pvc.Name))
	return nodeNames
}

// filterNodesByVolumeZone is a stub for filtering by volume zone
// TODO: Implement zone-based filtering for Zonal topology with Bound PVC.
func filterNodesByVolumeZone(
	log logger.Logger,
	nodeNames []string,
	pvc *corev1.PersistentVolumeClaim,
	_ *snc.ReplicatedStorageClass,
) []string {
	log.Debug(fmt.Sprintf("[filterNodesByVolumeZone] TODO: implement zone-based filtering for Zonal topology, PVC %s/%s", pvc.Namespace, pvc.Name))
	return nodeNames
}

// filterPVCsByProvisioner filters PVCs by provisioner
func filterPVCsByProvisioner(
	pvcs map[string]*corev1.PersistentVolumeClaim,
	scs map[string]*storagev1.StorageClass,
	provisioner string,
) map[string]*corev1.PersistentVolumeClaim {
	result := make(map[string]*corev1.PersistentVolumeClaim)
	for name, pvc := range pvcs {
		if pvc.Spec.StorageClassName == nil {
			continue
		}
		sc, exists := scs[*pvc.Spec.StorageClassName]
		if !exists {
			continue
		}
		if sc.Provisioner == provisioner {
			result[name] = pvc
		}
	}
	return result
}

// filterNodeForReplicatedPVCs filters node for replicated PVCs
func filterNodeForReplicatedPVCs(
	ctx context.Context,
	log logger.Logger,
	cl client.Client,
	schedulerCache *cache.Cache,
	nodeName string,
	node *corev1.Node,
	replicatedPVCs map[string]*corev1.PersistentVolumeClaim,
	scUsedByPVCs map[string]*storagev1.StorageClass,
	pvcRequests map[string]PVCRequest,
) (bool, string) {
	var failReasons []string

	for _, pvc := range replicatedPVCs {
		sc := scUsedByPVCs[*pvc.Spec.StorageClassName]
		pvcReq := pvcRequests[pvc.Name]

		// Get RSC (name = SC name)
		rsc, err := getReplicatedStorageClass(ctx, cl, sc.Name)
		if err != nil {
			failReasons = append(failReasons, fmt.Sprintf("PVC %s: unable to get RSC: %v", pvc.Name, err))
			continue
		}

		// Get RSP
		rsp, err := getReplicatedStoragePool(ctx, cl, rsc.Spec.StoragePool)
		if err != nil {
			failReasons = append(failReasons, fmt.Sprintf("PVC %s: unable to get RSP: %v", pvc.Name, err))
			continue
		}

		volumeAccess := rsc.Spec.VolumeAccess
		if volumeAccess == "" {
			volumeAccess = consts.VolumeAccessPreferablyLocal
		}

		// === R1: Check sds-replicated-volume-node label ===
		if !hasReplicatedNodeLabel(node) {
			failReasons = append(failReasons, fmt.Sprintf("PVC %s: node %s missing label %s", pvc.Name, nodeName, consts.LabelReplicatedNode))
			continue
		}

		// === R2: Check zones from RSC ===
		if len(rsc.Spec.Zones) > 0 && !isNodeInZones(node, rsc.Spec.Zones) {
			failReasons = append(failReasons, fmt.Sprintf("PVC %s: node %s not in zones %v", pvc.Name, nodeName, rsc.Spec.Zones))
			continue
		}

		// === R3 (TODO): For Zonal topology + Bound PVC ===
		if rsc.Spec.Topology == consts.TopologyZonal && pvc.Status.Phase == corev1.ClaimBound {
			log.Debug(fmt.Sprintf("[filterNodeForReplicatedPVCs] TODO: zone filtering for Zonal topology, pvc=%s", pvc.Name))
		}

		// === LVG and space checks ===
		switch pvc.Status.Phase {
		case corev1.ClaimPending:
			if requiresLVGCheck(volumeAccess) {
				ok, reason := checkNodeHasLVGWithSpaceForReplicated(ctx, log, cl, schedulerCache, nodeName, rsp, pvcReq.RequestedSize)
				if !ok {
					failReasons = append(failReasons, fmt.Sprintf("PVC %s: %s", pvc.Name, reason))
				}
			}

		case corev1.ClaimBound:
			switch volumeAccess {
			case consts.VolumeAccessLocal:
				log.Debug(fmt.Sprintf("[filterNodeForReplicatedPVCs] TODO: filter by replicas for Local, pvc=%s", pvc.Name))

			case consts.VolumeAccessEventuallyLocal:
				ok, reason := checkNodeHasLVGWithSpaceForReplicated(ctx, log, cl, schedulerCache, nodeName, rsp, pvcReq.RequestedSize)
				if !ok {
					failReasons = append(failReasons, fmt.Sprintf("PVC %s: %s", pvc.Name, reason))
				}

			default:
				// PreferablyLocal, Any - do not check LVG
			}
		}
	}

	if len(failReasons) > 0 {
		return false, strings.Join(failReasons, "; ")
	}
	return true, ""
}

// calculateReplicatedPVCScore calculates score for replicated PVC using client.Client and reservation cache.
func calculateReplicatedPVCScore(
	ctx context.Context,
	log logger.Logger,
	cl client.Client,
	reservationCache *cache.Cache,
	nodeName string,
	rsp *snc.ReplicatedStoragePool,
	pvcReq PVCRequest,
	divisor float64,
) int64 {
	deviceType := getDeviceTypeFromRSP(rsp)

	lvgRef, found := findLVGForNodeInRSP(ctx, cl, nodeName, rsp)
	if !found {
		return 0
	}

	key := storagePoolKeyFromRSPLVG(lvgRef, deviceType)
	score, err := calculatePoolScore(ctx, cl, reservationCache, key, pvcReq.RequestedSize, divisor)
	if err != nil {
		log.Error(err, fmt.Sprintf("unable to calculate score for pool %s", key.String()))
		return 0
	}

	return int64(score)
}

// calculateReplicaBonus calculates bonus for replicas on the node
func calculateReplicaBonus(
	nodeName string,
	managedPVCs map[string]*corev1.PersistentVolumeClaim,
	replicaLocations map[string][]string,
) int64 {
	var bonus int64

	for pvcName := range managedPVCs {
		nodeList, exists := replicaLocations[pvcName]
		if !exists {
			continue
		}

		for _, n := range nodeList {
			if n == nodeName {
				bonus += 10 // 10 points for each replica on the node
				break
			}
		}
	}

	return bonus
}

// getNodes retrieves nodes by names
func getNodes(ctx context.Context, cl client.Client, nodeNames []string) (map[string]*corev1.Node, error) {
	nodes := make(map[string]*corev1.Node, len(nodeNames))
	for _, nodeName := range nodeNames {
		node := &corev1.Node{}
		err := cl.Get(ctx, client.ObjectKey{Name: nodeName}, node)
		if err != nil {
			return nil, fmt.Errorf("unable to get node %s: %w", nodeName, err)
		}
		nodes[nodeName] = node
	}
	return nodes, nil
}

// hasReplicatedPVCs checks if there are any replicated PVCs in the map
func hasReplicatedPVCs(pvcs map[string]*corev1.PersistentVolumeClaim, scs map[string]*storagev1.StorageClass) bool {
	for _, pvc := range pvcs {
		if pvc.Spec.StorageClassName == nil {
			continue
		}
		sc, exists := scs[*pvc.Spec.StorageClassName]
		if !exists {
			continue
		}
		if sc.Provisioner == consts.SdsReplicatedVolumeProvisioner {
			return true
		}
	}
	return false
}
