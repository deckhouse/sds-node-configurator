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
	srv "github.com/deckhouse/sds-replicated-volume/api/v1alpha1"

	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/cache"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/consts"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/logger"
)

func getReplicatedStorageClass(ctx context.Context, cl client.Client, scName string) (*srv.ReplicatedStorageClass, error) {
	rsc := &srv.ReplicatedStorageClass{}
	err := cl.Get(ctx, client.ObjectKey{Name: scName}, rsc)
	if err != nil {
		return nil, fmt.Errorf("unable to get ReplicatedStorageClass %s: %w", scName, err)
	}
	return rsc, nil
}

func getReplicatedStoragePool(ctx context.Context, cl client.Client, rspName string) (*srv.ReplicatedStoragePool, error) {
	rsp := &srv.ReplicatedStoragePool{}
	err := cl.Get(ctx, client.ObjectKey{Name: rspName}, rsp)
	if err != nil {
		return nil, fmt.Errorf("unable to get ReplicatedStoragePool %s: %w", rspName, err)
	}
	return rsp, nil
}

// rscStoragePoolName returns the storage pool name from RSC, falling back
// to the deprecated Spec.StoragePool if Status.StoragePoolName is empty.
func rscStoragePoolName(rsc *srv.ReplicatedStorageClass) string {
	if rsc.Status.StoragePoolName != "" {
		return rsc.Status.StoragePoolName
	}
	return rsc.Spec.StoragePool
}

func getDeviceTypeFromRSP(rsp *srv.ReplicatedStoragePool) string {
	switch rsp.Spec.Type {
	case srv.ReplicatedStoragePoolTypeLVM:
		return consts.Thick
	case srv.ReplicatedStoragePoolTypeLVMThin:
		return consts.Thin
	default:
		return consts.Thick
	}
}

func requiresLVGCheck(volumeAccess srv.ReplicatedStorageClassVolumeAccess) bool {
	return volumeAccess == srv.VolumeAccessLocal ||
		volumeAccess == srv.VolumeAccessEventuallyLocal
}

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

func hasReplicatedNodeLabel(node *corev1.Node) bool {
	_, exists := node.Labels[srv.AgentNodeLabelKey]
	return exists
}

// findLVGForNodeInRSP finds an LVG from RSP that belongs to the node.
func findLVGForNodeInRSP(
	ctx context.Context,
	cl client.Client,
	nodeName string,
	rsp *srv.ReplicatedStoragePool,
) (*srv.ReplicatedStoragePoolLVMVolumeGroups, bool) {
	for i := range rsp.Spec.LVMVolumeGroups {
		lvgRef := &rsp.Spec.LVMVolumeGroups[i]
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

func checkNodeHasLVGFromRSP(
	ctx context.Context,
	cl client.Client,
	nodeName string,
	rsp *srv.ReplicatedStoragePool,
) bool {
	_, found := findLVGForNodeInRSP(ctx, cl, nodeName, rsp)
	return found
}

func storagePoolKeyFromRSPLVG(lvgRef *srv.ReplicatedStoragePoolLVMVolumeGroups, deviceType string) cache.StoragePoolKey {
	key := cache.StoragePoolKey{LVGName: lvgRef.Name}
	if deviceType == consts.Thin {
		key.ThinPoolName = lvgRef.ThinPoolName
	}
	return key
}

func checkNodeHasLVGWithSpaceForReplicated(
	ctx context.Context,
	log logger.Logger,
	cl client.Client,
	reservationCache *cache.Cache,
	nodeName string,
	rsp *srv.ReplicatedStoragePool,
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

// getReplicaNodesForBoundPVC resolves a bound PVC to the set of node names
// that host Diskful replicas. Chain: PVC -> PV -> RV (via CSI VolumeHandle) ->
// datamesh members with HasBackingVolume().
func getReplicaNodesForBoundPVC(
	ctx context.Context,
	cl client.Client,
	pvc *corev1.PersistentVolumeClaim,
) ([]string, error) {
	if pvc.Spec.VolumeName == "" {
		return nil, fmt.Errorf("PVC %s/%s is bound but has no volume name", pvc.Namespace, pvc.Name)
	}

	pv := &corev1.PersistentVolume{}
	if err := cl.Get(ctx, client.ObjectKey{Name: pvc.Spec.VolumeName}, pv); err != nil {
		return nil, fmt.Errorf("unable to get PV %s: %w", pvc.Spec.VolumeName, err)
	}

	if pv.Spec.CSI == nil {
		return nil, fmt.Errorf("PV %s has no CSI spec", pv.Name)
	}

	rvName := pv.Spec.CSI.VolumeHandle
	if rvName == "" {
		return nil, fmt.Errorf("PV %s has empty CSI VolumeHandle", pv.Name)
	}

	rv := &srv.ReplicatedVolume{}
	if err := cl.Get(ctx, client.ObjectKey{Name: rvName}, rv); err != nil {
		return nil, fmt.Errorf("unable to get ReplicatedVolume %s: %w", rvName, err)
	}

	var nodes []string
	for _, m := range rv.Status.Datamesh.Members {
		if m.Type.HasBackingVolume() {
			nodes = append(nodes, m.NodeName)
		}
	}
	return nodes, nil
}

// buildReplicaLocations builds a map: PVC name -> list of node names with
// diskful replicas, for all bound replicated PVCs.
func buildReplicaLocations(
	ctx context.Context,
	log logger.Logger,
	cl client.Client,
	replicatedPVCs map[string]*corev1.PersistentVolumeClaim,
) map[string][]string {
	result := make(map[string][]string, len(replicatedPVCs))
	for _, pvc := range replicatedPVCs {
		if pvc.Status.Phase != corev1.ClaimBound {
			continue
		}
		nodes, err := getReplicaNodesForBoundPVC(ctx, cl, pvc)
		if err != nil {
			log.Warning(fmt.Sprintf("[buildReplicaLocations] unable to resolve replica nodes for PVC %s/%s: %v", pvc.Namespace, pvc.Name, err))
			continue
		}
		if len(nodes) > 0 {
			result[pvc.Name] = nodes
		}
	}
	return result
}

// filterNodesByVolumeReplicas filters nodes to only those hosting diskful
// replicas of the given bound PVC.
func filterNodesByVolumeReplicas(
	ctx context.Context,
	log logger.Logger,
	cl client.Client,
	nodeNames []string,
	pvc *corev1.PersistentVolumeClaim,
) []string {
	replicaNodes, err := getReplicaNodesForBoundPVC(ctx, cl, pvc)
	if err != nil {
		log.Warning(fmt.Sprintf("[filterNodesByVolumeReplicas] unable to resolve replica nodes for PVC %s/%s: %v; not filtering", pvc.Namespace, pvc.Name, err))
		return nodeNames
	}
	if len(replicaNodes) == 0 {
		log.Warning(fmt.Sprintf("[filterNodesByVolumeReplicas] no diskful replicas found for PVC %s/%s; not filtering", pvc.Namespace, pvc.Name))
		return nodeNames
	}

	replicaSet := make(map[string]struct{}, len(replicaNodes))
	for _, n := range replicaNodes {
		replicaSet[n] = struct{}{}
	}

	var filtered []string
	for _, n := range nodeNames {
		if _, ok := replicaSet[n]; ok {
			filtered = append(filtered, n)
		}
	}
	return filtered
}

// filterNodesByVolumeZone is a stub for filtering by volume zone.
// TODO: Implement zone-based filtering for Zonal topology with Bound PVC.
func filterNodesByVolumeZone(
	log logger.Logger,
	nodeNames []string,
	pvc *corev1.PersistentVolumeClaim,
	_ *srv.ReplicatedStorageClass,
) []string {
	log.Debug(fmt.Sprintf("[filterNodesByVolumeZone] TODO: implement zone-based filtering for Zonal topology, PVC %s/%s", pvc.Namespace, pvc.Name))
	return nodeNames
}

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

		rsc, err := getReplicatedStorageClass(ctx, cl, sc.Name)
		if err != nil {
			failReasons = append(failReasons, fmt.Sprintf("PVC %s: unable to get RSC: %v", pvc.Name, err))
			continue
		}

		rspName := rscStoragePoolName(rsc)
		rsp, err := getReplicatedStoragePool(ctx, cl, rspName)
		if err != nil {
			failReasons = append(failReasons, fmt.Sprintf("PVC %s: unable to get RSP: %v", pvc.Name, err))
			continue
		}

		volumeAccess := rsc.Spec.VolumeAccess
		if volumeAccess == "" {
			volumeAccess = srv.VolumeAccessPreferablyLocal
		}

		if !hasReplicatedNodeLabel(node) {
			failReasons = append(failReasons, fmt.Sprintf("PVC %s: node %s missing label %s", pvc.Name, nodeName, srv.AgentNodeLabelKey))
			continue
		}

		if len(rsc.Spec.Zones) > 0 && !isNodeInZones(node, rsc.Spec.Zones) {
			failReasons = append(failReasons, fmt.Sprintf("PVC %s: node %s not in zones %v", pvc.Name, nodeName, rsc.Spec.Zones))
			continue
		}

		if rsc.Spec.Topology == srv.TopologyZonal && pvc.Status.Phase == corev1.ClaimBound {
			log.Debug(fmt.Sprintf("[filterNodeForReplicatedPVCs] TODO: zone filtering for Zonal topology, pvc=%s", pvc.Name))
		}

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
			case srv.VolumeAccessLocal:
				replicaNodes, err := getReplicaNodesForBoundPVC(ctx, cl, pvc)
				if err != nil {
					log.Warning(fmt.Sprintf("[filterNodeForReplicatedPVCs] unable to resolve replica nodes for Local PVC %s: %v; passing node", pvc.Name, err))
				} else {
					hasReplica := false
					for _, rn := range replicaNodes {
						if rn == nodeName {
							hasReplica = true
							break
						}
					}
					if !hasReplica {
						failReasons = append(failReasons, fmt.Sprintf("PVC %s: node %s has no diskful replica (volumeAccess=Local)", pvc.Name, nodeName))
					}
				}

			case srv.VolumeAccessEventuallyLocal:
				ok, reason := checkNodeHasLVGWithSpaceForReplicated(ctx, log, cl, schedulerCache, nodeName, rsp, pvcReq.RequestedSize)
				if !ok {
					failReasons = append(failReasons, fmt.Sprintf("PVC %s: %s", pvc.Name, reason))
				}

			default:
				// PreferablyLocal, Any — no LVG/space check for bound PVCs
			}
		}
	}

	if len(failReasons) > 0 {
		return false, strings.Join(failReasons, "; ")
	}
	return true, ""
}

func calculateReplicatedPVCScore(
	ctx context.Context,
	log logger.Logger,
	cl client.Client,
	reservationCache *cache.Cache,
	nodeName string,
	rsp *srv.ReplicatedStoragePool,
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

// nodeHasReplicaForPVC checks if the node hosts a diskful replica for the PVC.
func nodeHasReplicaForPVC(nodeName, pvcName string, replicaLocations map[string][]string) bool {
	nodes, exists := replicaLocations[pvcName]
	if !exists {
		return false
	}
	for _, n := range nodes {
		if n == nodeName {
			return true
		}
	}
	return false
}

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
