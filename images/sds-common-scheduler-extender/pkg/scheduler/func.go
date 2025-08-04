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
	"context"
	"fmt"
	"math"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	v1 "k8s.io/api/storage/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	slv "github.com/deckhouse/sds-local-volume/api/v1alpha1"
	snc "github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/cache"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/consts"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/logger"
	lsrv "github.com/deckhouse/sds-replicated-volume/api/linstor"
	srv "github.com/deckhouse/sds-replicated-volume/api/v1alpha1"
	srv2 "github.com/deckhouse/sds-replicated-volume/api/v1alpha2"
)

const (
	annotationBetaStorageProvisioner = "volume.beta.kubernetes.io/storage-provisioner"
	annotationStorageProvisioner     = "volume.kubernetes.io/storage-provisioner"
)

func shouldProcessPod(ctx context.Context, cl client.Client, pvcMap map[string]*corev1.PersistentVolumeClaim, log *logger.Logger, pod *corev1.Pod) ([]corev1.Volume, error) {
	shouldProcessPod := false
	targetProvisionerVolumes := make([]corev1.Volume, 0)
	targetProvisioners := []string{consts.SdsLocalVolumeProvisioner, consts.SdsReplicatedVolumeProvisioner}

	log.Trace(fmt.Sprintf("[ShouldProcessPod] targetProvisioners=%+v, pod: %+v", targetProvisioners, pod))

	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim == nil {
			log.Trace(fmt.Sprintf("[ShouldProcessPod] skip volume %s because it doesn't have PVC", volume.Name))
			continue
		}

		log.Trace(fmt.Sprintf("[ShouldProcessPod] process volume: %+v that has pvc: %+v", volume, volume.PersistentVolumeClaim))
		pvcName := volume.PersistentVolumeClaim.ClaimName
		pvc, found := pvcMap[pvcName]
		if !found {
			return nil, fmt.Errorf("[ShouldProcessPod] error getting PVC %s/%s", pod.Namespace, pvcName)
		}

		log.Trace(fmt.Sprintf("[ShouldProcessPod] Successfully get PVC %s/%s: %+v", pod.Namespace, pvcName, pvc))

		discoveredProvisioner, err := getProvisionerFromPVC(ctx, cl, log, pvc)
		if err != nil {
			return nil, fmt.Errorf("[ShouldProcessPod] error getting provisioner from PVC %s/%s: %w", pod.Namespace, pvcName, err)
		}
		log.Trace(fmt.Sprintf("[ShouldProcessPod] discovered provisioner: %s", discoveredProvisioner))
		if slices.Contains(targetProvisioners, discoveredProvisioner) {
			log.Trace(fmt.Sprintf("[ShouldProcessPod] provisioner matches targetProvisioner %s. Pod: %s/%s", discoveredProvisioner, pod.Namespace, pod.Name))
			shouldProcessPod = true
			targetProvisionerVolumes = append(targetProvisionerVolumes, volume)
		} else {
			log.Trace(fmt.Sprintf("[ShouldProcessPod] provisioner %s doesn't match targetProvisioner. Skip volume %s.", discoveredProvisioner, volume.Name))
		}
	}

	if shouldProcessPod {
		log.Trace(fmt.Sprintf("[ShouldProcessPod] targetProvisioner found in pod volumes. Pod: %s/%s. Volumes that match: %+v", pod.Namespace, pod.Name, targetProvisionerVolumes))
		return targetProvisionerVolumes, nil
	}

	log.Trace(fmt.Sprintf("[ShouldProcessPod] can't find targetProvisioner in pod volumes. Skip pod: %s/%s", pod.Namespace, pod.Name))
	return nil, fmt.Errorf("[ShouldProcessPod] can't find targetProvisioner in pod volumes. Skip pod: %s/%s", pod.Namespace, pod.Name)
}

func getProvisionerFromPVC(ctx context.Context, cl client.Client, log *logger.Logger, pvc *corev1.PersistentVolumeClaim) (string, error) {
	discoveredProvisioner := ""
	log.Trace(fmt.Sprintf("[getProvisionerFromPVC] check provisioner in pvc annotations: %+v", pvc.Annotations))

	discoveredProvisioner = pvc.Annotations[annotationStorageProvisioner]
	if discoveredProvisioner != "" {
		log.Trace(fmt.Sprintf("[getProvisionerFromPVC] discovered provisioner in pvc annotations: %s", discoveredProvisioner))
	} else {
		discoveredProvisioner = pvc.Annotations[annotationBetaStorageProvisioner]
		log.Trace(fmt.Sprintf("[getProvisionerFromPVC] discovered provisioner in beta pvc annotations: %s", discoveredProvisioner))
	}

	if discoveredProvisioner == "" && pvc.Spec.StorageClassName != nil && *pvc.Spec.StorageClassName != "" {
		log.Trace(fmt.Sprintf("[getProvisionerFromPVC] can't find provisioner in pvc annotations, check in storageClass with name: %s", *pvc.Spec.StorageClassName))

		storageClass := &storagev1.StorageClass{}
		if err := cl.Get(ctx, client.ObjectKey{Name: *pvc.Spec.StorageClassName}, storageClass); err != nil {
			if !k8serrors.IsNotFound(err) {
				return "", fmt.Errorf("[getProvisionerFromPVC] error getting StorageClass %s: %v", *pvc.Spec.StorageClassName, err)
			}
			log.Warning(fmt.Sprintf("[getProvisionerFromPVC] StorageClass %s for PVC %s/%s not found", *pvc.Spec.StorageClassName, pvc.Namespace, pvc.Name))
		}
		discoveredProvisioner = storageClass.Provisioner
		log.Trace(fmt.Sprintf("[getProvisionerFromPVC] discover provisioner %s in storageClass: %+v", discoveredProvisioner, storageClass))
	}

	if discoveredProvisioner == "" && pvc.Spec.VolumeName != "" {
		log.Trace(fmt.Sprintf("[getProvisionerFromPVC] can't find provisioner in pvc annotations and StorageClass, check in PV with name: %s", pvc.Spec.VolumeName))

		pv := &corev1.PersistentVolume{}
		if err := cl.Get(ctx, client.ObjectKey{Name: pvc.Spec.VolumeName}, pv); err != nil {
			if !k8serrors.IsNotFound(err) {
				return "", fmt.Errorf("[getProvisionerFromPVC] error getting PV %s for PVC %s/%s: %v", pvc.Spec.VolumeName, pvc.Namespace, pvc.Name, err)
			}
			log.Warning(fmt.Sprintf("[getProvisionerFromPVC] PV %s for PVC %s/%s not found", pvc.Spec.VolumeName, pvc.Namespace, pvc.Name))
		}

		if pv.Spec.CSI != nil {
			discoveredProvisioner = pv.Spec.CSI.Driver
		}

		log.Trace(fmt.Sprintf("[getProvisionerFromPVC] discover provisioner %s in PV: %+v", discoveredProvisioner, pv))
	}

	return discoveredProvisioner, nil
}

func getReplicatedStoragePools(ctx context.Context, cl client.Client, log *logger.Logger) (map[string]*srv.ReplicatedStoragePool, error) {
	rsp := &srv.ReplicatedStoragePoolList{}
	err := cl.List(ctx, rsp)
	if err != nil {
		log.Error(err, "[getReplicatedStoragePools] failed to list replicated storage pools")
		return nil, err
	}

	rpsMap := make(map[string]*srv.ReplicatedStoragePool, len(rsp.Items))
	for _, rp := range rsp.Items {
		rpsMap[rp.Name] = &rp
	}

	log.Trace("[getReplicatedStoragePools]", "replicated storage pools", rpsMap)
	return rpsMap, nil
}

func getReplicatedStorageClasses(ctx context.Context, cl client.Client, log *logger.Logger) (map[string]*srv.ReplicatedStorageClass, error) {
	rscs := &srv.ReplicatedStorageClassList{}
	err := cl.List(ctx, rscs)
	if err != nil {
		log.Error(err, "[getReplicatedStorageClasses] failed to list replicated storage classes")
		return nil, err
	}

	rscMap := make(map[string]*srv.ReplicatedStorageClass, len(rscs.Items))
	for _, rsc := range rscs.Items {
		rscMap[rsc.Name] = &rsc
	}

	log.Trace("[getReplicatedStorageClasses]", "replicated storage classes", rscMap)
	return rscMap, nil
}

func getlvmVolumeGroups(ctx context.Context, cl client.Client, log *logger.Logger) (map[string]*snc.LVMVolumeGroup, error) {
	lvmList := &snc.LVMVolumeGroupList{}
	err := cl.List(ctx, lvmList)
	if err != nil {
		log.Error(err, "[getlvmVolumeGroups] failed to list LVM volume groups")
		return nil, err
	}

	lvmMap := make(map[string]*snc.LVMVolumeGroup, len(lvmList.Items))
	for _, lvm := range lvmList.Items {
		lvmMap[lvm.Name] = &lvm
	}

	log.Trace("[getlvmVolumeGroups]", "LVM volume groups map", lvmMap)
	return lvmMap, nil
}

func getNodeWithLvmVgsMap(ctx context.Context, cl client.Client, log *logger.Logger) (map[string][]*snc.LVMVolumeGroup, error) {
	lvmList := &snc.LVMVolumeGroupList{}
	err := cl.List(ctx, lvmList)
	if err != nil {
		log.Error(err, "[getNodeWithLvmVgsMap] failed to list LVM volume groups")
		return nil, err
	}

	nodeToLvmMap := make(map[string][]*snc.LVMVolumeGroup, len(lvmList.Items))
	for _, lvm := range lvmList.Items {
		nodeToLvmMap[lvm.Spec.Local.NodeName] = append(nodeToLvmMap[lvm.Spec.Local.NodeName], &lvm)
	}

	log.Trace("[getNodeWithLvmVgsMap]", "node to LVM volume groups map", nodeToLvmMap)
	return nodeToLvmMap, nil
}

// func getDRBDResourceMap(ctx context.Context, cl client.Client) (map[string]*srv.DRBDResource, error) {
// 	// TODO
// 	// drbdList := &srv.DRBDResourceList{}
// 	// err := cl.List(ctx, drbdList)
// 	// if err != nil {
// 	// 	return nil, err
// 	// }

// 	// drbdMap := make(map[string]*srv.DRBDResource, len(drbdList.Items))
// 	// for _, drbd := range drbdList.Items {
// 	// 	drbdMap[drbd.Name] = &drbd
// 	// }
// 	drbdMap := map[string]*srv.DRBDResource{}
// 	return drbdMap, nil
// }

func getDRBDNodesMap(ctx context.Context, cl client.Client, log *logger.Logger) (map[string]struct{}, error) {
	result := make(map[string]struct{})

	lvgList := &snc.LVMVolumeGroupList{}
	err := cl.List(ctx, lvgList)
	if err != nil {
		log.Error(err, "[getDRBDNodesMap] failed to list LVM volume groups")
		return result, err
	}

	lvgMap := make(map[string]*snc.LVMVolumeGroup, len(lvgList.Items))
	for _, lvg := range lvgList.Items {
		lvgMap[lvg.Name] = &lvg
	}
	log.Trace("[getDRBDNodesMap]", "LVM volume group map", lvgMap)

	rspList := &srv.ReplicatedStoragePoolList{}
	err = cl.List(ctx, rspList)
	if err != nil {
		log.Error(err, "[getDRBDNodesMap] failed to list replicated storage pools")
		return result, err
	}
	log.Trace("[getDRBDNodesMap]", "LVM volume group map", lvgMap)

	for _, rsc := range rspList.Items {
		for _, rscLVG := range rsc.Spec.LVMVolumeGroups {
			lvg, found := lvgMap[rscLVG.Name]
			if !found {
				log.Warning("[getDRBDNodesMap]", fmt.Sprintf("no LVM volume group %s found, skipping iteration", rscLVG.Name))
			}
			result[lvg.Spec.Local.NodeName] = struct{}{}
		}
	}

	log.Trace("[getDRBDNodesMap]", "DRBD nodes map", result)
	return result, nil
}

func getPersistentVolumeClaims(ctx context.Context, cl client.Client, log *logger.Logger) (map[string]*corev1.PersistentVolumeClaim, error) {
	pvs := &corev1.PersistentVolumeClaimList{}
	err := cl.List(ctx, pvs)
	if err != nil {
		log.Error(err, "[getPersistentVolumeClaims] failed to list persistent volume claims")
		return nil, err
	}

	pvcMap := make(map[string]*corev1.PersistentVolumeClaim, len(pvs.Items))
	for _, pvc := range pvs.Items {
		pvcMap[pvc.Name] = &pvc
	}

	log.Trace("[getPersistentVolumeClaims]", "persistent volume claims map", pvcMap)
	return pvcMap, nil
}

func getPersistentVolumes(ctx context.Context, cl client.Client, log *logger.Logger) (map[string]*corev1.PersistentVolume, error) {
	pvs := &corev1.PersistentVolumeList{}
	err := cl.List(ctx, pvs)
	if err != nil {
		log.Error(err, "[getPersistentVolumes] failed to list persistent volumes")
		return nil, err
	}

	pvMap := make(map[string]*corev1.PersistentVolume, len(pvs.Items))
	for _, pv := range pvs.Items {
		pvMap[pv.Name] = &pv
	}

	log.Trace("[getPersistentVolumes]", "persistent volumes map", pvMap)
	return pvMap, nil
}

func getLayerStorageVolumes(ctx context.Context, cl client.Client) (*lsrv.LayerStorageVolumesList, error) {
	cwt, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	layerStorageVolumes := &lsrv.LayerStorageVolumesList{}
	err := cl.List(cwt, layerStorageVolumes)
	if err != nil {
		return nil, err
	}

	return layerStorageVolumes, nil
}

func getDRBDReplicaList(ctx context.Context, cl client.Client) (*srv2.DRBDResourceReplicaList, error) {
	cwt, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	rrl := &srv2.DRBDResourceReplicaList{}
	if err := cl.List(cwt, rrl); err != nil {
		return nil, err
	}

	return rrl, nil
}

func getNodeNames(inputData ExtenderArgs, log *logger.Logger) ([]string, error) {
	if inputData.NodeNames != nil && len(*inputData.NodeNames) > 0 {
		log.Trace("[getNodeNames]", "node names from input", *inputData.NodeNames)
		return *inputData.NodeNames, nil
	}

	if inputData.Nodes != nil && len(inputData.Nodes.Items) > 0 {
		nodeNames := make([]string, 0, len(inputData.Nodes.Items))
		for _, node := range inputData.Nodes.Items {
			nodeNames = append(nodeNames, node.Name)
		}
		log.Trace("[getNodeNames]", "node names from nodes", nodeNames)
		return nodeNames, nil
	}

	log.Error(nil, "[getNodeNames] no nodes provided")
	return nil, fmt.Errorf("no nodes provided")
}

// collectLVGInfo gathers LVMVolumeGroup data.
func collectLVGInfo(s *scheduler, storageClasses map[string]*storagev1.StorageClass) (*LVGInfo, error) {
	lvgs := s.cacheMgr.GetAllLVG()
	for _, lvg := range lvgs {
		s.log.Trace(fmt.Sprintf("[filterNodes] LVMVolumeGroup %s in cache", lvg.Name))
	}

	scLVGs, err := CreateLVGsMapFromStorageClasses(storageClasses)
	if err != nil {
		return nil, err
	}

	filteredLVGs := GetCachedLVGsUsedByPodStorageClases(lvgs, scLVGs)
	thickFreeSpaces := getLVGThickFreeSpaces(filteredLVGs)
	thinFreeSpaces := getLVGThinFreeSpaces(filteredLVGs)

	for lvgName, freeSpace := range thickFreeSpaces {
		reserved, err := s.cacheMgr.GetLVGThickReservedSpace(lvgName)
		if err != nil {
			s.log.Error(err, fmt.Sprintf("[filterNodes] unable to get reserved space for LVMVolumeGroup %s", lvgName))
			continue
		}
		thickFreeSpaces[lvgName] = freeSpace - reserved
		s.log.Trace(fmt.Sprintf("[filterNodes] LVMVolumeGroup %s thick free space %s", lvgName, resource.NewQuantity(thickFreeSpaces[lvgName], resource.BinarySI)))
	}

	for lvgName, thinPools := range thinFreeSpaces {
		for tpName, freeSpace := range thinPools {
			reserved, err := s.cacheMgr.GetLVGThinReservedSpace(lvgName, tpName)
			if err != nil {
				s.log.Error(err, fmt.Sprintf("[filterNodes] unable to get reserved space for Thin pool %s of LVMVolumeGroup %s", tpName, lvgName))
				continue
			}
			thinFreeSpaces[lvgName][tpName] = freeSpace - reserved
			s.log.Trace(fmt.Sprintf("[filterNodes] LVMVolumeGroup %s Thin pool %s free space %s", lvgName, tpName, resource.NewQuantity(thinFreeSpaces[lvgName][tpName], resource.BinarySI)))
		}
	}

	nodeToLVGs := CreateNodeToCachedLVGsMap(filteredLVGs)
	s.log.Trace(fmt.Sprintf("[filterNodes] node name to LVM volume group map %+v", nodeToLVGs))
	return &LVGInfo{
		ThickFreeSpaces: thickFreeSpaces,
		ThinFreeSpaces:  thinFreeSpaces,
		NodeToLVGs:      nodeToLVGs,
		SCLVGs:          scLVGs,
	}, nil
}

func collectLVGScoreInfo(s *scheduler, storageClasses map[string]*storagev1.StorageClass) (*LVGScoreInfo, error) {
	lvgs := s.cacheMgr.GetAllLVG()
	scLVGs, err := CreateLVGsMapFromStorageClasses(storageClasses)
	if err != nil {
		return nil, err
	}

	usedLVGs := GetCachedLVGsUsedByPodStorageClases(lvgs, scLVGs)
	for lvgName := range usedLVGs {
		s.log.Trace(fmt.Sprintf("[collectLVGScoreInfo] used LVMVolumeGroup %s", lvgName))
	}

	nodeToLVGs := CreateNodeToCachedLVGsMap(usedLVGs)
	for nodeName, lvgList := range nodeToLVGs {
		for _, lvg := range lvgList {
			s.log.Trace(fmt.Sprintf("[collectLVGScoreInfo] LVMVolumeGroup %s belongs to node %s", lvg.Name, nodeName))
		}
	}

	res := &LVGScoreInfo{
		NodeToLVGs: nodeToLVGs,
		SCLVGs:     scLVGs,
		LVGs:       lvgs,
	}

	s.log.Trace(fmt.Sprintf("[collectLVGScoreInfo] LVGScoreInfo %+v", res))
	return res, nil
}

func calculateFreeSpace(
	lvg *snc.LVMVolumeGroup,
	schedulerCache *cache.CacheManager,
	pvcReq *PVCRequest,
	commonLVG *LVMVolumeGroup,
	log *logger.Logger,
	pvc *corev1.PersistentVolumeClaim,
	nodeName string,
) (resource.Quantity, error) {
	var freeSpace resource.Quantity

	switch pvcReq.DeviceType {
	case consts.Thick:
		freeSpace = lvg.Status.VGFree
		log.Trace(fmt.Sprintf("[scoreNodes] LVMVolumeGroup %s free Thick space before PVC reservation: %s", lvg.Name, freeSpace.String()))

		reserved, err := schedulerCache.GetLVGThickReservedSpace(lvg.Name)
		if err != nil {
			return freeSpace, fmt.Errorf("[scoreNodes] unable to count reserved space for the LVMVolumeGroup %s", lvg.Name)
		}
		log.Trace(fmt.Sprintf("[scoreNodes] LVMVolumeGroup %s PVC Space reservation: %s", lvg.Name, resource.NewQuantity(reserved, resource.BinarySI)))

		freeSpace = *resource.NewQuantity(freeSpace.Value()-reserved, resource.BinarySI)
		log.Trace(fmt.Sprintf("[scoreNodes] LVMVolumeGroup %s free Thick space after PVC reservation: %s", lvg.Name, freeSpace.String()))
	case consts.Thin:
		thinPool := findMatchedThinPool(lvg.Status.ThinPools, commonLVG.Thin.PoolName)
		if thinPool == nil {
			return freeSpace, fmt.Errorf("[scoreNodes] unable to match Storage Class's ThinPools with the node's one, Storage Class: %s, node: %s", *pvc.Spec.StorageClassName, nodeName)
		}

		freeSpace = thinPool.AvailableSpace
	}

	return freeSpace, nil
}

// TODO pick better naming to freeSize and method name +++
func getFreeSpaceLeftAsPercent(freeSpaceBytes, requestedSpace, totalSpace int64) int64 {
	freeSpaceLeft := freeSpaceBytes - requestedSpace
	fraction := float64(freeSpaceLeft) / float64(totalSpace)
	percent := fraction * 100
	return int64(percent)
}

// TODO change divisor to multiplier +++
func getNodeScore(freeSpace int64, multiplier float64) int {
	converted := int(math.Round(math.Log2(float64(freeSpace) * multiplier)))
	switch {
	case converted < 1:
		return 1
	case converted > 10:
		return 10
	default:
		return converted
	}
}

// func isDrbdDiskfulNode(drbdResourceMap map[string]*srv.DRBDResource, pvName string, nodeName string) bool {
// 	//TODO implement logic later when DRBDResource becomes available in the cluster
// 	return true
// 	// resource, found := drbdResourceMap[pvName]
// 	// if !found {
// 	// 	return false
// 	// }

// 	// for _, node := range resource.Spec.Peers {
// 	// 	if node.NodeName == nodeName && !node.Diskless {
// 	// 		return true
// 	// 	}
// 	// }

// 	// return false
// }

func isOkNode(_ string) bool {
	// TODO implement node online check
	return true
}

func getRSCByCS(ctx context.Context, cl client.Client, scs map[string]*v1.StorageClass, log *logger.Logger) (map[string]*srv.ReplicatedStorageClass, map[string]*slv.LocalStorageClass, error) {
	SRVresult := map[string]*srv.ReplicatedStorageClass{}
	SLVresult := map[string]*slv.LocalStorageClass{}

	rscList := &srv.ReplicatedStorageClassList{}
	err := cl.List(ctx, rscList)
	if err != nil {
		log.Error(err, "[getRSCByCS] failed to list replicated storage classes")
		return nil, nil, err
	}

	rscMap := make(map[string]*srv.ReplicatedStorageClass, len(rscList.Items))
	for _, rsc := range rscList.Items {
		rscMap[rsc.Name] = &rsc
	}

	lscList := &slv.LocalStorageClassList{}
	err = cl.List(ctx, lscList)
	if err != nil {
		log.Error(err, "[getRSCByCS] failed to list local storage classes")
		return nil, nil, err
	}

	lscMap := make(map[string]*slv.LocalStorageClass, len(lscList.Items))
	for _, lsc := range lscList.Items {
		lscMap[lsc.Name] = &lsc
	}

	for _, sc := range scs {
		if sc.Provisioner == consts.SdsReplicatedVolumeProvisioner {
			SRVresult[sc.Name] = rscMap[sc.Name]
		}
		if sc.Provisioner == consts.SdsLocalVolumeProvisioner {
			SLVresult[sc.Name] = lscMap[sc.Name]
		}
	}

	log.Debug("[getRSCByCS]", "replicated storage classes map", SRVresult, "local storage classes map", SLVresult)
	return SRVresult, SLVresult, nil
}

func isDrbdNode(targetNode string, drbdNodesMap map[string]struct{}) bool {
	_, ok := drbdNodesMap[targetNode]
	return ok
}

func nodeHasEnoughSpace(
	pvcRequests map[string]PVCRequest,
	lvgsThickFree map[string]int64,
	lvgsThinFree map[string]map[string]int64,
	commonLVG *LVMVolumeGroup,
	pvc *corev1.PersistentVolumeClaim,
	lvgMap map[string]*snc.LVMVolumeGroup,
	log *logger.Logger,
) bool {
	log.Debug("[nodeHasEnoughSpace] checking space for PVC", "pvc", pvc.Name)

	nodeIsOk := true
	pvcReq := pvcRequests[pvc.Name]
	thickMapMtx := &sync.RWMutex{}
	thinMapMtx := &sync.RWMutex{}

	switch pvcReq.DeviceType {
	case consts.Thick:
		thickMapMtx.RLock()
		freeSpace := lvgsThickFree[commonLVG.Name]
		thickMapMtx.RUnlock()

		if freeSpace < pvcReq.RequestedSize {
			log.Warning("[nodeHasEnoughSpace]", "insufficient thick space for PVC", "pvc", pvc.Name, "freeSpace", freeSpace, "requested", pvcReq.RequestedSize)
			nodeIsOk = false
			break
		}

		thickMapMtx.Lock()
		lvgsThickFree[commonLVG.Name] -= pvcReq.RequestedSize
		thickMapMtx.Unlock()
		log.Trace("[nodeHasEnoughSpace]", "updated thick free space", "lvg", commonLVG.Name, "remaining", lvgsThickFree[commonLVG.Name])

	case consts.Thin:
		lvg := lvgMap[commonLVG.Name]
		targetThinPool := findMatchedThinPool(lvg.Status.ThinPools, commonLVG.Thin.PoolName)

		thinMapMtx.RLock()
		freeSpace := lvgsThinFree[lvg.Name][targetThinPool.Name]
		thinMapMtx.RUnlock()

		if freeSpace < pvcReq.RequestedSize {
			log.Warning("[nodeHasEnoughSpace]", "insufficient thin space for PVC", "pvc", pvc.Name, "freeSpace", freeSpace, "requested", pvcReq.RequestedSize)
			nodeIsOk = false
			break
		}

		thinMapMtx.Lock()
		lvgsThinFree[lvg.Name][targetThinPool.Name] -= pvcReq.RequestedSize
		thinMapMtx.Unlock()
		log.Trace("[nodeHasEnoughSpace]", "updated thin free space", "lvg", lvg.Name, "thinPool", targetThinPool.Name, "remaining", lvgsThinFree[lvg.Name][targetThinPool.Name])
	}

	log.Trace("[nodeHasEnoughSpace]", "space check result", "pvc", pvc.Name, "nodeIsOk", nodeIsOk)
	return nodeIsOk
}

func findMatchedThinPool(thinPools []snc.LVMVolumeGroupThinPoolStatus, name string) *snc.LVMVolumeGroupThinPoolStatus {
	for _, tp := range thinPools {
		if tp.Name == name {
			return &tp
		}
	}

	return nil
}

// func findMatchedLVG(nodeLVGs []*snc.LVMVolumeGroup, scLVGs []srv.ReplicatedStoragePoolLVMVolumeGroups) *srv.ReplicatedStoragePoolLVMVolumeGroups {
// 	nodeLVGNames := make(map[string]struct{}, len(nodeLVGs))
// 	for _, lvg := range nodeLVGs {
// 		nodeLVGNames[lvg.Name] = struct{}{}
// 	}

// 	for _, lvg := range scLVGs {
// 		if _, match := nodeLVGNames[lvg.Name]; match {
// 			return &lvg
// 		}
// 	}

// 	return nil
// }

func findSharedLVG(nodeLVGs []*snc.LVMVolumeGroup, scLVGs []LVMVolumeGroup) *LVMVolumeGroup {
	nodeLVGNames := make(map[string]struct{}, len(nodeLVGs))
	for _, lvg := range nodeLVGs {
		nodeLVGNames[lvg.Name] = struct{}{}
	}

	for _, lvg := range scLVGs {
		if _, match := nodeLVGNames[lvg.Name]; match {
			return &lvg
		}
	}

	return nil
}

// func getAllNodesWithLVGs(ctx context.Context, cl client.Client) (map[string]*snc.LVMVolumeGroup, error) {
// 	result := map[string]*snc.LVMVolumeGroup{}
// 	lvgs := &snc.LVMVolumeGroupList{}
// 	err := cl.List(ctx, lvgs)
// 	if err != nil {
// 		return nil, err
// 	}

// 	for _, lvg := range lvgs.Items {
// 		result[lvg.Spec.Local.NodeName] = &lvg
// 	}

// 	return result, nil
// }

// func getAllLvgsFromPod(pvcs map[string]*corev1.PersistentVolumeClaim, rscMap map[string]*srv.ReplicatedStorageClass, spMap map[string]*srv.ReplicatedStoragePool, lvgMap map[string]*snc.LVMVolumeGroup) map[string]*snc.LVMVolumeGroup {
// 	result := map[string]*snc.LVMVolumeGroup{}

// 	for _, pvc := range pvcs {
// 		scName := *pvc.Spec.StorageClassName
// 		sc, found := rscMap[scName]
// 		if !found {
// 			continue //TODO
// 		}

// 		sp := spMap[sc.Spec.StoragePool]

// 		for _, lvgGr := range sp.Spec.LVMVolumeGroups {
// 			result[lvgGr.Name] = lvgMap[lvgGr.Name]
// 		}
// 	}

// 	return result
// }

func getLVGThinFreeSpaces(lvgs map[string]*snc.LVMVolumeGroup) map[string]map[string]int64 {
	result := make(map[string]map[string]int64, len(lvgs))

	for _, lvg := range lvgs {
		if result[lvg.Name] == nil {
			result[lvg.Name] = make(map[string]int64, len(lvg.Status.ThinPools))
		}

		for _, tp := range lvg.Status.ThinPools {
			result[lvg.Name][tp.Name] = tp.AvailableSpace.Value()
		}
	}

	return result
}

func getLVGThickFreeSpaces(lvgs map[string]*snc.LVMVolumeGroup) map[string]int64 {
	result := make(map[string]int64, len(lvgs))

	for _, lvg := range lvgs {
		result[lvg.Name] = lvg.Status.VGFree.Value()
	}

	return result
}

// func filterDRBDNodes(nodes []string, sp *srv.ReplicatedStoragePool, lvmGrMap map[string]*snc.LVMVolumeGroup) []string {
// 	result := []string{}
// 	allowedNodes := map[string]struct{}{} // nodes which contain lvgs

// 	for _, lvmVolGr := range sp.Spec.LVMVolumeGroups {
// 		lvmGr, found := lvmGrMap[lvmVolGr.Name]
// 		if !found {
// 			continue
// 		}
// 		allowedNodes[lvmGr.Spec.Local.NodeName] = struct{}{}
// 	}

// 	for _, nodeName := range nodes {
// 		if _, allowed := allowedNodes[nodeName]; allowed {
// 			result = append(result, nodeName)
// 		}
// 	}

// 	return result
// }

type PVCRequest struct {
	DeviceType    string
	RequestedSize int64
}

func extractRequestedSize(
	log *logger.Logger,
	pvcs map[string]*corev1.PersistentVolumeClaim,
	scs map[string]*v1.StorageClass,
	pvs map[string]*corev1.PersistentVolume,
) (map[string]PVCRequest, error) {
	pvcRequests := make(map[string]PVCRequest, len(pvcs))
	for _, pvc := range pvcs {
		sc := scs[*pvc.Spec.StorageClassName]
		log.Debug(fmt.Sprintf("[extractRequestedSize] PVC %s/%s has status phase: %s", pvc.Namespace, pvc.Name, pvc.Status.Phase))
		switch pvc.Status.Phase {
		case corev1.ClaimPending:
			switch sc.Parameters[consts.LvmTypeParamKey] {
			case consts.Thick:
				reqSize := pvc.Spec.Resources.Requests.Storage().Value()
				if reqSize < 0 {
					reqSize = 0
				}
				pvcRequests[pvc.Name] = PVCRequest{
					DeviceType:    consts.Thick,
					RequestedSize: reqSize,
				}
			case consts.Thin:
				reqSize := pvc.Spec.Resources.Requests.Storage().Value()
				if reqSize < 0 {
					reqSize = 0
				}
				pvcRequests[pvc.Name] = PVCRequest{
					DeviceType:    consts.Thin,
					RequestedSize: pvc.Spec.Resources.Requests.Storage().Value(),
				}
			}

		case corev1.ClaimBound:
			pv := pvs[pvc.Spec.VolumeName]
			switch sc.Parameters[consts.LvmTypeParamKey] {
			case consts.Thick:
				reqSize := pvc.Spec.Resources.Requests.Storage().Value() - pv.Spec.Capacity.Storage().Value()
				if reqSize < 0 {
					reqSize = 0
				}
				pvcRequests[pvc.Name] = PVCRequest{
					DeviceType:    consts.Thick,
					RequestedSize: reqSize,
				}
			case consts.Thin:
				reqSize := pvc.Spec.Resources.Requests.Storage().Value() - pv.Spec.Capacity.Storage().Value()
				if reqSize < 0 {
					reqSize = 0
				}
				pvcRequests[pvc.Name] = PVCRequest{
					DeviceType:    consts.Thin,
					RequestedSize: reqSize,
				}
			}
		}
	}

	for name, req := range pvcRequests {
		log.Trace(fmt.Sprintf("[extractRequestedSize] pvc %s has requested size: %d, device type: %s", name, req.RequestedSize, req.DeviceType))
	}

	return pvcRequests, nil
}

func getPodRelatedPVCs(ctx context.Context, cl client.Client, log *logger.Logger, pod *corev1.Pod) (map[string]*corev1.PersistentVolumeClaim, error) {
	pvcMap, err := getAllPVCsFromNamespace(ctx, cl, pod.Namespace)
	if err != nil {
		log.Error(err, fmt.Sprintf("[getUsedPVC] unable to get all PVC for Pod %s in the namespace %s", pod.Name, pod.Namespace))
		return nil, err
	}

	for pvcName := range pvcMap {
		log.Trace(fmt.Sprintf("[getUsedPVC] PVC %s is in namespace %s", pvcName, pod.Namespace))
	}

	usedPvc := make(map[string]*corev1.PersistentVolumeClaim, len(pod.Spec.Volumes))
	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim != nil {
			log.Trace(fmt.Sprintf("[getUsedPVC] Pod %s/%s uses PVC %s", pod.Namespace, pod.Name, volume.PersistentVolumeClaim.ClaimName))
			pvc := pvcMap[volume.PersistentVolumeClaim.ClaimName]
			usedPvc[volume.PersistentVolumeClaim.ClaimName] = &pvc
		}
	}

	return usedPvc, err
}

func getAllPVCsFromNamespace(ctx context.Context, cl client.Client, namespace string) (map[string]corev1.PersistentVolumeClaim, error) {
	list := &corev1.PersistentVolumeClaimList{}
	err := cl.List(ctx, list, &client.ListOptions{Namespace: namespace})
	if err != nil {
		return nil, err
	}

	pvcs := make(map[string]corev1.PersistentVolumeClaim, len(list.Items))
	for _, pvc := range list.Items {
		pvcs[pvc.Name] = pvc
	}

	return pvcs, nil
}

func getStorageClassesUsedByPVCs(ctx context.Context, cl client.Client, pvcs map[string]*corev1.PersistentVolumeClaim) (map[string]*v1.StorageClass, error) {
	scs := &v1.StorageClassList{}
	err := cl.List(ctx, scs)
	if err != nil {
		return nil, err
	}

	scMap := make(map[string]v1.StorageClass, len(scs.Items))
	for _, sc := range scs.Items {
		scMap[sc.Name] = sc
	}

	result := make(map[string]*v1.StorageClass, len(pvcs))
	for _, pvc := range pvcs {
		if pvc.Spec.StorageClassName == nil {
			err = fmt.Errorf("no StorageClass specified for PVC %s", pvc.Name)
			return nil, err
		}

		scName := *pvc.Spec.StorageClassName
		if sc, match := scMap[scName]; match {
			result[sc.Name] = &sc
		}
	}

	fmt.Printf("[getStorageClassesUsedByPVCs] result: %+v\n", result)
	return result, nil
}

func filterPVCsByProvisioner(log *logger.Logger, podRelatedPVCs map[string]*corev1.PersistentVolumeClaim, scsUsedByPodPVCs map[string]*v1.StorageClass) (map[string]*corev1.PersistentVolumeClaim, map[string]*corev1.PersistentVolumeClaim) {
	replicatedPVCs := make(map[string]*corev1.PersistentVolumeClaim, len(podRelatedPVCs))
	localPVCs := make(map[string]*corev1.PersistentVolumeClaim, len(podRelatedPVCs))

	for _, pvc := range podRelatedPVCs {
		sc := scsUsedByPodPVCs[*pvc.Spec.StorageClassName]
		if sc.Provisioner == consts.SdsLocalVolumeProvisioner {
			localPVCs[pvc.Name] = pvc
			continue
		}
		if sc.Provisioner == consts.SdsReplicatedVolumeProvisioner {
			replicatedPVCs[pvc.Name] = pvc
			continue
		}
		log.Debug(fmt.Sprintf("[filterNotManagedPVC] filter out PVC %s/%s due to used Storage class %s is not managed by sds-replicated-volume-provisioner", pvc.Name, pvc.Namespace, sc.Name))
	}

	return replicatedPVCs, localPVCs
}

// func getSortedLVGsFromStorageClasses(replicatedSCs map[string]*srv.ReplicatedStorageClass, spMap map[string]*srv.ReplicatedStoragePool) (map[string][]srv.ReplicatedStoragePoolLVMVolumeGroups, error) {
// 	result := make(map[string][]srv.ReplicatedStoragePoolLVMVolumeGroups, len(replicatedSCs))

// 	for _, sc := range replicatedSCs {
// 		pool := spMap[sc.Spec.StoragePool]
// 		result[sc.Name] = pool.Spec.LVMVolumeGroups
// 	}

// 	return result, nil
// }

func CreateLVGsMapFromStorageClasses(scs map[string]*v1.StorageClass) (map[string][]LVMVolumeGroup, error) {
	result := make(map[string][]LVMVolumeGroup, len(scs))

	for _, sc := range scs {
		lvgs, err := ExtractLVGsFromSC(sc)
		if err != nil {
			return nil, err
		}

		result[sc.Name] = append(result[sc.Name], lvgs...)
	}

	return result, nil
}

func ExtractLVGsFromSC(sc *v1.StorageClass) ([]LVMVolumeGroup, error) {
	lvms, ok := sc.Parameters[consts.LVMVolumeGroupsParamKey]
	if !ok {
		return nil, fmt.Errorf("key is %s not found in StorageClass parameters", consts.LvmTypeParamKey)
	}

	lvms = strings.Trim(lvms, "'")
	var lvmVolumeGroups []LVMVolumeGroup
	err := yaml.Unmarshal([]byte(lvms), &lvmVolumeGroups)
	if err != nil {
		return nil, err
	}
	return lvmVolumeGroups, nil
}

func GetCachedLVGsUsedByPodStorageClases(lvgs map[string]*snc.LVMVolumeGroup, scsLVGs map[string][]LVMVolumeGroup) map[string]*snc.LVMVolumeGroup {
	result := make(map[string]*snc.LVMVolumeGroup, len(lvgs))
	usedLvgs := make(map[string]struct{}, len(lvgs))

	for _, scLvgs := range scsLVGs {
		for _, lvg := range scLvgs {
			usedLvgs[lvg.Name] = struct{}{}
		}
	}

	for _, lvg := range lvgs {
		if _, used := usedLvgs[lvg.Name]; used {
			result[lvg.Name] = lvg
		}
	}

	return result
}

func CreateNodeToCachedLVGsMap(lvgs map[string]*snc.LVMVolumeGroup) map[string][]*snc.LVMVolumeGroup {
	sorted := make(map[string][]*snc.LVMVolumeGroup, len(lvgs))
	for _, lvg := range lvgs {
		for _, node := range lvg.Status.Nodes {
			sorted[node.Name] = append(sorted[node.Name], lvg)
		}
	}

	return sorted
}

// func isOnSameNode(nodeLVGs []*snc.LVMVolumeGroup, scLVGs []LVMVolumeGroup) bool {
// 	nodeLVGNames := make(map[string]struct{}, len(nodeLVGs))
// 	for _, lvg := range nodeLVGs {
// 		nodeLVGNames[lvg.Name] = struct{}{}
// 	}

// 	for _, lvg := range scLVGs {
// 		if _, found := nodeLVGNames[lvg.Name]; !found {
// 			return false
// 		}
// 	}

// 	return true
// }

func findMatchedLVGs(nodeLVGs []*snc.LVMVolumeGroup, scLVGs []LVMVolumeGroup) *LVMVolumeGroup {
	nodeLVGNames := make(map[string]struct{}, len(nodeLVGs))
	for _, lvg := range nodeLVGs {
		nodeLVGNames[lvg.Name] = struct{}{}
	}

	for _, lvg := range scLVGs {
		if _, match := nodeLVGNames[lvg.Name]; match {
			return &lvg
		}
	}

	return nil
}

func getSharedNodesByStorageClasses(podStorageClasses map[string]*v1.StorageClass, nodeToCachedLVGsMap map[string][]*snc.LVMVolumeGroup) (map[string][]*snc.LVMVolumeGroup, error) {
	result := make(map[string][]*snc.LVMVolumeGroup, len(nodeToCachedLVGsMap))

	for nodeName, lvgs := range nodeToCachedLVGsMap {
		lvgNames := make(map[string]struct{}, len(lvgs))
		for _, l := range lvgs {
			lvgNames[l.Name] = struct{}{}
		}

		nodeIncludesLVG := true
		for _, sc := range podStorageClasses {
			scLvgs, err := ExtractLVGsFromSC(sc)
			if err != nil {
				return nil, err
			}

			contains := false
			for _, lvg := range scLvgs {
				if _, exist := lvgNames[lvg.Name]; exist {
					contains = true
					break
				}
			}

			if !contains {
				nodeIncludesLVG = false
				break
			}
		}

		if nodeIncludesLVG {
			result[nodeName] = lvgs
		}
	}

	return result, nil
}

func Status(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, err := w.Write([]byte("ok"))
	if err != nil {
		fmt.Printf("error occurs on status route, err: %s\n", err.Error())
	}
}
