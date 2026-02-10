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
	"math"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/utils/strings/slices"
	"sigs.k8s.io/controller-runtime/pkg/client"

	d8commonapi "github.com/deckhouse/sds-common-lib/api/v1alpha1"
	snc "github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/cache"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/consts"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/logger"
	"github.com/stretchr/testify/assert/yaml"
)

const (
	annotationBetaStorageProvisioner = "volume.beta.kubernetes.io/storage-provisioner"
	annotationStorageProvisioner     = "volume.kubernetes.io/storage-provisioner"

	// IndexFieldLVGNodeName is the field indexer key for LVG -> node mapping.
	IndexFieldLVGNodeName = "status.nodes.name"
)

// PVCRequest is a request for a PVC
type PVCRequest struct {
	DeviceType    string
	RequestedSize int64
}

// SCLVMVolumeGroup represents an LVG reference from a StorageClass parameter.
type SCLVMVolumeGroup struct {
	Name string `yaml:"name"`
	Thin struct {
		PoolName string `yaml:"poolName"`
	} `yaml:"thin"`
}

// SCLVMVolumeGroups is a list of SCLVMVolumeGroup.
type SCLVMVolumeGroups []SCLVMVolumeGroup

// LVGSpaceInfo contains information about available space in LVG
type LVGSpaceInfo struct {
	AvailableSpace int64 // available space considering reservations
	TotalSize      int64 // total LVG size
}

// --- LVG readiness check ---

// isLVGSchedulable checks whether an LVG is eligible for scheduling.
// Returns (true, "") if schedulable, or (false, reason) if not.
// Extend this function to add new readiness conditions (e.g. Unschedulable field).
func isLVGSchedulable(lvg *snc.LVMVolumeGroup) (bool, string) {
	if lvg.Status.Phase != snc.PhaseReady {
		return false, fmt.Sprintf("LVG %s is not ready (phase: %s)", lvg.Name, lvg.Status.Phase)
	}
	// Future: check lvg.Status.Unschedulable, etc.
	return true, ""
}

// --- Available space helpers (combine informer cache + reservation cache) ---

// getAvailableSpace computes available space for a storage pool by combining
// LVG capacity from the informer cache with reserved space from the reservation cache.
func getAvailableSpace(
	ctx context.Context,
	cl client.Client,
	reservationCache *cache.Cache,
	key cache.StoragePoolKey,
) (LVGSpaceInfo, error) {
	lvg := &snc.LVMVolumeGroup{}
	if err := cl.Get(ctx, client.ObjectKey{Name: key.LVGName}, lvg); err != nil {
		return LVGSpaceInfo{}, fmt.Errorf("unable to get LVG %s: %w", key.LVGName, err)
	}

	if ok, reason := isLVGSchedulable(lvg); !ok {
		return LVGSpaceInfo{}, fmt.Errorf("%s", reason)
	}

	var totalFree int64
	var totalSize int64

	if key.ThinPoolName == "" {
		// Thick
		totalFree = lvg.Status.VGFree.Value()
		totalSize = lvg.Status.VGSize.Value()
	} else {
		// Thin
		tp := findMatchedThinPool(lvg.Status.ThinPools, key.ThinPoolName)
		if tp == nil {
			return LVGSpaceInfo{}, fmt.Errorf("thin pool %s not found in LVG %s", key.ThinPoolName, key.LVGName)
		}
		totalFree = tp.AvailableSpace.Value()
		totalSize = lvg.Status.VGSize.Value()
	}

	reserved := reservationCache.GetReservedSpace(key)
	return LVGSpaceInfo{
		AvailableSpace: totalFree - reserved,
		TotalSize:      totalSize,
	}, nil
}

// checkPoolHasSpace checks if a storage pool has enough space for the requested size.
func checkPoolHasSpace(
	ctx context.Context,
	cl client.Client,
	reservationCache *cache.Cache,
	key cache.StoragePoolKey,
	requestedSize int64,
) (bool, error) {
	spaceInfo, err := getAvailableSpace(ctx, cl, reservationCache, key)
	if err != nil {
		return false, err
	}
	return spaceInfo.AvailableSpace >= requestedSize, nil
}

// calculatePoolScore calculates a score (1-10) for a storage pool based on available space.
func calculatePoolScore(
	ctx context.Context,
	cl client.Client,
	reservationCache *cache.Cache,
	key cache.StoragePoolKey,
	requestedSize int64,
	divisor float64,
) (int, error) {
	spaceInfo, err := getAvailableSpace(ctx, cl, reservationCache, key)
	if err != nil {
		return 0, err
	}
	freeSpaceLeft := getFreeSpaceLeftPercent(spaceInfo.AvailableSpace, requestedSize, spaceInfo.TotalSize)
	score := getNodeScore(freeSpaceLeft, divisor)
	return score, nil
}

// storagePoolKeyFromSCLVG creates a StoragePoolKey from an SCLVMVolumeGroup and device type.
func storagePoolKeyFromSCLVG(scLVG SCLVMVolumeGroup, deviceType string) cache.StoragePoolKey {
	key := cache.StoragePoolKey{LVGName: scLVG.Name}
	if deviceType == consts.Thin {
		key.ThinPoolName = scLVG.Thin.PoolName
	}
	return key
}

// getLVGsOnNode returns all LVMVolumeGroups on a node using the field indexer.
func getLVGsOnNode(ctx context.Context, cl client.Client, nodeName string) ([]snc.LVMVolumeGroup, error) {
	var lvgList snc.LVMVolumeGroupList
	if err := cl.List(ctx, &lvgList, client.MatchingFields{IndexFieldLVGNodeName: nodeName}); err != nil {
		return nil, fmt.Errorf("unable to list LVGs for node %s: %w", nodeName, err)
	}
	return lvgList.Items, nil
}

// --- Score calculation helpers ---

// getFreeSpaceLeftPercent calculates the percentage of free space left after placing the requested volume
func getFreeSpaceLeftPercent(freeSize, requestedSpace, totalSize int64) int64 {
	leftFreeSize := freeSize - requestedSpace
	fraction := float64(leftFreeSize) / float64(totalSize)
	percent := fraction * 100
	return int64(percent)
}

// getNodeScore calculates score based on free space
func getNodeScore(freeSpace int64, divisor float64) int {
	converted := int(math.Round(math.Log2(float64(freeSpace) / divisor)))
	switch {
	case converted < 1:
		return 1
	case converted > 10:
		return 10
	default:
		return converted
	}
}

// --- PVC / StorageClass helpers (mostly unchanged) ---

// discoverProvisionerForPVC tries to detect a provisioner for the given PVC using:
// 1) PVC annotations
// 2) StorageClass referenced by the PVC
// 3) PV bound to the PVC
func discoverProvisionerForPVC(
	ctx context.Context,
	cl client.Client,
	log logger.Logger,
	pvc *corev1.PersistentVolumeClaim,
) (string, error) {
	var discoveredProvisioner string

	log.Trace(fmt.Sprintf("[discoverProvisionerForPVC] get pvc: %+v", pvc))
	log.Trace(fmt.Sprintf("[discoverProvisionerForPVC] check provisioner in pvc annotations: %+v", pvc.Annotations))

	// Get provisioner from PVC annotations
	discoveredProvisioner = pvc.Annotations[annotationStorageProvisioner]
	if discoveredProvisioner != "" {
		log.Trace(fmt.Sprintf("[discoverProvisionerForPVC] discovered provisioner in pvc annotations: %s", discoveredProvisioner))
		return discoveredProvisioner, nil
	}

	discoveredProvisioner = pvc.Annotations[annotationBetaStorageProvisioner]
	if discoveredProvisioner != "" {
		log.Trace(fmt.Sprintf("[discoverProvisionerForPVC] discovered provisioner in beta pvc annotations: %s", discoveredProvisioner))
		return discoveredProvisioner, nil
	}

	// Get provisioner from StorageClass
	if pvc.Spec.StorageClassName != nil && *pvc.Spec.StorageClassName != "" {
		log.Trace(fmt.Sprintf("[discoverProvisionerForPVC] can't find provisioner in pvc annotations, check in storageClass with name: %s", *pvc.Spec.StorageClassName))
		storageClass := &storagev1.StorageClass{}
		if err := cl.Get(ctx, client.ObjectKey{Name: *pvc.Spec.StorageClassName}, storageClass); err != nil {
			return "", fmt.Errorf("[discoverProvisionerForPVC] error getting StorageClass %s: %v", *pvc.Spec.StorageClassName, err)
		}
		discoveredProvisioner = storageClass.Provisioner
		log.Trace(fmt.Sprintf("[discoverProvisionerForPVC] discover provisioner %s in storageClass: %+v", discoveredProvisioner, storageClass))
		if discoveredProvisioner != "" {
			return discoveredProvisioner, nil
		}
	}

	// Get provisioner from PV
	if pvc.Spec.VolumeName != "" {
		log.Trace(fmt.Sprintf("[discoverProvisionerForPVC] can't find provisioner in pvc annotations and StorageClass, check in PV with name: %s", pvc.Spec.VolumeName))
		pv := &corev1.PersistentVolume{}
		if err := cl.Get(ctx, client.ObjectKey{Name: pvc.Spec.VolumeName}, pv); err != nil {
			return "", fmt.Errorf("[discoverProvisionerForPVC] error getting PV %s: %v", pvc.Spec.VolumeName, err)
		}

		if pv.Spec.CSI != nil {
			discoveredProvisioner = pv.Spec.CSI.Driver
		}

		log.Trace(fmt.Sprintf("[discoverProvisionerForPVC] discover provisioner %s in PV: %+v", discoveredProvisioner, pv))
	}

	return discoveredProvisioner, nil
}

// Get all node names from the request
func getNodeNames(inputData ExtenderArgs) ([]string, error) {
	if inputData.NodeNames != nil && len(*inputData.NodeNames) > 0 {
		return *inputData.NodeNames, nil
	}

	if inputData.Nodes != nil && len(inputData.Nodes.Items) > 0 {
		nodeNames := make([]string, 0, len(inputData.Nodes.Items))
		for _, node := range inputData.Nodes.Items {
			nodeNames = append(nodeNames, node.Name)
		}
		return nodeNames, nil
	}

	return nil, fmt.Errorf("no nodes provided")
}

// Get all PVCs from the Pod which are managed by our modules
func getManagedPVCsFromPod(ctx context.Context, cl client.Client, log logger.Logger, pod *corev1.Pod, targetProvisioners []string) (map[string]*corev1.PersistentVolumeClaim, error) {
	var discoveredProvisioner string
	managedPVCs := make(map[string]*corev1.PersistentVolumeClaim, len(pod.Spec.Volumes))
	var useLinstor *bool
	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim != nil {
			pvcName := volume.PersistentVolumeClaim.ClaimName
			log = log.WithValues("PVC", pvcName)

			pvc := &corev1.PersistentVolumeClaim{}
			err := cl.Get(ctx, client.ObjectKey{Namespace: pod.Namespace, Name: pvcName}, pvc)
			if err != nil {
				return nil, fmt.Errorf("[getManagedPVCsFromPod] error getting PVC: %v", err)
			}

			discoveredProvisioner, err = discoverProvisionerForPVC(ctx, cl, log, pvc)
			if err != nil {
				return nil, fmt.Errorf("[getManagedPVCsFromPod] error getting provisioner: %v", err)
			}
			log.Trace(fmt.Sprintf("[getManagedPVCsFromPod] discovered provisioner: %s", discoveredProvisioner))

			if !slices.Contains(targetProvisioners, discoveredProvisioner) {
				log.Debug(fmt.Sprintf("[getManagedPVCsFromPod] provisioner not matches targetProvisioners %+v", targetProvisioners))
				continue
			}

			if discoveredProvisioner == consts.SdsReplicatedVolumeProvisioner {
				if useLinstor == nil {
					useLinstor, err = getUseLinstor(ctx, cl, log)
					if err != nil {
						return nil, fmt.Errorf("[getManagedPVCsFromPod] error getting useLinstor: %v", err)
					}
				}

				if *useLinstor {
					log.Debug("[getManagedPVCsFromPod] filter out PVC due to used provisioner is managed by the Linstor")
					continue
				}
			}

			log.Debug("[getManagedPVCsFromPod] add PVC to the managed PVCs")
			managedPVCs[pvcName] = pvc
		}
	}

	return managedPVCs, nil
}

// Get all StorageClasses used by the PVCs
func getStorageClassesUsedByPVCs(ctx context.Context, cl client.Client, pvcs map[string]*corev1.PersistentVolumeClaim) (map[string]*storagev1.StorageClass, error) {
	scs := &storagev1.StorageClassList{}
	err := cl.List(ctx, scs)
	if err != nil {
		return nil, err
	}

	scMap := make(map[string]storagev1.StorageClass, len(scs.Items))
	for _, sc := range scs.Items {
		scMap[sc.Name] = sc
	}

	result := make(map[string]*storagev1.StorageClass, len(pvcs))
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

	return result, nil
}

// Get useLinstor value from the sds-replication-volume ModuleConfig
func getUseLinstor(ctx context.Context, cl client.Client, log logger.Logger) (*bool, error) {
	_true := true
	_false := false
	mc := &d8commonapi.ModuleConfig{}
	err := cl.Get(ctx, client.ObjectKey{Name: "sds-replicated-volume"}, mc)
	if err != nil {
		if client.IgnoreNotFound(err) == nil {
			log.Debug("[getUseLinstor] ModuleConfig sds-replicated-volume not found. Assume useLinstor is true")
			return &_true, nil
		}
		return &_true, err
	}

	if value, exists := mc.Spec.Settings["useLinstor"]; exists && value == true {
		log.Debug("[getUseLinstor] ModuleConfig sds-replicated-volume found. Assume useLinstor is true")
		return &_true, nil
	}

	log.Debug("[getUseLinstor] ModuleConfig sds-replicated-volume found. Assume useLinstor is false")
	return &_false, nil
}

// extractRequestedSize extracts the requested size from the PVC based on the PVC status phase and the StorageClass parameters.
func extractRequestedSize(
	ctx context.Context,
	cl client.Client,
	log logger.Logger,
	pvcs map[string]*corev1.PersistentVolumeClaim,
	scs map[string]*storagev1.StorageClass,
) (map[string]PVCRequest, error) {
	pvcRequests := make(map[string]PVCRequest, len(pvcs))
	for _, pvc := range pvcs {
		sc := scs[*pvc.Spec.StorageClassName]
		log.Debug(fmt.Sprintf("[extractRequestedSize] PVC %s/%s has status phase: %s", pvc.Namespace, pvc.Name, pvc.Status.Phase))

		var deviceType string
		isReplicated := sc.Provisioner == consts.SdsReplicatedVolumeProvisioner

		if isReplicated {
			rsc, err := getReplicatedStorageClassForExtract(ctx, cl, sc.Name)
			if err != nil {
				log.Error(err, fmt.Sprintf("[extractRequestedSize] unable to get RSC for SC %s", sc.Name))
				continue
			}
			rsp, err := getReplicatedStoragePoolForExtract(ctx, cl, rsc.Spec.StoragePool)
			if err != nil {
				log.Error(err, fmt.Sprintf("[extractRequestedSize] unable to get RSP %s", rsc.Spec.StoragePool))
				continue
			}
			switch rsp.Spec.Type {
			case consts.RSPTypeLVM:
				deviceType = consts.Thick
			case consts.RSPTypeLVMThin:
				deviceType = consts.Thin
			default:
				deviceType = consts.Thick
			}
		} else {
			deviceType = sc.Parameters[consts.LvmTypeParamKey]
		}

		if deviceType == "" {
			log.Debug(fmt.Sprintf("[extractRequestedSize] unable to determine device type for PVC %s/%s", pvc.Namespace, pvc.Name))
			continue
		}

		switch pvc.Status.Phase {
		case corev1.ClaimPending:
			pvcRequests[pvc.Name] = PVCRequest{
				DeviceType:    deviceType,
				RequestedSize: pvc.Spec.Resources.Requests.Storage().Value(),
			}

		case corev1.ClaimBound:
			pv := &corev1.PersistentVolume{}
			if err := cl.Get(ctx, client.ObjectKey{Name: pvc.Spec.VolumeName}, pv); err != nil {
				return nil, fmt.Errorf("[extractRequestedSize] error getting PV %s: %v", pvc.Spec.VolumeName, err)
			}
			pvcRequests[pvc.Name] = PVCRequest{
				DeviceType:    deviceType,
				RequestedSize: pvc.Spec.Resources.Requests.Storage().Value() - pv.Spec.Capacity.Storage().Value(),
			}
		}
	}

	return pvcRequests, nil
}

// getReplicatedStorageClassForExtract retrieves RSC by SC name for extractRequestedSize
func getReplicatedStorageClassForExtract(ctx context.Context, cl client.Client, scName string) (*snc.ReplicatedStorageClass, error) {
	rsc := &snc.ReplicatedStorageClass{}
	err := cl.Get(ctx, client.ObjectKey{Name: scName}, rsc)
	if err != nil {
		return nil, fmt.Errorf("unable to get ReplicatedStorageClass %s: %w", scName, err)
	}
	return rsc, nil
}

// getReplicatedStoragePoolForExtract retrieves RSP by name for extractRequestedSize
func getReplicatedStoragePoolForExtract(ctx context.Context, cl client.Client, rspName string) (*snc.ReplicatedStoragePool, error) {
	rsp := &snc.ReplicatedStoragePool{}
	err := cl.Get(ctx, client.ObjectKey{Name: rspName}, rsp)
	if err != nil {
		return nil, fmt.Errorf("unable to get ReplicatedStoragePool %s: %w", rspName, err)
	}
	return rsp, nil
}

// --- SC LVG extraction helpers ---

// GetLVGsFromStorageClasses gets LVMVolumeGroups from StorageClasses.
func GetLVGsFromStorageClasses(scs map[string]*storagev1.StorageClass) (map[string]SCLVMVolumeGroups, error) {
	result := make(map[string]SCLVMVolumeGroups, len(scs))
	for _, sc := range scs {
		lvgs, err := ExtractLVGsFromSC(sc)
		if err != nil {
			return nil, err
		}
		result[sc.Name] = append(result[sc.Name], lvgs...)
	}
	return result, nil
}

// ExtractLVGsFromSC extracts LVMVolumeGroups from StorageClass parameters.
func ExtractLVGsFromSC(sc *storagev1.StorageClass) (SCLVMVolumeGroups, error) {
	var lvmVolumeGroups SCLVMVolumeGroups
	err := yaml.Unmarshal([]byte(sc.Parameters[consts.LVMVolumeGroupsParamKey]), &lvmVolumeGroups)
	if err != nil {
		return nil, err
	}
	return lvmVolumeGroups, nil
}

// --- LVG matching helpers ---

// findMatchedThinPool finds a thin pool in the LVG by name.
func findMatchedThinPool(thinPools []snc.LVMVolumeGroupThinPoolStatus, name string) *snc.LVMVolumeGroupThinPoolStatus {
	for _, tp := range thinPools {
		if tp.Name == name {
			return &tp
		}
	}
	return nil
}

// findMatchedSCLVG finds a common LVG between node LVGs and SC LVGs.
// Only considers LVGs that pass the isLVGSchedulable check.
func findMatchedSCLVG(nodeLVGs []snc.LVMVolumeGroup, scLVGs SCLVMVolumeGroups) *SCLVMVolumeGroup {
	nodeLVGNames := make(map[string]struct{}, len(nodeLVGs))
	for _, lvg := range nodeLVGs {
		if ok, _ := isLVGSchedulable(&lvg); ok {
			nodeLVGNames[lvg.Name] = struct{}{}
		}
	}

	for _, lvg := range scLVGs {
		if _, match := nodeLVGNames[lvg.Name]; match {
			return &lvg
		}
	}
	return nil
}

// lvgHasNode checks if the LVG belongs to the specified node.
func lvgHasNode(lvg *snc.LVMVolumeGroup, nodeName string) bool {
	for _, n := range lvg.Status.Nodes {
		if n.Name == nodeName {
			return true
		}
	}
	return false
}
