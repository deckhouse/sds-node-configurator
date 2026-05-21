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
	"slices"

	"github.com/stretchr/testify/assert/yaml"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/client"

	d8commonapi "github.com/deckhouse/sds-common-lib/api/v1alpha1"
	snc "github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/cache"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/consts"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/logger"
	srv "github.com/deckhouse/sds-replicated-volume/api/v1alpha1"
)

const (
	annotationBetaStorageProvisioner = "volume.beta.kubernetes.io/storage-provisioner"
	annotationStorageProvisioner     = "volume.kubernetes.io/storage-provisioner"

	// IndexFieldLVGNodeName is the field indexer key for LVG -> node mapping.
	IndexFieldLVGNodeName = "status.nodes.name"

	// IndexFieldLLVLVGName is the field indexer key for LLV -> LVG name mapping.
	IndexFieldLLVLVGName = "spec.lvmVolumeGroupName"
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

// getAvailableSpace computes available space for a storage pool using
// min(llvBased, vgFreeBased) - reserved.
//
// The LLV-based value (totalCapacity - sumAllLLV - unaccountedSpace) reacts
// immediately when new LLV CRs appear, avoiding the VGFree staleness race.
// VGFree serves as a safety net before calibration and when manual LVs are
// created after the last calibration.
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

	var totalCapacity int64
	var reportedFree int64

	if key.ThinPoolName == "" {
		totalCapacity = lvg.Status.VGSize.Value()
		reportedFree = lvg.Status.VGFree.Value()
	} else {
		tp := findMatchedThinPool(lvg.Status.ThinPools, key.ThinPoolName)
		if tp == nil {
			return LVGSpaceInfo{}, fmt.Errorf("thin pool %s not found in LVG %s", key.ThinPoolName, key.LVGName)
		}
		totalCapacity = tp.AllocatedSize.Value() + tp.AvailableSpace.Value()
		reportedFree = tp.AvailableSpace.Value()
	}

	sumAll, err := sumLLVSpace(ctx, cl, key, false)
	if err != nil {
		return LVGSpaceInfo{}, fmt.Errorf("unable to compute LLV space for pool %s: %w", key, err)
	}

	unaccounted := reservationCache.GetUnaccountedSpace(key)
	reserved := reservationCache.GetReservedSpace(key)

	llvBased := totalCapacity - sumAll - unaccounted
	baseFree := llvBased
	if reportedFree < baseFree {
		baseFree = reportedFree
	}

	available := baseFree - reserved

	return LVGSpaceInfo{
		AvailableSpace: available,
		TotalSize:      totalCapacity,
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

// --- LLV space computation ---

// sumLLVSpace sums spec.size for all LLVs on the given storage pool.
// If onlyOnDisk is true, only LLVs whose space is reflected on disk are included:
// phase == Created OR status.actualSize > 0 (e.g. Failed LLVs that were already
// provisioned). This prevents double-counting of on-disk LLVs that are not in
// Created phase (their space is already captured by reportedFree / availableSpace).
// For thick pools (key.ThinPoolName == ""), only LLVs without a thin spec are counted.
// For thin pools, only LLVs matching the thin pool name are counted.
func sumLLVSpace(ctx context.Context, cl client.Client, key cache.StoragePoolKey, onlyOnDisk bool) (int64, error) {
	var llvList snc.LVMLogicalVolumeList
	if err := cl.List(ctx, &llvList, client.MatchingFields{IndexFieldLLVLVGName: key.LVGName}); err != nil {
		return 0, fmt.Errorf("unable to list LLVs for LVG %s: %w", key.LVGName, err)
	}

	var total int64
	for i := range llvList.Items {
		llv := &llvList.Items[i]

		if key.ThinPoolName == "" {
			if llv.Spec.Thin != nil {
				continue
			}
		} else {
			if llv.Spec.Thin == nil || llv.Spec.Thin.PoolName != key.ThinPoolName {
				continue
			}
		}

		if onlyOnDisk {
			if llv.Status == nil {
				continue
			}
			if llv.Status.Phase != snc.PhaseCreated && llv.Status.ActualSize.IsZero() {
				continue
			}
		}

		size, err := resource.ParseQuantity(llv.Spec.Size)
		if err != nil {
			return 0, fmt.Errorf("unable to parse LLV %s spec.size %q: %w", llv.Name, llv.Spec.Size, err)
		}
		total += size.Value()
	}
	return total, nil
}

// --- Unaccounted space calibration ---

// CalibratePoolUnaccountedSpace computes the space occupied by non-LLV volumes
// on the given storage pool and stores it in the cache. Uses on-disk LLVs
// (Created phase OR actualSize > 0) whose sizes are reflected in
// VGFree/AvailableSpace for calibration.
func CalibratePoolUnaccountedSpace(
	ctx context.Context,
	cl client.Client,
	schedulerCache *cache.Cache,
	lvg *snc.LVMVolumeGroup,
	key cache.StoragePoolKey,
) error {
	sumOnDisk, err := sumLLVSpace(ctx, cl, key, true)
	if err != nil {
		return fmt.Errorf("unable to compute on-disk LLV space for pool %s: %w", key, err)
	}

	var totalCapacity int64
	var reportedFree int64

	if key.ThinPoolName == "" {
		totalCapacity = lvg.Status.VGSize.Value()
		reportedFree = lvg.Status.VGFree.Value()
	} else {
		tp := findMatchedThinPool(lvg.Status.ThinPools, key.ThinPoolName)
		if tp == nil {
			return fmt.Errorf("thin pool %s not found in LVG %s", key.ThinPoolName, key.LVGName)
		}
		totalCapacity = tp.AllocatedSize.Value() + tp.AvailableSpace.Value()
		reportedFree = tp.AvailableSpace.Value()
	}

	unaccounted := totalCapacity - sumOnDisk - reportedFree
	if unaccounted < 0 {
		unaccounted = 0
	}

	schedulerCache.SetUnaccountedSpace(key, unaccounted)
	return nil
}

// CalibrateAllPoolsForLVG calibrates unaccounted space for the thick pool
// and all thin pools of the given LVG.
func CalibrateAllPoolsForLVG(
	ctx context.Context,
	cl client.Client,
	schedulerCache *cache.Cache,
	lvg *snc.LVMVolumeGroup,
) error {
	thickKey := cache.StoragePoolKey{LVGName: lvg.Name}
	if err := CalibratePoolUnaccountedSpace(ctx, cl, schedulerCache, lvg, thickKey); err != nil {
		return err
	}

	for _, tp := range lvg.Status.ThinPools {
		thinKey := cache.StoragePoolKey{LVGName: lvg.Name, ThinPoolName: tp.Name}
		if err := CalibratePoolUnaccountedSpace(ctx, cl, schedulerCache, lvg, thinKey); err != nil {
			return err
		}
	}

	return nil
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

	// Get provisioner from StorageClass.
	//
	// Tolerate a missing StorageClass: in Kubernetes, storageClassName is a
	// matching label between PV and PVC, not a hard reference, so PVs can
	// legitimately reference a non-existent SC (statically provisioned PVs,
	// PVs that survived their SC being deleted, SC migrations, etc.). In
	// such cases we fall through to PV-based provisioner discovery instead
	// of failing the whole scheduling request.
	if pvc.Spec.StorageClassName != nil && *pvc.Spec.StorageClassName != "" {
		log.Trace(fmt.Sprintf("[discoverProvisionerForPVC] can't find provisioner in pvc annotations, check in storageClass with name: %s", *pvc.Spec.StorageClassName))
		storageClass := &storagev1.StorageClass{}
		err := cl.Get(ctx, client.ObjectKey{Name: *pvc.Spec.StorageClassName}, storageClass)
		switch {
		case err == nil:
			discoveredProvisioner = storageClass.Provisioner
			log.Trace(fmt.Sprintf("[discoverProvisionerForPVC] discover provisioner %s in storageClass: %+v", discoveredProvisioner, storageClass))
			if discoveredProvisioner != "" {
				return discoveredProvisioner, nil
			}
		case apierrors.IsNotFound(err):
			log.Debug(fmt.Sprintf("[discoverProvisionerForPVC] StorageClass %s not found, falling back to PV-based provisioner discovery", *pvc.Spec.StorageClassName))
		default:
			return "", fmt.Errorf("[discoverProvisionerForPVC] error getting StorageClass %s: %v", *pvc.Spec.StorageClassName, err)
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
	var newControlPlane *bool
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
				if newControlPlane == nil {
					newControlPlane, err = getNewControlPlane(ctx, cl, log)
					if err != nil {
						return nil, fmt.Errorf("[getManagedPVCsFromPod] error getting newControlPlane: %v", err)
					}
				}

				if !*newControlPlane {
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

// getStorageClassesUsedByPVCs fetches only the StorageClasses referenced by PVCs.
//
// It is intentionally tolerant towards PVCs without a StorageClassName and towards
// references to non-existent StorageClass objects: in both cases the corresponding
// entry is just absent from the returned map. Such PVCs represent statically
// provisioned PersistentVolumes (or PVCs that survived their StorageClass being
// removed/renamed) — both are valid scenarios per the Kubernetes API contract,
// where storageClassName is a matching label rather than a hard reference to a
// StorageClass object (see https://kubernetes.io/docs/concepts/storage/persistent-volumes/#class).
//
// Callers should drop PVCs whose StorageClass is missing from the returned map
// (e.g. via dropPVCsWithMissingSC) instead of failing the whole scheduling
// request.
func getStorageClassesUsedByPVCs(ctx context.Context, cl client.Client, pvcs map[string]*corev1.PersistentVolumeClaim) (map[string]*storagev1.StorageClass, error) {
	uniqueSCNames := make(map[string]struct{}, len(pvcs))
	for _, pvc := range pvcs {
		if pvc.Spec.StorageClassName == nil || *pvc.Spec.StorageClassName == "" {
			continue
		}
		uniqueSCNames[*pvc.Spec.StorageClassName] = struct{}{}
	}

	result := make(map[string]*storagev1.StorageClass, len(uniqueSCNames))
	for scName := range uniqueSCNames {
		sc := &storagev1.StorageClass{}
		if err := cl.Get(ctx, client.ObjectKey{Name: scName}, sc); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return nil, fmt.Errorf("unable to get StorageClass %s: %w", scName, err)
		}
		result[sc.Name] = sc
	}

	return result, nil
}

// dropPVCsWithMissingSC removes from managedPVCs all PVCs whose StorageClass is
// not present in scs (either StorageClassName is empty/nil or the SC object does
// not exist in the cluster). It returns the list of dropped PVCs so the caller
// can log them and emit additional diagnostics (e.g. for Pending PVCs whose
// volume can never be provisioned without an existing StorageClass).
//
// Removing such PVCs (instead of failing the whole pod-scheduling request) is the
// behavior consistent with the upstream kube-scheduler VolumeBinding plugin:
// for bound PVCs it relies on the PV's nodeAffinity and never consults the
// StorageClass, so a PV referencing a missing StorageClass schedules normally.
func dropPVCsWithMissingSC(
	managedPVCs map[string]*corev1.PersistentVolumeClaim,
	scs map[string]*storagev1.StorageClass,
) []*corev1.PersistentVolumeClaim {
	var dropped []*corev1.PersistentVolumeClaim
	for pvcName, pvc := range managedPVCs {
		if pvc.Spec.StorageClassName == nil || *pvc.Spec.StorageClassName == "" {
			dropped = append(dropped, pvc)
			delete(managedPVCs, pvcName)
			continue
		}
		if _, ok := scs[*pvc.Spec.StorageClassName]; !ok {
			dropped = append(dropped, pvc)
			delete(managedPVCs, pvcName)
		}
	}
	return dropped
}

// formatDroppedPVCsForLog returns a "namespace/name" list and a separate list of
// Pending PVCs that reference a missing StorageClass — those will never be
// dynamically provisioned and the resulting Pod will get stuck in
// ContainerCreating with FailedMount, so the operator deserves a louder warning
// than the generic "dropped from scheduling decision" message.
func formatDroppedPVCsForLog(dropped []*corev1.PersistentVolumeClaim) (allKeys []string, pendingKeys []string) {
	for _, pvc := range dropped {
		key := pvc.Namespace + "/" + pvc.Name
		allKeys = append(allKeys, key)
		if pvc.Status.Phase == corev1.ClaimPending {
			pendingKeys = append(pendingKeys, key)
		}
	}
	return allKeys, pendingKeys
}

// getNewControlPlane checks whether the sds-replicated-volume module uses the new control plane
// (as opposed to LINSTOR). When true, the extender handles replicated PVC scheduling;
// when false (or MC not found), LINSTOR manages scheduling and the extender skips them.
func getNewControlPlane(ctx context.Context, cl client.Client, log logger.Logger) (*bool, error) {
	_true := true
	_false := false
	mc := &d8commonapi.ModuleConfig{}
	err := cl.Get(ctx, client.ObjectKey{Name: "sds-replicated-volume"}, mc)
	if err != nil {
		if client.IgnoreNotFound(err) == nil {
			log.Debug("[getNewControlPlane] ModuleConfig sds-replicated-volume not found. Assume newControlPlane is false")
			return &_false, nil
		}
		return &_false, err
	}

	if value, exists := mc.Spec.Settings["newControlPlane"]; exists && value == true {
		log.Debug("[getNewControlPlane] ModuleConfig sds-replicated-volume found, newControlPlane is true")
		return &_true, nil
	}

	log.Debug("[getNewControlPlane] ModuleConfig sds-replicated-volume found, newControlPlane is false")
	return &_false, nil
}

// isRawFileLocalSC reports whether the given StorageClass is produced by a
// LocalStorageClass with `spec.rawFile` (loop-device-backed) configuration.
//
// Such StorageClasses share the local-volume provisioner with LVM-backed ones
// but expose no LVM parameters and rely on `allowedTopologies` for node
// placement, so they MUST be skipped by the LVM-aware code paths of this
// extender.
func isRawFileLocalSC(sc *storagev1.StorageClass) bool {
	if sc == nil || sc.Provisioner != consts.SdsLocalVolumeProvisioner {
		return false
	}
	return sc.Parameters[consts.LocalStorageTypeParamKey] == consts.LocalStorageTypeRawFile
}

// extractRequestedSize extracts the requested size from the PVC based on the PVC status phase and the StorageClass parameters.
//
// PVCs whose StorageClass is rawfile-backed (see isRawFileLocalSC) are silently
// skipped: the extender has nothing useful to compute for them, and node
// placement is enforced via `allowedTopologies` on the StorageClass.
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

		if isRawFileLocalSC(sc) {
			log.Debug(fmt.Sprintf("[extractRequestedSize] PVC %s/%s uses rawfile-backed StorageClass %s, skipping LVM-aware extraction", pvc.Namespace, pvc.Name, sc.Name))
			continue
		}

		var deviceType string
		isReplicated := sc.Provisioner == consts.SdsReplicatedVolumeProvisioner

		if isReplicated {
			rsc, err := getReplicatedStorageClassForExtract(ctx, cl, sc.Name)
			if err != nil {
				return nil, fmt.Errorf("[extractRequestedSize] unable to get RSC for SC %s: %w", sc.Name, err)
			}
			rspName := rscStoragePoolName(rsc)
			rsp, err := getReplicatedStoragePoolForExtract(ctx, cl, rspName)
			if err != nil {
				return nil, fmt.Errorf("[extractRequestedSize] unable to get RSP %s: %w", rspName, err)
			}
			switch rsp.Spec.Type {
			case srv.ReplicatedStoragePoolTypeLVM:
				deviceType = consts.Thick
			case srv.ReplicatedStoragePoolTypeLVMThin:
				deviceType = consts.Thin
			default:
				deviceType = consts.Thick
			}
		} else {
			deviceType = sc.Parameters[consts.LvmTypeParamKey]
		}

		if deviceType == "" {
			return nil, fmt.Errorf("[extractRequestedSize] unable to determine device type for PVC %s/%s", pvc.Namespace, pvc.Name)
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

func getReplicatedStorageClassForExtract(ctx context.Context, cl client.Client, scName string) (*srv.ReplicatedStorageClass, error) {
	rsc := &srv.ReplicatedStorageClass{}
	err := cl.Get(ctx, client.ObjectKey{Name: scName}, rsc)
	if err != nil {
		return nil, fmt.Errorf("unable to get ReplicatedStorageClass %s: %w", scName, err)
	}
	return rsc, nil
}

func getReplicatedStoragePoolForExtract(ctx context.Context, cl client.Client, rspName string) (*srv.ReplicatedStoragePool, error) {
	rsp := &srv.ReplicatedStoragePool{}
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
