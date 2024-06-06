/*
Copyright 2023 Flant JSC

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

package controller

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sds-node-configurator/api/v1alpha1"
	"sds-node-configurator/config"
	"sds-node-configurator/internal"
	"sds-node-configurator/pkg/cache"
	"sds-node-configurator/pkg/logger"
	"sds-node-configurator/pkg/monitoring"
	"sds-node-configurator/pkg/utils"
	"strconv"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	kclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	LVMVolumeGroupDiscoverCtrlName = "lvm-volume-group-discover-controller"
)

func RunLVMVolumeGroupDiscoverController(
	mgr manager.Manager,
	cfg config.Options,
	log logger.Logger,
	metrics monitoring.Metrics,
	sdsCache *cache.Cache,
) (controller.Controller, error) {
	cl := mgr.GetClient()

	c, err := controller.New(LVMVolumeGroupDiscoverCtrlName, mgr, controller.Options{
		Reconciler: reconcile.Func(func(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
			log.Info("[RunLVMVolumeGroupDiscoverController] Reconciler starts LVMVolumeGroup resources reconciliation")

			shouldRequeue := LVMVolumeGroupDiscoverReconcile(ctx, cl, metrics, log, cfg, sdsCache)
			if shouldRequeue {
				log.Warning(fmt.Sprintf("[RunLVMVolumeGroupDiscoverController] an error occured while run the Reconciler func, retry in %s", cfg.VolumeGroupScanIntervalSec.String()))
				return reconcile.Result{
					RequeueAfter: cfg.VolumeGroupScanIntervalSec,
				}, nil
			}
			log.Info("[RunLVMVolumeGroupDiscoverController] Reconciler successfully ended LVMVolumeGroup resources reconciliation")
			return reconcile.Result{}, nil
		}),
	})

	if err != nil {
		log.Error(err, fmt.Sprintf(`[RunLVMVolumeGroupDiscoverController] unable to create controller: "%s"`, LVMVolumeGroupDiscoverCtrlName))
		return nil, err
	}

	return c, err
}

func LVMVolumeGroupDiscoverReconcile(ctx context.Context, cl kclient.Client, metrics monitoring.Metrics, log logger.Logger, cfg config.Options, sdsCache *cache.Cache) bool {
	reconcileStart := time.Now()
	log.Info("[RunLVMVolumeGroupDiscoverController] starts the reconciliation")

	currentLVMVGs, err := GetAPILVMVolumeGroups(ctx, cl, metrics)
	if err != nil {
		log.Error(err, "[RunLVMVolumeGroupDiscoverController] unable to run GetAPILVMVolumeGroups")
		return true
	}

	if len(currentLVMVGs) == 0 {
		log.Debug("[RunLVMVolumeGroupDiscoverController] no current LVMVolumeGroups found")
	}

	blockDevices, err := GetAPIBlockDevices(ctx, cl, metrics)
	if err != nil {
		log.Error(err, "[RunLVMVolumeGroupDiscoverController] unable to GetAPIBlockDevices")
		for _, lvg := range currentLVMVGs {
			err = updateLVGConditionIfNeeded(ctx, cl, log, &lvg, metav1.ConditionFalse, internal.VGReadyType, "NoBlockDevices", fmt.Sprintf("unable to get block devices resources, err: %s", err.Error()))
			if err != nil {
				log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupDiscoverController] unable to add a condition %s to the LVMVolumeGroup %s", internal.VGReadyType, lvg.Name))
			}

			if err = updateLVMVolumeGroupHealthStatus(ctx, cl, metrics, &lvg, NonOperational, err.Error()); err != nil {
				log.Error(err, fmt.Sprintf(`[RunLVMVolumeGroupDiscoverController] unable to change health param in LVMVolumeGroup, name: "%s"`, lvg.Name))
			}
		}
		return true
	}

	if len(blockDevices) == 0 {
		log.Info("[RunLVMVolumeGroupDiscoverController] no BlockDevices were found")
		return false
	}

	filteredLVGs := filterLVGsByNode(ctx, cl, log, metrics, currentLVMVGs, blockDevices, cfg.NodeName)

	log.Debug("[RunLVMVolumeGroupDiscoverController] tries to get LVMVolumeGroup candidates")
	candidates, err := GetLVMVolumeGroupCandidates(log, sdsCache, blockDevices, cfg.NodeName)
	if err != nil {
		log.Error(err, "[RunLVMVolumeGroupDiscoverController] unable to run GetLVMVolumeGroupCandidates")
		for _, lvg := range filteredLVGs {
			log.Trace(fmt.Sprintf("[RunLVMVolumeGroupDiscoverController] turn LVMVolumeGroup %s to non operational. LVG struct: %+v ", lvg.Name, lvg))
			err = updateLVGConditionIfNeeded(ctx, cl, log, &lvg, metav1.ConditionFalse, internal.VGReadyType, "DataConfigurationFailed", fmt.Sprintf("unable to configure data, err: %s", err.Error()))
			if err != nil {
				log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupDiscoverController] unable to add a condition %s to the LVMVolumeGroup %s", internal.VGReadyType, lvg.Name))
			}
		}
		return true
	}
	log.Debug("[RunLVMVolumeGroupDiscoverController] successfully got LVMVolumeGroup candidates")

	if len(candidates) == 0 {
		log.Debug("[RunLVMVolumeGroupDiscoverController] no candidates were found on the node")
	}

	shouldRequeue := false
	for _, candidate := range candidates {
		if lvg, exist := filteredLVGs[candidate.ActualVGNameOnTheNode]; exist {
			log.Debug(fmt.Sprintf("[RunLVMVolumeGroupDiscoverController] the LVMVolumeGroup %s is already exist. Tries to update it", lvg.Name))
			log.Trace(fmt.Sprintf("[RunLVMVolumeGroupDiscoverController] candidate: %v", candidate))
			log.Trace(fmt.Sprintf("[RunLVMVolumeGroupDiscoverController] lvg: %v", lvg))

			if !hasLVMVolumeGroupDiff(log, lvg, candidate) {
				log.Debug(fmt.Sprintf(`[RunLVMVolumeGroupDiscoverController] no data to update for LvmVolumeGroup, name: "%s"`, lvg.Name))
				err = updateLVGConditionIfNeeded(ctx, cl, log, &lvg, metav1.ConditionTrue, internal.VGReadyType, internal.Updated, "ready to create LV")
				if err != nil {
					log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupDiscoverController] unable to add a condition %s to the LVMVolumeGroup %s", internal.VGReadyType, lvg.Name))
					shouldRequeue = true
				}
				continue
			}

			log.Debug(fmt.Sprintf("[RunLVMVolumeGroupDiscoverController] the LvmVolumeGroup %s should be updated", lvg.Name))
			if err = UpdateLVMVolumeGroupByCandidate(ctx, cl, metrics, log, &lvg, candidate); err != nil {
				log.Error(err, fmt.Sprintf(`[RunLVMVolumeGroupDiscoverController] unable to update LvmVolumeGroup, name: "%s". Requeue the request in %s`,
					lvg.Name, cfg.VolumeGroupScanIntervalSec.String()))
				shouldRequeue = true
				continue
			}

			log.Info(fmt.Sprintf(`[RunLVMVolumeGroupDiscoverController] updated LvmVolumeGroup, name: "%s"`, lvg.Name))

		} else {
			log.Debug(fmt.Sprintf("[RunLVMVolumeGroupDiscoverController] the LVMVolumeGroup %s is not yet created. Create it", lvg.Name))
			lvm, err := CreateLVMVolumeGroupByCandidate(ctx, log, metrics, cl, candidate)
			if err != nil {
				log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupDiscoverController] unable to CreateLVMVolumeGroupByCandidate %s. Requeue the request in %s", candidate.LVMVGName, cfg.VolumeGroupScanIntervalSec.String()))
				shouldRequeue = true
				continue
			}

			err = updateLVGConditionIfNeeded(ctx, cl, log, &lvg, metav1.ConditionTrue, internal.VGConfigurationAppliedType, "Success", "all configuration has been applied")
			if err != nil {
				log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupDiscoverController] unable to add a condition %s to the LVMVolumeGroup %s", internal.VGConfigurationAppliedType, lvg.Name))
				shouldRequeue = true
				continue
			}

			err = updateLVGConditionIfNeeded(ctx, cl, log, &lvg, metav1.ConditionTrue, internal.VGReadyType, internal.Updated, "ready to create LV")
			if err != nil {
				log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupDiscoverController] unable to add a condition %s to the LVMVolumeGroup %s", internal.VGReadyType, lvg.Name))
				shouldRequeue = true
				continue
			}

			log.Info(fmt.Sprintf(`[RunLVMVolumeGroupDiscoverController] created new APILVMVolumeGroup, name: "%s"`, lvm.Name))
		}
	}

	if shouldRequeue {
		log.Warning(fmt.Sprintf("[RunLVMVolumeGroupDiscoverController] some problems have been occurred while iterating the lvmvolumegroup resources. Retry the reconcile in %s", cfg.VolumeGroupScanIntervalSec.String()))
		return true
	}

	err = ClearLVMVolumeGroupResources(ctx, cl, log, metrics, candidates, filteredLVGs, cfg.NodeName)
	if err != nil {
		log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupDiscoverController] an error has occurred while clearing the LVMVolumeGroups resources. Requeue the request in %s", cfg.VolumeGroupScanIntervalSec.String()))
		return true
	}

	log.Info("[RunLVMVolumeGroupDiscoverController] END discovery loop")
	metrics.ReconcileDuration(LVMVolumeGroupDiscoverCtrlName).Observe(metrics.GetEstimatedTimeInSeconds(reconcileStart))
	metrics.ReconcilesCountTotal(LVMVolumeGroupDiscoverCtrlName).Inc()
	return false
}

func filterLVGsByNode(
	ctx context.Context,
	cl kclient.Client,
	log logger.Logger,
	metrics monitoring.Metrics,
	lvgs map[string]v1alpha1.LvmVolumeGroup,
	blockDevices map[string]v1alpha1.BlockDevice,
	currentNode string,
) map[string]v1alpha1.LvmVolumeGroup {

	filtered := make(map[string]v1alpha1.LvmVolumeGroup, len(lvgs))
	blockDevicesNodes := make(map[string]string, len(blockDevices))

	for _, bd := range blockDevices {
		blockDevicesNodes[bd.Name] = bd.Status.NodeName
	}

	for _, lvg := range lvgs {
		switch lvg.Spec.Type {
		case Local:
			currentNodeDevices := 0
			for _, bdName := range lvg.Spec.BlockDeviceNames {
				if blockDevicesNodes[bdName] == currentNode {
					currentNodeDevices++
				}
			}

			// If we did not add every block device of local VG, that means a mistake, and we turn the resource's health to Nonoperational.
			if currentNodeDevices > 0 && currentNodeDevices < len(lvg.Spec.BlockDeviceNames) {
				if err := updateLVMVolumeGroupHealthStatus(ctx, cl, metrics, &lvg, NonOperational, "there are block devices from different nodes for local volume group"); err != nil {
					log.Error(err, `[filterLVGsByNode] unable to update resource, name: "%s"`, lvg.Name)
					continue
				}
			}

			// If we did not find any block device for our node, we skip the resource.
			if currentNodeDevices == 0 {
				continue
			}

			// Otherwise, we add the resource to the filtered ones.
			filtered[lvg.Spec.ActualVGNameOnTheNode] = lvg
		case Shared:
			if len(lvg.Spec.BlockDeviceNames) != 1 {
				if err := updateLVMVolumeGroupHealthStatus(ctx, cl, metrics, &lvg, NonOperational, "there are more than one block devices for shared volume group"); err != nil {
					log.Error(err, `[filterLVGsByNode] unable to update resource, name: "%s"`, lvg.Name)
					continue
				}
			}

			// If the only one block devices does not belong to our node, we skip the resource.
			singleBD := lvg.Spec.BlockDeviceNames[0]
			if blockDevicesNodes[singleBD] != currentNode {
				continue
			}

			// Otherwise, we add the resource to the filtered ones.
			filtered[lvg.Spec.ActualVGNameOnTheNode] = lvg
		}
	}

	return filtered
}

func hasLVMVolumeGroupDiff(log logger.Logger, res v1alpha1.LvmVolumeGroup, candidate internal.LVMVolumeGroupCandidate) bool {
	convertedStatusPools := convertStatusThinPools(candidate.StatusThinPools)
	log.Trace(fmt.Sprintf(`AllocatedSize, candidate: %s, res: %s`, candidate.AllocatedSize.String(), res.Status.AllocatedSize.String()))
	log.Trace(fmt.Sprintf(`ThinPools, candidate: %+v, res: %+v`, convertedStatusPools, res.Status.ThinPools))
	for _, tp := range convertedStatusPools {
		log.Trace(fmt.Sprintf("Candidate ThinPool name: %s, actual size: %s, used size: %s", tp.Name, tp.ActualSize.String(), tp.UsedSize.String()))
	}
	for _, tp := range res.Status.ThinPools {
		log.Trace(fmt.Sprintf("Resource ThinPool name: %s, actual size: %s, used size: %s", tp.Name, tp.ActualSize.String(), tp.UsedSize.String()))
	}
	log.Trace(fmt.Sprintf(`VGSize, candidate: %s, res: %s`, candidate.VGSize.String(), res.Status.VGSize.String()))
	log.Trace(fmt.Sprintf(`VGUuid, candidate: %s, res: %s`, candidate.VGUuid, res.Status.VGUuid))
	log.Trace(fmt.Sprintf(`Nodes, candidate: %+v, res: %+v`, convertLVMVGNodes(candidate.Nodes), res.Status.Nodes))

	return candidate.AllocatedSize.Value() != res.Status.AllocatedSize.Value() ||
		hasStatusPoolDiff(convertedStatusPools, res.Status.ThinPools) ||
		candidate.VGSize.Value() != res.Status.VGSize.Value() ||
		candidate.VGUuid != res.Status.VGUuid ||
		hasStatusNodesDiff(log, convertLVMVGNodes(candidate.Nodes), res.Status.Nodes)
}

func hasStatusNodesDiff(log logger.Logger, first, second []v1alpha1.LvmVolumeGroupNode) bool {
	if len(first) != len(second) {
		return true
	}

	for i := range first {
		if first[i].Name != second[i].Name {
			return true
		}

		if len(first[i].Devices) != len(second[i].Devices) {
			return true
		}

		for j := range first[i].Devices {
			log.Trace(fmt.Sprintf("[hasStatusNodesDiff] first Device: name %s, PVSize %s, DevSize %s", first[i].Devices[j].BlockDevice, first[i].Devices[j].PVSize.String(), first[i].Devices[j].DevSize.String()))
			log.Trace(fmt.Sprintf("[hasStatusNodesDiff] second Device: name %s, PVSize %s, DevSize %s", second[i].Devices[j].BlockDevice, second[i].Devices[j].PVSize.String(), second[i].Devices[j].DevSize.String()))
			fmt.Println("OLD path: ", first[i].Devices[j].Path)
			fmt.Println("NEW path: ", second[i].Devices[j].Path)
			if first[i].Devices[j].BlockDevice != second[i].Devices[j].BlockDevice ||
				first[i].Devices[j].Path != second[i].Devices[j].Path ||
				first[i].Devices[j].PVUuid != second[i].Devices[j].PVUuid ||
				first[i].Devices[j].PVSize.Value() != second[i].Devices[j].PVSize.Value() ||
				first[i].Devices[j].DevSize.Value() != second[i].Devices[j].DevSize.Value() {
				return true
			}
		}
	}

	return false
}

func hasStatusPoolDiff(first, second []v1alpha1.StatusThinPool) bool {
	if len(first) != len(second) {
		return true
	}

	for i := range first {
		if first[i].Name != second[i].Name ||
			first[i].UsedSize.Value() != second[i].UsedSize.Value() ||
			first[i].ActualSize.Value() != second[i].ActualSize.Value() ||
			first[i].Ready != second[i].Ready ||
			first[i].Message != second[i].Message {
			return true
		}
	}

	return false
}

// ClearLVMVolumeGroupResources Removes deprecated nodes and resources.
func ClearLVMVolumeGroupResources(
	ctx context.Context,
	cl kclient.Client,
	log logger.Logger,
	metrics monitoring.Metrics,
	candidates []internal.LVMVolumeGroupCandidate,
	lvgs map[string]v1alpha1.LvmVolumeGroup,
	currentNode string,
) error {
	actualVGs := make(map[string]struct{}, len(candidates))
	for _, candidate := range candidates {
		actualVGs[candidate.ActualVGNameOnTheNode] = struct{}{}
	}

	fmt.Println("ACTUAL VGS LEN: ", len(actualVGs))

	for _, lvg := range lvgs {
		if !reflect.ValueOf(lvg.Status.VGUuid).IsZero() {
			if _, exist := actualVGs[lvg.Spec.ActualVGNameOnTheNode]; !exist {
				log.Debug(fmt.Sprintf(`[ClearLVMVolumeGroupResources] Node "%s" does not belong to VG "%s". 
It will be removed from LVM resource, name "%s"'`, currentNode, lvg.Spec.ActualVGNameOnTheNode, lvg.Name))
				for i, node := range lvg.Status.Nodes {
					if node.Name == currentNode {
						// delete node
						lvg.Status.Nodes = append(lvg.Status.Nodes[:i], lvg.Status.Nodes[i+1:]...)
						log.Info(fmt.Sprintf(`[ClearLVMVolumeGroupResources] deleted node, name: "%s", 
from LVMVolumeGroup, name: "%s"`, node.Name, lvg.Name))
					}
				}

				// If current LVMVolumeGroup has no nodes left, and it is not cause of errors, delete it.
				if len(lvg.Status.Nodes) == 0 {
					if err := DeleteLVMVolumeGroup(ctx, cl, metrics, lvg.Name); err != nil {
						log.Error(err, fmt.Sprintf("Unable to delete LVMVolumeGroup, name: %s", lvg.Name))
						return err
					}

					log.Info(fmt.Sprintf("[ClearLVMVolumeGroupResources] deleted LVMVolumeGroup, name: %s", lvg.Name))
				}
			}
		}
	}

	return nil
}

func DeleteLVMVolumeGroup(ctx context.Context, kc kclient.Client, metrics monitoring.Metrics, lvmvgName string) error {
	lvm := &v1alpha1.LvmVolumeGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name: lvmvgName,
		},
	}

	start := time.Now()
	err := kc.Delete(ctx, lvm)
	metrics.ApiMethodsDuration(LVMVolumeGroupDiscoverCtrlName, "delete").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.ApiMethodsExecutionCount(LVMVolumeGroupDiscoverCtrlName, "delete").Inc()
	if err != nil {
		metrics.ApiMethodsErrors(LVMVolumeGroupDiscoverCtrlName, "delete").Inc()
		return err
	}
	return nil
}

func GetLVMVolumeGroupCandidates(log logger.Logger, sdsCache *cache.Cache, bds map[string]v1alpha1.BlockDevice, currentNode string) ([]internal.LVMVolumeGroupCandidate, error) {
	var candidates []internal.LVMVolumeGroupCandidate

	vgs, vgErrs := sdsCache.GetVGs()
	vgWithTag := filterVGByTag(vgs, internal.LVMTags)

	// If there is no VG with our tag, then there is no any candidate.
	if len(vgWithTag) == 0 {
		return candidates, nil
	}

	// If vgErrs is not empty, that means we have some problems on vgs, so we need to identify unhealthy vgs.
	var vgIssues map[string]string
	if vgErrs.Len() != 0 {
		log.Warning("[GetLVMVolumeGroupCandidates] some errors have been occurred while executing vgs command")
		vgIssues = sortVGIssuesByVG(log, vgWithTag)
	}

	pvs, pvErrs := sdsCache.GetPVs()
	if len(pvs) == 0 {
		err := errors.New("no PV found")
		log.Error(err, "[GetLVMVolumeGroupCandidates] no PV was found, but VG with tags are not empty")
		return nil, err
	}

	// If pvErrs is not empty, that means we have some problems on vgs, so we need to identify unhealthy vgs.
	var pvIssues map[string][]string
	if pvErrs.Len() != 0 {
		log.Warning("[GetLVMVolumeGroupCandidates] some errors have been occurred while executing pvs command")
		pvIssues = sortPVIssuesByVG(log, pvs)
	}

	lvs, lvErrs := sdsCache.GetLVs()
	var thinPools []internal.LVData
	if lvs != nil && len(lvs) > 0 {
		// Filter LV to get only thin pools as we do not support thick for now.
		thinPools = getThinPools(lvs)
	}

	// If lvErrs is not empty, that means we have some problems on vgs, so we need to identify unhealthy vgs.
	var lvIssues map[string][]string
	if lvErrs.Len() != 0 {
		log.Warning("[GetLVMVolumeGroupCandidates] some errors have been occurred while executing lvs command")
		lvIssues = sortLVIssuesByVG(log, thinPools)
	}

	// Sort PV,BlockDevices and LV by VG to fill needed information for LVMVolumeGroup resource further.
	sortedPVs := sortPVsByVG(pvs, vgWithTag)
	sortedBDs := sortBlockDevicesByVG(bds, vgWithTag)
	log.Trace(fmt.Sprintf("[GetLVMVolumeGroupCandidates] BlockDevices: %+v", bds))
	log.Trace(fmt.Sprintf("[GetLVMVolumeGroupCandidates] Sorted BlockDevices: %+v", sortedBDs))
	sortedThinPools := sortLVsByVG(thinPools, vgWithTag)

	for _, vg := range vgWithTag {
		allocateSize := vg.VGSize
		allocateSize.Sub(vg.VGFree)

		health, message := checkVGHealth(sortedBDs, vgIssues, pvIssues, lvIssues, vg)

		candidate := internal.LVMVolumeGroupCandidate{
			LVMVGName:             generateLVMVGName(),
			Finalizers:            internal.Finalizers,
			ActualVGNameOnTheNode: vg.VGName,
			BlockDevicesNames:     getBlockDevicesNames(sortedBDs, vg),
			SpecThinPools:         getSpecThinPools(sortedThinPools, vg),
			Type:                  getVgType(vg),
			AllocatedSize:         *resource.NewQuantity(allocateSize.Value(), resource.BinarySI),
			Health:                health,
			Message:               message,
			StatusThinPools:       getStatusThinPools(log, sortedThinPools, vg, lvIssues),
			VGSize:                *resource.NewQuantity(vg.VGSize.Value(), resource.BinarySI),
			VGUuid:                vg.VGUuid,
			Nodes:                 configureCandidateNodeDevices(sortedPVs, sortedBDs, vg, currentNode),
		}

		candidates = append(candidates, candidate)
	}

	return candidates, nil
}

func checkVGHealth(blockDevices map[string][]v1alpha1.BlockDevice, vgIssues map[string]string, pvIssues map[string][]string, lvIssues map[string][]string, vg internal.VGData) (health, message string) {
	issues := make([]string, 0, len(vgIssues)+len(pvIssues)+len(lvIssues)+1)

	if bds, exist := blockDevices[vg.VGName+vg.VGUuid]; !exist || len(bds) == 0 {
		issues = append(issues, fmt.Sprintf("[ERROR] Unable to get block devices for VG, name: %s ; uuid: %s", vg.VGName, vg.VGUuid))
	}

	if vgIssue, exist := vgIssues[vg.VGName+vg.VGUuid]; exist {
		issues = append(issues, vgIssue)
	}

	if pvIssue, exist := pvIssues[vg.VGName+vg.VGUuid]; exist {
		issues = append(issues, strings.Join(pvIssue, ""))
	}

	if lvIssue, exist := lvIssues[vg.VGName+vg.VGUuid]; exist {
		issues = append(issues, strings.Join(lvIssue, ""))
	}

	if len(issues) != 0 {
		result := removeDuplicates(issues)
		return internal.LVMVGHealthNonOperational, strings.Join(result, "")
	}

	return internal.LVMVGHealthOperational, ""
}

func removeDuplicates(strList []string) []string {
	unique := make(map[string]struct{}, len(strList))

	for _, str := range strList {
		if _, ok := unique[str]; !ok {
			unique[str] = struct{}{}
		}
	}

	result := make([]string, 0, len(unique))
	for str := range unique {
		result = append(result, str)
	}
	return result
}

func sortLVIssuesByVG(log logger.Logger, lvs []internal.LVData) map[string][]string {
	var lvIssuesByVG = make(map[string][]string, len(lvs))

	for _, lv := range lvs {
		_, cmd, stdErr, err := utils.GetLV(lv.VGName, lv.LVName)
		log.Debug(fmt.Sprintf("[sortLVIssuesByVG] runs cmd: %s", cmd))

		if err != nil {
			log.Error(err, fmt.Sprintf(`[sortLVIssuesByVG] unable to run lvs command for lv, name: "%s"`, lv.LVName))
			lvIssuesByVG[lv.VGName+lv.VGUuid] = append(lvIssuesByVG[lv.VGName+lv.VGUuid], err.Error())
		}

		if stdErr.Len() != 0 {
			log.Error(fmt.Errorf(stdErr.String()), fmt.Sprintf(`[sortLVIssuesByVG] lvs command for lv "%s" has stderr: `, lv.LVName))
			lvIssuesByVG[lv.VGName+lv.VGUuid] = append(lvIssuesByVG[lv.VGName+lv.VGUuid], stdErr.String())
			stdErr.Reset()
		}
	}

	return lvIssuesByVG
}

func sortPVIssuesByVG(log logger.Logger, pvs []internal.PVData) map[string][]string {
	pvIssuesByVG := make(map[string][]string, len(pvs))

	for _, pv := range pvs {
		_, cmd, stdErr, err := utils.GetPV(pv.PVName)
		log.Debug(fmt.Sprintf("[sortPVIssuesByVG] runs cmd: %s", cmd))

		if err != nil {
			log.Error(err, fmt.Sprintf(`[sortPVIssuesByVG] unable to run pvs command for pv "%s"`, pv.PVName))
			pvIssuesByVG[pv.VGName+pv.VGUuid] = append(pvIssuesByVG[pv.VGName+pv.VGUuid], err.Error())
		}

		if stdErr.Len() != 0 {
			log.Error(fmt.Errorf(stdErr.String()), fmt.Sprintf(`[sortPVIssuesByVG] pvs command for pv "%s" has stderr: %s`, pv.PVName, stdErr.String()))
			pvIssuesByVG[pv.VGName+pv.VGUuid] = append(pvIssuesByVG[pv.VGName+pv.VGUuid], stdErr.String())
			stdErr.Reset()
		}
	}

	return pvIssuesByVG
}

func sortVGIssuesByVG(log logger.Logger, vgs []internal.VGData) map[string]string {
	vgIssues := make(map[string]string, len(vgs))
	for _, vg := range vgs {
		_, cmd, stdErr, err := utils.GetVG(vg.VGName)
		log.Debug(fmt.Sprintf("[sortVGIssuesByVG] runs cmd: %s", cmd))
		if err != nil {
			log.Error(err, fmt.Sprintf(`[sortVGIssuesByVG] unable to run vgs command for vg, name: "%s"`, vg.VGName))
			vgIssues[vg.VGName+vg.VGUuid] = err.Error()
		}

		if stdErr.Len() != 0 {
			log.Error(fmt.Errorf(stdErr.String()), fmt.Sprintf(`[sortVGIssuesByVG] vgs command for vg "%s" has stderr: `, vg.VGName))
			vgIssues[vg.VGName+vg.VGUuid] = stdErr.String()
			stdErr.Reset()
		}
	}

	return vgIssues
}

func sortLVsByVG(lvs []internal.LVData, vgs []internal.VGData) map[string][]internal.LVData {
	result := make(map[string][]internal.LVData, len(vgs))
	for _, vg := range vgs {
		result[vg.VGName+vg.VGUuid] = make([]internal.LVData, 0, len(lvs))
	}

	for _, lv := range lvs {
		if _, ok := result[lv.VGName+lv.VGUuid]; ok {
			result[lv.VGName+lv.VGUuid] = append(result[lv.VGName+lv.VGUuid], lv)
		}
	}

	return result
}

func sortPVsByVG(pvs []internal.PVData, vgs []internal.VGData) map[string][]internal.PVData {
	result := make(map[string][]internal.PVData, len(vgs))
	for _, vg := range vgs {
		result[vg.VGName+vg.VGUuid] = make([]internal.PVData, 0, len(pvs))
	}

	for _, pv := range pvs {
		if _, ok := result[pv.VGName+pv.VGUuid]; ok {
			result[pv.VGName+pv.VGUuid] = append(result[pv.VGName+pv.VGUuid], pv)
		}
	}

	return result
}

func sortBlockDevicesByVG(bds map[string]v1alpha1.BlockDevice, vgs []internal.VGData) map[string][]v1alpha1.BlockDevice {
	result := make(map[string][]v1alpha1.BlockDevice, len(vgs))
	for _, vg := range vgs {
		result[vg.VGName+vg.VGUuid] = make([]v1alpha1.BlockDevice, 0, len(bds))
	}

	for _, bd := range bds {
		if _, ok := result[bd.Status.ActualVGNameOnTheNode+bd.Status.VGUuid]; ok {
			result[bd.Status.ActualVGNameOnTheNode+bd.Status.VGUuid] = append(result[bd.Status.ActualVGNameOnTheNode+bd.Status.VGUuid], bd)
		}
	}

	return result
}

func configureCandidateNodeDevices(pvs map[string][]internal.PVData, bds map[string][]v1alpha1.BlockDevice, vg internal.VGData, currentNode string) map[string][]internal.LVMVGDevice {
	filteredPV := pvs[vg.VGName+vg.VGUuid]
	filteredBds := bds[vg.VGName+vg.VGUuid]
	bdPathStatus := make(map[string]v1alpha1.BlockDevice, len(bds))
	result := make(map[string][]internal.LVMVGDevice, len(filteredPV))

	for _, blockDevice := range filteredBds {
		bdPathStatus[blockDevice.Status.Path] = blockDevice
	}

	for _, pv := range filteredPV {
		device := internal.LVMVGDevice{
			Path:   pv.PVName,
			PVSize: *resource.NewQuantity(pv.PVSize.Value(), resource.BinarySI),
			PVUuid: pv.PVUuid,
		}

		if bd, exist := bdPathStatus[pv.PVName]; exist {
			device.DevSize = *resource.NewQuantity(bd.Status.Size.Value(), resource.BinarySI)
			device.BlockDevice = bd.Name
		}

		result[currentNode] = append(result[currentNode], device)
	}

	return result
}

func getVgType(vg internal.VGData) string {
	if vg.VGShared == "" {
		return "Local"
	}

	return "Shared"
}

func getSpecThinPools(thinPools map[string][]internal.LVData, vg internal.VGData) map[string]resource.Quantity {
	lvs := thinPools[vg.VGName+vg.VGUuid]
	tps := make(map[string]resource.Quantity, len(lvs))

	for _, lv := range lvs {
		tps[lv.LVName] = lv.LVSize
	}

	return tps
}

func getThinPools(lvs []internal.LVData) []internal.LVData {
	thinPools := make([]internal.LVData, 0, len(lvs))

	for _, lv := range lvs {
		if isThinPool(lv) {
			thinPools = append(thinPools, lv)
		}
	}

	return thinPools
}

func getStatusThinPools(log logger.Logger, thinPools map[string][]internal.LVData, vg internal.VGData, lvIssues map[string][]string) []internal.LVMVGStatusThinPool {
	filtered := thinPools[vg.VGName+vg.VGUuid]
	tps := make([]internal.LVMVGStatusThinPool, 0, len(filtered))

	for _, lv := range filtered {
		usedSize, err := getLVUsedSize(lv)
		log.Trace(fmt.Sprintf("[getStatusThinPools] LV %v for VG name %s", lv, vg.VGName))
		if err != nil {
			log.Error(err, "[getStatusThinPools] unable to getLVUsedSize")
		}

		tp := internal.LVMVGStatusThinPool{
			Name:       lv.LVName,
			ActualSize: *resource.NewQuantity(lv.LVSize.Value(), resource.BinarySI),
			UsedSize:   *resource.NewQuantity(usedSize.Value(), resource.BinarySI),
			Ready:      true,
			Message:    "",
		}

		if lverrs, exist := lvIssues[vg.VGName+vg.VGUuid]; exist {
			tp.Ready = false
			tp.Message = strings.Join(lverrs, "")
		}

		tps = append(tps, tp)
	}
	return tps
}

func getLVUsedSize(lv internal.LVData) (*resource.Quantity, error) {
	var (
		err         error
		dataPercent float64
	)

	if lv.DataPercent == "" {
		dataPercent = 0.0
	} else {
		dataPercent, err = strconv.ParseFloat(lv.DataPercent, 64)
		if err != nil {
			return nil, err
		}
	}

	tmp := float64(lv.LVSize.Value()) * dataPercent

	return resource.NewQuantity(int64(tmp), resource.BinarySI), nil
}

func isThinPool(lv internal.LVData) bool {
	return string(lv.LVAttr[0]) == "t"
}

func getBlockDevicesNames(bds map[string][]v1alpha1.BlockDevice, vg internal.VGData) []string {
	sorted := bds[vg.VGName+vg.VGUuid]
	names := make([]string, 0, len(sorted))

	for _, bd := range sorted {
		names = append(names, bd.Name)
	}

	return names
}

func CreateLVMVolumeGroupByCandidate(
	ctx context.Context,
	log logger.Logger,
	metrics monitoring.Metrics,
	kc kclient.Client,
	candidate internal.LVMVolumeGroupCandidate,
) (*v1alpha1.LvmVolumeGroup, error) {
	lvmVolumeGroup := &v1alpha1.LvmVolumeGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:            candidate.LVMVGName,
			OwnerReferences: []metav1.OwnerReference{},
			Finalizers:      candidate.Finalizers,
		},
		Spec: v1alpha1.LvmVolumeGroupSpec{
			ActualVGNameOnTheNode: candidate.ActualVGNameOnTheNode,
			BlockDeviceNames:      candidate.BlockDevicesNames,
			ThinPools:             convertSpecThinPools(candidate.SpecThinPools),
			Type:                  candidate.Type,
		},
		Status: v1alpha1.LvmVolumeGroupStatus{
			AllocatedSize: candidate.AllocatedSize,
			Nodes:         convertLVMVGNodes(candidate.Nodes),
			ThinPools:     convertStatusThinPools(candidate.StatusThinPools),
			VGSize:        candidate.VGSize,
			VGUuid:        candidate.VGUuid,
		},
	}

	for _, node := range candidate.Nodes {
		for _, d := range node {
			i := len(d.BlockDevice)
			if i == 0 {
				log.Warning("The attempt to create the LVG resource failed because it was not possible to find a BlockDevice for it.")
				return lvmVolumeGroup, nil
			}
		}
	}

	start := time.Now()
	err := kc.Create(ctx, lvmVolumeGroup)
	metrics.ApiMethodsDuration(LVMVolumeGroupDiscoverCtrlName, "create").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.ApiMethodsExecutionCount(LVMVolumeGroupDiscoverCtrlName, "create").Inc()
	if err != nil {
		metrics.ApiMethodsErrors(LVMVolumeGroupDiscoverCtrlName, "create").Inc()
		return nil, fmt.Errorf("unable to сreate LVMVolumeGroup, err: %w", err)
	}

	return lvmVolumeGroup, nil
}

func findBlockDevicesNamesWithoutPV(lvg *v1alpha1.LvmVolumeGroup, candidate internal.LVMVolumeGroupCandidate) []string {
	actualBlockDevices := make(map[string]struct{}, len(lvg.Spec.BlockDeviceNames))
	for _, devices := range candidate.Nodes {
		for _, d := range devices {
			if d.BlockDevice != "" {
				actualBlockDevices[d.BlockDevice] = struct{}{}
			}
		}
	}

	result := make([]string, 0, len(lvg.Spec.BlockDeviceNames))

	for _, bd := range lvg.Spec.BlockDeviceNames {
		if _, exist := actualBlockDevices[bd]; !exist {
			result = append(result, bd)
		}
	}

	return result
}

func UpdateLVMVolumeGroupByCandidate(
	ctx context.Context,
	cl kclient.Client,
	metrics monitoring.Metrics,
	log logger.Logger,
	lvg *v1alpha1.LvmVolumeGroup,
	candidate internal.LVMVolumeGroupCandidate,
) error {
	// Check if we miss some PV for BlockDevices
	unhealthyBDs := findBlockDevicesNamesWithoutPV(lvg, candidate)
	if len(unhealthyBDs) > 0 {
		shouldUpdate := false
		for _, bd := range unhealthyBDs {
			for _, n := range lvg.Status.Nodes {
				for i := range n.Devices {
					if n.Devices[i].BlockDevice == bd {
						p := strings.Split(n.Devices[i].Path, " ")
						if len(p) > 1 {
							continue
						}

						shouldUpdate = true
						n.Devices[i].Path = fmt.Sprintf("%s [missed]", n.Devices[i].Path)
					}
				}
			}
		}

		if shouldUpdate {
			err := errors.New("missed PV")
			log.Error(err, fmt.Sprintf("BlockDevices %s miss their PVs. Its likely because the corresponding device was manually removed from the node", strings.Join(unhealthyBDs, ",")))
			err = cl.Status().Update(ctx, lvg)
			if err != nil {
				return err
			}
		}

		err := updateLVGConditionIfNeeded(ctx, cl, log, lvg, metav1.ConditionFalse, internal.VGReadyType, internal.ScanFailed, fmt.Sprintf("BlockDevices %s miss their PVs. Its likely because the corresponding device was manually removed from the node", strings.Join(unhealthyBDs, ",")))
		if err != nil {
			log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupDiscoverController] unable to add a condition %s to the LVMVolumeGroup %s", internal.VGReadyType, lvg.Name))
		}
		return err
	}

	// Check if VG has some problems
	if candidate.Health == NonOperational {
		err := updateLVGConditionIfNeeded(ctx, cl, log, lvg, metav1.ConditionFalse, internal.VGReadyType, internal.ScanFailed, candidate.Message)
		if err != nil {
			log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupDiscoverController] unable to add a condition %s to the LVMVolumeGroup %s", internal.VGReadyType, lvg.Name))
		}
		return err
	}

	// The resource.Status.Nodes can not be just re-written, it needs to be updated directly by a node.
	// We take all current resources nodes and convert them to map for better performance further.
	resourceNodes := make(map[string][]v1alpha1.LvmVolumeGroupDevice, len(lvg.Status.Nodes))
	for _, node := range lvg.Status.Nodes {
		resourceNodes[node.Name] = node.Devices
	}

	// Now we take our candidate's nodes, match them with resource's ones and upgrade devices for matched resource node.
	for candidateNode, devices := range candidate.Nodes {
		if _, match := resourceNodes[candidateNode]; match {
			resourceNodes[candidateNode] = convertLVMVGDevices(devices)
		}
	}

	// Now we take resource's nodes, match them with our map and fill with new info.
	for i, node := range lvg.Status.Nodes {
		if devices, match := resourceNodes[node.Name]; match {
			lvg.Status.Nodes[i].Devices = devices
		}
	}

	lvg.Status = v1alpha1.LvmVolumeGroupStatus{
		AllocatedSize: candidate.AllocatedSize,
		Nodes:         convertLVMVGNodes(candidate.Nodes),
		ThinPools:     convertStatusThinPools(candidate.StatusThinPools),
		VGSize:        candidate.VGSize,
		VGUuid:        candidate.VGUuid,
		Conditions:    lvg.Status.Conditions,
		Phase:         lvg.Status.Phase,
	}

	start := time.Now()
	err := cl.Status().Update(ctx, lvg)
	metrics.ApiMethodsDuration(LVMVolumeGroupDiscoverCtrlName, "update").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.ApiMethodsExecutionCount(LVMVolumeGroupDiscoverCtrlName, "update").Inc()
	if err != nil {
		metrics.ApiMethodsErrors(LVMVolumeGroupDiscoverCtrlName, "update").Inc()
		return fmt.Errorf(`[UpdateLVMVolumeGroupByCandidate] unable to update LVMVolumeGroup, name: "%s", err: %w`, lvg.Name, err)
	}

	err = updateLVGConditionIfNeeded(ctx, cl, log, lvg, metav1.ConditionTrue, internal.VGReadyType, internal.Updated, "ready to create LV")
	if err != nil {
		log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupDiscoverController] unable to add a condition %s to the LVMVolumeGroup %s", internal.VGReadyType, lvg.Name))
	}

	return err
}

func convertLVMVGNodes(nodes map[string][]internal.LVMVGDevice) []v1alpha1.LvmVolumeGroupNode {
	lvmvgNodes := make([]v1alpha1.LvmVolumeGroupNode, 0, len(nodes))

	for nodeName, nodeDevices := range nodes {
		convertedDevices := convertLVMVGDevices(nodeDevices)

		lvmvgNodes = append(lvmvgNodes, v1alpha1.LvmVolumeGroupNode{
			Devices: convertedDevices,
			Name:    nodeName,
		})
	}

	return lvmvgNodes
}

func convertLVMVGDevices(devices []internal.LVMVGDevice) []v1alpha1.LvmVolumeGroupDevice {
	convertedDevices := make([]v1alpha1.LvmVolumeGroupDevice, 0, len(devices))

	for _, dev := range devices {
		convertedDevices = append(convertedDevices, v1alpha1.LvmVolumeGroupDevice{
			BlockDevice: dev.BlockDevice,
			DevSize:     dev.DevSize,
			PVSize:      dev.PVSize,
			PVUuid:      dev.PVUuid,
			Path:        dev.Path,
		})
	}

	return convertedDevices
}

func convertSpecThinPools(thinPools map[string]resource.Quantity) []v1alpha1.SpecThinPool {
	result := make([]v1alpha1.SpecThinPool, 0, len(thinPools))
	for name, size := range thinPools {
		result = append(result, v1alpha1.SpecThinPool{
			Name: name,
			Size: size,
		})
	}

	return result
}

func convertStatusThinPools(thinPools []internal.LVMVGStatusThinPool) []v1alpha1.StatusThinPool {
	result := make([]v1alpha1.StatusThinPool, 0, len(thinPools))
	for _, tp := range thinPools {
		result = append(result, v1alpha1.StatusThinPool{
			Name:       tp.Name,
			ActualSize: tp.ActualSize,
			UsedSize:   tp.UsedSize,
			Ready:      tp.Ready,
			Message:    tp.Message,
		})
	}

	return result
}

func generateLVMVGName() string {
	return "vg-" + string(uuid.NewUUID())
}

func GetAPILVMVolumeGroups(ctx context.Context, kc kclient.Client, metrics monitoring.Metrics) (map[string]v1alpha1.LvmVolumeGroup, error) {
	lvgList := &v1alpha1.LvmVolumeGroupList{}

	start := time.Now()
	err := kc.List(ctx, lvgList)
	metrics.ApiMethodsDuration(LVMVolumeGroupDiscoverCtrlName, "list").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.ApiMethodsExecutionCount(LVMVolumeGroupDiscoverCtrlName, "list").Inc()
	if err != nil {
		metrics.ApiMethodsErrors(LVMVolumeGroupDiscoverCtrlName, "list").Inc()
		return nil, fmt.Errorf("[GetApiLVMVolumeGroups] unable to list LvmVolumeGroups, err: %w", err)
	}

	lvgs := make(map[string]v1alpha1.LvmVolumeGroup, len(lvgList.Items))
	for _, lvg := range lvgList.Items {
		lvgs[lvg.Name] = lvg
	}

	return lvgs, nil
}

func filterVGByTag(vgs []internal.VGData, tag []string) []internal.VGData {
	filtered := make([]internal.VGData, 0, len(vgs))

	for _, vg := range vgs {
		if strings.Contains(vg.VGTags, tag[0]) {
			filtered = append(filtered, vg)
		}
	}

	return filtered
}
