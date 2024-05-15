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
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

const LVMVolumeGroupDiscoverCtrlName = "lvm-volume-group-discover-controller"

func RunLVMVolumeGroupDiscoverController(
	ctx context.Context,
	mgr manager.Manager,
	cfg config.Options,
	log logger.Logger,
	metrics monitoring.Metrics,
) (controller.Controller, error) {
	cl := mgr.GetClient()
	cache := mgr.GetCache()

	c, err := controller.New(LVMVolumeGroupDiscoverCtrlName, mgr, controller.Options{
		Reconciler: reconcile.Func(func(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
			return reconcile.Result{}, nil
		}),
	})

	if err != nil {
		log.Error(err, fmt.Sprintf(`[RunLVMVolumeGroupDiscoverController] unable to create controller: "%s"`, LVMVolumeGroupDiscoverCtrlName))
		return nil, err
	}

	err = c.Watch(source.Kind(cache, &v1alpha1.LvmVolumeGroup{}), &handler.EnqueueRequestForObject{})
	if err != nil {
		log.Error(err, fmt.Sprintf(`[RunLVMVolumeGroupDiscoverController] unable to run "%s" controller watch`, LVMVolumeGroupDiscoverCtrlName))
	}

	log.Info("[RunLVMVolumeGroupDiscoverController] run discovery loop")

	go func() {
		for {
			time.Sleep(cfg.VolumeGroupScanInterval * time.Second)
			reconcileStart := time.Now()

			log.Info("[RunLVMVolumeGroupDiscoverController] START discovery loop")

			currentLVMVGs, err := GetAPILVMVolumeGroups(ctx, cl, metrics)
			if err != nil {
				log.Error(err, "[RunLVMVolumeGroupDiscoverController] unable to run GetAPILVMVolumeGroups")
				continue
			}

			if len(currentLVMVGs) == 0 {
				log.Debug("[RunLVMVolumeGroupDiscoverController] no current LVMVolumeGroups found")
			}

			blockDevices, err := GetAPIBlockDevices(ctx, cl, metrics)
			if err != nil {
				log.Error(err, "[RunLVMVolumeGroupDiscoverController] unable to GetAPIBlockDevices")
				for _, lvg := range currentLVMVGs {
					if err = turnLVMVGHealthToNonOperational(ctx, cl, lvg, err); err != nil {
						log.Error(err, fmt.Sprintf(`[RunLVMVolumeGroupDiscoverController] unable to change health param in LVMVolumeGroup, name: "%s"`, lvg.Name))
					}
				}
				continue
			}

			if len(blockDevices) == 0 {
				log.Info("[RunLVMVolumeGroupDiscoverController] no BlockDevices were found")
				continue
			}

			filteredResources := filterResourcesByNode(ctx, cl, log, currentLVMVGs, blockDevices, cfg.NodeName)

			candidates, err := GetLVMVolumeGroupCandidates(log, metrics, blockDevices, cfg.NodeName)
			if err != nil {
				log.Error(err, "[RunLVMVolumeGroupDiscoverController] unable to run GetLVMVolumeGroupCandidates")
				for _, lvg := range filteredResources {
					log.Trace(fmt.Sprintf("[RunLVMVolumeGroupDiscoverController] turn LVMVolumeGroup %q to non operational. LVG struct: %+v ", lvg.Name, lvg))
					if err = turnLVMVGHealthToNonOperational(ctx, cl, lvg, err); err != nil {
						log.Error(err, fmt.Sprintf(`[RunLVMVolumeGroupDiscoverController] unable to change health param in LVMVolumeGroup, name: "%s"`, lvg.Name))
					}
				}
				continue
			}

			for _, candidate := range candidates {
				if lvmVolumeGroup := getResourceByCandidate(filteredResources, candidate); lvmVolumeGroup != nil {
					if !hasLVMVolumeGroupDiff(log, *lvmVolumeGroup, candidate) {
						log.Debug(fmt.Sprintf(`[RunLVMVolumeGroupDiscoverController] no data to update for LvmVolumeGroup, name: "%s"`, lvmVolumeGroup.Name))
						continue
					}
					//TODO: take lock

					log.Debug(fmt.Sprintf("[RunLVMVolumeGroupDiscoverController] run UpdateLVMVolumeGroupByCandidate, lvmVolumeGroup name: %s", lvmVolumeGroup.Name))
					if err = UpdateLVMVolumeGroupByCandidate(ctx, cl, metrics, *lvmVolumeGroup, candidate); err != nil {
						log.Error(err, fmt.Sprintf(`[RunLVMVolumeGroupDiscoverController] unable to update LvmVolumeGroup, name: "%s"`,
							lvmVolumeGroup.Name))
						continue
					}

					log.Info(fmt.Sprintf(`[RunLVMVolumeGroupDiscoverController] updated LvmVolumeGroup, name: "%s"`, lvmVolumeGroup.Name))
					//TODO: release lock
				} else {
					lvm, err := CreateLVMVolumeGroup(ctx, log, metrics, cl, candidate)
					if err != nil {
						log.Error(err, "[RunLVMVolumeGroupDiscoverController] unable to CreateLVMVolumeGroup")
						continue
					}
					log.Info(fmt.Sprintf(`[RunLVMVolumeGroupDiscoverController] created new APILVMVolumeGroup, name: "%s"`, lvm.Name))
				}
			}

			ClearLVMVolumeGroupResources(ctx, cl, log, metrics, candidates, filteredResources, cfg.NodeName)

			log.Info("[RunLVMVolumeGroupDiscoverController] END discovery loop")
			metrics.ReconcileDuration(LVMVolumeGroupDiscoverCtrlName).Observe(metrics.GetEstimatedTimeInSeconds(reconcileStart))
			metrics.ReconcilesCountTotal(LVMVolumeGroupDiscoverCtrlName).Inc()
		}
	}()

	return c, err

}

func filterResourcesByNode(
	ctx context.Context,
	cl kclient.Client,
	log logger.Logger,
	lvgs map[string]v1alpha1.LvmVolumeGroup,
	blockDevices map[string]v1alpha1.BlockDevice,
	currentNode string) []v1alpha1.LvmVolumeGroup {

	filtered := make([]v1alpha1.LvmVolumeGroup, 0, len(lvgs))
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
				if err := turnLVMVGHealthToNonOperational(
					ctx, cl, lvg, fmt.Errorf("there are block devices from different nodes for local volume group")); err != nil {
					log.Error(err, `[filterResourcesByNode] unable to update resource, name: "%s"`, lvg.Name)
					continue
				}
			}

			// If we did not find any block device for our node, we skip the resource.
			if currentNodeDevices == 0 {
				continue
			}

			// Otherwise, we add the resource to the filtered ones.
			filtered = append(filtered, lvg)
		case Shared:
			if len(lvg.Spec.BlockDeviceNames) != 1 {
				if err := turnLVMVGHealthToNonOperational(
					ctx, cl, lvg, fmt.Errorf("there are more than one block devices for shared volume group")); err != nil {
					log.Error(err, `[filterResourcesByNode] unable to update resource, name: "%s"`, lvg.Name)
					continue
				}
			}

			// If the only one block devices does not belong to our node, we skip the resource.
			singleBD := lvg.Spec.BlockDeviceNames[0]
			if blockDevicesNodes[singleBD] != currentNode {
				continue
			}

			// Otherwise, we add the resource to the filtered ones.
			filtered = append(filtered, lvg)
		}
	}

	return filtered
}

func turnLVMVGHealthToNonOperational(ctx context.Context, cl kclient.Client, lvg v1alpha1.LvmVolumeGroup, err error) error {
	lvg.Status.Health = internal.LVMVGHealthNonOperational
	lvg.Status.Message = err.Error()

	return cl.Update(ctx, &lvg)
}

func hasLVMVolumeGroupDiff(log logger.Logger, res v1alpha1.LvmVolumeGroup, candidate internal.LVMVolumeGroupCandidate) bool {
	log.Trace(fmt.Sprintf(`AllocatedSize, candidate: %s, res: %s`, candidate.AllocatedSize.String(), res.Status.AllocatedSize.String()))
	log.Trace(fmt.Sprintf(`Health, candidate: %s, res: %s`, candidate.Health, res.Status.Health))
	log.Trace(fmt.Sprintf(`Message, candidate: %s, res: %s`, candidate.Message, res.Status.Message))
	log.Trace(fmt.Sprintf(`ThinPools, candidate: %v, res: %v`, convertStatusThinPools(candidate.StatusThinPools), res.Status.ThinPools))
	log.Trace(fmt.Sprintf(`VGSize, candidate: %s, res: %s`, candidate.VGSize.String(), res.Status.VGSize.String()))
	log.Trace(fmt.Sprintf(`VGUuid, candidate: %s, res: %s`, candidate.VGUuid, res.Status.VGUuid))
	log.Trace(fmt.Sprintf(`Nodes, candidate: %v, res: %v`, convertLVMVGNodes(candidate.Nodes), res.Status.Nodes))

	//TODO: Uncomment this
	//return strings.Join(candidate.Finalizers, "") == strings.Join(res.Finalizers, "") ||
	return candidate.AllocatedSize.Value() != res.Status.AllocatedSize.Value() ||
		candidate.Health != res.Status.Health ||
		candidate.Message != res.Status.Message ||
		!reflect.DeepEqual(convertStatusThinPools(candidate.StatusThinPools), res.Status.ThinPools) ||
		candidate.VGSize.Value() != res.Status.VGSize.Value() ||
		candidate.VGUuid != res.Status.VGUuid ||
		!reflect.DeepEqual(convertLVMVGNodes(candidate.Nodes), res.Status.Nodes)
}

func getResourceByCandidate(current []v1alpha1.LvmVolumeGroup, candidate internal.LVMVolumeGroupCandidate) *v1alpha1.LvmVolumeGroup {
	for _, lvm := range current {
		if lvm.Spec.ActualVGNameOnTheNode == candidate.ActualVGNameOnTheNode {
			return &lvm
		}
	}

	return nil
}

// ClearLVMVolumeGroupResources Removes deprecated nodes and resources.
func ClearLVMVolumeGroupResources(
	ctx context.Context,
	cl kclient.Client,
	log logger.Logger,
	metrics monitoring.Metrics,
	candidates []internal.LVMVolumeGroupCandidate,
	lvmVolumeGroups []v1alpha1.LvmVolumeGroup,
	currentNode string,
) {
	actualVGs := make(map[string]struct{}, len(candidates))
	for _, candidate := range candidates {
		actualVGs[candidate.ActualVGNameOnTheNode] = struct{}{}
	}

	for _, lvm := range lvmVolumeGroups {
		if !reflect.ValueOf(lvm.Status.VGUuid).IsZero() {
			if _, exist := actualVGs[lvm.Spec.ActualVGNameOnTheNode]; !exist {
				log.Debug(fmt.Sprintf(`[ClearLVMVolumeGroupResources] Node "%s" does not belong to VG "%s". 
It will be removed from LVM resource, name "%s"'`, currentNode, lvm.Spec.ActualVGNameOnTheNode, lvm.Name))
				for i, node := range lvm.Status.Nodes {
					if node.Name == currentNode {
						// delete node
						lvm.Status.Nodes = append(lvm.Status.Nodes[:i], lvm.Status.Nodes[i+1:]...)
						log.Info(fmt.Sprintf(`[ClearLVMVolumeGroupResources] deleted node, name: "%s", 
from LVMVolumeGroup, name: "%s"`, node.Name, lvm.Name))
					}
				}

				// If current LVMVolumeGroup has no nodes left, and it is not cause of errors, delete it.
				if len(lvm.Status.Nodes) == 0 {
					if err := DeleteLVMVolumeGroup(ctx, cl, metrics, lvm.Name); err != nil {
						log.Error(err, fmt.Sprintf("Unable to delete LVMVolumeGroup, name: %s", lvm.Name))
						continue
					}

					log.Info(fmt.Sprintf("[ClearLVMVolumeGroupResources] deleted LVMVolumeGroup, name: %s", lvm.Name))
				}
			}
		}
	}
}

func DeleteLVMVolumeGroup(ctx context.Context, kc kclient.Client, metrics monitoring.Metrics, lvmvgName string) error {
	lvm := &v1alpha1.LvmVolumeGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name: lvmvgName,
		},
		TypeMeta: metav1.TypeMeta{
			Kind:       v1alpha1.LVMVolumeGroupKind,
			APIVersion: v1alpha1.TypeMediaAPIVersion,
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

func GetLVMVolumeGroupCandidates(log logger.Logger, sdsCache *cache.Cache, metrics monitoring.Metrics, bds map[string]v1alpha1.BlockDevice, currentNode string) ([]internal.LVMVolumeGroupCandidate, error) {
	var candidates []internal.LVMVolumeGroupCandidate

	vgs := sdsCache.GetVGs()
	vgWithTag := filterVGByTag(vgs, internal.LVMTags)

	// If there is no VG with our tag, then there is no any candidate.
	if len(vgWithTag) == 0 {
		return candidates, nil
	}

	// If vgErrs is not empty, that means we have some problems on vgs, so we need to identify unhealthy vgs.
	var vgIssues map[string]string
	if vgErrs.Len() != 0 {
		vgIssues = sortVGIssuesByVG(log, vgWithTag)
	}

	pvs := sdsCache.GetPVs()
	if len(pvs) == 0 {
		err := errors.New("no PV found")
		log.Error(err, "[GetLVMVolumeGroupCandidates] no PV was found, but VG with tags are not empty")
		return nil, err
	}

	// If pvErrs is not empty, that means we have some problems on vgs, so we need to identify unhealthy vgs.
	var pvIssues map[string][]string
	if pvErrs.Len() != 0 {
		pvIssues = sortPVIssuesByVG(log, pvs)
	}

	lvs, cmdStr, lvErrs, err := utils.GetAllLVs()
	metrics.UtilsCommandsDuration(LVMVolumeGroupDiscoverCtrlName, "lvs").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.UtilsCommandsExecutionCount(LVMVolumeGroupDiscoverCtrlName, "lvs").Inc()
	log.Debug(fmt.Sprintf("[GetLVMVolumeGroupCandidates] exec cmd: %s", cmdStr))

	// As long as LVS data is used to fill ThinPool fields, that is optional, we won't break but log the error.
	if err != nil {
		metrics.UtilsCommandsErrorsCount(LVMVolumeGroupDiscoverCtrlName, "lvs").Inc()
		log.Error(err, "[GetLVMVolumeGroupCandidates] unable to GetAllLVs")
	}

	var thinPools []internal.LVData
	if lvs != nil && len(lvs) > 0 {
		// Filter LV to get only thin pools as we do not support thick for now.
		thinPools = getThinPools(lvs)
	}

	// If lvErrs is not empty, that means we have some problems on vgs, so we need to identify unhealthy vgs.
	var lvIssues map[string][]string
	if lvErrs.Len() != 0 {
		lvIssues = sortLVIssuesByVG(log, thinPools)
	}

	// Sort PV,BlockDevices and LV by VG to fill needed information for LVMVolumeGroup resource further.
	sortedPVs := sortPVsByVG(pvs, vgWithTag)
	sortedBDs := sortBlockDevicesByVG(bds, vgWithTag)
	sortedThinPools := sortLVsByVG(thinPools, vgWithTag)

	for _, vg := range vgWithTag {
		if err != nil {
			log.Error(err, "[GetLVMVolumeGroupCandidates] unable to count ParseInt vgSize, err: %w", err)
		}

		allocateSize := vg.VGSize
		allocateSize.Sub(vg.VGFree)

		health, message := checkVGHealth(err, sortedBDs, vgIssues, pvIssues, lvIssues, vg)

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
			StatusThinPools:       getStatusThinPools(log, sortedThinPools, vg),
			VGSize:                *resource.NewQuantity(vg.VGSize.Value(), resource.BinarySI),
			VGUuid:                vg.VGUuid,
			Nodes:                 configureCandidateNodeDevices(sortedPVs, sortedBDs, vg, currentNode),
		}

		candidates = append(candidates, candidate)
	}

	return candidates, nil
}

func checkVGHealth(err error, blockDevices map[string][]v1alpha1.BlockDevice, vgIssues map[string]string, pvIssues map[string][]string, lvIssues map[string][]string, vg internal.VGData) (health, message string) {
	issues := make([]string, 0, len(vgIssues)+len(pvIssues)+len(lvIssues)+1)

	if err != nil {
		issues = append(issues, err.Error())
	}

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

	return internal.LVMVGHealthOperational, "No problems detected"
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

func getStatusThinPools(log logger.Logger, thinPools map[string][]internal.LVData, vg internal.VGData) []internal.LVMVGStatusThinPool {
	filtered := thinPools[vg.VGName+vg.VGUuid]
	tps := make([]internal.LVMVGStatusThinPool, 0, len(filtered))

	for _, lv := range filtered {
		usedSize, err := getLVUsedSize(lv)
		if err != nil {
			log.Error(err, "[getStatusThinPools] unable to getLVUsedSize")
		}
		tps = append(tps, internal.LVMVGStatusThinPool{
			Name:       lv.LVName,
			ActualSize: *resource.NewQuantity(lv.LVSize.Value(), resource.BinarySI),
			UsedSize:   *resource.NewQuantity(usedSize.Value(), resource.BinarySI),
		})
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

func CreateLVMVolumeGroup(
	ctx context.Context,
	log logger.Logger,
	metrics monitoring.Metrics,
	kc kclient.Client,
	candidate internal.LVMVolumeGroupCandidate,
) (*v1alpha1.LvmVolumeGroup, error) {
	lvmVolumeGroup := &v1alpha1.LvmVolumeGroup{
		TypeMeta: metav1.TypeMeta{
			Kind:       v1alpha1.LVMVolumeGroupKind,
			APIVersion: v1alpha1.TypeMediaAPIVersion,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            candidate.LVMVGName,
			OwnerReferences: []metav1.OwnerReference{},
			//TODO: Uncomment this
			//Finalizers:      candidate.Finalizers,
		},
		Spec: v1alpha1.LvmVolumeGroupSpec{
			ActualVGNameOnTheNode: candidate.ActualVGNameOnTheNode,
			BlockDeviceNames:      candidate.BlockDevicesNames,
			ThinPools:             convertSpecThinPools(candidate.SpecThinPools),
			Type:                  candidate.Type,
		},
		Status: v1alpha1.LvmVolumeGroupStatus{
			AllocatedSize: candidate.AllocatedSize,
			Health:        candidate.Health,
			Message:       candidate.Message,
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

func UpdateLVMVolumeGroupByCandidate(
	ctx context.Context,
	kc kclient.Client,
	metrics monitoring.Metrics,
	res v1alpha1.LvmVolumeGroup,
	candidate internal.LVMVolumeGroupCandidate,
) error {
	// The resource.Status.Nodes can not be just re-written, it needs to be updated directly by node.
	// We take all current resources nodes and convert them to map for better performance further.
	resourceNodes := make(map[string][]v1alpha1.LvmVolumeGroupDevice, len(res.Status.Nodes))
	for _, node := range res.Status.Nodes {
		resourceNodes[node.Name] = node.Devices
	}

	// Now we take our candidate's nodes, match them with resource's ones and upgrade devices for matched resource node.
	for candidateNode, devices := range candidate.Nodes {
		if _, match := resourceNodes[candidateNode]; match {
			resourceNodes[candidateNode] = convertLVMVGDevices(devices)
		}
	}

	// Now we take resource's nodes, match them with our map and fill with new info.
	for i, node := range res.Status.Nodes {
		if devices, match := resourceNodes[node.Name]; match {
			res.Status.Nodes[i].Devices = devices
		}
	}

	// Update status.
	lvmvg := &v1alpha1.LvmVolumeGroup{
		TypeMeta: metav1.TypeMeta{
			Kind:       v1alpha1.LVMVolumeGroupKind,
			APIVersion: v1alpha1.TypeMediaAPIVersion,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            res.Name,
			OwnerReferences: res.OwnerReferences,
			ResourceVersion: res.ResourceVersion,
			Annotations:     res.Annotations,
			Labels:          res.Labels,
		},
		Spec: v1alpha1.LvmVolumeGroupSpec{
			ActualVGNameOnTheNode: res.Spec.ActualVGNameOnTheNode,
			BlockDeviceNames:      res.Spec.BlockDeviceNames,
			ThinPools:             res.Spec.ThinPools,
			Type:                  res.Spec.Type,
		},
		Status: v1alpha1.LvmVolumeGroupStatus{
			AllocatedSize: candidate.AllocatedSize,
			Health:        candidate.Health,
			Message:       candidate.Message,
			Nodes:         convertLVMVGNodes(candidate.Nodes),
			ThinPools:     convertStatusThinPools(candidate.StatusThinPools),
			VGSize:        candidate.VGSize,
			VGUuid:        candidate.VGUuid,
		},
	}

	start := time.Now()
	err := kc.Update(ctx, lvmvg)
	metrics.ApiMethodsDuration(LVMVolumeGroupDiscoverCtrlName, "update").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.ApiMethodsExecutionCount(LVMVolumeGroupDiscoverCtrlName, "update").Inc()
	if err != nil {
		metrics.ApiMethodsErrors(LVMVolumeGroupDiscoverCtrlName, "update").Inc()
		return fmt.Errorf(`[UpdateLVMVolumeGroupByCandidate] unable to update LVMVolumeGroup, name: "%s", err: %w`, lvmvg.Name, err)
	}

	return nil
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
		})
	}

	return result
}

func generateLVMVGName() string {
	return "vg-" + string(uuid.NewUUID())
}

func GetAPILVMVolumeGroups(ctx context.Context, kc kclient.Client, metrics monitoring.Metrics) (map[string]v1alpha1.LvmVolumeGroup, error) {
	listLvms := &v1alpha1.LvmVolumeGroupList{
		TypeMeta: metav1.TypeMeta{
			Kind:       v1alpha1.LVMVolumeGroupKind,
			APIVersion: v1alpha1.TypeMediaAPIVersion,
		},
		ListMeta: metav1.ListMeta{},
		Items:    []v1alpha1.LvmVolumeGroup{},
	}

	start := time.Now()
	err := kc.List(ctx, listLvms)
	metrics.ApiMethodsDuration(LVMVolumeGroupDiscoverCtrlName, "list").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.ApiMethodsExecutionCount(LVMVolumeGroupDiscoverCtrlName, "list").Inc()
	if err != nil {
		metrics.ApiMethodsErrors(LVMVolumeGroupDiscoverCtrlName, "list").Inc()
		return nil, fmt.Errorf("[GetApiLVMVolumeGroups] unable to list lvm volume groups, err: %w", err)
	}

	lvms := make(map[string]v1alpha1.LvmVolumeGroup, len(listLvms.Items))
	for _, lvm := range listLvms.Items {
		lvms[lvm.Name] = lvm
	}

	return lvms, nil
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
