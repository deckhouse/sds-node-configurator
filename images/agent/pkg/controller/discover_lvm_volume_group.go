package controller

import (
	"bytes"
	"context"
	"fmt"
	"github.com/alecthomas/units"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"os/exec"
	"reflect"
	kclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
	"storage-configurator/api/v1alpha1"
	"storage-configurator/config"
	"storage-configurator/internal"
	"storage-configurator/pkg/log"
	"storage-configurator/pkg/utils"
	"strconv"
	"strings"
	"time"
)

const discoveryLVMVGCtrlName = "discovery-lvmvg-controller"

func RunDiscoveryLVMVGController(
	ctx context.Context,
	mgr manager.Manager,
	cfg config.Options,
	log log.Logger,
) (controller.Controller, error) {
	cl := mgr.GetClient()
	cache := mgr.GetCache()

	c, err := controller.New(discoveryLVMVGCtrlName, mgr, controller.Options{
		Reconciler: reconcile.Func(func(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
			return reconcile.Result{}, nil
		}),
	})
	if err != nil {
		log.Error(err, fmt.Sprintf(`Unable to create controller: "%s"`, discoveryLVMVGCtrlName))
		return nil, err
	}

	err = c.Watch(source.Kind(cache, &v1alpha1.LvmVolumeGroup{}), &handler.EnqueueRequestForObject{})
	if err != nil {
		log.Error(err, fmt.Sprintf(`Unable to run "%s" controller watch`, discoveryLVMVGCtrlName))
	}

	log.Info("[RunDiscoveryLVMVGController] run discovery loop")
	go func() {
		for {
			time.Sleep(cfg.VolumeGroupScanInterval * time.Second)

			//if cfg.NodeName == "a-ohrimenko-worker-0" {
			//	continue
			//}
			//fmt.Println(" -=-=-=- ", cfg.NodeName)
			//
			//if cfg.NodeName == "a-ohrimenko-worker-1" {
			//
			//	//fmt.Println(">>>>>>> ")
			//	config, err := rest.InClusterConfig()
			//	if err != nil {
			//		fmt.Println("InClusterConfig", err.Error())
			//	}
			//
			//	dynamicClient, err := dynamic.NewForConfig(config)
			//	if err != nil {
			//		fmt.Println("NewForConfig", err.Error())
			//	}
			//
			//	gvr := schema.GroupVersionResource{
			//		Group:    "storage.deckhouse.io",
			//		Version:  "v1alpha1",
			//		Resource: "lvmvolumegroups",
			//	}
			//
			//	unstruct, err := dynamicClient.Resource(gvr).Get(context.TODO(), "vg-data-on-node-1", metav1.GetOptions{})
			//	if err != nil {
			//		fmt.Println("dynamicClient.Resource", err.Error())
			//	}

			//fmt.Println(unstruct)
			//fmt.Println(">>>>>>> ")

			//unstruct.Object["status"].(map[string]interface{})["allocatedSize"] = "777M"

			//unstruct.Object["status"].(map[string]interface{})["nodes"].([]interface{})[0].(map[string]interface{})["devices"].([]interface{})[0].(map[string]interface{})["devSize"] = "12G"
			//unstruct, _ = dynamicClient.Resource(gvr).Update(context.TODO(), unstruct, metav1.UpdateOptions{})
			// --------------------------

			//	lvg, _ := getLVMVolumeGroup(ctx, cl, "d8-storage-configurator", "vg-data-on-node-1")
			//fmt.Println(">>>>>>> ")
			//fmt.Println(lvg)
			//fmt.Println(">>>>>>> ")

			//	if len(lvg.Status.Nodes) != 0 {
			//		lvg.Status.Nodes[0].Devices[0].DevSize = "12G"
			//	}
			//
			//	err = updateLVMVolumeGroup(ctx, cl, lvg)
			//	if err != nil {
			//		fmt.Println("*** updateLVMVolumeGroup ", err.Error())
			//	}
			//
			//	continue
			//}

			currentLVMVGs, err := GetAPILVMVolumeGroups(ctx, cl)
			if err != nil {
				log.Error(err, "[RunDiscoveryLVMVGController] unable to run GetAPILVMVolumeGroups")
				continue
			}

			blockDevices, err := GetAPIBlockDevices(ctx, cl)
			if err != nil {
				log.Error(err, "[RunDiscoveryLVMVGController] unable to GetAPIBlockDevices")
				for _, lvm := range currentLVMVGs {
					if err = turnLVMVGHealthToNonOperational(ctx, cl, lvm, err); err != nil {
						log.Error(err, fmt.Sprintf(`unable to change health param in LVMVolumeGroup, name: "%s"`, lvm.Name))
					}
				}
				continue
			}

			if len(blockDevices) == 0 {
				log.Error(fmt.Errorf("no block devices found"), "[RunDiscoveryLVMVGController] unable to get block devices")
				for _, lvm := range currentLVMVGs {
					if err = turnLVMVGHealthToNonOperational(ctx, cl, lvm, fmt.Errorf("no block devices found")); err != nil {
						log.Error(err, fmt.Sprintf(`unable to change health param in LVMVolumeGroup, name: "%s"`, lvm.Name))
					}
				}
				continue
			}

			candidates, err := GetLVMVolumeGroupCandidates(log, blockDevices, cfg.NodeName)
			if err != nil {
				log.Error(err, "[RunDiscoveryLVMVGController] unable to run GetLVMVolumeGroupCandidates")
				for _, lvm := range currentLVMVGs {
					if err = turnLVMVGHealthToNonOperational(ctx, cl, lvm, err); err != nil {
						log.Error(err, fmt.Sprintf(`unable to change health param in LVMVolumeGroup, name: "%s"`, lvm.Name))
					}
				}
				continue
			}

			for _, candidate := range candidates {
				if resource := getResourceByCandidate(currentLVMVGs, candidate); resource != nil {
					if !hasLVMVolumeGroupDiff(*resource, candidate) {
						log.Debug(fmt.Sprintf(`No data to update for LvmVolumeGroup, name: "%s"`, resource.Name))
						continue
					}
					//TODO: take lock

					log.Debug(fmt.Sprintf("[RunDiscoveryLVMVGController] run UpdateLVMVolumeGroupByCandidate, resource name: %s; candidate: %v, resource: %v", resource.Name, candidate, resource))
					if err = UpdateLVMVolumeGroupByCandidate(ctx, cl, *resource, candidate); err != nil {
						log.Error(err, fmt.Sprintf(`[RunDiscoveryLVMVGController] unable to update resource, name: "%s"`,
							resource.Name))
						continue
					}

					log.Info(fmt.Sprintf(`[RunDiscoveryLVMVGController] updated resource, name: "%s"`, resource.Name))

					//TODO: release lock
				} else {
					lvm, err := CreateLVMVolumeGroup(ctx, cl, candidate)
					if err != nil {
						log.Error(err, "[RunDiscoveryLVMVGController] unable to CreateLVMVolumeGroup")
						continue
					}
					log.Info(fmt.Sprintf(`[RunDiscoveryLVMVGController] created new APILVMVolumeGroup, name: "%s"`, lvm.Name))
				}
			}

			ClearLVMVolumeGroupResources(ctx, cl, log, candidates, currentLVMVGs, cfg.NodeName)
		}
	}()

	return c, err
}

func turnLVMVGHealthToNonOperational(ctx context.Context, cl kclient.Client, lvg v1alpha1.LvmVolumeGroup, err error) error {
	lvg.Status.Health = internal.LVMVGHealthNonOperational
	lvg.Status.Message = err.Error()

	return cl.Update(ctx, &lvg)
}

func hasLVMVolumeGroupDiff(resource v1alpha1.LvmVolumeGroup, candidate internal.LVMVolumeGroupCandidate) bool {
	//TODO: Uncomment this
	//return strings.Join(candidate.Finalizers, "") == strings.Join(resource.Finalizers, "") ||
	return candidate.AllocatedSize != resource.Status.AllocatedSize ||
		candidate.Health != resource.Status.Health ||
		candidate.Message != resource.Status.Message ||
		!reflect.DeepEqual(convertStatusThinPools(candidate.StatusThinPools), resource.Status.ThinPools) ||
		candidate.VGSize != resource.Status.VGSize ||
		candidate.VGUuid != resource.Status.VGUuid ||
		!reflect.DeepEqual(convertLVMVGNodes(candidate.Nodes), resource.Status.Nodes)
}

func getResourceByCandidate(current map[string]v1alpha1.LvmVolumeGroup, candidate internal.LVMVolumeGroupCandidate) *v1alpha1.LvmVolumeGroup {
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
	log log.Logger,
	candidates []internal.LVMVolumeGroupCandidate,
	lvmVolumeGroups map[string]v1alpha1.LvmVolumeGroup,
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
					if err := DeleteLVMVolumeGroup(ctx, cl, lvm.Name); err != nil {
						log.Error(err, fmt.Sprintf("Unable to delete LVMVolumeGroup, name: %s", lvm.Name))
						continue
					}

					log.Info(fmt.Sprintf("[ClearLVMVolumeGroupResources] deleted LVMVolumeGroup, name: %s", lvm.Name))
				}
			}
		}
	}
}

func DeleteLVMVolumeGroup(ctx context.Context, kc kclient.Client, lvmvgName string) error {
	lvm := &v1alpha1.LvmVolumeGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name: lvmvgName,
		},
		TypeMeta: metav1.TypeMeta{
			Kind:       v1alpha1.LVMVolumeGroupKind,
			APIVersion: v1alpha1.TypeMediaAPIVersion,
		},
	}

	err := kc.Delete(ctx, lvm)
	if err != nil {
		return fmt.Errorf(
			`[DeleteLVMVolumeGroup] unable to delete DeleteLVMVolumeGroup with name "%s", err: %w`,
			lvmvgName, err)
	}
	return nil
}

func GetLVMVolumeGroupCandidates(log log.Logger, bds map[string]v1alpha1.BlockDevice, currentNode string) ([]internal.LVMVolumeGroupCandidate, error) {
	var candidates []internal.LVMVolumeGroupCandidate

	vgs, cmdStr, vgErrs, err := utils.GetAllVGs()
	log.Debug(fmt.Sprintf("[GetLVMVolumeGroupCandidates] runs cmd: %s", cmdStr))

	// If we can't run vgs command at all, that means we will not have important information, so we break.
	if err != nil {
		log.Error(err, "[GetLVMVolumeGroupCandidates] unable to GetAllVGs")
		return nil, err
	}

	vgWithTag := filterVGByTag(vgs, "storage.deckhouse.io/enabled=true")

	// If there is no VG with our tag, then there is no any candidate.
	if len(vgWithTag) == 0 {
		return candidates, nil
	}

	// If vgErrs is not empty, that means we have some problems on vgs, so we need to identify unhealthy vgs.
	var vgIssues map[string]string
	if vgErrs.Len() != 0 {
		vgIssues = sortVGIssuesByVG(log, vgWithTag)
	}

	// If we can't run pvs command at all, that means we will not have important information, so we break.
	pvs, cmdStr, pvErrs, err := utils.GetAllPVs()
	log.Debug(fmt.Sprintf("[GetLVMVolumeGroupCandidates] runs cmd: %s", cmdStr))
	if err != nil {
		log.Error(err, "[GetLVMVolumeGroupCandidates] unable to GetAllPVs")
		return nil, err
	}

	// If pvErrs is not empty, that means we have some problems on vgs, so we need to identify unhealthy vgs.
	var pvIssues map[string][]string
	if pvErrs.Len() != 0 {
		pvIssues = sortPVIssuesByVG(log, pvs)
	}

	// As long as LVS data is used to fill ThinPool fields, that is optional, we won't break but log the error.
	lvs, cmdStr, lvErrs, err := utils.GetAllLVs()
	log.Debug(fmt.Sprintf("[GetLVMVolumeGroupCandidates] runs cmd: %s", cmdStr))
	if err != nil {
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
		allocatedSize, err := getAllocatedSizeMiB(vg)
		if err != nil {
			log.Error(err, "[GetLVMVolumeGroupCandidates] unable to count AllocatedSize, err: %w", err)
		}

		health, message := checkVGHealth(err, sortedBDs, vgIssues, pvIssues, lvIssues, vg)

		candidate := internal.LVMVolumeGroupCandidate{
			LVMVGName:             generateLVMVGName(),
			Finalizers:            internal.Finalizers,
			ActualVGNameOnTheNode: vg.VGName,
			BlockDevicesNames:     getBlockDevicesNames(sortedBDs, vg),
			SpecThinPools:         getSpecThinPools(sortedThinPools, vg),
			Type:                  getVgType(vg),
			AllocatedSize:         allocatedSize,
			Health:                health,
			Message:               message,
			StatusThinPools:       getStatusThinPools(log, sortedThinPools, vg),
			VGSize:                vg.VGSize,
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

func sortLVIssuesByVG(log log.Logger, lvs []internal.LVData) map[string][]string {
	var (
		errs         bytes.Buffer
		cmd          *exec.Cmd
		lvIssuesByVG = make(map[string][]string, len(lvs))
	)

	for _, lv := range lvs {
		cmd = exec.Command("lvs", fmt.Sprintf("%s/%s", lv.VGName, lv.LVName))
		cmd.Stderr = &errs

		if err := cmd.Run(); err != nil {
			log.Error(err, fmt.Sprintf(`unable to run lvs command for lv, name: "%s"`, lv.LVName))
			lvIssuesByVG[lv.VGName+lv.VGUuid] = append(lvIssuesByVG[lv.VGName+lv.VGUuid], err.Error())
		}

		if errs.Len() != 0 {
			lvIssuesByVG[lv.VGName+lv.VGUuid] = append(lvIssuesByVG[lv.VGName+lv.VGUuid], errs.String())
			errs.Reset()
		}
	}

	return lvIssuesByVG
}

func sortPVIssuesByVG(log log.Logger, pvs []internal.PVData) map[string][]string {
	var (
		errs         bytes.Buffer
		cmd          *exec.Cmd
		pvIssuesByVG = make(map[string][]string, len(pvs))
	)

	for _, pv := range pvs {
		cmd = exec.Command("pvs", pv.PVName)
		cmd.Stderr = &errs

		if err := cmd.Run(); err != nil {
			log.Error(err, fmt.Sprintf(`unable to run pvs command for pv, name: "%s"`, pv.PVName))
			pvIssuesByVG[pv.VGName+pv.VGUuid] = append(pvIssuesByVG[pv.VGName+pv.VGUuid], err.Error())
		}

		if errs.Len() != 0 {
			pvIssuesByVG[pv.VGName+pv.VGUuid] = append(pvIssuesByVG[pv.VGName+pv.VGUuid], errs.String())
			errs.Reset()
		}
	}

	return pvIssuesByVG
}

func sortVGIssuesByVG(log log.Logger, vgs []internal.VGData) map[string]string {
	var (
		errs     bytes.Buffer
		cmd      *exec.Cmd
		vgIssues = make(map[string]string, len(vgs))
	)

	for _, vg := range vgs {
		cmd = exec.Command("vgs", vg.VGName)
		cmd.Stderr = &errs

		if err := cmd.Run(); err != nil {
			log.Error(err, fmt.Sprintf(`unable to run vgs command for vg, name: "%s"`, vg.VGName))
			vgIssues[vg.VGName+vg.VGUuid] = err.Error()
		}

		if errs.Len() != 0 {
			vgIssues[vg.VGName+vg.VGUuid] = errs.String()
			errs.Reset()
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
	//1. Получаем PV для выбранной VG
	//2. Каждую PV выбранной VG добавляем под нужную ноду
	for _, pv := range filteredPV {
		device := internal.LVMVGDevice{
			Path:   pv.PVName,
			PVSize: pv.PVSize,
			PVUuid: pv.PVUuid,
		}

		if bd, exist := bdPathStatus[pv.PVName]; exist {
			device.DevSize = bd.Status.Size
			device.BlockDevice = bd.Name
		}

		result[currentNode] = append(result[currentNode], device)
	}

	return result
}

func getAllocatedSizeMiB(vg internal.VGData) (string, error) {
	const iB = "iB"
	size, err := units.ParseBase2Bytes(vg.VGSize + iB)
	if err != nil {
		return "", err
	}

	free, err := units.ParseBase2Bytes(vg.VGFree + iB)
	if err != nil {
		return "", err
	}

	allocatedMiB := (size - free) / units.Mebibyte

	return strconv.Itoa(int(allocatedMiB)) + "M", err
}

func getVgType(vg internal.VGData) string {
	if vg.VGShared == "" {
		return "Local"
	}

	return "Shared"
}

func getSpecThinPools(thinPools map[string][]internal.LVData, vg internal.VGData) map[string]string {
	lvs := thinPools[vg.VGName+vg.VGUuid]
	tps := make(map[string]string, len(lvs))

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

func getStatusThinPools(log log.Logger, thinPools map[string][]internal.LVData, vg internal.VGData) []internal.LVMVGStatusThinPool {
	filtered := thinPools[vg.VGName+vg.VGUuid]
	tps := make([]internal.LVMVGStatusThinPool, 0, len(filtered))

	for _, lv := range filtered {
		usedSize, err := getUsedSizeMiB(lv)
		if err != nil {
			log.Error(err, "[getStatusThinPools] unable to getUsedSizeMiB")
		}
		tps = append(tps, internal.LVMVGStatusThinPool{
			Name:       lv.LVName,
			ActualSize: lv.LVSize,
			UsedSize:   usedSize,
		})
	}
	return tps
}

func getUsedSizeMiB(lv internal.LVData) (string, error) {
	size, err := units.ParseBase2Bytes(lv.LVSize + "iB")
	if err != nil {
		return "", err
	}

	var dataPercent float64
	if lv.DataPercent == "" {
		dataPercent = 0
	} else {
		dataPercent, err = strconv.ParseFloat(lv.DataPercent, 32)
		if err != nil {
			return "", err
		}
	}

	usedSize := float64(size/units.Mebibyte) * dataPercent / 100
	return strconv.Itoa(int(usedSize)) + "M", nil
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

func CreateLVMVolumeGroup(ctx context.Context, kc kclient.Client, candidate internal.LVMVolumeGroupCandidate) (*v1alpha1.LvmVolumeGroup, error) {
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

	if err := kc.Create(ctx, lvmVolumeGroup); err != nil {
		return nil, fmt.Errorf("unable to CreateLVMVolumeGroup, err: %w", err)
	}

	return lvmVolumeGroup, nil
}

func UpdateLVMVolumeGroupByCandidate(
	ctx context.Context,
	kc kclient.Client,
	resource v1alpha1.LvmVolumeGroup,
	candidate internal.LVMVolumeGroupCandidate,
) error {
	// The resource.Status.Nodes can not be just re-written, it needs to be updated directly by node.
	// We take all current resources nodes and convert them to map for better performance further.
	resourceNodes := make(map[string][]v1alpha1.LvmVolumeGroupDevice, len(resource.Status.Nodes))
	for _, node := range resource.Status.Nodes {
		resourceNodes[node.Name] = node.Devices
	}

	// Now we take our candidate's nodes, match them with resource's ones and upgrade devices for matched resource node.
	for candidateNode, devices := range candidate.Nodes {
		if _, match := resourceNodes[candidateNode]; match {
			resourceNodes[candidateNode] = convertLVMVGDevices(devices)
		}
	}

	// Now we take resource's nodes, match them with our map and fill with new info.
	for i, node := range resource.Status.Nodes {
		if devices, match := resourceNodes[node.Name]; match {
			resource.Status.Nodes[i].Devices = devices
		}
	}

	// Update status.
	lvmvg := &v1alpha1.LvmVolumeGroup{
		TypeMeta: metav1.TypeMeta{
			Kind:       v1alpha1.LVMVolumeGroupKind,
			APIVersion: v1alpha1.TypeMediaAPIVersion,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            resource.Name,
			OwnerReferences: resource.OwnerReferences,
			ResourceVersion: resource.ResourceVersion,
			Annotations:     resource.Annotations,
		},
		Spec: v1alpha1.LvmVolumeGroupSpec{
			ActualVGNameOnTheNode: resource.Spec.ActualVGNameOnTheNode,
			BlockDeviceNames:      resource.Spec.BlockDeviceNames,
			ThinPools:             resource.Spec.ThinPools,
			Type:                  resource.Spec.Type,
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

	if err := kc.Update(ctx, lvmvg); err != nil {
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

func convertSpecThinPools(thinPools map[string]string) []v1alpha1.SpecThinPool {
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

func GetAPILVMVolumeGroups(ctx context.Context, kc kclient.Client) (map[string]v1alpha1.LvmVolumeGroup, error) {
	listLvms := &v1alpha1.LvmVolumeGroupList{
		TypeMeta: metav1.TypeMeta{
			Kind:       v1alpha1.LVMVolumeGroupKind,
			APIVersion: v1alpha1.TypeMediaAPIVersion,
		},
		ListMeta: metav1.ListMeta{},
		Items:    []v1alpha1.LvmVolumeGroup{},
	}

	if err := kc.List(ctx, listLvms); err != nil {
		return nil, fmt.Errorf("[GetApiLVMVolumeGroups] unable to list lvm volume groups, err: %w", err)
	}

	lvms := make(map[string]v1alpha1.LvmVolumeGroup, len(listLvms.Items))
	for _, lvm := range listLvms.Items {
		lvms[lvm.Name] = lvm
	}

	return lvms, nil
}

func filterVGByTag(vgs []internal.VGData, tag string) []internal.VGData {
	filtered := make([]internal.VGData, 0, len(vgs))

	for _, vg := range vgs {
		if strings.Contains(vg.VGTags, tag) {
			filtered = append(filtered, vg)
		}
	}

	return filtered
}
