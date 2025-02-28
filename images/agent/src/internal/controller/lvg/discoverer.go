package lvg

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"agent/internal"
	"agent/internal/cache"
	"agent/internal/controller"
	"agent/internal/logger"
	"agent/internal/monitoring"
	"agent/internal/utils"
)

const DiscovererName = "lvm-volume-group-discover-controller"

type Discoverer struct {
	cl       client.Client
	log      logger.Logger
	lvgCl    *utils.LVGClient
	bdCl     *utils.BDClient
	metrics  monitoring.Metrics
	sdsCache *cache.Cache
	cfg      DiscovererConfig
}

type DiscovererConfig struct {
	NodeName                string
	VolumeGroupScanInterval time.Duration
}

func NewDiscoverer(
	cl client.Client,
	log logger.Logger,
	metrics monitoring.Metrics,
	sdsCache *cache.Cache,
	cfg DiscovererConfig,
) *Discoverer {
	return &Discoverer{
		cl:       cl,
		log:      log,
		lvgCl:    utils.NewLVGClient(cl, log, metrics, cfg.NodeName, DiscovererName),
		bdCl:     utils.NewBDClient(cl, metrics),
		metrics:  metrics,
		sdsCache: sdsCache,
		cfg:      cfg,
	}
}

func (d *Discoverer) Name() string {
	return DiscovererName
}

func (d *Discoverer) Discover(ctx context.Context) (controller.Result, error) {
	d.log.Info("[RunLVMVolumeGroupDiscoverController] Reconciler starts LVMVolumeGroup resources reconciliation")
	shouldRequeue := d.LVMVolumeGroupDiscoverReconcile(ctx)
	if shouldRequeue {
		d.log.Warning(fmt.Sprintf("[RunLVMVolumeGroupDiscoverController] an error occurred while run the Reconciler func, retry in %s", d.cfg.VolumeGroupScanInterval.String()))
		return controller.Result{
			RequeueAfter: d.cfg.VolumeGroupScanInterval,
		}, nil
	}
	d.log.Info("[RunLVMVolumeGroupDiscoverController] Reconciler successfully ended LVMVolumeGroup resources reconciliation")
	return controller.Result{}, nil
}

func (d *Discoverer) LVMVolumeGroupDiscoverReconcile(ctx context.Context) bool {
	reconcileStart := time.Now()
	d.log.Info("[RunLVMVolumeGroupDiscoverController] starts the reconciliation")

	currentLVMVGs, err := d.GetAPILVMVolumeGroups(ctx)
	if err != nil {
		d.log.Error(err, "[RunLVMVolumeGroupDiscoverController] unable to run GetAPILVMVolumeGroups")
		return true
	}

	if len(currentLVMVGs) == 0 {
		d.log.Debug("[RunLVMVolumeGroupDiscoverController] no current LVMVolumeGroups found")
	}

	blockDevices, err := d.bdCl.GetAPIBlockDevices(ctx, DiscovererName, nil)
	if err != nil {
		d.log.Error(err, "[RunLVMVolumeGroupDiscoverController] unable to GetAPIBlockDevices")
		for _, lvg := range currentLVMVGs {
			err = d.lvgCl.UpdateLVGConditionIfNeeded(ctx, &lvg, metav1.ConditionFalse, internal.TypeVGReady, "NoBlockDevices", fmt.Sprintf("unable to get block devices resources, err: %s", err.Error()))
			if err != nil {
				d.log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupDiscoverController] unable to add a condition %s to the LVMVolumeGroup %s", internal.TypeVGReady, lvg.Name))
			}
		}
		return true
	}

	if len(blockDevices) == 0 {
		d.log.Info("[RunLVMVolumeGroupDiscoverController] no BlockDevices were found")
		return false
	}
	d.log.Trace(fmt.Sprintf("[RunLVMVolumeGroupDiscoverController] BlockDevices: %+v", blockDevices))

	filteredLVGs := filterLVGsByNode(currentLVMVGs, d.cfg.NodeName)
	filteredBlockDevices := filterBlockDevicesByNodeName(blockDevices, d.cfg.NodeName)
	d.log.Trace(fmt.Sprintf("[RunLVMVolumeGroupDiscoverController] Filtered LVMVolumeGroups: %+v", filteredLVGs))
	d.log.Trace(fmt.Sprintf("[RunLVMVolumeGroupDiscoverController] Filtered BlockDevices: %+v", filteredBlockDevices))

	d.log.Debug("[RunLVMVolumeGroupDiscoverController] tries to get LVMVolumeGroup candidates")

	candidates, err := d.GetLVMVolumeGroupCandidates(filteredBlockDevices)
	if err != nil {
		d.log.Error(err, "[RunLVMVolumeGroupDiscoverController] unable to run GetLVMVolumeGroupCandidates")
		for _, lvg := range filteredLVGs {
			d.log.Trace(fmt.Sprintf("[RunLVMVolumeGroupDiscoverController] turn LVMVolumeGroup %s to non operational. LVG struct: %+v ", lvg.Name, lvg))
			err = d.lvgCl.UpdateLVGConditionIfNeeded(ctx, &lvg, metav1.ConditionFalse, internal.TypeVGReady, "DataConfigurationFailed", fmt.Sprintf("unable to configure data, err: %s", err.Error()))
			if err != nil {
				d.log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupDiscoverController] unable to add a condition %s to the LVMVolumeGroup %s", internal.TypeVGReady, lvg.Name))
			}
		}
		return true
	}
	d.log.Debug("[RunLVMVolumeGroupDiscoverController] successfully got LVMVolumeGroup candidates")

	if len(candidates) == 0 {
		d.log.Debug("[RunLVMVolumeGroupDiscoverController] no candidates were found on the node")
	}

	candidates, err = d.ReconcileUnhealthyLVMVolumeGroups(ctx, candidates, filteredLVGs)
	if err != nil {
		d.log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupDiscoverController] an error has occurred while clearing the LVMVolumeGroups resources. Requeue the request in %s", d.cfg.VolumeGroupScanInterval.String()))
		return true
	}

	shouldRequeue := false
	for _, candidate := range candidates {
		d.log.Trace(fmt.Sprintf("[RunLVMVolumeGroupDiscoverController] candidate: %+v", candidate))
		if lvg, exist := filteredLVGs[candidate.ActualVGNameOnTheNode]; exist {
			d.log.Debug(fmt.Sprintf("[RunLVMVolumeGroupDiscoverController] the LVMVolumeGroup %s is already exist. Tries to update it", lvg.Name))
			d.log.Trace(fmt.Sprintf("[RunLVMVolumeGroupDiscoverController] lvg: %+v", lvg))

			if !hasLVMVolumeGroupDiff(d.log, lvg, candidate) {
				d.log.Debug(fmt.Sprintf(`[RunLVMVolumeGroupDiscoverController] no data to update for LVMVolumeGroup, name: "%s"`, lvg.Name))
				err = d.lvgCl.UpdateLVGConditionIfNeeded(ctx, &lvg, metav1.ConditionTrue, internal.TypeVGReady, internal.ReasonUpdated, "ready to create LV")
				if err != nil {
					d.log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupDiscoverController] unable to add a condition %s to the LVMVolumeGroup %s", internal.TypeVGReady, lvg.Name))
					shouldRequeue = true
				}
				continue
			}

			d.log.Debug(fmt.Sprintf("[RunLVMVolumeGroupDiscoverController] the LVMVolumeGroup %s should be updated", lvg.Name))
			if err = d.UpdateLVMVolumeGroupByCandidate(ctx, &lvg, candidate); err != nil {
				d.log.Error(err, fmt.Sprintf(`[RunLVMVolumeGroupDiscoverController] unable to update LVMVolumeGroup, name: "%s". Requeue the request in %s`,
					lvg.Name, d.cfg.VolumeGroupScanInterval.String()))
				shouldRequeue = true
				continue
			}

			d.log.Info(fmt.Sprintf(`[RunLVMVolumeGroupDiscoverController] updated LVMVolumeGroup, name: "%s"`, lvg.Name))
		} else {
			d.log.Debug(fmt.Sprintf("[RunLVMVolumeGroupDiscoverController] the LVMVolumeGroup %s is not yet created. Create it", candidate.LVMVGName))
			createdLvg, err := d.CreateLVMVolumeGroupByCandidate(ctx, candidate)
			if err != nil {
				d.log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupDiscoverController] unable to CreateLVMVolumeGroupByCandidate %s. Requeue the request in %s", candidate.LVMVGName, d.cfg.VolumeGroupScanInterval.String()))
				shouldRequeue = true
				continue
			}

			err = d.lvgCl.UpdateLVGConditionIfNeeded(ctx, &lvg, metav1.ConditionTrue, internal.TypeVGConfigurationApplied, internal.ReasonApplied, "all configuration has been applied")
			if err != nil {
				d.log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupDiscoverController] unable to add a condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, createdLvg.Name))
				shouldRequeue = true
				continue
			}

			err = d.lvgCl.UpdateLVGConditionIfNeeded(ctx, &lvg, metav1.ConditionTrue, internal.TypeVGReady, internal.ReasonUpdated, "ready to create LV")
			if err != nil {
				d.log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupDiscoverController] unable to add a condition %s to the LVMVolumeGroup %s", internal.TypeVGReady, createdLvg.Name))
				shouldRequeue = true
				continue
			}

			d.log.Info(fmt.Sprintf(`[RunLVMVolumeGroupDiscoverController] created new APILVMVolumeGroup, name: "%s"`, createdLvg.Name))
		}
	}

	if shouldRequeue {
		d.log.Warning(fmt.Sprintf("[RunLVMVolumeGroupDiscoverController] some problems have been occurred while iterating the lvmvolumegroup resources. Retry the reconcile in %s", d.cfg.VolumeGroupScanInterval.String()))
		return true
	}

	d.log.Info("[RunLVMVolumeGroupDiscoverController] END discovery loop")
	d.metrics.ReconcileDuration(DiscovererName).Observe(d.metrics.GetEstimatedTimeInSeconds(reconcileStart))
	d.metrics.ReconcilesCountTotal(DiscovererName).Inc()
	return false
}

func (d *Discoverer) GetAPILVMVolumeGroups(ctx context.Context) (map[string]v1alpha1.LVMVolumeGroup, error) {
	lvgList := &v1alpha1.LVMVolumeGroupList{}

	start := time.Now()
	err := d.cl.List(ctx, lvgList)
	d.metrics.APIMethodsDuration(DiscovererName, "list").Observe(d.metrics.GetEstimatedTimeInSeconds(start))
	d.metrics.APIMethodsExecutionCount(DiscovererName, "list").Inc()
	if err != nil {
		d.metrics.APIMethodsErrors(DiscovererName, "list").Inc()
		return nil, fmt.Errorf("[GetApiLVMVolumeGroups] unable to list LVMVolumeGroups, err: %w", err)
	}

	lvgs := make(map[string]v1alpha1.LVMVolumeGroup, len(lvgList.Items))
	for _, lvg := range lvgList.Items {
		lvgs[lvg.Name] = lvg
	}

	return lvgs, nil
}

// ReconcileUnhealthyLVMVolumeGroups turns LVMVolumeGroup resources without VG or ThinPools to NotReady.
func (d *Discoverer) ReconcileUnhealthyLVMVolumeGroups(
	ctx context.Context,
	candidates []internal.LVMVolumeGroupCandidate,
	lvgs map[string]v1alpha1.LVMVolumeGroup,
) ([]internal.LVMVolumeGroupCandidate, error) {
	candidateMap := make(map[string]internal.LVMVolumeGroupCandidate, len(candidates))
	for _, candidate := range candidates {
		candidateMap[candidate.ActualVGNameOnTheNode] = candidate
	}
	vgNamesToSkip := make(map[string]struct{}, len(candidates))

	var err error
	for _, lvg := range lvgs {
		// this means VG was actually created on the node before
		if len(lvg.Status.VGUuid) > 0 {
			messageBldr := strings.Builder{}
			candidate, exist := candidateMap[lvg.Spec.ActualVGNameOnTheNode]
			if !exist {
				d.log.Warning(fmt.Sprintf("[ReconcileUnhealthyLVMVolumeGroups] the LVMVolumeGroup %s misses its VG %s", lvg.Name, lvg.Spec.ActualVGNameOnTheNode))
				messageBldr.WriteString(fmt.Sprintf("Unable to find VG %s (it should be created with special tag %s). ", lvg.Spec.ActualVGNameOnTheNode, internal.LVMTags[0]))
			} else {
				// candidate exists, check thin pools
				candidateTPs := make(map[string]internal.LVMVGStatusThinPool, len(candidate.StatusThinPools))
				for _, tp := range candidate.StatusThinPools {
					candidateTPs[tp.Name] = tp
				}

				// take thin-pools from status instead of spec to prevent miss never-created ones
				for i, statusTp := range lvg.Status.ThinPools {
					if candidateTp, exist := candidateTPs[statusTp.Name]; !exist {
						d.log.Warning(fmt.Sprintf("[ReconcileUnhealthyLVMVolumeGroups] the LVMVolumeGroup %s misses its ThinPool %s", lvg.Name, statusTp.Name))
						messageBldr.WriteString(fmt.Sprintf("Unable to find ThinPool %s. ", statusTp.Name))
						lvg.Status.ThinPools[i].Ready = false
					} else if !utils.AreSizesEqualWithinDelta(candidate.VGSize, statusTp.ActualSize, internal.ResizeDelta) &&
						candidateTp.ActualSize.Value()+internal.ResizeDelta.Value() < statusTp.ActualSize.Value() {
						// that means thin-pool is not 100%VG space
						// use candidate VGSize as lvg.Status.VGSize might not be updated yet
						d.log.Warning(fmt.Sprintf("[ReconcileUnhealthyLVMVolumeGroups] the LVMVolumeGroup %s ThinPool %s size %s is less than status one %s", lvg.Name, statusTp.Name, candidateTp.ActualSize.String(), statusTp.ActualSize.String()))
						messageBldr.WriteString(fmt.Sprintf("ThinPool %s on the node has size %s which is less than status one %s. ", statusTp.Name, candidateTp.ActualSize.String(), statusTp.ActualSize.String()))
					}
				}
			}

			if messageBldr.Len() > 0 {
				err = d.lvgCl.UpdateLVGConditionIfNeeded(ctx, &lvg, metav1.ConditionFalse, internal.TypeVGReady, internal.ReasonScanFailed, messageBldr.String())
				if err != nil {
					d.log.Error(err, fmt.Sprintf("[ReconcileUnhealthyLVMVolumeGroups] unable to update the LVMVolumeGroup %s", lvg.Name))
					return nil, err
				}

				d.log.Warning(fmt.Sprintf("[ReconcileUnhealthyLVMVolumeGroups] the LVMVolumeGroup %s and its data obejct will be removed from the reconcile due to unhealthy states", lvg.Name))
				vgNamesToSkip[candidate.ActualVGNameOnTheNode] = struct{}{}
			}
		}
	}

	for _, lvg := range lvgs {
		if _, shouldSkip := vgNamesToSkip[lvg.Spec.ActualVGNameOnTheNode]; shouldSkip {
			d.log.Warning(fmt.Sprintf("[ReconcileUnhealthyLVMVolumeGroups] remove the LVMVolumeGroup %s from the reconcile", lvg.Name))
			delete(lvgs, lvg.Spec.ActualVGNameOnTheNode)
		}
	}

	for i, c := range candidates {
		if _, shouldSkip := vgNamesToSkip[c.ActualVGNameOnTheNode]; shouldSkip {
			d.log.Debug(fmt.Sprintf("[ReconcileUnhealthyLVMVolumeGroups] remove the data object for VG %s from the reconcile", c.ActualVGNameOnTheNode))
			candidates = append(candidates[:i], candidates[i+1:]...)
		}
	}

	return candidates, nil
}

func (d *Discoverer) GetLVMVolumeGroupCandidates(bds map[string]v1alpha1.BlockDevice) ([]internal.LVMVolumeGroupCandidate, error) {
	vgs, vgErrs := d.sdsCache.GetVGs()
	vgWithTag := filterVGByTag(vgs, internal.LVMTags)
	candidates := make([]internal.LVMVolumeGroupCandidate, 0, len(vgWithTag))

	// If there is no VG with our tag, then there is no any candidate.
	if len(vgWithTag) == 0 {
		return candidates, nil
	}

	// If vgErrs is not empty, that means we have some problems on vgs, so we need to identify unhealthy vgs.
	var vgIssues map[string]string
	if vgErrs.Len() != 0 {
		d.log.Warning("[GetLVMVolumeGroupCandidates] some errors have been occurred while executing vgs command")
		vgIssues = sortVGIssuesByVG(d.log, vgWithTag)
	}

	pvs, pvErrs := d.sdsCache.GetPVs()
	if len(pvs) == 0 {
		err := errors.New("no PV found")
		d.log.Error(err, "[GetLVMVolumeGroupCandidates] no PV was found, but VG with tags are not empty")
		return nil, err
	}

	// If pvErrs is not empty, that means we have some problems on vgs, so we need to identify unhealthy vgs.
	var pvIssues map[string][]string
	if pvErrs.Len() != 0 {
		d.log.Warning("[GetLVMVolumeGroupCandidates] some errors have been occurred while executing pvs command")
		pvIssues = sortPVIssuesByVG(d.log, pvs)
	}

	lvs, lvErrs := d.sdsCache.GetLVs()
	var thinPools []internal.LVData
	if len(lvs) > 0 {
		// Filter LV to get only thin pools as we do not support thick for now.
		thinPools = getThinPools(lvs)
	}

	// If lvErrs is not empty, that means we have some problems on vgs, so we need to identify unhealthy vgs.
	var lvIssues map[string]map[string]string
	if lvErrs.Len() != 0 {
		d.log.Warning("[GetLVMVolumeGroupCandidates] some errors have been occurred while executing lvs command")
		lvIssues = sortThinPoolIssuesByVG(d.log, thinPools)
	}

	// Sort PV,BlockDevices and LV by VG to fill needed information for LVMVolumeGroup resource further.
	sortedPVs := sortPVsByVG(pvs, vgWithTag)
	sortedBDs := sortBlockDevicesByVG(bds, vgWithTag)
	d.log.Trace(fmt.Sprintf("[GetLVMVolumeGroupCandidates] BlockDevices: %+v", bds))
	d.log.Trace(fmt.Sprintf("[GetLVMVolumeGroupCandidates] Sorted BlockDevices: %+v", sortedBDs))
	sortedThinPools := sortThinPoolsByVG(thinPools, vgWithTag)
	sortedLVByThinPool := sortLVByThinPool(lvs)

	for _, vg := range vgWithTag {
		allocateSize := getVGAllocatedSize(vg)
		health, message := checkVGHealth(vgIssues, pvIssues, lvIssues, vg)

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
			StatusThinPools:       getStatusThinPools(d.log, sortedThinPools, sortedLVByThinPool, vg, lvIssues),
			VGSize:                *resource.NewQuantity(vg.VGSize.Value(), resource.BinarySI),
			VGFree:                *resource.NewQuantity(vg.VGFree.Value(), resource.BinarySI),
			VGUUID:                vg.VGUUID,
			Nodes:                 d.configureCandidateNodeDevices(sortedPVs, sortedBDs, vg, d.cfg.NodeName),
		}

		candidates = append(candidates, candidate)
	}

	return candidates, nil
}

func (d *Discoverer) CreateLVMVolumeGroupByCandidate(
	ctx context.Context,
	candidate internal.LVMVolumeGroupCandidate,
) (*v1alpha1.LVMVolumeGroup, error) {
	thinPools, err := convertStatusThinPools(v1alpha1.LVMVolumeGroup{}, candidate.StatusThinPools)
	if err != nil {
		return nil, err
	}

	lvmVolumeGroup := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:            candidate.LVMVGName,
			OwnerReferences: []metav1.OwnerReference{},
			Finalizers:      candidate.Finalizers,
		},
		Spec: v1alpha1.LVMVolumeGroupSpec{
			ActualVGNameOnTheNode: candidate.ActualVGNameOnTheNode,
			BlockDeviceSelector:   configureBlockDeviceSelector(candidate),
			ThinPools:             convertSpecThinPools(candidate.SpecThinPools),
			Type:                  candidate.Type,
			Local:                 v1alpha1.LVMVolumeGroupLocalSpec{NodeName: d.cfg.NodeName},
		},
		Status: v1alpha1.LVMVolumeGroupStatus{
			AllocatedSize: candidate.AllocatedSize,
			Nodes:         convertLVMVGNodes(candidate.Nodes),
			ThinPools:     thinPools,
			VGSize:        candidate.VGSize,
			VGUuid:        candidate.VGUUID,
			VGFree:        candidate.VGFree,
		},
	}

	for _, node := range candidate.Nodes {
		for _, dev := range node {
			i := len(dev.BlockDevice)
			if i == 0 {
				d.log.Warning("The attempt to create the LVG resource failed because it was not possible to find a BlockDevice for it.")
				return lvmVolumeGroup, nil
			}
		}
	}

	start := time.Now()
	err = d.cl.Create(ctx, lvmVolumeGroup)
	d.metrics.APIMethodsDuration(DiscovererName, "create").Observe(d.metrics.GetEstimatedTimeInSeconds(start))
	d.metrics.APIMethodsExecutionCount(DiscovererName, "create").Inc()
	if err != nil {
		d.metrics.APIMethodsErrors(DiscovererName, "create").Inc()
		return nil, fmt.Errorf("unable to сreate LVMVolumeGroup, err: %w", err)
	}

	return lvmVolumeGroup, nil
}

func (d *Discoverer) UpdateLVMVolumeGroupByCandidate(
	ctx context.Context,
	lvg *v1alpha1.LVMVolumeGroup,
	candidate internal.LVMVolumeGroupCandidate,
) error {
	// Check if VG has some problems
	if candidate.Health == internal.NonOperational {
		d.log.Warning(fmt.Sprintf("[UpdateLVMVolumeGroupByCandidate] candidate for LVMVolumeGroup %s has NonOperational health, message %s. Update the VGReady condition to False", lvg.Name, candidate.Message))
		updErr := d.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, metav1.ConditionFalse, internal.TypeVGReady, internal.ReasonScanFailed, candidate.Message)
		if updErr != nil {
			d.log.Error(updErr, fmt.Sprintf("[UpdateLVMVolumeGroupByCandidate] unable to add a condition %s to the LVMVolumeGroup %s", internal.TypeVGReady, lvg.Name))
		}
		return updErr
	}

	// The resource.Status.Nodes can not be just re-written, it needs to be updated directly by a node.
	// We take all current resources nodes and convert them to map for better performance further.
	resourceNodes := make(map[string][]v1alpha1.LVMVolumeGroupDevice, len(lvg.Status.Nodes))
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
	thinPools, err := convertStatusThinPools(*lvg, candidate.StatusThinPools)
	if err != nil {
		d.log.Error(err, fmt.Sprintf("[UpdateLVMVolumeGroupByCandidate] unable to convert status thin pools for the LVMVolumeGroup %s", lvg.Name))
		return err
	}

	lvg.Status.AllocatedSize = candidate.AllocatedSize
	lvg.Status.Nodes = convertLVMVGNodes(candidate.Nodes)
	lvg.Status.ThinPools = thinPools
	lvg.Status.VGSize = candidate.VGSize
	lvg.Status.VGFree = candidate.VGFree
	lvg.Status.VGUuid = candidate.VGUUID

	lvg.Spec.BlockDeviceSelector, _ = updateBlockDeviceSelectorIfNeeded(lvg.Spec.BlockDeviceSelector, candidate.BlockDevicesNames)

	d.log.Trace(fmt.Sprintf("[UpdateLVMVolumeGroupByCandidate] updated LVMVolumeGroup: %+v", lvg))

	start := time.Now()
	err = d.cl.Status().Update(ctx, lvg)
	d.metrics.APIMethodsDuration(DiscovererName, "update").Observe(d.metrics.GetEstimatedTimeInSeconds(start))
	d.metrics.APIMethodsExecutionCount(DiscovererName, "update").Inc()
	if err != nil {
		d.metrics.APIMethodsErrors(DiscovererName, "update").Inc()
		return fmt.Errorf(`[UpdateLVMVolumeGroupByCandidate] unable to update LVMVolumeGroup, name: "%s", err: %w`, lvg.Name, err)
	}

	err = d.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, metav1.ConditionTrue, internal.TypeVGReady, internal.ReasonUpdated, "ready to create LV")
	if err != nil {
		d.log.Error(err, fmt.Sprintf("[UpdateLVMVolumeGroupByCandidate] unable to add a condition %s to the LVMVolumeGroup %s", internal.TypeVGReady, lvg.Name))
	}

	return err
}

func (d *Discoverer) configureCandidateNodeDevices(pvs map[string][]internal.PVData, bds map[string][]v1alpha1.BlockDevice, vg internal.VGData, currentNode string) map[string][]internal.LVMVGDevice {
	filteredPV := pvs[vg.VGName+vg.VGUUID]
	filteredBds := bds[vg.VGName+vg.VGUUID]
	bdPathStatus := make(map[string]v1alpha1.BlockDevice, len(bds))
	result := make(map[string][]internal.LVMVGDevice, len(filteredPV))

	for _, blockDevice := range filteredBds {
		bdPathStatus[blockDevice.Status.Path] = blockDevice
	}

	for _, pv := range filteredPV {
		bd, exist := bdPathStatus[pv.PVName]
		// this is very rare case which might occurred while VG extend operation goes. In this case, in the cache the controller
		// sees a new PV included in the VG, but BlockDeviceDiscover did not update the corresponding BlockDevice resource on time,
		// so the BlockDevice resource does not have any info, that it is in the VG.
		if !exist {
			d.log.Warning(fmt.Sprintf("[configureCandidateNodeDevices] no BlockDevice resource is yet configured for PV %s in VG %s, retry on the next iteration", pv.PVName, vg.VGName))
			continue
		}

		device := internal.LVMVGDevice{
			Path:   pv.PVName,
			PVSize: *resource.NewQuantity(pv.PVSize.Value(), resource.BinarySI),
			PVUUID: pv.PVUuid,
		}

		device.DevSize = *resource.NewQuantity(bd.Status.Size.Value(), resource.BinarySI)
		device.BlockDevice = bd.Name

		result[currentNode] = append(result[currentNode], device)
	}

	return result
}

func checkVGHealth(vgIssues map[string]string, pvIssues map[string][]string, lvIssues map[string]map[string]string, vg internal.VGData) (health, message string) {
	issues := make([]string, 0, len(vgIssues)+len(pvIssues)+len(lvIssues)+1)

	if vgIssue, exist := vgIssues[vg.VGName+vg.VGUUID]; exist {
		issues = append(issues, vgIssue)
	}

	if pvIssue, exist := pvIssues[vg.VGName+vg.VGUUID]; exist {
		issues = append(issues, strings.Join(pvIssue, ""))
	}

	if lvIssue, exist := lvIssues[vg.VGName+vg.VGUUID]; exist {
		for lvName, issue := range lvIssue {
			issues = append(issues, fmt.Sprintf("%s: %s", lvName, issue))
		}
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

func sortThinPoolIssuesByVG(log logger.Logger, lvs []internal.LVData) map[string]map[string]string {
	var lvIssuesByVG = make(map[string]map[string]string, len(lvs))

	for _, lv := range lvs {
		_, cmd, stdErr, err := utils.GetLV(lv.VGName, lv.LVName)
		log.Debug(fmt.Sprintf("[sortThinPoolIssuesByVG] runs cmd: %s", cmd))

		if err != nil {
			log.Error(err, fmt.Sprintf(`[sortThinPoolIssuesByVG] unable to run lvs command for lv, name: "%s"`, lv.LVName))
			lvIssuesByVG[lv.VGName+lv.VGUuid] = make(map[string]string, len(lvs))
			lvIssuesByVG[lv.VGName+lv.VGUuid][lv.LVName] = err.Error()
		}

		if stdErr.Len() != 0 {
			log.Error(errors.New(stdErr.String()), fmt.Sprintf(`[sortThinPoolIssuesByVG] lvs command for lv "%s" has stderr: `, lv.LVName))
			lvIssuesByVG[lv.VGName+lv.VGUuid] = make(map[string]string, len(lvs))
			lvIssuesByVG[lv.VGName+lv.VGUuid][lv.LVName] = stdErr.String()
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
			log.Error(errors.New(stdErr.String()), fmt.Sprintf(`[sortPVIssuesByVG] pvs command for pv "%s" has stderr: %s`, pv.PVName, stdErr.String()))
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
			vgIssues[vg.VGName+vg.VGUUID] = err.Error()
		}

		if stdErr.Len() != 0 {
			log.Error(errors.New(stdErr.String()), fmt.Sprintf(`[sortVGIssuesByVG] vgs command for vg "%s" has stderr: `, vg.VGName))
			vgIssues[vg.VGName+vg.VGUUID] = stdErr.String()
			stdErr.Reset()
		}
	}

	return vgIssues
}

func sortLVByThinPool(lvs []internal.LVData) map[string][]internal.LVData {
	result := make(map[string][]internal.LVData, len(lvs))

	for _, lv := range lvs {
		if len(lv.PoolName) > 0 {
			result[lv.PoolName] = append(result[lv.PoolName], lv)
		}
	}

	return result
}

func sortThinPoolsByVG(lvs []internal.LVData, vgs []internal.VGData) map[string][]internal.LVData {
	result := make(map[string][]internal.LVData, len(vgs))
	for _, vg := range vgs {
		result[vg.VGName+vg.VGUUID] = make([]internal.LVData, 0, len(lvs))
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
		result[vg.VGName+vg.VGUUID] = make([]internal.PVData, 0, len(pvs))
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
		result[vg.VGName+vg.VGUUID] = make([]v1alpha1.BlockDevice, 0, len(bds))
	}

	for _, bd := range bds {
		if _, ok := result[bd.Status.ActualVGNameOnTheNode+bd.Status.VGUuid]; ok {
			result[bd.Status.ActualVGNameOnTheNode+bd.Status.VGUuid] = append(result[bd.Status.ActualVGNameOnTheNode+bd.Status.VGUuid], bd)
		}
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
	lvs := thinPools[vg.VGName+vg.VGUUID]
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

func getStatusThinPools(log logger.Logger, thinPools, sortedLVs map[string][]internal.LVData, vg internal.VGData, lvIssues map[string]map[string]string) []internal.LVMVGStatusThinPool {
	tps := thinPools[vg.VGName+vg.VGUUID]
	result := make([]internal.LVMVGStatusThinPool, 0, len(tps))

	for _, thinPool := range tps {
		usedSize, err := thinPool.GetUsedSize()
		log.Trace(fmt.Sprintf("[getStatusThinPools] LV %v for VG name %s", thinPool, vg.VGName))
		if err != nil {
			log.Error(err, "[getStatusThinPools] unable to getThinPoolUsedSize")
		}

		allocatedSize := getThinPoolAllocatedSize(thinPool.LVName, sortedLVs[thinPool.LVName])
		tp := internal.LVMVGStatusThinPool{
			Name:          thinPool.LVName,
			ActualSize:    *resource.NewQuantity(thinPool.LVSize.Value(), resource.BinarySI),
			UsedSize:      *resource.NewQuantity(usedSize.Value(), resource.BinarySI),
			AllocatedSize: *resource.NewQuantity(allocatedSize, resource.BinarySI),
			Ready:         true,
			Message:       "",
		}

		if lverrs, exist := lvIssues[vg.VGName+vg.VGUUID][thinPool.LVName]; exist {
			tp.Ready = false
			tp.Message = lverrs
		}

		result = append(result, tp)
	}
	return result
}

func getThinPoolAllocatedSize(tpName string, lvs []internal.LVData) int64 {
	var size int64
	for _, lv := range lvs {
		if lv.PoolName == tpName {
			size += lv.LVSize.Value()
		}
	}

	return size
}

func getBlockDevicesNames(bds map[string][]v1alpha1.BlockDevice, vg internal.VGData) []string {
	sorted := bds[vg.VGName+vg.VGUUID]
	names := make([]string, 0, len(sorted))

	for _, bd := range sorted {
		names = append(names, bd.Name)
	}

	return names
}

func filterLVGsByNode(lvgs map[string]v1alpha1.LVMVolumeGroup, currentNode string) map[string]v1alpha1.LVMVolumeGroup {
	filtered := make(map[string]v1alpha1.LVMVolumeGroup, len(lvgs))
	for _, lvg := range lvgs {
		if lvg.Spec.Local.NodeName == currentNode {
			filtered[lvg.Spec.ActualVGNameOnTheNode] = lvg
		}
	}

	return filtered
}

func hasLVMVolumeGroupDiff(log logger.Logger, lvg v1alpha1.LVMVolumeGroup, candidate internal.LVMVolumeGroupCandidate) bool {
	convertedStatusPools, err := convertStatusThinPools(lvg, candidate.StatusThinPools)
	if err != nil {
		log.Error(err, fmt.Sprintf("[hasLVMVolumeGroupDiff] unable to identify candidate difference for the LVMVolumeGroup %s", lvg.Name))
		return false
	}
	log.Trace(fmt.Sprintf(`AllocatedSize, candidate: %s, lvg: %s`, candidate.AllocatedSize.String(), lvg.Status.AllocatedSize.String()))
	log.Trace(fmt.Sprintf(`ThinPools, candidate: %+v, lvg: %+v`, convertedStatusPools, lvg.Status.ThinPools))
	for _, tp := range convertedStatusPools {
		log.Trace(fmt.Sprintf("Candidate ThinPool name: %s, actual size: %s, used size: %s", tp.Name, tp.ActualSize.String(), tp.UsedSize.String()))
	}
	for _, tp := range lvg.Status.ThinPools {
		log.Trace(fmt.Sprintf("Resource ThinPool name: %s, actual size: %s, used size: %s", tp.Name, tp.ActualSize.String(), tp.UsedSize.String()))
	}
	log.Trace(fmt.Sprintf(`VGSize, candidate: %s, lvg: %s`, candidate.VGSize.String(), lvg.Status.VGSize.String()))
	log.Trace(fmt.Sprintf(`VGUUID, candidate: %s, lvg: %s`, candidate.VGUUID, lvg.Status.VGUuid))
	log.Trace(fmt.Sprintf(`Nodes, candidate: %+v, lvg: %+v`, convertLVMVGNodes(candidate.Nodes), lvg.Status.Nodes))

	_, blockDeviceSelectorUpdated := updateBlockDeviceSelectorIfNeeded(lvg.Spec.BlockDeviceSelector, candidate.BlockDevicesNames)

	return candidate.AllocatedSize.Value() != lvg.Status.AllocatedSize.Value() ||
		hasStatusPoolDiff(convertedStatusPools, lvg.Status.ThinPools) ||
		candidate.VGSize.Value() != lvg.Status.VGSize.Value() ||
		candidate.VGFree.Value() != lvg.Status.VGFree.Value() ||
		candidate.VGUUID != lvg.Status.VGUuid ||
		hasStatusNodesDiff(log, convertLVMVGNodes(candidate.Nodes), lvg.Status.Nodes) ||
		blockDeviceSelectorUpdated
}

func hasStatusNodesDiff(log logger.Logger, first, second []v1alpha1.LVMVolumeGroupNode) bool {
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

func hasStatusPoolDiff(first, second []v1alpha1.LVMVolumeGroupThinPoolStatus) bool {
	if len(first) != len(second) {
		return true
	}

	for i := range first {
		if first[i].Name != second[i].Name ||
			first[i].UsedSize.Value() != second[i].UsedSize.Value() ||
			first[i].ActualSize.Value() != second[i].ActualSize.Value() ||
			first[i].AllocatedSize.Value() != second[i].AllocatedSize.Value() ||
			first[i].Ready != second[i].Ready ||
			first[i].Message != second[i].Message ||
			first[i].AvailableSpace.Value() != second[i].AvailableSpace.Value() {
			return true
		}
	}

	return false
}

func configureBlockDeviceSelector(candidate internal.LVMVolumeGroupCandidate) *metav1.LabelSelector {
	return &metav1.LabelSelector{
		MatchExpressions: []metav1.LabelSelectorRequirement{
			{
				Key:      internal.MetadataNameLabelKey,
				Operator: metav1.LabelSelectorOpIn,
				Values:   candidate.BlockDevicesNames,
			},
		},
	}
}

func updateBlockDeviceSelectorIfNeeded(existedLabelSelector *metav1.LabelSelector, blockDeviceNames []string) (*metav1.LabelSelector, bool) {
	if existedLabelSelector == nil {
		return &metav1.LabelSelector{
			MatchExpressions: []metav1.LabelSelectorRequirement{
				{
					Key:      internal.MetadataNameLabelKey,
					Operator: metav1.LabelSelectorOpIn,
					Values:   blockDeviceNames,
				},
			},
		}, true
	}

	updated := false
	found := false
	for i := range existedLabelSelector.MatchExpressions {
		if existedLabelSelector.MatchExpressions[i].Key == internal.MetadataNameLabelKey {
			found = true

			existingValuesMap := make(map[string]struct{})
			for _, v := range existedLabelSelector.MatchExpressions[i].Values {
				existingValuesMap[v] = struct{}{}
			}

			for _, bd := range blockDeviceNames {
				if _, exist := existingValuesMap[bd]; !exist {
					existedLabelSelector.MatchExpressions[i].Values = append(existedLabelSelector.MatchExpressions[i].Values, bd)
					existingValuesMap[bd] = struct{}{}
					updated = true
				}
			}
		}
	}

	if !found {
		existedLabelSelector.MatchExpressions = append(existedLabelSelector.MatchExpressions, metav1.LabelSelectorRequirement{
			Key:      internal.MetadataNameLabelKey,
			Operator: metav1.LabelSelectorOpIn,
			Values:   blockDeviceNames,
		})
		updated = true
	}
	return existedLabelSelector, updated
}

func convertLVMVGNodes(nodes map[string][]internal.LVMVGDevice) []v1alpha1.LVMVolumeGroupNode {
	lvmvgNodes := make([]v1alpha1.LVMVolumeGroupNode, 0, len(nodes))

	for nodeName, nodeDevices := range nodes {
		lvmvgNodes = append(lvmvgNodes, v1alpha1.LVMVolumeGroupNode{
			Devices: convertLVMVGDevices(nodeDevices),
			Name:    nodeName,
		})
	}

	return lvmvgNodes
}

func convertLVMVGDevices(devices []internal.LVMVGDevice) []v1alpha1.LVMVolumeGroupDevice {
	convertedDevices := make([]v1alpha1.LVMVolumeGroupDevice, 0, len(devices))

	for _, dev := range devices {
		convertedDevices = append(convertedDevices, v1alpha1.LVMVolumeGroupDevice{
			BlockDevice: dev.BlockDevice,
			DevSize:     dev.DevSize,
			PVSize:      dev.PVSize,
			PVUuid:      dev.PVUUID,
			Path:        dev.Path,
		})
	}

	return convertedDevices
}

func convertSpecThinPools(thinPools map[string]resource.Quantity) []v1alpha1.LVMVolumeGroupThinPoolSpec {
	result := make([]v1alpha1.LVMVolumeGroupThinPoolSpec, 0, len(thinPools))
	for name, size := range thinPools {
		result = append(result, v1alpha1.LVMVolumeGroupThinPoolSpec{
			Name:            name,
			AllocationLimit: "150%",
			Size:            size.String(),
		})
	}

	return result
}

func convertStatusThinPools(lvg v1alpha1.LVMVolumeGroup, thinPools []internal.LVMVGStatusThinPool) ([]v1alpha1.LVMVolumeGroupThinPoolStatus, error) {
	tpLimits := make(map[string]string, len(lvg.Spec.ThinPools))
	for _, tp := range lvg.Spec.ThinPools {
		tpLimits[tp.Name] = tp.AllocationLimit
	}

	result := make([]v1alpha1.LVMVolumeGroupThinPoolStatus, 0, len(thinPools))
	for _, tp := range thinPools {
		limit := tpLimits[tp.Name]
		if len(limit) == 0 {
			limit = internal.AllocationLimitDefaultValue
		}

		freeSpace, err := utils.GetThinPoolAvailableSpace(tp.ActualSize, tp.AllocatedSize, limit)
		if err != nil {
			return nil, err
		}

		result = append(result, v1alpha1.LVMVolumeGroupThinPoolStatus{
			Name:            tp.Name,
			ActualSize:      tp.ActualSize,
			AllocationLimit: limit,
			AllocatedSize:   tp.AllocatedSize,
			AvailableSpace:  freeSpace,
			UsedSize:        tp.UsedSize,
			Ready:           tp.Ready,
			Message:         tp.Message,
		})
	}

	return result, nil
}

func generateLVMVGName() string {
	return "vg-" + string(uuid.NewUUID())
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
