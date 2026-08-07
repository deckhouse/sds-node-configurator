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

package lvg

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/cache"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/controller"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/logger"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/monitoring"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/repository"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/utils"
)

const DiscovererName = "lvm-volume-group-discover-controller"

type Discoverer struct {
	cl       client.Client
	log      logger.Logger
	lvgCl    *repository.LVGClient
	bdCl     *repository.BDClient
	metrics  *monitoring.Metrics
	sdsCache *cache.Cache
	cfg      DiscovererConfig
	commands utils.Commands
	// resolver resolves a host path to its canonical form. It is only used to
	// learn what the configured file-devices base directory really points at, so
	// a backing file reported under the resolved path can be written into a spec
	// as a path under the configured one. Defaults to
	// utils.HostNsenterCanonicalResolver; overridable in tests, mirroring
	// Reconciler.resolver.
	resolver utils.CanonicalPathResolver
	// refusedImports maps a Volume Group on this node to the reason its import was
	// refused on the previous pass, so the reason is logged when it appears or
	// changes rather than once per scan interval for as long as it holds. The
	// alertable form of the same fact is the lvm_volume_group_import_refused_total
	// counter — this is only about the log.
	refusedImports map[string]string
}

type DiscovererConfig struct {
	NodeName                string
	VolumeGroupScanInterval time.Duration
	// CmdDeadlineDuration bounds every host command the discoverer runs, the same
	// way it already bounds the reconciler's and the scanner's. Discovery gained
	// host commands of its own with spec.fileDevices (losetup, stat -f), and an
	// unbounded one of those blocks the whole loop: discovery runs to completion
	// or not at all, so a single hung filesystem freezes status.nodes[] for every
	// LVMVolumeGroup on the node.
	//
	// A non-positive value disables the deadline (see utils.RunWithTimeout),
	// which is what the unit tests with mocked commands rely on.
	CmdDeadlineDuration time.Duration
	// FileDevicesDirectory is the base directory backing files are confined to.
	// The discoverer needs it to express a re-imported spec.fileDevices entry as a
	// path its own validation will accept; see buildSpecFileDevicesFromCandidate.
	// Empty means "no restriction", matching ReconcilerConfig.
	FileDevicesDirectory string
}

func NewDiscoverer(
	cl client.Client,
	log logger.Logger,
	metrics *monitoring.Metrics,
	sdsCache *cache.Cache,
	commands utils.Commands,
	cfg DiscovererConfig,
) *Discoverer {
	return &Discoverer{
		cl:       cl,
		log:      log,
		lvgCl:    repository.NewLVGClient(cl, log, metrics, cfg.NodeName, DiscovererName),
		bdCl:     repository.NewBDClient(cl, metrics),
		metrics:  metrics,
		sdsCache: sdsCache,
		cfg:      cfg,
		commands: commands,
		resolver: utils.HostNsenterCanonicalResolver,

		refusedImports: make(map[string]string),
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

	filteredLVGs := filterLVGsByNode(currentLVMVGs, d.cfg.NodeName)

	// An empty BlockDevice list used to mean "no storage on this node, nothing to
	// discover". It stopped meaning that with spec.fileDevices: a file-backed
	// LVMVolumeGroup lives on a loop device, and the BlockDevice discoverer
	// deliberately does not publish loops as BlockDevices. Returning here left
	// such a node with no discovery at all — no candidates, so no VGReady and no
	// AgentReady, and the resource sat Pending with vgSize=0 indefinitely.
	//
	// It looked intermittent only because it depended on whether some unrelated
	// disk happened to be attached at the time: with one present the discoverer
	// ran and file-backed VGs were picked up as a side effect.
	//
	// The cache is consulted as well, and not only the LVMVolumeGroup resources:
	// importing a Volume Group whose resource is gone is exactly the case where
	// there is no resource to look at. Checking only the resources would make a
	// node whose sole storage is a file-backed VG unable to ever re-adopt it.
	if len(blockDevices) == 0 && !anyLVGHasFileDevices(filteredLVGs) && !d.cacheHasManagedVG() {
		d.log.Info("[RunLVMVolumeGroupDiscoverController] no BlockDevices, no file-backed LVMVolumeGroups and no managed VGs were found")
		return false
	}

	// Store managed VG names in cache for metrics filtering
	d.sdsCache.StoreManagedVGs(maps.Keys(filteredLVGs))

	d.log.Debug("[RunLVMVolumeGroupDiscoverController] tries to get LVMVolumeGroup candidates")
	candidates, err := d.GetLVMVolumeGroupCandidates(ctx, blockDevices)
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

	shouldRequeue := false

	// When LVM reports more than one VG with the same name, it stops resolving
	// such VGs by name (`vgs <name>`/`lvs <name>` fail with
	// `Multiple VGs found with the same name: skipping`). The agent's name-keyed
	// caches (`cache.FindVG`/`FindLV`) and the discoverer's `filteredLVGs` map
	// would silently mix LV data from unrelated VGs in that case, producing
	// misleading conditions such as `ThinPool ... size X is less than status one Y`.
	// Refuse to act on those LVGs and surface a single, actionable error message
	// instead.
	allVGs, _ := d.sdsCache.GetVGs()
	duplicateVGs := findDuplicateVGNames(allVGs)
	if len(duplicateVGs) > 0 {
		for name, uuids := range duplicateVGs {
			d.log.Warning(fmt.Sprintf("[RunLVMVolumeGroupDiscoverController] VG name %q is used by multiple VGs on the node (UUIDs: %s); LVMVolumeGroup resources referencing it will be marked NotReady until the duplicate is resolved", name, strings.Join(uuids, ", ")))
		}

		for _, lvg := range filteredLVGs {
			uuids, dup := duplicateVGs[lvg.Spec.ActualVGNameOnTheNode]
			if !dup {
				continue
			}
			if updErr := d.lvgCl.UpdateLVGConditionIfNeeded(ctx, &lvg, metav1.ConditionFalse, internal.TypeVGReady, internal.ReasonScanFailed, duplicateVGMessage(lvg.Spec.ActualVGNameOnTheNode, uuids)); updErr != nil {
				d.log.Error(updErr, fmt.Sprintf("[RunLVMVolumeGroupDiscoverController] unable to add VGReady=False condition for duplicate VG name to the LVMVolumeGroup %s", lvg.Name))
				shouldRequeue = true
			}
		}

		for vgName := range duplicateVGs {
			delete(filteredLVGs, vgName)
		}
		candidates = filterCandidatesByDuplicateVGs(candidates, duplicateVGs)
	}

	candidates, err = d.ReconcileUnhealthyLVMVolumeGroups(ctx, candidates, filteredLVGs)
	if err != nil {
		d.log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupDiscoverController] an error has occurred while clearing the LVMVolumeGroups resources. Requeue the request in %s", d.cfg.VolumeGroupScanInterval.String()))
		return true
	}

	// Rebuilt from scratch every pass rather than added to, so a Volume Group that
	// stops being refused — the tag was fixed, the resource was deleted, the LUN
	// went away — drops out and the next refusal of it is reported afresh. Keyed by
	// the VG name on this node, so it is bounded by what the node can see.
	refusedThisPass := make(map[string]string, len(d.refusedImports))

	for _, candidate := range candidates {
		// A candidate whose file devices could not all be classified describes the
		// node incompletely, and every use below turns it into a claim: an update
		// would drop a provisioned entry out of status.nodes[].fileDevices (read
		// downstream as "never provisioned" — no drift reported, a requested growth
		// skipped, capacity double-counted in thin-pool validation, and the record
		// refuseUnlinkedBackingFile needs to avoid a second backing file gone), and
		// an import would rebuild spec.fileDevices missing an entry whose Physical
		// Volume is in the Volume Group. Leave the resource exactly as it is and come
		// back; the cause is a host command that did not answer, which the next cycle
		// retries for free. See buildFileDeviceFromLoopPV.
		if candidate.FileDeviceStateUnknown {
			d.log.Warning(fmt.Sprintf("[RunLVMVolumeGroupDiscoverController] the file devices of VG %s could not all be classified this cycle; leaving the LVMVolumeGroup as it is and retrying in %s",
				candidate.ActualVGNameOnTheNode, d.cfg.VolumeGroupScanInterval.String()))
			shouldRequeue = true
			continue
		}

		if lvg, exist := filteredLVGs[candidate.ActualVGNameOnTheNode]; exist {
			d.log.Debug(fmt.Sprintf("[RunLVMVolumeGroupDiscoverController] the LVMVolumeGroup %s is already exist. Tries to update it", lvg.Name))
			d.log.Trace(fmt.Sprintf("[RunLVMVolumeGroupDiscoverController] candidate: %+v", candidate))
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
			// A file-backed Volume Group cannot be imported under any name but the
			// one in its backing files. They are named after the owning
			// LVMVolumeGroup, so under a generated name the agent would not
			// recognise the file devices already in the VG, would provision a
			// second set from the reconstructed spec and vgextend them in,
			// silently doubling the VG and leaving the original PVs orphaned —
			// with no drift reported, because the entry names would match.
			//
			// Two things force a generated name: the tag holds something that is
			// not a usable resource name (GetLVMVolumeGroupCandidates already said
			// so), or the name it holds is taken by another LVMVolumeGroup.
			refuseFileBackedImport := func(why string) {
				d.log.Error(fmt.Errorf("unable to import a file-backed Volume Group"), fmt.Sprintf(
					"[RunLVMVolumeGroupDiscoverController] VG %s is backed by file devices whose names encode the owning LVMVolumeGroup, so it cannot be imported under a generated name: %s. "+
						"Retag the VG with a usable %s value, or free that name",
					candidate.ActualVGNameOnTheNode, why, internal.LVMVolumeGroupTag))
				shouldRequeue = true
			}

			if candidate.LVMVGNameGenerated && len(candidate.FileDeviceNodes) > 0 {
				refuseFileBackedImport(fmt.Sprintf("the %s tag does not hold a usable LVMVolumeGroup name", internal.LVMVolumeGroupTag))
				continue
			}

			// The candidate's name comes from the Volume Group's own tag whenever it
			// has one, so that an import restores the LVMVolumeGroup it belonged to.
			// If something else already holds that name — it was reused for another
			// Volume Group, or the same tag ended up on two of them — creating it
			// would fail with AlreadyExists on every pass.
			if taken, exists := currentLVMVGs[candidate.LVMVGName]; exists {
				if len(candidate.FileDeviceNodes) > 0 {
					refuseFileBackedImport(fmt.Sprintf("the name %s it is tagged with already belongs to the LVMVolumeGroup of VG %s",
						candidate.LVMVGName, taken.Spec.ActualVGNameOnTheNode))
					continue
				}

				// Refused, not renamed. Minting a generated name here and creating the
				// resource anyway is an unbounded loop: the name is random, so the next
				// cycle finds no resource for this Volume Group either and creates
				// another one. On a cluster where this fired it produced ninety
				// LMVolumeGroups for one Volume Group inside four seconds and about nine
				// hundred over a day, every one of them Pending and never reconcilable.
				//
				// The loop needs shared storage to get going, which is ordinary here: a
				// LUN-backed Volume Group is presented to several hosts, the
				// LVMVolumeGroup for it belongs to one of them, and the agents on the
				// others do not find it in their per-node map — filteredLVGs is keyed by
				// the VG name of the resources on THIS node — so they all try to import
				// what is already owned.
				//
				// A Volume Group whose tag names an existing resource has an owner, and
				// that is the answer regardless of which node is asking. The other way to
				// get here is one tag on two Volume Groups, which is a mistake to report
				// rather than paper over with a name nobody chose.
				//
				// The refusal is terminal and leaves nothing in the API to look at:
				// there is no resource for this Volume Group, and the LVMVolumeGroup
				// whose name the tag claims is healthy on its own node and must not be
				// marked otherwise. The counter is what an operator can alert on; the
				// log line carries the detail, and only when the detail changes —
				// repeating it every scan interval buries the first, useful copy.
				why := importRefusalReason(candidate, taken)
				d.metrics.LVMVolumeGroupImportRefusedTotal(candidate.ActualVGNameOnTheNode, candidate.LVMVGName).Inc()
				refusedThisPass[candidate.ActualVGNameOnTheNode] = why
				if d.refusedImports[candidate.ActualVGNameOnTheNode] == why {
					d.log.Debug(fmt.Sprintf("[RunLVMVolumeGroupDiscoverController] still not importing VG %s: %s",
						candidate.ActualVGNameOnTheNode, why))
				} else {
					d.log.Warning(fmt.Sprintf("[RunLVMVolumeGroupDiscoverController] not importing VG %s: %s",
						candidate.ActualVGNameOnTheNode, why))
				}
				continue
			}

			d.log.Debug(fmt.Sprintf("[RunLVMVolumeGroupDiscoverController] the LVMVolumeGroup %s is not yet created. Create it", candidate.LVMVGName))
			createdLvg, err := d.CreateLVMVolumeGroupByCandidate(ctx, candidate)
			if err != nil {
				d.log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupDiscoverController] unable to CreateLVMVolumeGroupByCandidate %s. Requeue the request in %s", candidate.LVMVGName, d.cfg.VolumeGroupScanInterval.String()))
				shouldRequeue = true
				continue
			}

			// The conditions go on the resource that was just created, not on
			// `lvg` — which in this branch is the zero value the map lookup
			// returned, an LVMVolumeGroup with no name that the apiserver rejects.
			// It went unnoticed while this branch was near-unreachable (a
			// block-backed VG waits for its BlockDevices instead); automatic
			// re-import of a file-backed VG makes it a normal path.
			err = d.lvgCl.UpdateLVGConditionIfNeeded(ctx, createdLvg, metav1.ConditionTrue, internal.TypeVGConfigurationApplied, internal.ReasonApplied, "all configuration has been applied")
			if err != nil {
				d.log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupDiscoverController] unable to add a condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, createdLvg.Name))
				shouldRequeue = true
				continue
			}

			err = d.lvgCl.UpdateLVGConditionIfNeeded(ctx, createdLvg, metav1.ConditionTrue, internal.TypeVGReady, internal.ReasonUpdated, "ready to create LV")
			if err != nil {
				d.log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupDiscoverController] unable to add a condition %s to the LVMVolumeGroup %s", internal.TypeVGReady, createdLvg.Name))
				shouldRequeue = true
				continue
			}

			d.log.Info(fmt.Sprintf(`[RunLVMVolumeGroupDiscoverController] created new APILVMVolumeGroup, name: "%s"`, createdLvg.Name))
		}
	}

	d.refusedImports = refusedThisPass

	if shouldRequeue {
		d.log.Warning(fmt.Sprintf("[RunLVMVolumeGroupDiscoverController] some problems have been occurred while iterating the lvmvolumegroup resources. Retry the reconcile in %s", d.cfg.VolumeGroupScanInterval.String()))
		return true
	}

	// Update LVMVolumeGroup status metrics
	if errs := d.metrics.UpdateLVGStatusMetrics(filteredLVGs); len(errs) > 0 {
		for _, err := range errs {
			d.log.Warning(fmt.Sprintf("[RunLVMVolumeGroupDiscoverController] metrics update error: %v", err))
		}
	}

	d.metrics.UpdateFileDeviceMetrics(filteredLVGs, d.collectFileDeviceUsage(ctx, filteredLVGs))

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
					} else {
						extentSize := extentSizeForThinPoolAlign(&lvg, nil)
						vgTpDiff := candidate.VGSize.Value() - statusTp.ActualSize.Value()
						isFullVG := vgTpDiff >= 0 && vgTpDiff <= extentSize.Value()
						if !isFullVG && candidateTp.ActualSize.Value()+extentSize.Value() < statusTp.ActualSize.Value() {
							d.log.Warning(fmt.Sprintf("[ReconcileUnhealthyLVMVolumeGroups] the LVMVolumeGroup %s ThinPool %s size %s is less than status one %s", lvg.Name, statusTp.Name, candidateTp.ActualSize.String(), statusTp.ActualSize.String()))
							messageBldr.WriteString(fmt.Sprintf("ThinPool %s on the node has size %s which is less than status one %s. ", statusTp.Name, candidateTp.ActualSize.String(), statusTp.ActualSize.String()))
						}
					}
				}
			}

			if messageBldr.Len() > 0 {
				err = d.lvgCl.UpdateLVGConditionIfNeeded(ctx, &lvg, metav1.ConditionFalse, internal.TypeVGReady, internal.ReasonScanFailed, messageBldr.String())
				if err != nil {
					d.log.Error(err, fmt.Sprintf("[ReconcileUnhealthyLVMVolumeGroups] unable to update the LVMVolumeGroup %s", lvg.Name))
					return nil, err
				}

				d.log.Warning(fmt.Sprintf("[ReconcileUnhealthyLVMVolumeGroups] the LVMVolumeGroup %s and its data object will be removed from the reconcile due to unhealthy states", lvg.Name))
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

func (d *Discoverer) GetLVMVolumeGroupCandidates(ctx context.Context, bds map[string]v1alpha1.BlockDevice) ([]internal.LVMVolumeGroupCandidate, error) {
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
		vgIssues = d.sortVGIssuesByVG(d.log, vgWithTag)
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
		pvIssues = d.sortPVIssuesByVG(d.log, pvs)
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
		lvIssues = d.sortThinPoolIssuesByVG(d.log, thinPools)
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

		lvgName, fromTag, nameUsable := lvgNameForCandidate(vg)
		if !nameUsable {
			d.log.Warning(fmt.Sprintf("[GetLVMVolumeGroupCandidates] VG %s is tagged %s=%q, which is not a usable LVMVolumeGroup name; generating one instead",
				vg.VGName, internal.LVMVolumeGroupTag, lvgName))
			lvgName, fromTag = generateLVMVGName(), false
		}

		candidate := internal.LVMVolumeGroupCandidate{
			LVMVGName:             lvgName,
			LVMVGNameGenerated:    !fromTag,
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
			ExtentSize:            *resource.NewQuantity(vg.VGExtentSize.Value(), resource.BinarySI),
		}
		var fileDevicesKnown bool
		candidate.Nodes, candidate.FileDeviceNodes, fileDevicesKnown = d.configureCandidateNodeDevices(ctx, sortedPVs, sortedBDs, vg, d.cfg.NodeName)
		candidate.FileDeviceStateUnknown = !fileDevicesKnown

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

	// Reconstruct spec.fileDevices from the backing files found on the node. The
	// basename encodes the owning LVMVolumeGroup and the entry name, and the
	// PV size gives the size, so a file-backed Volume Group can be re-adopted
	// after its resource is lost — the same automatic import that block-backed
	// VGs tagged storage.deckhouse.io/enabled=true already get.
	specFileDevices, err := d.buildSpecFileDevicesFromCandidate(ctx, candidate)
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
			FileDevices:           specFileDevices,
		},
		Status: v1alpha1.LVMVolumeGroupStatus{
			AllocatedSize: candidate.AllocatedSize,
			Nodes:         convertLVMVGNodes(candidate.Nodes, candidate.FileDeviceNodes),
			ThinPools:     thinPools,
			VGSize:        candidate.VGSize,
			VGUuid:        candidate.VGUUID,
			VGFree:        candidate.VGFree,
			ExtentSize:    candidate.ExtentSize,
		},
	}

	if len(candidate.BlockDevicesNames) == 0 {
		// A Volume Group built entirely on file devices has no BlockDevices by
		// design; waiting for them would postpone its import forever.
		if len(specFileDevices) == 0 {
			d.log.Warning(fmt.Sprintf("[CreateLVMVolumeGroupByCandidate] no BlockDevices found for VG %s, postponing LVMVolumeGroup creation until BlockDevices are discovered", candidate.ActualVGNameOnTheNode))
			return lvmVolumeGroup, nil
		}
		d.log.Info(fmt.Sprintf("[CreateLVMVolumeGroupByCandidate] VG %s is backed by %d file device(s) and no BlockDevices; importing it", candidate.ActualVGNameOnTheNode, len(specFileDevices)))
	} else {
		// A mixed Volume Group still waits for all of its block devices: importing
		// it while some are undiscovered would derive a blockDeviceSelector that
		// does not cover the whole VG.
		for _, node := range candidate.Nodes {
			for _, dev := range node {
				if len(dev.BlockDevice) == 0 {
					d.log.Warning("The attempt to create the LVG resource failed because it was not possible to find a BlockDevice for it.")
					return lvmVolumeGroup, nil
				}
			}
		}
	}

	start := time.Now()
	err = d.cl.Create(ctx, lvmVolumeGroup)
	d.metrics.APIMethodsDuration(DiscovererName, "create").Observe(d.metrics.GetEstimatedTimeInSeconds(start))
	d.metrics.APIMethodsExecutionCount(DiscovererName, "create").Inc()
	if err != nil {
		d.metrics.APIMethodsErrors(DiscovererName, "create").Inc()
		return nil, fmt.Errorf("unable to create LVMVolumeGroup, err: %w", err)
	}

	return lvmVolumeGroup, nil
}

func (d *Discoverer) UpdateLVMVolumeGroupByCandidate(
	ctx context.Context,
	lvg *v1alpha1.LVMVolumeGroup,
	candidate internal.LVMVolumeGroupCandidate,
) error {
	if len(candidate.BlockDevicesNames) > 0 && hasEmptyBlockDeviceSelector(lvg) {
		d.log.Warning(fmt.Sprintf("[UpdateLVMVolumeGroupByCandidate] the LVMVolumeGroup %s has an empty blockDeviceSelector, updating it with discovered BlockDevices", lvg.Name))
		lvg.Spec.BlockDeviceSelector = configureBlockDeviceSelector(candidate)
		if err := d.cl.Update(ctx, lvg); err != nil {
			return fmt.Errorf("[UpdateLVMVolumeGroupByCandidate] unable to fix empty blockDeviceSelector for LVMVolumeGroup %s: %w", lvg.Name, err)
		}
	}

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
	lvg.Status.Nodes = convertLVMVGNodes(candidate.Nodes, candidate.FileDeviceNodes)
	lvg.Status.ThinPools = thinPools
	lvg.Status.VGSize = candidate.VGSize
	lvg.Status.VGFree = candidate.VGFree
	lvg.Status.VGUuid = candidate.VGUUID
	lvg.Status.ExtentSize = candidate.ExtentSize

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

// configureCandidateNodeDevices maps the VG's Physical Volumes onto the block
// devices and file devices that back them.
//
// fileDevicesKnown is false when at least one loop PV could not be classified,
// i.e. when the returned file-device set is known to be incomplete. The caller
// must not write a status built from it — see buildFileDeviceFromLoopPV.
func (d *Discoverer) configureCandidateNodeDevices(ctx context.Context, pvs map[string][]internal.PVData, bds map[string][]v1alpha1.BlockDevice, vg internal.VGData, currentNode string) (devices map[string][]internal.LVMVGDevice, fileDevices map[string][]internal.LVMVGFileDevice, fileDevicesKnown bool) {
	filteredPV := pvs[vg.VGName+vg.VGUUID]
	filteredBds := bds[vg.VGName+vg.VGUUID]
	bdPathStatus := make(map[string]v1alpha1.BlockDevice, len(bds))
	result := make(map[string][]internal.LVMVGDevice, len(filteredPV))
	fileResult := make(map[string][]internal.LVMVGFileDevice)
	fileDevicesKnown = true

	for _, blockDevice := range filteredBds {
		bdPathStatus[blockDevice.Status.Path] = blockDevice
	}

	// The agent stamps the owning LVMVolumeGroup name onto its VGs via
	// the storage.deckhouse.io/lvmVolumeGroupName tag — use it as the
	// authoritative owner for any loop PV that lives in this VG. An
	// untagged VG (foreign LVM imported by hand and then tagged enabled)
	// must never be claimed; in that case we leave file-device discovery
	// off entirely for safety.
	_, ownerLVGName := utils.ReadValueFromTags(vg.VGTags, internal.LVMVolumeGroupTag)

	for _, pv := range filteredPV {
		if strings.HasPrefix(pv.PVName, utils.LoopDevicePathPrefix) {
			fileDev, known := d.buildFileDeviceFromLoopPV(ctx, pv, ownerLVGName, false)
			if !known {
				fileDevicesKnown = false
			}
			if fileDev != nil {
				fileResult[currentNode] = append(fileResult[currentNode], *fileDev)
			}
			continue
		}

		bd, exist := bdPathStatus[pv.PVName]
		if !exist {
			// When udev integration is unavailable LVM may report a managed
			// loop PV under a /dev/block/MAJ:MIN or /dev/disk/by-id alias
			// instead of /dev/loopN (the same aliasing FilterForeignPVs
			// resolves). Probe it as a loop device before giving up:
			// buildFileDeviceFromLoopPV refuses anything whose backing file
			// is not one of ours, and a non-loop device simply yields no
			// backing file. The probe is quiet (Debug) so a genuine
			// not-yet-registered BlockDevice does not spam warnings.
			if ownerLVGName != "" {
				// The probe never reports "unknown"; see buildFileDeviceFromLoopPV
				// for why an unreadable alias cannot be told apart from the ordinary
				// not-yet-registered BlockDevice below.
				if fileDev, _ := d.buildFileDeviceFromLoopPV(ctx, pv, ownerLVGName, true); fileDev != nil {
					fileResult[currentNode] = append(fileResult[currentNode], *fileDev)
					continue
				}
			}
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

	return result, fileResult, fileDevicesKnown
}

// buildFileDeviceFromLoopPV resolves the backing file for a loop PV and
// returns a managed file-device record iff the basename matches the
// owner pattern produced by utils.BuildFileDevicePath. Any other loop
// PV — a foreign losetup-attached qcow2, a snap squashfs loop that
// somehow ended up in a VG, a manual experiment — is skipped with a
// warning so the agent never writes its path into status (and therefore
// never tries to rm it later during cleanup).
//
// expectedLVGName is the owner name pulled from the VG tag; when empty
// the function refuses to claim any loop PV.
//
// probe is set when the PV is not a /dev/loopN node but an alias we are
// only speculatively checking (it was not found among BlockDevices): in
// that mode "not a loop" / "not ours" outcomes are expected and logged at
// Debug instead of Warning, so an ordinary not-yet-registered BlockDevice
// does not produce misleading warnings.
//
// known separates "this loop PV is not a managed file device" from "the node
// could not be asked". A nil device with known=false is NOT a verdict, and the
// caller must not publish a status derived from it: dropping a provisioned entry
// out of status.nodes[].fileDevices makes the reconciler read it as never
// provisioned, which stops drift being reported for it, skips a requested growth,
// has validateLVGForUpdateFunc count its capacity as new on top of a VG size that
// already contains it, and takes away the record refuseUnlinkedBackingFile reads
// to avoid creating a second backing file at the same path. Everywhere else in
// this feature an unreadable device leaves things alone (LoopVGUnknown,
// cleanupFileDevices, rollbackProvisionedFileDevices, newPVView); this is the same
// rule for discovery.
//
// A failed probe still returns known=true, and deliberately so: an unreadable
// alias PV cannot be told apart from the ordinary, benign, self-healing state of a
// BlockDevice that is not registered yet — which is the overwhelmingly common
// reason a PV reaches the probe at all — so treating it as unknown would stop
// status from being written during every routine VG extension. The guarantee that
// matters is on the canonical /dev/loopN path, which is how a managed file device
// is reported once udev has settled.
//
// NOTE: this is one of two places that canonicalize an alias-reported loop
// PV. Here we resolve backing-file → canonical /dev/loopN via losetup (we
// have the backing file and need the loop). The reconciler does the inverse
// in Reconciler.loopPVState (canonical loop is known, alias PV names are resolved
// via readlink). They use different methods because their inputs differ; keep
// their ownership/aliasing assumptions in sync.
func (d *Discoverer) buildFileDeviceFromLoopPV(ctx context.Context, pv internal.PVData, expectedLVGName string, probe bool) (dev *internal.LVMVGFileDevice, known bool) {
	if expectedLVGName == "" {
		// Debug, not Warning: this is a state that never clears itself — a VG
		// tagged managed but carrying no owner name stays that way — so a Warning
		// would repeat for every loop PV on every discovery cycle, forever, and
		// asks nothing of the operator. Skipping the PV is the correct and final
		// answer, not a problem report.
		d.log.Debug(fmt.Sprintf("[buildFileDeviceFromLoopPV] VG of loop PV %s carries no %s tag; skipping file-device discovery", pv.PVName, internal.LVMVolumeGroupTag))
		return nil, true
	}

	// Each command gets its own deadline from CMD_DEADLINE_DURATION, the same
	// budget every other host command in the agent runs under. A per-command
	// bound rather than one shared by the whole function: this runs once per loop
	// PV per cycle and may issue two commands, and a hung losetup would otherwise
	// stall the entire discovery loop.
	backing, err := utils.RunWithTimeout(ctx, d.cfg.CmdDeadlineDuration, func(ctx context.Context) (internal.LoopBackingFile, error) {
		cmd, backing, err := d.commands.GetLoopBackingFile(ctx, pv.PVName)
		d.log.Debug(cmd)
		return backing, err
	})
	if err != nil {
		msg := fmt.Sprintf("[buildFileDeviceFromLoopPV] unable to read backing file for %s: %v", pv.PVName, err)
		if probe {
			d.log.Debug(msg)
		} else {
			d.log.Warning(msg + "; ownership cannot be established this round, so the status of this Volume Group is left as it is")
		}
		// known = probe: a gap on the canonical path, an expected answer on the
		// speculative one.
		return nil, probe
	}
	if backing.Path == "" {
		// For a device lvm has just reported as a Physical Volume, "not attached"
		// cannot be true — the same reasoning ClassifyLoopVGs applies. Count it as
		// unreadable rather than as evidence that this is not a file device. On the
		// probe path an empty path is the expected answer for a device that is
		// simply not a loop.
		if !probe {
			d.log.Warning(fmt.Sprintf("[buildFileDeviceFromLoopPV] losetup reports no backing file for loop PV %s, which cannot be true of a device lvm just listed as a PV; leaving the status of this Volume Group as it is", pv.PVName))
		}
		return nil, probe
	}

	if !utils.IsManagedFileDevicePath(backing.Path, expectedLVGName) {
		msg := fmt.Sprintf("[buildFileDeviceFromLoopPV] backing file %q of loop PV %s does not match managed pattern for LVG %s; skipping", backing.Path, pv.PVName, expectedLVGName)
		if probe {
			d.log.Debug(msg)
		} else {
			d.log.Warning(msg)
		}
		// A read that succeeded and said "not ours" is a verdict, not a gap.
		return nil, true
	}

	// A backing file that has been unlinked while its loop is still attached is
	// still recorded, not hidden. The Physical Volume is live and the volumes on it
	// are working, so dropping the device from status would be a lie in the
	// direction that costs the most: the entry would look unprovisioned, drift would
	// stop being reported for it, and — because the record is what
	// Reconciler.refuseUnlinkedBackingFile reads to recognise the state — the
	// reconciler would create a second backing file at the same path, a second loop
	// and a second Physical Volume, doubling the Volume Group with half of it on an
	// inode nobody can open.
	//
	// It is also said out loud here, on every discovery cycle, because the state is
	// invisible otherwise: `kubectl get lvg` shows a healthy resource and the path
	// in status resolves to nothing.
	if backing.Deleted {
		d.log.Warning(fmt.Sprintf("[buildFileDeviceFromLoopPV] the backing file %q of loop PV %s (LVG %s) has been unlinked while the loop is still attached; the Physical Volume is live but the file is gone — restore it, or move the PV out with pvmove + vgreduce + pvremove",
			backing.Path, pv.PVName, expectedLVGName))
	}

	// Status must record the canonical /dev/loopN device, never an alias.
	// In the probe path pv.PVName is a /dev/disk/by-id or /dev/block alias;
	// recording it verbatim makes status.loopDevice flip-flop between the
	// alias and /dev/loopN across reconciles (whichever lvm happens to
	// report), churning the resource with no-op updates. Re-resolve the
	// canonical loop from the backing file; keep pv.PVName only if that fails.
	loopDevice := pv.PVName
	if !strings.HasPrefix(loopDevice, utils.LoopDevicePathPrefix) {
		canonical, ferr := utils.RunWithTimeout(ctx, d.cfg.CmdDeadlineDuration, func(ctx context.Context) (string, error) {
			findCmd, canonical, err := d.commands.FindLoopDeviceByFile(ctx, backing.Path)
			d.log.Debug(findCmd)
			return canonical, err
		})
		if ferr != nil {
			d.log.Debug(fmt.Sprintf("[buildFileDeviceFromLoopPV] unable to resolve canonical loop for %s: %v; keeping %s", backing.Path, ferr, pv.PVName))
		} else if canonical != "" {
			loopDevice = canonical
		}
	}

	return &internal.LVMVGFileDevice{
		FilePath: backing.Path,
		// Size is the PV size, not the raw backing-file size: it is what lvm
		// reports for this device and is always slightly smaller than the
		// requested spec.fileDevices[].size (LVM metadata overhead). This
		// mirrors how block devices expose status...devices[].pvSize, and keeps
		// the value self-consistent across reconciles (hasStatusNodesDiff
		// compares PV size against PV size, so it never churns). Callers that
		// need the requested size must read spec, not status.
		LoopDevice: loopDevice,
		Size:       *resource.NewQuantity(pv.PVSize.Value(), resource.BinarySI),
		PVUUID:     pv.PVUuid,
	}, true
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

func (d *Discoverer) sortThinPoolIssuesByVG(log logger.Logger, lvs []internal.LVData) map[string]map[string]string {
	var lvIssuesByVG = make(map[string]map[string]string, len(lvs))

	// One map per Volume Group, created once and added to. Re-creating it per
	// Logical Volume — which is what this did — keeps only whichever LV came last
	// in the listing, so a Volume Group with several unhealthy thin pools reports
	// one of them and the operator fixes that one, waits, and gets a different
	// message with no sign the list was ever longer.
	issuesOf := func(lv internal.LVData) map[string]string {
		key := lv.VGName + lv.VGUuid
		if issues, ok := lvIssuesByVG[key]; ok {
			return issues
		}
		issues := make(map[string]string, len(lvs))
		lvIssuesByVG[key] = issues
		return issues
	}

	for _, lv := range lvs {
		_, cmd, stdErr, err := d.commands.GetLV(lv.VGName, lv.LVName)
		log.Debug(fmt.Sprintf("[sortThinPoolIssuesByVG] runs cmd: %s", cmd))

		if err != nil {
			log.Error(err, fmt.Sprintf(`[sortThinPoolIssuesByVG] unable to run lvs command for lv, name: "%s"`, lv.LVName))
			issuesOf(lv)[lv.LVName] = err.Error()
		}

		if stdErr.Len() != 0 {
			log.Error(errors.New(stdErr.String()), fmt.Sprintf(`[sortThinPoolIssuesByVG] lvs command for lv "%s" has stderr: `, lv.LVName))
			// Logged whole above, attributed in part: only what lvm said about this
			// Logical Volume may become this Volume Group's health. See
			// utils.ObjectDiagnostics.
			if aboutTheLV := utils.ObjectDiagnostics(cmd, stdErr); aboutTheLV.Len() != 0 {
				// Appended to whatever the command's own error already said, rather
				// than replacing it: a failed lvs that also printed a diagnostic about
				// this Logical Volume is two facts about it, not one.
				if existing := issuesOf(lv)[lv.LVName]; existing != "" {
					issuesOf(lv)[lv.LVName] = existing + "\n" + aboutTheLV.String()
				} else {
					issuesOf(lv)[lv.LVName] = aboutTheLV.String()
				}
			}
			stdErr.Reset()
		}
	}

	return lvIssuesByVG
}

func (d *Discoverer) sortPVIssuesByVG(log logger.Logger, pvs []internal.PVData) map[string][]string {
	pvIssuesByVG := make(map[string][]string, len(pvs))

	for _, pv := range pvs {
		_, cmd, stdErr, err := d.commands.GetPV(pv.PVName)
		log.Debug(fmt.Sprintf("[sortPVIssuesByVG] runs cmd: %s", cmd))

		if err != nil {
			log.Error(err, fmt.Sprintf(`[sortPVIssuesByVG] unable to run pvs command for pv "%s"`, pv.PVName))
			pvIssuesByVG[pv.VGName+pv.VGUuid] = append(pvIssuesByVG[pv.VGName+pv.VGUuid], err.Error())
		}

		if stdErr.Len() != 0 {
			log.Error(errors.New(stdErr.String()), fmt.Sprintf(`[sortPVIssuesByVG] pvs command for pv "%s" has stderr: %s`, pv.PVName, stdErr.String()))
			if aboutThePV := utils.ObjectDiagnostics(cmd, stdErr); aboutThePV.Len() != 0 {
				pvIssuesByVG[pv.VGName+pv.VGUuid] = append(pvIssuesByVG[pv.VGName+pv.VGUuid], aboutThePV.String())
			}
			stdErr.Reset()
		}
	}

	return pvIssuesByVG
}

func (d *Discoverer) sortVGIssuesByVG(log logger.Logger, vgs []internal.VGData) map[string]string {
	vgIssues := make(map[string]string, len(vgs))
	for _, vg := range vgs {
		_, cmd, stdErr, err := d.commands.GetVG(vg.VGName)
		log.Debug(fmt.Sprintf("[sortVGIssuesByVG] runs cmd: %s", cmd))
		if err != nil {
			log.Error(err, fmt.Sprintf(`[sortVGIssuesByVG] unable to run vgs command for vg, name: "%s"`, vg.VGName))
			vgIssues[vg.VGName+vg.VGUUID] = err.Error()
		}

		if stdErr.Len() != 0 {
			log.Error(errors.New(stdErr.String()), fmt.Sprintf(`[sortVGIssuesByVG] vgs command for vg "%s" has stderr: `, vg.VGName))
			if aboutTheVG := utils.ObjectDiagnostics(cmd, stdErr); aboutTheVG.Len() != 0 {
				vgIssues[vg.VGName+vg.VGUUID] = aboutTheVG.String()
			}
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

		if lvErrs, exist := lvIssues[vg.VGName+vg.VGUUID][thinPool.LVName]; exist {
			tp.Ready = false
			tp.Message = lvErrs
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

// cacheHasManagedVG reports whether the node's LVM scan holds a Volume Group
// tagged as managed by this module. Such a VG is something to discover even when
// nothing else on the node suggests it: its LVMVolumeGroup resource may simply
// not exist yet, which is the state an import starts from.
func (d *Discoverer) cacheHasManagedVG() bool {
	vgs, _ := d.sdsCache.GetVGs()
	for _, vg := range vgs {
		if utils.HasManagedTag(vg.VGTags) {
			return true
		}
	}
	return false
}

// anyLVGHasFileDevices reports whether any of the LVMVolumeGroups declares
// spec.fileDevices, i.e. whether discovery has something to do on a node that
// carries no BlockDevices.
func anyLVGHasFileDevices(lvgs map[string]v1alpha1.LVMVolumeGroup) bool {
	for _, lvg := range lvgs {
		if len(lvg.Spec.FileDevices) > 0 {
			return true
		}
	}
	return false
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
	log.Trace(fmt.Sprintf(`Nodes, candidate: %+v, lvg: %+v`, convertLVMVGNodes(candidate.Nodes, candidate.FileDeviceNodes), lvg.Status.Nodes))

	return candidate.AllocatedSize.Value() != lvg.Status.AllocatedSize.Value() ||
		hasStatusPoolDiff(convertedStatusPools, lvg.Status.ThinPools) ||
		candidate.VGSize.Value() != lvg.Status.VGSize.Value() ||
		candidate.VGFree.Value() != lvg.Status.VGFree.Value() ||
		candidate.VGUUID != lvg.Status.VGUuid ||
		candidate.ExtentSize.Value() != lvg.Status.ExtentSize.Value() ||
		hasStatusNodesDiff(log, convertLVMVGNodes(candidate.Nodes, candidate.FileDeviceNodes), lvg.Status.Nodes)
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

		// File devices must be diffed too: a loop minor can change across
		// reboots (ReattachFileDevices re-attaches via `losetup --find`), so
		// without this the discoverer would never refresh a stale
		// loopDevice/pvUUID in status, and cleanup on delete would later act
		// on the stale value.
		//
		// Match by FilePath rather than by slice position: the candidate
		// slice is built in raw `lvm pvs` report order (sortPVsByVG does not
		// sort within a VG), and a loop PV can be reported under a /dev/loopN
		// or an alias on different scans, so the two slices may list the same
		// devices in a different order. A positional comparison would then
		// flag a spurious diff and rewrite status on every reconcile.
		if hasStatusFileDevicesDiff(first[i].FileDevices, second[i].FileDevices) {
			return true
		}
	}

	return false
}

func hasStatusFileDevicesDiff(first, second []v1alpha1.LVMVolumeGroupFileDevice) bool {
	if len(first) != len(second) {
		return true
	}

	byPath := make(map[string]v1alpha1.LVMVolumeGroupFileDevice, len(second))
	for _, fd := range second {
		byPath[fd.FilePath] = fd
	}

	for _, fd := range first {
		other, ok := byPath[fd.FilePath]
		if !ok ||
			fd.Name != other.Name ||
			fd.LoopDevice != other.LoopDevice ||
			fd.PVUuid != other.PVUuid ||
			fd.Size.Value() != other.Size.Value() {
			return true
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

func hasEmptyBlockDeviceSelector(lvg *v1alpha1.LVMVolumeGroup) bool {
	if lvg.Spec.BlockDeviceSelector == nil {
		return true
	}
	for _, me := range lvg.Spec.BlockDeviceSelector.MatchExpressions {
		if me.Key == internal.MetadataNameLabelKey && me.Operator == metav1.LabelSelectorOpIn {
			return len(me.Values) == 0
		}
	}
	return false
}

func configureBlockDeviceSelector(candidate internal.LVMVolumeGroupCandidate) *metav1.LabelSelector {
	// A Volume Group built entirely on file devices has no block devices to
	// select. Emitting an `In` requirement with an empty value list produces a
	// selector the apiserver rejects outright ("for 'in', 'notin' operators,
	// values set can't be empty"), so every attempt to list BlockDevices for
	// that LVMVolumeGroup fails and it never leaves NoBlockDevices. The CRD
	// makes blockDeviceSelector optional exactly for this case: leave it unset.
	if len(candidate.BlockDevicesNames) == 0 {
		return nil
	}

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

// collectFileDeviceUsage samples, per backing-file directory used on this node,
// how much the module has allocated there and how much the filesystem has left.
//
// The allocated figure is the requested size of the entries that are actually on
// the node — those present in status.nodes[].fileDevices — because the metric is
// documented as bytes this module has taken from the filesystem, and an entry that
// was never provisioned took none. Summing the whole spec instead over-reports by
// the size of every entry rejected at validation or refused for want of space,
// which is exactly the situation in which the figure is being read. The requested
// size rather than the PV size, because that is what fallocate reserved; the PV is
// a little smaller.
//
// The free figure needs the node, so it costs one `stat -f` per distinct directory
// per discovery cycle — and only on nodes that actually use file devices, since
// the loop below produces nothing otherwise.
func (d *Discoverer) collectFileDeviceUsage(ctx context.Context, lvgs map[string]v1alpha1.LVMVolumeGroup) []monitoring.FileDeviceDirectoryUsage {
	allocated := make(map[string]int64)
	for _, lvg := range lvgs {
		provisioned := make(map[string]struct{})
		for _, n := range lvg.Status.Nodes {
			if n.Name != d.cfg.NodeName {
				continue
			}
			for _, fd := range n.FileDevices {
				provisioned[fd.Name] = struct{}{}
			}
		}
		for _, fd := range lvg.Spec.FileDevices {
			if fd.Directory == "" {
				continue
			}
			if _, ok := provisioned[fd.Name]; !ok {
				continue
			}
			allocated[fd.Directory] += fd.Size.Value()
		}
	}
	if len(allocated) == 0 {
		return nil
	}

	usage := make([]monitoring.FileDeviceDirectoryUsage, 0, len(allocated))
	for dir, bytes := range allocated {
		u := monitoring.FileDeviceDirectoryUsage{Directory: dir, AllocatedBytes: bytes}

		// Bounded like every other host command: `stat -f` blocks
		// uninterruptibly on a filesystem that has gone away, and this call sits
		// at the end of the discovery loop. Without a deadline a single
		// unreachable backing-file directory — a network or hot-plugged mount
		// under the base path, which the docs actively suggest — stops
		// status.nodes[] from being written for every LVMVolumeGroup on the node,
		// and it reads as "the agent is alive but nothing moves".
		space, err := utils.RunWithTimeout(ctx, d.cfg.CmdDeadlineDuration, func(ctx context.Context) (internal.FilesystemSpace, error) {
			cmd, space, err := d.commands.GetFilesystemSpace(ctx, dir)
			d.log.Debug(cmd)
			return space, err
		})
		if err != nil {
			// Leave the previous sample standing rather than publishing a false
			// zero, which would look exactly like a full filesystem.
			d.log.Warning(fmt.Sprintf("[collectFileDeviceUsage] unable to read free space in %s: %v", dir, err))
		} else {
			u.FreeBytes, u.TotalBytes, u.Known = space.AvailableBytes, space.TotalBytes, true
		}

		usage = append(usage, u)
	}

	sort.Slice(usage, func(i, j int) bool { return usage[i].Directory < usage[j].Directory })
	return usage
}

// buildSpecFileDevicesFromCandidate rebuilds spec.fileDevices from the managed
// backing files discovered on the node, so that a Volume Group whose
// LVMVolumeGroup resource was lost can be imported back complete rather than as
// a block-device-only shell.
//
// Everything needed is on the node: the basename carries the entry name, the
// path carries the directory, and the PV size gives the size. The PV size is
// slightly below what the entry originally requested (LVM reserves PV metadata
// off the front of the device), so it is mapped back through the extent grid LVM
// itself works in — see reconstructFileDeviceSize.
//
// Rounding up to the next whole GiB instead would overshoot every entry whose
// size was not a multiple of a GiB — `1536Mi` is a valid size and would come
// back as `2Gi`. That is not a cosmetic difference: the reconciler compares the
// spec against the PV size and grows the file device to close the gap, so an
// import would silently enlarge the Volume Group it was supposed to restore, and
// fail outright when the filesystem has no room for the difference.
//
// The directory needs one translation before it can go into a spec. It comes from
// `losetup --output BACK-FILE`, which canonicalizes symlink components, whereas
// spec.fileDevices[].directory is validated against the configured base directory
// lexically (isWithinBaseDir does not resolve symlinks, deliberately). Make the
// base directory — or a component of it — a symlink, which is the natural way to
// point the default path at a data disk without touching the module config, and
// the canonical path is no longer under the configured base: the imported entry
// would be rejected by the agent's own validation on every reconcile, with no way
// out through the API, since `directory` is immutable once the entry exists.
//
// So the canonical path is mapped back onto the configured base by resolving that
// base once and substituting the prefix. Refusing the import outright is the
// fallback when the file genuinely does not live under the base: writing a
// directory that resolves somewhere else would have the reconciler provision a
// second backing file next to the one already in the Volume Group.
func (d *Discoverer) buildSpecFileDevicesFromCandidate(ctx context.Context, candidate internal.LVMVolumeGroupCandidate) ([]v1alpha1.LVMVolumeGroupFileDeviceSpec, error) {
	entries := make([]v1alpha1.LVMVolumeGroupFileDeviceSpec, 0)
	seen := make(map[string]struct{})

	// Resolved at most once per candidate, and only when a directory actually
	// needs translating — the common case (no symlink anywhere) costs nothing.
	resolvedBase := ""
	baseResolved := false

	for _, devices := range candidate.FileDeviceNodes {
		for _, dev := range devices {
			_, entryName, ok := utils.ParseFileDevicePath(dev.FilePath)
			if !ok {
				continue
			}
			if _, dup := seen[entryName]; dup {
				continue
			}
			seen[entryName] = struct{}{}

			dir := filepath.Dir(dev.FilePath)
			if d.cfg.FileDevicesDirectory != "" && !isWithinBaseDir(dir, d.cfg.FileDevicesDirectory) {
				if !baseResolved {
					var err error
					resolvedBase, err = utils.RunWithTimeout(ctx, d.cfg.CmdDeadlineDuration, func(ctx context.Context) (string, error) {
						return d.resolver(ctx, d.cfg.FileDevicesDirectory)
					})
					if err != nil {
						return nil, fmt.Errorf("unable to resolve the base directory %q to map the backing file %s of VG %s onto it: %w",
							d.cfg.FileDevicesDirectory, dev.FilePath, candidate.ActualVGNameOnTheNode, err)
					}
					baseResolved = true
				}

				relocated, ok := relocateUnderBase(dir, resolvedBase, d.cfg.FileDevicesDirectory)
				if !ok {
					return nil, fmt.Errorf("backing file %s of VG %s is outside the configured base directory %q (which resolves to %q), so its spec.fileDevices entry cannot be expressed; move the file under the base directory or point fileDevicesDirectory at it",
						dev.FilePath, candidate.ActualVGNameOnTheNode, d.cfg.FileDevicesDirectory, resolvedBase)
				}
				d.log.Info(fmt.Sprintf("[buildSpecFileDevicesFromCandidate] backing file %s is reported under the resolved base %q; recording its directory as %q so the imported entry passes validation",
					dev.FilePath, resolvedBase, relocated))
				dir = relocated
			}

			entries = append(entries, v1alpha1.LVMVolumeGroupFileDeviceSpec{
				Name:      entryName,
				Directory: dir,
				Size:      *reconstructFileDeviceSize(dev.Size, candidate.ExtentSize),
			})
		}
	}

	if len(entries) == 0 {
		return nil, nil
	}

	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	return entries, nil
}

// relocateUnderBase rewrites dir — a path as the node reports it, i.e. with every
// symlink component already resolved — to the equivalent path expressed under
// configuredBase, given that configuredBase resolves to resolvedBase.
//
// It reports false when dir is not under resolvedBase at all, which means the
// backing file is genuinely outside the configured subtree and no spec entry can
// describe it.
func relocateUnderBase(dir, resolvedBase, configuredBase string) (string, bool) {
	dir = filepath.Clean(dir)
	resolvedBase = filepath.Clean(resolvedBase)
	configuredBase = filepath.Clean(configuredBase)

	if dir == resolvedBase {
		return configuredBase, true
	}
	prefix := resolvedBase + string(filepath.Separator)
	if !strings.HasPrefix(dir, prefix) {
		return "", false
	}
	return filepath.Join(configuredBase, strings.TrimPrefix(dir, prefix)), true
}

// reconstructFileDeviceSize turns an observed PV size back into the size the
// spec.fileDevices entry that produced it most likely asked for.
//
// `pvs` reports the usable area, which is the backing file minus the metadata
// LVM keeps at the head of the device, floored to whole extents — so it lands
// somewhere in the extent below the requested size. Adding exactly one extent to
// the floored value recovers the request exactly whenever it was a multiple of
// the extent size — which the CRD guarantees, since `size` must be a whole number
// of the binary unit it names.
//
// The guarantee that matters is stated in terms of the PV, not the request: the
// result exceeds the PV size by at most fileDeviceGrowTolerance, which is exactly
// what growFileDevicesIfNeeded ignores. Flooring first is what delivers it.
//
// It can therefore land slightly ABOVE the original request — an entry grown to
// 1025Mi yields a 1024Mi PV and reconstructs as 1028Mi — and that is safe for the
// same reason: the reconciler closes a spec-to-PV gap by growing the backing file,
// and a gap within the tolerance is not a gap it acts on. What must never happen
// is a result that overshoots by MORE than that, because the import would then
// enlarge the Volume Group it was meant to restore and fail outright if the
// filesystem had no room for the difference.
//
// The lower clamp covers the smallest legal entry. A 1Gi file yields a PV of
// 1020Mi, and without the clamp a slightly smaller one could reconstruct to
// below the 1Gi minimum — an imported resource its own agent would then reject
// as invalid.
func reconstructFileDeviceSize(pvSize, extent resource.Quantity) *resource.Quantity {
	extentBytes := extent.Value()
	if extentBytes <= 0 {
		extentBytes = lvmDefaultPhysicalExtent.Value()
	}

	size := pvSize.Value()/extentBytes*extentBytes + extentBytes
	if size < minFileDeviceSize.Value() {
		size = minFileDeviceSize.Value()
	}

	// Round up to a whole mebibyte, because the value has to be writable into
	// spec.fileDevices[].size, whose CRD pattern is ^[0-9]+(Mi|Gi|Ti|Pi|Ei)$.
	// resource.Quantity in BinarySI prints a suffix only for a multiple of the
	// corresponding power of 1024 and falls back to a bare byte count otherwise —
	// and Ki is not in the pattern either. With LVM's default 4Mi extent every
	// result is already a multiple of a mebibyte, but `vgcreate -s` accepts 128Ki,
	// and a Volume Group an administrator handed over can have one: the import
	// would then fail apiserver validation on every discovery cycle with nothing
	// pointing at the extent size.
	//
	// This rounding is why the tolerance on the other side is
	// max(extent, 1Mi) and not one extent: for a sub-mebibyte extent it pushes
	// the result up to 1Mi above the PV, which is several extents. See
	// fileDeviceGrowTolerance.
	size = (size + fileDeviceSizeGranularity - 1) / fileDeviceSizeGranularity * fileDeviceSizeGranularity

	return resource.NewQuantity(size, resource.BinarySI)
}

func convertLVMVGNodes(nodes map[string][]internal.LVMVGDevice, fileNodes map[string][]internal.LVMVGFileDevice) []v1alpha1.LVMVolumeGroupNode {
	// Fast path for the overwhelmingly common case of no file devices
	// (every pure block-device LVG): a single pass over nodes, no extra
	// node-name set to allocate. This runs per LVG on every discover
	// reconcile via hasLVMVolumeGroupDiff, so the allocation matters.
	if len(fileNodes) == 0 {
		lvmvgNodes := make([]v1alpha1.LVMVolumeGroupNode, 0, len(nodes))
		for nodeName, nodeDevices := range nodes {
			lvmvgNodes = append(lvmvgNodes, v1alpha1.LVMVolumeGroupNode{
				Devices: convertLVMVGDevices(nodeDevices),
				Name:    nodeName,
			})
		}
		return lvmvgNodes
	}

	allNodeNames := make(map[string]struct{}, len(nodes)+len(fileNodes))
	for n := range nodes {
		allNodeNames[n] = struct{}{}
	}
	for n := range fileNodes {
		allNodeNames[n] = struct{}{}
	}

	lvmvgNodes := make([]v1alpha1.LVMVolumeGroupNode, 0, len(allNodeNames))
	for nodeName := range allNodeNames {
		node := v1alpha1.LVMVolumeGroupNode{
			Devices: convertLVMVGDevices(nodes[nodeName]),
			Name:    nodeName,
		}
		if fds := fileNodes[nodeName]; len(fds) > 0 {
			node.FileDevices = convertLVMVGFileDevices(fds)
		}
		lvmvgNodes = append(lvmvgNodes, node)
	}

	return lvmvgNodes
}

func convertLVMVGFileDevices(devices []internal.LVMVGFileDevice) []v1alpha1.LVMVolumeGroupFileDevice {
	result := make([]v1alpha1.LVMVolumeGroupFileDevice, 0, len(devices))
	for _, dev := range devices {
		// The entry name is read back out of the backing file's own name rather
		// than looked up in spec.fileDevices. That is what the invertible naming
		// buys: a device whose spec entry has just been removed keeps reporting
		// which entry created it, so the removal shows up as drift instead of
		// erasing the evidence for itself.
		_, entryName, _ := utils.ParseFileDevicePath(dev.FilePath)
		result = append(result, v1alpha1.LVMVolumeGroupFileDevice{
			Name:       entryName,
			FilePath:   dev.FilePath,
			LoopDevice: dev.LoopDevice,
			Size:       dev.Size,
			PVUuid:     dev.PVUUID,
		})
	}
	return result
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

// importRefusalReason explains why a Volume Group whose owner tag names an existing
// LVMVolumeGroup must not be imported under a new name.
//
// Separated from the discovery loop so the wording — the only thing an operator
// gets — is testable, and because the two cases it distinguishes are diagnosed
// completely differently.
func importRefusalReason(candidate internal.LVMVolumeGroupCandidate, taken v1alpha1.LVMVolumeGroup) string {
	why := fmt.Sprintf("its %s tag names the LVMVolumeGroup %s, which already exists", internal.LVMVolumeGroupTag, candidate.LVMVGName)
	if taken.Spec.ActualVGNameOnTheNode == candidate.ActualVGNameOnTheNode {
		return why + fmt.Sprintf(" for the same VG on node %s — a Volume Group on shared storage must not be imported twice."+
			" Nothing is wrong if that node is the one serving it; otherwise move the LVMVolumeGroup to this node",
			taken.Spec.Local.NodeName)
	}
	// The remedy belongs in the message for the same reason it does in
	// refuseFileBackedImport: this is a terminal refusal, and the operator's only
	// other source is a metric that says a count and not what to do about it.
	return why + fmt.Sprintf(" for VG %s — the tag is on two Volume Groups."+
		" Retag one of them with vgchange --deltag/--addtag, or delete the LVMVolumeGroup that no longer describes its Volume Group",
		taken.Spec.ActualVGNameOnTheNode)
}

func generateLVMVGName() string {
	return "vg-" + string(uuid.NewUUID())
}

// lvgNameForCandidate picks the name the imported LVMVolumeGroup will get.
//
// A Volume Group this module created carries the name of its LVMVolumeGroup in
// the storage.deckhouse.io/lvmVolumeGroupName tag, so re-importing one after its
// resource was lost should restore that name rather than mint a new one. For
// file-backed Volume Groups it is not merely nicer: the backing files are named
// after the owning LVMVolumeGroup, so a freshly generated name would not match
// them, the agent would fail to recognise its own file devices, and the import
// would produce a resource that cannot describe what is on the node.
//
// A Volume Group tagged only as managed — one an administrator created by hand
// and handed over with storage.deckhouse.io/enabled=true — carries no name, and
// still gets a generated one.
//
// The tag value is validated rather than trusted. The agent only ever writes a
// name that was already a valid resource name into it, but an administrator
// handing a Volume Group over can write the tag by hand, and LVM's own tag
// charset is far wider than a DNS-1123 subdomain (uppercase, '/', '=', ':', up
// to 1024 characters). An unusable value would make Create fail with Invalid on
// every discovery cycle, with nothing in the log pointing at the tag. ok is
// false in that case so the caller can say so once and decide what to do —
// for a file-backed VG the name is not substitutable, so it must not silently
// fall back to a generated one.
func lvgNameForCandidate(vg internal.VGData) (name string, fromTag, ok bool) {
	_, tagged := utils.ReadValueFromTags(vg.VGTags, internal.LVMVolumeGroupTag)
	if tagged == "" {
		return generateLVMVGName(), false, true
	}
	if errs := validation.IsDNS1123Subdomain(tagged); len(errs) > 0 {
		return tagged, true, false
	}
	// The agent copies the resource name into the kubernetes.io/metadata.name
	// label, and a label value is capped at 63 characters. A longer name leaves
	// the LVMVolumeGroup unable to ever be labelled, so it is no more usable than
	// a malformed one. The bound is the label-value limit specifically, not the
	// DNS-1035 one that happens to share its number: the two are independent and
	// only one of them is what actually rejects the write.
	if len(tagged) > validation.LabelValueMaxLength {
		return tagged, true, false
	}
	return tagged, true, true
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
