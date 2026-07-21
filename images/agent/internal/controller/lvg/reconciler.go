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
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

const ReconcilerName = "lvm-volume-group-watcher-controller"

// udevadmTriggerTimeout caps a single best-effort `udevadm trigger
// --action=change <paths>` invocation that the reconciler issues after
// pvcreate/vgcreate. The trigger only enqueues uevents in the kernel and
// normally returns within ~100ms; 10s leaves a wide safety margin for
// nodes under heavy stress without blocking a graceful shutdown for long.
const udevadmTriggerTimeout = 10 * time.Second

// udevadmTriggerCmdLabel is the metric label used for UdevadmTrigger in
// UtilsCommandsDuration / UtilsCommandsExecutionCount / UtilsCommandsErrorsCount.
// Keep it in sync with how operators query Prometheus.
const udevadmTriggerCmdLabel = "udevadm-trigger"

// lvmDefaultPhysicalExtent is LVM's default PE when vgcreate is run without --physicalextentsize.
var lvmDefaultPhysicalExtent = resource.MustParse("4Mi")

// extentSizeForThinPoolAlign returns a positive extent size for AlignSizeToExtent.
// Status.ExtentSize stays zero until the discoverer runs; it is also zero before the VG exists.
func extentSizeForThinPoolAlign(lvg *v1alpha1.LVMVolumeGroup, vg *internal.VGData) resource.Quantity {
	if lvg != nil && lvg.Status.ExtentSize.Value() > 0 {
		return lvg.Status.ExtentSize
	}
	if vg != nil && vg.VGExtentSize.Value() > 0 {
		return vg.VGExtentSize
	}
	return lvmDefaultPhysicalExtent
}

// alignThinPoolSizeForValidation aligns a thin-pool's requested size to the
// extent boundary for the create-time capacity check.
//
// Absolute sizes are rounded UP — that is the real number of extents LVM will
// consume, so an oversized absolute pool must still be rejected.
//
// Percentage sizes (e.g. "100%") are rounded DOWN instead. During create
// validation the VG size is the raw block-device/file sum, which is not
// extent-aligned, so a "100%" request rounded up lands one extent past the VG
// and would wrongly fail the capacity check — even though the pool is created
// with %FREE and fits. A percentage of the VG can never legitimately exceed
// it, so flooring is always safe and also makes split percentages (e.g.
// "50%"+"50%") sum to at most the VG size.
func alignThinPoolSizeForValidation(specSize string, requested, extentSize resource.Quantity) (resource.Quantity, error) {
	if utils.IsPercentSize(specSize) {
		extentBytes := extentSize.Value()
		if extentBytes <= 0 {
			return resource.Quantity{}, fmt.Errorf("extent size must be positive, got %d", extentBytes)
		}
		floored := (requested.Value() / extentBytes) * extentBytes
		return *resource.NewQuantity(floored, resource.BinarySI), nil
	}
	return utils.AlignSizeToExtent(requested, extentSize)
}

type Reconciler struct {
	cl       client.Client
	log      logger.Logger
	lvgCl    *repository.LVGClient
	bdCl     *repository.BDClient
	metrics  *monitoring.Metrics
	sdsCache *cache.Cache
	cfg      ReconcilerConfig
	commands utils.Commands
	// resolver maps a /dev/* PV name to its canonical block device. It is
	// only used to recognise a managed loop PV that lvm.static reported
	// under a /dev/disk/by-id or /dev/block/MAJ:MIN alias. Defaults to
	// utils.HostNsenterCanonicalResolver; overridable in tests.
	resolver utils.CanonicalPathResolver

	// aliasResolveFailures counts, per LVG name, how many consecutive
	// extendFileDevicesIfNeeded rounds made no progress purely because
	// alias-form PV names could not be resolved. It escalates the condition
	// from a generic "Updating" retry to ReasonAliasResolutionFailed once the
	// failures look persistent, so a stuck resolver is alertable instead of
	// looking like an ordinary in-flight update. Reset on any round that makes
	// progress or genuinely has nothing to do. The reconciler runs with
	// MaxConcurrentReconciles==1, but the mutex keeps the map safe if that ever
	// changes.
	aliasResolveFailuresMu sync.Mutex
	aliasResolveFailures   map[string]int
}

// aliasResolveFailureEscalationThreshold is the number of consecutive
// resolver-only failed rounds after which extendFileDevicesIfNeeded switches
// the VGConfigurationApplied reason to ReasonAliasResolutionFailed.
const aliasResolveFailureEscalationThreshold = 3

type ReconcilerConfig struct {
	NodeName                string
	BlockDeviceScanInterval time.Duration
	VolumeGroupScanInterval time.Duration
	CmdDeadlineDuration     time.Duration
	// FileDevicesDirectory is the base directory backing files are confined
	// to; spec.fileDevices[].directory must be this path or a subdirectory of
	// it. Empty means "no restriction" (used by unit tests that do not care).
	FileDevicesDirectory string
}

func NewReconciler(
	cl client.Client,
	log logger.Logger,
	metrics *monitoring.Metrics,
	sdsCache *cache.Cache,
	commands utils.Commands,
	cfg ReconcilerConfig,
) *Reconciler {
	return &Reconciler{
		cl:  cl,
		log: log,
		lvgCl: repository.NewLVGClient(
			cl,
			log,
			metrics,
			cfg.NodeName,
			ReconcilerName,
		),
		bdCl:                 repository.NewBDClient(cl, metrics),
		metrics:              metrics,
		sdsCache:             sdsCache,
		cfg:                  cfg,
		commands:             commands,
		resolver:             utils.HostNsenterCanonicalResolver,
		aliasResolveFailures: make(map[string]int),
	}
}

func (r *Reconciler) Name() string {
	return ReconcilerName
}

func (r *Reconciler) MaxConcurrentReconciles() int {
	return 1
}

// ShouldReconcileUpdate implements controller.Reconciler.
func (r *Reconciler) ShouldReconcileUpdate(objectOld *v1alpha1.LVMVolumeGroup, objectNew *v1alpha1.LVMVolumeGroup) bool {
	return r.shouldLVGWatcherReconcileUpdateEvent(objectOld, objectNew)
}

// ShouldReconcileCreate implements controller.Reconciler.
func (r *Reconciler) ShouldReconcileCreate(obj *v1alpha1.LVMVolumeGroup) bool {
	return checkIfLVGBelongsToNode(obj, r.cfg.NodeName)
}

// Reconcile implements controller.Reconciler.
func (r *Reconciler) Reconcile(ctx context.Context, request controller.ReconcileRequest[*v1alpha1.LVMVolumeGroup]) (controller.Result, error) {
	r.log.Info(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] Reconciler starts to reconcile the request %s", request.Object.Name))

	lvg := request.Object

	belongs := checkIfLVGBelongsToNode(lvg, r.cfg.NodeName)
	if !belongs {
		r.log.Info(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] the LVMVolumeGroup %s does not belong to the node %s", lvg.Name, r.cfg.NodeName))
		return controller.Result{}, nil
	}
	r.log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] the LVMVolumeGroup %s belongs to the node %s. Starts to reconcile", lvg.Name, r.cfg.NodeName))

	r.log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] tries to add the finalizer %s to the LVMVolumeGroup %s", internal.SdsNodeConfiguratorFinalizer, lvg.Name))
	added, err := r.addLVGFinalizerIfNotExist(ctx, lvg)
	if err != nil {
		r.log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to add the finalizer %s to the LVMVolumeGroup %s", internal.SdsNodeConfiguratorFinalizer, lvg.Name))
		return controller.Result{}, err
	}

	if added {
		r.log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] successfully added a finalizer %s to the LVMVolumeGroup %s", internal.SdsNodeConfiguratorFinalizer, lvg.Name))
	} else {
		r.log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] no need to add a finalizer %s to the LVMVolumeGroup %s", internal.SdsNodeConfiguratorFinalizer, lvg.Name))
	}

	// this case handles the situation when a user decides to remove LVMVolumeGroup resource without created VG
	deleted, err := r.deleteLVGIfNeeded(ctx, lvg)
	if err != nil {
		return controller.Result{}, err
	}

	if deleted {
		r.log.Info(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] the LVMVolumeGroup %s was deleted, stop the reconciliation", lvg.Name))
		return controller.Result{}, nil
	}

	if _, exist := lvg.Labels[internal.LVGUpdateTriggerLabel]; exist {
		delete(lvg.Labels, internal.LVGUpdateTriggerLabel)
		err = r.cl.Update(ctx, lvg)
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to update the LVMVolumeGroup %s", lvg.Name))
			return controller.Result{}, err
		}
		r.log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] successfully removed the label %s from the LVMVolumeGroup %s", internal.LVGUpdateTriggerLabel, lvg.Name))
	}

	// blockDeviceSelector is optional: a file-only LVMVolumeGroup (only
	// spec.fileDevices) carries no selector. Listing block devices with a
	// nil selector would match EVERY BlockDevice on the node (the repository
	// maps an empty selector to "select all"), and validateSpecBlockDevices
	// dereferences the nil selector. So skip block-device discovery and
	// validation entirely when there is no selector, and reconcile only the
	// file devices.
	blockDevices := make(map[string]v1alpha1.BlockDevice)
	if lvg.Spec.BlockDeviceSelector != nil {
		r.log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] tries to get block device resources for the LVMVolumeGroup %s by the selector %v", lvg.Name, lvg.Spec.BlockDeviceSelector))
		blockDevices, err = r.bdCl.GetAPIBlockDevices(ctx, ReconcilerName, lvg.Spec.BlockDeviceSelector)
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to get BlockDevices. Retry in %s", r.cfg.BlockDeviceScanInterval.String()))
			err = r.lvgCl.UpdateLVGConditionIfNeeded(
				ctx,
				lvg,
				v1.ConditionFalse,
				internal.TypeVGConfigurationApplied,
				"NoBlockDevices",
				fmt.Sprintf("unable to get block devices resources, err: %s", err.Error()),
			)
			if err != nil {
				r.log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to add a condition %s to the LVMVolumeGroup %s. Retry in %s", internal.TypeVGConfigurationApplied, lvg.Name, r.cfg.BlockDeviceScanInterval.String()))
			}

			return controller.Result{RequeueAfter: r.cfg.BlockDeviceScanInterval}, nil
		}
		r.log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] successfully got block device resources for the LVMVolumeGroup %s by the selector %v", lvg.Name, lvg.Spec.BlockDeviceSelector))

		blockDevices = filterBlockDevicesByNodeName(blockDevices, lvg.Spec.Local.NodeName)

		valid, reason := validateSpecBlockDevices(lvg, blockDevices)
		if !valid {
			r.log.Warning(fmt.Sprintf("[RunLVMVolumeGroupController] validation failed for the LVMVolumeGroup %s, reason: %s", lvg.Name, reason))
			err = r.lvgCl.UpdateLVGConditionIfNeeded(
				ctx,
				lvg,
				v1.ConditionFalse,
				internal.TypeVGConfigurationApplied,
				internal.ReasonValidationFailed,
				reason,
			)
			if err != nil {
				r.log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to add a condition %s to the LVMVolumeGroup %s. Retry in %s", internal.TypeVGConfigurationApplied, lvg.Name, r.cfg.VolumeGroupScanInterval.String()))
				return controller.Result{}, err
			}

			return controller.Result{}, nil
		}
		r.log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] successfully validated BlockDevices of the LVMVolumeGroup %s", lvg.Name))
	} else {
		r.log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] the LVMVolumeGroup %s has no blockDeviceSelector (file-only); skipping block device discovery and validation", lvg.Name))
	}

	r.log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] tries to add label %s to the LVMVolumeGroup %s", internal.LVGMetadataNameLabelKey, r.cfg.NodeName))
	added, err = r.addLVGLabelIfNeeded(ctx, lvg, internal.LVGMetadataNameLabelKey, lvg.Name)
	if err != nil {
		r.log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to add label %s to the LVMVolumeGroup %s", internal.LVGMetadataNameLabelKey, lvg.Name))
		return controller.Result{}, err
	}

	if added {
		r.log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] successfully added label %s to the LVMVolumeGroup %s", internal.LVGMetadataNameLabelKey, lvg.Name))
	} else {
		r.log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] no need to add label %s to the LVMVolumeGroup %s", internal.LVGMetadataNameLabelKey, lvg.Name))
	}

	// We do this after BlockDevices validation and node belonging check to prevent multiple updates by all agents pods
	bds, _ := r.sdsCache.GetDevices()
	if len(bds) == 0 {
		r.log.Warning(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] no block devices in the cache, add the LVMVolumeGroup %s to requeue", lvg.Name))
		err = r.lvgCl.UpdateLVGConditionIfNeeded(
			ctx,
			lvg,
			v1.ConditionFalse,
			internal.TypeVGConfigurationApplied,
			"CacheEmpty",
			"unable to apply configuration due to the cache's state",
		)
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to add a condition %s to the LVMVolumeGroup %s. Retry in %s", internal.TypeVGConfigurationApplied, lvg.Name, r.cfg.VolumeGroupScanInterval.String()))
		}

		return controller.Result{
			RequeueAfter: r.cfg.VolumeGroupScanInterval,
		}, nil
	}

	r.log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] tries to sync status and spec thin-pool AllocationLimit fields for the LVMVolumeGroup %s", lvg.Name))
	err = r.syncThinPoolsAllocationLimit(ctx, lvg)
	if err != nil {
		r.log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to sync status and spec thin-pool AllocationLimit fields for the LVMVolumeGroup %s", lvg.Name))
		return controller.Result{}, err
	}

	shouldRequeue, err := r.runEventReconcile(ctx, lvg, blockDevices)
	if err != nil {
		r.log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to reconcile the LVMVolumeGroup %s", lvg.Name))
	}

	if shouldRequeue {
		r.log.Warning(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] the LVMVolumeGroup %s event will be requeued in %s", lvg.Name, r.cfg.VolumeGroupScanInterval.String()))
		return controller.Result{
			RequeueAfter: r.cfg.VolumeGroupScanInterval,
		}, nil
	}
	r.log.Info(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] Reconciler successfully reconciled the LVMVolumeGroup %s", lvg.Name))

	return controller.Result{}, nil
}

func (r *Reconciler) runEventReconcile(
	ctx context.Context,
	lvg *v1alpha1.LVMVolumeGroup,
	blockDevices map[string]v1alpha1.BlockDevice,
) (bool, error) {
	recType := r.identifyLVGReconcileFunc(lvg)

	switch recType {
	case internal.CreateReconcile:
		r.log.Info(fmt.Sprintf("[runEventReconcile] CreateReconcile starts the reconciliation for the LVMVolumeGroup %s", lvg.Name))
		return r.reconcileLVGCreateFunc(ctx, lvg, blockDevices)
	case internal.UpdateReconcile:
		r.log.Info(fmt.Sprintf("[runEventReconcile] UpdateReconcile starts the reconciliation for the LVMVolumeGroup %s", lvg.Name))
		return r.reconcileLVGUpdateFunc(ctx, lvg, blockDevices)
	case internal.DeleteReconcile:
		r.log.Info(fmt.Sprintf("[runEventReconcile] DeleteReconcile starts the reconciliation for the LVMVolumeGroup %s", lvg.Name))
		return r.reconcileLVGDeleteFunc(ctx, lvg)
	default:
		r.log.Info(fmt.Sprintf("[runEventReconcile] no need to reconcile the LVMVolumeGroup %s", lvg.Name))
	}
	return false, nil
}

func (r *Reconciler) reconcileLVGDeleteFunc(ctx context.Context, lvg *v1alpha1.LVMVolumeGroup) (bool, error) {
	r.log.Debug(fmt.Sprintf("[reconcileLVGDeleteFunc] starts to reconcile the LVMVolumeGroup %s", lvg.Name))
	r.log.Debug(fmt.Sprintf("[reconcileLVGDeleteFunc] tries to add the condition %s status false to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))

	// this check prevents the LVMVolumeGroup resource's infinity updating after a retry
	for _, c := range lvg.Status.Conditions {
		if c.Type == internal.TypeVGConfigurationApplied && c.Reason != internal.ReasonTerminating {
			err := r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonTerminating, "trying to delete VG")
			if err != nil {
				r.log.Error(err, fmt.Sprintf("[reconcileLVGDeleteFunc] unable to add the condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
				return true, err
			}
			break
		}
	}

	_, exist := lvg.Annotations[internal.DeletionProtectionAnnotation]
	if exist {
		r.log.Debug(fmt.Sprintf("[reconcileLVGDeleteFunc] the LVMVolumeGroup %s has a deletion timestamp but also has a deletion protection annotation %s. Remove it to proceed the delete operation", lvg.Name, internal.DeletionProtectionAnnotation))
		err := r.lvgCl.UpdateLVGConditionIfNeeded(
			ctx,
			lvg,
			v1.ConditionFalse,
			internal.TypeVGConfigurationApplied,
			internal.ReasonTerminating,
			fmt.Sprintf("to delete the LVG remove the annotation %s", internal.DeletionProtectionAnnotation),
		)
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[reconcileLVGDeleteFunc] unable to add the condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
			return true, err
		}

		return false, nil
	}

	r.log.Debug(fmt.Sprintf("[reconcileLVGDeleteFunc] check if VG %s of the LVMVolumeGroup %s uses LVs", lvg.Spec.ActualVGNameOnTheNode, lvg.Name))
	usedLVs := r.getLVForVG(lvg.Spec.ActualVGNameOnTheNode)
	if len(usedLVs) > 0 {
		err := fmt.Errorf("VG %s uses LVs: %v. Delete used LVs first", lvg.Spec.ActualVGNameOnTheNode, usedLVs)
		r.log.Error(err, fmt.Sprintf("[reconcileLVGDeleteFunc] unable to reconcile LVG %s", lvg.Name))
		r.log.Debug(fmt.Sprintf("[reconcileLVGDeleteFunc] tries to add the condition %s status False to the LVMVolumeGroup %s due to LV does exist", internal.TypeVGConfigurationApplied, lvg.Name))
		err = r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonTerminating, err.Error())
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[reconcileLVGDeleteFunc] unable to add the condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
			return true, err
		}

		return true, nil
	}

	r.log.Debug(fmt.Sprintf("[reconcileLVGDeleteFunc] VG %s of the LVMVolumeGroup %s does not use any LV. Start to delete the VG", lvg.Spec.ActualVGNameOnTheNode, lvg.Name))
	err := r.deleteVGIfExist(lvg.Spec.ActualVGNameOnTheNode)
	if err != nil {
		r.log.Error(err, fmt.Sprintf("[reconcileLVGDeleteFunc] unable to delete VG %s", lvg.Spec.ActualVGNameOnTheNode))
		err = r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonTerminating, err.Error())
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[reconcileLVGDeleteFunc] unable to add the condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
			return true, err
		}

		return true, err
	}

	if err := r.cleanupFileDevices(ctx, lvg); err != nil {
		r.log.Error(err, fmt.Sprintf("[reconcileLVGDeleteFunc] unable to clean up file devices for the LVMVolumeGroup %s", lvg.Name))
		condErr := r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonTerminating, err.Error())
		if condErr != nil {
			r.log.Error(condErr, fmt.Sprintf("[reconcileLVGDeleteFunc] unable to add the condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
		}
		return true, err
	}

	removed, err := r.removeLVGFinalizerIfExist(ctx, lvg)
	if err != nil {
		r.log.Error(err, fmt.Sprintf("[reconcileLVGDeleteFunc] unable to remove a finalizer %s from the LVMVolumeGroup %s", internal.SdsNodeConfiguratorFinalizer, lvg.Name))
		err = r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonTerminating, err.Error())
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[reconcileLVGDeleteFunc] unable to add the condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
		}
		return true, err
	}

	if removed {
		r.log.Debug(fmt.Sprintf("[reconcileLVGDeleteFunc] successfully removed a finalizer %s from the LVMVolumeGroup %s", internal.SdsNodeConfiguratorFinalizer, lvg.Name))
	} else {
		r.log.Debug(fmt.Sprintf("[reconcileLVGDeleteFunc] no need to remove a finalizer %s from the LVMVolumeGroup %s", internal.SdsNodeConfiguratorFinalizer, lvg.Name))
	}

	err = r.lvgCl.DeleteLVMVolumeGroup(ctx, lvg)
	if err != nil {
		r.log.Error(err, fmt.Sprintf("[reconcileLVGDeleteFunc] unable to delete the LVMVolumeGroup %s", lvg.Name))
		return true, err
	}

	r.resetAliasResolveFailure(lvg.Name)
	r.log.Info(fmt.Sprintf("[reconcileLVGDeleteFunc] successfully reconciled VG %s of the LVMVolumeGroup %s", lvg.Spec.ActualVGNameOnTheNode, lvg.Name))
	return false, nil
}

func (r *Reconciler) reconcileLVGUpdateFunc(
	ctx context.Context,
	lvg *v1alpha1.LVMVolumeGroup,
	blockDevices map[string]v1alpha1.BlockDevice,
) (bool, error) {
	r.log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] starts to reconcile the LVMVolumeGroup %s", lvg.Name))

	r.log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] tries to validate the LVMVolumeGroup %s", lvg.Name))
	pvs, _ := r.sdsCache.GetPVs()
	valid, reason := r.validateLVGForUpdateFunc(ctx, lvg, blockDevices)
	if !valid {
		r.log.Warning(fmt.Sprintf("[reconcileLVGUpdateFunc] the LVMVolumeGroup %s is not valid", lvg.Name))
		err := r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonValidationFailed, reason)
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to add a condition %s reason %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, internal.ReasonValidationFailed, lvg.Name))
		}

		return true, err
	}
	r.log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] successfully validated the LVMVolumeGroup %s", lvg.Name))

	r.log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] tries to get VG %s for the LVMVolumeGroup %s", lvg.Spec.ActualVGNameOnTheNode, lvg.Name))
	found, vg := r.tryGetVG(lvg.Spec.ActualVGNameOnTheNode)
	if !found {
		err := fmt.Errorf("VG %s not found", lvg.Spec.ActualVGNameOnTheNode)
		r.log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to reconcile the LVMVolumeGroup %s", lvg.Name))
		err = r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, "VGNotFound", err.Error())
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to add a condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
		}
		return true, err
	}
	r.log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] VG %s found for the LVMVolumeGroup %s", vg.VGName, lvg.Name))

	r.log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] tries to check and update VG %s tag %s", lvg.Spec.ActualVGNameOnTheNode, internal.LVMTags[0]))
	updated, err := r.updateVGTagIfNeeded(ctx, lvg, vg)
	if err != nil {
		r.log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to update VG %s tag of the LVMVolumeGroup %s", vg.VGName, lvg.Name))
		err = r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, "VGUpdateFailed", fmt.Sprintf("unable to update VG tag, err: %s", err.Error()))
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to add a condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
		}

		return true, err
	}

	if updated {
		r.log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] successfully updated VG %s tag of the LVMVolumeGroup %s", vg.VGName, lvg.Name))
	} else {
		r.log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] no need to update VG %s tag of the LVMVolumeGroup %s", vg.VGName, lvg.Name))
	}

	r.log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] starts to resize PV of the LVMVolumeGroup %s", lvg.Name))
	err = r.resizePVIfNeeded(ctx, lvg)
	if err != nil {
		r.log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to resize PV of the LVMVolumeGroup %s", lvg.Name))
		err = r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, "PVResizeFailed", fmt.Sprintf("unable to resize PV, err: %s", err.Error()))
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to add a condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
		}
		return true, err
	}
	r.log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] successfully ended the resize operation for PV of the LVMVolumeGroup %s", lvg.Name))

	r.log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] starts to extend VG %s of the LVMVolumeGroup %s", vg.VGName, lvg.Name))
	err = r.extendVGIfNeeded(ctx, lvg, vg, pvs, blockDevices)
	if err != nil {
		r.log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to extend VG of the LVMVolumeGroup %s", lvg.Name))
		err = r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, "VGExtendFailed", fmt.Sprintf("unable to extend VG, err: %s", err.Error()))
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to add a condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
		}

		return true, err
	}
	r.log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] successfully ended the extend operation for VG of the LVMVolumeGroup %s", lvg.Name))

	r.log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] starts to extend VG %s of the LVMVolumeGroup %s with file devices", vg.VGName, lvg.Name))
	err = r.extendFileDevicesIfNeeded(ctx, lvg, vg, pvs)
	if err != nil {
		r.log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to extend VG of the LVMVolumeGroup %s with file devices", lvg.Name))
		err = r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, "VGExtendFailed", fmt.Sprintf("unable to extend VG with file devices, err: %s", err.Error()))
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to add a condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
		}

		return true, err
	}
	r.log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] successfully ended the file-device extend operation for VG of the LVMVolumeGroup %s", lvg.Name))

	if lvg.Spec.ThinPools != nil {
		r.log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] starts to reconcile thin-pools of the LVMVolumeGroup %s", lvg.Name))
		lvs, _ := r.sdsCache.GetLVs()
		err = r.reconcileThinPoolsIfNeeded(ctx, lvg, vg, lvs)
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to reconcile thin-pools of the LVMVolumeGroup %s", lvg.Name))
			err = r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, "ThinPoolReconcileFailed", fmt.Sprintf("unable to reconcile thin-pools, err: %s", err.Error()))
			if err != nil {
				r.log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to add a condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
			}
			return true, err
		}
		r.log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] successfully reconciled thin-pools operation of the LVMVolumeGroup %s", lvg.Name))
	}

	r.log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] tries to add a condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
	err = r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionTrue, internal.TypeVGConfigurationApplied, internal.ReasonApplied, "configuration has been applied")
	if err != nil {
		r.log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to add a condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
		return true, err
	}
	r.log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] successfully added a condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
	r.log.Info(fmt.Sprintf("[reconcileLVGUpdateFunc] successfully reconciled the LVMVolumeGroup %s", lvg.Name))

	return false, nil
}

func (r *Reconciler) reconcileLVGCreateFunc(
	ctx context.Context,
	lvg *v1alpha1.LVMVolumeGroup,
	blockDevices map[string]v1alpha1.BlockDevice,
) (bool, error) {
	r.log.Debug(fmt.Sprintf("[reconcileLVGCreateFunc] starts to reconcile the LVMVolumeGroup %s", lvg.Name))

	// this check prevents the LVMVolumeGroup resource's infinity updating after a retry
	exist := false
	for _, c := range lvg.Status.Conditions {
		if c.Type == internal.TypeVGConfigurationApplied {
			exist = true
			break
		}
	}

	if !exist {
		r.log.Debug(fmt.Sprintf("[reconcileLVGCreateFunc] tries to add the condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
		err := r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonCreating, "trying to apply the configuration")
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[reconcileLVGCreateFunc] unable to add the condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
			return true, err
		}
	}

	r.log.Debug(fmt.Sprintf("[reconcileLVGCreateFunc] tries to validate the LVMVolumeGroup %s", lvg.Name))
	valid, reason := r.validateLVGForCreateFunc(ctx, lvg, blockDevices)
	if !valid {
		r.log.Warning(fmt.Sprintf("[reconcileLVGCreateFunc] validation fails for the LVMVolumeGroup %s", lvg.Name))
		err := r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonValidationFailed, reason)
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to add a condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
		}

		return true, err
	}
	r.log.Debug(fmt.Sprintf("[reconcileLVGCreateFunc] successfully validated the LVMVolumeGroup %s", lvg.Name))

	r.log.Debug(fmt.Sprintf("[reconcileLVGCreateFunc] tries to create VG for the LVMVolumeGroup %s", lvg.Name))
	err := r.createVGComplex(ctx, lvg, blockDevices)
	if err != nil {
		r.log.Error(err, fmt.Sprintf("[reconcileLVGCreateFunc] unable to create VG for the LVMVolumeGroup %s", lvg.Name))
		err = r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, "VGCreationFailed", fmt.Sprintf("unable to create VG, err: %s", err.Error()))
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to add a condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
		}
		return true, err
	}
	r.log.Info(fmt.Sprintf("[reconcileLVGCreateFunc] successfully created VG for the LVMVolumeGroup %s", lvg.Name))

	if lvg.Spec.ThinPools != nil {
		r.log.Debug(fmt.Sprintf("[reconcileLVGCreateFunc] the LVMVolumeGroup %s has thin-pools. Tries to create them", lvg.Name))

		var vgAfterCreate *internal.VGData
		if freshVG, _, _, getErr := r.commands.GetVG(lvg.Spec.ActualVGNameOnTheNode); getErr == nil {
			v := freshVG
			vgAfterCreate = &v
		} else {
			r.log.Warning(fmt.Sprintf("[reconcileLVGCreateFunc] unable to get VG %s after creation, will use default extent size for thin-pool alignment: %v", lvg.Spec.ActualVGNameOnTheNode, getErr))
		}
		extentForThinPools := extentSizeForThinPoolAlign(lvg, vgAfterCreate)

		for _, tp := range lvg.Spec.ThinPools {
			// vgSize must account for file-backed PVs too: a file-only VG has
			// no block devices, so countVGSizeByBlockDevices alone returns 0,
			// which collapses every percentage thin-pool to 0 and forces every
			// absolute-sized thin-pool into the full-VG-space branch (taking the
			// whole VG instead of the requested size). Add the spec.fileDevices
			// capacity, mirroring how validateLVGForCreateFunc computes totalVGSize.
			vgSize := countVGSizeByBlockDevices(blockDevices)
			vgSize.Add(countVGSizeByFileDevices(lvg))
			tpRequestedSize, err := utils.GetRequestedSizeFromString(tp.Size, vgSize)
			if err != nil {
				r.log.Error(err, fmt.Sprintf("[reconcileLVGCreateFunc] unable to get thin-pool %s requested size of the LVMVolumeGroup %s", tp.Name, lvg.Name))
				return false, err
			}

			var cmd string
			alignedTpSize, alignErr := utils.AlignSizeToExtent(tpRequestedSize, extentForThinPools)
			if alignErr != nil {
				r.log.Error(alignErr, fmt.Sprintf("[reconcileLVGCreateFunc] unable to align thin-pool %s size for LVMVolumeGroup %s", tp.Name, lvg.Name))
				return false, alignErr
			}
			if alignedTpSize.Value() >= vgSize.Value() {
				r.log.Debug(fmt.Sprintf("[reconcileLVGCreateFunc] Thin-pool %s of the LVMVolumeGroup %s will be created with full VG space size", tp.Name, lvg.Name))
				cmd, err = r.commands.CreateThinPoolFullVGSpace(tp.Name, lvg.Spec.ActualVGNameOnTheNode)
			} else {
				r.log.Debug(fmt.Sprintf("[reconcileLVGCreateFunc] Thin-pool %s of the LVMVolumeGroup %s will be created with size %s", tp.Name, lvg.Name, alignedTpSize.String()))
				cmd, err = r.commands.CreateThinPool(tp.Name, lvg.Spec.ActualVGNameOnTheNode, alignedTpSize.Value())
			}
			if err != nil {
				r.log.Error(err, fmt.Sprintf("[reconcileLVGCreateFunc] unable to create thin-pool %s of the LVMVolumeGroup %s, cmd: %s", tp.Name, lvg.Name, cmd))
				err = r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, "ThinPoolCreationFailed", fmt.Sprintf("unable to create thin-pool, err: %s", err.Error()))
				if err != nil {
					r.log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to add a condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
				}

				return true, err
			}
		}
		r.log.Debug(fmt.Sprintf("[reconcileLVGCreateFunc] successfully created thin-pools for the LVMVolumeGroup %s", lvg.Name))
	}

	err = r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionTrue, internal.TypeVGConfigurationApplied, internal.ReasonApplied, "all configuration has been applied")
	if err != nil {
		r.log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to add a condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
		return true, err
	}

	return false, nil
}

func (r *Reconciler) shouldUpdateLVGLabels(lvg *v1alpha1.LVMVolumeGroup, labelKey, labelValue string) bool {
	if lvg.Labels == nil {
		r.log.Debug(fmt.Sprintf("[shouldUpdateLVGLabels] the LVMVolumeGroup %s has no labels.", lvg.Name))
		return true
	}

	val, exist := lvg.Labels[labelKey]
	if !exist {
		r.log.Debug(fmt.Sprintf("[shouldUpdateLVGLabels] the LVMVolumeGroup %s has no label %s.", lvg.Name, labelKey))
		return true
	}

	if val != labelValue {
		r.log.Debug(fmt.Sprintf("[shouldUpdateLVGLabels] the LVMVolumeGroup %s has label %s but the value is incorrect - %s (should be %s)", lvg.Name, labelKey, val, labelValue))
		return true
	}

	return false
}

func (r *Reconciler) shouldLVGWatcherReconcileUpdateEvent(oldLVG, newLVG *v1alpha1.LVMVolumeGroup) bool {
	if !checkIfLVGBelongsToNode(newLVG, r.cfg.NodeName) {
		return false
	}

	if newLVG.DeletionTimestamp != nil {
		r.log.Debug(fmt.Sprintf("[shouldLVGWatcherReconcileUpdateEvent] update event should be reconciled as the LVMVolumeGroup %s has deletionTimestamp", newLVG.Name))
		return true
	}

	for _, c := range newLVG.Status.Conditions {
		if c.Type == internal.TypeVGConfigurationApplied {
			if c.Reason == internal.ReasonUpdating || c.Reason == internal.ReasonCreating {
				r.log.Debug(fmt.Sprintf("[shouldLVGWatcherReconcileUpdateEvent] update event should not be reconciled as the LVMVolumeGroup %s reconciliation still in progress", newLVG.Name))
				return false
			}
		}
	}

	if _, exist := newLVG.Labels[internal.LVGUpdateTriggerLabel]; exist {
		r.log.Debug(fmt.Sprintf("[shouldLVGWatcherReconcileUpdateEvent] update event should be reconciled as the LVMVolumeGroup %s has the label %s", newLVG.Name, internal.LVGUpdateTriggerLabel))
		return true
	}

	if r.shouldUpdateLVGLabels(newLVG, internal.LVGMetadataNameLabelKey, newLVG.Name) {
		r.log.Debug(fmt.Sprintf("[shouldLVGWatcherReconcileUpdateEvent] update event should be reconciled as the LVMVolumeGroup's %s labels have been changed", newLVG.Name))
		return true
	}

	if !reflect.DeepEqual(oldLVG.Spec, newLVG.Spec) {
		r.log.Debug(fmt.Sprintf("[shouldLVGWatcherReconcileUpdateEvent] update event should be reconciled as the LVMVolumeGroup %s configuration has been changed", newLVG.Name))
		return true
	}

	if hasStatusNodesDiff(r.log, oldLVG.Status.Nodes, newLVG.Status.Nodes) {
		r.log.Debug(fmt.Sprintf("[shouldLVGWatcherReconcileUpdateEvent] update event should be reconciled as the LVMVolumeGroup %s status nodes have changed", newLVG.Name))
		return true
	}

	return false
}

func (r *Reconciler) addLVGFinalizerIfNotExist(ctx context.Context, lvg *v1alpha1.LVMVolumeGroup) (bool, error) {
	if slices.Contains(lvg.Finalizers, internal.SdsNodeConfiguratorFinalizer) {
		return false, nil
	}

	lvg.Finalizers = append(lvg.Finalizers, internal.SdsNodeConfiguratorFinalizer)
	err := r.cl.Update(ctx, lvg)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (r *Reconciler) syncThinPoolsAllocationLimit(ctx context.Context, lvg *v1alpha1.LVMVolumeGroup) error {
	updated := false

	tpSpecLimits := make(map[string]string, len(lvg.Spec.ThinPools))
	for _, tp := range lvg.Spec.ThinPools {
		tpSpecLimits[tp.Name] = tp.AllocationLimit
	}

	var (
		space resource.Quantity
		err   error
	)
	for i := range lvg.Status.ThinPools {
		if specLimits, matched := tpSpecLimits[lvg.Status.ThinPools[i].Name]; matched {
			if lvg.Status.ThinPools[i].AllocationLimit != specLimits {
				r.log.Debug(fmt.Sprintf("[syncThinPoolsAllocationLimit] thin-pool %s status AllocationLimit: %s of the LVMVolumeGroup %s should be updated by spec one: %s", lvg.Status.ThinPools[i].Name, lvg.Status.ThinPools[i].AllocationLimit, lvg.Name, specLimits))
				updated = true
				lvg.Status.ThinPools[i].AllocationLimit = specLimits

				space, err = utils.GetThinPoolAvailableSpace(lvg.Status.ThinPools[i].ActualSize, lvg.Status.ThinPools[i].AllocatedSize, specLimits)
				if err != nil {
					r.log.Error(err, fmt.Sprintf("[syncThinPoolsAllocationLimit] unable to get thin pool %s available space", lvg.Status.ThinPools[i].Name))
					return err
				}
				r.log.Debug(fmt.Sprintf("[syncThinPoolsAllocationLimit] successfully got a new available space %s of the thin-pool %s", space.String(), lvg.Status.ThinPools[i].Name))
				lvg.Status.ThinPools[i].AvailableSpace = space
			}
		} else {
			r.log.Debug(fmt.Sprintf("[syncThinPoolsAllocationLimit] status thin-pool %s of the LVMVolumeGroup %s was not found as used in spec", lvg.Status.ThinPools[i].Name, lvg.Name))
		}
	}

	if updated {
		r.log.Trace(fmt.Sprintf("[syncThinPoolsAllocationLimit] LVMVolumeGroup %s ThinPools: %+v", lvg.Name, lvg.Status.ThinPools))
		r.log.Debug(fmt.Sprintf("[syncThinPoolsAllocationLimit] tries to update the LVMVolumeGroup %s", lvg.Name))
		err = r.cl.Status().Update(ctx, lvg)
		if err != nil {
			return err
		}
		r.log.Debug(fmt.Sprintf("[syncThinPoolsAllocationLimit] successfully updated the LVMVolumeGroup %s", lvg.Name))
	} else {
		r.log.Debug(fmt.Sprintf("[syncThinPoolsAllocationLimit] every status thin-pool AllocationLimit value is synced with spec one for the LVMVolumeGroup %s", lvg.Name))
	}

	return nil
}

func (r *Reconciler) deleteLVGIfNeeded(ctx context.Context, lvg *v1alpha1.LVMVolumeGroup) (bool, error) {
	if lvg.DeletionTimestamp == nil {
		return false, nil
	}

	vgs, _ := r.sdsCache.GetVGs()
	if !checkIfVGExist(lvg.Spec.ActualVGNameOnTheNode, vgs) {
		r.log.Info(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] VG %s was not yet created for the LVMVolumeGroup %s and the resource is marked as deleting. Delete the resource", lvg.Spec.ActualVGNameOnTheNode, lvg.Name))
		removed, err := r.removeLVGFinalizerIfExist(ctx, lvg)
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to remove the finalizer %s from the LVMVolumeGroup %s", internal.SdsNodeConfiguratorFinalizer, lvg.Name))
			return false, err
		}

		if removed {
			r.log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] successfully removed the finalizer %s from the LVMVolumeGroup %s", internal.SdsNodeConfiguratorFinalizer, lvg.Name))
		} else {
			r.log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] no need to remove the finalizer %s from the LVMVolumeGroup %s", internal.SdsNodeConfiguratorFinalizer, lvg.Name))
		}

		err = r.lvgCl.DeleteLVMVolumeGroup(ctx, lvg)
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to delete the LVMVolumeGroup %s", lvg.Name))
			return false, err
		}
		r.log.Info(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] successfully deleted the LVMVolumeGroup %s", lvg.Name))
		return true, nil
	}
	return false, nil
}

func (r *Reconciler) validateLVGForCreateFunc(
	ctx context.Context,
	lvg *v1alpha1.LVMVolumeGroup,
	blockDevices map[string]v1alpha1.BlockDevice,
) (bool, string) {
	reason := strings.Builder{}

	r.log.Debug(fmt.Sprintf("[validateLVGForCreateFunc] check if every selected BlockDevice of the LVMVolumeGroup %s is consumable", lvg.Name))
	// totalVGSize needs to count if there is enough space for requested thin-pools
	totalVGSize := countVGSizeByBlockDevices(blockDevices)
	for _, bd := range blockDevices {
		if !bd.Status.Consumable {
			r.log.Warning(fmt.Sprintf("[validateLVGForCreateFunc] BlockDevice %s is not consumable", bd.Name))
			r.log.Trace(fmt.Sprintf("[validateLVGForCreateFunc] BlockDevice name: %s, status: %+v", bd.Name, bd.Status))
			reason.WriteString(fmt.Sprintf("BlockDevice %s is not consumable. ", bd.Name))
		}
	}

	if reason.Len() == 0 {
		r.log.Debug(fmt.Sprintf("[validateLVGForCreateFunc] all BlockDevices of the LVMVolumeGroup %s are consumable", lvg.Name))
	}

	r.validateFileDevices(ctx, lvg, &reason, &totalVGSize)

	if lvg.Spec.ThinPools != nil {
		r.log.Debug(fmt.Sprintf("[validateLVGForCreateFunc] the LVMVolumeGroup %s has thin-pools. Validate if VG size has enough space for the thin-pools", lvg.Name))
		r.log.Trace(fmt.Sprintf("[validateLVGForCreateFunc] the LVMVolumeGroup %s has thin-pools %v", lvg.Name, lvg.Spec.ThinPools))
		r.log.Trace(fmt.Sprintf("[validateLVGForCreateFunc] total LVMVolumeGroup %s size: %s", lvg.Name, totalVGSize.String()))

		var totalThinPoolSize int64
		for _, tp := range lvg.Spec.ThinPools {
			tpRequestedSize, err := utils.GetRequestedSizeFromString(tp.Size, totalVGSize)
			if err != nil {
				reason.WriteString(err.Error())
				continue
			}

			if tpRequestedSize.Value() == 0 {
				reason.WriteString(fmt.Sprintf("Thin-pool %s has zero size. ", tp.Name))
				continue
			}

			alignedTpSize, alignErr := alignThinPoolSizeForValidation(tp.Size, tpRequestedSize, extentSizeForThinPoolAlign(lvg, nil))
			if alignErr != nil {
				reason.WriteString(fmt.Sprintf("Unable to align thin-pool %s size: %s. ", tp.Name, alignErr.Error()))
				continue
			}
			if alignedTpSize.Value() >= totalVGSize.Value() {
				if len(lvg.Spec.ThinPools) > 1 {
					reason.WriteString(fmt.Sprintf("Thin-pool %s requested size of full VG space, but there is any other thin-pool. ", tp.Name))
				}
			}

			totalThinPoolSize += alignedTpSize.Value()
		}
		r.log.Trace(fmt.Sprintf("[validateLVGForCreateFunc] LVMVolumeGroup %s thin-pools requested space: %d", lvg.Name, totalThinPoolSize))

		if totalThinPoolSize > totalVGSize.Value() {
			r.log.Trace(fmt.Sprintf("[validateLVGForCreateFunc] total thin pool size: %s, total vg size: %s", resource.NewQuantity(totalThinPoolSize, resource.BinarySI).String(), totalVGSize.String()))
			r.log.Warning(fmt.Sprintf("[validateLVGForCreateFunc] requested thin pool size is more than VG total size for the LVMVolumeGroup %s", lvg.Name))
			reason.WriteString(fmt.Sprintf("Required space for thin-pools %d is more than VG size %d.", totalThinPoolSize, totalVGSize.Value()))
		}
	}

	if reason.Len() != 0 {
		return false, reason.String()
	}

	return true, ""
}

var minFileDeviceSize = resource.MustParse("1Gi")

// isWithinBaseDir reports whether dir is base or a descendant of it. Both are
// cleaned before comparison so trailing slashes and redundant separators do
// not matter. dir is expected to be absolute and already free of '..'
// segments (validateFileDevice enforces that first), so the prefix check
// cannot be fooled into escaping base.
func isWithinBaseDir(dir, base string) bool {
	base = filepath.Clean(base)
	dir = filepath.Clean(dir)
	return dir == base || strings.HasPrefix(dir, base+string(filepath.Separator))
}

func (r *Reconciler) validateFileDevices(
	ctx context.Context,
	lvg *v1alpha1.LVMVolumeGroup,
	reason *strings.Builder,
	totalVGSize *resource.Quantity,
) {
	seen := make(map[string]int, len(lvg.Spec.FileDevices))
	for i, fd := range lvg.Spec.FileDevices {
		key := utils.BuildFileDevicePath(fd.Directory, lvg.Name, fd.Size)
		if prev, ok := seen[key]; ok {
			fmt.Fprintf(reason, "fileDevices[%d] collides with fileDevices[%d]: same backing file would be created. ", i, prev)
			continue
		}
		seen[key] = i
		r.validateFileDevice(ctx, fd, i, reason, totalVGSize)
	}
}

func (r *Reconciler) validateFileDevice(
	_ context.Context,
	fd v1alpha1.LVMVolumeGroupFileDeviceSpec,
	index int,
	reason *strings.Builder,
	totalVGSize *resource.Quantity,
) {
	if fd.Directory == "" {
		fmt.Fprintf(reason, "fileDevices[%d].directory is empty. ", index)
		return
	}

	// The agent runs in PID 1's mount namespace; an absolute path with
	// no `..` segment is the minimum sanity check that keeps `fallocate`
	// from creating runaway files outside whatever directory the cluster
	// admin intended.
	cleaned := filepath.Clean(fd.Directory)
	if !filepath.IsAbs(cleaned) {
		fmt.Fprintf(reason, "fileDevices[%d].directory %q must be an absolute path. ", index, fd.Directory)
		return
	}
	if cleaned != fd.Directory && strings.Contains(fd.Directory, "..") {
		fmt.Fprintf(reason, "fileDevices[%d].directory %q must not contain '..' segments. ", index, fd.Directory)
		return
	}

	// Confine backing files to the configured base directory (module config
	// fileDevicesDirectory, default /opt/deckhouse/sds/file-devices). Without
	// this an arbitrary host path — `/`, `/etc`, `/var/lib/kubelet` — could be
	// targeted, and one oversized file there fills the node's filesystem and
	// trips kubelet DiskPressure eviction. An empty base disables the check
	// (unit tests that do not exercise the allowlist).
	if r.cfg.FileDevicesDirectory != "" && !isWithinBaseDir(cleaned, r.cfg.FileDevicesDirectory) {
		fmt.Fprintf(reason, "fileDevices[%d].directory %q must be %q or a subdirectory of it. ", index, fd.Directory, r.cfg.FileDevicesDirectory)
		return
	}

	if fd.Size.Value() < minFileDeviceSize.Value() {
		// A common cause is a decimal unit (e.g. "1G" = 10^9 bytes) where a
		// binary unit was meant ("1Gi" = 2^30 bytes); the former is below the
		// minimum. Point the user at binary units so they do not get stuck on
		// an immutable, too-small entry.
		fmt.Fprintf(reason, "fileDevices[%d].size %s is less than the minimum %s; use a binary unit such as Gi. ", index, fd.Size.String(), minFileDeviceSize.String())
		return
	}

	// The backing directory is created on demand by provisionFileDevices
	// (mkdir -p in PID 1's mount namespace), so validation only enforces the
	// structural rules above. A genuinely unusable path (read-only FS, a file
	// where a directory is expected, …) surfaces as a provisioning error and
	// is reported on the VGConfigurationApplied condition.

	if totalVGSize != nil {
		totalVGSize.Add(fd.Size)
	}
}

func (r *Reconciler) validateLVGForUpdateFunc(
	ctx context.Context,
	lvg *v1alpha1.LVMVolumeGroup,
	blockDevices map[string]v1alpha1.BlockDevice,
) (bool, string) {
	reason := strings.Builder{}

	// Bail out before any name-keyed cache lookups when the underlying VG name
	// is ambiguous on the node. Without this, FindVG/FindLV would return data
	// from an arbitrary VG of the same name and produce misleading reasons
	// (e.g. "Added thin-pools requested sizes are more than allowed free space
	// in VG") that hide the real problem.
	allVGs, _ := r.sdsCache.GetVGs()
	if duplicateVGs := findDuplicateVGNames(allVGs); len(duplicateVGs) > 0 {
		if uuids, dup := duplicateVGs[lvg.Spec.ActualVGNameOnTheNode]; dup {
			return false, duplicateVGMessage(lvg.Spec.ActualVGNameOnTheNode, uuids)
		}
	}

	pvs, _ := r.sdsCache.GetPVs()
	r.log.Debug(fmt.Sprintf("[validateLVGForUpdateFunc] check if every new BlockDevice of the LVMVolumeGroup %s is comsumable", lvg.Name))
	actualPVPaths := make(map[string]struct{}, len(pvs))
	for _, pv := range pvs {
		actualPVPaths[pv.PVName] = struct{}{}
	}

	//TODO: add a check if BlockDevice size got less than PV size

	// Check if added BlockDevices are consumable
	// additionBlockDeviceSpace value is needed to count if VG will have enough space for thin-pools
	var additionBlockDeviceSpace int64
	for _, bd := range blockDevices {
		if _, found := actualPVPaths[bd.Status.Path]; !found {
			r.log.Debug(fmt.Sprintf("[validateLVGForUpdateFunc] unable to find the PV %s for BlockDevice %s. Check if the BlockDevice is already used", bd.Status.Path, bd.Name))
			for _, n := range lvg.Status.Nodes {
				for _, d := range n.Devices {
					if d.BlockDevice == bd.Name {
						r.log.Warning(fmt.Sprintf("[validateLVGForUpdateFunc] BlockDevice %s misses the PV %s. That might be because the corresponding device was removed from the node. Unable to validate BlockDevices", bd.Name, bd.Status.Path))
						reason.WriteString(fmt.Sprintf("BlockDevice %s misses the PV %s (that might be because the device was removed from the node). ", bd.Name, bd.Status.Path))
					}

					if reason.Len() == 0 {
						r.log.Debug(fmt.Sprintf("[validateLVGForUpdateFunc] BlockDevice %s does not miss a PV", d.BlockDevice))
					}
				}
			}

			r.log.Debug(fmt.Sprintf("[validateLVGForUpdateFunc] PV %s for BlockDevice %s of the LVMVolumeGroup %s is not created yet, check if the BlockDevice is consumable", bd.Status.Path, bd.Name, lvg.Name))
			if reason.Len() > 0 {
				r.log.Debug("[validateLVGForUpdateFunc] some BlockDevices misses its PVs, unable to check if they are consumable")
				continue
			}

			if !bd.Status.Consumable {
				reason.WriteString(fmt.Sprintf("BlockDevice %s is not consumable. ", bd.Name))
				continue
			}

			r.log.Debug(fmt.Sprintf("[validateLVGForUpdateFunc] BlockDevice %s is consumable", bd.Name))
			additionBlockDeviceSpace += bd.Status.Size.Value()
		}
	}

	r.validateFileDevices(ctx, lvg, &reason, nil)

	// additionFileDeviceSpace mirrors additionBlockDeviceSpace for file
	// devices: it accounts for spec.fileDevices entries that are not yet PVs
	// in the VG (i.e. not yet reflected in status.nodes[].fileDevices). Without
	// it, a combined "append a fileDevices entry + grow a thin-pool" edit would
	// be validated against the current VG size, wrongly rejecting a valid
	// request or forcing an absolute-sized thin-pool into the full-VG-space
	// branch — the same defect the create path avoids via countVGSizeByFileDevices.
	var additionFileDeviceSpace int64
	if len(lvg.Spec.FileDevices) > 0 {
		// Match by basename, not full path: status.nodes[].fileDevices[].FilePath
		// is the loop's backing file as reported by `losetup --output BACK-FILE`,
		// which canonicalizes symlink components of the directory (e.g. a spec
		// directory /data symlinked to /mnt/disk1/data is reported as
		// /mnt/disk1/data/...), while BuildFileDevicePath keeps the literal spec
		// directory. The basename `sds-<lvgName>-<hash>.img` is identical on both
		// sides (the hash is derived from the literal spec directory string and
		// losetup leaves the basename untouched), so a full-path compare would
		// miss an already-provisioned device whenever the directory is a symlink
		// and count it as new on every reconcile, inflating the VG size used for
		// thin-pool validation.
		existingBasenames := make(map[string]struct{})
		for _, n := range lvg.Status.Nodes {
			for _, fd := range n.FileDevices {
				existingBasenames[filepath.Base(fd.FilePath)] = struct{}{}
			}
		}
		for _, fd := range lvg.Spec.FileDevices {
			base := filepath.Base(utils.BuildFileDevicePath(fd.Directory, lvg.Name, fd.Size))
			if _, ok := existingBasenames[base]; !ok {
				additionFileDeviceSpace += fd.Size.Value()
			}
		}
	}

	if lvg.Spec.ThinPools != nil {
		r.log.Debug(fmt.Sprintf("[validateLVGForUpdateFunc] the LVMVolumeGroup %s has thin-pools. Validate them", lvg.Name))
		actualThinPools := make(map[string]internal.LVData, len(lvg.Spec.ThinPools))
		for _, tp := range lvg.Spec.ThinPools {
			lv := r.sdsCache.FindLV(lvg.Spec.ActualVGNameOnTheNode, tp.Name)
			if lv != nil {
				if !isThinPool(lv.Data) {
					reason.WriteString(fmt.Sprintf("LV %s is already created on the node and it is not a thin-pool", lv.Data.LVName))
					continue
				}

				actualThinPools[lv.Data.LVName] = lv.Data
			}
		}

		// check if added thin-pools has valid requested size
		var (
			addingThinPoolSize int64
			hasFullThinPool    = false
		)

		vg := r.sdsCache.FindVG(lvg.Spec.ActualVGNameOnTheNode)
		if vg == nil {
			reason.WriteString(fmt.Sprintf("Missed VG %s in the cache", lvg.Spec.ActualVGNameOnTheNode))
			return false, reason.String()
		}

		newTotalVGSize := resource.NewQuantity(vg.VGSize.Value()+additionBlockDeviceSpace+additionFileDeviceSpace, resource.BinarySI)
		for _, specTp := range lvg.Spec.ThinPools {
			// might be a case when Thin-pool is already created, but is not shown in status
			tpRequestedSize, err := utils.GetRequestedSizeFromString(specTp.Size, *newTotalVGSize)
			if err != nil {
				reason.WriteString(err.Error())
				continue
			}

			if tpRequestedSize.Value() == 0 {
				reason.WriteString(fmt.Sprintf("Thin-pool %s has zero size. ", specTp.Name))
				continue
			}

			r.log.Debug(fmt.Sprintf("[validateLVGForUpdateFunc] the LVMVolumeGroup %s thin-pool %s requested size %s, Status VG size %s", lvg.Name, specTp.Name, tpRequestedSize.String(), lvg.Status.VGSize.String()))
			alignedTpSize, alignErr := utils.AlignSizeToExtent(tpRequestedSize, extentSizeForThinPoolAlign(lvg, vg))
			if alignErr != nil {
				reason.WriteString(fmt.Sprintf("Unable to align thin-pool %s size: %s. ", specTp.Name, alignErr.Error()))
				continue
			}
			if alignedTpSize.Value() >= newTotalVGSize.Value() {
				hasFullThinPool = true
				if len(lvg.Spec.ThinPools) > 1 {
					reason.WriteString(fmt.Sprintf("Thin-pool %s requests size of full VG space, but there are any other thin-pools. ", specTp.Name))
				}
			} else {
				if actualThinPool, created := actualThinPools[specTp.Name]; !created {
					r.log.Debug(fmt.Sprintf("[validateLVGForUpdateFunc] thin-pool %s of the LVMVolumeGroup %s is not yet created, adds its requested size", specTp.Name, lvg.Name))
					addingThinPoolSize += alignedTpSize.Value()
				} else {
					r.log.Debug(fmt.Sprintf("[validateLVGForUpdateFunc] thin-pool %s of the LVMVolumeGroup %s is already created, check its requested size", specTp.Name, lvg.Name))
					if alignedTpSize.Value() < actualThinPool.LVSize.Value() {
						r.log.Debug(fmt.Sprintf("[validateLVGForUpdateFunc] the LVMVolumeGroup %s Spec.ThinPool %s size %s is less than Status one: %s", lvg.Name, specTp.Name, tpRequestedSize.String(), actualThinPool.LVSize.String()))
						reason.WriteString(fmt.Sprintf("Requested Spec.ThinPool %s size %s is less than actual one %s. ", specTp.Name, tpRequestedSize.String(), actualThinPool.LVSize.String()))
						continue
					}

					thinPoolSizeDiff := alignedTpSize.Value() - actualThinPool.LVSize.Value()
					if thinPoolSizeDiff > 0 {
						r.log.Debug(fmt.Sprintf("[validateLVGForUpdateFunc] the LVMVolumeGroup %s Spec.ThinPool %s size %s more than Status one: %s", lvg.Name, specTp.Name, tpRequestedSize.String(), actualThinPool.LVSize.String()))
						addingThinPoolSize += thinPoolSizeDiff
					}
				}
			}
		}

		if !hasFullThinPool {
			allocatedSize := getVGAllocatedSize(*vg)
			totalFreeSpace := newTotalVGSize.Value() - allocatedSize.Value()
			r.log.Trace(fmt.Sprintf("[validateLVGForUpdateFunc] new LVMVolumeGroup %s thin-pools requested %d size, additional BlockDevices space %d, total: %d", lvg.Name, addingThinPoolSize, additionBlockDeviceSpace, totalFreeSpace))
			if addingThinPoolSize != 0 && addingThinPoolSize > totalFreeSpace {
				reason.WriteString("Added thin-pools requested sizes are more than allowed free space in VG.")
			}
		}
	}

	if reason.Len() != 0 {
		return false, reason.String()
	}

	return true, ""
}

func (r *Reconciler) identifyLVGReconcileFunc(lvg *v1alpha1.LVMVolumeGroup) internal.ReconcileType {
	if r.shouldReconcileLVGByCreateFunc(lvg) {
		return internal.CreateReconcile
	}

	if r.shouldReconcileLVGByUpdateFunc(lvg) {
		return internal.UpdateReconcile
	}

	if r.shouldReconcileLVGByDeleteFunc(lvg) {
		return internal.DeleteReconcile
	}

	return "none"
}

func (r *Reconciler) shouldReconcileLVGByCreateFunc(lvg *v1alpha1.LVMVolumeGroup) bool {
	if lvg.DeletionTimestamp != nil {
		return false
	}

	vg := r.sdsCache.FindVG(lvg.Spec.ActualVGNameOnTheNode)
	return vg == nil
}

func (r *Reconciler) shouldReconcileLVGByUpdateFunc(lvg *v1alpha1.LVMVolumeGroup) bool {
	if lvg.DeletionTimestamp != nil {
		return false
	}

	vg := r.sdsCache.FindVG(lvg.Spec.ActualVGNameOnTheNode)
	return vg != nil
}

func (r *Reconciler) shouldReconcileLVGByDeleteFunc(lvg *v1alpha1.LVMVolumeGroup) bool {
	return lvg.DeletionTimestamp != nil
}

func (r *Reconciler) reconcileThinPoolsIfNeeded(
	ctx context.Context,
	lvg *v1alpha1.LVMVolumeGroup,
	vg internal.VGData,
	lvs []internal.LVData,
) error {
	actualThinPools := make(map[string]internal.LVData, len(lvs))
	for _, lv := range lvs {
		if isThinPool(lv) {
			actualThinPools[lv.LVName] = lv
		}
	}

	errs := strings.Builder{}
	for _, specTp := range lvg.Spec.ThinPools {
		tpRequestedSize, err := utils.GetRequestedSizeFromString(specTp.Size, lvg.Status.VGSize)
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[ReconcileThinPoolsIfNeeded] unable to get requested thin-pool %s size of the LVMVolumeGroup %s", specTp.Name, lvg.Name))
			return err
		}

		if actualTp, exist := actualThinPools[specTp.Name]; !exist {
			r.log.Debug(fmt.Sprintf("[ReconcileThinPoolsIfNeeded] thin-pool %s of the LVMVolumeGroup %s is not created yet. Create it", specTp.Name, lvg.Name))
			if isApplied(lvg) {
				err := r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonUpdating, "trying to apply the configuration")
				if err != nil {
					r.log.Error(err, fmt.Sprintf("[ReconcileThinPoolsIfNeeded] unable to add the condition %s status False reason %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, internal.ReasonUpdating, lvg.Name))
					return err
				}
			}

			var cmd string
			start := time.Now()
			alignedTpSize, alignErr := utils.AlignSizeToExtent(tpRequestedSize, extentSizeForThinPoolAlign(lvg, &vg))
			if alignErr != nil {
				r.log.Error(alignErr, fmt.Sprintf("[ReconcileThinPoolsIfNeeded] unable to align thin-pool %s size for LVMVolumeGroup %s", specTp.Name, lvg.Name))
				errs.WriteString(fmt.Sprintf("unable to align thin-pool %s size, err: %s. ", specTp.Name, alignErr.Error()))
				continue
			}
			if alignedTpSize.Value() >= lvg.Status.VGSize.Value() {
				r.log.Debug(fmt.Sprintf("[ReconcileThinPoolsIfNeeded] thin-pool %s of the LVMVolumeGroup %s will be created with size 100FREE", specTp.Name, lvg.Name))
				cmd, err = r.commands.CreateThinPoolFullVGSpace(specTp.Name, vg.VGName)
			} else {
				r.log.Debug(fmt.Sprintf("[ReconcileThinPoolsIfNeeded] thin-pool %s of the LVMVolumeGroup %s will be created with size %s", specTp.Name, lvg.Name, alignedTpSize.String()))
				cmd, err = r.commands.CreateThinPool(specTp.Name, vg.VGName, alignedTpSize.Value())
			}
			r.metrics.UtilsCommandsDuration(ReconcilerName, "lvcreate").Observe(r.metrics.GetEstimatedTimeInSeconds(start))
			r.metrics.UtilsCommandsExecutionCount(ReconcilerName, "lvcreate").Inc()
			if err != nil {
				r.metrics.UtilsCommandsErrorsCount(ReconcilerName, "lvcreate").Inc()
				r.log.Error(err, fmt.Sprintf("[ReconcileThinPoolsIfNeeded] unable to create thin-pool %s of the LVMVolumeGroup %s, cmd: %s", specTp.Name, lvg.Name, cmd))
				errs.WriteString(fmt.Sprintf("unable to create thin-pool %s, err: %s. ", specTp.Name, err.Error()))
				continue
			}

			r.log.Info(fmt.Sprintf("[ReconcileThinPoolsIfNeeded] thin-pool %s of the LVMVolumeGroup %s has been successfully created", specTp.Name, lvg.Name))
		} else {
			alignedTpSizeForResize, alignErr := utils.AlignSizeToExtent(tpRequestedSize, extentSizeForThinPoolAlign(lvg, &vg))
			if alignErr != nil {
				r.log.Error(alignErr, fmt.Sprintf("[ReconcileThinPoolsIfNeeded] unable to align thin-pool %s size for LVMVolumeGroup %s", specTp.Name, lvg.Name))
				errs.WriteString(fmt.Sprintf("unable to align thin-pool %s size, err: %s. ", specTp.Name, alignErr.Error()))
				continue
			}
			if actualTp.LVSize.Value() >= alignedTpSizeForResize.Value() {
				r.log.Debug(fmt.Sprintf("[ReconcileThinPoolsIfNeeded] the LVMVolumeGroup %s requested thin pool %s size is equal to actual one", lvg.Name, tpRequestedSize.String()))
				continue
			}

			r.log.Debug(fmt.Sprintf("[ReconcileThinPoolsIfNeeded] the LVMVolumeGroup %s requested thin pool %s size is more than actual one. Resize it", lvg.Name, tpRequestedSize.String()))
			if isApplied(lvg) {
				err = r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonUpdating, "trying to apply the configuration")
				if err != nil {
					r.log.Error(err, fmt.Sprintf("[ReconcileThinPoolsIfNeeded] unable to add the condition %s status False reason %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, internal.ReasonUpdating, lvg.Name))
					return err
				}
			}
			err = r.extendThinPool(lvg, specTp)
			if err != nil {
				r.log.Error(err, fmt.Sprintf("[ReconcileThinPoolsIfNeeded] unable to resize thin-pool %s of the LVMVolumeGroup %s", specTp.Name, lvg.Name))
				errs.WriteString(fmt.Sprintf("unable to resize thin-pool %s, err: %s. ", specTp.Name, err.Error()))
				continue
			}
		}
	}

	if errs.Len() != 0 {
		return errors.New(errs.String())
	}

	return nil
}

func (r *Reconciler) resizePVIfNeeded(ctx context.Context, lvg *v1alpha1.LVMVolumeGroup) error {
	if len(lvg.Status.Nodes) == 0 {
		r.log.Warning(fmt.Sprintf("[ResizePVIfNeeded] the LVMVolumeGroup %s nodes are empty. Wait for the next update", lvg.Name))
		return nil
	}

	vg := r.sdsCache.FindVG(lvg.Spec.ActualVGNameOnTheNode)
	extentSize := extentSizeForThinPoolAlign(lvg, vg)

	errs := strings.Builder{}
	for _, n := range lvg.Status.Nodes {
		for _, d := range n.Devices {
			if d.DevSize.Value()-d.PVSize.Value() > extentSize.Value() {
				if isApplied(lvg) {
					err := r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonUpdating, "trying to apply the configuration")
					if err != nil {
						r.log.Error(err, fmt.Sprintf("[ResizePVIfNeeded] unable to add the condition %s status False reason %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, internal.ReasonUpdating, lvg.Name))
						return err
					}
				}

				r.log.Debug(fmt.Sprintf("[ResizePVIfNeeded] the LVMVolumeGroup %s BlockDevice %s PVSize is less than actual device size. Resize PV", lvg.Name, d.BlockDevice))

				start := time.Now()
				cmd, err := r.commands.ResizePV(d.Path)
				r.metrics.UtilsCommandsDuration(ReconcilerName, "pvresize").Observe(r.metrics.GetEstimatedTimeInSeconds(start))
				r.metrics.UtilsCommandsExecutionCount(ReconcilerName, "pvresize")
				if err != nil {
					r.metrics.UtilsCommandsErrorsCount(ReconcilerName, "pvresize").Inc()
					r.log.Error(err, fmt.Sprintf("[ResizePVIfNeeded] unable to resize PV %s of BlockDevice %s of LVMVolumeGroup %s, cmd: %s", d.Path, d.BlockDevice, lvg.Name, cmd))
					errs.WriteString(fmt.Sprintf("unable to resize PV %s, err: %s. ", d.Path, err.Error()))
					continue
				}

				r.log.Info(fmt.Sprintf("[ResizePVIfNeeded] successfully resized PV %s of BlockDevice %s of LVMVolumeGroup %s", d.Path, d.BlockDevice, lvg.Name))
			} else {
				r.log.Debug(fmt.Sprintf("[ResizePVIfNeeded] no need to resize PV %s of BlockDevice %s of the LVMVolumeGroup %s", d.Path, d.BlockDevice, lvg.Name))
			}
		}
	}

	if errs.Len() != 0 {
		return errors.New(errs.String())
	}

	return nil
}

func (r *Reconciler) extendVGIfNeeded(
	ctx context.Context,
	lvg *v1alpha1.LVMVolumeGroup,
	vg internal.VGData,
	pvs []internal.PVData,
	blockDevices map[string]v1alpha1.BlockDevice,
) error {
	for _, n := range lvg.Status.Nodes {
		for _, d := range n.Devices {
			r.log.Trace(fmt.Sprintf("[ExtendVGIfNeeded] the LVMVolumeGroup %s status block device: %s", lvg.Name, d.BlockDevice))
		}
	}

	pvsMap := make(map[string]struct{}, len(pvs))
	for _, pv := range pvs {
		pvsMap[pv.PVName] = struct{}{}
	}

	devicesToExtend := make([]string, 0, len(blockDevices))
	for _, bd := range blockDevices {
		if _, exist := pvsMap[bd.Status.Path]; !exist {
			r.log.Debug(fmt.Sprintf("[ExtendVGIfNeeded] the BlockDevice %s of LVMVolumeGroup %s Spec is not counted as used", bd.Name, lvg.Name))
			devicesToExtend = append(devicesToExtend, bd.Name)
		}
	}

	if len(devicesToExtend) == 0 {
		r.log.Debug(fmt.Sprintf("[ExtendVGIfNeeded] VG %s of the LVMVolumeGroup %s should not be extended", vg.VGName, lvg.Name))
		return nil
	}

	if isApplied(lvg) {
		err := r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonUpdating, "trying to apply the configuration")
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[ExtendVGIfNeeded] unable to add the condition %s status False reason %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, internal.ReasonUpdating, lvg.Name))
			return err
		}
	}

	r.log.Debug(fmt.Sprintf("[ExtendVGIfNeeded] VG %s should be extended as there are some BlockDevices were added to Spec field of the LVMVolumeGroup %s", vg.VGName, lvg.Name))
	paths := extractPathsFromBlockDevices(devicesToExtend, blockDevices)
	err := r.extendVGComplex(ctx, paths, vg.VGName)
	if err != nil {
		r.log.Error(err, fmt.Sprintf("[ExtendVGIfNeeded] unable to extend VG %s of the LVMVolumeGroup %s", vg.VGName, lvg.Name))
		return err
	}
	r.log.Info(fmt.Sprintf("[ExtendVGIfNeeded] VG %s of the LVMVolumeGroup %s was extended", vg.VGName, lvg.Name))

	return nil
}

// extendFileDevicesIfNeeded provisions any spec.fileDevices entries that
// are not yet part of the VG and adds their loop devices as PVs. It is the
// update-path counterpart of provisionFileDevices+createVGComplex and is
// what makes the documented "add a new fileDevices entry to grow the VG"
// flow actually take effect — without it, appending an entry would pass
// validation and silently do nothing.
//
// provisionFileDevices is idempotent (it reuses already-attached loops and
// only creates missing files), so calling it on every update is safe; only
// loop devices that are not already PVs in the VG are handed to vgextend.
func (r *Reconciler) extendFileDevicesIfNeeded(
	ctx context.Context,
	lvg *v1alpha1.LVMVolumeGroup,
	vg internal.VGData,
	pvs []internal.PVData,
) (retErr error) {
	if len(lvg.Spec.FileDevices) == 0 {
		return nil
	}

	loopPaths, provisioned, err := r.provisionFileDevices(ctx, lvg)
	if err != nil {
		return fmt.Errorf("file device provisioning failed: %w", err)
	}

	// provisionFileDevices only rolls back artifacts it created within its own
	// call; once it returns, the new loops/files survive. Unlike createVGComplex
	// (which can roll back the whole VG via cleanupFileDevices because the VG is
	// brand new), here the VG already holds healthy file devices we must NOT
	// touch. So if a later step fails (condition update, vgextend), detach and
	// remove ONLY the devices this provision call just created — they are not
	// yet PVs in the VG. Otherwise a failed extend leaks a loop device and a
	// preallocated file on the node (and orphans them entirely if the admin
	// then removes the failing spec entry). Runs on a detached context because
	// the failure is frequently the reconcile ctx being cancelled.
	defer func() {
		if retErr == nil || len(provisioned) == 0 {
			return
		}
		rollbackCtx, cancel := r.newRollbackContext()
		defer cancel()
		r.rollbackProvisionedFileDevices(rollbackCtx, provisioned)
	}()

	// Build the set of current PVs from BOTH the caller's snapshot (captured at
	// the top of reconcileLVGUpdateFunc) and a fresh cache read. A loop may have
	// become a canonical /dev/loopN PV after that snapshot (a prior reconcile
	// pvcreated it but failed before the VG was assembled, or the create-rollback
	// kept it); the stale snapshot would not list it, the exact-name check below
	// would miss it, and loopAlreadyRegisteredAsPV only inspects /dev/disk/
	// /dev/block alias-form PVs (not canonical names) — so the loop would be
	// handed to pvcreate again and fail "already a PV", wedging the condition
	// every reconcile. Taking the union never drops a known PV, so the skip
	// decision stays safe. createVGComplex re-reads GetPVs() before pvcreate for
	// the same reason.
	if freshPVs, _ := r.sdsCache.GetPVs(); len(freshPVs) > 0 {
		pvs = append(append([]internal.PVData(nil), pvs...), freshPVs...)
	}
	pvsMap := make(map[string]struct{}, len(pvs))
	for _, pv := range pvs {
		pvsMap[pv.PVName] = struct{}{}
	}

	// provisionFileDevices always returns canonical /dev/loopN names, but
	// lvm.static has no udev integration and frequently reports a managed
	// loop PV under a /dev/disk/by-id or /dev/block/MAJ:MIN alias instead
	// (the same aliasing the discoverer resolves). A literal name match
	// would therefore miss an already-attached loop PV and wrongly hand it
	// to pvcreate/vgextend again — pvcreate then fails because the device
	// is already a PV, wedging the VGConfigurationApplied condition on
	// every reconcile. Resolve alias-form PV names before deciding a loop
	// is new.
	pvsToExtend := make([]string, 0, len(loopPaths))
	skippedOnResolverFailure := false
	for _, loop := range loopPaths {
		if _, exist := pvsMap[loop]; exist {
			continue
		}
		registered, resolveFailed := r.loopAlreadyRegisteredAsPV(ctx, loop, pvs)
		if registered {
			if resolveFailed {
				skippedOnResolverFailure = true
				r.log.Warning(fmt.Sprintf("[extendFileDevicesIfNeeded] loop %s skipped because an alias PV could not be resolved; will retry on the next reconcile", loop))
			} else {
				r.log.Debug(fmt.Sprintf("[extendFileDevicesIfNeeded] loop %s is already a PV of VG %s under an alias; skipping", loop, vg.VGName))
			}
			continue
		}
		pvsToExtend = append(pvsToExtend, loop)
	}

	if len(pvsToExtend) == 0 {
		// A skip forced by a resolver failure is not a real "nothing to do":
		// the loop might genuinely not be a PV yet, but we could not confirm
		// it this round. Returning nil here would mark the configuration
		// applied and the new file device would never join the VG, silently.
		// Surface it on the condition and return an error so the reconcile
		// requeues. The rollback defer is a no-op in this case: a skipped loop
		// was already attached, so provisionFileDevices reused it and recorded
		// nothing in `provisioned`.
		if skippedOnResolverFailure {
			// Escalate once the failure looks persistent: a resolver that stays
			// broken (missing nsenter binary, a genuinely dangling alias) would
			// otherwise requeue forever under the generic "Updating" reason,
			// indistinguishable from an ordinary in-flight update. After a few
			// consecutive no-progress rounds switch to a dedicated reason and
			// log at Error level so it can be alerted on.
			streak := r.noteAliasResolveFailure(lvg.Name)
			reason := internal.ReasonUpdating
			msg := "unable to resolve alias PV names to decide whether file-backed loop devices are already part of the VG; retrying"
			if streak >= aliasResolveFailureEscalationThreshold {
				reason = internal.ReasonAliasResolutionFailed
				msg = fmt.Sprintf("unable to resolve alias PV names for %d consecutive reconciles; file devices cannot be added to VG %s until path resolution recovers (check the nsenter binary and PV aliases on the node)", streak, vg.VGName)
				r.log.Error(fmt.Errorf("alias PV resolution stuck"), fmt.Sprintf("[extendFileDevicesIfNeeded] %s (LVMVolumeGroup %s)", msg, lvg.Name))
			}
			if err := r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, reason, msg); err != nil {
				r.log.Error(err, fmt.Sprintf("[extendFileDevicesIfNeeded] unable to add the condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
			}
			return fmt.Errorf("unable to resolve alias PV names for VG %s (attempt %d); retrying", vg.VGName, streak)
		}
		r.resetAliasResolveFailure(lvg.Name)
		r.log.Debug(fmt.Sprintf("[extendFileDevicesIfNeeded] VG %s of the LVMVolumeGroup %s has no new file devices to add", vg.VGName, lvg.Name))
		return nil
	}

	// We resolved enough to make progress this round; clear any prior
	// resolver-failure streak so a transient blip does not eventually escalate.
	r.resetAliasResolveFailure(lvg.Name)

	if isApplied(lvg) {
		if err := r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonUpdating, "trying to apply the configuration"); err != nil {
			r.log.Error(err, fmt.Sprintf("[extendFileDevicesIfNeeded] unable to add the condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
			return err
		}
	}

	r.log.Info(fmt.Sprintf("[extendFileDevicesIfNeeded] VG %s of the LVMVolumeGroup %s should be extended with file devices %v", vg.VGName, lvg.Name, pvsToExtend))
	if err := r.extendVGComplex(ctx, pvsToExtend, vg.VGName); err != nil {
		r.log.Error(err, fmt.Sprintf("[extendFileDevicesIfNeeded] unable to extend VG %s of the LVMVolumeGroup %s with file devices", vg.VGName, lvg.Name))
		return err
	}
	r.log.Info(fmt.Sprintf("[extendFileDevicesIfNeeded] VG %s of the LVMVolumeGroup %s was extended with file devices", vg.VGName, lvg.Name))

	return nil
}

// noteAliasResolveFailure records one more consecutive resolver-only failed
// round for lvgName and returns the new streak length.
func (r *Reconciler) noteAliasResolveFailure(lvgName string) int {
	r.aliasResolveFailuresMu.Lock()
	defer r.aliasResolveFailuresMu.Unlock()
	r.aliasResolveFailures[lvgName]++
	return r.aliasResolveFailures[lvgName]
}

// resetAliasResolveFailure clears the resolver-failure streak for lvgName
// after a round that made progress or genuinely had nothing to do.
func (r *Reconciler) resetAliasResolveFailure(lvgName string) {
	r.aliasResolveFailuresMu.Lock()
	defer r.aliasResolveFailuresMu.Unlock()
	delete(r.aliasResolveFailures, lvgName)
}

// loopAlreadyRegisteredAsPV reports whether the canonical loop device is
// already present in pvs under an alias name (e.g. /dev/disk/by-id/... or
// /dev/block/MAJ:MIN). It only resolves alias-form PV names, so the common
// case (lvm reports the canonical /dev/loopN) costs no extra host command.
//
// It returns two booleans: registered (skip the extend for this loop) and
// resolveFailed (the skip was forced by an unresolvable alias rather than a
// confirmed match). A resolver failure is treated conservatively as a match:
// we cannot rule out that the unresolved alias IS this loop already
// registered as a PV, and handing a possibly-already-registered loop to
// pvcreate fails ("already a PV") and wedges the VGConfigurationApplied
// condition. It is safer to skip the extend this round and let the next
// reconcile retry once the resolver recovers; provisionFileDevices keeps the
// loop attached idempotently in the meantime. The caller uses resolveFailed
// to surface a retrying condition instead of silently reporting "nothing to
// do" when every loop was skipped only because resolution failed.
//
// NOTE: this is one of two places that canonicalize an alias-reported loop
// PV. Here the canonical /dev/loopN is known and alias PV names are resolved
// via readlink (r.resolver). The discoverer does the inverse in
// Discoverer.buildFileDeviceFromLoopPV (backing-file → canonical loop via
// losetup). They use different methods because their inputs differ; keep
// their ownership/aliasing assumptions in sync.
func (r *Reconciler) loopAlreadyRegisteredAsPV(ctx context.Context, loop string, pvs []internal.PVData) (registered, resolveFailed bool) {
	for _, pv := range pvs {
		if !strings.HasPrefix(pv.PVName, "/dev/disk/") && !strings.HasPrefix(pv.PVName, "/dev/block/") {
			continue
		}
		resolved, err := utils.RunWithTimeout(ctx, r.cfg.CmdDeadlineDuration, func(ctx context.Context) (string, error) {
			return r.resolver(ctx, pv.PVName)
		})
		if err != nil {
			r.log.Warning(fmt.Sprintf("[loopAlreadyRegisteredAsPV] unable to resolve canonical path for PV %s: %v; treating loop %s as already registered to avoid a duplicate pvcreate", pv.PVName, err, loop))
			resolveFailed = true
			continue
		}
		if resolved == loop {
			return true, false
		}
	}
	// No confirmed match. If at least one alias PV could not be resolved,
	// stay on the safe side and report the loop as already registered so the
	// caller skips a potentially duplicate pvcreate; the next reconcile retries.
	return resolveFailed, resolveFailed
}

// newRollbackContext returns a fresh, detached context (bounded by the
// configured command deadline when set) for file-device cleanup that must run
// even when the reconcile context is already cancelled. The failure that
// triggers a rollback is frequently the reconcile ctx being cancelled (SIGTERM,
// deadline), and exec.CommandContext refuses to start a process under an
// already-cancelled context — which would strand the loop device and backing
// file we just created. The returned cancel func is always safe to defer.
func (r *Reconciler) newRollbackContext() (context.Context, context.CancelFunc) {
	if r.cfg.CmdDeadlineDuration > 0 {
		return context.WithTimeout(context.Background(), r.cfg.CmdDeadlineDuration)
	}
	return context.Background(), func() {}
}

func (r *Reconciler) tryGetVG(vgName string) (bool, internal.VGData) {
	vgs, _ := r.sdsCache.GetVGs()
	for _, vg := range vgs {
		if vg.VGName == vgName {
			return true, vg
		}
	}

	return false, internal.VGData{}
}

func (r *Reconciler) removeLVGFinalizerIfExist(ctx context.Context, lvg *v1alpha1.LVMVolumeGroup) (bool, error) {
	if !slices.Contains(lvg.Finalizers, internal.SdsNodeConfiguratorFinalizer) {
		return false, nil
	}

	for i := range lvg.Finalizers {
		if lvg.Finalizers[i] == internal.SdsNodeConfiguratorFinalizer {
			lvg.Finalizers = append(lvg.Finalizers[:i], lvg.Finalizers[i+1:]...)
			break
		}
	}

	err := r.cl.Update(ctx, lvg)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (r *Reconciler) getLVForVG(vgName string) []string {
	lvs, _ := r.sdsCache.GetLVs()
	usedLVs := make([]string, 0, len(lvs))
	for _, lv := range lvs {
		if lv.VGName == vgName {
			usedLVs = append(usedLVs, lv.LVName)
		}
	}

	return usedLVs
}

func (r *Reconciler) deleteVGIfExist(vgName string) error {
	vgs, _ := r.sdsCache.GetVGs()
	if !checkIfVGExist(vgName, vgs) {
		r.log.Debug(fmt.Sprintf("[DeleteVGIfExist] no VG %s found, nothing to delete", vgName))
		return nil
	}

	pvs, _ := r.sdsCache.GetPVs()
	if len(pvs) == 0 {
		err := errors.New("no any PV found")
		r.log.Error(err, fmt.Sprintf("[DeleteVGIfExist] no any PV was found while deleting VG %s", vgName))
		return err
	}

	start := time.Now()
	command, err := r.commands.RemoveVG(vgName)
	r.metrics.UtilsCommandsDuration(ReconcilerName, "vgremove").Observe(r.metrics.GetEstimatedTimeInSeconds(start))
	r.metrics.UtilsCommandsExecutionCount(ReconcilerName, "vgremove").Inc()
	r.log.Debug(command)
	if err != nil {
		r.metrics.UtilsCommandsErrorsCount(ReconcilerName, "vgremove").Inc()
		r.log.Error(err, "RemoveVG "+command)
		return err
	}
	r.log.Debug(fmt.Sprintf("[DeleteVGIfExist] VG %s was successfully deleted from the node", vgName))
	var pvsToRemove []string
	for _, pv := range pvs {
		if pv.VGName == vgName {
			pvsToRemove = append(pvsToRemove, pv.PVName)
		}
	}

	start = time.Now()
	command, err = r.commands.RemovePV(pvsToRemove)
	r.metrics.UtilsCommandsDuration(ReconcilerName, "pvremove").Observe(r.metrics.GetEstimatedTimeInSeconds(start))
	r.metrics.UtilsCommandsExecutionCount(ReconcilerName, "pvremove").Inc()
	r.log.Debug(command)
	if err != nil {
		r.metrics.UtilsCommandsErrorsCount(ReconcilerName, "pvremove").Inc()
		r.log.Error(err, "RemovePV "+command)
		return err
	}
	r.log.Debug(fmt.Sprintf("[DeleteVGIfExist] successfully delete PVs of VG %s from the node", vgName))

	return nil
}

func (r *Reconciler) extendVGComplex(ctx context.Context, extendPVs []string, vgName string) error {
	for _, pvPath := range extendPVs {
		start := time.Now()
		command, err := r.commands.CreatePV(pvPath)
		r.metrics.UtilsCommandsDuration(ReconcilerName, "pvcreate").Observe(r.metrics.GetEstimatedTimeInSeconds(start))
		r.metrics.UtilsCommandsExecutionCount(ReconcilerName, "pvcreate").Inc()
		r.log.Debug(command)
		if err != nil {
			r.metrics.UtilsCommandsErrorsCount(ReconcilerName, "pvcreate").Inc()
			r.log.Error(err, "CreatePV ")
			return err
		}
	}

	start := time.Now()
	command, err := r.commands.ExtendVG(vgName, extendPVs)
	r.metrics.UtilsCommandsDuration(ReconcilerName, "vgextend").Observe(r.metrics.GetEstimatedTimeInSeconds(start))
	r.metrics.UtilsCommandsExecutionCount(ReconcilerName, "vgextend").Inc()
	r.log.Debug(command)
	if err != nil {
		r.metrics.UtilsCommandsErrorsCount(ReconcilerName, "vgextend").Inc()
		r.log.Error(err, "ExtendVG ")
		return err
	}

	r.triggerUdevForPaths(ctx, extendPVs)

	return nil
}

// triggerUdevForPaths sends a "change" uevent for the given device paths so that
// the host udev re-probes them. lvm.static is built without udev integration, so
// after pvcreate/vgcreate the udev DB stays stale and lsblk never reports
// LVM2_member as fstype — which blocks the BD discoverer from linking the device
// to its VG.
//
// The call is best-effort: a failure is logged as a warning and never propagates
// to the caller. Operators observe the call frequency and error rate through
// the UtilsCommands* metrics with cmd label "udevadm-trigger".
//
// The timeout is bounded by a child context derived from the caller's context,
// so a SIGTERM from kubelet (or any cancellation of the reconcile loop) is
// honoured immediately instead of being absorbed by a detached background ctx.
func (r *Reconciler) triggerUdevForPaths(parent context.Context, paths []string) {
	if len(paths) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(parent, udevadmTriggerTimeout)
	defer cancel()

	start := time.Now()
	cmd, err := r.commands.UdevadmTrigger(ctx, paths)
	r.metrics.UtilsCommandsDuration(ReconcilerName, udevadmTriggerCmdLabel).Observe(r.metrics.GetEstimatedTimeInSeconds(start))
	r.metrics.UtilsCommandsExecutionCount(ReconcilerName, udevadmTriggerCmdLabel).Inc()
	r.log.Debug(cmd)
	if err != nil {
		r.metrics.UtilsCommandsErrorsCount(ReconcilerName, udevadmTriggerCmdLabel).Inc()
		r.log.Warning(fmt.Sprintf("[triggerUdevForPaths] udevadm trigger failed for %v (non-fatal): %v, cmd: %s", paths, err, cmd))
	}
}

// provisionFileDevices creates one preallocated backing file per
// spec.fileDevices entry and attaches each as a loop device. It is
// idempotent across reconcile retries: if a loop device is already
// attached to the target file, it reuses that loop device instead of
// creating a fresh one (`losetup --find --show` would otherwise hand
// out a new minor on every call, slowly leaking up to the system-wide
// loop limit).
//
// On any error mid-way the function rolls back everything it created
// in *this* invocation — detaches loop devices it just attached and
// removes files it just created — so a transient failure does not
// leave dangling files in the data directory. Loop devices and files
// that pre-existed (e.g. left behind by an earlier successful step)
// are preserved on purpose: they will be picked up by the next
// reconcile as "already attached".
// provisionedFileDevice records a backing file + loop device that
// provisionFileDevices newly created (fallocate + losetup) within a single
// call. Reused, already-attached devices are NOT included, so a caller can
// roll back exactly what this call added if a later step fails, without
// touching pre-existing healthy file devices of the same LVG.
type provisionedFileDevice struct {
	filePath string
	loopDev  string
}

func (r *Reconciler) provisionFileDevices(ctx context.Context, lvg *v1alpha1.LVMVolumeGroup) (loopPaths []string, provisioned []provisionedFileDevice, retErr error) {
	if len(lvg.Spec.FileDevices) == 0 {
		return nil, nil, nil
	}

	// Track resources we (and only we) just created so we can undo them
	// on failure. We deliberately do NOT roll back pre-existing artifacts.
	type rollback struct {
		filePath       string
		createdFile    bool
		loopDev        string
		attachedLoopOK bool
	}
	created := make([]rollback, 0, len(lvg.Spec.FileDevices))
	defer func() {
		if retErr == nil {
			return
		}
		rollbackCtx, cancel := r.newRollbackContext()
		defer cancel()
		for i := len(created) - 1; i >= 0; i-- {
			rb := created[i]
			if rb.attachedLoopOK && rb.loopDev != "" {
				if cmd, err := r.commands.DetachLoopDevice(rollbackCtx, rb.loopDev); err != nil {
					r.log.Warning(fmt.Sprintf("[provisionFileDevices][rollback] unable to detach %s: %v (cmd: %s)", rb.loopDev, err, cmd))
				}
			}
			if rb.createdFile && rb.filePath != "" {
				if cmd, err := r.commands.RemoveFileDevice(rollbackCtx, rb.filePath); err != nil {
					r.log.Warning(fmt.Sprintf("[provisionFileDevices][rollback] unable to remove %s: %v (cmd: %s)", rb.filePath, err, cmd))
				}
			}
		}
	}()

	loopPaths = make([]string, 0, len(lvg.Spec.FileDevices))
	provisioned = make([]provisionedFileDevice, 0, len(lvg.Spec.FileDevices))
	seenLoops := make(map[string]struct{}, len(lvg.Spec.FileDevices))
	for _, fd := range lvg.Spec.FileDevices {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}

		filePath := utils.BuildFileDevicePath(fd.Directory, lvg.Name, fd.Size)
		sizeBytes := fd.Size.Value()

		type findResult struct {
			cmd     string
			loopDev string
		}
		findRes, err := utils.RunWithTimeout(ctx, r.cfg.CmdDeadlineDuration, func(ctx context.Context) (findResult, error) {
			cmd, existing, err := r.commands.FindLoopDeviceByFile(ctx, filePath)
			return findResult{cmd: cmd, loopDev: existing}, err
		})
		r.log.Debug(findRes.cmd)
		if err != nil {
			return nil, nil, fmt.Errorf("query loop for %s: %w", filePath, err)
		}
		if findRes.loopDev != "" {
			r.log.Info(fmt.Sprintf("[provisionFileDevices] %s already attached to %s; reusing", filePath, findRes.loopDev))
			if _, ok := seenLoops[findRes.loopDev]; !ok {
				seenLoops[findRes.loopDev] = struct{}{}
				loopPaths = append(loopPaths, findRes.loopDev)
			}
			continue
		}

		r.log.Info(fmt.Sprintf("[provisionFileDevices] creating file device %s (%d bytes) for LVMVolumeGroup %s", filePath, sizeBytes, lvg.Name))

		// Create the backing directory on demand (mkdir -p, idempotent) so
		// the admin does not have to pre-create it on every node. A failure
		// here (read-only FS, a non-directory in the path) aborts the
		// provision and is reported on the VGConfigurationApplied condition.
		mkdirCmd, err := utils.RunWithTimeout(ctx, r.cfg.CmdDeadlineDuration, func(ctx context.Context) (string, error) {
			return r.commands.EnsureFileDeviceDirectory(ctx, fd.Directory)
		})
		r.log.Debug(mkdirCmd)
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[provisionFileDevices] unable to create directory %s", fd.Directory))
			return nil, nil, err
		}

		// Refuse to allocate a backing file larger than the free space of the
		// node's filesystem. `fallocate -l` preallocates the full size, so
		// without this guard a single oversized fileDevices entry (or a typo in
		// `directory`/`size`) can fill the node's root filesystem and push
		// kubelet into DiskPressure eviction — a node-level outage, not a mere
		// condition error. The check is best-effort: if we cannot determine the
		// free space we log and fall through to fallocate, which still fails
		// cleanly on a genuine ENOSPC.
		availableBytes, err := utils.RunWithTimeout(ctx, r.cfg.CmdDeadlineDuration, func(ctx context.Context) (int64, error) {
			cmd, available, err := r.commands.GetAvailableBytes(ctx, fd.Directory)
			r.log.Debug(cmd)
			return available, err
		})
		if err != nil {
			r.log.Warning(fmt.Sprintf("[provisionFileDevices] unable to check free space in %s, proceeding (fallocate will still fail on ENOSPC): %v", fd.Directory, err))
		} else if availableBytes < sizeBytes {
			return nil, nil, fmt.Errorf(
				"not enough free space in %q to create backing file %s: %d bytes available, %d bytes requested",
				fd.Directory, filePath, availableBytes, sizeBytes,
			)
		}

		rb := rollback{filePath: filePath}
		createCmd, err := utils.RunWithTimeout(ctx, r.cfg.CmdDeadlineDuration, func(ctx context.Context) (string, error) {
			return r.commands.CreateFileDevice(ctx, filePath, sizeBytes)
		})
		r.log.Debug(createCmd)
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[provisionFileDevices] unable to create file %s", filePath))
			created = append(created, rb)
			return nil, nil, err
		}
		rb.createdFile = true

		type setupResult struct {
			cmd     string
			loopDev string
		}
		setupRes, err := utils.RunWithTimeout(ctx, r.cfg.CmdDeadlineDuration, func(ctx context.Context) (setupResult, error) {
			cmd, loopDev, err := r.commands.SetupLoopDevice(ctx, filePath)
			return setupResult{cmd: cmd, loopDev: loopDev}, err
		})
		r.log.Debug(setupRes.cmd)
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[provisionFileDevices] unable to setup loop device for %s", filePath))
			created = append(created, rb)
			return nil, nil, err
		}
		rb.loopDev = setupRes.loopDev
		rb.attachedLoopOK = true
		created = append(created, rb)
		provisioned = append(provisioned, provisionedFileDevice{filePath: filePath, loopDev: setupRes.loopDev})

		r.log.Info(fmt.Sprintf("[provisionFileDevices] file %s attached to %s", filePath, setupRes.loopDev))
		if _, ok := seenLoops[setupRes.loopDev]; !ok {
			seenLoops[setupRes.loopDev] = struct{}{}
			loopPaths = append(loopPaths, setupRes.loopDev)
		}
	}
	return loopPaths, provisioned, nil
}

// rollbackProvisionedFileDevices tears down ONLY the file devices this reconcile
// just provisioned (created a fresh backing file and attached a fresh loop),
// after a later step (pvcreate/vgcreate/vgextend/condition update) failed.
//
// It is the create/extend-path counterpart to cleanupFileDevices and MUST be
// used instead of it on those paths: cleanupFileDevices walks spec+status and
// would remove the backing file of a loop that another, concurrent reconcile —
// or a pvcreate/vgcreate that materially succeeded but returned a non-zero
// status — has already turned into a live PV of the VG. Removing such a file
// leaves a PV backed by a deleted file while the VG keeps using it; the next
// reconcile then re-provisions a second loop and the VG silently doubles in
// size (observed on real clusters as one backing file attached to two loops,
// one shown "(deleted)").
//
// As a hard safety net it lists the current PVs once, authoritatively (a fresh
// `lvm pvs`, not the possibly-stale cache, because the result gates a
// destructive teardown), and SKIPS any provisioned loop that is already an LVM
// PV (matched canonically or via the /dev/disk//dev/block alias the discoverer
// also resolves). If the PV listing fails it tears nothing down — a leaked
// loop/file is recoverable (the next reconcile reuses it via
// FindLoopDeviceByFile), corrupting a live VG is not — and it never removes a
// backing file whose loop it could not detach, since the loop may still
// reference it.
func (r *Reconciler) rollbackProvisionedFileDevices(ctx context.Context, provisioned []provisionedFileDevice) {
	if len(provisioned) == 0 {
		return
	}

	pvs, cmd, _, err := r.commands.GetAllPVs(ctx)
	r.log.Debug(cmd)
	if err != nil {
		r.log.Warning(fmt.Sprintf("[rollbackProvisionedFileDevices] unable to list PVs to confirm rollback is safe; leaving %d provisioned file device(s) in place (a leak is recoverable, corrupting a live VG is not): %v", len(provisioned), err))
		return
	}
	pvNames := make(map[string]struct{}, len(pvs))
	for _, pv := range pvs {
		pvNames[pv.PVName] = struct{}{}
	}

	for i := len(provisioned) - 1; i >= 0; i-- {
		p := provisioned[i]
		if p.loopDev != "" {
			_, isPV := pvNames[p.loopDev]
			if !isPV {
				// lvm.static without udev may report the loop PV under a
				// /dev/disk or /dev/block alias instead of /dev/loopN.
				if registered, resolveFailed := r.loopAlreadyRegisteredAsPV(ctx, p.loopDev, pvs); registered || resolveFailed {
					isPV = true
				}
			}
			if isPV {
				r.log.Warning(fmt.Sprintf("[rollbackProvisionedFileDevices] loop %s (file %s) is already an LVM PV; skipping rollback so a concurrent or partially-succeeded create does not lose its backing storage", p.loopDev, p.filePath))
				continue
			}
			if cmd, derr := r.commands.DetachLoopDevice(ctx, p.loopDev); derr != nil {
				r.log.Warning(fmt.Sprintf("[rollbackProvisionedFileDevices] unable to detach %s: %v (cmd: %s); keeping backing file %s in place", p.loopDev, derr, cmd, p.filePath))
				continue
			}
		}
		if p.filePath != "" {
			if cmd, rerr := r.commands.RemoveFileDevice(ctx, p.filePath); rerr != nil {
				r.log.Warning(fmt.Sprintf("[rollbackProvisionedFileDevices] unable to remove %s: %v (cmd: %s)", p.filePath, rerr, cmd))
			}
		}
	}
}

// cleanupFileDevices detaches loop devices and removes backing files
// recorded for this LVG. It walks the union of status.nodes[].fileDevices
// (what the discoverer last observed) and spec.fileDevices (what the
// user asked for), so it cannot leak files that were created but never
// reflected in status — e.g. when the agent crashed mid-provision.
//
// `rm` errors are logged as warnings and do not abort the cleanup
// (a stale ENOENT after manual cleanup is harmless), but every
// `losetup -d` failure is reported back as an error: a busy loop
// device means there is still a live reference to the file (a mount,
// an LV, a sidecar) and removing the LVG resource at this point would
// strand state on the node.
func (r *Reconciler) cleanupFileDevices(ctx context.Context, lvg *v1alpha1.LVMVolumeGroup) error {
	type target struct {
		filePath   string
		loopDevice string
	}
	seen := make(map[string]target)
	add := func(t target) {
		if t.filePath == "" && t.loopDevice == "" {
			return
		}
		key := t.filePath
		if key == "" {
			key = "loop:" + t.loopDevice
		}
		// Prefer the entry that carries both fields.
		if prev, ok := seen[key]; ok {
			if prev.loopDevice == "" && t.loopDevice != "" {
				prev.loopDevice = t.loopDevice
				seen[key] = prev
			}
			return
		}
		seen[key] = t
	}
	for _, node := range lvg.Status.Nodes {
		for _, fd := range node.FileDevices {
			add(target{filePath: fd.FilePath, loopDevice: fd.LoopDevice})
		}
	}
	for _, fd := range lvg.Spec.FileDevices {
		add(target{filePath: utils.BuildFileDevicePath(fd.Directory, lvg.Name, fd.Size)})
	}

	keys := make([]string, 0, len(seen))
	for k := range seen {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var detachErrs []error
	for _, key := range keys {
		t := seen[key]
		// Defense in depth: never act on a path whose basename does not
		// match the agent's managed pattern. If it slipped into status
		// via a foreign loop PV (or a bug), refuse to rm and warn.
		if t.filePath != "" && !utils.IsManagedFileDevicePath(t.filePath, lvg.Name) {
			r.log.Warning(fmt.Sprintf("[cleanupFileDevices] refusing to act on unmanaged path %q for LVG %s", t.filePath, lvg.Name))
			continue
		}

		// The loop minor backing a file is NOT stable: after a reboot
		// ReattachFileDevices re-attaches via `losetup --find` and may pick a
		// different minor, and the kernel can later hand a freed minor to an
		// unrelated file. So the loopDevice recorded in status can be stale or
		// even point at a foreign device. Whenever we know the backing file,
		// re-resolve the loop from it and never detach a device we have not
		// just confirmed backs THIS file.
		if t.filePath != "" {
			loop, err := utils.RunWithTimeout(ctx, r.cfg.CmdDeadlineDuration, func(ctx context.Context) (string, error) {
				cmd, loop, err := r.commands.FindLoopDeviceByFile(ctx, t.filePath)
				r.log.Debug(cmd)
				return loop, err
			})
			if err != nil {
				detachErrs = append(detachErrs, fmt.Errorf("query loop for %s: %w", t.filePath, err))
				r.log.Error(err, fmt.Sprintf("[cleanupFileDevices] unable to query loop for %s", t.filePath))
				continue
			}
			t.loopDevice = loop
		} else if t.loopDevice != "" {
			// A loop-only target (no backing-file path known) must be
			// confirmed managed before we touch it: read its backing file and
			// refuse unless the basename matches our owner pattern, so a
			// stale/foreign minor recorded in status is never detached.
			backing, err := utils.RunWithTimeout(ctx, r.cfg.CmdDeadlineDuration, func(ctx context.Context) (string, error) {
				cmd, backing, err := r.commands.GetLoopBackingFile(ctx, t.loopDevice)
				r.log.Debug(cmd)
				return backing, err
			})
			if err != nil {
				detachErrs = append(detachErrs, fmt.Errorf("read backing file for %s: %w", t.loopDevice, err))
				r.log.Error(err, fmt.Sprintf("[cleanupFileDevices] unable to read backing file for loop %s", t.loopDevice))
				continue
			}
			if !utils.IsManagedFileDevicePath(backing, lvg.Name) {
				r.log.Warning(fmt.Sprintf("[cleanupFileDevices] refusing to detach loop %s backed by unmanaged file %q for LVG %s", t.loopDevice, backing, lvg.Name))
				continue
			}
		}

		if t.loopDevice != "" {
			cmd, err := utils.RunWithTimeout(ctx, r.cfg.CmdDeadlineDuration, func(ctx context.Context) (string, error) {
				return r.commands.DetachLoopDevice(ctx, t.loopDevice)
			})
			r.log.Debug(cmd)
			if err != nil {
				detachErrs = append(detachErrs, fmt.Errorf("detach %s: %w", t.loopDevice, err))
				r.log.Error(err, fmt.Sprintf("[cleanupFileDevices] unable to detach loop %s", t.loopDevice))
				continue
			}
		}
		if t.filePath != "" {
			cmd, err := utils.RunWithTimeout(ctx, r.cfg.CmdDeadlineDuration, func(ctx context.Context) (string, error) {
				return r.commands.RemoveFileDevice(ctx, t.filePath)
			})
			r.log.Debug(cmd)
			if err != nil {
				r.log.Warning(fmt.Sprintf("[cleanupFileDevices] unable to remove file %s: %v", t.filePath, err))
			}
		}
	}
	if len(detachErrs) > 0 {
		return errors.Join(detachErrs...)
	}
	return nil
}

func (r *Reconciler) createVGComplex(ctx context.Context, lvg *v1alpha1.LVMVolumeGroup, blockDevices map[string]v1alpha1.BlockDevice) (retErr error) {
	paths := extractPathsFromBlockDevices(nil, blockDevices)

	loopPaths, provisioned, err := r.provisionFileDevices(ctx, lvg)
	if err != nil {
		return fmt.Errorf("file device provisioning failed: %w", err)
	}
	paths = append(paths, loopPaths...)

	// If a later step (pvcreate/vgcreate) fails, tear down ONLY the file devices
	// this call provisioned — and never one that already became a PV. This must
	// NOT use the broad cleanupFileDevices (the delete-path cleanup): it walks
	// spec+status and could remove the backing file of a loop that a concurrent
	// reconcile, or a pvcreate/vgcreate that materially succeeded but returned a
	// non-zero status, had already turned into a live PV of the VG — which the
	// next reconcile then re-provisions with a second loop, doubling the VG.
	// See rollbackProvisionedFileDevices. Runs on a detached context because the
	// failure is frequently the reconcile ctx being cancelled.
	if len(provisioned) > 0 {
		defer func() {
			if retErr == nil {
				return
			}
			rollbackCtx, cancel := r.newRollbackContext()
			defer cancel()
			r.rollbackProvisionedFileDevices(rollbackCtx, provisioned)
		}()
	}

	r.log.Trace(fmt.Sprintf("[CreateVGComplex] LVMVolumeGroup %s devices paths %v", lvg.Name, paths))

	// Skip pvcreate for any device that is already an LVM PV. This guards a
	// retry after a create that pvcreated a device but failed before vgcreate
	// (SIGTERM, ctx cancel, a rollback whose detach failed): provisionFileDevices
	// reuses the still-attached loop, and handing an already-PV device to
	// pvcreate fails ("already a PV") and wedges the VGConfigurationApplied
	// condition. The vgcreate below still consumes an existing PV, so skipping
	// the redundant pvcreate is safe. Mirrors the already-a-PV guard in
	// extendFileDevicesIfNeeded; loop PVs reported under an alias are resolved
	// via loopAlreadyRegisteredAsPV.
	existingPVs, _ := r.sdsCache.GetPVs()
	existingPVNames := make(map[string]struct{}, len(existingPVs))
	for _, pv := range existingPVs {
		existingPVNames[pv.PVName] = struct{}{}
	}

	for _, path := range paths {
		if _, ok := existingPVNames[path]; ok {
			r.log.Info(fmt.Sprintf("[CreateVGComplex] %s is already a PV; skipping pvcreate", path))
			continue
		}
		if strings.HasPrefix(path, "/dev/loop") {
			if registered, _ := r.loopAlreadyRegisteredAsPV(ctx, path, existingPVs); registered {
				r.log.Info(fmt.Sprintf("[CreateVGComplex] loop %s is already a PV (under an alias); skipping pvcreate", path))
				continue
			}
		}

		start := time.Now()
		command, err := r.commands.CreatePV(path)
		r.metrics.UtilsCommandsDuration(ReconcilerName, "pvcreate").Observe(r.metrics.GetEstimatedTimeInSeconds(start))
		r.metrics.UtilsCommandsExecutionCount(ReconcilerName, "pvcreate").Inc()
		r.log.Debug(command)
		if err != nil {
			r.metrics.UtilsCommandsErrorsCount(ReconcilerName, "pvcreate").Inc()
			r.log.Error(err, fmt.Sprintf("[CreateVGComplex] unable to create PV by path %s", path))
			return err
		}
	}

	r.log.Debug(fmt.Sprintf("[CreateVGComplex] successfully created all PVs for the LVMVolumeGroup %s", lvg.Name))
	r.log.Debug(fmt.Sprintf("[CreateVGComplex] the LVMVolumeGroup %s type is %s", lvg.Name, lvg.Spec.Type))
	switch lvg.Spec.Type {
	case internal.Local:
		start := time.Now()
		cmd, err := r.commands.CreateVGLocal(lvg.Spec.ActualVGNameOnTheNode, lvg.Name, paths)
		r.metrics.UtilsCommandsDuration(ReconcilerName, "vgcreate").Observe(r.metrics.GetEstimatedTimeInSeconds(start))
		r.metrics.UtilsCommandsExecutionCount(ReconcilerName, "vgcreate").Inc()
		r.log.Debug(cmd)
		if err != nil {
			r.metrics.UtilsCommandsErrorsCount(ReconcilerName, "vgcreate").Inc()
			r.log.Error(err, "error CreateVGLocal")
			return err
		}
	case internal.Shared:
		start := time.Now()
		cmd, err := r.commands.CreateVGShared(lvg.Spec.ActualVGNameOnTheNode, lvg.Name, paths)
		r.metrics.UtilsCommandsDuration(ReconcilerName, "vgcreate").Observe(r.metrics.GetEstimatedTimeInSeconds(start))
		r.metrics.UtilsCommandsExecutionCount(ReconcilerName, "vgcreate").Inc()
		r.log.Debug(cmd)
		if err != nil {
			r.metrics.UtilsCommandsErrorsCount(ReconcilerName, "vgcreate").Inc()
			r.log.Error(err, "error CreateVGShared")
			return err
		}
	}

	r.log.Debug(fmt.Sprintf("[CreateVGComplex] successfully create VG %s of the LVMVolumeGroup %s", lvg.Spec.ActualVGNameOnTheNode, lvg.Name))

	r.triggerUdevForPaths(ctx, paths)

	return nil
}

func (r *Reconciler) updateVGTagIfNeeded(
	ctx context.Context,
	lvg *v1alpha1.LVMVolumeGroup,
	vg internal.VGData,
) (bool, error) {
	found, tagName := utils.ReadValueFromTags(vg.VGTags, internal.LVMVolumeGroupTag)
	if found && lvg.Name != tagName {
		if isApplied(lvg) {
			err := r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonUpdating, "trying to apply the configuration")
			if err != nil {
				r.log.Error(err, fmt.Sprintf("[UpdateVGTagIfNeeded] unable to add the condition %s status False reason %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, internal.ReasonUpdating, lvg.Name))
				return false, err
			}
		}

		start := time.Now()
		cmd, err := r.commands.VGChangeDelTag(ctx, vg.VGName, fmt.Sprintf("%s=%s", internal.LVMVolumeGroupTag, tagName))
		r.metrics.UtilsCommandsDuration(ReconcilerName, "vgchange").Observe(r.metrics.GetEstimatedTimeInSeconds(start))
		r.metrics.UtilsCommandsExecutionCount(ReconcilerName, "vgchange").Inc()
		r.log.Debug(fmt.Sprintf("[UpdateVGTagIfNeeded] exec cmd: %s", cmd))
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[UpdateVGTagIfNeeded] unable to delete LVMVolumeGroupTag: %s=%s, vg: %s", internal.LVMVolumeGroupTag, tagName, vg.VGName))
			r.metrics.UtilsCommandsErrorsCount(ReconcilerName, "vgchange").Inc()
			return false, err
		}

		start = time.Now()
		cmd, err = r.commands.VGChangeAddTag(ctx, vg.VGName, fmt.Sprintf("%s=%s", internal.LVMVolumeGroupTag, lvg.Name))
		r.metrics.UtilsCommandsDuration(ReconcilerName, "vgchange").Observe(r.metrics.GetEstimatedTimeInSeconds(start))
		r.metrics.UtilsCommandsExecutionCount(ReconcilerName, "vgchange").Inc()
		r.log.Debug(fmt.Sprintf("[UpdateVGTagIfNeeded] exec cmd: %s", cmd))
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[UpdateVGTagIfNeeded] unable to add LVMVolumeGroupTag: %s=%s, vg: %s", internal.LVMVolumeGroupTag, lvg.Name, vg.VGName))
			r.metrics.UtilsCommandsErrorsCount(ReconcilerName, "vgchange").Inc()
			return false, err
		}

		return true, nil
	}

	return false, nil
}

func (r *Reconciler) extendThinPool(lvg *v1alpha1.LVMVolumeGroup, specThinPool v1alpha1.LVMVolumeGroupThinPoolSpec) error {
	volumeGroupFreeSpaceBytes := lvg.Status.VGSize.Value() - lvg.Status.AllocatedSize.Value()
	tpRequestedSize, err := utils.GetRequestedSizeFromString(specThinPool.Size, lvg.Status.VGSize)
	if err != nil {
		return err
	}

	r.log.Trace(fmt.Sprintf("[ExtendThinPool] volumeGroupSize = %s", lvg.Status.VGSize.String()))
	r.log.Trace(fmt.Sprintf("[ExtendThinPool] volumeGroupAllocatedSize = %s", lvg.Status.AllocatedSize.String()))
	r.log.Trace(fmt.Sprintf("[ExtendThinPool] volumeGroupFreeSpaceBytes = %d", volumeGroupFreeSpaceBytes))

	r.log.Info(fmt.Sprintf("[ExtendThinPool] start resizing thin pool: %s; with new size: %s", specThinPool.Name, tpRequestedSize.String()))

	var cmd string
	start := time.Now()
	vg := r.sdsCache.FindVG(lvg.Spec.ActualVGNameOnTheNode)
	alignedTpSize, alignErr := utils.AlignSizeToExtent(tpRequestedSize, extentSizeForThinPoolAlign(lvg, vg))
	if alignErr != nil {
		r.log.Error(alignErr, fmt.Sprintf("[ExtendThinPool] unable to align thin-pool %s size for LVMVolumeGroup %s", specThinPool.Name, lvg.Name))
		return alignErr
	}
	if alignedTpSize.Value() >= lvg.Status.VGSize.Value() {
		r.log.Debug(fmt.Sprintf("[ExtendThinPool] thin-pool %s of the LVMVolumeGroup %s will be extend to size 100VG", specThinPool.Name, lvg.Name))
		cmd, err = r.commands.ExtendLVFullVGSpace(lvg.Spec.ActualVGNameOnTheNode, specThinPool.Name)
	} else {
		r.log.Debug(fmt.Sprintf("[ExtendThinPool] thin-pool %s of the LVMVolumeGroup %s will be extend to size %s", specThinPool.Name, lvg.Name, tpRequestedSize.String()))
		cmd, err = r.commands.ExtendLV(tpRequestedSize.Value(), lvg.Spec.ActualVGNameOnTheNode, specThinPool.Name)
	}
	r.metrics.UtilsCommandsDuration(ReconcilerName, "lvextend").Observe(r.metrics.GetEstimatedTimeInSeconds(start))
	r.metrics.UtilsCommandsExecutionCount(ReconcilerName, "lvextend").Inc()
	if err != nil {
		r.metrics.UtilsCommandsErrorsCount(ReconcilerName, "lvextend").Inc()
		r.log.Error(err, fmt.Sprintf("[ExtendThinPool] unable to extend LV, name: %s, cmd: %s", specThinPool.Name, cmd))
		return err
	}

	return nil
}

func (r *Reconciler) addLVGLabelIfNeeded(ctx context.Context, lvg *v1alpha1.LVMVolumeGroup, labelKey, labelValue string) (bool, error) {
	if !r.shouldUpdateLVGLabels(lvg, labelKey, labelValue) {
		return false, nil
	}

	if lvg.Labels == nil {
		lvg.Labels = make(map[string]string)
	}

	lvg.Labels[labelKey] = labelValue
	err := r.cl.Update(ctx, lvg)
	if err != nil {
		return false, err
	}

	return true, nil
}

func checkIfVGExist(vgName string, vgs []internal.VGData) bool {
	for _, vg := range vgs {
		if vg.VGName == vgName {
			return true
		}
	}

	return false
}

func validateSpecBlockDevices(lvg *v1alpha1.LVMVolumeGroup, blockDevices map[string]v1alpha1.BlockDevice) (bool, string) {
	if len(blockDevices) == 0 {
		return false, "none of specified BlockDevices were found"
	}

	if len(lvg.Status.Nodes) > 0 {
		lostBdNames := make([]string, 0, len(lvg.Status.Nodes[0].Devices))
		for _, n := range lvg.Status.Nodes {
			for _, d := range n.Devices {
				if _, found := blockDevices[d.BlockDevice]; !found {
					lostBdNames = append(lostBdNames, d.BlockDevice)
				}
			}
		}

		// that means some of the used BlockDevices no longer match the blockDeviceSelector
		if len(lostBdNames) > 0 {
			return false, fmt.Sprintf("these BlockDevices no longer match the blockDeviceSelector: %s", strings.Join(lostBdNames, ","))
		}
	}

	// A file-only LVMVolumeGroup has no blockDeviceSelector; there are no
	// match expressions to validate, and dereferencing the nil selector
	// would panic. (The production caller already skips this function for
	// file-only groups; this guard keeps it safe if called directly.)
	if lvg.Spec.BlockDeviceSelector == nil {
		return true, ""
	}

	for _, me := range lvg.Spec.BlockDeviceSelector.MatchExpressions {
		if me.Key == internal.MetadataNameLabelKey && me.Operator == v1.LabelSelectorOpIn {
			if len(me.Values) != len(blockDevices) {
				missedBds := make([]string, 0, len(me.Values))
				for _, bdName := range me.Values {
					if _, exist := blockDevices[bdName]; !exist {
						missedBds = append(missedBds, bdName)
					}
				}

				return false, fmt.Sprintf("unable to find specified BlockDevices: %s", strings.Join(missedBds, ","))
			}
		}
	}

	return true, ""
}

func filterBlockDevicesByNodeName(blockDevices map[string]v1alpha1.BlockDevice, nodeName string) map[string]v1alpha1.BlockDevice {
	bdsForUsage := make(map[string]v1alpha1.BlockDevice, len(blockDevices))
	for _, bd := range blockDevices {
		if bd.Status.NodeName == nodeName {
			bdsForUsage[bd.Name] = bd
		}
	}

	return bdsForUsage
}

func checkIfLVGBelongsToNode(lvg *v1alpha1.LVMVolumeGroup, nodeName string) bool {
	return lvg.Spec.Local.NodeName == nodeName
}

func extractPathsFromBlockDevices(targetDevices []string, blockDevices map[string]v1alpha1.BlockDevice) []string {
	var paths []string
	if len(targetDevices) > 0 {
		paths = make([]string, 0, len(targetDevices))
		for _, bdName := range targetDevices {
			bd := blockDevices[bdName]
			paths = append(paths, bd.Status.Path)
		}
	} else {
		paths = make([]string, 0, len(blockDevices))
		for _, bd := range blockDevices {
			paths = append(paths, bd.Status.Path)
		}
	}

	return paths
}

func countVGSizeByBlockDevices(blockDevices map[string]v1alpha1.BlockDevice) resource.Quantity {
	var totalVGSize int64
	for _, bd := range blockDevices {
		totalVGSize += bd.Status.Size.Value()
	}
	return *resource.NewQuantity(totalVGSize, resource.BinarySI)
}

// countVGSizeByFileDevices sums the capacity contributed by spec.fileDevices.
// File-backed PVs are part of the VG just like block devices, so thin-pool
// sizing on the create path must include them; otherwise a file-only VG is
// treated as zero-sized.
func countVGSizeByFileDevices(lvg *v1alpha1.LVMVolumeGroup) resource.Quantity {
	var totalSize int64
	for _, fd := range lvg.Spec.FileDevices {
		totalSize += fd.Size.Value()
	}
	return *resource.NewQuantity(totalSize, resource.BinarySI)
}
