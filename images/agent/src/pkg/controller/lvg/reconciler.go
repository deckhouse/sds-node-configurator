package lvg

import (
	"agent/internal"
	"agent/pkg/cache"
	"agent/pkg/controller"
	"agent/pkg/logger"
	"agent/pkg/monitoring"
	"agent/pkg/utils"
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/cloudflare/cfssl/log"
	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const Name = "lvm-volume-group-watcher-controller"
const LVGMetadateNameLabelKey = "kubernetes.io/metadata.name"

const (
	Local  = "Local"
	Shared = "Shared"

	Failed = "Failed"

	NonOperational = "NonOperational"

	deletionProtectionAnnotation = "storage.deckhouse.io/deletion-protection"

	LVMVolumeGroupTag = "storage.deckhouse.io/lvmVolumeGroupName"
)

type Reconciler struct {
	cl       client.Client
	log      logger.Logger
	metrics  monitoring.Metrics
	sdsCache *cache.Cache
	opts     Options
}

type Options struct {
	NodeName                   string
	BlockDeviceScanIntervalSec time.Duration
	VolumeGroupScanIntervalSec time.Duration
}

func NewReconciler(
	cl client.Client,
	log logger.Logger,
	metrics monitoring.Metrics,
	sdsCache *cache.Cache,
	opts Options,
) *Reconciler {
	return &Reconciler{
		cl:       cl,
		log:      log,
		metrics:  metrics,
		sdsCache: sdsCache,
		opts:     opts,
	}
}

func (r *Reconciler) Name() string {
	return Name
}

func (r *Reconciler) Reconcile(ctx context.Context, request controller.ReconcileRequest[*v1alpha1.LVMVolumeGroup]) (controller.Result, error) {
	r.log.Info(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] Reconciler starts to reconcile the request %s", request.Object.Name))

	lvg := request.Object

	belongs := checkIfLVGBelongsToNode(lvg, r.opts.NodeName)
	if !belongs {
		r.log.Info(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] the LVMVolumeGroup %s does not belong to the node %s", lvg.Name, r.opts.NodeName))
		return controller.Result{}, nil
	}
	r.log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] the LVMVolumeGroup %s belongs to the node %s. Starts to reconcile", lvg.Name, r.opts.NodeName))

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

	r.log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] tries to get block device resources for the LVMVolumeGroup %s by the selector %v", lvg.Name, lvg.Spec.BlockDeviceSelector.MatchLabels))
	blockDevices, err := r.getAPIBlockDevices(ctx, lvg.Spec.BlockDeviceSelector)
	if err != nil {
		r.log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to get BlockDevices. Retry in %s", r.opts.BlockDeviceScanIntervalSec.String()))
		err = r.updateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, "NoBlockDevices", fmt.Sprintf("unable to get block devices resources, err: %s", err.Error()))
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to add a condition %s to the LVMVolumeGroup %s. Retry in %s", internal.TypeVGConfigurationApplied, lvg.Name, r.opts.BlockDeviceScanIntervalSec.String()))
		}

		return controller.Result{RequeueAfter: r.opts.BlockDeviceScanIntervalSec}, nil
	}
	r.log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] successfully got block device resources for the LVMVolumeGroup %s by the selector %v", lvg.Name, lvg.Spec.BlockDeviceSelector.MatchLabels))

	valid, reason := validateSpecBlockDevices(lvg, blockDevices)
	if !valid {
		r.log.Warning(fmt.Sprintf("[RunLVMVolumeGroupController] validation failed for the LVMVolumeGroup %s, reason: %s", lvg.Name, reason))
		err = r.updateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonValidationFailed, reason)
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to add a condition %s to the LVMVolumeGroup %s. Retry in %s", internal.TypeVGConfigurationApplied, lvg.Name, cfg.VolumeGroupScanIntervalSec.String()))
			return controller.Result{}, err
		}

		return controller.Result{}, nil
	}
	r.log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] successfully validated BlockDevices of the LVMVolumeGroup %s", lvg.Name))

	r.log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] tries to add label %s to the LVMVolumeGroup %s", LVGMetadateNameLabelKey, cfg.NodeName))
	added, err = r.addLVGLabelIfNeeded(ctx, lvg, LVGMetadateNameLabelKey, lvg.Name)
	if err != nil {
		r.log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to add label %s to the LVMVolumeGroup %s", LVGMetadateNameLabelKey, lvg.Name))
		return controller.Result{}, err
	}

	if added {
		r.log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] successfully added label %s to the LVMVolumeGroup %s", LVGMetadateNameLabelKey, lvg.Name))
	} else {
		r.log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] no need to add label %s to the LVMVolumeGroup %s", LVGMetadateNameLabelKey, lvg.Name))
	}

	// We do this after BlockDevices validation and node belonging check to prevent multiple updates by all agents pods
	bds, _ := r.sdsCache.GetDevices()
	if len(bds) == 0 {
		r.log.Warning(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] no block devices in the cache, add the LVMVolumeGroup %s to requeue", lvg.Name))
		err = r.updateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, "CacheEmpty", "unable to apply configuration due to the cache's state")
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to add a condition %s to the LVMVolumeGroup %s. Retry in %s", internal.TypeVGConfigurationApplied, lvg.Name, cfg.VolumeGroupScanIntervalSec.String()))
		}

		return controller.Result{
			RequeueAfter: r.opts.VolumeGroupScanIntervalSec,
		}, nil
	}

	r.log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] tries to sync status and spec thin-pool AllicationLimit fields for the LVMVolumeGroup %s", lvg.Name))
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
		r.log.Warning(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] the LVMVolumeGroup %s event will be requeued in %s", lvg.Name, r.opts.VolumeGroupScanIntervalSec.String()))
		return controller.Result{
			RequeueAfter: r.opts.VolumeGroupScanIntervalSec,
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
	recType := r.identifyLVGReconcileFunc(lvg, sdsCache)

	switch recType {
	case CreateReconcile:
		r.log.Info(fmt.Sprintf("[runEventReconcile] CreateReconcile starts the reconciliation for the LVMVolumeGroup %s", lvg.Name))
		return r.reconcileLVGCreateFunc(ctx, lvg, blockDevices)
	case UpdateReconcile:
		r.log.Info(fmt.Sprintf("[runEventReconcile] UpdateReconcile starts the reconciliation for the LVMVolumeGroup %s", lvg.Name))
		return r.reconcileLVGUpdateFunc(ctx, lvg, blockDevices)
	case DeleteReconcile:
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
			err := r.updateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonTerminating, "trying to delete VG")
			if err != nil {
				r.log.Error(err, fmt.Sprintf("[reconcileLVGDeleteFunc] unable to add the condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
				return true, err
			}
			break
		}
	}

	_, exist := lvg.Annotations[deletionProtectionAnnotation]
	if exist {
		r.log.Debug(fmt.Sprintf("[reconcileLVGDeleteFunc] the LVMVolumeGroup %s has a deletion timestamp but also has a deletion protection annotation %s. Remove it to proceed the delete operation", lvg.Name, deletionProtectionAnnotation))
		err := r.updateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonTerminating, fmt.Sprintf("to delete the LVG remove the annotation %s", deletionProtectionAnnotation))
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
		err = r.updateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonTerminating, err.Error())
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
		err = r.updateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonTerminating, err.Error())
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[reconcileLVGDeleteFunc] unable to add the condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
			return true, err
		}

		return true, err
	}

	removed, err := r.removeLVGFinalizerIfExist(ctx, lvg)
	if err != nil {
		r.log.Error(err, fmt.Sprintf("[reconcileLVGDeleteFunc] unable to remove a finalizer %s from the LVMVolumeGroup %s", internal.SdsNodeConfiguratorFinalizer, lvg.Name))
		err = r.updateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonTerminating, err.Error())
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

	err = r.deleteLVMVolumeGroup(ctx, lvg, r.opts.NodeName)
	if err != nil {
		r.log.Error(err, fmt.Sprintf("[reconcileLVGDeleteFunc] unable to delete the LVMVolumeGroup %s", lvg.Name))
		return true, err
	}

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
	valid, reason := r.validateLVGForUpdateFunc(lvg, blockDevices)
	if !valid {
		r.log.Warning(fmt.Sprintf("[reconcileLVGUpdateFunc] the LVMVolumeGroup %s is not valid", lvg.Name))
		err := r.updateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonValidationFailed, reason)
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to add a condition %s reason %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, internal.ReasonValidationFailed, lvg.Name))
		}

		return true, err
	}
	r.log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] successfully validated the LVMVolumeGroup %s", lvg.Name))

	r.log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] tries to get VG %s for the LVMVolumeGroup %s", lvg.Spec.ActualVGNameOnTheNode, lvg.Name))
	found, vg := tryGetVG(r.sdsCache, lvg.Spec.ActualVGNameOnTheNode)
	if !found {
		err := fmt.Errorf("VG %s not found", lvg.Spec.ActualVGNameOnTheNode)
		r.log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to reconcile the LVMVolumeGroup %s", lvg.Name))
		err = r.updateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, "VGNotFound", err.Error())
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
		err = r.updateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, "VGUpdateFailed", fmt.Sprintf("unable to update VG tag, err: %s", err.Error()))
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
		err = r.updateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, "PVResizeFailed", fmt.Sprintf("unable to resize PV, err: %s", err.Error()))
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
		err = r.updateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, "VGExtendFailed", fmt.Sprintf("unable to extend VG, err: %s", err.Error()))
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to add a condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
		}

		return true, err
	}
	r.log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] successfully ended the extend operation for VG of the LVMVolumeGroup %s", lvg.Name))

	if lvg.Spec.ThinPools != nil {
		r.log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] starts to reconcile thin-pools of the LVMVolumeGroup %s", lvg.Name))
		lvs, _ := r.sdsCache.GetLVs()
		err = r.reconcileThinPoolsIfNeeded(ctx, lvg, vg, lvs)
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to reconcile thin-pools of the LVMVolumeGroup %s", lvg.Name))
			err = r.updateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, "ThinPoolReconcileFailed", fmt.Sprintf("unable to reconcile thin-pools, err: %s", err.Error()))
			if err != nil {
				r.log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to add a condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
			}
			return true, err
		}
		r.log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] successfully reconciled thin-pools operation of the LVMVolumeGroup %s", lvg.Name))
	}

	r.log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] tries to add a condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
	err = r.updateLVGConditionIfNeeded(ctx, lvg, v1.ConditionTrue, internal.TypeVGConfigurationApplied, "Applied", "configuration has been applied")
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
		err := r.updateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonCreating, "trying to apply the configuration")
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[reconcileLVGCreateFunc] unable to add the condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
			return true, err
		}
	}

	r.log.Debug(fmt.Sprintf("[reconcileLVGCreateFunc] tries to validate the LVMVolumeGroup %s", lvg.Name))
	valid, reason := r.validateLVGForCreateFunc(lvg, blockDevices)
	if !valid {
		r.log.Warning(fmt.Sprintf("[reconcileLVGCreateFunc] validation fails for the LVMVolumeGroup %s", lvg.Name))
		err := r.updateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonValidationFailed, reason)
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to add a condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
		}

		return true, err
	}
	r.log.Debug(fmt.Sprintf("[reconcileLVGCreateFunc] successfully validated the LVMVolumeGroup %s", lvg.Name))

	r.log.Debug(fmt.Sprintf("[reconcileLVGCreateFunc] tries to create VG for the LVMVolumeGroup %s", lvg.Name))
	err := r.createVGComplex(lvg, blockDevices)
	if err != nil {
		r.log.Error(err, fmt.Sprintf("[reconcileLVGCreateFunc] unable to create VG for the LVMVolumeGroup %s", lvg.Name))
		err = r.updateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, "VGCreationFailed", fmt.Sprintf("unable to create VG, err: %s", err.Error()))
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to add a condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
		}
		return true, err
	}
	r.log.Info(fmt.Sprintf("[reconcileLVGCreateFunc] successfully created VG for the LVMVolumeGroup %s", lvg.Name))

	if lvg.Spec.ThinPools != nil {
		r.log.Debug(fmt.Sprintf("[reconcileLVGCreateFunc] the LVMVolumeGroup %s has thin-pools. Tries to create them", lvg.Name))

		for _, tp := range lvg.Spec.ThinPools {
			vgSize := countVGSizeByBlockDevices(blockDevices)
			tpRequestedSize, err := getRequestedSizeFromString(tp.Size, vgSize)
			if err != nil {
				r.log.Error(err, fmt.Sprintf("[reconcileLVGCreateFunc] unable to get thin-pool %s requested size of the LVMVolumeGroup %s", tp.Name, lvg.Name))
				return false, err
			}

			var cmd string
			if utils.AreSizesEqualWithinDelta(tpRequestedSize, vgSize, internal.ResizeDelta) {
				r.log.Debug(fmt.Sprintf("[reconcileLVGCreateFunc] Thin-pool %s of the LVMVolumeGroup %s will be created with full VG space size", tp.Name, lvg.Name))
				cmd, err = utils.CreateThinPoolFullVGSpace(tp.Name, lvg.Spec.ActualVGNameOnTheNode)
			} else {
				r.log.Debug(fmt.Sprintf("[reconcileLVGCreateFunc] Thin-pool %s of the LVMVolumeGroup %s will be created with size %s", tp.Name, lvg.Name, tpRequestedSize.String()))
				cmd, err = utils.CreateThinPool(tp.Name, lvg.Spec.ActualVGNameOnTheNode, tpRequestedSize.Value())
			}
			if err != nil {
				r.log.Error(err, fmt.Sprintf("[reconcileLVGCreateFunc] unable to create thin-pool %s of the LVMVolumeGroup %s, cmd: %s", tp.Name, lvg.Name, cmd))
				err = r.updateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, "ThinPoolCreationFailed", fmt.Sprintf("unable to create thin-pool, err: %s", err.Error()))
				if err != nil {
					r.log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to add a condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
				}

				return true, err
			}
		}
		r.log.Debug(fmt.Sprintf("[reconcileLVGCreateFunc] successfully created thin-pools for the LVMVolumeGroup %s", lvg.Name))
	}

	err = r.updateLVGConditionIfNeeded(ctx, lvg, v1.ConditionTrue, internal.TypeVGConfigurationApplied, "Success", "all configuration has been applied")
	if err != nil {
		r.log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to add a condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
		return true, err
	}

	return false, nil
}

func (r *Reconciler) deleteLVMVolumeGroup(ctx context.Context, lvg *v1alpha1.LVMVolumeGroup, currentNode string) error {
	r.log.Debug(fmt.Sprintf(`[DeleteLVMVolumeGroup] Node "%s" does not belong to VG "%s". It will be removed from LVM resource, name "%s"'`, currentNode, lvg.Spec.ActualVGNameOnTheNode, lvg.Name))
	for i, node := range lvg.Status.Nodes {
		if node.Name == currentNode {
			// delete node
			lvg.Status.Nodes = append(lvg.Status.Nodes[:i], lvg.Status.Nodes[i+1:]...)
			r.log.Info(fmt.Sprintf(`[DeleteLVMVolumeGroup] deleted node "%s" from LVMVolumeGroup "%s"`, node.Name, lvg.Name))
		}
	}

	// If current LVMVolumeGroup has no nodes left, delete it.
	if len(lvg.Status.Nodes) == 0 {
		start := time.Now()
		err := r.cl.Delete(ctx, lvg)
		r.metrics.APIMethodsDuration(Name, "delete").Observe(r.metrics.GetEstimatedTimeInSeconds(start))
		r.metrics.APIMethodsExecutionCount(Name, "delete").Inc()
		if err != nil {
			r.metrics.APIMethodsErrors(Name, "delete").Inc()
			return err
		}
		r.log.Info(fmt.Sprintf("[DeleteLVMVolumeGroup] the LVMVolumeGroup %s deleted", lvg.Name))
	}

	return nil
}

func checkIfVGExist(vgName string, vgs []internal.VGData) bool {
	for _, vg := range vgs {
		if vg.VGName == vgName {
			return true
		}
	}

	return false
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
	if newLVG.DeletionTimestamp != nil {
		r.log.Debug(fmt.Sprintf("[shouldLVGWatcherReconcileUpdateEvent] update event should be reconciled as the LVMVolumeGroup %s has deletionTimestamp", newLVG.Name))
		return true
	}

	if _, exist := newLVG.Labels[internal.LVGUpdateTriggerLabel]; exist {
		r.log.Debug(fmt.Sprintf("[shouldLVGWatcherReconcileUpdateEvent] update event should be reconciled as the LVMVolumeGroup %s has the label %s", newLVG.Name, internal.LVGUpdateTriggerLabel))
		return true
	}

	if r.shouldUpdateLVGLabels(log, newLVG, LVGMetadateNameLabelKey, newLVG.Name) {
		r.log.Debug(fmt.Sprintf("[shouldLVGWatcherReconcileUpdateEvent] update event should be reconciled as the LVMVolumeGroup's %s labels have been changed", newLVG.Name))
		return true
	}

	if !reflect.DeepEqual(oldLVG.Spec, newLVG.Spec) {
		r.log.Debug(fmt.Sprintf("[shouldLVGWatcherReconcileUpdateEvent] update event should be reconciled as the LVMVolumeGroup %s configuration has been changed", newLVG.Name))
		return true
	}

	for _, c := range newLVG.Status.Conditions {
		if c.Type == internal.TypeVGConfigurationApplied {
			if c.Reason == internal.ReasonUpdating || c.Reason == internal.ReasonCreating {
				log.Debug(fmt.Sprintf("[shouldLVGWatcherReconcileUpdateEvent] update event should not be reconciled as the LVMVolumeGroup %s reconciliation still in progress", newLVG.Name))
				return false
			}
		}
	}

	for _, n := range newLVG.Status.Nodes {
		for _, d := range n.Devices {
			if !utils.AreSizesEqualWithinDelta(d.PVSize, d.DevSize, internal.ResizeDelta) {
				log.Debug(fmt.Sprintf("[shouldLVGWatcherReconcileUpdateEvent] update event should be reconciled as the LVMVolumeGroup %s PV size is different to device size", newLVG.Name))
				return true
			}
		}
	}

	return false
}

func shouldReconcileLVGByDeleteFunc(lvg *v1alpha1.LVMVolumeGroup) bool {
	return lvg.DeletionTimestamp != nil
}

func (r *Reconciler) updateLVGConditionIfNeeded(
	ctx context.Context,
	lvg *v1alpha1.LVMVolumeGroup,
	status v1.ConditionStatus,
	conType, reason, message string,
) error {
	exist := false
	index := 0
	newCondition := v1.Condition{
		Type:               conType,
		Status:             status,
		ObservedGeneration: lvg.Generation,
		LastTransitionTime: v1.NewTime(time.Now()),
		Reason:             reason,
		Message:            message,
	}

	if lvg.Status.Conditions == nil {
		r.log.Debug(fmt.Sprintf("[updateLVGConditionIfNeeded] the LVMVolumeGroup %s conditions is nil. Initialize them", lvg.Name))
		lvg.Status.Conditions = make([]v1.Condition, 0, 5)
	}

	if len(lvg.Status.Conditions) > 0 {
		r.log.Debug(fmt.Sprintf("[updateLVGConditionIfNeeded] there are some conditions in the LVMVolumeGroup %s. Tries to find a condition %s", lvg.Name, conType))
		for i, c := range lvg.Status.Conditions {
			if c.Type == conType {
				if checkIfEqualConditions(c, newCondition) {
					log.Debug(fmt.Sprintf("[updateLVGConditionIfNeeded] no need to update condition %s in the LVMVolumeGroup %s as new and old condition states are the same", conType, lvg.Name))
					return nil
				}

				index = i
				exist = true
				r.log.Debug(fmt.Sprintf("[updateLVGConditionIfNeeded] a condition %s was found in the LVMVolumeGroup %s at the index %d", conType, lvg.Name, i))
			}
		}

		if !exist {
			r.log.Debug(fmt.Sprintf("[updateLVGConditionIfNeeded] a condition %s was not found. Append it in the end of the LVMVolumeGroup %s conditions", conType, lvg.Name))
			lvg.Status.Conditions = append(lvg.Status.Conditions, newCondition)
		} else {
			r.log.Debug(fmt.Sprintf("[updateLVGConditionIfNeeded] insert the condition %s status %s reason %s message %s at index %d of the LVMVolumeGroup %s conditions", conType, status, reason, message, index, lvg.Name))
			lvg.Status.Conditions[index] = newCondition
		}
	} else {
		r.log.Debug(fmt.Sprintf("[updateLVGConditionIfNeeded] no conditions were found in the LVMVolumeGroup %s. Append the condition %s in the end", lvg.Name, conType))
		lvg.Status.Conditions = append(lvg.Status.Conditions, newCondition)
	}

	r.log.Debug(fmt.Sprintf("[updateLVGConditionIfNeeded] tries to update the condition type %s status %s reason %s message %s of the LVMVolumeGroup %s", conType, status, reason, message, lvg.Name))
	return r.cl.Status().Update(ctx, lvg)
}

func checkIfEqualConditions(first, second v1.Condition) bool {
	return first.Type == second.Type &&
		first.Status == second.Status &&
		first.Reason == second.Reason &&
		first.Message == second.Message &&
		first.ObservedGeneration == second.ObservedGeneration
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

				space, err = getThinPoolAvailableSpace(lvg.Status.ThinPools[i].ActualSize, lvg.Status.ThinPools[i].AllocatedSize, specLimits)
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
		fmt.Printf("%+v", lvg.Status.ThinPools)
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

	for _, me := range lvg.Spec.BlockDeviceSelector.MatchExpressions {
		if me.Key == internal.MetadataNameLabelKey {
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

	bdFromOtherNode := make([]string, 0, len(blockDevices))
	for _, bd := range blockDevices {
		if bd.Status.NodeName != lvg.Spec.Local.NodeName {
			bdFromOtherNode = append(bdFromOtherNode, bd.Name)
		}
	}

	if len(bdFromOtherNode) != 0 {
		return false, fmt.Sprintf("block devices %s have different node names from LVMVolumeGroup Local.NodeName", strings.Join(bdFromOtherNode, ","))
	}

	return true, ""
}

func (r *Reconciler) deleteLVGIfNeeded(ctx context.Context, lvg *v1alpha1.LVMVolumeGroup) (bool, error) {
	if lvg.DeletionTimestamp == nil {
		return false, nil
	}

	vgs, _ := r.sdsCache.GetVGs()
	if !checkIfVGExist(lvg.Spec.ActualVGNameOnTheNode, vgs) {
		r.log.Info(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] VG %s was not yet created for the LVMVolumeGroup %s and the resource is marked as deleting. Delete the resource", lvg.Spec.ActualVGNameOnTheNode, lvg.Name))
		removed, err := removeLVGFinalizerIfExist(ctx, lvg)
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to remove the finalizer %s from the LVMVolumeGroup %s", internal.SdsNodeConfiguratorFinalizer, lvg.Name))
			return false, err
		}

		if removed {
			r.log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] successfully removed the finalizer %s from the LVMVolumeGroup %s", internal.SdsNodeConfiguratorFinalizer, lvg.Name))
		} else {
			r.log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] no need to remove the finalizer %s from the LVMVolumeGroup %s", internal.SdsNodeConfiguratorFinalizer, lvg.Name))
		}

		err = r.deleteLVMVolumeGroup(ctx, lvg, r.opts.NodeName)
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to delete the LVMVolumeGroup %s", lvg.Name))
			return false, err
		}
		r.log.Info(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] successfully deleted the LVMVolumeGroup %s", lvg.Name))
		return true, nil
	}
	return false, nil
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

func getRequestedSizeFromString(size string, targetSpace resource.Quantity) (resource.Quantity, error) {
	switch isPercentSize(size) {
	case true:
		strPercent := strings.Split(size, "%")[0]
		percent, err := strconv.Atoi(strPercent)
		if err != nil {
			return resource.Quantity{}, err
		}
		lvSize := targetSpace.Value() * int64(percent) / 100
		return *resource.NewQuantity(lvSize, resource.BinarySI), nil
	case false:
		return resource.ParseQuantity(size)
	}

	return resource.Quantity{}, nil
}

func countVGSizeByBlockDevices(blockDevices map[string]v1alpha1.BlockDevice) resource.Quantity {
	var totalVGSize int64
	for _, bd := range blockDevices {
		totalVGSize += bd.Status.Size.Value()
	}
	return *resource.NewQuantity(totalVGSize, resource.BinarySI)
}

func (r *Reconciler) validateLVGForCreateFunc(
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

	if lvg.Spec.ThinPools != nil {
		r.log.Debug(fmt.Sprintf("[validateLVGForCreateFunc] the LVMVolumeGroup %s has thin-pools. Validate if VG size has enough space for the thin-pools", lvg.Name))
		r.log.Trace(fmt.Sprintf("[validateLVGForCreateFunc] the LVMVolumeGroup %s has thin-pools %v", lvg.Name, lvg.Spec.ThinPools))
		r.log.Trace(fmt.Sprintf("[validateLVGForCreateFunc] total LVMVolumeGroup %s size: %s", lvg.Name, totalVGSize.String()))

		var totalThinPoolSize int64
		for _, tp := range lvg.Spec.ThinPools {
			tpRequestedSize, err := getRequestedSizeFromString(tp.Size, totalVGSize)
			if err != nil {
				reason.WriteString(err.Error())
				continue
			}

			if tpRequestedSize.Value() == 0 {
				reason.WriteString(fmt.Sprintf("Thin-pool %s has zero size. ", tp.Name))
				continue
			}

			// means a user want a thin-pool with 100%FREE size
			if utils.AreSizesEqualWithinDelta(tpRequestedSize, totalVGSize, internal.ResizeDelta) {
				if len(lvg.Spec.ThinPools) > 1 {
					reason.WriteString(fmt.Sprintf("Thin-pool %s requested size of full VG space, but there is any other thin-pool. ", tp.Name))
				}
			}

			totalThinPoolSize += tpRequestedSize.Value()
		}
		r.log.Trace(fmt.Sprintf("[validateLVGForCreateFunc] LVMVolumeGroup %s thin-pools requested space: %d", lvg.Name, totalThinPoolSize))

		if totalThinPoolSize != totalVGSize.Value() && totalThinPoolSize+internal.ResizeDelta.Value() >= totalVGSize.Value() {
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

func (r *Reconciler) validateLVGForUpdateFunc(
	lvg *v1alpha1.LVMVolumeGroup,
	blockDevices map[string]v1alpha1.BlockDevice,
) (bool, string) {
	reason := strings.Builder{}
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

		newTotalVGSize := resource.NewQuantity(vg.VGSize.Value()+additionBlockDeviceSpace, resource.BinarySI)
		for _, specTp := range lvg.Spec.ThinPools {
			// might be a case when Thin-pool is already created, but is not shown in status
			tpRequestedSize, err := getRequestedSizeFromString(specTp.Size, *newTotalVGSize)
			if err != nil {
				reason.WriteString(err.Error())
				continue
			}

			if tpRequestedSize.Value() == 0 {
				reason.WriteString(fmt.Sprintf("Thin-pool %s has zero size. ", specTp.Name))
				continue
			}

			r.log.Debug(fmt.Sprintf("[validateLVGForUpdateFunc] the LVMVolumeGroup %s thin-pool %s requested size %s, Status VG size %s", lvg.Name, specTp.Name, tpRequestedSize.String(), lvg.Status.VGSize.String()))
			switch utils.AreSizesEqualWithinDelta(tpRequestedSize, *newTotalVGSize, internal.ResizeDelta) {
			// means a user wants 100% of VG space
			case true:
				hasFullThinPool = true
				if len(lvg.Spec.ThinPools) > 1 {
					// as if a user wants thin-pool with 100%VG size, there might be only one thin-pool
					reason.WriteString(fmt.Sprintf("Thin-pool %s requests size of full VG space, but there are any other thin-pools. ", specTp.Name))
				}
			case false:
				if actualThinPool, created := actualThinPools[specTp.Name]; !created {
					r.log.Debug(fmt.Sprintf("[validateLVGForUpdateFunc] thin-pool %s of the LVMVolumeGroup %s is not yet created, adds its requested size", specTp.Name, lvg.Name))
					addingThinPoolSize += tpRequestedSize.Value()
				} else {
					r.log.Debug(fmt.Sprintf("[validateLVGForUpdateFunc] thin-pool %s of the LVMVolumeGroup %s is already created, check its requested size", specTp.Name, lvg.Name))
					if tpRequestedSize.Value()+internal.ResizeDelta.Value() < actualThinPool.LVSize.Value() {
						r.log.Debug(fmt.Sprintf("[validateLVGForUpdateFunc] the LVMVolumeGroup %s Spec.ThinPool %s size %s is less than Status one: %s", lvg.Name, specTp.Name, tpRequestedSize.String(), actualThinPool.LVSize.String()))
						reason.WriteString(fmt.Sprintf("Requested Spec.ThinPool %s size %s is less than actual one %s. ", specTp.Name, tpRequestedSize.String(), actualThinPool.LVSize.String()))
						continue
					}

					thinPoolSizeDiff := tpRequestedSize.Value() - actualThinPool.LVSize.Value()
					if thinPoolSizeDiff > internal.ResizeDelta.Value() {
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
			if addingThinPoolSize != 0 && addingThinPoolSize+internal.ResizeDelta.Value() > totalFreeSpace {
				reason.WriteString("Added thin-pools requested sizes are more than allowed free space in VG.")
			}
		}
	}

	if reason.Len() != 0 {
		return false, reason.String()
	}

	return true, ""
}

func (r *Reconciler) identifyLVGReconcileFunc(lvg *v1alpha1.LVMVolumeGroup) reconcileType {
	if r.shouldReconcileLVGByCreateFunc(lvg) {
		return CreateReconcile
	}

	if r.shouldReconcileLVGByUpdateFunc(lvg) {
		return UpdateReconcile
	}

	if r.shouldReconcileLVGByDeleteFunc(lvg) {
		return DeleteReconcile
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

func (r *Reconciler) reconcileThinPoolsIfNeeded(
	ctx context.Context,
	lvg *v1alpha1.LVMVolumeGroup,
	vg internal.VGData,
	lvs []internal.LVData,
) error {
	actualThinPools := make(map[string]internal.LVData, len(lvs))
	for _, lv := range lvs {
		if string(lv.LVAttr[0]) == "t" {
			actualThinPools[lv.LVName] = lv
		}
	}

	errs := strings.Builder{}
	for _, specTp := range lvg.Spec.ThinPools {
		tpRequestedSize, err := getRequestedSizeFromString(specTp.Size, lvg.Status.VGSize)
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[ReconcileThinPoolsIfNeeded] unable to get requested thin-pool %s size of the LVMVolumeGroup %s", specTp.Name, lvg.Name))
			return err
		}

		if actualTp, exist := actualThinPools[specTp.Name]; !exist {
			r.log.Debug(fmt.Sprintf("[ReconcileThinPoolsIfNeeded] thin-pool %s of the LVMVolumeGroup %s is not created yet. Create it", specTp.Name, lvg.Name))
			if checkIfConditionIsTrue(lvg, internal.TypeVGConfigurationApplied) {
				err := r.updateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonUpdating, "trying to apply the configuration")
				if err != nil {
					r.log.Error(err, fmt.Sprintf("[ReconcileThinPoolsIfNeeded] unable to add the condition %s status False reason %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, internal.ReasonUpdating, lvg.Name))
					return err
				}
			}

			var cmd string
			start := time.Now()
			if utils.AreSizesEqualWithinDelta(tpRequestedSize, lvg.Status.VGSize, internal.ResizeDelta) {
				r.log.Debug(fmt.Sprintf("[ReconcileThinPoolsIfNeeded] thin-pool %s of the LVMVolumeGroup %s will be created with size 100FREE", specTp.Name, lvg.Name))
				cmd, err = utils.CreateThinPoolFullVGSpace(specTp.Name, vg.VGName)
			} else {
				r.log.Debug(fmt.Sprintf("[ReconcileThinPoolsIfNeeded] thin-pool %s of the LVMVolumeGroup %s will be created with size %s", specTp.Name, lvg.Name, tpRequestedSize.String()))
				cmd, err = utils.CreateThinPool(specTp.Name, vg.VGName, tpRequestedSize.Value())
			}
			r.metrics.UtilsCommandsDuration(Name, "lvcreate").Observe(r.metrics.GetEstimatedTimeInSeconds(start))
			r.metrics.UtilsCommandsExecutionCount(Name, "lvcreate").Inc()
			if err != nil {
				r.metrics.UtilsCommandsErrorsCount(Name, "lvcreate").Inc()
				r.log.Error(err, fmt.Sprintf("[ReconcileThinPoolsIfNeeded] unable to create thin-pool %s of the LVMVolumeGroup %s, cmd: %s", specTp.Name, lvg.Name, cmd))
				errs.WriteString(fmt.Sprintf("unable to create thin-pool %s, err: %s. ", specTp.Name, err.Error()))
				continue
			}

			r.log.Info(fmt.Sprintf("[ReconcileThinPoolsIfNeeded] thin-pool %s of the LVMVolumeGroup %s has been successfully created", specTp.Name, lvg.Name))
		} else {
			// thin-pool exists
			if utils.AreSizesEqualWithinDelta(tpRequestedSize, actualTp.LVSize, internal.ResizeDelta) {
				r.log.Debug(fmt.Sprintf("[ReconcileThinPoolsIfNeeded] the LVMVolumeGroup %s requested thin pool %s size is equal to actual one", lvg.Name, tpRequestedSize.String()))
				continue
			}

			log.Debug(fmt.Sprintf("[ReconcileThinPoolsIfNeeded] the LVMVolumeGroup %s requested thin pool %s size is more than actual one. Resize it", lvg.Name, tpRequestedSize.String()))
			if checkIfConditionIsTrue(lvg, internal.TypeVGConfigurationApplied) {
				err = r.updateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonUpdating, "trying to apply the configuration")
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

	errs := strings.Builder{}
	for _, n := range lvg.Status.Nodes {
		for _, d := range n.Devices {
			if d.DevSize.Value()-d.PVSize.Value() > internal.ResizeDelta.Value() {
				if checkIfConditionIsTrue(lvg, internal.TypeVGConfigurationApplied) {
					err := r.updateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonUpdating, "trying to apply the configuration")
					if err != nil {
						r.log.Error(err, fmt.Sprintf("[UpdateVGTagIfNeeded] unable to add the condition %s status False reason %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, internal.ReasonUpdating, lvg.Name))
						return err
					}
				}

				r.log.Debug(fmt.Sprintf("[ResizePVIfNeeded] the LVMVolumeGroup %s BlockDevice %s PVSize is less than actual device size. Resize PV", lvg.Name, d.BlockDevice))

				start := time.Now()
				cmd, err := utils.ResizePV(d.Path)
				r.metrics.UtilsCommandsDuration(Name, "pvresize").Observe(r.metrics.GetEstimatedTimeInSeconds(start))
				r.metrics.UtilsCommandsExecutionCount(Name, "pvresize")
				if err != nil {
					r.metrics.UtilsCommandsErrorsCount(Name, "pvresize").Inc()
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

	if checkIfConditionIsTrue(lvg, internal.TypeVGConfigurationApplied) {
		err := r.updateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonUpdating, "trying to apply the configuration")
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[UpdateVGTagIfNeeded] unable to add the condition %s status False reason %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, internal.ReasonUpdating, lvg.Name))
			return err
		}
	}

	r.log.Debug(fmt.Sprintf("[ExtendVGIfNeeded] VG %s should be extended as there are some BlockDevices were added to Spec field of the LVMVolumeGroup %s", vg.VGName, lvg.Name))
	paths := extractPathsFromBlockDevices(devicesToExtend, blockDevices)
	err := r.extendVGComplex(paths, vg.VGName)
	if err != nil {
		r.log.Error(err, fmt.Sprintf("[ExtendVGIfNeeded] unable to extend VG %s of the LVMVolumeGroup %s", vg.VGName, lvg.Name))
		return err
	}
	r.log.Info(fmt.Sprintf("[ExtendVGIfNeeded] VG %s of the LVMVolumeGroup %s was extended", vg.VGName, lvg.Name))

	return nil
}

func tryGetVG(sdsCache *cache.Cache, vgName string) (bool, internal.VGData) {
	vgs, _ := sdsCache.GetVGs()
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

func (r *Reconciler) getLVMVolumeGroup(ctx context.Context, name string) (*v1alpha1.LVMVolumeGroup, error) {
	obj := &v1alpha1.LVMVolumeGroup{}
	start := time.Now()
	err := r.cl.Get(ctx, client.ObjectKey{
		Name: name,
	}, obj)
	r.metrics.APIMethodsDuration(Name, "get").Observe(r.metrics.GetEstimatedTimeInSeconds(start))
	r.metrics.APIMethodsExecutionCount(Name, "get").Inc()
	if err != nil {
		r.metrics.APIMethodsErrors(Name, "get").Inc()
		return nil, err
	}
	return obj, nil
}

func (r *Reconciler) deleteVGIfExist(vgName string) error {
	vgs, _ := r.sdsCache.GetVGs()
	if !checkIfVGExist(vgName, vgs) {
		log.Debug(fmt.Sprintf("[DeleteVGIfExist] no VG %s found, nothing to delete", vgName))
		return nil
	}

	pvs, _ := r.sdsCache.GetPVs()
	if len(pvs) == 0 {
		err := errors.New("no any PV found")
		log.Error(err, fmt.Sprintf("[DeleteVGIfExist] no any PV was found while deleting VG %s", vgName))
		return err
	}

	start := time.Now()
	command, err := utils.RemoveVG(vgName)
	r.metrics.UtilsCommandsDuration(Name, "vgremove").Observe(r.metrics.GetEstimatedTimeInSeconds(start))
	r.metrics.UtilsCommandsExecutionCount(Name, "vgremove").Inc()
	r.log.Debug(command)
	if err != nil {
		r.metrics.UtilsCommandsErrorsCount(Name, "vgremove").Inc()
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
	command, err = utils.RemovePV(pvsToRemove)
	r.metrics.UtilsCommandsDuration(Name, "pvremove").Observe(r.metrics.GetEstimatedTimeInSeconds(start))
	r.metrics.UtilsCommandsExecutionCount(Name, "pvremove").Inc()
	r.log.Debug(command)
	if err != nil {
		r.metrics.UtilsCommandsErrorsCount(Name, "pvremove").Inc()
		r.log.Error(err, "RemovePV "+command)
		return err
	}
	r.log.Debug(fmt.Sprintf("[DeleteVGIfExist] successfully delete PVs of VG %s from the node", vgName))

	return nil
}

func (r *Reconciler) extendVGComplex(extendPVs []string, vgName string) error {
	for _, pvPath := range extendPVs {
		start := time.Now()
		command, err := utils.CreatePV(pvPath)
		r.metrics.UtilsCommandsDuration(Name, "pvcreate").Observe(r.metrics.GetEstimatedTimeInSeconds(start))
		r.metrics.UtilsCommandsExecutionCount(Name, "pvcreate").Inc()
		r.log.Debug(command)
		if err != nil {
			r.metrics.UtilsCommandsErrorsCount(Name, "pvcreate").Inc()
			r.log.Error(err, "CreatePV ")
			return err
		}
	}

	start := time.Now()
	command, err := utils.ExtendVG(vgName, extendPVs)
	r.metrics.UtilsCommandsDuration(Name, "vgextend").Observe(r.metrics.GetEstimatedTimeInSeconds(start))
	r.metrics.UtilsCommandsExecutionCount(Name, "vgextend").Inc()
	r.log.Debug(command)
	if err != nil {
		r.metrics.UtilsCommandsErrorsCount(Name, "vgextend").Inc()
		r.log.Error(err, "ExtendVG ")
		return err
	}
	return nil
}

func (r *Reconciler) createVGComplex(lvg *v1alpha1.LVMVolumeGroup, blockDevices map[string]v1alpha1.BlockDevice) error {
	paths := extractPathsFromBlockDevices(nil, blockDevices)

	r.log.Trace(fmt.Sprintf("[CreateVGComplex] LVMVolumeGroup %s devices paths %v", lvg.Name, paths))
	for _, path := range paths {
		start := time.Now()
		command, err := utils.CreatePV(path)
		r.metrics.UtilsCommandsDuration(Name, "pvcreate").Observe(r.metrics.GetEstimatedTimeInSeconds(start))
		r.metrics.UtilsCommandsExecutionCount(Name, "pvcreate").Inc()
		r.log.Debug(command)
		if err != nil {
			r.metrics.UtilsCommandsErrorsCount(Name, "pvcreate").Inc()
			r.log.Error(err, fmt.Sprintf("[CreateVGComplex] unable to create PV by path %s", path))
			return err
		}
	}

	r.log.Debug(fmt.Sprintf("[CreateVGComplex] successfully created all PVs for the LVMVolumeGroup %s", lvg.Name))
	r.log.Debug(fmt.Sprintf("[CreateVGComplex] the LVMVolumeGroup %s type is %s", lvg.Name, lvg.Spec.Type))
	switch lvg.Spec.Type {
	case Local:
		start := time.Now()
		cmd, err := utils.CreateVGLocal(lvg.Spec.ActualVGNameOnTheNode, lvg.Name, paths)
		r.metrics.UtilsCommandsDuration(Name, "vgcreate").Observe(r.metrics.GetEstimatedTimeInSeconds(start))
		r.metrics.UtilsCommandsExecutionCount(Name, "vgcreate").Inc()
		log.Debug(cmd)
		if err != nil {
			r.metrics.UtilsCommandsErrorsCount(Name, "vgcreate").Inc()
			log.Error(err, "error CreateVGLocal")
			return err
		}
	case Shared:
		start := time.Now()
		cmd, err := utils.CreateVGShared(lvg.Spec.ActualVGNameOnTheNode, lvg.Name, paths)
		r.metrics.UtilsCommandsDuration(Name, "vgcreate").Observe(r.metrics.GetEstimatedTimeInSeconds(start))
		r.metrics.UtilsCommandsExecutionCount(Name, "vgcreate").Inc()
		r.log.Debug(cmd)
		if err != nil {
			r.metrics.UtilsCommandsErrorsCount(Name, "vgcreate").Inc()
			r.log.Error(err, "error CreateVGShared")
			return err
		}
	}

	r.log.Debug(fmt.Sprintf("[CreateVGComplex] successfully create VG %s of the LVMVolumeGroup %s", lvg.Spec.ActualVGNameOnTheNode, lvg.Name))

	return nil
}

func (r *Reconciler) updateVGTagIfNeeded(
	ctx context.Context,
	lvg *v1alpha1.LVMVolumeGroup,
	vg internal.VGData,
) (bool, error) {
	found, tagName := checkTag(vg.VGTags)
	if found && lvg.Name != tagName {
		if checkIfConditionIsTrue(lvg, internal.TypeVGConfigurationApplied) {
			err := r.updateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonUpdating, "trying to apply the configuration")
			if err != nil {
				log.Error(err, fmt.Sprintf("[UpdateVGTagIfNeeded] unable to add the condition %s status False reason %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, internal.ReasonUpdating, lvg.Name))
				return false, err
			}
		}

		start := time.Now()
		cmd, err := utils.VGChangeDelTag(vg.VGName, fmt.Sprintf("%s=%s", LVMVolumeGroupTag, tagName))
		r.metrics.UtilsCommandsDuration(Name, "vgchange").Observe(r.metrics.GetEstimatedTimeInSeconds(start))
		r.metrics.UtilsCommandsExecutionCount(Name, "vgchange").Inc()
		log.Debug(fmt.Sprintf("[UpdateVGTagIfNeeded] exec cmd: %s", cmd))
		if err != nil {
			log.Error(err, fmt.Sprintf("[UpdateVGTagIfNeeded] unable to delete LVMVolumeGroupTag: %s=%s, vg: %s", LVMVolumeGroupTag, tagName, vg.VGName))
			r.metrics.UtilsCommandsErrorsCount(Name, "vgchange").Inc()
			return false, err
		}

		start = time.Now()
		cmd, err = utils.VGChangeAddTag(vg.VGName, fmt.Sprintf("%s=%s", LVMVolumeGroupTag, lvg.Name))
		r.metrics.UtilsCommandsDuration(Name, "vgchange").Observe(r.metrics.GetEstimatedTimeInSeconds(start))
		r.metrics.UtilsCommandsExecutionCount(Name, "vgchange").Inc()
		log.Debug(fmt.Sprintf("[UpdateVGTagIfNeeded] exec cmd: %s", cmd))
		if err != nil {
			log.Error(err, fmt.Sprintf("[UpdateVGTagIfNeeded] unable to add LVMVolumeGroupTag: %s=%s, vg: %s", LVMVolumeGroupTag, lvg.Name, vg.VGName))
			r.metrics.UtilsCommandsErrorsCount(Name, "vgchange").Inc()
			return false, err
		}

		return true, nil
	}

	return false, nil
}

func (r *Reconciler) extendThinPool(lvg *v1alpha1.LVMVolumeGroup, specThinPool v1alpha1.LVMVolumeGroupThinPoolSpec) error {
	volumeGroupFreeSpaceBytes := lvg.Status.VGSize.Value() - lvg.Status.AllocatedSize.Value()
	tpRequestedSize, err := getRequestedSizeFromString(specThinPool.Size, lvg.Status.VGSize)
	if err != nil {
		return err
	}

	r.log.Trace(fmt.Sprintf("[ExtendThinPool] volumeGroupSize = %s", lvg.Status.VGSize.String()))
	r.log.Trace(fmt.Sprintf("[ExtendThinPool] volumeGroupAllocatedSize = %s", lvg.Status.AllocatedSize.String()))
	r.log.Trace(fmt.Sprintf("[ExtendThinPool] volumeGroupFreeSpaceBytes = %d", volumeGroupFreeSpaceBytes))

	r.log.Info(fmt.Sprintf("[ExtendThinPool] start resizing thin pool: %s; with new size: %s", specThinPool.Name, tpRequestedSize.String()))

	var cmd string
	start := time.Now()
	if utils.AreSizesEqualWithinDelta(tpRequestedSize, lvg.Status.VGSize, internal.ResizeDelta) {
		r.log.Debug(fmt.Sprintf("[ExtendThinPool] thin-pool %s of the LVMVolumeGroup %s will be extend to size 100VG", specThinPool.Name, lvg.Name))
		cmd, err = utils.ExtendLVFullVGSpace(lvg.Spec.ActualVGNameOnTheNode, specThinPool.Name)
	} else {
		r.log.Debug(fmt.Sprintf("[ExtendThinPool] thin-pool %s of the LVMVolumeGroup %s will be extend to size %s", specThinPool.Name, lvg.Name, tpRequestedSize.String()))
		cmd, err = utils.ExtendLV(tpRequestedSize.Value(), lvg.Spec.ActualVGNameOnTheNode, specThinPool.Name)
	}
	r.metrics.UtilsCommandsDuration(Name, "lvextend").Observe(r.metrics.GetEstimatedTimeInSeconds(start))
	r.metrics.UtilsCommandsExecutionCount(Name, "lvextend").Inc()
	if err != nil {
		r.metrics.UtilsCommandsErrorsCount(Name, "lvextend").Inc()
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
