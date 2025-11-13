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
	"reflect"
	"slices"
	"strings"
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

type Reconciler struct {
	cl       client.Client
	log      logger.Logger
	lvgCl    *repository.LVGClient
	bdCl     *repository.BDClient
	metrics  monitoring.Metrics
	sdsCache *cache.Cache
	cfg      ReconcilerConfig
	commands utils.Commands
}

type ReconcilerConfig struct {
	NodeName                string
	BlockDeviceScanInterval time.Duration
	VolumeGroupScanInterval time.Duration
}

func NewReconciler(
	cl client.Client,
	log logger.Logger,
	metrics monitoring.Metrics,
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
		bdCl:     repository.NewBDClient(cl, metrics),
		metrics:  metrics,
		sdsCache: sdsCache,
		cfg:      cfg,
		commands: commands,
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
func (r *Reconciler) ShouldReconcileCreate(_ *v1alpha1.LVMVolumeGroup) bool {
	return true
}

// Reconcile implements controller.Reconciler.
func (r *Reconciler) Reconcile(ctx context.Context, request controller.ReconcileRequest[*v1alpha1.LVMVolumeGroup]) (controller.Result, error) {
	lvg := request.Object
	log := r.log.WithName("Reconcile")
	if lvg != nil {
		log = log.WithValues("lvgName", lvg.Name, "nodeName", r.cfg.NodeName)
	}
	log.Info("Reconciler starts to reconcile the request")

	belongs := checkIfLVGBelongsToNode(lvg, r.cfg.NodeName)
	if !belongs {
		log.Info("the LVMVolumeGroup does not belong to the node")
		return controller.Result{}, nil
	}
	log.Debug("the LVMVolumeGroup belongs to the node. Starts to reconcile")

	log.Debug("tries to add the finalizer to the LVMVolumeGroup", "finalizer", internal.SdsNodeConfiguratorFinalizer)
	added, err := r.addLVGFinalizerIfNotExist(ctx, lvg)
	if err != nil {
		log.Error(err, "unable to add the finalizer to the LVMVolumeGroup", "finalizer", internal.SdsNodeConfiguratorFinalizer)
		return controller.Result{}, err
	}

	if added {
		log.Debug("successfully added a finalizer to the LVMVolumeGroup", "finalizer", internal.SdsNodeConfiguratorFinalizer)
	} else {
		log.Debug("no need to add a finalizer to the LVMVolumeGroup", "finalizer", internal.SdsNodeConfiguratorFinalizer)
	}

	// this case handles the situation when a user decides to remove LVMVolumeGroup resource without created VG
	deleted, err := r.deleteLVGIfNeeded(ctx, lvg)
	if err != nil {
		return controller.Result{}, err
	}

	if deleted {
		log.Info("the LVMVolumeGroup was deleted, stop the reconciliation")
		return controller.Result{}, nil
	}

	if _, exist := lvg.Labels[internal.LVGUpdateTriggerLabel]; exist {
		delete(lvg.Labels, internal.LVGUpdateTriggerLabel)
		err = r.cl.Update(ctx, lvg)
		if err != nil {
			log.Error(err, "unable to update the LVMVolumeGroup")
			return controller.Result{}, err
		}
		log.Debug("successfully removed the label from the LVMVolumeGroup", "label", internal.LVGUpdateTriggerLabel)
	}

	log.Debug("tries to get block device resources for the LVMVolumeGroup by the selector",
		"selector", lvg.Spec.BlockDeviceSelector)
	blockDevices, err := r.bdCl.GetAPIBlockDevices(ctx, ReconcilerName, lvg.Spec.BlockDeviceSelector)
	if err != nil {
		log.Error(err, "unable to get BlockDevices", "retryIn", r.cfg.BlockDeviceScanInterval)
		err = r.lvgCl.UpdateLVGConditionIfNeeded(
			ctx,
			lvg,
			v1.ConditionFalse,
			internal.TypeVGConfigurationApplied,
			"NoBlockDevices",
			fmt.Sprintf("unable to get block devices resources, err: %s", err.Error()),
		)
		if err != nil {
			log.Error(err, "unable to add a condition to the LVMVolumeGroup",
				"conditionType", internal.TypeVGConfigurationApplied,
				"retryIn", r.cfg.BlockDeviceScanInterval)
		}

		return controller.Result{RequeueAfter: r.cfg.BlockDeviceScanInterval}, nil
	}
	log.Debug("successfully got block device resources for the LVMVolumeGroup by the selector",
		"selector", lvg.Spec.BlockDeviceSelector)

	blockDevices = filterBlockDevicesByNodeName(blockDevices, lvg.Spec.Local.NodeName)

	valid, reason := validateSpecBlockDevices(lvg, blockDevices)
	if !valid {
		log.Warning("validation failed for the LVMVolumeGroup",
			"reason", reason)
		err = r.lvgCl.UpdateLVGConditionIfNeeded(
			ctx,
			lvg,
			v1.ConditionFalse,
			internal.TypeVGConfigurationApplied,
			internal.ReasonValidationFailed,
			reason,
		)
		if err != nil {
			log.Error(err, "unable to add a condition to the LVMVolumeGroup",
				"conditionType", internal.TypeVGConfigurationApplied,
				"retryIn", r.cfg.VolumeGroupScanInterval)
			return controller.Result{}, err
		}

		return controller.Result{}, nil
	}
	log.Debug("successfully validated BlockDevices of the LVMVolumeGroup")

	log.Debug("tries to add label to the LVMVolumeGroup", "labelKey", internal.LVGMetadataNameLabelKey)
	added, err = r.addLVGLabelIfNeeded(ctx, lvg, internal.LVGMetadataNameLabelKey, lvg.Name)
	if err != nil {
		log.Error(err, "unable to add label to the LVMVolumeGroup", "labelKey", internal.LVGMetadataNameLabelKey)
		return controller.Result{}, err
	}

	if added {
		log.Debug("successfully added label to the LVMVolumeGroup", "labelKey", internal.LVGMetadataNameLabelKey)
	} else {
		log.Debug("no need to add label to the LVMVolumeGroup", "labelKey", internal.LVGMetadataNameLabelKey)
	}

	// We do this after BlockDevices validation and node belonging check to prevent multiple updates by all agents pods
	bds, _ := r.sdsCache.GetDevices()
	if len(bds) == 0 {
		log.Warning("no block devices in the cache, add the LVMVolumeGroup to requeue")
		err = r.lvgCl.UpdateLVGConditionIfNeeded(
			ctx,
			lvg,
			v1.ConditionFalse,
			internal.TypeVGConfigurationApplied,
			"CacheEmpty",
			"unable to apply configuration due to the cache's state",
		)
		if err != nil {
			log.Error(err, "unable to add a condition to the LVMVolumeGroup",
				"conditionType", internal.TypeVGConfigurationApplied,
				"retryIn", r.cfg.VolumeGroupScanInterval)
		}

		return controller.Result{
			RequeueAfter: r.cfg.VolumeGroupScanInterval,
		}, nil
	}

	log.Debug("tries to sync status and spec thin-pool AllocationLimit fields for the LVMVolumeGroup")
	err = r.syncThinPoolsAllocationLimit(ctx, lvg)
	if err != nil {
		log.Error(err, "unable to sync status and spec thin-pool AllocationLimit fields for the LVMVolumeGroup")
		return controller.Result{}, err
	}

	shouldRequeue, err := r.runEventReconcile(ctx, lvg, blockDevices)
	if err != nil {
		log.Error(err, "unable to reconcile the LVMVolumeGroup")
	}

	if shouldRequeue {
		log.Warning("the LVMVolumeGroup event will be requeued", "requeueIn", r.cfg.VolumeGroupScanInterval)
		return controller.Result{
			RequeueAfter: r.cfg.VolumeGroupScanInterval,
		}, nil
	}
	log.Info("Reconciler successfully reconciled the LVMVolumeGroup")

	return controller.Result{}, nil
}

func (r *Reconciler) runEventReconcile(
	ctx context.Context,
	lvg *v1alpha1.LVMVolumeGroup,
	blockDevices map[string]v1alpha1.BlockDevice,
) (bool, error) {
	log := r.log.WithName("runEventReconcile").WithValues("lvgName", lvg.Name)
	recType := r.identifyLVGReconcileFunc(lvg)

	switch recType {
	case internal.CreateReconcile:
		log.Info("CreateReconcile starts the reconciliation for the LVMVolumeGroup")
		return r.reconcileLVGCreateFunc(ctx, lvg, blockDevices)
	case internal.UpdateReconcile:
		log.Info("UpdateReconcile starts the reconciliation for the LVMVolumeGroup")
		return r.reconcileLVGUpdateFunc(ctx, lvg, blockDevices)
	case internal.DeleteReconcile:
		log.Info("DeleteReconcile starts the reconciliation for the LVMVolumeGroup")
		return r.reconcileLVGDeleteFunc(ctx, lvg)
	default:
		log.Info("no need to reconcile the LVMVolumeGroup")
	}
	return false, nil
}

func (r *Reconciler) reconcileLVGDeleteFunc(ctx context.Context, lvg *v1alpha1.LVMVolumeGroup) (bool, error) {
	log := r.log.WithName("reconcileLVGDeleteFunc").WithValues("lvgName", lvg.Name)
	log.Debug("starts to reconcile the LVMVolumeGroup")
	log.Debug("tries to add the condition status false to the LVMVolumeGroup", "conditionType", internal.TypeVGConfigurationApplied)

	// this check prevents the LVMVolumeGroup resource's infinity updating after a retry
	for _, c := range lvg.Status.Conditions {
		if c.Type == internal.TypeVGConfigurationApplied && c.Reason != internal.ReasonTerminating {
			err := r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonTerminating, "trying to delete VG")
			if err != nil {
				log.Error(err, "unable to add the condition to the LVMVolumeGroup", "conditionType", internal.TypeVGConfigurationApplied)
				return true, err
			}
			break
		}
	}

	_, exist := lvg.Annotations[internal.DeletionProtectionAnnotation]
	if exist {
		log.Debug("the LVMVolumeGroup has a deletion timestamp but also has a deletion protection annotation. Remove it to proceed the delete operation",
			"annotation", internal.DeletionProtectionAnnotation)
		err := r.lvgCl.UpdateLVGConditionIfNeeded(
			ctx,
			lvg,
			v1.ConditionFalse,
			internal.TypeVGConfigurationApplied,
			internal.ReasonTerminating,
			fmt.Sprintf("to delete the LVG remove the annotation %s", internal.DeletionProtectionAnnotation),
		)
		if err != nil {
			log.Error(err, "unable to add the condition to the LVMVolumeGroup",
				"conditionType", internal.TypeVGConfigurationApplied)
			return true, err
		}

		return false, nil
	}

	log.Debug("check if VG of the LVMVolumeGroup uses LVs", "vgName", lvg.Spec.ActualVGNameOnTheNode)
	usedLVs := r.getLVForVG(lvg.Spec.ActualVGNameOnTheNode)
	if len(usedLVs) > 0 {
		err := fmt.Errorf("VG %s uses LVs: %v. Delete used LVs first", lvg.Spec.ActualVGNameOnTheNode, usedLVs)
		log.Error(err, "unable to reconcile LVG")
		log.Debug("tries to add the condition status False to the LVMVolumeGroup due to LV does exist",
			"conditionType", internal.TypeVGConfigurationApplied)
		err = r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonTerminating, err.Error())
		if err != nil {
			log.Error(err, "unable to add the condition to the LVMVolumeGroup",
				"conditionType", internal.TypeVGConfigurationApplied)
			return true, err
		}

		return true, nil
	}

	log.Debug("VG of the LVMVolumeGroup does not use any LV. Start to delete the VG",
		"vgName", lvg.Spec.ActualVGNameOnTheNode)
	err := r.deleteVGIfExist(lvg.Spec.ActualVGNameOnTheNode)
	if err != nil {
		log.Error(err, "unable to delete VG",
			"vgName", lvg.Spec.ActualVGNameOnTheNode)
		err = r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonTerminating, err.Error())
		if err != nil {
			log.Error(err, "unable to add the condition to the LVMVolumeGroup",
				"conditionType", internal.TypeVGConfigurationApplied)
			return true, err
		}

		return true, err
	}

	removed, err := r.removeLVGFinalizerIfExist(ctx, lvg)
	if err != nil {
		log.Error(err, "unable to remove a finalizer from the LVMVolumeGroup",
			"finalizer", internal.SdsNodeConfiguratorFinalizer)
		err = r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonTerminating, err.Error())
		if err != nil {
			log.Error(err, "unable to add the condition to the LVMVolumeGroup",
				"conditionType", internal.TypeVGConfigurationApplied)
		}
		return true, err
	}

	if removed {
		log.Debug("successfully removed a finalizer from the LVMVolumeGroup",
			"finalizer", internal.SdsNodeConfiguratorFinalizer)
	} else {
		log.Debug("no need to remove a finalizer from the LVMVolumeGroup",
			"finalizer", internal.SdsNodeConfiguratorFinalizer)
	}

	err = r.lvgCl.DeleteLVMVolumeGroup(ctx, lvg)
	if err != nil {
		log.Error(err, "unable to delete the LVMVolumeGroup")
		return true, err
	}

	log.Info("successfully reconciled VG of the LVMVolumeGroup", "vgName", lvg.Spec.ActualVGNameOnTheNode)
	return false, nil
}

func (r *Reconciler) reconcileLVGUpdateFunc(
	ctx context.Context,
	lvg *v1alpha1.LVMVolumeGroup,
	blockDevices map[string]v1alpha1.BlockDevice,
) (bool, error) {
	log := r.log.WithName("reconcileLVGUpdateFunc").WithValues("lvgName", lvg.Name)
	log.Debug("starts to reconcile the LVMVolumeGroup")

	log.Debug("tries to validate the LVMVolumeGroup")
	pvs, _ := r.sdsCache.GetPVs()
	valid, reason := r.validateLVGForUpdateFunc(lvg, blockDevices)
	if !valid {
		log.Warning("the LVMVolumeGroup is not valid", "reason", reason)
		err := r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonValidationFailed, reason)
		if err != nil {
			log.Error(err, "unable to add a condition to the LVMVolumeGroup",
				"conditionType", internal.TypeVGConfigurationApplied,
				"reason", internal.ReasonValidationFailed)
		}

		return true, err
	}
	log.Debug("successfully validated the LVMVolumeGroup")

	log.Debug("tries to get VG for the LVMVolumeGroup", "vgName", lvg.Spec.ActualVGNameOnTheNode)
	found, vg := r.tryGetVG(lvg.Spec.ActualVGNameOnTheNode)
	if !found {
		err := fmt.Errorf("VG %s not found", lvg.Spec.ActualVGNameOnTheNode)
		log.Error(err, "unable to reconcile the LVMVolumeGroup")
		err = r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, "VGNotFound", err.Error())
		if err != nil {
			log.Error(err, "unable to add a condition to the LVMVolumeGroup",
				"conditionType", internal.TypeVGConfigurationApplied)
		}
		return true, err
	}
	log = log.WithValues("vgName", vg.VGName)
	log.Debug("VG found for the LVMVolumeGroup")

	log.Debug("tries to check and update VG tag", "vgName", lvg.Spec.ActualVGNameOnTheNode, "tags", internal.LVMTags)
	updated, err := r.updateVGTagIfNeeded(ctx, lvg, vg)
	if err != nil {
		log.Error(err, "unable to update VG tag of the LVMVolumeGroup")
		err = r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, "VGUpdateFailed", fmt.Sprintf("unable to update VG tag, err: %s", err.Error()))
		if err != nil {
			log.Error(err, "unable to add a condition to the LVMVolumeGroup",
				"conditionType", internal.TypeVGConfigurationApplied)
		}

		return true, err
	}

	if updated {
		log.Debug("successfully updated VG tag of the LVMVolumeGroup")
	} else {
		log.Debug("no need to update VG tag of the LVMVolumeGroup")
	}

	log.Debug("starts to resize PV of the LVMVolumeGroup")
	err = r.resizePVIfNeeded(ctx, lvg)
	if err != nil {
		log.Error(err, "unable to resize PV of the LVMVolumeGroup")
		err = r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, "PVResizeFailed", fmt.Sprintf("unable to resize PV, err: %s", err.Error()))
		if err != nil {
			log.Error(err, "unable to add a condition to the LVMVolumeGroup",
				"conditionType", internal.TypeVGConfigurationApplied)
		}
		return true, err
	}
	log.Debug("successfully ended the resize operation for PV of the LVMVolumeGroup")

	log.Debug("starts to extend VG of the LVMVolumeGroup")
	err = r.extendVGIfNeeded(ctx, lvg, vg, pvs, blockDevices)
	if err != nil {
		log.Error(err, "unable to extend VG of the LVMVolumeGroup")
		err = r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, "VGExtendFailed", fmt.Sprintf("unable to extend VG, err: %s", err.Error()))
		if err != nil {
			log.Error(err, "unable to add a condition to the LVMVolumeGroup",
				"conditionType", internal.TypeVGConfigurationApplied)
		}

		return true, err
	}
	log.Debug("successfully ended the extend operation for VG of the LVMVolumeGroup")

	if lvg.Spec.ThinPools != nil {
		log.Debug("starts to reconcile thin-pools of the LVMVolumeGroup")
		lvs, _ := r.sdsCache.GetLVs()
		err = r.reconcileThinPoolsIfNeeded(ctx, lvg, vg, lvs)
		if err != nil {
			log.Error(err, "unable to reconcile thin-pools of the LVMVolumeGroup")
			err = r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, "ThinPoolReconcileFailed", fmt.Sprintf("unable to reconcile thin-pools, err: %s", err.Error()))
			if err != nil {
				log.Error(err, "unable to add a condition to the LVMVolumeGroup", "conditionType", internal.TypeVGConfigurationApplied)
			}
			return true, err
		}
		log.Debug("successfully reconciled thin-pools operation of the LVMVolumeGroup")
	}

	log.Debug("tries to add a condition to the LVMVolumeGroup", "conditionType", internal.TypeVGConfigurationApplied)
	err = r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionTrue, internal.TypeVGConfigurationApplied, internal.ReasonApplied, "configuration has been applied")
	if err != nil {
		log.Error(err, "unable to add a condition to the LVMVolumeGroup",
			"conditionType", internal.TypeVGConfigurationApplied)
		return true, err
	}
	log.Debug("successfully added a condition to the LVMVolumeGroup", "conditionType", internal.TypeVGConfigurationApplied)
	log.Info("successfully reconciled the LVMVolumeGroup")

	return false, nil
}

func (r *Reconciler) reconcileLVGCreateFunc(
	ctx context.Context,
	lvg *v1alpha1.LVMVolumeGroup,
	blockDevices map[string]v1alpha1.BlockDevice,
) (bool, error) {
	log := r.log.WithName("reconcileLVGCreateFunc").WithValues("lvgName", lvg.Name)
	log.Debug("starts to reconcile the LVMVolumeGroup")

	// this check prevents the LVMVolumeGroup resource's infinity updating after a retry
	exist := false
	for _, c := range lvg.Status.Conditions {
		if c.Type == internal.TypeVGConfigurationApplied {
			exist = true
			break
		}
	}

	if !exist {
		log.Debug("tries to add the condition to the LVMVolumeGroup",
			"conditionType", internal.TypeVGConfigurationApplied)
		err := r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonCreating, "trying to apply the configuration")
		if err != nil {
			log.Error(err, "unable to add the condition to the LVMVolumeGroup",
				"conditionType", internal.TypeVGConfigurationApplied)
			return true, err
		}
	}

	log.Debug("tries to validate the LVMVolumeGroup")
	valid, reason := r.validateLVGForCreateFunc(lvg, blockDevices)
	if !valid {
		log.Warning("validation fails for the LVMVolumeGroup")
		err := r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonValidationFailed, reason)
		if err != nil {
			log.Error(err, "unable to add a condition to the LVMVolumeGroup",
				"conditionType", internal.TypeVGConfigurationApplied)
		}

		return true, err
	}
	log.Debug("successfully validated the LVMVolumeGroup")

	log.Debug("tries to create VG for the LVMVolumeGroup")
	err := r.createVGComplex(lvg, blockDevices)
	if err != nil {
		log.Error(err, "unable to create VG for the LVMVolumeGroup")
		err = r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, "VGCreationFailed", fmt.Sprintf("unable to create VG, err: %s", err.Error()))
		if err != nil {
			log.Error(err, "unable to add a condition to the LVMVolumeGroup",
				"conditionType", internal.TypeVGConfigurationApplied)
		}
		return true, err
	}
	log.Info("successfully created VG for the LVMVolumeGroup")

	if lvg.Spec.ThinPools != nil {
		log.Debug("the LVMVolumeGroup has thin-pools. Tries to create them")

		for _, tp := range lvg.Spec.ThinPools {
			log := log.WithValues("thinPoolName", tp.Name)
			vgSize := countVGSizeByBlockDevices(blockDevices)
			tpRequestedSize, err := utils.GetRequestedSizeFromString(tp.Size, vgSize)
			if err != nil {
				log.Error(err, "unable to get thin-pool requested size of the LVMVolumeGroup")
				return false, err
			}

			var cmd string
			if utils.AreSizesEqualWithinDelta(tpRequestedSize, vgSize, internal.ResizeDelta) {
				log.Debug("Thin-pool of the LVMVolumeGroup will be created with full VG space size")
				cmd, err = r.commands.CreateThinPoolFullVGSpace(tp.Name, lvg.Spec.ActualVGNameOnTheNode)
			} else {
				log.Debug("Thin-pool of the LVMVolumeGroup will be created with size", "size", tpRequestedSize)
				cmd, err = r.commands.CreateThinPool(tp.Name, lvg.Spec.ActualVGNameOnTheNode, tpRequestedSize.Value())
			}
			if err != nil {
				log.Error(err, "unable to create thin-pool of the LVMVolumeGroup", "cmd", cmd)
				err = r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, "ThinPoolCreationFailed", fmt.Sprintf("unable to create thin-pool, err: %s", err.Error()))
				if err != nil {
					log.Error(err, "unable to add a condition to the LVMVolumeGroup",
						"conditionType", internal.TypeVGConfigurationApplied)
				}

				return true, err
			}
		}
		log.Debug("successfully created thin-pools for the LVMVolumeGroup")
	}

	err = r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionTrue, internal.TypeVGConfigurationApplied, internal.ReasonApplied, "all configuration has been applied")
	if err != nil {
		log.Error(err, "unable to add a condition to the LVMVolumeGroup",
			"conditionType", internal.TypeVGConfigurationApplied)
		return true, err
	}

	return false, nil
}

func (r *Reconciler) shouldUpdateLVGLabels(lvg *v1alpha1.LVMVolumeGroup, labelKey, labelValue string) bool {
	log := r.log.WithName("shouldUpdateLVGLabels").WithValues("lvgName", lvg.Name, "labelKey", labelKey)
	if lvg.Labels == nil {
		log.Debug("the LVMVolumeGroup has no labels")
		return true
	}

	val, exist := lvg.Labels[labelKey]
	if !exist {
		log.Debug("the LVMVolumeGroup has no label")
		return true
	}

	if val != labelValue {
		log.Debug("the LVMVolumeGroup has label but the value is incorrect",
			"currentValue", val,
			"expectedValue", labelValue)
		return true
	}

	return false
}

func (r *Reconciler) shouldLVGWatcherReconcileUpdateEvent(oldLVG, newLVG *v1alpha1.LVMVolumeGroup) bool {
	log := r.log.WithName("shouldLVGWatcherReconcileUpdateEvent").WithValues("lvgName", newLVG.Name)
	if newLVG.DeletionTimestamp != nil {
		log.Debug("update event should be reconciled as the LVMVolumeGroup has deletionTimestamp")
		return true
	}

	for _, c := range newLVG.Status.Conditions {
		if c.Type == internal.TypeVGConfigurationApplied {
			if c.Reason == internal.ReasonUpdating || c.Reason == internal.ReasonCreating {
				log.Debug("update event should not be reconciled as the LVMVolumeGroup reconciliation still in progress")
				return false
			}
		}
	}

	if _, exist := newLVG.Labels[internal.LVGUpdateTriggerLabel]; exist {
		log.Debug("update event should be reconciled as the LVMVolumeGroup has the label", "label", internal.LVGUpdateTriggerLabel)
		return true
	}

	if r.shouldUpdateLVGLabels(newLVG, internal.LVGMetadataNameLabelKey, newLVG.Name) {
		log.Debug("update event should be reconciled as the LVMVolumeGroup's labels have been changed")
		return true
	}

	if !reflect.DeepEqual(oldLVG.Spec, newLVG.Spec) {
		log.Debug("update event should be reconciled as the LVMVolumeGroup configuration has been changed")
		return true
	}

	for _, n := range newLVG.Status.Nodes {
		for _, d := range n.Devices {
			if !utils.AreSizesEqualWithinDelta(d.PVSize, d.DevSize, internal.ResizeDelta) {
				log.Debug("update event should be reconciled as the LVMVolumeGroup PV size is different to device size")
				return true
			}
		}
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
	log := r.log.WithName("syncThinPoolsAllocationLimit").WithValues("lvgName", lvg.Name)
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
		log := log.WithValues("thinPoolName", lvg.Status.ThinPools[i].Name)
		if specLimits, matched := tpSpecLimits[lvg.Status.ThinPools[i].Name]; matched {
			if lvg.Status.ThinPools[i].AllocationLimit != specLimits {
				log.Debug("thin-pool status AllocationLimit should be updated by spec one",
					"currentLimit", lvg.Status.ThinPools[i].AllocationLimit,
					"specLimit", specLimits)
				updated = true
				lvg.Status.ThinPools[i].AllocationLimit = specLimits

				space, err = utils.GetThinPoolAvailableSpace(lvg.Status.ThinPools[i].ActualSize, lvg.Status.ThinPools[i].AllocatedSize, specLimits)
				if err != nil {
					log.Error(err, "unable to get thin pool available space")
					return err
				}
				log.Debug("successfully got a new available space of the thin-pool",
					"availableSpace", space)
				lvg.Status.ThinPools[i].AvailableSpace = space
			}
		} else {
			log.Debug("status thin-pool of the LVMVolumeGroup was not found as used in spec")
		}
	}

	if updated {
		fmt.Printf("%+v", lvg.Status.ThinPools)
		log.Debug("tries to update the LVMVolumeGroup")
		err = r.cl.Status().Update(ctx, lvg)
		if err != nil {
			return err
		}
		log.Debug("successfully updated the LVMVolumeGroup")
	} else {
		log.Debug("every status thin-pool AllocationLimit value is synced with spec one for the LVMVolumeGroup")
	}

	return nil
}

func (r *Reconciler) deleteLVGIfNeeded(ctx context.Context, lvg *v1alpha1.LVMVolumeGroup) (bool, error) {
	log := r.log.WithName("deleteLVGIfNeeded").WithValues("lvgName", lvg.Name)
	if lvg.DeletionTimestamp == nil {
		return false, nil
	}

	vgs, _ := r.sdsCache.GetVGs()
	if !checkIfVGExist(lvg.Spec.ActualVGNameOnTheNode, vgs) {
		log.Info("VG was not yet created for the LVMVolumeGroup and the resource is marked as deleting. Delete the resource",
			"vgName", lvg.Spec.ActualVGNameOnTheNode)
		removed, err := r.removeLVGFinalizerIfExist(ctx, lvg)
		if err != nil {
			log.Error(err, "unable to remove the finalizer from the LVMVolumeGroup",
				"finalizer", internal.SdsNodeConfiguratorFinalizer)
			return false, err
		}

		if removed {
			log.Debug("successfully removed the finalizer from the LVMVolumeGroup",
				"finalizer", internal.SdsNodeConfiguratorFinalizer)
		} else {
			log.Debug("no need to remove the finalizer from the LVMVolumeGroup",
				"finalizer", internal.SdsNodeConfiguratorFinalizer)
		}

		err = r.lvgCl.DeleteLVMVolumeGroup(ctx, lvg)
		if err != nil {
			log.Error(err, "unable to delete the LVMVolumeGroup")
			return false, err
		}
		log.Info("successfully deleted the LVMVolumeGroup")
		return true, nil
	}
	return false, nil
}

func (r *Reconciler) validateLVGForCreateFunc(
	lvg *v1alpha1.LVMVolumeGroup,
	blockDevices map[string]v1alpha1.BlockDevice,
) (bool, string) {
	log := r.log.WithName("validateLVGForCreateFunc").WithValues("lvgName", lvg.Name)
	reason := strings.Builder{}

	log.Debug("check if every selected BlockDevice of the LVMVolumeGroup is consumable")
	// totalVGSize needs to count if there is enough space for requested thin-pools
	totalVGSize := countVGSizeByBlockDevices(blockDevices)
	for _, bd := range blockDevices {
		if !bd.Status.Consumable {
			log.Warning("BlockDevice is not consumable", "blockDeviceName", bd.Name)
			log.Trace("BlockDevice", "name", bd.Name, "status", bd.Status)
			reason.WriteString(fmt.Sprintf("BlockDevice %s is not consumable. ", bd.Name))
		}
	}

	if reason.Len() == 0 {
		log.Debug("all BlockDevices of the LVMVolumeGroup are consumable")
	}

	if lvg.Spec.ThinPools != nil {
		log.Debug("the LVMVolumeGroup has thin-pools. Validate if VG size has enough space for the thin-pools")
		log.Trace("the LVMVolumeGroup has thin-pools and total size",
			"thinPools", lvg.Spec.ThinPools,
			"totalSize", totalVGSize)

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

			// means a user want a thin-pool with 100%FREE size
			if utils.AreSizesEqualWithinDelta(tpRequestedSize, totalVGSize, internal.ResizeDelta) {
				if len(lvg.Spec.ThinPools) > 1 {
					reason.WriteString(fmt.Sprintf("Thin-pool %s requested size of full VG space, but there is any other thin-pool. ", tp.Name))
				}
			}

			totalThinPoolSize += tpRequestedSize.Value()
		}
		log.Trace("LVMVolumeGroup thin-pools requested space", "requestedSpace", totalThinPoolSize)

		if totalThinPoolSize != totalVGSize.Value() && totalThinPoolSize+internal.ResizeDelta.Value() >= totalVGSize.Value() {
			log.Trace("total thin pool size and total vg size",
				"totalThinPoolSize", resource.NewQuantity(totalThinPoolSize, resource.BinarySI),
				"totalVGSize", totalVGSize)
			log.Warning("requested thin pool size is more than VG total size for the LVMVolumeGroup")
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
	log := r.log.WithName("validateLVGForUpdateFunc").WithValues("lvgName", lvg.Name)
	reason := strings.Builder{}
	pvs, _ := r.sdsCache.GetPVs()
	log.Debug("check if every new BlockDevice of the LVMVolumeGroup is consumable")
	actualPVPaths := make(map[string]struct{}, len(pvs))
	for _, pv := range pvs {
		actualPVPaths[pv.PVName] = struct{}{}
	}

	//TODO: add a check if BlockDevice size got less than PV size

	// Check if added BlockDevices are consumable
	// additionBlockDeviceSpace value is needed to count if VG will have enough space for thin-pools
	var additionBlockDeviceSpace int64
	for _, bd := range blockDevices {
		log := log.WithValues("blockDeviceName", bd.Name, "pvPath", bd.Status.Path)
		if _, found := actualPVPaths[bd.Status.Path]; !found {
			log.Debug("unable to find the PV for BlockDevice. Check if the BlockDevice is already used")
			for _, n := range lvg.Status.Nodes {
				for _, d := range n.Devices {
					if d.BlockDevice == bd.Name {
						log.Warning("BlockDevice misses the PV. That might be because the corresponding device was removed from the node. Unable to validate BlockDevices")
						reason.WriteString(fmt.Sprintf("BlockDevice %s misses the PV %s (that might be because the device was removed from the node). ", bd.Name, bd.Status.Path))
					}

					if reason.Len() == 0 {
						log.Debug("BlockDevice does not miss a PV")
					}
				}
			}

			log.Debug("PV for BlockDevice of the LVMVolumeGroup is not created yet, check if the BlockDevice is consumable")
			if reason.Len() > 0 {
				log.Debug("some BlockDevices misses its PVs, unable to check if they are consumable")
				continue
			}

			if !bd.Status.Consumable {
				reason.WriteString(fmt.Sprintf("BlockDevice %s is not consumable. ", bd.Name))
				continue
			}

			log.Debug("BlockDevice is consumable")
			additionBlockDeviceSpace += bd.Status.Size.Value()
		}
	}

	if lvg.Spec.ThinPools != nil {
		log.Debug("the LVMVolumeGroup has thin-pools. Validate them")
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
			log := log.WithValues("thinPoolName", specTp.Name)
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

			log.Debug("the LVMVolumeGroup thin-pool requested size and Status VG size",
				"requestedSize", tpRequestedSize,
				"statusVGSize", lvg.Status.VGSize)
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
					log.Debug("thin-pool of the LVMVolumeGroup is not yet created, adds its requested size")
					addingThinPoolSize += tpRequestedSize.Value()
				} else {
					log.Debug("thin-pool of the LVMVolumeGroup is already created, check its requested size")
					if tpRequestedSize.Value()+internal.ResizeDelta.Value() < actualThinPool.LVSize.Value() {
						log.Debug("the LVMVolumeGroup Spec.ThinPool size is less than Status one",
							"requestedSize", tpRequestedSize,
							"actualSize", actualThinPool.LVSize)
						reason.WriteString(fmt.Sprintf("Requested Spec.ThinPool %s size %s is less than actual one %s. ", specTp.Name, tpRequestedSize.String(), actualThinPool.LVSize.String()))
						continue
					}

					thinPoolSizeDiff := tpRequestedSize.Value() - actualThinPool.LVSize.Value()
					if thinPoolSizeDiff > internal.ResizeDelta.Value() {
						log.Debug("the LVMVolumeGroup Spec.ThinPool size more than Status one",
							"requestedSize", tpRequestedSize,
							"actualSize", actualThinPool.LVSize)
						addingThinPoolSize += thinPoolSizeDiff
					}
				}
			}
		}

		if !hasFullThinPool {
			allocatedSize := getVGAllocatedSize(*vg)
			totalFreeSpace := newTotalVGSize.Value() - allocatedSize.Value()
			log.Trace("new LVMVolumeGroup thin-pools requested size, additional BlockDevices space, total",
				"addingThinPoolSize", addingThinPoolSize,
				"additionBlockDeviceSpace", additionBlockDeviceSpace,
				"totalFreeSpace", totalFreeSpace)
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
	log := r.log.WithName("reconcileThinPoolsIfNeeded").WithValues("lvgName", lvg.Name)
	actualThinPools := make(map[string]internal.LVData, len(lvs))
	for _, lv := range lvs {
		if string(lv.LVAttr[0]) == "t" {
			actualThinPools[lv.LVName] = lv
		}
	}

	errs := strings.Builder{}
	for _, specTp := range lvg.Spec.ThinPools {
		log := log.WithValues("thinPoolName", specTp.Name)
		tpRequestedSize, err := utils.GetRequestedSizeFromString(specTp.Size, lvg.Status.VGSize)
		if err != nil {
			log.Error(err, "unable to get requested thin-pool size of the LVMVolumeGroup")
			return err
		}

		if actualTp, exist := actualThinPools[specTp.Name]; !exist {
			log.Debug("thin-pool of the LVMVolumeGroup is not created yet. Create it")
			if isApplied(lvg) {
				err := r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonUpdating, "trying to apply the configuration")
				if err != nil {
					log.Error(err, "unable to add the condition status False reason to the LVMVolumeGroup",
						"conditionType", internal.TypeVGConfigurationApplied,
						"reason", internal.ReasonUpdating)
					return err
				}
			}

			var cmd string
			start := time.Now()
			if utils.AreSizesEqualWithinDelta(tpRequestedSize, lvg.Status.VGSize, internal.ResizeDelta) {
				log.Debug("thin-pool of the LVMVolumeGroup will be created with size 100FREE")
				cmd, err = r.commands.CreateThinPoolFullVGSpace(specTp.Name, vg.VGName)
			} else {
				log.Debug("thin-pool of the LVMVolumeGroup will be created with size",
					"size", tpRequestedSize)
				cmd, err = r.commands.CreateThinPool(specTp.Name, vg.VGName, tpRequestedSize.Value())
			}
			r.metrics.UtilsCommandsDuration(ReconcilerName, "lvcreate").Observe(r.metrics.GetEstimatedTimeInSeconds(start))
			r.metrics.UtilsCommandsExecutionCount(ReconcilerName, "lvcreate").Inc()
			if err != nil {
				r.metrics.UtilsCommandsErrorsCount(ReconcilerName, "lvcreate").Inc()
				log.Error(err, "unable to create thin-pool of the LVMVolumeGroup",
					"cmd", cmd)
				errs.WriteString(fmt.Sprintf("unable to create thin-pool %s, err: %s. ", specTp.Name, err.Error()))
				continue
			}

			log.Info("thin-pool of the LVMVolumeGroup has been successfully created")
		} else {
			// thin-pool exists
			if utils.AreSizesEqualWithinDelta(tpRequestedSize, actualTp.LVSize, internal.ResizeDelta) {
				log.Debug("the LVMVolumeGroup requested thin pool size is equal to actual one",
					"requestedSize", tpRequestedSize)
				continue
			}

			log.Debug("the LVMVolumeGroup requested thin pool size is more than actual one. Resize it",
				"requestedSize", tpRequestedSize.String())
			if isApplied(lvg) {
				err = r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonUpdating, "trying to apply the configuration")
				if err != nil {
					log.Error(err, "unable to add the condition status False reason to the LVMVolumeGroup",
						"conditionType", internal.TypeVGConfigurationApplied,
						"reason", internal.ReasonUpdating)
					return err
				}
			}
			err = r.extendThinPool(lvg, specTp)
			if err != nil {
				log.Error(err, "unable to resize thin-pool of the LVMVolumeGroup")
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
	log := r.log.WithName("resizePVIfNeeded").WithValues("lvgName", lvg.Name)
	if len(lvg.Status.Nodes) == 0 {
		log.Warning("the LVMVolumeGroup nodes are empty. Wait for the next update")
		return nil
	}

	errs := strings.Builder{}
	for _, n := range lvg.Status.Nodes {
		for _, d := range n.Devices {
			log := log.WithValues("blockDeviceName", d.BlockDevice, "pvPath", d.Path)
			if d.DevSize.Value()-d.PVSize.Value() > internal.ResizeDelta.Value() {
				if isApplied(lvg) {
					err := r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonUpdating, "trying to apply the configuration")
					if err != nil {
						log.Error(err, "unable to add the condition status False reason to the LVMVolumeGroup",
							"conditionType", internal.TypeVGConfigurationApplied,
							"reason", internal.ReasonUpdating)
						return err
					}
				}

				log.Debug("the LVMVolumeGroup BlockDevice PVSize is less than actual device size. Resize PV")

				start := time.Now()
				cmd, err := r.commands.ResizePV(d.Path)
				r.metrics.UtilsCommandsDuration(ReconcilerName, "pvresize").Observe(r.metrics.GetEstimatedTimeInSeconds(start))
				r.metrics.UtilsCommandsExecutionCount(ReconcilerName, "pvresize")
				if err != nil {
					r.metrics.UtilsCommandsErrorsCount(ReconcilerName, "pvresize").Inc()
					log.Error(err, "unable to resize PV of BlockDevice of LVMVolumeGroup",
						"cmd", cmd)
					errs.WriteString(fmt.Sprintf("unable to resize PV %s, err: %s. ", d.Path, err.Error()))
					continue
				}

				log.Info("successfully resized PV of BlockDevice of LVMVolumeGroup")
			} else {
				log.Debug("no need to resize PV of BlockDevice of the LVMVolumeGroup")
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
	log := r.log.WithName("extendVGIfNeeded").WithValues("lvgName", lvg.Name, "vgName", vg.VGName)
	for _, n := range lvg.Status.Nodes {
		for _, d := range n.Devices {
			log := log.WithValues("blockDeviceName", d.BlockDevice)
			log.Trace("the LVMVolumeGroup status block device")
		}
	}

	pvsMap := make(map[string]struct{}, len(pvs))
	for _, pv := range pvs {
		pvsMap[pv.PVName] = struct{}{}
	}

	devicesToExtend := make([]string, 0, len(blockDevices))
	for _, bd := range blockDevices {
		if _, exist := pvsMap[bd.Status.Path]; !exist {
			log.Debug("the BlockDevice of LVMVolumeGroup Spec is not counted as used", "blockDeviceName", bd.Name)
			devicesToExtend = append(devicesToExtend, bd.Name)
		}
	}

	if len(devicesToExtend) == 0 {
		log.Debug("VG of the LVMVolumeGroup should not be extended")
		return nil
	}

	if isApplied(lvg) {
		err := r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonUpdating, "trying to apply the configuration")
		if err != nil {
			log.Error(err, "unable to add the condition status False reason to the LVMVolumeGroup",
				"conditionType", internal.TypeVGConfigurationApplied,
				"reason", internal.ReasonUpdating)
			return err
		}
	}

	log.Debug("VG should be extended as there are some BlockDevices were added to Spec field of the LVMVolumeGroup")
	paths := extractPathsFromBlockDevices(devicesToExtend, blockDevices)
	err := r.extendVGComplex(paths, vg.VGName)
	if err != nil {
		log.Error(err, "unable to extend VG of the LVMVolumeGroup")
		return err
	}
	log.Info("VG of the LVMVolumeGroup was extended")

	return nil
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
	log := r.log.WithName("deleteVGIfExist").WithValues("vgName", vgName)
	vgs, _ := r.sdsCache.GetVGs()
	if !checkIfVGExist(vgName, vgs) {
		log.Debug("no VG found, nothing to delete")
		return nil
	}

	pvs, _ := r.sdsCache.GetPVs()
	if len(pvs) == 0 {
		err := errors.New("no any PV found")
		log.Error(err, "no any PV was found while deleting VG")
		return err
	}

	start := time.Now()
	command, err := r.commands.RemoveVG(vgName)
	r.metrics.UtilsCommandsDuration(ReconcilerName, "vgremove").Observe(r.metrics.GetEstimatedTimeInSeconds(start))
	r.metrics.UtilsCommandsExecutionCount(ReconcilerName, "vgremove").Inc()
	if err != nil {
		r.metrics.UtilsCommandsErrorsCount(ReconcilerName, "vgremove").Inc()
		log.Error(err, "RemoveVG", "command", command)
		return err
	}
	log.Debug("VG was successfully deleted from the node", "command", command)
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
	if err != nil {
		r.metrics.UtilsCommandsErrorsCount(ReconcilerName, "pvremove").Inc()
		log.Error(err, "RemovePV", "command", command)
		return err
	}
	log.Debug("successfully delete PVs of VG from the node", "command", command)

	return nil
}

func (r *Reconciler) extendVGComplex(extendPVs []string, vgName string) error {
	log := r.log.WithName("extendVGComplex").WithValues("vgName", vgName)
	for _, pvPath := range extendPVs {
		start := time.Now()
		command, err := r.commands.CreatePV(pvPath)
		r.metrics.UtilsCommandsDuration(ReconcilerName, "pvcreate").Observe(r.metrics.GetEstimatedTimeInSeconds(start))
		r.metrics.UtilsCommandsExecutionCount(ReconcilerName, "pvcreate").Inc()
		log.Debug("CreatePV command", "command", command)
		if err != nil {
			r.metrics.UtilsCommandsErrorsCount(ReconcilerName, "pvcreate").Inc()
			log.Error(err, "CreatePV")
			return err
		}
	}

	start := time.Now()
	command, err := r.commands.ExtendVG(vgName, extendPVs)
	r.metrics.UtilsCommandsDuration(ReconcilerName, "vgextend").Observe(r.metrics.GetEstimatedTimeInSeconds(start))
	r.metrics.UtilsCommandsExecutionCount(ReconcilerName, "vgextend").Inc()
	log.Debug("ExtendVG command", "command", command)
	if err != nil {
		r.metrics.UtilsCommandsErrorsCount(ReconcilerName, "vgextend").Inc()
		log.Error(err, "ExtendVG")
		return err
	}
	return nil
}

func (r *Reconciler) createVGComplex(lvg *v1alpha1.LVMVolumeGroup, blockDevices map[string]v1alpha1.BlockDevice) error {
	log := r.log.WithName("createVGComplex").WithValues("lvgName", lvg.Name, "vgName", lvg.Spec.ActualVGNameOnTheNode)
	paths := extractPathsFromBlockDevices(nil, blockDevices)

	log.Trace("LVMVolumeGroup devices paths", "paths", paths)
	for _, path := range paths {
		log := log.WithValues("path", path)
		start := time.Now()
		command, err := r.commands.CreatePV(path)
		r.metrics.UtilsCommandsDuration(ReconcilerName, "pvcreate").Observe(r.metrics.GetEstimatedTimeInSeconds(start))
		r.metrics.UtilsCommandsExecutionCount(ReconcilerName, "pvcreate").Inc()
		log.Debug("CreatePV command", "command", command)
		if err != nil {
			r.metrics.UtilsCommandsErrorsCount(ReconcilerName, "pvcreate").Inc()
			log.Error(err, "unable to create PV by path")
			return err
		}
	}

	log.Debug("successfully created all PVs for the LVMVolumeGroup", "type", lvg.Spec.Type)
	switch lvg.Spec.Type {
	case internal.Local:
		start := time.Now()
		cmd, err := r.commands.CreateVGLocal(lvg.Spec.ActualVGNameOnTheNode, lvg.Name, paths)
		r.metrics.UtilsCommandsDuration(ReconcilerName, "vgcreate").Observe(r.metrics.GetEstimatedTimeInSeconds(start))
		r.metrics.UtilsCommandsExecutionCount(ReconcilerName, "vgcreate").Inc()
		log.Debug("CreateVGLocal command", "command", cmd)
		if err != nil {
			r.metrics.UtilsCommandsErrorsCount(ReconcilerName, "vgcreate").Inc()
			log.Error(err, "error CreateVGLocal")
			return err
		}
	case internal.Shared:
		start := time.Now()
		cmd, err := r.commands.CreateVGShared(lvg.Spec.ActualVGNameOnTheNode, lvg.Name, paths)
		r.metrics.UtilsCommandsDuration(ReconcilerName, "vgcreate").Observe(r.metrics.GetEstimatedTimeInSeconds(start))
		r.metrics.UtilsCommandsExecutionCount(ReconcilerName, "vgcreate").Inc()
		log.Debug("CreateVGShared command", "command", cmd)
		if err != nil {
			r.metrics.UtilsCommandsErrorsCount(ReconcilerName, "vgcreate").Inc()
			log.Error(err, "error CreateVGShared")
			return err
		}
	}

	log.Debug("successfully create VG of the LVMVolumeGroup")

	return nil
}

func (r *Reconciler) updateVGTagIfNeeded(
	ctx context.Context,
	lvg *v1alpha1.LVMVolumeGroup,
	vg internal.VGData,
) (bool, error) {
	log := r.log.WithName("updateVGTagIfNeeded").WithValues("lvgName", lvg.Name, "vgName", vg.VGName)
	found, tagName := utils.ReadValueFromTags(vg.VGTags, internal.LVMVolumeGroupTag)
	if found && lvg.Name != tagName {
		if isApplied(lvg) {
			err := r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonUpdating, "trying to apply the configuration")
			if err != nil {
				log.Error(err, "unable to add the condition status False reason to the LVMVolumeGroup",
					"conditionType", internal.TypeVGConfigurationApplied,
					"reason", internal.ReasonUpdating)
				return false, err
			}
		}

		start := time.Now()
		cmd, err := r.commands.VGChangeDelTag(vg.VGName, fmt.Sprintf("%s=%s", internal.LVMVolumeGroupTag, tagName))
		r.metrics.UtilsCommandsDuration(ReconcilerName, "vgchange").Observe(r.metrics.GetEstimatedTimeInSeconds(start))
		r.metrics.UtilsCommandsExecutionCount(ReconcilerName, "vgchange").Inc()
		log.Debug("VGChangeDelTag command", "command", cmd)
		if err != nil {
			log.Error(err, "unable to delete LVMVolumeGroupTag", "tagKey", internal.LVMVolumeGroupTag, "tagValue", tagName)
			r.metrics.UtilsCommandsErrorsCount(ReconcilerName, "vgchange").Inc()
			return false, err
		}

		start = time.Now()
		cmd, err = r.commands.VGChangeAddTag(vg.VGName, fmt.Sprintf("%s=%s", internal.LVMVolumeGroupTag, lvg.Name))
		r.metrics.UtilsCommandsDuration(ReconcilerName, "vgchange").Observe(r.metrics.GetEstimatedTimeInSeconds(start))
		r.metrics.UtilsCommandsExecutionCount(ReconcilerName, "vgchange").Inc()
		log.Debug("VGChangeAddTag command", "command", cmd)
		if err != nil {
			log.Error(err, "unable to add LVMVolumeGroupTag", "tagKey", internal.LVMVolumeGroupTag, "tagValue", lvg.Name)
			r.metrics.UtilsCommandsErrorsCount(ReconcilerName, "vgchange").Inc()
			return false, err
		}

		return true, nil
	}

	return false, nil
}

func (r *Reconciler) extendThinPool(lvg *v1alpha1.LVMVolumeGroup, specThinPool v1alpha1.LVMVolumeGroupThinPoolSpec) error {
	log := r.log.WithName("extendThinPool").WithValues("lvgName", lvg.Name, "thinPoolName", specThinPool.Name)
	volumeGroupFreeSpaceBytes := lvg.Status.VGSize.Value() - lvg.Status.AllocatedSize.Value()
	tpRequestedSize, err := utils.GetRequestedSizeFromString(specThinPool.Size, lvg.Status.VGSize)
	if err != nil {
		return err
	}

	log.Trace("volume group sizes",
		"vgSize", lvg.Status.VGSize,
		"allocatedSize", lvg.Status.AllocatedSize,
		"freeSpaceBytes", volumeGroupFreeSpaceBytes)

	log.Info("start resizing thin pool", "newSize", tpRequestedSize)

	var cmd string
	start := time.Now()
	if utils.AreSizesEqualWithinDelta(tpRequestedSize, lvg.Status.VGSize, internal.ResizeDelta) {
		log.Debug("thin-pool of the LVMVolumeGroup will be extend to size 100VG")
		cmd, err = r.commands.ExtendLVFullVGSpace(lvg.Spec.ActualVGNameOnTheNode, specThinPool.Name)
	} else {
		log.Debug("thin-pool of the LVMVolumeGroup will be extend to size", "size", tpRequestedSize)
		cmd, err = r.commands.ExtendLV(tpRequestedSize.Value(), lvg.Spec.ActualVGNameOnTheNode, specThinPool.Name)
	}
	r.metrics.UtilsCommandsDuration(ReconcilerName, "lvextend").Observe(r.metrics.GetEstimatedTimeInSeconds(start))
	r.metrics.UtilsCommandsExecutionCount(ReconcilerName, "lvextend").Inc()
	if err != nil {
		r.metrics.UtilsCommandsErrorsCount(ReconcilerName, "lvextend").Inc()
		log.Error(err, "unable to extend LV", "command", cmd)
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
