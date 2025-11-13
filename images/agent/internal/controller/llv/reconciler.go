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

package llv

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/google/go-cmp/cmp"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/strings/slices"
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

const ReconcilerName = "lvm-logical-volume-watcher-controller"

type cleanupsKey struct {
	vgName, lvName string
}

type cleanupStatus struct {
	cleanupRunning   bool
	prevFailedMethod *string
}
type cleanups struct {
	m      sync.Mutex
	status map[cleanupsKey]cleanupStatus
}
type Reconciler struct {
	cl              client.Client
	log             logger.Logger
	lvgCl           *repository.LVGClient
	llvCl           *repository.LLVClient
	metrics         monitoring.Metrics
	sdsCache        *cache.Cache
	cfg             ReconcilerConfig
	runningCleanups cleanups
	commands        utils.Commands
}

var errAlreadyRunning = errors.New("reconcile in progress")
var errCleanupSameAsPreviouslyFailed = errors.New("cleanup method was failed and not changed")

type ReconcilerConfig struct {
	NodeName                string
	Loglevel                logger.Verbosity
	VolumeGroupScanInterval time.Duration
	LLVRequeueInterval      time.Duration
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
		llvCl: repository.NewLLVClient(
			cl, log,
		),
		metrics:  metrics,
		sdsCache: sdsCache,
		cfg:      cfg,
		runningCleanups: cleanups{
			status: make(map[cleanupsKey]cleanupStatus, 50),
		},
		commands: commands,
	}
}

func (r *Reconciler) startCleanupRunning(vgName, lvName string) (inserted bool, prevFailedMethod *string) {
	r.runningCleanups.m.Lock()
	defer r.runningCleanups.m.Unlock()
	key := cleanupsKey{vgName: vgName, lvName: lvName}
	value, exists := r.runningCleanups.status[key]
	if exists && value.cleanupRunning {
		return false, nil
	}
	value.cleanupRunning = true
	r.runningCleanups.status[key] = value
	return true, value.prevFailedMethod
}

func (r *Reconciler) stopCleanupRunning(vgName, lvName string, failedMethod *string) error {
	r.runningCleanups.m.Lock()
	defer r.runningCleanups.m.Unlock()
	key := cleanupsKey{vgName: vgName, lvName: lvName}
	value, exists := r.runningCleanups.status[key]
	if !exists || !value.cleanupRunning {
		return errors.New("cleanup is not running")
	}
	if failedMethod == nil {
		delete(r.runningCleanups.status, key)
	} else {
		value.prevFailedMethod = failedMethod
		value.cleanupRunning = false
		r.runningCleanups.status[key] = value
	}
	return nil
}

// Name implements controller.Reconciler.
func (r *Reconciler) Name() string {
	return ReconcilerName
}

func (r *Reconciler) MaxConcurrentReconciles() int {
	return 10
}

// ShouldReconcileCreate implements controller.Reconciler.
func (r *Reconciler) ShouldReconcileCreate(_ *v1alpha1.LVMLogicalVolume) bool {
	return true
}

// ShouldReconcileUpdate implements controller.Reconciler.
func (r *Reconciler) ShouldReconcileUpdate(objectOld *v1alpha1.LVMLogicalVolume, objectNew *v1alpha1.LVMLogicalVolume) bool {
	log := r.log.WithName("ShouldReconcileUpdate")
	log.Info("got an update event for the LVMLogicalVolume",
		"llvName", objectNew.GetName())

	// TODO: Figure out how to log it in our logger.
	if r.cfg.Loglevel == "4" {
		fmt.Println("==============START DIFF==================")
		fmt.Println(cmp.Diff(objectOld, objectNew))
		fmt.Println("==============END DIFF==================")
	}

	if reflect.DeepEqual(objectOld.Spec, objectNew.Spec) && objectNew.DeletionTimestamp == nil {
		log.Info("no target changes were made for the LVMLogicalVolume. No need to reconcile the request",
			"llvName", objectNew.Name)
		return false
	}

	return true
}

// Reconcile implements controller.Reconciler.
func (r *Reconciler) Reconcile(
	ctx context.Context,
	req controller.ReconcileRequest[*v1alpha1.LVMLogicalVolume],
) (controller.Result, error) {
	llv := req.Object
	log := r.log.WithName("Reconcile")
	if llv != nil {
		log = log.WithValues(
			"llvName", llv.Name,
			"lvgName", llv.Spec.LVMVolumeGroupName)
	}
	log.Info("Reconciler starts reconciliation of the LVMLogicalVolume")

	lvg, err := r.lvgCl.GetLVMVolumeGroup(ctx, llv.Spec.LVMVolumeGroupName)
	if err != nil {
		if k8serr.IsNotFound(err) {
			log.Error(err, "LVMVolumeGroup not found for LVMLogicalVolume",
				"retryIn", r.cfg.VolumeGroupScanInterval)
			err = r.llvCl.UpdatePhaseIfNeeded(
				ctx,
				llv,
				v1alpha1.PhaseFailed,
				fmt.Sprintf("LVMVolumeGroup %s not found", llv.Spec.LVMVolumeGroupName),
			)
			if err != nil {
				log.Error(err, "unable to update the LVMLogicalVolume")
				return controller.Result{}, err
			}

			return controller.Result{
				RequeueAfter: r.cfg.VolumeGroupScanInterval,
			}, nil
		}

		err = r.llvCl.UpdatePhaseIfNeeded(
			ctx,
			llv,
			v1alpha1.PhaseFailed,
			fmt.Sprintf("Unable to get selected LVMVolumeGroup, err: %s", err.Error()),
		)
		if err != nil {
			log.Error(err, "unable to update the LVMLogicalVolume")
		}
		return controller.Result{}, err
	}

	if !utils.LVGBelongsToNode(lvg, r.cfg.NodeName) {
		log.Info("the LVMVolumeGroup of the LVMLogicalVolume does not belongs to the current node. Reconciliation stopped", "nodeName", r.cfg.NodeName)
		return controller.Result{}, nil
	}
	log.Info("the LVMVolumeGroup of the LVMLogicalVolume belongs to the current node. Reconciliation continues", "nodeName", r.cfg.NodeName)

	// this case prevents the unexpected behavior when the controller runs up with existing LVMLogicalVolumes
	if vgs, _ := r.sdsCache.GetVGs(); len(vgs) == 0 {
		log.Warning("unable to reconcile the request as no VG was found in the cache", "retryIn", r.cfg.VolumeGroupScanInterval)
		return controller.Result{RequeueAfter: r.cfg.VolumeGroupScanInterval}, nil
	}

	log.Debug("tries to add the finalizer to the LVMLogicalVolume", "finalizer", internal.SdsNodeConfiguratorFinalizer)
	added, err := r.addLLVFinalizerIfNotExist(ctx, llv)
	if err != nil {
		log.Error(err, "unable to update the LVMLogicalVolume")
		return controller.Result{}, err
	}
	if added {
		log.Debug("successfully added the finalizer to the LVMLogicalVolume", "finalizer", internal.SdsNodeConfiguratorFinalizer)
	} else {
		log.Debug("no need to add the finalizer to the LVMLogicalVolume", "finalizer", internal.SdsNodeConfiguratorFinalizer)
	}

	log.Info("starts to validate the LVMLogicalVolume")
	valid, reason := r.validateLVMLogicalVolume(llv, lvg)
	if !valid {
		log.Warning("the LVMLogicalVolume is not valid", "reason", reason)
		err = r.llvCl.UpdatePhaseIfNeeded(ctx, llv, v1alpha1.PhaseFailed, reason)
		if err != nil {
			log.Error(err, "unable to update the LVMLogicalVolume")
			return controller.Result{}, err
		}

		return controller.Result{}, nil
	}
	log.Info("successfully validated the LVMLogicalVolume")

	shouldRequeue, err := r.ReconcileLVMLogicalVolume(ctx, llv, lvg)
	if err != nil {
		log.Error(err, "an error occurred while reconciling the LVMLogicalVolume")
		if !errors.Is(err, errAlreadyRunning) && !errors.Is(err, errCleanupSameAsPreviouslyFailed) {
			updErr := r.llvCl.UpdatePhaseIfNeeded(ctx, llv, v1alpha1.PhaseFailed, err.Error())
			if updErr != nil {
				log.Error(updErr, "unable to update the LVMLogicalVolume")
				return controller.Result{}, updErr
			}
		}
	}
	if shouldRequeue {
		log.Info("some issues were occurred while reconciliation the LVMLogicalVolume", "requeueIn", r.cfg.LLVRequeueInterval)
		return controller.Result{RequeueAfter: r.cfg.LLVRequeueInterval}, nil
	}

	log.Info("successfully ended reconciliation of the LVMLogicalVolume")
	return controller.Result{}, nil
}

func (r *Reconciler) ReconcileLVMLogicalVolume(ctx context.Context, llv *v1alpha1.LVMLogicalVolume, lvg *v1alpha1.LVMVolumeGroup) (bool, error) {
	log := r.log.WithName("ReconcileLVMLogicalVolume").WithValues("llvName", llv.Name)
	log.Debug("starts the reconciliation for the LVMLogicalVolume")

	log.Debug("tries to identify the reconciliation type for the LVMLogicalVolume")
	log.Trace("LVMLogicalVolume", "llv", llv)

	switch r.identifyReconcileFunc(lvg.Spec.ActualVGNameOnTheNode, llv) {
	case internal.CreateReconcile:
		return r.reconcileLLVCreateFunc(ctx, llv, lvg)
	case internal.UpdateReconcile:
		return r.reconcileLLVUpdateFunc(ctx, llv, lvg)
	case internal.DeleteReconcile:
		return r.reconcileLLVDeleteFunc(ctx, llv, lvg)
	default:
		log.Info("the LVMLogicalVolume has completed configuration and should not be reconciled")
		if llv.Status.Phase != v1alpha1.PhaseCreated {
			log.Warning("the LVMLogicalVolume should not be reconciled but has an unexpected phase. Setting the phase to Created", "currentPhase", llv.Status.Phase, "expectedPhase", v1alpha1.PhaseCreated)
			err := r.llvCl.UpdatePhaseIfNeeded(ctx, llv, v1alpha1.PhaseCreated, "")
			if err != nil {
				return true, err
			}
		}
	}

	return false, nil
}

func (r *Reconciler) reconcileLLVCreateFunc(
	ctx context.Context,
	llv *v1alpha1.LVMLogicalVolume,
	lvg *v1alpha1.LVMVolumeGroup,
) (bool, error) {
	log := r.log.WithName("reconcileLLVCreateFunc").WithValues(
		"llvName", llv.Name,
		"lvName", llv.Spec.ActualLVNameOnTheNode,
		"vgName", lvg.Spec.ActualVGNameOnTheNode)
	log.Debug("starts reconciliation for the LVMLogicalVolume")

	// this check prevents infinite resource updating after retries
	if llv.Status == nil {
		err := r.llvCl.UpdatePhaseIfNeeded(ctx, llv, v1alpha1.PhasePending, "")
		if err != nil {
			log.Error(err, "unable to update the LVMLogicalVolume")
			return true, err
		}
	}
	llvRequestSize, err := utils.GetLLVRequestedSize(llv, lvg)
	if err != nil {
		log.Error(err, "unable to get LVMLogicalVolume requested size")
		return false, err
	}

	freeSpace := utils.GetFreeLVGSpaceForLLV(lvg, llv)
	log.Trace("the LVMLogicalVolume",
		"type", llv.Spec.Type,
		"requestedSize", llvRequestSize,
		"freeSpace", freeSpace)

	if !utils.AreSizesEqualWithinDelta(llvRequestSize, freeSpace, internal.ResizeDelta) {
		if freeSpace.Value() < llvRequestSize.Value()+internal.ResizeDelta.Value() {
			err = errors.New("not enough space")
			log.Error(err, "the LV requested size of the LVMLogicalVolume is more than the actual free space",
				"requestedSize", llvRequestSize,
				"freeSpace", freeSpace)

			// we return true cause the user might manage LVMVolumeGroup free space without changing the LLV
			return true, err
		}
	}

	var cmd string
	switch {
	case llv.Spec.Type == internal.Thick:
		log.Debug("LV will be created in VG with size",
			"size", llvRequestSize)
		cmd, err = r.commands.CreateThickLogicalVolume(lvg.Spec.ActualVGNameOnTheNode, llv.Spec.ActualLVNameOnTheNode, llvRequestSize.Value(), isContiguous(llv))
	case llv.Spec.Source == nil:
		log.Debug("LV of the LVMLogicalVolume will be created in Thin-pool with size",
			"thinPoolName", llv.Spec.Thin.PoolName,
			"size", llvRequestSize)
		cmd, err = r.commands.CreateThinLogicalVolume(lvg.Spec.ActualVGNameOnTheNode, llv.Spec.Thin.PoolName, llv.Spec.ActualLVNameOnTheNode, llvRequestSize.Value())
	case llv.Spec.Source.Kind == "LVMLogicalVolume":
		sourceLLV := &v1alpha1.LVMLogicalVolume{}
		if err := r.cl.Get(ctx, types.NamespacedName{Name: llv.Spec.Source.Name}, sourceLLV); err != nil {
			log.Error(err, "unable to get source LVMLogicalVolume for the LVMLogicalVolume",
				"sourceLLVName", llv.Spec.Source.Name)
			return true, err
		}

		if sourceLLV.Spec.LVMVolumeGroupName != lvg.Name {
			return false, errors.New("cloned volume should be in the same volume group as the source volume")
		}

		cmd, err = r.commands.CreateThinLogicalVolumeFromSource(llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode, sourceLLV.Spec.ActualLVNameOnTheNode)
	case llv.Spec.Source.Kind == "LVMLogicalVolumeSnapshot":
		cmdTmp, shouldRequeue, err := r.handleLLVSSource(ctx, llv, lvg)
		if err != nil {
			return shouldRequeue, err
		}
		cmd = cmdTmp
	}
	log.Debug("ran cmd", "cmd", cmd)
	if err != nil {
		log.Error(err, "unable to create a LogicalVolume for the LVMLogicalVolume", "type", llv.Spec.Type)
		return true, err
	}

	log.Info("successfully created LV in VG for LVMLogicalVolume resource")

	log.Debug("adds the LV to the cache")
	r.sdsCache.AddLV(lvg.Spec.ActualVGNameOnTheNode, llv.Spec.ActualLVNameOnTheNode)
	log.Debug("tries to get the LV actual size")
	actualSize := r.getLVActualSize(lvg.Spec.ActualVGNameOnTheNode, llv.Spec.ActualLVNameOnTheNode)
	if actualSize.Value() == 0 {
		log.Debug("unable to get actual size for LV in VG (likely LV was not found in the cache), retry...")
		return true, nil
	}
	log.Debug("successfully got the LV actual size")
	log.Trace("the LV in VG has actual size", "actualSize", actualSize)

	if err := r.llvCl.UpdatePhaseToCreatedIfNeeded(ctx, llv, actualSize); err != nil {
		return true, err
	}

	log.Info("successfully ended the reconciliation for the LVMLogicalVolume")
	return false, nil
}

func (r *Reconciler) reconcileLLVUpdateFunc(
	ctx context.Context,
	llv *v1alpha1.LVMLogicalVolume,
	lvg *v1alpha1.LVMVolumeGroup,
) (bool, error) {
	log := r.log.WithName("reconcileLLVUpdateFunc").WithValues(
		"llvName", llv.Name,
		"lvName", llv.Spec.ActualLVNameOnTheNode,
		"vgName", lvg.Spec.ActualVGNameOnTheNode)
	log.Debug("starts reconciliation for the LVMLogicalVolume")

	// status might be nil if a user creates the resource with LV name which matches existing LV on the node
	if llv.Status == nil {
		err := r.llvCl.UpdatePhaseIfNeeded(ctx, llv, v1alpha1.PhasePending, "")
		if err != nil {
			log.Error(err, "unable to update the LVMLogicalVolume")
			return true, err
		}
	}

	// it needs to get current LV size from the node as status might be nil
	log.Debug("tries to get LVMLogicalVolume actual size before the extension")
	actualSize := r.getLVActualSize(lvg.Spec.ActualVGNameOnTheNode, llv.Spec.ActualLVNameOnTheNode)
	if actualSize.Value() == 0 {
		log.Warning("LV of the LVMLogicalVolume has zero size (likely LV was not updated in the cache)")
		return true, nil
	}
	log.Debug("successfully got LVMLogicalVolume actual size before the extension", "actualSize", actualSize)

	log.Debug("tries to count the LVMLogicalVolume requested size")
	llvRequestSize, err := utils.GetLLVRequestedSize(llv, lvg)
	if err != nil {
		log.Error(err, "unable to get LVMLogicalVolume requested size")
		return false, err
	}
	log.Debug("successfully counted the LVMLogicalVolume requested size", "requestedSize", llvRequestSize)

	if utils.AreSizesEqualWithinDelta(actualSize, llvRequestSize, internal.ResizeDelta) {
		log.Warning("the LV in VG has the same actual size as the requested size",
			"actualSize", actualSize,
			"requestedSize", llvRequestSize)

		if err := r.llvCl.UpdatePhaseToCreatedIfNeeded(ctx, llv, actualSize); err != nil {
			return true, err
		}

		log.Info("successfully ended reconciliation for the LVMLogicalVolume")

		return false, nil
	}

	extendingSize := subtractQuantity(llvRequestSize, actualSize)
	log.Trace("the LV in VG has extending size",
		"extendingSize", extendingSize)
	if extendingSize.Value() < 0 {
		err = fmt.Errorf("specified LV size %dB is less than actual one on the node %dB", llvRequestSize.Value(), actualSize.Value())
		log.Error(err, "unable to extend the LVMLogicalVolume")
		return false, err
	}

	log.Info("the LVMLogicalVolume should be resized")
	// this check prevents infinite resource updates after retry
	if llv.Status.Phase != v1alpha1.PhaseFailed {
		err := r.llvCl.UpdatePhaseIfNeeded(ctx, llv, v1alpha1.PhaseResizing, "")
		if err != nil {
			log.Error(err, "unable to update the LVMLogicalVolume")
			return true, err
		}
	}

	freeSpace := utils.GetFreeLVGSpaceForLLV(lvg, llv)
	log.Trace("the LVMLogicalVolume",
		"type", llv.Spec.Type,
		"extendingSize", extendingSize,
		"freeSpace", freeSpace)

	if !utils.AreSizesEqualWithinDelta(freeSpace, extendingSize, internal.ResizeDelta) {
		if freeSpace.Value() < extendingSize.Value()+internal.ResizeDelta.Value() {
			err = errors.New("not enough space")
			log.Error(err, "the LV requested size of the LVMLogicalVolume is more than actual free space",
				"requestedSize", llvRequestSize,
				"freeSpace", freeSpace)

			// returns true cause a user might manage LVG free space without changing the LLV
			return true, err
		}
	}

	log.Debug("LV of the LVMLogicalVolume will be extended with size",
		"size", llvRequestSize)
	cmd, err := r.commands.ExtendLV(llvRequestSize.Value(), lvg.Spec.ActualVGNameOnTheNode, llv.Spec.ActualLVNameOnTheNode)
	log.Debug("runs cmd", "cmd", cmd)
	if err != nil {
		log.Error(err, "unable to ExtendLV", "name", llv.Spec.ActualLVNameOnTheNode, "type", llv.Spec.Type)
		return true, err
	}

	log.Info("successfully extended LV in VG for LVMLogicalVolume resource", "lvName", llv.Spec.ActualLVNameOnTheNode, "vgName", lvg.Spec.ActualVGNameOnTheNode)

	log.Debug("tries to get LVMLogicalVolume actual size after the extension")
	newActualSize := r.getLVActualSize(lvg.Spec.ActualVGNameOnTheNode, llv.Spec.ActualLVNameOnTheNode)

	// this case might be triggered if sds cache will not update lv state in time
	if newActualSize.Value() == actualSize.Value() {
		log.Warning("LV of the LVMLogicalVolume was extended but cache is not updated yet. It will be retried")
		return true, nil
	}

	log.Debug("successfully got LVMLogicalVolume actual size after the extension")
	log.Trace("the LV in VG actual size",
		"actualSize", newActualSize)

	// need this here as a user might create the LLV with existing LV
	if err := r.llvCl.UpdatePhaseToCreatedIfNeeded(ctx, llv, newActualSize); err != nil {
		return true, err
	}

	log.Info("successfully ended reconciliation for the LVMLogicalVolume")
	return false, nil
}

func (r *Reconciler) reconcileLLVDeleteFunc(
	ctx context.Context,
	llv *v1alpha1.LVMLogicalVolume,
	lvg *v1alpha1.LVMVolumeGroup,
) (bool, error) {
	log := r.log.WithName("reconcileLLVDeleteFunc").WithValues("llvName", llv.Name)
	log.Debug("starts reconciliation for the LVMLogicalVolume")

	// The controller won't remove the LLV resource and LV volume till the resource has any other finalizer.
	if len(llv.Finalizers) != 0 {
		if len(llv.Finalizers) > 1 ||
			llv.Finalizers[0] != internal.SdsNodeConfiguratorFinalizer {
			log.Debug("unable to delete LVMLogicalVolume for now due to it has any other finalizer")
			return false, nil
		}
	}

	shouldRequeue, err := r.deleteLVIfNeeded(ctx, lvg.Spec.ActualVGNameOnTheNode, llv)
	if err != nil {
		log.Error(err, "unable to delete the LV in VG", "lvName", llv.Spec.ActualLVNameOnTheNode, "vgName", lvg.Spec.ActualVGNameOnTheNode)
		return shouldRequeue, err
	}

	log.Info("successfully deleted the LV in VG", "lvName", llv.Spec.ActualLVNameOnTheNode, "vgName", lvg.Spec.ActualVGNameOnTheNode)

	err = r.removeLLVFinalizersIfExist(ctx, llv)
	if err != nil {
		log.Error(err, "unable to remove finalizers from the LVMLogicalVolume")
		return true, err
	}

	log.Info("successfully ended reconciliation for the LVMLogicalVolume")
	return false, nil
}

func (r *Reconciler) identifyReconcileFunc(vgName string, llv *v1alpha1.LVMLogicalVolume) internal.ReconcileType {
	should := r.shouldReconcileByCreateFunc(vgName, llv)
	if should {
		return internal.CreateReconcile
	}

	should = r.shouldReconcileByUpdateFunc(vgName, llv)
	if should {
		return internal.UpdateReconcile
	}

	should = shouldReconcileByDeleteFunc(llv)
	if should {
		return internal.DeleteReconcile
	}

	return ""
}

func shouldReconcileByDeleteFunc(llv *v1alpha1.LVMLogicalVolume) bool {
	return llv.DeletionTimestamp != nil
}

func (r *Reconciler) removeLLVFinalizersIfExist(
	ctx context.Context,
	llv *v1alpha1.LVMLogicalVolume,
) error {
	log := r.log.WithName("removeLLVFinalizersIfExist").WithValues("llvName", llv.Name)
	var removed bool
	for i, f := range llv.Finalizers {
		if f == internal.SdsNodeConfiguratorFinalizer {
			llv.Finalizers = append(llv.Finalizers[:i], llv.Finalizers[i+1:]...)
			removed = true
			log.Debug("removed finalizer from the LVMLogicalVolume", "finalizer", internal.SdsNodeConfiguratorFinalizer)
			break
		}
	}

	if removed {
		log.Trace("removed finalizer from the LVMLogicalVolume", "finalizer", internal.SdsNodeConfiguratorFinalizer)
		err := r.updateLVMLogicalVolumeSpec(ctx, llv)
		if err != nil {
			log.Error(err, "unable to update the LVMVolumeGroup")
			return err
		}
	}

	return nil
}

func checkIfLVBelongsToLLV(llv *v1alpha1.LVMLogicalVolume, lv *internal.LVData) bool {
	switch llv.Spec.Type {
	case internal.Thin:
		if lv.PoolName != llv.Spec.Thin.PoolName {
			return false
		}
	case internal.Thick:
		contiguous := string(lv.LVAttr[2]) == "c"
		if string(lv.LVAttr[0]) != "-" ||
			contiguous != isContiguous(llv) {
			return false
		}
	}

	return true
}

func (r *Reconciler) deleteLVIfNeeded(ctx context.Context, vgName string, llv *v1alpha1.LVMLogicalVolume) (bool, error) {
	log := r.log.WithName("deleteLVIfNeeded").WithValues("llvName", llv.Name)
	lv := r.sdsCache.FindLV(vgName, llv.Spec.ActualLVNameOnTheNode)
	if lv == nil || !lv.Exist {
		log.Warning("did not find LV in VG", "lvName", llv.Spec.ActualLVNameOnTheNode, "vgName", vgName)
		return false, nil
	}

	// this case prevents unexpected same-name LV deletions which does not actually belong to our LLV
	if !checkIfLVBelongsToLLV(llv, &lv.Data) {
		log.Warning("no need to delete LV as it doesn't belong to LVMLogicalVolume", "lvName", lv.Data.LVName)
		return false, nil
	}

	if cleanupMethodPtr := llv.Spec.VolumeCleanup; cleanupMethodPtr != nil {
		if shouldRequeue, err := r.cleanupVolume(ctx, llv, lv, vgName, *cleanupMethodPtr); err != nil {
			return shouldRequeue, err
		}
	}

	cmd, err := r.commands.RemoveLV(vgName, llv.Spec.ActualLVNameOnTheNode)
	log.Debug("runs cmd", "cmd", cmd)
	if err != nil {
		log.Error(err, "unable to remove LV from VG", "lvName", llv.Spec.ActualLVNameOnTheNode, "vgName", vgName)
		return true, err
	}

	log.Debug("mark LV in the cache as removed", "lvName", lv.Data.LVName)
	r.sdsCache.MarkLVAsRemoved(lv.Data.VGName, lv.Data.LVName)

	return false, nil
}

func (r *Reconciler) getLVActualSize(vgName, lvName string) resource.Quantity {
	lv := r.sdsCache.FindLV(vgName, lvName)
	if lv == nil {
		return resource.Quantity{}
	}

	result := resource.NewQuantity(lv.Data.LVSize.Value(), resource.BinarySI)

	return *result
}

func (r *Reconciler) addLLVFinalizerIfNotExist(ctx context.Context, llv *v1alpha1.LVMLogicalVolume) (bool, error) {
	log := r.log.WithName("addLLVFinalizerIfNotExist").WithValues("llvName", llv.Name)
	if slices.Contains(llv.Finalizers, internal.SdsNodeConfiguratorFinalizer) {
		return false, nil
	}

	llv.Finalizers = append(llv.Finalizers, internal.SdsNodeConfiguratorFinalizer)

	log.Trace("added finalizer to the LVMLogicalVolume", "finalizer", internal.SdsNodeConfiguratorFinalizer)
	err := r.updateLVMLogicalVolumeSpec(ctx, llv)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (r *Reconciler) shouldReconcileByCreateFunc(vgName string, llv *v1alpha1.LVMLogicalVolume) bool {
	if llv.DeletionTimestamp != nil {
		return false
	}

	lv := r.sdsCache.FindLV(vgName, llv.Spec.ActualLVNameOnTheNode)
	return lv == nil
}

func subtractQuantity(currentQuantity, quantityToSubtract resource.Quantity) resource.Quantity {
	resultingQuantity := currentQuantity.DeepCopy()
	resultingQuantity.Sub(quantityToSubtract)
	return resultingQuantity
}

func (r *Reconciler) validateLVMLogicalVolume(llv *v1alpha1.LVMLogicalVolume, lvg *v1alpha1.LVMVolumeGroup) (bool, string) {
	if llv.DeletionTimestamp != nil {
		// as the configuration doesn't matter if we want to delete it
		return true, ""
	}

	reason := strings.Builder{}

	if len(llv.Spec.ActualLVNameOnTheNode) == 0 {
		reason.WriteString("No LV name specified. ")
	}

	llvRequestedSize, err := utils.GetLLVRequestedSize(llv, lvg)
	if err != nil {
		reason.WriteString(err.Error())
	}

	if llvRequestedSize.Value() == 0 {
		reason.WriteString("Zero size for LV. ")
	}

	if llv.Status != nil {
		if llvRequestedSize.Value()+internal.ResizeDelta.Value() < llv.Status.ActualSize.Value() {
			reason.WriteString("Desired LV size is less than actual one. ")
		}
	}

	switch llv.Spec.Type {
	case internal.Thin:
		if llv.Spec.Thin == nil {
			reason.WriteString("No thin pool specified. ")
			break
		}

		exist := false
		for _, tp := range lvg.Status.ThinPools {
			if tp.Name == llv.Spec.Thin.PoolName {
				exist = true
				break
			}
		}

		if !exist {
			reason.WriteString("Selected thin pool does not exist in selected LVMVolumeGroup. ")
		}
	case internal.Thick:
		if llv.Spec.Thin != nil {
			reason.WriteString("Thin pool specified for Thick LV. ")
		}
	}

	// if a specified Thick LV name matches the existing Thin one
	lv := r.sdsCache.FindLV(lvg.Spec.ActualVGNameOnTheNode, llv.Spec.ActualLVNameOnTheNode)
	if lv != nil &&
		len(lv.Data.LVAttr) != 0 && !checkIfLVBelongsToLLV(llv, &lv.Data) {
		reason.WriteString(fmt.Sprintf("Specified LV %s is already created and it is doesn't match the one on the node.", lv.Data.LVName))
	}

	if reason.Len() > 0 {
		return false, reason.String()
	}

	return true, ""
}

func (r *Reconciler) updateLVMLogicalVolumeSpec(ctx context.Context, llv *v1alpha1.LVMLogicalVolume) error {
	return r.cl.Update(ctx, llv)
}

func (r *Reconciler) shouldReconcileByUpdateFunc(vgName string, llv *v1alpha1.LVMLogicalVolume) bool {
	if llv.DeletionTimestamp != nil {
		return false
	}

	lv := r.sdsCache.FindLV(vgName, llv.Spec.ActualLVNameOnTheNode)
	return lv != nil && lv.Exist
}

func isContiguous(llv *v1alpha1.LVMLogicalVolume) bool {
	if llv.Spec.Thick == nil || llv.Spec.Thick.Contiguous == nil {
		return false
	}

	return *llv.Spec.Thick.Contiguous
}
