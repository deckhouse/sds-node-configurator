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

	"github.com/deckhouse/sds-common-lib/conditions"
	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/cache"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/controller"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/monitoring"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/repository"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/thinpoolalloc"
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
	metrics         *monitoring.Metrics
	sdsCache        *cache.Cache
	cfg             ReconcilerConfig
	runningCleanups cleanups
	commands        utils.Commands
	thinPools       *thinpoolalloc.Locks
}

var errAlreadyRunning = errors.New("reconcile in progress")
var errCleanupSameAsPreviouslyFailed = errors.New("cleanup method was failed and not changed")

type ReconcilerConfig struct {
	NodeName                string
	Loglevel                logger.Verbosity
	VolumeGroupScanInterval time.Duration
	LLVRequeueInterval      time.Duration
	ThinPools               *thinpoolalloc.Locks
}

func NewReconciler(
	cl client.Client,
	log logger.Logger,
	metrics *monitoring.Metrics,
	sdsCache *cache.Cache,
	commands utils.Commands,
	recorder conditions.EventRecorder,
	cfg ReconcilerConfig,
) *Reconciler {
	if cfg.ThinPools == nil {
		panic("llv: ReconcilerConfig.ThinPools must be the thin pool locks shared by the agent")
	}
	return &Reconciler{
		cl:  cl,
		log: log.Named(ReconcilerName),
		lvgCl: repository.NewLVGClient(
			cl,
			log,
			metrics,
			cfg.NodeName,
			ReconcilerName,
			recorder,
		),
		llvCl: repository.NewLLVClient(
			cl, log, recorder,
		),
		metrics:  metrics,
		sdsCache: sdsCache,
		cfg:      cfg,
		runningCleanups: cleanups{
			status: make(map[cleanupsKey]cleanupStatus, 50),
		},
		commands:  commands,
		thinPools: cfg.ThinPools,
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
	log := r.log

	log.Info(fmt.Sprintf("got an update event for the LVMLogicalVolume: %s", objectNew.GetName()))

	// TODO: Figure out how to log it in our logger.
	if r.cfg.Loglevel == "4" {
		fmt.Println("==============START DIFF==================")
		fmt.Println(cmp.Diff(objectOld, objectNew))
		fmt.Println("==============END DIFF==================")
	}

	if reflect.DeepEqual(objectOld.Spec, objectNew.Spec) && objectNew.DeletionTimestamp == nil {
		log.Info(fmt.Sprintf("no target changes were made for the LVMLogicalVolume %s. No need to reconcile the request", objectNew.Name))
		return false
	}

	return true
}

// Reconcile implements controller.Reconciler.
func (r *Reconciler) Reconcile(
	ctx context.Context,
	req controller.ReconcileRequest[*v1alpha1.LVMLogicalVolume],
) (controller.Result, error) {
	reconcileLog := r.log
	log := r.log

	llv := req.Object
	log.Info(fmt.Sprintf("reconciler starts reconciliation of the LVMLogicalVolume: %s", llv.Name))

	lvg, err := r.lvgCl.GetLVMVolumeGroup(ctx, llv.Spec.LVMVolumeGroupName)
	if err != nil {
		if k8serr.IsNotFound(err) {
			reconcileLog.Error(fmt.Sprintf("LVMVolumeGroup %s not found for LVMLogicalVolume %s. Retry in %s", llv.Spec.LVMVolumeGroupName, llv.Name, r.cfg.VolumeGroupScanInterval.String()), logger.Err(err))
			err = r.llvCl.UpdatePhaseIfNeeded(
				ctx,
				llv,
				v1alpha1.PhaseFailed,
				fmt.Sprintf("LVMVolumeGroup %s not found", llv.Spec.LVMVolumeGroupName),
			)
			if err != nil {
				reconcileLog.Error(fmt.Sprintf("unable to update the LVMLogicalVolume %s", llv.Name), logger.Err(err))
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
			reconcileLog.Error(fmt.Sprintf("unable to update the LVMLogicalVolume %s", llv.Name), logger.Err(err))
		}
		return controller.Result{}, err
	}

	if !utils.LVGBelongsToNode(lvg, r.cfg.NodeName) {
		reconcileLog.Info(fmt.Sprintf("the LVMVolumeGroup %s of the LVMLogicalVolume %s does not belongs to the current node: %s. Reconciliation stopped", lvg.Name, llv.Name, r.cfg.NodeName))
		return controller.Result{}, nil
	}
	reconcileLog.Info(fmt.Sprintf("the LVMVolumeGroup %s of the LVMLogicalVolume %s belongs to the current node: %s. Reconciliation continues", lvg.Name, llv.Name, r.cfg.NodeName))

	// this case prevents the unexpected behavior when the controller runs up with existing LVMLogicalVolumes
	if vgs, _ := r.sdsCache.GetVGs(); len(vgs) == 0 {
		log.Warn(fmt.Sprintf("unable to reconcile the request as no VG was found in the cache. Retry in %s", r.cfg.VolumeGroupScanInterval.String()))
		return controller.Result{RequeueAfter: r.cfg.VolumeGroupScanInterval}, nil
	}

	if lvg.Status.ExtentSize.Value() == 0 {
		reconcileLog.Warn(fmt.Sprintf("LVMVolumeGroup %s has zero ExtentSize (discoverer has not populated it yet), retry in %s", lvg.Name, r.cfg.VolumeGroupScanInterval.String()))
		return controller.Result{RequeueAfter: r.cfg.VolumeGroupScanInterval}, nil
	}

	reconcileLog.Debug(fmt.Sprintf("tries to add the finalizer %s to the LVMLogicalVolume %s", internal.SdsNodeConfiguratorFinalizer, llv.Name))
	added, err := r.addLLVFinalizerIfNotExist(ctx, llv)
	if err != nil {
		reconcileLog.Error(fmt.Sprintf("unable to update the LVMLogicalVolume %s", llv.Name), logger.Err(err))
		return controller.Result{}, err
	}
	if added {
		reconcileLog.Debug(fmt.Sprintf("successfully added the finalizer %s to the LVMLogicalVolume %s", internal.SdsNodeConfiguratorFinalizer, llv.Name))
	} else {
		reconcileLog.Debug(fmt.Sprintf("no need to add the finalizer %s to the LVMLogicalVolume %s", internal.SdsNodeConfiguratorFinalizer, llv.Name))
	}

	reconcileLog.Info(fmt.Sprintf("starts to validate the LVMLogicalVolume %s", llv.Name))
	valid, reason := r.validateLVMLogicalVolume(llv, lvg)
	if !valid {
		reconcileLog.Warn(fmt.Sprintf("the LVMLogicalVolume %s is not valid, reason: %s", llv.Name, reason))
		err = r.llvCl.UpdatePhaseIfNeeded(ctx, llv, v1alpha1.PhaseFailed, reason)
		if err != nil {
			reconcileLog.Error(fmt.Sprintf("unable to update the LVMLogicalVolume %s", llv.Name), logger.Err(err))
			return controller.Result{}, err
		}

		return controller.Result{}, nil
	}
	reconcileLog.Info(fmt.Sprintf("successfully validated the LVMLogicalVolume %s", llv.Name))

	shouldRequeue, err := r.ReconcileLVMLogicalVolume(ctx, llv, lvg)
	if err != nil {
		reconcileLog.Error(fmt.Sprintf("an error occurred while reconciling the LVMLogicalVolume: %s", llv.Name), logger.Err(err))
		if !errors.Is(err, errAlreadyRunning) && !errors.Is(err, errCleanupSameAsPreviouslyFailed) {
			updErr := r.llvCl.UpdatePhaseIfNeeded(ctx, llv, v1alpha1.PhaseFailed, err.Error())
			if updErr != nil {
				reconcileLog.Error(fmt.Sprintf("unable to update the LVMLogicalVolume %s", llv.Name), logger.Err(updErr))
				return controller.Result{}, updErr
			}
		}
	}
	if shouldRequeue {
		reconcileLog.Info(fmt.Sprintf("some issues were occurred while reconciliation the LVMLogicalVolume %s. Requeue the request in %s", llv.Name, r.cfg.LLVRequeueInterval.String()))
		return controller.Result{RequeueAfter: r.cfg.LLVRequeueInterval}, nil
	}

	reconcileLog.Info(fmt.Sprintf("successfully ended reconciliation of the LVMLogicalVolume %s", llv.Name))
	return controller.Result{}, nil
}

func (r *Reconciler) ReconcileLVMLogicalVolume(ctx context.Context, llv *v1alpha1.LVMLogicalVolume, lvg *v1alpha1.LVMVolumeGroup) (bool, error) {
	runEventReconcileLog := r.log
	log := r.log

	log.Debug(fmt.Sprintf("starts the reconciliation for the LVMLogicalVolume %s", llv.Name))

	log.Debug(fmt.Sprintf("tries to identify the reconciliation type for the LVMLogicalVolume %s", llv.Name))
	log.Trace(fmt.Sprintf("%+v", llv))

	switch r.identifyReconcileFunc(lvg.Spec.ActualVGNameOnTheNode, llv) {
	case internal.CreateReconcile:
		return r.reconcileLLVCreateFunc(ctx, llv, lvg)
	case internal.UpdateReconcile:
		return r.reconcileLLVUpdateFunc(ctx, llv, lvg)
	case internal.DeleteReconcile:
		return r.reconcileLLVDeleteFunc(ctx, llv, lvg)
	default:
		runEventReconcileLog.Info(fmt.Sprintf("the LVMLogicalVolume %s has completed configuration and should not be reconciled", llv.Name))
		if llv.Status.Phase != v1alpha1.PhaseCreated {
			runEventReconcileLog.Warn(fmt.Sprintf("the LVMLogicalVolume %s should not be reconciled but has an unexpected phase: %s. Setting the phase to %s", llv.Name, llv.Status.Phase, v1alpha1.PhaseCreated))
			err := r.llvCl.UpdatePhaseIfNeeded(ctx, llv, v1alpha1.PhaseCreated, "")
			if err != nil {
				return true, err
			}
		}
		if err := r.llvCl.UpdateLocationIfNeeded(ctx, llv, lvg); err != nil {
			return true, err
		}
	}

	return false, nil
}

// llvExtentSize returns the LVG extent size, falling back to 4Mi (LVM default)
// when the discoverer has not yet populated Status.ExtentSize.
func llvExtentSize(lvg *v1alpha1.LVMVolumeGroup) resource.Quantity {
	if lvg.Status.ExtentSize.Value() > 0 {
		return lvg.Status.ExtentSize
	}
	return resource.MustParse("4Mi")
}

func (r *Reconciler) reconcileLLVCreateFunc(
	ctx context.Context,
	llv *v1alpha1.LVMLogicalVolume,
	lvg *v1alpha1.LVMVolumeGroup,
) (bool, error) {
	log := r.log

	log.Debug(fmt.Sprintf("starts reconciliation for the LVMLogicalVolume %s", llv.Name))

	// this check prevents infinite resource updating after retries
	if llv.Status == nil {
		err := r.llvCl.UpdatePhaseIfNeeded(ctx, llv, v1alpha1.PhasePending, "")
		if err != nil {
			log.Error(fmt.Sprintf("unable to update the LVMLogicalVolume %s", llv.Name), logger.Err(err))
			return true, err
		}
	}
	llvRequestSize, err := utils.GetLLVRequestedSize(llv, lvg)
	if err != nil {
		log.Error(fmt.Sprintf("unable to get LVMLogicalVolume %s requested size", llv.Name), logger.Err(err))
		return false, err
	}

	// Percent requests floor, absolute ones round up: 100% of a VG whose size
	// is not extent-aligned must not be rejected as too large for that VG.
	alignedRequestSize, err := utils.AlignRequestedSizeToExtent(llv.Spec.Size, llvRequestSize, llvExtentSize(lvg))
	if err != nil {
		log.Error(fmt.Sprintf("unable to align size for LVMLogicalVolume %s", llv.Name), logger.Err(err))
		return true, err
	}

	actualSize, shouldRequeue, err := r.createLV(ctx, llv, lvg, llvRequestSize, alignedRequestSize)
	if shouldRequeue || err != nil {
		return shouldRequeue, err
	}

	if err := r.llvCl.UpdatePhaseToCreatedIfNeeded(ctx, llv, lvg, actualSize); err != nil {
		return true, err
	}

	log.Info(fmt.Sprintf("successfully ended the reconciliation for the LVMLogicalVolume %s", llv.Name))
	return false, nil
}

func (r *Reconciler) createLV(
	ctx context.Context,
	llv *v1alpha1.LVMLogicalVolume,
	lvg *v1alpha1.LVMVolumeGroup,
	llvRequestSize, alignedRequestSize resource.Quantity,
) (resource.Quantity, bool, error) {
	log := r.log

	sourceLVName, shouldRequeue, err := r.thinSourceLVName(ctx, llv, lvg)
	if err != nil {
		return resource.Quantity{}, shouldRequeue, err
	}

	unlock, err := r.lockThinPool(ctx, llv, lvg)
	if err != nil {
		log.Error(fmt.Sprintf("unable to start creating the LV %s of the LVMLogicalVolume %s", llv.Spec.ActualLVNameOnTheNode, llv.Name), logger.Err(err))
		return resource.Quantity{}, true, err
	}
	defer unlock()

	if llv.Spec.Type == internal.Thin {
		log.Trace(fmt.Sprintf("the LVMLogicalVolume %s, LV: %s, VG: %s, thin pool: %s, requested size: %s, aligned size: %s, source LV: %s", llv.Name, llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode, llv.Spec.Thin.PoolName, llvRequestSize.String(), alignedRequestSize.String(), sourceLVName))
		if err := r.checkThinPoolSpace(lvg, llv.Spec.Thin.PoolName, llv.Spec.ActualLVNameOnTheNode, sourceLVName, alignedRequestSize); err != nil {
			log.Error(fmt.Sprintf("the LV %s of the LVMLogicalVolume %s cannot be created in the thin pool %s", llv.Spec.ActualLVNameOnTheNode, llv.Name, llv.Spec.Thin.PoolName), logger.Err(err))
			return resource.Quantity{}, true, err
		}
	} else {
		freeSpace := utils.GetFreeLVGSpaceForLLV(lvg, llv)
		log.Trace(fmt.Sprintf("the LVMLogicalVolume %s, LV: %s, VG: %s type: %s requested size: %s, aligned size: %s, free space: %s", llv.Name, llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode, llv.Spec.Type, llvRequestSize.String(), alignedRequestSize.String(), freeSpace.String()))

		if freeSpace.Value() < alignedRequestSize.Value() {
			err = errors.New("not enough space")
			log.Error(fmt.Sprintf("the LV %s aligned requested size %s of the LVMLogicalVolume %s is more than the actual free space %s", llv.Spec.ActualLVNameOnTheNode, alignedRequestSize.String(), llv.Name, freeSpace.String()), logger.Err(err))

			return resource.Quantity{}, true, err
		}
	}

	var cmd string
	switch {
	case llv.Spec.Type == internal.Thick:
		log.Debug(fmt.Sprintf("LV %s will be created in VG %s with size: %s", llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode, llvRequestSize.String()))
		cmd, err = r.commands.CreateThickLogicalVolume(lvg.Spec.ActualVGNameOnTheNode, llv.Spec.ActualLVNameOnTheNode, llvRequestSize.Value(), isContiguous(llv))
	case llv.Spec.Source == nil:
		log.Debug(fmt.Sprintf("LV %s of the LVMLogicalVolume %s will be created in Thin-pool %s with size %s", llv.Spec.ActualLVNameOnTheNode, llv.Name, llv.Spec.Thin.PoolName, llvRequestSize.String()))
		cmd, err = r.commands.CreateThinLogicalVolume(lvg.Spec.ActualVGNameOnTheNode, llv.Spec.Thin.PoolName, llv.Spec.ActualLVNameOnTheNode, llvRequestSize.Value())
	case llv.Spec.Source.Kind == "LVMLogicalVolume", llv.Spec.Source.Kind == "LVMLogicalVolumeSnapshot":
		cmd, err = r.commands.CreateThinLogicalVolumeFromSource(llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode, sourceLVName)
	}
	log.Debug(fmt.Sprintf("ran cmd: %s", cmd))
	if err != nil {
		log.Error(fmt.Sprintf("unable to create a %s LogicalVolume for the LVMLogicalVolume %s", llv.Spec.Type, llv.Name), logger.Err(err))
		return resource.Quantity{}, true, err
	}

	log.Info(fmt.Sprintf("successfully created LV %s in VG %s for LVMLogicalVolume resource with name: %s", llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode, llv.Name))

	log.Debug(fmt.Sprintf("adds the LV %s to the cache", llv.Spec.ActualLVNameOnTheNode))
	r.sdsCache.AddLV(lvg.Spec.ActualVGNameOnTheNode, llv.Spec.ActualLVNameOnTheNode)

	log.Debug(fmt.Sprintf("tries to get the LV %s actual size directly from LVM", llv.Spec.ActualLVNameOnTheNode))
	lvData, cmd, _, getLVErr := r.commands.GetLV(lvg.Spec.ActualVGNameOnTheNode, llv.Spec.ActualLVNameOnTheNode)
	log.Debug(fmt.Sprintf("ran cmd: %s", cmd))
	if getLVErr != nil {
		log.Warn(fmt.Sprintf("unable to get LV %s info from LVM: %s, will retry", llv.Spec.ActualLVNameOnTheNode, getLVErr.Error()))
		return resource.Quantity{}, true, nil
	}
	actualSize := lvData.LVSize
	if actualSize.Value() == 0 {
		log.Warn(fmt.Sprintf("LV %s in VG %s returned zero size from LVM, retry...", llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode))
		return resource.Quantity{}, true, nil
	}
	log.Debug(fmt.Sprintf("successfully got the LV %s actual size", llv.Spec.ActualLVNameOnTheNode))
	log.Trace(fmt.Sprintf("the LV %s in VG: %s has actual size: %s", llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode, actualSize.String()))

	// A thin volume created from a snapshot or another LV via `lvcreate -s` inherits
	// the origin size instead of being created at the requested size. When the
	// requested (extent-aligned) size is larger, extend the LV now; otherwise the CSI
	// CreateVolume call waits indefinitely for a size that is never reached.
	if llv.Spec.Type == internal.Thin && llv.Spec.Source != nil && actualSize.Value() < alignedRequestSize.Value() {
		log.Info(fmt.Sprintf("the LV %s cloned from source %s has actual size %s, extending to the requested aligned size %s", llv.Spec.ActualLVNameOnTheNode, llv.Spec.Source.Name, actualSize.String(), alignedRequestSize.String()))

		extendedSize, shouldRequeue, extendErr := r.extendLVToSize("reconcileLLVCreateFunc", llv, lvg, alignedRequestSize)
		if shouldRequeue {
			return resource.Quantity{}, true, extendErr
		}
		actualSize = extendedSize
	}

	return actualSize, false, nil
}

func (r *Reconciler) thinSourceLVName(ctx context.Context, llv *v1alpha1.LVMLogicalVolume, lvg *v1alpha1.LVMVolumeGroup) (string, bool, error) {
	if llv.Spec.Type != internal.Thin || llv.Spec.Source == nil {
		return "", false, nil
	}

	switch llv.Spec.Source.Kind {
	case "LVMLogicalVolume":
		sourceLLV := &v1alpha1.LVMLogicalVolume{}
		if err := r.cl.Get(ctx, types.NamespacedName{Name: llv.Spec.Source.Name}, sourceLLV); err != nil {
			r.log.Error(fmt.Sprintf("unable to get source LVMLogicalVolume %s for the LVMLogicalVolume %s", llv.Spec.Source.Name, llv.Name), logger.Err(err))
			return "", true, err
		}

		if sourceLLV.Spec.LVMVolumeGroupName != lvg.Name {
			return "", false, errors.New("cloned volume should be in the same volume group as the source volume")
		}

		return sourceLLV.Spec.ActualLVNameOnTheNode, false, nil
	case "LVMLogicalVolumeSnapshot":
		return r.snapshotSourceLVName(ctx, llv, lvg)
	}

	return "", false, nil
}

func (r *Reconciler) reconcileLLVUpdateFunc(
	ctx context.Context,
	llv *v1alpha1.LVMLogicalVolume,
	lvg *v1alpha1.LVMVolumeGroup,
) (bool, error) {
	log := r.log

	log.Debug(fmt.Sprintf("starts reconciliation for the LVMLogicalVolume %s", llv.Name))

	// status might be nil if a user creates the resource with LV name which matches existing LV on the node
	if llv.Status == nil {
		err := r.llvCl.UpdatePhaseIfNeeded(ctx, llv, v1alpha1.PhasePending, "")
		if err != nil {
			log.Error(fmt.Sprintf("unable to update the LVMLogicalVolume %s", llv.Name), logger.Err(err))
			return true, err
		}
	}

	// it needs to get current LV size from the node as status might be nil
	log.Debug(fmt.Sprintf("tries to get LVMLogicalVolume %s actual size before the extension", llv.Name))
	actualSize := r.getLVActualSize(lvg.Spec.ActualVGNameOnTheNode, llv.Spec.ActualLVNameOnTheNode)
	if actualSize.Value() == 0 {
		log.Warn(fmt.Sprintf("LV %s of the LVMLogicalVolume %s has zero size (likely LV was not updated in the cache) ", llv.Spec.ActualLVNameOnTheNode, llv.Name))
		return true, nil
	}
	log.Debug(fmt.Sprintf("successfully got LVMLogicalVolume %s actual size %s before the extension", llv.Name, actualSize.String()))

	log.Debug(fmt.Sprintf("tries to count the LVMLogicalVolume %s requested size", llv.Name))
	llvRequestSize, err := utils.GetLLVRequestedSize(llv, lvg)
	if err != nil {
		r.log.Error(fmt.Sprintf("unable to get LVMLogicalVolume %s requested size", llv.Name), logger.Err(err))
		return false, err
	}
	log.Debug(fmt.Sprintf("successfully counted the LVMLogicalVolume %s requested size: %s", llv.Name, llvRequestSize.String()))

	alignedRequestSize, err := utils.AlignRequestedSizeToExtent(llv.Spec.Size, llvRequestSize, llvExtentSize(lvg))
	if err != nil {
		log.Error(fmt.Sprintf("unable to align size for LVMLogicalVolume %s", llv.Name), logger.Err(err))
		return true, err
	}

	if actualSize.Value() >= alignedRequestSize.Value() {
		log.Warn(fmt.Sprintf("the LV %s in VG %s actual size %s is already >= aligned requested size %s", llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode, actualSize.String(), alignedRequestSize.String()))

		if err := r.llvCl.UpdatePhaseToCreatedIfNeeded(ctx, llv, lvg, actualSize); err != nil {
			return true, err
		}

		log.Info(fmt.Sprintf("successfully ended reconciliation for the LVMLogicalVolume %s", llv.Name))

		return false, nil
	}

	extendingSize := subtractQuantity(alignedRequestSize, actualSize)
	log.Trace(fmt.Sprintf("the LV %s in VG %s has extending size %s", llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode, extendingSize.String()))
	if extendingSize.Value() < 0 {
		err = fmt.Errorf("specified LV size %dB is less than actual one on the node %dB", alignedRequestSize.Value(), actualSize.Value())
		log.Error(fmt.Sprintf("unable to extend the LVMLogicalVolume %s", llv.Name), logger.Err(err))
		return false, err
	}

	log.Info(fmt.Sprintf("the LVMLogicalVolume %s should be resized", llv.Name))
	if llv.Status.Phase != v1alpha1.PhaseFailed {
		err := r.llvCl.UpdatePhaseIfNeeded(ctx, llv, v1alpha1.PhaseResizing, "")
		if err != nil {
			log.Error(fmt.Sprintf("unable to update the LVMLogicalVolume %s", llv.Name), logger.Err(err))
			return true, err
		}
	}

	newActualSize, shouldRequeue, err := r.extendLV(ctx, llv, lvg, extendingSize, alignedRequestSize)
	if shouldRequeue {
		return true, err
	}

	// need this here as a user might create the LLV with existing LV
	if err := r.llvCl.UpdatePhaseToCreatedIfNeeded(ctx, llv, lvg, newActualSize); err != nil {
		return true, err
	}

	log.Info(fmt.Sprintf("successfully ended reconciliation for the LVMLogicalVolume %s", llv.Name))
	return false, nil
}

func (r *Reconciler) extendLV(
	ctx context.Context,
	llv *v1alpha1.LVMLogicalVolume,
	lvg *v1alpha1.LVMVolumeGroup,
	extendingSize, alignedRequestSize resource.Quantity,
) (resource.Quantity, bool, error) {
	log := r.log

	unlock, err := r.lockThinPool(ctx, llv, lvg)
	if err != nil {
		log.Error(fmt.Sprintf("unable to start extending the LV %s of the LVMLogicalVolume %s", llv.Spec.ActualLVNameOnTheNode, llv.Name), logger.Err(err))
		return resource.Quantity{}, true, err
	}
	defer unlock()

	if llv.Spec.Type == internal.Thin {
		log.Trace(fmt.Sprintf("the LVMLogicalVolume %s, LV: %s, VG: %s, thin pool: %s, extending size: %s", llv.Name, llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode, llv.Spec.Thin.PoolName, extendingSize.String()))
		if err := r.checkThinPoolSpace(lvg, llv.Spec.Thin.PoolName, llv.Spec.ActualLVNameOnTheNode, "", alignedRequestSize); err != nil {
			log.Error(fmt.Sprintf("the LV %s of the LVMLogicalVolume %s cannot be extended to %s in the thin pool %s", llv.Spec.ActualLVNameOnTheNode, llv.Name, alignedRequestSize.String(), llv.Spec.Thin.PoolName), logger.Err(err))
			return resource.Quantity{}, true, err
		}
	} else {
		freeSpace := utils.GetFreeLVGSpaceForLLV(lvg, llv)
		log.Trace(fmt.Sprintf("the LVMLogicalVolume %s, LV: %s, VG: %s, type: %s, extending size: %s, free space: %s", llv.Name, llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode, llv.Spec.Type, extendingSize.String(), freeSpace.String()))

		if freeSpace.Value() < extendingSize.Value() {
			err = errors.New("not enough space")
			log.Error(fmt.Sprintf("the LV %s aligned requested size %s of the LVMLogicalVolume %s is more than actual free space %s", llv.Spec.ActualLVNameOnTheNode, alignedRequestSize.String(), llv.Name, freeSpace.String()), logger.Err(err))

			return resource.Quantity{}, true, err
		}
	}

	log.Debug(fmt.Sprintf("LV %s of the LVMLogicalVolume %s will be extended to size: %s", llv.Spec.ActualLVNameOnTheNode, llv.Name, alignedRequestSize.String()))
	return r.extendLVToSize("reconcileLLVUpdateFunc", llv, lvg, alignedRequestSize)
}

// extendLVToSize grows the LV of the given LVMLogicalVolume to alignedSize and
// returns the size LVM reports afterwards.
//
// The size is re-read and validated instead of being assumed, because ExtendLV
// reports a no-op resize as success: LVM signals it on stderr ("No size change.",
// "New size (...) matches existing size (...)") and filterStdErr deliberately
// treats those lines as benign. Handing an unverified size to
// UpdatePhaseToCreatedIfNeeded would move the LLV to Created at the wrong size
// with no further reconcile scheduled, and the CSI caller would then wait
// forever for a size that never materialises.
//
// The returned flag means "requeue"; it is set both on a hard error (returned as
// the third value) and when the extension simply could not be confirmed yet.
func (r *Reconciler) extendLVToSize(
	caller string,
	llv *v1alpha1.LVMLogicalVolume,
	lvg *v1alpha1.LVMVolumeGroup,
	alignedSize resource.Quantity,
) (resource.Quantity, bool, error) {
	cmd, err := r.commands.ExtendLV(alignedSize.Value(), lvg.Spec.ActualVGNameOnTheNode, llv.Spec.ActualLVNameOnTheNode)
	r.log.Debug(fmt.Sprintf("[%s] ran cmd: %s", caller, cmd))
	if err != nil {
		r.log.Error(fmt.Sprintf("[%s] unable to extend the LV %s of the LVMLogicalVolume %s to size %s, type: %s", caller, llv.Spec.ActualLVNameOnTheNode, llv.Name, alignedSize.String(), llv.Spec.Type), logger.Err(err))
		return resource.Quantity{}, true, err
	}

	r.log.Info(fmt.Sprintf("[%s] successfully extended LV %s in VG %s for LVMLogicalVolume resource with name: %s", caller, llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode, llv.Name))

	r.log.Debug(fmt.Sprintf("[%s] tries to get LVMLogicalVolume %s actual size after the extension directly from LVM", caller, llv.Name))
	lvData, getLVCmd, _, getLVErr := r.commands.GetLV(lvg.Spec.ActualVGNameOnTheNode, llv.Spec.ActualLVNameOnTheNode)
	r.log.Debug(fmt.Sprintf("[%s] ran cmd: %s", caller, getLVCmd))
	if getLVErr != nil {
		r.log.Warn(fmt.Sprintf("[%s] unable to get LV %s info from LVM after the extension: %s, will retry", caller, llv.Spec.ActualLVNameOnTheNode, getLVErr.Error()))
		return resource.Quantity{}, true, nil
	}

	newActualSize := lvData.LVSize
	if newActualSize.Value() < alignedSize.Value() {
		r.log.Warn(fmt.Sprintf("[%s] LV %s of the LVMLogicalVolume %s was extended but LVM still reports size %s which is less than the requested %s, will retry", caller, llv.Spec.ActualLVNameOnTheNode, llv.Name, newActualSize.String(), alignedSize.String()))
		return resource.Quantity{}, true, nil
	}

	r.log.Trace(fmt.Sprintf("[%s] the LV %s in VG %s has actual size %s after the extension", caller, llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode, newActualSize.String()))
	return newActualSize, false, nil
}

func (r *Reconciler) reconcileLLVDeleteFunc(
	ctx context.Context,
	llv *v1alpha1.LVMLogicalVolume,
	lvg *v1alpha1.LVMVolumeGroup,
) (bool, error) {
	log := r.log

	log.Debug(fmt.Sprintf("starts reconciliation for the LVMLogicalVolume %s", llv.Name))

	// The Logical Volume stays until every other controller has let go of the
	// resource: a finalizer of its consumer means the volume may still be in
	// use. The wait is published on the resource, since a deletion that holds
	// with nothing but a debug line looks like one that is stuck for no reason.
	if foreign := foreignFinalizers(llv); len(foreign) > 0 {
		message := fmt.Sprintf("waiting for the finalizers %s to be removed by their owners before deleting the Logical Volume", strings.Join(foreign, ", "))
		changed, err := r.llvCl.SetDeletingIfNeeded(ctx, llv, message)
		if err != nil {
			// Not returned: Reconcile would turn it into the Failed phase, and
			// nothing has failed on the Logical Volume, which is in place.
			log.Error(fmt.Sprintf("unable to report the pending deletion of the LVMLogicalVolume %s", llv.Name), logger.Err(err))
			return true, nil
		}
		if changed {
			log.Info(fmt.Sprintf("the deletion of the LVMLogicalVolume %s waits for the finalizers %s", llv.Name, strings.Join(foreign, ", ")))
		}
		return false, nil
	}

	shouldRequeue, err := r.deleteLVIfNeeded(ctx, lvg.Spec.ActualVGNameOnTheNode, llv)
	if err != nil {
		log.Error(fmt.Sprintf("unable to delete the LV %s in VG %s", llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode), logger.Err(err))
		return shouldRequeue, err
	}

	log.Info(fmt.Sprintf("successfully deleted the LV %s in VG %s", llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode))

	err = r.removeLLVFinalizersIfExist(ctx, llv)
	if err != nil {
		log.Error(fmt.Sprintf("unable to remove finalizers from the LVMLogicalVolume %s", llv.Name), logger.Err(err))
		return true, err
	}

	log.Info(fmt.Sprintf("successfully ended reconciliation for the LVMLogicalVolume %s", llv.Name))
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
	log := r.log

	var removed bool
	for i, f := range llv.Finalizers {
		if f == internal.SdsNodeConfiguratorFinalizer {
			llv.Finalizers = append(llv.Finalizers[:i], llv.Finalizers[i+1:]...)
			removed = true
			log.Debug(fmt.Sprintf("removed finalizer %s from the LVMLogicalVolume %s", internal.SdsNodeConfiguratorFinalizer, llv.Name))
			break
		}
	}

	if removed {
		log.Trace(fmt.Sprintf("removed finalizer %s from the LVMLogicalVolume %s", internal.SdsNodeConfiguratorFinalizer, llv.Name))
		err := r.updateLVMLogicalVolumeSpec(ctx, llv)
		if err != nil {
			r.log.Error(fmt.Sprintf("unable to update the LVMVolumeGroup %s", llv.Name), logger.Err(err))
			return err
		}
	}

	return nil
}

// checkIfLVBelongsToLLV reports whether the LV found on the node is the one the
// LVMLogicalVolume describes. It exists to keep the agent from touching a
// same-name LV that belongs to something else.
//
// The cache holds a zero-valued LVData for an LV added right after `lvcreate`
// (see cache.AddLV) until a scan fills it in, or drops it once two scans in a
// row did not list the LV. Such an entry carries
// no evidence either way, and indexing its empty LVAttr used to panic the whole
// agent. It is treated as belonging: the entry can only have been created by the
// agent itself for this exact VG/LV pair, and the delete path has to be able to
// remove a just-created LV — refusing to would drop the finalizer while the LV
// stays on the node.
func checkIfLVBelongsToLLV(llv *v1alpha1.LVMLogicalVolume, lv *internal.LVData) bool {
	if !utils.IsLVAttrKnown(lv.LVAttr) {
		return true
	}

	switch llv.Spec.Type {
	case internal.Thin:
		if llv.Spec.Thin == nil || lv.PoolName != llv.Spec.Thin.PoolName {
			return false
		}
	case internal.Thick:
		if !utils.IsLVThick(lv.LVAttr) ||
			utils.IsLVContiguous(lv.LVAttr) != isContiguous(llv) {
			return false
		}
	}

	return true
}

func (r *Reconciler) deleteLVIfNeeded(ctx context.Context, vgName string, llv *v1alpha1.LVMLogicalVolume) (bool, error) {
	log := r.log

	lv := r.sdsCache.FindLV(vgName, llv.Spec.ActualLVNameOnTheNode)
	if lv == nil || !lv.Exist {
		log.Warn(fmt.Sprintf("did not find LV %s in VG %s", llv.Spec.ActualLVNameOnTheNode, vgName))
		return false, nil
	}

	// this case prevents unexpected same-name LV deletions which does not actually belong to our LLV
	if !checkIfLVBelongsToLLV(llv, &lv.Data) {
		log.Warn(fmt.Sprintf("no need to delete LV %s as it doesn't belong to LVMLogicalVolume %s", lv.Data.LVName, llv.Name))
		return false, nil
	}

	if cleanupMethodPtr := llv.Spec.VolumeCleanup; cleanupMethodPtr != nil {
		// staticcheck reports SA4023 on the next line when the linter runs with
		// -tags=ce, and it is right about that edition: the CE cleanupVolume
		// (reconciler_ce.go) is a stub that always returns "volume cleanup is not
		// supported in your edition", so there the comparison cannot be false. The
		// check has to stay all the same — under ee the call does the actual cleanup
		// and is expected to succeed — and in CE this branch is exactly what reports
		// the unsupported request back to the user.
		//nolint:staticcheck // SA4023: unconditionally non-nil only in the CE stub
		if shouldRequeue, err := r.cleanupVolume(ctx, llv, lv, vgName, *cleanupMethodPtr); err != nil {
			return shouldRequeue, err
		}
	}

	cmd, err := r.commands.RemoveLV(vgName, llv.Spec.ActualLVNameOnTheNode)
	log.Debug(fmt.Sprintf("runs cmd: %s", cmd))
	if err != nil {
		if reflect.ValueOf(lv.Data).IsZero() {
			gone, checkErr := r.lvIsGoneFromVG(ctx, vgName, llv.Spec.ActualLVNameOnTheNode)
			if checkErr != nil {
				log.Warn(fmt.Sprintf("unable to check whether LV %s is still in VG %s", llv.Spec.ActualLVNameOnTheNode, vgName), logger.Err(checkErr))
			}
			if gone {
				log.Warn(fmt.Sprintf("LV %s is not in VG %s although the cache still holds it as just created, treat it as removed", llv.Spec.ActualLVNameOnTheNode, vgName))
				r.sdsCache.MarkLVAsRemoved(vgName, llv.Spec.ActualLVNameOnTheNode)
				return false, nil
			}
		}

		log.Error(fmt.Sprintf("unable to remove LV %s from VG %s", llv.Spec.ActualLVNameOnTheNode, vgName), logger.Err(err))
		return true, err
	}

	log.Debug(fmt.Sprintf("mark LV %s in the cache as removed", llv.Spec.ActualLVNameOnTheNode))
	r.sdsCache.MarkLVAsRemoved(vgName, llv.Spec.ActualLVNameOnTheNode)

	return false, nil
}

func (r *Reconciler) lvIsGoneFromVG(ctx context.Context, vgName, lvName string) (bool, error) {
	if _, cmd, _, err := r.commands.GetVG(vgName); err != nil {
		return false, fmt.Errorf("unable to confirm VG %s with %s: %w", vgName, cmd, err)
	}

	lvs, cmd, _, err := r.commands.GetAllLVs(ctx)
	if err != nil {
		return false, fmt.Errorf("unable to list LVs with %s: %w", cmd, err)
	}

	for _, lv := range lvs {
		if lv.VGName == vgName && lv.LVName == lvName {
			return false, nil
		}
	}

	return true, nil
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
	if slices.Contains(llv.Finalizers, internal.SdsNodeConfiguratorFinalizer) {
		return false, nil
	}

	llv.Finalizers = append(llv.Finalizers, internal.SdsNodeConfiguratorFinalizer)

	r.log.Trace(fmt.Sprintf("added finalizer %s to the LVMLogicalVolume %s", internal.SdsNodeConfiguratorFinalizer, llv.Name))
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

		// Cloning and restoring are done with `lvcreate -s`, which exists for thin
		// volumes only. Without this check the create path falls into its Thick
		// branch, produces an empty LV and drops the source silently, leaving the
		// user with a volume that reports Created but holds no data.
		if llv.Spec.Source != nil {
			reason.WriteString("Source specified for Thick LV: cloning and restoring are supported for Thin LVs only. ")
		}
	}

	// if a specified Thick LV name matches the existing Thin one
	lv := r.sdsCache.FindLV(lvg.Spec.ActualVGNameOnTheNode, llv.Spec.ActualLVNameOnTheNode)
	if lv != nil && !checkIfLVBelongsToLLV(llv, &lv.Data) {
		fmt.Fprintf(&reason, "Specified LV %s is already created and it is doesn't match the one on the node.", lv.Data.LVName)
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

// foreignFinalizers returns the finalizers on llv other than the agent's own.
func foreignFinalizers(llv *v1alpha1.LVMLogicalVolume) []string {
	var foreign []string
	for _, f := range llv.Finalizers {
		if f != internal.SdsNodeConfiguratorFinalizer {
			foreign = append(foreign, f)
		}
	}
	return foreign
}
