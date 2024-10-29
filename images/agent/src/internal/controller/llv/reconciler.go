package llv

import (
	"agent/internal"
	"agent/internal/cache"
	"agent/internal/controller"
	"agent/internal/logger"
	"agent/internal/monitoring"
	"agent/internal/utils"
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/google/go-cmp/cmp"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/strings/slices"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const ReconcilerName = "lvm-logical-volume-watcher-controller"

type Reconciler struct {
	cl       client.Client
	log      logger.Logger
	lvgCl    *utils.LVGClient
	llvCl    *utils.LLVClient
	metrics  monitoring.Metrics
	sdsCache *cache.Cache
	cfg      ReconcilerConfig
}

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
	cfg ReconcilerConfig,
) *Reconciler {
	return &Reconciler{
		cl:  cl,
		log: log,
		lvgCl: utils.NewLVGClient(
			cl,
			log,
			metrics,
			cfg.NodeName,
			ReconcilerName,
		),
		llvCl: utils.NewLLVClient(
			cl, log,
		),
		metrics:  metrics,
		sdsCache: sdsCache,
		cfg:      cfg,
	}
}

// Name implements controller.Reconciler.
func (r *Reconciler) Name() string {
	return ReconcilerName
}

func (r *Reconciler) MaxConcurrentReconciles() int {
	return 10
}

// ShouldReconcileUpdate implements controller.Reconciler.
func (r *Reconciler) ShouldReconcileUpdate(objectOld *v1alpha1.LVMLogicalVolume, objectNew *v1alpha1.LVMLogicalVolume) bool {
	r.log.Info(fmt.Sprintf("[RunLVMLogicalVolumeWatcherController] got an update event for the LVMLogicalVolume: %s", objectNew.GetName()))

	// TODO: Figure out how to log it in our logger.
	if r.cfg.Loglevel == "4" {
		fmt.Println("==============START DIFF==================")
		fmt.Println(cmp.Diff(objectOld, objectNew))
		fmt.Println("==============END DIFF==================")
	}

	if reflect.DeepEqual(objectOld.Spec, objectNew.Spec) && objectNew.DeletionTimestamp == nil {
		r.log.Info(fmt.Sprintf("[RunLVMLogicalVolumeWatcherController] no target changes were made for the LVMLogicalVolume %s. No need to reconcile the request", objectNew.Name))
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
	r.log.Info(fmt.Sprintf("[RunLVMLogicalVolumeWatcherController] Reconciler starts reconciliation of the LVMLogicalVolume: %s", llv.Name))

	lvg, err := r.lvgCl.GetLVMVolumeGroup(ctx, llv.Spec.LVMVolumeGroupName)
	if err != nil {
		if k8serr.IsNotFound(err) {
			r.log.Error(err, fmt.Sprintf("[ReconcileLVMLogicalVolume] LVMVolumeGroup %s not found for LVMLogicalVolume %s. Retry in %s", llv.Spec.LVMVolumeGroupName, llv.Name, r.cfg.VolumeGroupScanInterval.String()))
			err = r.llvCl.UpdatePhaseIfNeeded(
				ctx,
				llv,
				internal.LLVStatusPhaseFailed,
				fmt.Sprintf("LVMVolumeGroup %s not found", llv.Spec.LVMVolumeGroupName),
			)
			if err != nil {
				r.log.Error(err, fmt.Sprintf("[ReconcileLVMLogicalVolume] unable to update the LVMLogicalVolume %s", llv.Name))
				return controller.Result{}, err
			}

			return controller.Result{
				RequeueAfter: r.cfg.VolumeGroupScanInterval,
			}, nil
		}

		err = r.llvCl.UpdatePhaseIfNeeded(
			ctx,
			llv,
			internal.LLVStatusPhaseFailed,
			fmt.Sprintf("Unable to get selected LVMVolumeGroup, err: %s", err.Error()),
		)
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[ReconcileLVMLogicalVolume] unable to update the LVMLogicalVolume %s", llv.Name))
		}
		return controller.Result{}, err
	}

	if !utils.LVGBelongsToNode(lvg, r.cfg.NodeName) {
		r.log.Info(fmt.Sprintf("[ReconcileLVMLogicalVolume] the LVMVolumeGroup %s of the LVMLogicalVolume %s does not belongs to the current node: %s. Reconciliation stopped", lvg.Name, llv.Name, r.cfg.NodeName))
		return controller.Result{}, nil
	}
	r.log.Info(fmt.Sprintf("[ReconcileLVMLogicalVolume] the LVMVolumeGroup %s of the LVMLogicalVolume %s belongs to the current node: %s. Reconciliation continues", lvg.Name, llv.Name, r.cfg.NodeName))

	// this case prevents the unexpected behavior when the controller runs up with existing LVMLogicalVolumes
	if vgs, _ := r.sdsCache.GetVGs(); len(vgs) == 0 {
		r.log.Warning(fmt.Sprintf("[RunLVMLogicalVolumeWatcherController] unable to reconcile the request as no VG was found in the cache. Retry in %s", r.cfg.VolumeGroupScanInterval.String()))
		return controller.Result{RequeueAfter: r.cfg.VolumeGroupScanInterval}, nil
	}

	r.log.Debug(fmt.Sprintf("[ReconcileLVMLogicalVolume] tries to add the finalizer %s to the LVMLogicalVolume %s", internal.SdsNodeConfiguratorFinalizer, llv.Name))
	added, err := r.addLLVFinalizerIfNotExist(ctx, llv)
	if err != nil {
		r.log.Error(err, fmt.Sprintf("[ReconcileLVMLogicalVolume] unable to update the LVMLogicalVolume %s", llv.Name))
		return controller.Result{}, err
	}
	if added {
		r.log.Debug(fmt.Sprintf("[ReconcileLVMLogicalVolume] successfully added the finalizer %s to the LVMLogicalVolume %s", internal.SdsNodeConfiguratorFinalizer, llv.Name))
	} else {
		r.log.Debug(fmt.Sprintf("[ReconcileLVMLogicalVolume] no need to add the finalizer %s to the LVMLogicalVolume %s", internal.SdsNodeConfiguratorFinalizer, llv.Name))
	}

	r.log.Info(fmt.Sprintf("[ReconcileLVMLogicalVolume] starts to validate the LVMLogicalVolume %s", llv.Name))
	valid, reason := r.validateLVMLogicalVolume(llv, lvg)
	if !valid {
		r.log.Warning(fmt.Sprintf("[ReconcileLVMLogicalVolume] the LVMLogicalVolume %s is not valid, reason: %s", llv.Name, reason))
		err = r.llvCl.UpdatePhaseIfNeeded(ctx, llv, internal.LLVStatusPhaseFailed, reason)
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[ReconcileLVMLogicalVolume] unable to update the LVMLogicalVolume %s", llv.Name))
			return controller.Result{}, err
		}

		return controller.Result{}, nil
	}
	r.log.Info(fmt.Sprintf("[ReconcileLVMLogicalVolume] successfully validated the LVMLogicalVolume %s", llv.Name))

	shouldRequeue, err := r.ReconcileLVMLogicalVolume(ctx, llv, lvg)
	if err != nil {
		r.log.Error(err, fmt.Sprintf("[RunLVMLogicalVolumeWatcherController] an error occurred while reconciling the LVMLogicalVolume: %s", llv.Name))
		updErr := r.llvCl.UpdatePhaseIfNeeded(ctx, llv, internal.LLVStatusPhaseFailed, err.Error())
		if updErr != nil {
			r.log.Error(updErr, fmt.Sprintf("[RunLVMLogicalVolumeWatcherController] unable to update the LVMLogicalVolume %s", llv.Name))
			return controller.Result{}, updErr
		}
	}
	if shouldRequeue {
		r.log.Info(fmt.Sprintf("[RunLVMLogicalVolumeWatcherController] some issues were occurred while reconciliation the LVMLogicalVolume %s. Requeue the request in %s", llv.Name, r.cfg.LLVRequeueInterval.String()))
		return controller.Result{RequeueAfter: r.cfg.LLVRequeueInterval}, nil
	}

	r.log.Info(fmt.Sprintf("[RunLVMLogicalVolumeWatcherController] successfully ended reconciliation of the LVMLogicalVolume %s", llv.Name))
	return controller.Result{}, nil
}

func (r *Reconciler) ReconcileLVMLogicalVolume(ctx context.Context, llv *v1alpha1.LVMLogicalVolume, lvg *v1alpha1.LVMVolumeGroup) (bool, error) {
	r.log.Debug(fmt.Sprintf("[ReconcileLVMLogicalVolume] starts the reconciliation for the LVMLogicalVolume %s", llv.Name))

	r.log.Debug(fmt.Sprintf("[ReconcileLVMLogicalVolume] tries to identify the reconciliation type for the LVMLogicalVolume %s", llv.Name))
	r.log.Trace(fmt.Sprintf("[ReconcileLVMLogicalVolume] %+v", llv))

	switch r.identifyReconcileFunc(lvg.Spec.ActualVGNameOnTheNode, llv) {
	case internal.CreateReconcile:
		return r.reconcileLLVCreateFunc(ctx, llv, lvg)
	case internal.UpdateReconcile:
		return r.reconcileLLVUpdateFunc(ctx, llv, lvg)
	case internal.DeleteReconcile:
		return r.reconcileLLVDeleteFunc(ctx, llv, lvg)
	default:
		r.log.Info(fmt.Sprintf("[runEventReconcile] the LVMLogicalVolume %s has compeleted configuration and should not be reconciled", llv.Name))
		if llv.Status.Phase != internal.LLVStatusPhaseCreated {
			r.log.Warning(fmt.Sprintf("[runEventReconcile] the LVMLogicalVolume %s should not be reconciled but has an unexpected phase: %s. Setting the phase to %s", llv.Name, llv.Status.Phase, internal.LLVStatusPhaseCreated))
			err := r.llvCl.UpdatePhaseIfNeeded(ctx, llv, internal.LLVStatusPhaseCreated, "")
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
	r.log.Debug(fmt.Sprintf("[reconcileLLVCreateFunc] starts reconciliation for the LVMLogicalVolume %s", llv.Name))

	// this check prevents infinite resource updating after retries
	if llv.Status == nil {
		err := r.llvCl.UpdatePhaseIfNeeded(ctx, llv, internal.LLVStatusPhasePending, "")
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[reconcileLLVCreateFunc] unable to update the LVMLogicalVolume %s", llv.Name))
			return true, err
		}
	}
	llvRequestSize, err := utils.GetLLVRequestedSize(llv, lvg)
	if err != nil {
		r.log.Error(err, fmt.Sprintf("[reconcileLLVCreateFunc] unable to get LVMLogicalVolume %s requested size", llv.Name))
		return false, err
	}

	freeSpace := utils.GetFreeLVGSpaceForLLV(lvg, llv)
	r.log.Trace(fmt.Sprintf("[reconcileLLVCreateFunc] the LVMLogicalVolume %s, LV: %s, VG: %s type: %s requested size: %s, free space: %s", llv.Name, llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode, llv.Spec.Type, llvRequestSize.String(), freeSpace.String()))

	if !utils.AreSizesEqualWithinDelta(llvRequestSize, freeSpace, internal.ResizeDelta) {
		if freeSpace.Value() < llvRequestSize.Value()+internal.ResizeDelta.Value() {
			err = errors.New("not enough space")
			r.log.Error(err, fmt.Sprintf("[reconcileLLVCreateFunc] the LV %s requested size %s of the LVMLogicalVolume %s is more than the actual free space %s", llv.Spec.ActualLVNameOnTheNode, llvRequestSize.String(), llv.Name, freeSpace.String()))

			// we return true cause the user might manage LVMVolumeGroup free space without changing the LLV
			return true, err
		}
	}

	var cmd string
	switch llv.Spec.Type {
	case internal.Thick:
		r.log.Debug(fmt.Sprintf("[reconcileLLVCreateFunc] LV %s will be created in VG %s with size: %s", llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode, llvRequestSize.String()))
		cmd, err = utils.CreateThickLogicalVolume(lvg.Spec.ActualVGNameOnTheNode, llv.Spec.ActualLVNameOnTheNode, llvRequestSize.Value(), isContiguous(llv))
	case internal.Thin:
		if llv.Spec.Source == nil {
			r.log.Debug(fmt.Sprintf("[reconcileLLVCreateFunc] LV %s of the LVMLogicalVolume %s will be created in Thin-pool %s with size %s", llv.Spec.ActualLVNameOnTheNode, llv.Name, llv.Spec.Thin.PoolName, llvRequestSize.String()))
			cmd, err = utils.CreateThinLogicalVolume(lvg.Spec.ActualVGNameOnTheNode, llv.Spec.Thin.PoolName, llv.Spec.ActualLVNameOnTheNode, llvRequestSize.Value())
		} else {
			// volume is a clone
			r.log.Debug(fmt.Sprintf("[reconcileLLVCreateFunc] Snapshot (for source %s) LV %s of the LVMLogicalVolume %s will be created in Thin-pool %s with size %s", llv.Spec.Source.Name, llv.Spec.ActualLVNameOnTheNode, llv.Name, llv.Spec.Thin.PoolName, llvRequestSize.String()))

			var sourceVgName, sourceVolumeName string
			if llv.Spec.Source.Kind == "LVMLogicalVolume" {
				sourceLlv := &v1alpha1.LVMLogicalVolume{}
				if err = r.cl.Get(ctx, types.NamespacedName{Name: llv.Spec.Source.Name}, sourceLlv); err != nil {
					r.log.Error(err, fmt.Sprintf("[reconcileLLVCreateFunc] unable to find source LVMLogicalVolume %s", llv.Spec.Source.Name))
					return false, err
				}

				sourceVolumeName = sourceLlv.Spec.ActualLVNameOnTheNode
				sourceVgName = sourceLlv.Spec.LVMVolumeGroupName

				// TODO snapshots: validate source llv
			} else if llv.Spec.Source.Kind == "LVMLogicalVolumeSnapshot" {
				sourceSnapshot := &v1alpha1.LVMLogicalVolumeSnapshot{}
				if err = r.cl.Get(ctx, types.NamespacedName{Name: llv.Spec.Source.Name}, sourceSnapshot); err != nil {
					r.log.Error(err, fmt.Sprintf("[reconcileLLVCreateFunc] unable to find source LVMLogicalVolumeSnapshot %s", llv.Spec.Source.Name))
					return false, err
				}
				sourceVolumeName = sourceSnapshot.Spec.ActualLVNameOnTheNode
				sourceVgName = sourceSnapshot.Spec.LVMVolumeGroupName
				// TODO snapshots: validate source snapshot
			} else {
				return false, fmt.Errorf("source kind is not supported: %s", llv.Spec.Source.Kind)
			}
			cmd, err = utils.CreateThinLogicalVolumeSnapshot(llv.Spec.ActualLVNameOnTheNode, sourceVgName, sourceVolumeName)

		}
	}
	r.log.Debug(fmt.Sprintf("[reconcileLLVCreateFunc] runs cmd: %s", cmd))
	if err != nil {
		r.log.Error(err, fmt.Sprintf("[reconcileLLVCreateFunc] unable to create a %s LogicalVolume for the LVMLogicalVolume %s", llv.Spec.Type, llv.Name))
		return true, err
	}

	r.log.Info(fmt.Sprintf("[reconcileLLVCreateFunc] successfully created LV %s in VG %s for LVMLogicalVolume resource with name: %s", llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode, llv.Name))

	r.log.Debug(fmt.Sprintf("[reconcileLLVCreateFunc] adds the LV %s to the cache", llv.Spec.ActualLVNameOnTheNode))
	r.sdsCache.AddLV(lvg.Spec.ActualVGNameOnTheNode, llv.Spec.ActualLVNameOnTheNode)
	r.log.Debug(fmt.Sprintf("[reconcileLLVCreateFunc] tries to get the LV %s actual size", llv.Spec.ActualLVNameOnTheNode))
	actualSize := r.getLVActualSize(lvg.Spec.ActualVGNameOnTheNode, llv.Spec.ActualLVNameOnTheNode)
	if actualSize.Value() == 0 {
		r.log.Warning(fmt.Sprintf("[reconcileLLVCreateFunc] unable to get actual size for LV %s in VG %s (likely LV was not found in the cache), retry...", llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode))
		return true, nil
	}
	r.log.Debug(fmt.Sprintf("[reconcileLLVCreateFunc] successfully got the LV %s actual size", llv.Spec.ActualLVNameOnTheNode))
	r.log.Trace(fmt.Sprintf("[reconcileLLVCreateFunc] the LV %s in VG: %s has actual size: %s", llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode, actualSize.String()))

	if err := r.llvCl.UpdatePhaseToCreatedIfNeeded(ctx, llv, actualSize); err != nil {
		return true, err
	}

	r.log.Info(fmt.Sprintf("[reconcileLLVCreateFunc] successfully ended the reconciliation for the LVMLogicalVolume %s", llv.Name))
	return false, nil
}

func (r *Reconciler) reconcileLLVUpdateFunc(
	ctx context.Context,
	llv *v1alpha1.LVMLogicalVolume,
	lvg *v1alpha1.LVMVolumeGroup,
) (bool, error) {
	r.log.Debug(fmt.Sprintf("[reconcileLLVUpdateFunc] starts reconciliation for the LVMLogicalVolume %s", llv.Name))

	// status might be nil if a user creates the resource with LV name which matches existing LV on the node
	if llv.Status == nil {
		err := r.llvCl.UpdatePhaseIfNeeded(ctx, llv, internal.LLVStatusPhasePending, "")
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[reconcileLLVUpdateFunc] unable to update the LVMLogicalVolume %s", llv.Name))
			return true, err
		}
	}

	// it needs to get current LV size from the node as status might be nil
	r.log.Debug(fmt.Sprintf("[reconcileLLVUpdateFunc] tries to get LVMLogicalVolume %s actual size before the extension", llv.Name))
	actualSize := r.getLVActualSize(lvg.Spec.ActualVGNameOnTheNode, llv.Spec.ActualLVNameOnTheNode)
	if actualSize.Value() == 0 {
		r.log.Warning(fmt.Sprintf("[reconcileLLVUpdateFunc] LV %s of the LVMLogicalVolume %s has zero size (likely LV was not updated in the cache) ", llv.Spec.ActualLVNameOnTheNode, llv.Name))
		return true, nil
	}
	r.log.Debug(fmt.Sprintf("[reconcileLLVUpdateFunc] successfully got LVMLogicalVolume %s actual size %s before the extension", llv.Name, actualSize.String()))

	r.log.Debug(fmt.Sprintf("[reconcileLLVUpdateFunc] tries to count the LVMLogicalVolume %s requested size", llv.Name))
	llvRequestSize, err := utils.GetLLVRequestedSize(llv, lvg)
	if err != nil {
		r.log.Error(err, fmt.Sprintf("[reconcileLLVCreateFunc] unable to get LVMLogicalVolume %s requested size", llv.Name))
		return false, err
	}
	r.log.Debug(fmt.Sprintf("[reconcileLLVUpdateFunc] successfully counted the LVMLogicalVolume %s requested size: %s", llv.Name, llvRequestSize.String()))

	if utils.AreSizesEqualWithinDelta(actualSize, llvRequestSize, internal.ResizeDelta) {
		r.log.Warning(fmt.Sprintf("[reconcileLLVUpdateFunc] the LV %s in VG %s has the same actual size %s as the requested size %s", llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode, actualSize.String(), llvRequestSize.String()))

		if err := r.llvCl.UpdatePhaseToCreatedIfNeeded(ctx, llv, actualSize); err != nil {
			return true, err
		}

		r.log.Info(fmt.Sprintf("[reconcileLLVUpdateFunc] successfully ended reconciliation for the LVMLogicalVolume %s", llv.Name))

		return false, nil
	}

	extendingSize := subtractQuantity(llvRequestSize, actualSize)
	r.log.Trace(fmt.Sprintf("[reconcileLLVUpdateFunc] the LV %s in VG %s has extending size %s", llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode, extendingSize.String()))
	if extendingSize.Value() < 0 {
		err = fmt.Errorf("specified LV size %dB is less than actual one on the node %dB", llvRequestSize.Value(), actualSize.Value())
		r.log.Error(err, fmt.Sprintf("[reconcileLLVUpdateFunc] unable to extend the LVMLogicalVolume %s", llv.Name))
		return false, err
	}

	r.log.Info(fmt.Sprintf("[reconcileLLVUpdateFunc] the LVMLogicalVolume %s should be resized", llv.Name))
	// this check prevents infinite resource updates after retry
	if llv.Status.Phase != internal.LLVStatusPhaseFailed {
		err := r.llvCl.UpdatePhaseIfNeeded(ctx, llv, internal.LLVStatusPhaseResizing, "")
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[reconcileLLVUpdateFunc] unable to update the LVMLogicalVolume %s", llv.Name))
			return true, err
		}
	}

	freeSpace := utils.GetFreeLVGSpaceForLLV(lvg, llv)
	r.log.Trace(fmt.Sprintf("[reconcileLLVUpdateFunc] the LVMLogicalVolume %s, LV: %s, VG: %s, type: %s, extending size: %s, free space: %s", llv.Name, llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode, llv.Spec.Type, extendingSize.String(), freeSpace.String()))

	if !utils.AreSizesEqualWithinDelta(freeSpace, extendingSize, internal.ResizeDelta) {
		if freeSpace.Value() < extendingSize.Value()+internal.ResizeDelta.Value() {
			err = errors.New("not enough space")
			r.log.Error(err, fmt.Sprintf("[reconcileLLVUpdateFunc] the LV %s requested size %s of the LVMLogicalVolume %s is more than actual free space %s", llv.Spec.ActualLVNameOnTheNode, llvRequestSize.String(), llv.Name, freeSpace.String()))

			// returns true cause a user might manage LVG free space without changing the LLV
			return true, err
		}
	}

	r.log.Debug(fmt.Sprintf("[reconcileLLVUpdateFunc] LV %s of the LVMLogicalVolume %s will be extended with size: %s", llv.Spec.ActualLVNameOnTheNode, llv.Name, llvRequestSize.String()))
	cmd, err := utils.ExtendLV(llvRequestSize.Value(), lvg.Spec.ActualVGNameOnTheNode, llv.Spec.ActualLVNameOnTheNode)
	r.log.Debug(fmt.Sprintf("[reconcileLLVUpdateFunc] runs cmd: %s", cmd))
	if err != nil {
		r.log.Error(err, fmt.Sprintf("[reconcileLLVUpdateFunc] unable to ExtendLV, name: %s, type: %s", llv.Spec.ActualLVNameOnTheNode, llv.Spec.Type))
		return true, err
	}

	r.log.Info(fmt.Sprintf("[reconcileLLVUpdateFunc] successfully extended LV %s in VG %s for LVMLogicalVolume resource with name: %s", llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode, llv.Name))

	r.log.Debug(fmt.Sprintf("[reconcileLLVUpdateFunc] tries to get LVMLogicalVolume %s actual size after the extension", llv.Name))
	newActualSize := r.getLVActualSize(lvg.Spec.ActualVGNameOnTheNode, llv.Spec.ActualLVNameOnTheNode)

	// this case might be triggered if sds cache will not update lv state in time
	if newActualSize.Value() == actualSize.Value() {
		r.log.Warning(fmt.Sprintf("[reconcileLLVUpdateFunc] LV %s of the LVMLogicalVolume %s was extended but cache is not updated yet. It will be retried", llv.Spec.ActualLVNameOnTheNode, llv.Name))
		return true, nil
	}

	r.log.Debug(fmt.Sprintf("[reconcileLLVUpdateFunc] successfully got LVMLogicalVolume %s actual size before the extension", llv.Name))
	r.log.Trace(fmt.Sprintf("[reconcileLLVUpdateFunc] the LV %s in VG %s actual size %s", llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode, newActualSize.String()))

	// need this here as a user might create the LLV with existing LV
	if err := r.llvCl.UpdatePhaseToCreatedIfNeeded(ctx, llv, newActualSize); err != nil {
		return true, err
	}

	r.log.Info(fmt.Sprintf("[reconcileLLVUpdateFunc] successfully ended reconciliation for the LVMLogicalVolume %s", llv.Name))
	return false, nil
}

func (r *Reconciler) reconcileLLVDeleteFunc(
	ctx context.Context,
	llv *v1alpha1.LVMLogicalVolume,
	lvg *v1alpha1.LVMVolumeGroup,
) (bool, error) {
	r.log.Debug(fmt.Sprintf("[reconcileLLVDeleteFunc] starts reconciliation for the LVMLogicalVolume %s", llv.Name))

	// The controller won't remove the LLV resource and LV volume till the resource has any other finalizer.
	if len(llv.Finalizers) != 0 {
		if len(llv.Finalizers) > 1 ||
			llv.Finalizers[0] != internal.SdsNodeConfiguratorFinalizer {
			r.log.Debug(fmt.Sprintf("[reconcileLLVDeleteFunc] unable to delete LVMLogicalVolume %s for now due to it has any other finalizer", llv.Name))
			return false, nil
		}
	}

	err := r.deleteLVIfNeeded(lvg.Spec.ActualVGNameOnTheNode, llv)
	if err != nil {
		r.log.Error(err, fmt.Sprintf("[reconcileLLVDeleteFunc] unable to delete the LV %s in VG %s", llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode))
		return true, err
	}

	r.log.Info(fmt.Sprintf("[reconcileLLVDeleteFunc] successfully deleted the LV %s in VG %s", llv.Spec.ActualLVNameOnTheNode, lvg.Spec.ActualVGNameOnTheNode))

	err = r.removeLLVFinalizersIfExist(ctx, llv)
	if err != nil {
		r.log.Error(err, fmt.Sprintf("[reconcileLLVDeleteFunc] unable to remove finalizers from the LVMVolumeGroup %s", llv.Name))
		return true, err
	}

	r.log.Info(fmt.Sprintf("[reconcileLLVDeleteFunc] successfully ended reconciliation for the LVMLogicalVolume %s", llv.Name))
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
	var removed bool
	for i, f := range llv.Finalizers {
		if f == internal.SdsNodeConfiguratorFinalizer {
			llv.Finalizers = append(llv.Finalizers[:i], llv.Finalizers[i+1:]...)
			removed = true
			r.log.Debug(fmt.Sprintf("[removeLLVFinalizersIfExist] removed finalizer %s from the LVMLogicalVolume %s", internal.SdsNodeConfiguratorFinalizer, llv.Name))
			break
		}
	}

	if removed {
		r.log.Trace(fmt.Sprintf("[removeLLVFinalizersIfExist] removed finalizer %s from the LVMLogicalVolume %s", internal.SdsNodeConfiguratorFinalizer, llv.Name))
		err := r.updateLVMLogicalVolumeSpec(ctx, llv)
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[updateLVMLogicalVolumeSpec] unable to update the LVMVolumeGroup %s", llv.Name))
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

func (r *Reconciler) deleteLVIfNeeded(vgName string, llv *v1alpha1.LVMLogicalVolume) error {
	lv := r.sdsCache.FindLV(vgName, llv.Spec.ActualLVNameOnTheNode)
	if lv == nil || !lv.Exist {
		r.log.Warning(fmt.Sprintf("[deleteLVIfNeeded] did not find LV %s in VG %s", llv.Spec.ActualLVNameOnTheNode, vgName))
		return nil
	}

	// this case prevents unexpected same-name LV deletions which does not actually belong to our LLV
	if !checkIfLVBelongsToLLV(llv, &lv.Data) {
		r.log.Warning(fmt.Sprintf("[deleteLVIfNeeded] no need to delete LV %s as it doesnt belong to LVMLogicalVolume %s", lv.Data.LVName, llv.Name))
		return nil
	}

	cmd, err := utils.RemoveLV(vgName, llv.Spec.ActualLVNameOnTheNode)
	r.log.Debug(fmt.Sprintf("[deleteLVIfNeeded] runs cmd: %s", cmd))
	if err != nil {
		r.log.Error(err, fmt.Sprintf("[deleteLVIfNeeded] unable to remove LV %s from VG %s", llv.Spec.ActualLVNameOnTheNode, vgName))
		return err
	}

	r.log.Debug(fmt.Sprintf("[deleteLVIfNeeded] mark LV %s in the cache as removed", lv.Data.LVName))
	r.sdsCache.MarkLVAsRemoved(lv.Data.VGName, lv.Data.LVName)

	return nil
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

	r.log.Trace(fmt.Sprintf("[addLLVFinalizerIfNotExist] added finalizer %s to the LVMLogicalVolume %s", internal.SdsNodeConfiguratorFinalizer, llv.Name))
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
		reason.WriteString(fmt.Sprintf("Specified LV %s is already created and it is doesnt match the one on the node.", lv.Data.LVName))
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
	if llv.Spec.Thick == nil {
		return false
	}

	return *llv.Spec.Thick.Contiguous
}
