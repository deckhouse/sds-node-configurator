package llvs

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"agent/internal"
	"agent/internal/cache"
	"agent/internal/controller"
	"agent/internal/logger"
	"agent/internal/monitoring"
	"agent/internal/utils"
)

const ReconcilerName = "lvm-logical-volume-snapshot-watcher-controller"

type Reconciler struct {
	cl       client.Client
	log      logger.Logger
	lvgCl    *utils.LVGClient
	metrics  monitoring.Metrics
	sdsCache *cache.Cache
	cfg      ReconcilerConfig
}

type ReconcilerConfig struct {
	NodeName            string
	LLVRequeueInterval  time.Duration
	LLVSRequeueInterval time.Duration
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
		metrics:  metrics,
		sdsCache: sdsCache,
		cfg:      cfg,
	}
}

func (r *Reconciler) Name() string {
	return ReconcilerName
}

func (r *Reconciler) MaxConcurrentReconciles() int {
	return 10
}

func (r *Reconciler) ShouldReconcileUpdate(_ *v1alpha1.LVMLogicalVolumeSnapshot, _ *v1alpha1.LVMLogicalVolumeSnapshot) bool {
	return false
}

func (r *Reconciler) Reconcile(ctx context.Context, req controller.ReconcileRequest[*v1alpha1.LVMLogicalVolumeSnapshot]) (controller.Result, error) {
	llvs := req.Object

	// check node
	if llvs.Spec.NodeName != r.cfg.NodeName {
		r.log.Info(fmt.Sprintf("the LVMLogicalVolumeSnapshot %s of does not belong to the current node: %s. Reconciliation stopped", req.Object.Spec.NodeName, r.cfg.NodeName))
		return controller.Result{}, nil
	}

	// get LLV
	origin := &v1alpha1.LVMLogicalVolume{}
	if req.Object.DeletionTimestamp != nil {
		// origin may already be deleted at the moment of llvs deletion
		origin = nil
	} else if err := r.cl.Get(ctx, client.ObjectKey{Name: llvs.Spec.LVMLogicalVolumeName}, origin); err != nil {
		if k8serr.IsNotFound(err) {
			r.log.Error(err, fmt.Sprintf("LVMLogicalVolume %s not found for LVMLogicalVolumeSnapshot %s. Retry in %s", llvs.Spec.LVMLogicalVolumeName, llvs.Name, r.cfg.LLVRequeueInterval.String()))
			err = r.updatePhaseAndSizeIfNeeded(
				ctx,
				llvs,
				internal.LLVSStatusPhaseFailed, fmt.Sprintf("LVMLogicalVolume %s not found", llvs.Spec.LVMLogicalVolumeName),
				nil,
				nil,
			)
			if err != nil {
				r.log.Error(err, fmt.Sprintf("unable to update the LVMLogicalVolumeSnapshot %s", llvs.Name))
				return controller.Result{}, err
			}

			return controller.Result{
				RequeueAfter: r.cfg.LLVRequeueInterval,
			}, nil
		}
	}

	// this case prevents the unexpected behavior when the controller runs up with existing LVMLogicalVolumeSnapshots
	if lvs, _ := r.sdsCache.GetLVs(); len(lvs) == 0 {
		r.log.Warning(fmt.Sprintf("unable to reconcile the request as no LV was found in the cache. Retry in %s", r.cfg.LLVRequeueInterval.String()))
		return controller.Result{RequeueAfter: r.cfg.LLVRequeueInterval}, nil
	}

	// add finalizer
	r.log.Debug(fmt.Sprintf("try to add the finalizer %s to the LVMLogicalVolumeSnapshot %s", internal.SdsNodeConfiguratorFinalizer, llvs.Name))
	added, err := r.addLLVSFinalizerIfNotExist(ctx, llvs)
	if err != nil {
		r.log.Error(err, fmt.Sprintf("unable to update the LVMLogicalVolumeSnapshot %s", llvs.Name))
		return controller.Result{}, err
	}
	if added {
		r.log.Debug(fmt.Sprintf("successfully added the finalizer %s to the LVMLogicalVolumeSnapshot %s", internal.SdsNodeConfiguratorFinalizer, llvs.Name))
	} else {
		r.log.Debug(fmt.Sprintf("no need to add the finalizer %s to the LVMLogicalVolumeSnapshot %s", internal.SdsNodeConfiguratorFinalizer, llvs.Name))
	}

	//
	shouldRequeue, err := r.reconcileLVMLogicalVolumeSnapshot(ctx, llvs, origin)
	if err != nil {
		r.log.Error(err, fmt.Sprintf("an error occurred while reconciling the LVMLogicalVolumeSnapshot: %s", llvs.Name))
		updErr := r.updatePhaseAndSizeIfNeeded(ctx, llvs, internal.LLVSStatusPhaseFailed, err.Error(), nil, nil)
		if updErr != nil {
			r.log.Error(updErr, fmt.Sprintf("unable to update the LVMLogicalVolumeSnapshot %s", llvs.Name))
			return controller.Result{}, updErr
		}
	}
	if shouldRequeue {
		r.log.Info(fmt.Sprintf("some issues were occurred while reconciliation the LVMLogicalVolumeSnapshot %s. Requeue the request in %s", llvs.Name, r.cfg.LLVSRequeueInterval.String()))
		return controller.Result{RequeueAfter: r.cfg.LLVRequeueInterval}, nil
	}

	r.log.Info(fmt.Sprintf("successfully ended reconciliation of the LVMLogicalVolumeSnapshot %s", origin.Name))
	return controller.Result{}, nil
}

func (r *Reconciler) reconcileLVMLogicalVolumeSnapshot(
	ctx context.Context,
	llvs *v1alpha1.LVMLogicalVolumeSnapshot,
	origin *v1alpha1.LVMLogicalVolume,
) (bool, error) {
	lvExists := r.sdsCache.FindLV(llvs.Spec.LVMVolumeGroupName, llvs.Name) != nil

	if !lvExists && llvs.DeletionTimestamp == nil {
		return r.reconcileLLVSCreateFunc(ctx, llvs, origin)
	} else if llvs.DeletionTimestamp != nil {
		return r.reconcileLLVSDeleteFunc(ctx, llvs)
	}

	if llvs.Status.Phase != internal.LLVSStatusPhaseCreated {
		// update actual size
		size, actualSize, err := r.getLVActualSize(llvs.Spec.LVMVolumeGroupName, llvs.Name)
		if err != nil {
			r.log.Error(err, "error parsing LV size")
			return true, nil
		}
		if size == nil {
			r.log.Warning(fmt.Sprintf("[reconcileLVMLogicalVolumeSnapshot] unable to get actual size for LV %s in VG %s (likely LV was not found in the cache), retry...", llvs.Name, llvs.Spec.LVMVolumeGroupName))
			return true, nil
		}

		// finalize operation
		err = r.updatePhaseAndSizeIfNeeded(ctx, llvs, internal.LLVSStatusPhaseCreated, "", size, actualSize)
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[reconcileLVMLogicalVolumeSnapshot] unable update phase and actualSize for LV %s, retry...", llvs.Name))
			return true, err
		}
		r.log.Info(fmt.Sprintf("successfully ended the reconciliation for the LVMLogicalVolumeSnapshot %s", llvs.Name))
		return false, nil
	}

	r.log.Info(fmt.Sprintf("the LVMLogicalVolumeSnapshot %s has compeleted configuration and should not be reconciled", llvs.Name))
	return false, nil
}

func (r *Reconciler) reconcileLLVSCreateFunc(
	ctx context.Context,
	llvs *v1alpha1.LVMLogicalVolumeSnapshot,
	origin *v1alpha1.LVMLogicalVolume,
) (bool, error) {
	if llvs.Status == nil {
		err := r.updatePhaseAndSizeIfNeeded(ctx, llvs, internal.LLVSStatusPhasePending, "", nil, nil)
		if err != nil {
			r.log.Error(err, fmt.Sprintf("unable to update the LVMLogicalVolumeSnapshot %s", llvs.Name))
			return true, err
		}
	}

	cmd, err := utils.CreateThinLogicalVolumeSnapshot(llvs.Name, origin.Spec.LVMVolumeGroupName, origin.Spec.ActualLVNameOnTheNode)
	r.log.Debug(fmt.Sprintf("[reconcileLLVSCreateFunc] ran cmd: %s", cmd))
	if err != nil {
		r.log.Error(err, fmt.Sprintf("[reconcileLLVSCreateFunc] unable to create a LVMLogicalVolumeSnapshot %s for the LVMLogicalVolume %s", llvs.Name, origin.Spec.ActualLVNameOnTheNode))
		return true, err
	}
	r.log.Info(fmt.Sprintf("[reconcileLLVSCreateFunc] successfully created LV %s in VG %s for LVMLogicalVolumeSnapshot resource with name: %s", llvs.Name, origin.Spec.LVMVolumeGroupName, origin.Spec.ActualLVNameOnTheNode))

	r.log.Debug(fmt.Sprintf("[reconcileLLVSCreateFunc] adds the LV %s to the cache", llvs.Name))
	r.sdsCache.AddLV(origin.Spec.LVMVolumeGroupName, llvs.Name)

	// we'll have to update actual size when scanner ends it's job, so re-schedule
	return true, nil
}

func (r *Reconciler) reconcileLLVSDeleteFunc(
	ctx context.Context,
	llvs *v1alpha1.LVMLogicalVolumeSnapshot,
) (bool, error) {
	// The controller won't remove the LLV resource and LV volume till the resource has any other finalizer.
	if len(llvs.Finalizers) != 0 {
		if len(llvs.Finalizers) > 1 ||
			llvs.Finalizers[0] != internal.SdsNodeConfiguratorFinalizer {
			r.log.Debug(fmt.Sprintf("[reconcileLLVDeleteFunc] unable to delete LVMLogicalVolume %s for now due to it has any other finalizer", llvs.Name))
			return false, nil
		}
	}

	err := r.deleteLVIfNeeded(llvs)
	if err != nil {
		r.log.Error(err, fmt.Sprintf("[reconcileLLVDeleteFunc] unable to delete the LV %s in VG %s", llvs.Name, llvs.Spec.LVMVolumeGroupName))
		return true, err
	}

	r.log.Info(fmt.Sprintf("[reconcileLLVDeleteFunc] successfully deleted the LV %s in VG %s", llvs.Name, llvs.Spec.LVMVolumeGroupName))

	err = r.removeLLVSFinalizersIfExist(ctx, llvs)
	if err != nil {
		r.log.Error(err, fmt.Sprintf("[reconcileLLVDeleteFunc] unable to remove finalizers from the LVMVolumeGroup %s", llvs.Name))
		return true, err
	}

	r.log.Info(fmt.Sprintf("[reconcileLLVDeleteFunc] successfully ended reconciliation for the LVMLogicalVolume %s", llvs.Name))
	return false, nil
}

func (r *Reconciler) getLVActualSize(vgName, lvName string) (size *resource.Quantity, actualSize *resource.Quantity, err error) {
	lv := r.sdsCache.FindLV(vgName, lvName)
	if lv == nil {
		return
	}

	size = resource.NewQuantity(lv.Data.LVSize.Value(), resource.BinarySI)
	actualSize, err = lv.Data.GetUsedSize()
	return
}

func (r *Reconciler) updatePhaseAndSizeIfNeeded(
	ctx context.Context,
	llvs *v1alpha1.LVMLogicalVolumeSnapshot,
	phase string,
	reason string,
	size *resource.Quantity,
	actualSize *resource.Quantity,
) error {
	if llvs.Status != nil &&
		llvs.Status.Phase == phase &&
		llvs.Status.Reason == reason {
		r.log.Debug(fmt.Sprintf("[updatePhaseIfNeeded] no need to update the LVMLogicalVolume %s phase and reason", llvs.Name))
		return nil
	}

	if llvs.Status == nil {
		llvs.Status = new(v1alpha1.LVMLogicalVolumeSnapshotStatus)
	}

	llvs.Status.Phase = phase
	llvs.Status.Reason = reason

	if size != nil {
		llvs.Status.Size = *size
	}
	if actualSize != nil {
		llvs.Status.ActualSize = *actualSize
	}

	r.log.Debug(fmt.Sprintf("[updatePhaseIfNeeded] tries to update the LVMLogicalVolumeSnapshot %s status with phase: %s, reason: %s", llvs.Name, phase, reason))
	err := r.cl.Status().Update(ctx, llvs)
	if err != nil {
		return err
	}

	r.log.Debug(fmt.Sprintf("[updatePhaseIfNeeded] updated LVMLogicalVolumeSnapshot %s status.phase to %s and reason to %s", llvs.Name, phase, reason))
	return nil
}

func (r *Reconciler) addLLVSFinalizerIfNotExist(ctx context.Context, llvs *v1alpha1.LVMLogicalVolumeSnapshot) (bool, error) {
	if slices.Contains(llvs.Finalizers, internal.SdsNodeConfiguratorFinalizer) {
		return false, nil
	}

	llvs.Finalizers = append(llvs.Finalizers, internal.SdsNodeConfiguratorFinalizer)

	r.log.Trace(fmt.Sprintf("[addLLVSFinalizerIfNotExist] added finalizer %s to the LVMLogicalVolumeSnapshot %s", internal.SdsNodeConfiguratorFinalizer, llvs.Name))
	err := r.cl.Update(ctx, llvs)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (r *Reconciler) removeLLVSFinalizersIfExist(
	ctx context.Context,
	llvs *v1alpha1.LVMLogicalVolumeSnapshot,
) error {
	var removed bool
	for i, f := range llvs.Finalizers {
		if f == internal.SdsNodeConfiguratorFinalizer {
			llvs.Finalizers = append(llvs.Finalizers[:i], llvs.Finalizers[i+1:]...)
			removed = true
			r.log.Debug(fmt.Sprintf("[removeLLVSFinalizersIfExist] removed finalizer %s from the LVMLogicalVolumeSnapshot %s", internal.SdsNodeConfiguratorFinalizer, llvs.Name))
			break
		}
	}

	if removed {
		r.log.Trace(fmt.Sprintf("[removeLLVSFinalizersIfExist] removed finalizer %s from the LVMLogicalVolumeSnapshot %s", internal.SdsNodeConfiguratorFinalizer, llvs.Name))
		err := r.cl.Update(ctx, llvs)
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[updateLVMLogicalVolumeSpec] unable to update the LVMVolumeGroup %s", llvs.Name))
			return err
		}
	}

	return nil
}

func (r *Reconciler) deleteLVIfNeeded(llvs *v1alpha1.LVMLogicalVolumeSnapshot) error {
	lv := r.sdsCache.FindLV(llvs.Spec.LVMVolumeGroupName, llvs.Name)
	if lv == nil || !lv.Exist {
		r.log.Warning(fmt.Sprintf("[deleteLVIfNeeded] did not find LV %s in VG %s", llvs.Name, llvs.Spec.LVMVolumeGroupName))
		return nil
	}

	cmd, err := utils.RemoveLV(llvs.Spec.LVMVolumeGroupName, llvs.Name)
	r.log.Debug(fmt.Sprintf("[deleteLVIfNeeded] runs cmd: %s", cmd))
	if err != nil {
		r.log.Error(err, fmt.Sprintf("[deleteLVIfNeeded] unable to remove LV %s from VG %s", llvs.Name, llvs.Spec.LVMVolumeGroupName))
		return err
	}

	r.log.Debug(fmt.Sprintf("[deleteLVIfNeeded] mark LV %s in the cache as removed", lv.Data.LVName))
	r.sdsCache.MarkLVAsRemoved(lv.Data.VGName, lv.Data.LVName)

	return nil
}
