//go:build !ce

package llvs

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"time"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/cache"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/controller"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/logger"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/monitoring"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/utils"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const ReconcilerName = "lvm-logical-volume-snapshot-watcher-controller"

type Reconciler struct {
	cl       client.Client
	log      logger.Logger
	lvgCl    *utils.LVGClient
	metrics  monitoring.Metrics
	sdsCache *cache.Cache
	cfg      ReconcilerConfig
	commands utils.Commands
}

type ReconcilerConfig struct {
	NodeName                string
	VolumeGroupScanInterval time.Duration
	LLVRequeueInterval      time.Duration
	LLVSRequeueInterval     time.Duration
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
		commands: commands,
	}
}

func (r *Reconciler) Name() string {
	return ReconcilerName
}

func (r *Reconciler) MaxConcurrentReconciles() int {
	return 10
}

func (r *Reconciler) ShouldReconcileUpdate(_ *v1alpha1.LVMLogicalVolumeSnapshot, newObj *v1alpha1.LVMLogicalVolumeSnapshot) bool {
	return newObj.DeletionTimestamp != nil &&
		newObj.Status != nil &&
		newObj.Status.NodeName == r.cfg.NodeName &&
		len(newObj.Finalizers) == 1 &&
		newObj.Finalizers[0] == internal.SdsNodeConfiguratorFinalizer
}

func (r *Reconciler) ShouldReconcileCreate(_ *v1alpha1.LVMLogicalVolumeSnapshot) bool {
	return true
}

func (r *Reconciler) Reconcile(ctx context.Context, req controller.ReconcileRequest[*v1alpha1.LVMLogicalVolumeSnapshot]) (controller.Result, error) {
	llvs := req.Object

	// this case prevents the unexpected behavior when the controller runs up with existing LVMLogicalVolumeSnapshots
	if lvs, _ := r.sdsCache.GetLVs(); len(lvs) == 0 {
		r.log.Warning(fmt.Sprintf("unable to reconcile the request as no LV was found in the cache. Retry in %s", r.cfg.LLVRequeueInterval.String()))
		return controller.Result{RequeueAfter: r.cfg.LLVRequeueInterval}, nil
	}

	// reconcile
	shouldRequeue, err := r.reconcileLVMLogicalVolumeSnapshot(ctx, llvs)
	if err != nil {
		r.log.Error(err, fmt.Sprintf("an error occurred while reconciling the LVMLogicalVolumeSnapshot: %s", llvs.Name))
		// will lead to exponential backoff
		return controller.Result{}, err
	}
	if shouldRequeue {
		r.log.Info(fmt.Sprintf("reconciliation of LVMLogicalVolumeSnapshot %s is not finished. Requeue the request in %s", llvs.Name, r.cfg.LLVSRequeueInterval.String()))
		// will lead to retry after fixed time
		return controller.Result{RequeueAfter: r.cfg.LLVSRequeueInterval}, nil
	}

	r.log.Info(fmt.Sprintf("successfully ended reconciliation of the LVMLogicalVolumeSnapshot %s", llvs.Name))
	return controller.Result{}, nil
}

func (r *Reconciler) reconcileLVMLogicalVolumeSnapshot(
	ctx context.Context,
	llvs *v1alpha1.LVMLogicalVolumeSnapshot,
) (bool, error) {
	switch {
	case llvs.DeletionTimestamp != nil:
		// delete
		return r.reconcileLLVSDeleteFunc(ctx, llvs)
	case llvs.Status == nil || llvs.Status.Phase == v1alpha1.PhasePending:
		return r.reconcileLLVSCreateFunc(ctx, llvs)
	case llvs.Status.Phase == v1alpha1.PhaseCreated:
		r.log.Info(fmt.Sprintf("the LVMLogicalVolumeSnapshot %s is already Created and should not be reconciled", llvs.Name))
	default:
		r.log.Warning(fmt.Sprintf("skipping LLVS reconciliation, since it is in phase: %s", llvs.Status.Phase))
	}

	return false, nil
}

func (r *Reconciler) reconcileLLVSCreateFunc(
	ctx context.Context,
	llvs *v1alpha1.LVMLogicalVolumeSnapshot,
) (bool, error) {
	// should precede setting finalizer to be able to determine the node when deleting
	if llvs.Status == nil {
		llv := &v1alpha1.LVMLogicalVolume{}
		if err := r.getObjectOrSetPendingStatus(
			ctx,
			llvs,
			types.NamespacedName{Name: llvs.Spec.LVMLogicalVolumeName},
			llv,
		); err != nil {
			return true, err
		}

		if llv.Spec.Thin == nil {
			r.log.Error(nil, fmt.Sprintf("Failed reconciling LLVS %s, LLV %s is not Thin", llvs.Name, llv.Name))
			llvs.Status = &v1alpha1.LVMLogicalVolumeSnapshotStatus{
				Phase:  v1alpha1.PhaseFailed,
				Reason: fmt.Sprintf("Source LLV %s is not Thin", llv.Name),
			}
			return false, r.cl.Status().Update(ctx, llvs)
		}

		lvg := &v1alpha1.LVMVolumeGroup{}
		if err := r.getObjectOrSetPendingStatus(
			ctx,
			llvs,
			types.NamespacedName{Name: llv.Spec.LVMVolumeGroupName},
			lvg,
		); err != nil {
			return true, err
		}

		if lvg.Spec.Local.NodeName != r.cfg.NodeName {
			r.log.Info(fmt.Sprintf("LLVS %s is from node %s. Current node %s", llvs.Name, lvg.Spec.Local.NodeName, r.cfg.NodeName))
			return false, nil
		}

		thinPoolIndex := slices.IndexFunc(lvg.Status.ThinPools, func(tps v1alpha1.LVMVolumeGroupThinPoolStatus) bool {
			return tps.Name == llv.Spec.Thin.PoolName
		})
		if thinPoolIndex < 0 {
			r.log.Error(nil, fmt.Sprintf("LLVS %s thin pool %s is not found in LVG %s", llvs.Name, llv.Spec.Thin.PoolName, lvg.Name))
			llvs.Status = &v1alpha1.LVMLogicalVolumeSnapshotStatus{
				Phase:  v1alpha1.PhasePending,
				Reason: fmt.Sprintf("Thin pool %s is not found in LVG %s", llv.Spec.Thin.PoolName, lvg.Name),
			}
			return true, r.cl.Status().Update(ctx, llvs)
		}

		if llv.Status == nil || llv.Status.ActualSize.Value() == 0 {
			r.log.Error(nil, fmt.Sprintf("Error reconciling LLVS %s, source LLV %s ActualSize is not known", llvs.Name, llv.Name))
			llvs.Status = &v1alpha1.LVMLogicalVolumeSnapshotStatus{
				Phase:  v1alpha1.PhasePending,
				Reason: fmt.Sprintf("Source LLV %s ActualSize is not known", llv.Name),
			}
			return true, r.cl.Status().Update(ctx, llvs)
		}

		if lvg.Status.ThinPools[thinPoolIndex].AvailableSpace.Value() < llv.Status.ActualSize.Value() {
			r.log.Error(nil, fmt.Sprintf(
				"LLVS %s: not enough space available in thin pool %s: need at least %s, got %s",
				llvs.Name,
				llv.Spec.Thin.PoolName,
				llv.Status.ActualSize.String(),
				lvg.Status.ThinPools[thinPoolIndex].AvailableSpace.String(),
			))
			llvs.Status = &v1alpha1.LVMLogicalVolumeSnapshotStatus{
				Phase: v1alpha1.PhasePending,
				Reason: fmt.Sprintf(
					"Not enough space available in thin pool %s: need at least %s, got %s",
					llv.Spec.Thin.PoolName,
					llv.Status.ActualSize.String(),
					lvg.Status.ThinPools[thinPoolIndex].AvailableSpace.String(),
				),
			}
			return true, r.cl.Status().Update(ctx, llvs)
		}

		llvs.Status = &v1alpha1.LVMLogicalVolumeSnapshotStatus{
			NodeName:              lvg.Spec.Local.NodeName,
			ActualVGNameOnTheNode: lvg.Spec.ActualVGNameOnTheNode,
			ActualLVNameOnTheNode: llv.Spec.ActualLVNameOnTheNode,
			Phase:                 v1alpha1.PhasePending,
			Reason:                "Creating volume",
		}

		if err := r.cl.Status().Update(ctx, llvs); err != nil {
			r.log.Error(err, "Failed updating status of "+llvs.Name)
			return true, err
		}
	}

	// check node
	if llvs.Status.NodeName != r.cfg.NodeName {
		r.log.Info(fmt.Sprintf("LLVS %s has a Status with different node %s", llvs.Name, llvs.Status.NodeName))
		return false, nil
	}

	// this block should precede any side-effects, which should be reverted during delete
	if !slices.Contains(llvs.Finalizers, internal.SdsNodeConfiguratorFinalizer) {
		llvs.Finalizers = append(llvs.Finalizers, internal.SdsNodeConfiguratorFinalizer)
		r.log.Info("adding finalizer to LLVS " + llvs.Name)
		if err := r.cl.Update(ctx, llvs); err != nil {
			r.log.Error(err, "Failed adding finalizer to LLVS "+llvs.Name)
			return true, err
		}
	}

	snapshotLVData := r.sdsCache.FindLV(llvs.Status.ActualVGNameOnTheNode, llvs.ActualSnapshotNameOnTheNode())

	switch {
	case snapshotLVData == nil || !snapshotLVData.Exist:
		// create
		cmd, err := r.commands.CreateThinLogicalVolumeSnapshot(
			llvs.ActualSnapshotNameOnTheNode(),
			llvs.Status.ActualVGNameOnTheNode,
			llvs.Status.ActualLVNameOnTheNode,
			utils.NewEnabledTags(v1alpha1.LLVSNameTag, llvs.Name),
		)
		r.log.Debug(fmt.Sprintf("[reconcileLLVSCreateFunc] ran cmd: %s", cmd))
		if err != nil {
			r.log.Error(
				err,
				fmt.Sprintf(
					"[reconcileLLVSCreateFunc] unable to create a LVMLogicalVolumeSnapshot %s from %s/%s",
					llvs.ActualSnapshotNameOnTheNode(),
					llvs.Status.ActualVGNameOnTheNode,
					llvs.Status.ActualLVNameOnTheNode,
				))
			llvs.Status.Reason = fmt.Sprintf("Error during snapshot creation (will be retried): %v", err)
			updateErr := r.cl.Status().Update(ctx, llvs)
			err = errors.Join(err, updateErr)
			return true, err
		}
		r.log.Info(
			fmt.Sprintf(
				"[reconcileLLVSCreateFunc] successfully created LV %s in VG %s for LVMLogicalVolumeSnapshot resource with name: %s",
				llvs.ActualSnapshotNameOnTheNode(),
				llvs.Status.ActualVGNameOnTheNode,
				llvs.Name,
			),
		)
		r.sdsCache.AddLV(llvs.Status.ActualVGNameOnTheNode, llvs.ActualSnapshotNameOnTheNode())

		llvs.Status.Reason = "Waiting for created volume to become discovered"
		err = r.cl.Status().Update(ctx, llvs)
		return true, err
	case reflect.ValueOf(snapshotLVData.Data).IsZero():
		// still "Waiting for created volume to become discovered"
		r.log.Info("[reconcileLLVSCreateFunc] waiting for created volume to become discovered")
		return true, nil
	default:
		r.log.Info("[reconcileLLVSCreateFunc] updating LLVS size")

		// update size & phase
		size := resource.NewQuantity(snapshotLVData.Data.LVSize.Value(), resource.BinarySI)
		usedSize, err := snapshotLVData.Data.GetUsedSize()
		if err != nil {
			r.log.Error(err, "error parsing LV size")
			return true, err
		}

		llvs.Status.Size = *size
		llvs.Status.UsedSize = *usedSize
		llvs.Status.Phase = v1alpha1.PhaseCreated
		llvs.Status.Reason = ""
		err = r.cl.Status().Update(ctx, llvs)
		return false, err
	}
}

func (r *Reconciler) reconcileLLVSDeleteFunc(
	ctx context.Context,
	llvs *v1alpha1.LVMLogicalVolumeSnapshot,
) (bool, error) {
	if len(llvs.Finalizers) == 0 {
		// means that we've deleted everything already (see below)
		return false, nil
	}

	if len(llvs.Finalizers) > 1 || llvs.Finalizers[0] != internal.SdsNodeConfiguratorFinalizer {
		// postpone deletion until another finalizer gets removed
		r.log.Warning(fmt.Sprintf("[reconcileLLVSDeleteFunc] unable to delete LVMLogicalVolumeSnapshot %s for now due to it has any other finalizer", llvs.Name))
		return false, nil
	}

	err := r.deleteLVIfNeeded(llvs.Name, llvs.ActualSnapshotNameOnTheNode(), llvs.Status.ActualVGNameOnTheNode)
	if err != nil {
		r.log.Error(err, fmt.Sprintf("[reconcileLLVSDeleteFunc] unable to delete the LV %s in VG %s", llvs.ActualSnapshotNameOnTheNode(), llvs.Status.ActualVGNameOnTheNode))
		return true, err
	}

	r.log.Info(fmt.Sprintf("[reconcileLLVSDeleteFunc] successfully deleted the LV %s in VG %s", llvs.ActualSnapshotNameOnTheNode(), llvs.Status.ActualVGNameOnTheNode))

	// at this point we have exactly 1 finalizer
	llvs.Finalizers = nil
	if err := r.cl.Update(ctx, llvs); err != nil {
		r.log.Error(err, fmt.Sprintf("[reconcileLLVSDeleteFunc] unable to remove finalizers from the LVMLogicalVolumeSnapshot %s", llvs.Name))
		return true, err
	}

	r.log.Info(fmt.Sprintf("[reconcileLLVSDeleteFunc] successfully ended deletion of LVMLogicalVolumeSnapshot %s", llvs.Name))
	return false, nil
}

func (r *Reconciler) deleteLVIfNeeded(llvsName, llvsActualNameOnTheNode, vgActualNameOnTheNode string) error {
	lv := r.sdsCache.FindLV(vgActualNameOnTheNode, llvsActualNameOnTheNode)
	if lv == nil || !lv.Exist {
		r.log.Warning(fmt.Sprintf("[deleteLVIfNeeded] did not find LV %s in VG %s", llvsActualNameOnTheNode, vgActualNameOnTheNode))
		return nil
	}

	if ok, name := utils.ReadValueFromTags(lv.Data.LvTags, v1alpha1.LLVSNameTag); !ok {
		r.log.Warning(fmt.Sprintf("[deleteLVIfNeeded] did not find required tags on LV %s in VG %s", llvsActualNameOnTheNode, vgActualNameOnTheNode))
		return nil
	} else if name != llvsName {
		r.log.Warning(fmt.Sprintf("[deleteLVIfNeeded] name in tag doesn't match %s on LV %s in VG %s", llvsName, llvsActualNameOnTheNode, vgActualNameOnTheNode))
		return nil
	}

	cmd, err := r.commands.RemoveLV(vgActualNameOnTheNode, llvsActualNameOnTheNode)
	r.log.Debug(fmt.Sprintf("[deleteLVIfNeeded] runs cmd: %s", cmd))
	if err != nil {
		r.log.Error(err, fmt.Sprintf("[deleteLVIfNeeded] unable to remove LV %s from VG %s", llvsActualNameOnTheNode, vgActualNameOnTheNode))
		return err
	}

	r.log.Debug(fmt.Sprintf("[deleteLVIfNeeded] mark LV %s in the cache as removed", lv.Data.LVName))
	r.sdsCache.MarkLVAsRemoved(lv.Data.VGName, lv.Data.LVName)

	return nil
}

func (r *Reconciler) getObjectOrSetPendingStatus(
	ctx context.Context,
	llvs *v1alpha1.LVMLogicalVolumeSnapshot,
	key types.NamespacedName,
	obj client.Object,
) error {
	if err := r.cl.Get(ctx, key, obj); err != nil {
		llvs.Status = &v1alpha1.LVMLogicalVolumeSnapshotStatus{
			Phase:  v1alpha1.PhasePending,
			Reason: fmt.Sprintf("Error while getting object %s: %v", obj.GetName(), err),
		}
		updErr := r.cl.Status().Update(ctx, llvs)
		return errors.Join(err, updErr)
	}
	return nil
}
