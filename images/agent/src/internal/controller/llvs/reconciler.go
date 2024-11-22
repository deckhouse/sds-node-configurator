package llvs

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"time"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
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

func (r *Reconciler) ShouldReconcileUpdate(_ *v1alpha1.LVMLogicalVolumeSnapshot, newObj *v1alpha1.LVMLogicalVolumeSnapshot) bool {
	// to proceed with deletion when finalizers were updated
	return newObj.DeletionTimestamp != nil && newObj.Status != nil && newObj.Status.NodeName == r.cfg.NodeName
}

func (r *Reconciler) ShouldReconcileCreate(_ *v1alpha1.LVMLogicalVolumeSnapshot) bool {
	return true
}

func (r *Reconciler) Reconcile(ctx context.Context, req controller.ReconcileRequest[*v1alpha1.LVMLogicalVolumeSnapshot]) (controller.Result, error) {
	llvs := req.Object

	llv := &v1alpha1.LVMLogicalVolume{}
	if err := r.cl.Get(ctx, types.NamespacedName{Name: llvs.Spec.LVMLogicalVolumeName}, llv); err != nil {
		r.log.Warning(fmt.Sprintf("failed to get LLV %s. Retry in %s", llvs.Spec.LVMLogicalVolumeName, r.cfg.LLVRequeueInterval.String()))
		return controller.Result{RequeueAfter: r.cfg.LLVRequeueInterval}, nil
	}

	lvg := &v1alpha1.LVMVolumeGroup{}
	if err := r.cl.Get(ctx, types.NamespacedName{Name: llv.Spec.LVMVolumeGroupName}, lvg); err != nil {
		r.log.Warning(fmt.Sprintf("failed to get LVG %s. Retry in %s", llv.Spec.LVMVolumeGroupName, r.cfg.VolumeGroupScanInterval.String()))
		return controller.Result{RequeueAfter: r.cfg.VolumeGroupScanInterval}, nil
	}

	// check node
	if lvg.Spec.Local.NodeName != r.cfg.NodeName {
		r.log.Info(fmt.Sprintf("the LVMLogicalVolumeSnapshot %s of does not belong to the current node: %s. Reconciliation stopped", llvs.Name, r.cfg.NodeName))
		return controller.Result{}, nil
	}

	// this case prevents the unexpected behavior when the controller runs up with existing LVMLogicalVolumeSnapshots
	if lvs, _ := r.sdsCache.GetLVs(); len(lvs) == 0 {
		r.log.Warning(fmt.Sprintf("unable to reconcile the request as no LV was found in the cache. Retry in %s", r.cfg.LLVRequeueInterval.String()))
		return controller.Result{RequeueAfter: r.cfg.LLVRequeueInterval}, nil
	}

	// reconcile
	shouldRequeue, err := r.reconcileLVMLogicalVolumeSnapshot(ctx, lvg, llv, llvs)
	if err != nil {
		r.log.Error(err, fmt.Sprintf("an error occurred while reconciling the LVMLogicalVolumeSnapshot: %s", llvs.Name))
		shouldRequeue = true
	}
	if shouldRequeue {
		r.log.Info(fmt.Sprintf("reconciliation of LVMLogicalVolumeSnapshot %s is not finished. Requeue the request in %s", llvs.Name, r.cfg.LLVSRequeueInterval.String()))
		return controller.Result{RequeueAfter: r.cfg.LLVSRequeueInterval}, nil
	}

	r.log.Info(fmt.Sprintf("successfully ended reconciliation of the LVMLogicalVolumeSnapshot %s", llvs.Name))
	return controller.Result{}, nil
}

func (r *Reconciler) reconcileLVMLogicalVolumeSnapshot(
	ctx context.Context,
	lvg *v1alpha1.LVMVolumeGroup,
	llv *v1alpha1.LVMLogicalVolume,
	llvs *v1alpha1.LVMLogicalVolumeSnapshot,
) (bool, error) {
	switch {
	case llvs.DeletionTimestamp != nil:
		// delete
		return r.reconcileLLVSDeleteFunc(ctx, llvs, lvg)
	case llvs.Status == nil || llvs.Status.Phase == internal.LLVSStatusPhasePending:
		return r.reconcileLLVSCreateFunc(ctx, lvg, llv, llvs)
	case llvs.Status.Phase == internal.LLVSStatusPhaseCreated:
		r.log.Info(fmt.Sprintf("the LVMLogicalVolumeSnapshot %s is already Created and should not be reconciled", llvs.Name))
	default:
		r.log.Warning(fmt.Sprintf("skipping LLVS reconciliation, since it is in phase: %s", llvs.Status.Phase))
	}

	return false, nil
}

func (r *Reconciler) reconcileLLVSCreateFunc(
	ctx context.Context,
	lvg *v1alpha1.LVMVolumeGroup,
	llv *v1alpha1.LVMLogicalVolume,
	llvs *v1alpha1.LVMLogicalVolumeSnapshot,
) (bool, error) {
	if llvs.Status == nil {
		if !slices.Contains(llvs.Finalizers, internal.SdsNodeConfiguratorFinalizer) {
			llvs.Finalizers = append(llvs.Finalizers, internal.SdsNodeConfiguratorFinalizer)
			r.log.Debug(fmt.Sprintf("[reconcileLLVSCreateFunc] adding finalizer to LLVS %s", llvs.Name))
			if err := r.cl.Update(ctx, llvs); err != nil {
				return true, err
			}
		}

		// will be saved later
		llvs.Status = &v1alpha1.LVMLogicalVolumeSnapshotStatus{
			NodeName:              lvg.Spec.Local.NodeName,
			ActualVGNameOnTheNode: lvg.Spec.ActualVGNameOnTheNode,
			ActualLVNameOnTheNode: llv.Spec.ActualLVNameOnTheNode,
			Phase:                 internal.LLVSStatusPhasePending,
		}
	}

	snapshotLVData := r.sdsCache.FindLV(llvs.Status.ActualVGNameOnTheNode, llvs.ActualSnapshotNameOnTheNode())

	switch {
	case snapshotLVData == nil || !snapshotLVData.Exist:
		// create
		cmd, err := utils.CreateThinLogicalVolumeSnapshot(
			llvs.ActualSnapshotNameOnTheNode(),
			llvs.Status.ActualVGNameOnTheNode,
			llvs.Status.ActualLVNameOnTheNode,
			utils.NewEnabledTags(internal.LLVSNameTag, llvs.Name),
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
			llvs.Status.Reason = "Repeating volume creation"
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
		llvs.Status.Phase = internal.LLVSStatusPhaseCreated
		llvs.Status.Reason = ""
		err = r.cl.Status().Update(ctx, llvs)
		return false, err
	}
}

func (r *Reconciler) reconcileLLVSDeleteFunc(
	ctx context.Context,
	llvs *v1alpha1.LVMLogicalVolumeSnapshot,
	lvg *v1alpha1.LVMVolumeGroup,
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

	err := r.deleteLVIfNeeded(llvs.Name, llvs.ActualSnapshotNameOnTheNode(), lvg.Spec.ActualVGNameOnTheNode)
	if err != nil {
		r.log.Error(err, fmt.Sprintf("[reconcileLLVSDeleteFunc] unable to delete the LV %s in VG %s", llvs.ActualSnapshotNameOnTheNode(), lvg.Spec.ActualVGNameOnTheNode))
		return true, err
	}

	r.log.Info(fmt.Sprintf("[reconcileLLVSDeleteFunc] successfully deleted the LV %s in VG %s", llvs.ActualSnapshotNameOnTheNode(), lvg.Spec.ActualVGNameOnTheNode))

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

	if ok, name := utils.ReadValueFromTags(lv.Data.LvTags, internal.LLVSNameTag); !ok {
		r.log.Warning(fmt.Sprintf("[deleteLVIfNeeded] did not find required tags on LV %s in VG %s", llvsActualNameOnTheNode, vgActualNameOnTheNode))
		return nil
	} else if name != llvsName {
		r.log.Warning(fmt.Sprintf("[deleteLVIfNeeded] name in tag doesn't match %s on LV %s in VG %s", llvsName, llvsActualNameOnTheNode, vgActualNameOnTheNode))
		return nil
	}

	cmd, err := utils.RemoveLV(vgActualNameOnTheNode, llvsActualNameOnTheNode)
	r.log.Debug(fmt.Sprintf("[deleteLVIfNeeded] runs cmd: %s", cmd))
	if err != nil {
		r.log.Error(err, fmt.Sprintf("[deleteLVIfNeeded] unable to remove LV %s from VG %s", llvsActualNameOnTheNode, vgActualNameOnTheNode))
		return err
	}

	r.log.Debug(fmt.Sprintf("[deleteLVIfNeeded] mark LV %s in the cache as removed", lv.Data.LVName))
	r.sdsCache.MarkLVAsRemoved(lv.Data.VGName, lv.Data.LVName)

	return nil
}
