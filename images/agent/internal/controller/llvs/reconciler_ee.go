//go:build !ce

/*
Copyright 2025 Flant JSC
Licensed under the Deckhouse Platform Enterprise Edition (EE) license. See https://github.com/deckhouse/deckhouse/blob/main/ee/LICENSE
*/

package llvs

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
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

const ReconcilerName = "lvm-logical-volume-snapshot-watcher-controller"

type Reconciler struct {
	cl       client.Client
	log      logger.Logger
	lvgCl    *repository.LVGClient
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
		lvgCl: repository.NewLVGClient(
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
	log := r.log.WithName("Reconcile")
	if llvs != nil {
		log = log.WithValues("llvsName", llvs.Name)
	}

	// this case prevents the unexpected behavior when the controller runs up with existing LVMLogicalVolumeSnapshots
	if lvs, _ := r.sdsCache.GetLVs(); len(lvs) == 0 {
		log.Warning("unable to reconcile the request as no LV was found in the cache",
			"retryIn", r.cfg.LLVRequeueInterval)
		return controller.Result{RequeueAfter: r.cfg.LLVRequeueInterval}, nil
	}

	// reconcile
	shouldRequeue, err := r.reconcileLVMLogicalVolumeSnapshot(ctx, llvs)
	if err != nil {
		log.Error(err, "an error occurred while reconciling the LVMLogicalVolumeSnapshot")
		// will lead to exponential backoff
		return controller.Result{}, err
	}
	if shouldRequeue {
		log.Info("reconciliation of LVMLogicalVolumeSnapshot is not finished",
			"requeueIn", r.cfg.LLVSRequeueInterval)
		// will lead to retry after fixed time
		return controller.Result{RequeueAfter: r.cfg.LLVSRequeueInterval}, nil
	}

	log.Info("successfully ended reconciliation of the LVMLogicalVolumeSnapshot")
	return controller.Result{}, nil
}

func (r *Reconciler) reconcileLVMLogicalVolumeSnapshot(
	ctx context.Context,
	llvs *v1alpha1.LVMLogicalVolumeSnapshot,
) (bool, error) {
	log := r.log.WithName("reconcileLVMLogicalVolumeSnapshot").WithValues("llvsName", llvs.Name)
	switch {
	case llvs.DeletionTimestamp != nil:
		// delete
		return r.reconcileLLVSDeleteFunc(ctx, llvs)
	case llvs.Status == nil || llvs.Status.Phase == v1alpha1.PhasePending:
		return r.reconcileLLVSCreateFunc(ctx, llvs)
	case llvs.Status.Phase == v1alpha1.PhaseCreated:
		log.Info("the LVMLogicalVolumeSnapshot is already Created and should not be reconciled")
	default:
		log.Warning("skipping LLVS reconciliation, since it is in phase", "phase", llvs.Status.Phase)
	}

	return false, nil
}

func (r *Reconciler) reconcileLLVSCreateFunc(
	ctx context.Context,
	llvs *v1alpha1.LVMLogicalVolumeSnapshot,
) (bool, error) {
	log := r.log.WithName("reconcileLLVSCreateFunc").WithValues("llvsName", llvs.Name)
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
			log.Error(nil, "Failed reconciling LLVS, LLV is not Thin", "llvName", llv.Name)
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
			log.Info("LLVS is from node. Current node", "nodeName", lvg.Spec.Local.NodeName, "currentNode", r.cfg.NodeName)
			return false, nil
		}

		thinPoolIndex := slices.IndexFunc(lvg.Status.ThinPools, func(tps v1alpha1.LVMVolumeGroupThinPoolStatus) bool {
			return tps.Name == llv.Spec.Thin.PoolName
		})
		if thinPoolIndex < 0 {
			log.Error(nil, "LLVS thin pool is not found in LVG", "thinPoolName", llv.Spec.Thin.PoolName, "lvgName", lvg.Name)
			llvs.Status = &v1alpha1.LVMLogicalVolumeSnapshotStatus{
				Phase:  v1alpha1.PhasePending,
				Reason: fmt.Sprintf("Thin pool %s is not found in LVG %s", llv.Spec.Thin.PoolName, lvg.Name),
			}
			return true, r.cl.Status().Update(ctx, llvs)
		}

		if llv.Status == nil || llv.Status.ActualSize.Value() == 0 {
			log.Error(nil, "Error reconciling LLVS, source LLV ActualSize is not known", "llvName", llv.Name)
			llvs.Status = &v1alpha1.LVMLogicalVolumeSnapshotStatus{
				Phase:  v1alpha1.PhasePending,
				Reason: fmt.Sprintf("Source LLV %s ActualSize is not known", llv.Name),
			}
			return true, r.cl.Status().Update(ctx, llvs)
		}

		if lvg.Status.ThinPools[thinPoolIndex].AvailableSpace.Value() < llv.Status.ActualSize.Value() {
			log.Error(nil, "LLVS: not enough space available in thin pool",
				"thinPoolName", llv.Spec.Thin.PoolName,
				"needed", llv.Status.ActualSize.String(),
				"available", lvg.Status.ThinPools[thinPoolIndex].AvailableSpace.String())
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
			log.Error(err, "Failed updating status of LLVS")
			return true, err
		}
	}

	// check node
	if llvs.Status.NodeName != r.cfg.NodeName {
		log.Info("LLVS has a Status with different node", "statusNodeName", llvs.Status.NodeName)
		return false, nil
	}

	// this block should precede any side-effects, which should be reverted during delete
	if !slices.Contains(llvs.Finalizers, internal.SdsNodeConfiguratorFinalizer) {
		llvs.Finalizers = append(llvs.Finalizers, internal.SdsNodeConfiguratorFinalizer)
		log.Info("adding finalizer to LLVS")
		if err := r.cl.Update(ctx, llvs); err != nil {
			log.Error(err, "Failed adding finalizer to LLVS")
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
		log.Debug("ran cmd", "cmd", cmd)
		if err != nil {
			log.Error(err, "unable to create a LVMLogicalVolumeSnapshot",
				"snapshotName", llvs.ActualSnapshotNameOnTheNode(),
				"vgName", llvs.Status.ActualVGNameOnTheNode,
				"lvName", llvs.Status.ActualLVNameOnTheNode)
			llvs.Status.Reason = fmt.Sprintf("Error during snapshot creation (will be retried): %v", err)
			updateErr := r.cl.Status().Update(ctx, llvs)
			err = errors.Join(err, updateErr)
			return true, err
		}
		log.Info("successfully created LV in VG for LVMLogicalVolumeSnapshot resource",
			"lvName", llvs.ActualSnapshotNameOnTheNode(),
			"vgName", llvs.Status.ActualVGNameOnTheNode)
		r.sdsCache.AddLV(llvs.Status.ActualVGNameOnTheNode, llvs.ActualSnapshotNameOnTheNode())

		llvs.Status.Reason = "Waiting for created volume to become discovered"
		err = r.cl.Status().Update(ctx, llvs)
		return true, err
	case reflect.ValueOf(snapshotLVData.Data).IsZero():
		// still "Waiting for created volume to become discovered"
		log.Info("waiting for created volume to become discovered")
		return true, nil
	default:
		log.Info("updating LLVS size")

		// update size & phase
		size := resource.NewQuantity(snapshotLVData.Data.LVSize.Value(), resource.BinarySI)
		usedSize, err := snapshotLVData.Data.GetUsedSize()
		if err != nil {
			log.Error(err, "error parsing LV size")
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
	log := r.log.WithName("reconcileLLVSDeleteFunc").WithValues("llvsName", llvs.Name)
	if len(llvs.Finalizers) == 0 {
		// means that we've deleted everything already (see below)
		return false, nil
	}

	if len(llvs.Finalizers) > 1 || llvs.Finalizers[0] != internal.SdsNodeConfiguratorFinalizer {
		// postpone deletion until another finalizer gets removed
		log.Warning("unable to delete LVMLogicalVolumeSnapshot for now due to it has any other finalizer")
		return false, nil
	}

	err := r.deleteLVIfNeeded(llvs.Name, llvs.ActualSnapshotNameOnTheNode(), llvs.Status.ActualVGNameOnTheNode)
	if err != nil {
		log.Error(err, "unable to delete the LV in VG", "lvName", llvs.ActualSnapshotNameOnTheNode(), "vgName", llvs.Status.ActualVGNameOnTheNode)
		return true, err
	}

	log.Info("successfully deleted the LV in VG", "lvName", llvs.ActualSnapshotNameOnTheNode(), "vgName", llvs.Status.ActualVGNameOnTheNode)

	// at this point we have exactly 1 finalizer
	llvs.Finalizers = nil
	if err := r.cl.Update(ctx, llvs); err != nil {
		log.Error(err, "unable to remove finalizers from the LVMLogicalVolumeSnapshot")
		return true, err
	}

	log.Info("successfully ended deletion of LVMLogicalVolumeSnapshot")
	return false, nil
}

func (r *Reconciler) deleteLVIfNeeded(llvsName, llvsActualNameOnTheNode, vgActualNameOnTheNode string) error {
	log := r.log.WithName("deleteLVIfNeeded").WithValues("lvName", llvsActualNameOnTheNode, "vgName", vgActualNameOnTheNode)
	lv := r.sdsCache.FindLV(vgActualNameOnTheNode, llvsActualNameOnTheNode)
	if lv == nil || !lv.Exist {
		log.Warning("did not find LV in VG")
		return nil
	}

	if ok, name := utils.ReadValueFromTags(lv.Data.LvTags, v1alpha1.LLVSNameTag); !ok {
		log.Warning("did not find required tags on LV in VG")
		return nil
	} else if name != llvsName {
		log.Warning("name in tag doesn't match on LV in VG", "expectedName", llvsName)
		return nil
	}

	cmd, err := r.commands.RemoveLV(vgActualNameOnTheNode, llvsActualNameOnTheNode)
	log.Debug("runs cmd", "cmd", cmd)
	if err != nil {
		log.Error(err, "unable to remove LV from VG")
		return err
	}

	log.Debug("mark LV in the cache as removed", "lvName", lv.Data.LVName)
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
