package llv_extender

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
	"time"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const ReconcilerName = "lvm-logical-volume-extender-controller"

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
	VolumeGroupScanInterval time.Duration
}

func NewReconciler(
	cl client.Client,
	log logger.Logger,
	metrics monitoring.Metrics,
	sdsCache *cache.Cache,
	cfg ReconcilerConfig,
) controller.Reconciler[*v1alpha1.LVMVolumeGroup] {
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
		llvCl:    utils.NewLLVClient(cl, log),
		metrics:  metrics,
		sdsCache: sdsCache,
		cfg:      cfg,
	}
}

// Name implements controller.Reconciler.
func (r *Reconciler) Name() string {
	return ReconcilerName
}

// MaxConcurrentReconciles implements controller.Reconciler.
func (r *Reconciler) MaxConcurrentReconciles() int {
	return 1
}

// ShouldReconcileUpdate implements controller.Reconciler.
func (r *Reconciler) ShouldReconcileUpdate(_ *v1alpha1.LVMVolumeGroup, _ *v1alpha1.LVMVolumeGroup) bool {
	return true
}

// Reconcile implements controller.Reconciler.
func (r *Reconciler) Reconcile(
	ctx context.Context,
	req controller.ReconcileRequest[*v1alpha1.LVMVolumeGroup],
) (controller.Result, error) {
	lvg := req.Object

	if !r.shouldLLVExtenderReconcileEvent(lvg) {
		r.log.Info(fmt.Sprintf("[RunLVMLogicalVolumeExtenderWatcherController] no need to reconcile a request for the LVMVolumeGroup %s", lvg.Name))
		return controller.Result{}, nil
	}

	shouldRequeue := r.ReconcileLVMLogicalVolumeExtension(ctx, lvg)
	if shouldRequeue {
		r.log.Warning(fmt.Sprintf("[RunLVMLogicalVolumeExtenderWatcherController] Reconciler needs a retry for the LVMVolumeGroup %s. Retry in %s", lvg.Name, r.cfg.VolumeGroupScanInterval.String()))
		return controller.Result{
			RequeueAfter: r.cfg.VolumeGroupScanInterval,
		}, nil
	}

	r.log.Info(fmt.Sprintf("[RunLVMLogicalVolumeExtenderWatcherController] successfully reconciled LVMLogicalVolumes for the LVMVolumeGroup %s", lvg.Name))
	return controller.Result{}, nil
}

func (r *Reconciler) shouldLLVExtenderReconcileEvent(newLVG *v1alpha1.LVMVolumeGroup) bool {
	// for new LVMVolumeGroups
	if reflect.DeepEqual(newLVG.Status, v1alpha1.LVMVolumeGroupStatus{}) {
		r.log.Debug(fmt.Sprintf("[RunLVMLogicalVolumeExtenderWatcherController] the LVMVolumeGroup %s should not be reconciled as its Status is not initialized yet", newLVG.Name))
		return false
	}

	if !utils.LVGBelongsToNode(newLVG, r.cfg.NodeName) {
		r.log.Debug(fmt.Sprintf("[RunLVMLogicalVolumeExtenderWatcherController] the LVMVolumeGroup %s should not be reconciled as it does not belong to the node %s", newLVG.Name, r.cfg.NodeName))
		return false
	}

	if newLVG.Status.Phase != internal.PhaseReady {
		r.log.Debug(fmt.Sprintf("[RunLVMLogicalVolumeExtenderWatcherController] the LVMVolumeGroup %s should not be reconciled as its Status.Phase is not Ready", newLVG.Name))
		return false
	}

	return true
}

func (r *Reconciler) ReconcileLVMLogicalVolumeExtension(
	ctx context.Context,
	lvg *v1alpha1.LVMVolumeGroup,
) bool {
	r.log.Debug(fmt.Sprintf("[ReconcileLVMLogicalVolumeExtension] tries to get LLV resources with percent size for the LVMVolumeGroup %s", lvg.Name))
	llvs, err := r.getAllLLVsWithPercentSize(ctx, lvg.Name)
	if err != nil {
		r.log.Error(err, "[ReconcileLVMLogicalVolumeExtension] unable to get LLV resources")
		return true
	}
	r.log.Debug(fmt.Sprintf("[ReconcileLVMLogicalVolumeExtension] successfully got LLV resources for the LVMVolumeGroup %s", lvg.Name))

	if len(llvs) == 0 {
		r.log.Info(fmt.Sprintf("[ReconcileLVMLogicalVolumeExtension] no LVMLogicalVolumes with percent size were found for the LVMVolumeGroup %s", lvg.Name))
		return false
	}

	shouldRetry := false
	for _, llv := range llvs {
		r.log.Info(fmt.Sprintf("[ReconcileLVMLogicalVolumeExtension] starts to reconcile the LVMLogicalVolume %s", llv.Name))
		llvRequestedSize, err := utils.GetLLVRequestedSize(&llv, lvg)
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[ReconcileLVMLogicalVolumeExtension] unable to get requested size of the LVMLogicalVolume %s", llv.Name))
			shouldRetry = true
			continue
		}
		r.log.Debug(fmt.Sprintf("[ReconcileLVMLogicalVolumeExtension] successfully got the requested size of the LVMLogicalVolume %s, size: %s", llv.Name, llvRequestedSize.String()))

		lv := r.sdsCache.FindLV(lvg.Spec.ActualVGNameOnTheNode, llv.Spec.ActualLVNameOnTheNode)
		if lv == nil {
			err = fmt.Errorf("lv %s not found", llv.Spec.ActualLVNameOnTheNode)
			r.log.Error(err, fmt.Sprintf("[ReconcileLVMLogicalVolumeExtension] unable to find LV %s of the LVMLogicalVolume %s", llv.Spec.ActualLVNameOnTheNode, llv.Name))
			err = r.llvCl.UpdatePhaseIfNeeded(ctx, &llv, internal.LLVStatusPhaseFailed, err.Error())
			if err != nil {
				r.log.Error(err, fmt.Sprintf("[ReconcileLVMLogicalVolumeExtension] unable to update the LVMLogicalVolume %s", llv.Name))
			}
			shouldRetry = true
			continue
		}

		if utils.AreSizesEqualWithinDelta(llvRequestedSize, lv.Data.LVSize, internal.ResizeDelta) {
			r.log.Info(fmt.Sprintf("[ReconcileLVMLogicalVolumeExtension] the LVMLogicalVolume %s should not be extended", llv.Name))
			continue
		}

		if llvRequestedSize.Value() < lv.Data.LVSize.Value() {
			r.log.Warning(fmt.Sprintf("[ReconcileLVMLogicalVolumeExtension] the LVMLogicalVolume %s requested size %s is less than actual one on the node %s", llv.Name, llvRequestedSize.String(), lv.Data.LVSize.String()))
			continue
		}

		freeSpace := utils.GetFreeLVGSpaceForLLV(lvg, &llv)
		if llvRequestedSize.Value()+internal.ResizeDelta.Value() > freeSpace.Value() {
			err = errors.New("not enough space")
			r.log.Error(err, fmt.Sprintf("[ReconcileLVMLogicalVolumeExtension] unable to extend the LV %s of the LVMLogicalVolume %s", llv.Spec.ActualLVNameOnTheNode, llv.Name))
			err = r.llvCl.UpdatePhaseIfNeeded(ctx, &llv, internal.LLVStatusPhaseFailed, fmt.Sprintf("unable to extend LV, err: %s", err.Error()))
			if err != nil {
				r.log.Error(err, fmt.Sprintf("[ReconcileLVMLogicalVolumeExtension] unable to update the LVMLogicalVolume %s", llv.Name))
				shouldRetry = true
			}
			continue
		}

		r.log.Info(fmt.Sprintf("[ReconcileLVMLogicalVolumeExtension] the LVMLogicalVolume %s should be extended from %s to %s size", llv.Name, llv.Status.ActualSize.String(), llvRequestedSize.String()))
		err = r.llvCl.UpdatePhaseIfNeeded(ctx, &llv, internal.LLVStatusPhaseResizing, "")
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[ReconcileLVMLogicalVolumeExtension] unable to update the LVMLogicalVolume %s", llv.Name))
			shouldRetry = true
			continue
		}

		cmd, err := utils.ExtendLV(llvRequestedSize.Value(), lvg.Spec.ActualVGNameOnTheNode, llv.Spec.ActualLVNameOnTheNode)
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[ReconcileLVMLogicalVolumeExtension] unable to extend LV %s of the LVMLogicalVolume %s, cmd: %s", llv.Spec.ActualLVNameOnTheNode, llv.Name, cmd))
			err = r.llvCl.UpdatePhaseIfNeeded(ctx, &llv, internal.LLVStatusPhaseFailed, fmt.Sprintf("unable to extend LV, err: %s", err.Error()))
			if err != nil {
				r.log.Error(err, fmt.Sprintf("[ReconcileLVMLogicalVolumeExtension] unable to update the LVMLogicalVolume %s", llv.Name))
			}
			shouldRetry = true
			continue
		}
		r.log.Info(fmt.Sprintf("[ReconcileLVMLogicalVolumeExtension] the LVMLogicalVolume %s has been successfully extended", llv.Name))

		var (
			maxAttempts     = 5
			currentAttempts = 0
		)
		for currentAttempts < maxAttempts {
			lv = r.sdsCache.FindLV(lvg.Spec.ActualVGNameOnTheNode, llv.Spec.ActualLVNameOnTheNode)
			if utils.AreSizesEqualWithinDelta(lv.Data.LVSize, llvRequestedSize, internal.ResizeDelta) {
				r.log.Debug(fmt.Sprintf("[ReconcileLVMLogicalVolumeExtension] LV %s of the LVMLogicalVolume %s was successfully updated in the cache", lv.Data.LVName, llv.Name))
				break
			}

			r.log.Warning(fmt.Sprintf("[ReconcileLVMLogicalVolumeExtension] LV %s size of the LVMLogicalVolume %s was not yet updated in the cache, retry...", lv.Data.LVName, llv.Name))
			currentAttempts++
			time.Sleep(1 * time.Second)
		}

		if currentAttempts == maxAttempts {
			err = fmt.Errorf("LV %s is not updated in the cache", lv.Data.LVName)
			r.log.Error(err, fmt.Sprintf("[ReconcileLVMLogicalVolumeExtension] unable to resize the LVMLogicalVolume %s", llv.Name))
			shouldRetry = true

			if err = r.llvCl.UpdatePhaseIfNeeded(ctx, &llv, internal.LLVStatusPhaseFailed, err.Error()); err != nil {
				r.log.Error(err, fmt.Sprintf("[ReconcileLVMLogicalVolumeExtension] unable to update the LVMLogicalVolume %s", llv.Name))
			}
			continue
		}

		if err := r.llvCl.UpdatePhaseToCreatedIfNeeded(ctx, &llv, lv.Data.LVSize); err != nil {
			shouldRetry = true
			continue
		}
	}
	return shouldRetry
}

func (r *Reconciler) getAllLLVsWithPercentSize(ctx context.Context, lvgName string) ([]v1alpha1.LVMLogicalVolume, error) {
	llvList := &v1alpha1.LVMLogicalVolumeList{}
	err := r.cl.List(ctx, llvList)
	if err != nil {
		return nil, err
	}

	result := make([]v1alpha1.LVMLogicalVolume, 0, len(llvList.Items))
	for _, llv := range llvList.Items {
		if llv.Spec.LVMVolumeGroupName == lvgName && utils.IsPercentSize(llv.Spec.Size) {
			result = append(result, llv)
		}
	}

	return result, nil
}
