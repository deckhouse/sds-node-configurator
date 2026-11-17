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

package llv_extender

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
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

const ReconcilerName = "lvm-logical-volume-extender-controller"

type Reconciler struct {
	cl        client.Client
	log       logger.Logger
	lvgCl     *repository.LVGClient
	llvCl     *repository.LLVClient
	metrics   *monitoring.Metrics
	sdsCache  *cache.Cache
	cfg       ReconcilerConfig
	commands  utils.Commands
	thinPools *thinpoolalloc.Locks
}

type ReconcilerConfig struct {
	NodeName                string
	VolumeGroupScanInterval time.Duration
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
) controller.Reconciler[*v1alpha1.LVMVolumeGroup] {
	if cfg.ThinPools == nil {
		panic("llv_extender: ReconcilerConfig.ThinPools must be the thin pool locks shared by the agent")
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
		llvCl:     repository.NewLLVClient(cl, log, recorder),
		metrics:   metrics,
		sdsCache:  sdsCache,
		cfg:       cfg,
		commands:  commands,
		thinPools: cfg.ThinPools,
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
func (r *Reconciler) ShouldReconcileUpdate(_ *v1alpha1.LVMVolumeGroup, objectNew *v1alpha1.LVMVolumeGroup) bool {
	return objectNew.Spec.Local.NodeName == r.cfg.NodeName
}

// ShouldReconcileCreate implements controller.Reconciler.
func (r *Reconciler) ShouldReconcileCreate(obj *v1alpha1.LVMVolumeGroup) bool {
	return obj.Spec.Local.NodeName == r.cfg.NodeName
}

// Reconcile implements controller.Reconciler.
func (r *Reconciler) Reconcile(
	ctx context.Context,
	req controller.ReconcileRequest[*v1alpha1.LVMVolumeGroup],
) (controller.Result, error) {
	log := r.log

	lvg := req.Object

	if !r.shouldLLVExtenderReconcileEvent(lvg) {
		log.Info(fmt.Sprintf("no need to reconcile a request for the LVMVolumeGroup %s", lvg.Name))
		return controller.Result{}, nil
	}

	shouldRequeue := r.ReconcileLVMLogicalVolumeExtension(ctx, lvg)
	if shouldRequeue {
		log.Warn(fmt.Sprintf("reconciler needs a retry for the LVMVolumeGroup %s. Retry in %s", lvg.Name, r.cfg.VolumeGroupScanInterval.String()))
		return controller.Result{
			RequeueAfter: r.cfg.VolumeGroupScanInterval,
		}, nil
	}

	log.Info(fmt.Sprintf("successfully reconciled LVMLogicalVolumes for the LVMVolumeGroup %s", lvg.Name))
	return controller.Result{}, nil
}

func (r *Reconciler) shouldLLVExtenderReconcileEvent(newLVG *v1alpha1.LVMVolumeGroup) bool {
	log := r.log

	// for new LVMVolumeGroups
	if reflect.DeepEqual(newLVG.Status, v1alpha1.LVMVolumeGroupStatus{}) {
		log.Debug(fmt.Sprintf("the LVMVolumeGroup %s should not be reconciled as its Status is not initialized yet", newLVG.Name))
		return false
	}

	if newLVG.Status.Phase != internal.PhaseReady {
		log.Debug(fmt.Sprintf("the LVMVolumeGroup %s should not be reconciled as its Status.Phase is not Ready", newLVG.Name))
		return false
	}

	if newLVG.Status.ExtentSize.Value() == 0 {
		log.Debug(fmt.Sprintf("the LVMVolumeGroup %s should not be reconciled as its ExtentSize is not populated yet", newLVG.Name))
		return false
	}

	return true
}

func (r *Reconciler) ReconcileLVMLogicalVolumeExtension(
	ctx context.Context,
	lvg *v1alpha1.LVMVolumeGroup,
) bool {
	log := r.log

	log.Debug(fmt.Sprintf("tries to get LLV resources with percent size for the LVMVolumeGroup %s", lvg.Name))
	llvs, err := r.getAllLLVsWithPercentSize(ctx, lvg.Name)
	if err != nil {
		r.log.Error("unable to get LLV resources", logger.Err(err))
		return true
	}
	log.Debug(fmt.Sprintf("successfully got LLV resources for the LVMVolumeGroup %s", lvg.Name))

	if len(llvs) == 0 {
		log.Info(fmt.Sprintf("no LVMLogicalVolumes with percent size were found for the LVMVolumeGroup %s", lvg.Name))
		return false
	}

	shouldRetry := false
	for _, llv := range llvs {
		log.Info(fmt.Sprintf("starts to reconcile the LVMLogicalVolume %s", llv.Name))
		llvRequestedSize, err := utils.GetLLVRequestedSize(&llv, lvg)
		if err != nil {
			log.Error(fmt.Sprintf("unable to get requested size of the LVMLogicalVolume %s", llv.Name), logger.Err(err))
			shouldRetry = true
			continue
		}
		log.Debug(fmt.Sprintf("successfully got the requested size of the LVMLogicalVolume %s, size: %s", llv.Name, llvRequestedSize.String()))

		lv := r.sdsCache.FindLV(lvg.Spec.ActualVGNameOnTheNode, llv.Spec.ActualLVNameOnTheNode)
		if lv == nil {
			log.Warn(fmt.Sprintf("LV %s of the LVMLogicalVolume %s not found in cache, will retry", llv.Spec.ActualLVNameOnTheNode, llv.Name))
			shouldRetry = true
			continue
		}

		// Every LLV here has a percent size (that is what this reconciler
		// selects), so the alignment must floor: a "100%FREE" volume already
		// owns every free extent, and rounding its request up past the
		// unaligned VG size would send it into the extension below, where
		// there is nothing left to extend into.
		alignedRequestedSize, err := utils.AlignRequestedSizeToExtent(llv.Spec.Size, llvRequestedSize, lvg.Status.ExtentSize)
		if err != nil {
			log.Error(fmt.Sprintf("unable to align size for LVMLogicalVolume %s", llv.Name), logger.Err(err))
			shouldRetry = true
			continue
		}

		if lv.Data.LVSize.Value() >= alignedRequestedSize.Value() {
			log.Info(fmt.Sprintf("the LVMLogicalVolume %s should not be extended (actual %s >= aligned requested %s)", llv.Name, lv.Data.LVSize.String(), alignedRequestedSize.String()))
			// The volume is the size it asked for, so whatever an earlier pass
			// concluded about it no longer holds. A "not enough space" verdict
			// below is computed from the group's free space, which lags behind
			// the LV the agent has just created: for a moment the LV is full
			// size while the group still reports free extents, and the request
			// resolved against that stale free space asks for more than exists.
			// The phase written then is permanent - no later pass reaches the
			// extension that would clear it - and the volume stays Failed for
			// good, with a consumer (sds-elastic waits for every OSD volume)
			// blocked behind a volume that is in fact correct.
			if llv.Status == nil || llv.Status.Phase != v1alpha1.PhaseCreated {
				if err := r.llvCl.UpdatePhaseToCreatedIfNeeded(ctx, &llv, lvg, lv.Data.LVSize); err != nil {
					log.Error(fmt.Sprintf("unable to clear the stale phase of the LVMLogicalVolume %s", llv.Name), logger.Err(err))
					shouldRetry = true
				}
			}
			continue
		}

		if failure, err := r.extendLV(ctx, lvg, &llv, lv.Data.LVSize, alignedRequestedSize); failure != "" || err != nil {
			if failure != "" {
				if err := r.llvCl.UpdatePhaseIfNeeded(ctx, &llv, v1alpha1.PhaseFailed, failure); err != nil {
					log.Error(fmt.Sprintf("unable to update the LVMLogicalVolume %s", llv.Name), logger.Err(err))
				}
			}
			// Retry: the numbers this verdict rests on - the group's free space
			// and the cached LV - are both refreshed out of band, so the same
			// volume can well fit on the next pass. Without a retry the failure
			// waits for an unrelated event to disturb the group.
			shouldRetry = true
			continue
		}
		log.Info(fmt.Sprintf("the LVMLogicalVolume %s has been successfully extended", llv.Name))

		lvData, getLVCmd, _, getLVErr := r.commands.GetLV(lvg.Spec.ActualVGNameOnTheNode, llv.Spec.ActualLVNameOnTheNode)
		log.Debug(fmt.Sprintf("ran cmd: %s", getLVCmd))
		if getLVErr != nil {
			log.Error(fmt.Sprintf("unable to get LV %s info from LVM after extension, will retry", llv.Spec.ActualLVNameOnTheNode), logger.Err(getLVErr))
			shouldRetry = true
			continue
		}
		lv = &cache.LVData{Data: lvData, Exist: true}
		if lv.Data.LVSize.Value() < alignedRequestedSize.Value() {
			log.Warn(fmt.Sprintf("LV %s of the LVMLogicalVolume %s was extended but LVM reports size %s, expected at least %s, will retry", lv.Data.LVName, llv.Name, lv.Data.LVSize.String(), alignedRequestedSize.String()))
			shouldRetry = true
			continue
		}
		log.Debug(fmt.Sprintf("LV %s of the LVMLogicalVolume %s has actual size %s matching aligned request", lv.Data.LVName, llv.Name, lv.Data.LVSize.String()))

		if err := r.llvCl.UpdatePhaseToCreatedIfNeeded(ctx, &llv, lvg, lv.Data.LVSize); err != nil {
			shouldRetry = true
			continue
		}
	}
	return shouldRetry
}

func (r *Reconciler) extendLV(
	ctx context.Context,
	lvg *v1alpha1.LVMVolumeGroup,
	llv *v1alpha1.LVMLogicalVolume,
	lvSize, size resource.Quantity,
) (string, error) {
	log := r.log

	if llv.Spec.Type == internal.Thin && llv.Spec.Thin != nil {
		unlock, err := r.thinPools.Lock(ctx, lvg.Spec.ActualVGNameOnTheNode, llv.Spec.Thin.PoolName)
		if err != nil {
			log.Error(fmt.Sprintf("unable to extend the LV %s of the LVMLogicalVolume %s", llv.Spec.ActualLVNameOnTheNode, llv.Name), logger.Err(err))
			return "", err
		}
		defer unlock()

		err = thinpoolalloc.CheckSpace(log, r.commands, lvg, llv.Spec.Thin.PoolName, llv.Spec.ActualLVNameOnTheNode, "", size)
		if errors.Is(err, thinpoolalloc.ErrNotEnoughSpace) {
			log.Error(fmt.Sprintf("unable to extend the LV %s of the LVMLogicalVolume %s to %s", llv.Spec.ActualLVNameOnTheNode, llv.Name, size.String()), logger.Err(err))
			return fmt.Sprintf("unable to extend LV, err: %s", err.Error()), nil
		}
		if err != nil {
			log.Error(fmt.Sprintf("unable to extend the LV %s of the LVMLogicalVolume %s", llv.Spec.ActualLVNameOnTheNode, llv.Name), logger.Err(err))
			return "", err
		}
	} else {
		extendingSize := size.Value() - lvSize.Value()
		freeSpace := utils.GetFreeLVGSpaceForLLV(lvg, llv)
		if extendingSize > freeSpace.Value() {
			err := errors.New("not enough space")
			log.Error(fmt.Sprintf("unable to extend the LV %s of the LVMLogicalVolume %s by %s, free space: %s", llv.Spec.ActualLVNameOnTheNode, llv.Name, resource.NewQuantity(extendingSize, resource.BinarySI).String(), freeSpace.String()), logger.Err(err))
			return fmt.Sprintf("unable to extend LV, err: %s", err.Error()), nil
		}
	}

	log.Info(fmt.Sprintf("the LVMLogicalVolume %s should be extended from %s to %s size", llv.Name, lvSize.String(), size.String()))
	if err := r.llvCl.UpdatePhaseIfNeeded(ctx, llv, v1alpha1.PhaseResizing, ""); err != nil {
		log.Error(fmt.Sprintf("unable to update the LVMLogicalVolume %s", llv.Name), logger.Err(err))
		return "", err
	}

	cmd, err := r.commands.ExtendLV(size.Value(), lvg.Spec.ActualVGNameOnTheNode, llv.Spec.ActualLVNameOnTheNode)
	if err != nil {
		log.Error(fmt.Sprintf("unable to extend LV %s of the LVMLogicalVolume %s, cmd: %s", llv.Spec.ActualLVNameOnTheNode, llv.Name, cmd), logger.Err(err))
		return fmt.Sprintf("unable to extend LV, err: %s", err.Error()), nil
	}

	return "", nil
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
