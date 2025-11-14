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

const ReconcilerName = "lvm-logical-volume-extender-controller"

type Reconciler struct {
	cl       client.Client
	log      logger.Logger
	lvgCl    *repository.LVGClient
	llvCl    *repository.LLVClient
	metrics  monitoring.Metrics
	sdsCache *cache.Cache
	cfg      ReconcilerConfig
	commands utils.Commands
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
	commands utils.Commands,
	cfg ReconcilerConfig,
) controller.Reconciler[*v1alpha1.LVMVolumeGroup] {
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
		llvCl:    repository.NewLLVClient(cl, log),
		metrics:  metrics,
		sdsCache: sdsCache,
		cfg:      cfg,
		commands: commands,
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

// ShouldReconcileCreate implements controller.Reconciler.
func (r *Reconciler) ShouldReconcileCreate(_ *v1alpha1.LVMVolumeGroup) bool {
	return true
}

// Reconcile implements controller.Reconciler.
func (r *Reconciler) Reconcile(
	ctx context.Context,
	req controller.ReconcileRequest[*v1alpha1.LVMVolumeGroup],
) (controller.Result, error) {
	lvg := req.Object
	log := r.log.WithName("Reconcile")
	if lvg != nil {
		log = log.WithValues("lvgName", lvg.Name)
	}

	if !r.shouldLLVExtenderReconcileEvent(lvg) {
		log.Info("no need to reconcile a request for the LVMVolumeGroup")
		return controller.Result{}, nil
	}

	shouldRequeue := r.ReconcileLVMLogicalVolumeExtension(ctx, lvg)
	if shouldRequeue {
		log.Warning("Reconciler needs a retry for the LVMVolumeGroup", "retryIn", r.cfg.VolumeGroupScanInterval)
		return controller.Result{
			RequeueAfter: r.cfg.VolumeGroupScanInterval,
		}, nil
	}

	log.Info("successfully reconciled LVMLogicalVolumes for the LVMVolumeGroup")
	return controller.Result{}, nil
}

func (r *Reconciler) shouldLLVExtenderReconcileEvent(newLVG *v1alpha1.LVMVolumeGroup) bool {
	// for new LVMVolumeGroups
	if reflect.DeepEqual(newLVG.Status, v1alpha1.LVMVolumeGroupStatus{}) {
		r.log.Debug("the LVMVolumeGroup should not be reconciled as its Status is not initialized yet", "lvgName", newLVG.Name)
		return false
	}

	if !utils.LVGBelongsToNode(newLVG, r.cfg.NodeName) {
		r.log.Debug("the LVMVolumeGroup should not be reconciled as it does not belong to the node", "lvgName", newLVG.Name, "nodeName", r.cfg.NodeName)
		return false
	}

	if newLVG.Status.Phase != internal.PhaseReady {
		r.log.Debug("the LVMVolumeGroup should not be reconciled as its Status.Phase is not Ready", "lvgName", newLVG.Name)
		return false
	}

	return true
}

func (r *Reconciler) ReconcileLVMLogicalVolumeExtension(
	ctx context.Context,
	lvg *v1alpha1.LVMVolumeGroup,
) bool {
	log := r.log.WithName("ReconcileLVMLogicalVolumeExtension").WithValues("lvgName", lvg.Name)
	log.Debug("tries to get LLV resources with percent size for the LVMVolumeGroup")
	llvs, err := r.getAllLLVsWithPercentSize(ctx, lvg.Name)
	if err != nil {
		log.Error(err, "unable to get LLV resources")
		return true
	}
	log.Debug("successfully got LLV resources for the LVMVolumeGroup")

	if len(llvs) == 0 {
		log.Info("no LVMLogicalVolumes with percent size were found for the LVMVolumeGroup")
		return false
	}

	shouldRetry := false
	for _, llv := range llvs {
		log := log.WithValues("llvName", llv.Name)
		log.Info("starts to reconcile the LVMLogicalVolume")
		llvRequestedSize, err := utils.GetLLVRequestedSize(&llv, lvg)
		if err != nil {
			log.Error(err, "unable to get requested size of the LVMLogicalVolume")
			shouldRetry = true
			continue
		}
		log.Debug("successfully got the requested size of the LVMLogicalVolume", "size", llvRequestedSize)

		lv := r.sdsCache.FindLV(lvg.Spec.ActualVGNameOnTheNode, llv.Spec.ActualLVNameOnTheNode)
		if lv == nil {
			err = fmt.Errorf("lv %s not found", llv.Spec.ActualLVNameOnTheNode)
			log.Error(err, "unable to find LV of the LVMLogicalVolume", "lvName", llv.Spec.ActualLVNameOnTheNode)
			err = r.llvCl.UpdatePhaseIfNeeded(ctx, &llv, v1alpha1.PhaseFailed, err.Error())
			if err != nil {
				log.Error(err, "unable to update the LVMLogicalVolume")
			}
			shouldRetry = true
			continue
		}

		if utils.AreSizesEqualWithinDelta(llvRequestedSize, lv.Data.LVSize, internal.ResizeDelta) {
			log.Info("the LVMLogicalVolume should not be extended")
			continue
		}

		if llvRequestedSize.Value() < lv.Data.LVSize.Value() {
			log.Warning("the LVMLogicalVolume requested size is less than actual one on the node",
				"requestedSize", llvRequestedSize,
				"actualSize", lv.Data.LVSize)
			continue
		}

		freeSpace := utils.GetFreeLVGSpaceForLLV(lvg, &llv)
		if llvRequestedSize.Value()+internal.ResizeDelta.Value() > freeSpace.Value() {
			err = errors.New("not enough space")
			log.Error(err, "unable to extend the LV of the LVMLogicalVolume", "lvName", llv.Spec.ActualLVNameOnTheNode)
			err = r.llvCl.UpdatePhaseIfNeeded(ctx, &llv, v1alpha1.PhaseFailed, fmt.Sprintf("unable to extend LV, err: %s", err.Error()))
			if err != nil {
				log.Error(err, "unable to update the LVMLogicalVolume")
				shouldRetry = true
			}
			continue
		}

		log.Info("the LVMLogicalVolume should be extended",
			"fromSize", llv.Status.ActualSize,
			"toSize", llvRequestedSize)
		err = r.llvCl.UpdatePhaseIfNeeded(ctx, &llv, v1alpha1.PhaseResizing, "")
		if err != nil {
			log.Error(err, "unable to update the LVMLogicalVolume")
			shouldRetry = true
			continue
		}

		cmd, err := r.commands.ExtendLV(llvRequestedSize.Value(), lvg.Spec.ActualVGNameOnTheNode, llv.Spec.ActualLVNameOnTheNode)
		if err != nil {
			log.Error(err, "unable to extend LV of the LVMLogicalVolume", "lvName", llv.Spec.ActualLVNameOnTheNode, "cmd", cmd)
			err = r.llvCl.UpdatePhaseIfNeeded(ctx, &llv, v1alpha1.PhaseFailed, fmt.Sprintf("unable to extend LV, err: %s", err.Error()))
			if err != nil {
				log.Error(err, "unable to update the LVMLogicalVolume")
			}
			shouldRetry = true
			continue
		}
		log.Info("the LVMLogicalVolume has been successfully extended")

		var (
			maxAttempts     = 5
			currentAttempts = 0
		)
		for currentAttempts < maxAttempts {
			lv = r.sdsCache.FindLV(lvg.Spec.ActualVGNameOnTheNode, llv.Spec.ActualLVNameOnTheNode)
			if utils.AreSizesEqualWithinDelta(lv.Data.LVSize, llvRequestedSize, internal.ResizeDelta) {
				log.Debug("LV of the LVMLogicalVolume was successfully updated in the cache", "lvName", lv.Data.LVName)
				break
			}

			log.Warning("LV size of the LVMLogicalVolume was not yet updated in the cache, retry...", "lvName", lv.Data.LVName)
			currentAttempts++
			time.Sleep(1 * time.Second)
		}

		if currentAttempts == maxAttempts {
			err = fmt.Errorf("LV %s is not updated in the cache", lv.Data.LVName)
			log.Error(err, "unable to resize the LVMLogicalVolume")
			shouldRetry = true

			if err = r.llvCl.UpdatePhaseIfNeeded(ctx, &llv, v1alpha1.PhaseFailed, err.Error()); err != nil {
				log.Error(err, "unable to update the LVMLogicalVolume")
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
