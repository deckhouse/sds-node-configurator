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

// Package lsllv creates and destroys the volumes of a shared pool.
//
// Only the metadata owner of a group runs it. LVM metadata has one writer by
// construction, and the alternative — every node creating the volumes it needs
// — is a queue on the group lock with no upper bound on its length.
//
// Destroying is the delicate half. Capacity carved out of a shared LUN goes
// back into the same pool and comes out again as somebody else's volume, so a
// volume that was removed without being erased hands its contents to the next
// tenant. That is why the erase is marked in the volume's own tags before it
// starts: the marker has to outlive the metadata owner changing halfway
// through, and a status field written by the old owner is not read by the new
// one.
package lsllv

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/cache"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/controller"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/logger"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/utils"
)

const ReconcilerName = "lvm-shared-logical-volume-controller"

const (
	PhasePending = "Pending"
	PhaseCreated = "Created"
	PhaseFailed  = "Failed"

	ConditionCreated = "Created"
	ConditionZeroed  = "Zeroed"
	ConditionReady   = "Ready"

	// PendingCleanupTag marks a volume whose erase has started and may not have
	// finished. It lives on the volume rather than in the resource because the
	// metadata owner can change between the two, and a marker the new owner
	// cannot see is a volume that gets removed without being erased.
	PendingCleanupTag = "storage.deckhouse.io/pending-cleanup"
)

type Reconciler struct {
	cl       client.Client
	log      logger.Logger
	sdsCache *cache.Cache
	commands utils.Commands
	cfg      ReconcilerConfig
}

type ReconcilerConfig struct {
	NodeName string
}

func NewReconciler(
	cl client.Client,
	log logger.Logger,
	sdsCache *cache.Cache,
	commands utils.Commands,
	cfg ReconcilerConfig,
) *Reconciler {
	return &Reconciler{cl: cl, log: log, sdsCache: sdsCache, commands: commands, cfg: cfg}
}

func (r *Reconciler) Name() string {
	return ReconcilerName
}

func (r *Reconciler) MaxConcurrentReconciles() int {
	return 1
}

func (r *Reconciler) ShouldReconcileCreate(_ *v1alpha1.LVMSharedLogicalVolume) bool {
	return true
}

func (r *Reconciler) ShouldReconcileUpdate(_, _ *v1alpha1.LVMSharedLogicalVolume) bool {
	return true
}

func (r *Reconciler) Reconcile(
	ctx context.Context,
	req controller.ReconcileRequest[*v1alpha1.LVMSharedLogicalVolume],
) (controller.Result, error) {
	volume := req.Object

	group := &v1alpha1.LVMSharedVolumeGroup{}
	if err := r.cl.Get(ctx, client.ObjectKey{Name: volume.Spec.LVMSharedVolumeGroupName}, group); err != nil {
		if errors.IsNotFound(err) {
			// Without the group there is nothing to create the volume in and
			// nothing to erase it on.
			return controller.Result{}, nil
		}
		return controller.Result{}, fmt.Errorf("get LVMSharedVolumeGroup %s: %w", volume.Spec.LVMSharedVolumeGroupName, err)
	}

	if group.Spec.MetadataOwner != r.cfg.NodeName {
		return controller.Result{}, nil
	}

	if volume.DeletionTimestamp != nil {
		return r.remove(ctx, volume, group)
	}

	return r.create(ctx, volume, group)
}

func (r *Reconciler) create(
	ctx context.Context,
	volume *v1alpha1.LVMSharedLogicalVolume,
	group *v1alpha1.LVMSharedVolumeGroup,
) (controller.Result, error) {
	if err := r.addFinalizer(ctx, volume); err != nil {
		return controller.Result{}, err
	}

	vgName := group.Spec.ActualVGNameOnTheNode
	lvName := volume.Spec.ActualLVNameOnTheNode

	if lv := r.findLV(vgName, lvName); lv != nil {
		r.setStatus(ctx, volume, PhaseCreated, true, "Created",
			fmt.Sprintf("%s/%s exists", vgName, lvName))
		return controller.Result{}, nil
	}

	r.log.Info(fmt.Sprintf("[%s] creating %s/%s of %s", ReconcilerName, vgName, lvName, volume.Spec.Size))
	cmd, err := r.commands.CreateLVShared(ctx, vgName, lvName, volume.Spec.Size)
	if err != nil {
		r.setStatus(ctx, volume, PhasePending, false, "CreationFailed",
			fmt.Sprintf("cannot create %s/%s: %s", vgName, lvName, err.Error()))
		return controller.Result{}, fmt.Errorf("create %s/%s (cmd: %s): %w", vgName, lvName, cmd, err)
	}

	// Creating is not attaching. lvcreate leaves the volume active here, and a
	// metadata owner that kept every volume it made would hold the exclusive
	// lock of the whole pool — no other node could ever attach anything.
	if cmd, err := r.commands.LVDeactivateShared(ctx, vgName, []string{lvName}); err != nil {
		r.log.Warning(fmt.Sprintf("[%s] created %s/%s but could not release it (cmd: %s): %s",
			ReconcilerName, vgName, lvName, cmd, err.Error()))
		return controller.Result{RequeueAfter: 10 * time.Second}, nil
	}

	r.setStatus(ctx, volume, PhaseCreated, true, "Created",
		fmt.Sprintf("%s/%s is created and released", vgName, lvName))
	return controller.Result{}, nil
}

// remove erases the volume and then destroys it, in that order and never the
// other way round.
func (r *Reconciler) remove(
	ctx context.Context,
	volume *v1alpha1.LVMSharedLogicalVolume,
	group *v1alpha1.LVMSharedVolumeGroup,
) (controller.Result, error) {
	if !slices.Contains(volume.Finalizers, internal.SdsNodeConfiguratorFinalizer) {
		return controller.Result{}, nil
	}

	vgName := group.Spec.ActualVGNameOnTheNode
	lvName := volume.Spec.ActualLVNameOnTheNode

	lv := r.findLV(vgName, lvName)
	if lv == nil {
		// Already gone. Nothing to erase and nothing to remove.
		return controller.Result{}, r.removeFinalizer(ctx, volume)
	}

	cleanup := effectiveCleanup(volume, group)
	if cleanup != "" {
		// The marker goes on before the first byte is written, so that an owner
		// that takes over halfway through sees a volume that may still hold the
		// previous tenant's data and erases it again. Erasing twice costs time;
		// not erasing once costs the data.
		// lvs reports tags as one comma-separated string.
		if !slices.Contains(strings.Split(lv.LvTags, ","), PendingCleanupTag) {
			if cmd, err := r.commands.SetLVTagShared(ctx, vgName, lvName, PendingCleanupTag, true); err != nil {
				return controller.Result{}, fmt.Errorf("mark %s/%s for cleanup (cmd: %s): %w", vgName, lvName, cmd, err)
			}
		}

		requeue, err := r.cleanupVolume(ctx, volume, group, cleanup)
		if err != nil {
			r.setStatus(ctx, volume, PhaseFailed, false, "CleanupFailed",
				fmt.Sprintf("cannot erase %s/%s: %s", vgName, lvName, err.Error()))
			return controller.Result{}, err
		}
		if requeue {
			return controller.Result{RequeueAfter: 10 * time.Second}, nil
		}

		if cmd, err := r.commands.SetLVTagShared(ctx, vgName, lvName, PendingCleanupTag, false); err != nil {
			// The volume is erased; failing to drop the marker only means the
			// next pass erases it again, which is safe.
			r.log.Warning(fmt.Sprintf("[%s] erased %s/%s but could not clear its marker (cmd: %s): %s",
				ReconcilerName, vgName, lvName, cmd, err.Error()))
		}
	}

	if cmd, err := r.commands.RemoveLVShared(ctx, vgName, lvName); err != nil {
		return controller.Result{}, fmt.Errorf("remove %s/%s (cmd: %s): %w", vgName, lvName, cmd, err)
	}

	r.log.Info(fmt.Sprintf("[%s] %s/%s is removed", ReconcilerName, vgName, lvName))
	return controller.Result{}, r.removeFinalizer(ctx, volume)
}

// effectiveCleanup resolves the policy a volume is erased with. The volume's
// own value wins, and the group's is the floor rather than the default: a
// volume asking for nothing still gets what the pool demands.
func effectiveCleanup(volume *v1alpha1.LVMSharedLogicalVolume, group *v1alpha1.LVMSharedVolumeGroup) string {
	if volume.Spec.VolumeCleanup != "" {
		return volume.Spec.VolumeCleanup
	}
	return group.Spec.VolumeCleanup
}

func (r *Reconciler) findLV(vgName, lvName string) *internal.LVData {
	lvs, _ := r.sdsCache.GetLVs()
	for i := range lvs {
		if lvs[i].VGName == vgName && lvs[i].LVName == lvName {
			return &lvs[i]
		}
	}
	return nil
}

func (r *Reconciler) addFinalizer(ctx context.Context, volume *v1alpha1.LVMSharedLogicalVolume) error {
	if slices.Contains(volume.Finalizers, internal.SdsNodeConfiguratorFinalizer) {
		return nil
	}

	patch := client.MergeFrom(volume.DeepCopy())
	volume.Finalizers = append(volume.Finalizers, internal.SdsNodeConfiguratorFinalizer)
	if err := r.cl.Patch(ctx, volume, patch); err != nil {
		return fmt.Errorf("add finalizer to %s: %w", volume.Name, err)
	}
	return nil
}

func (r *Reconciler) removeFinalizer(ctx context.Context, volume *v1alpha1.LVMSharedLogicalVolume) error {
	patch := client.MergeFrom(volume.DeepCopy())
	volume.Finalizers = slices.DeleteFunc(volume.Finalizers, func(f string) bool {
		return f == internal.SdsNodeConfiguratorFinalizer
	})
	if err := r.cl.Patch(ctx, volume, patch); err != nil {
		return fmt.Errorf("remove finalizer from %s: %w", volume.Name, err)
	}
	return nil
}

func (r *Reconciler) setStatus(
	ctx context.Context,
	volume *v1alpha1.LVMSharedLogicalVolume,
	phase string,
	ready bool,
	reason, message string,
) {
	patch := client.MergeFrom(volume.DeepCopy())

	if volume.Status == nil {
		volume.Status = &v1alpha1.LVMSharedLogicalVolumeStatus{}
	}
	volume.Status.Phase = phase
	volume.Status.ObservedGeneration = volume.Generation

	status := metav1.ConditionFalse
	if ready {
		status = metav1.ConditionTrue
	}
	for _, conditionType := range []string{ConditionCreated, ConditionZeroed, ConditionReady} {
		setCondition(&volume.Status.Conditions, metav1.Condition{
			Type:               conditionType,
			Status:             status,
			ObservedGeneration: volume.Generation,
			Reason:             reason,
			Message:            message,
		})
	}

	if err := r.cl.Status().Patch(ctx, volume, patch); err != nil {
		r.log.Error(err, fmt.Sprintf("[%s] unable to update the status of %s", ReconcilerName, volume.Name))
	}
}

// setCondition bumps lastTransitionTime only when the status itself changes, so
// that the timestamp keeps saying when the state last moved rather than when a
// reconcile last ran.
func setCondition(conditions *[]metav1.Condition, condition metav1.Condition) {
	now := metav1.Now()
	for i := range *conditions {
		if (*conditions)[i].Type != condition.Type {
			continue
		}
		if (*conditions)[i].Status != condition.Status {
			condition.LastTransitionTime = now
		} else {
			condition.LastTransitionTime = (*conditions)[i].LastTransitionTime
		}
		(*conditions)[i] = condition
		return
	}
	condition.LastTransitionTime = now
	*conditions = append(*conditions, condition)
}
