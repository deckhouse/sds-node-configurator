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

// Package lsllva activates the volumes of a shared pool on this node, and
// deactivates them when their attachment goes away.
//
// The attachment resource is the whole protocol. Its existence means "this
// volume belongs on this node", its deletion means "let it go", and the
// exclusive activation LVM performs underneath is what actually keeps a second
// node out — not any bookkeeping here. That is why deletion runs through a
// finalizer: an attachment that disappeared without deactivating would leave
// the volume active with nothing left to say so, and the next node to want it
// would wait for a lock whose owner no longer has a reason to release it.
package lsllva

import (
	"context"
	"fmt"
	"path/filepath"
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

const ReconcilerName = "lvm-shared-logical-volume-attachment-controller"

const (
	PhasePending  = "Pending"
	PhaseAttached = "Attached"
	PhaseFailed   = "Failed"

	ConditionLockAcquired = "LockAcquired"
	ConditionActivated    = "Activated"
	ConditionReady        = "Ready"
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

func (r *Reconciler) ShouldReconcileCreate(obj *v1alpha1.LVMSharedLogicalVolumeAttachment) bool {
	return r.isOurs(obj)
}

func (r *Reconciler) ShouldReconcileUpdate(_, objectNew *v1alpha1.LVMSharedLogicalVolumeAttachment) bool {
	return r.isOurs(objectNew)
}

func (r *Reconciler) isOurs(obj *v1alpha1.LVMSharedLogicalVolumeAttachment) bool {
	return obj.Spec.NodeName == r.cfg.NodeName
}

func (r *Reconciler) Reconcile(
	ctx context.Context,
	req controller.ReconcileRequest[*v1alpha1.LVMSharedLogicalVolumeAttachment],
) (controller.Result, error) {
	attachment := req.Object

	if !r.isOurs(attachment) {
		return controller.Result{}, nil
	}

	if attachment.DeletionTimestamp != nil {
		return r.detach(ctx, attachment)
	}

	return r.attach(ctx, attachment)
}

func (r *Reconciler) attach(
	ctx context.Context,
	attachment *v1alpha1.LVMSharedLogicalVolumeAttachment,
) (controller.Result, error) {
	volume, group, err := r.resolve(ctx, attachment)
	if err != nil {
		return controller.Result{}, err
	}
	if volume == nil || group == nil {
		// The volume has not been created yet by the metadata owner. Ordinary
		// on a fresh PersistentVolumeClaim, not a failure.
		return controller.Result{RequeueAfter: 5 * time.Second}, nil
	}

	if err := r.addFinalizer(ctx, attachment); err != nil {
		return controller.Result{}, err
	}

	vgName := group.Spec.ActualVGNameOnTheNode
	lvName := volume.Spec.ActualLVNameOnTheNode
	shared := attachment.Spec.AccessMode == v1alpha1.LVMSharedLogicalVolumeAccessModeRWX

	if !r.isActive(vgName, lvName) {
		// Everything this node owes in the same group and the same mode goes in
		// one command, not just the volume this reconcile is about.
		//
		// A batch takes the group lock once. Measured on a 32-node pool: sixteen
		// volumes in one command give 68 activations per second against 13 for a
		// loop over the same sixteen, and the lock — not the disk — is where the
		// difference comes from. Mass events are the reason this matters: a node
		// coming back after a reboot has every one of its attachments fire at
		// once, and one command for the burst turns a minute into seconds. The
		// reconciles that follow find their volumes already active and do
		// nothing at all.
		batch := r.pendingIn(ctx, group, lvName, shared)

		r.log.Info(fmt.Sprintf("[%s] activating %d volume(s) of %s (shared=%t): %s",
			ReconcilerName, len(batch), vgName, shared, strings.Join(batch, ", ")))
		cmd, err := r.commands.LVActivateShared(ctx, vgName, batch, shared)
		if err != nil {
			// The ordinary reason is that another node still holds the lock,
			// which is a state of the pool rather than a fault of this node —
			// the volume is being moved and the previous holder has not let go.
			r.log.Warning(fmt.Sprintf("[%s] unable to activate %s/%s (cmd: %s): %s",
				ReconcilerName, vgName, lvName, cmd, err.Error()))
			r.setStatus(ctx, attachment, PhasePending, "", false,
				"LockNotAcquired", fmt.Sprintf("cannot activate %s/%s yet: %s", vgName, lvName, err.Error()))
			return controller.Result{RequeueAfter: 10 * time.Second}, nil
		}

		// One command, one exit code — so the state is read back rather than
		// inferred from the fact that the command returned zero.
		if !r.isActive(vgName, lvName) {
			r.setStatus(ctx, attachment, PhasePending, "", false,
				"NotActiveYet", fmt.Sprintf("%s/%s is not active after the activation command", vgName, lvName))
			return controller.Result{RequeueAfter: 5 * time.Second}, nil
		}
	}

	devicePath := filepath.Join("/dev", vgName, lvName)
	r.setStatus(ctx, attachment, PhaseAttached, devicePath, true, "Activated",
		fmt.Sprintf("%s is active on %s", devicePath, r.cfg.NodeName))

	return controller.Result{}, nil
}

// detach releases the volume and only then lets the object go. The order is the
// point: the finalizer is removed after the deactivation succeeded, so an
// attachment that is gone from the API is a volume that is gone from this node.
func (r *Reconciler) detach(
	ctx context.Context,
	attachment *v1alpha1.LVMSharedLogicalVolumeAttachment,
) (controller.Result, error) {
	if !slices.Contains(attachment.Finalizers, internal.SdsNodeConfiguratorFinalizer) {
		return controller.Result{}, nil
	}

	volume, group, err := r.resolve(ctx, attachment)
	if err != nil {
		return controller.Result{}, err
	}

	if volume != nil && group != nil {
		vgName := group.Spec.ActualVGNameOnTheNode
		lvName := volume.Spec.ActualLVNameOnTheNode

		if r.isActive(vgName, lvName) {
			cmd, err := r.commands.LVDeactivateShared(ctx, vgName, []string{lvName})
			if err != nil {
				// A busy volume — something still has it open — must not let the
				// object go: the lock would stay held with nothing describing it.
				r.log.Warning(fmt.Sprintf("[%s] unable to deactivate %s/%s (cmd: %s): %s",
					ReconcilerName, vgName, lvName, cmd, err.Error()))
				return controller.Result{RequeueAfter: 10 * time.Second}, nil
			}
			if r.isActive(vgName, lvName) {
				return controller.Result{RequeueAfter: 5 * time.Second}, nil
			}
		}
	}
	// A volume or group that no longer exists means there is nothing to hold on
	// to: releasing the object is then the only thing left to do.

	return controller.Result{}, r.removeFinalizer(ctx, attachment)
}

// pendingIn collects the volumes this node has attachments for in one group and
// one activation mode, and which are not active yet. The volume of the current
// reconcile is always first, so a batch that lvm truncates for any reason still
// contains the one the caller is about to check.
//
// Listing is deliberately done against the API rather than kept as local state:
// the alternative is a cache of intentions that can disagree with the objects,
// and disagreement here means activating a volume nobody asked for.
func (r *Reconciler) pendingIn(
	ctx context.Context,
	group *v1alpha1.LVMSharedVolumeGroup,
	lvName string,
	shared bool,
) []string {
	batch := []string{lvName}

	attachments := &v1alpha1.LVMSharedLogicalVolumeAttachmentList{}
	if err := r.cl.List(ctx, attachments); err != nil {
		// One volume is still correct, only slower.
		r.log.Error(err, fmt.Sprintf("[%s] unable to list attachments, activating one volume at a time", ReconcilerName))
		return batch
	}

	for i := range attachments.Items {
		other := &attachments.Items[i]
		if other.Spec.NodeName != r.cfg.NodeName || other.DeletionTimestamp != nil {
			continue
		}
		if (other.Spec.AccessMode == v1alpha1.LVMSharedLogicalVolumeAccessModeRWX) != shared {
			continue
		}

		volume, otherGroup, err := r.resolve(ctx, other)
		if err != nil || volume == nil || otherGroup == nil {
			continue
		}
		if otherGroup.Name != group.Name {
			continue
		}

		name := volume.Spec.ActualLVNameOnTheNode
		if name == lvName || slices.Contains(batch, name) || r.isActive(group.Spec.ActualVGNameOnTheNode, name) {
			continue
		}
		batch = append(batch, name)
	}

	return batch
}

// resolve returns the volume and its group, or nils when either is not there.
func (r *Reconciler) resolve(
	ctx context.Context,
	attachment *v1alpha1.LVMSharedLogicalVolumeAttachment,
) (*v1alpha1.LVMSharedLogicalVolume, *v1alpha1.LVMSharedVolumeGroup, error) {
	volume := &v1alpha1.LVMSharedLogicalVolume{}
	if err := r.cl.Get(ctx, client.ObjectKey{Name: attachment.Spec.LVMSharedLogicalVolumeName}, volume); err != nil {
		if errors.IsNotFound(err) {
			return nil, nil, nil
		}
		return nil, nil, fmt.Errorf("get LVMSharedLogicalVolume %s: %w", attachment.Spec.LVMSharedLogicalVolumeName, err)
	}

	group := &v1alpha1.LVMSharedVolumeGroup{}
	if err := r.cl.Get(ctx, client.ObjectKey{Name: volume.Spec.LVMSharedVolumeGroupName}, group); err != nil {
		if errors.IsNotFound(err) {
			return nil, nil, nil
		}
		return nil, nil, fmt.Errorf("get LVMSharedVolumeGroup %s: %w", volume.Spec.LVMSharedVolumeGroupName, err)
	}

	return volume, group, nil
}

// isActive reads the node's own view of device-mapper rather than asking lvm.
// Every lvm command against a shared group takes the group lock, and the lock
// is the pool's scarcest resource: its throughput is flat at 13-21 operations
// per second no matter how many nodes there are.
func (r *Reconciler) isActive(vgName, lvName string) bool {
	lvs, _ := r.sdsCache.GetLVs()
	for _, lv := range lvs {
		if lv.VGName == vgName && lv.LVName == lvName {
			return len(lv.LVAttr) > 4 && lv.LVAttr[4] == 'a'
		}
	}
	return false
}

func (r *Reconciler) addFinalizer(ctx context.Context, attachment *v1alpha1.LVMSharedLogicalVolumeAttachment) error {
	if slices.Contains(attachment.Finalizers, internal.SdsNodeConfiguratorFinalizer) {
		return nil
	}

	patch := client.MergeFrom(attachment.DeepCopy())
	attachment.Finalizers = append(attachment.Finalizers, internal.SdsNodeConfiguratorFinalizer)
	if err := r.cl.Patch(ctx, attachment, patch); err != nil {
		return fmt.Errorf("add finalizer to %s: %w", attachment.Name, err)
	}
	return nil
}

func (r *Reconciler) removeFinalizer(ctx context.Context, attachment *v1alpha1.LVMSharedLogicalVolumeAttachment) error {
	patch := client.MergeFrom(attachment.DeepCopy())
	attachment.Finalizers = slices.DeleteFunc(attachment.Finalizers, func(f string) bool {
		return f == internal.SdsNodeConfiguratorFinalizer
	})
	if err := r.cl.Patch(ctx, attachment, patch); err != nil {
		return fmt.Errorf("remove finalizer from %s: %w", attachment.Name, err)
	}
	return nil
}

// setStatus is best-effort on purpose: a status that could not be written must
// not undo an activation that succeeded. The failure is logged and the next
// reconcile writes it again.
func (r *Reconciler) setStatus(
	ctx context.Context,
	attachment *v1alpha1.LVMSharedLogicalVolumeAttachment,
	phase, devicePath string,
	ready bool,
	reason, message string,
) {
	patch := client.MergeFrom(attachment.DeepCopy())

	if attachment.Status == nil {
		attachment.Status = &v1alpha1.LVMSharedLogicalVolumeAttachmentStatus{}
	}
	attachment.Status.Phase = phase
	attachment.Status.DevicePath = devicePath
	attachment.Status.ObservedGeneration = attachment.Generation

	status := metav1.ConditionFalse
	if ready {
		status = metav1.ConditionTrue
	}
	for _, conditionType := range []string{ConditionLockAcquired, ConditionActivated, ConditionReady} {
		setCondition(&attachment.Status.Conditions, metav1.Condition{
			Type:               conditionType,
			Status:             status,
			ObservedGeneration: attachment.Generation,
			Reason:             reason,
			Message:            message,
		})
	}

	if err := r.cl.Status().Patch(ctx, attachment, patch); err != nil {
		r.log.Error(err, fmt.Sprintf("[%s] unable to update the status of %s", ReconcilerName, attachment.Name))
	}
}

// setCondition keeps lastTransitionTime honest: it is bumped when the status
// changes and left alone when only the reason or the message does, because a
// timestamp that moves on every reconcile says nothing about when the state
// last actually changed.
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
