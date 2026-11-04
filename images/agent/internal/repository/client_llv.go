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

package repository

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-common-lib/conditions"
	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
)

// LLVClient writes the status of LVMLogicalVolumes. A write that moves Ready
// records an Event on the volume through recorder, which may be nil.
type LLVClient struct {
	cl       client.Client
	log      logger.Logger
	recorder conditions.EventRecorder
}

func NewLLVClient(
	cl client.Client,
	log logger.Logger,
	recorder conditions.EventRecorder,
) *LLVClient {
	return &LLVClient{
		cl:       cl,
		log:      log,
		recorder: recorder,
	}
}

func (llvCl *LLVClient) UpdatePhaseIfNeeded(
	ctx context.Context,
	llv *v1alpha1.LVMLogicalVolume,
	phase string,
	reason string,
) error {
	log := llvCl.log

	log.Debug(fmt.Sprintf("tries to update the LVMLogicalVolume %s status with phase: %s, reason: %s", llv.Name, phase, reason))

	// The state UpdateStatus read and wrote, kept so the server-side values can be
	// mirrored onto the caller's object below.
	var written *v1alpha1.LVMLogicalVolume

	err := conditions.UpdateStatus(ctx, llvCl.cl, llv, func(fresh *v1alpha1.LVMLogicalVolume) {
		written = fresh
		if fresh.Status == nil {
			fresh.Status = new(v1alpha1.LVMLogicalVolumeStatus)
		}
		fresh.Status.Phase = phase
		fresh.Status.Reason = reason
		fresh.Status.ObservedGeneration = fresh.Generation
		conditions.Set(&fresh.Status.Conditions, internal.ReadyConditionForPhase(fresh.Generation, phase, reason, "LVMLogicalVolume"))
	}, conditions.WithEvents(llvCl.recorder, internal.LLVConditions))
	if err != nil {
		return err
	}

	llv.Status = written.Status
	llv.ResourceVersion = written.ResourceVersion

	log.Debug(fmt.Sprintf("updated LVMLogicalVolume %s status.phase to %s and reason to %s", llv.Name, phase, reason))
	return nil
}

func (llvCl *LLVClient) UpdatePhaseToCreatedIfNeeded(
	ctx context.Context,
	llv *v1alpha1.LVMLogicalVolume,
	lvg *v1alpha1.LVMVolumeGroup,
	actualSize resource.Quantity,
) error {
	log := llvCl.log

	var contiguous *bool
	if llv.Spec.Thick != nil && llv.Spec.Thick.Contiguous != nil {
		if *llv.Spec.Thick.Contiguous {
			contiguous = llv.Spec.Thick.Contiguous
		}
	}

	// The state UpdateStatus read and wrote, kept so the server-side values can be
	// mirrored onto the caller's object below.
	var written *v1alpha1.LVMLogicalVolume

	err := conditions.UpdateStatus(ctx, llvCl.cl, llv, func(fresh *v1alpha1.LVMLogicalVolume) {
		written = fresh
		// Status is a pointer and is absent until something writes it. The
		// comparison this replaces dereferenced it unconditionally.
		if fresh.Status == nil {
			fresh.Status = new(v1alpha1.LVMLogicalVolumeStatus)
		}
		fresh.Status.Phase = v1alpha1.PhaseCreated
		fresh.Status.Reason = ""
		fresh.Status.ActualSize = actualSize
		fresh.Status.Contiguous = contiguous
		setLocation(fresh.Status, lvg)
		fresh.Status.ObservedGeneration = fresh.Generation
		conditions.Set(&fresh.Status.Conditions,
			internal.ReadyConditionForPhase(fresh.Generation, v1alpha1.PhaseCreated, "", "LVMLogicalVolume"))
	}, conditions.WithEvents(llvCl.recorder, internal.LLVConditions))
	if err != nil {
		log.Error(fmt.Sprintf("unable to update the LVMLogicalVolume %s", llv.Name), logger.Err(err))
		return err
	}

	llv.Status = written.Status
	llv.ResourceVersion = written.ResourceVersion

	log.Info(fmt.Sprintf("the LVMLogicalVolume %s was successfully updated", llv.Name))
	return nil
}

// UpdateLocationIfNeeded records the node and the Volume Group of a volume
// created before the status carried them.
func (llvCl *LLVClient) UpdateLocationIfNeeded(
	ctx context.Context,
	llv *v1alpha1.LVMLogicalVolume,
	lvg *v1alpha1.LVMVolumeGroup,
) error {
	if llv.Status != nil &&
		llv.Status.NodeName == lvg.Spec.Local.NodeName &&
		llv.Status.ActualVGNameOnTheNode == lvg.Spec.ActualVGNameOnTheNode {
		return nil
	}

	var written *v1alpha1.LVMLogicalVolume

	err := conditions.UpdateStatus(ctx, llvCl.cl, llv, func(fresh *v1alpha1.LVMLogicalVolume) {
		written = fresh
		if fresh.Status == nil {
			fresh.Status = new(v1alpha1.LVMLogicalVolumeStatus)
		}
		setLocation(fresh.Status, lvg)
	})
	if err != nil {
		return err
	}

	llv.Status = written.Status
	llv.ResourceVersion = written.ResourceVersion

	llvCl.log.Debug(fmt.Sprintf("recorded node %s and VG %s in the LVMLogicalVolume %s status", lvg.Spec.Local.NodeName, lvg.Spec.ActualVGNameOnTheNode, llv.Name))
	return nil
}

func setLocation(status *v1alpha1.LVMLogicalVolumeStatus, lvg *v1alpha1.LVMVolumeGroup) {
	status.NodeName = lvg.Spec.Local.NodeName
	status.ActualVGNameOnTheNode = lvg.Spec.ActualVGNameOnTheNode
}

// SetDeletingIfNeeded reports, as Ready=False with the reason Deleting, a volume
// whose deletion is held by the finalizers of other controllers. The phase is
// left alone: it still describes the Logical Volume, which is in place. It
// returns whether anything was written, so the caller can log the wait when it
// starts or changes rather than on every pass.
func (llvCl *LLVClient) SetDeletingIfNeeded(
	ctx context.Context,
	llv *v1alpha1.LVMLogicalVolume,
	message string,
) (bool, error) {
	message = conditions.TruncateMessage(message)

	if llv.Status != nil {
		if ready := conditions.Get(llv.Status.Conditions, conditions.TypeReady); ready != nil &&
			ready.Status == metav1.ConditionFalse &&
			ready.Reason == conditions.ReasonDeleting &&
			ready.Message == message {
			return false, nil
		}
	}

	var (
		written *v1alpha1.LVMLogicalVolume
		// The cached llv can lag behind the server, which may already carry
		// this condition: UpdateStatus then writes nothing, and neither should
		// the caller log a change.
		changed bool
	)

	err := conditions.UpdateStatus(ctx, llvCl.cl, llv, func(fresh *v1alpha1.LVMLogicalVolume) {
		written = fresh
		if fresh.Status == nil {
			fresh.Status = new(v1alpha1.LVMLogicalVolumeStatus)
		}
		changed = conditions.Set(&fresh.Status.Conditions, metav1.Condition{
			Type:               conditions.TypeReady,
			Status:             metav1.ConditionFalse,
			Reason:             conditions.ReasonDeleting,
			Message:            message,
			ObservedGeneration: fresh.Generation,
		})
	}, conditions.WithEvents(llvCl.recorder, internal.LLVConditions))
	if err != nil {
		return false, err
	}

	llv.Status = written.Status
	llv.ResourceVersion = written.ResourceVersion
	return changed, nil
}
