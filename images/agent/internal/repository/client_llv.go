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
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-common-lib/conditions"
	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/logger"
)

type LLVClient struct {
	cl  client.Client
	log logger.Logger
}

func NewLLVClient(
	cl client.Client,
	log logger.Logger,
) *LLVClient {
	return &LLVClient{
		cl:  cl,
		log: log,
	}
}

func (llvCl *LLVClient) UpdatePhaseIfNeeded(
	ctx context.Context,
	llv *v1alpha1.LVMLogicalVolume,
	phase string,
	reason string,
) error {
	llvCl.log.Debug(fmt.Sprintf("[UpdatePhaseIfNeeded] tries to update the LVMLogicalVolume %s status with phase: %s, reason: %s", llv.Name, phase, reason))

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
	})
	if err != nil {
		return err
	}

	llv.Status = written.Status
	llv.ResourceVersion = written.ResourceVersion

	llvCl.log.Debug(fmt.Sprintf("[UpdatePhaseIfNeeded] updated LVMLogicalVolume %s status.phase to %s and reason to %s", llv.Name, phase, reason))
	return nil
}

func (llvCl *LLVClient) UpdatePhaseToCreatedIfNeeded(
	ctx context.Context,
	llv *v1alpha1.LVMLogicalVolume,
	actualSize resource.Quantity,
) error {
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
		fresh.Status.ObservedGeneration = fresh.Generation
		conditions.Set(&fresh.Status.Conditions,
			internal.ReadyConditionForPhase(fresh.Generation, v1alpha1.PhaseCreated, "", "LVMLogicalVolume"))
	})
	if err != nil {
		llvCl.log.Error(err, fmt.Sprintf("[UpdatePhaseToCreatedIfNeeded] unable to update the LVMLogicalVolume %s", llv.Name))
		return err
	}

	llv.Status = written.Status
	llv.ResourceVersion = written.ResourceVersion

	llvCl.log.Info(fmt.Sprintf("[UpdatePhaseToCreatedIfNeeded] the LVMLogicalVolume %s was successfully updated", llv.Name))
	return nil
}
