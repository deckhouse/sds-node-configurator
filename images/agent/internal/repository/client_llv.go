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

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
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
	if llv.Status != nil &&
		llv.Status.Phase == phase &&
		llv.Status.Reason == reason {
		llvCl.log.Debug(fmt.Sprintf("[updateLVMLogicalVolumePhaseIfNeeded] no need to update the LVMLogicalVolume %s phase and reason", llv.Name))
		return nil
	}

	if llv.Status == nil {
		llv.Status = new(v1alpha1.LVMLogicalVolumeStatus)
	}

	llv.Status.Phase = phase
	llv.Status.Reason = reason

	llvCl.log.Debug(fmt.Sprintf("[updateLVMLogicalVolumePhaseIfNeeded] tries to update the LVMLogicalVolume %s status with phase: %s, reason: %s", llv.Name, phase, reason))
	err := llvCl.cl.Status().Update(ctx, llv)
	if err != nil {
		return err
	}

	llvCl.log.Debug(fmt.Sprintf("[updateLVMLogicalVolumePhaseIfNeeded] updated LVMLogicalVolume %s status.phase to %s and reason to %s", llv.Name, phase, reason))
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

	updateNeeded := llv.Status.Phase != v1alpha1.PhaseCreated ||
		llv.Status.ActualSize.Value() != actualSize.Value() ||
		llv.Status.Reason != "" ||
		llv.Status.Contiguous != contiguous

	if !updateNeeded {
		llvCl.log.Info(fmt.Sprintf("[UpdatePhaseToCreatedIfNeeded] no need to update the LVMLogicalVolume %s", llv.Name))
		return nil
	}

	llv.Status.Phase = v1alpha1.PhaseCreated
	llv.Status.Reason = ""
	llv.Status.ActualSize = actualSize
	llv.Status.Contiguous = contiguous
	err := llvCl.cl.Status().Update(ctx, llv)
	if err != nil {
		llvCl.log.Error(err, fmt.Sprintf("[UpdatePhaseToCreatedIfNeeded] unable to update the LVMLogicalVolume %s", llv.Name))
		return err
	}

	llvCl.log.Info(fmt.Sprintf("[UpdatePhaseToCreatedIfNeeded] the LVMLogicalVolume %s was successfully updated", llv.Name))
	return nil
}
