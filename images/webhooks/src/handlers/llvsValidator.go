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

package handlers

import (
	"context"

	snc "github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	commonvalidating "github.com/deckhouse/sds-node-configurator/lib/go/common/pkg/validating"
	"github.com/slok/kubewebhook/v2/pkg/model"
	kwhvalidating "github.com/slok/kubewebhook/v2/pkg/webhook/validating"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
)

func LLVSValidate(ctx context.Context, arReview *model.AdmissionReview, obj metav1.Object) (*kwhvalidating.ValidatorResult, error) {
	if arReview.Operation == model.OperationDelete {
		return &kwhvalidating.ValidatorResult{
			Valid: true,
		}, nil
	}

	llvs, ok := obj.(*snc.LVMLogicalVolumeSnapshot)
	if !ok {
		return &kwhvalidating.ValidatorResult{}, nil
	}

	cl, err := NewKubeClient("")
	if err != nil {
		klog.Fatal(err) // pod restarting
	}

	llv := &snc.LVMLogicalVolume{}
	msg, err := commonvalidating.ValidateLVMLogicalVolumeSnapshot(ctx, cl, llvs, llv)
	if err != nil {
		klog.ErrorS(err, "LLVS validation failed with error")
		return &kwhvalidating.ValidatorResult{
			Valid:   false,
			Message: err.Error(),
		}, nil
	}
	if msg != "" {
		return &kwhvalidating.ValidatorResult{
			Valid:   false,
			Message: msg,
		}, nil
	}

	return &kwhvalidating.ValidatorResult{
		Valid: true,
	}, nil
}
