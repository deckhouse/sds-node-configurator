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

	"github.com/deckhouse/sds-node-configurator/lib/go/common/pkg/feature"
	"github.com/slok/kubewebhook/v2/pkg/model"
	kwhvalidating "github.com/slok/kubewebhook/v2/pkg/webhook/validating"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func LLVSValidate(_ context.Context, _ *model.AdmissionReview, _ metav1.Object) (*kwhvalidating.ValidatorResult, error) {
	if !feature.SnapshotsEnabled() {
		return &kwhvalidating.ValidatorResult{
			Valid:   false,
			Message: "LVMLogicalVolumeSnapshot is not available in this Deckhouse edition.",
		}, nil
	}
	return &kwhvalidating.ValidatorResult{
		Valid: true,
	}, nil
}
