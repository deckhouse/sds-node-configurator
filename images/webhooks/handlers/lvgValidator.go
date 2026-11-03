/*
Copyright 2026 Flant JSC

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
	"fmt"

	"github.com/slok/kubewebhook/v2/pkg/model"
	kwhvalidating "github.com/slok/kubewebhook/v2/pkg/webhook/validating"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-common-lib/logger"
	cn "github.com/deckhouse/sds-node-configurator/api/v1alpha1"
)

// LVGValidate rejects an LVMVolumeGroup created for a Volume Group of a node that
// another LVMVolumeGroup already describes. Two resources over one Volume Group
// both report it Ready, the scheduler counts its space twice, and the agent
// retags the Volume Group back and forth between them.
//
// The fields compared are immutable, so only CREATE is checked. A failed list
// lets the request through: the agent refuses to serve the second resource on
// its own, and creating LVMVolumeGroups must not depend on this list succeeding.
func LVGValidate(cl client.Reader, log logger.Logger) func(context.Context, *model.AdmissionReview, metav1.Object) (*kwhvalidating.ValidatorResult, error) {
	return func(ctx context.Context, ar *model.AdmissionReview, obj metav1.Object) (*kwhvalidating.ValidatorResult, error) {
		if ar.Operation != model.OperationCreate {
			return &kwhvalidating.ValidatorResult{Valid: true}, nil
		}

		lvg, ok := obj.(*cn.LVMVolumeGroup)
		if !ok {
			return &kwhvalidating.ValidatorResult{Valid: true}, nil
		}

		nodeName := lvg.Spec.Local.NodeName
		vgName := lvg.Spec.ActualVGNameOnTheNode
		if nodeName == "" || vgName == "" {
			return &kwhvalidating.ValidatorResult{Valid: true}, nil
		}

		var lvgs cn.LVMVolumeGroupList
		if err := cl.List(ctx, &lvgs); err != nil {
			log.Warn("unable to list LVMVolumeGroups, allowing the request", "lvg", lvg.Name, "error", err.Error())
			return &kwhvalidating.ValidatorResult{Valid: true}, nil
		}

		for i := range lvgs.Items {
			other := &lvgs.Items[i]
			if other.Name == lvg.Name {
				continue
			}
			if other.Spec.Local.NodeName == nodeName && other.Spec.ActualVGNameOnTheNode == vgName {
				return &kwhvalidating.ValidatorResult{
					Valid: false,
					Message: fmt.Sprintf("the Volume Group %s on the node %s is already described by the LVMVolumeGroup %s; "+
						"edit that resource instead of creating another one", vgName, nodeName, other.Name),
				}, nil
			}
		}

		return &kwhvalidating.ValidatorResult{Valid: true}, nil
	}
}
