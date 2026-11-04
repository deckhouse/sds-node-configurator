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
	"fmt"
	"strings"

	"github.com/slok/kubewebhook/v2/pkg/model"
	kwhvalidating "github.com/slok/kubewebhook/v2/pkg/webhook/validating"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cn "github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/lib/go/common/pkg/feature"
)

// maxLVNameLength is the longest logical volume name LVM accepts.
const maxLVNameLength = 127

// reservedLVNamePrefixes are the prefixes LVM refuses in a logical volume name.
var reservedLVNamePrefixes = []string{"snapshot", "pvmove"}

func LLVSValidate(_ context.Context, ar *model.AdmissionReview, obj metav1.Object) (*kwhvalidating.ValidatorResult, error) {
	if !feature.SnapshotsEnabled() {
		return &kwhvalidating.ValidatorResult{
			Valid:   false,
			Message: "LVMLogicalVolumeSnapshot is not available in this Deckhouse edition.",
		}, nil
	}

	if ar.Operation == model.OperationCreate {
		if llvs, ok := obj.(*cn.LVMLogicalVolumeSnapshot); ok {
			if problem := llvsObjectNameProblem(llvs); problem != "" {
				return &kwhvalidating.ValidatorResult{Valid: false, Message: problem}, nil
			}
		}
	}

	return &kwhvalidating.ValidatorResult{
		Valid: true,
	}, nil
}

// llvsObjectNameProblem says why LVM would refuse the object name as the name
// of the snapshot, which the agent uses when spec.actualSnapshotNameOnTheNode
// is empty, or returns "".
//
// The CRD checks spec.actualSnapshotNameOnTheNode itself. The object name can
// only be reached by a rule at the root of the schema, which the API server
// evaluates on every update, so it would also refuse to remove the finalizer of
// a snapshot created before; here it is checked on CREATE only. An object name
// cannot start with a hyphen, be "." or "..", or contain an underscore, so of the
// LVM rules only the length and the reserved prefixes are left.
func llvsObjectNameProblem(llvs *cn.LVMLogicalVolumeSnapshot) string {
	if llvs.Spec.ActualSnapshotNameOnTheNode != "" {
		return ""
	}

	if len(llvs.Name) > maxLVNameLength {
		return fmt.Sprintf("spec.actualSnapshotNameOnTheNode is empty, so the snapshot would be named after the LVMLogicalVolumeSnapshot, and LVM does not accept names longer than %d characters; set spec.actualSnapshotNameOnTheNode", maxLVNameLength)
	}

	for _, prefix := range reservedLVNamePrefixes {
		if strings.HasPrefix(llvs.Name, prefix) {
			return fmt.Sprintf("spec.actualSnapshotNameOnTheNode is empty, so the snapshot would be named after the LVMLogicalVolumeSnapshot, and LVM reserves names that start with %s; set spec.actualSnapshotNameOnTheNode or choose another name", prefix)
		}
	}

	return ""
}
