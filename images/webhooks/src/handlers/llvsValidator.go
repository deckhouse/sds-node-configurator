package handlers

import (
	"context"

	"github.com/slok/kubewebhook/v2/pkg/model"
	kwhvalidating "github.com/slok/kubewebhook/v2/pkg/webhook/validating"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func LLVSValidate(_ context.Context, _ *model.AdmissionReview, _ metav1.Object) (*kwhvalidating.ValidatorResult, error) {
	return &kwhvalidating.ValidatorResult{
		Valid:   false,
		Message: "LVMLogicalVolumeSnapshot is not available in this Deckhouse edition.",
	}, nil
}
