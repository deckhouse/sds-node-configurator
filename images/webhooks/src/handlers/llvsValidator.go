package handlers

import (
	"context"
	cn "github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/slok/kubewebhook/v2/pkg/model"
	kwhvalidating "github.com/slok/kubewebhook/v2/pkg/webhook/validating"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	mc "webhooks/api"
)

const (
	sdsNodeConfiguratorModuleName = "sds-node-configurator"
)

func LLVSValidate(ctx context.Context, arReview *model.AdmissionReview, obj metav1.Object) (*kwhvalidating.ValidatorResult, error) {
	llvs, ok := obj.(*cn.)
	if !ok {
		// If not a storage class just continue the validation chain(if there is one) and do nothing.
		return &kwhvalidating.ValidatorResult{}, nil
	}

	if arReview.UserInfo.Username == allowedUserName {
		klog.Infof("User %s is allowed to manage NFS storage classes", arReview.UserInfo.Username)
		return &kwhvalidating.ValidatorResult{Valid: true}, nil
	}

	cl, err := NewKubeClient("")
	if err != nil {
		klog.Fatal(err)
	}

	listClasses := &cn.NFSStorageClassList{}
	err = cl.List(ctx, listClasses)

	if nsc.ObjectMeta.DeletionTimestamp == nil && arReview.Operation != "delete" && nsc.Spec.Connection.NFSVersion == "3" {
		v3presents = true
	}

	klog.Infof("NFSv3 NFSStorageClass exists: %t", v3presents)

	nfsModuleConfig := &mc.ModuleConfig{}

	err = cl.Get(ctx, types.NamespacedName{Name: sdsNodeConfiguratorModuleName, Namespace: ""}, nfsModuleConfig)
	if err != nil {
		klog.Fatal(err)
	}

	return &kwhvalidating.ValidatorResult{Valid: true},
		nil
}

//
