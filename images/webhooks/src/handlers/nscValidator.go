package handlers

import (
	"context"
	"fmt"
	cn "github.com/deckhouse/csi-nfs/api/v1alpha1"
	"github.com/slok/kubewebhook/v2/pkg/model"
	kwhvalidating "github.com/slok/kubewebhook/v2/pkg/webhook/validating"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	mc "webhooks/api"
)

const (
	csiNfsModuleName = "csi-nfs"
)

func NSCValidate(ctx context.Context, arReview *model.AdmissionReview, obj metav1.Object) (*kwhvalidating.ValidatorResult, error) {
	nsc, ok := obj.(*cn.NFSStorageClass)
	if !ok {
		// If not a storage class just continue the validation chain(if there is one) and do nothing.
		return &kwhvalidating.ValidatorResult{}, nil
	}

	if arReview.UserInfo.Username == allowedUserName {
		klog.Infof("User %s is allowed to manage NFS storage classes", arReview.UserInfo.Username)
		return &kwhvalidating.ValidatorResult{Valid: true}, nil
	}

	v3presents := false
	v3enabled := false

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

	err = cl.Get(ctx, types.NamespacedName{Name: csiNfsModuleName, Namespace: ""}, nfsModuleConfig)
	if err != nil {
		klog.Fatal(err)
	}

	if value, exists := nfsModuleConfig.Spec.Settings["v3support"]; exists && value == true {
		v3enabled = true
	} else {
		v3enabled = false
	}

	klog.Infof("NFSv3 support enabled: %t", v3enabled)

	if v3presents && !v3enabled {
		klog.Info("NFS v3 is not enabled in module config, enable it first")

		return &kwhvalidating.ValidatorResult{Valid: false, Message: fmt.Sprint("NFS v3 is not enabled in module config, enable it first")}, err
	} else if !v3presents && v3enabled {
		klog.Info("NFS v3 is enabled in module config, but not used in NFSStorageCLass - disable it first")
		return &kwhvalidating.ValidatorResult{Valid: false, Message: fmt.Sprint("NFS v3 is enabled in module config, but not used in NFSStorageCLass - disable it first")}, err
	}

	return &kwhvalidating.ValidatorResult{Valid: true},
		nil
}

//
