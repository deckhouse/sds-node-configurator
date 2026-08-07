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

// Package hooks_common removes CRDs the module no longer ships.
//
// Deckhouse applies crds/*.yaml on every converge but never deletes a CRD that disappears
// from the folder, so dropping a file leaves the CRD (and any objects of that kind) in every
// upgraded cluster forever. This hook is the deletion half of such a removal.
//
// Currently it removes lvmvolumegroupbackups.storage.deckhouse.io, a scratch space for the
// one-time LvmVolumeGroup -> LVMVolumeGroup kind migration. The Python hook that created it
// was dropped in April 2025; nothing has read or written those objects since, and the CRD is
// no longer in crds/. The migration copied the LVG finalizer onto every backup it created, and
// no controller removes it anymore, so a plain CRD delete would hang in Terminating: the
// leftovers must be stripped of finalizers first.
//
// The hook runs OnBeforeHelm and is idempotent: when the CRD is absent (fresh install, or a
// previous converge already removed it) it is a no-op.
//
// It deliberately never fails the converge. Everything it touches is vestigial, so a
// transient API error must not block the module release — the failure is logged and the next
// beforeHelm retries. The CRD is deleted only after every leftover was successfully stripped,
// which keeps that retry meaningful instead of leaving a half-deleted CRD behind.
package hooks_common

import (
	"context"
	"fmt"

	extv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/module-sdk/pkg"
	"github.com/deckhouse/module-sdk/pkg/registry"
	"github.com/deckhouse/sds-common-lib/kubeclient"
	"github.com/deckhouse/sds-node-configurator/hooks/go/consts"
)

const removeObsoleteCRDsHookName = "remove-obsolete-crds"

// obsoleteCRD is a CRD the module used to ship and now deletes.
type obsoleteCRD struct {
	// name is the CRD resource name, e.g. "lvmvolumegroupbackups.storage.deckhouse.io".
	name string
	// gvk addresses the objects of that CRD, so their finalizers can be stripped before the
	// CRD is deleted. Kind is case-sensitive here — see the note on
	// consts.ObsoleteLVMVolumeGroupBackupKind.
	gvk schema.GroupVersionKind
}

var obsoleteCRDs = []obsoleteCRD{
	{
		name: consts.ObsoleteLVMVolumeGroupBackupCRDName,
		gvk: schema.GroupVersionKind{
			Group:   consts.APIGroup,
			Version: "v1alpha1",
			Kind:    consts.ObsoleteLVMVolumeGroupBackupKind,
		},
	},
}

var _ = registry.RegisterFunc(configRemoveObsoleteCRDs, handlerRemoveObsoleteCRDs)

var configRemoveObsoleteCRDs = &pkg.HookConfig{
	OnBeforeHelm: &pkg.OrderedConfig{Order: 5},
}

// handlerRemoveObsoleteCRDs is the thin OnBeforeHelm wrapper: it builds the kube client and
// delegates to removeObsoleteCRDs, which holds the testable logic.
func handlerRemoveObsoleteCRDs(ctx context.Context, input *pkg.HookInput) error {
	cl, err := kubeclient.New(clientgoscheme.AddToScheme, extv1.AddToScheme)
	if err != nil {
		// Even this is not worth failing the converge over: the hook only does housekeeping.
		input.Logger.Error(fmt.Sprintf("[%s]: failed to initialize kube client, skipping: %v", removeObsoleteCRDsHookName, err))
		return nil
	}
	removeObsoleteCRDs(ctx, cl, input.Logger)
	return nil
}

// removeObsoleteCRDs deletes every CRD in obsoleteCRDs that is still present, after stripping
// finalizers from its leftover objects. It reports nothing: all failures are logged and left
// for the next converge to retry.
func removeObsoleteCRDs(ctx context.Context, cl client.Client, logger pkg.Logger) {
	for _, obsolete := range obsoleteCRDs {
		crd := &extv1.CustomResourceDefinition{}
		if err := cl.Get(ctx, client.ObjectKey{Name: obsolete.name}, crd); err != nil {
			if apierrors.IsNotFound(err) {
				// Nothing to do: fresh install, or an earlier converge already deleted it.
				continue
			}
			logger.Warn(fmt.Sprintf("[%s]: failed to get CRD %s, retrying next converge: %v", removeObsoleteCRDsHookName, obsolete.name, err))
			continue
		}

		logger.Info(fmt.Sprintf("[%s]: obsolete CRD %s found, cleaning up leftovers before deleting it", removeObsoleteCRDsHookName, obsolete.name))

		if !stripFinalizersFromLeftovers(ctx, cl, logger, obsolete) {
			// Keep the CRD until every leftover is free of finalizers, otherwise the delete
			// below would leave the CRD stuck in Terminating.
			logger.Warn(fmt.Sprintf("[%s]: keeping CRD %s, not every leftover could be stripped of finalizers, retrying next converge", removeObsoleteCRDsHookName, obsolete.name))
			continue
		}

		logger.Info(fmt.Sprintf("[%s]: deleting obsolete CRD %s", removeObsoleteCRDsHookName, obsolete.name))
		// A NotFound here means the CRD was deleted between the Get above and now (e.g. by hand);
		// that is the desired end state, so it is not a failure.
		if err := cl.Delete(ctx, crd); client.IgnoreNotFound(err) != nil {
			logger.Warn(fmt.Sprintf("[%s]: failed to delete CRD %s, retrying next converge: %v", removeObsoleteCRDsHookName, obsolete.name, err))
		}
	}
}

// stripFinalizersFromLeftovers clears the finalizers of every object of the obsolete kind and
// reports whether the CRD is safe to delete now. Finalizers are removed wholesale rather than
// selectively: the kind itself is going away, so no controller will ever process any of them.
func stripFinalizersFromLeftovers(ctx context.Context, cl client.Client, logger pkg.Logger, obsolete obsoleteCRD) bool {
	listGVK := obsolete.gvk
	listGVK.Kind += "List"

	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(listGVK)
	if err := cl.List(ctx, list); err != nil {
		if meta.IsNoMatchError(err) {
			// The CRD object exists but the apiserver serves no such kind (a half-deleted CRD,
			// or a version the CRD no longer serves). Then no object of it can be reached — or
			// created — so there is nothing to strip and the delete may proceed.
			logger.Info(fmt.Sprintf("[%s]: kind %s is not served, nothing to strip", removeObsoleteCRDsHookName, obsolete.gvk.Kind))
			return true
		}
		logger.Warn(fmt.Sprintf("[%s]: failed to list %s: %v", removeObsoleteCRDsHookName, obsolete.gvk.Kind, err))
		return false
	}

	ok := true
	for i := range list.Items {
		leftover := &list.Items[i]
		if len(leftover.GetFinalizers()) == 0 {
			continue
		}
		// No optimistic lock: nothing owns this kind anymore, so there is no concurrent writer
		// whose finalizer could be clobbered, and a Conflict retry loop would only stall the
		// cleanup. A NotFound means the object is already gone — the desired end state.
		patch := client.MergeFrom(leftover.DeepCopy())
		leftover.SetFinalizers(nil)
		if err := cl.Patch(ctx, leftover, patch); client.IgnoreNotFound(err) != nil {
			logger.Warn(fmt.Sprintf("[%s]: failed to strip finalizers from %s %s: %v", removeObsoleteCRDsHookName, obsolete.gvk.Kind, leftover.GetName(), err))
			ok = false
			continue
		}
		logger.Info(fmt.Sprintf("[%s]: stripped finalizers from %s %s", removeObsoleteCRDsHookName, obsolete.gvk.Kind, leftover.GetName()))
	}
	return ok
}
