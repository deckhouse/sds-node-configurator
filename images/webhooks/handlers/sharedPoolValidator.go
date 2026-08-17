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
	"slices"

	"github.com/slok/kubewebhook/v2/pkg/model"
	kwhvalidating "github.com/slok/kubewebhook/v2/pkg/webhook/validating"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cn "github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/lib/go/common/pkg/feature"
)

// cleanupStrength orders the erase policies. It exists so that "at least as
// strong as the pool demands" is a comparison rather than a set of special
// cases, and so that adding a policy later cannot silently be treated as the
// weakest one.
var cleanupStrength = map[string]int{
	"":                                   0,
	cn.VolumeCleanupDiscard:              1,
	cn.VolumeCleanupRandomFillSinglePass: 2,
	cn.VolumeCleanupRandomFillThreePass:  3,
}

// SharedPoolValidator validates what a schema cannot: the rules that involve
// more than one object.
//
// Everything expressible inside a single resource is already enforced by the
// CRD — the 44-character Volume Group name, Thick as the only volume type,
// immutability, metadataOwner being one of nodes. What is left needs to read a
// second object, and that is what this is for.
type SharedPoolValidator struct {
	cl client.Client
	// cleanupAvailable is injected rather than called directly so that the
	// cross-object rules can be tested in every edition. The rules themselves
	// have nothing to do with which edition this is; only the gate does.
	cleanupAvailable func() bool
}

func NewSharedPoolValidator(cl client.Client) *SharedPoolValidator {
	return &SharedPoolValidator{cl: cl, cleanupAvailable: feature.VolumeCleanupEnabled}
}

// ValidateVolume refuses a volume whose erase policy is weaker than its pool
// demands, and one whose pool does not exist.
//
// The pool's policy is a floor rather than a default, and the difference
// matters here: capacity carved out of a shared LUN returns to the same pool
// and comes back out as somebody else's volume, so a single volume opting into
// a weaker erase would undo the guarantee for the whole pool rather than only
// for itself.
func (v *SharedPoolValidator) ValidateVolume(
	ctx context.Context,
	_ *model.AdmissionReview,
	obj metav1.Object,
) (*kwhvalidating.ValidatorResult, error) {
	if result := v.requireEdition(); result != nil {
		return result, nil
	}

	volume, ok := obj.(*cn.LVMSharedLogicalVolume)
	if !ok {
		return invalid("expected an LVMSharedLogicalVolume"), nil
	}

	group := &cn.LVMSharedVolumeGroup{}
	if err := v.cl.Get(ctx, client.ObjectKey{Name: volume.Spec.LVMSharedVolumeGroupName}, group); err != nil {
		return invalid(fmt.Sprintf(
			"LVMSharedVolumeGroup %q does not exist, so there is nothing to carve this volume out of",
			volume.Spec.LVMSharedVolumeGroupName)), nil
	}

	requested, known := cleanupStrength[volume.Spec.VolumeCleanup]
	if !known {
		return invalid(fmt.Sprintf("unknown volumeCleanup %q", volume.Spec.VolumeCleanup)), nil
	}
	floor := cleanupStrength[group.Spec.VolumeCleanup]

	if requested < floor {
		return invalid(fmt.Sprintf(
			"volumeCleanup %q is weaker than the %q required by the pool %s: "+
				"the capacity of this volume goes back into the same LUN and comes out as another tenant's volume, "+
				"so a weaker policy here weakens the pool rather than only this volume",
			volume.Spec.VolumeCleanup, group.Spec.VolumeCleanup, group.Name)), nil
	}

	return valid(), nil
}

// ValidateAttachment refuses an attachment that cannot be satisfied.
//
// A node outside the group's member list does not hold the group's lockspace,
// and only a member can be granted a lock. Such an attachment would sit in
// Pending forever while looking like a scheduling problem, so it is refused at
// the point where the mistake is still visible.
func (v *SharedPoolValidator) ValidateAttachment(
	ctx context.Context,
	_ *model.AdmissionReview,
	obj metav1.Object,
) (*kwhvalidating.ValidatorResult, error) {
	if result := v.requireEdition(); result != nil {
		return result, nil
	}

	attachment, ok := obj.(*cn.LVMSharedLogicalVolumeAttachment)
	if !ok {
		return invalid("expected an LVMSharedLogicalVolumeAttachment"), nil
	}

	// An attachment on its way out asks for nothing, so there is nothing here to
	// refuse. Judging it anyway is not caution but a deadlock: the one update
	// such an object still needs is the removal of a finalizer, and the node it
	// names is exactly the node that stopped being a member. Measured on the
	// stand — a node was fenced and deleted, and the cleanup that had to release
	// its attachment was denied by this validator every time it tried.

	// An attachment on its way out asks for nothing, so there is nothing here to
	// refuse. Judging it anyway is not caution but a deadlock: the one update
	// such an object still needs is the removal of a finalizer, and the node it
	// names is exactly the node that stopped being a member. Measured on the
	// stand — a node was fenced and deleted, and the cleanup that had to release
	// its attachment was denied by this validator every time it tried.
	if attachment.DeletionTimestamp != nil {
		return valid(), nil
	}

	// A read that failed is not the same answer as an object that is absent, and
	// only the first of the two is this validator's to report. Anything else —
	// an API timeout, a denied read, a cold cache — is returned as an error, so
	// the webhook's own failurePolicy decides, rather than telling the reader to
	// go looking for a resource nobody deleted.
	volume := &cn.LVMSharedLogicalVolume{}
	if err := v.cl.Get(ctx, client.ObjectKey{Name: attachment.Spec.LVMSharedLogicalVolumeName}, volume); err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("read LVMSharedLogicalVolume %s: %w", attachment.Spec.LVMSharedLogicalVolumeName, err)
		}
		return invalid(fmt.Sprintf("LVMSharedLogicalVolume %q does not exist",
			attachment.Spec.LVMSharedLogicalVolumeName)), nil
	}

	group := &cn.LVMSharedVolumeGroup{}
	if err := v.cl.Get(ctx, client.ObjectKey{Name: volume.Spec.LVMSharedVolumeGroupName}, group); err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("read LVMSharedVolumeGroup %s: %w", volume.Spec.LVMSharedVolumeGroupName, err)
		}
		return invalid(fmt.Sprintf("LVMSharedVolumeGroup %q does not exist",
			volume.Spec.LVMSharedVolumeGroupName)), nil
	}

	if !slices.Contains(group.Spec.Nodes, attachment.Spec.NodeName) {
		return invalid(fmt.Sprintf(
			"node %q is not a member of the pool %s, so it cannot be granted a lock on %s: "+
				"only a member holds the lockspace that hands locks out",
			attachment.Spec.NodeName, group.Name, volume.Name)), nil
	}

	return valid(), nil
}

// requireEdition gates both validators on volume cleanup being available, which
// today means Enterprise Edition and CSE Pro and not SE — a narrower set than
// "EE-gated" suggests, and worth stating because SE has snapshots but not this.
//
// The dependency is not incidental. Capacity carved out of a shared LUN returns
// to the pool and comes back out as another tenant's volume, so a pool whose
// volumes cannot be erased is a pool that hands data between tenants. Refusing
// at admission is the only honest place: refusing at deletion would leave
// volumes that can be created and never removed.
func (v *SharedPoolValidator) requireEdition() *kwhvalidating.ValidatorResult {
	if v.cleanupAvailable() {
		return nil
	}
	return invalid("shared pools need volume cleanup, which is not available in this Deckhouse edition")
}

func valid() *kwhvalidating.ValidatorResult {
	return &kwhvalidating.ValidatorResult{Valid: true}
}

func invalid(message string) *kwhvalidating.ValidatorResult {
	return &kwhvalidating.ValidatorResult{Valid: false, Message: message}
}
