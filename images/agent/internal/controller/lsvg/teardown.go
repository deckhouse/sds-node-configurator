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

package lsvg

import (
	"context"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/controller"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/utils"
)

// teardownRetryInterval is how long a member waits between the steps of a
// removal it cannot hurry. sanlock will not vouch for the absence of other
// owners until it has sat out its own interval, and nothing here shortens that.
const teardownRetryInterval = 20 * time.Second

// teardown removes the Volume Group of a pool when its resource is deleted.
//
// This is a cluster-wide operation dressed as one command, and the whole reason
// it lives here rather than in the metadata owner alone is that no node can stop
// another node's lockspace. The order is the protocol:
//
//  1. Every member stops its own lockspace and says so in status.nodes. A member
//     holding an active volume of the group does not stop — the attachment side
//     is still unwinding, and stopping under a live volume would leave it
//     writable with no lock behind it.
//  2. The metadata owner waits until nobody else reports a running lockspace.
//     It keeps its own: vgremove is run from a node that holds one.
//  3. The owner removes the group. lvm answers "not stopped on other hosts" or
//     "unknown host state" while sanlock is still waiting out its interval; both
//     are the protocol asking for time, so the pass retries and says what it is
//     waiting for rather than reporting a failure.
//  4. Only then does the finalizer go, which is what lets the resource
//     disappear.
//
// Volumes are the one thing that stops all of this. A group with volumes in it
// holds somebody's data, and removing it is not a decision that follows from
// deleting a resource — the volumes have resources of their own, and deleting
// those is how they go.
func (r *Reconciler) teardown(
	ctx context.Context,
	lsvg *v1alpha1.LVMSharedVolumeGroup,
) (controller.Result, error) {
	if !hasFinalizer(lsvg) {
		// Nothing to unwind: the group was never taken up by this agent, or the
		// finalizer has already gone with the work done.
		return controller.Result{}, nil
	}

	// Every member stops its own lockspace first, owner included at the end.
	if res, done, err := r.stopForTeardown(ctx, lsvg); err != nil || !done {
		return res, err
	}

	if lsvg.Spec.MetadataOwner != r.cfg.NodeName {
		// A member that is not the owner has done its part: it has left the
		// lockspace, and it says so where the owner reads it.
		return controller.Result{}, nil
	}

	if remaining, err := r.volumesOfGroup(ctx, lsvg.Name); err != nil {
		return controller.Result{}, err
	} else if remaining > 0 {
		// Said plainly and repeatedly rather than forced: the group is not empty,
		// and what is in it is data somebody put there.
		r.publishNodeState(ctx, lsvg, false, ReasonVolumesRemain, fmt.Sprintf(
			"the volume group %s still holds %d volume(s) and will not be removed: delete their "+
				"LVMSharedLogicalVolume resources first, which is how the volumes themselves go. "+
				"This resource stays until then.", lsvg.Spec.ActualVGNameOnTheNode, remaining))
		return controller.Result{RequeueAfter: teardownRetryInterval}, nil
	}

	if waiting := r.membersStillInLockspace(lsvg); len(waiting) > 0 {
		r.log.Info(fmt.Sprintf("[%s] waiting to remove %s: %d member(s) still hold the lockspace (%v)",
			ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode, len(waiting), waiting))
		return controller.Result{RequeueAfter: teardownRetryInterval}, nil
	}

	r.log.Info(fmt.Sprintf("[%s] removing the volume group %s of the deleted pool", ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode))
	cmd, err := r.commands.RemoveVGShared(ctx, lsvg.Spec.ActualVGNameOnTheNode)
	if err != nil {
		if utils.SharedVGRemovalNeedsTime(err) {
			// Not a fault. sanlock is waiting out its interval, or a member has
			// not finished leaving; both end on their own.
			r.log.Info(fmt.Sprintf("[%s] %s cannot be removed yet, the lock manager is asking for time (cmd: %s): %s",
				ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode, cmd, err.Error()))
			return controller.Result{RequeueAfter: teardownRetryInterval}, nil
		}

		r.log.Error(err, fmt.Sprintf("[%s] unable to remove the volume group %s (cmd: %s)",
			ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode, cmd))
		r.publishNodeState(ctx, lsvg, false, ReasonRemovalFailed,
			"the volume group "+lsvg.Spec.ActualVGNameOnTheNode+" could not be removed: "+err.Error())
		return controller.Result{RequeueAfter: teardownRetryInterval}, nil
	}

	r.log.Info(fmt.Sprintf("[%s] the volume group %s is removed", ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode))
	return controller.Result{}, r.dropFinalizer(ctx, lsvg)
}

// stopForTeardown takes this node out of the lockspace of a group that is going
// away. It reports done only when the node holds no lockspace any more, because
// everything after it depends on that being true of every member.
func (r *Reconciler) stopForTeardown(
	ctx context.Context,
	lsvg *v1alpha1.LVMSharedVolumeGroup,
) (controller.Result, bool, error) {
	res, err := r.leave(ctx, lsvg)
	if err != nil {
		return controller.Result{}, false, err
	}
	if res.RequeueAfter > 0 {
		// leave is waiting for something of its own — an active volume, most
		// likely — and it has already said so.
		return res, false, nil
	}

	node, err := r.node(ctx)
	if err != nil {
		return controller.Result{}, false, err
	}
	if r.lockspaceStarted(node, lsvg.Name, "") {
		return controller.Result{RequeueAfter: teardownRetryInterval}, false, nil
	}

	return controller.Result{}, true, nil
}

// membersStillInLockspace names the members that have not left yet, as they
// themselves report it. Only a node can answer this about itself, which is why
// it is read from status rather than measured from here.
func (r *Reconciler) membersStillInLockspace(lsvg *v1alpha1.LVMSharedVolumeGroup) []string {
	if lsvg.Status == nil {
		return nil
	}

	var waiting []string
	for _, node := range lsvg.Status.Nodes {
		if node.Name != r.cfg.NodeName && node.LockspaceStarted {
			waiting = append(waiting, node.Name)
		}
	}
	return waiting
}

// volumesOfGroup counts the volumes still declared in the group.
func (r *Reconciler) volumesOfGroup(ctx context.Context, groupName string) (int, error) {
	volumes := &v1alpha1.LVMSharedLogicalVolumeList{}
	if err := r.cl.List(ctx, volumes); err != nil {
		return 0, fmt.Errorf("list the volumes of %s: %w", groupName, err)
	}

	count := 0
	for _, volume := range volumes.Items {
		if volume.Spec.LVMSharedVolumeGroupName == groupName {
			count++
		}
	}
	return count, nil
}

func hasFinalizer(lsvg *v1alpha1.LVMSharedVolumeGroup) bool {
	for _, f := range lsvg.Finalizers {
		if f == internal.SdsNodeConfiguratorFinalizer {
			return true
		}
	}
	return false
}

// addFinalizer marks the group as something this agent has work to undo. Only
// the metadata owner adds it: it is the node that will run the removal, and a
// finalizer nobody removes is a resource nobody can delete.
func (r *Reconciler) addFinalizer(ctx context.Context, lsvg *v1alpha1.LVMSharedVolumeGroup) error {
	if lsvg.Spec.MetadataOwner != r.cfg.NodeName || hasFinalizer(lsvg) || lsvg.DeletionTimestamp != nil {
		return nil
	}

	patch := client.MergeFrom(lsvg.DeepCopy())
	lsvg.Finalizers = append(lsvg.Finalizers, internal.SdsNodeConfiguratorFinalizer)
	if err := r.cl.Patch(ctx, lsvg, patch); err != nil {
		if apierrors.IsNotFound(err) {
			// The resource went away between the read and the write. There is
			// nothing to hold back, and failing the pass over it would only stop
			// the work that is still worth doing.
			return nil
		}
		return fmt.Errorf("add the finalizer to %s: %w", lsvg.Name, err)
	}
	return nil
}

func (r *Reconciler) dropFinalizer(ctx context.Context, lsvg *v1alpha1.LVMSharedVolumeGroup) error {
	patch := client.MergeFrom(lsvg.DeepCopy())
	kept := lsvg.Finalizers[:0]
	for _, f := range lsvg.Finalizers {
		if f != internal.SdsNodeConfiguratorFinalizer {
			kept = append(kept, f)
		}
	}
	lsvg.Finalizers = kept

	if err := r.cl.Patch(ctx, lsvg, patch); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("remove the finalizer from %s: %w", lsvg.Name, err)
	}
	return nil
}
