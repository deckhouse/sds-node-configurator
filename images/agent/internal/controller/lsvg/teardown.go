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
//  1. Nothing happens while the group still holds volumes. Removing a volume
//     takes the group's lock and the lock comes from a running lockspace, so a
//     teardown that stopped it first would leave the pool unable to delete the
//     volumes its own message asks for.
//  2. Then every member stops its own lockspace and says so in status.nodes. A
//     member holding an active volume does not stop — the attachment side is
//     still unwinding, and stopping under a live volume would leave it writable
//     with no lock behind it.
//  3. The metadata owner waits until nobody else reports a running lockspace.
//     It keeps its own: vgremove is run from a node that holds one.
//  4. The owner removes the group. lvm answers "not stopped on other hosts" or
//     "unknown host state" while sanlock is still waiting out its interval; both
//     are the protocol asking for time, so the pass retries and says what it is
//     waiting for rather than reporting a failure.
//  5. Only then does the finalizer go, which is what lets the resource
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

	// The volumes come first, and the order is not a preference.
	//
	// Removing a volume takes the group's lock, and the lock comes from a
	// running lockspace. A teardown that stopped the lockspace before checking
	// would leave the pool unable to delete the very volumes its own message
	// asks an operator to delete — measured on the stand: the owner stopped its
	// lockspace, and lvchange for the volume's cleanup then failed for as long
	// as the pool existed. So nothing stops while the group still holds
	// anything.
	remaining, err := r.volumesOfGroup(ctx, lsvg.Name)
	if err != nil {
		return controller.Result{}, err
	}
	if remaining > 0 {
		// And the pool keeps working while it waits. A lockspace that is down —
		// stopped by an earlier version of this teardown, or by a node that
		// left and came back — is what makes the volumes undeletable, so it is
		// started again rather than left for somebody to notice. Nothing about
		// this touches data: it is the same lockspace the pool ran on a minute
		// ago, and without it the deletion this pass is waiting for cannot
		// happen at all.
		r.ensureLockspaceRunning(ctx, lsvg)

		if lsvg.Spec.MetadataOwner == r.cfg.NodeName {
			// Said plainly and repeatedly rather than forced: the group is not
			// empty, and what is in it is data somebody put there.
			r.publishNodeState(ctx, lsvg, r.lockspaceStartedInStatus(lsvg), ReasonVolumesRemain, fmt.Sprintf(
				"the volume group %s still holds %d volume(s) and will not be removed: delete their "+
					"LVMSharedLogicalVolume resources first, which is how the volumes themselves go. "+
					"This resource stays until then.", lsvg.Spec.ActualVGNameOnTheNode, remaining))
		}
		return controller.Result{RequeueAfter: teardownRetryInterval}, nil
	}

	// Empty now. Everyone except the owner leaves the lockspace; the owner keeps
	// its own, because vgremove is run from a node that holds one — measured on
	// the stand, where an owner that stopped first got "Cannot access VG vgtest
	// due to failed lock" and nothing else, forever.
	if lsvg.Spec.MetadataOwner != r.cfg.NodeName {
		res, done, err := r.stopForTeardown(ctx, lsvg)
		if err != nil || !done {
			return res, err
		}
		// This member has done its part: it has left, and it says so where the
		// owner reads it.
		//
		// Its copy of the group's identity goes too. Only the owner runs the
		// removal, so only the owner used to forget it — and the other members
		// were left pointing the fencing handler at a group that no longer
		// exists. Seen on the stand: a pool removed cleanly, and two of its
		// three nodes still named it in vg-uuid.json afterwards. Nothing of the
		// group is active here by now — leave refuses to stop the lockspace
		// while a volume of it is.
		r.forgetVGUUID(lsvg.Spec.ActualVGNameOnTheNode)
		if r.metrics != nil {
			r.metrics.ForgetSharedPool(lsvg.Spec.ActualVGNameOnTheNode)
		}
		return controller.Result{}, nil
	}

	// And the owner's own lockspace has to be up for the same reason. It may not
	// be — an earlier version of this teardown stopped it, or the node left the
	// pool and came back — so it is started rather than assumed.
	r.ensureLockspaceRunning(ctx, lsvg)

	if waiting := r.membersStillInLockspace(lsvg); len(waiting) > 0 {
		r.log.Info(fmt.Sprintf("[%s] waiting to remove %s: %d member(s) still hold the lockspace (%v)",
			ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode, len(waiting), waiting))
		return controller.Result{RequeueAfter: teardownRetryInterval}, nil
	}

	r.log.Info(fmt.Sprintf("[%s] removing the volume group %s of the deleted pool", ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode))
	cmd, err := r.commands.RemoveVGShared(ctx, lsvg.Spec.ActualVGNameOnTheNode)
	if err != nil && r.groupIsAlreadyGone(ctx, lsvg) {
		// A group that is not there any more is a group that is removed. It gets
		// here after another member removed it, after an administrator did, and
		// after a removal that succeeded and whose bookkeeping did not — and in
		// all three the command fails with something that says nothing about the
		// removal: "Global lock failed: check that global lockspace is started",
		// because there is no volume group left to take a lock in. Found on the
		// stand, where a resource sat on its finalizer reporting RemovalFailed
		// once a minute over a volume group that had been gone for an hour.
		r.log.Info(fmt.Sprintf("[%s] %s is already gone; treating the removal as done (cmd: %s)",
			ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode, cmd))
		err = nil
	}
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

	// The group is gone, so its identity must go with it: an entry left behind
	// points the fencing handler at a UUID that no longer exists, and it would
	// find no maps and report a barrier it never raised.
	r.forgetVGUUID(lsvg.Spec.ActualVGNameOnTheNode)

	// The pool's gauge goes with the pool. Left behind it would keep reporting
	// its last value about something that no longer exists.
	if r.metrics != nil {
		r.metrics.ForgetSharedPool(lsvg.Spec.ActualVGNameOnTheNode)
	}

	// The lockspace went with the group — vgremove takes it down — so the fact
	// this node published about itself stops being true here.
	if err := r.setLockspaceStarted(ctx, lsvg.Name, "", false); err != nil {
		r.log.Warning(fmt.Sprintf("[%s] unable to record that the lockspace of %s is gone: %s",
			ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode, err.Error()))
	}

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

// ensureLockspaceRunning starts this node's lockspace when the lock manager does
// not have it and something about to run needs it.
//
// Removing a volume takes the group's lock. A pool whose lockspace is down
// cannot lose its volumes, and a pool that cannot lose its volumes cannot be
// removed — so a teardown that merely refrains from stopping the lockspace
// fixes the next pool and leaves this one where it is. Measured on the stand:
// an earlier order of these steps stopped the lockspace first, and the volume
// stayed undeletable afterwards no matter what an operator did.
func (r *Reconciler) ensureLockspaceRunning(ctx context.Context, lsvg *v1alpha1.LVMSharedVolumeGroup) {
	node, err := r.node(ctx)
	if err != nil {
		r.log.Warning(fmt.Sprintf("[%s] unable to read this node while unwinding %s: %s",
			ReconcilerName, lsvg.Name, err.Error()))
		return
	}
	if r.lockspaceStarted(node, lsvg.Name, "") {
		return
	}

	hostID, err := hostIDOf(node)
	if err != nil || hostID == 0 {
		// Without an id there is nothing to start with, and the allocator is not
		// going to hand one to a pool being deleted. Said once per pass rather
		// than silently skipped.
		r.log.Warning(fmt.Sprintf("[%s] cannot restart the lockspace of %s: no sanlock host id on this node",
			ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode))
		return
	}

	r.log.Info(fmt.Sprintf("[%s] restarting the lockspace of %s so its volumes can be removed",
		ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode))
	if cmd, err := r.startLockspaceUnderReservations(ctx, lsvg, hostID); err != nil {
		r.log.Warning(fmt.Sprintf("[%s] unable to restart the lockspace of %s (cmd: %s): %s",
			ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode, cmd, err.Error()))
		return
	}
	if err := r.setLockspaceStarted(ctx, lsvg.Name, r.vgUUID(lsvg.Spec.ActualVGNameOnTheNode), true); err != nil {
		r.log.Warning(fmt.Sprintf("[%s] unable to record the restarted lockspace of %s: %s",
			ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode, err.Error()))
	}
}

// Nothing clears the reservation here, and that was tried: clearing it before
// `vgremove` leaves the group requiring a reservation that nobody holds, and the
// removal then fails with "Persistent reservation is not started" for good.
//
// The normal path needs no cleaning. Every member gives up its key as it leaves,
// the node doing the removal keeps its own registration through it, and lvm2
// releases that one itself — measured on the stand, where the LUN was left with
// no keys and no reservation the moment the group was gone.
//
// What is left is the case of a key nobody can be asked to give up, because the
// node that holds it is gone. The removal then reports lvm's own message, which
// names the keys and the device, and an administrator clears them: `vgchange -y
// --persist clear`, then `--persist start` to register again, then the removal
// proceeds. Doing that from here would mean taking a registration away from a
// node that never said it had left, which is the one thing this module refuses
// to infer.

// groupIsAlreadyGone reports whether the volume group has stopped existing.
//
// It is asked only after a removal has failed, and it asks for the whole list
// rather than for the group by name: `vgs <name>` fails for a group that is not
// there, which is the same shape of failure as not being able to read at all,
// and this must not confuse the two. Listing succeeds either way, and the
// group's absence from a list that was read is a fact.
func (r *Reconciler) groupIsAlreadyGone(ctx context.Context, lsvg *v1alpha1.LVMSharedVolumeGroup) bool {
	groups, _, _, err := r.commands.GetAllVGs(ctx)
	if err != nil {
		return false
	}
	for _, group := range groups {
		if group.VGName == lsvg.Spec.ActualVGNameOnTheNode {
			return false
		}
	}
	return true
}
