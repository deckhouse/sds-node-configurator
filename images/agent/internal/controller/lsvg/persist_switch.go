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
	"strings"
	"time"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/controller"
)

// The states a member reports while a pool is being switched to reservations.
// PersistentReservationsRequired is the intent that starts all of this.
const PersistentReservationsRequired = "Required"

const (
	PRStateOff     = "Off"
	PRStateStopped = "Stopped"
	PRStateEnabled = "Enabled"

	// PersistRequired is what lvm2 reports for a group whose door is already
	// open: `vgs -o vg_persist` says "require" once `--setpersist require` has
	// succeeded, and it says it without taking a lock.
	PersistRequired = "require"

	// PersistInLockArgs is how a group that has been all the way through the
	// switch describes its own leases — "2.0.0:lvmlock:persist" against
	// "1.0.0:lvmlock" before it. It is the group's own record that the procedure
	// finished, and it is the only record that survives everything: an agent
	// restart, a status that was never written, a node that has never taken
	// part.
	PersistInLockArgs = ":persist"
)

// prSwitchRetryInterval is the pace of the switch. It is slow on purpose: two of
// its steps wait for sanlock to accept that other hosts have gone, and sanlock
// takes its own time about that.
const prSwitchRetryInterval = 20 * time.Second

// switchToPersistentReservations carries a pool from lockspaces alone to
// lockspaces held under SCSI-3 persistent reservations.
//
// The order below is not a design; it is what the array and lvm2 accept,
// established by running it. Every deviation tried first was refused:
//
//  1. Every member except the executor stops its lockspace and its reservations
//     and says so. `vgchange --setlockargs persist` on the executor checks this
//     twice and differently — "Found N PR keys …, stop PR and lockspace on other
//     hosts" is about the array, "Lockspace for … not stopped on other hosts" is
//     about sanlock having noticed — so the members must be gone from both.
//  2. The executor keeps ITS lockspace running: `--setpersist require` fails with
//     "Cannot access VG … due to failed lock" without one. The instruction "stop
//     the lockspace everywhere" applies to the neighbours, not to the node
//     running the command.
//  3. `--setpersist require` → `--persist start` → `--lock-start` →
//     `--setlockargs persist`, in that order. The first of those is the one-way
//     door: from the moment it succeeds the group answers "Persistent
//     reservation is not started" to everything, and only `--persist start`
//     reopens it. Nothing here may be attempted unless the channel has already
//     been established as working — see persist.go, which answers that by
//     reading.
//  4. The members come back with `--persist start` and `--lock-start`.
//
// A pool that cannot complete step 3 is a pool nobody can use, which is why the
// readiness of every member is a precondition rather than a hope.
func (r *Reconciler) switchToPersistentReservations(
	ctx context.Context,
	lsvg *v1alpha1.LVMSharedVolumeGroup,
) (controller.Result, bool) {
	if lsvg.Spec.PersistentReservations != PersistentReservationsRequired {
		// Nothing asked for. A pool already holding reservations is not switched
		// back here: giving up a reservation is its own procedure, and doing it
		// as a side effect of an edit is not it.
		return controller.Result{}, false
	}

	if r.prStateHere(lsvg) == PRStateEnabled {
		return controller.Result{}, false
	}

	// Whether the switch has already happened is asked of the group, not
	// remembered. A group that has been through it records it in its own lock
	// args — "2.0.0:lvmlock:persist" — and that record outlives everything this
	// node might have forgotten: an agent restart, a status entry that was never
	// written, a member that has never taken part.
	//
	// Without this a pool switches itself again after every restart: the node
	// reads its own silence as "no reservations", stands its neighbours aside,
	// and asks them to leave a lockspace they have already left. Found on the
	// stand doing exactly that, in a loop.
	if switched, err := r.groupAlreadySwitched(ctx, lsvg); err != nil {
		return controller.Result{RequeueAfter: prSwitchRetryInterval}, true
	} else if switched {
		r.log.Info(fmt.Sprintf("[%s] %s is already under reservations; this node joins it rather than switching it",
			ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode))
		r.publishPRState(ctx, lsvg, PRStateEnabled, r.lockspaceReallyRunning(ctx, lsvg))
		return controller.Result{}, false
	}

	if lsvg.Spec.MetadataOwner != r.cfg.NodeName {
		return r.standAsideForTheSwitch(ctx, lsvg), true
	}
	return r.runTheSwitch(ctx, lsvg), true
}

// standAsideForTheSwitch is a member's whole part in it: stop, say so, and wait
// to be told the group is under reservations before coming back.
func (r *Reconciler) standAsideForTheSwitch(
	ctx context.Context,
	lsvg *v1alpha1.LVMSharedVolumeGroup,
) controller.Result {
	if r.prStateOf(lsvg, lsvg.Spec.MetadataOwner) == PRStateEnabled {
		// The group is under reservations now. Coming back means starting the
		// reservation first and the lockspace second: the lockspace cannot be
		// started over a group whose PR has not been started here.
		if cmd, err := r.commands.VGPersistStart(ctx, lsvg.Spec.ActualVGNameOnTheNode, r.hostIDFor(ctx)); err != nil {
			r.log.Warning(fmt.Sprintf("[%s] cannot rejoin %s under reservations yet (cmd: %s): %s",
				ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode, cmd, err.Error()))
			return controller.Result{RequeueAfter: prSwitchRetryInterval}
		}
		// The reservation is started here; the lockspace is not yet, and the
		// ordinary pass starts it next.
		r.publishPRState(ctx, lsvg, PRStateEnabled, false)
		return controller.Result{}
	}

	if r.prStateOf(lsvg, r.cfg.NodeName) == PRStateStopped {
		// Already out of the way. The executor is working; nothing to do but
		// wait, and the pool says who it is waiting for.
		return controller.Result{RequeueAfter: prSwitchRetryInterval}
	}

	// Volumes first: a member with an active volume of the group is serving a
	// workload, and stopping its lockspace under one would leave the volume
	// mapped with no lock behind it. The switch waits for the workload to be
	// gone rather than taking it down.
	if active := r.activeLVs(lsvg.Spec.ActualVGNameOnTheNode); len(active) > 0 {
		r.log.Warning(fmt.Sprintf("[%s] not stepping aside for the reservation switch of %s: %d volume(s) still active here",
			ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode, len(active)))
		r.publishNodeState(ctx, lsvg, r.lockspaceReallyRunning(ctx, lsvg), ReasonPRWaitingForVolumes, fmt.Sprintf(
			"the switch to persistent reservations is waiting for %d volume(s) of %s to be released on this node: "+
				"the members have to leave the lockspace for it, and a node cannot leave while it serves a volume. "+
				"Evacuate the workload from this node to continue", len(active), lsvg.Spec.ActualVGNameOnTheNode))
		return controller.Result{RequeueAfter: prSwitchRetryInterval}
	}

	// The lockspace goes first. lvm2 refuses the other order outright — "VG %s
	// locking should be stopped before PR (vgchange --lockstop)" — and a member
	// that kept trying it stood in front of the switch for as long as it kept
	// trying, which is what the stand showed.
	if err := r.setLockspaceStarted(ctx, lsvg.Name, "", false); err != nil {
		return controller.Result{RequeueAfter: prSwitchRetryInterval}
	}
	if cmd, err := r.commands.VGLockStop(ctx, lsvg.Spec.ActualVGNameOnTheNode); err != nil {
		r.log.Warning(fmt.Sprintf("[%s] unable to stop the lockspace of %s for the switch (cmd: %s): %s",
			ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode, cmd, err.Error()))
		return controller.Result{RequeueAfter: prSwitchRetryInterval}
	}
	if cmd, err := r.commands.VGPersistStop(ctx, lsvg.Spec.ActualVGNameOnTheNode, r.hostIDFor(ctx)); err != nil {
		r.log.Warning(fmt.Sprintf("[%s] unable to stop reservations of %s (cmd: %s): %s",
			ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode, cmd, err.Error()))
		return controller.Result{RequeueAfter: prSwitchRetryInterval}
	}

	r.log.Info(fmt.Sprintf("[%s] stepped aside for the reservation switch of %s", ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode))
	r.publishPRState(ctx, lsvg, PRStateStopped, false)
	return controller.Result{RequeueAfter: prSwitchRetryInterval}
}

// runTheSwitch is the executor's half, and the only place in this module that
// opens a door it cannot close.
func (r *Reconciler) runTheSwitch(
	ctx context.Context,
	lsvg *v1alpha1.LVMSharedVolumeGroup,
) controller.Result {
	// Nothing is attempted until every member has said the channel works. The
	// verdict is read rather than assumed for the same reason the door is
	// one-way: a member that cannot register would be left outside a pool the
	// others are holding, and the way back is another maintenance window.
	if blocked := r.membersWithoutReservations(lsvg); len(blocked) > 0 {
		r.publishNodeState(ctx, lsvg, r.lockspaceReallyRunning(ctx, lsvg), ReasonPRNotReady, fmt.Sprintf(
			"the pool is not switched to persistent reservations because %d member(s) cannot take part: %v. "+
				"Their reasons are in status.nodes of this resource; nothing is changed until every member can",
			len(blocked), blocked))
		return controller.Result{RequeueAfter: prSwitchRetryInterval}
	}

	if r.hostIDFor(ctx) <= 0 {
		// lvm2 derives the key a node registers with from its host id, so
		// without one every reservation command stops before it starts. The id
		// is allocated by the pool controller and lands on the node as an
		// annotation; a node without it is not one to take through the door.
		r.publishNodeState(ctx, lsvg, r.lockspaceReallyRunning(ctx, lsvg), ReasonPRNotReady,
			"the pool is not switched to persistent reservations because this node has no sanlock host id yet, "+
				"and lvm2 derives the key a node registers with from it")
		return controller.Result{RequeueAfter: prSwitchRetryInterval}
	}

	if waiting := r.membersNotStoodAside(lsvg); len(waiting) > 0 {
		r.log.Info(fmt.Sprintf("[%s] waiting to switch %s to reservations: %d member(s) have not stepped aside (%v)",
			ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode, len(waiting), waiting))
		return controller.Result{RequeueAfter: prSwitchRetryInterval}
	}

	// Where this is picking up from. Once `--setpersist require` has succeeded
	// the group answers "Cannot access VG due to failed lock" to everything that
	// takes a lock — `--setpersist` included — so a procedure that always began
	// at the beginning could never finish what it had started. Found on the
	// stand, from inside exactly that state.
	setting, err := r.commands.VGPersistSetting(ctx, lsvg.Spec.ActualVGNameOnTheNode)
	if err != nil {
		r.log.Warning(fmt.Sprintf("[%s] cannot read whether %s already requires reservations: %s",
			ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode, err.Error()))
		return controller.Result{RequeueAfter: prSwitchRetryInterval}
	}

	if !strings.Contains(setting, PersistRequired) {
		// And this node's own lockspace has to be running, which is the opposite
		// of what "stop the lockspace everywhere" sounds like: --setpersist
		// require fails without one.
		r.ensureLockspaceRunning(ctx, lsvg)

		r.log.Info(fmt.Sprintf("[%s] switching %s to persistent reservations", ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode))
		if cmd, err := r.commands.VGSetPersist(ctx, lsvg.Spec.ActualVGNameOnTheNode, r.hostIDFor(ctx)); err != nil {
			// Refused before the door opened, which is the good case: the group
			// is still usable and nothing has to be undone.
			r.log.Error(err, fmt.Sprintf("[%s] %s was not switched (cmd: %s)", ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode, cmd))
			r.publishNodeState(ctx, lsvg, r.lockspaceReallyRunning(ctx, lsvg), ReasonPRSwitchFailed,
				"the group was not switched to persistent reservations and is unchanged: "+err.Error())
			return controller.Result{RequeueAfter: prSwitchRetryInterval}
		}
	} else {
		r.log.Info(fmt.Sprintf("[%s] %s already requires reservations; continuing the switch from --persist start",
			ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode))
	}

	// Past this line the group answers "Persistent reservation is not started"
	// to everything. Only --persist start reopens it, and the retry exists for
	// exactly that: giving up here would leave the pool unusable.
	if cmd, err := r.commands.VGPersistStart(ctx, lsvg.Spec.ActualVGNameOnTheNode, r.hostIDFor(ctx)); err != nil {
		r.log.Error(err, fmt.Sprintf("[%s] %s is between states: --setpersist require succeeded and --persist start did not (cmd: %s)",
			ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode, cmd))
		r.publishNodeState(ctx, lsvg, false, ReasonPRSwitchIncomplete, fmt.Sprintf(
			"the volume group %s requires persistent reservations and has not started them, so it is unusable "+
				"until it does: every command on it answers \"Persistent reservation is not started\". This is "+
				"retried; the cause is %s", lsvg.Spec.ActualVGNameOnTheNode, err.Error()))
		return controller.Result{RequeueAfter: prSwitchRetryInterval}
	}

	if err := r.setLockspaceStarted(ctx, lsvg.Name, "", false); err != nil {
		return controller.Result{RequeueAfter: prSwitchRetryInterval}
	}
	// Registration is not repeated here: --persist start ran three lines above,
	// and this is the lock-start it exists for.
	if cmd, err := r.commands.VGLockStart(ctx, lsvg.Spec.ActualVGNameOnTheNode, r.hostIDFor(ctx)); err != nil {
		r.log.Warning(fmt.Sprintf("[%s] the lockspace of %s did not start under reservations (cmd: %s): %s",
			ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode, cmd, err.Error()))
		return controller.Result{RequeueAfter: prSwitchRetryInterval}
	}
	if err := r.setLockspaceStarted(ctx, lsvg.Name, r.vgUUID(lsvg.Spec.ActualVGNameOnTheNode), true); err != nil {
		return controller.Result{RequeueAfter: prSwitchRetryInterval}
	}

	if cmd, err := r.commands.VGSetLockArgsPersist(ctx, lsvg.Spec.ActualVGNameOnTheNode, r.hostIDFor(ctx)); err != nil {
		// The two checks behind this are about the neighbours: keys still on the
		// array, or sanlock not yet convinced they have gone. Both pass with
		// time, so this waits rather than reporting a fault.
		r.log.Info(fmt.Sprintf("[%s] %s is not ready to record the lock args yet (cmd: %s): %s",
			ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode, cmd, err.Error()))
		return controller.Result{RequeueAfter: prSwitchRetryInterval}
	}

	// `--setlockargs persist` writes the new lock args by stopping the lockspace
	// and dropping this node's registration with it — visible in sanlock's log
	// as "free lockspace" and on the array as a key that is simply gone. So the
	// executor comes back the same way every other member does.
	if err := r.setLockspaceStarted(ctx, lsvg.Name, "", false); err != nil {
		return controller.Result{RequeueAfter: prSwitchRetryInterval}
	}
	if cmd, err := r.rejoinUnderReservations(ctx, lsvg); err != nil {
		r.log.Warning(fmt.Sprintf("[%s] %s is under reservations and this node has not rejoined it yet (cmd: %s): %s",
			ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode, cmd, err.Error()))
		r.publishPRState(ctx, lsvg, PRStateEnabled, false)
		return controller.Result{RequeueAfter: prSwitchRetryInterval}
	}
	if err := r.setLockspaceStarted(ctx, lsvg.Name, r.vgUUID(lsvg.Spec.ActualVGNameOnTheNode), true); err != nil {
		return controller.Result{RequeueAfter: prSwitchRetryInterval}
	}

	r.log.Info(fmt.Sprintf("[%s] %s is under persistent reservations", ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode))
	r.publishPRState(ctx, lsvg, PRStateEnabled, true)
	return controller.Result{}
}

// membersWithoutReservations names the members whose channel is not ready. A
// member that has not answered counts: silence is not consent to open a
// one-way door.
func (r *Reconciler) membersWithoutReservations(lsvg *v1alpha1.LVMSharedVolumeGroup) []string {
	said := map[string]*v1alpha1.NodePersistentReservations{}
	if lsvg.Status != nil {
		for _, node := range lsvg.Status.Nodes {
			said[node.Name] = node.PersistentReservations
		}
	}

	var blocked []string
	for _, name := range lsvg.Spec.Nodes {
		pr, found := said[name]
		if !found || pr == nil || !pr.Ready {
			blocked = append(blocked, name)
		}
	}
	return blocked
}

// membersNotStoodAside names the members that still hold the group.
func (r *Reconciler) membersNotStoodAside(lsvg *v1alpha1.LVMSharedVolumeGroup) []string {
	var waiting []string
	for _, name := range lsvg.Spec.Nodes {
		if name == r.cfg.NodeName {
			continue
		}
		if r.prStateOf(lsvg, name) != PRStateStopped {
			waiting = append(waiting, name)
		}
	}
	return waiting
}

func (r *Reconciler) prStateOf(lsvg *v1alpha1.LVMSharedVolumeGroup, nodeName string) string {
	if lsvg.Status == nil {
		return PRStateOff
	}
	for _, node := range lsvg.Status.Nodes {
		if node.Name != nodeName {
			continue
		}
		if node.PersistentReservations == nil || node.PersistentReservations.State == "" {
			return PRStateOff
		}
		return node.PersistentReservations.State
	}
	return PRStateOff
}

// hostIDFor reads the id the controller allocated, and answers zero when it
// cannot — the caller's command then fails and is retried, which is better than
// starting a lockspace with an id nobody assigned.
func (r *Reconciler) hostIDFor(ctx context.Context) int {
	node, err := r.node(ctx)
	if err != nil {
		return 0
	}
	hostID, err := hostIDOf(node)
	if err != nil {
		return 0
	}
	return hostID
}

// publishPRState records where this node stands in the switch, which is the
// only channel the members have to each other: the executor waits for their
// Stopped and they wait for its Enabled.
// publishPRState says where this node stands, together with whether its
// lockspace is running.
//
// The two are published together on purpose. The first version read the
// lockspace flag out of the status it was about to overwrite, so a node that had
// just stopped its lockspace to step aside went on saying it was running —
// established on the stand, where two members reported `Stopped` and
// `lockspaceStarted: true` at once. Everything that reads that field reads it to
// decide whether the node still holds leases: the pool's readiness, the removal
// protocol, and the moment a LUN may be taken away from a node.
func (r *Reconciler) publishPRState(
	ctx context.Context,
	lsvg *v1alpha1.LVMSharedVolumeGroup,
	state string,
	lockspaceRunning bool,
) {
	r.prStates[lsvg.Name] = state
	r.publishNodeState(ctx, lsvg, lockspaceRunning, "", "")
}

// startLockspaceUnderReservations starts this node's lockspace, registering with
// the array first when the group is held under persistent reservations.
//
// The order is not ours to choose. Under reservations the sanlock lease is
// renewed with a SCSI command an unregistered initiator may not issue, so a
// lockspace started without a registration is a lockspace that cannot be kept —
// the node reads the LUN, fails every write, and is fenced by its own watchdog.
//
// Every path back into a pool goes through here: a member returning after the
// switch, a node whose lock daemons restarted, and the executor itself —
// `vgchange --setlockargs persist` stops the lockspace and drops the
// registration as part of writing the new lock args, which was found on the
// stand as an executor holding a running group with no key on the array.
func (r *Reconciler) startLockspaceUnderReservations(
	ctx context.Context,
	lsvg *v1alpha1.LVMSharedVolumeGroup,
	hostID int,
) (string, error) {
	// Whether this node is registered with the array is decided by where it
	// already stands, not by what the pool has asked for: between the request
	// and the group actually requiring reservations there is a window, and
	// registering inside it is refused — which would leave the executor unable
	// to bring up the lockspace that `--setpersist require` needs.
	if r.prStateHere(lsvg) == PRStateEnabled {
		if cmd, err := r.commands.VGPersistStart(ctx, lsvg.Spec.ActualVGNameOnTheNode, hostID); err != nil {
			return cmd, err
		}
		r.forgetPRVerdict(lsvg)
	}
	return r.commands.VGLockStart(ctx, lsvg.Spec.ActualVGNameOnTheNode, hostID)
}

// rejoinUnderReservations is the executor's way back in, run once the lock args
// are written: register, then start the lockspace. It does not consult the
// node's published state because the node is about to acquire it — this is the
// pass that makes it true.
func (r *Reconciler) rejoinUnderReservations(
	ctx context.Context,
	lsvg *v1alpha1.LVMSharedVolumeGroup,
) (string, error) {
	hostID := r.hostIDFor(ctx)
	if cmd, err := r.commands.VGPersistStart(ctx, lsvg.Spec.ActualVGNameOnTheNode, hostID); err != nil {
		return cmd, err
	}
	r.forgetPRVerdict(lsvg)
	return r.commands.VGLockStart(ctx, lsvg.Spec.ActualVGNameOnTheNode, hostID)
}

// prStateHere is where this node stands, and it outlives the process.
//
// The state is a fact about the array and the group — this node holds a
// registration, or has given one up on purpose — not bookkeeping that may start
// empty. An agent restart used to erase it: the node published no state, its
// own switch logic then read that back as "not under reservations", and the two
// things that follow from it are both wrong. It would take the pool through the
// switch again, standing its neighbours aside; and it would start its lockspace
// without registering first, which under a Write Exclusive, all registrants
// reservation is a node that cannot renew its lease and fences itself.
//
// So memory answers only while it has something to say, and what was published
// answers otherwise.
func (r *Reconciler) prStateHere(lsvg *v1alpha1.LVMSharedVolumeGroup) string {
	if state := r.prStates[lsvg.Name]; state != "" {
		return state
	}
	return publishedPRStateOf(lsvg, r.cfg.NodeName)
}

// publishedPRStateOf reads a member's last published state, with "" for a member
// that has never published one.
func publishedPRStateOf(lsvg *v1alpha1.LVMSharedVolumeGroup, nodeName string) string {
	if lsvg.Status == nil {
		return ""
	}
	for _, node := range lsvg.Status.Nodes {
		if node.Name == nodeName && node.PersistentReservations != nil {
			return node.PersistentReservations.State
		}
	}
	return ""
}

// groupAlreadySwitched asks the group whether the switch has been made.
func (r *Reconciler) groupAlreadySwitched(
	ctx context.Context,
	lsvg *v1alpha1.LVMSharedVolumeGroup,
) (bool, error) {
	answer, err := r.commands.VGPersistSetting(ctx, lsvg.Spec.ActualVGNameOnTheNode)
	if err != nil {
		r.log.Warning(fmt.Sprintf("[%s] cannot ask %s whether it is already under reservations: %s",
			ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode, err.Error()))
		return false, err
	}
	return strings.Contains(answer, PersistInLockArgs), nil
}
