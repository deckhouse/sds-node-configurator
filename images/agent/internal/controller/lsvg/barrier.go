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

package lsvg

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/utils"
)

// barrierResult is what the fencing handler leaves behind. The handler has no
// API access of its own — a token in an OnDelete image would make every change
// to it an operation with a drain — so the file is the only way the fact
// reaches anything that can act on it.
type barrierResult struct {
	VG          string    `json:"vg"`
	VGUUID      string    `json:"vgUUID"`
	FinishedAt  time.Time `json:"finishedAt"`
	MapsCovered int       `json:"mapsCovered"`
	Complete    bool      `json:"complete"`
	CoveredMaps []string  `json:"coveredMaps,omitempty"`
	FailedMaps  []string  `json:"failedMaps,omitempty"`
	Error       string    `json:"error,omitempty"`
}

// barrierResultPath is where the handler writes it, by the group's name.
func (r *Reconciler) barrierResultPath(vgName string) string {
	return filepath.Join(r.cfg.HostIDDir, "killpath-"+vgName+".json")
}

// recoverFromBarrier brings a fenced node back into the pool without anybody
// asking it to.
//
// A node that lost its lease has every map of the group replaced by an error
// target: that is the barrier doing its job, and it is what keeps writes still
// in flight from reaching an array that has already given the volumes to
// somebody else. What used to follow was an operator with two commands. On a
// platform meant to run without one, a node that can see its LUN again has to
// rejoin by itself.
//
// The recovery is safe for data by construction: an error target holds nothing,
// so removing it destroys nothing, and the volumes come back only when their
// attachments ask for them — through the lock, from the node that holds it.
//
// The one precondition is the LUN. Returning to the pool while the paths are
// still broken buys a second fencing one io_timeout later, so the devices are
// resolved first and the recovery waits if they are missing.
func (r *Reconciler) recoverFromBarrier(ctx context.Context, lsvg *v1alpha1.LVMSharedVolumeGroup) bool {
	path := r.barrierResultPath(lsvg.Spec.ActualVGNameOnTheNode)
	raw, err := os.ReadFile(path)
	if err != nil {
		// The ordinary case by far: this node has never been fenced.
		return false
	}

	var res barrierResult
	if err := json.Unmarshal(raw, &res); err != nil {
		// Unreadable, and nothing can be concluded from it. Left in place: a
		// file nobody could parse is evidence, and deleting evidence to make a
		// reconcile tidy is the wrong trade.
		r.log.Warning(fmt.Sprintf("[%s] the barrier result at %s cannot be read: %s",
			ReconcilerName, path, err.Error()))
		return false
	}

	wwids := make([]string, 0, len(lsvg.Spec.Devices))
	for _, device := range lsvg.Spec.Devices {
		wwids = append(wwids, device.WWID)
	}
	_, missing, err := utils.ResolveWWIDs(wwids)
	if err != nil || len(missing) > 0 {
		// The paths are still gone. Rejoining now would earn another barrier one
		// io_timeout later, so the node waits — and says why, because a node out
		// of its pool with no explanation is the thing this whole file exists to
		// prevent.
		r.log.Info(fmt.Sprintf("[%s] %s was fenced at %s and cannot return yet: %d device(s) still missing",
			ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode, res.FinishedAt.Format(time.RFC3339), len(missing)))
		return true
	}

	r.log.Info(fmt.Sprintf("[%s] %s was fenced at %s (%d map(s) covered), the LUNs are back, removing the error targets",
		ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode, res.FinishedAt.Format(time.RFC3339), res.MapsCovered))

	// The lease area among the covered maps is not one more volume to clean up.
	// It is the storage sanlock renews its lease through, and an error target
	// under it means this node's lockspace is dead however healthy lvmlockd
	// looks: every renewal returns EIO. It also means the map cannot be removed
	// the ordinary way, because sanlock still holds it open, which is exactly
	// how a node ends up repeating "Device or resource busy" every thirty
	// seconds for the rest of its life.
	//
	// This module's own barrier never covers the lease area. A handler from an
	// older release, or one somebody wrote by hand, does — and a recovery that
	// only works against the handler shipped with it is not a recovery.
	//
	// So the dead lockspace is stopped first. Nothing is lost by it: the leases
	// it held stopped being renewable the moment the barrier went up, and the
	// volumes underneath are error targets holding no data. Stopping it makes
	// sanlock let go of the lease area, and the ordinary path starts the
	// lockspace again on a later pass.
	leaseMap := utils.DMName(lsvg.Spec.ActualVGNameOnTheNode, utils.LeaseAreaLVName)
	if slices.Contains(res.CoveredMaps, leaseMap) {
		r.log.Warning(fmt.Sprintf("[%s] the barrier covered the lease area of %s, so the lockspace here is dead: stopping it before the error targets go",
			ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode))

		if err := r.setLockspaceStarted(ctx, lsvg.Name, "", false); err != nil {
			r.log.Warning(fmt.Sprintf("[%s] unable to record that the lockspace of %s is stopped: %s",
				ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode, err.Error()))
			return true
		}
		if cmd, err := r.commands.VGLockStop(ctx, lsvg.Spec.ActualVGNameOnTheNode); err != nil {
			// Not fatal on its own: the removal below is tried anyway, and it is
			// what decides whether this converges.
			r.log.Warning(fmt.Sprintf("[%s] unable to stop the dead lockspace of %s (cmd: %s): %s",
				ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode, cmd, err.Error()))
		}
	}

	for _, dmName := range res.CoveredMaps {
		// A map that is already gone is the goal, not a case to handle: it may
		// have been removed by a previous pass, by a deferred removal that has
		// since fired, or by an operator. Asking device-mapper to remove it
		// would fail with "No such device", and a recovery that reads its own
		// success as an error never finishes.
		if !utils.DMDeviceExists(dmName) {
			continue
		}

		cmd, err := r.commands.RemoveDMDevice(ctx, dmName)
		if err == nil {
			continue
		}

		// A map that is still open belongs to something that has not let go
		// yet, and the error target under it is already refusing every write —
		// so the next pass is normally the whole answer.
		r.log.Warning(fmt.Sprintf("[%s] the error target %s could not be removed (cmd: %s): %s",
			ReconcilerName, dmName, cmd, err.Error()))

		if dmName != leaseMap {
			return true
		}

		// Except here, where waiting is what fails. The opener is sanlock, it
		// has just been asked to leave, and if it has not closed the lease area
		// by now it is because it cannot — its own device answers EIO. Handing
		// the removal to the kernel ends the loop: the map goes on the last
		// close, whenever that is, with nobody watching for the moment.
		if deferredCmd, deferredErr := r.commands.RemoveDMDeviceDeferred(ctx, dmName); deferredErr != nil {
			r.publishBarrierStall(ctx, lsvg, dmName, deferredErr)
			r.log.Error(deferredErr, fmt.Sprintf("[%s] the lease area %s can be removed neither now nor on close (cmd: %s)",
				ReconcilerName, dmName, deferredCmd))
		} else {
			r.log.Info(fmt.Sprintf("[%s] the lease area %s will be removed when its last opener closes it",
				ReconcilerName, dmName))
		}
		return true
	}

	// Only now, and only if every map went: the file is the record that this
	// node was fenced, and it stops being true when the last error target is
	// gone. Removing it earlier would lose the recovery halfway through.
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		r.log.Warning(fmt.Sprintf("[%s] the barrier result at %s could not be removed: %s",
			ReconcilerName, path, err.Error()))
	}

	r.log.Info(fmt.Sprintf("[%s] %s is ready to rejoin the pool", ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode))
	return false
}

// Reasons a node gives for its own state. They are the machine-readable half of
// what the pool publishes about its members; the message beside them is the half
// meant for a person.
const (
	ReasonLockspaceStarted    = "LockspaceStarted"
	ReasonReservationConflict = "ReservationConflict"
	ReasonLUNNotReadyYet      = "LUNNotReadyYet"
	ReasonLockspaceNotStarted = "LockspaceNotStarted"
	ReasonBarrierNotCleared   = "BarrierNotCleared"
)

// publishNodeState records what this node has to say about its own membership.
//
// It is written with a server-side apply of one array entry, keyed by the node's
// name and owned by a field manager of its own. Every member of a pool writes
// here at the same time, and a merge patch of the whole array would make each
// node's view the last writer's view — the value would be true and useless.
func (r *Reconciler) publishNodeState(
	ctx context.Context,
	lsvg *v1alpha1.LVMSharedVolumeGroup,
	started bool,
	reason, message string,
) {
	// Nothing is written while the answer stays the same. A member re-confirms
	// itself every pass, and a pass that rewrote the timestamp would make every
	// node of every pool a steady write to the API server for no news at all.
	since := metav1.Now()
	if lsvg.Status != nil {
		for _, node := range lsvg.Status.Nodes {
			if node.Name != r.cfg.NodeName {
				continue
			}
			if node.LockspaceStarted == started && node.Reason == reason && node.Message == message {
				return
			}
			break
		}
	}

	entry := map[string]any{
		"name":             r.cfg.NodeName,
		"lockspaceStarted": started,
		"reason":           reason,
		"since":            since.UTC().Format(time.RFC3339),
	}
	if message != "" {
		entry["message"] = message
	}

	patch := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": v1alpha1.SchemeGroupVersion.String(),
		"kind":       "LVMSharedVolumeGroup",
		"metadata":   map[string]any{"name": lsvg.Name},
		"status":     map[string]any{"nodes": []any{entry}},
	}}

	if err := r.cl.Status().Patch(ctx, patch, client.Apply,
		client.FieldOwner("sds-node-configurator-agent-"+r.cfg.NodeName),
		client.ForceOwnership,
	); err != nil {
		r.log.Warning(fmt.Sprintf("[%s] unable to publish the state of %s on this node: %s",
			ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode, err.Error()))
	}
}

// publishLockStartFailure says why the node is not in the pool, and names the
// one cause a retry will never fix.
//
// Most failures here resolve on their own: a LUN still arriving, a lease from
// this node's previous incarnation not yet expired, a group being created a
// moment ago. One does not. A SCSI reservation left on the LUN by another
// initiator lets this node read the volume group and refuses its writes, so the
// delta lease — taken with COMPARE AND WRITE — comes back as a reservation
// conflict, and sanlock reports it as add_lockspace fail result -286. Retrying
// that until somebody notices is the failure mode this exists to prevent.
//
// The reservation is not cleared here on purpose. Clearing somebody else's
// registration is how a pool takes a LUN from a host that may be using it
// legitimately, and this module refuses to make that decision on its own — the
// boundary is the data, and here it runs through an array shared with hosts this
// cluster knows nothing about.
func (r *Reconciler) publishLockStartFailure(
	ctx context.Context,
	lsvg *v1alpha1.LVMSharedVolumeGroup,
	cause error,
) {
	reason, message := ReasonLockspaceNotStarted, cause.Error()

	text := strings.ToLower(cause.Error())
	switch {
	case strings.Contains(text, "reservation conflict") || strings.Contains(text, "result -286"):
		reason = ReasonReservationConflict
		message = "the LUN carries a SCSI reservation from another initiator: the node can read the " +
			"volume group and cannot take its lease, and this will not change on its own. " +
			"Check `sg_persist --in --read-reservation` on a path of the LUN and clear the leftover " +
			"registration from the host that owns the key. This module does not clear it: the LUN may " +
			"belong to somebody who is using it."
	case strings.Contains(text, "lockspace is not started") || strings.Contains(text, "no such device"):
		reason = ReasonLUNNotReadyYet
		message = "the volume group is not readable on this node yet, which is ordinary while the array " +
			"is presenting the LUN: " + cause.Error()
	}

	r.publishNodeState(ctx, lsvg, false, reason, message)
}

// publishBarrierStall says that a fenced node cannot come back on its own.
//
// It is published in one situation, and the situation is rare: the lease area
// is still mapped to an error target, its opener will not close it, and the
// kernel refused to take the removal on close either. Everything reversible has
// been spent by the time this is written, and what remains — restarting the
// lock daemons of this node, or restarting the node — is a decision with a
// blast radius, so it is stated rather than taken.
func (r *Reconciler) publishBarrierStall(
	ctx context.Context,
	lsvg *v1alpha1.LVMSharedVolumeGroup,
	dmName string,
	cause error,
) {
	r.publishNodeState(ctx, lsvg, false, ReasonBarrierNotCleared,
		"the node was fenced and cannot rejoin the pool because the lease area "+dmName+
			" is still mapped to an error target and its opener will not release it ("+cause.Error()+
			"). No volume of this group is at risk — the map holds nothing and every write through it "+
			"fails — but this node stays out of the pool until its lock daemons are restarted.")
}

// retractNodeState removes this node's entry when it leaves the pool.
//
// The same field manager that wrote the entry applies an empty list, and
// server-side apply prunes exactly what that manager owned — which is this
// node's entry and nothing else. Leaving the old answer behind would be worse
// than saying nothing: a reader would see a member that is no longer one.
func (r *Reconciler) retractNodeState(ctx context.Context, lsvg *v1alpha1.LVMSharedVolumeGroup) {
	patch := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": v1alpha1.SchemeGroupVersion.String(),
		"kind":       "LVMSharedVolumeGroup",
		"metadata":   map[string]any{"name": lsvg.Name},
		"status":     map[string]any{"nodes": []any{}},
	}}

	if err := r.cl.Status().Patch(ctx, patch, client.Apply,
		client.FieldOwner("sds-node-configurator-agent-"+r.cfg.NodeName),
		client.ForceOwnership,
	); err != nil {
		r.log.Warning(fmt.Sprintf("[%s] unable to withdraw the state of this node from %s: %s",
			ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode, err.Error()))
	}
}
