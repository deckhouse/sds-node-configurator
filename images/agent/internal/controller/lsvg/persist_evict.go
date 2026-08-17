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
	"fmt"
	"sort"
	"strings"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/controller"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/utils"
)

// EvictNodeAnnotation names a node whose access to the pool's LUNs is to be
// taken away, and it is set by a person.
//
// Eviction is not automatic here, and that is a decision rather than an
// omission: in this release the verdict "that node is gone" belongs to sanlock,
// which reaches it from the lease it can no longer renew, and a second
// mechanism reaching the same verdict from the API server would fence a node
// during an API outage that its storage never noticed.
//
// What is left to a person is the case sanlock cannot see — a node that holds
// its lease and its storage but has stopped serving, so nothing evicts it and
// nothing releases its volumes. The procedure for it is otherwise a hand-typed
// SCSI command repeated across every path of every LUN, in a state where the
// node it is aimed at must not be guessed at. This annotation is that procedure,
// written once and checked.
const EvictNodeAnnotation = "storage.deckhouse.io/evict-node"

// evictRequestedNode carries out an eviction a person asked for.
//
// The registration is removed per path, not on the multipath map: on the map
// every preempt is refused by the library behind mpathpersist, and a
// registration exists per initiator-target pair in any case — a key taken off
// one path leaves the node writing through the rest.
//
// Removing the key stops more than the writes. Under reservations the lease
// itself is renewed with a SCSI command that an unregistered initiator may not
// issue, so sanlock on the evicted node loses its lease next and runs its own
// kill path afterwards. The two mechanisms are in series, and this one comes
// first.
func (r *Reconciler) evictRequestedNode(ctx context.Context, lsvg *v1alpha1.LVMSharedVolumeGroup) {
	target := strings.TrimSpace(lsvg.Annotations[EvictNodeAnnotation])
	if target == "" {
		return
	}

	if target == r.cfg.NodeName {
		// A node does not evict itself: it would be issuing the command with the
		// key it is removing, and it can stand down on its own anyway.
		r.log.Warning(fmt.Sprintf("[%s] %s asks this node to evict itself from %s; ignoring",
			ReconcilerName, EvictNodeAnnotation, lsvg.Name))
		return
	}

	executor := evictionExecutor(lsvg, target)
	if executor == "" {
		// Nobody can carry it out: no member holds a registration to preempt
		// with. Said by one node — the first member that is not the target,
		// which every member computes the same way — and said at all, because
		// silence is indistinguishable from an eviction in progress, and this
		// one will never happen.
		//
		// Not by the metadata owner: the case arises precisely when the owner is
		// the node being evicted, and a node does not act on its own eviction.
		if firstMemberBesides(lsvg, target) == r.cfg.NodeName {
			r.publishNodeState(ctx, lsvg, r.lockspaceStartedInStatus(lsvg), ReasonEvictionImpossible,
				fmt.Sprintf("Node %q is to be evicted, and no member of the pool holds a registration to do it "+
					"with: an eviction is issued by a node the array already accepts. Wait for a member to come "+
					"back under reservations, or take the key off from the array's side.", target))
		}
		return
	}
	if executor != r.cfg.NodeName {
		// One node carries it out, so that three of them do not preempt each
		// other's freshly taken reservation in turn.
		return
	}

	ourKey := r.prKeyOf(lsvg, r.cfg.NodeName)
	if ourKey == "" {
		r.log.Warning(fmt.Sprintf("[%s] cannot evict %s from %s: this node has not reported a reservation key of its own",
			ReconcilerName, target, lsvg.Name))
		return
	}

	targetKey := r.prKeyOf(lsvg, target)
	if targetKey == "" {
		// The key is not known, and a preempt needs one. Nothing here can be
		// inferred from the keys on the LUN: which of them belongs to the node
		// that stopped answering is exactly what is missing, and picking the
		// unfamiliar one would fence whichever node reported last.
		r.log.Error(nil, fmt.Sprintf("[%s] cannot evict %s from %s: that node never reported its reservation key, so there is nothing to preempt by name",
			ReconcilerName, target, lsvg.Name))
		r.publishNodeState(ctx, lsvg, r.lockspaceStartedInStatus(lsvg), ReasonEvictionImpossible,
			fmt.Sprintf("Node %q is to be evicted, but it never reported the reservation key it registers with, "+
				"and a key that is only probably its own would take the wrong node off the LUNs. "+
				"Evict it by the array's own tools, naming the key from the array side.", target))
		return
	}

	if utils.SameRegistrationKey(ourKey, targetKey) {
		// Same key on two nodes is a misconfiguration this must not act on:
		// preempting it would cut off this node together with the target.
		r.log.Error(nil, fmt.Sprintf("[%s] refusing to evict %s from %s: it reports the same reservation key as this node (%s)",
			ReconcilerName, target, lsvg.Name, targetKey))
		return
	}

	r.preemptKeyEverywhere(ctx, lsvg, target, ourKey, targetKey)
}

// preemptKeyEverywhere removes one key from every path of every LUN of the pool,
// and says plainly which paths still carry it afterwards.
func (r *Reconciler) preemptKeyEverywhere(
	ctx context.Context,
	lsvg *v1alpha1.LVMSharedVolumeGroup,
	target, ourKey, targetKey string,
) {
	wwids := make([]string, 0, len(lsvg.Spec.Devices))
	for _, device := range lsvg.Spec.Devices {
		wwids = append(wwids, device.WWID)
	}
	devices, missing, err := utils.ResolveWWIDs(wwids)
	if err != nil {
		r.log.Error(err, fmt.Sprintf("[%s] cannot resolve the LUNs of %s to evict %s", ReconcilerName, lsvg.Name, target))
		return
	}
	if len(missing) > 0 {
		// Evicting from the LUNs that are here would report success while the
		// node keeps writing to the ones that are not.
		r.log.Error(nil, fmt.Sprintf("[%s] cannot evict %s from %s: this node does not see LUNs %s",
			ReconcilerName, target, lsvg.Name, strings.Join(missing, ", ")))
		return
	}

	var remaining []string
	for _, wwid := range utils.SortedWWIDs(devices) {
		for _, path := range devices[wwid].Paths {
			keys, _, err := r.commands.ReadRegistrationKeys(ctx, path)
			if err != nil {
				r.log.Error(err, fmt.Sprintf("[%s] cannot read the registrations on %s", ReconcilerName, path))
				remaining = append(remaining, path)
				continue
			}
			if !containsKey(keys, targetKey) {
				// Already gone. Re-issuing the preempt would be answered with a
				// conflict, which is why this reads before it writes: an
				// eviction that is complete has to look complete when it is
				// repeated.
				continue
			}

			if cmd, err := r.commands.PreemptRegistration(ctx, path, ourKey, targetKey); err != nil {
				r.log.Error(err, fmt.Sprintf("[%s] unable to preempt %s on %s, cmd: %s",
					ReconcilerName, targetKey, path, cmd))
				remaining = append(remaining, path)
				continue
			}
			r.log.Info(fmt.Sprintf("[%s] preempted the registration of %s (%s) on %s",
				ReconcilerName, target, targetKey, path))
		}
	}

	// A key that is simply absent proves less than it looks like. lvm2 derives
	// the key from the host id and the lockspace generation, so a node that
	// restarted its lockspace since it last published is registered under a key
	// nobody here has ever seen — and preempting the published one would then
	// remove nothing while reporting success. So what is left on the LUNs is
	// compared against what the members have published, and anything unaccounted
	// for is named rather than assumed to be nobody's.
	if unknown := r.unaccountedKeys(ctx, lsvg, devices); len(unknown) > 0 {
		r.publishNodeState(ctx, lsvg, r.lockspaceStartedInStatus(lsvg), ReasonEvictionIncomplete,
			fmt.Sprintf("Node %q was evicted by the key it published, and %d key(s) on the pool's LUNs belong to "+
				"nobody that has published one: %s. A node that restarted its lockspace registers under a new key, "+
				"so one of these may still be that node — check them from the array before treating it as fenced.",
				target, len(unknown), strings.Join(unknown, ", ")))
		return
	}

	if len(remaining) > 0 {
		r.publishNodeState(ctx, lsvg, r.lockspaceStartedInStatus(lsvg), ReasonEvictionIncomplete,
			fmt.Sprintf("Node %q is still registered on %d of the pool's paths (%s). "+
				"It can keep writing through those paths, so the eviction has not taken effect.",
				target, len(remaining), strings.Join(remaining, ", ")))
		return
	}

	r.log.Info(fmt.Sprintf("[%s] node %s is no longer registered on any path of %s", ReconcilerName, target, lsvg.Name))
}

// evictionExecutor picks the single node that carries the eviction out.
//
// The pool's metadata owner does it, unless the owner is the node being evicted
// — then the first member that reports a running registration, by name, so that
// every node reaches the same answer without agreeing on anything.
func evictionExecutor(lsvg *v1alpha1.LVMSharedVolumeGroup, target string) string {
	if lsvg.Spec.MetadataOwner != "" && lsvg.Spec.MetadataOwner != target {
		return lsvg.Spec.MetadataOwner
	}

	candidates := make([]string, 0, len(lsvg.Status.Nodes))
	for _, node := range lsvg.Status.Nodes {
		if node.Name == target || node.PersistentReservations == nil {
			continue
		}
		if node.PersistentReservations.State == PRStateEnabled {
			candidates = append(candidates, node.Name)
		}
	}
	sort.Strings(candidates)
	if len(candidates) == 0 {
		return ""
	}
	return candidates[0]
}

// prKeyOf reads the key a member published for itself.
func (r *Reconciler) prKeyOf(lsvg *v1alpha1.LVMSharedVolumeGroup, nodeName string) string {
	for _, node := range lsvg.Status.Nodes {
		if node.Name == nodeName && node.PersistentReservations != nil {
			return node.PersistentReservations.Key
		}
	}
	return ""
}

func containsKey(keys []string, wanted string) bool {
	for _, key := range keys {
		if utils.SameRegistrationKey(key, wanted) {
			return true
		}
	}
	return false
}

// unaccountedKeys lists the registrations on the pool's LUNs that no member has
// claimed by publishing it.
func (r *Reconciler) unaccountedKeys(
	ctx context.Context,
	lsvg *v1alpha1.LVMSharedVolumeGroup,
	devices map[string]utils.SharedDevice,
) []string {
	claimed := make([]string, 0, len(lsvg.Status.Nodes))
	for _, node := range lsvg.Status.Nodes {
		if node.PersistentReservations != nil && node.PersistentReservations.Key != "" {
			claimed = append(claimed, node.PersistentReservations.Key)
		}
	}

	seen := map[string]bool{}
	var unknown []string
	for _, wwid := range utils.SortedWWIDs(devices) {
		for _, path := range devices[wwid].Paths {
			keys, _, err := r.commands.ReadRegistrationKeys(ctx, path)
			if err != nil {
				continue
			}
			for _, key := range keys {
				if containsKey(claimed, key) || seen[key] {
					continue
				}
				seen[key] = true
				unknown = append(unknown, key)
			}
		}
	}
	sort.Strings(unknown)
	return unknown
}

// standDownIfEvicted keeps this node out of a pool it has been evicted from.
//
// An eviction removes the node's registration, and nothing about that stops the
// node from making another one: it is the same command the ordinary pass runs
// whenever it finds its lockspace missing, and the lockspace goes missing
// precisely because the eviction worked. The annotation is what a person set to
// mean "this node must not be in this pool", so it is honoured here until they
// remove it.
//
// It is a refusal to rejoin, not an undoing: the node's volumes were released
// when its lease went, and the pool has moved on.
func (r *Reconciler) standDownIfEvicted(
	ctx context.Context,
	lsvg *v1alpha1.LVMSharedVolumeGroup,
) (controller.Result, bool) {
	if strings.TrimSpace(lsvg.Annotations[EvictNodeAnnotation]) != r.cfg.NodeName {
		return controller.Result{}, false
	}

	r.log.Warning(fmt.Sprintf("[%s] this node is evicted from %s and stays out until %s is removed",
		ReconcilerName, lsvg.Name, EvictNodeAnnotation))
	r.publishNodeState(ctx, lsvg, false, ReasonEvicted, fmt.Sprintf(
		"This node has been evicted from the pool: its registration was removed from the LUNs, so the array "+
			"refuses its writes and its lease cannot be renewed. It will not rejoin while the annotation %s "+
			"names it. Remove the annotation once the node is fit to serve the pool again.", EvictNodeAnnotation))
	return controller.Result{RequeueAfter: groupRecheckInterval}, true
}

// firstMemberBesides names one member deterministically, so that a thing which
// has to be said once is said once.
func firstMemberBesides(lsvg *v1alpha1.LVMSharedVolumeGroup, exclude string) string {
	members := append([]string(nil), lsvg.Spec.Nodes...)
	sort.Strings(members)
	for _, name := range members {
		if name != exclude {
			return name
		}
	}
	return ""
}
