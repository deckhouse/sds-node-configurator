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

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/utils"
)

// prCheckInterval is how often a node re-establishes what it can say about the
// reservation channel.
//
// Rarely, on purpose. Nothing it looks at changes without somebody changing it:
// the multipath-tools version is fixed when the image is built, the reservation
// key comes from a drop-in an administrator writes, and the pod's network
// namespace is decided by its manifest. A node that asked a minute ago would ask
// again for nothing, and each answer costs two commands in the daemons'
// namespace.
const prCheckInterval = 10 * time.Minute

// prVerdict is what this node last established, and when.
type prVerdict struct {
	at      time.Time
	verdict utils.PRReadiness
}

// persistentReservations is what the node publishes about the reservation
// channel of this pool's LUNs.
//
// It is established by reading and never by trying. Switching a pool to
// reservations is a one-way door in the middle of its own procedure — the volume
// group is unusable between `vgchange --setpersist require` and a successful
// `--persist start` — so the preconditions are answered before anybody opens it,
// not discovered behind it.
func (r *Reconciler) persistentReservations(
	ctx context.Context,
	lsvg *v1alpha1.LVMSharedVolumeGroup,
) *v1alpha1.NodePersistentReservations {
	if cached, found := r.prVerdicts[lsvg.Name]; found && time.Since(cached.at) < prCheckInterval {
		return prStatus(cached.verdict)
	}

	verdict := r.checkPersistentReservations(ctx, lsvg)
	if r.prVerdicts == nil {
		r.prVerdicts = map[string]prVerdict{}
	}
	r.prVerdicts[lsvg.Name] = prVerdict{at: time.Now(), verdict: verdict}

	return prStatus(verdict)
}

func (r *Reconciler) checkPersistentReservations(
	ctx context.Context,
	lsvg *v1alpha1.LVMSharedVolumeGroup,
) utils.PRReadiness {
	missing, err := r.commands.MissingReservationTools(ctx)
	if err != nil {
		// The daemons' mount namespace could not be entered or looked into.
		// Nothing else about the channel is knowable from here.
		r.log.Warning(fmt.Sprintf("[%s] cannot check the reservation tooling for %s: %s",
			ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode, err.Error()))
		return utils.PRReadinessFrom(nil, false, false)
	}
	if len(missing) > 0 {
		return utils.PRReadinessFrom(missing, false, true)
	}

	config, err := r.commands.MultipathConfiguration(ctx)
	if err != nil {
		// multipathd could not be asked. That is what a pod which cannot reach
		// the host's multipathd looks like, and it is not a verdict about the
		// key.
		r.log.Warning(fmt.Sprintf("[%s] cannot read the multipath configuration for %s: %s",
			ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode, err.Error()))
		return utils.PRReadinessFrom(nil, false, false)
	}
	if !utils.ReservationKeyConfigured(config) {
		return utils.PRReadinessFrom(nil, false, true)
	}

	readiness := utils.PRReadinessFrom(nil, true, true)
	readiness.Key = r.reservationKeyOfThisNode(ctx, lsvg)
	return readiness
}

// reservationKeyOfThisNode is the key this node registers with, once it holds a
// registration. It is empty before the pool is switched, and empty is the honest
// answer then: there is no key yet, and a neighbour must not fence by a guess.
func (r *Reconciler) reservationKeyOfThisNode(
	ctx context.Context,
	lsvg *v1alpha1.LVMSharedVolumeGroup,
) string {
	wwids := make([]string, 0, len(lsvg.Spec.Devices))
	for _, device := range lsvg.Spec.Devices {
		wwids = append(wwids, device.WWID)
	}
	devices, missing, err := utils.ResolveWWIDs(wwids)
	if err != nil || len(missing) > 0 {
		// A key read from some of the pool's LUNs says nothing about the rest.
		return ""
	}

	keys := make([]string, 0, len(devices))
	for _, wwid := range utils.SortedWWIDs(devices) {
		answer, err := r.commands.ReservationKeyOf(ctx, utils.MultipathNameOf(devices[wwid].Path))
		if err != nil {
			return ""
		}
		keys = append(keys, utils.KeyOfMap(answer))
	}
	return utils.SingleReservationKey(keys)
}

func prStatus(v utils.PRReadiness) *v1alpha1.NodePersistentReservations {
	return &v1alpha1.NodePersistentReservations{
		Ready:   v.Ready,
		Reason:  v.Reason,
		Message: v.Message,
		Key:     v.Key,
	}
}

// forgetPRVerdict drops the cached answer so the next pass reads it again.
//
// It is called after this node registers, because that is when its key changes:
// lvm2 derives the key from the host id and the lockspace generation, so a node
// that restarts its lockspace comes back with a different one — measured on the
// stand, where host id 1 went from 0x1000000000010001 to 0x1000000000040001. A
// key published for ten more minutes after that is a key a neighbour would fence
// by and miss.
func (r *Reconciler) forgetPRVerdict(lsvg *v1alpha1.LVMSharedVolumeGroup) {
	delete(r.prVerdicts, lsvg.Name)
}
