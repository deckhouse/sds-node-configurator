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
		return utils.PRReadinessFrom(nil, nil, false)
	}
	if len(missing) > 0 {
		return utils.PRReadinessFrom(missing, nil, true)
	}

	wwids := make([]string, 0, len(lsvg.Spec.Devices))
	for _, device := range lsvg.Spec.Devices {
		wwids = append(wwids, device.WWID)
	}
	devices, missingLUNs, err := utils.ResolveWWIDs(wwids)
	if err != nil || len(missingLUNs) > 0 {
		// The LUNs are not here yet. That is not a verdict on the channel, so
		// the previous answer — if any — stands rather than being replaced by a
		// worse one for an unrelated reason.
		return utils.PRReadinessFrom(nil, nil, true)
	}

	var withoutKey, keys []string
	for _, wwid := range wwids {
		device, resolved := devices[wwid]
		if !resolved {
			continue
		}
		name := utils.MultipathNameOf(device.Path)
		key, err := r.commands.ReservationKeyOf(ctx, name)
		if err != nil {
			r.log.Warning(fmt.Sprintf("[%s] cannot read the reservation key of %s: %s",
				ReconcilerName, name, err.Error()))
			return utils.PRReadinessFrom(nil, nil, false)
		}
		if !utils.ReservationKeyConfigured(key) {
			withoutKey = append(withoutKey, name)
			continue
		}
		keys = append(keys, key)
	}

	readiness := utils.PRReadinessFrom(nil, withoutKey, true)
	readiness.Key = utils.SingleReservationKey(keys)
	return readiness
}

func prStatus(v utils.PRReadiness) *v1alpha1.NodePersistentReservations {
	return &v1alpha1.NodePersistentReservations{
		Ready:   v.Ready,
		Reason:  v.Reason,
		Message: v.Message,
		Key:     v.Key,
	}
}
