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
	"time"

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

	for _, dmName := range res.CoveredMaps {
		if cmd, err := r.commands.RemoveDMDevice(ctx, dmName); err != nil {
			// Left for the next pass rather than treated as fatal: a map that is
			// still open belongs to something that has not let go yet, and the
			// error target under it is already refusing every write.
			r.log.Warning(fmt.Sprintf("[%s] the error target %s could not be removed (cmd: %s): %s",
				ReconcilerName, dmName, cmd, err.Error()))
			return true
		}
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
