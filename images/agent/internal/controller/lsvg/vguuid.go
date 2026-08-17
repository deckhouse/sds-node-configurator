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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// vgUUIDFileName is where the fencing handler looks up the identity of the
// Volume Group it has been asked to fence.
//
// sanlock hands the handler a NAME and nothing else, and by the time it runs the
// storage is gone — so `vgs` cannot be asked. The maps of the group are found by
// their device-mapper UUID, which begins with the group's UUID, and that mapping
// from name to UUID has to be on local disk before it is needed.
const vgUUIDFileName = "vg-uuid.json"

// rememberVGUUID keeps this node's name-to-UUID map for the fencing handler
// current.
//
// It exists because the barrier was inert without it. Measured on a live pool:
// a node lost its lease, sanlock ran the handler at exactly 8 x io_timeout, and
// the handler answered "vg-uuid.json has no entry for vgext" — mapsFound 0,
// mapsCovered 0, complete false. The volume stayed writable on a node that had
// just been told it may no longer write, which is the one outcome the whole
// design exists to prevent. The file held one entry, for another pool, with a
// UUID from an incarnation of it that no longer existed: nothing in the module
// had ever written it.
//
// So it is written on the pass that knows the answer, and rewritten only when
// the answer changes. A group whose UUID cannot be read is left alone rather
// than recorded as empty: an entry that says "this group has no identity" would
// send the handler looking for maps prefixed with nothing at all.
func (r *Reconciler) rememberVGUUID(vgName, vgUUID string) {
	if vgName == "" || vgUUID == "" {
		return
	}

	path := filepath.Join(r.cfg.HostIDDir, vgUUIDFileName)

	known := map[string]string{}
	if raw, err := os.ReadFile(path); err == nil {
		if err := json.Unmarshal(raw, &known); err != nil {
			// Unreadable, and rewriting it from one group's knowledge would drop
			// every other pool on this node. Said out loud and left alone.
			r.log.Warning(fmt.Sprintf("[%s] %s cannot be read (%s); the fencing handler may not find its maps",
				ReconcilerName, path, err.Error()))
			return
		}
	}

	if known[vgName] == vgUUID {
		return
	}

	known[vgName] = vgUUID
	r.writeVGUUIDs(path, known, fmt.Sprintf("recorded %s=%s for the fencing handler", vgName, vgUUID))
}

// forgetVGUUID drops a group from the map once it is gone. A stale entry is not
// harmless: it points the handler at a UUID that no longer exists, so it would
// find no maps and report a barrier it never raised.
func (r *Reconciler) forgetVGUUID(vgName string) {
	if vgName == "" {
		return
	}

	path := filepath.Join(r.cfg.HostIDDir, vgUUIDFileName)
	raw, err := os.ReadFile(path)
	if err != nil {
		return
	}

	known := map[string]string{}
	if err := json.Unmarshal(raw, &known); err != nil {
		return
	}
	if _, found := known[vgName]; !found {
		return
	}

	delete(known, vgName)
	r.writeVGUUIDs(path, known, fmt.Sprintf("dropped %s from the fencing handler's map", vgName))
}

func (r *Reconciler) writeVGUUIDs(path string, known map[string]string, what string) {
	body, err := json.Marshal(known)
	if err != nil {
		r.log.Warning(fmt.Sprintf("[%s] unable to encode %s: %s", ReconcilerName, path, err.Error()))
		return
	}

	if err := os.MkdirAll(r.cfg.HostIDDir, 0o755); err != nil {
		r.log.Warning(fmt.Sprintf("[%s] unable to create %s: %s", ReconcilerName, r.cfg.HostIDDir, err.Error()))
		return
	}

	// Written through a temporary file and renamed: the handler reads this while
	// the node is losing its storage, and a half-written map is worse than an
	// old one.
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, body, 0o644); err != nil {
		r.log.Warning(fmt.Sprintf("[%s] unable to write %s: %s", ReconcilerName, tmp, err.Error()))
		return
	}
	if err := os.Rename(tmp, path); err != nil {
		r.log.Warning(fmt.Sprintf("[%s] unable to replace %s: %s", ReconcilerName, path, err.Error()))
		_ = os.Remove(tmp)
		return
	}

	r.log.Info(fmt.Sprintf("[%s] %s", ReconcilerName, what))
}
