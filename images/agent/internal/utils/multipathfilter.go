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

// This file keeps LVM away from multipath maps that have lost every path, and
// only from those.
//
// Multipath devices must stay visible: a Volume Group on top of a SAN LUN is a
// supported layout, and rejecting device-mapper wholesale would hide those VGs
// from the agent. What cannot be scanned is a map with no paths left. Reading
// from one does not fail — it blocks in the kernel, and the reader blocks with
// it. Measured on a node with 99 pathless maps: two `lvm lvs`/`lvm vgs` stuck in
// read_events and two more in exit_aio for three days and eighteen hours, the
// latter in uninterruptible sleep, so not even SIGKILL removes them.
//
// The cost is not the stalled scan. Each stuck process holds the map open, so
// `multipath -f` cannot remove it (deferred remove waits for the open count to
// reach zero), the CSI driver's NodeUnstageVolume fails, kubelet keeps the volume
// in Node.status.volumesInUse, and the attach/detach controller therefore never
// detaches it. A ReadWriteOnce volume in that state can never move to another
// node: any pod that needs it elsewhere waits in ContainerCreating forever.
//
// A pathless map carries no information for LVM anyway — there is nothing behind
// it to read — so skipping it loses nothing and costs one sysfs read per map.

package utils

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	// multipathUUIDPrefix is what device-mapper puts in dm/uuid for a multipath
	// map. Every other dm target (crypt, linear, thin, ...) uses its own prefix,
	// so this is what separates "a map whose paths are gone" from "a dm device
	// that legitimately has no slaves".
	multipathUUIDPrefix = "mpath-"

	// pathlessMapsTTL bounds how stale the set may be. The filter is rebuilt for
	// every LVM command and the agent runs several per second, so the scan cannot
	// happen per invocation; a map losing its last path is not a sub-second event,
	// so a short TTL is enough. The window it leaves is bounded by the same
	// reasoning that makes the problem survivable at all: the first scan that
	// catches a freshly pathless map hangs, every later one skips it.
	pathlessMapsTTL = 30 * time.Second
)

// sysBlockPath is a variable so tests can point the scan at a fake tree.
var sysBlockPath = "/sys/block"

var pathlessMaps struct {
	mu        sync.Mutex
	devices   []string
	refreshed time.Time
}

// PathlessMultipathDevices returns the device paths of multipath maps that have
// no paths left, both spellings LVM may scan: /dev/dm-N and /dev/mapper/<name>.
//
// The result is cached for pathlessMapsTTL. A read error on a single map is
// ignored rather than propagated: the map is then simply not skipped, which is
// the behaviour before this filter existed, and failing the whole LVM command
// because one sysfs entry disappeared mid-scan would be a worse trade.
func PathlessMultipathDevices() []string {
	pathlessMaps.mu.Lock()
	defer pathlessMaps.mu.Unlock()

	if time.Since(pathlessMaps.refreshed) < pathlessMapsTTL && pathlessMaps.refreshed != (time.Time{}) {
		return pathlessMaps.devices
	}

	devices := scanPathlessMultipathDevices()
	pathlessMaps.devices = devices
	pathlessMaps.refreshed = time.Now()

	return devices
}

// ResetPathlessMultipathCache drops the cached set. The scanner calls it when it
// has reason to believe the picture changed, and tests use it to make the cache
// invisible.
func ResetPathlessMultipathCache() {
	pathlessMaps.mu.Lock()
	defer pathlessMaps.mu.Unlock()

	pathlessMaps.devices = nil
	pathlessMaps.refreshed = time.Time{}
}

func scanPathlessMultipathDevices() []string {
	entries, err := os.ReadDir(sysBlockPath)
	if err != nil {
		return nil
	}

	out := make([]string, 0)
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, "dm-") {
			continue
		}

		uuid, err := os.ReadFile(filepath.Join(sysBlockPath, name, "dm", "uuid"))
		if err != nil || !strings.HasPrefix(strings.TrimSpace(string(uuid)), multipathUUIDPrefix) {
			continue
		}

		slaves, err := os.ReadDir(filepath.Join(sysBlockPath, name, "slaves"))
		if err != nil || len(slaves) > 0 {
			continue
		}

		out = append(out, "/dev/"+name)

		// The mapper alias is what shows up in lvm's own output and in
		// multipath's, so reject it too — LVM scans whichever path it reaches
		// the device by.
		if mapName, err := os.ReadFile(filepath.Join(sysBlockPath, name, "dm", "name")); err == nil {
			if trimmed := strings.TrimSpace(string(mapName)); trimmed != "" {
				out = append(out, "/dev/mapper/"+trimmed)
			}
		}
	}

	sort.Strings(out)

	return out
}
