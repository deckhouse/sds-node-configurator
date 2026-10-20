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

// This file keeps LVM away from multipath maps that cannot be read, and only
// from those.
//
// Multipath devices must stay visible: a Volume Group on top of a SAN LUN is a
// supported layout, and rejecting device-mapper wholesale would hide those VGs
// from the agent. What cannot be scanned is a map with no usable path. Reading
// from one does not fail — it blocks in the kernel, and the reader blocks with
// it. Measured on a node with 99 pathless maps: two `lvm lvs`/`lvm vgs` stuck in
// read_events and two more in exit_aio for three days and eighteen hours, the
// latter in uninterruptible sleep, so not even SIGKILL removes them.
//
// Two shapes of map block a reader. One has no slaves at all: every path device
// is gone and the map outlived them. The other still lists its slaves, but every
// one of them is failed — the LUN was unmapped on the array or the transport
// session dropped while the map existed — and with queue_if_no_path, which the
// usual no_path_retry setting turns on, the kernel holds every read until a
// path comes back. sysfs tells the first shape apart (an empty slaves
// directory); only the multipath target's own status report tells the second,
// so the scan asks dmsetup for it. Seen on a client cluster: `lvm lvs` stuck in
// read_events on such a map for over an hour, holding the next map open.
//
// The cost is not the stalled scan. Each stuck process holds every map it had
// opened before blocking, so `multipath -f` cannot remove them (deferred remove
// waits for the open count to reach zero), the CSI driver's NodeUnstageVolume
// fails, kubelet keeps the volume in Node.status.volumesInUse, and the
// attach/detach controller therefore never detaches it. A ReadWriteOnce volume
// in that state can never move to another node: any pod that needs it elsewhere
// waits in ContainerCreating forever.
//
// Such a map carries no information for LVM anyway — there is nothing behind it
// to read — so skipping it loses nothing.

package utils

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
)

const (
	// multipathUUIDPrefix is what device-mapper puts in dm/uuid for a multipath
	// map. Every other dm target (crypt, linear, thin, ...) uses its own prefix,
	// so this is what separates "a map whose paths are gone" from "a dm device
	// that legitimately has no slaves".
	multipathUUIDPrefix = "mpath-"

	// unreadableMapsTTL bounds how stale the set may be. The filter is rebuilt
	// for every LVM command and the agent runs several per second, so the scan
	// cannot happen per invocation; a map losing its last path is not a
	// sub-second event, so a short TTL is enough. The window it leaves is bounded
	// by the same reasoning that makes the problem survivable at all: the first
	// scan that catches a freshly unreadable map hangs, every later one skips it.
	unreadableMapsTTL = 30 * time.Second

	// multipathStatusTimeout bounds the status report. dmsetup only talks to the
	// device-mapper ioctl interface and issues no I/O to the maps, so it does not
	// share their fate; the bound is against the host being unable to run
	// anything at all, which must not stall the scan that is about to decide the
	// filter.
	multipathStatusTimeout = 10 * time.Second
)

// sysBlockPath is a variable so tests can point the scan at a fake tree.
var sysBlockPath = "/sys/block"

// multipathStatus returns the multipath target's status report for every map on
// the host, one `<name>: <status>` line per map. A variable so tests can feed a
// fixed report.
var multipathStatus = func() ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), multipathStatusTimeout)
	defer cancel()

	args := nsentrerExpendedArgs(internal.DMSetupCmd, "status", "--target", "multipath")
	return exec.CommandContext(ctx, internal.NSENTERCmd, args...).Output()
}

// pathDeviceToken matches the <major>:<minor> of a path device in the status
// report; the token right after it is the path's state, A for active, F for
// failed.
var pathDeviceToken = regexp.MustCompile(`^\d+:\d+$`)

var unreadableMaps struct {
	mu        sync.Mutex
	devices   []string
	refreshed time.Time
}

// UnreadableMultipathDevices returns the device paths of multipath maps that
// cannot be read — no slaves left, or not one active path among them — in both
// spellings LVM may scan: /dev/dm-N and /dev/mapper/<name>.
//
// The result is cached for unreadableMapsTTL. A read error on a single map is
// ignored rather than propagated: the map is then simply not skipped, which is
// the behaviour before this filter existed, and failing the whole LVM command
// because one sysfs entry disappeared mid-scan would be a worse trade. The same
// goes for a status report the host could not produce: the scan then falls back
// to what sysfs alone can tell.
func UnreadableMultipathDevices() []string {
	unreadableMaps.mu.Lock()
	defer unreadableMaps.mu.Unlock()

	if time.Since(unreadableMaps.refreshed) < unreadableMapsTTL && unreadableMaps.refreshed != (time.Time{}) {
		return unreadableMaps.devices
	}

	devices := scanUnreadableMultipathDevices()
	unreadableMaps.devices = devices
	unreadableMaps.refreshed = time.Now()

	return devices
}

// ResetUnreadableMultipathCache drops the cached set. The scanner calls it when
// it has reason to believe the picture changed, and tests use it to make the
// cache invisible.
func ResetUnreadableMultipathCache() {
	unreadableMaps.mu.Lock()
	defer unreadableMaps.mu.Unlock()

	unreadableMaps.devices = nil
	unreadableMaps.refreshed = time.Time{}
}

func scanUnreadableMultipathDevices() []string {
	entries, err := os.ReadDir(sysBlockPath)
	if err != nil {
		return nil
	}

	withoutActivePath := map[string]struct{}{}
	if report, err := multipathStatus(); err == nil {
		withoutActivePath = mapsWithoutActivePath(report)
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

		mapName := ""
		if raw, err := os.ReadFile(filepath.Join(sysBlockPath, name, "dm", "name")); err == nil {
			mapName = strings.TrimSpace(string(raw))
		}

		slaves, err := os.ReadDir(filepath.Join(sysBlockPath, name, "slaves"))
		if err != nil {
			continue
		}
		_, noActivePath := withoutActivePath[mapName]
		if len(slaves) > 0 && !noActivePath {
			continue
		}

		out = append(out, "/dev/"+name)

		// The mapper alias is what shows up in lvm's own output and in
		// multipath's, so reject it too — LVM scans whichever path it reaches
		// the device by.
		if mapName != "" {
			out = append(out, "/dev/mapper/"+mapName)
		}
	}

	sort.Strings(out)

	return out
}

// mapsWithoutActivePath reads the multipath target's status report and returns
// the names of the maps in which no path is active.
//
// A line reads `<name>: <start> <length> multipath <features...> <handler...>
// <groups...>`, and inside the groups every path is reported as
// `<major>:<minor> <A|F> <fail count> ...`. Nothing else in the line looks like a
// <major>:<minor>, so a map is alive exactly when one such token is followed by
// A. A map with no paths at all has no such token and counts as unreadable,
// which agrees with what sysfs says about it.
func mapsWithoutActivePath(report []byte) map[string]struct{} {
	out := map[string]struct{}{}
	for _, line := range strings.Split(string(report), "\n") {
		name, rest, found := strings.Cut(line, ": ")
		if !found {
			continue
		}
		name = strings.TrimSpace(name)
		fields := strings.Fields(rest)
		target := slices.Index(fields, "multipath")
		if name == "" || target < 0 {
			continue
		}

		alive := false
		for i := target + 1; i+1 < len(fields); i++ {
			if pathDeviceToken.MatchString(fields[i]) && fields[i+1] == "A" {
				alive = true
				break
			}
		}
		if !alive {
			out[name] = struct{}{}
		}
	}

	return out
}
