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

// This file answers a question every single LVM command has to ask: which loop
// devices on this node are ours?
//
// internal.LVMGlobalFilter rejects /dev/loop* because on a hypervisor a loop
// device is a virtual machine's disk and the LVM inside it is the guest's. The
// agent's own file-backed devices (spec.fileDevices) are loop devices too, so
// they have to be exempted by name — and the exemption has to be exact, because
// the guest's disk is very likely the neighbouring minor.
//
// The set is kept in process memory rather than resolved per invocation. Asking
// losetup on every LVM command would double the number of times the agent enters
// PID 1's namespace, and the agent runs several LVM commands per second per node.
// It stays truthful through two paths that between them cover every way the set
// can change:
//
//   - the commands that change it say so. SetupLoopDevice registers what it just
//     attached and DetachLoopDevice forgets it, both inside the Commands
//     implementation, so the very next LVM command in the same code path — the
//     pvcreate on a loop that was created three lines earlier — already sees it.
//     Nothing at the call sites has to remember to do this.
//   - the scanner reconciles the whole set once per cache fill (RefreshOwnedLoops),
//     which is what covers the cases no in-process bookkeeping can: loops attached
//     by a previous incarnation of the agent before a restart, and an operator
//     attaching a managed backing file by hand.

package utils

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/logger"
)

// ownedLoops is the process-wide registry. Package-level state, deliberately: the
// LVM filter is assembled deep inside lvmStaticExtendedArgs, which every one of
// the ~30 command wrappers calls and none of them has a reason to know about loop
// ownership.
var ownedLoops = struct {
	mu      sync.RWMutex
	devices map[string]struct{}
}{devices: make(map[string]struct{})}

// RememberOwnedLoop records a loop device as this agent's, so LVM commands stop
// filtering it out. Called by SetupLoopDevice for a managed backing file; safe to
// call for a device already known.
func RememberOwnedLoop(loopDev string) {
	if loopDev == "" {
		return
	}
	ownedLoops.mu.Lock()
	defer ownedLoops.mu.Unlock()
	ownedLoops.devices[loopDev] = struct{}{}
}

// RememberLoopIfManaged registers loopDev only when backingPath is a file this agent
// named itself, and is what SetupLoopDevice and FindLoopDeviceByFile call.
//
// A function rather than an inline condition at those two call sites because it is
// the whole ownership decision for the LVM filter: get it wrong in the permissive
// direction and a virtual machine's disk becomes visible to lvm again; get it wrong
// in the strict direction and the agent hides its own file-backed Volume Group from
// itself. Both call sites pass a path the agent is about to attach or has just found,
// never an arbitrary one.
func RememberLoopIfManaged(backingPath, loopDev string) {
	// The empty LVG name matches any managed basename: the question here is "did this
	// agent create the file", not "for which LVMVolumeGroup".
	if loopDev == "" || !IsManagedFileDevicePath(backingPath, "") {
		return
	}
	RememberOwnedLoop(loopDev)
}

// ForgetOwnedLoop drops a loop device from the registry, called when it is
// detached. Keeping a detached minor in the set would exempt whatever the kernel
// hands that number to next — quite possibly a virtual machine's disk.
func ForgetOwnedLoop(loopDev string) {
	if loopDev == "" {
		return
	}
	ownedLoops.mu.Lock()
	defer ownedLoops.mu.Unlock()
	delete(ownedLoops.devices, loopDev)
}

// OwnedLoops returns the registry's contents, sorted so that the filter string
// built from it is stable across invocations. A changing filter would make the
// command line in the log different every time for no reason, and the commands
// the agent runs are compared by eye across scans more often than anything else
// here.
func OwnedLoops() []string {
	ownedLoops.mu.RLock()
	defer ownedLoops.mu.RUnlock()

	out := make([]string, 0, len(ownedLoops.devices))
	for dev := range ownedLoops.devices {
		out = append(out, dev)
	}
	slices.Sort(out)

	return out
}

// LVMGlobalFilterForOwnedLoops is the filter every LVM command the agent runs
// carries: foreign devices rejected, this agent's own loop devices accepted.
func LVMGlobalFilterForOwnedLoops() string {
	return internal.LVMGlobalFilterAcceptingLoops(OwnedLoops())
}

// RefreshOwnedLoops rebuilds the registry from the node's actual loop devices,
// keeping the ones whose backing file this agent named (IsManagedFileDevicePath).
//
// It replaces the set rather than adding to it, which is the point: a loop the
// agent attached, then detached outside this process, has to leave. Failure leaves
// the previous set in place and is reported — an empty set on a transient losetup
// failure would hide the node's own file-backed Volume Groups from the very scan
// that is about to decide whether they still exist.
func RefreshOwnedLoops(ctx context.Context, log logger.Logger, commands Commands, cmdTimeout time.Duration) error {
	type listing struct {
		cmd     string
		entries []internal.LoopDeviceEntry
	}
	res, err := RunWithTimeout(ctx, cmdTimeout, func(ctx context.Context) (listing, error) {
		cmd, entries, err := commands.ListLoopDevices(ctx)
		return listing{cmd: cmd, entries: entries}, err
	})
	log.Trace(res.cmd)
	if err != nil {
		return fmt.Errorf("unable to list the node's loop devices, keeping the %d already known: %w", len(OwnedLoops()), err)
	}

	fresh := make(map[string]struct{}, len(res.entries))
	for _, entry := range res.entries {
		if entry.Device == "" || entry.Backing.Path == "" {
			continue
		}
		// The empty LVG name matches any managed basename: this is asking "did this
		// agent create the file", not "does it belong to a particular LVMVolumeGroup".
		// A deleted backing file still identifies its owner — the loop is live and its
		// PV is still in a Volume Group, so hiding it now would drop that group from
		// the cache.
		if !IsManagedFileDevicePath(entry.Backing.Path, "") {
			continue
		}
		fresh[entry.Device] = struct{}{}
	}

	ownedLoops.mu.Lock()
	previous := len(ownedLoops.devices)
	ownedLoops.devices = fresh
	ownedLoops.mu.Unlock()

	if previous != len(fresh) {
		log.Debug(fmt.Sprintf("[RefreshOwnedLoops] the agent's own loop devices: %s (was %d, now %d)",
			strings.Join(OwnedLoops(), ", "), previous, len(fresh)))
	}

	return nil
}
