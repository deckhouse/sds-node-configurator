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

package utils

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
)

// SysBlockRoot is a variable so tests can point it at a fixture. Nothing else
// is meant to change it.
var SysBlockRoot = "/sys/block"

// ProcRoot is a variable for the same reason SysBlockRoot is.
var ProcRoot = "/proc"

// LeaseAreaLVName is the hidden volume lvm creates for sanlock's leases. It
// belongs to lvm rather than to the pool, and nothing here may deactivate it.
const LeaseAreaLVName = "lvmlock"

// SharedDevice is one LUN of a shared pool as this node sees it.
type SharedDevice struct {
	WWID string
	// Path is the multipath map, never a single path. A Volume Group built on
	// /dev/sdX loses its physical volume the moment a path flaps.
	Path string
	// LogicalBlockSize and PhysicalBlockSize decide the lease geometry, and the
	// rule is LVM's: 4096 in EITHER of them means 4K geometry. A 512e device
	// and a 4Kn device are therefore the same class, and an honest 512-byte
	// device is a different one.
	LogicalBlockSize  int
	PhysicalBlockSize int
	// DiscardGranularity is what the array frees in one go. An extent size that
	// is not a multiple of it means discarding a volume frees nothing, however
	// honestly the array reports zeroes afterwards.
	DiscardGranularity int
}

// FourKGeometry reports the class this device belongs to, by the rule lvm2
// itself applies when it lays out sanlock leases.
func (d SharedDevice) FourKGeometry() bool {
	return d.LogicalBlockSize == 4096 || d.PhysicalBlockSize == 4096
}

// ResolveWWIDs finds the multipath map of every WWID on this node.
//
// It reads sysfs rather than calling multipath, and the difference is not
// stylistic: the answer has to be the same on every node of the pool, and the
// device-mapper UUID is the only identifier that is. A map created for a LUN
// carries "mpath-<wwid>" there, whatever the map is called locally and whatever
// the paths under it are named.
//
// A WWID with no map yet is not an error — the LUN may still be arriving — so
// it is returned as missing and the caller decides.
func ResolveWWIDs(wwids []string) (found map[string]SharedDevice, missing []string, err error) {
	byWWID, err := mpathDevicesByWWID()
	if err != nil {
		return nil, nil, err
	}

	found = make(map[string]SharedDevice, len(wwids))
	for _, wwid := range wwids {
		device, ok := byWWID[normaliseWWID(wwid)]
		if !ok {
			missing = append(missing, wwid)
			continue
		}
		device.WWID = wwid
		found[wwid] = device
	}

	sort.Strings(missing)
	return found, missing, nil
}

func mpathDevicesByWWID() (map[string]SharedDevice, error) {
	entries, err := os.ReadDir(SysBlockRoot)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", SysBlockRoot, err)
	}

	out := map[string]SharedDevice{}
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), "dm-") {
			continue
		}

		base := filepath.Join(SysBlockRoot, entry.Name())
		uuid, err := readSysAttr(filepath.Join(base, "dm", "uuid"))
		if err != nil || !strings.HasPrefix(uuid, "mpath-") {
			continue
		}

		name, err := readSysAttr(filepath.Join(base, "dm", "name"))
		if err != nil {
			continue
		}

		device := SharedDevice{Path: filepath.Join("/dev/mapper", name)}
		device.LogicalBlockSize, _ = readSysInt(filepath.Join(base, "queue", "logical_block_size"))
		device.PhysicalBlockSize, _ = readSysInt(filepath.Join(base, "queue", "physical_block_size"))
		device.DiscardGranularity, _ = readSysInt(filepath.Join(base, "queue", "discard_granularity"))

		out[normaliseWWID(strings.TrimPrefix(uuid, "mpath-"))] = device
	}

	return out, nil
}

// normaliseWWID makes the comparison independent of how the identifier was
// written down, and the canonical form is multipath's own: the designator type
// digit followed by the hex, lower case.
//
// Two spellings of one LUN reach this module. multipath — and therefore
// /sys/block/dm-*/dm/uuid, and therefore an administrator reading "multipath
// -ll" — writes "36c89…". Device discovery writes "naa.6c89…" into a SCSIDevice,
// and a pool built from those devices carries that spelling. Comparing the
// strings as written makes one of the two silently resolve to nothing: the group
// waits for a LUN that is plainly there, and says only that it is missing.
func normaliseWWID(wwid string) string {
	wwid = strings.ToLower(strings.TrimSpace(wwid))
	for _, prefix := range []string{"naa.", "eui.", "t10."} {
		if after, found := strings.CutPrefix(wwid, prefix); found {
			return designatorDigit(prefix) + after
		}
	}
	return wwid
}

// designatorDigit is the type digit multipath puts in front of the identifier.
// The mapping is SPC's designator type: 1 is T10 vendor id, 2 is EUI-64, 3 is
// NAA.
func designatorDigit(prefix string) string {
	switch prefix {
	case "naa.":
		return "3"
	case "eui.":
		return "2"
	default:
		return "1"
	}
}

func readSysAttr(path string) (string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(raw)), nil
}

func readSysInt(path string) (int, error) {
	raw, err := readSysAttr(path)
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(raw)
}

// CheckSharedDeviceInvariants refuses a set of devices that cannot carry one
// shared Volume Group. Both checks are made BEFORE the group is created,
// because neither can be repaired afterwards: the lease layout and the extent
// size are fixed by vgcreate and outlive every volume in the group.
func CheckSharedDeviceInvariants(devices []SharedDevice, extentSizeBytes int) error {
	if len(devices) == 0 {
		return fmt.Errorf("no devices")
	}

	// One geometry class. sanlock lays its leases out differently for 4K
	// devices, and a group spanning both classes has a layout that is wrong for
	// half of it — which surfaces as corrupted neighbouring leases rather than
	// as an error.
	first := devices[0]
	for _, device := range devices[1:] {
		if device.FourKGeometry() != first.FourKGeometry() {
			return fmt.Errorf(
				"devices of different geometry classes cannot share a volume group: "+
					"%s is %d/%d and %s is %d/%d (logical/physical), and 4096 in either makes a device 4K",
				first.WWID, first.LogicalBlockSize, first.PhysicalBlockSize,
				device.WWID, device.LogicalBlockSize, device.PhysicalBlockSize)
		}
	}

	// The extent size has to be a whole number of unmap granules. Otherwise
	// discarding a volume aligns to nothing the array recognises and frees
	// nothing at all, while reporting success.
	if extentSizeBytes > 0 {
		for _, device := range devices {
			if device.DiscardGranularity > 0 && extentSizeBytes%device.DiscardGranularity != 0 {
				return fmt.Errorf(
					"extent size %d is not a multiple of the unmap granularity %d of %s: "+
						"discard would free nothing, and the extent size cannot be changed after the group is created",
					extentSizeBytes, device.DiscardGranularity, device.WWID)
			}
		}
	}

	return nil
}

// SortedPaths returns the device paths in a stable order, so that a vgcreate
// retried after a failure is the same command as the one that failed.
func SortedPaths(devices map[string]SharedDevice) []string {
	paths := make([]string, 0, len(devices))
	for _, device := range devices {
		paths = append(paths, device.Path)
	}
	sort.Strings(paths)
	return paths
}

// LVIsActiveHere reports whether the logical volume has a device-mapper device
// on this node, which is what "active here" means under lvmlockd.
//
// Sysfs rather than lvm, and the reason is the pool's scarcest resource: every
// lvm command against a shared group takes the group lock, whose throughput is
// flat at 13-21 operations per second no matter how many nodes there are. A
// readiness check is asked far more often than a volume is activated, so putting
// it behind that lock would spend the pool's throughput on questions.
//
// Sysfs rather than the scan cache, for the opposite reason: the cache is filled
// by a scanner with no schedule of its own, so it cannot answer a question about
// something that happened a moment ago — and the question always comes a moment
// after an activation.
func LVIsActiveHere(vgName, lvName string) bool {
	want := DMName(vgName, lvName)

	entries, err := os.ReadDir(SysBlockRoot)
	if err != nil {
		return false
	}

	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), "dm-") {
			continue
		}
		name, err := readSysAttr(filepath.Join(SysBlockRoot, entry.Name(), "dm", "name"))
		if err == nil && name == want {
			return true
		}
	}
	return false
}

// DMName is the name device-mapper gives a logical volume: the group and the
// volume joined by a dash, with any dash inside either of them doubled. Without
// the doubling, "vg-a/lv" and "vg/a-lv" would be the same device.
func DMName(vgName, lvName string) string {
	escape := func(s string) string { return strings.ReplaceAll(s, "-", "--") }
	return escape(vgName) + "-" + escape(lvName)
}

// SharedLVMNamespacePID finds a process whose mount namespace contains an lvm
// that can talk to lvmlockd.
//
// The daemon itself is the marker, which is deliberate: a shared command needs
// lvmlockd running quite apart from needing the binary, so looking for the
// daemon answers both questions at once. When it is not there, the caller has
// nothing to do but wait — and the message says exactly that, instead of lvm's
// "Using a shared lock type requires lvmlockd", which reads as a configuration
// mistake.
func SharedLVMNamespacePID() (int, error) {
	entries, err := os.ReadDir(ProcRoot)
	if err != nil {
		return 0, fmt.Errorf("read %s: %w", ProcRoot, err)
	}

	best := 0
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}

		raw, err := os.ReadFile(filepath.Join(ProcRoot, entry.Name(), "cmdline"))
		if err != nil || len(raw) == 0 {
			// A process that ended between the listing and the read.
			continue
		}

		argv0 := string(bytes.SplitN(raw, []byte{0}, 2)[0])
		if filepath.Base(argv0) != internal.SharedLockDaemonProcess {
			continue
		}
		// The lowest pid of the daemon's processes, so that repeated calls build
		// the same command and a reader comparing two log lines sees the same one.
		if best == 0 || pid < best {
			best = pid
		}
	}

	if best == 0 {
		return 0, fmt.Errorf("no %s process on this node: the lock daemons of the shared pool are not running here",
			internal.SharedLockDaemonProcess)
	}
	return best, nil
}

// ActiveLVsOfGroupHere lists the logical volumes of a group that have a
// device-mapper device on this node.
//
// Sysfs again, and for the same two reasons as LVIsActiveHere: asking lvm would
// spend the pool's group lock on a question, and the scan cache cannot answer a
// question about right now. The hidden lease volume is never returned — it is
// lvm's own and is not a volume of the pool.
func ActiveLVsOfGroupHere(vgName string) ([]string, error) {
	prefix := strings.ReplaceAll(vgName, "-", "--") + "-"

	entries, err := os.ReadDir(SysBlockRoot)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", SysBlockRoot, err)
	}

	var names []string
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), "dm-") {
			continue
		}
		name, err := readSysAttr(filepath.Join(SysBlockRoot, entry.Name(), "dm", "name"))
		if err != nil || !strings.HasPrefix(name, prefix) {
			continue
		}

		// Back from device-mapper's spelling: it doubles every dash so that
		// "vg-a/lv" and "vg/a-lv" cannot collide.
		lvName := strings.ReplaceAll(strings.TrimPrefix(name, prefix), "--", "-")
		if lvName == "" || lvName == LeaseAreaLVName {
			continue
		}
		names = append(names, lvName)
	}

	sort.Strings(names)
	return names, nil
}
