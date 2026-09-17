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

// This file keeps LVM away from the block devices this cluster has handed to a
// workload, and from everything stacked on top of them.
//
// A device published to a pod as a block volume belongs to that pod, and whatever
// is written inside it is the consumer's data. On a DVP hypervisor the consumer
// is a virtual machine; the cluster inside it builds an LVM Volume Group on the
// volume, and the hypervisor's lvm reads that Volume Group — label, tags and all —
// right through the volume. Both clusters run this module and write the same
// tags, so nothing in the LVM metadata tells the guest's group from the node's.
// Measured on a five-node hypervisor: 108 of 113 LVMVolumeGroups were nested
// clusters' groups, the host activated their thin pools under its own dmeventd,
// and deleting such a resource would have vgremove'd a running guest's storage.
//
// What does tell them apart is kubelet's own bookkeeping. For every block volume
// it publishes, kubelet keeps a device node under
// /var/lib/kubelet/plugins/kubernetes.io/csi/volumeDevices/publish/<pv>/<podUID>
// (and stages the volume under staging/<pv> first). That node names the device
// the cluster gave away, on the node where it gave it away, for any CSI driver,
// with no cooperation from the guest — the guest need not be a Deckhouse cluster
// at all.
//
// The set is applied where the multipath and loop rules already are: as exact
// reject rules in the devices/global_filter every lvm command of the agent
// carries. That is deliberate and is the difference from the first attempt at
// this filter, which pruned the PV list after the scan and then needed a gate on
// each of the six paths that write to the node's LVM — two of which the first
// revision missed. Rejected in lvm's own filter, a guest's Physical Volume is
// never read: it is in no listing, no activation can reach it, vgremove by name
// cannot find it, and two Volume Groups sharing a name no longer poison every
// command's stderr. The post-scan filter in the scanner and the skip in the
// BlockDevice discoverer read the same set and exist for the window between two
// refreshes, not as gates of their own.
//
// The set is walked DOWNWARDS from every published device — its partitions and
// its holders — and never upwards. A guest with root-on-LVM puts its Physical
// Volume on mpathX-part1, not on mpathX, so matching the published name alone
// brought the whole defect back one level down. Upwards is forbidden because the
// slaves of a published Logical Volume are the node's own disks.
//
// Failure direction: an unreadable tree yields an empty set, and an empty set is
// the behaviour the agent had before this file existed. A node that has published
// nothing has no publish directory at all; a node whose kubelet root is elsewhere
// looks the same, and the directory-present metric is what tells them apart.
//
// Rollout order: this file must not ship ahead of the filter for device-mapper
// devices that carry no UUID (a shared LUN's `shlun-*` maps, MR !407). Hiding the
// guests' groups is also what stops hiding those devices behind them: lvm reads
// each one as a duplicate of a Physical Volume it already has, and that warning
// pins itself on every Volume Group of the node. Measured on the test stand: this
// file alone, on a node with a shared LUN, sent the node's own Volume Groups
// into ScanFailed; with the other filter in the same image, none.

package utils

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"

	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
)

const (
	// PublishedSubdir holds one device node per (block volume, consuming pod):
	// `publish/<pvName>/<podUID>`.
	PublishedSubdir = "publish"
	// StagingSubdir holds the volumes staged on the node: `staging/<pvName>`.
	// What a driver leaves there is up to the driver — a device node, a symlink
	// to one, a bind mount of one — and all three stat as a block device.
	StagingSubdir = "staging"

	// publishedDevicesTTL bounds how stale the set may be between the scanner's
	// forced refreshes. The filter is rebuilt for every lvm command and the agent
	// runs several per second, so the tree cannot be walked per invocation; the
	// scanner refreshes it at the start of every cache fill, so this only governs
	// the commands in between.
	publishedDevicesTTL = 30 * time.Second
)

// Package-level knobs, variables so tests can point the scan at a fake tree and
// stat regular files as if they were device nodes.
var (
	publishedDevicesRoot = internal.DefaultCSIVolumeDevicesDirectory
	sysClassBlockPath    = "/sys/class/block"
	sysDevBlockPath      = "/sys/dev/block"
	statDeviceNode       = statBlockDeviceNode
)

// PublishedBlockDevices is the set of block devices kubelet has published to a
// workload on this node, together with everything stacked on top of them.
type PublishedBlockDevices struct {
	kNames map[string]struct{}
	// paths is every spelling the scan found for the devices — /dev/<kname>,
	// /dev/block/<major>:<minor>, /dev/mapper/<name> — because the PV list and the
	// device list spell the same device differently and Has has to answer for all
	// of them.
	paths map[string]struct{}
	// rejects is one path per device, /dev/<kname>, sorted, so the filter string
	// built from it is stable from one command to the next and can be compared by
	// eye in the log. One and not all of paths — see RejectPaths.
	rejects []string
}

// NewPublishedBlockDevices builds a set from kernel names alone, without the
// alias spellings a scan would add. It exists for the tests and for callers that
// already have the answer.
func NewPublishedBlockDevices(kNames ...string) PublishedBlockDevices {
	out := newPublishedSet()
	for _, kName := range kNames {
		if kName == "" {
			continue
		}
		out.add(path.Base(kName), "/dev/"+path.Base(kName))
	}
	out.finish()
	return out
}

func newPublishedSet() PublishedBlockDevices {
	return PublishedBlockDevices{kNames: map[string]struct{}{}, paths: map[string]struct{}{}}
}

func (p *PublishedBlockDevices) add(kName string, devPaths ...string) {
	p.kNames[kName] = struct{}{}
	for _, devPath := range devPaths {
		if devPath != "" {
			p.paths[devPath] = struct{}{}
		}
	}
}

func (p *PublishedBlockDevices) finish() {
	p.rejects = make([]string, 0, len(p.kNames))
	for kName := range p.kNames {
		p.rejects = append(p.rejects, "/dev/"+kName)
	}
	sort.Strings(p.rejects)
}

// Len is how many distinct devices the set holds, descendants included.
func (p PublishedBlockDevices) Len() int { return len(p.kNames) }

// Empty reports whether nothing is published, in which case every caller keeps
// the behaviour it had before this filter existed.
func (p PublishedBlockDevices) Empty() bool { return len(p.kNames) == 0 }

// Has reports whether name is a published device. It takes a bare kernel name
// ("dm-43"), a path ending in one ("/dev/dm-43") or any alias the scan recorded
// ("/dev/mapper/mpathbaq", "/dev/block/253:43"), because the device list and the
// PV list spell the same device differently.
func (p PublishedBlockDevices) Has(name string) bool {
	if name == "" {
		return false
	}
	if _, ok := p.paths[name]; ok {
		return true
	}
	_, ok := p.kNames[path.Base(name)]
	return ok
}

// RejectPaths returns one path per published device — /dev/<kname>, the node
// devtmpfs always has — sorted, for the devices/global_filter builder.
//
// One spelling is enough. lvm applies the filter to every path name it knows
// for a device and rejects the device as soon as one of them matches a reject
// rule and none matches an accept rule; path names that match nothing do not
// take part in the decision (lvm.conf(5), devices/global_filter). The accept
// rules the agent emits name loop devices only, so a published device never has
// an alias that could save it. Checked on a node: rejecting /dev/block/259:1
// alone took /dev/nvme0n1 out of `pvs`.
//
// One and not three, because every rule is compiled into lvm's regex engine on
// every command. Measured on the test stand with 150 published devices: 154
// rules cost `vgs` 30 ms over the 4-rule baseline, 454 rules — the same devices
// under three spellings — cost it 650 ms, on every one of the several commands
// a scan pass runs.
func (p PublishedBlockDevices) RejectPaths() []string { return p.rejects }

// KNames returns the kernel names in the set, sorted, for a log line.
func (p PublishedBlockDevices) KNames() []string {
	out := make([]string, 0, len(p.kNames))
	for kName := range p.kNames {
		out = append(out, kName)
	}
	sort.Strings(out)
	return out
}

var publishedCache struct {
	mu        sync.Mutex
	set       PublishedBlockDevices
	present   bool
	refreshed time.Time

	// What the log has already said, tracked apart from the cache above because
	// the cache is also filled silently. PublishedDevices refreshes it for
	// whichever lvm command finds it stale, and the first such command runs
	// before the scanner does — ActivateAllManagedVGs, at startup — so keying the
	// log on "the cache was empty" said nothing on a node where anything else
	// touched lvm first, which is every node. Keyed on the announcement instead:
	// the first forced refresh always speaks, and a set that changed while only
	// the silent path was looking is still said out loud by the next one.
	announced        bool
	announcedPresent bool
	// announcedRejects is the set as announced. It is the rejects slice of that
	// set, held by reference: a scan builds a new set rather than mutating the
	// one in the cache, so nothing can change it from under this field.
	announcedRejects []string
}

// SetPublishedDevicesRoot points the scan at root, kubelet's CSI volumeDevices
// tree, and drops what was cached for the previous one. An empty root disables
// the recognition entirely.
func SetPublishedDevicesRoot(root string) {
	publishedCache.mu.Lock()
	defer publishedCache.mu.Unlock()
	publishedDevicesRoot = root
	publishedCache.set = PublishedBlockDevices{}
	publishedCache.present = false
	publishedCache.refreshed = time.Time{}
	// A different tree is a different story, so it is told from the beginning.
	publishedCache.announced = false
	publishedCache.announcedPresent = false
	publishedCache.announcedRejects = nil
}

// PublishedDevicesRoot is the tree the scan reads, for a log line.
func PublishedDevicesRoot() string {
	publishedCache.mu.Lock()
	defer publishedCache.mu.Unlock()
	return publishedDevicesRoot
}

// PublishedDevices returns the current set, walking kubelet's tree at most once
// per publishedDevicesTTL. This is what every lvm command's filter reads.
func PublishedDevices() PublishedBlockDevices {
	publishedCache.mu.Lock()
	defer publishedCache.mu.Unlock()

	if !publishedCache.refreshed.IsZero() && time.Since(publishedCache.refreshed) < publishedDevicesTTL {
		return publishedCache.set
	}
	publishedCache.set, publishedCache.present = scanPublishedDevices(publishedDevicesRoot)
	publishedCache.refreshed = time.Now()
	return publishedCache.set
}

// RefreshPublishedDevices walks kubelet's tree now, whatever the cache holds, and
// reports the set and whether the publish directory is there at all. The scanner
// calls it at the start of every cache fill so the filter the pass runs under is
// as fresh as the pass itself; this is also where the set is said out loud.
func RefreshPublishedDevices(log logger.Logger) (set PublishedBlockDevices, directoryPresent bool) {
	publishedCache.mu.Lock()
	defer publishedCache.mu.Unlock()

	publishedCache.set, publishedCache.present = scanPublishedDevices(publishedDevicesRoot)
	publishedCache.refreshed = time.Now()
	announcePublishedDevices(log)

	return publishedCache.set, publishedCache.present
}

// announcePublishedDevices logs where the set is read from and what is in it —
// the first time, and afterwards only when either answer changes. Silence is the
// point: the scanner refreshes on every udev event, and a node that publishes
// nothing new has nothing to say.
//
// Called with publishedCache.mu held.
func announcePublishedDevices(log logger.Logger) {
	first := !publishedCache.announced

	if first || publishedCache.announcedPresent != publishedCache.present {
		if publishedCache.present {
			log.Info(fmt.Sprintf("block volumes published by kubelet are read from %s", filepath.Join(publishedDevicesRoot, PublishedSubdir)))
		} else {
			log.Info(fmt.Sprintf("%s does not exist: no block volume has been published on this node, or kubelet's root is elsewhere; nothing is filtered as published",
				filepath.Join(publishedDevicesRoot, PublishedSubdir)))
		}
	}

	// "none" rather than "0: " with nothing after it: a line that ends in a bare
	// colon reads as a truncated log, and an empty set is the ordinary state of
	// every node that hands no block volume to a workload.
	composition := "none"
	if !publishedCache.set.Empty() {
		composition = fmt.Sprintf("%d: %s", publishedCache.set.Len(), strings.Join(publishedCache.set.KNames(), ", "))
	}
	switch {
	case first:
		log.Info("devices published to workloads as block volumes, with their partitions and holders: " + composition)
	case !slices.Equal(publishedCache.announcedRejects, publishedCache.set.rejects):
		log.Info(fmt.Sprintf("devices published to workloads as block volumes, with their partitions and holders (was %d): %s",
			len(publishedCache.announcedRejects), composition))
	}

	publishedCache.announced = true
	publishedCache.announcedPresent = publishedCache.present
	publishedCache.announcedRejects = publishedCache.set.rejects
}

// ResetPublishedDevicesCache drops the cached set; the next reader walks the tree
// again. Tests use it to make the cache invisible, which is what the TTL does on
// a node.
//
// What the log has already said is deliberately left alone: expiry is not an
// event, and a set that is found again unchanged is not news. Use
// SetPublishedDevicesRoot to start a test from silence.
func ResetPublishedDevicesCache() {
	publishedCache.mu.Lock()
	defer publishedCache.mu.Unlock()
	publishedCache.set = PublishedBlockDevices{}
	publishedCache.present = false
	publishedCache.refreshed = time.Time{}
}

// scanPublishedDevices walks root's publish and staging subtrees and returns the
// devices found there plus everything stacked on them.
//
// Per-entry failures are skipped, not propagated: the tree changes under the
// walk every time a pod starts or stops, and one racing entry must not cost the
// whole set. The scan is silent for the same reason the pathless-map scan is —
// it runs for every lvm command — and the scanner's forced refresh is where the
// result is said out loud.
func scanPublishedDevices(root string) (PublishedBlockDevices, bool) {
	out := newPublishedSet()
	if root == "" {
		out.finish()
		return out, false
	}

	publishDir := filepath.Join(root, PublishedSubdir)
	info, err := os.Stat(publishDir)
	present := err == nil && info.IsDir()

	var roots []string
	// publish/<pv>/<podUID>: a device node per consuming pod.
	roots = append(roots, deviceNodesUnder(publishDir, 2)...)
	// staging/<pv>: the volume staged on the node, a node, a symlink or a bind
	// mount at that path or right under it.
	roots = append(roots, deviceNodesUnder(filepath.Join(root, StagingSubdir), 2)...)

	for _, kName := range roots {
		if _, seen := out.kNames[kName]; seen {
			continue
		}
		out.add(kName, deviceAliases(kName)...)
	}
	addDescendants(&out, roots)
	out.finish()

	return out, present
}

// deviceNodesUnder returns the kernel names of the block device nodes found under
// dir, walking depth levels down. A directory entry is checked itself and — when
// depth allows — descended into; symlinks are followed, so a driver that leaves a
// link to /dev/mapper/<name> at the staging path counts too.
func deviceNodesUnder(dir string, depth int) []string {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}

	var out []string
	for _, entry := range entries {
		entryPath := filepath.Join(dir, entry.Name())
		if major, minor, isBlock, err := statDeviceNode(entryPath); err == nil && isBlock {
			if kName, err := kernelName(major, minor); err == nil {
				out = append(out, kName)
			}
			continue
		}
		if depth > 1 {
			out = append(out, deviceNodesUnder(entryPath, depth-1)...)
		}
	}
	return out
}

// statBlockDeviceNode is the production statDeviceNode: whether p is (or points
// at) a block device node and, if so, its major and minor numbers.
//
// unix.Stat rather than os.Stat plus a type assertion on FileInfo.Sys: that
// assertion has to name *syscall.Stat_t, and *unix.Stat_t — the identical struct
// from another package — is a different type the assertion silently fails on.
// Stat, not Lstat: a symlink to the device is evidence of the same publication.
func statBlockDeviceNode(p string) (major, minor uint32, isBlockDevice bool, err error) {
	var st unix.Stat_t
	if err := unix.Stat(p, &st); err != nil {
		return 0, 0, false, err
	}
	if st.Mode&unix.S_IFMT != unix.S_IFBLK {
		return 0, 0, false, nil
	}
	//nolint:unconvert // Rdev is uint64 on amd64 and uint32 on some other arches.
	rdev := uint64(st.Rdev)
	return unix.Major(rdev), unix.Minor(rdev), true, nil
}

// kernelName turns major:minor into the kernel name through /sys/dev/block.
func kernelName(major, minor uint32) (string, error) {
	target, err := os.Readlink(filepath.Join(sysDevBlockPath, fmt.Sprintf("%d:%d", major, minor)))
	if err != nil {
		return "", err
	}
	return path.Base(target), nil
}

// deviceAliases lists the spellings under which the device list and the PV list
// may report kName, from sysfs: /dev/<kName> always, /dev/block/<major>:<minor>
// when the `dev` file is readable, /dev/mapper/<name> for a device-mapper device.
// They feed Has, which the post-scan PV filter and the BlockDevice discoverer
// ask with whatever spelling their listing used; the lvm filter itself needs one
// of them only (RejectPaths).
func deviceAliases(kName string) []string {
	dir := filepath.Join(sysClassBlockPath, kName)
	out := []string{"/dev/" + kName}

	if raw, err := os.ReadFile(filepath.Join(dir, "dev")); err == nil {
		if numbers := strings.TrimSpace(string(raw)); numbers != "" {
			out = append(out, "/dev/block/"+numbers)
		}
	}
	if raw, err := os.ReadFile(filepath.Join(dir, "dm", "name")); err == nil {
		if name := strings.TrimSpace(string(raw)); name != "" {
			out = append(out, "/dev/mapper/"+name)
		}
	}
	return out
}

// sysfsDescendants lists what sits directly on top of kName: its partitions —
// subdirectories carrying a `partition` file, which keeps nvme, mmcblk and dm
// naming out of it — and the entries of its holders directory, which is where a
// kpartx partition of a multipath map and a guest's Logical Volume still mapped
// on the host both appear.
func sysfsDescendants(kName string) []string {
	dir := filepath.Join(sysClassBlockPath, kName)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}

	var out []string
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		if _, err := os.Stat(filepath.Join(dir, entry.Name(), "partition")); err == nil {
			out = append(out, entry.Name())
		}
	}
	if holders, err := os.ReadDir(filepath.Join(dir, "holders")); err == nil {
		for _, holder := range holders {
			out = append(out, holder.Name())
		}
	}
	return out
}

// addDescendants walks down from every root and adds what it finds. Breadth-first
// over the set itself as the visited marker, so a device under two published
// parents is walked once and a sysfs tree that somehow loops cannot spin here.
func addDescendants(set *PublishedBlockDevices, roots []string) {
	queue := append([]string(nil), roots...)
	for len(queue) > 0 {
		kName := queue[0]
		queue = queue[1:]
		for _, child := range sysfsDescendants(kName) {
			if child == "" {
				continue
			}
			if _, seen := set.kNames[child]; seen {
				continue
			}
			set.add(child, deviceAliases(child)...)
			queue = append(queue, child)
		}
	}
}

// FilterPublishedPVs returns a copy of pvs without the Physical Volumes that sit
// on a published device.
//
// With the same set in lvm's own filter this is a second line, not the first: it
// catches a PV the scan read in the window between the device being published
// and the filter learning of it. It matches the PV's path against the recorded
// spellings and, failing that, its basename against the kernel names — no host
// command, no resolver. A PV reported under an alias the set does not know is
// kept, which is the safe direction here: a Volume Group of the node's own must
// never vanish over a guess, and the next pass rejects the device in lvm anyway.
func FilterPublishedPVs(log logger.Logger, pvs []internal.PVData, published PublishedBlockDevices) []internal.PVData {
	if published.Empty() {
		return pvs
	}
	log = log.Named("FilterPublishedPVs")

	out := make([]internal.PVData, 0, len(pvs))
	for _, pv := range pvs {
		if published.Has(pv.PVName) {
			log.Info(fmt.Sprintf("dropping PV %q: its device is published to a workload as a block volume, so its content belongs to that consumer (VG=%q VG_UUID=%q)",
				pv.PVName, pv.VGName, pv.VGUuid))
			continue
		}
		out = append(out, pv)
	}
	return out
}
