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

package internal

import (
	"regexp"
	"strings"
)

const (
	// LVGUpdateTriggerLabel if you change this value, you must change its value in controller/pkg/block_device_labels_watcher.go as well
	LVGUpdateTriggerLabel = "storage.deckhouse.io/update-trigger"

	PartType                     = "part"
	MultiPathType                = "mpath"
	CDROMDeviceType              = "rom"
	DRBDName                     = "/dev/drbd"
	RBDName                      = "/dev/rbd"
	NBDName                      = "/dev/nbd"
	LoopDeviceType               = "loop"
	LVMDeviceType                = "lvm"
	LVMFSType                    = "LVM2_member"
	MultiPathMemberFSType        = "mpath_member"
	SdsNodeConfiguratorFinalizer = "storage.deckhouse.io/sds-node-configurator"
	LVMVGHealthOperational       = "Operational"
	LVMVGHealthNonOperational    = "NonOperational"
	BlockDeviceValidSize         = "1G"
	NSENTERCmd                   = "/opt/deckhouse/sds/bin/nsenter"
	DMSetupCmd                   = "/opt/deckhouse/sds/bin/dmsetup"
	LSBLKCmd                     = "/opt/deckhouse/sds/bin/lsblk.dynamic"
	LVMCmd                       = "/opt/deckhouse/sds/bin/lvm"

	// SharedLVMCmd is lvm inside the lock daemons' image, and it is a different
	// binary from LVMCmd on purpose — not a preference but the only one that
	// works.
	//
	// The lvm this module ships is configured "--enable-static_link
	// --disable-readline --enable-blkid_wiping" and nothing else: it has no
	// lvmlockd support compiled in at all, so every command against a shared
	// group answers "Using a shared lock type requires lvmlockd" no matter what
	// is running on the node. The image that carries lvmlockd carries an lvm
	// built against it, and a node has no lvm of its own to fall back on.
	//
	// It also removes a failure mode rather than adding one: the client and the
	// daemon are then the same build, so they cannot disagree about the protocol
	// between them.
	SharedLVMCmd = "/usr/sbin/lvm"

	// SharedLockCtlCmd asks the lock manager what it is actually holding. It
	// lives beside lvm in the daemons' image, and it is the only source that can
	// answer whether a lockspace is running: this module's own annotations say
	// what it believes, and a daemon restart makes belief and fact diverge
	// without a word to anybody.
	SharedLockCtlCmd = "/usr/sbin/lvmlockctl"

	// SharedMultipathdCmd answers whether a map has a reservation key, and it is
	// run in the HOST's mount namespace rather than the lock daemons'.
	//
	// multipathd is the host's: it owns the maps, it holds the keys in
	// /etc/multipath/prkeys, and it is what re-registers a path that comes back.
	// The daemons' image does not carry it and should not — nothing in the pool
	// asks multipathd to do anything, it is only asked what it already knows.
	SharedMultipathdCmd = "/usr/sbin/multipathd"

	// SharedSgPersistCmd is how a registration is taken away from a node that
	// cannot be asked to give it up.
	//
	// The library behind mpathpersist compares --param-rk against a key it reads
	// itself and gets zero, so every preempt on a multipath map is refused —
	// measured on two versions of multipath-tools, from the container and from
	// the host, with the key set every way there is. sg_persist on a single path
	// does the same operation in a third of a second.
	SharedSgPersistCmd = "/usr/bin/sg_persist"

	// SharedLvmPersistCmd is the script lvm2 runs for every `vgchange --persist`
	// and `--setpersist`, by a path compiled into it. It is checked rather than
	// called: a pool that cannot find it fails in the middle of the one-way
	// door, and the node can say so beforehand instead.
	SharedLvmPersistCmd = "/usr/sbin/lvmpersist"

	// SharedLockDaemonProcess is how the mount namespace holding SharedLVMCmd is
	// found: by the daemon that must be running for any of this to mean anything.
	SharedLockDaemonProcess = "lvmlockd"

	// SharedLockDaemonsStateDir is the only channel between this agent and the
	// lock daemons of a shared pool. They run from an image pinned to OnDelete
	// and have no API access at all — a token there would make every change to
	// them an operation with a drain — so the host_id goes in through a file and
	// the result of a fencing barrier comes back out through one.
	SharedLockDaemonsStateDir = "/opt/deckhouse/sds/lvmlockd"
	ThinDumpCmd               = "thin_dump"

	// The commands below are the node's own, not the module's. Everything above
	// ships in the agent image under /opt/deckhouse/sds/bin precisely so the
	// agent does not depend on what the host happens to have installed;
	// spec.fileDevices cannot do the same, because attaching a loop device and
	// preallocating a file have to happen in PID 1's mount namespace with the
	// host's own tooling.
	//
	// They are named here rather than spelled out at each call site for the same
	// reason the four above are: a path that appears seven times as a literal is
	// a path nobody can move.
	//
	// LosetupCmd carries the only hard version requirement the feature adds:
	// `--nooverlap` is util-linux 2.29 (2016) or newer. On an older host every
	// provisioning attempt fails with "unrecognized option", which reaches the
	// operator as a bare FileDeviceNotApplied. See docs/FAQ.md.
	LosetupCmd   = "/sbin/losetup"
	FallocateCmd = "/usr/bin/fallocate"
	StatCmd      = "/usr/bin/stat"
	MkdirCmd     = "/bin/mkdir"
	RmCmd        = "/bin/rm"

	// LVMGlobalFilter is passed via `lvm --config` for every LVM
	// subcommand the agent runs. It rejects canonical names of block
	// devices that always belong to a foreign storage layer (Ceph RBD,
	// DRBD, NBD) so lvm does not even read PV labels from them
	// when udev integration is unavailable.
	//
	// Loop devices are rejected too, and this filter is therefore the
	// FALLBACK form — the one used when the agent owns no loop device.
	// Build the filter through LVMGlobalFilterAcceptingLoops instead
	// wherever the agent's own file-backed devices have to be visible;
	// utils.LVMGlobalFilterForOwnedLoops does that from the registry of
	// loops the agent attached itself.
	//
	// Why the loop rule has to be here at all: on a hypervisor /dev/loopN
	// is a block-mode PersistentVolume handed to a virtual machine, and
	// the LVM inside it is the guest's. Without the rule the agent reads
	// the guests' Volume Groups as if they were the node's — the guests'
	// names collide with each other and with the node's own, lvm then
	// warns about that on every single invocation and archives metadata
	// on reads, and the node's own storage goes offline through channels
	// that have nothing to do with adoption. The module's own
	// NodeGroupConfiguration already puts this rule in the host's
	// /etc/lvm/lvm.conf; the agent's --config overrides that file, so
	// dropping the rule here silently un-does the node configuration the
	// module itself shipped.
	//
	// There is intentionally no blanket "a|.*|" accept rule. When a
	// device matches none of the reject patterns, LVM accepts it by
	// default. Adding an explicit accept-all rule would override LVM's
	// built-in device filter and cause it to scan non-standard paths
	// (e.g. /dev/disk/by-diskseq/*), surfacing duplicate VG names when
	// the same PV is visible through multiple aliases and breaking
	// commands like lvremove that address LVs by VG name.
	//
	// The authoritative foreign-PV filter (FilterForeignPVs) still runs
	// after lvm returns and catches any PVs that slip through
	// via /dev/block/MAJ:MIN or /dev/disk/by-id/... aliases.
	LVMGlobalFilter = `devices/global_filter=[` + lvmForeignDeviceRejects + `]`

	// lvmForeignDeviceRejects is the reject-rule body shared by every form
	// of the filter. Order matters only against the accept rules that may
	// precede it: LVM takes the first matching rule.
	lvmForeignDeviceRejects = `"r|^/dev/rbd|","r|^/dev/drbd|","r|^/dev/nbd|","r|^/dev/loop|"`

	// LVMArchiveRetention caps the size of /etc/lvm/archive: keep at
	// most the last 10 metadata snapshots and at most 7 days of history.
	// This only affects new metadata-changing operations; existing
	// archives must be pruned manually on impacted nodes.
	LVMArchiveRetention = `backup/retain_min=10 backup/retain_days=7`

	// SharedLVMNoArchive turns lvm's metadata archive off for commands against a
	// shared Volume Group, and it is a requirement rather than a preference.
	//
	// Those commands run in the mount namespace of the lock daemons (see
	// SharedLVMCmd), whose root filesystem is read-only: lvm writes /etc/lvm/archive
	// BEFORE touching metadata, so with the archive on, every write fails — and
	// fails in a way that names the operation rather than the cause, as
	// "Failed to create sanlock lv lvmlock in vg <name>".
	//
	// Nothing is lost by it. The metadata of a shared group lives on the LUN, where
	// every member reads it; the archive is a local convenience, and a local
	// convenience written into a container that is thrown away on the next restart
	// is not one.
	SharedLVMNoArchive = `backup/archive=0 backup/backup=0`

	TypeVGConfigurationApplied = "VGConfigurationApplied"
	TypeVGReady                = "VGReady"

	AllocationLimitDefaultValue = "150%"

	PhaseReady = "Ready"

	ReasonValidationFailed = "ValidationFailed"
	ReasonCreating         = "Creating"
	ReasonUpdating         = "Updating"
	ReasonTerminating      = "Terminating"
	ReasonScanFailed       = "ScanFailed"
	// ReasonResizing marks an LVMLogicalVolume whose Logical Volume is being grown
	// to the requested size. Not a failure, but not usable at the new size yet.
	ReasonResizing = "Resizing"
	// ReasonCleaning marks an LVMLogicalVolume whose Logical Volume is being wiped
	// before removal, as spec.volumeCleanup asks. The volume still exists and is
	// on its way out.
	ReasonCleaning = "Cleaning"
	ReasonUpdated  = "Updated"
	ReasonApplied  = "Applied"
	// ReasonAliasResolutionFailed is set when the agent has repeatedly failed
	// to canonicalize the alias-form PV names it needs to decide whether a
	// file-backed loop device is already part of the VG. Unlike the transient
	// ReasonUpdating requeue, this reason signals a stuck resolver (e.g. a
	// missing nsenter binary or a genuinely broken alias) that will not clear
	// on its own, so it can be alerted on distinctly.
	ReasonAliasResolutionFailed = "AliasResolutionFailed"
	// ReasonFileDeviceDrift is set when spec.fileDevices no longer describes the
	// file-backed Physical Volumes the Volume Group actually has — the only way
	// to reach it is removing an entry that backs a live PV, which the apiserver
	// allows on purpose (an unprovisioned entry must stay removable). It is not a
	// validation error: the spec is well-formed and the agent applied everything
	// it could. Keeping it apart from ReasonValidationFailed is what lets an
	// operator alert on "the node and the spec disagree" — which needs a human
	// decision (restore the entry, or pvmove + vgreduce by hand) — without
	// drowning in genuinely malformed entries.
	ReasonFileDeviceDrift = "FileDeviceDrift"
	// ReasonFileDeviceNotApplied is set when a spec.fileDevices entry could not
	// be brought up on the node — no free space for the backing file, losetup
	// refused, pvresize failed while growing one — while the Volume Group itself
	// is intact and serving every volume on it.
	//
	// It is deliberately distinct from the reasons that mean "the Volume Group
	// failed" (VGExtendFailed, VGCreationFailed). Those describe storage that is
	// broken; this one describes capacity that has not arrived yet. Reporting the
	// second as the first is what would let a single oversized entry appended to
	// a healthy LVMVolumeGroup take it out of service and stop the scheduler from
	// placing new volumes on it.
	ReasonFileDeviceNotApplied = "FileDeviceNotApplied"
	// ReasonFileDeviceGrowFailed is set when growing a spec.fileDevices entry in
	// place did not go through — the filesystem had no room for the larger
	// backing file, losetup could not refresh the device's capacity, pvresize
	// failed, or the reconcile was cut short before the sequence finished.
	//
	// It is not fatal, for the reason the growth sequence is ordered the way it
	// is: every step fails towards the smaller size, so a grow that stopped
	// half-way leaves a working file device that is merely still small. Nothing
	// that already exists has broken, and reporting it as a broken Volume Group
	// would take an LVMVolumeGroup out of service over capacity that has not
	// arrived — the same mistake VGExtendFailed used to make.
	ReasonFileDeviceGrowFailed = "FileDeviceGrowFailed"
	// ReasonCacheStale is set when the node has a Volume Group that the agent's
	// own LVM cache does not know about yet, so neither the create path (refused,
	// the VG is there) nor the update path (no cached VG to work from) can run.
	//
	// It clears itself once the scanner catches up. It exists as a distinct
	// reason because the alternative was silence: the reconcile requeued with no
	// condition at all, which is indistinguishable from an update in flight.
	ReasonCacheStale = "CacheStale"
	// ReasonVGCheckFailed is set when the agent cannot read the node's Volume
	// Groups at all and therefore refuses to decide whether one has to be
	// created. Unlike ReasonCacheStale this does not clear itself — lvm.static is
	// missing, nsenter is broken, /etc/lvm is unreadable — and the two must not
	// be reported as the same thing, or the operator waits for a cache that is
	// not the problem.
	ReasonVGCheckFailed = "VGCheckFailed"
	// ReasonBlockDeviceNotFound is set when SOME of the node's Volume Group's
	// Physical Volumes have no BlockDevice resource to name them, while the rest
	// could be named: the status published in that pass describes the node, and only
	// the device entries for the unnamed PVs are absent from it.
	//
	// Usually it clears itself within seconds, as soon as the block-device
	// discoverer registers the device. It does not clear itself when the device is
	// one that never becomes a BlockDevice — below BlockDeviceValidSize, or excluded
	// by a BlockDeviceFilter — which is the case this reason exists for: the agent
	// stops retrying at that point, and without a condition the resource would sit
	// with a device silently missing from status.nodes and nothing to say so.
	//
	// It has to be listed in the controller's acceptableReasons (mirrored there as
	// internal.ReasonBlockDeviceNotFound), because everything the aggregate verdict
	// is about is still true: vgSize, vgFree and thin-pool usage were all written
	// this pass and none of them is affected by the missing device entry, so the
	// Volume Group keeps serving its volumes and can take new ones. A PV that is
	// deliberately below BlockDeviceValidSize or filtered out must not take an
	// otherwise healthy Volume Group out of service.
	ReasonBlockDeviceNotFound = "BlockDeviceNotFound"
	// ReasonNodeNotDescribed is set when NOT ONE of the Volume Group's Physical
	// Volumes could be named by a BlockDevice resource. The candidate then carries
	// no node at all, and since status.nodes is written wholesale the agent declines
	// to write it: publishing would empty the node entry, or never create one on a
	// resource that has none yet, and the controller only sets AgentReady on an
	// LVMVolumeGroup whose status.nodes names a node.
	//
	// So what the resource keeps showing is the previous pass's status, and its
	// free-space numbers are as old as it is. Unlike ReasonBlockDeviceNotFound this
	// reason is deliberately NOT mirrored in the controller and therefore NOT among
	// its acceptableReasons — the same arrangement ReasonVGCheckFailed has. Placing
	// new volumes against figures nobody refreshed is exactly what the scheduler
	// must not do, so this one has to drag the aggregate Ready condition, the phase
	// and the scheduler's willingness down with it.
	ReasonNodeNotDescribed = "NodeNotDescribed"

	MetadataNameLabelKey = "kubernetes.io/metadata.name"
	HostNameLabelKey     = "kubernetes.io/hostname"

	Thick = "Thick"
	Thin  = "Thin"

	Local  = "Local"
	Shared = "Shared"

	NonOperational = "NonOperational"

	DeletionProtectionAnnotation = "storage.deckhouse.io/deletion-protection"
	LVMVolumeGroupTag            = "storage.deckhouse.io/lvmVolumeGroupName"
	LVGMetadataNameLabelKey      = "kubernetes.io/metadata.name"

	// FileDeviceImageSuffix is the trailing component the agent appends
	// to every backing file it creates for spec.fileDevices entries.
	FileDeviceImageSuffix = ".img"

	// FileDevicePrefix anchors the basename of every backing file the
	// agent creates. The full pattern is `sds-<lvgName>.<entryName>.img` and
	// is the sole owner marker: the discoverer treats a loop PV as a
	// managed file device only when its backing file's basename matches
	// this pattern (see utils.IsManagedFileDevicePath). Reusing the LVG
	// name in the basename means a foreign loop device backed by an
	// unrelated file (a libvirt qcow2, a snap, …) is never misidentified
	// as ours during discovery or cleanup.
	FileDevicePrefix = "sds-"
)

var (
	AllowedFSTypes     = [...]string{LVMFSType}
	InvalidDeviceTypes = [...]string{LoopDeviceType, LVMDeviceType, CDROMDeviceType}
	Finalizers         = []string{SdsNodeConfiguratorFinalizer}
	LVMTags            = []string{"storage.deckhouse.io/enabled=true", "linstor-"}

	// ForeignDeviceBasePrefixes lists canonical block-device basenames
	// that always belong to a foreign storage layer and must never be
	// considered an LVM PV by the agent regardless of what lvm
	// reported. The list intentionally matches /proc/devices entries:
	//
	//   rbd   - Ceph RBD (kernel rbd module, major 251)
	//   drbd  - DRBD     (sds-replicated-volume, major 147)
	//   nbd   - network block device (major 43)
	//
	// Loop devices (major 7) are NOT in this list: the agent manages
	// file-backed loop devices as LVM PVs via spec.fileDevices.
	// Unmanaged loop PVs forming a whole VG are dropped from the cache by
	// utils.FilterForeignLoopPVs so they cannot collide by name with a
	// managed VG; the discoverer additionally only acts on VGs tagged with
	// storage.deckhouse.io/enabled=true.
	//
	// Used after lvm returns the PV list, against the canonical
	// path resolved via readlink -f in the host mount namespace.
	ForeignDeviceBasePrefixes = []string{"rbd", "drbd", "nbd"}
)

const (
	CreateReconcile ReconcileType = "Create"
	UpdateReconcile ReconcileType = "Update"
	DeleteReconcile ReconcileType = "Delete"
)

type (
	ReconcileType string
)

// loopDevicePath is what a loop device may look like for the filter builder to
// accept it. Anything else is refused rather than escaped: the value is spliced
// into an LVM configuration string whose own field separator is `|`, and the
// only inputs this function has any business taking are canonical loop paths.
var loopDevicePath = regexp.MustCompile(`^/dev/loop\d+$`)

// LVMGlobalFilterAcceptingLoops builds the `lvm --config` filter that rejects
// foreign devices as LVMGlobalFilter does, but first accepts the loop devices
// named in loops.
//
// The accept rules have to come first because LVM applies the first matching
// rule, and they have to be exact (`^/dev/loopN$`) because the point is to admit
// this agent's own file-backed devices without admitting the neighbouring minor
// that belongs to a virtual machine.
//
// A path that is not a canonical loop device is skipped, and skipping is the safe
// direction: the device stays invisible to lvm, which for a file device the agent
// is about to provision surfaces as a plain provisioning failure, whereas a
// mangled filter value makes EVERY lvm command fail with "Invalid filter pattern"
// and takes the whole node's storage with it.
func LVMGlobalFilterAcceptingLoops(loops []string) string {
	if len(loops) == 0 {
		return LVMGlobalFilter
	}

	var b strings.Builder
	b.WriteString(`devices/global_filter=[`)
	for _, loop := range loops {
		if !loopDevicePath.MatchString(loop) {
			continue
		}
		b.WriteString(`"a|^`)
		b.WriteString(loop)
		b.WriteString(`$|",`)
	}
	b.WriteString(lvmForeignDeviceRejects)
	b.WriteString(`]`)

	return b.String()
}
