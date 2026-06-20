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
	ThinDumpCmd                  = "thin_dump"

	// LVMGlobalFilter is passed via `lvm --config` for every LVM
	// subcommand the agent runs. It rejects canonical names of block
	// devices that always belong to a foreign storage layer (Ceph RBD,
	// DRBD, NBD, loopback) so lvm does not even read PV labels
	// from them when udev integration is unavailable.
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
	LVMGlobalFilter = `devices/global_filter=["r|^/dev/rbd|","r|^/dev/drbd|","r|^/dev/nbd|","r|^/dev/loop|"]`

	// LVMArchiveRetention caps the size of /etc/lvm/archive: keep at
	// most the last 10 metadata snapshots and at most 7 days of history.
	// This only affects new metadata-changing operations; existing
	// archives must be pruned manually on impacted nodes.
	LVMArchiveRetention = `backup/retain_min=10 backup/retain_days=7`

	TypeVGConfigurationApplied = "VGConfigurationApplied"
	TypeVGReady                = "VGReady"

	AllocationLimitDefaultValue = "150%"

	PhaseReady = "Ready"

	ReasonValidationFailed = "ValidationFailed"
	ReasonCreating         = "Creating"
	ReasonUpdating         = "Updating"
	ReasonTerminating      = "Terminating"
	ReasonScanFailed       = "ScanFailed"
	ReasonUpdated          = "Updated"
	ReasonApplied          = "Applied"

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
	//   loop  - loopback (major 7) — typically backs QEMU/file-based VM disks
	//
	// Used after lvm returns the PV list, against the canonical
	// path resolved via readlink -f in the host mount namespace.
	ForeignDeviceBasePrefixes = []string{"rbd", "drbd", "nbd", "loop"}
)

const (
	CreateReconcile ReconcileType = "Create"
	UpdateReconcile ReconcileType = "Update"
	DeleteReconcile ReconcileType = "Delete"
)

type (
	ReconcileType string
)
