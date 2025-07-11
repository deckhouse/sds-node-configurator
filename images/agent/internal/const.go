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

import "k8s.io/apimachinery/pkg/api/resource"

const (
	// LVGUpdateTriggerLabel if you change this value, you must change its value in sds-health-watcher-controller/pkg/block_device_labels_watcher.go as well
	LVGUpdateTriggerLabel = "storage.deckhouse.io/update-trigger"

	resizeDelta                  = "32Mi"
	PartType                     = "part"
	MultiPathType                = "mpath"
	CDROMDeviceType              = "rom"
	DRBDName                     = "/dev/drbd"
	LoopDeviceType               = "loop"
	LVMDeviceType                = "lvm"
	LVMFSType                    = "LVM2_member"
	MultiPathMemberFSType        = "mpath_member"
	SdsNodeConfiguratorFinalizer = "storage.deckhouse.io/sds-node-configurator"
	LVMVGHealthOperational       = "Operational"
	LVMVGHealthNonOperational    = "NonOperational"
	BlockDeviceValidSize         = "1G"
	NSENTERCmd                   = "/opt/deckhouse/sds/bin/nsenter.static"
	DMSetupCmd                   = "/opt/deckhouse/sds/bin/dmsetup.static"
	LSBLKCmd                     = "/opt/deckhouse/sds/bin/lsblk.dynamic"
	LVMCmd                       = "/opt/deckhouse/sds/bin/lvm.static"
	ThinDumpCmd                  = "/opt/deckhouse/sds/bin/thin_dump"

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
	ResizeDelta        = resource.MustParse(resizeDelta)
)

const (
	CreateReconcile ReconcileType = "Create"
	UpdateReconcile ReconcileType = "Update"
	DeleteReconcile ReconcileType = "Delete"
)

type (
	ReconcileType string
)
