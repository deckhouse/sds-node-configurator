---
title: "Release Notes"
---

## v0.5.7

* Added release notes

## v0.5.6

* Added additional mounts for containerd v2 support

## v0.5.5

* CVE fixes

## v0.5.4

* CVE fixes
* Internal changes for containerd v2 support

## v0.5.3

* Fixed bug preventing LVG status updates when missed/notready nodes are present in the cluster

## v0.5.2

* Removed support for sds-drbd module
* Added hiding of csi-scsi-generic devices

## v0.5.1

* Technical release, module refactoring

## v0.5.0

* Added ability to wipe data in enterprise versions
* Multiple documentation fixes

## v0.4.6

* Technical release. Removed "Preview" status from documentation

## v0.4.5

* Numerous documentation fixes
* Added LVMVolumeGroupSet and LVMLogicalVolumeSnapshots
* Fixed work with labels and finalizers in some internal objects
* Added dm_snapshot loading when needed

## v0.4.3

* Fixed crash of BlockDevices migration script when finalizers are missing on resources

## v0.4.2

* Fix for supporting BlockDevice serial numbers up to 63 characters long

## v0.4.1

* Fixed potential issue with missing labels in LvmVolumeGroup->LVMVolumeGroup migration hook
* Added controller that tracks labels on BlockDevice resources for quick updates of LVMVolumeGroup resources

## v0.4.0

* LvmVolumeGroups resources will be migrated to LVMVolumeGroups
* Exact BlockDevices lists in LVMVolumeGroups will be migrated to selectors
* Multiple fixes in controllers and documentation

## v0.3.2

* Fix in sds-health-watcher-controller which could incorrectly handle the status of some nodes

## v0.3.1

* Updated golang to current 1.22.6 to close known vulnerabilities

## v0.3.0

* Added thin provisioning configuration in mc for automatic dm_thin_pool module loading
* Images moved to distroless
* Added labels to BlockDevices entity (for future use in BlockDevicesSelectors)
* Multiple bug fixes and documentation improvements

## v0.2.5

* Added metrics and their collection in Prometheus, health check ports moved to correct ones

## v0.2.4

* Added auto-expansion for thin pools LVM volume groups and LVM logical volumes

## v0.2.3

* Added internal cache for performance improvement
* Added health and readiness checks to controller
* Improved resource display in cli
* Now LVM volume groups are deleted when corresponding k8s resource is deleted, not when annotation is added
* Added LVM volume group size specification in percentages
* Added AllocationLimit support in LVM volume group
* Added support for contiguous volumes in sds-local-volume

## v0.2.1

* Added multipath devices support

## v0.2.0

* Added LVMLogicalVolumeWatcher CRDs
* Added more test cases
* Added AreSizesEqualWithinDelta function
* Fixed resize for thinPools
* Added tests to LvmLogicalVolumeWatcher controller
* Fixed naming in LVMLogicalVolume
* Added node affinity
* Enhanced LVMLogicalVolume handling with logging, size display, and event processing
* Statically linked nsenter, lsblk and lvm utils
* Fixed serial discovery by switching to dynamic lsblk
* Rewrote bin-copier script in Golang
* Moved lsblk and its libraries inside the agent image
* Added age field
* Implemented stderr filtering for LVM commands
* Added bench-tests for LVMLogicalVolume controller
* Changed size type from string to quantity
* Added parallel reconciliation to lvm_logical_volume_watcher and VG size validation to lvm_volume_group_watcher
