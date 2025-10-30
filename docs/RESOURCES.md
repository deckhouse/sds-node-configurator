---
title: "sds-node-configurator module: working with resources"
linkTitle: "Working with resources"
description: "Creating and managing BlockDevice and LVMVolumeGroup resources."
---

{{< alert level="warning" >}}
Module functionality is guaranteed only when using stock kernels provided with [supported distributions](/products/kubernetes-platform/documentation/v1/reference/supported_versions.html#linux).

Module functionality when using other kernels or distributions is possible but not guaranteed.
{{< /alert >}}

The controller works with two types of resources:
- [BlockDevice](./cr.html#blockdevice);
- [LVMVolumeGroup](./cr.html#lvmvolumegroup).

## Working with BlockDevice resources

### Creating a BlockDevice resource

The controller regularly scans devices on the node. If a device meets the controller requirements, a [BlockDevice](./cr.html#blockdevice) resource with a unique name is created, containing complete information about the device.

#### Controller requirements for device

For the controller to create a [BlockDevice](./cr.html#blockdevice) resource for a device, it must meet the following requirements:

- The device must not be a DRBD device.
- The device must not be a pseudo-device (loop device).
- The device must not be a Logical Volume.
- The file system must be absent or of type `LVM2_MEMBER`.
- The block device must not have partitions.
- The block device size must exceed 1 GB.
- If the device is a virtual disk, it must have a serial number.

#### Creating a partition on a block device

If a partition is created on a block device, the controller will automatically create a [BlockDevice](./cr.html#blockdevice) resource for this partition.

Example of creating a partition on a block device:

```shell
fdisk /dev/sdb
```

The controller uses information from the created resource when working with [LVMVolumeGroup](./cr.html#lvmvolumegroup) resources.

### Updating a BlockDevice resource

The controller automatically updates information in the resource when the state of the corresponding block device on the node changes.

### Deleting a BlockDevice resource

The controller will automatically delete the resource if the corresponding block device becomes unavailable. Deletion will only occur in the following cases:
- if the resource was in Consumable status;
- if the block device belongs to a Volume Group that does not have the LVM tag `storage.deckhouse.io/enabled=true` (this Volume Group is not managed by the module controller).

{{< alert level="info" >}}
The controller performs all listed operations automatically without user intervention.

If the resource is manually deleted, it will be recreated by the controller.
{{< /alert >}}

## Working with LVMVolumeGroup resources

[BlockDevice](./cr.html#blockdevice) resources are required to create and update [LVMVolumeGroup](./cr.html#lvmvolumegroup) resources. Currently, only local Volume Groups are supported.

[LVMVolumeGroup](./cr.html#lvmvolumegroup) resources are designed to interact with LVM Volume Groups on nodes and display current information about their state.

### Creating an LVMVolumeGroup resource

An [LVMVolumeGroup](./cr.html#lvmvolumegroup) resource can be created in three ways:

#### Automatic creation

The controller automatically scans LVM Volume Groups on nodes. If an LVM Volume Group has the LVM tag `storage.deckhouse.io/enabled=true` and the corresponding Kubernetes resource [LVMVolumeGroup](./cr.html#lvmvolumegroup) is missing, the controller creates it automatically.

In this case, the controller automatically fills all fields in the `spec` section of the resource, except `thinPools`. To manage an existing thin pool on the node, you need to manually add information about it to the `spec` section of the resource.

#### User creation

The user manually creates the resource by filling in the `metadata.name` and `spec` fields, specifying the desired state of the new Volume Group.

The specified configuration is validated. After successful validation, the controller creates the LVM Volume Group on the node according to the configuration. Then the controller updates the [LVMVolumeGroup](./cr.html#lvmvolumegroup) resource with current information about the state of the created LVM Volume Group.

#### Manual Volume Group creation on the node

The user manually creates a Volume Group on the node using standard LVM commands.

After creating the Volume Group, the user adds the LVM tag `storage.deckhouse.io/enabled=true` to transfer control to the controller.

The controller automatically discovers the Volume Group with this tag and creates the corresponding [LVMVolumeGroup](./cr.html#lvmvolumegroup) resource.

Detailed instructions for adding an existing Volume Group to Kubernetes are described in the [FAQ](./faq.html) section.

#### Examples of creating LVMVolumeGroup resources

The examples below show various configuration options using different selectors and with or without thin pool.

Examples without thin pool:

Creating an LVM Volume Group with selection of specific devices by [BlockDevice](./cr.html#blockdevice) resource names (using `matchExpressions`):

```yaml
apiVersion: storage.deckhouse.io/v1alpha1
kind: LVMVolumeGroup
metadata:
  name: "vg-0-on-node-0"
spec:
  type: Local
  local:
    nodeName: "node-0"
  blockDeviceSelector:
    matchExpressions:
      - key: kubernetes.io/metadata.name
        operator: In
        values:
          - dev-07ad52cef2348996b72db262011f1b5f896bb68f
          - dev-e90e8915902bd6c371e59f89254c0fd644126da7
  actualVGNameOnTheNode: "vg-0"
```

Creating an LVM Volume Group with selection of all devices on the node (using `matchLabels`):

```yaml
apiVersion: storage.deckhouse.io/v1alpha1
kind: LVMVolumeGroup
metadata:
  name: "vg-0-on-node-0"
spec:
  type: Local
  local:
    nodeName: "node-0"
  blockDeviceSelector:
    matchLabels:
      kubernetes.io/hostname: node-0
  actualVGNameOnTheNode: "vg-0"
```

Examples with thin pool:

Creating an LVM Volume Group with thin pool and selection of specific devices by names (using `matchExpressions`):

```yaml
apiVersion: storage.deckhouse.io/v1alpha1
kind: LVMVolumeGroup
metadata:
  name: "vg-0-on-node-0"
spec:
  type: Local
  local:
    nodeName: "node-0"
  blockDeviceSelector:
    matchExpressions:
      - key: kubernetes.io/metadata.name
        operator: In
        values:
          - dev-07ad52cef2348996b72db262011f1b5f896bb68f
          - dev-e90e8915902bd6c371e59f89254c0fd644126da7
  actualVGNameOnTheNode: "vg-0"
  thinPools:
    - name: thin-1
      size: 250Gi
```

Creating an LVM Volume Group with thin pool and selection of all devices on the node (using `matchLabels`):

```yaml
apiVersion: storage.deckhouse.io/v1alpha1
kind: LVMVolumeGroup
metadata:
  name: "vg-0-on-node-0"
spec:
  type: Local
  local:
    nodeName: "node-0"
  blockDeviceSelector:
    matchLabels:
      kubernetes.io/hostname: node-0
  actualVGNameOnTheNode: "vg-0"
  thinPools:
    - name: thin-1
      size: 250Gi
```

{{< alert level="info" >}}
You can use any convenient selectors for [BlockDevice](./cr.html#blockdevice) resources. For example, select all devices on a node (using `matchLabels`) or part of devices by specifying their names or other parameters.

The `spec.local` field is mandatory for the `Local` type. If there's a discrepancy between the name in `spec.local.nodeName` and selectors, [LVMVolumeGroup](./cr.html#lvmvolumegroup) creation will not be executed.
{{< /alert >}}

{{< alert level="warning" >}}
All selected block devices must belong to the same node for [LVMVolumeGroup](./cr.html#lvmvolumegroup) with type `Local`.
{{< /alert >}}

### Updating an LVMVolumeGroup resource

To change the desired state of a Volume Group or thin pool on nodes, modify the `spec` field of the corresponding [LVMVolumeGroup](./cr.html#lvmvolumegroup) resource. The controller automatically validates the new data and, if correct, makes the necessary changes to entities on the node.

The controller automatically updates the `status` field of the [LVMVolumeGroup](./cr.html#lvmvolumegroup) resource, displaying current data about the corresponding LVM Volume Group on the node. It is not recommended to manually modify the `status` field.

{{< alert level="info" >}}
The controller does not modify the `spec` field, as it reflects the desired state of the LVM Volume Group. To change the state of the corresponding LVM Volume Group on the node, the user can modify the `spec` field.
{{< /alert >}}

### Deleting an LVMVolumeGroup resource

#### Automatic deletion

The controller automatically deletes the resource if the Volume Group specified in it becomes unavailable (for example, all block devices that made up the Volume Group were disconnected on the node).

#### Manual deletion

The user can delete the LVM Volume Group on the node along with the associated LVM Physical Volumes by deleting the [LVMVolumeGroup](./cr.html#lvmvolumegroup) resource:

```shell
d8 k delete lvg %lvg-name%
```

#### Deletion protection

The user can prevent deletion of the [LVMVolumeGroup](./cr.html#lvmvolumegroup) resource by adding the `storage.deckhouse.io/deletion-protection` annotation to the resource. If this annotation is present, the controller will not delete either the resource or the corresponding Volume Group until the annotation is removed from the resource.

{{< alert level="warning" >}}
If the deleting [LVMVolumeGroup](./cr.html#lvmvolumegroup) resource contains a Logical Volume (even if it's only a thin pool specified in `spec`), you must manually delete all Logical Volumes in the Volume Group being deleted. Otherwise, neither the resource nor the Volume Group will be deleted.
{{< /alert >}}

### Extracting a BlockDevice resource from LVMVolumeGroup

To extract a [BlockDevice](./cr.html#blockdevice) resource from an [LVMVolumeGroup](./cr.html#lvmvolumegroup) resource, you need to:

1. Modify the `spec.blockDeviceSelector` field of the [LVMVolumeGroup](./cr.html#lvmvolumegroup) resource (add other selectors) or modify the corresponding labels on the [BlockDevice](./cr.html#blockdevice) resource so they no longer match the [LVMVolumeGroup](./cr.html#lvmvolumegroup) selectors.
1. Manually execute the `pvmove`, `vgreduce`, and `pvremove` commands on the node.

## Protection against data leakage between volumes

When deleting files, the operating system does not physically delete the contents, but only marks the corresponding blocks as "free". If a new volume receives physical blocks previously used by another volume, the previous user's data may remain in them.

### Data leakage problem

Data leakage is possible under the following conditions:

1. User #1 created a volume from StorageClass 1 on node 1 (mode "Block" or "Filesystem" doesn't matter), placed data in it, and then deleted the volume.
1. Physical blocks occupied by user #1 are marked as free, but their contents are not cleared.
1. User #2 requests a new volume from the same StorageClass 1 on the same node 1 in "Block" mode.
1. When allocating volumes, the system may provide the same physical blocks that user #1 previously used.
1. User #2 gets access to user #1's data that remained in these blocks.

### Thick volumes

To prevent leaks through thick volumes, the `volumeCleanup` parameter is provided, which allows you to select the volume cleanup method before deleting the Physical Volume.

#### Available cleanup methods

- Parameter not specified — no additional actions are performed when deleting a volume. Data may remain available to the next user who gets the same volume.

- `RandomFillSinglePass` — the volume will be overwritten with random data once before deletion. Not recommended for solid-state drives as it reduces the drive lifespan.

- `RandomFillThreePass` — the volume will be overwritten with random data three times before deletion. Not recommended for solid-state drives as it reduces the drive lifespan.

- `Discard` — all volume blocks will be marked as free using the `discard` system call before deletion. This option is only applicable to solid-state drives.

#### Recommendations for using `Discard`

Most modern solid-state drives guarantee that after executing the `discard` command, a block will not return previous data when read. This makes the `Discard` option the most effective way to prevent data leakage when using solid-state drives.

The following limitations should be considered:

- Cell clearing is a relatively long operation, so it's performed by the device in the background.
- Many drives cannot clear individual cells, only groups of cells (pages).
- Not all drives guarantee immediate unavailability of freed data.
- Not all drives that claim such guarantees actually meet them in practice.

It's not recommended to use drives that don't guarantee Deterministic TRIM (DRAT) and Deterministic Read Zero after TRIM (RZAT) or haven't been tested for compliance with these standards.

### Thin volumes

When a thin volume block is released via `discard` by the guest operating system, the command is passed to the physical device. When using a hard disk drive or when there's no `discard` support on the solid-state drive, data may remain in the thin pool until the block is reused by another volume.

Users only have access to thin volumes, not the thin pool itself. When allocating a new volume from the pool for thin volumes, LVM performs zeroing of the thin pool block before using it. This prevents data leakage between clients and is guaranteed by the `thin_pool_zero=1` setting in LVM.
