---
title: "sds-node-configurator module: FAQ"
description: "sds-node-configurator module: frequently asked questions and answers."
weight: 2
---
{{< alert level="warning" >}}
Module functionality is guaranteed only when using stock kernels provided with [supported distributions](/products/kubernetes-platform/documentation/v1/reference/supported_versions.html#linux).

Module functionality when using other kernels or distributions is possible but not guaranteed.
{{< /alert >}}

## Why are BlockDevice and LVMVolumeGroup resources not created in the cluster?

- [BlockDevice](./cr.html#blockdevice) resources may not be created if devices do not pass controller filtering. Ensure that devices meet the [requirements](./resources.html#controller-requirements-for-devices).

- [LVMVolumeGroup](./cr.html#lvmvolumegroup) resources may not be created due to the absence of [BlockDevice](./cr.html#blockdevice) resources in the cluster, as their names are used in the [LVMVolumeGroup](./cr.html#lvmvolumegroup) specification.

- If [BlockDevice](./cr.html#blockdevice) resources exist but [LVMVolumeGroup](./cr.html#lvmvolumegroup) resources are missing, ensure that existing LVM Volume Groups on the node have the LVM tag `storage.deckhouse.io/enabled=true`.

## Why did the LVMVolumeGroup resource and Volume Group remain after deletion attempt?

This situation can occur in two cases:

1. The Volume Group contains Logical Volumes.

   The controller is not responsible for deleting Logical Volumes from the node. If the Volume Group created using the resource contains Logical Volumes, delete them manually on the node. After that, the resource and Volume Group along with Physical Volumes will be automatically deleted.

1. The resource has the `storage.deckhouse.io/deletion-protection` annotation.

   This annotation protects the resource and the Volume Group it created from deletion. Remove the annotation by running the command:

   ```shell
   d8 k annotate lvg %lvg-name% storage.deckhouse.io/deletion-protection-
   ```

   After executing the command, the resource and Volume Group will be automatically deleted.

## Why is it not possible to create a Volume Group using the LVMVolumeGroup resource?

The resource does not pass controller validation (Kubernetes validation was successful). The reason can be seen in the `status.message` field of the resource or in the controller logs.

Most often the problem is related to incorrectly specified [BlockDevice](./cr.html#blockdevice) resources. Ensure that the selected resources meet the following requirements:

- The `status.consumable` field has the value `true`.
- For `Local` type Volume Groups, the specified [BlockDevice](./cr.html#blockdevice) resources belong to the same node.<!-- > - For `Shared` type Volume Groups, a single [BlockDevice](./cr.html#blockdevice) resource is specified. -->
- Current [BlockDevice](./cr.html#blockdevice) resource names are specified.

The complete list of expected values is available in the [LVMVolumeGroup](./cr.html#lvmvolumegroup) resource description.

## What happens if I disconnect one of the devices in a Volume Group? Will the corresponding LVMVolumeGroup resource be deleted?

The [LVMVolumeGroup](./cr.html#lvmvolumegroup) resource exists as long as the corresponding Volume Group exists. As long as at least one device exists, the Volume Group is preserved but marked as non-functional. The current state is reflected in the `status` field of the resource.

After restoring the disconnected device on the node, the LVM Volume Group will restore functionality, and the corresponding [LVMVolumeGroup](./cr.html#lvmvolumegroup) resource will display the current state.

## How do I transfer control of an existing LVM Volume Group to the controller?

Add the LVM tag `storage.deckhouse.io/enabled=true` to the LVM Volume Group on the node:

```shell
vgchange myvg-0 --addtag storage.deckhouse.io/enabled=true
```

## How do I stop the controller from tracking an LVM Volume Group?

Remove the LVM tag `storage.deckhouse.io/enabled=true` from the desired LVM Volume Group on the node:

```shell
vgchange myvg-0 --deltag storage.deckhouse.io/enabled=true
```

After this, the controller will stop tracking the selected Volume Group and will independently delete the associated [LVMVolumeGroup](./cr.html#lvmvolumegroup) resource.

## Why does the LVM tag `storage.deckhouse.io/enabled=true` appear automatically?

The LVM tag appears in the following cases:

- LVM Volume Group was created through the [LVMVolumeGroup](./cr.html#lvmvolumegroup) resource. In this case, the controller automatically adds the LVM tag `storage.deckhouse.io/enabled=true` to the created LVM Volume Group.
- The Volume Group or its thin pool had the `linstor` module LVM tag — `linstor-*`.

When migrating from the built-in `linstor` module to `sds-node-configurator` and `sds-replicated-volume` modules, `linstor-*` LVM tags are automatically replaced with `storage.deckhouse.io/enabled=true` in Volume Groups. Management of these Volume Groups is transferred to the `sds-node-configurator` module.

## How do I create LVMVolumeGroup using LVMVolumeGroupSet?

To create [LVMVolumeGroup](./cr.html#lvmvolumegroup) resources using [LVMVolumeGroupSet](./cr.html#lvmvolumegroupset), specify node selectors and a template for the created [LVMVolumeGroup](./cr.html#lvmvolumegroup) resources in the [LVMVolumeGroupSet](./cr.html#lvmvolumegroupset) specification.

Only the `PerNode` strategy is supported: the controller creates one [LVMVolumeGroup](./cr.html#lvmvolumegroup) resource from the template for each node matching the selector.

Example [LVMVolumeGroupSet](./cr.html#lvmvolumegroupset) specification:

```yaml
apiVersion: storage.deckhouse.io/v1alpha1
kind: LVMVolumeGroupSet
metadata:
  name: my-lvm-volume-group-set
  labels:
    my-label: my-value
spec:
  strategy: PerNode
  nodeSelector:
    matchLabels:
      node-role.kubernetes.io/worker: ""
  lvmVolumeGroupTemplate:
    metadata:
      labels:
        my-label-for-lvg: my-value-for-lvg
    type: Local
    blockDeviceSelector:
      matchLabels:
        status.blockdevice.storage.deckhouse.io/model: <model>
    actualVGNameOnTheNode: <actual-vg-name-on-the-node>
```

## How do I change the UUID of Volume Groups when cloning virtual machines?

UUID of Volume Groups can only be changed when there are no active Logical Volumes in the Volume Group.

If the Volume Group has active Logical Volumes, perform the following steps:

1. Unmount the Logical Volume by running the command:

   ```shell
   umount /mount/point
   ```

1. Deactivate the Logical Volume or Volume Group by running the command:

    - To deactivate a specific Logical Volume, run the command, changing `<LV_NAME>` to the Logical Volume name:

      ```shell
      lvchange -an <LV_NAME>
      ```

    - To deactivate all Logical Volumes in the group, run the command, changing `<VG_NAME>` to the Volume Group name:

      ```shell
      lvchange -an <VG_NAME>
      ```

1. After deactivating all Logical Volumes, change the UUID of Volume Groups by running the command:

   ```shell
   vgchange -u <VG_NAME>
   ```

   The command will generate new UUIDs for the specified Volume Group. To change UUIDs of all Volume Groups on the virtual machine, run:

   ```shell
   vgchange -u
   ```

If necessary, the command can be added to the `cloud-init` script for automatic execution when creating virtual machines.

## How do file-backed devices (fileDevices) work?

File-backed devices allow you to allocate part of an existing filesystem for LVM without dedicated block devices. The agent creates a preallocated file in the specified directory, attaches it as a loop device via `losetup`, and uses it as an LVM Physical Volume.

### Limitations

- **Confined to a base directory**: Each `directory` must be the `fileDevicesDirectory` module setting (default `/opt/deckhouse/sds/file-devices`) or a subdirectory of it. Paths outside this subtree are rejected so an arbitrary host path cannot be filled up. Point `fileDevicesDirectory` at a dedicated data disk to use a different location.
- **Directory auto-created**: The agent creates the backing directory automatically (`mkdir -p`) on the node if it does not exist. The path must be absolute and free of `..` segments; provisioning fails only if the path is on a read-only filesystem or a non-directory component is in the way.
- **No resize**: File device size cannot be changed after creation. To increase capacity, add a new `fileDevices` entry.
- **Preallocated only**: Files are created with `fallocate`, which preallocates space on the filesystem. The agent refuses to create a backing file larger than the directory's free space, so a too-large entry is reported on the resource instead of filling the node.
- **Minimum size**: Each file device must be at least 1Gi.
- **Performance overhead**: LVM on a loop device over a filesystem adds double indirection. Use file-backed devices only when dedicated disks are not available.
- **Host LVM filter**: NodeGroupConfiguration adds loop devices to the host-wide LVM `global_filter`, so unprivileged `lvm`/`pvs` on the node do not see them. The agent re-attaches managed backing files at startup and uses its own LVM config for managed Volume Groups.

### Reclaiming space

Backing files are preallocated with `fallocate`, so a file occupies its full size on the node's filesystem from the moment it is created and does **not** grow or shrink automatically as data is written or deleted inside the volume.

Space can still be returned to the node's filesystem through the discard (TRIM) chain, which works end to end for these devices:

`filesystem on the volume` → `thin LV` → `thin pool` (created with `discards=passdown` by default) → `/dev/loopN` → backing file.

A loop device translates discards into `FALLOC_FL_PUNCH_HOLE` on its backing file, turning the reclaimed regions into holes and making the file sparse. To trigger reclamation after deleting data:

- run `fstrim <mountpoint>` on the volume's filesystem periodically (for example via the `fstrim.timer` systemd unit), or
- mount the volume with `-o discard` for continuous (online) discard.

Caveats:

- Only whole thin-pool chunks are reclaimed, so the effect depends on the pool's chunk size and alignment.
- Snapshots pin the chunks they reference, so space shared with a snapshot is not freed until the snapshot is removed.
- The backing file only shrinks after a write → delete → trim cycle; a freshly created file always occupies its full preallocated size.

### Reboot recovery

After a node reboot, the agent automatically re-establishes loop device mappings before activating Volume Groups. The backing file path is stored in `status.nodes[].fileDevices[].filePath`.

### Deletion

When an LVMVolumeGroup with file-backed devices is deleted, the agent detaches the loop devices and removes the backing files.

## What labels are added by the controller to BlockDevice resources?

- `status.blockdevice.storage.deckhouse.io/type`: LVM type.
- `status.blockdevice.storage.deckhouse.io/fstype`: Filesystem type.
- `status.blockdevice.storage.deckhouse.io/pvuuid`: Physical Volume UUID.
- `status.blockdevice.storage.deckhouse.io/vguuid`: Volume Group UUID.
- `status.blockdevice.storage.deckhouse.io/partuuid`: Partition UUID.
- `status.blockdevice.storage.deckhouse.io/lvmvolumegroupname`: Name of the [LVMVolumeGroup](./cr.html#lvmvolumegroup) resource to which the device belongs.
- `status.blockdevice.storage.deckhouse.io/actualvgnameonthenode`: Volume Group name on the node.
- `status.blockdevice.storage.deckhouse.io/wwn`: WWN (World Wide Name) identifier for the device.
- `status.blockdevice.storage.deckhouse.io/serial`: Device serial number.
- `status.blockdevice.storage.deckhouse.io/size`: Device size.
- `status.blockdevice.storage.deckhouse.io/model`: Device model.
- `status.blockdevice.storage.deckhouse.io/rota`: Indicates whether the device is rotational.
- `status.blockdevice.storage.deckhouse.io/hotplug`: Indicates device hot-plug capability.
- `status.blockdevice.storage.deckhouse.io/machineid`: Identifier of the server where the block device is installed.
