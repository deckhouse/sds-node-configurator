---
title: "sds-node-configurator module: FAQ"
description: "sds-node-configurator module: frequently asked questions and answers."
---
{{< alert level="warning" >}}
Module functionality is guaranteed only when using stock kernels provided with [supported distributions](/products/kubernetes-platform/documentation/v1/reference/supported_versions.html#linux).

Module functionality when using other kernels or distributions is possible but not guaranteed.
{{< /alert >}}

## BlockDevice and LVMVolumeGroup resources are not created in the cluster

- [BlockDevice](./cr.html#blockdevice) resources may not be created if devices do not pass controller filtering. Ensure that devices meet the [requirements](./resources.html#controller-requirements-for-devices).

- [LVMVolumeGroup](./cr.html#lvmvolumegroup) resources may not be created due to the absence of [BlockDevice](./cr.html#blockdevice) resources in the cluster, as their names are used in the [LVMVolumeGroup](./cr.html#lvmvolumegroup) specification.

- If [BlockDevice](./cr.html#blockdevice) resources exist but [LVMVolumeGroup](./cr.html#lvmvolumegroup) resources are missing, ensure that existing LVM Volume Groups on the node have the LVM tag `storage.deckhouse.io/enabled=true`.

## LVMVolumeGroup resource and Volume Group remain after deletion attempt

This situation can occur in two cases:

1. The Volume Group contains Logical Volumes.

   The controller is not responsible for deleting Logical Volumes from the node. If the Volume Group created using the resource contains Logical Volumes, delete them manually on the node. After that, the resource and Volume Group along with Physical Volumes will be automatically deleted.

1. The resource has the `storage.deckhouse.io/deletion-protection` annotation.

   This annotation protects the resource and the Volume Group it created from deletion. Remove the annotation by running the command:

   ```shell
   d8 k annotate lvg %lvg-name% storage.deckhouse.io/deletion-protection-
   ```

   After executing the command, the resource and Volume Group will be automatically deleted.

## Unable to create Volume Group using LVMVolumeGroup resource

The resource does not pass controller validation (Kubernetes validation was successful). The reason can be seen in the `status.message` field of the resource or in the controller logs.

Most often the problem is related to incorrectly specified [BlockDevice](./cr.html#blockdevice) resources. Ensure that the selected resources meet the following requirements:

- The `status.consumable` field has the value `true`.
- For `Local` type Volume Groups, the specified [BlockDevice](./cr.html#blockdevice) resources belong to the same node.<!-- > - For `Shared` type Volume Groups, a single [BlockDevice](./cr.html#blockdevice) resource is specified. -->
- Current [BlockDevice](./cr.html#blockdevice) resource names are specified.

The complete list of expected values is available in the [LVMVolumeGroup](./cr.html#lvmvolumegroup) resource description.

## Device disconnection in Volume Group

The [LVMVolumeGroup](./cr.html#lvmvolumegroup) resource exists as long as the corresponding Volume Group exists. As long as at least one device exists, the Volume Group is preserved but marked as non-functional. The current state is reflected in the `status` field of the resource.

After restoring the disconnected device on the node, the LVM Volume Group will restore functionality, and the corresponding [LVMVolumeGroup](./cr.html#lvmvolumegroup) resource will display the current state.

## Transferring control of existing LVM Volume Group to controller

Add the LVM tag `storage.deckhouse.io/enabled=true` to the LVM Volume Group on the node:

```shell
vgchange myvg-0 --addtag storage.deckhouse.io/enabled=true
```

## Stopping LVM Volume Group tracking by controller

Remove the LVM tag `storage.deckhouse.io/enabled=true` from the desired LVM Volume Group on the node:

```shell
vgchange myvg-0 --deltag storage.deckhouse.io/enabled=true
```

After this, the controller will stop tracking the selected Volume Group and will independently delete the associated [LVMVolumeGroup](./cr.html#lvmvolumegroup) resource.

## LVM tag `storage.deckhouse.io/enabled=true` appeared automatically

The LVM tag appears in the following cases:

- LVM Volume Group was created through the [LVMVolumeGroup](./cr.html#lvmvolumegroup) resource. In this case, the controller automatically adds the LVM tag `storage.deckhouse.io/enabled=true` to the created LVM Volume Group.
- The Volume Group or its thin pool had the `linstor` module LVM tag — `linstor-*`.

When migrating from the built-in `linstor` module to `sds-node-configurator` and `sds-replicated-volume` modules, `linstor-*` LVM tags are automatically replaced with `storage.deckhouse.io/enabled=true` in Volume Groups. Management of these Volume Groups is transferred to the `sds-node-configurator` module.

## Creating LVMVolumeGroup using LVMVolumeGroupSet resource

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

## Changing UUID of Volume Groups when cloning virtual machines

UUID of Volume Groups can only be changed when there are no active Logical Volumes in the Volume Group.

If the Volume Group has active Logical Volumes, perform the following steps:

1. Unmount the Logical Volume by running the command:

   ```shell
   umount /mount/point
   ```

1. Deactivate the Logical Volume or Volume Group by running the command:

    - To deactivate a specific Logical Volume, run the command, changing <LV_NAME> to the Logical Volume name:

      ```shell
      lvchange -an <LV_NAME>
      ```

    - To deactivate all Logical Volumes in the group, run the command, changing <VG_NAME> to the Volume Group name:

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

## Labels added by controller to BlockDevice resources

- status.blockdevice.storage.deckhouse.io/type: LVM type.
- status.blockdevice.storage.deckhouse.io/fstype: Filesystem type.
- status.blockdevice.storage.deckhouse.io/pvuuid: Physical Volume UUID.
- status.blockdevice.storage.deckhouse.io/vguuid: Volume Group UUID.
- status.blockdevice.storage.deckhouse.io/partuuid: Partition UUID.
- status.blockdevice.storage.deckhouse.io/lvmvolumegroupname: Name of the [LVMVolumeGroup](./cr.html#lvmvolumegroup) resource to which the device belongs.
- status.blockdevice.storage.deckhouse.io/actualvgnameonthenode: Volume Group name on the node.
- status.blockdevice.storage.deckhouse.io/wwn: WWN (World Wide Name) identifier for the device.
- status.blockdevice.storage.deckhouse.io/serial: Device serial number.
- status.blockdevice.storage.deckhouse.io/size: Device size.
- status.blockdevice.storage.deckhouse.io/model: Device model.
- status.blockdevice.storage.deckhouse.io/rota: Indicates whether the device is rotational.
- status.blockdevice.storage.deckhouse.io/hotplug: Indicates device hot-plug capability.
- status.blockdevice.storage.deckhouse.io/machineid: Identifier of the server where the block device is installed.
