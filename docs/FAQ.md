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

- For virtual machines, a common cause is a virtual disk without a serial number. See ["How to configure serial numbers for virtual disks?"](#how-to-configure-serial-numbers-for-virtual-disks) for how to check and set it.

- With a usual layout, the disk the OS is installed on and its partitions get no [BlockDevice](./cr.html#blockdevice) resources; see [The system disk](./resources.html#the-system-disk) for why. For how to give the module the free space on it, see ["How do I give the module the free space on the system disk?"](#how-do-i-give-the-module-the-free-space-on-the-system-disk).

- [LVMVolumeGroup](./cr.html#lvmvolumegroup) resources may not be created due to the absence of [BlockDevice](./cr.html#blockdevice) resources in the cluster, as their names are used in the [LVMVolumeGroup](./cr.html#lvmvolumegroup) specification.

- If [BlockDevice](./cr.html#blockdevice) resources exist but [LVMVolumeGroup](./cr.html#lvmvolumegroup) resources are missing, ensure that existing LVM Volume Groups on the node have the LVM tag `storage.deckhouse.io/enabled=true`.

- A device that kubelet has published to a pod as a block volume (a `volumeMode: Block` PersistentVolume, e.g. a virtual machine's disk on a hypervisor) never becomes a [BlockDevice](./cr.html#blockdevice), and a Volume Group inside it never becomes an [LVMVolumeGroup](./cr.html#lvmvolumegroup), even when it carries the module's tags: the LVM inside such a volume belongs to the volume's consumer (a nested cluster), not to the node. The agent reads the set of published devices from kubelet's `/var/lib/kubelet/plugins/kubernetes.io/csi/volumeDevices` tree and reports its size as the `sds_node_configurator_published_block_devices` metric; `sds_node_configurator_published_devices_directory_present` is `0` when that tree is not where the agent expects it.

- A multipath map with no usable path (no slaves left, or every path failed, as `multipath -ll` shows after a SAN volume was unmapped while the map stayed) never becomes a [BlockDevice](./cr.html#blockdevice), and the agent's LVM commands do not read it: reading such a map blocks in the kernel instead of failing, and a blocked scan holds every device it opened before, which is what stops a CSI driver from detaching its volume from the node.
  The agent only steps around such maps; the `sds_node_configurator_unreadable_multipath_maps` metric counts them, and the agent log names them whenever the set changes. A scan that still blocks past `CMD_DEADLINE_DURATION` is left running rather than killed (killed, it would become a D-state process only a reboot removes); `sds_node_configurator_lvm_scans_abandoned_total` counts these and `sds_node_configurator_lvm_scans_outstanding` is `1` while such a process is still on the node. Remove the maps with `multipath -f <map>` after disabling queueing on them (`dmsetup message <map> 0 fail_if_no_path`), and the abandoned scan exits on its own.

- A path of a multipath map (a device listed in `/sys/block/dm-N/slaves` of a map whose `dm/uuid` starts with `mpath-`) never becomes a [BlockDevice](./cr.html#blockdevice), even when `lsblk` shows it without the `mpath_member` filesystem type. udev marks a path `mpath_member` only if multipath claimed it when it appeared, and with `find_multipaths yes` it does not claim a path that appeared before the LUN's WWID was registered — the order in which a CSI driver connects a volume. Such a path is a raw way into the LUN the map serves; an LVM Volume Group on it would overwrite the volume's data.

## A nested cluster's Logical Volumes are still active on the hypervisor after the upgrade. What do I do with them?

Before the agent learned to keep away from published block volumes, it could activate a nested cluster's Volume Group right through a virtual machine's disk: the guest's thin pools and Logical Volumes appeared on the hypervisor as device-mapper devices stacked on the published volume. Upgrading does not undo that. The agent now rejects the volume and everything stacked on it, so its `lvs` no longer lists those Logical Volumes and `vgchange -an` cannot find their Volume Group — but the device-mapper devices stay, and they hold the volume open. The symptom appears when the virtual machine's disk is deleted: `lvremove` fails with `Logical volume … is used by another device`, and the PersistentVolumeClaim stays `Terminating`.

Remove those mappings by hand, on the node:

1. Find them. The agent logs the set when it starts and whenever it changes — `devices published to workloads as block volumes, with their partitions and holders: <count>: <kernel names>` — and `dmsetup ls --tree` shows what is stacked on each published volume. A device stacked on a published volume is the guest's, seen through the volume.
2. Make sure nothing on the hypervisor holds it: `dmsetup info -c --noheadings -o name,open_count <name>` must report `0`.
3. Remove it, from the top of the stack down: `dmsetup remove <name>`. A mapping that still has something on top of it answers `Device or resource busy` — remove that first.

The guest's data is not touched: the virtual machine reads its disk through the published volume, not through the hypervisor's mapping, and the guest's own LVM activates its Logical Volumes inside the machine as before. Delete the stale [LVMVolumeGroup](./cr.html#lvmvolumegroup) resources that described those Volume Groups afterwards; the agent no longer sees the Volume Group, so deleting the resource does not touch it.

## Why does an LVMVolumeGroup report `BlockDeviceNotFound` or `NodeNotDescribed`?

Both reasons appear on the `VGReady` condition and mean the same thing: a Physical Volume of the Volume Group has no [BlockDevice](./cr.html#blockdevice) resource naming it, so `status.nodes` cannot list that device. They differ in how much of the node the agent could still describe, and that difference decides whether the Volume Group keeps taking new volumes.

| Reason | Meaning | What to do |
|---|---|---|
| `BlockDeviceNotFound` | Some Physical Volumes were named, some were not. `status.nodes` was refreshed in that same pass, so `vgSize`, `vgFree` and thin-pool usage are current — only the entries for the unnamed devices are missing. The Volume Group stays `Ready` and keeps receiving volumes. | Usually nothing: it clears within seconds, once the block-device discoverer registers the device. If it persists, the device is one that never becomes a `BlockDevice` — see below. |
| `NodeNotDescribed` | Not one Physical Volume could be named, so the agent declined to overwrite `status.nodes` at all. What the resource shows is an earlier pass's status, and its free space is as old as that pass. The LVMVolumeGroup leaves `Ready` on purpose, and the scheduler stops placing new volumes on it. | Find out why the node's devices produce no `BlockDevice` at all — see below — then check the agent's log on that node. |

A device never becomes a `BlockDevice` in two cases, and in both the condition stays until you act:

- it is smaller than the minimum size the controller accepts (see the [requirements](./resources.html#controller-requirements-for-devices));
- it is excluded by a [BlockDeviceFilter](./cr.html#blockdevicefilter).

The agent retries for a bounded number of discovery passes and then stops, logging that it has given up; the condition keeps reporting the state. Editing the `BlockDeviceFilter` to admit the device runs a discovery pass of its own, so the Volume Group recovers without an agent restart.

An LVMVolumeGroup that has never described its node stays in the `Pending` phase rather than `NotReady`, because the `AgentReady` condition is only ever set on a resource whose `status.nodes` names a node. Its aggregate `Ready` message carries the blocking reason, so `kubectl describe` shows the cause rather than a bare "waiting for the conditions AgentReady to be configured".

## Why did the LVMVolumeGroup resource and Volume Group remain after deletion attempt?

This situation can occur in four cases:

1. The Volume Group contains Logical Volumes.

   The controller is not responsible for deleting Logical Volumes from the node. If the Volume Group created using the resource contains Logical Volumes, delete them manually on the node. After that, the resource and Volume Group along with Physical Volumes will be automatically deleted.

1. The resource has the `storage.deckhouse.io/deletion-protection` annotation.

   This annotation protects the resource and the Volume Group it created from deletion. Remove the annotation by running the command:

   ```shell
   d8 k annotate lvg %lvg-name% storage.deckhouse.io/deletion-protection-
   ```

   After executing the command, the resource and Volume Group will be automatically deleted.

1. The resource has finalizers of other controllers.

   The agent does not touch the Volume Group while another controller still holds the resource. The `VGConfigurationApplied` condition names the finalizers it waits for:

   ```shell
   d8 k get lvg %lvg-name% -o jsonpath='{.status.conditions[?(@.type=="VGConfigurationApplied")].message}'
   ```

   Once their owners remove them, the resource and Volume Group will be automatically deleted.

1. The node of the resource was deleted from the cluster.

   Only the agent of a node removes its finalizer `storage.deckhouse.io/sds-node-configurator`, and a deleted node has no agent. The controller removes the finalizer for it once the node has been gone for `missingNodeGracePeriodMinutes` of the module configuration (10 minutes by default) and no LVMLogicalVolume or LVMLogicalVolumeSnapshot is left on the resource. The Volume Group stays on the node's disks. See [A node was deleted from the cluster](#a-node-was-deleted-from-the-cluster-what-happens-to-its-resources-and-how-do-i-clean-them-up).

## Why is an LVMLogicalVolume not deleted?

The agent removes the Logical Volume only after every other controller has removed its finalizer from the [LVMLogicalVolume](./cr.html#lvmlogicalvolume): a finalizer of another module, such as `storage.deckhouse.io/sds-local-volume-csi` of `sds-local-volume`, means the volume may still be in use. While it waits, the resource reports it in the `Ready` condition: `status: "False"`, `reason: Deleting`, and the finalizers it waits for in the message:

```shell
d8 k get lvmlogicalvolume <name> -o jsonpath='{.status.conditions[?(@.type=="Ready")]}'
```

The finalizer belongs to the module that put it there and is removed by that module once it has released the volume (for `sds-local-volume`, when the PersistentVolume is deleted). Look for the cause on that side rather than removing the finalizer by hand: doing so lets the agent delete a Logical Volume that may still hold data in use.

## A node was deleted from the cluster. What happens to its resources, and how do I clean them up?

Deleting the Node object of a node does not delete the module's resources of that node, and does not touch its disks:

- **LVMVolumeGroup.** The controller sets its `NodeReady` condition to `False` with the reason `MissingNode`, and records when it first found the node gone in the `storage.deckhouse.io/node-missing-since` annotation. The resource is not deleted automatically: the Volume Group on the disks outlives the node, and a node reinstalled under the same name finds it again. An LVMVolumeGroupSet stops listing it in its status but does not delete it either. After 6 hours the `D8SdsNodeConfiguratorLVMVolumeGroupNodeMissing` alert fires.
- **LVMLogicalVolume and LVMLogicalVolumeSnapshot.** They stay as they are until they are deleted.
- **BlockDevice.** The controller marks them `Missing` with the reason `NodeNotFound` and deletes them after `missingBlockDeviceRetentionHours`, except those an LVMVolumeGroup still refers to: they go after the LVMVolumeGroup.

A node counts as gone only when the API server has no Node object with its name. A node that is `NotReady`, or whose agent is not running, is still in the cluster, and nothing described here happens to its resources.

The agent normally removes its finalizer `storage.deckhouse.io/sds-node-configurator` after it has cleaned up on the node. For a resource of a deleted node, the controller removes it instead, once:

- the resource is being deleted;
- the node has been gone for `missingNodeGracePeriodMinutes` of the module configuration (10 minutes by default; `0` disables the removal), counted from the time in the annotation, so a restart of the controller does not start the period over;
- the agent's finalizer is the only one left. A finalizer of another controller, such as `storage.deckhouse.io/sds-local-volume-csi`, is never removed: its owner removes it, usually when the PersistentVolume is deleted;
- for an LVMVolumeGroup, no LVMLogicalVolume or LVMLogicalVolumeSnapshot is left on it.

Every removal is recorded as an Event with the reason `AgentFinalizerReleased`. The Events of cluster-scoped resources are in the `default` namespace:

```shell
d8 k -n default get events --field-selector reason=AgentFinalizerReleased
```

To clean up after a node that will not come back:

1. Make sure the node is gone and will not return under the same name:

   ```shell
   d8 k get node <node-name>
   ```

1. Find its LVMVolumeGroups and the volumes on them:

   ```shell
   d8 k get lvg -o json | jq -r '.items[] | select(.spec.local.nodeName == "<node-name>") | .metadata.name'
   d8 k get lvmlogicalvolume -o json | jq -r '.items[] | select(.spec.lvmVolumeGroupName == "<lvg-name>") | .metadata.name'
   ```

1. Delete the PersistentVolumeClaims and VolumeSnapshots that use those volumes; an LVMLogicalVolume or LVMLogicalVolumeSnapshot that nothing else owns can be deleted directly.
1. Delete the LVMVolumeGroups:

   ```shell
   d8 k delete lvg <lvg-name>
   ```

   The resources go away once the conditions above hold. Their BlockDevices are deleted after `missingBlockDeviceRetentionHours`.

1. If a resource stays in `Terminating`, the `D8SdsNodeConfiguratorResourcesOfMissingNodeStuck` alert names the reason. If the node is gone for good and nothing should wait any more, remove the agent's finalizer by hand. List the finalizers first and remove the agent's by its index, so that no other controller's finalizer is lost:

   ```shell
   d8 k get lvmlogicalvolume <name> -o jsonpath='{.metadata.finalizers}'
   d8 k patch lvmlogicalvolume <name> --type=json -p '[{"op":"remove","path":"/metadata/finalizers/<index>"}]'
   ```

If the server is later added to a cluster again, its disks still carry the Volume Groups and the data. Wipe them before reusing the disks, or let the agent import the Volume Group as described in [How do I transfer control of an existing LVM Volume Group to the controller?](#how-do-i-transfer-control-of-an-existing-lvm-volume-group-to-the-controller).

## How do I remove a thin pool?

Remove it from `spec.thinPools` of the [LVMVolumeGroup](./cr.html#lvmvolumegroup). The agent removes the thin pool from the node once it holds no thin volumes.

While the thin pool still holds volumes, the agent keeps it and the volumes in it, and the `VGConfigurationApplied` condition is `False` with the reason `ThinPoolHasVolumes` and the volumes in the message:

```shell
d8 k get lvg %lvg-name% -o jsonpath='{.status.conditions[?(@.type=="VGConfigurationApplied")]}'
```

Delete those volumes first, for example the PersistentVolumeClaims behind them. The agent removes the thin pool on its next pass. The LVMVolumeGroup stays `Ready` meanwhile.

Only a thin pool that was listed in `spec.thinPools` is removed: the agent tags every such thin pool with `storage.deckhouse.io/lvmVolumeGroupName=<LVMVolumeGroup name>`. A thin pool that the spec never listed, such as one in a Volume Group the module took over, is not touched.

Removing a thin pool from the template of an [LVMVolumeGroupSet](./cr.html#lvmvolumegroupset) removes it from every LVMVolumeGroup of the set.

Before removing a thin pool, remove it from the classes of storage that use it: the LocalStorageClass of `sds-local-volume` and the ReplicatedStoragePool of `sds-replicated-volume`.

## How does allocationLimit of a thin pool work, and can I change it?

`allocationLimit` in `spec.thinPools[]` of an [LVMVolumeGroup](./cr.html#lvmvolumegroup) limits overprovisioning of the thin pool. A thin volume takes space in the pool only as data is written to it, so the total size of the volumes may exceed the size of the pool. The agent allows that up to the pool size multiplied by `allocationLimit` and reports what is left in `status.thinPools[].availableSpace`:

```text
availableSpace = actualSize × allocationLimit − allocatedSize
```

`allocatedSize` is the total size of all thin volumes in the pool, snapshots included. `availableSpace` is therefore not the physical free space of the pool: how much of the pool is actually taken by data is shown by `usedSize`.

The default is `150%`, which allows overprovisioning. `100%` turns it off: the volumes in the pool can then never hold more data than the pool fits. The limit counts data space only. The pool metadata can run out regardless of it, so monitor the thin pool with the `sds_node_configurator_lvm_thin_pool_used_percent` and `sds_node_configurator_lvm_thin_pool_metadata_used_percent` metrics. If the pool runs out of space, writes to its volumes fail and data may be lost.

A request that does not fit into `availableSpace` is refused rather than carried out:

- a new thin LVMLogicalVolume, or an expansion of an existing one, gets the `Failed` phase with `not enough space` in the reason, and the agent retries it;
- an LVMLogicalVolumeSnapshot stays `Pending` until `availableSpace` is at least the size of the source volume: a snapshot is counted at the full size of its source, although it shares the data with it;
- the scheduler extender does not place a new volume on a node whose thin pool lacks the space.

The value can be changed at any time, for both a new and an existing thin pool. The agent applies it on its next pass, and `status.thinPools[].allocationLimit` and `availableSpace` show the result:

```shell
d8 k get lvg %lvg-name% -o jsonpath='{range .status.thinPools[*]}{.name}{"\t"}{.allocationLimit}{"\t"}{.allocatedSize}{"\t"}{.availableSpace}{"\n"}{end}'
```

Lowering the limit below what is already allocated deletes nothing: the existing volumes and snapshots stay as they are and keep working, `availableSpace` becomes negative, and new volumes, expansions and snapshots in the pool are refused until volumes are deleted or the limit is raised again.

A thin LVMLogicalVolume whose `size` is set in percent takes that share of the pool size multiplied by `allocationLimit`. Raising the limit therefore makes the agent extend such a volume if `availableSpace` allows it; lowering the limit does not shrink it.

The limit applies to the volumes of `sds-local-volume`. `sds-replicated-volume` does not use it: overprovisioning of its LVMThin pools is limited by LINSTOR.

## What happens when one thin pool of an LVMVolumeGroup cannot be created?

The LVMVolumeGroup stays `Ready`, and its other thin pools keep receiving volumes. The `VGConfigurationApplied` condition is `False` with the reason `ThinPoolCreationFailed` (the Volume Group was being created) or `ThinPoolReconcileFailed` (a thin pool of an existing Volume Group could not be created, extended or removed), and its message names the thin pools that failed and the `lvcreate` error. A thin pool that was not created is absent from `status.thinPools`, and `status.thinPoolReady` shows how many of the declared thin pools exist. Volumes that ask for that thin pool are not scheduled to the node. The agent retries on every reconcile, so the thin pool appears once its cause is fixed, for example once there is enough free space in the Volume Group.

## Why is it not possible to create a Volume Group using the LVMVolumeGroup resource?

The resource does not pass controller validation (Kubernetes validation was successful). The reason can be seen in the `status.message` field of the resource or in the controller logs.

Most often the problem is related to incorrectly specified [BlockDevice](./cr.html#blockdevice) resources. Ensure that the selected resources meet the following requirements:

- The `status.consumable` field has the value `true`.
- For `Local` type Volume Groups, the specified [BlockDevice](./cr.html#blockdevice) resources belong to the same node.<!-- > - For `Shared` type Volume Groups, a single [BlockDevice](./cr.html#blockdevice) resource is specified. -->
- Current [BlockDevice](./cr.html#blockdevice) resource names are specified.

The complete list of expected values is available in the [LVMVolumeGroup](./cr.html#lvmvolumegroup) resource description.

## Why is an LVMVolumeGroup not created with `/dev/<vg>: already exists in filesystem`?

The `VGConfigurationApplied` condition of the LVMVolumeGroup is `False` with the reason `VGNameTakenOnNode`. `vgcreate` refuses to create a Volume Group while the path `/dev/<vg>` exists on the node, even if LVM knows no Volume Group of that name. Such a directory is left when a Volume Group is removed by hand, for example with `dmsetup remove` or by wiping the disks, and its entries (links to logical volumes, for example to old LINSTOR snapshots) now point nowhere. The condition message names the node and the path.

The agent does not remove `/dev/<vg>` itself: a Volume Group of that name can exist on devices that the module's filter hides from it.

To check, run on the node:

```shell
ls -l /dev/<vg>
vgs <vg>
```

If `vgs` reports `Volume group "<vg>" not found` and every entry in `/dev/<vg>` is a dangling link, remove the directory:

```shell
rm -r /dev/<vg>
```

The agent retries on its own, so the LVMVolumeGroup is created on the next reconcile and no other action is needed.

## Why is a BlockDevice not consumable with the reason `HasPartitionTable`?

The device carries a partition table, possibly one without any partitions in it. The kernel then shows no partitions, and `lsblk` shows the disk as empty, but LVM refuses to create a Physical Volume on it. Depending on the LVM version, the error is either `Cannot use /dev/<device>: device is partitioned` or `WARNING: gpt signature detected on /dev/<device> …` followed by `Aborted wiping of gpt.`. A disk that was partitioned before and cleaned only partially, or that came with a GPT header from the vendor, often looks like this.

The agent never wipes devices: whether the table is disposable is the owner's decision. Check what the device carries:

```shell
blkid /dev/<device>
wipefs /dev/<device>
```

If the device holds nothing you need, remove the signatures; the BlockDevice becomes consumable on the agent's next scan:

```shell
wipefs --all /dev/<device>
```

The command removes the partition table, after which the data in its partitions can no longer be reached.

## Why is a BlockDevice that is already a Physical Volume consumable with the reason `OrphanPhysicalVolume`?

The device is an LVM Physical Volume that belongs to no Volume Group: `pvs` shows it with an empty `VG` column. It is what an LVMVolumeGroup creation that failed after `pvcreate` leaves behind, or a Physical Volume created by hand and never added to a Volume Group. A Volume Group takes such a device as it is, so the agent offers it like an empty device: a new LVMVolumeGroup can use it, and the retry of the failed one succeeds.

A device that carries a Physical Volume of a Volume Group, any Volume Group, stays not consumable with the reason `HasFilesystem`. If an orphaned Physical Volume should not be used, remove it on the node with `pvremove /dev/<device>`.

## How do I give the module the free space on the system disk?

The disk the OS is installed on and its partitions in use are not available to the module (see [The system disk](./resources.html#the-system-disk) for why). The free space on it can be given to the module in one of the following ways, depending on where the space is:

- **Unpartitioned space on the disk.** Create a new partition in it. The controller creates a [BlockDevice](./cr.html#blockdevice) resource for the partition if the partition meets the [requirements](./resources.html#controller-requirements-for-devices): it has no file system, it is larger than 1 GB, and it has a PARTUUID (see [Creating a partition on a block device](./resources.html#creating-a-partition-on-a-block-device)). If `lsblk` does not show the partition after it is created (the kernel may not re-read the partition table of a disk that is in use), add the partition to the kernel with `partx --add --nr <partition number> /dev/<disk>`.
- **Free space in the LVM Volume Group that holds the root.** Hand the Volume Group over to the module with the LVM tag `storage.deckhouse.io/enabled=true` (see ["How do I transfer control of an existing LVM Volume Group to the controller?"](#how-do-i-transfer-control-of-an-existing-lvm-volume-group-to-the-controller)). The controller creates an [LVMVolumeGroup](./cr.html#lvmvolumegroup) resource for it, and its Physical Volumes get BlockDevice resources that are not consumable, with the reason `HasFilesystem`. To take the Volume Group back from the module, remove the tag (see ["How do I stop the controller from tracking an LVM Volume Group?"](#how-do-i-stop-the-controller-from-tracking-an-lvm-volume-group)).
- **Free space in a file system.** Use file-backed devices (`spec.fileDevices`): the agent creates a file in the `fileDevicesDirectory` directory, attaches it as a loop device, and uses it as a Physical Volume (see [Creating an LVMVolumeGroup with file-backed devices](./resources.html#creating-an-lvmvolumegroup-with-file-backed-devices)). The default directory is on the same file system as `/var/lib/kubelet` and `/var/lib/containerd`, so only a small amount of space can be given this way; for the limitations, see ["How do file-backed devices (fileDevices) work?"](#how-do-file-backed-devices-filedevices-work).

## What happens if I disconnect one of the devices in a Volume Group? Will the corresponding LVMVolumeGroup resource be deleted?

The [LVMVolumeGroup](./cr.html#lvmvolumegroup) resource exists as long as the corresponding Volume Group exists. As long as at least one device exists, the Volume Group is preserved but marked as non-functional. The current state is reflected in the `status` field of the resource.

After restoring the disconnected device on the node, the LVM Volume Group will restore functionality, and the corresponding [LVMVolumeGroup](./cr.html#lvmvolumegroup) resource will display the current state.

## How do I transfer control of an existing LVM Volume Group to the controller?

Add the LVM tag `storage.deckhouse.io/enabled=true` to the LVM Volume Group on the node:

```shell
vgchange myvg-0 --addtag storage.deckhouse.io/enabled=true
```

## Can two LVMVolumeGroup resources describe the same Volume Group?

No. A Volume Group of a node is described by one LVMVolumeGroup. Creating a second resource with the same `spec.local.nodeName` and `spec.actualVGNameOnTheNode` is rejected by the module's webhook with a message that names the existing resource; edit that resource instead.

If a second resource gets into the cluster anyway, for example while the webhook was unavailable, the agent does not serve it: the resource gets `VGConfigurationApplied=False` with the reason `ValidationFailed` and a message that names the resource serving the Volume Group, and nothing is changed on the node for it. The Volume Group is served by the resource its `storage.deckhouse.io/lvmVolumeGroupName` tag names or, when the tag names neither, by the older resource. Delete the extra resource: the Volume Group stays, because it is not tagged for it.

## How do I stop the controller from tracking an LVM Volume Group?

Remove the LVM tag `storage.deckhouse.io/enabled=true` from the desired LVM Volume Group on the node:

```shell
vgchange myvg-0 --deltag storage.deckhouse.io/enabled=true
```

After this, the controller will stop tracking the selected Volume Group and will independently delete the associated [LVMVolumeGroup](./cr.html#lvmvolumegroup) resource.

## Why does `lvs` not show the module's volume groups on a node with an LVM devices file (RHEL 9, Rocky Linux 9, AlmaLinux 9)?

Starting with RHEL 9, the host's LVM uses only the devices listed in its devices file (`/etc/lvm/devices/system.devices`) by default. The agent runs LVM commands with `--devicesfile ""`: it neither reads nor changes this file and selects devices by its own filter. Disks the agent gives to LVMVolumeGroup resources are not added to the host's devices file, so `lvs`, `vgs` and LVM autoactivation at boot do not see them. The agent activates the module's volume groups itself after the node boots.

To see the module's volume groups on the node:

```shell
vgs --devicesfile ""
lvs --devicesfile ""
```

To make the host's LVM see a disk permanently, add it to the devices file:

```shell
lvmdevices --adddev /dev/sdX
```

## Why does the LVM tag `storage.deckhouse.io/enabled=true` appear automatically?

The LVM tag appears in the following cases:

- LVM Volume Group was created through the [LVMVolumeGroup](./cr.html#lvmvolumegroup) resource. In this case, the controller automatically adds the LVM tag `storage.deckhouse.io/enabled=true` to the created LVM Volume Group.
- The Volume Group or its thin pool had the `linstor` module LVM tag — `linstor-*`.

When migrating from the built-in `linstor` module to `sds-node-configurator` and `sds-replicated-volume` modules, `linstor-*` LVM tags are automatically replaced with `storage.deckhouse.io/enabled=true` in Volume Groups. Management of these Volume Groups is transferred to the `sds-node-configurator` module.

## Why is a Volume Group with the module's tags not imported, and how do I adopt it?

A Volume Group that carries `storage.deckhouse.io/enabled=true` is imported automatically — an [LVMVolumeGroup](./cr.html#lvmvolumegroup) is created for it — with one exception. When the Volume Group also carries `storage.deckhouse.io/lvmVolumeGroupName=<name>` and no LVMVolumeGroup named `<name>` exists in this cluster, the agent refuses the import.

The reason is where that tag comes from: the agent writes it when it creates a Volume Group, and nothing else does. A Volume Group tagged with a name nobody in this cluster knows was therefore created by another cluster's agent — typically a nested cluster running in a virtual machine whose disk this node can see (a SAN LUN mapped to several hosts, a LUN mapped by hand to both the hypervisor and the guest, a LUN whose volume was deleted but whose mapping was not). Importing it would make the Volume Group of a running cluster look like this node's storage, and deleting the resulting LVMVolumeGroup would run `vgremove` on it.

The refusal is reported once in the agent's log on the node (`not importing VG <vg>: its storage.deckhouse.io/lvmVolumeGroupName tag names the LVMVolumeGroup <name>, which does not exist in this cluster …`) and counted in the `sds_node_configurator_lvm_volume_group_import_refused_total` metric with `lvg_name=<name>` (empty when `<name>` is not a valid resource name: another cluster's string does not become a series of this cluster's metrics).

The same refusal meets a cluster bootstrapped anew on nodes that kept their disks — a reinstall, a recovery that did not restore etcd, a node moved with its data into another cluster: every Volume Group there carries the name of the previous cluster's resource, and none is imported until an operator acts as described below. Earlier versions of the module imported them silently.

If the Volume Group really is this node's — its LVMVolumeGroup was deleted here, for example around its finalizer — take it under management deliberately, in one of two ways:

- Create an LVMVolumeGroup with the name from the tag, pointing at the existing Volume Group. The agent finds the Volume Group on the node and adopts it instead of creating a new one:

  ```yaml
  apiVersion: storage.deckhouse.io/v1alpha1
  kind: LVMVolumeGroup
  metadata:
    name: <name from the tag>
  spec:
    type: Local
    local:
      nodeName: <node>
    actualVGNameOnTheNode: <vg>
    blockDeviceSelector:
      matchExpressions:
        - key: kubernetes.io/metadata.name
          operator: In
          values:
            - <BlockDevice of each Physical Volume of the Volume Group>
  ```

- Or remove the name tag on the node, after which the Volume Group is imported as a newly handed-over one under a generated name:

  ```shell
  vgchange --deltag storage.deckhouse.io/lvmVolumeGroupName=<name> <vg>
  ```

Do neither for a Volume Group that belongs to another cluster.

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

The controller keeps the `thinPools`, `blockDeviceSelector` and `fileDevices` fields of the created resources in line with the template, and recreates a resource of the set that is deleted while its node still matches the selector. Deleting the set leaves its LVMVolumeGroup resources in place.

The `CURRENT LVG COUNT`, `DESIRED LVG COUNT` and `READY LVG COUNT` columns of `d8 k get lvgset` show how many LVMVolumeGroup resources the set has, how many nodes match the selector, and how many of the resources are in the `Ready` phase.

## How do I make storage on a new node come up by itself?

Mark the node with a label of your own before it joins the cluster, and select nodes by that label in the module settings and in an [LVMVolumeGroupSet](./cr.html#lvmvolumegroupset). A node with the label then gets the agent and its LVMVolumeGroup without any action in the cluster.

1. On the node, before adding it to the cluster, put the label into a file under `/var/lib/node_labels`, one `key=value` per line. Deckhouse puts labels from these files on the node when the node is added to the cluster (see "How to automatically put custom labels on the node" in the Deckhouse documentation on node customization):

   ```shell
   mkdir -p /var/lib/node_labels
   echo "storage.example.com/local-nvme=true" > /var/lib/node_labels/storage
   ```

   System labels such as `kubernetes.io/hostname` or `node.deckhouse.io/group` cannot be set this way.

2. Select the same label in `spec.settings.dataNodes.nodeSelector` of the `sds-local-volume` or `sds-replicated-volume` module, so that the module, and with it the `sds-node-configurator` agent, runs on the node:

   ```shell
   d8 k patch mc sds-local-volume --type merge -p '{"spec":{"settings":{"dataNodes":{"nodeSelector":{"storage.example.com/local-nvme":"true"}}}}}'
   ```

3. Create an LVMVolumeGroupSet that selects the same label, as in [How do I create LVMVolumeGroup using LVMVolumeGroupSet?](#how-do-i-create-lvmvolumegroup-using-lvmvolumegroupset):

   ```yaml
   apiVersion: storage.deckhouse.io/v1alpha1
   kind: LVMVolumeGroupSet
   metadata:
     name: local-nvme
   spec:
     strategy: PerNode
     nodeSelector:
       matchLabels:
         storage.example.com/local-nvme: "true"
     lvmVolumeGroupTemplate:
       type: Local
       blockDeviceSelector:
         matchLabels:
           status.blockdevice.storage.deckhouse.io/model: <model>
       actualVGNameOnTheNode: local-nvme
   ```

When such a node joins the cluster, it gets the label, the agent starts on it and creates BlockDevice resources for its disks, and the controller creates an LVMVolumeGroup for the node from the template, which builds the Volume Group on the disks matching `blockDeviceSelector`. To add storage to a node that is already in the cluster, put the label on it with `d8 k label node`.

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

If the Volume Group was created by the module, the clone also carries its tag `storage.deckhouse.io/lvmVolumeGroupName=<name>`, which names the LVMVolumeGroup of the source node. The agent on the clone does not import such a Volume Group: it logs `not importing VG <VG_NAME>: its storage.deckhouse.io/lvmVolumeGroupName tag names the LVMVolumeGroup <name>, which already exists for the same VG on node <source node> …` and counts the refusal in the `sds_node_configurator_lvm_volume_group_import_refused_total` metric. After changing the UUID, remove the tag on the clone:

```shell
vgchange --deltag storage.deckhouse.io/lvmVolumeGroupName=<name> <VG_NAME>
```

The Volume Group is then imported as a new one, under a generated LVMVolumeGroup name. Do not remove the tag on the source node. For more on this tag, see [Why is a Volume Group with the module's tags not imported, and how do I adopt it?](#why-is-a-volume-group-with-the-modules-tags-not-imported-and-how-do-i-adopt-it).

If necessary, the commands can be added to the `cloud-init` script for automatic execution when creating virtual machines.

When cloning, also check the disk serial numbers: if the template sets them explicitly, the clones end up sharing them — see ["How to configure serial numbers for virtual disks?"](#how-to-configure-serial-numbers-for-virtual-disks).

## How to configure serial numbers for virtual disks?

The controller identifies block devices by their serial numbers, so a virtual disk without a serial number never becomes a [BlockDevice](./cr.html#blockdevice) resource — and therefore never joins an LVM volume group.

### Why the serial number is required

The name of a [BlockDevice](./cr.html#blockdevice) resource is a hash of the node name, WWN, model, serial number, and PartUUID of the device. The serial number is required on its own: if the agent cannot resolve it, the device is skipped even when it has a WWN and a model. The log then contains a warning:

```text
WARNING [CreateCandidateName] Unable to obtain serial number or its equivalent; skipping device: /dev/vdc. Error: serial is empty
```

To find such devices on a node:

```shell
d8 k -n d8-sds-node-configurator logs <agent-pod-name> -c sds-node-configurator-agent | grep 'Unable to obtain serial number'
```

Partitions are the exception: for a partition the PartUUID is enough, and the serial number of the parent disk is not required.

The serial number also becomes the `status.blockdevice.storage.deckhouse.io/serial` label, which lets you select the device in the `blockDeviceSelector` of [LVMVolumeGroup](./cr.html#lvmvolumegroup) and [BlockDeviceFilter](./cr.html#blockdevicefilter) resources.

### How to check whether the guest OS sees the serial number

Run the following on the node:

```shell
# the same fields the agent reads
lsblk -o NAME,TYPE,SIZE,SERIAL,WWN,MODEL

# where lsblk takes SERIAL from — the udev properties of the device
udevadm info --query=property --name=/dev/sda | grep -E 'ID_SERIAL|ID_SCSI_SERIAL|SCSI_IDENT'

# the fallback source the agent uses for virtio-blk and NVMe
cat /sys/block/sda/serial
```

`lsblk` fills the `SERIAL` field from udev in the following order: `SCSI_IDENT_SERIAL` → `ID_SCSI_SERIAL` → `ID_SERIAL_SHORT` → `ID_SERIAL`. If all of them are empty, the agent additionally reads `/sys/block/<device>/serial`, or `/sys/block/<device>/md/uuid` for mdraid. Multipath devices and partitions inherit the serial number from their parent device.

{{< alert level="info" >}}
The sysfs `serial` attribute exists for virtio-blk and NVMe devices, but not for SCSI disks. For a disk attached over a SCSI bus (including virtio-scsi), udev is the only source of the serial number, so empty output of `lsblk -o SERIAL` means the hypervisor does not expose the identifier.
{{< /alert >}}

### Configuration in virtualization systems

| Virtualization system | What to set | Notes |
| --- | --- | --- |
| Deckhouse Virtualization Platform | Nothing to configure | Every `VirtualDisk` resource is attached to the virtual machine over the virtio-scsi bus with a serial number already set. The serial number is the MD5 hash of the `.metadata.uid` of the `VirtualDisk` resource, so it can be used to find the disk in the DVP cluster (see below) |
| QEMU/KVM (libvirt) | The `<serial>` element inside `<disk>` in the domain XML | Allowed characters are Latin letters, digits, `_`, `.`, `+`, `-`, and space. The serial number is accepted by the virtio-blk, scsi-hd, ide-hd, and nvme device models; with full device passthrough (`scsi-block`) the serial number comes from the physical disk and cannot be set manually |
| Proxmox VE | The `serial=` sub-option of the disk option, for example `scsi0: local-lvm:vm-100-disk-0,serial=disk0` | The value is url-encoded and up to 20 bytes long; supported for `ide[n]`, `sata[n]`, `scsi[n]`, and `virtio[n]` disks |
| VMware vSphere | The `disk.EnableUUID = "TRUE"` parameter in the virtual machine configuration | The guest OS receives the disk identifier from VPD page 0x83; to check it inside the guest OS, run `/lib/udev/scsi_id --whitelisted --device=/dev/sda` |
| Hyper-V | The serial number cannot be set | A Hyper-V virtual disk has no serial number and the property is not configurable. Use the partition workaround (see below) |

To match a disk in the guest OS with a `VirtualDisk` resource in DVP:

```shell
# in the guest OS: the serial number of the disk
lsblk -o NAME,SERIAL /dev/sda

# in the DVP cluster: the same identifier for the VirtualDisk in question
d8 k -n <namespace> get virtualdisk <name> -o jsonpath='{.metadata.uid}' | tr -d '\n' | md5sum
```

### If the serial number cannot be set

Create a partition on the disk (see [Creating a partition on a block device](./resources.html#creating-a-partition-on-a-block-device)) and hand the partition to the controller: a partition only needs a PartUUID, so the [BlockDevice](./cr.html#blockdevice) resource is created even when the disk has no serial number.

Make sure the partition has a PARTUUID:

```shell
lsblk -o NAME,TYPE,PARTUUID /dev/sdb
```

After that, a [BlockDevice](./cr.html#blockdevice) resource appears for the `/dev/sdb1` partition, and it is that resource the `blockDeviceSelector` of the [LVMVolumeGroup](./cr.html#lvmvolumegroup) has to select — by the `kubernetes.io/metadata.name` label.

### Limitations to keep in mind

- **The serial number must be unique within a node.** The name of a [BlockDevice](./cr.html#blockdevice) resource is built from the node name, WWN, model, serial number, and PartUUID. Two disks on the same node that share a serial number and a model and have no WWN produce the same name, so a single resource appears instead of two. The usual cause is cloning virtual machines from a template that sets disk serial numbers explicitly.
- **The label value is transformed.** The value of the `status.blockdevice.storage.deckhouse.io/serial` label is converted into a label-safe form: letters are lowercased and the remaining characters are replaced with a hyphen. Use the already converted value in `blockDeviceSelector` — you can see it with `d8 k get blockdevices --show-labels`.

## How do file-backed devices (fileDevices) work?

File-backed devices allow you to allocate part of an existing filesystem for LVM without dedicated block devices. The agent creates a preallocated file in the specified directory, attaches it as a loop device via `losetup`, and uses it as an LVM Physical Volume.

### Limitations

- **Host tooling on the node**: unlike `lvm`, `nsenter` and `lsblk`, which the agent ships under `/var/lib/deckhouse/sds/bin`, the loop and file operations run the node's own `losetup`, `fallocate`, `stat`, `mkdir` and `rm` in PID 1's mount namespace. `losetup` must support `--nooverlap`, i.e. **util-linux 2.29 or newer** (2016; every currently supported distribution qualifies, RHEL/CentOS 7 does not). On an older host every provisioning attempt fails with `unrecognized option '--nooverlap'`, reported on the resource as `FileDeviceNotApplied`. `--direct-io` is not a requirement: when it is missing or the kernel refuses it, the agent logs a warning and continues with buffered I/O.
- **Confined to a base directory**: Each `directory` must be the `fileDevicesDirectory` module setting (default `/var/lib/deckhouse/sds/file-devices`) or a subdirectory of it. Paths outside this subtree are rejected so an arbitrary host path cannot be filled up, with one exception: the previous default, `/opt/deckhouse/sds/file-devices`, is still accepted so that entries created before it moved keep working (see [Backing files created before the base directory moved](#backing-files-created-before-the-base-directory-moved)).
- **The default shares a filesystem with kubelet and containerd**: On a standard node layout `/var/lib/deckhouse/sds/file-devices` sits on the same filesystem as `/var/lib/kubelet` and `/var/lib/containerd`, whose sizing DKP accounts for in full. Filling it up evicts Pods and breaks image pulls on the node, so for anything beyond a small entry mount a dedicated disk and point `fileDevicesDirectory` at it. The `fileDevicesMinFreeSpacePercent` reserve below is the guard, not a licence to skip this.
- **Directory auto-created**: The agent creates the backing directory automatically (`mkdir -p`) on the node if it does not exist. The path must be absolute and free of `..` segments — a relative path is rejected at admission, a `..` segment by the agent — and `directory` cannot be changed afterwards. Provisioning then fails only if the path is on a read-only filesystem or a non-directory component is in the way.
- **Growth**: `size` can be increased on an existing entry — the backing file, the loop device and the Physical Volume grow in place, online, without unmounting anything. `directory` cannot be changed; to take capacity from a different filesystem, add a new entry under a new `name` (at most 32 entries).
- **No shrink**: `size` cannot be lowered, and the edit is rejected at admission. Returning capacity means shrinking the Volume Group (`pvmove` + `vgreduce`), which can be impossible when the remaining Physical Volumes have no room and is destructive when it is not; the module does not shrink a Volume Group for block devices either. To reclaim space without changing sizes, use discard/TRIM (see below).
- **Removing an entry**: An entry that was never provisioned can be removed from the spec. Removing one that backs a live Physical Volume is reported as drift on the `VGConfigurationApplied` condition (reason `FileDeviceDrift`) and never acted upon. The Volume Group keeps working and stays `Ready` — the reason is treated as an acceptable state, so the report does not take the node's storage out of service. To resolve the drift, either restore the entry or remove the Physical Volume by hand: `pvmove` + `vgreduce` + `pvremove`.
- **Preallocated only**: Files are created with `fallocate`, which preallocates space on the filesystem. The agent refuses to create or grow a backing file that would leave the filesystem with less free space than the `fileDevicesMinFreeSpacePercent` module setting reserves (15% by default), so a too-large entry is reported on the resource instead of pushing the node into `DiskPressure`. Lower the setting only when `fileDevicesDirectory` points at a disk nothing else on the node depends on.
- **Minimum size**: Each file device must be at least 1Gi.
- **Performance overhead**: LVM on a loop device over a filesystem adds double indirection. Use file-backed devices only when dedicated disks are not available. The agent asks the loop driver for direct I/O to avoid caching every page twice; on a backing filesystem that does not support `O_DIRECT` (for example tmpfs) the kernel refuses, the agent logs a warning and continues with buffered I/O.
- **Host LVM filter**: NodeGroupConfiguration adds loop devices to the host-wide LVM `global_filter`, so `lvm`/`pvs` run on the node do not see them (the filter is in `lvm.conf` and applies regardless of privileges). The agent re-attaches managed backing files at startup and passes its own `--config` for managed Volume Groups.
- **No dmeventd monitoring of thin pools on file devices**: `dmeventd` reads the host's `lvm.conf`, so the filter above hides the loop-backed Physical Volumes from it. A thin pool on a file-backed Volume Group therefore gets neither the kernel-side autoextend nor the "pool is 80% full" warnings that a thin pool on a block device gets. This is not a regression in capacity management — the module sizes thin pools from `spec.thinPools` rather than from autoextend — but the usual `dmeventd` messages will not appear. Watch the module's own metrics instead (`sds_node_configurator_lvg_thin_pool_used_size_bytes` and the `..._file_devices_directory_*` gauges below).
- **A stray `losetup` on the node is left alone**: The agent recognises a loop-backed Volume Group as its own only when the backing file's basename matches `sds-<lvgName>.<entryName>.img`, and it will not activate, re-tag, adopt or reconcile one that does not. That matters for images that carry the module's own LVM tags — a backup of a node disk attached with `losetup -f` for a restore, or a nested cluster on a rawfile-backed volume: the tag alone would make such a Volume Group look managed, and one sharing a name with a live Volume Group would then be reported as a duplicate. Nothing on the host needs to be done about it, but the agent log names any Volume Group it skips for this reason.
- **Never `rm` a backing file while its loop device is attached**: The Physical Volume stays live on the unlinked inode — `losetup -a` shows the path as `(deleted)` — while the path itself resolves to nothing. The agent detects this and refuses to provision the entry, reporting `FileDeviceNotApplied` rather than creating a second file at the same path (which would add a second Physical Volume and double the Volume Group). Recover by restoring the file, or by moving the Physical Volume out with `pvmove` + `vgreduce` + `pvremove` and then removing the entry. To free space inside a file device, use `fstrim` (see below).
- **Never `losetup` a managed backing file yourself**: Two loop devices over one backing file are two Physical Volumes of the same size over the same blocks. The agent refuses to act on such a file at all — it will not provision, grow or clean it up, and deleting the LVMVolumeGroup stops with the loop devices named on the condition rather than detaching one of the two and removing the file the other is still reading from. Detach the extra device with `losetup -d` and the reconcile continues on its own.

### Backing files created before the base directory moved

The default base directory used to be `/opt/deckhouse/sds/file-devices`; it is now `/var/lib/deckhouse/sds/file-devices`, together with the rest of the module's node tree. Entries created under the old default keep working exactly as before, and nothing has to be done about them. Do not create new entries there: the agent does not stop you — it cannot tell a new entry from an old one by the path — but on an immutable node `/opt` is not writable, and each such entry keeps the compatibility path alive:

- `spec.fileDevices[].directory` is immutable, so an existing entry cannot be edited to the new path — the apiserver rejects the change;
- the agent therefore keeps accepting the old directory alongside the configured one. Such an entry is validated, grown, re-attached after a reboot and cleaned up on delete exactly as one under the new default;
- **do not move the backing file.** The entry would still name the old directory, so the next reconcile would create a second backing file there and add a second Physical Volume to the Volume Group.

The module reports what is left: the `sds_node_configurator_legacy_file_devices_directory_lvgs` metric counts the LVMVolumeGroups that still name the old directory, and the module's log names them.

Moving one to the new directory is optional, and is done the same way capacity is moved off any Physical Volume — online, with the volumes on top untouched:

1. Add a second entry under a new `name`, with `directory: /var/lib/deckhouse/sds/file-devices` and at least the `size` of the old one. Wait for its Physical Volume to appear in `status.nodes[].fileDevices`.
2. Move the data off the old Physical Volume: `pvmove <old loop device>`.
3. Take it out of the Volume Group: `vgreduce <vg> <old loop device>`, then `pvremove <old loop device>`.
4. Remove the old entry from `spec.fileDevices`. Nothing is torn down for a removed entry, so detach and delete its backing file by hand: `losetup -d <old loop device>` and `rm <old backing file>`.

Every LVM command above needs the filter override below, or it will not see the loop-backed Physical Volumes at all. Which loop device belongs to which entry is in `status.nodes[].fileDevices` and in `losetup -a`.

### Inspecting a file-backed Volume Group on the node

Because of the host-wide filter above, a plain `pvs` on the node shows a file-backed Volume Group as a VG with no Physical Volumes — which is the first thing that will be run while diagnosing one. Override the filter for a single invocation to see the real picture (this is the same filter the agent itself passes):

```bash
# Physical Volumes, including the loop-backed ones
lvm pvs --config 'devices/global_filter=["r|^/dev/rbd|","r|^/dev/drbd|","r|^/dev/nbd|"]'

# ... and the same for vgs / lvs
lvm vgs --config 'devices/global_filter=["r|^/dev/rbd|","r|^/dev/drbd|","r|^/dev/nbd|"]'

# which backing file each loop minor is attached to
losetup -a

# the backing files the module owns; the basename is sds-<lvgName>.<entryName>.img
ls -l /var/lib/deckhouse/sds/file-devices
```

Do not remove the loop entry from `lvm.conf`: the filter is what keeps the host's own `pvscan`/activation units from claiming these devices behind the agent's back.

### When an entry cannot be applied

An entry the node cannot bring up — no room left for the backing file, `losetup` refusing, a grow that did not go through — is reported on the `VGConfigurationApplied` condition with reason `FileDeviceNotApplied` and retried on every reconcile. The Volume Group itself is untouched: it keeps serving every volume on it and stays `Ready`, and the rest of the reconcile (the other entries, thin-pool growth, Physical Volume resize) goes ahead. Capacity that has not arrived is not the same thing as storage that has broken, so a single bad entry never takes the node's storage out of service.

The reasons the agent reports on this condition while the Volume Group keeps working are:

| Reason | Meaning | What to do |
|---|---|---|
| `ValidationFailed` | An entry is malformed (a directory outside the base path, a size below 1Gi). | Fix the entry. |
| `FileDeviceNotApplied` | The node could not bring an entry up. | Free space in `directory`, or check the agent log for the exact command that failed. |
| `FileDeviceGrowFailed` | Raising an entry's `size` did not go through. Every step of the growth sequence fails towards the smaller size, so the Volume Group is still the size it was. | Free space in `directory`; the round is retried. |
| `AliasResolutionFailed` | The agent cannot canonicalize the PV paths LVM reported and so cannot tell whether a loop device is already in the Volume Group. New file devices will not join until it clears. | Check the agent's `nsenter` binary and the `/dev/disk/by-id` links on the node. |
| `FileDeviceDrift` | An entry backing a live Physical Volume was removed from the spec. | Restore the entry, or remove the Physical Volume by hand. |
| `CacheStale` | The node has a Volume Group the agent's LVM cache does not know about yet, so the reconcile has nothing to work from. Clears itself. | Nothing; it resolves on the next scan. |

One further reason, `VGCheckFailed`, is **not** in this list: it means the agent cannot read the node's Volume Groups at all, and an LVMVolumeGroup whose storage the agent has lost sight of is taken out of service on purpose. Check that `lvm.static` and `nsenter` work inside the agent pod.

#### Undoing a size that is too large

`size` can only be increased, so an entry raised past what the filesystem can hold cannot simply be lowered back — the LVMVolumeGroup would report `FileDeviceNotApplied` on every reconcile. There is a way back without recreating the resource, in two steps:

1. Remove the entry from `spec.fileDevices`. The apiserver accepts the removal. If the entry had already been provisioned, its Physical Volume and backing file are kept and the condition switches to `FileDeviceDrift`.
2. Add the entry back under the same `name` with the size it had before. The transition rule only compares against the entry as it is now, and there is none, so the smaller size is accepted. It matches the existing Physical Volume, nothing is grown, and the LVMVolumeGroup returns to `Ready`.

Between the two steps the Volume Group keeps working; nothing is deleted at any point.

### Reclaiming space (the usual answer to "how do I shrink it?")

Most requests to reduce a file device's size really mean "give me back the space I freed by deleting data". That needs no resize — it needs `fstrim`.

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

### Monitoring

The agent exports four gauges per node for file-backed devices:

| Metric | Labels | Meaning |
|---|---|---|
| `sds_node_configurator_file_device_size_bytes` | `node`, `lvg_name`, `volume_group`, `file_device` | size of the Physical Volume created on one backing file |
| `sds_node_configurator_file_devices_directory_allocated_bytes` | `node`, `directory` | total size the module has preallocated in the directory |
| `sds_node_configurator_file_devices_directory_free_bytes` | `node`, `directory` | free space left on the filesystem holding the directory |
| `sds_node_configurator_file_devices_directory_total_bytes` | `node`, `directory` | size of that filesystem |

The free/total pair is worth an alert. Backing files are preallocated, so the module holds a fixed share of a filesystem it does not own — by default the node's root. The agent keeps `fileDevicesMinFreeSpacePercent` of it out of its own reach, but nothing stops anything else on the node from filling that filesystem afterwards, and running it out triggers kubelet `DiskPressure` eviction for the whole node. Alert on `free / total` falling below the reserve:

```promql
sds_node_configurator_file_devices_directory_free_bytes
  / sds_node_configurator_file_devices_directory_total_bytes < 0.15
```

If the free-space reading fails on a cycle, the previous value is kept rather than reported as zero, so a transient error does not look like a full disk.

One more gauge comes from the module's hooks rather than the agent, so it is cluster-wide and carries no labels:

| Metric | Labels | Meaning |
|---|---|---|
| `sds_node_configurator_legacy_file_devices_directory_lvgs` | — | LVMVolumeGroups whose entries still name the previous default base directory, see [Backing files created before the base directory moved](#backing-files-created-before-the-base-directory-moved) |

It is not an error condition — those entries keep working — but while it is above zero the old directory is still in use on some node. A listing failure leaves the previous value in place instead of reporting zero.

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
- `status.blockdevice.storage.deckhouse.io/partlabel`: GPT name of the partition (PARTLABEL), as a label value; empty if the partition has no name. See [How do I select disks by the GPT partition name?](#how-do-i-select-disks-by-the-gpt-partition-name)
- `status.blockdevice.storage.deckhouse.io/lvmvolumegroupname`: Name of the [LVMVolumeGroup](./cr.html#lvmvolumegroup) resource to which the device belongs.
- `status.blockdevice.storage.deckhouse.io/actualvgnameonthenode`: Volume Group name on the node.
- `status.blockdevice.storage.deckhouse.io/wwn`: WWN (World Wide Name) identifier for the device.
- `status.blockdevice.storage.deckhouse.io/serial`: Device serial number.
- `status.blockdevice.storage.deckhouse.io/size`: Device size.
- `status.blockdevice.storage.deckhouse.io/model`: Device model.
- `status.blockdevice.storage.deckhouse.io/rota`: Indicates whether the device is rotational.
- `status.blockdevice.storage.deckhouse.io/hotplug`: Indicates device hot-plug capability.
- `status.blockdevice.storage.deckhouse.io/machineid`: Identifier of the server where the block device is installed.
- `status.blockdevice.storage.deckhouse.io/scsitarget`: The iSCSI or Fibre Channel target the device is a LUN of; empty for any other device. See [How do CSI drivers hide their LUNs from the module?](#how-do-csi-drivers-hide-their-luns-from-the-module) for the value format.

The agent sets these labels from the resource's status, and `spec.blockDeviceSelector` of LVMVolumeGroup and LVMVolumeGroupSet selects devices by them. Changing, removing or adding a label with one of these keys (`kubernetes.io/hostname`, `kubernetes.io/metadata.name` or the `status.blockdevice.storage.deckhouse.io/` prefix) is rejected by the `d8-sds-node-configurator-blockdevice-label-guard` ValidatingAdmissionPolicy for everyone except the agent. Labels with other keys can be set and removed freely.

## How do I select disks by the GPT partition name?

The agent reads the GPT name of a partition (PARTLABEL) into the `status.partLabel` field of the [BlockDevice](./cr.html#blockdevice) resource and into the `status.blockdevice.storage.deckhouse.io/partlabel` label, so a `blockDeviceSelector` can pick partitions by a name you chose.

Set the name when you create the partition, or later:

```shell
sgdisk -c 1:lvg-data /dev/sdb
```

Select the partition in an [LVMVolumeGroup](./cr.html#lvmvolumegroup):

```yaml
apiVersion: storage.deckhouse.io/v1alpha1
kind: LVMVolumeGroup
metadata:
  name: vg-data
spec:
  type: Local
  local:
    nodeName: worker-0
  actualVGNameOnTheNode: vg-data
  blockDeviceSelector:
    matchLabels:
      status.blockdevice.storage.deckhouse.io/partlabel: lvg-data
```

Keep in mind:

- Only partitions of a GPT table have a name. MBR has no partition names, so a partition of an MBR disk and a whole disk have an empty `status.partLabel` and an empty label value.
- The label value is not the name itself but its slug: spaces and other characters that a label cannot hold become `-`, and non-Latin letters are transliterated, the letter case is kept. `My data` becomes `My-data`, and a Russian word for "data" is written as `Dannye`. Take the value from an existing BlockDevice rather than writing it by hand. The name itself is stored in `status.partLabel`.
- Partition names are not unique: two partitions with the same name get the same label value, and a selector matches both.
- Devices discovered before the update get the label on the next discovery pass of the agent. A name changed with `sgdisk -c` is seen once the kernel rereads the partition table; for a disk in use (a Physical Volume or a mounted partition), after `partprobe` or a node reboot.

## How do CSI drivers hide their LUNs from the module?

A LUN that a CSI driver hands out as a volume must never become a [BlockDevice](./cr.html#blockdevice): an [LVMVolumeGroup](./cr.html#lvmvolumegroup) whose `blockDeviceSelector` matches it would run `pvcreate` or `vgextend` on it and overwrite the data of the volume.

A driver hides its LUNs with a [BlockDeviceFilter](./cr.html#blockdevicefilter) resource. Its `blockDeviceSelector` selects the devices to keep, by the [labels of BlockDevice resources](#what-labels-are-added-by-the-controller-to-blockdevice-resources). The agent combines the selectors of all BlockDeviceFilter resources with a logical AND: a device becomes a BlockDevice only when every filter keeps it, and an existing BlockDevice that a filter no longer keeps is deleted. One driver's filter therefore cannot reveal a device another driver hides. A `matchExpressions` entry with `In` or `NotIn` and an empty `values` list is ignored.

The drivers of the Deckhouse storage modules use one of the following rules, or several of them at once:

- By WWN: `status.blockdevice.storage.deckhouse.io/wwn` `NotIn` the WWNs of the LUNs the driver has discovered. Partitions and multipath maps carry the WWN of the disk beneath them, so they are hidden too. A LUN that appeared on the node before the driver discovered it is not covered by this rule.
- By model: `status.blockdevice.storage.deckhouse.io/model` `NotIn` the model the storage system reports, for drivers that own every LUN of their storage system. The value keeps the letter case and has dots replaced with dashes (`TATLIN.UNIFIED` becomes `TATLIN-UNIFIED`); take it from an existing BlockDevice rather than writing it by hand.
- By target: `status.blockdevice.storage.deckhouse.io/scsitarget` `NotIn` the targets the driver owns (for example, every target declared in a `SCSITarget` resource of the `csi-scsi-generic` module). Every LUN of such a target is hidden from the moment it appears on the node, before the driver knows its WWN.

The agent reads the target from sysfs: the `targetname` of the iSCSI session (`/sys/class/iscsi_session/session*/targetname`) or the `port_name` of the Fibre Channel remote port (`/sys/class/fc_remote_ports/rport-*/port_name`) the disk came in by. Partitions, multipath maps and other mappings take it from the disks beneath them; when the paths of a map lead to more than one target, the label names the smallest of them. The value is built as follows:

- a WWPN becomes its 16 lower-case hex digits: `0x500A0981891B8DC5` and `50:0a:09:81:89:1b:8d:c5` both become `500a0981891b8dc5`;
- an iSCSI name is lower-cased and its colons become underscores: `iqn.2000-01.com.example:target` becomes `iqn.2000-01.com.example_target`;
- an iSCSI name that is still not a valid label value after that (longer than 63 characters, for example) keeps its first 46 characters, followed by a dash and the first 16 hex digits of the SHA-256 of the whole lower-cased name.

A driver written in Go computes the value with `SCSITargetLabelValue` from the module's API package, `github.com/deckhouse/sds-node-configurator/api/v1alpha1`. When the agent cannot read the target of a LUN, it does not create a BlockDevice for it until it can.

A `NotIn` requirement also keeps a device that has no label with that key at all, so a filter with the target rule changes nothing on nodes where an older agent does not set the label yet.

Limitations:

- The rule by target covers LUNs of iSCSI and Fibre Channel targets only; NVMe-oF namespaces carry no such label.
- When a LUN is reached through several targets, the label names the smallest of them, so a driver has to list every target port (for Fibre Channel, every WWPN of the storage system zoned to the node) to hide such LUNs reliably.
- The filters decide which BlockDevice resources exist, not what the node itself does: an LVM command run on the node by hand still sees every device.

## How do I disable the module?

Disabling the module removes its controller and agent, and nothing else. Volume Groups and Logical Volumes stay on the nodes with the data on them, and the module's resources ([BlockDevice](./cr.html#blockdevice), [LVMVolumeGroup](./cr.html#lvmvolumegroup), LVMVolumeGroupSet, LVMLogicalVolume, LVMLogicalVolumeSnapshot, BlockDeviceFilter) stay in the cluster; the module only removes their finalizers so that they can be deleted later.

The `sds-local-volume`, `sds-replicated-volume` and `sds-elastic` modules work on top of `sds-node-configurator`. Disable them first, following their documentation, and only then disable this module.

1. If the disks have to be freed, delete the module's LVM objects while the module is still running — after it is disabled, nobody removes them from the nodes:

   1. Delete the LVMVolumeGroupSet resources. While a set exists, the controller recreates every LVMVolumeGroup of it that is deleted:

      ```shell
      d8 k get lvmvolumegroupsets.storage.deckhouse.io
      d8 k delete lvmvolumegroupsets.storage.deckhouse.io <name>
      ```

   1. Delete the LVMVolumeGroup resources:

      ```shell
      d8 k get lvmvolumegroups.storage.deckhouse.io
      d8 k delete lvmvolumegroups.storage.deckhouse.io <name>
      ```

      The agent removes the Volume Group and its Physical Volumes from the node; for file-backed devices it also detaches the loop devices and removes the backing files. The deletion waits while the Volume Group has Logical Volumes or the resource has the `storage.deckhouse.io/deletion-protection` annotation, see [Why did the LVMVolumeGroup resource and Volume Group remain after deletion attempt?](#why-did-the-lvmvolumegroup-resource-and-volume-group-remain-after-deletion-attempt).

   1. Wait until the command returns `No resources found`:

      ```shell
      d8 k get lvmvolumegroups.storage.deckhouse.io
      ```

1. Disable the module:

   ```shell
   d8 k patch moduleconfig sds-node-configurator --type=merge -p '{"spec":{"enabled":false}}'
   ```

What remains after the module is disabled:

- BlockDevice resources. They are not deleted on disabling. While the module runs, a BlockDevice is deleted only after its device disappears from the node, or after its node is deleted or stops running the agent (see `missingBlockDeviceRetentionHours`); one deleted by hand for a device that is still there is created again.
- Volume Groups whose LVMVolumeGroup was not deleted before disabling. They keep working as plain LVM Volume Groups. If their LVMVolumeGroup resources are deleted while the module is disabled, the Volume Groups the module created are not imported again after the module is re-enabled, see [Why is a Volume Group with the module's tags not imported, and how do I adopt it?](#why-is-a-volume-group-with-the-modules-tags-not-imported-and-how-do-i-adopt-it).
- Volume Groups on file-backed devices are not activated after a node reboot: the agent that attached the loop devices is gone.
- The module's files under `/var/lib/deckhouse/sds` and the loop device filter in `/etc/lvm/lvm.conf` on the nodes.
