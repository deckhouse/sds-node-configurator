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

- **Host tooling on the node**: unlike `lvm`, `nsenter` and `lsblk`, which the agent ships under `/opt/deckhouse/sds/bin`, the loop and file operations run the node's own `losetup`, `fallocate`, `stat`, `mkdir` and `rm` in PID 1's mount namespace. `losetup` must support `--nooverlap`, i.e. **util-linux 2.29 or newer** (2016; every currently supported distribution qualifies, RHEL/CentOS 7 does not). On an older host every provisioning attempt fails with `unrecognized option '--nooverlap'`, reported on the resource as `FileDeviceNotApplied`. `--direct-io` is not a requirement: when it is missing or the kernel refuses it, the agent logs a warning and continues with buffered I/O.
- **Confined to a base directory**: Each `directory` must be the `fileDevicesDirectory` module setting (default `/opt/deckhouse/sds/file-devices`) or a subdirectory of it. Paths outside this subtree are rejected so an arbitrary host path cannot be filled up. Point `fileDevicesDirectory` at a dedicated data disk to use a different location.
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
ls -l /opt/deckhouse/sds/file-devices
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
