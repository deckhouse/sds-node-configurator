---
title: "The sds-node-configurator module: FAQ"
description: "Deckhouse Kubernetes Platform. The sds-node-configurator module. Common questions and answers."
---

{{< alert level="warning" >}}
The module is guaranteed to work only with stock kernels that are shipped with the [supported distributions](https://deckhouse.io/documentation/v1/supported_versions.html#linux).

The functionality of the module with other kernels or distributions is possible but not guaranteed.
{{< /alert >}}

## Why does creating `BlockDevice` and `LVMVolumeGroup` resources in a cluster fail?

* In most cases, the creation of `BlockDevice` resources fails because the existing devices fail filtering by the controller. Please make sure that your devices meet the [requirements](./usage.html#the-conditions-the-controller-imposes-on-the-device).

* Creating LVMVolumeGroup resources may fail due to the absence of BlockDevice resources in the cluster, as their names are used in the LVMVolumeGroup specification.

* If the `BlockDevice` resources are present and the `LVMVolumeGroup` resources are not present, please make sure the existing `LVM Volume Group` on the node has a special tag `storage.deckhouse.io/enabled=true` attached.

## I have deleted the `LVMVolumeGroup` resource, but the `Volume Group` is still there. What do I do?

Deleting a `LVMVolumeGroup` resource does not delete the `Volume Group` it references. To delete it, add a special `storage.deckhouse.io/sds-delete-vg: ""` annotation to trigger the deletion process. The controller will then automatically delete the `LVM Volume Group` from the node and its associated resource.

> Note that simply deleting the `LVMVolumeGroup` resource will result in the creation of a new resource with a generated name based on the existing `LVM Volume Group` on the node.

## I have attached the delete annotation, but the `LVMVolumeGroup` resource is still there as well as the `Volume Group` on the node. Why?

The usual case is that the corresponding `LVM Volume Group` on the node has `Logical Volumes`. The controller does not delete `Logical Volumes` because these `volumes` may contain important data and the user must purge them manually.

Once the `Logical Volume` has been deleted, the controller will proceed to delete the `Volume Group` and its corresponding resource.

## I'm trying to create a `Volume Group` using the `LVMVolumeGroup` resource, but I'm not getting anywhere. Why?

Most likely, your resource fails controller validation, although it has passed the Kubernetes validation successfully.
The exact cause of the failure can be found in the `status.message` field of the resource itself, 
or you can refer to the controller's logs.

The problem usually stems from incorrectly defined `BlockDevice` resources. Please make sure that these resources meet the following requirements:
- The `Consumable` field is set to `true`.
- For a `Volume Group` of type `Local`, the specified `BlockDevice` belong to the same node.<!-- > - For a `Volume Group` of type `Shared`, the specified `BlockDevice` is the only resource. -->
- The current names of the `BlockDevice` resources are specified.

The full list of expected values can be found in the [CR reference](./cr.html) of the `LVMVolumeGroup` resource.

## What happens if I unplug one of the devices in a `Volume Group`? Will the linked `LVMVolumeGroup` resource be deleted?

The `LVMVolumeGroup` resource will persist as long as the corresponding `Volume Group` exists. As long as at least one device exists, the `Volume Group` will be there, albeit in an unhealthy state.
Note that these issues will be reflected in the resource's `status`.

When the unplugged device is reactivated, the `LVM Volume Group` will recover while the linked `LVMVolumeGroup` resource be brought to its current state as well.

## How to transfer control of an existing `LVM Volume Group` on the node to the controller?

Simply add the LVM tag `storage.deckhouse.io/enabled=true` to the LVM Volume Group on the node:

```shell
vgchange myvg-0 --addtag storage.deckhouse.io/enabled=true
```

## How do I get the controller to stop monitoring the `LVM Volume Group` on the node?

Delete the `storage.deckhouse.io/enabled=true` LVM tag for the target `Volume Group` on the node:

```shell
vgchange myvg-0 --addtag storage.deckhouse.io/enabled=true
```

The controller will then stop tracking the selected `Volume Group` and delete the associated `LVMVolumeGroup` resource automatically.

## I haven't added the `storage.deckhouse.io/enabled=true` LVM tag to the `Volume Group`, but it is there. How is this possible?

This is possible if you have created the `LVM Volume Group` using the `LVMVolumeGroup` resource, in which case the controller will automatically add this LVM tag to the created `LVM Volume Group`. Alternatively, this applies if the `Volume Group` or its `Thin-pool` already had the `linstor` module LVM tag `linstor-*`.

When you switch from the `linstor` module to the `sds-node-configurator` and `sds-drbd` modules, the `linstor-*` LVM tags are automatically replaced with the `storage.deckhouse.io/enabled=true` LVM tag in the `Volume Group`. This way, the `sds-node-configurator` gets control of these `Volume Groups`.
