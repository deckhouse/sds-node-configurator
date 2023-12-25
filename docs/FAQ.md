---
title: "The SDS-Node-Configurator module: FAQ"
description: "Common questions and answers."
---
{% alert level="warning" %}
The module is guaranteed to work in the following cases only:
- if stock kernels shipped with the [supported distributions](../../supported_versions.html#linux) are used;
- if a 10Gbps network is used.

As for any other configurations, the module may work, but its smooth operation is not guaranteed.
{% endalert %}

## Why does creating `BlockDevice` and `LVMVolumeGroup` resources in a cluster fail?

* In most cases, the creation of `BlockDevice` resources fails because the existing devices fail filtering by the controller. Please make sure that your devices meet the [requirements](link to requirements).

* Creating `LVMVolumeGroup` resources may fail due to the missing `BlockDevice` resources, as they use them as the data source.

* If the `BlockDevice` resources are present and the `LVMVolumeGroup` resources are not present, please make sure the existing `Volume Group` has a special tag `storage.deckhouse.io/enabled=true` attached.

## I have deleted the `LVMVolumeGroup` resource, but the `Volume Group` is still there. What do I do?

Deleting a `LVMVolumeGroup` resource does not delete the `Volume Group` it references. To delete it, 
add a special `storage.deckhouse.io/sds-delete-vg: ""` annotation to trigger the deletion process. The controller will then automatically delete the
`Volume Group` and its associated resource.

> Note thatSimply deleting the `LVMVolumeGroup` resource will result in the creation of a new resource with a generated name based on the existing `Volume Group`.

## I have attached the delete annotation, but the `LVMVolumeGroup` resource is still there as well as the `Volume Group` on the node. Why?

The usual case is that there are `Logical Volumes` for the `Volume Group` the resource references. The controller does not delete `Logical Volumes` because these `volumes` may contain data and the user must purge them manually.

Once the `Logical Volume` has been deleted, the controller will proceed to delete the `Volume Group` and its corresponding resource.

> The time it takes to delete may be longer if the controller's reconcile queue is crowded with other events. To delete the `Volume Group` and its linked resource immediately, update the delete annotation, e. g., by adding any number to its value: `storage.deckhouse.io/sds-delete-vg: ""` -> `storage.deckhouse.io/sds-delete-vg: "1"`.
> In this case, it will be deleted immediately.

## I'm trying to create a `Volume Group` using the `LVMVolumeGroup` resource, but I'm not getting anywhere. Why?

Most likely, your resource fails controller validation.
The exact cause of the failure can be found in the `Status.Message` field of the resource itself, 
or you can refer to the controller's logs.

> The problem usually stems from incorrectly defined `BlockDevice` resources. Please make sure that these resources meet the following requirements:
> - The `Consumable` field is set to `true`.
> - For a `Volume Group` of type `Local`, the specified `BlockDevice` belong to the same node.
> - For a `Volume Group` of type `Shared`, the specified `BlockDevice` is the only resource.
> - The selected `BlockDevice` does not share other `LVMVolumeGroup` resources (other `Volume Groups`).
> - The current names of the `BlockDevice` resources are specified.
> The full list of expected values can be found in the [CR reference](link to the reference) of the `LVMVolumeGroup` resource.

## What happens if I unplug one of the devices in a `Volume Group`? Will the linked `LVMVolumeGroup` resource be deleted?

The `LVMVolumeGroup` resource will persist as long as the corresponding `Volume Group` exists. As long as at least one device exists, the `Volume Group` will be there, albeit in an unhealthy state.
Note that these issues will be reflected in the resource's `Status`.

When the unplugged device is reactivated, the `Volume Group` will recover while the linked `LVMVolumeGroup` resource be brought to its current state as well.

## How do I get the controller to stop monitoring the `Volume Group`?

Delete the `storage.deckhouse.io/enabled=true` tag for the target `Volume Group`. The controller will then stop tracking the selected `Volume Group` and delete the associated `LVMVolumeGroup` resource automatically.

## I haven't added the `storage.deckhouse.io/enabled=true` tag to the `Volume Group`, but it is there. How is this possible?

This is possible if you have created the `Volume group` using the `LVMVolumeGroup` resource (in this case, the controller will automatically add this tag to the created `Volume Group`) or if this `Volume Group` had the `Linstor` module tag (`linstor-*`).

The `sds-node-configurator` module replaces some of the functionality of the `linstor-pools-importer` controller of the built-in `Linstor` module.
So when you switch from the `Linstor` module to the `sds-node-configurator` and `sds-drbd` modules, the `linstor-*` tags are automatically replaced with the `storage.deckhouse.io/enabled=true` tag in the `Volume Group`. This way, the `sds-node-configurator` gets control of these `Volume Groups`.

> The controller performs a one-time re-tagging operation on all existing `Volume Groups` when it starts up.
