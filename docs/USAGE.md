---
title: "The SDS-Node-Configurator module: usage examples"
description: Usage and examples of the SDS-Node-Configurator controller operation.
---

{{< alert level="warning" >}}
The module is guaranteed to work only with stock kernels that are shipped with the [supported distributions](https://deckhouse.io/documentation/v1/supported_versions.html#linux).

The functionality of the module with other kernels or distributions is possible but not guaranteed.
{{< /alert >}}


The controller supports two types of resources:
* `BlockDevice`;
* `LVMVolumeGroup`.

## [BlockDevice](block device) resources

### Creating a `BlockDevice` resource

The controller regularly scans the existing devices on the node. If a device meets all the conditions 
imposed by the controller, a `BlockDevice` `custom resource` (CR) with a unique name is created. 
It contains all the information about the device in question.

#### The conditions the controller imposes on the device

* The device is not a drbd device.
* The device is not a pseudo-device (i.e. not a loop device).
* The device is not a `Logical Volume`.
* File system is missing or matches LVM2_MEMBER.
* The block device has no partitions.
* The size of the block device is greater than 1 Gi.
* If the device is a virtual disk, it must have a serial number.

The controller will use the information from the custom resource to handle `LVMVolumeGroup` resources going forward.

### Updating a `BlockDevice` resource

The controller updates the information in the custom resource independently if the state of the block device it refers to has changed.

### Deleting a `BlockDevice` resource

The following are the cases in which the controller will automatically delete a resource if the block device it refers to has become unavailable:
* if the resource had a Consumable status;
* if the block device belongs to a `Volume Group` that does not have the tag `storage.deckhouse.io/enabled=true` attached to it (this `Volume Group` is not managed by our controller).


> The controller performs the above activities automatically and requires no user intervention.

## [LVMVolumeGroup](lvmVolumeGroup) resources

The `BlockDevice` resources are required to create and update `LVMVolumeGroup` resources.

The `LVMVolumeGroup` resources are designed to communicate with the `Volume Group` and display up-to-date information about their state.

### Creating a `LVMVolumeGroup` resource and a `Volume Group`

There are two ways to create a `LVMVolumeGroup` resource:
* Automatically:
  * The controller automatically scans for information about the existing `Volume Groups` on nodes and creates a resource 
  if a `Volume Group` is tagged with `storage.deckhouse.io/enabled=true` and there is no matching resource for it.
  * In this case, the controller populates all fields of the resource on its own.
* By the user:
  * The user manually creates the resource by filling in only the `Spec` field. In it, they specify the desired state of the new `Volume Group`.
  * This information is then validated to ensure that the configuration provided is correct and can be implemented.
  * After successful validation, the controller uses the provided information to create the specified `Volume Group` and update the user resource with the actual information about the state of the created `Volume Group`.  

### Updating a `LVMVolumeGroup` resource and a `Volume Group`

The controller automatically updates the `Status` field of the `LVMVolumeGroup` with the current data about the `Volume Group` in question.
We do **not recommend** making manual changes to the `Status` field.

> The controller does not update the `Spec` field since it represents the desired state of the `Volume Group`. The user can make changes to the `Spec` field to change the state of the `Volume Group`.

### Deleting a `LVMVolumeGroup` resource and a `Volume Group`

The controller will automatically delete a resource if the `Volume Group` it references has become unavailable.

> The user may delete a resource manually. However, if the corresponding `Volume Group` still exists at the moment the resource is deleted, 
> the controller will create a resource *automatically* based on the existing `Volume Group` 
> and assign it a new generated name.

To delete a `Volume Group` and its associated `Physical Volume`, append the `storage.deckhouse.io/sds-delete-vg: ""` annotation to the corresponding `LVMVolumeGroup` resource.

The controller will detect that the annotation has been added and initiate the process of deleting the `Volume Group` and its parts.

This will result in the `Volume Group` being deleted, as well as its associated `Physical Volume`, and the `LVMVolumeGroup` resource (**if there is no `Logical Volume`** on the `Volume Group`**). If there is a `Logical Volume` on the `Volume Group`, the user must first manually delete the `Logical Volume` on the node.
