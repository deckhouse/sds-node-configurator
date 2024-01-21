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

## `BlockDevice` resources

### Creating a `BlockDevice` resource

The controller regularly scans the existing devices on the node. If a device meets all the conditions 
imposed by the controller, a `BlockDevice` `custom resource` (CR) with a unique name is created. 
It contains all the information about the device in question.

#### The conditions the controller imposes on the device

* The device is not a drbd device.
* The device is not a pseudo-device (i.e. not a loop device).
* The device is not a `Logical Volume`.
* File system is missing or matches `LVM2_MEMBER`.
* The block device has no partitions.
* The size of the block device is greater than 1 Gi.
* If the device is a virtual disk, it must have a serial number.

The controller will use the information from the custom resource to handle `LVMVolumeGroup` resources going forward.

### Updating a `BlockDevice` resource

The controller independently updates the information in the custom resource if the state of the block device, to which it refers, has changed on the node.

### Deleting a `BlockDevice` resource

The following are the cases in which the controller will automatically delete a resource if the block device it refers to has become unavailable:
* if the resource had a Consumable status;
* if the block device belongs to a `Volume Group` that does not have the LVM tag `storage.deckhouse.io/enabled=true` attached to it (this `Volume Group` is not managed by our controller).

> The controller performs the above activities automatically and requires no user intervention.

> In case of manual deletion of the resource, it will be recreated by the controller.

## `LVMVolumeGroup` resources

`BlockDevice` resources are required to create and update `LVMVolumeGroup` resources.
Currently, only local `Volume Groups` are supported.
`LVMVolumeGroup` resources are designed to communicate with the `LVM Volume Groups` on nodes and display up-to-date information about their state.

### Creating a `LVMVolumeGroup` resource

There are two ways to create a `LVMVolumeGroup` resource:
* Automatically:
  * The controller automatically scans for information about the existing `LVM Volume Groups` on nodes and creates a resource if a `LVM Volume Group` is tagged with LVM tag `storage.deckhouse.io/enabled=true` and there is no matching Kubernetes resource for it.
  * In this case, the controller populates all fields of the resource on its own.
* By the user:
  * The user manually creates the resource by filling in only the `metadata.name` and `spec` fields. In it, they specify the desired state of the new `Volume Group`.
  * This configuration is then validated to ensure its correctness.
  * After successful validation, the controller uses the provided configuration to create the specified `LVM Volume Group` on the node and update the user resource with the actual information about the state of the created `LVM Volume Group`.
  * An example of a resource for creating a local `LVM Volume Group` from multiple `BlockDevices`:

    ```yaml
    apiVersion: storage.deckhouse.io/v1alpha1
    kind: LvmVolumeGroup
    metadata:
      name: "vg-0-on-node-0"
    spec:
      type: Local
      blockDeviceNames:
        - dev-c1de9f9b534bf5c0b44e8b1cd39da80d5cda7c3f
        - dev-f3269d92a99e1f668255a47d5d3500add1462711
      actualVGNameOnTheNode: "vg-0"
    ```
  
  * An example of a resource for creating a local `LVM Volume Group` and a `Thin-pool` on it from multiple `BlockDevices`:

    ```yaml
    apiVersion: storage.deckhouse.io/v1alpha1
    kind: LvmVolumeGroup
    metadata:
      name: "vg-thin-on-node-0"
    spec:
      type: Local
      blockDeviceNames:
        - dev-0428672e39334e545eb96c85f8760fd59dcf15f1
        - dev-456977ded72ef804dd7cec90eec94b10acdf99b7
      actualVGNameOnTheNode: "vg-thin"
      thinPools:
      - name: thin-1
        size: 250Gi
    ```
  
  > Please note that the resource does not specify the node on which the `Volume Group` will be created. The node is determined from the `BlockDevice` resources whose names are specified in `spec.blockDeviceNames`.

  > **Caution!** All selected block devices must belong to the same node for an 'Local' `LVMVolumeGroup`.

### Updating a `LVMVolumeGroup` resource and a `Volume Group`

The controller automatically updates the `status` field of the `LVMVolumeGroup` resource to display up-to-date data about the corresponding `LVM Volume Group` on the node.
We do **not recommend** making manual changes to the `status` field.

> The controller does not update the `spec` field since it represents the desired state of the `LVM Volume Group`. The user can make changes to the `spec` field to change the state of the `LVM Volume Group` on the node.

### Deleting a `LVMVolumeGroup` resource and a `Volume Group`

The controller will automatically delete a resource if the `Volume Group` it references has become unavailable (e.g. all block devices forming the `Volume Group` have been unplugged).

> The user may delete a resource manually. However, if the corresponding `LVM Volume Group` still exists at the moment the resource is deleted, the controller will create a resource *automatically* based on the existing `Volume Group` and assign it a new generated name.

To delete a `LVM Volume Group` and its associated `LVM Physical Volume`, append the `storage.deckhouse.io/sds-delete-vg: ""` annotation to the corresponding `LVMVolumeGroup` resource. The controller will detect that the annotation has been added and initiate the process of deleting the `Volume Group` and its parts from the node.

> **Caution!** It is forbidden to delete a `LVM Volume Group` using the above method if it contains a `Logical Volume`, even if it is only a `Thin-pool` that is specified in `spec`. The user must delete all `Logical Volumes` that the `Volume Group` to be deleted contains.
