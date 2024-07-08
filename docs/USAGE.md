---
title: "The sds-node-configurator module: usage examples"
description: Usage and examples of the sds-node-configurator controller operation. Deckhouse Kubernetes Platform.
---

{{< alert level="warning" >}}
The module is guaranteed to work only with stock kernels that are shipped with the [supported distributions](https://deckhouse.io/documentation/v1/supported_versions.html#linux).

The module may work with other kernels or distributions, but its stable operation and availability of all features is not guaranteed.
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

The controller independently updates the information in the custom resource if the state of the block device to which it refers to has changed on the node.

### Deleting a `BlockDevice` resource

The following are the cases in which the controller will automatically delete a resource if the block device it refers to has become unavailable:
* if the resource had a Consumable status;
* if the block device belongs to a `Volume Group` that does not have the LVM tag `storage.deckhouse.io/enabled=true` attached to it (this `Volume Group` is not managed by our controller).

> The controller performs the above activities automatically and requires no user intervention.

> If the resource is manually deleted, it will be recreated by the controller.

## `LVMVolumeGroup` resources

`BlockDevice` resources are required to create and update `LVMVolumeGroup` resources.
Currently, only local `Volume Groups` are supported.
`LVMVolumeGroup` resources are designed to communicate with the `LVM Volume Groups` on nodes and display up-to-date information about their state.

### Creating an `LVMVolumeGroup` resource

There are two ways to create an `LVMVolumeGroup` resource:
* Automatically:
  * The controller automatically scans for information about the existing `LVM Volume Groups` on nodes and creates a resource if an `LVM Volume Group` is tagged with the `storage.deckhouse.io/enabled=true` LVM tag and there is no matching Kubernetes resource for it.
  * In this case, the controller populates all `Spec` fields of the resource but `thinPools` on its own. A user should manually add an information about thin-pools on the node to the `Spec` field, if they want to make the controller manage the thin-pools. 
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
  
  > Please note that the resource does not specify the node on which the `Volume Group` will be created. The node is picked from the `BlockDevice` resources whose names are listed in `spec.blockDeviceNames`.

  > **Caution!** All the selected block devices must belong to the same node for a 'Local' `LVMVolumeGroup`.

### Updating an `LVMVolumeGroup` resource and a `Volume Group`

The controller automatically updates the `status` field of the `LVMVolumeGroup` resource to display up-to-date data about the corresponding `LVM Volume Group` on the node.
We do **not recommend** making manual changes to the `status` field.

> The controller does not update the `spec` field since it represents the desired state of the `LVM Volume Group`. The user can make changes to the `spec` field to change the state of the `LVM Volume Group` on the node.

### Deleting an `LVMVolumeGroup` resource and a `Volume Group`

The controller will automatically delete a resource if the `Volume Group` it references has become unavailable (e.g., all block devices that made up the `Volume Group` have been unplugged).

A user can delete an `LVM Volume Group` and its associated `LVM Physical Volume` using the following command:

```shell
kubectl delete lvg %lvg-name%
```

> **Caution!** If the deleting `LVM Volume Group` resource contains any `Logical Volume` (even if it is only the `Thin-pool` that is specified in `spec`), a user must delete all those `Logical Volumes` manually. Otherwise, the `LVMVolumeGroup` resource and its `Volume Group` will not be deleted. 

> A user can forbid to delete the `LVMVolumeGroup` resource by annotate it with `storage.deckhouse.io/deletion-protection`. If the controller finds the annotation, it will not delete nether the resource or the `Volume Group` till the annotation removal.
