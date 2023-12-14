---
title: "The SDS-Node-Configurator module"
description: "The SDS-Node-Configurator module: General Concepts and Principles."
---
The `SDS-Node-Configurator` module enables the management of `LVM Volume Group` and `LVM Thin-pool` through the application of Kubernetes manifests. This module operates closely in conjunction with the `SDS-DRBD` module: `SDS-Node-Configurator` generates `LVMVolumeGroup` resources, which are utilized in the creation of a `DRBDStoragePool`.

The module operates on [Kubernetes custom resources](resource link), the information from which is utilized to perform operations related to the creation, updating, and deletion of `Volume Group` and `Thin-pool` on them.

The user has the ability to describe the desired state of the `Volume Group` and `Thin pool` in the `Spec` field of the `LVMVolumeGroup` resource. Subsequently, the `SDS-Node-Configurator` module automatically performs all necessary actions to achieve the specified state. The module continuously monitors the created resources, ensuring their current state aligns with the defined parameters.

It is important to note that the module also facilitates automatic detection of `Physical Volumes` sizes. In the event of an expansion in the sizes of block devices, the corresponding `Physical Volumes` and `Volume Group` (VG) will be automatically extended, providing dynamic support for changes. However, it is essential to be aware that reducing the size of a block device is not supported.

Note that [Kubernetes custom resources](resource link) have a Cluster scope.
