---
title: "The SDS-Node-Configurator module"
description: "The SDS-Node-Configurator module: General Concepts and Principles."
---

{{< alert level="warning" >}}
The module is guaranteed to work only with stock kernels that are shipped with the [supported distributions](https://deckhouse.io/documentation/v1/supported_versions.html#linux).

The functionality of the module with other kernels or distributions is possible but not guaranteed.
{{< /alert >}}

The `SDS-Node-Configurator` module allows you to manage `LVM Volume Group` and `LVM Thin-pool` by applying Kubernetes manifests. This module runs together with the `SDS-DRBD` module: `SDS-Node-Configurator` generates the `LVMVolumeGroup` resources that are then used to create the `DRBDStoragePool`.

The module exctracts information from the [Kubernetes Custom Resources](resource link) and uses it to create, update, and delete `Volume Group` and `Thin-pool` resources based on them.

The user can define the desired state of the `Volume Group` and `Thin-pool` in the `Spec` field of the `LVMVolumeGroup` resource. The `SDS-Node-Configurator` will then automatically perform all necessary steps to bring the resource to the desired state. The module continuously monitors the created resources to ensure that their current state matches the specified parameters.

Note that the module also automatically detects the size of `Physical Volumes`. If block device sizes are expanded, the corresponding `Physical Volumes` and the `Volume Group` (VG) will be automatically expanded as well, providing dynamic support for the changes. However, keep in mind that block device **downsizing is not supported**.

Note that [Kubernetes Custom Resources](resource link) are cluster-scoped.
