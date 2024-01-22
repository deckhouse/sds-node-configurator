---
title: "The sds-node-configurator module"
description: "General Concepts and Principles of  the sds-node-configurator module. Deckhouse Kubernetes Platform."
moduleStatus: experimental
---

{{< alert level="warning" >}}
The module is guaranteed to work only with stock kernels that are shipped with the [supported distributions](https://deckhouse.io/documentation/v1/supported_versions.html#linux).

The functionality of the module with other kernels or distributions is possible but not guaranteed.
{{< /alert >}}

The module manages `LVM` on cluster nodes through [Kubernetes custom resources](./cr.html), performing the following operations:

  - Detecting block devices and creating/updating/deleting the corresponding [BlockDevice resources](./cr.html#blockdevice).

   > **Caution!** Manual creation and modification of the `BlockDevice` resource is prohibited.

  - Detecting `LVM Volume Groups` with the `storage.deckhouse.io/enabled=true` LVM tag and `Thin-pools` on them on the nodes, as well as managing the corresponding [LVMVolumeGroup resources](./cr.html#lvmvolumegroup). The module automatically creates an `LVMVolumeGroup` resource if it does not yet exist for the discovered `LVM Volume Group`.

  - Scanning `LVM Physical Volumes` on the nodes that are part of managed `LVM Volume Groups`. In case of expansion of underlying block device sizes, the corresponding `LVM Physical Volumes` will be automatically expanded (`pvresize` will occur).

  > **Caution!** Reduction in the size of block devices is not supported.

  - Creating/expanding/deleting `LVM Volume Groups` on the node in accordance with user changes in `LVMVolumeGroup` resources. [Usage examples](./usage.html#lvmvolumegroup-resources)
