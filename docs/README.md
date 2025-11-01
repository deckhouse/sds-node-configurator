---
title: "sds-node-configurator module"
description: "sds-node-configurator module: block device and LVM management"
---

{{< alert level="warning" >}}
Module functionality is guaranteed only when using stock kernels provided with [supported distributions](/products/kubernetes-platform/documentation/v1/reference/supported_versions.html#linux).

Module functionality when using other kernels or distributions is possible but not guaranteed.
{{< /alert >}}

The `sds-node-configurator` module manages block devices and LVM on Kubernetes cluster nodes through [Kubernetes custom resources](./cr.html). Main module capabilities:

- Automatic discovery of block devices and creation/update/deletion of corresponding [BlockDevice resources](./cr.html#blockdevice).

  > **Warning**. Manual creation and modification of BlockDevice resource is prohibited.

- Automatic discovery of LVM Volume Groups with LVM tag `storage.deckhouse.io/enabled=true` and thin pools on them on nodes, as well as management of corresponding [LVMVolumeGroup](./cr.html#lvmvolumegroup) resources. The module automatically creates an [LVMVolumeGroup](./cr.html#lvmvolumegroup) resource if it doesn't exist yet for the discovered LVM Volume Group.

- Scanning LVM Physical Volumes on nodes that are part of managed LVM Volume Groups. When underlying block devices are expanded, corresponding LVM Physical Volumes are automatically increased (performs `pvresize`).

  > **Warning**. Reducing block device sizes is not supported.

- Creation/expansion/deletion of LVM Volume Groups on the node according to [LVMVolumeGroup](./cr.html#lvmvolumegroup) resource settings. To see examples, refer to [Working with LVMVolumeGroup resources](./resources.html#working-with-lvmvolumegroup-resources).

## Documentation

- [Working with resources](./resources.html): Practical examples of creating, modifying and deleting [BlockDevice](./cr.html#blockdevice) and [LVMVolumeGroup](./cr.html#lvmvolumegroup) resources.
- [Custom Resources](./cr.html): Module CRD reference.
- [Configuration](./configuration.html): Module parameter configuration.
- [Configuration scenarios](./layouts.html): Typical disk subsystem configuration scenarios for various storage configurations.
- [FAQ](./faq.html): Frequently asked questions and answers.