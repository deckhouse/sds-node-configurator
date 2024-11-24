---
title: "Module sds-node-configurator: Usage Scenarios"
linkTitle: "Usage Scenarios"
---

{{< alert level="warning" >}}
The module's functionality is guaranteed only when using stock kernels provided with [supported distributions](https://deckhouse.io/documentation/v1/supported_versions.html#linux).

The module may work with other kernels or distributions, but this is not guaranteed.
{{< /alert >}}

Examples of configuring the disk subsystem on Kubernetes cluster nodes.

{{< alert level="warning" >}}
IMPORTANT! If you create virtual machines by cloning, you must change the UUID of the volume groups (VGs) on the cloned VMs. To do this, run the command `vgchange -u` — this will generate new UUIDs for all VGs on the virtual machine (you can add this to the cloud init script).

Note: You can only change the UUID of a VG if it has no active logical volumes (LVs). To deactivate logical volumes, unmount them first, then run the command `lvchange -an <vg name (to deactivate all LVs in the VG) or LV name (to deactivate a specific LV)>`.
{{< /alert >}}

## Multiple Identical Disks

### Recommended Scenario

* Assemble a mirror of the entire disks (hardware or software) to be used for both the root system and data.
* During OS installation:
  * Create a VG named `main` on the mirror.
  * Create an LV named `root` in the `main` VG.
  * Install the OS on the `root` LV.
* Add the tag `storage.deckhouse.io/enabled=true` to the `main` VG using the following command:

```shell
vgchange main --addtag storage.deckhouse.io/enabled=true
```

* Add the prepared node to the Deckhouse cluster.

If the node matches the `nodeSelector` specified in the `spec.nodeSelector` of the `sds-replicated-volume` and/or `sds-local-volume` modules, the `sds-node-configurator` module agent will start on that node. It will detect the `main` VG and add a corresponding `LVMVolumeGroup` resource to the Deckhouse cluster. The `LVMVolumeGroup` resource can then be used to create volumes in the `sds-replicated-volume` and/or `sds-local-volume` modules.

#### Pros and Cons of the Scenario

| Pros                       | Cons                                           |
|----------------------------|------------------------------------------------|
| Reliable                   | Overhead in disk space for SDS, which replicates data itself |
| Easy to configure and use  |                                                |
| Convenient for allocating (and reallocating) space between different SDSes | |

#### Example Configuration of SDS Modules Using the Recommended Scenario

Suppose you have three nodes configured according to the recommended scenario. In this case, the Deckhouse cluster will have three `LVMVolumeGroup` resources with randomly generated names (in the future, it will be possible to specify a name for `LVMVolumeGroup` resources created during automatic VG discovery by adding an LVM tag with the desired resource name).

```shell
kubectl get lvmvolumegroups.storage.deckhouse.io

NAME                                      THINPOOLS   CONFIGURATION APPLIED   PHASE   NODE       SIZE      ALLOCATED SIZE   VG     AGE
vg-08d3730c-9201-428d-966c-45795cba55a6   0/0         True                    Ready   worker-2   25596Mi   0                main   61s
vg-b59ff9e1-6ef2-4761-b5d2-6172926d4f4d   0/0         True                    Ready   worker-0   25596Mi   0                main   4m17s
vg-c7863e12-c143-42bb-8e33-d578ce50d6c7   0/0         True                    Ready   worker-1   25596Mi   0                main   108s
```

##### Configuring the `sds-local-volume` Module

* Create a `LocalStorageClass` resource and include all your `LVMVolumeGroup` resources to use the `main` VG on all your nodes in the `sds-local-volume` module:

```yaml
kubectl apply -f -<<EOF
apiVersion: storage.deckhouse.io/v1alpha1
kind: LocalStorageClass
metadata:
  name: local-sc
spec:
  lvm:
    lvmVolumeGroups:
      - name: vg-08d3730c-9201-428d-966c-45795cba55a6
      - name: vg-b59ff9e1-6ef2-4761-b5d2-6172926d4f4d
      - name: vg-c7863e12-c143-42bb-8e33-d578ce50d6c7
    type: Thick
  reclaimPolicy: Delete
  volumeBindingMode: WaitForFirstConsumer
EOF
```

##### Configuring the `sds-replicated-volume` Module

* Create a `ReplicatedStoragePool` resource and add all your `LVMVolumeGroup` resources to use the `main` VG on all your nodes in the `sds-replicated-volume` module:

```yaml
kubectl apply -f -<<EOF
apiVersion: storage.deckhouse.io/v1alpha1
kind: ReplicatedStoragePool
metadata:
  name: data
spec:
  type: LVM
  lvmVolumeGroups:
    - name: vg-08d3730c-9201-428d-966c-45795cba55a6
    - name: vg-b59ff9e1-6ef2-4761-b5d2-6172926d4f4d
    - name: vg-c7863e12-c143-42bb-8e33-d578ce50d6c7
EOF
```

* Create `ReplicatedStorageClass` resources and specify the name of the previously created `ReplicatedStoragePool` resource in the `storagePool` field:

```yaml
kubectl apply -f -<<EOF
---
apiVersion: storage.deckhouse.io/v1alpha1
kind: ReplicatedStorageClass
metadata:
  name: replicated-sc-r1
spec:
  storagePool: data
  replication: None
  reclaimPolicy: Delete
  topology: Ignored # If this topology is specified, the cluster should not have zones (nodes labeled with topology.kubernetes.io/zone).

---
apiVersion: storage.deckhouse.io/v1alpha1
kind: ReplicatedStorageClass
metadata:
  name: replicated-sc-r2
spec:
  storagePool: data
  replication: Availability
  reclaimPolicy: Delete
  topology: Ignored # If this topology is specified, the cluster should not have zones (nodes labeled with topology.kubernetes.io/zone).

---
apiVersion: storage.deckhouse.io/v1alpha1
kind: ReplicatedStorageClass
metadata:
  name: replicated-sc-r3
spec:
  storagePool: data
  replication: ConsistencyAndAvailability
  reclaimPolicy: Delete
  topology: Ignored # If this topology is specified, the cluster should not have zones (nodes labeled with topology.kubernetes.io/zone).
EOF
```

### Hybrid Scenario with Multiple Partitions for Space Efficiency

{{< alert level="warning" >}}
IMPORTANT! Using partitions with the same part UUID is not supported, nor is changing the part UUID of a partition used for creating a VG. When creating partition tables, it is recommended to choose GPT, as part UUIDs in MBR are pseudo-random and contain the partition number. Additionally, MBR does not support partlabel, which can be convenient for subsequent partition identification in Deckhouse.
{{< /alert >}}

In this scenario, two partitions are used on each disk: one for the root system and SDS data that is not replicated, and another for SDS data that is replicated. The first partition of each disk is used to create a mirror, while the second partition is used to create a separate VG without mirroring. This maximizes disk space efficiency.

* During OS installation:
  * Create two partitions on each disk.
  * Create a mirror from the first partitions on each disk.
  * Create a VG named `main-safe` on the mirror.
  * Create an LV named `root` in the `main-safe` VG.
  * Install the OS on the `root` LV.
* Add the tag `storage.deckhouse.io/enabled=true` to the `main-safe` VG using the following command:

```shell
vgchange main-safe --addtag storage.deckhouse.io/enabled=true
```

* Create a VG named `main-unsafe` from the second partitions of each disk.
* Add the tag `storage.deckhouse.io/enabled=true` to the `main-unsafe` VG using the following command:

```shell
vgchange main-unsafe --addtag storage.deckhouse.io/enabled=true
```

* Add the prepared node to the Deckhouse cluster.

If the node matches the `nodeSelector` specified in the `spec.nodeSelector` of the `sds-replicated-volume` and/or `sds-local-volume` modules, the `sds-node-configurator` module agent will start on that node. It will detect the `main-safe` and `main-unsafe` VGs and add corresponding `LVMVolumeGroup` resources to the Deckhouse cluster. These `LVMVolumeGroup` resources can then be used to create volumes in the `sds-replicated-volume` and/or `sds-local-volume` modules.

#### Pros and Cons of the Scenario

| Pros                       | Cons                                             |
|----------------------------|--------------------------------------------------|
| Reliable                   | Complex to configure and use                    |
| Maximizes disk space usage | Difficult to reallocate space between safe and unsafe partitions |

#### Example Configuration of SDS Modules Using the Hybrid Scenario

Suppose you have three nodes configured according to the hybrid scenario. In this case, the Deckhouse cluster will have six `LVMVolumeGroup` resources with randomly generated names (in the future, it will be possible to specify a name for `LVMVolumeGroup` resources created during automatic VG discovery by adding an LVM tag with the desired resource name).

```shell
kubectl get lvmvolumegroups.storage.deckhouse.io

NAME                                      THINPOOLS   CONFIGURATION APPLIED   PHASE   NODE       SIZE      ALLOCATED SIZE   VG            AGE
vg-08d3730c-9201-428d-966c-45795cba55a6   0/0         True                    Ready   worker-2   25596Mi   0                main-safe     61s
vg-b59ff9e1-6ef2-4761-b5d2-6172926d4f4d   0/0         True                    Ready   worker-0   25596Mi   0                main-safe     4m17s
vg-c7863e12-c143-42bb-8e33-d578ce50d6c7   0/0         True                    Ready   worker-1   25596Mi   0                main-safe     108s
vg-deccf08a-44d4-45f2-aea9-6232c0eeef91   0/0         True                    Ready   worker-2   25596Mi   0                main-unsafe   61s
vg-e0f00cab-03b3-49cf-a2f6-595628a2593c   0/0         True                    Ready   worker-0   25596Mi   0                main-unsafe   4m17s
vg-fe679d22-2bc7-409c-85a9-9f0ee29a6ca2   0/0         True                    Ready   worker-1   25596Mi   0                main-unsafe   108s
```

##### Configuring the `sds-local-volume` Module

* Create a `LocalStorageClass` resource and include `LVMVolumeGroup` resources to use only the `main-safe` VG on all your nodes in the `sds-local-volume` module:

```yaml
kubectl apply -f -<<EOF
apiVersion: storage.deckhouse.io/v1alpha1
kind: LocalStorageClass
metadata:
  name: local-sc
spec:
  lvm:
    lvmVolumeGroups:
      - name: vg-08d3730c-9201-428d-966c-45795cba55a6
      - name: vg-b59ff9e1-6ef2-4761-b5d2-6172926d4f4d
      - name: vg-c7863e12-c143-42bb-8e33-d578ce50d6c7
    type: Thick
  reclaimPolicy: Delete
  volumeBindingMode: WaitForFirstConsumer
EOF
```

##### Configuring the `sds-replicated-volume` Module

* Create a `ReplicatedStoragePool` resource named `data-safe` and add `LVMVolumeGroup` resources to use only the `main-safe` VG on all your nodes in the `sds-replicated-volume` module with replication set to `None`:

```yaml
kubectl apply -f -<<EOF
apiVersion: storage.deckhouse.io/v1alpha1
kind: ReplicatedStoragePool
metadata:
  name: data-safe
spec:
  type: LVM
  lvmVolumeGroups:
    - name: vg-08d3730c-9201-428d-966c-45795cba55a6
    - name: vg-b59ff9e1-6ef2-4761-b5d2-6172926d4f4d
    - name: vg-c7863e12-c143-42bb-8e33-d578ce50d6c7
EOF
```

* Create a `ReplicatedStoragePool` resource named `data-unsafe` and add `LVMVolumeGroup` resources to use only the `main-unsafe` VG on all your nodes in the `sds-replicated-volume` module with replication set to `Availability` or `ConsistencyAndAvailability`:

```yaml
kubectl apply -f -<<EOF
apiVersion: storage.deckhouse.io/v1alpha1
kind: ReplicatedStoragePool
metadata:
  name: data-unsafe
spec:
  type: LVM
  lvmVolumeGroups:
    - name: vg-deccf08a-44d4-45f2-aea9-6232c0eeef91
    - name: vg-e0f00cab-03b3-49cf-a2f6-595628a2593c
    - name: vg-fe679d22-2bc7-409c-85a9-9f0ee29a6ca2
EOF
```

* Create `ReplicatedStorageClass` resources and specify the name of the previously created `ReplicatedStoragePool` resources in the `storagePool` field to use the `main-safe` and `main-unsafe` VGs on all our nodes:

```yaml
kubectl apply -f -<<EOF
---
apiVersion: storage.deckhouse.io/v1alpha1
kind: ReplicatedStorageClass
metadata:
  name: replicated-sc-r1
spec:
  storagePool: data-safe # Note that we use data-safe for this resource because it has replication: None, and data replication will NOT occur for PVs created with this StorageClass.
  replication: None
  reclaimPolicy: Delete
  topology: Ignored # If this topology is specified, the cluster should not have zones (nodes labeled with topology.kubernetes.io/zone).

---
apiVersion: storage.deckhouse.io/v1alpha1
kind: ReplicatedStorageClass
metadata:
  name: replicated-sc-r2
spec:
  storagePool: data-unsafe # Note that we use data-unsafe for this resource because it has replication: Availability, and data replication will occur for PVs created with this StorageClass.
  replication: Availability
  reclaimPolicy: Delete
  topology: Ignored # If this topology is specified, the cluster should not have zones (nodes labeled with topology.kubernetes.io/zone).

---
apiVersion: storage.deckhouse.io/v1alpha1
kind: ReplicatedStorageClass
metadata:
  name: replicated-sc-r3
spec:
  storagePool: data-unsafe # Note that we use data-unsafe for this resource because it has replication: ConsistencyAndAvailability, and data replication will occur for PVs created with this StorageClass.
  replication: ConsistencyAndAvailability
  reclaimPolicy: Delete
  topology: Ignored # If this topology is specified, the cluster should not have zones (nodes labeled with topology.kubernetes.io/zone).
EOF
```

## Multiple Identical Disks and Additional Disks

In this case, it is recommended to create a mirror from identical disks, install the OS on it, and NOT use it for SDS. Use additional disks for SDS.

It is advisable to categorize additional disks by type and use them for different purposes.

### Additional Disks - NVMe SSD

NVMe SSDs are recommended for creating volumes requiring high performance.

#### Recommended Scenario

* Create a mirror from all NVMe SSD disks (hardware or software).
* Create a VG named `ssd-nvme` on the mirror.
* Tag the VG `ssd-nvme` with `storage.deckhouse.io/enabled=true` using the following command:

```shell
vgchange ssd-nvme --addtag storage.deckhouse.io/enabled=true
```

##### Advantages and Disadvantages of the Scenario

| Advantages                 | Disadvantages                                         |
|----------------------------|------------------------------------------------------|
| Reliable                   | Disk space overhead for SDS that replicate data      |
| Simple to set up and use   |                                                      |
| Convenient for managing and reallocating space between different SDS instances |  |

##### Examples of Configuring SDS Modules Using the Recommended Scenario

Suppose you have three nodes configured according to the recommended scenario. In this case, the Deckhouse cluster will have three `LVMVolumeGroup` resources with randomly generated names (in the future, it will be possible to specify names for `LVMVolumeGroup` resources created during automatic VG discovery by adding an LVM tag with the desired resource name).

```shell
kubectl get lvmvolumegroups.storage.deckhouse.io

NAME                                      THINPOOLS   CONFIGURATION APPLIED   PHASE   NODE       SIZE      ALLOCATED SIZE   VG         AGE
vg-08d3730c-9201-428d-966c-45795cba55a6   0/0         True                    Ready   worker-2   25596Mi   0                ssd-nvme   61s
vg-b59ff9e1-6ef2-4761-b5d2-6172926d4f4d   0/0         True                    Ready   worker-0   25596Mi   0                ssd-nvme   4m17s
vg-c7863e12-c143-42bb-8e33-d578ce50d6c7   0/0         True                    Ready   worker-1   25596Mi   0                ssd-nvme   108s
```

###### Configuring the `sds-local-volume` Module

* Create a `LocalStorageClass` resource and add all our `LVMVolumeGroup` resources to use the `ssd-nvme` VG on all our nodes in the `sds-local-volume` module:

```yaml
kubectl apply -f -<<EOF
apiVersion: storage.deckhouse.io/v1alpha1
kind: LocalStorageClass
metadata:
  name: local-sc-ssd-nvme
spec:
  lvm:
    lvmVolumeGroups:
      - name: vg-08d3730c-9201-428d-966c-45795cba55a6
      - name: vg-b59ff9e1-6ef2-4761-b5d2-6172926d4f4d
      - name: vg-c7863e12-c143-42bb-8e33-d578ce50d6c7
    type: Thick
  reclaimPolicy: Delete
  volumeBindingMode: WaitForFirstConsumer
EOF
```

###### Configuring the `sds-replicated-volume` Module

* Create a `ReplicatedStoragePool` resource and add all our `LVMVolumeGroup` resources to use the `ssd-nvme` VG on all our nodes in the `sds-replicated-volume` module:

```yaml
kubectl apply -f -<<EOF
apiVersion: storage.deckhouse.io/v1alpha1
kind: ReplicatedStoragePool
metadata:
  name: data-ssd-nvme
spec:
  type: LVM
  lvmVolumeGroups:
    - name: vg-08d3730c-9201-428d-966c-45795cba55a6
    - name: vg-b59ff9e1-6ef2-4761-b5d2-6172926d4f4d
    - name: vg-c7863e12-c143-42bb-8e33-d578ce50d6c7
EOF
```

* Create `ReplicatedStorageClass` resources and specify the name of the previously created `ReplicatedStoragePool` resource in the `storagePool` field:

```yaml
kubectl apply -f -<<EOF
---
apiVersion: storage.deckhouse.io/v1alpha1
kind: ReplicatedStorageClass
metadata:
  name: replicated-sc-ssd-nvme-r1
spec:
  storagePool: data-ssd-nvme
  replication: None
  reclaimPolicy: Delete
  topology: Ignored # If this topology is specified, the cluster should not have zones (nodes labeled with topology.kubernetes.io/zone).

---
apiVersion: storage.deckhouse.io/v1alpha1
kind: ReplicatedStorageClass
metadata:
  name: replicated-sc-ssd-nvme-r2
spec:
  storagePool: data-ssd-nvme
  replication: Availability
  reclaimPolicy: Delete
  topology: Ignored # If this topology is specified, the cluster should not have zones (nodes labeled with topology.kubernetes.io/zone).

---
apiVersion: storage.deckhouse.io/v1alpha1
kind: ReplicatedStorageClass
metadata:
  name: replicated-sc-ssd-nvme-r3
spec:
  storagePool: data-ssd-nvme
  replication: ConsistencyAndAvailability
  reclaimPolicy: Delete
  topology: Ignored # If this topology is specified, the cluster should not have zones (nodes labeled with topology.kubernetes.io/zone).
EOF
```

#### Hybrid Scenario with NVMe SSD

{{< alert level="warning" >}}
IMPORTANT! Using partitions with identical part UUIDs or modifying the part UUID of a partition used to create a VG is not supported. When creating a partition table, it is recommended to select GPT, as part UUIDs in MBR are pseudo-random and include the partition number. Additionally, MBR does not support part labels, which can be convenient for identifying partitions in Deckhouse.
{{< /alert >}}

In this scenario, two partitions are used on each disk: one for storing non-replicated SDS data and the other for replicated SDS data. The first partition of each disk is used to create a mirror, and the second partition is used to create a separate VG without mirroring. This approach maximizes disk space utilization.

* Create two partitions on each disk.
* Create a mirror from the first partitions on each disk.
* Create a VG named `ssd-nvme-safe` on the mirror.
* Create a VG named `ssd-nvme-unsafe` from the second partitions on each disk.
* Tag the VGs `ssd-nvme-safe` and `ssd-nvme-unsafe` with `storage.deckhouse.io/enabled=true` using the following commands:

```shell
vgchange ssd-nvme-safe --addtag storage.deckhouse.io/enabled=true
vgchange ssd-nvme-unsafe --addtag storage.deckhouse.io/enabled=true
```

##### Advantages and Disadvantages of the Scenario

| Advantages                 | Disadvantages                                         |
|----------------------------|------------------------------------------------------|
| Reliable                   | Complex to configure and manage                      |
| Maximizes disk space efficiency | Very difficult to redistribute space between safe and unsafe partitions |

##### Examples of Configuring SDS Modules Using the Hybrid Scenario with NVMe SSD

Suppose you have three nodes configured using the hybrid scenario with NVMe SSDs. In this case, the Deckhouse cluster will have six `LVMVolumeGroup` resources with randomly generated names (in the future, it will be possible to specify names for `LVMVolumeGroup` resources created during automatic VG discovery by adding an LVM tag with the desired resource name).

```shell
kubectl get lvmvolumegroups.storage.deckhouse.io

NAME                                      THINPOOLS   CONFIGURATION APPLIED   PHASE   NODE       SIZE      ALLOCATED SIZE   VG                AGE
vg-08d3730c-9201-428d-966c-45795cba55a6   0/0         True                    Ready   worker-2   25596Mi   0                ssd-nvme-safe     61s
vg-b59ff9e1-6ef2-4761-b5d2-6172926d4f4d   0/0         True                    Ready   worker-0   25596Mi   0                ssd-nvme-safe     4m17s
vg-c7863e12-c143-42bb-8e33-d578ce50d6c7   0/0         True                    Ready   worker-1   25596Mi   0                ssd-nvme-safe     108s
vg-deccf08a-44d4-45f2-aea9-6232c0eeef91   0/0         True                    Ready   worker-2   25596Mi   0                ssd-nvme-unsafe   61s
vg-e0f00cab-03b3-49cf-a2f6-595628a2593c   0/0         True                    Ready   worker-0   25596Mi   0                ssd-nvme-unsafe   4m17s
vg-fe679d22-2bc7-409c-85a9-9f0ee29a6ca2   0/0         True                    Ready   worker-1   25596Mi   0                ssd-nvme-unsafe   108s
```

###### Configuring the `sds-local-volume` Module

* Create a `LocalStorageClass` resource and add `LVMVolumeGroup` resources to use only the `ssd-nvme-safe` VG on all nodes in the `sds-local-volume` module:

```yaml
kubectl apply -f -<<EOF
apiVersion: storage.deckhouse.io/v1alpha1
kind: LocalStorageClass
metadata:
  name: local-sc-ssd-nvme
spec:
  lvm:
    lvmVolumeGroups:
      - name: vg-08d3730c-9201-428d-966c-45795cba55a6
      - name: vg-b59ff9e1-6ef2-4761-b5d2-6172926d4f4d
      - name: vg-c7863e12-c143-42bb-8e33-d578ce50d6c7
    type: Thick
  reclaimPolicy: Delete
  volumeBindingMode: WaitForFirstConsumer
EOF
```

###### Configuring the `sds-replicated-volume` Module

* Create a `ReplicatedStoragePool` resource named `data-ssd-nvme-safe` and add `LVMVolumeGroup` resources for using only the `ssd-nvme-safe` VG on all nodes in the `sds-replicated-volume` module with `ReplicatedStorageClass` replication set to `None`:

```yaml
kubectl apply -f -<<EOF
apiVersion: storage.deckhouse.io/v1alpha1
kind: ReplicatedStoragePool
metadata:
  name: data-ssd-nvme-safe
spec:
  type: LVM
  lvmVolumeGroups:
    - name: vg-08d3730c-9201-428d-966c-45795cba55a6
    - name: vg-b59ff9e1-6ef2-4761-b5d2-6172926d4f4d
    - name: vg-c7863e12-c143-42bb-8e33-d578ce50d6c7
EOF
```

* Create a `ReplicatedStoragePool` resource named `data-ssd-nvme-unsafe` and add `LVMVolumeGroup` resources for using only the `ssd-nvme-unsafe` VG on all nodes in the `sds-replicated-volume` module with `ReplicatedStorageClass` replication set to `Availability` or `ConsistencyAndAvailability`:

```yaml
kubectl apply -f -<<EOF
apiVersion: storage.deckhouse.io/v1alpha1
kind: ReplicatedStoragePool
metadata:
  name: data-ssd-nvme-unsafe
spec:
  type: LVM
  lvmVolumeGroups:
    - name: vg-deccf08a-44d4-45f2-aea9-6232c0eeef91
    - name: vg-e0f00cab-03b3-49cf-a2f6-595628a2593c
    - name: vg-fe679d22-2bc7-409c-85a9-9f0ee29a6ca2
EOF
```

* Create `ReplicatedStorageClass` resources and specify the names of the previously created `ReplicatedStoragePool` resources for using `ssd-nvme-safe` and `ssd-nvme-unsafe` VGs on all nodes:

```yaml
kubectl apply -f -<<EOF
---
apiVersion: storage.deckhouse.io/v1alpha1
kind: ReplicatedStorageClass
metadata:
  name: replicated-sc-ssd-nvme-r1
spec:
  storagePool: data-ssd-nvme-safe # Note: `data-ssd-nvme-safe` is used here because replication: None ensures no replication for PVs created with this StorageClass.
  replication: None
  reclaimPolicy: Delete
  topology: Ignored # If this topology is specified, the cluster must not have zones (nodes labeled with topology.kubernetes.io/zone).

---
apiVersion: storage.deckhouse.io/v1alpha1
kind: ReplicatedStorageClass
metadata:
  name: replicated-sc-ssd-nvme-r2
spec:
  storagePool: data-ssd-nvme-unsafe # Note: `data-ssd-nvme-unsafe` is used here because replication: Availability enables data replication for PVs created with this StorageClass.
  replication: Availability
  reclaimPolicy: Delete
  topology: Ignored # If this topology is specified, the cluster must not have zones (nodes labeled with topology.kubernetes.io/zone).

---
apiVersion: storage.deckhouse.io/v1alpha1
kind: ReplicatedStorageClass
metadata:
  name: replicated-sc-ssd-nvme-r3
spec:
  storagePool: data-ssd-nvme-unsafe # Note: `data-ssd-nvme-unsafe` is used here because replication: ConsistencyAndAvailability ensures data replication for PVs created with this StorageClass.
  replication: ConsistencyAndAvailability
  reclaimPolicy: Delete
  topology: Ignored # If this topology is specified, the cluster must not have zones (nodes labeled with topology.kubernetes.io/zone).
EOF
```

### Additional Disks - SATA SSD

It is recommended to use SATA SSDs for creating volumes that do not require high performance.

#### Recommended Scenario

* Create a VG named `ssd-sata` from all SATA SSD disks.
* Tag the VG `ssd-sata` with `storage.deckhouse.io/enabled=true` using the following command:

```shell
vgchange ssd-sata --addtag storage.deckhouse.io/enabled=true
```

##### Advantages and Disadvantages of the Scenario

| Advantages                 | Disadvantages                                         |
|----------------------------|------------------------------------------------------|
| Reliable                   | Overhead on disk space for SDS, which replicate data themselves |
| Simple to configure and use|                                                      |
| Convenient for distributing (and redistributing) space between different SDS |   |

##### Examples of Configuring SDS Modules Using the Recommended Scenario

Suppose you have three nodes configured according to the recommended scenario. In this case, the Deckhouse cluster will have three `LVMVolumeGroup` resources with randomly generated names (in the future, it will be possible to specify names for `LVMVolumeGroup` resources created during automatic VG discovery by adding an LVM tag with the desired resource name).

```shell
kubectl get lvmvolumegroups.storage.deckhouse.io

NAME                                      THINPOOLS   CONFIGURATION APPLIED   PHASE   NODE       SIZE      ALLOCATED SIZE   VG         AGE
vg-08d3730c-9201-428d-966c-45795cba55a6   0/0         True                    Ready   worker-2   25596Mi   0                ssd-sata   61s
vg-b59ff9e1-6ef2-4761-b5d2-6172926d4f4d   0/0         True                    Ready   worker-0   25596Mi   0                ssd-sata   4m17s
vg-c7863e12-c143-42bb-8e33-d578ce50d6c7   0/0         True                    Ready   worker-1   25596Mi   0                ssd-sata   108s
```

###### Configuring the `sds-local-volume` Module

* Create a `LocalStorageClass` resource and add all `LVMVolumeGroup` resources to use the VG `ssd-sata` on all nodes in the `sds-local-volume` module:

```yaml
kubectl apply -f -<<EOF
apiVersion: storage.deckhouse.io/v1alpha1
kind: LocalStorageClass
metadata:
  name: local-sc-ssd-sata
spec:
  lvm:
    lvmVolumeGroups:
      - name: vg-08d3730c-9201-428d-966c-45795cba55a6
      - name: vg-b59ff9e1-6ef2-4761-b5d2-6172926d4f4d
      - name: vg-c7863e12-c143-42bb-8e33-d578ce50d6c7
    type: Thick
  reclaimPolicy: Delete
  volumeBindingMode: WaitForFirstConsumer
EOF
```

###### Configuring the `sds-replicated-volume` Module

* Create a `ReplicatedStoragePool` resource and add all `LVMVolumeGroup` resources to use the VG `ssd-sata` on all nodes in the `sds-replicated-volume` module:

```yaml
kubectl apply -f -<<EOF
apiVersion: storage.deckhouse.io/v1alpha1
kind: ReplicatedStoragePool
metadata:
  name: data-ssd-sata
spec:
  type: LVM
  lvmVolumeGroups:
    - name: vg-08d3730c-9201-428d-966c-45795cba55a6
    - name: vg-b59ff9e1-6ef2-4761-b5d2-6172926d4f4d
    - name: vg-c7863e12-c143-42bb-8e33-d578ce50d6c7
EOF
```

* Create `ReplicatedStorageClass` resources and specify the name of the previously created `ReplicatedStoragePool` resource:

```yaml
kubectl apply -f -<<EOF
---
apiVersion: storage.deckhouse.io/v1alpha1
kind: ReplicatedStorageClass
metadata:
  name: replicated-sc-ssd-sata-r1
spec:
  storagePool: data-ssd-sata
  replication: None
  reclaimPolicy: Delete
  topology: Ignored # If this topology is specified, the cluster must not have zones (nodes labeled with topology.kubernetes.io/zone).

---
apiVersion: storage.deckhouse.io/v1alpha1
kind: ReplicatedStorageClass
metadata:
  name: replicated-sc-ssd-sata-r2
spec:
  storagePool: data-ssd-sata
  replication: Availability
  reclaimPolicy: Delete
  topology: Ignored # If this topology is specified, the cluster must not have zones (nodes labeled with topology.kubernetes.io/zone).

---
apiVersion: storage.deckhouse.io/v1alpha1
kind: ReplicatedStorageClass
metadata:
  name: replicated-sc-ssd-sata-r3
spec:
  storagePool: data-ssd-sata
  replication: ConsistencyAndAvailability
  reclaimPolicy: Delete
  topology: Ignored # If this topology is specified, the cluster must not have zones (nodes labeled with topology.kubernetes.io/zone).
EOF
```

#### Hybrid Scenario with SATA SSD

{{< alert level="warning" >}}
IMPORTANT! Using partitions with identical part UUIDs or modifying the part UUID of a partition used for creating a VG is not supported. When creating a partition table, it is recommended to choose GPT, as the part UUID in MBR is pseudo-random and includes the partition number. Additionally, MBR does not allow setting a partlabel, which can be useful for subsequent partition identification in Deckhouse.
{{< /alert >}}

In this scenario, two partitions are used on each disk: one for storing non-replicated SDS data and the other for replicated SDS data. The first partition on each disk is used to create a mirror, and the second partition is used to create a separate VG without mirroring. This allows disk space to be used as efficiently as possible.

* Create two partitions on each disk.
* Assemble a mirror from the first partitions on each disk.
* Create a VG named `ssd-sata-safe` on the mirror.
* Create a VG named `ssd-sata-unsafe` from the second partitions on each disk.
* Tag the VGs `ssd-sata-safe` and `ssd-sata-unsafe` with `storage.deckhouse.io/enabled=true` using the following commands:

```shell
vgchange ssd-sata-safe --addtag storage.deckhouse.io/enabled=true
vgchange ssd-sata-unsafe --addtag storage.deckhouse.io/enabled=true
```

##### Advantages and Disadvantages of the Scenario

| Advantages                 | Disadvantages                          |
|----------------------------|-----------------------------------------|
| Reliable                   | Complex to configure and manage        |
| Space is used most efficiently | Very difficult to redistribute space between safe and unsafe partitions |

##### Examples of Configuring SDS Modules Using the Hybrid Scenario with SATA SSD

Suppose you have three nodes configured according to the hybrid scenario with SATA SSD. In this case, the Deckhouse cluster will have six `LVMVolumeGroup` resources with randomly generated names (in the future, it will be possible to specify names for `LVMVolumeGroup` resources created during automatic VG discovery by adding an LVM tag with the desired resource name).

```shell
kubectl get lvmvolumegroups.storage.deckhouse.io

NAME                                      THINPOOLS   CONFIGURATION APPLIED   PHASE   NODE       SIZE      ALLOCATED SIZE   VG                AGE
vg-08d3730c-9201-428d-966c-45795cba55a6   0/0         True                    Ready   worker-2   25596Mi   0                ssd-sata-safe     61s
vg-b59ff9e1-6ef2-4761-b5d2-6172926d4f4d   0/0         True                    Ready   worker-0   25596Mi   0                ssd-sata-safe     4m17s
vg-c7863e12-c143-42bb-8e33-d578ce50d6c7   0/0         True                    Ready   worker-1   25596Mi   0                ssd-sata-safe     108s
vg-deccf08a-44d4-45f2-aea9-6232c0eeef91   0/0         True                    Ready   worker-2   25596Mi   0                ssd-sata-unsafe   61s
vg-e0f00cab-03b3-49cf-a2f6-595628a2593c   0/0         True                    Ready   worker-0   25596Mi   0                ssd-sata-unsafe   4m17s
vg-fe679d22-2bc7-409c-85a9-9f0ee29a6ca2   0/0         True                    Ready   worker-1   25596Mi   0                ssd-sata-unsafe   108s
```

###### Configuring the `sds-local-volume` Module

* Create a `LocalStorageClass` resource and add the `LVMVolumeGroup` resources to use only the VG `ssd-sata-safe` on all nodes in the `sds-local-volume` module:

```yaml
kubectl apply -f -<<EOF
apiVersion: storage.deckhouse.io/v1alpha1
kind: LocalStorageClass
metadata:
  name: local-sc-ssd-sata
spec:
  lvm:
    lvmVolumeGroups:
      - name: vg-08d3730c-9201-428d-966c-45795cba55a6
      - name: vg-b59ff9e1-6ef2-4761-b5d2-6172926d4f4d
      - name: vg-c7863e12-c143-42bb-8e33-d578ce50d6c7
    type: Thick
  reclaimPolicy: Delete
  volumeBindingMode: WaitForFirstConsumer
EOF
```

###### Configuring the `sds-replicated-volume` Module

* Create a `ReplicatedStoragePool` resource named `data-ssd-sata-safe` and add the `LVMVolumeGroup` resources to use only the VG `ssd-sata-safe` on all nodes in the `sds-replicated-volume` module in the `ReplicatedStorageClass` with replication: `None`:

```yaml
kubectl apply -f -<<EOF
apiVersion: storage.deckhouse.io/v1alpha1
kind: ReplicatedStoragePool
metadata:
  name: data-ssd-sata-safe
spec:
  type: LVM
  lvmVolumeGroups:
    - name: vg-08d3730c-9201-428d-966c-45795cba55a6
    - name: vg-b59ff9e1-6ef2-4761-b5d2-6172926d4f4d
    - name: vg-c7863e12-c143-42bb-8e33-d578ce50d6c7
EOF
```

* Create a `ReplicatedStoragePool` resource named `data-ssd-sata-unsafe` and add the `LVMVolumeGroup` resources to use only the VG `ssd-sata-unsafe` on all nodes in the `sds-replicated-volume` module in the `ReplicatedStorageClass` with replication: `Availability` or `ConsistencyAndAvailability`:

```yaml
kubectl apply -f -<<EOF
apiVersion: storage.deckhouse.io/v1alpha1
kind: ReplicatedStoragePool
metadata:
  name: data-ssd-sata-unsafe
spec:
  type: LVM
  lvmVolumeGroups:
    - name: vg-deccf08a-44d4-45f2-aea9-6232c0eeef91
    - name: vg-e0f00cab-03b3-49cf-a2f6-595628a2593c
    - name: vg-fe679d22-2bc7-409c-85a9-9f0ee29a6ca2
EOF
```

* Create `ReplicatedStorageClass` resources and specify the names of the previously created `ReplicatedStoragePool` resources for using the VGs `ssd-sata-safe` and `ssd-sata-unsafe` on all nodes:

```yaml
kubectl apply -f -<<EOF
---
apiVersion: storage.deckhouse.io/v1alpha1
kind: ReplicatedStorageClass
metadata:
  name: replicated-sc-ssd-sata-r1
spec:
  storagePool: data-ssd-sata-safe # Note: We use `data-ssd-sata-safe` for this resource because it has replication: None, meaning no data replication for PVs created with this StorageClass.
  replication: None
  reclaimPolicy: Delete
  topology: Ignored # If this topology is specified, the cluster must not have zones (nodes labeled with topology.kubernetes.io/zone).

---
apiVersion: storage.deckhouse.io/v1alpha1
kind: ReplicatedStorageClass
metadata:
  name: replicated-sc-ssd-sata-r2
spec:
  storagePool: data-ssd-sata-unsafe # Note: We use `data-ssd-sata-unsafe` for this resource because it has replication: Availability, meaning data replication will occur for PVs created with this StorageClass.
  replication: Availability
  reclaimPolicy: Delete
  topology: Ignored # If this topology is specified, the cluster must not have zones (nodes labeled with topology.kubernetes.io/zone).

---
apiVersion: storage.deckhouse.io/v1alpha1
kind: ReplicatedStorageClass
metadata:
  name: replicated-sc-ssd-sata-r3
spec:
  storagePool: data-ssd-sata-unsafe # Note: We use `data-ssd-sata-unsafe` for this resource because it has replication: ConsistencyAndAvailability, meaning data replication will occur for PVs created with this StorageClass.
  replication: ConsistencyAndAvailability
  reclaimPolicy: Delete
  topology: Ignored # If this topology is specified, the cluster must not have zones (nodes labeled with topology.kubernetes.io/zone).
EOF
```

### Additional Disks - HDD

HDDs are recommended for creating volumes that do not require high performance.

#### Recommended Scenario

* Create a VG named `hdd` from all HDD disks.
* Add the tag `storage.deckhouse.io/enabled=true` to the VG `hdd` using the following command:

```shell
vgchange hdd --addtag storage.deckhouse.io/enabled=true
```

##### Advantages and Disadvantages of the Scenario

| Advantages                 | Disadvantages                          |
|----------------------------|-----------------------------------------|
| Reliable                   | Overhead on disk space for SDS, which replicates data independently |
| Simple to configure and use|                                         |
| Convenient for allocating and reallocating space between different SDS | |

##### Examples of Configuring SDS Modules Using the Recommended Scenario

Suppose you have three nodes configured according to the recommended scenario. In this case, the Deckhouse cluster will have three `LVMVolumeGroup` resources with randomly generated names (in the future, it will be possible to specify names for `LVMVolumeGroup` resources created during automatic VG discovery by adding an LVM tag with the desired resource name).

```shell
kubectl get lvmvolumegroups.storage.deckhouse.io

NAME                                      THINPOOLS   CONFIGURATION APPLIED   PHASE   NODE       SIZE      ALLOCATED SIZE   VG    AGE
vg-08d3730c-9201-428d-966c-45795cba55a6   0/0         True                    Ready   worker-2   25596Mi   0                hdd   61s
vg-b59ff9e1-6ef2-4761-b5d2-6172926d4f4d   0/0         True                    Ready   worker-0   25596Mi   0                hdd   4m17s
vg-c7863e12-c143-42bb-8e33-d578ce50d6c7   0/0         True                    Ready   worker-1   25596Mi   0                hdd   108s
```

###### Configuring the `sds-local-volume` Module

* Create a `LocalStorageClass` resource and add all `LVMVolumeGroup` resources for using the VG `hdd` on all nodes in the `sds-local-volume` module:

```yaml
kubectl apply -f -<<EOF
apiVersion: storage.deckhouse.io/v1alpha1
kind: LocalStorageClass
metadata:
  name: local-sc-hdd
spec:
  lvm:
    lvmVolumeGroups:
      - name: vg-08d3730c-9201-428d-966c-45795cba55a6
      - name: vg-b59ff9e1-6ef2-4761-b5d2-6172926d4f4d
      - name: vg-c7863e12-c143-42bb-8e33-d578ce50d6c7
    type: Thick
  reclaimPolicy: Delete
  volumeBindingMode: WaitForFirstConsumer
EOF
```

###### Configuring the `sds-replicated-volume` Module

* Create a `ReplicatedStoragePool` resource and add all `LVMVolumeGroup` resources for using the VG `hdd` on all nodes in the `sds-replicated-volume` module:

```yaml
kubectl apply -f -<<EOF
apiVersion: storage.deckhouse.io/v1alpha1
kind: ReplicatedStoragePool
metadata:
  name: data-hdd
spec:
  type: LVM
  lvmVolumeGroups:
    - name: vg-08d3730c-9201-428d-966c-45795cba55a6
    - name: vg-b59ff9e1-6ef2-4761-b5d2-6172926d4f4d
    - name: vg-c7863e12-c143-42bb-8e33-d578ce50d6c7
EOF
```

* Create `ReplicatedStorageClass` resources and specify the previously created `ReplicatedStoragePool` resource in the `storagePool` field:

```yaml
kubectl apply -f -<<EOF
---
apiVersion: storage.deckhouse.io/v1alpha1
kind: ReplicatedStorageClass
metadata:
  name: replicated-sc-hdd-r1
spec:
  storagePool: data-hdd
  replication: None
  reclaimPolicy: Delete
  topology: Ignored # If this topology is specified, the cluster must not have zones (nodes labeled with topology.kubernetes.io/zone).

---
apiVersion: storage.deckhouse.io/v1alpha1
kind: ReplicatedStorageClass
metadata:
  name: replicated-sc-hdd-r2
spec:
  storagePool: data-hdd
  replication: Availability
  reclaimPolicy: Delete
  topology: Ignored # If this topology is specified, the cluster must not have zones (nodes labeled with topology.kubernetes.io/zone).

---
apiVersion: storage.deckhouse.io/v1alpha1
kind: ReplicatedStorageClass
metadata:
  name: replicated-sc-hdd-r3
spec:
  storagePool: data-hdd
  replication: ConsistencyAndAvailability
  reclaimPolicy: Delete
  topology: Ignored # If this topology is specified, the cluster must not have zones (nodes labeled with topology.kubernetes.io/zone).
EOF
```

#### Hybrid Scenario with HDD

{{< alert level="warning" >}}
IMPORTANT! Using partitions with identical part UUIDs or modifying the part UUID of a partition used for creating a VG is not supported. When creating a partition table, it is recommended to choose GPT, as the part UUID in MBR is pseudo-random and includes the partition number. Additionally, MBR does not allow setting a partlabel, which can be useful for subsequent partition identification in Deckhouse.
{{< /alert >}}

In this scenario, two partitions are used on each disk: one for storing non-replicated SDS data and the other for replicated SDS data. The first partition on each disk is used to create a mirror, and the second partition is used to create a separate VG without mirroring. This allows disk space to be used as efficiently as possible.

* Create two partitions on each disk.
* Assemble a mirror from the first partitions on each disk.
* Create a VG named `hdd-safe` on the mirror.
* Create a VG named `hdd-unsafe` from the second partitions on each disk.
* Add the tag `storage.deckhouse.io/enabled=true` to the VGs `hdd-safe` and `hdd-unsafe` using the following commands:

```shell
vgchange hdd-safe --addtag storage.deckhouse.io/enabled=true
vgchange hdd-unsafe --addtag storage.deckhouse.io/enabled=true
```

##### Advantages and Disadvantages of the Scenario

| Advantages                 | Disadvantages                          |
|----------------------------|-----------------------------------------|
| Reliable                   | Complex to configure and manage        |
| Space is used most efficiently | Very difficult to redistribute space between safe and unsafe partitions |

##### Examples of Configuring SDS Modules Using the Hybrid Scenario with HDD

Suppose you have three nodes configured according to the hybrid scenario with HDD. In this case, the Deckhouse cluster will have six `LVMVolumeGroup` resources with randomly generated names (in the future, it will be possible to specify names for `LVMVolumeGroup` resources created during automatic VG discovery by adding an LVM tag with the desired resource name).

```shell
kubectl get lvmvolumegroups.storage.deckhouse.io

NAME                                      THINPOOLS   CONFIGURATION APPLIED   PHASE   NODE       SIZE      ALLOCATED SIZE   VG           AGE
vg-08d3730c-9201-428d-966c-45795cba55a6   0/0         True                    Ready   worker-2   25596Mi   0                hdd-safe     61s
vg-b59ff9e1-6ef2-4761-b5d2-6172926d4f4d   0/0         True                    Ready   worker-0   25596Mi   0                hdd-safe     4m17s
vg-c7863e12-c143-42bb-8e33-d578ce50d6c7   0/0         True                    Ready   worker-1   25596Mi   0                hdd-safe     108s
vg-deccf08a-44d4-45f2-aea9-6232c0eeef91   0/0         True                    Ready   worker-2   25596Mi   0                hdd-unsafe   61s
vg-e0f00cab-03b3-49cf-a2f6-595628a2593c   0/0         True                    Ready   worker-0   25596Mi   0                hdd-unsafe   4m17s
vg-fe679d22-2bc7-409c-85a9-9f0ee29a6ca2   0/0         True                    Ready   worker-1   25596Mi   0                hdd-unsafe   108s
```

###### Configuring the `sds-local-volume` Module

* Create a `LocalStorageClass` resource and add `LVMVolumeGroup` resources for using only the VG `hdd-safe` on all nodes in the `sds-local-volume` module:

```yaml
kubectl apply -f -<<EOF
apiVersion: storage.deckhouse.io/v1alpha1
kind: LocalStorageClass
metadata:
  name: local-sc-hdd
spec:
  lvm:
    lvmVolumeGroups:
      - name: vg-08d3730c-9201-428d-966c-45795cba55a6
      - name: vg-b59ff9e1-6ef2-4761-b5d2-6172926d4f4d
      - name: vg-c7863e12-c143-42bb-8e33-d578ce50d6c7
    type: Thick
  reclaimPolicy: Delete
  volumeBindingMode: WaitForFirstConsumer
EOF
```

###### Configuring the `sds-replicated-volume` Module

* Create a `ReplicatedStoragePool` named `data-hdd-safe` and add `LVMVolumeGroup` resources for using only the VG `hdd-safe` on all nodes in the `sds-replicated-volume` module in the `ReplicatedStorageClass` with replication: `None`:

```yaml
kubectl apply -f -<<EOF
apiVersion: storage.deckhouse.io/v1alpha1
kind: ReplicatedStoragePool
metadata:
  name: data-hdd-safe
spec:
  type: LVM
  lvmVolumeGroups:
    - name: vg-08d3730c-9201-428d-966c-45795cba55a6
    - name: vg-b59ff9e1-6ef2-4761-b5d2-6172926d4f4d
    - name: vg-c7863e12-c143-42bb-8e33-d578ce50d6c7
EOF
```

* Create a `ReplicatedStoragePool` named `data-hdd-unsafe` and add `LVMVolumeGroup` resources for using only the VG `hdd-unsafe` on all nodes in the `sds-replicated-volume` module in the `ReplicatedStorageClass` with replication `Availability` or `ConsistencyAndAvailability`:

```yaml
kubectl apply -f -<<EOF
apiVersion: storage.deckhouse.io/v1alpha1
kind: ReplicatedStoragePool
metadata:
  name: data-hdd-unsafe
spec:
  type: LVM
  lvmVolumeGroups:
    - name: vg-deccf08a-44d4-45f2-aea9-6232c0eeef91
    - name: vg-e0f00cab-03b3-49cf-a2f6-595628a2593c
    - name: vg-fe679d22-2bc7-409c-85a9-9f0ee29a6ca2
EOF
```

* Create `ReplicatedStorageClass` resources and specify the names of the previously created `ReplicatedStoragePool` resources for using the VGs `hdd-safe` and `hdd-unsafe` on all nodes:

```yaml
kubectl apply -f -<<EOF
---
apiVersion: storage.deckhouse.io/v1alpha1
kind: ReplicatedStorageClass
metadata:
  name: replicated-sc-hdd-r1
spec:
  storagePool: data-hdd-safe # Note: We use `data-hdd-safe` for this resource because it has replication: None, meaning no data replication for PVs created with this StorageClass.
  replication: None
  reclaimPolicy: Delete
  topology: Ignored # If this topology is specified, the cluster must not have zones (nodes labeled with topology.kubernetes.io/zone).

---
apiVersion: storage.deckhouse.io/v1alpha1
kind: ReplicatedStorageClass
metadata:
  name: replicated-sc-hdd-r2
spec:
  storagePool: data-hdd-unsafe # Note: We use `data-hdd-unsafe` for this resource because it has replication: Availability, meaning data replication will occur for PVs created with this StorageClass.
  replication: Availability
  reclaimPolicy: Delete
  topology: Ignored # If this topology is specified, the cluster must not have zones (nodes labeled with topology.kubernetes.io/zone).

---
apiVersion: storage.deckhouse.io/v1alpha1
kind: ReplicatedStorageClass
metadata:
  name: replicated-sc-hdd-r3
spec:
  storagePool: data-hdd-unsafe # Note: We use `data-hdd-unsafe` for this resource because it has replication: ConsistencyAndAvailability, meaning data replication will occur for PVs created with this StorageClass.
  replication: ConsistencyAndAvailability
  reclaimPolicy: Delete
  topology: Ignored # If this topology is specified, the cluster must not have zones (nodes labeled with topology.kubernetes.io/zone).
EOF
```
