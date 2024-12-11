---
title: "Модуль sds-node-configurator: сценарии конфигурации sds-модулей"
description: "Сценарии конфигурации sds-модулей с помощью sds-node-configurator"
---

{{< alert level="warning" >}}
Работоспособность модуля гарантируется только при использовании стоковых ядер, поставляемых вместе с [поддерживаемыми дистрибутивами](https://deckhouse.ru/documentation/v1/supported_versions.html#linux).

Работоспособность модуля при использовании других ядер или дистрибутивов возможна, но не гарантируется.
{{< /alert >}}

{{< alert level="info" >}}
Если вы создаёте виртуальные машины клонированием,
необходимо изменить UUID у групп томов (VG) на созданных таким образом виртуальных машинах, выполнив команду `vgchange -u`.
Данная команда сгенерирует новые UUID для всех VG на виртуальной машине.
При необходимости команду можно добавить в скрипт `cloud-init`.

Изменить UUID у VG можно, только если в группе томов нет активных логических томов (LV).
Чтобы деактивировать логический том, отмонтируйте его и выполните следующую команду:

```shell
lvchange -an <название VG (для деактивации всех томов в группе) или LV (для деактивации конкретного тома)>
```

{{< /alert >}}

На данной странице приведены сценарии конфигурации дисковой подсистемы на узлах кластера Kubernetes
в зависимости от условий организации хранилища: на одинаковых дисках или с использованием дополнительных дисков.

Для каждого из условий существует два сценария конфигурации: рекомендуемый и гибридный.
Плюсы и минусы каждого из них приведены в таблице:

| Сценарий конфигурации | Плюсы | Минусы |
|-----------------------|-------|--------|
| Рекомендуемый | - Надежно<br>- Просто в настройке и использовании<br>- Удобно распределять место между разными SDS | - Избыточное место на диске для программно-определяемых хранилищ (SDS), которые сами реплицируют данные  |
| Гибридный | - Надежно<br>- Максимально эффективное использование места | - Сложно в настройке и использовании<br>- Очень сложно перераспределять место между safe- и unsafe-разделами |

## Хранилище с одинаковыми дисками

### Рекомендуемый сценарий

Мы рекомендуем использовать данный сценарий конфигурации, поскольку он достаточно надёжен и прост в настройке.

Чтобы настроить узел по рекомендуемому сценарию, выполните следующее:

1. Соберите зеркало из дисков целиком (аппаратно или программно),
   которое будет использоваться как для корневой системы, так и для данных.
1. При установке операционной системы:
   * создайте VG с именем `main` на зеркале;
   * создайте LV с именем `root` в VG `main`;
   * установите операционную систему на LV `root`.
1. Установите тег `storage.deckhouse.io/enabled=true` для VG `main`, используя следующую команду:

   ```shell
   vgchange main --addtag storage.deckhouse.io/enabled=true
   ```

1. Добавьте подготовленный узел в кластер Deckhouse.

   Если узел подходит под `nodeSelector`, который указан в `spec.nodeSelector` модулей `sds-replicated-volume` или `sds-local-volume`,
   то на этом узле запустится агент модуля `sds-node-configurator`,
   который определит VG `main` и добавит соответствующий этой VG ресурс `LVMVolumeGroup` в кластер Deckhouse.
   Дальше ресурс `LVMVolumeGroup` можно использовать для создания томов в модулях `sds-replicated-volume` или `sds-local-volume`.

#### Пример настройки модулей SDS

В данном примере предполагается, что вы настроили три узла по рекомендуемому сценарию.
В кластере Deckhouse при этом появятся три ресурса `LVMVolumeGroup` со случайно сгенерированными именами.
В будущем добавится возможность указывать имя для ресурсов `LVMVolumeGroup`,
которые создаются в процессе автоматического обнаружения VG, с помощью тега `LVM` с желаемым именем ресурса.

Чтобы вывести список ресурсов `LVMVolumeGroup`, выполните следующую команду:

```shell
kubectl get lvmvolumegroups.storage.deckhouse.io
```

В результате будет выведен следующий список:

```console
NAME                                      THINPOOLS   CONFIGURATION APPLIED   PHASE   NODE       SIZE      ALLOCATED SIZE   VG     AGE
vg-08d3730c-9201-428d-966c-45795cba55a6   0/0         True                    Ready   worker-2   25596Mi   0                main   61s
vg-b59ff9e1-6ef2-4761-b5d2-6172926d4f4d   0/0         True                    Ready   worker-0   25596Mi   0                main   4m17s
vg-c7863e12-c143-42bb-8e33-d578ce50d6c7   0/0         True                    Ready   worker-1   25596Mi   0                main   108s
```

##### Настройка модуля `sds-local-volume`

Чтобы настроить модуль `sds-local-volume` по рекомендуемому сценарию, создайте ресурс `LocalStorageClass`
и добавьте в него все ресурсы `LVMVolumeGroup`, чтобы VG `main` использовалась на всех узлах в модуле `sds-local-volume`:

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

##### Настройка модуля `sds-replicated-volume`

Чтобы настроить модуль `sds-replicated-volume` по рекомендуемому сценарию, выполните следующее:

1. Создайте ресурс `ReplicatedStoragePool` и добавьте в него все ресурсы `LVMVolumeGroup`,
   чтобы VG `main` использовалась на всех узлах в модуле `sds-replicated-volume`:

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

1. Создайте ресурс `ReplicatedStorageClass` и в поле `storagePool` укажите имя созданного ранее ресурса `ReplicatedStoragePool`:

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
     topology: Ignored # Если указать данную топологию, в кластере не должно быть зон (узлов с метками topology.kubernetes.io/zone)

   ---
   apiVersion: storage.deckhouse.io/v1alpha1
   kind: ReplicatedStorageClass
   metadata:
     name: replicated-sc-r2
   spec:
     storagePool: data
     replication: Availability
     reclaimPolicy: Delete
     topology: Ignored

   ---
   apiVersion: storage.deckhouse.io/v1alpha1
   kind: ReplicatedStorageClass
   metadata:
     name: replicated-sc-r3
   spec:
     storagePool: data
     replication: ConsistencyAndAvailability
     reclaimPolicy: Delete
     topology: Ignored
   EOF
   ```

### Гибридный сценарий с несколькими разделами

{{< alert level="warning" >}}
Не поддерживается использование разделов с одинаковыми PARTUUID,
а также изменение PARTUUID раздела, который используется для создания VG.
При создании таблицы разделов рекомендуется выбрать формат `GPT`,
так как PARTUUID в `MBR` является псевдослучайным и содержит в себе номер раздела.
Помимо этого, в `MBR` нельзя задать атрибут PARTLABEL,
который может пригодиться для последующей идентификации раздела в Deckhouse.
{{< /alert >}}

В данном сценарии используются два раздела на каждом диске:
один для корневой системы и хранения данных SDS, которые не реплицируются,
и другой для данных SDS, которые реплицируются.
Первый раздел каждого диска используется для создания зеркала, а второй – для создания отдельной VG без зеркалирования.
Это позволяет максимально эффективно использовать место на диске.

Чтобы настроить узел по гибридному сценарию, выполните следующее:

1. При установке операционной системы:
   * создайте по два раздела на каждом диске;
   * соберите зеркало из первых разделов на каждом диске;
   * создайте VG с именем `main-safe` на зеркале;
   * создать LV с именем `root` в VG `main-safe`;
   * установите операционную систему на LV `root`.
1. Установите тег `storage.deckhouse.io/enabled=true` для VG `main-safe`, используя следующую команду:
  
   ```shell
   vgchange main-safe --addtag storage.deckhouse.io/enabled=true
   ```

1. Создайте VG с именем `main-unsafe` из вторых разделов каждого диска.
1. Установите тег `storage.deckhouse.io/enabled=true` для VG `main-unsafe`, используя следующую команду:

   ```shell
   vgchange main-unsafe --addtag storage.deckhouse.io/enabled=true
   ```

1. Добавьте подготовленный узел в кластер Deckhouse.

   Если узел подходит под `nodeSelector`, который указан в `spec.nodeSelector` модулей `sds-replicated-volume` или `sds-local-volume`,
   то на этом узле запустится агент модуля `sds-node-configurator`,
   который определит VG `main-safe` и `main-unsafe` и добавит соответствующие этим VG ресурсы `LVMVolumeGroup` в кластер Deckhouse.
   Дальше ресурсы `LVMVolumeGroup` можно использовать для создания томов в модулях `sds-replicated-volume` или `sds-local-volume`.

#### Пример настройки модулей SDS

В данном примере предполагается, что вы настроили три узла по гибридному сценарию.
В кластере Deckhouse при этом появятся шесть ресурсов `LVMVolumeGroup` со случайно сгенерированными именами.
В будущем добавится возможность указывать имя для ресурсов `LVMVolumeGroup`,
которые создаются в процессе автоматического обнаружения VG, с помощью тега `LVM` с желаемым именем ресурса.

Чтобы вывести список ресурсов `LVMVolumeGroup`, выполните следующую команду:

```shell
kubectl get lvmvolumegroups.storage.deckhouse.io
```

В результате будет выведен следующий список:

```console
NAME                                      THINPOOLS   CONFIGURATION APPLIED   PHASE   NODE       SIZE      ALLOCATED SIZE   VG            AGE
vg-08d3730c-9201-428d-966c-45795cba55a6   0/0         True                    Ready   worker-2   25596Mi   0                main-safe     61s
vg-b59ff9e1-6ef2-4761-b5d2-6172926d4f4d   0/0         True                    Ready   worker-0   25596Mi   0                main-safe     4m17s
vg-c7863e12-c143-42bb-8e33-d578ce50d6c7   0/0         True                    Ready   worker-1   25596Mi   0                main-safe     108s
vg-deccf08a-44d4-45f2-aea9-6232c0eeef91   0/0         True                    Ready   worker-2   25596Mi   0                main-unsafe   61s
vg-e0f00cab-03b3-49cf-a2f6-595628a2593c   0/0         True                    Ready   worker-0   25596Mi   0                main-unsafe   4m17s
vg-fe679d22-2bc7-409c-85a9-9f0ee29a6ca2   0/0         True                    Ready   worker-1   25596Mi   0                main-unsafe   108s
```

##### Настройка модуля `sds-local-volume`

Чтобы настроить модуль `sds-local-volume` по гибридному сценарию, создайте ресурс `LocalStorageClass`
и добавьте в него ресурсы `LVMVolumeGroup`, чтобы на всех узлах в модуле `sds-local-volume` использовалась только VG `main-safe`:

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

##### Настройка модуля `sds-replicated-volume`

Чтобы настроить модуль `sds-replicated-volume` по гибридному сценарию, выполните следующее:

1. Создайте ресурс `ReplicatedStoragePool` с именем `data-safe` и добавьте в него ресурсы `LVMVolumeGroup`,
   чтобы на всех узлах в модуле `sds-replicated-volume` в `ReplicatedStorageClass` с параметром `replication: None`
   использовалась только VG `main-safe`:

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

1. Создайте ресурс `ReplicatedStoragePool` с именем `data-unsafe` и добавьте в него ресурсы `LVMVolumeGroup`,
   чтобы на всех узлах в модуле `sds-replicated-volume` в `ReplicatedStorageClass` с параметром `replication: Availability` или
   `replication: ConsistencyAndAvailability` использовалась только VG `main-unsafe`:

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

1. Создайте ресурс `ReplicatedStoragePool` и в поле `storagePool` укажите имя созданных ранее ресурсов `ReplicatedStoragePool`,
   чтобы на всех узлах использовались VG `main-safe` и `main-unsafe`:

   ```yaml
   kubectl apply -f -<<EOF
   ---
   apiVersion: storage.deckhouse.io/v1alpha1
   kind: ReplicatedStorageClass
   metadata:
     name: replicated-sc-r1
   spec:
     storagePool: data-safe # Обратите внимание, что из-за replication: None для этого ресурса используется data-safe; следовательно, репликация данных для постоянных томов (PV), созданных с этим StorageClass, проводиться не будет
     replication: None
     reclaimPolicy: Delete
     topology: Ignored # Если указать данную топологию, в кластере не должно быть зон (узлов с метками topology.kubernetes.io/zone)

   ---
   apiVersion: storage.deckhouse.io/v1alpha1
   kind: ReplicatedStorageClass
   metadata:
     name: replicated-sc-r2
   spec:
     storagePool: data-unsafe # Обратите внимание, что из-за replication: Availability для этого ресурса используется data-unsafe; следовательно, будет проводиться репликация данных для PV, созданных с этим StorageClass
     replication: Availability
     reclaimPolicy: Delete
     topology: Ignored

   ---
   apiVersion: storage.deckhouse.io/v1alpha1
   kind: ReplicatedStorageClass
   metadata:
     name: replicated-sc-r3
   spec:
     storagePool: data-unsafe # Обратите внимание, что из-за replication: ConsistencyAndAvailability для этого ресурса используется data-unsafe; следовательно, будет проводиться репликация данных для PV, созданных с этим StorageClass
     replication: ConsistencyAndAvailability
     reclaimPolicy: Delete
     topology: Ignored
   EOF
   ```

## Хранилище с одинаковыми дисками и дополнительными дисками

В ситуации, когда несколько одинаковых дисков комбинируются с дополнительными,
мы рекомендуем сделать зеркало из одинаковых дисков и установить на него операционную систему, но не использовать для SDS.
Вместо этого, для SDS используйте дополнительные диски.

Также рекомендуем использовать дополнительные диски для разных целей, в зависимости от их типа.

### NVMe SSD

Мы рекомендуем использовать диски NVMe SSD для создания томов, которые требуют высокой производительности.

#### Рекомендуемый сценарий

Чтобы настроить узел по рекомендуемому сценарию, выполните следующее:

1. Соберите зеркало из всех дисков NVMe SSD целиком (аппаратно или программно).
1. Создайте VG с именем `ssd-nvme` на зеркале.
1. Установите тег `storage.deckhouse.io/enabled=true` для VG `ssd-nvme`, используя следующую команду:

   ```shell
   vgchange ssd-nvme --addtag storage.deckhouse.io/enabled=true
   ```

##### Пример настройки модулей SDS

В данном примере предполагается, что вы настроили три узла по рекомендуемому сценарию.
В кластере Deckhouse при этом появятся три ресурса `LVMVolumeGroup` со случайно сгенерированными именами.
В будущем добавится возможность указывать имя для ресурсов `LVMVolumeGroup`,
которые создаются в процессе автоматического обнаружения VG, с помощью тега `LVM` с желаемым именем ресурса.

Чтобы вывести список ресурсов `LVMVolumeGroup`, выполните следующую команду:

```shell
kubectl get lvmvolumegroups.storage.deckhouse.io
```

В результате будет выведен следующий список:

```console
NAME                                      THINPOOLS   CONFIGURATION APPLIED   PHASE   NODE       SIZE      ALLOCATED SIZE   VG         AGE
vg-08d3730c-9201-428d-966c-45795cba55a6   0/0         True                    Ready   worker-2   25596Mi   0                ssd-nvme   61s
vg-b59ff9e1-6ef2-4761-b5d2-6172926d4f4d   0/0         True                    Ready   worker-0   25596Mi   0                ssd-nvme   4m17s
vg-c7863e12-c143-42bb-8e33-d578ce50d6c7   0/0         True                    Ready   worker-1   25596Mi   0                ssd-nvme   108s
```

###### Настройка модуля `sds-local-volume`

Чтобы настроить модуль `sds-local-volume` по рекомендуемому сценарию, создайте ресурс `LocalStorageClass`
и добавьте в него все ресурсы `LVMVolumeGroup`, чтобы VG `ssd-nvme` использовалась на всех узлах в модуле `sds-local-volume`:

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

###### Настройка модуля `sds-replicated-volume`

Чтобы настроить модуль `sds-replicated-volume` по рекомендуемому сценарию, выполните следующее:

1. Создайте ресурс `ReplicatedStoragePool` и добавьте в него все ресурсы `LVMVolumeGroup`,
   чтобы VG `ssd-nvme` использовалась на всех узлах в модуле `sds-replicated-volume`:

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

1. Создайте ресурс `ReplicatedStorageClass` и в поле `storagePool` укажите имя созданного ранее ресурса `ReplicatedStoragePool`:

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
     topology: Ignored # Если указать данную топологию, в кластере не должно быть зон (узлов с метками topology.kubernetes.io/zone)

   ---
   apiVersion: storage.deckhouse.io/v1alpha1
   kind: ReplicatedStorageClass
   metadata:
     name: replicated-sc-ssd-nvme-r2
   spec:
     storagePool: data-ssd-nvme
     replication: Availability
     reclaimPolicy: Delete
     topology: Ignored

   ---
   apiVersion: storage.deckhouse.io/v1alpha1
   kind: ReplicatedStorageClass
   metadata:
     name: replicated-sc-ssd-nvme-r3
   spec:
     storagePool: data-ssd-nvme
     replication: ConsistencyAndAvailability
     reclaimPolicy: Delete
     topology: Ignored
   EOF
   ```

#### Гибридный сценарий с NVMe SSD

{{< alert level="warning" >}}
Не поддерживается использование разделов с одинаковыми PARTUUID,
а также изменение PARTUUID раздела, который используется для создания VG.
При создании таблицы разделов рекомендуется выбрать формат `GPT`,
так как PARTUUID в `MBR` является псевдослучайным и содержит в себе номер раздела.
Помимо этого, в `MBR` нельзя задать атрибут PARTLABEL,
который может пригодиться для последующей идентификации раздела в Deckhouse.
{{< /alert >}}

В данном сценарии используются два раздела на каждом диске:
один для хранения данных SDS, которые не реплицируются,
и другой для данных SDS, которые реплицируются.
Первый раздел каждого диска используется для создания зеркала, а второй – для создания отдельной VG без зеркалирования.
Это позволяет максимально эффективно использовать место на диске.

Чтобы настроить узел по гибридному сценарию с NVMe SSD, выполните следующее:

1. создайте по два раздела на каждом диске;
1. соберите зеркало из первых разделов на каждом диске;
1. создайте VG с именем `ssd-nvme-safe` на зеркале;
1. создайте VG с именем `ssd-nvme-unsafe` из вторых разделов каждого диска;
1. установите тег `storage.deckhouse.io/enabled=true` для VG `ssd-nvme-safe` и `ssd-nvme-unsafe`, используя следующую команду:

   ```shell
   vgchange ssd-nvme-safe --addtag storage.deckhouse.io/enabled=true
   vgchange ssd-nvme-unsafe --addtag storage.deckhouse.io/enabled=true
   ```

##### Пример настройки модулей SDS

В данном примере предполагается, что вы настроили три узла по гибридному сценарию с NVMe SSD.
В кластере Deckhouse при этом появятся шесть ресурсов `LVMVolumeGroup` со случайно сгенерированными именами.
В будущем добавится возможность указывать имя для ресурсов `LVMVolumeGroup`,
которые создаются в процессе автоматического обнаружения VG, с помощью тега `LVM` с желаемым именем ресурса.

Чтобы вывести список ресурсов `LVMVolumeGroup`, выполните следующую команду:

```shell
kubectl get lvmvolumegroups.storage.deckhouse.io
```

В результате будет выведен следующий список:

```console
NAME                                      THINPOOLS   CONFIGURATION APPLIED   PHASE   NODE       SIZE      ALLOCATED SIZE   VG                AGE
vg-08d3730c-9201-428d-966c-45795cba55a6   0/0         True                    Ready   worker-2   25596Mi   0                ssd-nvme-safe     61s
vg-b59ff9e1-6ef2-4761-b5d2-6172926d4f4d   0/0         True                    Ready   worker-0   25596Mi   0                ssd-nvme-safe     4m17s
vg-c7863e12-c143-42bb-8e33-d578ce50d6c7   0/0         True                    Ready   worker-1   25596Mi   0                ssd-nvme-safe     108s
vg-deccf08a-44d4-45f2-aea9-6232c0eeef91   0/0         True                    Ready   worker-2   25596Mi   0                ssd-nvme-unsafe   61s
vg-e0f00cab-03b3-49cf-a2f6-595628a2593c   0/0         True                    Ready   worker-0   25596Mi   0                ssd-nvme-unsafe   4m17s
vg-fe679d22-2bc7-409c-85a9-9f0ee29a6ca2   0/0         True                    Ready   worker-1   25596Mi   0                ssd-nvme-unsafe   108s
```

###### Настройка модуля `sds-local-volume`

Чтобы настроить модуль `sds-local-volume` по гибридному сценарию c NVMe SSD, создайте ресурс `LocalStorageClass`
и добавьте в него ресурсы `LVMVolumeGroup`, чтобы на всех узлах в модуле `sds-local-volume` использовалась только VG `ssd-nvme-safe`:

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

###### Настройка модуля `sds-replicated-volume`

Чтобы настроить модуль `sds-replicated-volume` по гибридному сценарию c NVMe SSD, выполните следующее:

1. Создайте ресурс `ReplicatedStoragePool` с именем `data-ssd-nvme-safe` и добавьте в него ресурсы `LVMVolumeGroup`,
   чтобы на всех узлах в модуле `sds-replicated-volume` в `ReplicatedStorageClass` с параметром `replication: None`
   использовалась только VG `ssd-nvme-safe`:

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

1. Создайте ресурс `ReplicatedStoragePool` с именем `data-ssd-nvme-unsafe` и добавьте в него ресурсы `LVMVolumeGroup`,
   чтобы на всех узлах в модуле `sds-replicated-volume` в `ReplicatedStorageClass` с параметром `replication: Availability` или
   `replication: ConsistencyAndAvailability` использовалась только VG `ssd-nvme-unsafe`:

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

1. Создайте ресурс `ReplicatedStoragePool` и в поле `storagePool` укажите имя созданных ранее ресурсов `ReplicatedStoragePool`,
   чтобы на всех узлах использовались VG `ssd-nvme-safe` и `ssd-nvme-unsafe`:

   ```yaml
   kubectl apply -f -<<EOF
   ---
   apiVersion: storage.deckhouse.io/v1alpha1
   kind: ReplicatedStorageClass
   metadata:
     name: replicated-sc-ssd-nvme-r1
   spec:
     storagePool: data-ssd-nvme-safe # Обратите внимание, что из-за replication: None для этого ресурса используется data-ssd-nvme-safe; следовательно, репликация данных для PV, созданных с этим StorageClass, проводиться не будет
     replication: None
     reclaimPolicy: Delete
     topology: Ignored # Если указать данную топологию, в кластере не должно быть зон (узлов с метками topology.kubernetes.io/zone)

   ---
   apiVersion: storage.deckhouse.io/v1alpha1
   kind: ReplicatedStorageClass
   metadata:
     name: replicated-sc-ssd-nvme-r2
   spec:
     storagePool: data-ssd-nvme-unsafe # Обратите внимание, что из-за replication: Availability для этого ресурса используется data-ssd-nvme-unsafe; следовательно, будет проводиться репликация данных для PV, созданных с этим StorageClass
     replication: Availability
     reclaimPolicy: Delete
     topology: Ignored

   ---
   apiVersion: storage.deckhouse.io/v1alpha1
   kind: ReplicatedStorageClass
   metadata:
     name: replicated-sc-ssd-nvme-r3
   spec:
     storagePool: data-ssd-nvme-unsafe # Обратите внимание, что из-за replication: ConsistencyAndAvailability для этого ресурса используется data-ssd-nvme-unsafe; следовательно, будет проводиться репликация данных для PV, созданных с этим StorageClass
     replication: ConsistencyAndAvailability
     reclaimPolicy: Delete
     topology: Ignored
   EOF
   ```

### SATA SSD

Мы рекомендуем использовать диски SATA SSD для создания томов, которые не требуют высокой производительности.

#### Рекомендуемый сценарий

Чтобы настроить узел по рекомендуемому сценарию, выполните следующее:

1. Создайте VG с именем `ssd-sata` из всех дисков SATA SSD.
1. Установите тег `storage.deckhouse.io/enabled=true` для VG `ssd-sata`, используя следующую команду:

   ```shell
   vgchange ssd-sata --addtag storage.deckhouse.io/enabled=true
   ```

##### Пример настройки модулей SDS

В данном примере предполагается, что вы настроили три узла по рекомендуемому сценарию.
В кластере Deckhouse при этом появятся три ресурса `LVMVolumeGroup` со случайно сгенерированными именами.
В будущем добавится возможность указывать имя для ресурсов `LVMVolumeGroup`,
которые создаются в процессе автоматического обнаружения VG, с помощью тега `LVM` с желаемым именем ресурса.

Чтобы вывести список ресурсов `LVMVolumeGroup`, выполните следующую команду:

```shell
kubectl get lvmvolumegroups.storage.deckhouse.io
```

В результате будет выведен следующий список:

```console
NAME                                      THINPOOLS   CONFIGURATION APPLIED   PHASE   NODE       SIZE      ALLOCATED SIZE   VG         AGE
vg-08d3730c-9201-428d-966c-45795cba55a6   0/0         True                    Ready   worker-2   25596Mi   0                ssd-sata   61s
vg-b59ff9e1-6ef2-4761-b5d2-6172926d4f4d   0/0         True                    Ready   worker-0   25596Mi   0                ssd-sata   4m17s
vg-c7863e12-c143-42bb-8e33-d578ce50d6c7   0/0         True                    Ready   worker-1   25596Mi   0                ssd-sata   108s
```

###### Настройка модуля `sds-local-volume`

Чтобы настроить модуль `sds-local-volume` по рекомендуемому сценарию, создайте ресурс `LocalStorageClass`
и добавьте в него все ресурсы `LVMVolumeGroup`, чтобы VG `ssd-sata` использовалась на всех узлах в модуле `sds-local-volume`:

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

###### Настройка модуля `sds-replicated-volume`

Чтобы настроить модуль `sds-replicated-volume` по рекомендуемому сценарию, выполните следующее:

1. Создайте ресурс `ReplicatedStoragePool` и добавьте в него все ресурсы `LVMVolumeGroup`,
   чтобы VG `ssd-sata` использовалась на всех узлах в модуле `sds-replicated-volume`:

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

1. Создайте ресурс `ReplicatedStorageClass` и в поле `storagePool` укажите имя созданного ранее ресурса `ReplicatedStoragePool`:

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
     topology: Ignored # Если указать данную топологию, в кластере не должно быть зон (узлов с метками topology.kubernetes.io/zone)

   ---
   apiVersion: storage.deckhouse.io/v1alpha1
   kind: ReplicatedStorageClass
   metadata:
     name: replicated-sc-ssd-sata-r2
   spec:
     storagePool: data-ssd-sata
     replication: Availability
     reclaimPolicy: Delete
     topology: Ignored

   ---
   apiVersion: storage.deckhouse.io/v1alpha1
   kind: ReplicatedStorageClass
   metadata:
     name: replicated-sc-ssd-sata-r3
   spec:
     storagePool: data-ssd-sata
     replication: ConsistencyAndAvailability
     reclaimPolicy: Delete
     topology: Ignored
   EOF
   ```

#### Гибридный сценарий с SATA SSD

{{< alert level="warning" >}}
Не поддерживается использование разделов с одинаковыми PARTUUID,
а также изменение PARTUUID раздела, который используется для создания VG.
При создании таблицы разделов рекомендуется выбрать формат `GPT`,
так как PARTUUID в `MBR` является псевдослучайным и содержит в себе номер раздела.
Помимо этого, в `MBR` нельзя задать атрибут PARTLABEL,
который может пригодиться для последующей идентификации раздела в Deckhouse.
{{< /alert >}}

В данном сценарии используются два раздела на каждом диске:
один для хранения данных SDS, которые не реплицируются,
и другой для данных SDS, которые реплицируются.
Первый раздел каждого диска используется для создания зеркала, а второй – для создания отдельной VG без зеркалирования.
Это позволяет максимально эффективно использовать место на диске.

Чтобы настроить узел по гибридному сценарию с SATA SSD, выполните следующее:

1. создайте по два раздела на каждом диске;
1. соберите зеркало из первых разделов на каждом диске;
1. создайте VG с именем `ssd-sata-safe` на зеркале;
1. создайте VG с именем `ssd-sata-unsafe` из вторых разделов каждого диска;
1. установите тег `storage.deckhouse.io/enabled=true` для VG `ssd-sata-safe` и `ssd-sata-unsafe`, используя следующую команду:

   ```shell
   vgchange ssd-sata-safe --addtag storage.deckhouse.io/enabled=true
   vgchange ssd-sata-unsafe --addtag storage.deckhouse.io/enabled=true
   ```

##### Пример настройки модулей SDS

В данном примере предполагается, что вы настроили три узла по гибридному сценарию с SATA SSD.
В кластере Deckhouse при этом появятся шесть ресурсов `LVMVolumeGroup` со случайно сгенерированными именами.
В будущем добавится возможность указывать имя для ресурсов `LVMVolumeGroup`,
которые создаются в процессе автоматического обнаружения VG, с помощью тега `LVM` с желаемым именем ресурса.

Чтобы вывести список ресурсов `LVMVolumeGroup`, выполните следующую команду:

```shell
kubectl get lvmvolumegroups.storage.deckhouse.io
```

В результате будет выведен следующий список:

```console
NAME                                      THINPOOLS   CONFIGURATION APPLIED   PHASE   NODE       SIZE      ALLOCATED SIZE   VG                AGE
vg-08d3730c-9201-428d-966c-45795cba55a6   0/0         True                    Ready   worker-2   25596Mi   0                ssd-sata-safe     61s
vg-b59ff9e1-6ef2-4761-b5d2-6172926d4f4d   0/0         True                    Ready   worker-0   25596Mi   0                ssd-sata-safe     4m17s
vg-c7863e12-c143-42bb-8e33-d578ce50d6c7   0/0         True                    Ready   worker-1   25596Mi   0                ssd-sata-safe     108s
vg-deccf08a-44d4-45f2-aea9-6232c0eeef91   0/0         True                    Ready   worker-2   25596Mi   0                ssd-sata-unsafe   61s
vg-e0f00cab-03b3-49cf-a2f6-595628a2593c   0/0         True                    Ready   worker-0   25596Mi   0                ssd-sata-unsafe   4m17s
vg-fe679d22-2bc7-409c-85a9-9f0ee29a6ca2   0/0         True                    Ready   worker-1   25596Mi   0                ssd-sata-unsafe   108s
```

###### Настройка модуля `sds-local-volume`

Чтобы настроить модуль `sds-local-volume` по гибридному сценарию c SATA SSD, создайте ресурс `LocalStorageClass`
и добавьте в него ресурсы `LVMVolumeGroup`, чтобы на всех узлах в модуле `sds-local-volume` использовалась только VG `ssd-sata-safe`:

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

###### Настройка модуля `sds-replicated-volume`

Чтобы настроить модуль `sds-replicated-volume` по гибридному сценарию c SATA SSD, выполните следующее:

1. Создайте ресурс `ReplicatedStoragePool` с именем `data-ssd-sata-safe` и добавьте в него ресурсы `LVMVolumeGroup`,
   чтобы на всех узлах в модуле `sds-replicated-volume` в `ReplicatedStorageClass` с параметром `replication: None`
   использовалась только VG `ssd-sata-safe`:

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

1. Создайте ресурс `ReplicatedStoragePool` с именем `data-ssd-sata-unsafe` и добавьте в него ресурсы `LVMVolumeGroup`,
   чтобы на всех узлах в модуле `sds-replicated-volume` в `ReplicatedStorageClass` с параметром `replication: Availability` или
   `replication: ConsistencyAndAvailability` использовалась только VG `ssd-sata-unsafe`:

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

1. Создайте ресурс `ReplicatedStoragePool` и в поле `storagePool` укажите имя созданных ранее ресурсов `ReplicatedStoragePool`,
   чтобы на всех узлах использовались VG `ssd-sata-safe` и `ssd-sata-unsafe`:

   ```yaml
   kubectl apply -f -<<EOF
   ---
   apiVersion: storage.deckhouse.io/v1alpha1
   kind: ReplicatedStorageClass
   metadata:
     name: replicated-sc-ssd-sata-r1
   spec:
     storagePool: data-ssd-sata-safe # Обратите внимание, что из-за replication: None для этого ресурса используется data-ssd-sata-safe; следовательно, репликация данных для PV, созданных с этим StorageClass, проводиться не будет
     replication: None
     reclaimPolicy: Delete
     topology: Ignored # Если указать данную топологию, в кластере не должно быть зон (узлов с метками topology.kubernetes.io/zone)

   ---
   apiVersion: storage.deckhouse.io/v1alpha1
   kind: ReplicatedStorageClass
   metadata:
     name: replicated-sc-ssd-sata-r2
   spec:
     storagePool: data-ssd-sata-unsafe # Обратите внимание, что из-за replication: Availability для этого ресурса используется data-ssd-sata-unsafe; следовательно, будет проводиться репликация данных для PV, созданных с этим StorageClass
     replication: Availability
     reclaimPolicy: Delete
     topology: Ignored

   ---
   apiVersion: storage.deckhouse.io/v1alpha1
   kind: ReplicatedStorageClass
   metadata:
     name: replicated-sc-ssd-sata-r3
   spec:
     storagePool: data-ssd-sata-unsafe # Обратите внимание, что из-за replication: ConsistencyAndAvailability для этого ресурса используется data-ssd-sata-unsafe; следовательно, будет проводиться репликация данных для PV, созданных с этим StorageClass
     replication: ConsistencyAndAvailability
     reclaimPolicy: Delete
     topology: Ignored
   EOF
   ```

### HDD

Мы рекомендуем использовать диски HDD для создания томов, которые не требуют производительности.

#### Рекомендуемый сценарий

Чтобы настроить узел по рекомендуемому сценарию, выполните следующее:

1. Создайте VG с именем `hdd` из всех дисков HDD.
2. Установите тег `storage.deckhouse.io/enabled=true` для VG `hdd`, используя следующую команду:

   ```shell
   vgchange hdd --addtag storage.deckhouse.io/enabled=true
   ```

##### Пример настройки модулей SDS

В данном примере предполагается, что вы настроили три узла по рекомендуемому сценарию.
В кластере Deckhouse при этом появятся три ресурса `LVMVolumeGroup` со случайно сгенерированными именами.
В будущем добавится возможность указывать имя для ресурсов `LVMVolumeGroup`,
которые создаются в процессе автоматического обнаружения VG, с помощью тега `LVM` с желаемым именем ресурса.

Чтобы вывести список ресурсов `LVMVolumeGroup`, выполните следующую команду:

```shell
kubectl get lvmvolumegroups.storage.deckhouse.io
```

В результате будет выведен следующий список:

```console
NAME                                      THINPOOLS   CONFIGURATION APPLIED   PHASE   NODE       SIZE      ALLOCATED SIZE   VG    AGE
vg-08d3730c-9201-428d-966c-45795cba55a6   0/0         True                    Ready   worker-2   25596Mi   0                hdd   61s
vg-b59ff9e1-6ef2-4761-b5d2-6172926d4f4d   0/0         True                    Ready   worker-0   25596Mi   0                hdd   4m17s
vg-c7863e12-c143-42bb-8e33-d578ce50d6c7   0/0         True                    Ready   worker-1   25596Mi   0                hdd   108s
```

###### Настройка модуля `sds-local-volume`

Чтобы настроить модуль `sds-local-volume` по рекомендуемому сценарию, создайте ресурс `LocalStorageClass`
и добавьте в него все ресурсы `LVMVolumeGroup`, чтобы VG `hdd` использовалась на всех узлах в модуле `sds-local-volume`:

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

###### Настройка модуля `sds-replicated-volume`

Чтобы настроить модуль `sds-replicated-volume` по рекомендуемому сценарию, выполните следующее:

1. Создайте ресурс `ReplicatedStoragePool` и добавьте в него все ресурсы `LVMVolumeGroup`,
   чтобы VG `hdd` использовалась на всех узлах в модуле `sds-replicated-volume`:

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

1. Создайте ресурс `ReplicatedStorageClass` и в поле `storagePool` укажите имя созданного ранее ресурса `ReplicatedStoragePool`:

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
     topology: Ignored # Если указать данную топологию, в кластере не должно быть зон (узлов с метками topology.kubernetes.io/zone)

   ---
   apiVersion: storage.deckhouse.io/v1alpha1
   kind: ReplicatedStorageClass
   metadata:
     name: replicated-sc-hdd-r2
   spec:
     storagePool: data-hdd
     replication: Availability
     reclaimPolicy: Delete
     topology: Ignored

   ---
   apiVersion: storage.deckhouse.io/v1alpha1
   kind: ReplicatedStorageClass
   metadata:
     name: replicated-sc-hdd-r3
   spec:
     storagePool: data-hdd
     replication: ConsistencyAndAvailability
     reclaimPolicy: Delete
     topology: Ignored
   EOF
   ```

#### Гибридный сценарий с HDD

{{< alert level="warning" >}}
Не поддерживается использование разделов с одинаковыми PARTUUID,
а также изменение PARTUUID раздела, который используется для создания VG.
При создании таблицы разделов рекомендуется выбрать формат `GPT`,
так как PARTUUID в `MBR` является псевдослучайным и содержит в себе номер раздела.
Помимо этого, в `MBR` нельзя задать атрибут PARTLABEL,
который может пригодиться для последующей идентификации раздела в Deckhouse.
{{< /alert >}}

В данном сценарии используются два раздела на каждом диске:
один для хранения данных SDS, которые не реплицируются,
и другой для данных SDS, которые реплицируются.
Первый раздел каждого диска используется для создания зеркала, а второй – для создания отдельной VG без зеркалирования.
Это позволяет максимально эффективно использовать место на диске.

Чтобы настроить узел по гибридному сценарию с SATA SSD, выполните следующее:

1. создайте по два раздела на каждом диске;
1. соберите зеркало из первых разделов на каждом диске;
1. создайте VG с именем `hdd-safe` на зеркале;
1. создайте VG с именем `hdd-unsafe` из вторых разделов каждого диска;
1. установите тег `storage.deckhouse.io/enabled=true` для VG `hdd-safe` и `hdd-unsafe`, используя следующую команду:

   ```shell
   vgchange hdd-safe --addtag storage.deckhouse.io/enabled=true
   vgchange hdd-unsafe --addtag storage.deckhouse.io/enabled=true
   ```

##### Примеры настройки модулей SDS

В данном примере предполагается, что вы настроили три узла по гибридному сценарию с HDD.
В кластере Deckhouse при этом появятся шесть ресурсов `LVMVolumeGroup` со случайно сгенерированными именами.
В будущем добавится возможность указывать имя для ресурсов `LVMVolumeGroup`,
которые создаются в процессе автоматического обнаружения VG, с помощью тега `LVM` с желаемым именем ресурса.

Чтобы вывести список ресурсов `LVMVolumeGroup`, выполните следующую команду:

```shell
kubectl get lvmvolumegroups.storage.deckhouse.io
```

В результате будет выведен следующий список:

```console
NAME                                      THINPOOLS   CONFIGURATION APPLIED   PHASE   NODE       SIZE      ALLOCATED SIZE   VG           AGE
vg-08d3730c-9201-428d-966c-45795cba55a6   0/0         True                    Ready   worker-2   25596Mi   0                hdd-safe     61s
vg-b59ff9e1-6ef2-4761-b5d2-6172926d4f4d   0/0         True                    Ready   worker-0   25596Mi   0                hdd-safe     4m17s
vg-c7863e12-c143-42bb-8e33-d578ce50d6c7   0/0         True                    Ready   worker-1   25596Mi   0                hdd-safe     108s
vg-deccf08a-44d4-45f2-aea9-6232c0eeef91   0/0         True                    Ready   worker-2   25596Mi   0                hdd-unsafe   61s
vg-e0f00cab-03b3-49cf-a2f6-595628a2593c   0/0         True                    Ready   worker-0   25596Mi   0                hdd-unsafe   4m17s
vg-fe679d22-2bc7-409c-85a9-9f0ee29a6ca2   0/0         True                    Ready   worker-1   25596Mi   0                hdd-unsafe   108s
```

###### Настройка модуля `sds-local-volume`

Чтобы настроить модуль `sds-local-volume` по гибридному сценарию c HDD, создайте ресурс `LocalStorageClass`
и добавьте в него ресурсы `LVMVolumeGroup`, чтобы на всех узлах в модуле `sds-local-volume` использовалась только VG `hdd-safe`:

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

###### Настройка модуля `sds-replicated-volume`

Чтобы настроить модуль `sds-replicated-volume` по гибридному сценарию c HDD, выполните следующее:

1. Создайте ресурс `ReplicatedStoragePool` с именем `data-hdd-safe` и добавьте в него ресурсы `LVMVolumeGroup`,
   чтобы на всех узлах в модуле `sds-replicated-volume` в `ReplicatedStorageClass` с параметром `replication: None`
   использовалась только VG `hdd-safe`:

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

1. Создайте ресурс `ReplicatedStoragePool` с именем `data-hdd-unsafe` и добавьте в него ресурсы `LVMVolumeGroup`,
   чтобы на всех узлах в модуле `sds-replicated-volume` в `ReplicatedStorageClass` с параметром `replication: Availability` или
   `replication: ConsistencyAndAvailability` использовалась только VG `hdd-unsafe`:

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

1. Создайте ресурс `ReplicatedStoragePool` и в поле `storagePool` укажите имя созданных ранее ресурсов `ReplicatedStoragePool`,
   чтобы на всех узлах использовались VG `hdd-safe` и `hdd-unsafe`:

   ```yaml
   kubectl apply -f -<<EOF
   ---
   apiVersion: storage.deckhouse.io/v1alpha1
   kind: ReplicatedStorageClass
   metadata:
     name: replicated-sc-hdd-r1
   spec:
     storagePool: data-hdd-safe # Обратите внимание, что из-за replication: None для этого ресурса используется data-hdd-safe; следовательно, репликация данных для PV, созданных с этим StorageClass, проводиться не будет
     replication: None
     reclaimPolicy: Delete
     topology: Ignored # Если указать данную топологию, в кластере не должно быть зон (узлов с метками topology.kubernetes.io/zone)

   ---
   apiVersion: storage.deckhouse.io/v1alpha1
   kind: ReplicatedStorageClass
   metadata:
     name: replicated-sc-hdd-r2
   spec:
     storagePool: data-hdd-unsafe # Обратите внимание, что из-за replication: Availability для этого ресурса используется data-hdd-unsafe; следовательно, будет проводиться репликация данных для PV, созданных с этим StorageClass
     replication: Availability
     reclaimPolicy: Delete
     topology: Ignored

   ---
   apiVersion: storage.deckhouse.io/v1alpha1
   kind: ReplicatedStorageClass
   metadata:
     name: replicated-sc-hdd-r3
   spec:
     storagePool: data-hdd-unsafe # Обратите внимание, что из-за replication: ConsistencyAndAvailability для этого ресурса используется data-hdd-unsafe; следовательно, будет проводиться репликация данных для PV, созданных с этим StorageClass
     replication: ConsistencyAndAvailability
     reclaimPolicy: Delete
     topology: Ignored
   EOF
   ```
