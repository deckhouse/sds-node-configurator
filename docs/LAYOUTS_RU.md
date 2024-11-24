---
title: "Модуль sds-node-configurator: Сценарии использования"
linkTitle: "Сценарии использования"
---

{{< alert level="warning" >}}
Работоспособность модуля гарантируется только при использовании стоковых ядер, поставляемых вместе с [поддерживаемыми дистрибутивами](https://deckhouse.ru/documentation/v1/supported_versions.html#linux).

Работоспособность модуля при использовании других ядер или дистрибутивов возможна, но не гарантируется.
{{< /alert >}}

Примеры конфигурирования дисковой подсистемы на узлах кластера Kubernetes.

{{< alert level="warning" >}}
ВАЖНО! Если вы создаете виртуальные машины клонированием, то необходимо изменить uuid у vg в созданных таким образом виртуальных машинах. Для этого выполните команду `vgchange -u` - данная команда сгенерирует новые UUID для всех vg в виртуальной машине (можно добавить в cloud init).

Внимание! Изменить UUID у vg можно, только если в ней нет активных логических томов (lv). Для деактивации логических томов необходимо сначала их отмонтировать, а затем выполнить команду `lvchange -an <название vg (если необходимо деактивировать все тома в vg) или lv (если необходимо деактивировать конкретный том)>`.
{{< /alert >}}

## Несколько одинаковых дисков

### Рекомендованный сценарий

* Собрать зеркало из дисков целиком (аппаратно или программно), которое будет использоваться как для корневой системы, так и для данных.
* При установке операционной системы:
  * Создать vg с именем `main` на зеркале.
  * Создать lv с именем `root` в vg `main`.
  * Установить ОС на lv `root`.
* Поставить тег `storage.deckhouse.io/enabled=true` на vg `main` такой командой:

```shell
vgchange main --addtag storage.deckhouse.io/enabled=true
```

* Добавить подготовленный узел в кластер Deckhouse.

Если узел подходит под `nodeSelector`, который указан в `spec.nodeSelector` модулей `sds-replicated-volume` и/или `sds-local-volume`, то на этом узле запустится агент модуля `sds-node-configurator`, который определит vg `main` и добавит соответствующий этой vg ресурс `LVMVolumeGroup` в кластер Deckhouse. Дальше ресурс `LVMVolumeGroup` можно использовать для создания томов в модулях `sds-replicated-volume` и/или `sds-local-volume`.

#### Плюсы и минусы сценария

| Плюсы                     | Минусы      |
|---------------------------|-------------|
| Надежно  | Overhead по месту на диске для SDS, которые сами реплицируют данные  |
| Просто в настройке и использовании |  |
| Удобно распределять (и перераспределять) место между разными SDS |  |

#### Примеры настройки модулей SDS при использовании рекомендованного сценария

Допустим, что у Вас есть три узла, настроенных по рекомендованному сценарию. В таком случае в кластере Deckhouse появятся три ресурса `LVMVolumeGroup` со случайно сгенерированными именами (в будущем добавится возможность указывать имя для ресурсов `LVMVolumeGroup`, которые создаются в процессе автоматического обнаружения vg, путем добавления lvm тега с желаемым именем ресурса).

```shell
kubectl get lvmvolumegroups.storage.deckhouse.io

NAME                                      THINPOOLS   CONFIGURATION APPLIED   PHASE   NODE       SIZE      ALLOCATED SIZE   VG     AGE
vg-08d3730c-9201-428d-966c-45795cba55a6   0/0         True                    Ready   worker-2   25596Mi   0                main   61s
vg-b59ff9e1-6ef2-4761-b5d2-6172926d4f4d   0/0         True                    Ready   worker-0   25596Mi   0                main   4m17s
vg-c7863e12-c143-42bb-8e33-d578ce50d6c7   0/0         True                    Ready   worker-1   25596Mi   0                main   108s
```

##### Настройка модуля `sds-local-volume`

* Создаем ресурс `LocalStorageClass` и добавляем в него все наши ресурсы `LVMVolumeGroup` для использования vg `main` на всех наших узлах в модуле `sds-local-volume`:

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

* Создаем ресурс `ReplicatedStoragePool` и добавляем в него все наши ресурсы `LVMVolumeGroup` для использования vg `main` на всех наших узлах в модуле `sds-replicated-volume`:

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

* Создаем ресурсы `ReplicatedStorageClass` и указываем в поле `storagePool` имя созданного ранее ресурса `ReplicatedStoragePool`:

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
  topology: Ignored # - если указываем такую топологию, то в кластере не должно быть зон (узлов с метками topology.kubernetes.io/zone).

---
apiVersion: storage.deckhouse.io/v1alpha1
kind: ReplicatedStorageClass
metadata:
  name: replicated-sc-r2
spec:
  storagePool: data
  replication: Availability
  reclaimPolicy: Delete
  topology: Ignored # - если указываем такую топологию, то в кластере не должно быть зон (узлов с метками topology.kubernetes.io/zone).

---
apiVersion: storage.deckhouse.io/v1alpha1
kind: ReplicatedStorageClass
metadata:
  name: replicated-sc-r3
spec:
  storagePool: data
  replication: ConsistencyAndAvailability
  reclaimPolicy: Delete
  topology: Ignored # - если указываем такую топологию, то в кластере не должно быть зон (узлов с метками topology.kubernetes.io/zone).
EOF
```

### Гибридный сценарий с несколькими разделами для экономии места

{{< alert level="warning" >}}
ВАЖНО! Не поддерживается использование разделов с одинаковыми part UUID, а так же изменение part UUID раздела, который используется для создания vg. При создании таблицы разделов рекомендуется выбрать GPT, так как part UUID в MBR является псевдослучайным и содержит в себе номер раздела. Помимо этого, в MBR нельзя задать partlabel, который может быть удобным для последующей идентификации раздела в Deckhouse.
{{< /alert >}}

В этом сценарии используются два раздела на каждом диске: один для корневой системы и хранения данных SDS, которые не реплицируются, другой для данных SDS, которые реплицируются. Первый раздел каждого диска используется для создания зеркала, а второй раздел для создания отдельной vg без зеркалирования. Таким образом, место на диске используется максимально эффективно.

* При установке операционной системы:
  * Создать по 2 раздела на каждом диске.
  * Собрать зеркало из первых разделов на каждом диске.
  * Создать vg с именем `main-safe` на зеркале.
  * Создать lv с именем `root` в vg `main-safe`.
  * Установить ОС на lv `root`.
* Поставить тег `storage.deckhouse.io/enabled=true` на vg `main-safe` такой командой:
  
```shell
vgchange main-safe --addtag storage.deckhouse.io/enabled=true
```

* Создать vg с именем `main-unsafe` из вторых разделов каждого диска.
* Поставить тег `storage.deckhouse.io/enabled=true` на vg `main-unsafe` такой командой:

```shell
vgchange main-unsafe --addtag storage.deckhouse.io/enabled=true
```

* Добавить подготовленный узел в кластер Deckhouse.

Если узел подходит под `nodeSelector`, который указан в `spec.nodeSelector` модулей `sds-replicated-volume` и/или `sds-local-volume`, то на этом узле запустится агент модуля `sds-node-configurator`, который определит vg `main-safe` и `main-unsafe` и добавит соответствующие этим vg ресурсы `LVMVolumeGroup` в кластер Deckhouse. Дальше ресурсы `LVMVolumeGroup` можно использовать для создания томов в модулях `sds-replicated-volume` и/или `sds-local-volume`.

#### Плюсы и минусы сценария

| Плюсы                     | Минусы      |
|---------------------------|-------------|
| Надежно  | Сложно в настройке и использовании  |
| Место используется максимально эффективно | Очень сложно перераспределять место между safe и unsafe разделами |

#### Примеры настройки модулей SDS при использовании гибридного сценария

Допустим, что у Вас есть три узла, настроенных по гибридному сценарию. В таком случае в кластере Deckhouse появятся шесть ресурсов `LVMVolumeGroup` со случайно сгенерированными именами (в будущем добавится возможность указывать имя для ресурсов `LVMVolumeGroup`, которые создаются в процессе автоматического обнаружения vg, путем добавления lvm тега с желаемым именем ресурса).

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

##### Настройка модуля `sds-local-volume`

* Создаем ресурс `LocalStorageClass` и добавляем в него ресурсы `LVMVolumeGroup` для использования только vg `main-safe` на всех наших узлах в модуле `sds-local-volume`:

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

* Создаем ресурс `ReplicatedStoragePool` с именем data-safe и добавляем в него ресурсы `LVMVolumeGroup` для использования только vg `main-safe` на всех наших узлах в модуле `sds-replicated-volume` в `ReplicatedStorageClass` с replication: `None`:

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

* Создаем ресурс `ReplicatedStoragePool` с именем data-unsafe и добавляем в него ресурсы `LVMVolumeGroup` для использования только vg `main-unsafe` на всех наших узлах в модуле `sds-replicated-volume` в `ReplicatedStorageClass` с replication `Availability` или `ConsistencyAndAvailability`:

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

* Создаем ресурсы `ReplicatedStorageClass` и указываем в поле `storagePool` имя созданных ранее ресурсов `ReplicatedStoragePool` для использования vg `main-safe` и `main-unsafe` на всех наших узлах:

```yaml
kubectl apply -f -<<EOF
---
apiVersion: storage.deckhouse.io/v1alpha1
kind: ReplicatedStorageClass
metadata:
  name: replicated-sc-r1
spec:
  storagePool: data-safe # обратите внимание, что мы используем data-safe для этого ресурса, так как в нем replication: None и репликация данных НЕ будет производиться для PV, созданных с этим StorageClass
  replication: None
  reclaimPolicy: Delete
  topology: Ignored # - если указываем такую топологию, то в кластере не должно быть зон (узлов с метками topology.kubernetes.io/zone).

---
apiVersion: storage.deckhouse.io/v1alpha1
kind: ReplicatedStorageClass
metadata:
  name: replicated-sc-r2
spec:
  storagePool: data-unsafe # обратите внимание, что мы используем data-unsafe для этого ресурса, так как в нем replication: Availability и репликация данных будет производиться для PV, созданных с этим StorageClass
  replication: Availability
  reclaimPolicy: Delete
  topology: Ignored # - если указываем такую топологию, то в кластере не должно быть зон (узлов с метками topology.kubernetes.io/zone).

---
apiVersion: storage.deckhouse.io/v1alpha1
kind: ReplicatedStorageClass
metadata:
  name: replicated-sc-r3
spec:
  storagePool: data-unsafe # обратите внимание, что мы используем data-unsafe для этого ресурса, так как в нем replication: ConsistencyAndAvailability и репликация данных будет производиться для PV, созданных с этим StorageClass
  replication: ConsistencyAndAvailability
  reclaimPolicy: Delete
  topology: Ignored # - если указываем такую топологию, то в кластере не должно быть зон (узлов с метками topology.kubernetes.io/zone).
EOF
```

## Несколько одинаковых дисков и дополнительные диски

В таком случае рекомендуется из одинаковых дисков сделать зеркало, установить на него ОС и НЕ использовать его для SDS. Для SDS использовать дополнительные диски.

Рекомендуется разделить дополнительные диски на типы и использовать их для разных целей.

### Дополнительные диски - NVMe SSD

Рекомендуется использовать NVMe SSD для создания томов, которые требуют высокой производительности.

#### Рекомендованный сценарий

* Собрать зеркало из всех NVMe SSD дисков целиком (аппаратно или программно).
* Создать vg с именем `ssd-nvme` на зеркале.
* Поставить тег `storage.deckhouse.io/enabled=true` на vg `ssd-nvme` такой командой:

```shell
vgchange ssd-nvme --addtag storage.deckhouse.io/enabled=true
```

##### Плюсы и минусы сценария

| Плюсы                     | Минусы      |
|---------------------------|-------------|
| Надежно  | Overhead по месту на диске для SDS, которые сами реплицируют данные  |
| Просто в настройке и использовании |  |
| Удобно распределять (и перераспределять) место между разными SDS |  |

##### Примеры настройки модулей SDS при использовании рекомендованного сценария

Допустим, что у Вас есть три узла, настроенных по рекомендованному сценарию. В таком случае в кластере Deckhouse появится три ресурса `LVMVolumeGroup` со случайно сгенерированными именами (в будущем добавится возможность указывать имя для ресурсов `LVMVolumeGroup`, которые создаются в процессе автоматического обнаружения vg, путем добавления lvm тега с желаемым именем ресурса).

```shell
kubectl get lvmvolumegroups.storage.deckhouse.io

NAME                                      THINPOOLS   CONFIGURATION APPLIED   PHASE   NODE       SIZE      ALLOCATED SIZE   VG         AGE
vg-08d3730c-9201-428d-966c-45795cba55a6   0/0         True                    Ready   worker-2   25596Mi   0                ssd-nvme   61s
vg-b59ff9e1-6ef2-4761-b5d2-6172926d4f4d   0/0         True                    Ready   worker-0   25596Mi   0                ssd-nvme   4m17s
vg-c7863e12-c143-42bb-8e33-d578ce50d6c7   0/0         True                    Ready   worker-1   25596Mi   0                ssd-nvme   108s
```

###### Настройка модуля `sds-local-volume`

* Создаем ресурс `LocalStorageClass` и добавляем в него все наши ресурсы `LVMVolumeGroup` для использования vg `ssd-nvme` на всех наших узлах в модуле `sds-local-volume`:

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

* Создаем ресурс `ReplicatedStoragePool` и добавляем в него все наши ресурсы `LVMVolumeGroup` для использования vg `ssd-nvme` на всех наших узлах в модуле `sds-replicated-volume`:

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

* Создаем ресурсы `ReplicatedStorageClass` и указываем в поле `storagePool` имя созданного ранее ресурса `ReplicatedStoragePool`:

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
  topology: Ignored # - если указываем такую топологию, то в кластере не должно быть зон (узлов с метками topology.kubernetes.io/zone).

---
apiVersion: storage.deckhouse.io/v1alpha1
kind: ReplicatedStorageClass
metadata:
  name: replicated-sc-ssd-nvme-r2
spec:
  storagePool: data-ssd-nvme
  replication: Availability
  reclaimPolicy: Delete
  topology: Ignored # - если указываем такую топологию, то в кластере не должно быть зон (узлов с метками topology.kubernetes.io/zone).

---
apiVersion: storage.deckhouse.io/v1alpha1
kind: ReplicatedStorageClass
metadata:
  name: replicated-sc-ssd-nvme-r3
spec:
  storagePool: data-ssd-nvme
  replication: ConsistencyAndAvailability
  reclaimPolicy: Delete
  topology: Ignored # - если указываем такую топологию, то в кластере не должно быть зон (узлов с метками topology.kubernetes.io/zone).
EOF
```

#### Гибридный сценарий с NVMe SSD

{{< alert level="warning" >}}
ВАЖНО! Не поддерживается использование разделов с одинаковыми part UUID, а так же изменение part UUID раздела, который используется для создания vg. При создании таблицы разделов рекомендуется выбрать GPT, так как part UUID в MBR является псевдослучайным и содержит в себе номер раздела. Помимо этого, в MBR нельзя задать partlabel, который может быть удобным для последующей идентификации раздела в Deckhouse.
{{< /alert >}}

В этом сценарии используются два раздела на каждом диске: один для хранения данных SDS, которые не реплицируются, другой для данных SDS, которые реплицируются. Первый раздел каждого диска используется для создания зеркала, а второй раздел для создания отдельной vg без зеркалирования. Таким образом, место на диске используется максимально эффективно.

* Создать по 2 раздела на каждом диске.
* Собрать зеркало из первых разделов на каждом диске.
* Создать vg с именем `ssd-nvme-safe` на зеркале.
* Создать vg с именем `ssd-nvme-unsafe` из вторых разделов каждого диска.
* Поставить тег `storage.deckhouse.io/enabled=true` на vg `ssd-nvme-safe` и `ssd-nvme-unsafe` такой командой:

```shell
vgchange ssd-nvme-safe --addtag storage.deckhouse.io/enabled=true
vgchange ssd-nvme-unsafe --addtag storage.deckhouse.io/enabled=true
```

##### Плюсы и минусы сценария

| Плюсы                     | Минусы      |
|---------------------------|-------------|
| Надежно  | Сложно в настройке и использовании  |
| Место используется максимально эффективно | Очень сложно перераспределять место между safe и unsafe разделами |

##### Примеры настройки модулей SDS при использовании гибридного сценария с NVMe SSD

Допустим, что у Вас есть три узла, настроенных по гибридному сценарию с NVMe SSD. В таком случае в кластере Deckhouse появятся шесть ресурсов `LVMVolumeGroup` со случайно сгенерированными именами (в будущем добавится возможность указывать имя для ресурсов `LVMVolumeGroup`, которые создаются в процессе автоматического обнаружения vg, путем добавления lvm тега с желаемым именем ресурса).

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

###### Настройка модуля `sds-local-volume`

* Создаем ресурс `LocalStorageClass` и добавляем в него ресурсы `LVMVolumeGroup` для использования только vg `ssd-nvme-safe` на всех наших узлах в модуле `sds-local-volume`:

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

* Создаем ресурс `ReplicatedStoragePool` с именем data-ssd-nvme-safe и добавляем в него ресурсы `LVMVolumeGroup` для использования только vg `ssd-nvme-safe` на всех наших узлах в модуле `sds-replicated-volume` в `ReplicatedStorageClass` с replication: `None`:

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

* Создаем ресурс `ReplicatedStoragePool` с именем data-ssd-nvme-unsafe и добавляем в него ресурсы `LVMVolumeGroup` для использования только vg `ssd-nvme-unsafe` на всех наших узлах в модуле `sds-replicated-volume` в `ReplicatedStorageClass` с replication `Availability` или `ConsistencyAndAvailability`:

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

* Создаем ресурсы `ReplicatedStorageClass` и указываем в поле `storagePool` имя созданных ранее ресурсов `ReplicatedStoragePool` для использования vg `ssd-nvme-safe` и `ssd-nvme-unsafe` на всех наших узлах:

```yaml
kubectl apply -f -<<EOF
---
apiVersion: storage.deckhouse.io/v1alpha1
kind: ReplicatedStorageClass
metadata:
  name: replicated-sc-ssd-nvme-r1
spec:
  storagePool: data-ssd-nvme-safe # обратите внимание, что мы используем data-ssd-nvme-safe для этого ресурса, так как в нем replication: None и репликация данных НЕ будет производиться для PV, созданных с этим StorageClass
  replication: None
  reclaimPolicy: Delete
  topology: Ignored # - если указываем такую топологию, то в кластере не должно быть зон (узлов с метками topology.kubernetes.io/zone).

---
apiVersion: storage.deckhouse.io/v1alpha1
kind: ReplicatedStorageClass
metadata:
  name: replicated-sc-ssd-nvme-r2
spec:
  storagePool: data-ssd-nvme-unsafe # обратите внимание, что мы используем data-ssd-nvme-unsafe для этого ресурса, так как в нем replication: Availability и репликация данных будет производиться для PV, созданных с этим StorageClass
  replication: Availability
  reclaimPolicy: Delete
  topology: Ignored # - если указываем такую топологию, то в кластере не должно быть зон (узлов с метками topology.kubernetes.io/zone).

---
apiVersion: storage.deckhouse.io/v1alpha1
kind: ReplicatedStorageClass
metadata:
  name: replicated-sc-ssd-nvme-r3
spec:
  storagePool: data-ssd-nvme-unsafe # обратите внимание, что мы используем data-ssd-nvme-unsafe для этого ресурса, так как в нем replication: ConsistencyAndAvailability и репликация данных будет производиться для PV, созданных с этим StorageClass
  replication: ConsistencyAndAvailability
  reclaimPolicy: Delete
  topology: Ignored # - если указываем такую топологию, то в кластере не должно быть зон (узлов с метками topology.kubernetes.io/zone).
EOF
```

### Дополнительные диски - SATA SSD

Рекомендуется использовать SATA SSD для создания томов, которые не требуют высокой производительности.

#### Рекомендованный сценарий

* Создать vg с именем `ssd-sata` из всех SATA SSD дисков.
* Поставить тег `storage.deckhouse.io/enabled=true` на vg `ssd-sata` такой командой:

```shell
vgchange ssd-sata --addtag storage.deckhouse.io/enabled=true
```

##### Плюсы и минусы сценария

| Плюсы                     | Минусы      |
|---------------------------|-------------|
| Надежно  | Overhead по месту на диске для SDS, которые сами реплицируют данные  |
| Просто в настройке и использовании |  |
| Удобно распределять (и перераспределять) место между разными SDS |  |

##### Примеры настройки модулей SDS при использовании рекомендованного сценария

Допустим, что у Вас есть три узла, настроенных по рекомендованному сценарию. В таком случае в кластере Deckhouse появится три ресурса `LVMVolumeGroup` со случайно сгенерированными именами (в будущем добавится возможность указывать имя для ресурсов `LVMVolumeGroup`, которые создаются в процессе автоматического обнаружения vg, путем добавления lvm тега с желаемым именем ресурса).

```shell
kubectl get lvmvolumegroups.storage.deckhouse.io

NAME                                      THINPOOLS   CONFIGURATION APPLIED   PHASE   NODE       SIZE      ALLOCATED SIZE   VG         AGE
vg-08d3730c-9201-428d-966c-45795cba55a6   0/0         True                    Ready   worker-2   25596Mi   0                ssd-sata   61s
vg-b59ff9e1-6ef2-4761-b5d2-6172926d4f4d   0/0         True                    Ready   worker-0   25596Mi   0                ssd-sata   4m17s
vg-c7863e12-c143-42bb-8e33-d578ce50d6c7   0/0         True                    Ready   worker-1   25596Mi   0                ssd-sata   108s
```

###### Настройка модуля `sds-local-volume`

* Создаем ресурс `LocalStorageClass` и добавляем в него все наши ресурсы `LVMVolumeGroup` для использования vg `ssd-sata` на всех наших узлах в модуле `sds-local-volume`:

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

* Создаем ресурс `ReplicatedStoragePool` и добавляем в него все наши ресурсы `LVMVolumeGroup` для использования vg `ssd-sata` на всех наших узлах в модуле `sds-replicated-volume`:

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

* Создаем ресурсы `ReplicatedStorageClass` и указываем в поле `storagePool` имя созданного ранее ресурса `ReplicatedStoragePool`:

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
  topology: Ignored # - если указываем такую топологию, то в кластере не должно быть зон (узлов с метками topology.kubernetes.io/zone).

---
apiVersion: storage.deckhouse.io/v1alpha1
kind: ReplicatedStorageClass
metadata:
  name: replicated-sc-ssd-sata-r2
spec:
  storagePool: data-ssd-sata
  replication: Availability
  reclaimPolicy: Delete
  topology: Ignored # - если указываем такую топологию, то в кластере не должно быть зон (узлов с метками topology.kubernetes.io/zone).

---
apiVersion: storage.deckhouse.io/v1alpha1
kind: ReplicatedStorageClass
metadata:
  name: replicated-sc-ssd-sata-r3
spec:
  storagePool: data-ssd-sata
  replication: ConsistencyAndAvailability
  reclaimPolicy: Delete
  topology: Ignored # - если указываем такую топологию, то в кластере не должно быть зон (узлов с метками topology.kubernetes.io/zone).
EOF
```

#### Гибридный сценарий с SATA SSD

{{< alert level="warning" >}}
ВАЖНО! Не поддерживается использование разделов с одинаковыми part UUID, а так же изменение part UUID раздела, который используется для создания vg. При создании таблицы разделов рекомендуется выбрать GPT, так как part UUID в MBR является псевдослучайным и содержит в себе номер раздела. Помимо этого, в MBR нельзя задать partlabel, который может быть удобным для последующей идентификации раздела в Deckhouse.
{{< /alert >}}

В этом сценарии используются два раздела на каждом диске: один для хранения данных SDS, которые не реплицируются, другой для данных SDS, которые реплицируются. Первый раздел каждого диска используется для создания зеркала, а второй раздел для создания отдельной vg без зеркалирования. Таким образом, место на диске используется максимально эффективно.

* Создать по 2 раздела на каждом диске.
* Собрать зеркало из первых разделов на каждом диске.
* Создать vg с именем `ssd-sata-safe` на зеркале.
* Создать vg с именем `ssd-sata-unsafe` из вторых разделов каждого диска.
* Поставить тег `storage.deckhouse.io/enabled=true` на vg `ssd-sata-safe` и `ssd-sata-unsafe` такой командой:

```shell
vgchange ssd-sata-safe --addtag storage.deckhouse.io/enabled=true
vgchange ssd-sata-unsafe --addtag storage.deckhouse.io/enabled=true
```

##### Плюсы и минусы сценария

| Плюсы                     | Минусы      |
|---------------------------|-------------|
| Надежно  | Сложно в настройке и использовании  |
| Место используется максимально эффективно | Очень сложно перераспределять место между safe и unsafe разделами |

##### Примеры настройки модулей SDS при использовании гибридного сценария с SATA SSD

Допустим, что у Вас есть три узла, настроенных по гибридному сценарию с SATA SSD. В таком случае в кластере Deckhouse появятся шесть ресурсов `LVMVolumeGroup` со случайно сгенерированными именами (в будущем добавится возможность указывать имя для ресурсов `LVMVolumeGroup`, которые создаются в процессе автоматического обнаружения vg, путем добавления lvm тега с желаемым именем ресурса).

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

###### Настройка модуля `sds-local-volume`

* Создаем ресурс `LocalStorageClass` и добавляем в него ресурсы `LVMVolumeGroup` для использования только vg `ssd-sata-safe` на всех наших узлах в модуле `sds-local-volume`:

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

* Создаем ресурс `ReplicatedStoragePool` с именем data-ssd-sata-safe и добавляем в него ресурсы `LVMVolumeGroup` для использования только vg `ssd-sata-safe` на всех наших узлах в модуле `sds-replicated-volume` в `ReplicatedStorageClass` с replication: `None`:

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

* Создаем ресурс `ReplicatedStoragePool` с именем data-ssd-sata-unsafe и добавляем в него ресурсы `LVMVolumeGroup` для использования только vg `ssd-sata-unsafe` на всех наших узлах в модуле `sds-replicated-volume` в `ReplicatedStorageClass` с replication `Availability` или `ConsistencyAndAvailability`:

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

* Создаем ресурсы `ReplicatedStorageClass` и указываем в поле `storagePool` имя созданных ранее ресурсов `ReplicatedStoragePool` для использования vg `ssd-sata-safe` и `ssd-sata-unsafe` на всех наших узлах:

```yaml
kubectl apply -f -<<EOF
---
apiVersion: storage.deckhouse.io/v1alpha1
kind: ReplicatedStorageClass
metadata:
  name: replicated-sc-ssd-sata-r1
spec:
  storagePool: data-ssd-sata-safe # обратите внимание, что мы используем data-ssd-sata-safe для этого ресурса, так как в нем replication: None и репликация данных НЕ будет производиться для PV, созданных с этим StorageClass
  replication: None
  reclaimPolicy: Delete
  topology: Ignored # - если указываем такую топологию, то в кластере не должно быть зон (узлов с метками topology.kubernetes.io/zone).

---
apiVersion: storage.deckhouse.io/v1alpha1
kind: ReplicatedStorageClass
metadata:
  name: replicated-sc-ssd-sata-r2
spec:
  storagePool: data-ssd-sata-unsafe # обратите внимание, что мы используем data-ssd-sata-unsafe для этого ресурса, так как в нем replication: Availability и репликация данных будет производиться для PV, созданных с этим StorageClass
  replication: Availability
  reclaimPolicy: Delete
  topology: Ignored # - если указываем такую топологию, то в кластере не должно быть зон (узлов с метками topology.kubernetes.io/zone).

---
apiVersion: storage.deckhouse.io/v1alpha1
kind: ReplicatedStorageClass
metadata:
  name: replicated-sc-ssd-sata-r3
spec:
  storagePool: data-ssd-sata-unsafe # обратите внимание, что мы используем data-ssd-sata-unsafe для этого ресурса, так как в нем replication: ConsistencyAndAvailability и репликация данных будет производиться для PV, созданных с этим StorageClass
  replication: ConsistencyAndAvailability
  reclaimPolicy: Delete
  topology: Ignored # - если указываем такую топологию, то в кластере не должно быть зон (узлов с метками topology.kubernetes.io/zone).
EOF
```

### Дополнительные диски - HDD

Рекомендуется использовать HDD для создания томов, которые не требуют производительности.

#### Рекомендованный сценарий

* Создать vg с именем `hdd` из всех HDD дисков.
* Поставить тег `storage.deckhouse.io/enabled=true` на vg `hdd` такой командой:

```shell
vgchange hdd --addtag storage.deckhouse.io/enabled=true
```

##### Плюсы и минусы сценария

| Плюсы                     | Минусы      |
|---------------------------|-------------|
| Надежно  | Overhead по месту на диске для SDS, которые сами реплицируют данные  |
| Просто в настройке и использовании |  |
| Удобно распределять (и перераспределять) место между разными SDS |  |

##### Примеры настройки модулей SDS при использовании рекомендованного сценария

Допустим, что у Вас есть три узла, настроенных по рекомендованному сценарию. В таком случае в кластере Deckhouse появится три ресурса `LVMVolumeGroup` со случайно сгенерированными именами (в будущем добавится возможность указывать имя для ресурсов `LVMVolumeGroup`, которые создаются в процессе автоматического обнаружения vg, путем добавления lvm тега с желаемым именем ресурса).

```shell
kubectl get lvmvolumegroups.storage.deckhouse.io

NAME                                      THINPOOLS   CONFIGURATION APPLIED   PHASE   NODE       SIZE      ALLOCATED SIZE   VG    AGE
vg-08d3730c-9201-428d-966c-45795cba55a6   0/0         True                    Ready   worker-2   25596Mi   0                hdd   61s
vg-b59ff9e1-6ef2-4761-b5d2-6172926d4f4d   0/0         True                    Ready   worker-0   25596Mi   0                hdd   4m17s
vg-c7863e12-c143-42bb-8e33-d578ce50d6c7   0/0         True                    Ready   worker-1   25596Mi   0                hdd   108s
```

###### Настройка модуля `sds-local-volume`

* Создаем ресурс `LocalStorageClass` и добавляем в него все наши ресурсы `LVMVolumeGroup` для использования vg `hdd` на всех наших узлах в модуле `sds-local-volume`:

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

* Создаем ресурс `ReplicatedStoragePool` и добавляем в него все наши ресурсы `LVMVolumeGroup` для использования vg `hdd` на всех наших узлах в модуле `sds-replicated-volume`:

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

* Создаем ресурсы `ReplicatedStorageClass` и указываем в поле `storagePool` имя созданного ранее ресурса `ReplicatedStoragePool`:

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
  topology: Ignored # - если указываем такую топологию, то в кластере не должно быть зон (узлов с метками topology.kubernetes.io/zone).

---
apiVersion: storage.deckhouse.io/v1alpha1
kind: ReplicatedStorageClass
metadata:
  name: replicated-sc-hdd-r2
spec:
  storagePool: data-hdd
  replication: Availability
  reclaimPolicy: Delete
  topology: Ignored # - если указываем такую топологию, то в кластере не должно быть зон (узлов с метками topology.kubernetes.io/zone).

---
apiVersion: storage.deckhouse.io/v1alpha1
kind: ReplicatedStorageClass
metadata:
  name: replicated-sc-hdd-r3
spec:
  storagePool: data-hdd
  replication: ConsistencyAndAvailability
  reclaimPolicy: Delete
  topology: Ignored # - если указываем такую топологию, то в кластере не должно быть зон (узлов с метками topology.kubernetes.io/zone).
EOF
```

#### Гибридный сценарий с HDD

{{< alert level="warning" >}}
ВАЖНО! Не поддерживается использование разделов с одинаковыми part UUID, а так же изменение part UUID раздела, который используется для создания vg. При создании таблицы разделов рекомендуется выбрать GPT, так как part UUID в MBR является псевдослучайным и содержит в себе номер раздела. Помимо этого, в MBR нельзя задать partlabel, который может быть удобным для последующей идентификации раздела в Deckhouse.
{{< /alert >}}

В этом сценарии используются два раздела на каждом диске: один для хранения данных SDS, которые не реплицируются, другой для данных SDS, которые реплицируются. Первый раздел каждого диска используется для создания зеркала, а второй раздел для создания отдельной vg без зеркалирования. Таким образом, место на диске используется максимально эффективно.

* Создать по 2 раздела на каждом диске.
* Собрать зеркало из первых разделов на каждом диске.
* Создать vg с именем `hdd-safe` на зеркале.
* Создать vg с именем `hdd-unsafe` из вторых разделов каждого диска.
* Поставить тег `storage.deckhouse.io/enabled=true` на vg `hdd-safe` и `hdd-unsafe` такой командой:

```shell
vgchange hdd-safe --addtag storage.deckhouse.io/enabled=true
vgchange hdd-unsafe --addtag storage.deckhouse.io/enabled=true
```

##### Плюсы и минусы сценария

| Плюсы                     | Минусы      |
|---------------------------|-------------|
| Надежно  | Сложно в настройке и использовании  |
| Место используется максимально эффективно | Очень сложно перераспределять место между safe и unsafe разделами |

##### Примеры настройки модулей SDS при использовании гибридного сценария с HDD

Допустим, что у Вас есть три узла, настроенных по гибридному сценарию с HDD. В таком случае в кластере Deckhouse появятся шесть ресурсов `LVMVolumeGroup` со случайно сгенерированными именами (в будущем добавится возможность указывать имя для ресурсов `LVMVolumeGroup`, которые создаются в процессе автоматического обнаружения vg, путем добавления lvm тега с желаемым именем ресурса).

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

###### Настройка модуля `sds-local-volume`

* Создаем ресурс `LocalStorageClass` и добавляем в него ресурсы `LVMVolumeGroup` для использования только vg `hdd-safe` на всех наших узлах в модуле `sds-local-volume`:

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

* Создаем ресурс `ReplicatedStoragePool` с именем data-hdd-safe и добавляем в него ресурсы `LVMVolumeGroup` для использования только vg `hdd-safe` на всех наших узлах в модуле `sds-replicated-volume` в `ReplicatedStorageClass` с replication: `None`:

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

* Создаем ресурс `ReplicatedStoragePool` с именем data-hdd-unsafe и добавляем в него ресурсы `LVMVolumeGroup` для использования только vg `hdd-unsafe` на всех наших узлах в модуле `sds-replicated-volume` в `ReplicatedStorageClass` с replication `Availability` или `ConsistencyAndAvailability`:

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

* Создаем ресурсы `ReplicatedStorageClass` и указываем в поле `storagePool` имя созданных ранее ресурсов `ReplicatedStoragePool` для использования vg `hdd-safe` и `hdd-unsafe` на всех наших узлах:

```yaml
kubectl apply -f -<<EOF
---
apiVersion: storage.deckhouse.io/v1alpha1
kind: ReplicatedStorageClass
metadata:
  name: replicated-sc-hdd-r1
spec:
  storagePool: data-hdd-safe # обратите внимание, что мы используем data-hdd-safe для этого ресурса, так как в нем replication: None и репликация данных НЕ будет производиться для PV, созданных с этим StorageClass
  replication: None
  reclaimPolicy: Delete
  topology: Ignored # - если указываем такую топологию, то в кластере не должно быть зон (узлов с метками topology.kubernetes.io/zone).

---
apiVersion: storage.deckhouse.io/v1alpha1
kind: ReplicatedStorageClass
metadata:
  name: replicated-sc-hdd-r2
spec:
  storagePool: data-hdd-unsafe # обратите внимание, что мы используем data-hdd-unsafe для этого ресурса, так как в нем replication: Availability и репликация данных будет производиться для PV, созданных с этим StorageClass
  replication: Availability
  reclaimPolicy: Delete
  topology: Ignored # - если указываем такую топологию, то в кластере не должно быть зон (узлов с метками topology.kubernetes.io/zone).

---
apiVersion: storage.deckhouse.io/v1alpha1
kind: ReplicatedStorageClass
metadata:
  name: replicated-sc-hdd-r3
spec:
  storagePool: data-hdd-unsafe # обратите внимание, что мы используем data-hdd-unsafe для этого ресурса, так как в нем replication: ConsistencyAndAvailability и репликация данных будет производиться для PV, созданных с этим StorageClass
  replication: ConsistencyAndAvailability
  reclaimPolicy: Delete
  topology: Ignored # - если указываем такую топологию, то в кластере не должно быть зон (узлов с метками topology.kubernetes.io/zone).
EOF
```
