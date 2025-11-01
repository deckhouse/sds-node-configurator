---
title: "Модуль sds-node-configurator: FAQ"
description: "Модуль sds-node-configurator: частые вопросы и ответы."
---
{{< alert level="warning" >}}
Работоспособность модуля гарантируется только при использовании стоковых ядер, поставляемых вместе с [поддерживаемыми дистрибутивами](/products/kubernetes-platform/documentation/v1/reference/supported_versions.html#linux).

Работоспособность модуля при использовании других ядер или дистрибутивов возможна, но не гарантируется.
{{< /alert >}}

## Ресурсы BlockDevice и LVMVolumeGroup не создаются в кластере

- Ресурсы [BlockDevice](./cr.html#blockdevice) могут не создаваться, если устройства не проходят фильтрацию контроллера. Убедитесь, что устройства соответствуют [требованиям](./resources.html#требования-контроллера-к-устройству).

- Ресурсы [LVMVolumeGroup](./cr.html#lvmvolumegroup) могут не создаваться из-за отсутствия в кластере ресурсов [BlockDevice](./cr.html#blockdevice), так как их имена используются в спецификации [LVMVolumeGroup](./cr.html#lvmvolumegroup).

- Если ресурсы [BlockDevice](./cr.html#blockdevice) существуют, а ресурсы [LVMVolumeGroup](./cr.html#lvmvolumegroup) отсутствуют, убедитесь, что у существующих групп томов LVM на узле имеется LVM-тег `storage.deckhouse.io/enabled=true`.

## Ресурс LVMVolumeGroup и группа томов остались после попытки удаления

Такая ситуация возможна в двух случаях:

1. В группе томов имеются логические тома (Logical Volume).

   Контроллер не отвечает за удаление логических томов с узла. Если в созданной с помощью ресурса группе томов имеются логические тома, удалите их вручную на узле. После этого ресурс и группа томов вместе с физическими томами (Physical Volume) будут удалены автоматически.

1. На ресурсе имеется аннотация `storage.deckhouse.io/deletion-protection`.

   Данная аннотация защищает от удаления ресурс и созданную им группу томов. Удалите аннотацию, выполнив команду:

   ```shell
   d8 k annotate lvg %lvg-name% storage.deckhouse.io/deletion-protection-
   ```

   После выполнения команды ресурс и группа томов будут удалены автоматически.

## Не удаётся создать группу томов с помощью ресурса LVMVolumeGroup

Ресурс не проходит валидацию контроллера (валидация Kubernetes прошла успешно). Причину можно увидеть в поле `status.message` ресурса или в логах контроллера.

Чаще всего проблема связана с некорректно указанными ресурсами [BlockDevice](./cr.html#blockdevice). Убедитесь, что выбранные ресурсы удовлетворяют следующим требованиям:

- поле `status.consumable` имеет значение `true`;
- для групп томов типа `Local` указанные ресурсы [BlockDevice](./cr.html#blockdevice) принадлежат одному узлу;<!-- > - Для групп томов типа `Shared` указан единственный ресурс [BlockDevice](./cr.html#blockdevice). -->
- указаны актуальные имена ресурсов [BlockDevice](./cr.html#blockdevice).

Полный список ожидаемых значений доступен в описании ресурса [LVMVolumeGroup](./cr.html#lvmvolumegroup).

## Отключение устройства в группе томов

Ресурс [LVMVolumeGroup](./cr.html#lvmvolumegroup) существует до тех пор, пока существует соответствующая группа томов. Пока существует хотя бы одно устройство, группа томов сохраняется, но помечается как неработоспособная. Текущее состояние отражается в поле `status` ресурса.

После восстановления отключённого устройства на узле группа томов LVM восстановит работоспособность, а соответствующий ресурс [LVMVolumeGroup](./cr.html#lvmvolumegroup) отобразит актуальное состояние.

## Передача управления существующей группой томов LVM контроллеру

Добавьте LVM-тег `storage.deckhouse.io/enabled=true` на группу томов LVM на узле:

```shell
vgchange myvg-0 --addtag storage.deckhouse.io/enabled=true
```

## Остановка отслеживания группы томов LVM контроллером

Удалите LVM-тег `storage.deckhouse.io/enabled=true` у нужной группы томов LVM на узле:

```shell
vgchange myvg-0 --deltag storage.deckhouse.io/enabled=true
```

После этого контроллер перестанет отслеживать выбранную группу томов и самостоятельно удалит связанный с ней ресурс [LVMVolumeGroup](./cr.html#lvmvolumegroup).

## LVM-тег `storage.deckhouse.io/enabled=true` появился автоматически

LVM-тег появляется в следующих случаях:

- Группа томов LVM создана через ресурс [LVMVolumeGroup](./cr.html#lvmvolumegroup). В этом случае контроллер автоматически добавляет LVM-тег `storage.deckhouse.io/enabled=true` на созданную группу томов LVM.
- На группе томов или её thin pool был LVM-тег модуля `linstor` — `linstor-*`.

При миграции со встроенного модуля `linstor` на модули `sds-node-configurator` и `sds-replicated-volume` LVM-теги `linstor-*` автоматически заменяются на `storage.deckhouse.io/enabled=true` в группах томов. Управление этими группами томов передаётся модулю `sds-node-configurator`.

## Создание LVMVolumeGroup с помощью ресурса LVMVolumeGroupSet

Для создания ресурсов [LVMVolumeGroup](./cr.html#lvmvolumegroup) с помощью [LVMVolumeGroupSet](./cr.html#lvmvolumegroupset) укажите в спецификации [LVMVolumeGroupSet](./cr.html#lvmvolumegroupset) селекторы для узлов и шаблон для создаваемых ресурсов [LVMVolumeGroup](./cr.html#lvmvolumegroup).

Поддерживается только стратегия `PerNode`: контроллер создаёт по одному ресурсу [LVMVolumeGroup](./cr.html#lvmvolumegroup) из шаблона для каждого узла, соответствующего селектору.

Пример спецификации [LVMVolumeGroupSet](./cr.html#lvmvolumegroupset):

```yaml
apiVersion: storage.deckhouse.io/v1alpha1
kind: LVMVolumeGroupSet
metadata:
  name: my-lvm-volume-group-set
  labels:
    my-label: my-value
spec:
  strategy: PerNode
  nodeSelector:
    matchLabels:
      node-role.kubernetes.io/worker: ""
  lvmVolumeGroupTemplate:
    metadata:
      labels:
        my-label-for-lvg: my-value-for-lvg
    type: Local
    blockDeviceSelector:
      matchLabels:
        status.blockdevice.storage.deckhouse.io/model: <model>
    actualVGNameOnTheNode: <actual-vg-name-on-the-node>
```

## Изменение UUID у групп томов при клонировании виртуальных машин

UUID у групп томов можно изменить только при отсутствии активных логических томов в группе томов.

Если в группе томов есть активные логические тома, выполните следующие действия:

1. Отмонтируйте логический том, выполнив команду:

   ```shell
   umount /mount/point
   ```

1. Деактивируйте логический том или группу томов, выполнив команду:

    - Для деактивации конкретного логического тома выполните команду, изменив `<LV_NAME>` на имя логического тома:

      ```shell
      lvchange -an <LV_NAME>
      ```

    - Для деактивации всех логических томов в группе выполните команду, изменив `<VG_NAME>` на имя группы томов:

      ```shell
      lvchange -an <VG_NAME>
      ```

1. После деактивации всех логических томов измените UUID у групп томов, выполнив команду:

   ```shell
   vgchange -u <VG_NAME>
   ```

   Команда сгенерирует новые UUID для указанной группы томов. Для изменения UUID всех групп томов на виртуальной машине выполните:

   ```shell
   vgchange -u
   ```

При необходимости команду можно добавить в скрипт `cloud-init` для автоматического выполнения при создании виртуальных машин.

## Лейблы, добавляемые контроллером на ресурсы BlockDevice

- `status.blockdevice.storage.deckhouse.io/type` — тип LVM.
- `status.blockdevice.storage.deckhouse.io/fstype` — тип файловой системы.
- `status.blockdevice.storage.deckhouse.io/pvuuid` — UUID физического тома.
- `status.blockdevice.storage.deckhouse.io/vguuid` — UUID группы томов.
- `status.blockdevice.storage.deckhouse.io/partuuid` — UUID раздела.
- `status.blockdevice.storage.deckhouse.io/lvmvolumegroupname` — имя ресурса [LVMVolumeGroup](./cr.html#lvmvolumegroup), к которому относится устройство.
- `status.blockdevice.storage.deckhouse.io/actualvgnameonthenode` — имя группы томов на узле.
- `status.blockdevice.storage.deckhouse.io/wwn` — идентификатор WWN (World Wide Name) для устройства.
- `status.blockdevice.storage.deckhouse.io/serial` — серийный номер устройства.
- `status.blockdevice.storage.deckhouse.io/size` — размер устройства.
- `status.blockdevice.storage.deckhouse.io/model` — модель устройства.
- `status.blockdevice.storage.deckhouse.io/rota` — указывает, является ли устройство ротационным.
- `status.blockdevice.storage.deckhouse.io/hotplug` — указывает возможность горячей замены устройства (HotPlug).
- `status.blockdevice.storage.deckhouse.io/machineid` — идентификатор сервера, на котором установлено блочное устройство.
