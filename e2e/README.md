# E2E-тесты для sds-node-configurator

End-to-end (e2e) тесты модуля `sds-node-configurator`: обнаружение и стабильность
`BlockDevice`, жизненный цикл `LVMVolumeGroup` (в т.ч. thin-pool), рестарт
контроллера, netlink-дискавери, scheduler-extender и стресс-сценарий
(максимум VG на ноду).

Тесты написаны на [Ginkgo](https://onsi.github.io/ginkgo/) / [Gomega](https://onsi.github.io/gomega/)
и подключаются к **уже поднятому** кластеру через SDK
[storage-e2e](https://github.com/deckhouse/storage-e2e) (`e2e.Connect`).
Провижн кластера сюда **не входит** — им занимается CI (reusable workflow
storage-e2e) либо вы указываете существующий кластер локально.

> Подробное руководство (CI, локальный запуск, полная таблица переменных
> окружения, troubleshooting, написание нового теста) — в [`E2E_USAGE.md`](E2E_USAGE.md).

## Краткое описание

- Сьют подключается к готовому кластеру и **не бутстрапит** его.
- Единственная точка входа `go test` — `TestSdsNodeConfigurator`
  (`tests/sds_node_configurator_suite_test.go`).
- Запуск **строго последовательный** (serial-only): `ginkgo -p` / `--procs` и
  `go test -parallel` для спеков **запрещены** (спеки делят один кластер, ноду и
  ref-counted cluster-lease внутри `e2e.Connect`).
- Каждый спека-файл сам вызывает `e2e.Connect` в `BeforeAll` и регистрирует
  `DeferCleanup(cl.Close)`.

## Предварительные требования

1. **Go 1.26.5** (см. `go.mod`; родительский `go.work` в корне репозитория
   перехватывает сборку — запускайте из `e2e/` с `GOWORK=off`).
2. Уже поднятый тестовый кластер с установленным модулем `sds-node-configurator`
   (для scheduler-extender также `sds-local-volume`), к которому SDK может
   подключиться (провайдер `dvp`).
3. Доступ к базовому кластеру виртуализации через SSH + kubeconfig — задаётся
   переменными окружения `E2E_DVP_BASE_CLUSTER_*` (см. `E2E_USAGE.md`).
4. `kubectl` — для запуска в кластере через Job и для отладки.
5. Опционально: `ginkgo` CLI (`make install-ginkgo`) для точечного запуска.

## Структура пакета

```
e2e/
├── Makefile                     # цели: test, test-go, test-stress, test-focus,
│                                #   deps, install-ginkgo, clean, lint, check-env, ...
├── README.md                    # этот файл
├── E2E_USAGE.md                 # детальное руководство (EN)
├── go.mod / go.sum
├── cfg/                         # конфиг сьюта из env (stateless)
│   ├── config.go                #   Config + Load() -> (*Config, error)
│   ├── stress.go                #   Stress + LoadStress() (лениво, только stress)
│   └── config_test.go
├── framework/                   # stateless, assertion-free хелперы (без Ginkgo)
│   ├── poll.go                  #   Poll
│   ├── exec.go                  #   NodeExecChecked
│   ├── parse.go                 #   ParseLsblk / LsblkLine
│   ├── blockdevice.go           #   BlockDeviceName, WaitNewConsumableBlockDevice,
│   │                            #     TriggerLVMDiscovery
│   └── lvm.go                   #   PVNamesInListing, CountPVsInVG, VGInListing,
│                                #     ThinPoolDataLVPresent, RemoveThinPoolStackScript
├── sdsclient/                   # sdsclient.New(*rest.Config) -> client.Client
│                                #   (приватный scheme: client-go + v1alpha1)
└── tests/                       # спеки + Ginkgo-coupled хелперы (package tests)
    ├── sds_node_configurator_suite_test.go   # TestSdsNodeConfigurator
    ├── *_test.go                             # доменные спеки
    ├── helpers_*_test.go                     # оркестрация/cleanup
    ├── cluster_config.yml                    # описание кластера (провижн)
    └── cluster_config.ci.yml                 # то же для CI (PR-образ модуля)
```

## Быстрый старт (локально)

Конфиг с секретами держите **вне git** (например, в `e2e/config/`, добавьте в
`.gitignore`) и сделайте `source` перед запуском. Минимально нужны переменные
подключения SDK (`E2E_TEST_CLUSTER_PROVIDER`, `E2E_CLUSTER_CONFIG_YAML_PATH`) и
набор `E2E_DVP_BASE_CLUSTER_*` (SSH-доступ, kubeconfig, storage class). Полный
список — в [`E2E_USAGE.md`](E2E_USAGE.md).

```bash
source <ваш git-ignored файл с export ...>   # экспорт переменных окружения
cd e2e
make deps        # go mod download/tidy + fix-mod-permissions
make test        # смоук (label-filter !stress-test), как в CI
```

Эквивалент напрямую через `go test`:

```bash
cd e2e
GOWORK=off go test -v -count=1 -timeout 90m ./tests/ \
  -run '^TestSdsNodeConfigurator$' -ginkgo.label-filter='!stress-test'
```

Точечные запуски:

```bash
make test-focus FOCUS='^TestSdsNodeConfigurator$'   # по имени go-теста
make test-stress                                    # только стресс (label stress-test)
```

Сьют не бутстрапит кластер сам — он подключается к уже поднятому кластеру через
SDK `e2e.Connect`. В CI провижн/подключение делает reusable-workflow storage-e2e
(см. `E2E_USAGE.md`).

## Лейблы и фильтры

Спеки размечены Ginkgo-лейблами; фильтр по умолчанию в `Makefile` —
`GINKGO_LABEL_FILTER ?= !stress-test` (стресс исключён из смоука; совпадает с
дефолтом reusable-workflow storage-e2e).

Реальные лейблы из кода `tests/*.go`:

| Лейбл | Где применяется |
|-------|-----------------|
| `sds-node-configurator` | почти все спеки модуля |
| `block-device`, `discovery` | обнаружение BlockDevice |
| `device-types` | матрица типов устройств (disk/mpath/crypt/loop/LUKS) + LVG; только этот спек |
| `block-device-stable` | стабильность BlockDevice по стадиям |
| `netlink-discovery` | netlink-дискавери |
| `lvmvolumegroup` | сценарии LVMVolumeGroup (в т.ч. thin-pool) |
| `file-devices` | LVMVolumeGroup поверх файлов (`spec.fileDevices`), см. ниже |
| `node-reboot` | перезагрузка ноды (входит в `file-devices`; исключается `!node-reboot`) |
| `needs-disks` | спеку нужен настоящий диск (create/attach/detach) — см. ниже |
| `controller-restart` | устойчивость к рестарту контроллера |
| `schedule-extender` | scheduler-extender: размещение и отказ |
| `sched-steer-spec`, `sched-steer-annotation` | увод пода на ноду с местом (PVC из `spec.volumes` / из аннотации) |
| `sched-block-spec`, `sched-block-annotation` | отказ в размещении, когда места нет нигде |
| `regress` | вложенный регресс-кейс block-device-stable |
| `stress-test` | стресс: максимум независимых VG на ноду (исключён по умолчанию) |

### Два провайдера в CI

Сьют делится по тому, что спеку нужно от инфраструктуры, а не по тому, что он
проверяет:

| задание | провайдер | метка PR | фильтр | спеков |
|---|---|---|---|---|
| `e2e (dvp, block devices)` | `dvp` | `e2e/run`, `e2e/dvp/run` | `!stress-test && (!file-devices \|\| needs-disks)` | 40 |
| `e2e (commander, file devices)` | `commander` | `e2e/commander/run` | `!stress-test && file-devices && !needs-disks` | 41 |

Фильтры комплементарны: каждый спек выполняется ровно один раз (40 + 41 = 81 —
весь сьют без стресса).

Метка `e2e/run` намеренно **не** запускает commander-задание: тому нужны свои
учётные данные и шаблон кластера, и до их появления такой прогон падал бы на
bootstrap. Когда Commander будет настроен, имеет смысл сделать `e2e/run`
запускающей оба.

Метка `e2e/label:<x>` целиком заменяет фильтр задания (так устроен resolve-шаг
reusable-workflow) и тем самым отменяет это разделение — полезно как аварийный
выход, неожиданно, если поставить её не подумав.

Причина деления в SDK: провайдер `commander` выдаёт кластер, но не инфраструктуру
под ним, поэтому его `DiskManager` отсутствует и любая операция с диском
возвращает `ErrDisksUnsupported`. Выполнение команд на узлах по SSH он при этом
поддерживает — а `spec.fileDevices` ровно для узлов без свободного диска и
придуман, так что файловым спекам диски не нужны.

Единственный файловый спек, которому диск нужен (смешанный block+file), помечен
`needs-disks` и поэтому остаётся на `dvp`. Если он всё же окажется на провайдере
без дисков, `fdCreateDiskOrSkip` пропустит его с внятным сообщением вместо
падения.

```bash
# подмножество:
make test-go GINKGO_LABEL_FILTER='discovery || block-device'
# только матрица типов устройств:
make test-device-types
#   == go test ... -ginkgo.label-filter=device-types
# только стресс:
make test-stress
# всё вместе:
make test-go GINKGO_LABEL_FILTER=''
```

## LVMVolumeGroup на файлах (`spec.fileDevices`)

Лейбл `file-devices`, 25 спек в шести файлах; общие хелперы — `tests/helpers_filedevices_test.go`.
Кроме смешанного сценария отдельный диск не нужен нигде — включая сценарии с PVC,
где том нарезается из того же file-backed VG: агент создаёт
backing-файл (файл-подложку, поверх которой делается PV) в базовом каталоге
`/opt/deckhouse/sds/file-devices`, подключает его как loop-устройство и делает `pvcreate`.
Нода выбирается автоматически — первая с Ready-подом агента, worker'ы в приоритете.

> Все проверки на ноде, которые дёргают LVM, обязаны передавать тот же `--config`, что
> и агент (`fdLVMCfg` в `helpers_filedevices_test.go`). Модуль через NodeGroupConfiguration
> прописывает в `/etc/lvm/lvm.conf` фильтр `["r|^/dev/loop[0-9]+|"]`, поэтому host-wide LVM
> **не видит loop-устройств**: обычный `vgs` отрапортует рабочий VG как отсутствующий, а
> обычный `lvremove` молча не снесёт thin-pool, и LVG залипнет в `Terminating`.

```bash
make test-go GINKGO_LABEL_FILTER='file-devices'
```

### `lvmvolumegroup_filedevices_test.go` — жизненный цикл

| Спека | Что проверяется |
|-------|-----------------|
| Создание и удаление | file-only VG (`1Gi`) → `Ready`; в `status.nodes[].fileDevices` — `filePath` под базовым каталогом, `loopDevice` `/dev/loop*`, `pvUUID`; на ноде ровно один loop с `DIO=1`, VG с тегами `storage.deckhouse.io/enabled=true` и `.../lvmVolumeGroupName=<lvg>`. После удаления файл убран, loop отсоединён |
| Thin-pool на файле | file-only VG (`2Gi`) + thin-pool `50%` → `Ready`, data-LV thin-pool'а присутствует на ноде |
| Расширение | добавление второй записи `fileDevices` → две записи в статусе, `vgSize` растёт, оба PV на ноде — `/dev/loop*` |
| Reattach при потере loop | фаза 1: обычный рестарт агента — loop переживает под, ничего не меняется и второй loop не появляется; фаза 2: `losetup -d` + `vgchange -an` (имитация ребута) → рестарт агента → VG снова `Ready`, тот же `filePath`, новый loop, без дублей файла и loop'а. Настоящий ребут — в `..._node_test.go` |
| Идемпотентность | три реконсайла подряд (правка label + рестарт агента) → один файл, один loop, один PV, `vgSize` не дрейфует |
| Смешанный VG (блочка + файл) | LVG с `blockDeviceSelector` **и** `spec.fileDevices` → `Ready`; в статусе одновременно `devices` и `fileDevices`, на ноде ≥2 PV (один `/dev/loop*`, один блочный); после удаления файл убран |

### `..._validation_test.go` — отказы

CEL-правила CRD (проверяются только на живом apiserver, unit-тестами недостижимы):
отсутствие и `blockDeviceSelector`, и `fileDevices`; правка `directory` или `size`
существующей записи; удаление записи (сжатие VG не поддерживается).

Валидация агента (`VGConfigurationApplied=False`, reason `ValidationFailed`, и на ноде
ничего не создано): `directory` вне базового каталога (allowlist — белый список
разрешённых путей); `size: 1G` — CRD-паттерн такое пропускает, агент отбивает по минимуму
`1Gi`; две записи с одинаковыми `directory`+`size` (коллизия имени файла); относительный
путь; запрос больше, чем `statfs` показывает свободного места (гард перед `fallocate`,
он же единственный дешёвый способ прогнать rollback).

### `..._foreign_test.go` — изоляция чужих loop-устройств

Ради `spec.fileDevices` агент перестал резать loop-PV в LVM-скане, поэтому здесь
фиксируется новая граница: чужой untagged loop-VG на ноде не усыновляется (нет ни
`BlockDevice`, ни `LVMVolumeGroup`) и не ломается; чужой файл с именем-подделкой
`sds-<чужой-lvg>.<имя-записи>.img` в базовом каталоге не заявляется как свой и не удаляется;
удаление своего LVG не трогает чужой файл и loop в том же каталоге.

Отдельная группа спек — про **tagged** чужой loop-VG. Это и вероятнее, и опаснее:
образ диска узла, которым модуль когда-то управлял, несёт
`storage.deckhouse.io/enabled=true`, поэтому `losetup -f /backup/node2-root.img` при
восстановлении уже достаточно, чтобы такая группа появилась. Владение решается не тегом,
а именем файла-подложки (`utils.ClassifyLoopVGs`), и спеки проверяют все четыре
последствия: одноимённая чужая группа не роняет живой LVG в `Multiple LVM VGs share the
name` и не попадает в его `status.nodes[].fileDevices`; `ReTag` не заменяет чужой
legacy-тег `linstor-*` на управляемый (после такой замены дискаверер усыновил бы группу, а
тега, который её опознавал, уже не осталось бы); `vgchange -ay` не поднимает на хосте
логические томá гостя — ни при старте агента, ни при последующих наполнениях кэша; и
чужая группа с тем же именем не подменяет диагноз на `CacheStale`, из которого нет выхода,
а даёт настоящий `VGCreationFailed`.

### `lvmvolumegroup_nested_lvm_test.go` — вложенный LVM гостевых VM

Про то же самое, что и `..._foreign_test.go`, но с другой стороны: усыновление чужой
группы там уже покрыто, а здесь чужая группа не усыновляется **и всё равно** выводит из
строя своё хранилище узла. Для гипервизора это не краевой случай, а норма: block-mode PVC,
отданный виртуальной машине, виден на хосте как `/dev/loopN`, и LVM гостя внутри него
видят команды агента — потому что агент переопределяет `global_filter` из
`NodeGroupConfiguration` модуля на каждом вызове.

Первая спека строит LVG **на реальном диске** — это форма, которая есть на каждом узле, и
именно на ней случился инцидент; остальные берут в качестве «соседа» file-backed LVG.
Все запускаются по метке `nested-lvm` и проверяют:

| Спека | Что воспроизводит |
|---|---|
| гостевая VG с именем нашей | LVG остаётся `Ready`, скан продолжает работать, в логе агента нет ошибок разбора отчёта LVM |
| две **гостевые** VG с одинаковым именем между собой | к нашему LVG это отношения не имеет: ни `VGReady=False/ScanFailed`, ни текста `is used by VGs` в его condition'ах. Предусловие проверяется отдельно — lvm печатает предупреждение в stderr даже на запрос о нашей VG. Сосед здесь file-backed: третий подряд create/attach/delete реального диска на одном узле в пределах одного `Ordered` — это место, где в этом сьюте перестаёт успевать линковка BlockDevice, и спека начинала докладывать про свою фикстуру вместо коллизии |
| `/etc/lvm/archive` из 9000 записей + дубль имени | lvm начинает мешать `Consider pruning ... VG archive` **в середину** своего же JSON-отчёта; агент всё равно разбирает отчёт, кэш наполняется, `CacheEmpty` не появляется. Если lvm узла такую строку не печатает, спека честно делает `Skip` (гарантия на парсер — в unit-тесте `TestReportSurvivesLVMAdvisoriesOnStdout`) |
| свой file-backed LVG **и** гостевой loop-VG на одном узле | пара, которую не пройти ни «глухим» фильтром, ни открытым: свой LVG остаётся Ready и переживает рестарт агента (значит свои loop-устройства видны), а гостевая VG после рестарта не пере-тегирована, её LV не активирован и группа не усыновлена (значит чужие — нет). «Не видит» проверяется через код, который проходит по всему, что агент видит: `ReTag` и `ActivateAllManagedVGs` |

Сам фильтр в e2e не проверяется: модуль e2e не может импортировать константу агента, а
проверять её копию — значит проверять копию. Инвариант «loop всегда отвергается, кроме
явно своих устройств» зафиксирован unit-тестом `TestLVMFilterAlwaysRejectsUnownedLoopDevices`.
Обратите внимание: `fdLVMConfig` в хелперах — **не** фильтр агента, спекам loop-устройства
видны специально, иначе им нечем строить фикстуры.

### `lvmvolumegroup_ownership_test.go` — кому принадлежит VG

Про то, что имя VG не является идентификатором, со стороны последствий. VG здесь
создаются **на реальных дисках**, а не на loop: с вернувшимся правилом `loop` в
фильтре чужая loop-VG агенту не видна и ничего из этого проверить не может.

| Спека | Что воспроизводит |
|---|---|
| мусорный CR над VG, тег которой называет другого владельца, и с `blockDeviceSelector`, не выбирающим ничего | ресурс удаляется до конца (финализатор снят, а не вечный Terminating — валидация spec больше не блокирует удаление), при этом VG остаётся на узле с тем же UUID и тем же тегом |
| две VG с одним и тем же тегом владельца | вторая **не** импортируется под сгенерированным именем: число LVMVolumeGroup не растёт две минуты, ни один ресурс не создан на эту VG, а в логе агента есть `not importing VG` с именем владельца |

Первая спека покрывает сразу две правки, и это не экономия: ресурс, который нельзя
было удалить, и ресурс, чьё удаление снесло бы чужое хранилище, — это один и тот же
ресурс, ровно в том состоянии, в котором на живом кластере нашлись сотни таких.

### `..._recovery_test.go` — состояния, в которых узел может оказаться

Не то, что оставляет за собой happy path, и там, где ошибка восстановления стоит данных,
а не condition'а.

| Сценарий | Что проверяется |
|---|---|
| Backing-файл удалён при живом loop | `losetup -j` ищет по inode, поэтому после `rm` запись выглядит непровижиненной, а loop продолжает отдавать живой PV. Агент обязан сообщить `FileDeviceNotApplied` с упоминанием `pvmove` и **не** создавать второй файл по тому же пути: считается число loop'ов по basename (`losetup -a`), PV в VG остаётся один, LVG остаётся `Ready`. На удалении осиротевший loop должен быть отцеплен — иначе minor переживёт единственную запись о себе |
| `directory` через симлинк | `status...filePath` приходит от `losetup` с раскрытыми симлинками, а spec хранит буквальный путь. Одно устройство не должно превратиться в два: одна запись в статусе, один PV, thin-pool создаётся и `Ready` не мигает; на удалении файл исчезает по обоим написаниям пути |
| loop, который уже PV | Ровно то, что оставляет create, прерванный между `pvcreate` и `vgextend`, — и udev-события при этом не было, так что кэш агента единственное место, где этого PV нет. Добавление записи должно переиспользовать loop и завести его в VG (2 PV), а не упасть на повторном `pvcreate` в фатальный `VGExtendFailed` |

### `..._consumer_test.go` — сквозной путь до нагрузки

Thick `LocalStorageClass` поверх file-backed LVG → PVC (`WaitForFirstConsumer`) → под-писатель
пишет маркер, под-читатель его читает; попутно проверяется, что появился `LVMLogicalVolume`
в фазе `Created` на нашем LVG. Отдельно — thin `LVMLogicalVolume` на пуле поверх loop'а и
LVM-снапшот с него (`lvcreate -s`) с чтением маркера, записанного до снапшота.

Снапшот через CR `LVMLogicalVolumeSnapshot` идёт отдельной спекой: реконсайлер гейтится
редакцией (`cmd/llvs_ee.go` собирается по `//go:build !ce`), тогда как CRD ставится везде.
Спека это детектирует — если объект так и не получил статус, она `Skip`, а не падает.

### `..._multinode_test.go` — кластерный уровень

`LVMVolumeGroupSet` с `fileDevices` в шаблоне разворачивается в отдельный `Ready` LVG на
каждой ноде, у каждого — свой backing-файл и свой loop. Затем шаблон расширяется второй
записью `fileDevices` (правка существующей запрещена тем же CEL-правилом) — и каждый узел
должен дорастить свой VG на месте, без появления лишних LVG. Плюс стабильность после
рестарта: `UID`, `vgUUID`, `filePath`, `loopDevice` и `pvUUID` не меняются.

### `..._node_test.go` — настоящая перезагрузка ноды (лейбл `node-reboot`)

Имитация через `losetup -d` не проходит стартовую последовательность агента относительно
udev и systemd, поэтому здесь воркер перезагружается по-настоящему: факт ребута
подтверждается сменой `/proc/sys/kernel/random/boot_id`, затем проверяется, что backing-файл
пережил перезагрузку, к нему привязан ровно один loop, VG вернулся в `Ready` с тем же
`vgUUID`, thin-pool на месте и второго файла не появилось. Control-plane не трогается.

Второй спекой проверяется инвариант, на котором держится идемпотентность: `losetup --find
--nooverlap` при повторном вызове обязан вернуть **тот же** minor. Флаг требует util-linux
≥ 2.29 — если образ ноды его не поддерживает, стартовый reattach и реконсайлер привяжут к
одному файлу два loop'а и VG молча удвоится.

Самая разрушительная спека в сьюте. Исключается фильтром:

```bash
make test-go GINKGO_LABEL_FILTER='!stress-test && !node-reboot'
```

Требования: кластер собран из этой ветки (поле `fileDevices` и настройка модуля
`fileDevicesDirectory` есть только здесь); на выбранной ноде под
`/opt/deckhouse/sds` нужно ≥8Gi свободного места. `E2E_DVP_BASE_CLUSTER_STORAGE_CLASS`
нужна только смешанному сценарию (он подключает `VirtualDisk`); без неё он `Skip`.

## См. также

- [`E2E_USAGE.md`](E2E_USAGE.md) — детальное руководство (CI, локальный запуск,
  переменные окружения, troubleshooting, написание нового теста).
- [`Makefile`](Makefile) — все цели (`make help`).
- [storage-e2e](https://github.com/deckhouse/storage-e2e) — SDK и CI-workflow.
