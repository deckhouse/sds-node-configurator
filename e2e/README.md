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
│   └── lvm.go                   #   CountPVsInVG, VGInListing, ThinPoolDataLVPresent,
│                                #     RemoveThinPoolStackScript
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
| `block-device-stable` | стабильность BlockDevice по стадиям |
| `netlink-discovery` | netlink-дискавери |
| `lvmvolumegroup` | сценарии LVMVolumeGroup (в т.ч. thin-pool) |
| `controller-restart` | устойчивость к рестарту контроллера |
| `schedule-extender` (+ `small`/`medium`/`large`) | scheduler-extender |
| `regress` | вложенный регресс-кейс block-device-stable |
| `stress-test` | стресс: максимум независимых VG на ноду (исключён по умолчанию) |

```bash
# подмножество:
make test-go GINKGO_LABEL_FILTER='discovery || block-device'
# только стресс:
make test-stress
# всё вместе:
make test-go GINKGO_LABEL_FILTER=''
```

## См. также

- [`E2E_USAGE.md`](E2E_USAGE.md) — детальное руководство (CI, локальный запуск,
  переменные окружения, troubleshooting, написание нового теста).
- [`Makefile`](Makefile) — все цели (`make help`).
- [storage-e2e](https://github.com/deckhouse/storage-e2e) — SDK и CI-workflow.
