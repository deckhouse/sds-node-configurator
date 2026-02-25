# Интеграция E2E с storage-e2e (виртуальное окружение)

Подробный план перевода тестов sds-node-configurator в режим «виртуальное окружение»: кластер ВМ создаётся или переиспользуется, тест при необходимости сам создаёт виртуальные диски через API storage-e2e.

---

## 1. Обзор двух режимов

### Текущий режим (без storage-e2e)

- Подключение к кластеру через `kubeconfig` (KUBECONFIG или `~/.kube/config`).
- Переменные `E2E_NODE_NAME`, `E2E_DEVICE_PATH`, `E2E_DEVICE_SERIAL` задают ноду и устройство вручную.
- Тест **ожидает**, что кто-то уже добавил диск на ноду; сам диск не создаётся.
- Запуск: `make run-in-cluster` (Job в кластере) или `make test` (локально).

### Целевой режим (с storage-e2e)

- **Базовый кластер** — кластер, к которому подключаемся по SSH (`SSH_HOST`, `SSH_USER`). В нём создаются ВМ (модуль virtualization) в namespace `TEST_CLUSTER_NAMESPACE` (по умолчанию `e2e-test-cluster`).
- **Вложенный (тестовый) кластер** — Kubernetes, развёрнутый DKP на этих ВМ. В нём работают sds-node-configurator и тесты. Имена нод = hostname ВМ (например `worker-1-x7k2m`).
- Тест при необходимости сам создаёт виртуальный диск через `kubernetes.AttachVirtualDiskToVM()` в **базовом** кластере, затем ждёт появления BlockDevice во **вложенном** кластере.
- **Важно:** `BaseKubeconfig` (доступ к базовому кластеру) есть только после `CreateTestCluster()` (режим `alwaysCreateNew`). При `alwaysUseExisting` тест подключается только к вложенному кластеру и не может создавать диски — сценарий с AttachVirtualDisk в этом режиме не поддерживается (можно оставить текущую логику «ожидание уже добавленного диска» по E2E_*).

---

## 2. Где что смотреть в storage-e2e

| Цель | Файл / раздел |
|------|----------------|
| Быстрый старт, переменные окружения | `storage-e2e/README.md` |
| Архитектура, lifecycle, пакеты | `storage-e2e/ARCHITECTURE.md` |
| Список экспортируемых функций | `storage-e2e/pkg/FUNCTIONS_GLOSSARY.md` |
| Создание/подключение кластера, TestClusterResources | `storage-e2e/pkg/cluster/cluster.go` |
| ВМ: имена, namespace, VMResources | `storage-e2e/pkg/cluster/vms.go` |
| Виртуальный диск: создание и ожидание attachment | `storage-e2e/pkg/kubernetes/virtualdisk.go` |
| BlockDevice (consumable, по ноде) | `storage-e2e/pkg/kubernetes/blockdevice.go`, `internal/kubernetes/storage/blockdevice.go` |
| Пример теста: кластер + модули + BlockDevice | `sds-local-volume/e2e/` (suite + тест) |
| Конфиг кластера (masters/workers, модули) | `storage-e2e/tests/test-template/cluster_config.yml` |
| Валидация env, константы | `storage-e2e/internal/config/env.go`, `types.go` |

---

## 3. Ключевые API storage-e2e

### Кластер

- `cluster.CreateOrConnectToTestCluster()` — создать новый кластер (ВМ + DKP + модули) или подключиться к существующему (lock). Возвращает `*cluster.TestClusterResources`. При создании нового заполняются в т.ч. `BaseKubeconfig`, `VMResources`, `ClusterDefinition`.
- `cluster.CleanupTestClusterResources(resources)` — освободить lock, при `TEST_CLUSTER_CLEANUP=true` удалить ВМ и ресурсы.
- `config.ValidateEnvironment()` — проверить обязательные переменные (вызывать в BeforeSuite).

### Виртуальный диск (базовый кластер)

- `kubernetes.AttachVirtualDiskToVM(ctx, baseKubeconfig, kubernetes.VirtualDiskAttachmentConfig{...})` — создать VirtualDisk и VirtualMachineBlockDeviceAttachment. Обязательные поля: `VMName`, `Namespace`, `DiskSize`, `StorageClassName`. Опционально: `DiskName`.
- `kubernetes.WaitForVirtualDiskAttached(ctx, baseKubeconfig, namespace, attachmentName, pollInterval)` — ждать фазу Attached.

Использовать **только** если `resources.BaseKubeconfig != nil` (режим создания нового кластера).

### BlockDevice (вложенный кластер)

- `kubernetes.GetConsumableBlockDevices(ctx, nestedKubeconfig)` — список consumable BlockDevice (kubeconfig вложенного кластера = `resources.Kubeconfig`).
- В коде теста можно по-прежнему использовать controller-runtime client и список `BlockDevice` по ноде/пути.

### Соответствие ВМ и ноды

- `resources.VMResources.VMNames` — имена ВМ в базовом кластере (например `worker-1-x7k2m`).
- Во вложенном кластере имя ноды совпадает с hostname ВМ = именем ВМ. То есть для первой worker-ВМ: `nodeName = resources.VMResources.VMNames[0]` (или выбрать по конфигу workers).
- Namespace для ВМ и VirtualDisk: `resources.VMResources.Namespace` (равен `config.TestClusterNamespace`).

---

## 4. Пошаговый план изменений в sds-node-configurator/e2e

### Шаг 1. Зависимости (go.mod)

В `e2e/go.mod` добавить:

```go
require (
    // существующие require...
    github.com/deckhouse/storage-e2e v0.0.0
)

replace github.com/deckhouse/storage-e2e => ../../storage-e2e
```

Выполнить в каталоге `e2e`: `go mod tidy`. При конфликтах версий k8s/api — при необходимости использовать replace/версии из storage-e2e.

### Шаг 2. Конфиг и переменные окружения

1. Создать каталог `e2e/tests/sds-node-configurator-e2e/` (или оставить тесты в `tests/`, но добавить конфиг).
2. Скопировать и адаптировать `cluster_config.yml` из `storage-e2e/tests/test-template/cluster_config.yml` в `e2e/cluster_config.yml` (или в подкаталог теста). В `dkpParameters.modules` обязательно включить:
   - `snapshot-controller`
   - `sds-node-configurator` (с `enableThinProvisioning: true` при необходимости)
   - при необходимости `sds-local-volume` и др.
3. Создать файл `e2e/test_exports` (или `e2e/tests/test_exports`) — шаблон переменных окружения, без коммита секретов (добавить в .gitignore при необходимости). Минимум:
   - `SSH_USER`, `SSH_HOST`
   - `TEST_CLUSTER_CREATE_MODE` = `alwaysUseExisting` или `alwaysCreateNew`
   - `TEST_CLUSTER_STORAGE_CLASS` (для создания ВМ и дисков, например `lsc-thick`)
   - `DKP_LICENSE_KEY`, `REGISTRY_DOCKER_CFG`
   - `YAML_CONFIG_FILENAME` = путь к `cluster_config.yml` (или по умолчанию искать `cluster_config.yml` в текущей/тестовой директории)

Переменные по полному списку см. в `storage-e2e/README.md` (Environment Variables).

### Шаг 3. Suite: инициализация и lifecycle (block_device_discovery_suite_test.go)

- В **BeforeSuite**:
  - Вызвать `config.ValidateEnvironment()` (из `github.com/deckhouse/storage-e2e/internal/config`).
  - Инициализировать логгер: `logger.Initialize()` (из `github.com/deckhouse/storage-e2e/internal/logger`).
  - По желанию оставить текущую инициализацию k8s client для режима «без storage-e2e» (см. шаг 6).
- Добавить **глобальную переменную** для ресурсов кластера, например:
  ```go
  var testClusterResources *cluster.TestClusterResources
  ```
- В **AfterSuite**: вызвать `logger.Close()`; при наличии `testClusterResources` — `cluster.CleanupTestClusterResources(testClusterResources)`.

Либо оформить два сценария: один `Describe` с storage-e2e (Ordered, BeforeAll/AfterAll создают/очищают кластер), второй — текущий «standalone» без storage-e2e. Тогда общий BeforeSuite только валидирует env и поднимает клиент, если не используется storage-e2e.

### Шаг 4. Создание/подключение кластера (первый It)

- В тесте с storage-e2e в **BeforeAll** (или в первом **It**):
  - Вызвать `cluster.OutputEnvironmentVariables()` для отладки.
- В первом **It**:
  ```go
  It("should create or connect to test cluster and wait for it to become ready", func() {
      testClusterResources = cluster.CreateOrConnectToTestCluster()
      Expect(testClusterResources).NotTo(BeNil())
      Expect(testClusterResources.Kubeconfig).NotTo(BeNil())
  })
  ```
- Дальше все тесты используют `testClusterResources.Kubeconfig` вместо глобального `cfg` и создают controller-runtime client от этого конфига (или используют общий k8sClient, переключенный на вложенный кластер). Для режима «только вложенный кластер» (без создания диска) достаточно одного Kubeconfig.

### Шаг 5. Тест discovery с виртуальным диском (block_device_discovery_test.go)

Логику разбить на два варианта.

**Вариант A: режим с виртуальным диском** (когда `testClusterResources != nil && testClusterResources.BaseKubeconfig != nil`):

1. Выбрать целевую ВМ, например первую worker:
   ```go
   var targetVMName, targetNodeName, namespace string
   if testClusterResources.VMResources != nil && len(testClusterResources.VMResources.VMNames) > 0 {
       namespace = testClusterResources.VMResources.Namespace
       targetVMName = testClusterResources.VMResources.VMNames[0] // или выбрать по имени из ClusterDefinition.Workers
       targetNodeName = targetVMName // во вложенном кластере имя ноды = hostname ВМ
   }
   ```
2. Снимок существующих BlockDevice во вложенном кластере (по `testClusterResources.Kubeconfig`), чтобы потом искать «новый».
3. Создать и прикрепить диск в **базовом** кластере:
   ```go
   attachCfg := kubernetes.VirtualDiskAttachmentConfig{
       VMName:           targetVMName,
       Namespace:        namespace,
       DiskSize:         "5Gi",
       StorageClassName: config.TestClusterStorageClass, // или из env
   }
   result, err := kubernetes.AttachVirtualDiskToVM(ctx, testClusterResources.BaseKubeconfig, attachCfg)
   Expect(err).NotTo(HaveOccurred())
   ```
4. Ждать attachment (с таймаутом):
   ```go
   attachCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
   defer cancel()
   err = kubernetes.WaitForVirtualDiskAttached(attachCtx, testClusterResources.BaseKubeconfig, namespace, result.AttachmentName, 10*time.Second)
   Expect(err).NotTo(HaveOccurred())
   ```
5. Во **вложенном** кластере ждать появления нового BlockDevice на ноде `targetNodeName` (как сейчас: список BlockDevice, фильтр по `!initialNames[bd.Name]` и `bd.Status.NodeName == targetNodeName`, путь при желании можно не фиксировать — виртуальный диск может появиться как `/dev/vdX` или иначе).
6. Дальше оставить текущие проверки (nodeName, path, size, consumable, fsType и т.д.), подставляя найденный `foundBD` и `targetNodeName`.

**Вариант B: режим без создания диска** (текущий или `alwaysUseExisting`):

- Если `testClusterResources == nil` или `testClusterResources.BaseKubeconfig == nil`, использовать текущую логику: читать `E2E_NODE_NAME`, `E2E_DEVICE_PATH`, ждать появления нового BlockDevice без вызова AttachVirtualDisk (ручное добавление диска или уже существующий диск).

Условие в начале теста, например:

```go
if testClusterResources != nil && testClusterResources.BaseKubeconfig != nil && testClusterResources.VMResources != nil && len(testClusterResources.VMResources.VMNames) > 0 {
    // вариант A: прикрепить виртуальный диск и ждать BlockDevice
} else {
    // вариант B: только ожидание (E2E_NODE_NAME / E2E_DEVICE_PATH)
}
```

### Шаг 6. Совместимость со старым запуском (make test / run-in-cluster)

- Если тесты запускаются **без** переменных storage-e2e (например только `make test` с KUBECONFIG), `config.ValidateEnvironment()` упадёт. Варианты:
  - В BeforeSuite проверять наличие хотя бы `TEST_CLUSTER_CREATE_MODE` (или `SSH_HOST`); при отсутствии — не вызывать ValidateEnvironment и не использовать cluster.CreateOrConnectToTestCluster(), а инициализировать только k8s client из `config.GetConfig()` как сейчас. Тогда тесты с виртуальным диском пропускать (Skip), а тесты discovery/LVM — выполнять в режиме «ручной диск» (E2E_NODE_NAME и т.д.).
  - Либо оформить два отдельных suite/пакета: один для «standalone» (текущий), второй для «storage-e2e», и выбирать при запуске через build tags или отдельную директорию тестов.

Рекомендация: один suite с условной инициализацией: если заданы env storage-e2e — создаём/подключаем кластер и при наличии BaseKubeconfig тестируем с виртуальным диском; иначе — текущее поведение (ожидание ручного диска).

### Шаг 7. LVMVolumeGroup тест

- Без изменений по сути: он уже использует `discoveredNodeName` и `discoveredBDName` из теста discovery. При варианте A эти переменные заполняются найденным BlockDevice после AttachVirtualDisk; при варианте B — как сейчас. Клиент для вложенного кластера везде брать из `testClusterResources.Kubeconfig` (или глобальный k8sClient, инициализированный из него при использовании storage-e2e).

### Шаг 8. Makefile и README

- Добавить цель, например `make test-storage-e2e`, которая устанавливает переменные из `test_exports` и запускает тесты с таймаутом, например:
  ```makefile
  test-storage-e2e:
      source test_exports && go test -timeout=120m -v ./tests/ -count=1
  ```
- В README описать два режима: «локально с kubeconfig» (текущий) и «виртуальное окружение (storage-e2e)» с перечислением переменных и ссылкой на `storage-e2e/README.md`.

---

## 5. Переменные окружения (кратко)

Обязательные для режима storage-e2e:

- `TEST_CLUSTER_CREATE_MODE` — `alwaysUseExisting` | `alwaysCreateNew` | `commander`
- `SSH_USER`, `SSH_HOST`
- `TEST_CLUSTER_STORAGE_CLASS` — StorageClass для ВМ и для VirtualDisk (например `lsc-thick`)
- `DKP_LICENSE_KEY`, `REGISTRY_DOCKER_CFG`

Опционально: `TEST_CLUSTER_NAMESPACE`, `YAML_CONFIG_FILENAME`, `SSH_PRIVATE_KEY`, `SSH_PASSPHRASE`, `LOG_LEVEL`, `TEST_CLUSTER_CLEANUP` и др. — см. `storage-e2e/README.md`.

---

## 6. Порядок внедрения (рекомендуемый)

1. Добавить зависимость storage-e2e в go.mod и replace, проверить сборку.
2. Добавить cluster_config.yml и test_exports, не коммитить секреты.
3. В suite: BeforeSuite — условная валидация env и инициализация логгера; переменная testClusterResources; AfterSuite — cleanup и logger.Close().
4. Первый It — CreateOrConnectToTestCluster(), переключение k8sClient на testClusterResources.Kubeconfig при успехе.
5. В тесте discovery: ветка «если BaseKubeconfig и VMResources есть — AttachVirtualDisk + ожидание BlockDevice», иначе текущая логика (ожидание ручного диска).
6. Прогнать в режиме alwaysUseExisting с уже поднятым кластером (без AttachVirtualDisk), затем в режиме alwaysCreateNew с созданием диска.
7. Обновить Makefile и README.

После этого тесты будут поддерживать и «виртуальное окружение» с созданием виртуального диска, и текущий ручной режим.
