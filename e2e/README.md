# E2E тесты для sds-node-configurator

Данный каталог содержит end-to-end (e2e) тесты для модуля `sds-node-configurator`.

## Описание

E2E тесты предназначены для проверки полного цикла работы модуля в реальном Kubernetes кластере. Тесты используют фреймворк [storage-e2e](https://github.com/deckhouse/storage-e2e) для управления тестовым кластером и [Ginkgo](https://onsi.github.io/ginkgo/) / [Gomega](https://onsi.github.io/gomega/) для организации тестовых сценариев.

## Предварительные требования

1. **Kubernetes кластер** с модулем `sds-node-configurator`; для сценариев Common Scheduler Extender — также `sds-local-volume` и настроенный scheduler extender при необходимости
2. **Go 1.25+** (см. `go.mod`)
3. **SSH доступ** к мастер-ноде (для storage-e2e), при необходимости jump host

## Структура

```
e2e/
├── Makefile              # Команды для запуска тестов
├── README.md             # Данный файл
├── E2E_USAGE.md          # Подробная инструкция по запуску
├── go.mod                # Go модуль
├── go.sum                # Зависимости
├── config/               # Локальные конфиги (в .gitignore)
└── tests/
    ├── e2e_suite_test.go              # TestE2E, BeforeSuite/AfterSuite
    ├── e2e_shared_test.go             # общие хелперы
    ├── common_scheduler_test.go       # Common Scheduler Extender
    ├── sds_node_configurator_test.go  # BlockDevice, LVMVolumeGroup
    └── cluster_config.yml             # Конфигурация вложенного кластера (storage-e2e)
```

## Быстрый старт

### 1. Подготовьте конфигурацию

Папка `e2e/config/` в `.gitignore`. Создайте там файл с переменными окружения:

```bash
# e2e/config/test_exports_storage_e2e
export TEST_CLUSTER_CREATE_MODE=`alwaysUseExisting` or `alwaysCreateNew`
export TEST_CLUSTER_NAMESPACE='<test_namespace>'
export TEST_CLUSTER_STORAGE_CLASS='<test_storage_class>'
export TEST_CLUSTER_CLEANUP='false'

export SSH_HOST='<master-ip>'
export SSH_USER='<ssh-user>'
export SSH_PRIVATE_KEY='/path/to/ssh/key'

export KUBE_CONFIG_PATH='/path/to/kubeconfig'

export DKP_LICENSE_KEY='<license>'
export REGISTRY_DOCKER_CFG='<base64-encoded>'
```

### 2. Запустите тесты

```bash
source e2e/config/test_exports_storage_e2e
cd e2e
make test
```

Или конкретный тест:

```bash
make test-focus FOCUS="TestE2E"
```

Один вход как в CI: `go test -v -count=1 -timeout 60m ./tests/ -run '^TestE2E$'` — сначала сценарии Common Scheduler, затем sds-node-configurator (порядок файлов в пакете).

## Тестовые сценарии

### Common Scheduler Extender

Сценарии в `common_scheduler_test.go`: фильтрация нод по LVMVolumeGroup для local PVC, Pending при нехватке места, конкурентные PVC. См. также [E2E_USAGE.md](E2E_USAGE.md).

### BlockDevice Discovery

- На ноде появляется новый неразмеченный диск
- Через некоторое время в кластере появляется объект BlockDevice
- Проверяется корректность всех полей объекта

**Проверки**:
- ✅ Объект BlockDevice существует
- ✅ `status.nodeName` соответствует имени ноды
- ✅ `status.path` соответствует пути к устройству
- ✅ `status.size` больше 0
- ✅ `status.consumable` = true для неразмеченного диска

### LVMVolumeGroup

- Создание LVMVolumeGroup на основе BlockDevice
- Проверка статуса и capacity

### Отладка scheduler-extender

```bash
kubectl logs -n d8-sds-node-configurator -l app=sds-common-scheduler-extender -f
```

## Кластер заблокирован (cluster is already locked)

Если предыдущий запуск тестов завершился по Ctrl+C или упал до cleanup:

```bash
export TEST_CLUSTER_FORCE_LOCK_RELEASE='true'
source e2e/config/test_exports_storage_e2e
cd e2e && make test
```

## Отладка

### Просмотр логов агента

```bash
kubectl get pods -n d8-sds-node-configurator -o wide | grep <node-name>
kubectl logs -n d8-sds-node-configurator <agent-pod-name> -f
```

### Проверка BlockDevice

```bash
kubectl get blockdevice
kubectl describe blockdevice <bd-name>
kubectl get blockdevice -l kubernetes.io/hostname=<node-name>
```

### Проверка LVMVolumeGroup

```bash
kubectl get lvmvolumegroup
kubectl describe lvmvolumegroup <lvg-name>
```

## Дополнительная информация

- [E2E_USAGE.md](E2E_USAGE.md) — подробная инструкция по CI и локальному запуску
- [storage-e2e](https://github.com/deckhouse/storage-e2e) — фреймворк для E2E тестов
- [Ginkgo](https://onsi.github.io/ginkgo/) — тестовый фреймворк
