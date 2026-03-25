# E2E тесты для sds-node-configurator

Данный каталог содержит end-to-end (e2e) тесты для модуля `sds-node-configurator`.

## Описание

E2E тесты предназначены для проверки полного цикла работы модуля в реальном Kubernetes кластере. Тесты используют фреймворк [storage-e2e](https://github.com/deckhouse/storage-e2e) для управления тестовым кластером и [Ginkgo](https://onsi.github.io/ginkgo/) / [Gomega](https://onsi.github.io/gomega/) для организации тестовых сценариев.

## Предварительные требования

1. **Kubernetes кластер**: Доступный Kubernetes кластер с установленным модулем `sds-node-configurator`
2. **Go 1.22+**: Для запуска тестов
3. **SSH доступ**: К мастер-ноде кластера (для storage-e2e)

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
    ├── sds_node_configurator_suite_test.go  # Инициализация Ginkgo suite
    ├── sds_node_configurator_test.go        # Основные тесты
    └── cluster_config.yml                   # Конфигурация тестового кластера
```

## Быстрый старт

### 1. Подготовьте конфигурацию

Папка `e2e/config/` в `.gitignore`. Создайте там файл с переменными окружения:

```bash
# e2e/config/test_exports_storage_e2e
export TEST_CLUSTER_CREATE_MODE='alwaysUseExisting'
export TEST_CLUSTER_NAMESPACE='e2e-test'
export TEST_CLUSTER_STORAGE_CLASS='linstor-r1'
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
make test-focus FOCUS="TestSdsNodeConfigurator"
```

## Тестовые сценарии

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
