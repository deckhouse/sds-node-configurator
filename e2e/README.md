# E2E тесты для sds-node-configurator

Данный каталог содержит end-to-end (e2e) тесты для модуля `sds-node-configurator`: сценарии BlockDevice / LVMVolumeGroup и **Common Scheduler Extender** (local volumes).

## Описание

E2E тесты предназначены для проверки полного цикла работы модуля в реальном Kubernetes кластере. Используются [storage-e2e](https://github.com/deckhouse/storage-e2e), [Ginkgo](https://onsi.github.io/ginkgo/) и [Gomega](https://onsi.github.io/gomega/).

## Предварительные требования

1. **Kubernetes кластер** с установленным модулем `sds-node-configurator`
2. Для сценариев scheduler-extender: модуль `sds-local-volume` (local PVC) и при необходимости настроенный **Common Scheduler Extender**
3. **kubectl** с доступом к кластеру
4. **Go 1.25+** (см. `go.mod`)
5. **SSH** к мастер-ноде при использовании вложенного кластера (storage-e2e)
6. Для local-volume тестов: хотя бы один Ready **LVMVolumeGroup** и **StorageClass**

## Структура

```
e2e/
├── Dockerfile              # образ для Job в кластере
├── Makefile                # deps, go test, docker, Job
├── README.md
├── E2E_USAGE.md            # CI, smoke, секреты, label e2e-smoke-test
├── go.mod / go.sum
├── config/                 # локально, в .gitignore
├── manifests/              # RBAC и Job
└── tests/
    ├── e2e_suite_test.go      # TestE2E, BeforeSuite/AfterSuite (storage-e2e)
    ├── e2e_shared_test.go     # общие хелперы
    ├── common_scheduler_test.go
    ├── sds_node_configurator_test.go
    └── cluster_config.yml     # вложенный кластер (storage-e2e)
```

## Быстрый старт (локально)

Папка `e2e/config/` в `.gitignore`. Создайте переменные окружения (пример):

```bash
# e2e/config/test_exports_storage_e2e
export TEST_CLUSTER_CREATE_MODE='alwaysUseExisting'  # или alwaysCreateNew
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

```bash
source e2e/config/test_exports_storage_e2e
cd e2e
make deps
make test                    # полный прогон (TestE2E)
make test-go                 # как в CI: -run '^TestE2E$' (сначала Common Scheduler, затем модуль)
```

Фокус по имени теста:

```bash
make test-focus FOCUS="TestE2E"
```

Один вход, как в CI по label `e2e-smoke-test`: `TestE2E` — сначала сценарии **Common Scheduler Extender**, затем **Sds Node Configurator** (порядок спеков по файлам: `common_scheduler_test.go`, затем `sds_node_configurator_test.go`).

```bash
go test -v -count=1 -timeout 60m ./tests/ -run '^TestE2E$'
# или
make test-go
```

Ginkgo-сценарий по имени (нужен `ginkgo` в PATH):

```bash
ginkgo -v --progress --focus="Should schedule Pod with local PVC" ./tests/
```

## Тестовые сценарии

### Модуль sds-node-configurator (BlockDevice / LVM)

- **BlockDevice discovery**: появление диска; корректные `status.nodeName`, `status.path`, `status.size`, `consumable`.
- **LVMVolumeGroup**: создание на основе BlockDevice, статус и capacity.

### Common Scheduler Extender

Сценарии в `common_scheduler_test.go`: фильтрация нод по LVMVolumeGroup для local PVC, Pending при нехватке места, резервация при конкурентных PVC. См. [E2E_USAGE.md](E2E_USAGE.md).

## Кластер заблокирован (cluster is already locked)

Если предыдущий запуск прервался до cleanup:

```bash
export TEST_CLUSTER_FORCE_LOCK_RELEASE='true'
source e2e/config/test_exports_storage_e2e
cd e2e && make test
```

## Запуск в кластере (Job)

```bash
cd e2e
make docker-build E2E_IMAGE=your-registry/e2e-tests:latest
make docker-push E2E_IMAGE=your-registry/e2e-tests:latest
make run-in-cluster E2E_IMAGE=your-registry/e2e-tests:latest
make logs
make cleanup
```

## Переменные окружения

| Переменная | Описание |
|------------|----------|
| `TEST_CLUSTER_CREATE_MODE` | Режим кластера (`alwaysUseExisting` и т.д.) |
| `TEST_CLUSTER_NAMESPACE` | Namespace тестов |
| `TEST_CLUSTER_STORAGE_CLASS` | StorageClass для PVC |
| `SSH_HOST`, `SSH_USER`, `KUBE_CONFIG_PATH` | Доступ к кластеру |
| `DKP_LICENSE_KEY`, `REGISTRY_DOCKER_CFG` | При необходимости для nested setup |

См. также `config/test_exports_storage_e2e`.

## Отладка

### Агент / BlockDevice / LVMVolumeGroup

```bash
kubectl get pods -n d8-sds-node-configurator -o wide
kubectl get blockdevice
kubectl get lvmvolumegroup
```

### Scheduler extender

```bash
kubectl logs -n d8-sds-node-configurator -l app=sds-common-scheduler-extender -f
```

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

- [E2E_USAGE.md](E2E_USAGE.md) — CI и детальный локальный запуск
- [storage-e2e](https://github.com/deckhouse/storage-e2e)
- [Ginkgo](https://onsi.github.io/ginkgo/)
