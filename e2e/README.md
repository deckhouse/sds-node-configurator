# E2E тесты для sds-node-configurator

Данный каталог содержит end-to-end (e2e) тесты для модуля `sds-node-configurator`: сценарии BlockDevice / LVMVolumeGroup и **Common Scheduler Extender** (local volumes).

## Описание

E2E тесты проверяют работу модуля в реальном Kubernetes кластере. Используются [storage-e2e](https://github.com/deckhouse/storage-e2e), [Ginkgo](https://onsi.github.io/ginkgo/) и [Gomega](https://onsi.github.io/gomega/).

## Предварительные требования

1. **Kubernetes кластер** с модулем `sds-node-configurator`
2. Для scheduler-extender: `sds-local-volume` и настроенный extender при необходимости
3. **kubectl** с доступом к кластеру
4. **Go 1.25+** (см. `go.mod`)
5. **SSH** к мастер-ноде при использовании вложенного кластера (storage-e2e)
6. Для local-тестов: Ready **LVMVolumeGroup** и **StorageClass**

## Структура

```
e2e/
├── Dockerfile
├── Makefile
├── README.md
├── E2E_USAGE.md          # CI, smoke, секреты, label e2e-smoke-test
├── go.mod / go.sum
├── config/               # локально, в .gitignore
├── manifests/            # Job / RBAC для запуска в кластере
└── tests/
    ├── common_scheduler_suite_test.go   # TestCommonScheduler
    ├── common_scheduler_test.go
    ├── sds_node_configurator_suite_test.go  # TestSdsNodeConfigurator, BeforeSuite/AfterSuite
    ├── sds_node_configurator_test.go
    └── cluster_config.yml
```

## Быстрый старт (локально)

Создайте `e2e/config/test_exports_storage_e2e` (не коммитится):

```bash
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
make test                    # оба suite: TestCommonScheduler и TestSdsNodeConfigurator
# как в CI (только модульный suite):
make test-go                 # go test -run TestSdsNodeConfigurator
```

Фокус по имени теста:

```bash
make test-focus FOCUS="TestSdsNodeConfigurator"
make test-focus FOCUS="TestCommonScheduler"
```

Ginkgo-сценарий по имени (нужен `ginkgo` в PATH):

```bash
ginkgo -v --progress --focus="Should schedule Pod with local PVC" ./tests/
```

## Тестовые сценарии

### Common Scheduler Extender

Сценарии в `common_scheduler_test.go`: фильтрация нод по LVMVolumeGroup для local PVC, Pending при нехватке места, конкурентные PVC. См. [E2E_USAGE.md](E2E_USAGE.md).

### BlockDevice Discovery

- На ноде появляется новый неразмеченный диск
- В кластере появляется объект BlockDevice с корректными полями

### LVMVolumeGroup

- Создание LVMVolumeGroup на основе BlockDevice
- Проверка статуса и capacity

## Кластер заблокирован (cluster is already locked)

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

## Отладка

**Агент / BlockDevice / LVMVolumeGroup:**

```bash
kubectl get pods -n d8-sds-node-configurator -o wide
kubectl get blockdevice
kubectl get lvmvolumegroup
```

**Scheduler extender:**

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

- [E2E_USAGE.md](E2E_USAGE.md) — smoke в CI, секреты, label `e2e-smoke-test`
- [storage-e2e](https://github.com/deckhouse/storage-e2e)
- [Ginkgo](https://onsi.github.io/ginkgo/)
