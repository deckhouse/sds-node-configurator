# E2E тесты для sds-node-configurator (Common Scheduler Extender)

Данный каталог содержит end-to-end (e2e) тесты для модуля `sds-node-configurator`, в частности для компонента Common Scheduler Extender.

## Описание

E2E тесты предназначены для проверки полного цикла работы scheduler-extender в реальном Kubernetes кластере. Тесты используют фреймворк [Ginkgo](https://onsi.github.io/ginkgo/) и библиотеку [storage-e2e](https://github.com/deckhouse/storage-e2e) для настройки тестового окружения.

## Предварительные требования

1. **Kubernetes кластер**: Доступный Kubernetes кластер с установленными модулями:
   - `sds-node-configurator`
   - `sds-local-volume` (для local PVC тестов)
2. **kubectl**: Настроенный доступ к кластеру
3. **Go**: Версия Go 1.24.9 или выше
4. **LVMVolumeGroup**: Хотя бы один Ready LVMVolumeGroup в кластере
5. **StorageClass**: StorageClass для local volumes

## Структура

```
e2e/
├── Dockerfile                                   # Образ для запуска тестов в кластере
├── Makefile                                     # Команды для сборки и запуска
├── README.md                                    # Данный файл
├── go.mod                                       # Go модуль
├── config/
│   ├── test_exports                             # Переменные окружения (базовые)
│   └── test_exports_storage_e2e                 # Переменные окружения для storage-e2e
├── manifests/
│   ├── rbac.yaml                                # ServiceAccount, ClusterRole, ClusterRoleBinding
│   └── job.yaml                                 # Job для запуска тестов
└── tests/
    ├── common_scheduler_suite_test.go           # Инициализация Ginkgo suite
    └── common_scheduler_test.go                 # Тесты scheduler-extender
```

## Тестовые сценарии

### 1. Фильтрация нод для local volumes

**Сценарий**: `Should schedule Pod with local PVC to node with LVMVolumeGroup having enough space`

**Описание**:
- Создается PVC с local StorageClass
- Создается Pod, использующий этот PVC
- Проверяется, что Pod размещается на ноде с LVMVolumeGroup

**Проверки**:
- ✅ Pod успешно запланирован
- ✅ Нода имеет LVMVolumeGroup с достаточным местом

### 2. Фильтрация нод при недостатке места

**Сценарий**: `Should filter out nodes without sufficient space in LVMVolumeGroup`

**Описание**:
- Создается PVC, запрашивающий больше места, чем есть в любом LVMVolumeGroup
- Проверяется, что Pod остается в статусе Pending

**Проверки**:
- ✅ Pod не запланирован
- ✅ Pod остается в статусе Pending

### 3. Резервация места для конкурентных PVC

**Сценарий**: `Should correctly reserve space for multiple concurrent PVCs`

**Описание**:
- Создается несколько PVC и Pod'ов одновременно
- Проверяется, что все Pod'ы успешно запланированы

**Проверки**:
- ✅ Все Pod'ы запланированы
- ✅ Место правильно зарезервировано

## Запуск тестов

### Локальный запуск

```bash
cd e2e

# Настроить переменные окружения
source config/test_exports_storage_e2e

# Установить зависимости
go mod tidy

# Установить Ginkgo (если не установлен)
go install github.com/onsi/ginkgo/v2/ginkgo@latest

# Запустить тесты
ginkgo -v --progress ./tests/

# Или через make
make test
```

### Запуск в кластере

```bash
cd e2e

# Собрать образ
make docker-build E2E_IMAGE=your-registry/e2e-tests:latest

# Отправить в registry
make docker-push E2E_IMAGE=your-registry/e2e-tests:latest

# Запустить тесты
make run-in-cluster E2E_IMAGE=your-registry/e2e-tests:latest TEST_CLUSTER_STORAGE_CLASS=your-storage-class

# Посмотреть логи
make logs

# Очистить ресурсы
make cleanup
```

## Переменные окружения

| Переменная                 | Описание                                      | Обязательная |
|----------------------------|-----------------------------------------------|--------------|
| `TEST_CLUSTER_CREATE_MODE` | Режим создания кластера (`alwaysUseExisting`) | Да           |
| `TEST_CLUSTER_NAMESPACE`   | Namespace для тестов                          | Нет          |
| `TEST_CLUSTER_STORAGE_CLASS` | StorageClass для local PVC                  | Да           |
| `SSH_HOST`                 | SSH хост для подключения к кластеру           | Да           |
| `SSH_USER`                 | SSH пользователь                              | Да           |
| `KUBE_CONFIG_PATH`         | Путь к kubeconfig                             | Да           |
| `DKP_LICENSE_KEY`          | Лицензионный ключ DKP                         | Да           |
| `REGISTRY_DOCKER_CFG`      | Docker config для registry                    | Да           |

См. `config/test_exports_storage_e2e` для примера настройки.

## Отладка

### Просмотр состояния LVMVolumeGroups

```bash
kubectl get lvmvolumegroups -o wide

# Детальная информация
kubectl describe lvmvolumegroup <lvg-name>
```

### Просмотр логов scheduler-extender

```bash
kubectl logs -n d8-sds-node-configurator -l app=sds-common-scheduler-extender -f
```

### Проверка событий Pod

```bash
kubectl describe pod <pod-name> -n <namespace>
```

## Дополнительная информация

- [Документация Ginkgo](https://onsi.github.io/ginkgo/)
- [Документация Gomega](https://onsi.github.io/gomega/)
- [storage-e2e библиотека](https://github.com/deckhouse/storage-e2e)
