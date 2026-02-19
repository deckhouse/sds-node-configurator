# E2E тесты для sds-node-configurator

Данный каталог содержит end-to-end (e2e) тесты для модуля `sds-node-configurator`.

## Описание

E2E тесты предназначены для проверки полного цикла работы модуля в реальном Kubernetes кластере. Тесты используют фреймворк [Ginkgo](https://onsi.github.io/ginkgo/) для организации тестовых сценариев и [Gomega](https://onsi.github.io/gomega/) для утверждений (assertions).

## Предварительные требования

1. **Kubernetes кластер**: Доступный Kubernetes кластер с установленным модулем `sds-node-configurator`
2. **kubectl**: Настроенный доступ к кластеру через kubectl
3. **Docker**: Для сборки образа тестов (при запуске в кластере)

## Структура

```
e2e/
├── Dockerfile                                   # Образ для запуска тестов в кластере
├── Makefile                                     # Команды для сборки и запуска
├── README.md                                    # Данный файл
├── go.mod                                       # Go модуль
├── go.sum                                       # Зависимости
├── manifests/
│   ├── rbac.yaml                                # ServiceAccount, ClusterRole, ClusterRoleBinding
│   └── job.yaml                                 # Job для запуска тестов
└── tests/
    ├── block_device_discovery_suite_test.go     # Инициализация Ginkgo suite
    ├── block_device_discovery_test.go           # Основные тесты
    └── helpers.go                               # Вспомогательные функции
```

## Тестовые сценарии

### 1. Автоматическое обнаружение нового блочного устройства

**Сценарий**: `Должен обнаружить новый неразмеченный диск и создать объект BlockDevice`

**Описание**:
- На ноде появляется новый неразмеченный диск (например, `/dev/sdb`)
- Через некоторое время в кластере появляется объект BlockDevice
- Проверяется корректность всех полей объекта

**Проверки**:
- ✅ Объект BlockDevice существует
- ✅ `status.nodeName` соответствует имени ноды
- ✅ `status.path` соответствует пути к устройству
- ✅ `status.size` больше 0 (минимум 1Gi)
- ✅ `status.serial` содержит серийный номер устройства
- ✅ `status.consumable` = true для неразмеченного диска
- ✅ `status.fsType` пустой для неразмеченного диска

## Запуск тестов в кластере

### 1. Сборка образа

```bash
cd e2e

# Собрать образ
make docker-build E2E_IMAGE=your-registry/e2e-tests:latest

# Отправить в registry
make docker-push E2E_IMAGE=your-registry/e2e-tests:latest
```

### 2. Запуск тестов

```bash
# Запустить тесты в кластере
make run-in-cluster \
  E2E_IMAGE=your-registry/e2e-tests:latest \
  E2E_NODE_NAME=worker-0 \
  E2E_DEVICE_PATH=/dev/sdb \
  E2E_DEVICE_SERIAL=<серийный номер>

# Просмотреть логи
make logs

# Проверить статус
make status
```

### 3. Очистка

```bash
make cleanup
```

## Переменные окружения

| Переменная         | Описание                              | Значение по умолчанию |
|--------------------|---------------------------------------|-----------------------|
| `E2E_IMAGE`        | Docker образ с тестами                | `e2e-tests:latest`    |
| `E2E_NODE_NAME`    | Имя ноды для тестирования             | `worker-0`            |
| `E2E_DEVICE_PATH`  | Путь к блочному устройству            | `/dev/sdb`            |
| `E2E_DEVICE_SERIAL`| Ожидаемый серийный номер устройства   | (не задан)            |

## Пример полного цикла

```bash
cd e2e

# 1. Проверить серийный номер диска на ноде
kubectl debug node/worker-0 -it --image=alpine -- lsblk -o NAME,SERIAL

# 2. Собрать и отправить образ
make docker-build E2E_IMAGE=registry.example.com/e2e-tests:v1
make docker-push E2E_IMAGE=registry.example.com/e2e-tests:v1

# 3. Запустить тесты
make run-in-cluster \
  E2E_IMAGE=registry.example.com/e2e-tests:v1 \
  E2E_NODE_NAME=worker-0 \
  E2E_DEVICE_PATH=/dev/sdb \
  E2E_DEVICE_SERIAL=ABC123

# 4. Следить за выполнением
make logs

# 5. Очистить после выполнения
make cleanup
```

## Отладка

### Просмотр логов агента

```bash
# Найти под агента на нужной ноде
kubectl get pods -n d8-sds-node-configurator -o wide | grep <node-name>

# Просмотреть логи
kubectl logs -n d8-sds-node-configurator <agent-pod-name> -f
```

### Проверка состояния BlockDevice

```bash
# Получить список всех BlockDevice
kubectl get blockdevice

# Получить детальную информацию
kubectl describe blockdevice <bd-name>

# Получить BlockDevice на конкретной ноде
kubectl get blockdevice -l kubernetes.io/hostname=<node-name>
```

## Устранение неполадок

### Тест не находит BlockDevice

**Возможные причины**:
1. Агент не запущен на ноде
2. Диск не появился в системе (проверьте `lsblk` на ноде)
3. Интервал сканирования агента слишком большой
4. Диск отфильтрован (проверьте фильтры BlockDeviceFilter)

**Решение**:
```bash
# Проверьте статус агента
kubectl get pods -n d8-sds-node-configurator -o wide

# Проверьте логи агента
kubectl logs -n d8-sds-node-configurator <agent-pod> | grep "block-device-controller"

# Проверьте устройство на ноде
kubectl debug node/<node-name> -it --image=alpine -- lsblk
```

### Таймаут при ожидании BlockDevice

Тесты ожидают появления BlockDevice в течение 5 минут. Если агент сканирует устройства реже, увеличьте таймаут в коде теста или проверьте настройки агента.

## Дополнительная информация

- [Документация Ginkgo](https://onsi.github.io/ginkgo/)
- [Документация Gomega](https://onsi.github.io/gomega/)
