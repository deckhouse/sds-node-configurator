# E2E тесты для sds-node-configurator

Данный каталог содержит end-to-end (e2e) тесты для модуля `sds-node-configurator`.

## Описание

E2E тесты предназначены для проверки полного цикла работы модуля в реальном Kubernetes кластере. Тесты используют фреймворк [Ginkgo](https://onsi.github.io/ginkgo/) для организации тестовых сценариев и [Gomega](https://onsi.github.io/gomega/) для утверждений (assertions).

## Предварительные требования

1. **Kubernetes кластер**: Доступный Kubernetes кластер с установленным модулем `sds-node-configurator`
2. **kubectl**: Настроенный доступ к кластеру через kubectl (файл `~/.kube/config`)
3. **Go**: Версия Go 1.24.9 или выше
4. **Права доступа**: Достаточные права для создания/чтения/удаления ресурсов BlockDevice

## Структура тестов

```
e2e/
├── README.md                                    # Данный файл
├── go.mod                                       # Go модуль для e2e тестов
├── go.sum                                       # Зависимости Go
└── tests/
    ├── block_device_discovery_suite_test.go     # Инициализация тестового окружения
    ├── block_device_discovery_test.go           # Основные e2e тесты
    └── helpers.go                               # Вспомогательные функции
```

## Тестовые сценарии

### 1. Автоматическое обнаружение нового блочного устройства

**Сценарий**: `Должен обнаружить новый неразмеченный диск и создать объект BlockDevice`

**Описание**:
- На ноде появляется новый неразмеченный диск (например, `/dev/vdb`)
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
- ✅ Имя ресурса BlockDevice вычислено на основе серийника

## Запуск тестов

### Подготовка окружения

1. **Установите зависимости**:
```bash
cd e2e
go mod tidy
```

2. **Подготовьте тестовую ноду**:
   - Убедитесь, что на ноде есть агент `sds-node-configurator`
   - Подключите новое блочное устройство к ноде (например, через виртуализацию)
   - **Важно**: Задайте серийный номер устройству при подключении

### Пример подключения устройства в libvirt/KVM:

```bash
# Создайте виртуальный диск
qemu-img create -f raw /var/lib/libvirt/images/test-disk.img 10G

# Подключите диск к виртуальной машине с заданным серийником
virsh attach-disk <vm-name> \
  --source /var/lib/libvirt/images/test-disk.img \
  --target vdb \
  --persistent \
  --driver qemu \
  --subdriver raw \
  --serial "E2E-TEST-DISK-001"
```

### Запуск тестов

#### Базовый запуск

```bash
cd e2e
go test -v ./tests/
```

#### Запуск с параметрами

```bash
# Указать конкретную ноду для тестирования
export E2E_NODE_NAME="worker-0"

# Указать путь к устройству
export E2E_DEVICE_PATH="/dev/vdb"

# Указать ожидаемый серийный номер (рекомендуется)
export E2E_DEVICE_SERIAL="E2E-TEST-DISK-001"

go test -v ./tests/
```

#### Запуск с использованием Ginkgo CLI

```bash
# Установка Ginkgo CLI (если еще не установлен)
go install github.com/onsi/ginkgo/v2/ginkgo@latest

# Запуск тестов
cd e2e
ginkgo -v ./tests/

# Запуск с подробным выводом
ginkgo -v --progress ./tests/

# Запуск конкретного теста
ginkgo -v --focus="Автоматическое обнаружение" ./tests/
```

## Переменные окружения

| Переменная         | Описание                                      | Значение по умолчанию |
|--------------------|-----------------------------------------------|-----------------------|
| `E2E_NODE_NAME`    | Имя ноды для тестирования                     | `worker-0`            |
| `E2E_DEVICE_PATH`  | Путь к блочному устройству                    | `/dev/vdb`            |
| `E2E_DEVICE_SERIAL`| Ожидаемый серийный номер устройства           | (не задан)            |

## Примеры использования

### Тест 1: Базовая проверка обнаружения устройства

```bash
# 1. Подключите диск к ноде worker-0 с серийником
virsh attach-disk worker-0 \
  --source /path/to/disk.img \
  --target vdb \
  --serial "TEST-001" \
  --persistent

# 2. Запустите тест
export E2E_NODE_NAME="worker-0"
export E2E_DEVICE_PATH="/dev/vdb"
export E2E_DEVICE_SERIAL="TEST-001"

cd e2e
go test -v ./tests/ -timeout 10m
```

### Тест 2: Проверка на другой ноде

```bash
export E2E_NODE_NAME="worker-1"
export E2E_DEVICE_PATH="/dev/vdc"
export E2E_DEVICE_SERIAL="TEST-002"

cd e2e
ginkgo -v ./tests/
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

### Ручная очистка после тестов

```bash
# Удалить BlockDevice
kubectl delete blockdevice <bd-name>

# Отключить диск от ноды
virsh detach-disk <vm-name> vdb --persistent

# Удалить тестовый диск
rm /path/to/test-disk.img
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
kubectl debug node/<node-name> -it --image=ubuntu -- lsblk
```

### Неправильное имя BlockDevice

**Причина**: Имя BlockDevice генерируется на основе хэша от `nodeName`, `wwn`, `model`, `serial` и `partUUID`.

**Решение**: Убедитесь, что серийный номер устройства задан корректно:
```bash
# На ноде проверьте серийник
udevadm info --query=all --name=/dev/vdb | grep ID_SERIAL
```

### Таймаут при ожидании BlockDevice

**Решение**: Увеличьте таймаут теста:
```bash
go test -v ./tests/ -timeout 15m
```

## Интеграция в CI/CD

### Пример для GitHub Actions

```yaml
name: E2E Tests

on:
  pull_request:
  push:
    branches: [main]

jobs:
  e2e:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Set up Go
        uses: actions/setup-go@v4
        with:
          go-version: '1.24'
      
      - name: Set up Kubernetes cluster
        uses: helm/kind-action@v1
      
      - name: Deploy sds-node-configurator
        run: |
          # Deploy your module here
          kubectl apply -f deploy/
      
      - name: Run E2E tests
        run: |
          cd e2e
          go test -v ./tests/ -timeout 15m
        env:
          E2E_NODE_NAME: kind-control-plane
          E2E_DEVICE_PATH: /dev/vdb
```

## Дополнительная информация

- [Документация Ginkgo](https://onsi.github.io/ginkgo/)
- [Документация Gomega](https://onsi.github.io/gomega/)
- [Документация sds-node-configurator](../docs/)
- [API документация BlockDevice](../api/v1alpha1/block_device.go)

## Вклад в разработку

При добавлении новых e2e тестов:

1. Следуйте структуре существующих тестов
2. Используйте описательные названия тестовых сценариев
3. Добавляйте комментарии для сложной логики
4. Обновляйте данный README при добавлении новых сценариев
5. Используйте вспомогательные функции из `helpers.go`

## Контакты

При возникновении вопросов или проблем создайте issue в репозитории проекта.

