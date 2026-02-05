#!/bin/bash

# Скрипт для подготовки тестового блочного устройства
# Использование: ./prepare-test-device.sh <vm-name> <node-name>

set -euo pipefail

VM_NAME="${1:-}"
NODE_NAME="${2:-worker-0}"
DISK_SIZE="${DISK_SIZE:-10G}"
DISK_PATH="${DISK_PATH:-/var/lib/libvirt/images/e2e-test-disk.img}"
DEVICE_TARGET="${DEVICE_TARGET:-vdb}"
SERIAL_NUMBER="${SERIAL_NUMBER:-E2E-TEST-DISK-$(date +%s)}"

if [ -z "$VM_NAME" ]; then
    echo "Ошибка: укажите имя виртуальной машины"
    echo "Использование: $0 <vm-name> [node-name]"
    exit 1
fi

echo "=========================================="
echo "Подготовка тестового устройства"
echo "=========================================="
echo "VM Name:         $VM_NAME"
echo "Node Name:       $NODE_NAME"
echo "Disk Path:       $DISK_PATH"
echo "Disk Size:       $DISK_SIZE"
echo "Device Target:   $DEVICE_TARGET"
echo "Serial Number:   $SERIAL_NUMBER"
echo "=========================================="

# Проверка, существует ли уже диск
if [ -f "$DISK_PATH" ]; then
    echo "⚠️  Диск $DISK_PATH уже существует"
    read -p "Удалить существующий диск и создать новый? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        rm -f "$DISK_PATH"
        echo "✓ Старый диск удален"
    else
        echo "Используем существующий диск"
    fi
fi

# Создание диска, если он не существует
if [ ! -f "$DISK_PATH" ]; then
    echo "Создание виртуального диска..."
    qemu-img create -f raw "$DISK_PATH" "$DISK_SIZE"
    echo "✓ Диск создан: $DISK_PATH"
fi

# Проверка, запущена ли VM
if ! virsh domstate "$VM_NAME" | grep -q "running"; then
    echo "⚠️  Виртуальная машина $VM_NAME не запущена"
    read -p "Запустить VM? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        virsh start "$VM_NAME"
        echo "✓ VM запущена"
        sleep 5
    else
        echo "Пожалуйста, запустите VM вручную"
        exit 1
    fi
fi

# Проверка, подключен ли уже диск
if virsh domblklist "$VM_NAME" | grep -q "$DEVICE_TARGET"; then
    echo "⚠️  Устройство $DEVICE_TARGET уже подключено к VM"
    read -p "Отключить существующее устройство и подключить новое? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        virsh detach-disk "$VM_NAME" "$DEVICE_TARGET" --persistent || true
        sleep 2
        echo "✓ Старое устройство отключено"
    else
        echo "Используем существующее подключение"
        exit 0
    fi
fi

# Подключение диска к VM
echo "Подключение диска к VM..."
virsh attach-disk "$VM_NAME" \
    --source "$DISK_PATH" \
    --target "$DEVICE_TARGET" \
    --persistent \
    --driver qemu \
    --subdriver raw \
    --serial "$SERIAL_NUMBER"

echo "✓ Диск подключен к VM"

# Ожидание появления устройства в системе
echo "Ожидание появления устройства в системе (это может занять несколько секунд)..."
sleep 3

echo ""
echo "=========================================="
echo "✓ Подготовка завершена!"
echo "=========================================="
echo ""
echo "Экспортируйте следующие переменные окружения для запуска тестов:"
echo ""
echo "  export E2E_NODE_NAME=\"$NODE_NAME\""
echo "  export E2E_DEVICE_PATH=\"/dev/$DEVICE_TARGET\""
echo "  export E2E_DEVICE_SERIAL=\"$SERIAL_NUMBER\""
echo ""
echo "Затем запустите тесты:"
echo ""
echo "  cd e2e && go test -v ./tests/"
echo ""
echo "Или используйте Makefile:"
echo ""
echo "  E2E_NODE_NAME=\"$NODE_NAME\" E2E_DEVICE_PATH=\"/dev/$DEVICE_TARGET\" E2E_DEVICE_SERIAL=\"$SERIAL_NUMBER\" make -C e2e test"
echo ""

# Сохранение переменных в файл для удобства
cat > /tmp/e2e-test-env.sh <<EOF
# E2E Test Environment Variables
# Сгенерировано: $(date)
export E2E_NODE_NAME="$NODE_NAME"
export E2E_DEVICE_PATH="/dev/$DEVICE_TARGET"
export E2E_DEVICE_SERIAL="$SERIAL_NUMBER"
EOF

echo "Переменные окружения также сохранены в: /tmp/e2e-test-env.sh"
echo "Вы можете загрузить их командой: source /tmp/e2e-test-env.sh"
echo ""

