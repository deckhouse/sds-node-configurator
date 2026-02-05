#!/bin/bash

# Скрипт для очистки тестового блочного устройства
# Использование: ./cleanup-test-device.sh <vm-name>

set -euo pipefail

VM_NAME="${1:-}"
DISK_PATH="${DISK_PATH:-/var/lib/libvirt/images/e2e-test-disk.img}"
DEVICE_TARGET="${DEVICE_TARGET:-vdb}"
NODE_NAME="${E2E_NODE_NAME:-}"

if [ -z "$VM_NAME" ]; then
    echo "Ошибка: укажите имя виртуальной машины"
    echo "Использование: $0 <vm-name>"
    exit 1
fi

echo "=========================================="
echo "Очистка тестового устройства"
echo "=========================================="
echo "VM Name:         $VM_NAME"
echo "Disk Path:       $DISK_PATH"
echo "Device Target:   $DEVICE_TARGET"
echo "=========================================="

# Отключение диска от VM
if virsh domblklist "$VM_NAME" | grep -q "$DEVICE_TARGET"; then
    echo "Отключение диска от VM..."
    virsh detach-disk "$VM_NAME" "$DEVICE_TARGET" --persistent || {
        echo "⚠️  Не удалось отключить диск. Возможно, он уже отключен."
    }
    echo "✓ Диск отключен"
else
    echo "ℹ️  Диск $DEVICE_TARGET не подключен к VM"
fi

# Удаление файла диска
if [ -f "$DISK_PATH" ]; then
    read -p "Удалить файл диска $DISK_PATH? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        rm -f "$DISK_PATH"
        echo "✓ Файл диска удален"
    else
        echo "ℹ️  Файл диска оставлен: $DISK_PATH"
    fi
else
    echo "ℹ️  Файл диска не найден: $DISK_PATH"
fi

# Очистка BlockDevice в Kubernetes (если задано имя ноды)
if [ -n "$NODE_NAME" ]; then
    echo ""
    echo "Очистка BlockDevice в Kubernetes..."
    
    if command -v kubectl &> /dev/null; then
        # Получаем список BlockDevice на ноде
        BD_LIST=$(kubectl get blockdevice -o json | \
            jq -r ".items[] | select(.status.nodeName == \"$NODE_NAME\" and .status.path == \"/dev/$DEVICE_TARGET\") | .metadata.name" 2>/dev/null || echo "")
        
        if [ -n "$BD_LIST" ]; then
            echo "Найдены BlockDevice для удаления:"
            echo "$BD_LIST"
            read -p "Удалить эти BlockDevice? (y/N): " -n 1 -r
            echo
            if [[ $REPLY =~ ^[Yy]$ ]]; then
                for bd_name in $BD_LIST; do
                    kubectl delete blockdevice "$bd_name" && echo "✓ Удален BlockDevice: $bd_name"
                done
            fi
        else
            echo "ℹ️  BlockDevice не найдены на ноде $NODE_NAME с путем /dev/$DEVICE_TARGET"
        fi
    else
        echo "⚠️  kubectl не найден, пропускаем очистку BlockDevice"
    fi
fi

# Удаление файла с переменными окружения
if [ -f /tmp/e2e-test-env.sh ]; then
    rm -f /tmp/e2e-test-env.sh
    echo "✓ Файл с переменными окружения удален"
fi

echo ""
echo "=========================================="
echo "✓ Очистка завершена!"
echo "=========================================="

