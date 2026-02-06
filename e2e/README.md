# E2E tests for sds-node-configurator

This directory contains end-to-end (e2e) tests for the `sds-node-configurator` module.

## Description

E2E tests are intended to verify the full lifecycle of the module in a real Kubernetes cluster. The tests use the [Ginkgo](https://onsi.github.io/ginkgo/) framework for organizing test scenarios and [Gomega](https://onsi.github.io/gomega/) for assertions.

## Prerequisites

1. **Kubernetes cluster**: An accessible Kubernetes cluster with the `sds-node-configurator` module installed
2. **kubectl**: Configured access to the cluster via kubectl (file `~/.kube/config`)
3. **Go**: Go version 1.24.9 or higher
4. **Permissions**: Sufficient permissions to create/read/delete BlockDevice resources

## Test structure

```
e2e/
├── README.md                                    # This file
├── go.mod                                       # Go module for e2e tests
├── go.sum                                       # Go dependencies
└── tests/
    ├── block_device_discovery_suite_test.go     # Test environment initialization
    ├── block_device_discovery_test.go           # Main e2e tests
    └── helpers.go                               # Helper functions
```

## Test scenarios

### 1. Automatic discovery of a new block device

**Scenario**: `Should discover a new unformatted disk and create a BlockDevice object`

**Description**:
- A new unformatted disk appears on the node (e.g. `/dev/sdb`)
- After some time, a BlockDevice object appears in the cluster
- All fields of the object are verified for correctness

**Checks**:
- ✅ BlockDevice object exists
- ✅ `status.nodeName` matches the node name
- ✅ `status.path` matches the device path
- ✅ `status.size` is greater than 0 (minimum 1Gi)
- ✅ `status.serial` contains the device serial number
- ✅ `status.consumable` = true for unformatted disk
- ✅ `status.fsType` is empty for unformatted disk
- ✅ BlockDevice resource name is computed from the serial number

## Running tests

### Environment setup

1. **Install dependencies**:
```bash
cd e2e
go mod tidy
```

2. **Prepare the test node**:
   - Ensure the `sds-node-configurator` agent is running on the node
   - Attach a new block device to the node
   - Ensure the device has a serial number (check with `lsblk -o NAME,SERIAL`)

### Running tests

#### Running via Ginkgo suite

```bash
# Install Ginkgo CLI (if not already installed)
go install github.com/onsi/ginkgo/v2/ginkgo@latest

cd e2e
ginkgo -v --progress ./tests/

# Run with parameters
export E2E_NODE_NAME="worker-0"
export E2E_DEVICE_PATH="/dev/sdb"
export E2E_DEVICE_SERIAL="<device serial number>"
ginkgo -v --progress ./tests/

# Run a specific test
ginkgo -v --progress --focus="Automatic discovery" ./tests/
```

## Environment variables

| Variable           | Description                              | Default    |
|--------------------|------------------------------------------|------------|
| `E2E_NODE_NAME`    | Node name for testing                    | `worker-0` |
| `E2E_DEVICE_PATH`  | Path to the block device                 | `/dev/sdb` |
| `E2E_DEVICE_SERIAL`| Expected device serial number            | (not set)  |

## Usage examples

### Test 1: Basic device discovery check

```bash
# 1. Attach a disk to the node and check its serial number
lsblk -o NAME,SERIAL

# 2. Run the test
export E2E_NODE_NAME="worker-0"
export E2E_DEVICE_PATH="/dev/sdb"
export E2E_DEVICE_SERIAL="<serial from lsblk>"

cd e2e
ginkgo -v --progress --timeout 10m ./tests/
```

### Test 2: Check on a different node

```bash
export E2E_NODE_NAME="worker-1"
export E2E_DEVICE_PATH="/dev/sdc"
export E2E_DEVICE_SERIAL="<device serial number>"

cd e2e
ginkgo -v --progress ./tests/
```

## Debugging

### Viewing agent logs

```bash
# Find the agent pod on the target node
kubectl get pods -n d8-sds-node-configurator -o wide | grep <node-name>

# View logs
kubectl logs -n d8-sds-node-configurator <agent-pod-name> -f
```

### Checking BlockDevice state

```bash
# List all BlockDevices
kubectl get blockdevice

# Get detailed information
kubectl describe blockdevice <bd-name>

# Get BlockDevices on a specific node
kubectl get blockdevice -l kubernetes.io/hostname=<node-name>
```

### Manual cleanup after tests

```bash
# Delete BlockDevice
kubectl delete blockdevice <bd-name>

# Detach the disk from the node (depends on attachment method)
```

## Troubleshooting

### Test does not find BlockDevice

**Possible causes**:
1. Agent is not running on the node
2. Disk did not appear in the system (check `lsblk` on the node)
3. Agent scan interval is too long
4. Disk is filtered (check BlockDeviceFilter filters)

**Solution**:
```bash
# Check agent status
kubectl get pods -n d8-sds-node-configurator -o wide

# Check agent logs
kubectl logs -n d8-sds-node-configurator <agent-pod> | grep "block-device-controller"

# Check device on the node
kubectl debug node/<node-name> -it --image=ubuntu -- lsblk
```

### Incorrect BlockDevice name

**Cause**: BlockDevice name is generated from a hash of `nodeName`, `wwn`, `model`, `serial`, and `partUUID`.

**Solution**: Ensure the device serial number is set correctly:
```bash
# On the node, check the serial
udevadm info --query=all --name=/dev/vdb | grep ID_SERIAL
```

### Timeout waiting for BlockDevice

**Solution**: Increase the test timeout:
```bash
ginkgo -v --progress --timeout 15m ./tests/
```

## CI/CD integration

### Example for GitHub Actions

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
          ginkgo -v --progress --timeout 15m ./tests/
        env:
          E2E_NODE_NAME: kind-control-plane
          E2E_DEVICE_PATH: /dev/sdb
```

## Additional information

- [Ginkgo documentation](https://onsi.github.io/ginkgo/)
- [Gomega documentation](https://onsi.github.io/gomega/)
- [sds-node-configurator documentation](../docs/)
- [BlockDevice API documentation](../api/v1alpha1/block_device.go)

## Contributing

When adding new e2e tests:

1. Follow the structure of existing tests
2. Use descriptive names for test scenarios
3. Add comments for complex logic
4. Update this README when adding new scenarios
5. Use helper functions from `helpers.go`

## Contact

If you have questions or issues, please create an issue in the project repository.
