# How to Run E2E Smoke Tests

This guide explains how to run the **storage-e2e** smoke tests for `sds-node-configurator` (BlockDevice discovery, LVMVolumeGroup) in CI and locally.

## What the test does

- Uses the [storage-e2e](https://github.com/deckhouse/storage-e2e) framework.
- Entry point: `TestE2E` in `e2e/tests/e2e_suite_test.go` (single Ginkgo run: Common Scheduler scenarios first, then Sds Node Configurator — see file order / `Describe` order).
- Covers BlockDevice discovery and LVMVolumeGroup flows on a test cluster (existing or created via framework).
- Test cluster is reached via SSH (base cluster host or jump host) and kubeconfig (often through an SSH tunnel to the API).

For test scenarios and debugging, see [README.md](README.md).

---

## Running in CI (GitHub Actions)

E2E runs as part of **Build and push for dev** when the **Build and checks** workflow is triggered and the PR has the right label.

### 1. Trigger E2E on a PR

1. Open your pull request.
2. Add the label **`e2e-smoke-test`** to the PR.
3. Push or re-run the workflow. The **Build and checks** workflow will call `build_dev.yml` with `run_e2e_smoke_tests: true`, and the E2E job will run after the image is built.

Removing the label or not adding it means E2E smoke tests will not run.

### 2. Required repository configuration

Configure the following in the repo **Settings → Secrets and variables → Actions**.

#### Secrets (required for E2E)

| Secret | Description |
|--------|-------------|
| `E2E_SSH_PRIVATE_KEY` | Private SSH key content for cluster/VM access (e.g. base cluster master and test VMs). |
| `E2E_SSH_PUBLIC_KEY` | Public key matching the private key (used for VM `authorized_keys`). |
| `E2E_SSH_HOST` | SSH host for the base cluster (e.g. master node). Used for tunnel and as default jump host. |
| `E2E_SSH_USER` | SSH user for the base cluster. |
| `E2E_CLUSTER_KUBECONFIG` | Test cluster kubeconfig, **base64-encoded**. Written to a temp file; `KUBE_CONFIG_PATH` is set from it. |
| `E2E_TEST_CLUSTER_CREATE_MODE` | `alwaysUseExisting` or `alwaysCreateNew` (use existing cluster or create via framework). |
| `E2E_TEST_CLUSTER_STORAGE_CLASS` | Storage class name for the test cluster (e.g. `linstor-r1`). |
| `E2E_TEST_CLUSTER_CLEANUP` | e.g. `true` / `false` — whether to clean up the test cluster after runs. |
| `E2E_DECKHOUSE_LICENSE` | Deckhouse/DKP license key (if creating clusters). |
| `E2E_REGISTRY_DOCKER_CFG` | Registry Docker config (base64) for pulling images (e.g. for VMs). |

#### Optional secrets

| Secret | Description |
|--------|-------------|
| `E2E_SSH_JUMP_HOST` | Jump host for test cluster nodes (e.g. 10.10.10.x). Defaults to `E2E_SSH_HOST` if unset. |
| `E2E_SSH_JUMP_USER` | SSH user on the jump host. Defaults to `E2E_SSH_USER` if unset. |

#### Variables (repository)

| Variable | Description |
|----------|-------------|
| `MODULE_NAME` | Module name (e.g. `sds-node-configurator`). Used in namespace and artifact names. |
| `E2E_LOG_LEVEL` | Optional. e.g. `debug` or `info`. |

### 3. Results

- Workflow run: **Actions** → select the **Build and push for dev** run.
- PR comment: a bot comment reports **E2E Smoke Tests Results** (passed/failed) with a link to the run and **Exit Code**.
- Logs: download **e2e-smoke-test-logs-\<MODULE_NAME\>-\<run_id\>** from the workflow artifacts.

---

## Running locally

Local runs use the same suite and the same environment variables (often via a config file).

### 1. Prepare config (not in git)

The directory `e2e/config/` is in `.gitignore`. Create there a script that exports the same variables the CI uses (with your values), for example:

- `e2e/config/test_exports_storage_e2e` — recommended name used in examples below.

Set at least:

- `TEST_CLUSTER_CREATE_MODE` — `alwaysUseExisting` or `alwaysCreateNew`.
- `SSH_HOST`, `SSH_USER` — test cluster SSH target and user on those nodes; with jump, also set `SSH_JUMP_HOST` / `SSH_JUMP_USER` (see §4).
- `SSH_PRIVATE_KEY` (path) — SSH key for cluster/VMs.
- `KUBE_CONFIG_PATH` (path) — test cluster kubeconfig.
- `TEST_CLUSTER_STORAGE_CLASS`, `TEST_CLUSTER_NAMESPACE`, `TEST_CLUSTER_CLEANUP`.
- For create mode: `DKP_LICENSE_KEY`, `REGISTRY_DOCKER_CFG`.

### 2. Source config and run

From the repo root:

```bash
source e2e/config/test_exports_storage_e2e
cd e2e
make test
```

Or run specific test:

```bash
# Ginkgo focus on a spec name; CI runs: go test ./tests/ -run '^TestE2E$'
make test-focus FOCUS="Should schedule Pod with local PVC"
```

### 3. Cluster lock (stale lock)

If a previous run was interrupted (e.g. Ctrl+C) or failed before cleanup, the framework may leave the cluster locked. You will see: `failed to acquire cluster lock: cluster is already locked`.

To release the lock once (only when no other run is using the cluster):

```bash
export TEST_CLUSTER_FORCE_LOCK_RELEASE='true'
source e2e/config/test_exports_storage_e2e
cd e2e && make test
```

For `alwaysUseExisting`, this suite retries once after clearing a stale lock: first it tries deleting ConfigMap `default/e2e-cluster-lock` via `KUBE_CONFIG_PATH` (works when the API URL is reachable directly). If that fails (common when `server` is `https://127.0.0.1:…` and no tunnel is running yet), it opens the same SSH + port-forward as the test connect and releases the lock. Disable with `E2E_NO_CLUSTER_LOCK_RETRY=true` (e.g. shared cluster).

### 4. Jump host (test cluster nodes)

If test cluster nodes (e.g. 10.10.10.x) are not reachable directly from your machine, set:

- `SSH_JUMP_HOST` — jump host (often the base cluster master).
- `SSH_JUMP_USER` — user on the jump host (defaults to `SSH_USER` if unset).

**Bastion user vs cluster node user (storage-e2e):** With a jump host, the framework connects as `SSH_JUMP_USER@SSH_JUMP_HOST`, then as **`SSH_USER@SSH_HOST`** to reach the test cluster (kubeconfig / API path). It does **not** use `SSH_VM_USER` for that second hop. Typical Deckhouse lab: bastion login `you@bastion`, nodes `cloud@10.x.x.x` — set `SSH_JUMP_USER` to your bastion account and `SSH_USER` to the SSH account on cluster nodes (often `cloud`). Keep `SSH_VM_USER` aligned with node SSH (often also `cloud`).

---

## Quick reference: environment variables

| Variable | Required | Description |
|----------|----------|-------------|
| `TEST_CLUSTER_CREATE_MODE` | Yes | `alwaysUseExisting` \| `alwaysCreateNew` |
| `SSH_HOST` | Yes | Test cluster SSH target (master/API host or node); with jump, second hop address. |
| `SSH_USER` | Yes | SSH user **on test cluster nodes** for the `SSH_HOST` hop (with jump: not the bastion user; set `SSH_JUMP_USER` for that). |
| `SSH_PRIVATE_KEY` | Yes | Path to private SSH key file. |
| `KUBE_CONFIG_PATH` | Yes | Path to test cluster kubeconfig. |
| `TEST_CLUSTER_NAMESPACE` | Yes | Namespace used for test cluster / lock. |
| `TEST_CLUSTER_STORAGE_CLASS` | Yes | Storage class name. |
| `TEST_CLUSTER_CLEANUP` | Yes | e.g. `true` / `false`. |
| `SSH_JUMP_HOST` | If nodes behind jump | Jump host; default = `SSH_HOST`. |
| `SSH_JUMP_USER` | If using jump | Default = `SSH_USER`. |
| `DKP_LICENSE_KEY` | If create mode | License for cluster creation. |
| `REGISTRY_DOCKER_CFG` | If create mode | Registry auth (base64). |
| `LOG_LEVEL` | No | e.g. `debug`, `info`. |
| `TEST_CLUSTER_FORCE_LOCK_RELEASE` | No | Set to `true` once to clear a stale lock (read at process start by storage-e2e). |
| `E2E_NO_CLUSTER_LOCK_RETRY` | No | If `true`, do not delete lock ConfigMap + retry (default: retry once when lock denied). |

---

## See also

- [README.md](README.md) — test scenarios, debugging, troubleshooting.
- [Makefile](Makefile) — `make test`, `make test-focus FOCUS="..."` for local runs.
