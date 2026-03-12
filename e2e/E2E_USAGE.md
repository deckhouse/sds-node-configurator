# How to Run E2E Smoke Tests

This guide explains how to run the **storage-e2e** smoke tests for `sds-node-configurator` (BlockDevice discovery, LVMVolumeGroup) in CI and locally.

## What the test does

- Uses the [storage-e2e](https://github.com/deckhouse/storage-e2e) framework.
- Suite: `TestSdsNodeConfigurator` in `e2e/tests/`.
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

Configure the following in the repo **Settings → Secrets and variables → Actions** (and, if used, **Environments**).

#### Secrets (required for E2E)

| Secret | Description |
|--------|-------------|
| `E2E_SSH_PRIVATE_KEY` | Private SSH key content for cluster/VM access (e.g. base cluster master and test VMs). |
| `E2E_SSH_PUBLIC_KEY` | Public key matching the private key (used for VM `authorized_keys`). Must be the one from `ssh-keygen -y -f <private_key_file>`. |
| `E2E_SSH_HOST` | SSH host for the base cluster (e.g. master node). Used for tunnel and as default jump host. |
| `E2E_SSH_USER` | SSH user for the base cluster. |
| `E2E_CLUSTER_KUBECONFIG` | Test cluster kubeconfig, **base64-encoded**. Written to a file in the job; `KUBE_CONFIG_PATH` is set from it. |
| `E2E_TEST_CLUSTER_CREATE_MODE` | `alwaysUseExisting` or `alwaysCreateNew` (use existing cluster or create via framework). |
| `E2E_TEST_CLUSTER_NAMESPACE` | Not used directly; namespace is derived from `MODULE_NAME`, PR number, and run ID in the workflow. |
| `E2E_TEST_CLUSTER_STORAGE_CLASS` | Storage class name for the test cluster (e.g. `linstor-r1`). |
| `E2E_TEST_CLUSTER_CLEANUP` | e.g. `true` / `false` — whether to clean up the test cluster after runs. |
| `E2E_DECKHOUSE_LICENSE` | Deckhouse/DKP license key (if creating clusters). |
| `E2E_REGISTRY_DOCKER_CFG` | Registry Docker config (base64) for pulling images (e.g. for VMs). |

#### Optional secrets

| Secret | Description |
|--------|-------------|
| `E2E_SSH_JUMP_HOST` | Jump host for test cluster nodes (e.g. 10.10.10.x). Defaults to `E2E_SSH_HOST` if unset. |
| `E2E_SSH_JUMP_USER` | SSH user on the jump host. Defaults to `E2E_SSH_USER` if unset. |

#### Variables (repository or environment)

| Variable | Description |
|----------|-------------|
| `MODULE_NAME` | Module name (e.g. `sds-node-configurator`). Used in namespace and artifact names. |
| `E2E_LOG_LEVEL` | Optional. e.g. `debug` or `info`. |

### 3. Results

- Workflow run: **Actions** → select the **Build and push for dev** run.
- PR comment: a bot comment reports **E2E Smoke Tests Results** (passed/failed) with a link to the run and **Exit Code**.
- Logs: download **e2e-smoke-test-logs-&lt;MODULE_NAME&gt;-&lt;run_id&gt;** from the workflow artifacts.

---

## Running locally

Local runs use the same suite and the same environment variables (often via a config file).

### 1. Prepare config (not in git)

The directory `e2e/config/` is in `.gitignore`. Create there a script that exports the same variables the CI uses (with your values), for example:

- `e2e/config/test_exports_storage_e2e` — recommended name used in examples below.

Set at least:

- `TEST_CLUSTER_CREATE_MODE` — `alwaysUseExisting` or `alwaysCreateNew`.
- `SSH_HOST`, `SSH_USER` — base cluster (and jump host if needed).
- `E2E_SSH_PRIVATE_KEY` or `SSH_PRIVATE_KEY` (path) — SSH key for cluster/VMs.
- `E2E_CLUSTER_KUBECONFIG` (base64) or `KUBE_CONFIG_PATH` (path) — test cluster kubeconfig.
- `TEST_CLUSTER_STORAGE_CLASS`, `TEST_CLUSTER_NAMESPACE`, `TEST_CLUSTER_CLEANUP`.
- For create mode: `E2E_DKP_LICENSE_KEY` / `DKP_LICENSE_KEY`, `E2E_REGISTRY_DOCKER_CFG` / `REGISTRY_DOCKER_CFG`.

You can copy the structure from `e2e/config/test_exports_storage_e2e` if it exists as a template (without committing real secrets). Use `E2E_*` names and, where the framework expects different names (e.g. `DKP_LICENSE_KEY`), export both or set the one the framework reads.

### 2. Source config and run

From the repo root:

```bash
source e2e/config/test_exports_storage_e2e   # or your config file
cd e2e
go test -v -count=1 -timeout 60m ./tests/ -run TestSdsNodeConfigurator
```

Or in one line:

```bash
source e2e/config/test_exports_storage_e2e && cd e2e && go test -v -count=1 -timeout 60m ./tests/ -run TestSdsNodeConfigurator
```

### 3. Cluster lock (stale lock)

If a previous run was interrupted (e.g. Ctrl+C) or failed before cleanup, the framework may leave the cluster locked. You will see something like: `failed to acquire cluster lock: cluster is already locked`.

To release the lock once (only when no other run is using the cluster):

```bash
export TEST_CLUSTER_FORCE_LOCK_RELEASE='true'
source e2e/config/test_exports_storage_e2e
cd e2e && go test -v -run TestSdsNodeConfigurator ./tests/
```

You can add `export TEST_CLUSTER_FORCE_LOCK_RELEASE='true'` to your config temporarily, or leave it unset after the first successful run.

### 4. Jump host (test cluster nodes)

If test cluster nodes (e.g. 10.10.10.x) are not reachable directly from your machine, set:

- `SSH_JUMP_HOST` — jump host (often the base cluster master).
- `SSH_JUMP_USER` — user on the jump host (defaults to `SSH_USER` if unset).

Then SSH to nodes will go through the jump host.

---

## Quick reference: environment variables

| Variable | Required | Description |
|----------|----------|-------------|
| `TEST_CLUSTER_CREATE_MODE` | Yes | `alwaysUseExisting` \| `alwaysCreateNew` |
| `SSH_HOST` | Yes | Base cluster SSH host (and default jump host). |
| `SSH_USER` | Yes | SSH user. |
| `SSH_PRIVATE_KEY` | Yes* | Path to private SSH key file. (*Or provide key content via setup that writes to file and sets this.) |
| `E2E_SSH_PRIVATE_KEY` | (CI / local) | Private key **content**; CI writes it to a file and sets `SSH_PRIVATE_KEY`. |
| `E2E_SSH_PUBLIC_KEY` | Recommended | Public key content (must match private). |
| `KUBE_CONFIG_PATH` | Yes* | Path to test cluster kubeconfig. (*CI sets it from `E2E_CLUSTER_KUBECONFIG`.) |
| `E2E_CLUSTER_KUBECONFIG` | (CI / local) | Base64-encoded kubeconfig; script or CI writes to file and sets `KUBE_CONFIG_PATH`. |
| `TEST_CLUSTER_NAMESPACE` | Yes | Namespace used for test cluster / lock. |
| `TEST_CLUSTER_STORAGE_CLASS` | Yes | Storage class name. |
| `TEST_CLUSTER_CLEANUP` | Yes | e.g. `true` / `false`. |
| `SSH_JUMP_HOST` | If nodes behind jump | Jump host; default = `SSH_HOST`. |
| `SSH_JUMP_USER` | If using jump | Default = `SSH_USER`. |
| `DKP_LICENSE_KEY` / `E2E_DKP_LICENSE_KEY` | If create mode | License for cluster creation. |
| `REGISTRY_DOCKER_CFG` / `E2E_REGISTRY_DOCKER_CFG` | If create mode | Registry auth (base64). |
| `LOG_LEVEL` | No | e.g. `debug`, `info`. |
| `TEST_CLUSTER_FORCE_LOCK_RELEASE` | No | Set to `true` once to clear a stale lock. |

---

## See also

- [README.md](README.md) — test scenarios, running in cluster (Docker/Job), debugging, troubleshooting.
- [Makefile](Makefile) — `make test`, `make test-focus FOCUS="...` for local runs; `make run-in-cluster` for in-cluster Job.
