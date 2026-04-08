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
2. Add the label **`e2e-smoke-test`** to the PR (this sends a `labeled` event and should start **Build and checks** if Actions are allowed for this PR).
3. The **Build and checks** workflow calls `build_dev.yml`; the **Run E2E Smoke Tests** job runs only when the PR has the `e2e-smoke-test` label, after the dev image build.

Removing the label or not adding it means E2E smoke tests will not run.

**Draft PRs:** If nothing appears under **Actions** when you add the label, the repository or organization may be configured to **skip workflows for draft pull requests**. In that case either mark the PR as ready for review (a `ready_for_review` run is included) or change the Actions policy for draft PRs in **Settings → Actions** (exact option depends on your GitHub plan).

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
- `SSH_HOST`, `SSH_USER` — base cluster (and jump host if needed).
- `SSH_PRIVATE_KEY` (path) — SSH key for cluster/VMs.
- `KUBE_CONFIG_PATH` (path) — test cluster kubeconfig.
- `TEST_CLUSTER_STORAGE_CLASS`, `TEST_CLUSTER_NAMESPACE`, `TEST_CLUSTER_CLEANUP`.
- For create mode: `DKP_LICENSE_KEY`, `REGISTRY_DOCKER_CFG`.

### 2. Source config and run

From the repo root:

```bash
source e2e/config/test_exports_storage_e2e
cd e2e
go mod tidy
ginkgo -v --progress ./tests/
```

Or run specific test:

```bash
# Ginkgo focus on a spec name; CI runs: go test ./tests/ -run '^TestE2E$'
ginkgo -v --progress --focus="Should schedule Pod with local PVC" ./tests/
```

### 3. Cluster lock (stale lock)

If a previous run was interrupted (e.g. Ctrl+C) or failed before cleanup, the framework may leave the cluster locked. You will see: `failed to acquire cluster lock: cluster is already locked`.

To release the lock once (only when no other run is using the cluster):

```bash
export TEST_CLUSTER_FORCE_LOCK_RELEASE='true'
source e2e/config/test_exports_storage_e2e
cd e2e && ginkgo -v --progress ./tests/
```

### 4. Jump host (test cluster nodes)

If test cluster nodes (e.g. 10.10.10.x) are not reachable directly from your machine, set:

- `SSH_JUMP_HOST` — jump host (often the base cluster master).
- `SSH_JUMP_USER` — user on the jump host (defaults to `SSH_USER` if unset).

### 5. `permission denied` under `.../pkg/mod/.../storage-e2e/.../temp`

The storage-e2e library writes bootstrap state under a `temp/` directory inside the **checked-out** `storage-e2e` module in the Go module cache. On self-hosted runners that cache is often under a **shared read-only** path (e.g. `/opt/.../go/pkg/mod`), so `mkdir` fails even after `chmod`.

**Fix (recommended):** point the module cache at a writable directory (CI uses this):

```bash
export GOMODCACHE="$(pwd)/e2e/.gomodcache"
export GOCACHE="$(pwd)/e2e/.gocache"
mkdir -p "$GOMODCACHE" "$GOCACHE"
cd e2e && go mod download && go test ...
```

**Alternative (local):** `make deps` from `e2e/` runs `fix-mod-permissions` (chmod + `mkdir` under your current `GOPATH`/`GOMODCACHE`), which helps only if that cache is writable by your user.

### 6. Virtualization module stuck in `Reconciling`

storage-e2e checks the Deckhouse `Module/virtualization` once with a short timeout. Before nested cluster creation (`TEST_CLUSTER_CREATE_MODE=alwaysCreateNew`), the suite polls `Module/virtualization` via the Kubernetes API until `status.phase == Ready` (uses `KUBE_CONFIG_PATH`). Override total wait with `E2E_VIRTUALIZATION_MODULE_WAIT_TIMEOUT` (e.g. `30m`). To disable this pre-wait entirely, set `E2E_SKIP_VIRTUALIZATION_MODULE_WAIT=true`.

---

## Quick reference: environment variables

| Variable | Required | Description |
|----------|----------|-------------|
| `TEST_CLUSTER_CREATE_MODE` | Yes | `alwaysUseExisting` \| `alwaysCreateNew` |
| `SSH_HOST` | Yes | Base cluster SSH host (and default jump host). |
| `SSH_USER` | Yes | SSH user. |
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
| `TEST_CLUSTER_FORCE_LOCK_RELEASE` | No | Set to `true` once to clear a stale lock. |
| `E2E_VIRTUALIZATION_MODULE_WAIT_TIMEOUT` | No | Max wait for Module `virtualization` Ready before nested cluster create (default ~25m). |
| `E2E_SKIP_VIRTUALIZATION_MODULE_WAIT` | No | Set to `true` to skip the Module pre-wait (not recommended if you hit Reconciling flakes). |

---

## See also

- [README.md](README.md) — test scenarios, debugging, troubleshooting.
- Local runs: `ginkgo -v --progress ./tests/` or `go test -v -count=1 -timeout 60m ./tests/ -run '^TestE2E$'` (same as CI).
