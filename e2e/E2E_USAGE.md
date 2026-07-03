# How to Run E2E Smoke Tests

This guide explains how to run the **storage-e2e** smoke tests for `sds-node-configurator` (BlockDevice discovery, LVMVolumeGroup) in CI and locally.

## What the test does

- Uses the [storage-e2e](https://github.com/deckhouse/storage-e2e) framework.
- Entry point: `TestSdsNodeConfigurator` in `e2e/tests/sds_node_configurator_suite_test.go` (single Ginkgo run; nested cluster in `BeforeSuite`; Common Scheduler + Sds specs in package).
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
go mod tidy
ginkgo -v --progress --label-filter=e2e-tests ./tests/
```

Or run specific test:

```bash
# Ginkgo focus on a spec name; CI runs: go test ./tests/ -run '^TestSdsNodeConfigurator$'
ginkgo -v --progress --label-filter=e2e-tests --focus="Should schedule Pod with local PVC" ./tests/
```

### Ginkgo labels (CI / local filter)

Specs are tagged for selective runs:

| Label | Specs |
|-------|--------|
| `e2e-tests` | Smoke e2e (scheduler, BlockDevice, LVMVolumeGroup, …) — **default** (suite, CI, `make test`) |
| `stress-test` | Max independent LVMVolumeGroups per node — **not run by default** |

Without `-ginkgo.label-filter`, `TestSdsNodeConfigurator` applies label filter `e2e-tests` automatically. Stress runs only with `make test-stress`, `-ginkgo.label-filter=stress-test`, or `E2E_GINKGO_LABEL_FILTER=stress-test`. Full package: `E2E_GINKGO_LABEL_FILTER='e2e-tests || stress-test'` or `E2E_GINKGO_LABEL_FILTER=all`.

```bash
# Smoke only (same as CI default)
go test -v -count=1 -timeout 3h30m ./tests/ -run '^TestSdsNodeConfigurator$' -ginkgo.label-filter=e2e-tests

# Stress only
go test -v -count=1 -timeout 240m ./tests/ -run '^TestSdsNodeConfigurator$' -ginkgo.label-filter=stress-test

# Override in CI via env
export E2E_GINKGO_LABEL_FILTER=stress-test
```

Focus only: `ginkgo -v --label-filter=stress-test ./tests/`

### 3. Cluster lock (stale lock)

If a previous run was interrupted (e.g. Ctrl+C) or failed before cleanup, the framework may leave the cluster locked. You will see: `failed to acquire cluster lock: cluster is already locked`.

To release the lock once (only when no other run is using the cluster):

```bash
export TEST_CLUSTER_FORCE_LOCK_RELEASE='true'
source e2e/config/test_exports_storage_e2e
cd e2e && ginkgo -v --progress --label-filter=e2e-tests ./tests/
```

For `alwaysUseExisting`, this suite retries once after clearing a stale lock: first it tries deleting ConfigMap `default/e2e-cluster-lock` via `KUBE_CONFIG_PATH` (works when the API URL is reachable directly). If that fails (common when `server` is `https://127.0.0.1:…` and no tunnel is running yet), it opens the same SSH + port-forward as the test connect and releases the lock. Disable with `E2E_NO_CLUSTER_LOCK_RETRY=true` (e.g. shared cluster).

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

> Note: the old `make fix-mod-permissions` workaround (chmod + `mkdir` inside the storage-e2e module cache, plus local source patches under `patches/`) has been removed. With storage-e2e ≥ the dvp-provider revision the suite no longer writes under the module path, so no cache patching is needed.

### 6. Virtualization module stuck in `Reconciling`

storage-e2e checks the Deckhouse `Module/virtualization` once with a short timeout. Before nested cluster creation (`TEST_CLUSTER_CREATE_MODE=alwaysCreateNew`), the suite polls `Module/virtualization` via the Kubernetes API until `status.phase == Ready` (uses `KUBE_CONFIG_PATH`). Override total wait with `E2E_VIRTUALIZATION_MODULE_WAIT_TIMEOUT` (e.g. `30m`). To disable this pre-wait entirely, set `E2E_SKIP_VIRTUALIZATION_MODULE_WAIT=true`.

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
| `TEST_CLUSTER_FORCE_LOCK_RELEASE` | No | Set to `true` once to clear a stale lock. |
| `E2E_VIRTUALIZATION_MODULE_WAIT_TIMEOUT` | No | Max wait for Module `virtualization` Ready before nested cluster create (default ~25m). |
| `E2E_SKIP_VIRTUALIZATION_MODULE_WAIT` | No | Set to `true` to skip the Module pre-wait (not recommended if you hit Reconciling flakes). |
| `E2E_GINKGO_LABEL_FILTER` | No | Ginkgo label filter for CI/local (default in workflow: `e2e-tests`). Use `stress-test` for max-VG stress. |
| `E2E_TEST_TIMEOUT` | No | Ginkgo suite timeout only on CI (default `3h30m`). CI workflow uses a **fixed** `go test -timeout 3h30m` so org/repo vars cannot shorten it to `60m`. Local default `90m`. |

### Stress: maximum VGs per node

Spec **`Stress: maximum independent LVMVolumeGroups per node`** (`sds_node_configurator_stress_max_vgs_test.go`), label **`stress-test`** (excluded from CI smoke; smoke uses **`e2e-tests`**). LVM2 has no fixed VG count limit; the test ramps **one VirtualDisk → one BlockDevice → one LVMVolumeGroup (one VG)** per slot on a single node in batches and prints an empirical report (`Ready` count, on-node `vgs`/`pvs` totals).

Optional tuning: `E2E_STRESS_MAX_VG_TARGET` (default 15), `E2E_STRESS_MAX_VM_BLOCK_DEVICES` (default 15; Deckhouse virt allows 16 block devices per VM), `E2E_STRESS_MAX_VG_BATCH_SIZE`, `E2E_STRESS_MAX_VG_DISK_SIZE`, `E2E_STRESS_MAX_VG_STRICT`, `E2E_STRESS_MAX_VG_MIN_READY`. The ramp stops gracefully when the VM attachment limit is hit.

Focus or label: `ginkgo -v --label-filter=stress-test ./tests/`

---

## See also

- [README.md](README.md) — test scenarios, debugging, troubleshooting.
- Local smoke (same as CI): `make -C e2e test-go` or `go test ... -ginkgo.label-filter=e2e-tests`
- Full package including stress: `go test ... -ginkgo.label-filter='e2e-tests || stress-test'` or `-ginkgo.label-filter=stress-test` for stress only
