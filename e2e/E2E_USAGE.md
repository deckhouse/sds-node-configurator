# Running the sds-node-configurator E2E tests

This is the detailed guide for the `sds-node-configurator` end-to-end suite. For a
short overview and package layout see [`README.md`](README.md).

## What the suite does & architecture

- **Attach-only, no bootstrap.** The suite connects to an **already-provisioned**
  cluster through the [storage-e2e](https://github.com/deckhouse/storage-e2e) SDK
  (`e2e.Connect`). Provisioning/teardown of the cluster is a separate operation
  handled by CI (the storage-e2e reusable workflow) — the Go test binary never
  bootstraps a cluster.
- **Single entry point.** `TestSdsNodeConfigurator` in
  `tests/sds_node_configurator_suite_test.go` registers the Ginkgo fail handler,
  computes the suite timeout, and calls `RunSpecs`. There is no `BeforeSuite`
  cluster setup.
- **Per-spec connection.** Each spec file connects in its own `BeforeAll`:

  ```go
  cl, err = e2e.Connect(ctx, e2e.WithTestName("block-device-discovery"))
  Expect(err).NotTo(HaveOccurred())
  DeferCleanup(func() { _ = cl.Close(context.Background()) })

  k8sClient, err = sdsclient.New(cl.RESTConfig()) // controller-runtime client
  conf, err = cfg.Load()                          // suite config from env
  ```

- **Cluster lease.** `e2e.Connect` acquires a `coordination.k8s.io/v1` Lease
  (ref-counted, renewed in the background); a stale lease from a dead run
  self-expires. `cl.Close` releases it.
- **Serial-only.** Specs share one cluster, pick the same node
  (`Nodes().List()[0]`), use common resource-name prefixes, and share the
  cluster lease. Running with `ginkgo -p` / `--procs` or `go test -parallel`
  (multiple processes) causes lease contention and races — **do not** enable
  parallelism.
- **Config is stateless.** `cfg.Load()` returns a fresh `*Config`; stress config
  is loaded lazily and separately via `cfg.LoadStress()` inside the stress spec's
  `BeforeAll`, so a broken `E2E_STRESS_*` value never breaks non-stress runs.
- **`framework` is stateless & assertion-free** (no Ginkgo/Gomega). It exposes
  pure helpers (see the reference below) that take explicit `ctx`, cluster/client
  and `node` arguments.

### Cluster surface used by specs (`*e2e.Cluster`)

| Accessor | Returns | Used for |
|----------|---------|----------|
| `cl.RESTConfig()` | `*rest.Config` | build `sdsclient.New(...)` |
| `cl.Clientset()` | `kubernetes.Interface` | core objects (nodes, pods, PVCs) |
| `cl.Dynamic()` | `dynamic.Interface` | CRDs / dynamic access |
| `cl.Nodes()` | `NodeExecutor` | `Exec(ctx, node, cmd)` on nodes |
| `cl.Disks()` | `DiskManager` | `CreateDisk`/`AttachDisk`/`DetachDisk`/`DeleteDisk`/`ResizeDisk` |
| `cl.Close(ctx)` | `error` | release the lease + connection |

> Disk resize uses the SDK directly:
> `cl.Disks().ResizeDisk(ctx, diskName, resource.Quantity)`. There is **no**
> `framework.ResizeDisk` (it was removed).

### `framework` package reference

| Function (package `framework`) | Signature (abbreviated) |
|--------------------------------|--------------------------|
| `Poll` | `Poll(ctx, interval, timeout, cond func(context.Context) (done bool, err error)) error` |
| `NodeExecChecked` | `NodeExecChecked(ctx, cl *e2e.Cluster, node, cmd string) (string, error)` — non-zero exit ⇒ error (for setup/mutations) |
| `ParseLsblk` | `ParseLsblk(out string) map[string]LsblkLine` (keyed by PATH) |
| `LsblkLine` | struct `{ Path, Serial, Size string; SizeBytes int64 }` |
| `BlockDeviceName` | `BlockDeviceName(nodeName, wwn, model, serial, partUUID string) string` — mirrors the agent's `createUniqDeviceName` |
| `WaitNewConsumableBlockDevice` | `WaitNewConsumableBlockDevice(ctx, restCfg *rest.Config, node string, before []kubernetes.BlockDevice, timeout) (kubernetes.BlockDevice, error)` |
| `TriggerLVMDiscovery` | `TriggerLVMDiscovery(ctx, cl *e2e.Cluster, node string)` — pvscan + udevadm trigger |
| `CountPVsInVG` | `CountPVsInVG(out string) int` (parses `pvs` output) |
| `VGInListing` | `VGInListing(out, vgName string) bool` (parses `vgs` output) |
| `ThinPoolDataLVPresent` | `ThinPoolDataLVPresent(out, thinPoolName string) bool` (parses `lvs` output) |
| `RemoveThinPoolStackScript` | `RemoveThinPoolStackScript(vgName, thinPoolName string) string` — teardown shell script |

Client creation lives in `sdsclient`:
`sdsclient.New(cfg *rest.Config) (client.Client, error)` builds a
controller-runtime client with a **private** `runtime.NewScheme()` (registers
client-go core types + `sds-node-configurator/api/v1alpha1`) and never mutates
the global `scheme.Scheme`.

---

## Running in CI (GitHub Actions)

The repo workflow `.github/workflows/e2e-tests.yml` is a thin caller that gates
on the `e2e/run` PR label and delegates to the storage-e2e reusable workflow:

```yaml
jobs:
  e2e:
    if: ${{ contains(github.event.pull_request.labels.*.name, 'e2e/run') }}
    uses: deckhouse/storage-e2e/.github/workflows/e2e.yml@main
    with:
      module_slug: sds-node-configurator
      module_path: e2e
      test_package: ./tests/
      cluster_config: e2e/tests/cluster_config.ci.yml
      cluster_provider: dvp
    secrets: inherit
```

### PR labels (Prow-style, resolved by the reusable workflow)

| Label | Effect |
|-------|--------|
| `e2e/run` | **Gate.** Without it the reusable workflow is not invoked at all. |
| `e2e/keep-cluster` | Skip teardown (re-run tests on the same cluster). |
| `e2e/label:<x>` | Ginkgo label(s); multiple are joined with ` \|\| `. Falls back to the reusable workflow's `label_filter` input when none are set. |

### PR image of the module under test

`cluster_config.ci.yml` differs from `cluster_config.yml` in one line: the
`sds-node-configurator` module is installed from the PR image via

```yaml
- name: "sds-node-configurator"
  ...
  modulePullOverride: "${E2E_MODULE_IMAGE_TAG}"
```

The reusable workflow exposes its `module_image_tag` input to the enable-modules
step as `E2E_MODULE_IMAGE_TAG`, which this cluster config references, so the PR
build of the module is the one exercised by the tests.

### Required secrets / vars (dvp provider, inherited)

Configured in **Settings → Secrets and variables → Actions** and passed via
`secrets: inherit`:

| Secret | Required | Purpose |
|--------|----------|---------|
| `E2E_DVP_BASE_CLUSTER_KUBECONFIG` | Yes | base64 kubeconfig of the base virtualization cluster (decoded inline) |
| `E2E_DVP_BASE_CLUSTER_SSH_PRIVATE_KEY` | Yes | SSH private key **content** for the base cluster |
| `E2E_DVP_BASE_CLUSTER_SSH_USER` | Yes | SSH user |
| `E2E_DVP_BASE_CLUSTER_SSH_HOST` | Yes | SSH host |
| `E2E_DVP_BASE_CLUSTER_SSH_PASSPHRASE` | No | SSH key passphrase |
| `E2E_DVP_DKP_LICENSE_KEY` | Yes (bootstrap) | DKP license for the dhctl install image (used by bootstrap only) |
| `E2E_DVP_REGISTRY_DOCKER_CFG` | Yes (bootstrap) | base64 dockercfg embedded into the bootstrap config (bootstrap only) |
| `E2E_DVP_BASE_CLUSTER_SSH_JUMP_HOST` / `_SSH_JUMP_USER` / `_SSH_JUMP_PRIVATE_KEY` | No | jump host — **all-or-nothing** (a partial set fails validation) |

### Results

- **Actions** → the E2E run for the PR.
- Test pass/fail is reported by the reusable workflow; teardown runs regardless of
  the test result unless `e2e/keep-cluster` is set.

---

## Running locally

Local runs use the same suite and the same SDK connection environment; the
cluster must already exist and be reachable.

### 1. Prepare a git-ignored config

Keep secrets **out of git**. A common convention is a shell file under
`e2e/config/` (add the directory to `.gitignore`) that `export`s the variables
below. Then `source` it before running.

### 2. Export the connection environment

Minimum for the `dvp` provider (see the full table further down):

```bash
# SDK connection (required)
export E2E_TEST_CLUSTER_PROVIDER='dvp'
export E2E_CLUSTER_CONFIG_YAML_PATH='<abs path to a cluster_config yaml>'

# dvp base cluster: SSH + kubeconfig + storage class
export E2E_DVP_BASE_CLUSTER_SSH_USER='<user>'
export E2E_DVP_BASE_CLUSTER_SSH_HOST='<host>'
export E2E_DVP_BASE_CLUSTER_SSH_PRIVATE_KEY_PATH='<path to key>'   # or ..._SSH_PRIVATE_KEY (content)
export E2E_DVP_BASE_CLUSTER_KUBECONFIG_PATH='<path>'              # or ..._KUBECONFIG (content)
export E2E_DVP_BASE_CLUSTER_STORAGE_CLASS='<storage-class>'        # required by specs for VirtualDisk creation
```

> Exactly **one** of `..._SSH_PRIVATE_KEY_PATH` / `..._SSH_PRIVATE_KEY` and
> exactly **one** of `..._KUBECONFIG_PATH` / `..._KUBECONFIG` must be set — the
> SDK rejects "both" and "neither".

### 3. Run

```bash
source <your git-ignored env file>
cd e2e
make deps      # go mod download/tidy + fix-mod-permissions
make test      # smoke: -ginkgo.label-filter='!stress-test'
```

Raw `go test` equivalent (what `make test-go` runs):

```bash
cd e2e
GOWORK=off go test -v -count=1 -timeout 90m ./tests/ \
  -run '^TestSdsNodeConfigurator$' -ginkgo.label-filter='!stress-test'
```

Via the Ginkgo CLI (install with `make install-ginkgo`; run serial, never `-p`):

```bash
cd e2e
ginkgo run --label-filter='discovery || block-device' ./tests/
ginkgo run --label-filter='!stress-test' --focus='BlockDevice discovery' ./tests/
```

### Focus & labels

Real labels (from `tests/*.go`): `sds-node-configurator`, `block-device`,
`discovery`, `block-device-stable`, `netlink-discovery`, `lvmvolumegroup`,
`controller-restart`, `schedule-extender` (with `small`/`medium`/`large`),
`regress`, and `stress-test`. The Makefile default is `GINKGO_LABEL_FILTER ?= !stress-test`
(matching the storage-e2e reusable workflow default).

```bash
# smoke (default)
make test
# a subset
make test-go GINKGO_LABEL_FILTER='lvmvolumegroup'
# stress only (label stress-test)
make test-stress
#   == go test ... -ginkgo.label-filter=stress-test   (timeout 240m)
# everything (smoke + stress)
make test-go GINKGO_LABEL_FILTER=''
```

### Timeouts

- Local default: `90m`. Override with `E2E_TEST_TIMEOUT` (e.g. `E2E_TEST_TIMEOUT=120m`)
  or the `Makefile` var (`make test-go E2E_TEST_TIMEOUT=120m`).
- In CI the suite enforces a minimum of `3h30m`.

---

## Environment variables reference

Only variables confirmed against the code are listed.

### SDK connection (required by `e2e.Connect`)

| Variable | Required | Notes |
|----------|----------|-------|
| `E2E_TEST_CLUSTER_PROVIDER` | Yes | provider mode; use `dvp` |
| `E2E_CLUSTER_CONFIG_YAML_PATH` | Yes | path to the cluster config yaml |

### dvp base cluster (`E2E_DVP_BASE_CLUSTER_*`, consumed by the SDK dvp provider)

| Variable | Required | Default | Notes |
|----------|----------|---------|-------|
| `E2E_DVP_BASE_CLUSTER_SSH_USER` | Yes | — | SSH user |
| `E2E_DVP_BASE_CLUSTER_SSH_HOST` | Yes | — | SSH host |
| `E2E_DVP_BASE_CLUSTER_SSH_PRIVATE_KEY_PATH` | one of | — | path to key… |
| `E2E_DVP_BASE_CLUSTER_SSH_PRIVATE_KEY` | one of | — | …or key content (exactly one) |
| `E2E_DVP_BASE_CLUSTER_SSH_PASSPHRASE` | No | — | key passphrase |
| `E2E_DVP_BASE_CLUSTER_KUBECONFIG_PATH` | one of | — | path to kubeconfig… |
| `E2E_DVP_BASE_CLUSTER_KUBECONFIG` | one of | — | …or kubeconfig content (exactly one) |
| `E2E_DVP_BASE_CLUSTER_STORAGE_CLASS` | Yes (specs) | — | storage class; also read by `cfg` (see below) |
| `E2E_DVP_BASE_CLUSTER_NAMESPACE` | No | `e2e-test-cluster` | base cluster namespace |
| `E2E_DVP_BASE_CLUSTER_VM_CLASS` | No | `generic` | VM class |
| `E2E_DVP_BASE_CLUSTER_DEFAULT_VM_CLASS` | No | `generic` | default VM class |
| `E2E_DVP_VM_SSH_USER` | No | `cloud` | SSH user on created VMs |
| `E2E_DVP_BASE_CLUSTER_SSH_JUMP_HOST` | jump set | — | jump host (all-or-nothing) |
| `E2E_DVP_BASE_CLUSTER_SSH_JUMP_USER` | jump set | — | jump user |
| `E2E_DVP_BASE_CLUSTER_SSH_JUMP_PRIVATE_KEY_PATH` / `_SSH_JUMP_PRIVATE_KEY` | jump set | — | exactly one when using a jump host |
| `E2E_DVP_BASE_CLUSTER_SSH_JUMP_KEY_PASSPHRASE` | No | — | jump key passphrase |
| `E2E_DVP_DKP_LICENSE_KEY` | bootstrap | — | only used by bootstrap, not attach |
| `E2E_DVP_REGISTRY_DOCKER_CFG` | bootstrap | — | only used by bootstrap, not attach |

### Suite-specific (`cfg.Load`)

`cfg.Load()` parses these **without** an `E2E_` prefix (the storage-e2e reusable
workflow re-exports the needed values under these names before `go test`):

| Variable | Field | Default | Notes |
|----------|-------|---------|-------|
| `TEST_CLUSTER_NAMESPACE` | `TestCluster.Namespace` | `e2e-test-cluster` | test namespace |
| `E2E_DVP_BASE_CLUSTER_STORAGE_CLASS` | `TestCluster.StorageClass` | — | required by specs for `VirtualDisk` creation |
| `MODULES_MODULE_TAG` | `ModulesImageTag` | `main` | module image tag used by specs |

### Stress config (`cfg.LoadStress`, loaded lazily only by the stress spec)

| Variable | Default | Notes |
|----------|---------|-------|
| `E2E_STRESS_MAX_VG_TARGET` | `15` | target VG count; clamped to `MAX_VM_BLOCK_DEVICES` |
| `E2E_STRESS_MAX_VG_DISK_SIZE` | `1Gi` | per-slot disk size (must parse as a quantity) |
| `E2E_STRESS_MAX_VG_BATCH_SIZE` | `5` | batch size (must be ≤ target) |
| `E2E_STRESS_MAX_VG_STRICT` | `false` | strict mode sets `MinReady = Target` |
| `E2E_STRESS_MAX_VG_MIN_READY` | `0` | ≤0 ⇒ `Target` when strict, else `1` |
| `E2E_STRESS_MAX_VM_BLOCK_DEVICES` | `15` | hard ceiling `16` (VMBDA per VM) |

### Suite runtime

| Variable | Default | Notes |
|----------|---------|-------|
| `E2E_TEST_TIMEOUT` | `90m` local | Ginkgo suite timeout; CI enforces a `3h30m` minimum |
| `CI` | — | when set, enables `FailFast` and the CI timeout floor |

---

## Troubleshooting

### Missing connection env (the #1 gotcha)

If the SDK connection variables are not exported, **every** `BeforeAll` fails at
`e2e.Connect` with an error like:

```
env: required environment variable "E2E_TEST_CLUSTER_PROVIDER" is not set
env: required environment variable "E2E_CLUSTER_CONFIG_YAML_PATH" is not set
```

Fix: `source` your git-ignored env file (or export the variables) so at least
`E2E_TEST_CLUSTER_PROVIDER`, `E2E_CLUSTER_CONFIG_YAML_PATH` and the required
`E2E_DVP_BASE_CLUSTER_*` set are present before running.

Related SDK validation errors:

- `exactly one of E2E_DVP_BASE_CLUSTER_KUBECONFIG_PATH or E2E_DVP_BASE_CLUSTER_KUBECONFIG must be set`
- `exactly one of E2E_DVP_BASE_CLUSTER_SSH_PRIVATE_KEY_PATH or E2E_DVP_BASE_CLUSTER_SSH_PRIVATE_KEY must be set`

### Cluster lease / "already locked"

`e2e.Connect` takes a `coordination.k8s.io/v1` Lease that is renewed in the
background and **self-expires** if the holder dies, so a crashed run no longer
leaves a permanent lock. If you must run against a cluster you know is
exclusively yours and want to skip the lease entirely, a spec can use
`e2e.Connect(ctx, e2e.WithoutLock())` — do this only when nothing else touches
the cluster.

### Jump host

If test-cluster nodes are not reachable directly, configure the jump host as an
**all-or-nothing** set: `E2E_DVP_BASE_CLUSTER_SSH_JUMP_HOST`,
`E2E_DVP_BASE_CLUSTER_SSH_JUMP_USER`, and exactly one of
`E2E_DVP_BASE_CLUSTER_SSH_JUMP_PRIVATE_KEY_PATH` /
`E2E_DVP_BASE_CLUSTER_SSH_JUMP_PRIVATE_KEY` (optionally
`E2E_DVP_BASE_CLUSTER_SSH_JUMP_KEY_PASSPHRASE`). A partial set fails validation
with `jump host requires ...`.

### `permission denied` under the storage-e2e module cache

The storage-e2e module may need a writable checkout in the Go module cache. Run:

```bash
cd e2e
make fix-mod-permissions   # also run as part of `make deps`
```

`fix-mod-permissions` `chmod -R +w`s `…/github.com/deckhouse/storage-e2e@*` in
your `GOMODCACHE`/`GOPATH/pkg/mod`. On self-hosted CI with a shared read-only
cache, point the cache at a writable directory instead:

```bash
export GOMODCACHE="$(pwd)/.gomodcache"
mkdir -p "$GOMODCACHE"
```

### Ginkgo CLI / library version mismatch

`make install-ginkgo` installs the Ginkgo CLI at `@latest`, which may differ from
`github.com/onsi/ginkgo/v2` pinned in `go.mod` (currently `v2.28.2`) and print a
version-mismatch warning. Either avoid the standalone CLI and use `make test-go`
(plain `go test`), or run the pinned CLI:

```bash
cd e2e
GOWORK=off go run github.com/onsi/ginkgo/v2/ginkgo run --label-filter='!stress-test' ./tests/
```

### `go.work` / toolchain

The repo root has a `go.work` that interferes with the `e2e` module. Run Go
commands from `e2e/` with `GOWORK=off`; if your base toolchain is older than the
required `go 1.26.5`, pin it with `GOTOOLCHAIN=go1.26.5`.

---

## Writing a new test

1. **Create a spec file** in `tests/` (`package tests`). Give the top-level
   `Describe` `Ordered` and the appropriate labels, e.g.:

   ```go
   var _ = Describe("My new scenario",
       Label("sds-node-configurator", "block-device"), Ordered, func() { /* ... */ })
   ```

2. **Connect in `BeforeAll`** and register cleanup:

   ```go
   BeforeAll(func() {
       ctx = context.Background()
       var err error
       conf, err = cfg.Load()
       Expect(err).NotTo(HaveOccurred())

       cl, err = e2e.Connect(ctx, e2e.WithTestName("my-new-scenario"))
       Expect(err).NotTo(HaveOccurred())
       DeferCleanup(func() {
           if cerr := cl.Close(context.Background()); cerr != nil {
               GinkgoWriter.Println("Error closing cluster:", cerr)
           }
       })

       k8sClient, err = sdsclient.New(cl.RESTConfig())
       Expect(err).NotTo(HaveOccurred())
   })
   ```

3. **Drive the cluster via the SDK**: nodes with `cl.Clientset().CoreV1().Nodes()`,
   commands with `cl.Nodes().Exec(...)` (or `framework.NodeExecChecked` when a
   non-zero exit must be an error), disks with
   `cl.Disks().CreateDisk/AttachDisk/DetachDisk/DeleteDisk/ResizeDisk`.

4. **Use `framework` helpers** for polling and parsing (`Poll`, `ParseLsblk`,
   `WaitNewConsumableBlockDevice`, `BlockDeviceName`, `TriggerLVMDiscovery`, the
   LVM parsers). Keep assertions (`Expect`/`Eventually`) in the spec — `framework`
   stays assertion-free.

5. **Labeling conventions**: always include `sds-node-configurator` for
   module-level specs plus a domain label (`block-device`, `lvmvolumegroup`,
   `netlink-discovery`, `controller-restart`, …). Use `stress-test` only for heavy
   stress specs (excluded from the default `!stress-test` filter).

6. **No parallelism.** Assume serial execution: pick a node explicitly, clean up
   what you create (`AfterEach`/`AfterAll`/`DeferCleanup`), and never rely on or
   enable `ginkgo -p`.

## See also

- [`README.md`](README.md) — overview, package structure, quickstart.
- [`Makefile`](Makefile) — all targets (`make help`).
- [storage-e2e](https://github.com/deckhouse/storage-e2e) — SDK, reusable CI
  workflow, and env reference.
