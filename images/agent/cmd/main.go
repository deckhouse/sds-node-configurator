/*
	Copyright 2026 Flant JSC

	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at

		http://www.apache.org/licenses/LICENSE-2.0

	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.
*/

package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	goruntime "runtime"
	"syscall"
	"time"

	v1 "k8s.io/api/core/v1"
	sv1 "k8s.io/api/storage/v1"
	extv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	crcache "sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/cache"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/config"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/controller"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/controller/bd"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/controller/bdf"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/controller/llv"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/controller/llv_extender"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/controller/lvg"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/kubutils"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/monitoring"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/scanner"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/thinpoolalloc"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/utils"
	"github.com/deckhouse/sds-node-configurator/lib/go/common/pkg/feature"
)

var (
	resourcesSchemeFuncs = []func(*runtime.Scheme) error{
		v1alpha1.AddToScheme,
		clientgoscheme.AddToScheme,
		extv1.AddToScheme,
		v1.AddToScheme,
		sv1.AddToScheme,
	}
)

func main() {
	// Keep main minimal so that `defer cancel()` from signal.NotifyContext
	// (set up in run) is honored even on startup failures. Doing os.Exit
	// next to a defer in the same function would skip the deferred cleanup
	// (gocritic: exitAfterDefer).
	os.Exit(run())
}

func run() int {
	// Cancel ctx on SIGTERM/SIGINT so the controller-runtime manager (and
	// any nsenter-backed LVM commands launched via exec.CommandContext)
	// terminate gracefully instead of being killed mid-flight.
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	cfgParams, err := config.NewConfig()
	if err != nil {
		// NewConfig returns nil on error, so the next line -- cfgParams.Loglevel --
		// used to panic on a nil pointer and bury the message printed here. A
		// missing NODE_NAME or an unreadable machine-id is a plain configuration
		// error and has to read as one.
		fmt.Printf("unable to create the config: %v\n", err)
		return 1
	}

	baseLog, err := logger.NewLogger(cfgParams.Loglevel)
	if err != nil {
		fmt.Printf("unable to create NewLogger, err: %v\n", err)
		return 1
	}
	log := baseLog.Named("main")

	log.Info(fmt.Sprintf("go Version:%s ", goruntime.Version()))
	log.Info(fmt.Sprintf("OS/Arch:Go OS/Arch:%s/%s ", goruntime.GOOS, goruntime.GOARCH))

	log.Info(fmt.Sprintf("feature SnapshotsEnabled: %t", feature.SnapshotsEnabled()))
	log.Info(fmt.Sprintf("feature VolumeCleanupEnabled: %t", feature.VolumeCleanupEnabled()))
	log.Info(fmt.Sprintf("feature NetlinkBlockDeviceDiscoveryEnabled: %t", cfgParams.Features.NetlinkBlockDeviceDiscovery))

	log.Info("CfgParams has been successfully created")
	log.Info(fmt.Sprintf("%s = %s", logger.EnvLogLevel, cfgParams.Loglevel))
	log.Info(fmt.Sprintf("%s = %s", config.NodeName, cfgParams.NodeName))
	log.Info(fmt.Sprintf("%s = %s", config.MachineID, cfgParams.MachineID))
	log.Info(fmt.Sprintf("%s = %s", config.ScanInterval, cfgParams.BlockDeviceScanInterval.String()))
	log.Info(fmt.Sprintf("%s = %s", config.ThrottleInterval, cfgParams.ThrottleInterval.String()))
	log.Info(fmt.Sprintf("%s = %s", config.CmdDeadlineDuration, cfgParams.CmdDeadlineDuration.String()))
	log.Info(fmt.Sprintf("%s = %s", config.MissingBlockDeviceRetentionHours, cfgParams.MissingBlockDeviceRetention.String()))

	kConfig, err := kubutils.KubernetesDefaultConfigCreate()
	if err != nil {
		// Without a config the manager cannot be built at all; continuing only moved
		// the failure a few lines down, to a message that does not name the cause.
		// return and not os.Exit: main is kept minimal precisely so that the
		// `defer cancel()` above still runs on a startup failure.
		log.Error("unable to KubernetesDefaultConfigCreate", logger.Err(err))
		return 1
	}
	log.Info("kubernetes config has been successfully created.")

	scheme := runtime.NewScheme()
	for _, f := range resourcesSchemeFuncs {
		err := f(scheme)
		if err != nil {
			log.Error("unable to add scheme to func", logger.Err(err))
			return 1
		}
	}
	log.Info("successfully read scheme CR")

	managerOpts := manager.Options{
		Scheme:                 scheme,
		Logger:                 baseLog.GetLogger(),
		Metrics:                server.Options{BindAddress: cfgParams.MetricsBindAddress},
		HealthProbeBindAddress: cfgParams.HealthProbeBindAddress,
		Cache: crcache.Options{
			DefaultTransform: crcache.TransformStripManagedFields(),
		},
	}

	mgr, err := manager.New(kConfig, managerOpts)
	if err != nil {
		log.Error("unable to manager.New", logger.Err(err))
		return 1
	}
	log.Info("successfully created kubernetes manager")

	metrics := monitoring.GetMetrics(cfgParams.NodeName)
	commands := utils.NewCommands()

	// Run ReTag and VG activation only after the manager has started its
	// HTTP servers (including health probes). These nsenter-backed LVM
	// operations may take longer than the liveness probe failure window,
	// so executing them synchronously before mgr.Start() makes kubelet
	// SIGTERM the container with exit code 143 (no error in logs).
	// The runnable also receives a cancellable context, so a graceful
	// shutdown can interrupt long-running LVM commands.
	if err := mgr.Add(manager.RunnableFunc(func(ctx context.Context) error {
		// Before ReTag, because every LVM command below is filtered by what the
		// agent believes its own loop devices to be, and after a restart that belief
		// starts out empty while the loops of a file-backed Volume Group are still
		// attached from the previous incarnation. Without this pass ReTag and
		// ActivateVGs would not see those Volume Groups at all.
		if err := utils.RefreshOwnedLoops(ctx, log, commands, cfgParams.CmdDeadlineDuration); err != nil {
			log.Warn("unable to refresh the owned loop devices", logger.Err(err))
		}

		log.Info("ReTag starts")
		if err := commands.ReTag(ctx, log, metrics, bd.DiscovererName, cfgParams.CmdDeadlineDuration); err != nil {
			log.Error("unable to run ReTag", logger.Err(err))
		}

		log.Info("ReattachFileDevices starts")
		// Best-effort fast path: re-establish loop mappings for file-backed
		// PVs so ActivateVGs can see them right after a reboot. A failure
		// here must NOT suppress ActivateAllManagedVGs — that would hold
		// every healthy VG on the node (including pure block-device ones)
		// hostage to one unrelated file device. The LVG reconciler retries
		// reattach idempotently via provisionFileDevices on its next pass,
		// so a transient losetup failure recovers without a pod restart.
		if err := reattachFileDevicesAtStartup(ctx, log, mgr, commands, cfgParams.NodeName, cfgParams.FileDevicesDirectory, cfgParams.CmdDeadlineDuration); err != nil {
			log.Error("file device reattach failed; the LVG reconciler will retry, continuing to ActivateVGs", logger.Err(err))
		} else {
			log.Info("ReattachFileDevices completed")
		}

		log.Info("ActivateVGs starts")
		if err := utils.ActivateAllManagedVGs(ctx, log, commands, metrics, cfgParams.CmdDeadlineDuration); err != nil {
			log.Error("unable to activate managed VGs", logger.Err(err))
		}
		log.Info("ActivateVGs completed")
		return nil
	})); err != nil {
		log.Error("unable to add startup tasks runnable", logger.Err(err))
		return 1
	}

	sdsCache := cache.New()

	rediscoverBlockDevices, err := controller.AddDiscoverer(
		mgr,
		log,
		bd.NewDiscoverer(
			mgr.GetClient(),
			log,
			metrics,
			sdsCache,
			bd.DiscovererConfig{
				NodeName:                    cfgParams.NodeName,
				MachineID:                   cfgParams.MachineID,
				BlockDeviceScanInterval:     cfgParams.BlockDeviceScanInterval,
				MissingBlockDeviceRetention: cfgParams.MissingBlockDeviceRetention,
			},
		),
	)
	if err != nil {
		log.Error("unable to controller.RunBlockDeviceController", logger.Err(err))
		return 1
	}

	rediscoverLVGs, err := controller.AddDiscoverer(
		mgr,
		log,
		lvg.NewDiscoverer(
			mgr.GetClient(),
			// Uncached on purpose: this discoverer runs right after the block-device
			// one in the same scanner pass and has to be able to see the BlockDevice
			// status it just wrote. See Discoverer.bdAPICl.
			lvg.UncachedReader{Reader: mgr.GetAPIReader()},
			log,
			metrics,
			sdsCache,
			commands,
			internal.NewEventRecorder(mgr, lvg.DiscovererName),
			lvg.DiscovererConfig{
				NodeName:                cfgParams.NodeName,
				VolumeGroupScanInterval: cfgParams.VolumeGroupScanInterval,
				CmdDeadlineDuration:     cfgParams.CmdDeadlineDuration,
				FileDevicesDirectory:    cfgParams.FileDevicesDirectory,
			},
		),
	)
	if err != nil {
		log.Error("unable to controller.RunLVMVolumeGroupDiscoverController", logger.Err(err))
		return 1
	}

	// One discovery pass, and the only place the order of the two discoverers is
	// stated. Everything that runs discovery runs this — the BlockDeviceFilter
	// reconciler below and the scanner further down — so neither can run the
	// block-device half without the LVMVolumeGroup half after it. See
	// controller.DiscoverInOrder for why that is a contract and not a convenience.
	//
	// Wrapped once, here, so that both of those callers share ONE retry chain. The
	// requeue is a property of the node's state rather than of whoever noticed it,
	// and lvg.maxUnnamedPVPasses is counted in passes on the assumption that a pass
	// is worth about a scan interval — an assumption that only holds while there is
	// a single chain. See controller.WithSingleRetryChain.
	//
	// ctx and not a per-caller context: the chain outlives the pass that started it,
	// and it has to end with the process rather than with one reconcile.
	discover := controller.WithSingleRetryChain(
		ctx,
		log,
		controller.DiscoveryPassName,
		controller.DiscoverInOrder(rediscoverBlockDevices, rediscoverLVGs),
	)

	err = controller.AddReconciler(
		mgr,
		log,
		bdf.NewReconciler(
			mgr.GetClient(),
			log,
			metrics,
			discover,
			bdf.ReconcilerConfig{
				NodeName: cfgParams.NodeName,
				Loglevel: cfgParams.Loglevel,
			},
		),
	)
	if err != nil {
		log.Error("unable to run BlockDeviceFilter controller", logger.Err(err))
		return 1
	}

	err = controller.AddReconciler(
		mgr,
		log,
		lvg.NewReconciler(
			mgr.GetClient(),
			lvg.UncachedReader{Reader: mgr.GetAPIReader()},
			log,
			metrics,
			sdsCache,
			commands,
			internal.NewEventRecorder(mgr, lvg.ReconcilerName),
			lvg.ReconcilerConfig{
				NodeName:                cfgParams.NodeName,
				VolumeGroupScanInterval: cfgParams.VolumeGroupScanInterval,
				BlockDeviceScanInterval: cfgParams.BlockDeviceScanInterval,
				CmdDeadlineDuration:     cfgParams.CmdDeadlineDuration,
				FileDevicesDirectory:    cfgParams.FileDevicesDirectory,

				FileDevicesMinFreeSpacePercent: cfgParams.FileDevicesMinFreeSpacePercent,
			},
		),
	)
	if err != nil {
		log.Error("unable to controller.RunLVMVolumeGroupWatcherController", logger.Err(err))
		return 1
	}

	go func() {
		if err = scanner.NewScanner(commands).Run(
			ctx,
			log,
			*cfgParams,
			sdsCache,
			metrics,
			discover,
		); err != nil {
			log.Error("unable to run scanner", logger.Err(err))
			os.Exit(1)
		}
	}()

	thinPoolCfgs := newThinPoolReconcilerConfigs(cfgParams)

	err = controller.AddReconciler(
		mgr,
		log,
		llv.NewReconciler(
			mgr.GetClient(),
			log,
			metrics,
			sdsCache,
			commands,
			internal.NewEventRecorder(mgr, llv.ReconcilerName),
			thinPoolCfgs.llv,
		),
	)
	if err != nil {
		log.Error("unable to controller.RunLVMVolumeGroupWatcherController", logger.Err(err))
		return 1
	}

	err = controller.AddReconciler(
		mgr,
		log,
		llv_extender.NewReconciler(
			mgr.GetClient(),
			log,
			metrics,
			sdsCache,
			commands,
			internal.NewEventRecorder(mgr, llv_extender.ReconcilerName),
			thinPoolCfgs.llvExtender,
		),
	)
	if err != nil {
		log.Error("unable to controller.RunLVMLogicalVolumeExtenderWatcherController", logger.Err(err))
		return 1
	}

	addLLVSReconciler(mgr, log, metrics, sdsCache, commands, thinPoolCfgs.llvs)

	if err = mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		log.Error("unable to mgr.AddHealthzCheck", logger.Err(err))
		return 1
	}
	log.Info("successfully AddHealthzCheck")

	if err = mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		log.Error("unable to mgr.AddReadyzCheck", logger.Err(err))
		return 1
	}
	log.Info("successfully AddReadyzCheck")

	err = mgr.Start(ctx)
	if err != nil {
		log.Error("unable to mgr.Start", logger.Err(err))
		return 1
	}

	log.Info("successfully starts the manager")
	return 0
}

// reattachFileDevicesAtStartup re-establishes the loop mappings of every managed
// backing file on this node, before ActivateAllManagedVGs, so LVM can see the
// file-backed PVs of a Volume Group that has just survived a reboot.
//
// baseDir is the configured file-devices base directory. Spec-derived paths are
// confined to it exactly as provisioning and cleanup confine theirs: a directory
// outside it cannot name a file this agent created, so there is nothing here to
// reattach for it, and losetup must not be pointed at an arbitrary host path
// just because it happens to carry a matching basename. Empty disables the check,
// matching ReconcilerConfig.FileDevicesDirectory.
func reattachFileDevicesAtStartup(ctx context.Context, log logger.Logger, mgr manager.Manager, commands utils.Commands, nodeName, baseDir string, cmdTimeout time.Duration) error {
	var lvgList v1alpha1.LVMVolumeGroupList
	if err := mgr.GetAPIReader().List(ctx, &lvgList); err != nil {
		log.Error("unable to list LVMVolumeGroups", logger.Err(err))
		return err
	}

	items := collectFileDevicesToReattach(log, lvgList.Items, nodeName, baseDir)
	if len(items) == 0 {
		log.Debug("no file devices to reattach")
		return nil
	}

	return utils.ReattachFileDevices(ctx, log, commands, cmdTimeout, items)
}

// collectFileDevicesToReattach works out which backing files on this node have to
// be re-attached, and is where the confinement to baseDir is applied. Separated
// from the listing and the losetup calls so that decision is testable on its own.
func collectFileDevicesToReattach(log logger.Logger, lvgs []v1alpha1.LVMVolumeGroup, nodeName, baseDir string) []utils.LVGWithFileDevices {
	var items []utils.LVGWithFileDevices
	for _, lvg := range lvgs {
		if lvg.Spec.Local.NodeName != nodeName {
			continue
		}

		// Collect backing-file paths from BOTH the observed status and the
		// desired spec. Status alone is not enough: a node can reboot after
		// the LVG was provisioned but before the discoverer wrote
		// status.nodes[].fileDevices, in which case reattach would find
		// nothing and ActivateVGs would bring the VG up missing its loop PV.
		// The spec path is deterministic (BuildFileDevicePath), so it lets
		// reattach recover even without a status entry. ReattachFileDevices
		// never creates files (only losetup --find on an existing backing
		// file), so a spec entry whose file does not exist yet is a harmless
		// best-effort miss the LVG reconciler provisions on its next pass.
		seen := make(map[string]struct{})
		fds := make([]utils.FileDeviceStatus, 0)
		for _, node := range lvg.Status.Nodes {
			if node.Name != nodeName {
				continue
			}
			for _, fd := range node.FileDevices {
				if fd.FilePath == "" {
					continue
				}
				if _, ok := seen[fd.FilePath]; ok {
					continue
				}
				seen[fd.FilePath] = struct{}{}
				fds = append(fds, utils.FileDeviceStatus{
					FilePath:   fd.FilePath,
					LoopDevice: fd.LoopDevice,
				})
			}
		}
		for _, fd := range lvg.Spec.FileDevices {
			// Status-derived paths above are already known to be ours — the
			// discoverer only records a backing file that matched the managed
			// pattern. A spec-derived path is not: the apiserver does not
			// constrain `directory` to the allowed base directories, so this is
			// the same confinement validateFileDevice puts in front of
			// provisioning and cleanupFileDevices puts in front of `rm`. Without
			// it, this is the one file-device path that would hand an arbitrary
			// host path to losetup.
			if !utils.IsFileDeviceDirectoryAllowed(filepath.Clean(fd.Directory), baseDir) {
				log.Warn(fmt.Sprintf("skipping the fileDevices entry %q of the LVMVolumeGroup %s: its directory %q is neither inside the configured base directory %q nor inside the legacy %q, so nothing there was created by this agent",
					fd.Name, lvg.Name, fd.Directory, baseDir, internal.LegacyFileDevicesDirectory))
				continue
			}
			path := utils.BuildFileDevicePath(fd.Directory, lvg.Name, fd.Name)
			if _, ok := seen[path]; ok {
				continue
			}
			seen[path] = struct{}{}
			fds = append(fds, utils.FileDeviceStatus{FilePath: path})
		}

		if len(fds) == 0 {
			continue
		}
		items = append(items, utils.LVGWithFileDevices{
			LVGName:     lvg.Name,
			FileDevices: fds,
		})
	}

	return items
}

type thinPoolReconcilerConfigs struct {
	llv         llv.ReconcilerConfig
	llvExtender llv_extender.ReconcilerConfig
	llvs        llvsReconcilerConfig
}

func newThinPoolReconcilerConfigs(cfgParams *config.Config) thinPoolReconcilerConfigs {
	thinPools := &thinpoolalloc.Locks{}
	return thinPoolReconcilerConfigs{
		llv: llv.ReconcilerConfig{
			NodeName:                cfgParams.NodeName,
			VolumeGroupScanInterval: cfgParams.VolumeGroupScanInterval,
			Loglevel:                cfgParams.Loglevel,
			LLVRequeueInterval:      cfgParams.LLVRequeueInterval,
			ThinPools:               thinPools,
		},
		llvExtender: llv_extender.ReconcilerConfig{
			NodeName:                cfgParams.NodeName,
			VolumeGroupScanInterval: cfgParams.VolumeGroupScanInterval,
			ThinPools:               thinPools,
		},
		llvs: newLLVSReconcilerConfig(cfgParams, thinPools),
	}
}
