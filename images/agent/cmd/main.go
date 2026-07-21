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
	goruntime "runtime"
	"syscall"
	"time"

	v1 "k8s.io/api/core/v1"
	sv1 "k8s.io/api/storage/v1"
	extv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/cache"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/config"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/controller"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/controller/bd"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/controller/bdf"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/controller/llv"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/controller/llv_extender"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/controller/lvg"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/kubutils"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/logger"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/monitoring"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/scanner"
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
		fmt.Println("unable to create NewConfig " + err.Error())
	}

	log, err := logger.NewLogger(cfgParams.Loglevel)
	if err != nil {
		fmt.Printf("unable to create NewLogger, err: %v\n", err)
		return 1
	}

	log.Info(fmt.Sprintf("[main] Go Version:%s ", goruntime.Version()))
	log.Info(fmt.Sprintf("[main] OS/Arch:Go OS/Arch:%s/%s ", goruntime.GOOS, goruntime.GOARCH))

	log.Info(fmt.Sprintf("[main] Feature SnapshotsEnabled: %t", feature.SnapshotsEnabled()))
	log.Info(fmt.Sprintf("[main] Feature VolumeCleanupEnabled: %t", feature.VolumeCleanupEnabled()))
	log.Info(fmt.Sprintf("[main] Feature NetlinkBlockDeviceDiscoveryEnabled: %t", cfgParams.Features.NetlinkBlockDeviceDiscovery))

	log.Info("[main] CfgParams has been successfully created")
	log.Info(fmt.Sprintf("[main] %s = %s", config.LogLevel, cfgParams.Loglevel))
	log.Info(fmt.Sprintf("[main] %s = %s", config.NodeName, cfgParams.NodeName))
	log.Info(fmt.Sprintf("[main] %s = %s", config.MachineID, cfgParams.MachineID))
	log.Info(fmt.Sprintf("[main] %s = %s", config.ScanInterval, cfgParams.BlockDeviceScanInterval.String()))
	log.Info(fmt.Sprintf("[main] %s = %s", config.ThrottleInterval, cfgParams.ThrottleInterval.String()))
	log.Info(fmt.Sprintf("[main] %s = %s", config.CmdDeadlineDuration, cfgParams.CmdDeadlineDuration.String()))

	kConfig, err := kubutils.KubernetesDefaultConfigCreate()
	if err != nil {
		log.Error(err, "[main] unable to KubernetesDefaultConfigCreate")
	}
	log.Info("[main] kubernetes config has been successfully created.")

	scheme := runtime.NewScheme()
	for _, f := range resourcesSchemeFuncs {
		err := f(scheme)
		if err != nil {
			log.Error(err, "[main] unable to add scheme to func")
			return 1
		}
	}
	log.Info("[main] successfully read scheme CR")

	managerOpts := manager.Options{
		Scheme:                 scheme,
		Logger:                 log.GetLogger(),
		Metrics:                server.Options{BindAddress: cfgParams.MetricsPort},
		HealthProbeBindAddress: cfgParams.HealthProbeBindAddress,
	}

	mgr, err := manager.New(kConfig, managerOpts)
	if err != nil {
		log.Error(err, "[main] unable to manager.New")
		return 1
	}
	log.Info("[main] successfully created kubernetes manager")

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
		log.Info("[main] ReTag starts")
		if err := commands.ReTag(ctx, log, metrics, bd.DiscovererName, cfgParams.CmdDeadlineDuration); err != nil {
			log.Error(err, "[main] unable to run ReTag")
		}

		log.Info("[main] ReattachFileDevices starts")
		// Best-effort fast path: re-establish loop mappings for file-backed
		// PVs so ActivateVGs can see them right after a reboot. A failure
		// here must NOT suppress ActivateAllManagedVGs — that would hold
		// every healthy VG on the node (including pure block-device ones)
		// hostage to one unrelated file device. The LVG reconciler retries
		// reattach idempotently via provisionFileDevices on its next pass,
		// so a transient losetup failure recovers without a pod restart.
		if err := reattachFileDevicesAtStartup(ctx, log, mgr, commands, cfgParams.NodeName, cfgParams.CmdDeadlineDuration); err != nil {
			log.Error(err, "[main] file device reattach failed; the LVG reconciler will retry, continuing to ActivateVGs")
		} else {
			log.Info("[main] ReattachFileDevices completed")
		}

		log.Info("[main] ActivateVGs starts")
		if err := utils.ActivateAllManagedVGs(ctx, log, commands, metrics, cfgParams.CmdDeadlineDuration); err != nil {
			log.Error(err, "[main] unable to activate managed VGs")
		}
		log.Info("[main] ActivateVGs completed")
		return nil
	})); err != nil {
		log.Error(err, "[main] unable to add startup tasks runnable")
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
				NodeName:                cfgParams.NodeName,
				MachineID:               cfgParams.MachineID,
				BlockDeviceScanInterval: cfgParams.BlockDeviceScanInterval,
			},
		),
	)
	if err != nil {
		log.Error(err, "[main] unable to controller.RunBlockDeviceController")
		return 1
	}

	rediscoverLVGs, err := controller.AddDiscoverer(
		mgr,
		log,
		lvg.NewDiscoverer(
			mgr.GetClient(),
			log,
			metrics,
			sdsCache,
			commands,
			lvg.DiscovererConfig{
				NodeName:                cfgParams.NodeName,
				VolumeGroupScanInterval: cfgParams.VolumeGroupScanInterval,
			},
		),
	)
	if err != nil {
		log.Error(err, "[main] unable to controller.RunLVMVolumeGroupDiscoverController")
		return 1
	}

	err = controller.AddReconciler(
		mgr,
		log,
		bdf.NewReconciler(
			mgr.GetClient(),
			log,
			metrics,
			rediscoverBlockDevices,
			bdf.ReconcilerConfig{
				NodeName: cfgParams.NodeName,
				Loglevel: cfgParams.Loglevel,
			},
		),
	)
	if err != nil {
		log.Error(err, "[main] unable to run BlockDeviceFilter controller")
		return 1
	}

	err = controller.AddReconciler(
		mgr,
		log,
		lvg.NewReconciler(
			mgr.GetClient(),
			log,
			metrics,
			sdsCache,
			commands,
			lvg.ReconcilerConfig{
				NodeName:                cfgParams.NodeName,
				VolumeGroupScanInterval: cfgParams.VolumeGroupScanInterval,
				BlockDeviceScanInterval: cfgParams.BlockDeviceScanInterval,
				CmdDeadlineDuration:     cfgParams.CmdDeadlineDuration,
				FileDevicesDirectory:    cfgParams.FileDevicesDirectory,
			},
		),
	)
	if err != nil {
		log.Error(err, "[main] unable to controller.RunLVMVolumeGroupWatcherController")
		return 1
	}

	go func() {
		if err = scanner.NewScanner(commands).Run(
			ctx,
			log,
			*cfgParams,
			sdsCache,
			metrics,
			rediscoverBlockDevices,
			rediscoverLVGs,
		); err != nil {
			log.Error(err, "[main] unable to run scanner")
			os.Exit(1)
		}
	}()

	err = controller.AddReconciler(
		mgr,
		log,
		llv.NewReconciler(
			mgr.GetClient(),
			log,
			metrics,
			sdsCache,
			commands,
			llv.ReconcilerConfig{
				NodeName:                cfgParams.NodeName,
				VolumeGroupScanInterval: cfgParams.VolumeGroupScanInterval,
				Loglevel:                cfgParams.Loglevel,
				LLVRequeueInterval:      cfgParams.LLVRequeueInterval,
			},
		),
	)
	if err != nil {
		log.Error(err, "[main] unable to controller.RunLVMVolumeGroupWatcherController")
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
			llv_extender.ReconcilerConfig{
				NodeName:                cfgParams.NodeName,
				VolumeGroupScanInterval: cfgParams.VolumeGroupScanInterval,
			},
		),
	)
	if err != nil {
		log.Error(err, "[main] unable to controller.RunLVMLogicalVolumeExtenderWatcherController")
		return 1
	}

	addLLVSReconciler(mgr, log, metrics, sdsCache, commands, cfgParams)

	if err = mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		log.Error(err, "[main] unable to mgr.AddHealthzCheck")
		return 1
	}
	log.Info("[main] successfully AddHealthzCheck")

	if err = mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		log.Error(err, "[main] unable to mgr.AddReadyzCheck")
		return 1
	}
	log.Info("[main] successfully AddReadyzCheck")

	err = mgr.Start(ctx)
	if err != nil {
		log.Error(err, "[main] unable to mgr.Start")
		return 1
	}

	log.Info("[main] successfully starts the manager")
	return 0
}

func reattachFileDevicesAtStartup(ctx context.Context, log logger.Logger, mgr manager.Manager, commands utils.Commands, nodeName string, cmdTimeout time.Duration) error {
	var lvgList v1alpha1.LVMVolumeGroupList
	if err := mgr.GetAPIReader().List(ctx, &lvgList); err != nil {
		log.Error(err, "[reattachFileDevicesAtStartup] unable to list LVMVolumeGroups")
		return err
	}

	var items []utils.LVGWithFileDevices
	for _, lvg := range lvgList.Items {
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
			path := utils.BuildFileDevicePath(fd.Directory, lvg.Name, fd.Size)
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

	if len(items) == 0 {
		log.Debug("[reattachFileDevicesAtStartup] no file devices to reattach")
		return nil
	}

	return utils.ReattachFileDevices(ctx, log, commands, cmdTimeout, items)
}
