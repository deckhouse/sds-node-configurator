/*
Copyright 2023 Flant JSC

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
	goruntime "runtime"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/lib/go/common/pkg/feature"
	v1 "k8s.io/api/core/v1"
	sv1 "k8s.io/api/storage/v1"
	extv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"agent/internal/cache"
	"agent/internal/config"
	"agent/internal/controller"
	"agent/internal/controller/bd"
	"agent/internal/controller/llv"
	"agent/internal/controller/llv_extender"
	"agent/internal/controller/lvg"
	"agent/internal/kubutils"
	"agent/internal/logger"
	"agent/internal/monitoring"
	"agent/internal/scanner"
	"agent/internal/utils"
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
	ctx := context.Background()

	cfgParams, err := config.NewConfig()
	if err != nil {
		fmt.Println("unable to create NewConfig " + err.Error())
	}

	log, err := logger.NewLogger(cfgParams.Loglevel)
	if err != nil {
		fmt.Printf("unable to create NewLogger, err: %v\n", err)
		os.Exit(1)
	}

	log.Info(fmt.Sprintf("[main] Go Version:%s ", goruntime.Version()))
	log.Info(fmt.Sprintf("[main] OS/Arch:Go OS/Arch:%s/%s ", goruntime.GOOS, goruntime.GOARCH))

	log.Info(fmt.Sprintf("[main] Feature SnapshotsEnabled: %t", feature.SnapshotsEnabled()))

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
			os.Exit(1)
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
		os.Exit(1)
	}
	log.Info("[main] successfully created kubernetes manager")

	metrics := monitoring.GetMetrics(cfgParams.NodeName)

	log.Info("[main] ReTag starts")
	if err := utils.ReTag(ctx, log, metrics, bd.DiscovererName); err != nil {
		log.Error(err, "[main] unable to run ReTag")
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
		os.Exit(1)
	}

	rediscoverLVGs, err := controller.AddDiscoverer(
		mgr,
		log,
		lvg.NewDiscoverer(
			mgr.GetClient(),
			log,
			metrics,
			sdsCache,
			lvg.DiscovererConfig{
				NodeName:                cfgParams.NodeName,
				VolumeGroupScanInterval: cfgParams.VolumeGroupScanInterval,
			},
		),
	)
	if err != nil {
		log.Error(err, "[main] unable to controller.RunLVMVolumeGroupDiscoverController")
		os.Exit(1)
	}

	err = controller.AddReconciler(
		mgr,
		log,
		lvg.NewReconciler(
			mgr.GetClient(),
			log,
			metrics,
			sdsCache,
			lvg.ReconcilerConfig{
				NodeName:                cfgParams.NodeName,
				VolumeGroupScanInterval: cfgParams.VolumeGroupScanInterval,
				BlockDeviceScanInterval: cfgParams.BlockDeviceScanInterval,
			},
		),
	)
	if err != nil {
		log.Error(err, "[main] unable to controller.RunLVMVolumeGroupWatcherController")
		os.Exit(1)
	}

	go func() {
		if err = scanner.RunScanner(
			ctx,
			log,
			*cfgParams,
			sdsCache,
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
		os.Exit(1)
	}

	err = controller.AddReconciler(
		mgr,
		log,
		llv_extender.NewReconciler(
			mgr.GetClient(),
			log,
			metrics,
			sdsCache,
			llv_extender.ReconcilerConfig{
				NodeName:                cfgParams.NodeName,
				VolumeGroupScanInterval: cfgParams.VolumeGroupScanInterval,
			},
		),
	)
	if err != nil {
		log.Error(err, "[main] unable to controller.RunLVMLogicalVolumeExtenderWatcherController")
		os.Exit(1)
	}

	addLLVSReconciler(mgr, log, metrics, sdsCache, cfgParams)

	if err = mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		log.Error(err, "[main] unable to mgr.AddHealthzCheck")
		os.Exit(1)
	}
	log.Info("[main] successfully AddHealthzCheck")

	if err = mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		log.Error(err, "[main] unable to mgr.AddReadyzCheck")
		os.Exit(1)
	}
	log.Info("[main] successfully AddReadyzCheck")

	err = mgr.Start(ctx)
	if err != nil {
		log.Error(err, "[main] unable to mgr.Start")
		os.Exit(1)
	}

	log.Info("[main] successfully starts the manager")
}
