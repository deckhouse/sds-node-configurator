/*
Copyright 2024 Flant JSC

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
	v1 "k8s.io/api/core/v1"
	extv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/runtime"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/metrics/server"

	mc "sds-health-watcher-controller/api"
	"sds-health-watcher-controller/config"
	"sds-health-watcher-controller/pkg/controller"
	"sds-health-watcher-controller/pkg/kubutils"
	"sds-health-watcher-controller/pkg/logger"
	"sds-health-watcher-controller/pkg/monitoring"
)

var (
	resourcesSchemeFuncs = []func(*apiruntime.Scheme) error{
		mc.AddToScheme,
		v1alpha1.AddToScheme,
		clientgoscheme.AddToScheme,
		extv1.AddToScheme,
		v1.AddToScheme,
		extv1.AddToScheme,
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

	log.Info("[main] CfgParams has been successfully created")
	log.Info(fmt.Sprintf("[main] %s = %s", config.LogLevel, cfgParams.Loglevel))
	log.Info(fmt.Sprintf("[main] %s = %s", config.MetricsPort, cfgParams.MetricsPort))
	log.Info(fmt.Sprintf("[main] %s = %s", config.ScanInterval, cfgParams.ScanIntervalSec))

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
	controller.RunSdsInfraWatcher(ctx, mgr, *cfgParams, metrics, *log)

	err = controller.RunLVGConditionsWatcher(mgr, *cfgParams, *log)
	if err != nil {
		log.Error(err, "[main] unable to run LVGConditionsWatcher controller")
		os.Exit(1)
	}

	err = controller.RunLVGStatusWatcher(mgr, *log)
	if err != nil {
		log.Error(err, "[main] unable to run LVGConfigurationWatcher controller")
		os.Exit(1)
	}

	err = controller.RunMCWatcher(mgr, *log)
	if err != nil {
		log.Error(err, "[main] unable to run MCWatcher controller")
		os.Exit(1)
	}

	err = controller.RunBlockDeviceLabelsWatcher(mgr, *log)
	if err != nil {
		log.Error(err, "[main] unable to run BlockDeviceWatcher controller")
		os.Exit(1)
	}

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
