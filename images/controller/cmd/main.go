/*
Copyright 2025 Flant JSC

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

	v1 "k8s.io/api/core/v1"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/metrics/server"

	d8commonapi "github.com/deckhouse/sds-common-lib/api/v1alpha1"
	"github.com/deckhouse/sds-common-lib/logger"
	commonmonitoring "github.com/deckhouse/sds-common-lib/monitoring"
	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/controller/config"
	"github.com/deckhouse/sds-node-configurator/images/controller/pkg/controller"
	"github.com/deckhouse/sds-node-configurator/images/controller/pkg/kubutils"
	"github.com/deckhouse/sds-node-configurator/images/controller/pkg/monitoring"
)

var (
	resourcesSchemeFuncs = []func(*apiruntime.Scheme) error{
		d8commonapi.AddToScheme,
		v1alpha1.AddToScheme,
		clientgoscheme.AddToScheme,
		v1.AddToScheme,
	}
)

func main() {
	ctx := context.Background()

	cfgParams, err := config.NewConfig()
	if err != nil {
		// NewConfig returns nil on error, so the next line -- cfgParams.Loglevel --
		// used to panic on a nil pointer and bury the message printed here.
		fmt.Printf("unable to create the config: %v\n", err)
		os.Exit(1)
	}

	baseLog, err := logger.NewLogger(cfgParams.Loglevel)
	if err != nil {
		fmt.Printf("unable to create NewLogger, err: %v\n", err)
		os.Exit(1)
	}
	log := baseLog.Named("main")

	log.Info(fmt.Sprintf("go Version:%s ", goruntime.Version()))
	log.Info(fmt.Sprintf("OS/Arch:Go OS/Arch:%s/%s ", goruntime.GOOS, goruntime.GOARCH))

	log.Info("CfgParams has been successfully created")
	log.Info(fmt.Sprintf("%s = %s", logger.EnvLogLevel, cfgParams.Loglevel))
	log.Info(fmt.Sprintf("%s = %s", commonmonitoring.EnvMetricsBindAddress, cfgParams.MetricsBindAddress))
	log.Info(fmt.Sprintf("%s = %s", config.ScanInterval, cfgParams.ScanInterval))
	log.Info(fmt.Sprintf("%s = %t", config.LeaderElectionEnvName, cfgParams.LeaderElection))
	log.Info(fmt.Sprintf("%s = %s", config.LeaderElectionNamespaceEnvName, cfgParams.LeaderElectionNamespace))
	log.Info(fmt.Sprintf("%s = %s", config.LeaderElectionIDEnvName, cfgParams.LeaderElectionID))
	log.Info(fmt.Sprintf("%s = %s", config.AgentNamespaceEnvName, cfgParams.AgentNamespace))
	log.Info(fmt.Sprintf("%s = %s", config.MissingBlockDeviceRetentionHours, cfgParams.MissingBlockDeviceRetention))

	kConfig, err := kubutils.KubernetesDefaultConfigCreate()
	if err != nil {
		// Without a config the manager cannot be built at all; continuing only moved
		// the failure a few lines down, to a message that does not name the cause.
		log.Error("unable to KubernetesDefaultConfigCreate", logger.Err(err))
		os.Exit(1)
	}
	log.Info("kubernetes config has been successfully created.")

	scheme := apiruntime.NewScheme()
	for _, f := range resourcesSchemeFuncs {
		err := f(scheme)
		if err != nil {
			log.Error("unable to add scheme to func", logger.Err(err))
			os.Exit(1)
		}
	}
	log.Info("successfully read scheme CR")

	managerOpts := manager.Options{
		Scheme:                  scheme,
		Logger:                  baseLog.GetLogger(),
		Metrics:                 server.Options{BindAddress: cfgParams.MetricsBindAddress},
		HealthProbeBindAddress:  cfgParams.HealthProbeBindAddress,
		LeaderElection:          cfgParams.LeaderElection,
		LeaderElectionID:        cfgParams.LeaderElectionID,
		LeaderElectionNamespace: cfgParams.LeaderElectionNamespace,
	}

	mgr, err := manager.New(kConfig, managerOpts)
	if err != nil {
		log.Error("unable to manager.New", logger.Err(err))
		os.Exit(1)
	}
	log.Info("successfully created kubernetes manager")

	metrics := monitoring.GetMetrics(cfgParams.NodeName)
	err = controller.RunSdsInfraWatcher(mgr, *cfgParams, metrics, baseLog)
	if err != nil {
		log.Error("unable to run SdsInfraWatcher controller", logger.Err(err))
		os.Exit(1)
	}

	err = controller.RunLVGConditionsWatcher(mgr, *cfgParams, baseLog)
	if err != nil {
		log.Error("unable to run LVGConditionsWatcher controller", logger.Err(err))
		os.Exit(1)
	}

	err = controller.RunLVGStatusWatcher(mgr, baseLog)
	if err != nil {
		log.Error("unable to run LVGConfigurationWatcher controller", logger.Err(err))
		os.Exit(1)
	}

	err = controller.RunBlockDeviceLabelsWatcher(mgr, baseLog, *cfgParams)
	if err != nil {
		log.Error("unable to run BlockDeviceWatcher controller", logger.Err(err))
		os.Exit(1)
	}

	err = controller.RunLVMVolumeGroupSetWatcher(mgr, baseLog, *cfgParams, metrics)
	if err != nil {
		log.Error("unable to run RunLVMVolumeGroupSetWatcher controller", logger.Err(err))
		os.Exit(1)
	}

	err = controller.RunBlockDeviceNodeGC(mgr, baseLog, *cfgParams)
	if err != nil {
		log.Error("unable to run RunBlockDeviceNodeGC", logger.Err(err))
		os.Exit(1)
	}

	if err = mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		log.Error("unable to mgr.AddHealthzCheck", logger.Err(err))
		os.Exit(1)
	}
	log.Info("successfully AddHealthzCheck")

	if err = mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		log.Error("unable to mgr.AddReadyzCheck", logger.Err(err))
		os.Exit(1)
	}
	log.Info("successfully AddReadyzCheck")

	err = mgr.Start(ctx)
	if err != nil {
		log.Error("unable to mgr.Start", logger.Err(err))
		os.Exit(1)
	}

	log.Info("successfully starts the manager")
}
