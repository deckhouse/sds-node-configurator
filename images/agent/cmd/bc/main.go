package main

import (
	"context"
	"fmt"
	v1 "k8s.io/api/core/v1"
	sv1 "k8s.io/api/storage/v1"
	extv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/runtime"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"os"
	goruntime "runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"storage-configurator/api/v1alpha1"
	"storage-configurator/config"
	"storage-configurator/pkg/controller"
	"storage-configurator/pkg/kubutils"
	"storage-configurator/pkg/logger"
)

var (
	resourcesSchemeFuncs = []func(*apiruntime.Scheme) error{
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
		fmt.Println(fmt.Sprintf("unable to create NewLogger, err: %v", err))
		os.Exit(1)
	}

	log.Info(fmt.Sprintf("[main] Go Version:%s ", goruntime.Version()))
	log.Info(fmt.Sprintf("[main] OS/Arch:Go OS/Arch:%s/%s ", goruntime.GOOS, goruntime.GOARCH))

	log.Info("[main] CfgParams has been successfully created")
	log.Info(fmt.Sprintf("[main] %s = %s", config.LogLevel, cfgParams.Loglevel))
	log.Info(fmt.Sprintf("[main] %s = %s", config.NodeName, cfgParams.NodeName))
	log.Info(fmt.Sprintf("[main] %s = %s", config.MachineID, cfgParams.MachineId))
	log.Info(fmt.Sprintf("[main] %s = %d", config.ScanInterval, cfgParams.BlockDeviceScanInterval))

	kConfig, err := kubutils.KubernetesDefaultConfigCreate()
	if err != nil {
		log.Error(err, "[main] unable to KubernetesDefaultConfigCreate")
	}
	log.Info("[main] kubernetes config has been successfully created.")

	// Setup scheme for all resources
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
		Scheme:             scheme,
		MetricsBindAddress: cfgParams.MetricsPort,
		Logger:             log.GetLogger(),
	}

	mgr, err := manager.New(kConfig, managerOpts)
	if err != nil {
		log.Error(err, "[main] unable to manager.New")
		os.Exit(1)
	}

	log.Info("[main] successfully created kubernetes manager")

	if _, err := controller.RunBlockDeviceController(ctx, mgr, *cfgParams, *log); err != nil {
		log.Error(err, "[main] unable to controller.RunBlockDeviceController")
		os.Exit(1)
	}

	log.Info("[main] controller BlockDevice started")

	if _, err := controller.RunLVMVolumeGroupController(ctx, mgr, cfgParams.NodeName, *log); err != nil {
		log.Error(err, "[main] error Run RunLVMVolumeGroupController")
		os.Exit(1)
	}

	if _, err := controller.RunDiscoveryLVMVGController(ctx, mgr, *cfgParams, *log); err != nil {
		log.Error(err, "[main] unable to controller.RunDiscoveryLVMVGController")
		os.Exit(1)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		log.Error(err, "[main] unable to mgr.AddHealthzCheck")
		os.Exit(1)
	}
	log.Info("[main] successfully AddHealthzCheck")

	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
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
