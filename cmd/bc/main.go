package main

import (
	"context"
	"errors"
	"fmt"
	extv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"os"
	"os/signal"
	goruntime "runtime"
	"storage-configurator/api/v2alpha1"
	"storage-configurator/config"
	"storage-configurator/internal/blockdev"
	"storage-configurator/pkg/kubutils"
	"syscall"

	"k8s.io/klog"
)

var (
	resourcesSchemeFuncs = []func(scheme *runtime.Scheme) error{
		clientgoscheme.AddToScheme,
		extv1.AddToScheme,
		v2alpha1.AddToScheme,
	}
)

func main() {

	// Create context
	ctx, cancel := context.WithCancel(context.Background())

	// Print Version OS and GO
	klog.Info(fmt.Sprintf("Go Version:%s ", goruntime.Version()))
	klog.Info(fmt.Sprintf("OS/Arch:Go OS/Arch:%s/%s ", goruntime.GOOS, goruntime.GOARCH))

	// Parse config params
	cliParams, err := config.NewConfig()
	if err != nil {
		klog.Fatalln(err)
	}
	klog.Info(config.NodeName+" ", cliParams.NodeName)

	// Create default config Kubernetes client
	kConfig, err := kubutils.KubernetesDefaultConfigCreate()
	if err != nil {
		klog.Fatalln(err)
	}
	klog.Info("read Kubernetes config")

	// Setup scheme for all resources
	scheme := runtime.NewScheme()
	for _, f := range resourcesSchemeFuncs {
		err := f(scheme)
		if err != nil {
			klog.Error("failed to add to scheme", err)
			os.Exit(1)
		}
	}
	klog.Info("read scheme CR")

	// Create Kubernetes client
	kClient, err := kubutils.CreateKubernetesClient(kConfig, scheme)
	if err != nil {
		klog.Fatalln(err)
	}
	klog.Info("create kubernetes client")

	// Get node UID
	nodeUID, err := kubutils.GetNodeUID(ctx, kClient, cliParams.NodeName)
	if err != nil {
		klog.Fatalln(err)
	}
	klog.Info("get node UID ", nodeUID)

	klog.Infof("starting main loop...")
	// Main loop: searching empty block devices and creating resources in Kubernetes
	stop := make(chan struct{})
	go func() {
		defer cancel()
		err := blockdev.ScanBlockDevices(ctx, kClient, cliParams.NodeName, cliParams.ScanInterval, nodeUID)
		if errors.Is(err, context.Canceled) {
			// only occurs if the context was cancelled, and it only can be cancelled on SIGINT
			stop <- struct{}{}
			return
		}
		klog.Fatalln(err)
	}()

	// Block waiting signals from OS.
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)

	<-ch
	cancel()
	<-stop
}
