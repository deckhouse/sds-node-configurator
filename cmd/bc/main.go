package main

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"storage-configurator/config"
	"storage-configurator/internal/blockdev"
	"storage-configurator/pkg/kubutils"
	"storage-configurator/pkg/utils"
	"syscall"

	"k8s.io/klog"
)

func main() {

	// Create context
	ctx, cancel := context.WithCancel(context.Background())

	// Print Version OS and GO
	utils.PrintVersion()

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

	// Create Kubernetes client
	kClient, err := kubutils.CreateKubernetesClient(kConfig)
	if err != nil {
		klog.Fatalln(err)
	}

	klog.Infof("Starting main loop...")

	// Main loop: searching empty block devices and creating resources in Kubernetes
	stop := make(chan struct{})
	go func() {
		defer cancel()
		err := blockdev.ScanBlockDevices(ctx, kClient, cliParams.NodeName, cliParams.ScanInterval)
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
