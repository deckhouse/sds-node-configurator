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
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/yaml"

	d8commonapi "github.com/deckhouse/sds-common-lib/api/v1alpha1"
	slv "github.com/deckhouse/sds-local-volume/api/v1alpha1"
	snc "github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/cache"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/controller"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/kubutils"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/logger"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/scheduler"
)

const (
	defaultDivisor         = 1
	defaultListenAddr      = ":8000"
	defaultcertFile        = "/etc/sds-common-scheduler-extender/certs/tls.crt"
	defaultkeyFile         = "/etc/sds-common-scheduler-extender/certs/tls.key"
	defaultCleanupInterval = 30 * time.Second
)

type Config struct {
	ListenAddr             string  `json:"listen"`
	DefaultDivisor         float64 `json:"default-divisor"`
	LogLevel               string  `json:"log-level"`
	HealthProbeBindAddress string  `json:"health-probe-bind-address"`
	CertFile               string  `json:"cert-file"`
	KeyFile                string  `json:"key-file"`
}

var cfgFilePath string

var resourcesSchemeFuncs = []func(*runtime.Scheme) error{
	slv.AddToScheme,
	snc.AddToScheme,
	corev1.AddToScheme,
	storagev1.AddToScheme,
	d8commonapi.AddToScheme,
}

var config = &Config{
	ListenAddr:     defaultListenAddr,
	DefaultDivisor: defaultDivisor,
	LogLevel:       "2",
	CertFile:       defaultcertFile,
	KeyFile:        defaultkeyFile,
}

var rootCmd = &cobra.Command{
	Use:     "sds-common-scheduler-extender",
	Version: "development",
	Short:   "a scheduler-extender for sds modules",
	RunE: func(cmd *cobra.Command, _ []string) error {
		// to avoid printing usage information when error is returned
		cmd.SilenceUsage = true
		// to avoid printing errors (we log it closer to the place where it has happened)
		cmd.SilenceErrors = true
		return subMain(cmd.Context())
	},
}

func init() {
	rootCmd.PersistentFlags().StringVar(&cfgFilePath, "config", "", "config file")
}

func main() {
	ctx, _ := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)

	if err := rootCmd.ExecuteContext(ctx); err != nil {
		// we expect err to be logged already
		os.Exit(1)
	}
}

func subMain(ctx context.Context) error {
	if len(cfgFilePath) != 0 {
		b, err := os.ReadFile(cfgFilePath)
		if err != nil {
			print(err)
			return err
		}

		if err = yaml.Unmarshal(b, config); err != nil {
			print(err)
			return err
		}
	}

	// Override log level from environment variable if set
	if envLogLevel := os.Getenv("LOG_LEVEL"); envLogLevel != "" {
		config.LogLevel = envLogLevel
	}

	log, err := logger.NewLogger(logger.Verbosity(config.LogLevel))
	if err != nil {
		print(fmt.Sprintf("unable to initialize logger, err: %s", err))
		return err
	}
	mainLog := log.WithName("main")
	mainLog.Info(fmt.Sprintf("logger has been initialized, log level: %s", config.LogLevel))

	// Set the logger for the controller-runtime
	ctrl.SetLogger(log.GetLogger())

	kConfig, err := kubutils.KubernetesDefaultConfigCreate()
	if err != nil {
		mainLog.Error(err, "unable to KubernetesDefaultConfigCreate")
		return err
	}
	mainLog.Info("kubernetes config has been successfully created.")

	scheme := runtime.NewScheme()
	for _, f := range resourcesSchemeFuncs {
		if err := f(scheme); err != nil {
			mainLog.Error(err, "unable to add scheme to func")
			return err
		}
	}
	mainLog.Info("successfully read scheme CR")

	managerOpts := manager.Options{
		Scheme:                 scheme,
		Logger:                 log.GetLogger(),
		HealthProbeBindAddress: config.HealthProbeBindAddress,
		BaseContext:            func() context.Context { return ctx },
	}

	mgr, err := manager.New(kConfig, managerOpts)
	if err != nil {
		mainLog.Error(err, "unable to create manager for creating controllers")
		return err
	}

	// Register field indexer for LVMVolumeGroup -> node name mapping.
	// This enables efficient node-to-LVG lookups via client.MatchingFields.
	// Also implicitly starts the LVG informer when the manager starts.
	err = mgr.GetFieldIndexer().IndexField(ctx, &snc.LVMVolumeGroup{}, scheduler.IndexFieldLVGNodeName,
		func(obj client.Object) []string {
			lvg := obj.(*snc.LVMVolumeGroup)
			names := make([]string, 0, len(lvg.Status.Nodes))
			for _, node := range lvg.Status.Nodes {
				names = append(names, node.Name)
			}
			return names
		},
	)
	if err != nil {
		mainLog.Error(err, "unable to register field indexer for LVMVolumeGroup")
		return err
	}
	mainLog.Info("successfully registered LVMVolumeGroup field indexer on status.nodes.name")

	schedulerCache := cache.NewCache(log, defaultCleanupInterval)
	mainLog.Info("scheduler cache was initialized")

	schedulerHandler, err := scheduler.NewHandler(ctx, mgr.GetClient(), log, schedulerCache, config.DefaultDivisor)
	if err != nil {
		mainLog.Error(err, "unable to create http.Handler of the scheduler extender")
		return err
	}
	mainLog.Info("scheduler handler initialized")

	if err = controller.RunPVCWatcherCacheController(mgr, log, schedulerCache); err != nil {
		mainLog.Error(err, fmt.Sprintf("unable to run %s controller", controller.PVCWatcherCacheCtrlName))
		return err
	}
	mainLog.Info(fmt.Sprintf("successfully ran %s controller", controller.PVCWatcherCacheCtrlName))

	if err = controller.RunLLVWatcherCacheController(mgr, log, schedulerCache); err != nil {
		mainLog.Error(err, fmt.Sprintf("unable to run %s controller", controller.LLVWatcherCacheCtrlName))
		return err
	}
	mainLog.Info(fmt.Sprintf("successfully ran %s controller", controller.LLVWatcherCacheCtrlName))

	if err = mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		mainLog.Error(err, "unable to mgr.AddHealthzCheck")
		return err
	}
	mainLog.Info("successfully AddHealthzCheck")

	if err = mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		mainLog.Error(err, "unable to mgr.AddReadyzCheck")
		return err
	}
	mainLog.Info("successfully AddReadyzCheck")

	serv := &http.Server{
		Addr:         config.ListenAddr,
		Handler:      accessLogHandler(log, schedulerHandler),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}
	mainLog.Info("server was initialized")

	return runServer(ctx, serv, mgr, log)
}

func runServer(ctx context.Context, serv *http.Server, mgr manager.Manager, log logger.Logger) error {
	ctx, stop := context.WithCancel(ctx)

	var wg sync.WaitGroup
	defer wg.Wait()
	defer stop() // stop() should be called before wg.Wait() to stop the goroutine correctly.
	wg.Add(1)

	go func() {
		defer wg.Done()
		<-ctx.Done()
		if err := serv.Shutdown(ctx); err != nil {
			log.Error(err, "[runServer] failed to shutdown gracefully")
		}
	}()

	go func() {
		log.Info("[runServer] kube manager will start now")
		if err := mgr.Start(ctx); err != nil {
			log.Error(err, "[runServer] unable to mgr.Start")
		}
	}()

	log.Info(fmt.Sprintf("[runServer] starts serving on: %s", config.ListenAddr))

	if err := serv.ListenAndServeTLS(config.CertFile, config.KeyFile); !errors.Is(err, http.ErrServerClosed) {
		log.Error(err, "[runServer] unable to run the server")
		return err
	}

	return nil
}
