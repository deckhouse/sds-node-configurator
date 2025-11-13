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
	v1 "k8s.io/api/core/v1"
	sv1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/yaml"

	slv "github.com/deckhouse/sds-local-volume/api/v1alpha1"
	snc "github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/cache"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/controller"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/kubutils"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/logger"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/scheduler"
)

const (
	defaultDivisor    = 1
	defaultListenAddr = ":8000"
	defaultCacheSize  = 10
	defaultcertFile   = "/etc/sds-common-scheduler-extender/certs/tls.crt"
	defaultkeyFile    = "/etc/sds-common-scheduler-extender/certs/tls.key"
)

type Config struct {
	ListenAddr             string  `json:"listen"`
	DefaultDivisor         float64 `json:"default-divisor"`
	LogLevel               string  `json:"log-level"`
	CacheSize              int     `json:"cache-size"`
	HealthProbeBindAddress string  `json:"health-probe-bind-address"`
	CertFile               string  `json:"cert-file"`
	KeyFile                string  `json:"key-file"`
	PVCExpiredDurationSec  int     `json:"pvc-expired-duration-sec"`
}

var cfgFilePath string

var resourcesSchemeFuncs = []func(*runtime.Scheme) error{
	slv.AddToScheme,
	snc.AddToScheme,
	v1.AddToScheme,
	sv1.AddToScheme,
}

var config = &Config{
	ListenAddr:            defaultListenAddr,
	DefaultDivisor:        defaultDivisor,
	LogLevel:              "2",
	CacheSize:             defaultCacheSize,
	CertFile:              defaultcertFile,
	KeyFile:               defaultkeyFile,
	PVCExpiredDurationSec: cache.DefaultPVCExpiredDurationSec,
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

	log, err := logger.NewLogger(logger.Verbosity(config.LogLevel))
	if err != nil {
		print(fmt.Sprintf("[subMain] unable to initialize logger, err: %s", err))
		return err
	}
	log.Info(fmt.Sprintf("[subMain] logger has been initialized, log level: %s", config.LogLevel))
	ctrl.SetLogger(log.GetLogger())

	kConfig, err := kubutils.KubernetesDefaultConfigCreate()
	if err != nil {
		log.Error(err, "[subMain] unable to KubernetesDefaultConfigCreate")
		return err
	}
	log.Info("[subMain] kubernetes config has been successfully created.")

	scheme := runtime.NewScheme()
	for _, f := range resourcesSchemeFuncs {
		if err := f(scheme); err != nil {
			log.Error(err, "[subMain] unable to add scheme to func")
			return err
		}
	}
	log.Info("[subMain] successfully read scheme CR")

	managerOpts := manager.Options{
		Scheme:                 scheme,
		Logger:                 log.GetLogger(),
		HealthProbeBindAddress: config.HealthProbeBindAddress,
		BaseContext:            func() context.Context { return ctx },
	}

	mgr, err := manager.New(kConfig, managerOpts)
	if err != nil {
		log.Error(err, "[subMain] unable to create manager for creating controllers")
		return err
	}

	schedulerCache := cache.NewCache(*log, config.PVCExpiredDurationSec)
	log.Info("[subMain] scheduler cache was initialized")

	h, err := scheduler.NewHandler(ctx, mgr.GetClient(), *log, schedulerCache, config.DefaultDivisor)
	if err != nil {
		log.Error(err, "[subMain] unable to create http.Handler of the scheduler extender")
		return err
	}
	log.Info("[subMain] scheduler handler initialized")

	if _, err = controller.RunLVGWatcherCacheController(mgr, *log, schedulerCache); err != nil {
		log.Error(err, fmt.Sprintf("[subMain] unable to run %s controller", controller.LVGWatcherCacheCtrlName))
		return err
	}
	log.Info(fmt.Sprintf("[subMain] successfully ran %s controller", controller.LVGWatcherCacheCtrlName))

	if err = controller.RunPVCWatcherCacheController(mgr, *log, schedulerCache); err != nil {
		log.Error(err, fmt.Sprintf("[subMain] unable to run %s controller", controller.PVCWatcherCacheCtrlName))
		return err
	}
	log.Info(fmt.Sprintf("[subMain] successfully ran %s controller", controller.PVCWatcherCacheCtrlName))

	if err = mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		log.Error(err, "[subMain] unable to mgr.AddHealthzCheck")
		return err
	}
	log.Info("[subMain] successfully AddHealthzCheck")

	if err = mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		log.Error(err, "[subMain] unable to mgr.AddReadyzCheck")
		return err
	}
	log.Info("[subMain] successfully AddReadyzCheck")

	serv := &http.Server{
		Addr:         config.ListenAddr,
		Handler:      accessLogHandler(ctx, h),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}
	log.Info("[subMain] server was initialized")

	return runServer(ctx, serv, mgr, log)
}

func runServer(ctx context.Context, serv *http.Server, mgr manager.Manager, log *logger.Logger) error {
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
