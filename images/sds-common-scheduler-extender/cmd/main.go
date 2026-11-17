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
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	ctrl "sigs.k8s.io/controller-runtime"
	crcache "sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/yaml"

	d8commonapi "github.com/deckhouse/sds-common-lib/api/v1alpha1"
	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/sds-common-lib/monitoring"
	"github.com/deckhouse/sds-common-lib/monitoring/metricsauth"
	slv "github.com/deckhouse/sds-local-volume/api/v1alpha1"
	snc "github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/cache"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/controller"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/kubutils"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/leader"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/scheduler"
	srv "github.com/deckhouse/sds-replicated-volume/api/v1alpha1"
)

const (
	defaultDivisor         = 1
	defaultListenAddr      = ":8000"
	defaultcertFile        = "/etc/sds-common-scheduler-extender/certs/tls.crt"
	defaultkeyFile         = "/etc/sds-common-scheduler-extender/certs/tls.key"
	defaultCAFile          = "/etc/sds-common-scheduler-extender/certs/ca.crt"
	defaultCleanupInterval = 30 * time.Second

	// defaultLeaderServerName is a name the serving certificate carries (see the
	// certificate hook of the module); a standby checks the leader against it.
	defaultLeaderServerName = "sds-common-scheduler-extender"
	leaseName               = "sds-common-scheduler-extender"

	// shutdownTimeout bounds the drain of in-flight requests before the lease is
	// released. The handlers give up after 4 s and kube-scheduler after 5 s.
	shutdownTimeout = 10 * time.Second

	envPodName      = "POD_NAME"
	envPodNamespace = "POD_NAMESPACE"
	envPodIP        = "POD_IP"
)

type Config struct {
	ListenAddr             string  `json:"listen"`
	DefaultDivisor         float64 `json:"default-divisor"`
	LogLevel               string  `json:"log-level"`
	HealthProbeBindAddress string  `json:"health-probe-bind-address"`
	CertFile               string  `json:"cert-file"`
	KeyFile                string  `json:"key-file"`
	CAFile                 string  `json:"ca-file"`
	LeaderServerName       string  `json:"leader-server-name"`
}

var cfgFilePath string

var resourcesSchemeFuncs = []func(*runtime.Scheme) error{
	slv.AddToScheme,
	snc.AddToScheme,
	srv.AddToScheme,
	corev1.AddToScheme,
	storagev1.AddToScheme,
	d8commonapi.AddToScheme,
}

var config = &Config{
	ListenAddr:       defaultListenAddr,
	DefaultDivisor:   defaultDivisor,
	LogLevel:         "2",
	CertFile:         defaultcertFile,
	KeyFile:          defaultkeyFile,
	CAFile:           defaultCAFile,
	LeaderServerName: defaultLeaderServerName,
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

// defaultMetricsBindAddress keeps the endpoint on loopback when the chart says
// nothing: the fleet-wide fallback is the wildcard ":8080", which would answer
// unauthenticated on the pod IP.
const defaultMetricsBindAddress = "127.0.0.1:8080"

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

	// LOG_LEVEL wins over the config file: the file is a ConfigMap the module
	// renders once, the env is what lib-helm keeps in step with the module setting.
	if envLogLevel := os.Getenv(logger.EnvLogLevel); envLogLevel != "" {
		config.LogLevel = envLogLevel
	}

	baseLog, err := logger.NewLogger(logger.Verbosity(config.LogLevel))
	if err != nil {
		print(fmt.Sprintf("unable to initialize logger, err: %s", err))
		return err
	}
	log := baseLog.Named("main")
	mainLog := log
	mainLog.Info(fmt.Sprintf("logger has been initialized, log level: %s", config.LogLevel))

	// Set the logger for the controller-runtime
	ctrl.SetLogger(log.GetLogger())

	kConfig, err := kubutils.KubernetesDefaultConfigCreate()
	if err != nil {
		mainLog.Error("unable to KubernetesDefaultConfigCreate", logger.Err(err))
		return err
	}
	mainLog.Info("kubernetes config has been successfully created.")

	scheme := runtime.NewScheme()
	for _, f := range resourcesSchemeFuncs {
		if err := f(scheme); err != nil {
			mainLog.Error("unable to add scheme to func", logger.Err(err))
			return err
		}
	}
	mainLog.Info("successfully read scheme CR")

	managerOpts := manager.Options{
		Scheme: scheme,
		Logger: log.GetLogger(),
		// controller-runtime binds :8080 whenever this is empty, so the extender has
		// always served its own reconcile and workqueue metrics -- nothing declared the
		// port or scraped it. Naming the address here puts it under the same
		// METRICS_BIND_ADDRESS the rest of the fleet reads, and the chart declares
		// the port and ships a PodMonitor for it.
		Metrics:                metricsauth.ApplyFromEnv(metricsserver.Options{BindAddress: monitoring.BindAddressFromEnvOr(defaultMetricsBindAddress)}),
		HealthProbeBindAddress: config.HealthProbeBindAddress,
		BaseContext:            func() context.Context { return ctx },
		Cache: crcache.Options{
			DefaultTransform: crcache.TransformStripManagedFields(),
		},
	}

	mgr, err := manager.New(kConfig, managerOpts)
	if err != nil {
		mainLog.Error("unable to create manager for creating controllers", logger.Err(err))
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
		mainLog.Error("unable to register field indexer for LVMVolumeGroup", logger.Err(err))
		return err
	}
	mainLog.Info("successfully registered LVMVolumeGroup field indexer on status.nodes.name")

	// Register field indexer for LVMLogicalVolume -> LVG name mapping.
	// Enables efficient per-LVG LLV listings for available space computation.
	err = mgr.GetFieldIndexer().IndexField(ctx, &snc.LVMLogicalVolume{}, scheduler.IndexFieldLLVLVGName,
		func(obj client.Object) []string {
			llv := obj.(*snc.LVMLogicalVolume)
			return []string{llv.Spec.LVMVolumeGroupName}
		},
	)
	if err != nil {
		mainLog.Error("unable to register field indexer for LVMLogicalVolume", logger.Err(err))
		return err
	}
	mainLog.Info("successfully registered LVMLogicalVolume field indexer on spec.lvmVolumeGroupName")

	schedulerCache := cache.NewCache(log, defaultCleanupInterval)
	mainLog.Info("scheduler cache was initialized")

	schedulerHandler, err := scheduler.NewHandler(ctx, mgr.GetClient(), log, schedulerCache, config.DefaultDivisor)
	if err != nil {
		mainLog.Error("unable to create http.Handler of the scheduler extender", logger.Err(err))
		return err
	}
	mainLog.Info("scheduler handler initialized")

	if err = controller.RunPVCWatcherCacheController(mgr, log, schedulerCache); err != nil {
		mainLog.Error(fmt.Sprintf("unable to run %s controller", controller.PVCWatcherCacheCtrlName), logger.Err(err))
		return err
	}
	mainLog.Info(fmt.Sprintf("successfully ran %s controller", controller.PVCWatcherCacheCtrlName))

	if err = controller.RunLLVWatcherCacheController(mgr, log, schedulerCache); err != nil {
		mainLog.Error(fmt.Sprintf("unable to run %s controller", controller.LLVWatcherCacheCtrlName), logger.Err(err))
		return err
	}
	mainLog.Info(fmt.Sprintf("successfully ran %s controller", controller.LLVWatcherCacheCtrlName))

	if err = controller.RunLVGWatcherCacheController(mgr, log, schedulerCache); err != nil {
		mainLog.Error(fmt.Sprintf("unable to run %s controller", controller.LVGWatcherCacheCtrlName), logger.Err(err))
		return err
	}
	mainLog.Info(fmt.Sprintf("successfully ran %s controller", controller.LVGWatcherCacheCtrlName))

	if err = mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		mainLog.Error("unable to mgr.AddHealthzCheck", logger.Err(err))
		return err
	}
	mainLog.Info("successfully AddHealthzCheck")

	if err = mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		mainLog.Error("unable to mgr.AddReadyzCheck", logger.Err(err))
		return err
	}
	mainLog.Info("successfully AddReadyzCheck")

	leaderState, lock, err := newLeaderElection(log, kConfig)
	if err != nil {
		mainLog.Error("unable to set up the leader election", logger.Err(err))
		return err
	}
	mainLog.Info(fmt.Sprintf("leader election is set up, identity %q", leaderState.Identity()))

	caPEM, err := os.ReadFile(config.CAFile)
	if err != nil {
		mainLog.Error("unable to read the CA bundle", logger.Err(err))
		return err
	}
	leaderTransport, err := leader.NewLeaderTransport(caPEM, config.LeaderServerName)
	if err != nil {
		mainLog.Error("unable to build the transport to the leader", logger.Err(err))
		return err
	}
	router := leader.NewRouter(log.Named("leader"), leaderState, schedulerHandler, scheduler.NeedsReservationCache, leaderTransport)

	serv := &http.Server{
		Addr:         config.ListenAddr,
		Handler:      accessLogHandler(log, router),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}
	mainLog.Info("server was initialized")

	return runServer(ctx, serv, mgr, leaderState, lock, log)
}

// newLeaderElection returns the election state of this replica and the Lease it
// competes for. The pod coordinates come from the downward API; without them a
// replica could not tell the others where it listens, so their absence is fatal
// rather than a silent fallback to serving reservations from every replica.
func newLeaderElection(log logger.Logger, kConfig *rest.Config) (*leader.State, resourcelock.Interface, error) {
	podName, podNamespace, podIP := os.Getenv(envPodName), os.Getenv(envPodNamespace), os.Getenv(envPodIP)
	if podName == "" || podNamespace == "" || podIP == "" {
		return nil, nil, fmt.Errorf("%s, %s and %s must all be set", envPodName, envPodNamespace, envPodIP)
	}
	if net.ParseIP(podIP) == nil {
		return nil, nil, fmt.Errorf("%s=%q is not an IP address", envPodIP, podIP)
	}

	_, port, err := net.SplitHostPort(config.ListenAddr)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to take the port from the listen address %q: %w", config.ListenAddr, err)
	}

	clientset, err := kubernetes.NewForConfig(rest.CopyConfig(kConfig))
	if err != nil {
		return nil, nil, fmt.Errorf("unable to create the leader election client: %w", err)
	}

	identity := leader.Identity(podName, podIP)
	lock := &resourcelock.LeaseLock{
		LeaseMeta:  metav1.ObjectMeta{Name: leaseName, Namespace: podNamespace},
		Client:     clientset.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{Identity: identity},
	}
	return leader.NewState(log.Named("leader-election"), identity, port), lock, nil
}

// runServer serves until ctx is done or the lease is lost. The order of the
// shutdown is what keeps a single replica answering reservation requests: the
// HTTP server drains first, and only then is the lease released, so the next
// leader starts after the last reservation decision of this one.
func runServer(ctx context.Context, serv *http.Server, mgr manager.Manager, leaderState *leader.State, lock resourcelock.Interface, log logger.Logger) error {
	serveCtx, stopServing := context.WithCancel(ctx)
	defer stopServing()

	// Not derived from ctx: cancelling it releases the lease, which must wait
	// for the drain below.
	electionCtx, stopElection := context.WithCancel(context.Background())
	defer stopElection()

	var (
		wg          sync.WaitGroup
		electionErr error
	)
	wg.Add(2)

	go func() {
		defer wg.Done()
		// A replica that took the lease with cold informers would block the
		// first requests on the sync, past the kube-scheduler timeout. This waits
		// for the informers started up front (LVMVolumeGroup, LVMLogicalVolume,
		// PVC); the ones the handlers open lazily still sync on first use.
		if !mgr.GetCache().WaitForCacheSync(serveCtx) {
			return
		}
		log.Info("informer caches are synced, joining the leader election")
		electionErr = leaderState.Run(electionCtx, lock)
		if electionErr != nil {
			log.Error("stopping: the reservations of this replica are no longer authoritative", logger.Err(electionErr))
			stopServing()
		}
	}()

	go func() {
		defer wg.Done()
		<-serveCtx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := serv.Shutdown(shutdownCtx); err != nil {
			log.Error("failed to shutdown gracefully", logger.Err(err))
		}
		stopElection()
	}()

	go func() {
		log.Info("kube manager will start now")
		if err := mgr.Start(ctx); err != nil {
			log.Error("unable to mgr.Start", logger.Err(err))
		}
	}()

	log.Info(fmt.Sprintf("starts serving on: %s", config.ListenAddr))

	if err := serv.ListenAndServeTLS(config.CertFile, config.KeyFile); !errors.Is(err, http.ErrServerClosed) {
		log.Error("unable to run the server", logger.Err(err))
		stopServing()
		wg.Wait()
		return err
	}

	wg.Wait()
	return electionErr
}
