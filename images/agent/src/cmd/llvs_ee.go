//go:build EE

package main

import (
	"os"

	"sigs.k8s.io/controller-runtime/pkg/manager"

	"agent/internal/cache"
	"agent/internal/config"
	"agent/internal/controller"
	"agent/internal/controller/llvs"
	"agent/internal/logger"
	"agent/internal/monitoring"
)

func addLLVSReconciler(
	mgr manager.Manager,
	log logger.Logger,
	metrics monitoring.Metrics,
	sdsCache *cache.Cache,
	cfgParams *config.Config,
) {
	err := controller.AddReconciler(
		mgr,
		log,
		llvs.NewReconciler(
			mgr.GetClient(),
			log,
			metrics,
			sdsCache,
			llvs.ReconcilerConfig{
				NodeName:                cfgParams.NodeName,
				LLVRequeueInterval:      cfgParams.LLVRequeueInterval,
				VolumeGroupScanInterval: cfgParams.VolumeGroupScanInterval,
				LLVSRequeueInterval:     cfgParams.LLVSRequeueInterval,
			},
		),
	)
	if err != nil {
		log.Error(err, "[main] unable to start llvs.NewReconciler")
		os.Exit(1)
	}
}
