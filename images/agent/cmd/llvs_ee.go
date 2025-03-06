//go:build !ce

/*
Copyright 2025 Flant JSC
Licensed under the Deckhouse Platform Enterprise Edition (EE) license.
See https://github.com/deckhouse/deckhouse/blob/main/ee/LICENSE
*/

package main

import (
	"os"

	"github.com/deckhouse/sds-node-configurator/lib/go/common/pkg/feature"
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
	if !feature.SnapshotsEnabled() {
		log.Info("[addLLVSReconciler] Snapshot feature is disabled")
		return
	}

	log.Info("[addLLVSReconciler] Snapshot feature is enabled. Adding LLVS reconciler")

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
