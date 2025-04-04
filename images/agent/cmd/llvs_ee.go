//go:build !ce

/*
Copyright 2025 Flant JSC
Licensed under the Deckhouse Platform Enterprise Edition (EE) license.
See https://github.com/deckhouse/deckhouse/blob/main/ee/LICENSE
*/

package main

import (
	"os"

	"github.com/deckhouse/sds-node-configurator/images/agent/internal/cache"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/config"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/controller"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/controller/llvs"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/logger"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/monitoring"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/utils"
	"github.com/deckhouse/sds-node-configurator/lib/go/common/pkg/feature"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

func addLLVSReconciler(
	mgr manager.Manager,
	log logger.Logger,
	metrics monitoring.Metrics,
	sdsCache *cache.Cache,
	commands utils.Commands,
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
			commands,
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
