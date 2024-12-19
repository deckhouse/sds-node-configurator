//go:build !ee

package main

import (
	"agent/internal/cache"
	"agent/internal/config"
	"agent/internal/logger"
	"agent/internal/monitoring"

	"sigs.k8s.io/controller-runtime/pkg/manager"
)

func addLLVSReconciler(
	_ manager.Manager,
	_ logger.Logger,
	_ monitoring.Metrics,
	_ *cache.Cache,
	_ *config.Config,
) {
	// noop
}
