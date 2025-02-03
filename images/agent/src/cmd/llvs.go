//go:build !EE

package main

import (
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"agent/internal/cache"
	"agent/internal/config"
	"agent/internal/logger"
	"agent/internal/monitoring"
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
