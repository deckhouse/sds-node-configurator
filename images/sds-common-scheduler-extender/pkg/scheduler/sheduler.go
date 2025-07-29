package scheduler

import (
	"context"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/cache"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/logger"
)

type scheduler struct {
	ctx            context.Context
	log            *logger.Logger
	client         client.Client
	cacheMgr       *cache.CacheManager
	defaultDivisor float64
}

func NewScheduler(ctx context.Context, cl client.Client, log *logger.Logger, cacheMgr *cache.CacheManager, defaultDiv float64) *scheduler {
	return &scheduler{
		defaultDivisor: defaultDiv,
		log:            log,
		client:         cl,
		ctx:            ctx,
		cacheMgr:       cacheMgr,
	}
}
