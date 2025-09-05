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

package scheduler

import (
	"context"

	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/cache"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/logger"

	"sigs.k8s.io/controller-runtime/pkg/client"
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
