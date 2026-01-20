/*
Copyright YEAR Flant JSC

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
	"fmt"
	"strings"

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

// ReplicaEvaluate performs filtering and scoring for a single replica placement request,
// and optionally reserves space in the cache using the provided replica key.
func (s *scheduler) ReplicaEvaluate(args ReplicaEvaluateArgs) (*ReplicaEvaluateResult, error) {
	if args.RequestedBytes <= 0 {
		return nil, fmt.Errorf("requestedBytes must be > 0")
	}
	// Build free space snapshots from cache
	lvgs := s.cacheMgr.GetAllLVG()

	thickFree := map[string]int64{}
	thinFree := map[string]map[string]int64{}
	for name, lvg := range lvgs {
		thickFree[name] = lvg.Status.VGFree.Value() - func() int64 { v, _ := s.cacheMgr.GetLVGThickReservedSpace(name); return v }()
		if lvg.Status.ThinPools != nil {
			thinFree[name] = map[string]int64{}
			for _, tp := range lvg.Status.ThinPools {
				v, _ := s.cacheMgr.GetLVGThinReservedSpace(name, tp.Name)
				thinFree[name][tp.Name] = tp.AvailableSpace.Value() - v
			}
		}
	}

	failed := map[string]string{}
	passed := make([]string, 0)

	// Evaluate Thick candidates
	for _, lvgName := range args.CandidatesThick {
		lvg := s.cacheMgr.TryGetLVG(lvgName)
		if lvg == nil {
			failed[lvgName] = "LVG not found in cache"
			continue
		}
		free := thickFree[lvgName]
		if free < args.RequestedBytes {
			failed[lvgName] = "insufficient thick space"
			continue
		}
		thickFree[lvgName] = free - args.RequestedBytes
		passed = append(passed, lvgName)
	}

	// Evaluate Thin candidates
	for _, c := range args.CandidatesThin {
		lvg := s.cacheMgr.TryGetLVG(c.LVGName)
		if lvg == nil {
			failed[c.LVGName+"/"+c.ThinPool] = "LVG not found in cache"
			continue
		}
		pools, ok := thinFree[c.LVGName]
		if !ok {
			failed[c.LVGName+"/"+c.ThinPool] = "no thin pools in LVG"
			continue
		}
		free := pools[c.ThinPool]
		if free < args.RequestedBytes {
			failed[c.LVGName+"/"+c.ThinPool] = "insufficient thin space"
			continue
		}
		thinFree[c.LVGName][c.ThinPool] = free - args.RequestedBytes
		passed = append(passed, c.LVGName+"/"+c.ThinPool)
	}

	// Prioritize candidates: more free space left is better
	priorities := make([]HostPriority, 0, len(passed))
	for _, id := range passed {
		if strings.Contains(id, "/") {
			// Thin candidate id format: LVG/Pool
			parts := strings.SplitN(id, "/", 2)
			lvg := s.cacheMgr.TryGetLVG(parts[0])
			if lvg == nil {
				continue
			}
			free := thinFree[parts[0]][parts[1]]
			var total int64
			for _, tp := range lvg.Status.ThinPools {
				if tp.Name == parts[1] {
					total = tp.ActualSize.Value()
					break
				}
			}
			pct := getFreeSpaceLeftAsPercent(free, 0, total)
			priorities = append(priorities, HostPriority{Host: id, Score: getNodeScore(pct, 1/s.defaultDivisor)})
		} else {
			// Thick LVG
			lvg := s.cacheMgr.TryGetLVG(id)
			if lvg == nil {
				continue
			}
			free := thickFree[id]
			total := lvg.Status.VGSize.Value()
			pct := getFreeSpaceLeftAsPercent(free, 0, total)
			priorities = append(priorities, HostPriority{Host: id, Score: getNodeScore(pct, 1/s.defaultDivisor)})
		}
	}

	// Optionally reserve in shared cache for all passed candidates
	if args.Reserve {
		for _, id := range passed {
			if strings.Contains(id, "/") {
				parts := strings.SplitN(id, "/", 2)
				_ = s.cacheMgr.ReserveThinReplica(parts[0], parts[1], args.ReplicaKey, args.RequestedBytes)
			} else {
				_ = s.cacheMgr.ReserveThickReplica(id, args.ReplicaKey, args.RequestedBytes)
			}
		}
	}

	return &ReplicaEvaluateResult{NodeNames: passed, FailedNodes: failed, Priorities: priorities}, nil
}

// ReplicaCleanup clears replica reservations according to provided flags
func (s *scheduler) ReplicaCleanup(args ReplicaCleanupArgs) error {
	s.cacheMgr.RemoveReplicaReservations(args.ReplicaKey, args.SelectedThickLVG, args.SelectedThinLVG, args.SelectedThinPool, args.ClearNonSelected, args.ClearSelected)
	return nil
}
