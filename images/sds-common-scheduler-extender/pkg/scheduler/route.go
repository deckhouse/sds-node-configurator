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
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync/atomic"

	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/cache"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/consts"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/tracing"
)

const (
	envTargetProvisioners = "TARGET_PROVISIONERS"
)

// NeedsReservationCache reports whether the route reads or writes the space
// reservations. The reservations live in the memory of one process, so these
// routes must all be served by the same replica. /cache and /stat also read them,
// but only to show what the replica they reached holds.
func NeedsReservationCache(path string) bool {
	switch path {
	case "/scheduler/filter", "/scheduler/prioritize", "/v1/lvg/filter-and-score", "/v1/lvg/narrow-reservation":
		return true
	}
	return false
}

type scheduler struct {
	client             client.Client
	ctx                context.Context
	log                logger.Logger
	cache              *cache.Cache
	defaultDivisor     float64
	targetProvisioners []string
	// Request counters for /stat. ServeHTTP runs in a goroutine per connection, so
	// they are atomic. The dry-run counters are part of the totals and count only
	// requests decoded with a Pod: the router does not read the body.
	filterRequestCount           atomic.Int64
	filterDryRunRequestCount     atomic.Int64
	prioritizeRequestCount       atomic.Int64
	prioritizeDryRunRequestCount atomic.Int64
	filterAndScoreRequestCount   atomic.Int64
}

func (s *scheduler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Get logger with trace ID from request context
	// Named once per request: Named allocates before the level is checked, and this
	// handler logs on every route it dispatches.
	requestLog := tracing.WithTraceIDLogger(r.Context(), s.log).Named("http")

	switch r.URL.Path {
	case "/scheduler/filter":
		s.filterRequestCount.Add(1)
		requestLog.Debug("filter route starts handling the request")
		s.filter(w, r)
		requestLog.Debug("filter route ends handling the request")
	case "/scheduler/prioritize":
		s.prioritizeRequestCount.Add(1)
		requestLog.Debug("prioritize route starts handling the request")
		s.prioritize(w, r)
		requestLog.Debug("prioritize route ends handling the request")
	case "/v1/lvg/filter-and-score":
		s.filterAndScoreRequestCount.Add(1)
		requestLog.Debug("filter-and-score route starts handling the request")
		s.filterAndScore(w, r)
		requestLog.Debug("filter-and-score route ends handling the request")
	case "/v1/lvg/narrow-reservation":
		requestLog.Debug("narrow-reservation route starts handling the request")
		s.narrowReservation(w, r)
		requestLog.Debug("narrow-reservation route ends handling the request")
	case "/status":
		requestLog.Debug("status route starts handling the request")
		status(w, r)
		requestLog.Debug("status route ends handling the request")
	case "/cache":
		requestLog.Debug("cache route starts handling the request")
		s.getCache(w, r)
		requestLog.Debug("cache route ends handling the request")
	case "/stat":
		requestLog.Debug("stat route starts handling the request")
		s.getCacheStat(w, r)
		requestLog.Debug("stat route ends handling the request")
	default:
		http.Error(w, "not found", http.StatusNotFound)
	}
}

// getTargetProvisioners reads target provisioners from environment variable.
func getTargetProvisioners(log logger.Logger) []string {
	envValue := os.Getenv(envTargetProvisioners)
	if envValue == "" {
		defaultProvisioners := []string{consts.SdsLocalVolumeProvisioner, consts.SdsReplicatedVolumeProvisioner}
		log.Info(fmt.Sprintf("TARGET_PROVISIONERS environment variable is not set, using default provisioners: %v", defaultProvisioners))
		return defaultProvisioners
	}

	provisioners := strings.Split(envValue, ",")
	result := make([]string, 0, len(provisioners))
	for _, p := range provisioners {
		trimmed := strings.TrimSpace(p)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}

	if len(result) == 0 {
		defaultProvisioners := []string{consts.SdsLocalVolumeProvisioner, consts.SdsReplicatedVolumeProvisioner}
		log.Warn(fmt.Sprintf("TARGET_PROVISIONERS environment variable is set but empty after parsing, using default provisioners: %v", defaultProvisioners))
		return defaultProvisioners
	}

	log.Info(fmt.Sprintf("Using target provisioners from TARGET_PROVISIONERS environment variable: %v", result))
	return result
}

// NewHandler return new http.Handler of the scheduler extender
func NewHandler(ctx context.Context, cl client.Client, log logger.Logger, lvgCache *cache.Cache, defaultDiv float64) (http.Handler, error) {
	targetProvisioners := getTargetProvisioners(log)
	return &scheduler{
		defaultDivisor:     defaultDiv,
		log:                log,
		client:             cl,
		ctx:                ctx,
		cache:              lvgCache,
		targetProvisioners: targetProvisioners,
	}, nil
}

func status(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, err := w.Write([]byte("ok"))
	if err != nil {
		fmt.Printf("error occurs on status route, err: %s\n", err.Error())
	}
}

func (s *scheduler) getCache(w http.ResponseWriter, r *http.Request) {
	// Named once per request: Named allocates before the level is checked, and this
	// handler logs on every route it dispatches.
	requestLog := tracing.WithTraceIDLogger(r.Context(), s.log).Named("http")
	w.WriteHeader(http.StatusOK)

	// Print pools
	pools := s.cache.GetAllPools()
	for key, reservedSize := range pools {
		line := fmt.Sprintf("Pool: %s, reserved: %s\n", key.String(), resource.NewQuantity(reservedSize, resource.BinarySI).String())
		if _, err := w.Write([]byte(line)); err != nil {
			requestLog.Error("error writing pool info", logger.Err(err))
			return
		}
	}

	// Print reservations
	reservations := s.cache.GetAllReservations()
	for id, info := range reservations {
		prefix := "Reservation"
		if info.Expired {
			prefix = "[EXPIRED] Reservation"
		}
		line := fmt.Sprintf("%s: %s, size: %s, expires: %s, pools: [", prefix, id, resource.NewQuantity(info.Size, resource.BinarySI).String(), info.ExpiresAt.Format("15:04:05"))
		poolStrs := make([]string, 0, len(info.Pools))
		for _, p := range info.Pools {
			poolStrs = append(poolStrs, p.String())
		}
		line += strings.Join(poolStrs, ", ") + "]\n"
		if _, err := w.Write([]byte(line)); err != nil {
			requestLog.Error("error writing reservation info", logger.Err(err))
			return
		}
	}
}

func (s *scheduler) getCacheStat(w http.ResponseWriter, r *http.Request) {
	// Named once per request: Named allocates before the level is checked, and this
	// handler logs on every route it dispatches.
	requestLog := tracing.WithTraceIDLogger(r.Context(), s.log).Named("http")
	w.WriteHeader(http.StatusOK)

	pools := s.cache.GetAllPools()
	reservations := s.cache.GetAllReservations()

	var totalReserved int64
	for _, reservedSize := range pools {
		totalReserved += reservedSize
	}

	var expiredCount int
	for _, info := range reservations {
		if info.Expired {
			expiredCount++
		}
	}
	activeCount := len(reservations) - expiredCount

	var sb strings.Builder
	// Dry-run counters first: a request adds to the total before its dry-run counter,
	// so reading them in this order never shows more dry runs than requests.
	filterDryRun, prioritizeDryRun := s.filterDryRunRequestCount.Load(), s.prioritizeDryRunRequestCount.Load()
	filter, prioritize := s.filterRequestCount.Load(), s.prioritizeRequestCount.Load()
	fmt.Fprintf(&sb, "Filter request count: %d (real: %d, dry-run: %d), Prioritize request count: %d (real: %d, dry-run: %d), Filter-and-score request count: %d\n",
		filter, filter-filterDryRun, filterDryRun, prioritize, prioritize-prioritizeDryRun, prioritizeDryRun, s.filterAndScoreRequestCount.Load())
	fmt.Fprintf(&sb, "Pools: %d, Reservations: %d (active: %d, expired: %d)\n", len(pools), len(reservations), activeCount, expiredCount)
	fmt.Fprintf(&sb, "Total reserved across all pools: %s\n", resource.NewQuantity(totalReserved, resource.BinarySI).String())

	poolsForJSON := make(map[string]int64, len(pools))
	for k, v := range pools {
		poolsForJSON[k.String()] = v
	}
	result, err := json.Marshal(poolsForJSON)
	if err == nil {
		fmt.Fprintf(&sb, "Pools detail: %s\n", string(result))
	}

	if _, err := w.Write([]byte(sb.String())); err != nil {
		requestLog.Error("error write response for cache stat", logger.Err(err))
	}
}
