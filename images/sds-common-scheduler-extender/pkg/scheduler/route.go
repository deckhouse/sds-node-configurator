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

	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/cache"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/consts"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/logger"
)

const (
	envTargetProvisioners = "TARGET_PROVISIONERS"
)

type scheduler struct {
	defaultDivisor         float64
	log                    logger.Logger
	client                 client.Client
	ctx                    context.Context
	cache                  *cache.Cache
	targetProvisioners     []string
	filterRequestCount     int
	prioritizeRequestCount int
}

func (s *scheduler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Get logger with trace ID from request context
	requestLog := logger.WithTraceIDLogger(r.Context(), s.log)

	switch r.URL.Path {
	case "/scheduler/filter":
		s.filterRequestCount++
		requestLog.Debug("[ServeHTTP] filter route starts handling the request")
		s.filter(w, r)
		requestLog.Debug("[ServeHTTP] filter route ends handling the request")
	case "/scheduler/prioritize":
		s.prioritizeRequestCount++
		requestLog.Debug("[ServeHTTP] prioritize route starts handling the request")
		s.prioritize(w, r)
		requestLog.Debug("[ServeHTTP] prioritize route ends handling the request")
	case "/v1/lvg/filter-and-score":
		requestLog.Debug("[ServeHTTP] filter-and-score route starts handling the request")
		s.filterAndScore(w, r)
		requestLog.Debug("[ServeHTTP] filter-and-score route ends handling the request")
	case "/v1/lvg/narrow-reservation":
		requestLog.Debug("[ServeHTTP] narrow-reservation route starts handling the request")
		s.narrowReservation(w, r)
		requestLog.Debug("[ServeHTTP] narrow-reservation route ends handling the request")
	case "/status":
		requestLog.Debug("[ServeHTTP] status route starts handling the request")
		status(w, r)
		requestLog.Debug("[ServeHTTP] status route ends handling the request")
	case "/cache":
		requestLog.Debug("[ServeHTTP] cache route starts handling the request")
		s.getCache(w, r)
		requestLog.Debug("[ServeHTTP] cache route ends handling the request")
	case "/stat":
		requestLog.Debug("[ServeHTTP] stat route starts handling the request")
		s.getCacheStat(w, r)
		requestLog.Debug("[ServeHTTP] stat route ends handling the request")
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
		log.Warning(fmt.Sprintf("TARGET_PROVISIONERS environment variable is set but empty after parsing, using default provisioners: %v", defaultProvisioners))
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
	requestLog := logger.WithTraceIDLogger(r.Context(), s.log)
	w.WriteHeader(http.StatusOK)

	// Print pools
	pools := s.cache.GetAllPools()
	for key, reservedSize := range pools {
		line := fmt.Sprintf("Pool: %s, reserved: %s\n", key.String(), resource.NewQuantity(reservedSize, resource.BinarySI).String())
		if _, err := w.Write([]byte(line)); err != nil {
			requestLog.Error(err, "error writing pool info")
			return
		}
	}

	// Print reservations
	reservations := s.cache.GetAllReservations()
	for id, info := range reservations {
		line := fmt.Sprintf("Reservation: %s, size: %s, expires: %s, pools: [", id, resource.NewQuantity(info.Size, resource.BinarySI).String(), info.ExpiresAt.Format("15:04:05"))
		poolStrs := make([]string, 0, len(info.Pools))
		for _, p := range info.Pools {
			poolStrs = append(poolStrs, p.String())
		}
		line += strings.Join(poolStrs, ", ") + "]\n"
		if _, err := w.Write([]byte(line)); err != nil {
			requestLog.Error(err, "error writing reservation info")
			return
		}
	}
}

func (s *scheduler) getCacheStat(w http.ResponseWriter, r *http.Request) {
	requestLog := logger.WithTraceIDLogger(r.Context(), s.log)
	w.WriteHeader(http.StatusOK)

	pools := s.cache.GetAllPools()
	reservations := s.cache.GetAllReservations()

	var totalReserved int64
	for _, reservedSize := range pools {
		totalReserved += reservedSize
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Filter request count: %d, Prioritize request count: %d\n", s.filterRequestCount, s.prioritizeRequestCount))
	sb.WriteString(fmt.Sprintf("Pools: %d, Reservations: %d\n", len(pools), len(reservations)))
	sb.WriteString(fmt.Sprintf("Total reserved across all pools: %s\n", resource.NewQuantity(totalReserved, resource.BinarySI).String()))

	result, err := json.Marshal(pools)
	if err == nil {
		sb.WriteString(fmt.Sprintf("Pools detail: %s\n", string(result)))
	}

	if _, err := w.Write([]byte(sb.String())); err != nil {
		requestLog.Error(err, "error write response for cache stat")
	}
}
