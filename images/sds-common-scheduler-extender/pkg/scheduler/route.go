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
	case "/api/v1/volumes/filter-prioritize":
		requestLog.Debug("[ServeHTTP] filter-prioritize route starts handling the request")
		s.filterAndPrioritize(w, r)
		requestLog.Debug("[ServeHTTP] filter-prioritize route ends handling the request")
	case "/api/v1/volumes/bind":
		requestLog.Debug("[ServeHTTP] bind route starts handling the request")
		s.bindVolume(w, r)
		requestLog.Debug("[ServeHTTP] bind route ends handling the request")
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
// If TARGET_PROVISIONERS is not set, returns default provisioners.
// The environment variable can contain comma-separated list of provisioners.
func getTargetProvisioners(log logger.Logger) []string {
	envValue := os.Getenv(envTargetProvisioners)
	if envValue == "" {
		// Return default provisioners if environment variable is not set
		defaultProvisioners := []string{consts.SdsLocalVolumeProvisioner, consts.SdsReplicatedVolumeProvisioner}
		log.Info(fmt.Sprintf("TARGET_PROVISIONERS environment variable is not set, using default provisioners: %v", defaultProvisioners))
		return defaultProvisioners
	}

	// Parse comma-separated provisioners
	provisioners := strings.Split(envValue, ",")
	result := make([]string, 0, len(provisioners))
	for _, p := range provisioners {
		trimmed := strings.TrimSpace(p)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}

	if len(result) == 0 {
		// Fallback to default if parsing resulted in empty list
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

	s.cache.PrintTheCacheLog()

	lvgs := s.cache.GetAllLVG()
	for _, lvg := range lvgs {
		reserved, err := s.cache.GetLVGThickReservedSpace(lvg.Name)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_, err = w.Write([]byte("unable to write the cache"))
			if err != nil {
				requestLog.Error(err, "error write response")
			}
		}

		_, err = w.Write([]byte(fmt.Sprintf("LVMVolumeGroup: %s Thick Reserved: %s\n", lvg.Name, resource.NewQuantity(reserved, resource.BinarySI).String())))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_, err = w.Write([]byte("unable to write the cache"))
			if err != nil {
				requestLog.Error(err, "error write response")
			}
		}

		thickPvcs, err := s.cache.GetAllThickPVCLVG(lvg.Name)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_, err = w.Write([]byte("unable to write the cache"))
			if err != nil {
				requestLog.Error(err, "error write response")
			}
		}
		for _, pvc := range thickPvcs {
			_, err = w.Write([]byte(fmt.Sprintf("\t\tThick PVC: %s, reserved: %s, selected node: %s\n", pvc.Name, pvc.Spec.Resources.Requests.Storage().String(), pvc.Annotations[cache.SelectedNodeAnnotation])))
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				requestLog.Error(err, "error write response")
			}
		}

		for _, tp := range lvg.Status.ThinPools {
			thinReserved, err := s.cache.GetLVGThinReservedSpace(lvg.Name, tp.Name)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				requestLog.Error(err, "error write response")
			}
			_, err = w.Write([]byte(fmt.Sprintf("\tThinPool: %s, reserved: %s\n", tp.Name, resource.NewQuantity(thinReserved, resource.BinarySI).String())))
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				requestLog.Error(err, "error write response")
			}

			thinPvcs, err := s.cache.GetAllPVCFromLVGThinPool(lvg.Name, tp.Name)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				requestLog.Error(err, "error write response")
			}

			for _, pvc := range thinPvcs {
				_, err = w.Write([]byte(fmt.Sprintf("\t\tThin PVC: %s, reserved: %s, selected node:%s\n", pvc.Name, pvc.Spec.Resources.Requests.Storage().String(), pvc.Annotations[cache.SelectedNodeAnnotation])))
				if err != nil {
					w.WriteHeader(http.StatusInternalServerError)
					requestLog.Error(err, "error write response")
				}
			}
		}
	}
}

func (s *scheduler) getCacheStat(w http.ResponseWriter, r *http.Request) {
	requestLog := logger.WithTraceIDLogger(r.Context(), s.log)
	w.WriteHeader(http.StatusOK)

	pvcTotalCount := 0
	var totalReserved int64
	var sb strings.Builder
	lvgs := s.cache.GetAllLVG()
	for _, lvg := range lvgs {
		pvcs, err := s.cache.GetAllPVCForLVG(lvg.Name)
		if err != nil {
			requestLog.Error(err, "something bad")
		}

		pvcTotalCount += len(pvcs)

		// sum thick reserved
		thickReserved, err := s.cache.GetLVGThickReservedSpace(lvg.Name)
		if err != nil {
			requestLog.Error(err, "unable to get thick reserved space")
		}
		totalReserved += thickReserved
		// sum thin reserved across all thin pools
		for _, tp := range lvg.Status.ThinPools {
			thinReserved, err := s.cache.GetLVGThinReservedSpace(lvg.Name, tp.Name)
			if err != nil {
				requestLog.Error(err, "unable to get thin reserved space")
				continue
			}
			totalReserved += thinReserved
		}
	}

	sb.WriteString(fmt.Sprintf("Filter request count: %d, Prioritize request count: %d\n", s.filterRequestCount, s.prioritizeRequestCount))
	sb.WriteString(fmt.Sprintf("Total reserved (thick+thin) across all PVCs (%d items): %s\n", pvcTotalCount, resource.NewQuantity(totalReserved, resource.BinarySI).String()))

	_, err := w.Write([]byte(sb.String()))
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, err = w.Write([]byte("unable to write the cache"))
		if err != nil {
			requestLog.Error(err, "error write response for cache stat")
		}
	}
}
