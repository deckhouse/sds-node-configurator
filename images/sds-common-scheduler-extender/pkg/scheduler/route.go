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
	"strings"

	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/cache"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/logger"
)

type scheduler struct {
	defaultDivisor         float64
	log                    logger.Logger
	client                 client.Client
	ctx                    context.Context
	cache                  *cache.Cache
	filterRequestCount     int
	prioritizeRequestCount int
}

func (s *scheduler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/scheduler/filter":
		s.filterRequestCount++
		s.log.Debug("[ServeHTTP] filter route starts handling the request")
		s.filter(w, r)
		s.log.Debug("[ServeHTTP] filter route ends handling the request")
	case "/scheduler/prioritize":
		s.prioritizeRequestCount++
		s.log.Debug("[ServeHTTP] prioritize route starts handling the request")
		s.prioritize(w, r)
		s.log.Debug("[ServeHTTP] prioritize route ends handling the request")
	case "/status":
		s.log.Debug("[ServeHTTP] status route starts handling the request")
		status(w, r)
		s.log.Debug("[ServeHTTP] status route ends handling the request")
	case "/cache":
		s.log.Debug("[ServeHTTP] cache route starts handling the request")
		s.getCache(w, r)
		s.log.Debug("[ServeHTTP] cache route ends handling the request")
	case "/stat":
		s.log.Debug("[ServeHTTP] stat route starts handling the request")
		s.getCacheStat(w, r)
		s.log.Debug("[ServeHTTP] stat route ends handling the request")
	default:
		http.Error(w, "not found", http.StatusNotFound)
	}
}

// NewHandler return new http.Handler of the scheduler extender
func NewHandler(ctx context.Context, cl client.Client, log logger.Logger, lvgCache *cache.Cache, defaultDiv float64) (http.Handler, error) {
	return &scheduler{
		defaultDivisor: defaultDiv,
		log:            log,
		client:         cl,
		ctx:            ctx,
		cache:          lvgCache,
	}, nil
}

func status(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, err := w.Write([]byte("ok"))
	if err != nil {
		fmt.Printf("error occurs on status route, err: %s\n", err.Error())
	}
}

func (s *scheduler) getCache(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)

	s.cache.PrintTheCacheLog()

	lvgs := s.cache.GetAllLVG()
	for _, lvg := range lvgs {
		reserved, err := s.cache.GetLVGThickReservedSpace(lvg.Name)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_, err = w.Write([]byte("unable to write the cache"))
			if err != nil {
				s.log.Error(err, "error write response")
			}
		}

		_, err = w.Write([]byte(fmt.Sprintf("LVMVolumeGroup: %s Thick Reserved: %s\n", lvg.Name, resource.NewQuantity(reserved, resource.BinarySI).String())))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_, err = w.Write([]byte("unable to write the cache"))
			if err != nil {
				s.log.Error(err, "error write response")
			}
		}

		thickPvcs, err := s.cache.GetAllThickPVCLVG(lvg.Name)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_, err = w.Write([]byte("unable to write the cache"))
			if err != nil {
				s.log.Error(err, "error write response")
			}
		}
		for _, pvc := range thickPvcs {
			_, err = w.Write([]byte(fmt.Sprintf("\t\tThick PVC: %s, reserved: %s, selected node: %s\n", pvc.Name, pvc.Spec.Resources.Requests.Storage().String(), pvc.Annotations[cache.SelectedNodeAnnotation])))
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				s.log.Error(err, "error write response")
			}
		}

		for _, tp := range lvg.Status.ThinPools {
			thinReserved, err := s.cache.GetLVGThinReservedSpace(lvg.Name, tp.Name)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				s.log.Error(err, "error write response")
			}
			_, err = w.Write([]byte(fmt.Sprintf("\tThinPool: %s, reserved: %s\n", tp.Name, resource.NewQuantity(thinReserved, resource.BinarySI).String())))
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				s.log.Error(err, "error write response")
			}

			thinPvcs, err := s.cache.GetAllPVCFromLVGThinPool(lvg.Name, tp.Name)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				s.log.Error(err, "error write response")
			}

			for _, pvc := range thinPvcs {
				_, err = w.Write([]byte(fmt.Sprintf("\t\tThin PVC: %s, reserved: %s, selected node:%s\n", pvc.Name, pvc.Spec.Resources.Requests.Storage().String(), pvc.Annotations[cache.SelectedNodeAnnotation])))
				if err != nil {
					w.WriteHeader(http.StatusInternalServerError)
					s.log.Error(err, "error write response")
				}
			}
		}
	}
}

func (s *scheduler) getCacheStat(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)

	pvcTotalCount := 0
	var totalReserved int64
	var sb strings.Builder
	lvgs := s.cache.GetAllLVG()
	for _, lvg := range lvgs {
		pvcs, err := s.cache.GetAllPVCForLVG(lvg.Name)
		if err != nil {
			s.log.Error(err, "something bad")
		}

		pvcTotalCount += len(pvcs)

		// sum thick reserved
		thickReserved, err := s.cache.GetLVGThickReservedSpace(lvg.Name)
		if err != nil {
			s.log.Error(err, "unable to get thick reserved space")
		}
		totalReserved += thickReserved
		// sum thin reserved across all thin pools
		for _, tp := range lvg.Status.ThinPools {
			thinReserved, err := s.cache.GetLVGThinReservedSpace(lvg.Name, tp.Name)
			if err != nil {
				s.log.Error(err, "unable to get thin reserved space")
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
			s.log.Error(err, "error write response for cache stat")
		}
	}
}
