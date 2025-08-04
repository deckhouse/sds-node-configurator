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
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/logger"
)

// type Middleware struct {
// 	Handler http.Handler
// 	Log     *logger.Logger
// }

// func NewMiddleware(handler http.Handler, log *logger.Logger) *Middleware {
// 	return &Middleware{
// 		Handler: handler,
// 		Log:     log,
// 	}
// }

func BodyUnmarshalMiddleware(next http.Handler, log *logger.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var inputData ExtenderArgs
		reader := http.MaxBytesReader(w, r.Body, 10<<20)
		if err := json.NewDecoder(reader).Decode(&inputData); err != nil {
			log.Error(err, "[handler] unable to decode filter request")
			http.Error(w, "unable to decode request", http.StatusBadRequest)
			return
		}

		cwv := context.WithValue(r.Context(), "inputData", inputData)
		req := r.WithContext(cwv)
		next.ServeHTTP(w, req)
	})
}

func LogMiddleware(next http.Handler, log *logger.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r)

		startTime := time.Now()
		// status := m.handler.Status

		fields := []interface{}{
			"type", "access",
			"response_time", time.Since(startTime).Seconds(),
			"protocol", r.Proto,
			// "http_status_code", status,
			"http_method", r.Method,
			"url", r.RequestURI,
			"http_host", r.Host,
			"request_size", r.ContentLength,
			// "response_size", wr.size,
		}
		ip, _, err := net.SplitHostPort(r.RemoteAddr)
		if err == nil {
			fields = append(fields, "remote_ipaddr", ip)
		}
		ua := r.Header.Get("User-Agent")
		if len(ua) > 0 {
			fields = append(fields, "http_user_agent", ua)
		}
		log.Info("access", fields...)
	})
}

func PodCheckMiddleware(ctx context.Context, cl client.Client, next http.Handler, log *logger.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inputData, ok := r.Context().Value("inputData").(ExtenderArgs)
		if !ok {
			log.Error(errors.New("pod data not found in context"), "[WithPodCheck] missing pod data")
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		pod := inputData.Pod

		pvcs := &corev1.PersistentVolumeClaimList{}
		if err := cl.List(ctx, pvcs); err != nil {
			log.Error(err, "[WithPodCheck] error listing PVCs")
			http.Error(w, "error listing PVCs", http.StatusInternalServerError)
		}

		pvcMap := make(map[string]*corev1.PersistentVolumeClaim, len(pvcs.Items))
		for _, pvc := range pvcs.Items {
			pvcMap[pvc.Name] = &pvc
		}

		volumes, err := shouldProcessPod(ctx, cl, pvcMap, log, pod)
		if err != nil {
			log.Error(err, fmt.Sprintf("[WithPodCheck] error processing pod %s/%s", pod.Namespace, pod.Name))
			result := &ExtenderFilterResult{NodeNames: inputData.NodeNames}
			if err := json.NewEncoder(w).Encode(result); err != nil {
				log.Error(err, "[WithPodCheck] unable to decode request")
				http.Error(w, "unable to decode request", http.StatusBadRequest)
				return
			}
			return
		}

		log.Trace(fmt.Sprintf("[WithPodCheck] pod %s/%s is eligible, matched volumes: %+v", pod.Namespace, pod.Name, volumes))
		next.ServeHTTP(w, r)
	})
}
