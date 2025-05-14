package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/logger"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Middleware-функция
// type Middleware func(http.Handler) http.Handler

type Middleware struct {
	Handler http.Handler
	Log     *logger.Logger
}

func NewMiddleware(handler http.Handler, log *logger.Logger) *Middleware {
	return &Middleware{
		Handler: handler,
		Log:     log,
	}
}

func (m *Middleware) WithLog() *Middleware {
	return &Middleware{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			m.Handler.ServeHTTP(w, r)

			startTime := time.Now()
			// status := m.handler.Status

			fields := []interface{}{
				"type", "access",
				"response_time", time.Since(startTime).Seconds(),
				"protocol", r.Proto,
				"http_status_code", status,
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
			m.Log.Info("access", fields...)
		}),
	}
}

func (m *Middleware) WithPodCheck(ctx context.Context, cl client.Client) *Middleware {
	return &Middleware{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var inputData ExtenderArgs

			reader := http.MaxBytesReader(w, r.Body, 10<<20)
			if err := json.NewDecoder(reader).Decode(&inputData); err != nil {
				m.Log.Error(err, "[ShouldProcessPodMiddleware] unable to decode filter request")
				httpError(w, "unable to decode request", http.StatusBadRequest)
				return
			}
			pod := inputData.Pod
			if pod == nil {
				m.Log.Error(errors.New("[ShouldProcessPodMiddleware] no pod in request"), "")
				httpError(w, "[ShouldProcessPodMiddleware] no pod in request", http.StatusInternalServerError)
			}

			pvcs := &corev1.PersistentVolumeClaimList{}
			if err := cl.List(ctx, pvcs); err != nil {
				m.Log.Error(err, "[shouldProcessPodMiddleware] error listing PVCs")
				http.Error(w, "error listing PVCs", http.StatusInternalServerError)
			}

			pvcMap := make(map[string]*corev1.PersistentVolumeClaim, len(pvcs.Items))
			for _, pvc := range pvcs.Items {
				pvcMap[pvc.Name] = &pvc
			}

			shouldProcess, volumes, err := shouldProcessPod(ctx, cl, pvcMap, m.Log, pod)
			if err != nil {
				m.Log.Error(err, fmt.Sprintf("[shouldProcessPodMiddleware] error processing pod %s/%s: %v", pod.Namespace, pod.Name))
				http.Error(w, fmt.Sprintf("Error processing pod: %v", err), http.StatusInternalServerError)
				return
			}

			if !shouldProcess {
				m.Log.Trace(fmt.Sprintf("[shouldProcessPodMiddleware] pod %s/%s should not be processed", pod.Namespace, pod.Name))
				result := &ExtenderFilterResult{NodeNames: inputData.NodeNames}
				if err := json.NewEncoder(w).Encode(result); err != nil {
					m.Log.Error(err, "[ShouldProcessPodMiddleware] unable to decode filter request")
					httpError(w, "unable to decode request", http.StatusBadRequest)
					return
				}
				return
			}
			m.Log.Trace(fmt.Sprintf("[shouldProcessPodMiddleware] pod %s/%s is eligible, matched volumes: %+v", pod.Namespace, pod.Name, volumes))
			m.Handler.ServeHTTP(w, r)
		}),
	}
}
