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

func (m *Middleware) WithBodyUnmarshal() *Middleware {
	m.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var inputData ExtenderArgs
		reader := http.MaxBytesReader(w, r.Body, 10<<20)
		if err := json.NewDecoder(reader).Decode(&inputData); err != nil {
			m.Log.Error(err, "[handler] unable to decode filter request")
			httpError(w, "unable to decode request", http.StatusBadRequest)
			return
		}

		m.Log.Trace(fmt.Sprintf("[handler] filter input data: %+v", inputData))
		if inputData.Pod == nil {
			m.Log.Error(errors.New("no pod in request"), "[handler] no pod provided for filtering")
			httpError(w, "no pod in request", http.StatusBadRequest)
			return
		}

		cwv := context.WithValue(r.Context(), "pod", inputData)
		req := r.WithContext(cwv)
		m.Handler.ServeHTTP(w, req)
	})
	return m
}

func (m *Middleware) WithLog() *Middleware {
	m.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	})
	return m
}

func (m *Middleware) WithPodCheck(ctx context.Context, cl client.Client) *Middleware {
	m.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inputData, ok := r.Context().Value("pod").(ExtenderArgs)
		if !ok {
			m.Log.Error(errors.New("pod data not found in context"), "[Filter] missing pod data")
			httpError(w, "internal error", http.StatusInternalServerError)
			return
		}
		pod := inputData.Pod

		pvcs := &corev1.PersistentVolumeClaimList{}
		if err := cl.List(ctx, pvcs); err != nil {
			m.Log.Error(err, "[shouldProcessPodMiddleware] error listing PVCs")
			http.Error(w, "error listing PVCs", http.StatusInternalServerError)
		}

		pvcMap := make(map[string]*corev1.PersistentVolumeClaim, len(pvcs.Items))
		for _, pvc := range pvcs.Items {
			pvcMap[pvc.Name] = &pvc
		}

		volumes, err := shouldProcessPod(ctx, cl, pvcMap, m.Log, pod)
		if err != nil {
			m.Log.Error(err, fmt.Sprintf("[shouldProcessPodMiddleware] error processing pod %s/%s: %v", pod.Namespace, pod.Name))
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
	})
	return m
}
