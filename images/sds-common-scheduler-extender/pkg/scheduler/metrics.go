/*
Copyright 2026 Flant JSC

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
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/tracing"
)

const (
	verbFilter     = "filter"
	verbPrioritize = "prioritize"

	podOurs    = "ours"
	podForeign = "foreign"
	podUnknown = "unknown"

	resultOK    = "ok"
	resultError = "error"
)

var requestDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name: "sds_common_scheduler_extender_request_duration_seconds",
		Help: "Time the extender spends on a filter or prioritize call from kube-scheduler, from the first byte of the request to the end of the handler. pod tells whether the Pod mounts a PVC of a target provisioner (ours), mounts none (foreign), or the handler failed before it could tell (unknown).",
		Buckets: []float64{
			0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 4, 5,
		},
	},
	[]string{"verb", "pod", "result", "dry_run"},
)

func init() {
	ctrlmetrics.Registry.MustRegister(requestDuration)
}

type requestRecorder struct {
	http.ResponseWriter
	status int
	pod    string
	failed bool
}

func (r *requestRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *requestRecorder) Unwrap() http.ResponseWriter {
	return r.ResponseWriter
}

func markPod(w http.ResponseWriter, managedPVCs int) {
	rec, ok := w.(*requestRecorder)
	if !ok {
		return
	}
	if managedPVCs > 0 {
		rec.pod = podOurs
		return
	}
	rec.pod = podForeign
}

func markFailed(w http.ResponseWriter) {
	if rec, ok := w.(*requestRecorder); ok {
		rec.failed = true
	}
}

func observeRequest(verb string, w http.ResponseWriter, r *http.Request, handle func(http.ResponseWriter, *http.Request)) {
	rec := &requestRecorder{ResponseWriter: w, status: http.StatusOK, pod: podUnknown}
	start := time.Now()

	handle(rec, r)

	result := resultOK
	if rec.failed || rec.status >= http.StatusBadRequest {
		result = resultError
	}
	requestDuration.
		WithLabelValues(verb, rec.pod, result, strconv.FormatBool(tracing.IsDryRun(rec.Header()))).
		Observe(time.Since(start).Seconds())
}
