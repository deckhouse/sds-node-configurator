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
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/consts"
)

func requestCount(t *testing.T, verb, pod, result, dryRun string) uint64 {
	t.Helper()
	obs, err := requestDuration.GetMetricWithLabelValues(verb, pod, result, dryRun)
	require.NoError(t, err)
	var m dto.Metric
	require.NoError(t, obs.(prometheus.Metric).Write(&m))
	return m.GetHistogram().GetSampleCount()
}

func serve(t *testing.T, s *scheduler, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	raw, ok := body.([]byte)
	if !ok {
		var err error
		raw, err = json.Marshal(body)
		require.NoError(t, err)
	}
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, bytes.NewReader(raw)))
	return rec
}

func metricsScheduler() *scheduler {
	const scName = "local-sc"
	cl := newFakeClient(
		localSCWithLVGs(scName, "lvg-a"),
		pendingPVCWithSize("pvc-vm", "default", scName, tenGiB),
		readyLVGOnNode("lvg-a", "node-a", hundredGiB, hundredGiB),
	)
	s := newTestScheduler(cl, newTestCache())
	s.targetProvisioners = []string{consts.SdsLocalVolumeProvisioner}
	return s
}

func TestRequestDuration_ExportedOnControllerRuntimeRegistry(t *testing.T) {
	requestDuration.WithLabelValues(verbFilter, podOurs, resultOK, "false").Observe(0)

	families, err := ctrlmetrics.Registry.Gather()
	require.NoError(t, err)
	names := make([]string, 0, len(families))
	for _, f := range families {
		names = append(names, f.GetName())
	}
	assert.Contains(t, names, "sds_common_scheduler_extender_request_duration_seconds")
}

func TestObserveRequest_ForeignPodIsLabelledForeign(t *testing.T) {
	s := metricsScheduler()
	nodes := []string{"node-a"}
	before := requestCount(t, verbFilter, podForeign, resultOK, "false")
	beforePrio := requestCount(t, verbPrioritize, podForeign, resultOK, "false")

	assert.Equal(t, http.StatusOK, serve(t, s, "/scheduler/filter", ExtenderArgs{Pod: podWithPVCs(""), NodeNames: &nodes}).Code)
	assert.Equal(t, http.StatusOK, serve(t, s, "/scheduler/prioritize", ExtenderArgs{Pod: podWithPVCs(""), NodeNames: &nodes}).Code)

	assert.Equal(t, before+1, requestCount(t, verbFilter, podForeign, resultOK, "false"))
	assert.Equal(t, beforePrio+1, requestCount(t, verbPrioritize, podForeign, resultOK, "false"))
}

func TestObserveRequest_OurPodIsLabelledOurs(t *testing.T) {
	s := metricsScheduler()
	nodes := []string{"node-a"}
	before := requestCount(t, verbFilter, podOurs, resultOK, "false")
	beforePrio := requestCount(t, verbPrioritize, podOurs, resultOK, "false")

	assert.Equal(t, http.StatusOK, serve(t, s, "/scheduler/filter", ExtenderArgs{Pod: vmPod(), NodeNames: &nodes}).Code)
	assert.Equal(t, http.StatusOK, serve(t, s, "/scheduler/prioritize", ExtenderArgs{Pod: vmPod(), NodeNames: &nodes}).Code)

	assert.Equal(t, before+1, requestCount(t, verbFilter, podOurs, resultOK, "false"))
	assert.Equal(t, beforePrio+1, requestCount(t, verbPrioritize, podOurs, resultOK, "false"))
}

func TestObserveRequest_DryRunIsSeparated(t *testing.T) {
	s := metricsScheduler()
	nodes := []string{"node-a"}
	before := requestCount(t, verbFilter, podOurs, resultOK, "true")
	realBefore := requestCount(t, verbFilter, podOurs, resultOK, "false")

	serve(t, s, "/scheduler/filter", ExtenderArgs{Pod: vmPod(), NodeNames: &nodes, DryRun: true})

	assert.Equal(t, before+1, requestCount(t, verbFilter, podOurs, resultOK, "true"))
	assert.Equal(t, realBefore, requestCount(t, verbFilter, podOurs, resultOK, "false"))
}

func TestObserveRequest_FailAllNodesIsAnError(t *testing.T) {
	s := metricsScheduler()
	nodes := []string{"node-a"}
	before := requestCount(t, verbFilter, podUnknown, resultError, "false")

	rec := serve(t, s, "/scheduler/filter", ExtenderArgs{Pod: podWithPVCs("", "missing-pvc"), NodeNames: &nodes})

	assert.Equal(t, http.StatusOK, rec.Code, "a failed filter still answers 200 with every node rejected")
	assert.Equal(t, before+1, requestCount(t, verbFilter, podUnknown, resultError, "false"))
}

func TestObserveRequest_HTTPErrorIsAnError(t *testing.T) {
	s := metricsScheduler()
	filterBefore := requestCount(t, verbFilter, podUnknown, resultError, "false")
	prioBefore := requestCount(t, verbPrioritize, podUnknown, resultError, "false")

	assert.Equal(t, http.StatusInternalServerError, serve(t, s, "/scheduler/filter", []byte("not json")).Code)
	assert.Equal(t, http.StatusInternalServerError, serve(t, s, "/scheduler/prioritize", []byte("not json")).Code)

	assert.Equal(t, filterBefore+1, requestCount(t, verbFilter, podUnknown, resultError, "false"))
	assert.Equal(t, prioBefore+1, requestCount(t, verbPrioritize, podUnknown, resultError, "false"))
}

func TestObserveRequest_OtherRoutesAreNotObserved(t *testing.T) {
	s := metricsScheduler()
	families := func() int {
		fs, err := ctrlmetrics.Registry.Gather()
		require.NoError(t, err)
		n := 0
		for _, f := range fs {
			if f.GetName() == "sds_common_scheduler_extender_request_duration_seconds" {
				for _, m := range f.GetMetric() {
					n += int(m.GetHistogram().GetSampleCount())
				}
			}
		}
		return n
	}
	before := families()

	serve(t, s, "/status", []byte("{}"))
	serve(t, s, "/stat", []byte("{}"))

	assert.Equal(t, before, families())
}
