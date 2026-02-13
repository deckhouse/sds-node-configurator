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
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/cache"
)

func Test_filterAndScore(t *testing.T) {
	oneGiB := int64(1024 * 1024 * 1024)
	hundredGiB := int64(100 * 1024 * 1024 * 1024)

	t.Run("Wrong HTTP method (GET)", func(t *testing.T) {
		cl := newFakeClient(readyLVG("lvg1", hundredGiB, hundredGiB))
		c := newTestCache()
		s := newTestScheduler(cl, c)

		req := httptest.NewRequest(http.MethodGet, "/v1/lvg/filter-and-score", bytes.NewReader([]byte("{}")))
		w := httptest.NewRecorder()

		s.filterAndScore(w, req)

		assert.Equal(t, http.StatusMethodNotAllowed, w.Code)
		assert.Contains(t, w.Body.String(), "method not allowed")
	})

	t.Run("Invalid JSON body", func(t *testing.T) {
		cl := newFakeClient()
		c := newTestCache()
		s := newTestScheduler(cl, c)

		req := httptest.NewRequest(http.MethodPost, "/v1/lvg/filter-and-score", bytes.NewReader([]byte("not json")))
		w := httptest.NewRecorder()

		s.filterAndScore(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Contains(t, w.Body.String(), "bad request")
	})

	t.Run("Empty reservationID", func(t *testing.T) {
		cl := newFakeClient(readyLVG("lvg1", hundredGiB, hundredGiB))
		c := newTestCache()
		s := newTestScheduler(cl, c)

		body := `{"reservationID":"","reservationTTL":"60s","size":1073741824,"lvgs":[{"name":"lvg1"}]}`
		req := httptest.NewRequest(http.MethodPost, "/v1/lvg/filter-and-score", bytes.NewReader([]byte(body)))
		w := httptest.NewRecorder()

		s.filterAndScore(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Contains(t, w.Body.String(), "reservationID is required")
	})

	t.Run("Empty lvgs list", func(t *testing.T) {
		cl := newFakeClient()
		c := newTestCache()
		s := newTestScheduler(cl, c)

		body := `{"reservationID":"ns/pvc","reservationTTL":"60s","size":1073741824,"lvgs":[]}`
		req := httptest.NewRequest(http.MethodPost, "/v1/lvg/filter-and-score", bytes.NewReader([]byte(body)))
		w := httptest.NewRecorder()

		s.filterAndScore(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Contains(t, w.Body.String(), "lvgs list is empty")
	})

	t.Run("Invalid size (0)", func(t *testing.T) {
		cl := newFakeClient(readyLVG("lvg1", hundredGiB, hundredGiB))
		c := newTestCache()
		s := newTestScheduler(cl, c)

		body := `{"reservationID":"ns/pvc","reservationTTL":"60s","size":0,"lvgs":[{"name":"lvg1"}]}`
		req := httptest.NewRequest(http.MethodPost, "/v1/lvg/filter-and-score", bytes.NewReader([]byte(body)))
		w := httptest.NewRecorder()

		s.filterAndScore(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Contains(t, w.Body.String(), "invalid size")
	})

	t.Run("Invalid reservationTTL", func(t *testing.T) {
		cl := newFakeClient(readyLVG("lvg1", hundredGiB, hundredGiB))
		c := newTestCache()
		s := newTestScheduler(cl, c)

		body := `{"reservationID":"ns/pvc","reservationTTL":"invalid","size":1073741824,"lvgs":[{"name":"lvg1"}]}`
		req := httptest.NewRequest(http.MethodPost, "/v1/lvg/filter-and-score", bytes.NewReader([]byte(body)))
		w := httptest.NewRecorder()

		s.filterAndScore(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Contains(t, w.Body.String(), "invalid reservationTTL")
	})

	t.Run("All LVGs have enough space (thick)", func(t *testing.T) {
		cl := newFakeClient(
			readyLVG("lvg1", hundredGiB, hundredGiB),
			readyLVG("lvg2", hundredGiB, hundredGiB),
		)
		c := newTestCache()
		s := newTestScheduler(cl, c)

		body := `{"reservationID":"ns/pvc","reservationTTL":"60s","size":1073741824,"lvgs":[{"name":"lvg1"},{"name":"lvg2"}]}`
		req := httptest.NewRequest(http.MethodPost, "/v1/lvg/filter-and-score", bytes.NewReader([]byte(body)))
		w := httptest.NewRecorder()

		s.filterAndScore(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, "application/json", w.Header().Get("content-type"))

		var resp FilterAndScoreResponse
		require.NoError(t, decodeJSON(w.Body.Bytes(), &resp))
		require.Len(t, resp.LVGS, 2)
		assert.GreaterOrEqual(t, resp.LVGS[0].Score, resp.LVGS[1].Score)
		assert.Contains(t, []string{"lvg1", "lvg2"}, resp.LVGS[0].Name)
		assert.Contains(t, []string{"lvg1", "lvg2"}, resp.LVGS[1].Name)

		assert.True(t, c.HasReservation("ns/pvc"))
		assert.Equal(t, oneGiB, c.GetReservedSpace(cache.StoragePoolKey{LVGName: "lvg1"}))
		assert.Equal(t, oneGiB, c.GetReservedSpace(cache.StoragePoolKey{LVGName: "lvg2"}))
	})

	t.Run("Some LVGs filtered out (not enough space)", func(t *testing.T) {
		cl := newFakeClient(
			readyLVG("lvg-small", hundredGiB, 2*oneGiB),   // 2Gi free
			readyLVG("lvg-large", hundredGiB, hundredGiB), // 100Gi free
		)
		c := newTestCache()
		s := newTestScheduler(cl, c)

		body := `{"reservationID":"ns/pvc","reservationTTL":"60s","size":5368709120,"lvgs":[{"name":"lvg-small"},{"name":"lvg-large"}]}` // 5Gi
		req := httptest.NewRequest(http.MethodPost, "/v1/lvg/filter-and-score", bytes.NewReader([]byte(body)))
		w := httptest.NewRecorder()

		s.filterAndScore(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var resp FilterAndScoreResponse
		require.NoError(t, decodeJSON(w.Body.Bytes(), &resp))
		require.Len(t, resp.LVGS, 1)
		assert.Equal(t, "lvg-large", resp.LVGS[0].Name)

		key := cache.StoragePoolKey{LVGName: "lvg-large"}
		assert.True(t, c.HasReservation("ns/pvc"))
		assert.Equal(t, int64(5368709120), c.GetReservedSpace(key))
	})

	t.Run("No LVGs have enough space", func(t *testing.T) {
		cl := newFakeClient(
			readyLVG("lvg1", hundredGiB, oneGiB),
			readyLVG("lvg2", hundredGiB, oneGiB),
		)
		c := newTestCache()
		s := newTestScheduler(cl, c)

		body := `{"reservationID":"ns/pvc","reservationTTL":"60s","size":2147483648,"lvgs":[{"name":"lvg1"},{"name":"lvg2"}]}` // 2Gi
		req := httptest.NewRequest(http.MethodPost, "/v1/lvg/filter-and-score", bytes.NewReader([]byte(body)))
		w := httptest.NewRecorder()

		s.filterAndScore(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var resp FilterAndScoreResponse
		require.NoError(t, decodeJSON(w.Body.Bytes(), &resp))
		assert.Empty(t, resp.LVGS)

		assert.False(t, c.HasReservation("ns/pvc"))
	})

	t.Run("NotReady LVG filtered out", func(t *testing.T) {
		cl := newFakeClient(
			notReadyLVG("lvg-not-ready", "NotReady"),
			readyLVG("lvg-ready", hundredGiB, hundredGiB),
		)
		c := newTestCache()
		s := newTestScheduler(cl, c)

		body := `{"reservationID":"ns/pvc","reservationTTL":"60s","size":1073741824,"lvgs":[{"name":"lvg-not-ready"},{"name":"lvg-ready"}]}`
		req := httptest.NewRequest(http.MethodPost, "/v1/lvg/filter-and-score", bytes.NewReader([]byte(body)))
		w := httptest.NewRecorder()

		s.filterAndScore(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var resp FilterAndScoreResponse
		require.NoError(t, decodeJSON(w.Body.Bytes(), &resp))
		require.Len(t, resp.LVGS, 1)
		assert.Equal(t, "lvg-ready", resp.LVGS[0].Name)
	})

	t.Run("Thin pool LVG", func(t *testing.T) {
		tpAvailable := int64(10 * 1024 * 1024 * 1024) // 10Gi
		vgSize := int64(100 * 1024 * 1024 * 1024)
		cl := newFakeClient(readyLVGWithThinPool("lvg-thin", vgSize, "tp0", tpAvailable))
		c := newTestCache()
		s := newTestScheduler(cl, c)

		body := `{"reservationID":"ns/pvc","reservationTTL":"60s","size":1073741824,"lvgs":[{"name":"lvg-thin","thinPoolName":"tp0"}]}`
		req := httptest.NewRequest(http.MethodPost, "/v1/lvg/filter-and-score", bytes.NewReader([]byte(body)))
		w := httptest.NewRecorder()

		s.filterAndScore(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var resp FilterAndScoreResponse
		require.NoError(t, decodeJSON(w.Body.Bytes(), &resp))
		require.Len(t, resp.LVGS, 1)
		assert.Equal(t, "lvg-thin", resp.LVGS[0].Name)
		assert.Equal(t, "tp0", resp.LVGS[0].ThinPoolName)

		key := cache.StoragePoolKey{LVGName: "lvg-thin", ThinPoolName: "tp0"}
		assert.True(t, c.HasReservation("ns/pvc"))
		assert.Equal(t, oneGiB, c.GetReservedSpace(key))
	})

	t.Run("Idempotent reservation replace", func(t *testing.T) {
		cl := newFakeClient(
			readyLVG("lvg1", hundredGiB, hundredGiB),
			readyLVG("lvg2", hundredGiB, hundredGiB),
		)
		c := newTestCache()
		s := newTestScheduler(cl, c)

		body := `{"reservationID":"ns/pvc","reservationTTL":"60s","size":1073741824,"lvgs":[{"name":"lvg1"},{"name":"lvg2"}]}`
		req1 := httptest.NewRequest(http.MethodPost, "/v1/lvg/filter-and-score", bytes.NewReader([]byte(body)))
		w1 := httptest.NewRecorder()
		s.filterAndScore(w1, req1)
		assert.Equal(t, http.StatusOK, w1.Code)

		// Second call with same reservationID - only lvg1 this time
		body2 := `{"reservationID":"ns/pvc","reservationTTL":"60s","size":1073741824,"lvgs":[{"name":"lvg1"}]}`
		req2 := httptest.NewRequest(http.MethodPost, "/v1/lvg/filter-and-score", bytes.NewReader([]byte(body2)))
		w2 := httptest.NewRecorder()
		s.filterAndScore(w2, req2)
		assert.Equal(t, http.StatusOK, w2.Code)

		// Reservation should be replaced: only lvg1 now
		assert.True(t, c.HasReservation("ns/pvc"))
		assert.Equal(t, oneGiB, c.GetReservedSpace(cache.StoragePoolKey{LVGName: "lvg1"}))
		assert.Equal(t, int64(0), c.GetReservedSpace(cache.StoragePoolKey{LVGName: "lvg2"}))
	})
}

func decodeJSON(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}
