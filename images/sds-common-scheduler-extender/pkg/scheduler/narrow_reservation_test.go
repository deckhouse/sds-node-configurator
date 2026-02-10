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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/cache"
)

func Test_narrowReservation(t *testing.T) {
	oneGiB := int64(1024 * 1024 * 1024)
	pool1 := cache.StoragePoolKey{LVGName: "lvg1"}
	pool2 := cache.StoragePoolKey{LVGName: "lvg2"}

	t.Run("Wrong HTTP method (GET)", func(t *testing.T) {
		cl := newFakeClient()
		c := newTestCache()
		s := newTestScheduler(cl, c)

		req := httptest.NewRequest(http.MethodGet, "/v1/lvg/narrow-reservation", bytes.NewReader([]byte("{}")))
		w := httptest.NewRecorder()

		s.narrowReservation(w, req)

		assert.Equal(t, http.StatusMethodNotAllowed, w.Code)
		assert.Contains(t, w.Body.String(), "method not allowed")
	})

	t.Run("Invalid JSON body", func(t *testing.T) {
		cl := newFakeClient()
		c := newTestCache()
		s := newTestScheduler(cl, c)

		req := httptest.NewRequest(http.MethodPost, "/v1/lvg/narrow-reservation", bytes.NewReader([]byte("not json")))
		w := httptest.NewRecorder()

		s.narrowReservation(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Contains(t, w.Body.String(), "bad request")
	})

	t.Run("Empty reservationID", func(t *testing.T) {
		cl := newFakeClient()
		c := newTestCache()
		s := newTestScheduler(cl, c)

		body := `{"reservationID":"","reservationTTL":"60s","lvg":{"name":"lvg1"}}`
		req := httptest.NewRequest(http.MethodPost, "/v1/lvg/narrow-reservation", bytes.NewReader([]byte(body)))
		w := httptest.NewRecorder()

		s.narrowReservation(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Contains(t, w.Body.String(), "reservationID is required")
	})

	t.Run("Empty lvg name", func(t *testing.T) {
		cl := newFakeClient()
		c := newTestCache()
		s := newTestScheduler(cl, c)

		body := `{"reservationID":"ns/pvc","reservationTTL":"60s","lvg":{"name":""}}`
		req := httptest.NewRequest(http.MethodPost, "/v1/lvg/narrow-reservation", bytes.NewReader([]byte(body)))
		w := httptest.NewRecorder()

		s.narrowReservation(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Contains(t, w.Body.String(), "lvg name is required")
	})

	t.Run("Invalid reservationTTL", func(t *testing.T) {
		cl := newFakeClient()
		c := newTestCache()
		s := newTestScheduler(cl, c)

		body := `{"reservationID":"ns/pvc","reservationTTL":"invalid","lvg":{"name":"lvg1"}}`
		req := httptest.NewRequest(http.MethodPost, "/v1/lvg/narrow-reservation", bytes.NewReader([]byte(body)))
		w := httptest.NewRecorder()

		s.narrowReservation(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Contains(t, w.Body.String(), "invalid reservationTTL")
	})

	t.Run("Narrow existing reservation to one pool", func(t *testing.T) {
		cl := newFakeClient()
		c := newTestCache()
		s := newTestScheduler(cl, c)

		c.AddReservation("ns/pvc", 60*time.Second, oneGiB, []cache.StoragePoolKey{pool1, pool2})
		assert.True(t, c.HasReservation("ns/pvc"))
		assert.Equal(t, oneGiB, c.GetReservedSpace(pool1))
		assert.Equal(t, oneGiB, c.GetReservedSpace(pool2))

		body := `{"reservationID":"ns/pvc","reservationTTL":"60s","lvg":{"name":"lvg1"}}`
		req := httptest.NewRequest(http.MethodPost, "/v1/lvg/narrow-reservation", bytes.NewReader([]byte(body)))
		w := httptest.NewRecorder()

		s.narrowReservation(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, "application/json", w.Header().Get("content-type"))

		var resp NarrowReservationResponse
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))

		assert.True(t, c.HasReservation("ns/pvc"))
		assert.Equal(t, oneGiB, c.GetReservedSpace(pool1))
		assert.Equal(t, int64(0), c.GetReservedSpace(pool2))
	})

	t.Run("Narrow non-existent reservation", func(t *testing.T) {
		cl := newFakeClient()
		c := newTestCache()
		s := newTestScheduler(cl, c)

		body := `{"reservationID":"nonexistent","reservationTTL":"60s","lvg":{"name":"lvg1"}}`
		req := httptest.NewRequest(http.MethodPost, "/v1/lvg/narrow-reservation", bytes.NewReader([]byte(body)))
		w := httptest.NewRecorder()

		s.narrowReservation(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var resp NarrowReservationResponse
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
		assert.False(t, c.HasReservation("nonexistent"))
	})

	t.Run("Narrow to pool not in reservation", func(t *testing.T) {
		cl := newFakeClient()
		c := newTestCache()
		s := newTestScheduler(cl, c)

		c.AddReservation("ns/pvc", 60*time.Second, oneGiB, []cache.StoragePoolKey{pool1})
		assert.True(t, c.HasReservation("ns/pvc"))

		// Narrow to lvg2 which is not in the reservation - reservation gets removed
		body := `{"reservationID":"ns/pvc","reservationTTL":"60s","lvg":{"name":"lvg2"}}`
		req := httptest.NewRequest(http.MethodPost, "/v1/lvg/narrow-reservation", bytes.NewReader([]byte(body)))
		w := httptest.NewRecorder()

		s.narrowReservation(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.False(t, c.HasReservation("ns/pvc"))
		assert.Equal(t, int64(0), c.GetReservedSpace(pool1))
		assert.Equal(t, int64(0), c.GetReservedSpace(pool2))
	})
}
