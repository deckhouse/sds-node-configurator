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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	snc "github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/cache"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/logger"
)

func TestBindVolume(t *testing.T) {
	t.Run("valid thick bind removes unselected reservations", func(t *testing.T) {
		log := logger.Logger{}
		ch := cache.NewCache(log, cache.DefaultPVCExpiredDurationSec)

		// Seed LVGs
		ch.AddLVG(&snc.LVMVolumeGroup{ObjectMeta: metav1.ObjectMeta{Name: "lvg-a"}})
		ch.AddLVG(&snc.LVMVolumeGroup{ObjectMeta: metav1.ObjectMeta{Name: "lvg-b"}})

		// Add thick volume reservations
		require.NoError(t, ch.AddThickVolume("lvg-a", "vol-1", 1024))
		require.NoError(t, ch.AddThickVolume("lvg-b", "vol-1", 1024))

		s := &scheduler{log: log, cache: ch}

		reqBody := BindVolumeRequest{
			Volume:       VolumeInput{Name: "vol-1", Type: "thick"},
			SelectedLVGs: []LVGInput{{Name: "lvg-a"}},
		}
		body, _ := json.Marshal(reqBody)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/volumes/bind", bytes.NewReader(body))
		w := httptest.NewRecorder()

		s.bindVolume(w, req)

		assert.Equal(t, http.StatusOK, w.Code)

		var resp BindVolumeResponse
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
		assert.Empty(t, resp.Error)

		// lvg-a should still have the reservation (checked via reserved space)
		reserved, err := ch.GetLVGThickReservedSpace("lvg-a")
		require.NoError(t, err)
		assert.Equal(t, int64(1024), reserved)

		// lvg-b reservation should be removed
		reserved, err = ch.GetLVGThickReservedSpace("lvg-b")
		require.NoError(t, err)
		assert.Equal(t, int64(0), reserved)
	})

	t.Run("valid thin bind removes unselected reservations", func(t *testing.T) {
		log := logger.Logger{}
		ch := cache.NewCache(log, cache.DefaultPVCExpiredDurationSec)

		// Seed LVG with thin pools
		ch.AddLVG(&snc.LVMVolumeGroup{
			ObjectMeta: metav1.ObjectMeta{Name: "lvg-a"},
			Status: snc.LVMVolumeGroupStatus{
				ThinPools: []snc.LVMVolumeGroupThinPoolStatus{
					{Name: "tp-1", ActualSize: resource.MustParse("10Gi")},
					{Name: "tp-2", ActualSize: resource.MustParse("10Gi")},
				},
			},
		})

		// Reserve in both thin pools
		require.NoError(t, ch.AddThinVolume("lvg-a", "tp-1", "vol-1", 1024))
		require.NoError(t, ch.AddThinVolume("lvg-a", "tp-2", "vol-1", 1024))

		s := &scheduler{log: log, cache: ch}

		reqBody := BindVolumeRequest{
			Volume:       VolumeInput{Name: "vol-1", Type: "thin"},
			SelectedLVGs: []LVGInput{{Name: "lvg-a", ThinPoolName: "tp-1"}},
		}
		body, _ := json.Marshal(reqBody)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/volumes/bind", bytes.NewReader(body))
		w := httptest.NewRecorder()

		s.bindVolume(w, req)

		assert.Equal(t, http.StatusOK, w.Code)

		// tp-1 should keep the reservation
		reserved, err := ch.GetLVGThinReservedSpace("lvg-a", "tp-1")
		require.NoError(t, err)
		assert.Equal(t, int64(1024), reserved)

		// tp-2 reservation should be removed
		reserved, err = ch.GetLVGThinReservedSpace("lvg-a", "tp-2")
		require.NoError(t, err)
		assert.Equal(t, int64(0), reserved)
	})

	t.Run("missing volume name returns 400", func(t *testing.T) {
		log := logger.Logger{}
		ch := cache.NewCache(log, cache.DefaultPVCExpiredDurationSec)
		s := &scheduler{log: log, cache: ch}

		reqBody := BindVolumeRequest{
			Volume:       VolumeInput{Name: "", Type: "thick"},
			SelectedLVGs: []LVGInput{{Name: "lvg-a"}},
		}
		body, _ := json.Marshal(reqBody)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/volumes/bind", bytes.NewReader(body))
		w := httptest.NewRecorder()

		s.bindVolume(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("invalid volume type returns 400", func(t *testing.T) {
		log := logger.Logger{}
		ch := cache.NewCache(log, cache.DefaultPVCExpiredDurationSec)
		s := &scheduler{log: log, cache: ch}

		reqBody := BindVolumeRequest{
			Volume:       VolumeInput{Name: "vol-1", Type: "invalid"},
			SelectedLVGs: []LVGInput{{Name: "lvg-a"}},
		}
		body, _ := json.Marshal(reqBody)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/volumes/bind", bytes.NewReader(body))
		w := httptest.NewRecorder()

		s.bindVolume(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("empty selectedLVGs removes all reservations", func(t *testing.T) {
		log := logger.Logger{}
		ch := cache.NewCache(log, cache.DefaultPVCExpiredDurationSec)

		ch.AddLVG(&snc.LVMVolumeGroup{ObjectMeta: metav1.ObjectMeta{Name: "lvg-a"}})
		require.NoError(t, ch.AddThickVolume("lvg-a", "vol-1", 1024))

		s := &scheduler{log: log, cache: ch}

		reqBody := BindVolumeRequest{
			Volume:       VolumeInput{Name: "vol-1", Type: "thick"},
			SelectedLVGs: []LVGInput{},
		}
		body, _ := json.Marshal(reqBody)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/volumes/bind", bytes.NewReader(body))
		w := httptest.NewRecorder()

		s.bindVolume(w, req)

		assert.Equal(t, http.StatusOK, w.Code)

		reserved, err := ch.GetLVGThickReservedSpace("lvg-a")
		require.NoError(t, err)
		assert.Equal(t, int64(0), reserved)
	})

	t.Run("method not allowed for GET", func(t *testing.T) {
		log := logger.Logger{}
		ch := cache.NewCache(log, cache.DefaultPVCExpiredDurationSec)
		s := &scheduler{log: log, cache: ch}

		req := httptest.NewRequest(http.MethodGet, "/api/v1/volumes/bind", nil)
		w := httptest.NewRecorder()

		s.bindVolume(w, req)

		assert.Equal(t, http.StatusMethodNotAllowed, w.Code)
	})
}
