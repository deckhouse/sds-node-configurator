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
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/cache"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/logger"
)

func (s *scheduler) filterAndScore(w http.ResponseWriter, r *http.Request) {
	servingLog := logger.WithTraceIDLogger(r.Context(), s.log).WithName("filter-and-score")

	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req FilterAndScoreRequest
	reader := http.MaxBytesReader(w, r.Body, 10<<20) // 10MB
	err := json.NewDecoder(reader).Decode(&req)
	if err != nil {
		servingLog.Error(err, "unable to decode request")
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	// Validation
	if req.ReservationID == "" {
		http.Error(w, "reservationID is required", http.StatusBadRequest)
		return
	}
	if len(req.LVGS) == 0 {
		http.Error(w, "lvgs list is empty", http.StatusBadRequest)
		return
	}
	if req.Size <= 0 {
		http.Error(w, "invalid size", http.StatusBadRequest)
		return
	}

	ttl, err := time.ParseDuration(req.ReservationTTL)
	if err != nil {
		servingLog.Error(err, "unable to parse reservation TTL")
		http.Error(w, "invalid reservationTTL", http.StatusBadRequest)
		return
	}

	servingLog.Debug(fmt.Sprintf("request: reservationID=%s, size=%d bytes (%.2f Gi), lvgs count=%d, ttl=%s",
		req.ReservationID, req.Size, float64(req.Size)/(1024*1024*1024), len(req.LVGS), ttl))

	// Convert to StoragePoolKeys
	poolKeys := make([]cache.StoragePoolKey, 0, len(req.LVGS))
	for _, lvg := range req.LVGS {
		poolKeys = append(poolKeys, cache.StoragePoolKey{
			LVGName:      lvg.Name,
			ThinPoolName: lvg.ThinPoolName,
		})
	}

	// Filter by available space
	var filteredKeys []cache.StoragePoolKey
	var filteredLVGs []LVMVolumeGroupInput
	for i, key := range poolKeys {
		spaceInfo, err := getAvailableSpace(s.ctx, s.client, s.cache, key)
		if err != nil {
			servingLog.Error(err, fmt.Sprintf("[filterAndScore] unable to get available space for %s", key.String()))
			continue
		}

		if spaceInfo.AvailableSpace >= req.Size {
			servingLog.Debug(fmt.Sprintf("[filterAndScore] %s has enough space (available: %d >= requested: %d)", key.String(), spaceInfo.AvailableSpace, req.Size))
			filteredKeys = append(filteredKeys, key)
			filteredLVGs = append(filteredLVGs, req.LVGS[i])
		} else {
			servingLog.Debug(fmt.Sprintf("[filterAndScore] %s does not have enough space (available: %d < requested: %d)", key.String(), spaceInfo.AvailableSpace, req.Size))
		}
	}

	if len(filteredKeys) == 0 {
		response := FilterAndScoreResponse{LVGS: []ScoredLVMVolumeGroup{}}
		responseJSON, _ := json.Marshal(response)
		servingLog.Debug(fmt.Sprintf("response: %s", string(responseJSON)))
		w.Header().Set("content-type", "application/json")
		_ = json.NewEncoder(w).Encode(response)
		return
	}

	// Score filtered LVGs
	scored := make([]ScoredLVMVolumeGroup, 0, len(filteredKeys))
	for i, key := range filteredKeys {
		score, err := calculatePoolScore(s.ctx, s.client, s.cache, key, req.Size, s.defaultDivisor)
		if err != nil {
			servingLog.Error(err, fmt.Sprintf("[filterAndScore] unable to calculate score for %s", key.String()))
			continue
		}
		scored = append(scored, ScoredLVMVolumeGroup{
			LVMVolumeGroupInput: filteredLVGs[i],
			Score:               score,
		})
	}

	// Sort by score (highest first)
	sort.Slice(scored, func(i, j int) bool {
		return scored[i].Score > scored[j].Score
	})

	// Reserve space for all filtered pools
	s.cache.AddReservation(req.ReservationID, ttl, req.Size, filteredKeys)

	// Build response
	response := FilterAndScoreResponse{LVGS: scored}

	responseJSON, err := json.Marshal(response)
	if err != nil {
		servingLog.Error(err, "unable to marshal response")
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	servingLog.Debug(fmt.Sprintf("response: %s", string(responseJSON)))

	w.Header().Set("content-type", "application/json")
	if _, err = w.Write(responseJSON); err != nil {
		servingLog.Error(err, "unable to write response")
	}
}
