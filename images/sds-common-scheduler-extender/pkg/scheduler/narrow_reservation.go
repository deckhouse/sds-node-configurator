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
	"time"

	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/cache"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/logger"
)

func (s *scheduler) narrowReservation(w http.ResponseWriter, r *http.Request) {
	servingLog := logger.WithTraceIDLogger(r.Context(), s.log).WithName("narrow-reservation")

	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req NarrowReservationRequest
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
	if req.LVG.Name == "" {
		http.Error(w, "lvg name is required", http.StatusBadRequest)
		return
	}

	ttl, err := time.ParseDuration(req.ReservationTTL)
	if err != nil {
		servingLog.Error(err, "unable to parse reservation TTL")
		http.Error(w, "invalid reservationTTL", http.StatusBadRequest)
		return
	}

	keepPool := cache.StoragePoolKey{
		LVGName:      req.LVG.Name,
		ThinPoolName: req.LVG.ThinPoolName,
	}

	servingLog.Debug(fmt.Sprintf("request: reservationID=%s, keepPool=%s, ttl=%s", req.ReservationID, keepPool.String(), ttl))

	ok := s.cache.NarrowReservation(req.ReservationID, []cache.StoragePoolKey{keepPool}, ttl)
	if !ok {
		servingLog.Debug(fmt.Sprintf("reservation %s not found", req.ReservationID))
	}

	// Build response
	response := NarrowReservationResponse{}
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
