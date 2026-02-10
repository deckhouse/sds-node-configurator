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

	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/cache"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/logger"
)

func (s *scheduler) bindVolume(w http.ResponseWriter, r *http.Request) {
	servingLog := logger.WithTraceIDLogger(r.Context(), s.log).WithName("bind-volume")

	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req BindVolumeRequest
	reader := http.MaxBytesReader(w, r.Body, 10<<20) // 10MB
	err := json.NewDecoder(reader).Decode(&req)
	if err != nil {
		servingLog.Error(err, "unable to decode request")
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	// Validation
	if req.Volume.Name == "" {
		http.Error(w, "volume name is required", http.StatusBadRequest)
		return
	}

	// Convert to cache types
	keepLVGs := make([]cache.LVGRef, len(req.SelectedLVGs))
	for i, lvg := range req.SelectedLVGs {
		keepLVGs[i] = cache.LVGRef{
			Name:         lvg.Name,
			ThinPoolName: lvg.ThinPoolName,
		}
	}

	servingLog.Debug(fmt.Sprintf("request: volume=%s, selectedLVGs count=%d", req.Volume.Name, len(req.SelectedLVGs)))
	for i, lvg := range req.SelectedLVGs {
		servingLog.Debug(fmt.Sprintf("request: selectedLVG[%d]=%s, thinPoolName=%s", i, lvg.Name, lvg.ThinPoolName))
	}

	// Remove reservations for unselected LVGs (type inferred from keep; when empty, removes from both)
	s.cache.RemoveVolumeReservationsExcept(req.Volume.Name, keepLVGs)

	// Build response
	response := BindVolumeResponse{}
	responseJSON, err := json.Marshal(response)
	if err != nil {
		servingLog.Error(err, "unable to marshal response")
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	servingLog.Debug(fmt.Sprintf("response: %s", string(responseJSON)))

	w.Header().Set("content-type", "application/json")
	_, err = w.Write(responseJSON)
	if err != nil {
		servingLog.Error(err, "unable to write response")
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}
}
