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

	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/consts"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/logger"
)

func (s *scheduler) filterAndPrioritize(w http.ResponseWriter, r *http.Request) {
	servingLog := s.log.WithName("filter-and-prioritize")

	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req FilterPrioritizeRequest
	reader := http.MaxBytesReader(w, r.Body, 10<<20) // 10MB
	err := json.NewDecoder(reader).Decode(&req)
	if err != nil {
		servingLog.Error(err, "unable to decode request")
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	// Validation
	if len(req.LVGs) == 0 {
		http.Error(w, "lvgs list is empty", http.StatusBadRequest)
		return
	}
	if req.Volume.Name == "" || req.Volume.Size <= 0 {
		http.Error(w, "invalid volume data", http.StatusBadRequest)
		return
	}
	if req.Volume.Type != consts.Thick && req.Volume.Type != consts.Thin {
		http.Error(w, "invalid volume type", http.StatusBadRequest)
		return
	}

	// Validate thinPoolName for thin volumes
	if req.Volume.Type == consts.Thin {
		for _, lvg := range req.LVGs {
			if lvg.ThinPoolName == "" {
				http.Error(w, "thinPoolName is required for thin volumes", http.StatusBadRequest)
				return
			}
		}
	}

	// Filter LVGs by available space
	// Uses common function checkLVGHasSpace
	filteredLVGs, err := s.filterLVGs(servingLog, req.LVGs, req.Volume)
	if err != nil {
		servingLog.Error(err, "unable to filter LVGs")
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	if len(filteredLVGs) == 0 {
		response := FilterPrioritizeResponse{
			LVGs: []LVGScore{},
		}
		responseJSON, _ := json.Marshal(response)
		servingLog.Debug(fmt.Sprintf("response: %s", string(responseJSON)))
		w.Header().Set("content-type", "application/json")
		json.NewEncoder(w).Encode(response)
		return
	}

	// Score filtered LVGs
	// Uses common function calculateLVGScore
	scoredLVGs := s.scoreLVGs(servingLog, filteredLVGs, req.Volume)

	// Reserve space for all filtered LVGs
	err = s.reserveSpaceForVolumes(servingLog, filteredLVGs, req.Volume)
	if err != nil {
		servingLog.Error(err, "unable to reserve space")
		// Don't return error, as filtering and scoring are already done
		// Reservation can be retried later
	}

	// Build response
	response := FilterPrioritizeResponse{
		LVGs: scoredLVGs,
	}

	// Log response body at DEBUG level
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

// filterLVGs filters LVGs by available space
// Uses common function checkLVGHasSpace
func (s *scheduler) filterLVGs(log logger.Logger, lvgs []LVGInput, volume VolumeInput) ([]LVGInput, error) {
	var filtered []LVGInput

	for _, lvgInput := range lvgs {
		lvg := s.cache.TryGetLVG(lvgInput.Name)
		if lvg == nil {
			log.Debug(fmt.Sprintf("[filterLVGs] LVG %s not found in cache, skipping", lvgInput.Name))
			continue
		}

		// Use common function to check available space
		hasSpace, err := checkLVGHasSpace(s.cache, lvg, volume.Type, lvgInput.ThinPoolName, volume.Size)
		if err != nil {
			log.Error(err, fmt.Sprintf("[filterLVGs] unable to check space for LVG %s", lvgInput.Name))
			continue
		}

		if hasSpace {
			filtered = append(filtered, lvgInput)
		}
	}

	return filtered, nil
}

// scoreLVGs scores LVGs
// Uses common function calculateLVGScore
func (s *scheduler) scoreLVGs(log logger.Logger, lvgs []LVGInput, volume VolumeInput) []LVGScore {
	var scored []LVGScore

	for _, lvgInput := range lvgs {
		lvg := s.cache.TryGetLVG(lvgInput.Name)
		if lvg == nil {
			log.Debug(fmt.Sprintf("[scoreLVGs] LVG %s not found in cache, skipping", lvgInput.Name))
			continue
		}

		// Use common function to calculate score
		score, err := calculateLVGScore(s.cache, lvg, volume.Type, lvgInput.ThinPoolName, volume.Size, s.defaultDivisor)
		if err != nil {
			log.Error(err, fmt.Sprintf("[scoreLVGs] unable to calculate score for LVG %s", lvgInput.Name))
			continue
		}

		lvgScore := LVGScore{
			Name:  lvgInput.Name,
			Score: score,
		}

		scored = append(scored, lvgScore)
	}

	// Sort by score (from highest to lowest)
	sort.Slice(scored, func(i, j int) bool {
		return scored[i].Score > scored[j].Score
	})

	return scored
}

func (s *scheduler) reserveSpaceForVolumes(log logger.Logger, lvgs []LVGInput, volume VolumeInput) error {
	for _, lvgInput := range lvgs {
		switch volume.Type {
		case consts.Thick:
			err := s.cache.AddThickVolume(lvgInput.Name, volume.Name, volume.Size)
			if err != nil {
				log.Error(err, fmt.Sprintf("[reserveSpaceForVolumes] unable to reserve space for volume %s in LVG %s", volume.Name, lvgInput.Name))
				return err
			}
			log.Debug(fmt.Sprintf("[reserveSpaceForVolumes] reserved %d bytes for volume %s in LVG %s", volume.Size, volume.Name, lvgInput.Name))

		case consts.Thin:
			if lvgInput.ThinPoolName == "" {
				continue
			}
			err := s.cache.AddThinVolume(lvgInput.Name, lvgInput.ThinPoolName, volume.Name, volume.Size)
			if err != nil {
				log.Error(err, fmt.Sprintf("[reserveSpaceForVolumes] unable to reserve space for volume %s in LVG %s Thin Pool %s", volume.Name, lvgInput.Name, lvgInput.ThinPoolName))
				return err
			}
			log.Debug(fmt.Sprintf("[reserveSpaceForVolumes] reserved %d bytes for volume %s in LVG %s Thin Pool %s", volume.Size, volume.Name, lvgInput.Name, lvgInput.ThinPoolName))
		}
	}

	return nil
}
