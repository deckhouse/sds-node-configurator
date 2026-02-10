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
	apiv1 "k8s.io/api/core/v1"
)

// === Kubernetes scheduler extender contract (unchanged) ===

// ExtenderArgs is copied from https://godoc.org/k8s.io/kubernetes/pkg/scheduler/api/v1#ExtenderArgs
type ExtenderArgs struct {
	// Pod being scheduled
	Pod *apiv1.Pod `json:"pod"`
	// List of candidate nodes where the pod can be scheduled; to be populated
	// only if ExtenderConfig.NodeCacheCapable == false
	Nodes *apiv1.NodeList `json:"nodes,omitempty"`
	// List of candidate node names where the pod can be scheduled; to be
	// populated only if ExtenderConfig.NodeCacheCapable == true
	NodeNames *[]string `json:"nodenames,omitempty"`
}

// HostPriority is copied from https://godoc.org/k8s.io/kubernetes/pkg/scheduler/api/v1#HostPriority
type HostPriority struct {
	// Name of the host
	Host string `json:"host"`
	// Score associated with the host
	Score int `json:"score"`
}

// HostPriorityList is copied from https://godoc.org/k8s.io/kubernetes/pkg/scheduler/api/v1#HostPriorityList
type HostPriorityList []HostPriority

// ExtenderFilterResult is copied from https://godoc.org/k8s.io/kubernetes/pkg/scheduler/api/v1#ExtenderFilterResult
type ExtenderFilterResult struct {
	// Filtered set of nodes where the pod can be scheduled; to be populated
	// only if ExtenderConfig.NodeCacheCapable == false
	Nodes *apiv1.NodeList `json:"nodes,omitempty"`
	// Filtered set of nodes where the pod can be scheduled; to be populated
	// only if ExtenderConfig.NodeCacheCapable == true
	NodeNames *[]string `json:"nodenames,omitempty"`
	// Filtered out nodes where the pod can't be scheduled and the failure messages
	FailedNodes FailedNodesMap `json:"failedNodes,omitempty"`
	// Error message indicating failure
	Error string `json:"error,omitempty"`
}

// FailedNodesMap is copied from https://godoc.org/k8s.io/kubernetes/pkg/scheduler/api/v1#FailedNodesMap
type FailedNodesMap map[string]string

// === /v1/lvg/filter-and-score API ===

// LVMVolumeGroupInput represents a single LVM Volume Group in a request.
type LVMVolumeGroupInput struct {
	Name         string `json:"name"`
	ThinPoolName string `json:"thinPoolName,omitempty"`
}

// ScoredLVMVolumeGroup is an LVMVolumeGroupInput with an assigned capacity score.
type ScoredLVMVolumeGroup struct {
	LVMVolumeGroupInput
	Score int `json:"score"`
}

// FilterAndScoreRequest is the request for /v1/lvg/filter-and-score.
type FilterAndScoreRequest struct {
	ReservationID  string                `json:"reservationID"`
	ReservationTTL string                `json:"reservationTTL"` // duration string, e.g. "30s"
	Size           int64                 `json:"size"`
	LVGS           []LVMVolumeGroupInput `json:"lvgs"`
}

// FilterAndScoreResponse is the response for /v1/lvg/filter-and-score.
type FilterAndScoreResponse struct {
	LVGS  []ScoredLVMVolumeGroup `json:"lvgs"`
	Error string                 `json:"error,omitempty"`
}

// === /v1/lvg/narrow-reservation API ===

// NarrowReservationRequest is the request for /v1/lvg/narrow-reservation.
type NarrowReservationRequest struct {
	ReservationID  string              `json:"reservationID"`
	ReservationTTL string              `json:"reservationTTL"`
	LVG            LVMVolumeGroupInput `json:"lvg"`
}

// NarrowReservationResponse is the response for /v1/lvg/narrow-reservation.
type NarrowReservationResponse struct {
	Error string `json:"error,omitempty"`
}
