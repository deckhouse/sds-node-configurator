/*
Copyright 2024 Flant JSC

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
	slv "github.com/deckhouse/sds-local-volume/api/v1alpha1"
	snc "github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	srv "github.com/deckhouse/sds-replicated-volume/api/v1alpha1"
	srv2 "github.com/deckhouse/sds-replicated-volume/api/v1alpha2"
	apiv1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
)

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

// ExtenderFilterResult is copied from https://godoc.org/k8s.io/kubernetes/pkg/scheduler/api/v1#ExtenderFilterResult
type ExtenderFilterResult struct {
	// Filtered set of nodes where the pod can be scheduled; to be populated
	// only if ExtenderConfig.NodeCacheCapable == false
	Nodes *apiv1.NodeList `json:"nodes,omitempty"`
	// Filtered set of nodes where the pod can be scheduled; to be populated
	// only if ExtenderConfig.NodeCacheCapable == true
	NodeNames *[]string `json:"nodenames,omitempty"`
	// Filtered out nodes where the pod can't be scheduled and the failure messages
	FailedNodes map[string]string `json:"failedNodes,omitempty"`
	// Error message indicating failure
	Error string `json:"error,omitempty"`
}

// HostPriority is copied from https://godoc.org/k8s.io/kubernetes/pkg/scheduler/api/v1#HostPriority
type HostPriority struct {
	// Name of the host
	Host string `json:"host"`
	// Score associated with the host
	Score int `json:"score"`
}

// FilterInput holds input data for filtering nodes
type FilterInput struct {
	Pod                        *v1.Pod
	NodeNames                  []string
	ReplicatedProvisionPVCs    map[string]*v1.PersistentVolumeClaim
	LocalProvisionPVCs         map[string]*v1.PersistentVolumeClaim
	SCSUsedByPodPVCs           map[string]*storagev1.StorageClass
	PVCSizeRequests            map[string]PVCRequest
	ReplicatedSCSUsedByPodPVCs map[string]*srv.ReplicatedStorageClass
	LocalSCSUsedByPodPVCs      map[string]*slv.LocalStorageClass
	// DRBDResourceMap            map[string]*srv.DRBDResource
	DRBDNodesMap           map[string]struct{}
	DRBDResourceReplicaMap map[string]*srv2.DRBDResourceReplica
}

// LVGInfo holds LVMVolumeGroup-related data
type LVGInfo struct {
	ThickFreeSpaces map[string]int64
	ThinFreeSpaces  map[string]map[string]int64
	NodeToLVGs      map[string][]*snc.LVMVolumeGroup
	SCLVGs          map[string][]LVMVolumeGroup
}

// ResultWithError holds the result of filtering a single node
type ResultWithError struct {
	NodeName string
	Err      error
}

// PrioritizeInput holds input data for prioritizing nodes
type PrioritizeInput struct {
	Pod                     *v1.Pod
	NodeNames               []string
	ReplicatedProvisionPVCs map[string]*v1.PersistentVolumeClaim
	LocalProvisionPVCs      map[string]*v1.PersistentVolumeClaim
	StorageClasses          map[string]*storagev1.StorageClass
	PVCRequests             map[string]PVCRequest
	StoragePoolMap          map[string]*srv.ReplicatedStoragePool
	DefaultDivisor          float64
	DRBDResourceReplicaMap  map[string]*srv2.DRBDResourceReplica
}

// LVGScoreInfo holds LVMVolumeGroup-related data for scoring
type LVGScoreInfo struct {
	NodeToLVGs map[string][]*snc.LVMVolumeGroup
	SCLVGs     map[string][]LVMVolumeGroup
	LVGs       map[string]*snc.LVMVolumeGroup
}

type LVMVolumeGroup struct {
	Name string `yaml:"name"`
	Thin struct {
		PoolName string `yaml:"poolName"`
	} `yaml:"Thin"`
}
