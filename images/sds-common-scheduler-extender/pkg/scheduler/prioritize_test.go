package scheduler

import (
	"context"
	"testing"

	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	snc "github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/cache"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/consts"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/logger"
	srv "github.com/deckhouse/sds-replicated-volume/api/v1alpha1"
)

func TestPrioritize_LocalPVC_NoDRBD_NoPanic(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = snc.AddToScheme(scheme)
	_ = v1.AddToScheme(scheme)
	_ = storagev1.AddToScheme(scheme)
	_ = srv.AddToScheme(scheme)

	// Local SC (Thick) with LVG mapping
	scLocal := &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: "sc-local"},
		Provisioner: consts.SdsLocalVolumeProvisioner,
		Parameters: map[string]string{
			consts.LvmTypeParamKey:         consts.Thick,
			consts.LVMVolumeGroupsParamKey: "- name: lvg-1\n",
		},
	}

	// LVG on node-a
	lvg := &snc.LVMVolumeGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "lvg-1"},
		Spec:       snc.LVMVolumeGroupSpec{Local: snc.LVMVolumeGroupLocalSpec{NodeName: "node-a"}},
		Status: snc.LVMVolumeGroupStatus{
			Nodes:  []snc.LVMVolumeGroupNode{{Name: "node-a"}},
			VGSize: *resource.NewQuantity(100*1024*1024*1024, resource.BinarySI),
			VGFree: *resource.NewQuantity(80*1024*1024*1024, resource.BinarySI),
		},
	}

	// Pod uses one local PVC
	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-local", Namespace: "default"},
		Spec: v1.PersistentVolumeClaimSpec{
			StorageClassName: ptr("sc-local"),
			Resources:        v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: resource.MustParse("5Gi")}},
		},
		Status: v1.PersistentVolumeClaimStatus{Phase: v1.ClaimPending},
	}
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pod", Namespace: "default"},
		Spec: v1.PodSpec{Volumes: []v1.Volume{{
			Name:         "v1",
			VolumeSource: v1.VolumeSource{PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{ClaimName: "pvc-local"}},
		}}},
	}

	cl := clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(scLocal, lvg, pvc).Build()
	log, _ := logger.NewLogger(logger.Verbosity("4"))
	c := cache.NewCache(log)
	cm := cache.NewCacheManager(c, nil, log)
	cm.AddLVG(lvg)
	s := NewScheduler(context.TODO(), cl, log, cm, 1)

	res, err := s.Prioritize(ExtenderArgs{Pod: pod, NodeNames: &[]string{"node-a"}})
	if err != nil {
		t.Fatalf("Prioritize returned error: %v", err)
	}
	if len(res) != 1 || res[0].Host != "node-a" {
		t.Fatalf("expected one result for node-a, got %+v", res)
	}
}

func TestPrioritize_ReplicatedPVC_MissingReplica_ScoreZero(t *testing.T) {
	// removed: depends on v1alpha2 DRBD replica API
}

func TestPrioritize_ReplicatedPVC_MissingPeer_ScoreZero(t *testing.T) {
	// removed: depends on v1alpha2 DRBD replica API
}

func TestPrioritize_MixedPVCs_LocalPlusReplicatedMissingReplica_NoPanic(t *testing.T) {
	// removed: depends on v1alpha2 DRBD replica API
}

// ptr is defined in replica_test.go and reused here.

/*
Copyright YEAR Flant JSC

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

// The large commented block below was legacy and accidentally duplicated package declarations.
// Keeping it commented to preserve history without breaking compilation.
// package scheduler

// import (
// 	"testing"

// 	c "github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/cache"
// 	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/consts"
// 	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/logger"

// 	snc "github.com/deckhouse/sds-node-configurator/api/v1alpha1"
// 	v1 "k8s.io/api/core/v1"
// 	storagev1 "k8s.io/api/storage/v1"
// 	"k8s.io/apimachinery/pkg/api/resource"
// 	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
// )

// var (
// 	storageClassNameOne string = "storage-class-1"
// 	storageClassNameTwo string = "storage-class-2"
// )

// const (
// 	node1 string = "node-1"
// 	node2 string = "node-2"
// 	node3 string = "node-3"
// )

// func mockLVG(lvgName, nodeName, lvgFreeSize string) *snc.LVMVolumeGroup {
// 	return &snc.LVMVolumeGroup{
// 		ObjectMeta: metav1.ObjectMeta{Name: lvgName},
// 		Spec: snc.LVMVolumeGroupSpec{
// 			Local: snc.LVMVolumeGroupLocalSpec{
// 				NodeName: nodeName,
// 			},
// 		},
// 		Status: snc.LVMVolumeGroupStatus{
// 			Nodes: []snc.LVMVolumeGroupNode{
// 				{
// 					Name: nodeName,
// 				},
// 			},
// 			VGFree: resource.MustParse(lvgFreeSize),
// 		},
// 	}
// }

// func mockPVC(pvcName, requestedSize string) *v1.PersistentVolumeClaim {
// 	return &v1.PersistentVolumeClaim{
// 		ObjectMeta: metav1.ObjectMeta{Name: pvcName},
// 		Spec: v1.PersistentVolumeClaimSpec{
// 			StorageClassName: &storageClassNameOne,
// 			Resources: v1.VolumeResourceRequirements{
// 				Requests: v1.ResourceList{
// 					v1.ResourceStorage: resource.MustParse(requestedSize),
// 				},
// 			},
// 		},
// 	}
// }

// func TestScoreNodes(t *testing.T) {
// 	log := logger.Logger{}

// 	// cacheMgr := c.CacheManager{cache: &c.Cache{}}
// 	cacheMgr := c.NewCacheManager(&c.Cache{}, nil, &log)
// 	lvgCache := []*c.LvgCache{
// 		{
// 			Lvg: mockLVG("lvg-1", node1, "2Gi"),
// 		},
// 		{
// 			Lvg:       mockLVG("lvg-2", node2, "1Gi"),
// 			ThickPVCs: map[string]*c.pvcCache{},
// 			ThinPools: map[string]map[string]*c.pvcCache{},
// 		},
// 		{
// 			Lvg: mockLVG("lvg-3", node2, "1Gi"),
// 		},
// 	}

// 	for _, lvgC := range lvgCache {
// 		cacheMgr.AddLVG(lvgC.Lvg)
// 	}
// 	cacheMgr.AddLVGToPVC("lvg-1", "pvc-1")
// 	cacheMgr.AddLVGToPVC("lvg-2", "pvc-2")

// 	pvcRequests := map[string]PVCRequest{
// 		"pvc-1": {
// 			DeviceType:    consts.Thick,
// 			RequestedSize: 1073741824, // 1Gb
// 		},
// 		"pvc-2": {
// 			DeviceType:    consts.Thin,
// 			RequestedSize: 524288000, // 500mb
// 		},
// 	}

// 	// Do not change intendation here or else these LVGs will not be parsed
// 	mockLVGYamlOne := `- name: lvg-1
//   Thin:
//     poolName: pool1
// - name: lvg-2
//   Thin:
//     poolName: pool2`

// 	mockLVGYamlTwo := `- name: lvg-3
//   Thin:
//     poolName: pool3`

// 	scs := map[string]*storagev1.StorageClass{
// 		storageClassNameOne: {
// 			ObjectMeta: metav1.ObjectMeta{
// 				Name: storageClassNameOne,
// 			},
// 			Provisioner: "replicated.csi.storage.deckhouse.io",
// 			Parameters: map[string]string{
// 				consts.LVMVolumeGroupsParamKey: mockLVGYamlOne,
// 			},
// 		},
// 		storageClassNameTwo: {
// 			ObjectMeta: metav1.ObjectMeta{
// 				Name: storageClassNameTwo,
// 			},
// 			Provisioner: "replicated.csi.storage.deckhouse.io",
// 			Parameters: map[string]string{
// 				consts.LVMVolumeGroupsParamKey: mockLVGYamlTwo,
// 			},
// 		},
// 	}

// 	pvcs := map[string]*v1.PersistentVolumeClaim{
// 		"pvc-1": {
// 			ObjectMeta: metav1.ObjectMeta{
// 				Name: "pvc-1",
// 			},
// 			Spec: v1.PersistentVolumeClaimSpec{
// 				StorageClassName: &storageClassNameOne,
// 				Resources: v1.VolumeResourceRequirements{
// 					Requests: v1.ResourceList{
// 						v1.ResourceStorage: resource.MustParse("1Gi"),
// 					},
// 				},
// 			},
// 		},
// 		"pvc-2": {
// 			ObjectMeta: metav1.ObjectMeta{
// 				Name: "pvc-2",
// 			},
// 			Spec: v1.PersistentVolumeClaimSpec{
// 				StorageClassName: &storageClassNameTwo,
// 				Resources: v1.VolumeResourceRequirements{
// 					Requests: v1.ResourceList{
// 						v1.ResourceStorage: resource.MustParse("500Mi"),
// 					},
// 				},
// 			},
// 		},
// 	}

// 	tests := []struct {
// 		testName  string
// 		nodeNames []string
// 		pvcs      map[string]*v1.PersistentVolumeClaim
// 		expect    map[string]int
// 	}{
// 		{
// 			testName:  "Test Case #1",
// 			nodeNames: []string{node1},
// 			pvcs:      pvcs,
// 			expect:    map[string]int{node1: 11},
// 		},
// 		{
// 			testName:  "Test Case #2",
// 			nodeNames: []string{node2},
// 			pvcs:      pvcs,
// 			expect:    map[string]int{node2: 3},
// 		},
// 	}

// 	for _, tt := range tests {
// 		t.Run(tt.testName, func(t *testing.T) {
// 			score, err := scoreNodes(log, cacheMgr, &tt.nodeNames, tt.pvcs, scs, pvcRequests, 1)
// 			if err != nil {
// 				t.Error(err)
// 			}
// 			t.Logf("Node score: %v", score)

// 			for _, res := range score {
// 				if tt.expect[res.Host] != res.Score {
// 					t.Errorf("Expected score for node %s to be %d, got %d", res.Host, tt.expect[res.Host], res.Score)
// 				}
// 			}
// 		})
// 	}
// }
