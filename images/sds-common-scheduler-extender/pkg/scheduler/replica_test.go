package scheduler

import (
	"context"
	"testing"

	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"

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

func TestReplicaEvaluate_ThinReserveAndCleanup(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = snc.AddToScheme(scheme)
	_ = v1.AddToScheme(scheme)
	_ = storagev1.AddToScheme(scheme)
	_ = srv.AddToScheme(scheme)

	// Prepare SC with Thin LVG param
	sc := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{Name: "replicated-sc"},
		Parameters: map[string]string{
			consts.LvmTypeParamKey:         consts.Thin,
			consts.LVMVolumeGroupsParamKey: "- name: lvg-1\n  Thin:\n    poolName: tp1\n",
		},
	}

	// LVG with thin pool and node mapping
	lvg := &snc.LVMVolumeGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "lvg-1"},
		Spec:       snc.LVMVolumeGroupSpec{Local: snc.LVMVolumeGroupLocalSpec{NodeName: "node-a"}},
		Status: snc.LVMVolumeGroupStatus{
			Nodes:  []snc.LVMVolumeGroupNode{{Name: "node-a"}},
			VGSize: *resource.NewQuantity(100*1024*1024*1024, resource.BinarySI),
			VGFree: *resource.NewQuantity(80*1024*1024*1024, resource.BinarySI),
			ThinPools: []snc.LVMVolumeGroupThinPoolStatus{{
				Name:           "tp1",
				ActualSize:     *resource.NewQuantity(100*1024*1024*1024, resource.BinarySI),
				AvailableSpace: *resource.NewQuantity(80*1024*1024*1024, resource.BinarySI),
				Ready:          true,
			}},
		},
	}

	// Replicated storage pool that references our LVG to mark node as DRBD-capable
	rsp := &srv.ReplicatedStoragePool{
		ObjectMeta: metav1.ObjectMeta{Name: "rsp-1"},
		Spec: srv.ReplicatedStoragePoolSpec{
			LVMVolumeGroups: []srv.ReplicatedStoragePoolLVMVolumeGroups{{Name: "lvg-1"}},
		},
	}

	cl := clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(sc, lvg, rsp).Build()

	log, _ := logger.NewLogger(logger.Verbosity("4"))
	c := cache.NewCache(log)
	cm := cache.NewCacheManager(c, nil, log)
	cm.AddLVG(lvg)

	s := NewScheduler(context.TODO(), cl, log, cm, 1)

	// evaluate and reserve by explicit thin candidates
	res, err := s.ReplicaEvaluate(ReplicaEvaluateArgs{
		CandidatesThin: []ThinCandidateItem{{LVGName: "lvg-1", ThinPool: "tp1"}},
		RequestedBytes: 10 * 1024 * 1024 * 1024,
		ReplicaKey:     "ns/rr-1",
		Reserve:        true,
	})
	if err != nil {
		t.Fatalf("ReplicaEvaluate returned error: %v", err)
	}
	if len(res.NodeNames) == 0 {
		t.Fatalf("expected some nodes passed, got 0")
	}

	// ensure reservation accounted in thin free space
	reserved, err := cm.GetLVGThinReservedSpace("lvg-1", "tp1")
	if err != nil {
		t.Fatalf("GetLVGThinReservedSpace returned error: %v", err)
	}
	if reserved <= 0 {
		t.Fatalf("expected reserved space > 0, got %d", reserved)
	}

	// cleanup non-selected (no-op because only one candidate)
	if err := s.ReplicaCleanup(ReplicaCleanupArgs{ReplicaKey: "ns/rr-1", SelectedThinLVG: "lvg-1", SelectedThinPool: "tp1", ClearNonSelected: true}); err != nil {
		t.Fatalf("ReplicaCleanup returned error: %v", err)
	}

	// cleanup selected reservation
	if err := s.ReplicaCleanup(ReplicaCleanupArgs{ReplicaKey: "ns/rr-1", SelectedThinLVG: "lvg-1", SelectedThinPool: "tp1", ClearSelected: true}); err != nil {
		t.Fatalf("ReplicaCleanup (selected) returned error: %v", err)
	}
}

func TestReplicaEvaluate_ThickReserve_ScoringAndCleanup(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = snc.AddToScheme(scheme)
	_ = v1.AddToScheme(scheme)
	_ = storagev1.AddToScheme(scheme)
	_ = srv.AddToScheme(scheme)

	// SC with two LVGs for thick
	sc := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{Name: "replicated-thick"},
		Parameters: map[string]string{
			consts.LvmTypeParamKey:         consts.Thick,
			consts.LVMVolumeGroupsParamKey: "- name: lvg-a\n- name: lvg-b\n",
		},
	}

	// LVG A on node-a with more free
	lvgA := &snc.LVMVolumeGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "lvg-a"},
		Spec:       snc.LVMVolumeGroupSpec{Local: snc.LVMVolumeGroupLocalSpec{NodeName: "node-a"}},
		Status: snc.LVMVolumeGroupStatus{
			Nodes:  []snc.LVMVolumeGroupNode{{Name: "node-a"}},
			VGSize: *resource.NewQuantity(100*1024*1024*1024, resource.BinarySI),
			VGFree: *resource.NewQuantity(80*1024*1024*1024, resource.BinarySI),
		},
	}
	// LVG B on node-b with less free
	lvgB := &snc.LVMVolumeGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "lvg-b"},
		Spec:       snc.LVMVolumeGroupSpec{Local: snc.LVMVolumeGroupLocalSpec{NodeName: "node-b"}},
		Status: snc.LVMVolumeGroupStatus{
			Nodes:  []snc.LVMVolumeGroupNode{{Name: "node-b"}},
			VGSize: *resource.NewQuantity(100*1024*1024*1024, resource.BinarySI),
			VGFree: *resource.NewQuantity(40*1024*1024*1024, resource.BinarySI),
		},
	}

	// RSP references both LVGs to mark nodes as DRBD
	rsp := &srv.ReplicatedStoragePool{
		ObjectMeta: metav1.ObjectMeta{Name: "rsp-1"},
		Spec: srv.ReplicatedStoragePoolSpec{
			LVMVolumeGroups: []srv.ReplicatedStoragePoolLVMVolumeGroups{{Name: "lvg-a"}, {Name: "lvg-b"}},
		},
	}

	cl := clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(sc, lvgA, lvgB, rsp).Build()
	log, _ := logger.NewLogger(logger.Verbosity("4"))
	c := cache.NewCache(log)
	cm := cache.NewCacheManager(c, nil, log)
	cm.AddLVG(lvgA)
	cm.AddLVG(lvgB)

	s := NewScheduler(context.TODO(), cl, log, cm, 1)

	res, err := s.ReplicaEvaluate(ReplicaEvaluateArgs{
		CandidatesThick: []string{"lvg-a", "lvg-b"},
		RequestedBytes:  10 * 1024 * 1024 * 1024,
		ReplicaKey:      "ns/rr-thick",
		Reserve:         true,
	})
	if err != nil {
		t.Fatalf("ReplicaEvaluate (thick) returned error: %v", err)
	}
	if len(res.NodeNames) != 2 {
		t.Fatalf("expected 2 nodes to pass, got %d", len(res.NodeNames))
	}

	// Check reservations per LVG
	thickReservedA, err := cm.GetLVGThickReservedSpace("lvg-a")
	if err != nil || thickReservedA <= 0 {
		t.Fatalf("expected reserved on lvg-a > 0, got %d, err=%v", thickReservedA, err)
	}
	thickReservedB, err := cm.GetLVGThickReservedSpace("lvg-b")
	if err != nil || thickReservedB <= 0 {
		t.Fatalf("expected reserved on lvg-b > 0, got %d, err=%v", thickReservedB, err)
	}

	// Scoring: lvg-a should have higher score than lvg-b
	var scoreA, scoreB int
	for _, p := range res.Priorities {
		if p.Host == "lvg-a" {
			scoreA = p.Score
		}
		if p.Host == "lvg-b" {
			scoreB = p.Score
		}
	}
	if scoreA <= scoreB {
		t.Fatalf("expected node-a score > node-b, got %d vs %d", scoreA, scoreB)
	}

	// Cleanup non-selected: keep only lvg-b
	if err := s.ReplicaCleanup(ReplicaCleanupArgs{ReplicaKey: "ns/rr-thick", SelectedThickLVG: "lvg-b", ClearNonSelected: true}); err != nil {
		t.Fatalf("ReplicaCleanup non-selected failed: %v", err)
	}
	thickReservedA2, _ := cm.GetLVGThickReservedSpace("lvg-a")
	if thickReservedA2 != 0 {
		t.Fatalf("expected lvg-a reservation cleared, got %d", thickReservedA2)
	}
	thickReservedB2, _ := cm.GetLVGThickReservedSpace("lvg-b")
	if thickReservedB2 == 0 {
		t.Fatalf("expected lvg-b reservation to remain, got %d", thickReservedB2)
	}

	// Cleanup selected: remove on lvg-b
	if err := s.ReplicaCleanup(ReplicaCleanupArgs{ReplicaKey: "ns/rr-thick", SelectedThickLVG: "lvg-b", ClearSelected: true}); err != nil {
		t.Fatalf("ReplicaCleanup selected failed: %v", err)
	}
	thickReservedB3, _ := cm.GetLVGThickReservedSpace("lvg-b")
	if thickReservedB3 != 0 {
		t.Fatalf("expected lvg-b reservation cleared, got %d", thickReservedB3)
	}
}

func TestReplicaReservation_CombinedWithPVC_Thick(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = snc.AddToScheme(scheme)
	_ = v1.AddToScheme(scheme)
	_ = storagev1.AddToScheme(scheme)
	_ = srv.AddToScheme(scheme)

	sc := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{Name: "replicated-thick-2"},
		Parameters: map[string]string{
			consts.LvmTypeParamKey:         consts.Thick,
			consts.LVMVolumeGroupsParamKey: "- name: lvg-x\n",
		},
	}
	lvg := &snc.LVMVolumeGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "lvg-x"},
		Spec:       snc.LVMVolumeGroupSpec{Local: snc.LVMVolumeGroupLocalSpec{NodeName: "node-x"}},
		Status: snc.LVMVolumeGroupStatus{
			Nodes:  []snc.LVMVolumeGroupNode{{Name: "node-x"}},
			VGSize: *resource.NewQuantity(100*1024*1024*1024, resource.BinarySI),
			VGFree: *resource.NewQuantity(90*1024*1024*1024, resource.BinarySI),
		},
	}
	rsp := &srv.ReplicatedStoragePool{ObjectMeta: metav1.ObjectMeta{Name: "rsp-x"}, Spec: srv.ReplicatedStoragePoolSpec{LVMVolumeGroups: []srv.ReplicatedStoragePoolLVMVolumeGroups{{Name: "lvg-x"}}}}

	cl := clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(sc, lvg, rsp).Build()
	log, _ := logger.NewLogger(logger.Verbosity("4"))
	c := cache.NewCache(log)
	cm := cache.NewCacheManager(c, nil, log)
	cm.AddLVG(lvg)
	s := NewScheduler(context.TODO(), cl, log, cm, 1)

	// Add a PVC reservation first (e.g., Pending)
	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1", Namespace: "default"},
		Spec: v1.PersistentVolumeClaimSpec{
			StorageClassName: ptr("replicated-thick-2"),
			Resources:        v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: resource.MustParse("5Gi")}},
		},
		Status: v1.PersistentVolumeClaimStatus{Phase: v1.ClaimPending},
	}
	if err := cm.AddThickPVC("lvg-x", pvc, "replicated.csi.storage.deckhouse.io"); err != nil {
		t.Fatalf("AddThickPVC failed: %v", err)
	}

	// Now evaluate and reserve a replica of 3Gi
	_, err := s.ReplicaEvaluate(ReplicaEvaluateArgs{CandidatesThick: []string{"lvg-x"}, RequestedBytes: 3 * 1024 * 1024 * 1024, ReplicaKey: "ns/rr-combined", Reserve: true})
	if err != nil {
		t.Fatalf("ReplicaEvaluate (combined) returned error: %v", err)
	}

	// Total reserved thick = 5Gi (PVC) + 3Gi (replica)
	got, err := cm.GetLVGThickReservedSpace("lvg-x")
	if err != nil {
		t.Fatalf("GetLVGThickReservedSpace failed: %v", err)
	}
	want := int64(8 * 1024 * 1024 * 1024)
	if got != want {
		t.Fatalf("expected reserved %d, got %d", want, got)
	}
}

func ptr[T any](v T) *T { return &v }

// Negative cases
func TestReplicaEvaluate_Errors(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = snc.AddToScheme(scheme)
	_ = v1.AddToScheme(scheme)
	_ = storagev1.AddToScheme(scheme)
	_ = srv.AddToScheme(scheme)

	log, _ := logger.NewLogger(logger.Verbosity("4"))
	c := cache.NewCache(log)
	cm := cache.NewCacheManager(c, nil, log)

	// 1) zero requested size
	cl := clientfake.NewClientBuilder().WithScheme(scheme).Build()
	s := NewScheduler(context.TODO(), cl, log, cm, 1)
	if _, err := s.ReplicaEvaluate(ReplicaEvaluateArgs{RequestedBytes: 0}); err == nil {
		t.Fatalf("expected validation error for zero size, got nil")
	}

	// 2) thin pool missing or mismatched: pass LVG without such thin pool
	lvg := &snc.LVMVolumeGroup{ObjectMeta: metav1.ObjectMeta{Name: "lvg"}, Spec: snc.LVMVolumeGroupSpec{Local: snc.LVMVolumeGroupLocalSpec{NodeName: "node"}}, Status: snc.LVMVolumeGroupStatus{Nodes: []snc.LVMVolumeGroupNode{{Name: "node"}}, VGSize: *resource.NewQuantity(10*1024*1024*1024, resource.BinarySI), VGFree: *resource.NewQuantity(10*1024*1024*1024, resource.BinarySI)}}
	cm.AddLVG(lvg)
	s2 := NewScheduler(context.TODO(), cl, log, cm, 1)
	res, err := s2.ReplicaEvaluate(ReplicaEvaluateArgs{CandidatesThin: []ThinCandidateItem{{LVGName: "lvg", ThinPool: "tp-x"}}, RequestedBytes: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(res.NodeNames) != 0 {
		t.Fatalf("expected 0 candidates due to missing thin pool, got %d", len(res.NodeNames))
	}

	// 3) empty candidates are allowed -> no passed
	res4, err := s2.ReplicaEvaluate(ReplicaEvaluateArgs{RequestedBytes: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(res4.NodeNames) != 0 {
		t.Fatalf("expected empty candidates result, got %d", len(res4.NodeNames))
	}
}

// Integration-like handler tests (without full HTTP server), via direct handler methods
func TestHandlers_ReplicaEvaluateAndCleanup(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = snc.AddToScheme(scheme)
	_ = v1.AddToScheme(scheme)
	_ = storagev1.AddToScheme(scheme)
	_ = srv.AddToScheme(scheme)

	// Prepare minimal world
	sc := &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "sc-h"}, Parameters: map[string]string{consts.LvmTypeParamKey: consts.Thin, consts.LVMVolumeGroupsParamKey: "- name: lvg-h\n  Thin:\n    poolName: tp-h\n"}}
	lvg := &snc.LVMVolumeGroup{ObjectMeta: metav1.ObjectMeta{Name: "lvg-h"}, Spec: snc.LVMVolumeGroupSpec{Local: snc.LVMVolumeGroupLocalSpec{NodeName: "node-h"}}, Status: snc.LVMVolumeGroupStatus{Nodes: []snc.LVMVolumeGroupNode{{Name: "node-h"}}, VGSize: *resource.NewQuantity(10*1024*1024*1024, resource.BinarySI), VGFree: *resource.NewQuantity(10*1024*1024*1024, resource.BinarySI), ThinPools: []snc.LVMVolumeGroupThinPoolStatus{{Name: "tp-h", ActualSize: *resource.NewQuantity(10*1024*1024*1024, resource.BinarySI), AvailableSpace: *resource.NewQuantity(10*1024*1024*1024, resource.BinarySI), Ready: true}}}}
	rsp := &srv.ReplicatedStoragePool{ObjectMeta: metav1.ObjectMeta{Name: "rsp-h"}, Spec: srv.ReplicatedStoragePoolSpec{LVMVolumeGroups: []srv.ReplicatedStoragePoolLVMVolumeGroups{{Name: "lvg-h"}}}}
	cl := clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(sc, lvg, rsp).Build()
	log, _ := logger.NewLogger(logger.Verbosity("4"))
	cm := cache.NewCacheManager(cache.NewCache(log), nil, log)
	cm.AddLVG(lvg)
	s := NewScheduler(context.TODO(), cl, log, cm, 1)
	h := NewHandler(log, s)

	// Call /replica/evaluate with explicit thin candidate
	evalReq := ReplicaEvaluateArgs{CandidatesThin: []ThinCandidateItem{{LVGName: "lvg-h", ThinPool: "tp-h"}}, RequestedBytes: 1024, ReplicaKey: "ns/h"}
	body, _ := json.Marshal(evalReq)
	r := httptest.NewRequest(http.MethodPost, "/replica/evaluate", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.ReplicaEvaluate(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	// Call /replica/cleanup by thin selection
	clReq := ReplicaCleanupArgs{ReplicaKey: "ns/h", SelectedThinLVG: "lvg-h", SelectedThinPool: "tp-h", ClearSelected: true}
	body2, _ := json.Marshal(clReq)
	r2 := httptest.NewRequest(http.MethodPost, "/replica/cleanup", bytes.NewReader(body2))
	w2 := httptest.NewRecorder()
	h.ReplicaCleanup(w2, r2)
	if w2.Code != http.StatusOK {
		t.Fatalf("expected 200 on cleanup, got %d", w2.Code)
	}
}

func TestReplicaEvaluate_ThinScoringByFreePercent(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = snc.AddToScheme(scheme)
	_ = v1.AddToScheme(scheme)
	_ = storagev1.AddToScheme(scheme)
	_ = srv.AddToScheme(scheme)

	sc := &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "sc-thin-score"}, Parameters: map[string]string{consts.LvmTypeParamKey: consts.Thin, consts.LVMVolumeGroupsParamKey: "- name: lvg-a\n  Thin:\n    poolName: tp\n- name: lvg-b\n  Thin:\n    poolName: tp\n"}}
	lvgA := &snc.LVMVolumeGroup{ObjectMeta: metav1.ObjectMeta{Name: "lvg-a"}, Spec: snc.LVMVolumeGroupSpec{Local: snc.LVMVolumeGroupLocalSpec{NodeName: "node-a"}}, Status: snc.LVMVolumeGroupStatus{Nodes: []snc.LVMVolumeGroupNode{{Name: "node-a"}}, VGSize: *resource.NewQuantity(100*1024*1024*1024, resource.BinarySI), VGFree: *resource.NewQuantity(80*1024*1024*1024, resource.BinarySI), ThinPools: []snc.LVMVolumeGroupThinPoolStatus{{Name: "tp", ActualSize: *resource.NewQuantity(100*1024*1024*1024, resource.BinarySI), AvailableSpace: *resource.NewQuantity(80*1024*1024*1024, resource.BinarySI), Ready: true}}}}
	lvgB := &snc.LVMVolumeGroup{ObjectMeta: metav1.ObjectMeta{Name: "lvg-b"}, Spec: snc.LVMVolumeGroupSpec{Local: snc.LVMVolumeGroupLocalSpec{NodeName: "node-b"}}, Status: snc.LVMVolumeGroupStatus{Nodes: []snc.LVMVolumeGroupNode{{Name: "node-b"}}, VGSize: *resource.NewQuantity(100*1024*1024*1024, resource.BinarySI), VGFree: *resource.NewQuantity(50*1024*1024*1024, resource.BinarySI), ThinPools: []snc.LVMVolumeGroupThinPoolStatus{{Name: "tp", ActualSize: *resource.NewQuantity(100*1024*1024*1024, resource.BinarySI), AvailableSpace: *resource.NewQuantity(50*1024*1024*1024, resource.BinarySI), Ready: true}}}}
	rsp := &srv.ReplicatedStoragePool{ObjectMeta: metav1.ObjectMeta{Name: "rsp-s"}, Spec: srv.ReplicatedStoragePoolSpec{LVMVolumeGroups: []srv.ReplicatedStoragePoolLVMVolumeGroups{{Name: "lvg-a"}, {Name: "lvg-b"}}}}

	cl := clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(sc, lvgA, lvgB, rsp).Build()
	log, _ := logger.NewLogger(logger.Verbosity("4"))
	c := cache.NewCache(log)
	cm := cache.NewCacheManager(c, nil, log)
	cm.AddLVG(lvgA)
	cm.AddLVG(lvgB)
	s := NewScheduler(context.TODO(), cl, log, cm, 1)

	res, err := s.ReplicaEvaluate(ReplicaEvaluateArgs{CandidatesThin: []ThinCandidateItem{{LVGName: "lvg-a", ThinPool: "tp"}, {LVGName: "lvg-b", ThinPool: "tp"}}, RequestedBytes: 10 * 1024 * 1024 * 1024, Reserve: false})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	var scoreA, scoreB int
	for _, p := range res.Priorities {
		if p.Host == "lvg-a/tp" {
			scoreA = p.Score
		}
		if p.Host == "lvg-b/tp" {
			scoreB = p.Score
		}
	}
	if scoreA <= scoreB {
		t.Fatalf("expected node-a score > node-b, got %d vs %d", scoreA, scoreB)
	}
}

func TestHandlers_BadJSON(t *testing.T) {
	// minimal handler with nil-inert scheduler (will fail fast on decode)
	scheme := runtime.NewScheme()
	_ = snc.AddToScheme(scheme)
	_ = v1.AddToScheme(scheme)
	_ = storagev1.AddToScheme(scheme)
	_ = srv.AddToScheme(scheme)

	log, _ := logger.NewLogger(logger.Verbosity("4"))
	cm := cache.NewCacheManager(cache.NewCache(log), nil, log)
	cl := clientfake.NewClientBuilder().WithScheme(scheme).Build()
	s := NewScheduler(context.TODO(), cl, log, cm, 1)
	h := NewHandler(log, s)

	// bad JSON for evaluate
	r := httptest.NewRequest(http.MethodPost, "/replica/evaluate", bytes.NewBufferString("{bad"))
	w := httptest.NewRecorder()
	h.ReplicaEvaluate(w, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for bad json, got %d", w.Code)
	}

	// bad JSON for cleanup
	r2 := httptest.NewRequest(http.MethodPost, "/replica/cleanup", bytes.NewBufferString("{bad"))
	w2 := httptest.NewRecorder()
	h.ReplicaCleanup(w2, r2)
	if w2.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for bad json cleanup, got %d", w2.Code)
	}
}

func TestReplicaCleanup_Idempotency_AndNoReserve(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = snc.AddToScheme(scheme)
	_ = v1.AddToScheme(scheme)
	_ = storagev1.AddToScheme(scheme)
	_ = srv.AddToScheme(scheme)

	sc := &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "sc-idem"}, Parameters: map[string]string{consts.LvmTypeParamKey: consts.Thick, consts.LVMVolumeGroupsParamKey: "- name: lvg-i\n"}}
	lvg := &snc.LVMVolumeGroup{ObjectMeta: metav1.ObjectMeta{Name: "lvg-i"}, Spec: snc.LVMVolumeGroupSpec{Local: snc.LVMVolumeGroupLocalSpec{NodeName: "node-i"}}, Status: snc.LVMVolumeGroupStatus{Nodes: []snc.LVMVolumeGroupNode{{Name: "node-i"}}, VGSize: *resource.NewQuantity(10*1024*1024*1024, resource.BinarySI), VGFree: *resource.NewQuantity(10*1024*1024*1024, resource.BinarySI)}}
	rsp := &srv.ReplicatedStoragePool{ObjectMeta: metav1.ObjectMeta{Name: "rsp-i"}, Spec: srv.ReplicatedStoragePoolSpec{LVMVolumeGroups: []srv.ReplicatedStoragePoolLVMVolumeGroups{{Name: "lvg-i"}}}}

	cl := clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(sc, lvg, rsp).Build()
	log, _ := logger.NewLogger(logger.Verbosity("4"))
	cm := cache.NewCacheManager(cache.NewCache(log), nil, log)
	cm.AddLVG(lvg)
	s := NewScheduler(context.TODO(), cl, log, cm, 1)

	// Cleanup with nothing reserved should be OK
	if err := s.ReplicaCleanup(ReplicaCleanupArgs{ReplicaKey: "ns/rr-idem", SelectedThickLVG: "lvg-i", ClearSelected: true}); err != nil {
		t.Fatalf("cleanup with no reservations failed: %v", err)
	}

	// Evaluate with reserve=false must not create reservation
	if _, err := s.ReplicaEvaluate(ReplicaEvaluateArgs{CandidatesThick: []string{"lvg-i"}, RequestedBytes: 1024, Reserve: false}); err != nil {
		t.Fatalf("evaluate failed: %v", err)
	}
	// Ensure no reservation in thick space
	if got, _ := cm.GetLVGThickReservedSpace("lvg-i"); got != 0 {
		t.Fatalf("expected 0 reserved after reserve=false, got %d", got)
	}

	// Create reservation and cleanup twice
	if _, err := s.ReplicaEvaluate(ReplicaEvaluateArgs{CandidatesThick: []string{"lvg-i"}, RequestedBytes: 1024, Reserve: true, ReplicaKey: "ns/rr-idem"}); err != nil {
		t.Fatalf("evaluate reserve failed: %v", err)
	}
	if got, _ := cm.GetLVGThickReservedSpace("lvg-i"); got == 0 {
		t.Fatalf("expected some reserved > 0, got %d", got)
	}
	if err := s.ReplicaCleanup(ReplicaCleanupArgs{ReplicaKey: "ns/rr-idem", SelectedThickLVG: "lvg-i", ClearSelected: true}); err != nil {
		t.Fatalf("cleanup failed: %v", err)
	}
	// second cleanup should be idempotent
	if err := s.ReplicaCleanup(ReplicaCleanupArgs{ReplicaKey: "ns/rr-idem", SelectedThickLVG: "lvg-i", ClearSelected: true}); err != nil {
		t.Fatalf("second cleanup failed: %v", err)
	}
	if got, _ := cm.GetLVGThickReservedSpace("lvg-i"); got != 0 {
		t.Fatalf("expected reserved 0 after cleanup, got %d", got)
	}
}
