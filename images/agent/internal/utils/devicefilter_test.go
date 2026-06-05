/*
Copyright 2026 Flant JSC

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

package utils

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/logger"
)

func TestIsForeignDeviceBase(t *testing.T) {
	tests := []struct {
		name string
		base string
		want bool
	}{
		{"rbd canonical", "rbd1", true},
		{"rbd with partition", "rbd14p1", true},
		{"drbd canonical", "drbd0", true},
		{"nbd canonical", "nbd5", true},
		{"loop canonical", "loop970", true},
		{"loop with high index", "loop1234", true},

		{"nvme canonical", "nvme4n1p1", false},
		{"sda canonical", "sda1", false},
		{"md raid", "md1", false},
		{"dm-mapper", "dm-3", false},
		{"empty", "", false},

		// Sanity: do not be fooled by a foreign-prefix appearing anywhere
		// other than the start of the basename. We only match prefixes.
		{"rbd in the middle", "sd-rbd-cache", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, IsForeignDeviceBase(tt.base))
		})
	}
}

func TestFilterForeignPVs(t *testing.T) {
	log, err := logger.NewLogger(logger.ErrorLevel)
	if err != nil {
		t.Fatalf("init logger: %v", err)
	}

	pvs := []internal.PVData{
		{PVName: "/dev/nvme4n1p1", VGName: "vg-thin-data", VGUuid: "crBlB1"},
		{PVName: "/dev/nvme4n1p2", VGName: "vg-sds-local", VGUuid: "ksm1nq"},
		{PVName: "/dev/md1", VGName: "vg0", VGUuid: "IZMRUl"},
		{PVName: "/dev/block/251:144", VGName: "vg-1", VGUuid: "Czf0Sf"},                                 // -> rbd9
		{PVName: "/dev/block/251:16", VGName: "vg-thin-data", VGUuid: "zR5ouf"},                          // -> rbd1
		{PVName: "/dev/disk/by-id/lvm-pv-uuid-9Uuprg-IFQM-5c2y", VGName: "vg-1", VGUuid: "kHmPc6"},       // -> loop808
		{PVName: "/dev/disk/by-id/lvm-pv-uuid-xl1soD-zQZM-BRAt", VGName: "vg-thin-data", VGUuid: "He4J"}, // -> loop485
	}

	// Resolver mimics the readlink -f result observed on d8-virt-node-0
	// during diagnosis: foreign aliases resolve to /dev/rbdN or
	// /dev/loopN, local PVs resolve to themselves.
	resolver := func(_ context.Context, p string) (string, error) {
		switch p {
		case "/dev/nvme4n1p1", "/dev/nvme4n1p2", "/dev/md1":
			return p, nil
		case "/dev/block/251:144":
			return "/dev/rbd9", nil
		case "/dev/block/251:16":
			return "/dev/rbd1", nil
		case "/dev/disk/by-id/lvm-pv-uuid-9Uuprg-IFQM-5c2y":
			return "/dev/loop808", nil
		case "/dev/disk/by-id/lvm-pv-uuid-xl1soD-zQZM-BRAt":
			return "/dev/loop485", nil
		}
		return "", errors.New("unknown path")
	}

	got := FilterForeignPVs(context.Background(), log, resolver, pvs, 0)
	wantNames := []string{"/dev/nvme4n1p1", "/dev/nvme4n1p2", "/dev/md1"}
	gotNames := make([]string, 0, len(got))
	for _, pv := range got {
		gotNames = append(gotNames, pv.PVName)
	}
	assert.ElementsMatch(t, wantNames, gotNames)
}

func TestFilterForeignPVs_keepsOnResolverError(t *testing.T) {
	log, err := logger.NewLogger(logger.ErrorLevel)
	if err != nil {
		t.Fatalf("init logger: %v", err)
	}

	pvs := []internal.PVData{
		{PVName: "/dev/nvme4n1p1", VGUuid: "u1"},
		{PVName: "/dev/something-transient", VGUuid: "u2"},
	}
	resolver := func(_ context.Context, p string) (string, error) {
		if p == "/dev/something-transient" {
			return "", errors.New("transient resolver failure")
		}
		return p, nil
	}

	got := FilterForeignPVs(context.Background(), log, resolver, pvs, 0)
	assert.Len(t, got, 2, "transient resolver failure must not drop a PV")
}

func TestFilterForeignPVs_emptyPVName(t *testing.T) {
	log, err := logger.NewLogger(logger.ErrorLevel)
	if err != nil {
		t.Fatalf("init logger: %v", err)
	}

	pvs := []internal.PVData{{PVName: "", VGUuid: "u1"}}
	calls := 0
	resolver := func(_ context.Context, _ string) (string, error) {
		calls++
		return "", nil
	}

	got := FilterForeignPVs(context.Background(), log, resolver, pvs, 0)
	assert.Len(t, got, 1)
	assert.Equal(t, 0, calls, "empty pv_name must not trigger the resolver")
}

// TestFilterForeignPVs_perCallTimeout pins the contract introduced for
// PR #290 / scanner.fillTheCache: a hung resolver call must not block
// the scan loop. With a tiny per-call deadline a resolver that respects
// ctx must return a context.DeadlineExceeded-class error, the PV is
// then conservatively kept in the cache (matching the "keep on resolver
// error" branch verified above), and the overall call returns within a
// few timeouts rather than hanging on the first PV.
func TestFilterForeignPVs_perCallTimeout(t *testing.T) {
	log, err := logger.NewLogger(logger.ErrorLevel)
	if err != nil {
		t.Fatalf("init logger: %v", err)
	}

	// Three PVs all backed by a "hung" resolver. If the per-call
	// timeout were not honored, the test would hang here.
	pvs := []internal.PVData{
		{PVName: "/dev/hang-a", VGUuid: "a"},
		{PVName: "/dev/hang-b", VGUuid: "b"},
		{PVName: "/dev/hang-c", VGUuid: "c"},
	}

	calls := 0
	resolver := func(ctx context.Context, _ string) (string, error) {
		calls++
		// Block until the per-call deadline fires.
		<-ctx.Done()
		return "", ctx.Err()
	}

	const perCallTimeout = 20 * time.Millisecond
	// Allow generous slack vs perCallTimeout * len(pvs); on a heavily
	// loaded CI runner go's scheduler can be sluggish, but we still
	// want this test to fail loudly if the timeout is missing entirely
	// (in which case it would hang until `go test -timeout`).
	deadline := time.Now().Add(2 * time.Second)

	start := time.Now()
	got := FilterForeignPVs(context.Background(), log, resolver, pvs, perCallTimeout)
	elapsed := time.Since(start)

	if time.Now().After(deadline) {
		t.Fatalf("FilterForeignPVs took too long (%s) — per-call timeout not honored", elapsed)
	}
	assert.Equal(t, len(pvs), calls, "resolver must be invoked once per non-empty PV")
	assert.Len(t, got, len(pvs),
		"PVs whose resolver hits the per-call timeout must be conservatively kept "+
			"(same contract as the transient-error branch)")
}

func TestFilterVGsByPresentPVs(t *testing.T) {
	tests := []struct {
		name      string
		vgs       []internal.VGData
		pvs       []internal.PVData
		wantUUIDs []string
	}{
		{
			name: "drops phantom VGs whose PVs were filtered out",
			vgs: []internal.VGData{
				{VGName: "vg-thin-data", VGUUID: "crBlB1"},
				{VGName: "vg-thin-data", VGUUID: "zR5ouf"},
				{VGName: "vg-1", VGUUID: "Czf0Sf"},
				{VGName: "vg0", VGUUID: "IZMRUl"},
			},
			pvs: []internal.PVData{
				{PVName: "/dev/nvme4n1p1", VGUuid: "crBlB1"},
				{PVName: "/dev/md1", VGUuid: "IZMRUl"},
			},
			wantUUIDs: []string{"crBlB1", "IZMRUl"},
		},
		{
			name:      "no PVs leaves no VGs",
			vgs:       []internal.VGData{{VGName: "vg-thin-data", VGUUID: "crBlB1"}},
			pvs:       nil,
			wantUUIDs: nil,
		},
		{
			name: "VG without VGUUID is dropped",
			vgs: []internal.VGData{
				{VGName: "broken", VGUUID: ""},
				{VGName: "vg0", VGUUID: "IZMRUl"},
			},
			pvs:       []internal.PVData{{PVName: "/dev/md1", VGUuid: "IZMRUl"}},
			wantUUIDs: []string{"IZMRUl"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FilterVGsByPresentPVs(tt.vgs, tt.pvs)
			gotUUIDs := make([]string, 0, len(got))
			for _, vg := range got {
				gotUUIDs = append(gotUUIDs, vg.VGUUID)
			}
			assert.ElementsMatch(t, tt.wantUUIDs, gotUUIDs)
		})
	}
}

func TestFilterLVsByPresentVGs(t *testing.T) {
	vgs := []internal.VGData{
		{VGName: "vg-thin-data", VGUUID: "crBlB1"},
		{VGName: "vg0", VGUUID: "IZMRUl"},
	}
	lvs := []internal.LVData{
		{LVName: "thin-data", VGName: "vg-thin-data", VGUuid: "crBlB1"},
		{LVName: "ghost-thin", VGName: "vg-thin-data", VGUuid: "zR5ouf"},
		{LVName: "system-root", VGName: "vg0", VGUuid: "IZMRUl"},
		{LVName: "ghost-vg1-lv", VGName: "vg-1", VGUuid: "Czf0Sf"},
	}
	got := FilterLVsByPresentVGs(lvs, vgs)
	wantNames := []string{"thin-data", "system-root"}
	gotNames := make([]string, 0, len(got))
	for _, lv := range got {
		gotNames = append(gotNames, lv.LVName)
	}
	assert.ElementsMatch(t, wantNames, gotNames)
}
