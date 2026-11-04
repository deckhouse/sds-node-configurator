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

package feature

import "testing"

func TestFeaturesPerEdition(t *testing.T) {
	want := map[string]struct{ snapshots, volumeCleanup bool }{
		"ce":     {snapshots: false, volumeCleanup: false},
		"se":     {snapshots: true, volumeCleanup: true},
		"seplus": {snapshots: true, volumeCleanup: true},
		"csepro": {snapshots: true, volumeCleanup: true},
		"ee":     {snapshots: true, volumeCleanup: true},
	}

	w, ok := want[edition]
	if !ok {
		t.Fatalf("no expectation for edition %q", edition)
	}
	if got := SnapshotsEnabled(); got != w.snapshots {
		t.Errorf("edition %s: SnapshotsEnabled() = %v, want %v", edition, got, w.snapshots)
	}
	if got := VolumeCleanupEnabled(); got != w.volumeCleanup {
		t.Errorf("edition %s: VolumeCleanupEnabled() = %v, want %v", edition, got, w.volumeCleanup)
	}
}
