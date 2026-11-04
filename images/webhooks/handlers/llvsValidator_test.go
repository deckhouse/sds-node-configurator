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

package handlers

import (
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cn "github.com/deckhouse/sds-node-configurator/api/v1alpha1"
)

func TestLLVSObjectNameProblem(t *testing.T) {
	tests := []struct {
		name        string
		objectName  string
		actualName  string
		wantProblem string
	}{
		{name: "plain name", objectName: "snap-1"},
		{name: "snapshot inside the name", objectName: "my-snapshot"},
		{name: "snapshot prefix", objectName: "snapshot-1", wantProblem: "start with snapshot"},
		{name: "pvmove prefix", objectName: "pvmove-1", wantProblem: "start with pvmove"},
		{name: "snapshot prefix with an actual name", objectName: "snapshot-1", actualName: "snap-1"},
		{name: "127 characters", objectName: strings.Repeat("s", 127)},
		{name: "128 characters", objectName: strings.Repeat("s", 128), wantProblem: "longer than 127"},
		{name: "128 characters with an actual name", objectName: strings.Repeat("s", 128), actualName: "snap-1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			llvs := &cn.LVMLogicalVolumeSnapshot{
				ObjectMeta: metav1.ObjectMeta{Name: tt.objectName},
				Spec:       cn.LVMLogicalVolumeSnapshotSpec{ActualSnapshotNameOnTheNode: tt.actualName},
			}

			got := llvsObjectNameProblem(llvs)
			if tt.wantProblem == "" {
				if got != "" {
					t.Fatalf("expected no problem, got %q", got)
				}
				return
			}
			if !strings.Contains(got, tt.wantProblem) {
				t.Fatalf("expected a problem mentioning %q, got %q", tt.wantProblem, got)
			}
		})
	}
}
