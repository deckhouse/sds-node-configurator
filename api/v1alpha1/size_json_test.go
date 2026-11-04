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

package v1alpha1_test

import (
	"encoding/json"
	"testing"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
)

func TestSizeAcceptsStringOrNumber(t *testing.T) {
	cases := []struct {
		name string
		size string
		want string
	}{
		{"number", `1073741824`, "1073741824"},
		{"decimal number", `1.5`, "1.5"},
		{"quantity", `"10Gi"`, "10Gi"},
		{"percent", `"50%"`, "50%"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var lvg v1alpha1.LVMVolumeGroup
			if err := json.Unmarshal([]byte(`{"spec":{"thinPools":[{"name":"tp","size":`+tc.size+`,"allocationLimit":"150%"}]}}`), &lvg); err != nil {
				t.Fatalf("LVMVolumeGroup: %v", err)
			}
			if got := lvg.Spec.ThinPools[0]; got.Size != tc.want || got.Name != "tp" || got.AllocationLimit != "150%" {
				t.Fatalf("LVMVolumeGroup thin pool = %+v, want size %q", got, tc.want)
			}

			var llv v1alpha1.LVMLogicalVolume
			if err := json.Unmarshal([]byte(`{"spec":{"type":"Thin","size":`+tc.size+`,"lvmVolumeGroupName":"vg","thin":{"poolName":"tp"}}}`), &llv); err != nil {
				t.Fatalf("LVMLogicalVolume: %v", err)
			}
			if llv.Spec.Size != tc.want || llv.Spec.LVMVolumeGroupName != "vg" || llv.Spec.Thin == nil || llv.Spec.Thin.PoolName != "tp" {
				t.Fatalf("LVMLogicalVolume spec = %+v, want size %q", llv.Spec, tc.want)
			}
		})
	}

	t.Run("a list with one numeric size decodes", func(t *testing.T) {
		var lvgs v1alpha1.LVMVolumeGroupList
		data := `{"items":[{"metadata":{"name":"a"},"spec":{"thinPools":[{"name":"tp","size":"10Gi"}]}},{"metadata":{"name":"b"},"spec":{"thinPools":[{"name":"tp","size":1073741824}]}}]}`
		if err := json.Unmarshal([]byte(data), &lvgs); err != nil {
			t.Fatalf("list: %v", err)
		}
		if len(lvgs.Items) != 2 || lvgs.Items[1].Spec.ThinPools[0].Size != "1073741824" {
			t.Fatalf("list items = %+v", lvgs.Items)
		}
	})

	t.Run("a size of another type is an error", func(t *testing.T) {
		var llv v1alpha1.LVMLogicalVolume
		if err := json.Unmarshal([]byte(`{"spec":{"size":true}}`), &llv); err == nil {
			t.Fatal("a boolean size was accepted")
		}
	})
}
