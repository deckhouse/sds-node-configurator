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
	"testing"

	"k8s.io/apimachinery/pkg/util/validation"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
)

// The expected values are the contract with every CSI driver that selects by
// the label: a change here hides nothing on clusters where the driver still
// computes the old value.
func TestSCSITargetLabelValue(t *testing.T) {
	const longIQN = "iqn.2000-01.com.example:storage.array-with-a-rather-long-name.controller-a.port-1"

	cases := []struct {
		name     string
		identity string
		want     string
	}{
		{"empty", "", ""},
		{"blank", "  ", ""},
		{"iqn", "iqn.2000-01.com.example:target", "iqn.2000-01.com.example_target"},
		{"iqn in upper case", "IQN.2000-01.COM.EXAMPLE:Target", "iqn.2000-01.com.example_target"},
		{"iqn with surrounding space", " iqn.2000-01.com.example:target\n", "iqn.2000-01.com.example_target"},
		{"iqn without a colon", "iqn.2000-01.com.example", "iqn.2000-01.com.example"},
		{"eui name", "eui.02004567A425678D", "eui.02004567a425678d"},
		{"wwpn as sysfs reports it", "0x500a0981891b8dc5", "500a0981891b8dc5"},
		{"wwpn in upper case", "0x500A0981891B8DC5", "500a0981891b8dc5"},
		{"wwpn with colons", "50:0a:09:81:89:1b:8d:c5", "500a0981891b8dc5"},
		{"bare wwpn", "500a0981891b8dc5", "500a0981891b8dc5"},
		{"long iqn", longIQN, "iqn.2000-01.com.example_storage.array-with-a-r-4c5d5b419c8d3a93"},
		{"long iqn in upper case", "IQN.2000-01.COM.EXAMPLE:STORAGE.ARRAY-WITH-A-RATHER-LONG-NAME.CONTROLLER-A.PORT-1",
			"iqn.2000-01.com.example_storage.array-with-a-r-4c5d5b419c8d3a93"},
		{"iqn ending in a colon", "iqn.2000-01.com.example:", "iqn.2000-01.com.example-631ebd0904ebbabb"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := v1alpha1.SCSITargetLabelValue(tc.identity)
			if got != tc.want {
				t.Fatalf("SCSITargetLabelValue(%q) = %q, want %q", tc.identity, got, tc.want)
			}
			if errs := validation.IsValidLabelValue(got); len(errs) != 0 {
				t.Fatalf("SCSITargetLabelValue(%q) = %q is not a valid label value: %v", tc.identity, got, errs)
			}
		})
	}
}

// Two long names that share the part kept in the clear must still differ.
func TestSCSITargetLabelValueKeepsLongNamesApart(t *testing.T) {
	const common = "iqn.2000-01.com.example:storage.array-with-a-rather-long-name.controller-"

	a := v1alpha1.SCSITargetLabelValue(common + "a")
	b := v1alpha1.SCSITargetLabelValue(common + "b")
	if a == b {
		t.Fatalf("two different names share the label value %q", a)
	}
}
