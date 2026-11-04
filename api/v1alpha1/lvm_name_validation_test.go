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
	"context"
	"fmt"
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
)

type lvmNameCase struct {
	name    string
	value   string
	wantErr string
}

var lvmNameCommonCases = []lvmNameCase{
	{name: "plain", value: "data"},
	{name: "every allowed character", value: "Data+1_x.y-z"},
	{name: "127 characters", value: strings.Repeat("v", 127)},
	{name: "128 characters", value: strings.Repeat("v", 128), wantErr: "127"},
	{name: "leading hyphen", value: "-data", wantErr: "hyphen"},
	{name: "dot", value: ".", wantErr: "'.' or '..'"},
	{name: "two dots", value: "..", wantErr: "'.' or '..'"},
	{name: "space", value: "da ta", wantErr: "only the characters"},
	{name: "slash", value: "da/ta", wantErr: "only the characters"},
}

var lvmLVNameCases = append([]lvmNameCase{
	{name: "snapshot inside", value: "my-snapshot"},
	{name: "snap prefix", value: "snap-1"},
	{name: "snapshot", value: "snapshot", wantErr: "start with snapshot or pvmove"},
	{name: "snapshot prefix", value: "snapshot-1", wantErr: "start with snapshot or pvmove"},
	{name: "pvmove prefix", value: "pvmove0", wantErr: "start with snapshot or pvmove"},
	{name: "tdata", value: "pool_tdata", wantErr: "_tdata"},
	{name: "tmeta inside", value: "a_tmeta_b", wantErr: "_tmeta"},
	{name: "rimage", value: "lv_rimage_0", wantErr: "_rimage"},
	{name: "vorigin", value: "x_vorigin", wantErr: "_vorigin"},
}, lvmNameCommonCases...)

func TestLVGActualVGNameFollowsLVMRules(t *testing.T) {
	cases := append([]lvmNameCase{
		// LVM checks the reserved logical volume names only for logical volumes.
		{name: "snapshot prefix", value: "snapshot-vg"},
		{name: "tdata", value: "vg_tdata"},
		{name: "empty", value: "", wantErr: "actualVGNameOnTheNode"},
	}, lvmNameCommonCases...)
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lvg := newLVG(fmt.Sprintf("lvg-vg-name-%d", i), tc.value, nil)
			expectCreate(t, lvg, tc.wantErr)
		})
	}
}

func TestLVGThinPoolNameFollowsLVMRules(t *testing.T) {
	cases := append([]lvmNameCase{
		{name: "empty", value: "", wantErr: "name"},
	}, lvmLVNameCases...)
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lvg := newLVG(fmt.Sprintf("lvg-tp-name-%d", i), "vg-data", []v1alpha1.LVMVolumeGroupThinPoolSpec{{Name: tc.value, Size: "1Gi", AllocationLimit: "150%"}})
			expectCreate(t, lvg, tc.wantErr)
		})
	}
}

func TestLLVActualLVNameRejectsLVMReservedNames(t *testing.T) {
	// The field keeps its own pattern, lowercase only and no leading or
	// trailing punctuation, so only the cases that pattern allows are run here.
	cases := []lvmNameCase{
		{name: "plain", value: "data"},
		{name: "snapshot inside", value: "my-snapshot"},
		{name: "127 characters", value: strings.Repeat("v", 127)},
		{name: "128 characters", value: strings.Repeat("v", 128), wantErr: "127"},
		{name: "snapshot prefix", value: "snapshot-1", wantErr: "start with snapshot or pvmove"},
		{name: "pvmove prefix", value: "pvmove0", wantErr: "start with snapshot or pvmove"},
		{name: "tdata", value: "pool_tdata", wantErr: "_tdata"},
		{name: "cmeta inside", value: "a_cmeta_b", wantErr: "_cmeta"},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			llv := &v1alpha1.LVMLogicalVolume{
				ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("llv-lv-name-%d", i)},
				Spec: v1alpha1.LVMLogicalVolumeSpec{
					ActualLVNameOnTheNode: tc.value,
					Type:                  "Thick",
					Size:                  "1Gi",
					LVMVolumeGroupName:    "lvg",
				},
			}
			expectCreate(t, llv, tc.wantErr)
		})
	}
}

func TestLLVSActualSnapshotNameFollowsLVMRules(t *testing.T) {
	for i, tc := range lvmLVNameCases {
		t.Run(tc.name, func(t *testing.T) {
			llvs := &v1alpha1.LVMLogicalVolumeSnapshot{
				ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("llvs-snap-name-%d", i)},
				Spec: v1alpha1.LVMLogicalVolumeSnapshotSpec{
					ActualSnapshotNameOnTheNode: tc.value,
					LVMLogicalVolumeName:        "src-llv",
				},
			}
			expectCreate(t, llvs, tc.wantErr)
		})
	}
}

func newLVG(name, vgName string, thinPools []v1alpha1.LVMVolumeGroupThinPoolSpec) *v1alpha1.LVMVolumeGroup {
	return &v1alpha1.LVMVolumeGroup{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1alpha1.LVMVolumeGroupSpec{
			ActualVGNameOnTheNode: vgName,
			Type:                  "Local",
			Local:                 v1alpha1.LVMVolumeGroupLocalSpec{NodeName: "node-1"},
			BlockDeviceSelector:   &metav1.LabelSelector{MatchLabels: map[string]string{"kubernetes.io/hostname": "node-1"}},
			ThinPools:             thinPools,
		},
	}
}

// expectCreate creates obj and checks that the API server accepts it when
// wantErr is empty and otherwise rejects it as invalid with wantErr in the
// message.
func expectCreate(t *testing.T, obj client.Object, wantErr string) {
	t.Helper()
	ctx := context.Background()
	err := k8sClient.Create(ctx, obj)
	if err == nil {
		t.Cleanup(func() { _ = k8sClient.Delete(ctx, obj) })
	}
	if wantErr == "" {
		if err != nil {
			t.Fatalf("expected the object to be accepted, got: %v", err)
		}
		return
	}
	if err == nil {
		t.Fatalf("expected the object to be rejected with %q, it was accepted", wantErr)
	}
	if !apierrors.IsInvalid(err) {
		t.Fatalf("expected Invalid, got %T: %v", err, err)
	}
	if !strings.Contains(err.Error(), wantErr) {
		t.Fatalf("expected the error to mention %q, got: %v", wantErr, err)
	}
}
