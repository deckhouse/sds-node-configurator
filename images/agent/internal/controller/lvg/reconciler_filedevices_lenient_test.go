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

package lvg

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/utils"
)

// A mistyped entry added to a healthy LVMVolumeGroup used to make the whole
// resource invalid, and the apiserver would not let the entry be removed
// again — the LVG was stuck in ValidationFailed with no way back short of
// deleting it and its data. The entry must be reported and skipped instead,
// while the rest of the configuration keeps reconciling.
func TestValidateLVGForUpdateFunc_BadFileDeviceDoesNotInvalidateTheLVG(t *testing.T) {
	const vgName = "test-vg-file"

	r := setupReconciler()
	r.sdsCache.StoreVGs([]internal.VGData{{
		VGName: vgName,
		VGSize: resource.MustParse("10Gi"),
		VGFree: resource.MustParse("10Gi"),
	}}, bytes.Buffer{})
	r.sdsCache.StorePVs(nil, bytes.Buffer{})

	lvg := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: v1.ObjectMeta{Name: "vg-file"},
		Spec: v1alpha1.LVMVolumeGroupSpec{
			ActualVGNameOnTheNode: vgName,
			FileDevices: []v1alpha1.LVMVolumeGroupFileDeviceSpec{
				{Name: "good", Directory: "/opt/deckhouse/sds/file-devices", Size: resource.MustParse("2Gi")},
				// 1G is 10^9 bytes — below the 1Gi minimum. The classic typo.
				{Name: "typo", Directory: "/opt/deckhouse/sds/file-devices", Size: resource.MustParse("1G")},
			},
		},
	}

	valid, reason, issues := r.validateLVGForUpdateFunc(context.Background(), lvg, map[string]v1alpha1.BlockDevice{})

	assert.True(t, valid, "one bad entry must not invalidate the LVMVolumeGroup")
	assert.Empty(t, reason)
	assert.NotEmpty(t, issues.reason, "the bad entry must still be reported")
	assert.Contains(t, issues.reason, "fileDevices[1].size")
	assert.True(t, issues.shouldSkip("typo"), "the bad entry must be skipped when provisioning")
	assert.False(t, issues.shouldSkip("good"), "the valid entry must still be provisioned")
}

// The capacity of an entry that will never be provisioned must not be counted
// towards the VG size, or a thin-pool sized against it would be accepted and
// then fail to be created.
func TestValidateFileDevices_InvalidEntrySizeIsNotCounted(t *testing.T) {
	r := setupReconciler()

	lvg := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: v1.ObjectMeta{Name: "vg-a"},
		Spec: v1alpha1.LVMVolumeGroupSpec{
			FileDevices: []v1alpha1.LVMVolumeGroupFileDeviceSpec{
				{Name: "good", Directory: "/opt/deckhouse/sds/file-devices", Size: resource.MustParse("4Gi")},
				{Name: "relative", Directory: "not/absolute", Size: resource.MustParse("100Gi")},
			},
		},
	}

	var reason strings.Builder
	total := resource.MustParse("0")
	invalid := r.validateFileDevices(context.Background(), lvg, &reason, &total)

	assert.Contains(t, invalid, "relative")
	assert.NotContains(t, invalid, "good")
	want := resource.MustParse("4Gi")
	assert.Equal(t, want.Value(), total.Value(),
		"only the valid entry may contribute to the VG size")
}

// The apiserver bounds the LVMVolumeGroup name and the entry name separately;
// only the agent knows they end up in one path component.
func TestValidateFileDevices_RejectsNamesThatOverflowNameMax(t *testing.T) {
	r := setupReconciler()

	lvg := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: v1.ObjectMeta{Name: strings.Repeat("a", 253)},
		Spec: v1alpha1.LVMVolumeGroupSpec{
			FileDevices: []v1alpha1.LVMVolumeGroupFileDeviceSpec{
				{Name: strings.Repeat("x", 63), Directory: "/opt/deckhouse/sds/file-devices", Size: resource.MustParse("2Gi")},
			},
		},
	}

	var reason strings.Builder
	invalid := r.validateFileDevices(context.Background(), lvg, &reason, nil)

	assert.Len(t, invalid, 1)
	assert.Contains(t, reason.String(), "NAME_MAX")
}

// Dropping an entry that still backs a PV is drift, not an instruction: the
// module never shrinks a Volume Group. It has to be visible on the condition
// rather than silently ignored or, worse, acted upon.
func TestFileDeviceDriftReason(t *testing.T) {
	r := setupReconciler()
	lvgWith := func(spec []v1alpha1.LVMVolumeGroupFileDeviceSpec, status []v1alpha1.LVMVolumeGroupFileDevice) *v1alpha1.LVMVolumeGroup {
		return &v1alpha1.LVMVolumeGroup{
			ObjectMeta: v1.ObjectMeta{Name: "vg-a"},
			Spec: v1alpha1.LVMVolumeGroupSpec{
				ActualVGNameOnTheNode: "vg-a-on-node",
				FileDevices:           spec,
			},
			Status: v1alpha1.LVMVolumeGroupStatus{
				Nodes: []v1alpha1.LVMVolumeGroupNode{{Name: "test_node", FileDevices: status}},
			},
		}
	}
	entry := func(name string) v1alpha1.LVMVolumeGroupFileDeviceSpec {
		return v1alpha1.LVMVolumeGroupFileDeviceSpec{Name: name, Directory: "/opt/deckhouse/sds/file-devices", Size: resource.MustParse("2Gi")}
	}

	t.Run("in_sync", func(t *testing.T) {
		lvg := lvgWith([]v1alpha1.LVMVolumeGroupFileDeviceSpec{entry("d1")},
			[]v1alpha1.LVMVolumeGroupFileDevice{{Name: "d1"}})
		assert.Empty(t, r.fileDeviceDriftReason(lvg))
	})

	t.Run("entry_removed_while_pv_is_live", func(t *testing.T) {
		lvg := lvgWith([]v1alpha1.LVMVolumeGroupFileDeviceSpec{entry("d1")},
			[]v1alpha1.LVMVolumeGroupFileDevice{{Name: "d1"}, {Name: "d2"}})
		got := r.fileDeviceDriftReason(lvg)
		assert.Contains(t, got, "d2")
		assert.NotContains(t, got, "d1")
		assert.Contains(t, got, "vg-a-on-node")
	})

	t.Run("entry_never_provisioned_is_not_drift", func(t *testing.T) {
		lvg := lvgWith([]v1alpha1.LVMVolumeGroupFileDeviceSpec{entry("d1"), entry("d2")},
			[]v1alpha1.LVMVolumeGroupFileDevice{{Name: "d1"}})
		assert.Empty(t, r.fileDeviceDriftReason(lvg))
	})

	t.Run("nameless_status_entry_is_ignored", func(t *testing.T) {
		// Written before the name field existed; the discoverer refills it.
		lvg := lvgWith([]v1alpha1.LVMVolumeGroupFileDeviceSpec{entry("d1")},
			[]v1alpha1.LVMVolumeGroupFileDevice{{FilePath: "/opt/deckhouse/sds/file-devices/sds-vg-a.d1.img"}})
		assert.Empty(t, r.fileDeviceDriftReason(lvg))
	})
}

// The status entry name must come from the backing file's own name, not from a
// lookup in spec.fileDevices. Resolving it against the spec made the drift
// report unreachable: removing the entry blanked the name on the next discovery
// pass, so the very act being reported erased its own evidence.
func TestConvertLVMVGFileDevices_NameComesFromTheBackingFile(t *testing.T) {
	const dir = "/opt/deckhouse/sds/file-devices"

	got := convertLVMVGFileDevices([]internal.LVMVGFileDevice{
		{FilePath: dir + "/sds-vg-a.d1.img", LoopDevice: "/dev/loop0", PVUUID: "uuid-a"},
		{FilePath: dir + "/sds-vg-a.dropped.img", LoopDevice: "/dev/loop1", PVUUID: "uuid-b"},
		{FilePath: "/var/lib/libvirt/images/disk0.qcow2", LoopDevice: "/dev/loop2", PVUUID: "uuid-c"},
	})

	if assert.Len(t, got, 3) {
		assert.Equal(t, "d1", got[0].Name)
		// Still named although no spec entry references it any more — this is
		// exactly the device fileDeviceDriftReason has to be able to see.
		assert.Equal(t, "dropped", got[1].Name)
		// Not one of ours: nothing to attribute it to.
		assert.Empty(t, got[2].Name)
	}
}

// Two entries of equal size in one directory are distinct files now — the
// previous size-keyed scheme could not express this at all.
func TestBuildFileDevicePath_EqualSizesInOneDirectoryDiffer(t *testing.T) {
	const dir = "/opt/deckhouse/sds/file-devices"
	assert.NotEqual(t,
		utils.BuildFileDevicePath(dir, "vg-a", "d1"),
		utils.BuildFileDevicePath(dir, "vg-a", "d2"))
}

// The three ways a file device can be left unapplied are different problems for
// whoever is on call, so each carries its own reason: a malformed entry needs a
// spec edit, an entry the node could not bring up may well fix itself once there
// is room, and drift needs an operator to decide what happens to a Physical
// Volume the spec no longer mentions. An alert on "malformed LVMVolumeGroup"
// must not fire on the other two.
func TestFileDeviceConditionReason(t *testing.T) {
	const malformed = "fileDevices[0].size 1G is less than the minimum 1Gi. "
	const unapplied = "not enough free space in \"/opt/deckhouse/sds/file-devices\". "

	t.Run("drift alone", func(t *testing.T) {
		assert.Equal(t, internal.ReasonFileDeviceDrift, fileDeviceConditionReason("", "", ""))
	})

	t.Run("an entry the node could not bring up", func(t *testing.T) {
		assert.Equal(t, internal.ReasonFileDeviceNotApplied, fileDeviceConditionReason("", unapplied, ""))
	})

	t.Run("a more specific cause overrides the generic one", func(t *testing.T) {
		assert.Equal(t, internal.ReasonAliasResolutionFailed,
			fileDeviceConditionReason("", unapplied, internal.ReasonAliasResolutionFailed))
	})

	t.Run("a malformed entry takes precedence over both", func(t *testing.T) {
		// Every problem is reported in the message; the reason names the one to
		// fix first.
		assert.Equal(t, internal.ReasonValidationFailed, fileDeviceConditionReason(malformed, "", ""))
		assert.Equal(t, internal.ReasonValidationFailed, fileDeviceConditionReason(malformed, unapplied, ""))
		assert.Equal(t, internal.ReasonValidationFailed,
			fileDeviceConditionReason(malformed, unapplied, internal.ReasonAliasResolutionFailed))
	})

	t.Run("the reasons are distinguishable", func(t *testing.T) {
		reasons := []string{
			internal.ReasonValidationFailed,
			internal.ReasonFileDeviceDrift,
			internal.ReasonFileDeviceNotApplied,
			internal.ReasonAliasResolutionFailed,
		}
		seen := make(map[string]struct{}, len(reasons))
		for _, r := range reasons {
			_, dup := seen[r]
			assert.False(t, dup, "reason %q is not distinct", r)
			seen[r] = struct{}{}
		}
	})
}

// An entry the node could not bring up must not be reported the way a broken
// Volume Group is: the Volume Group is intact, and the reason the caller picks
// decides whether the LVMVolumeGroup stays schedulable. splitUnappliedFileDevices
// is what draws that line.
func TestSplitUnappliedFileDevices(t *testing.T) {
	t.Run("nil stays nil", func(t *testing.T) {
		msg, reason, fatal := splitUnappliedFileDevices(nil)
		assert.Empty(t, msg)
		assert.Empty(t, reason)
		assert.NoError(t, fatal)
	})

	t.Run("an ordinary error stays fatal", func(t *testing.T) {
		msg, reason, fatal := splitUnappliedFileDevices(errors.New("vgextend failed"))
		assert.Empty(t, msg)
		assert.Empty(t, reason)
		assert.EqualError(t, fatal, "vgextend failed")
	})

	t.Run("a per-entry problem becomes a message, not a failure", func(t *testing.T) {
		msg, reason, fatal := splitUnappliedFileDevices(
			unappliedFileDevices("", errors.New("fileDevices entry \"d0\": not enough free space")))
		assert.NoError(t, fatal)
		assert.Empty(t, reason, "no override means the default reason")
		assert.Equal(t, "fileDevices entry \"d0\": not enough free space. ", msg,
			"the message is punctuated so it can be concatenated with the other file-device issues")
	})

	t.Run("a reason override travels with the error", func(t *testing.T) {
		_, reason, fatal := splitUnappliedFileDevices(
			unappliedFileDevices(internal.ReasonAliasResolutionFailed, errors.New("resolver stuck")))
		assert.NoError(t, fatal)
		assert.Equal(t, internal.ReasonAliasResolutionFailed, reason)
	})

	t.Run("it is found through a wrapper", func(t *testing.T) {
		wrapped := fmt.Errorf("extend: %w", unappliedFileDevices("", errors.New("no space")))
		_, _, fatal := splitUnappliedFileDevices(wrapped)
		assert.NoError(t, fatal)
	})
}
