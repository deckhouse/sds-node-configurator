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

package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
)

// A file-only LVMVolumeGroupSet template carries no blockDeviceSelector;
// the child LVMVolumeGroup is admitted only if configureLVGBySet copies
// fileDevices over. Without it the child has neither selector nor
// fileDevices and is rejected by its own CEL rule.
func TestConfigureLVGBySet_CopiesFileDevices(t *testing.T) {
	fileDevices := []v1alpha1.LVMVolumeGroupFileDeviceSpec{
		{Directory: "/data/lvm-backing", Size: resource.MustParse("50Gi")},
	}
	lvgSet := &v1alpha1.LVMVolumeGroupSet{
		ObjectMeta: metav1.ObjectMeta{Name: "set-a"},
		Spec: v1alpha1.LVMVolumeGroupSetSpec{
			LVGTemplate: v1alpha1.LVMVolumeGroupTemplate{
				ActualVGNameOnTheNode: "vg-file",
				Type:                  "Local",
				FileDevices:           fileDevices,
			},
		},
	}

	lvg := configureLVGBySet(lvgSet, v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-0"}})

	assert.Equal(t, fileDevices, lvg.Spec.FileDevices)
	assert.Nil(t, lvg.Spec.BlockDeviceSelector)
	assert.Equal(t, "node-0", lvg.Spec.Local.NodeName)
}

// Editing fileDevices on the set template (the documented "add an entry to
// grow capacity" flow) must propagate to already-created child LVGs.
func TestUpdateLVMVolumeGroupByConfiguredFromSet_PropagatesFileDevices(t *testing.T) {
	configured := &v1alpha1.LVMVolumeGroup{
		Spec: v1alpha1.LVMVolumeGroupSpec{
			FileDevices: []v1alpha1.LVMVolumeGroupFileDeviceSpec{
				{Directory: "/data", Size: resource.MustParse("10Gi")},
				{Directory: "/data", Size: resource.MustParse("20Gi")},
			},
		},
	}
	existing := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "set-a-0"},
		Spec: v1alpha1.LVMVolumeGroupSpec{
			FileDevices: []v1alpha1.LVMVolumeGroupFileDeviceSpec{
				{Directory: "/data", Size: resource.MustParse("10Gi")},
			},
		},
	}

	ctx := context.Background()
	cl := NewFakeClient()
	assert.NoError(t, cl.Create(ctx, existing))

	err := updateLVMVolumeGroupByConfiguredFromSet(ctx, cl, existing, configured)
	assert.NoError(t, err)
	assert.Equal(t, configured.Spec.FileDevices, existing.Spec.FileDevices)
}
