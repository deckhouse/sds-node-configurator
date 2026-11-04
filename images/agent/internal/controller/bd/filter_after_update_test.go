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

package bd

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
)

// A BlockDevice whose update is what takes it out of the filters goes in the
// same pass. The agent runs a pass only on a udev event, so "the next pass" can
// be a long time, and the device stays offered to LVMVolumeGroups all along.
func TestBlockDeviceReconcile_FiltersOnTheLabelsItJustWrote(t *testing.T) {
	ctx := context.Background()
	d := setupDiscoverer()

	require.NoError(t, d.cl.Create(ctx, &v1alpha1.BlockDeviceFilter{
		ObjectMeta: metav1.ObjectMeta{Name: "hide-large-devices"},
		Spec: v1alpha1.BlockDeviceFilterSpec{
			BlockDeviceSelector: &metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{
					{Key: v1alpha1.BlockDeviceSizeLabelKey, Operator: metav1.LabelSelectorOpNotIn, Values: []string{"20Gi"}},
				},
			},
		},
	}))

	// A LUN grown on the array: the same device, under the same name, with a
	// size the filter hides.
	device := internal.Device{Name: "/dev/sdb", KName: "/dev/sdb", Type: "disk", Serial: "serial-b", Size: resource.MustParse("10Gi")}
	d.sdsCache.StoreDevices([]internal.Device{device}, bytes.Buffer{})
	_, err := d.blockDeviceReconcile(ctx)
	require.NoError(t, err)
	require.Len(t, blockDevicesByPath(t, d), 1)

	device.Size = resource.MustParse("20Gi")
	d.sdsCache.StoreDevices([]internal.Device{device}, bytes.Buffer{})
	_, err = d.blockDeviceReconcile(ctx)
	require.NoError(t, err)

	assert.Empty(t, blockDevicesByPath(t, d))
}

func blockDevicesByPath(t *testing.T, d *Discoverer) map[string]v1alpha1.BlockDevice {
	t.Helper()

	var list v1alpha1.BlockDeviceList
	require.NoError(t, d.cl.List(context.Background(), &list))

	byPath := make(map[string]v1alpha1.BlockDevice, len(list.Items))
	for _, bd := range list.Items {
		byPath[bd.Status.Path] = bd
	}
	return byPath
}
