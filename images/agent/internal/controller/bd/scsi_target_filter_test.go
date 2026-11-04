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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
)

const (
	declaredIQN   = "iqn.2000-01.com.example:declared"
	undeclaredIQN = "iqn.2000-01.com.example:undeclared"
)

// The filter a CSI driver that owns declaredIQN keeps: none of the WWNs it
// has discovered yet, and nothing reached through its target.
func declaredTargetFilter() *v1alpha1.BlockDeviceFilter {
	return &v1alpha1.BlockDeviceFilter{
		ObjectMeta: metav1.ObjectMeta{Name: "csi-driver-filter"},
		Spec: v1alpha1.BlockDeviceFilterSpec{
			BlockDeviceSelector: &metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{
					{Key: v1alpha1.BlockDeviceWWNLabelKey, Operator: metav1.LabelSelectorOpNotIn, Values: []string{"0x6001405000000000000000000000000a"}},
					{Key: v1alpha1.BlockDeviceSCSITargetLabelKey, Operator: metav1.LabelSelectorOpNotIn, Values: []string{v1alpha1.SCSITargetLabelValue(declaredIQN)}},
				},
			},
		},
	}
}

func scsiTargetDevices() []internal.Device {
	size := resource.MustParse("10Gi")
	return []internal.Device{
		// A LUN of the declared target the driver has not discovered yet.
		{Name: "/dev/sdb", KName: "/dev/sdb", Type: "disk", Size: size, Serial: "serial-b", Wwn: "0x6001405000000000000000000000000b"},
		{Name: "/dev/sdc", KName: "/dev/sdc", Type: "disk", Size: size, Serial: "serial-c", Wwn: "0x6001405000000000000000000000000c"},
		{Name: "/dev/vdb", KName: "/dev/vdb", Type: "disk", Size: size, Serial: "serial-v"},
	}
}

func scsiTargets(targets map[string]string, failing ...string) func(string) (string, error) {
	return func(kName string) (string, error) {
		for _, f := range failing {
			if f == kName {
				return "", errors.New("sysfs went away under the scan")
			}
		}
		return targets[kName], nil
	}
}

func TestBlockDeviceReconcile_HidesEveryLUNOfADeclaredTarget(t *testing.T) {
	ctx := context.Background()
	d := setupDiscoverer()
	d.scsiTargetOf = scsiTargets(map[string]string{"/dev/sdb": declaredIQN, "/dev/sdc": undeclaredIQN})
	require.NoError(t, d.cl.Create(ctx, declaredTargetFilter()))
	d.sdsCache.StoreDevices(scsiTargetDevices(), bytes.Buffer{})

	requeue, err := d.blockDeviceReconcile(ctx)
	require.NoError(t, err)
	assert.False(t, requeue)

	byPath := blockDevicesByPath(t, d)
	assert.NotContains(t, byPath, "/dev/sdb", "a LUN of a declared target is the driver's, whether or not it knows the WWN yet")
	if assert.Contains(t, byPath, "/dev/sdc") {
		assert.Equal(t, "iqn.2000-01.com.example_undeclared", byPath["/dev/sdc"].Labels[v1alpha1.BlockDeviceSCSITargetLabelKey])
	}
	if assert.Contains(t, byPath, "/dev/vdb") {
		assert.Equal(t, "", byPath["/dev/vdb"].Labels[v1alpha1.BlockDeviceSCSITargetLabelKey])
	}
}

// A BlockDevice published before the agent knew the target goes away once it
// does, as one hidden by WWN always has.
func TestBlockDeviceReconcile_RemovesABlockDeviceOnceItsTargetIsKnown(t *testing.T) {
	ctx := context.Background()
	d := setupDiscoverer()
	d.sdsCache.StoreDevices(scsiTargetDevices(), bytes.Buffer{})

	_, err := d.blockDeviceReconcile(ctx)
	require.NoError(t, err)
	require.Contains(t, blockDevicesByPath(t, d), "/dev/sdb")

	d.scsiTargetOf = scsiTargets(map[string]string{"/dev/sdb": declaredIQN})
	require.NoError(t, d.cl.Create(ctx, declaredTargetFilter()))

	_, err = d.blockDeviceReconcile(ctx)
	require.NoError(t, err)
	assert.NotContains(t, blockDevicesByPath(t, d), "/dev/sdb")
}

func TestBlockDeviceReconcile_WithholdsANewDeviceWhoseTargetCannotBeRead(t *testing.T) {
	ctx := context.Background()
	d := setupDiscoverer()
	d.scsiTargetOf = scsiTargets(map[string]string{"/dev/sdc": undeclaredIQN}, "/dev/sdb")
	require.NoError(t, d.cl.Create(ctx, declaredTargetFilter()))
	d.sdsCache.StoreDevices(scsiTargetDevices(), bytes.Buffer{})

	requeue, err := d.blockDeviceReconcile(ctx)
	require.NoError(t, err)
	assert.True(t, requeue, "nothing but a udev event would bring the device back otherwise")

	byPath := blockDevicesByPath(t, d)
	assert.NotContains(t, byPath, "/dev/sdb")
	assert.Contains(t, byPath, "/dev/sdc")
	assert.Contains(t, byPath, "/dev/vdb")

	d.scsiTargetOf = scsiTargets(map[string]string{"/dev/sdb": declaredIQN, "/dev/sdc": undeclaredIQN})
	requeue, err = d.blockDeviceReconcile(ctx)
	require.NoError(t, err)
	assert.False(t, requeue)
	assert.NotContains(t, blockDevicesByPath(t, d), "/dev/sdb", "and once it can, the filter hides it")
}

func TestBlockDeviceReconcile_KeepsTheTargetLabelWhenItCannotBeRead(t *testing.T) {
	ctx := context.Background()
	d := setupDiscoverer()
	d.scsiTargetOf = scsiTargets(map[string]string{"/dev/sdc": undeclaredIQN})
	d.sdsCache.StoreDevices(scsiTargetDevices(), bytes.Buffer{})

	_, err := d.blockDeviceReconcile(ctx)
	require.NoError(t, err)

	d.scsiTargetOf = scsiTargets(nil, "/dev/sdc")
	_, err = d.blockDeviceReconcile(ctx)
	require.NoError(t, err)

	byPath := blockDevicesByPath(t, d)
	if assert.Contains(t, byPath, "/dev/sdc", "a device already published is not taken away over a read error") {
		assert.Equal(t, "iqn.2000-01.com.example_undeclared", byPath["/dev/sdc"].Labels[v1alpha1.BlockDeviceSCSITargetLabelKey])
	}
}

// The order the two modules are upgraded in must not matter. A driver that
// already selects by the label, next to an agent that does not write it yet,
// hides what it hid before and nothing more: NotIn matches a BlockDevice with
// no such label at all.
func TestSCSITargetFilter_KeepsBlockDevicesOfAnAgentWithoutTheLabel(t *testing.T) {
	selector, err := metav1.LabelSelectorAsSelector(declaredTargetFilter().Spec.BlockDeviceSelector)
	require.NoError(t, err)

	assert.True(t, selector.Matches(labels.Set{v1alpha1.BlockDeviceWWNLabelKey: "0x6001405000000000000000000000000b"}))
	assert.False(t, selector.Matches(labels.Set{v1alpha1.BlockDeviceWWNLabelKey: "0x6001405000000000000000000000000a"}))
	assert.False(t, selector.Matches(labels.Set{
		v1alpha1.BlockDeviceWWNLabelKey:        "0x6001405000000000000000000000000b",
		v1alpha1.BlockDeviceSCSITargetLabelKey: v1alpha1.SCSITargetLabelValue(declaredIQN),
	}))
}
