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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
)

const gcNode = "test-node"

func missingBD(name string, status metav1.ConditionStatus, reason string, since time.Time) v1alpha1.BlockDevice {
	return v1alpha1.BlockDevice{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: v1alpha1.BlockDeviceStatus{
			NodeName: gcNode,
			Path:     "/dev/" + name,
			Conditions: []metav1.Condition{{
				Type:               v1alpha1.BlockDeviceConditionMissing,
				Status:             status,
				Reason:             reason,
				LastTransitionTime: metav1.NewTime(since),
			}},
		},
	}
}

func TestShouldCollectMissingBlockDevice(t *testing.T) {
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	const retention = 24 * time.Hour
	old := now.Add(-retention - time.Minute)
	fresh := now.Add(-retention + time.Minute)

	lvgs := []v1alpha1.LVMVolumeGroup{{
		ObjectMeta: metav1.ObjectMeta{Name: "lvg-1"},
		Status: v1alpha1.LVMVolumeGroupStatus{
			Nodes: []v1alpha1.LVMVolumeGroupNode{{
				Name:    gcNode,
				Devices: []v1alpha1.LVMVolumeGroupDevice{{BlockDevice: "in-lvg-status"}},
			}},
		},
	}}
	refs := newLVGReferences(lvgs)

	tests := []struct {
		name      string
		bd        func() v1alpha1.BlockDevice
		present   bool
		retention time.Duration
		want      bool
	}{
		{
			name: "Missing=True over the retention and unreferenced is deleted",
			bd: func() v1alpha1.BlockDevice {
				return missingBD("gone", metav1.ConditionTrue, v1alpha1.ReasonDeviceNotFound, old)
			},
			want: true,
		},
		{
			name: "exactly at the retention is deleted",
			bd: func() v1alpha1.BlockDevice {
				return missingBD("gone", metav1.ConditionTrue, v1alpha1.ReasonDeviceNotFound, now.Add(-retention))
			},
			want: true,
		},
		{
			name: "Missing=True under the retention is kept",
			bd: func() v1alpha1.BlockDevice {
				return missingBD("gone", metav1.ConditionTrue, v1alpha1.ReasonDeviceNotFound, fresh)
			},
		},
		{
			name: "Missing=Unknown, the grace period, is kept however old",
			bd: func() v1alpha1.BlockDevice {
				return missingBD("gone", metav1.ConditionUnknown, v1alpha1.ReasonDeviceNotFound, old)
			},
		},
		{
			name: "no Missing condition is kept",
			bd: func() v1alpha1.BlockDevice {
				bd := missingBD("gone", metav1.ConditionTrue, v1alpha1.ReasonDeviceNotFound, old)
				bd.Status.Conditions = nil
				return bd
			},
		},
		{
			name: "listed in an LVMVolumeGroup's status.nodes is kept",
			bd: func() v1alpha1.BlockDevice {
				return missingBD("in-lvg-status", metav1.ConditionTrue, v1alpha1.ReasonDeviceNotFound, old)
			},
		},
		{
			name: "naming an existing LVMVolumeGroup is kept: a lost Physical Volume drops out of status.nodes",
			bd: func() v1alpha1.BlockDevice {
				bd := missingBD("lost-pv", metav1.ConditionTrue, v1alpha1.ReasonDeviceNotFound, old)
				bd.Status.LVMVolumeGroupName = "lvg-1"
				return bd
			},
		},
		{
			name: "naming an LVMVolumeGroup this cluster does not have is deleted: a nested cluster's tag",
			bd: func() v1alpha1.BlockDevice {
				bd := missingBD("guest-pv", metav1.ConditionTrue, v1alpha1.ReasonDeviceNotFound, old)
				bd.Status.LVMVolumeGroupName = "guest-lvg"
				return bd
			},
			want: true,
		},
		{
			name: "Consumable is kept: shouldDeleteBlockDevice owns it",
			bd: func() v1alpha1.BlockDevice {
				bd := missingBD("gone", metav1.ConditionTrue, v1alpha1.ReasonDeviceNotFound, old)
				bd.Status.Consumable = true
				return bd
			},
		},
		{
			name: "another node's is kept",
			bd: func() v1alpha1.BlockDevice {
				bd := missingBD("gone", metav1.ConditionTrue, v1alpha1.ReasonDeviceNotFound, old)
				bd.Status.NodeName = "other-node"
				return bd
			},
		},
		{
			name: "seen in the current scan is kept: the stale condition is being cleared",
			bd: func() v1alpha1.BlockDevice {
				return missingBD("gone", metav1.ConditionTrue, v1alpha1.ReasonDeviceNotFound, old)
			},
			present: true,
		},
		{
			name: "retention zero disables the deletion",
			bd: func() v1alpha1.BlockDevice {
				return missingBD("gone", metav1.ConditionTrue, v1alpha1.ReasonDeviceNotFound, old)
			},
			retention: -1,
		},
		{
			// A published device is filtered out before it becomes a candidate and
			// goes Missing under the same reason; any other reason gets the same
			// retention, so the rule does not depend on the reason's spelling.
			name: "another reason follows the same retention",
			bd: func() v1alpha1.BlockDevice {
				return missingBD("gone", metav1.ConditionTrue, "DevicePublishedToWorkload", old)
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bd := tt.bd()
			candidates := map[string]struct{}{}
			if tt.present {
				candidates[bd.Name] = struct{}{}
			}
			r := retention
			if tt.retention < 0 {
				r = 0
			}
			assert.Equal(t, tt.want, shouldCollectMissingBlockDevice(bd, candidates, gcNode, refs, now, r))
		})
	}
}

func TestCollectMissingBlockDevices(t *testing.T) {
	ctx := context.Background()
	d := setupDiscoverer()
	d.cfg.MissingBlockDeviceRetention = time.Hour
	old := time.Now().Add(-2 * time.Hour)

	lvg := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "lvg-1"},
		Status: v1alpha1.LVMVolumeGroupStatus{
			Nodes: []v1alpha1.LVMVolumeGroupNode{{
				Name:    gcNode,
				Devices: []v1alpha1.LVMVolumeGroupDevice{{BlockDevice: "kept-in-lvg"}},
			}},
		},
	}
	require.NoError(t, d.cl.Create(ctx, lvg))

	devices := []v1alpha1.BlockDevice{
		missingBD("collected", metav1.ConditionTrue, v1alpha1.ReasonDeviceNotFound, old),
		missingBD("kept-in-lvg", metav1.ConditionTrue, v1alpha1.ReasonDeviceNotFound, old),
		missingBD("kept-fresh", metav1.ConditionTrue, v1alpha1.ReasonDeviceNotFound, time.Now()),
	}
	api := make(map[string]v1alpha1.BlockDevice, len(devices))
	for i := range devices {
		require.NoError(t, d.cl.Create(ctx, &devices[i]))
		api[devices[i].Name] = devices[i]
	}

	d.collectMissingBlockDevices(ctx, map[string]struct{}{}, api)

	err := d.cl.Get(ctx, client.ObjectKey{Name: "collected"}, &v1alpha1.BlockDevice{})
	assert.True(t, apierrors.IsNotFound(err), "expired and unreferenced BlockDevice must be deleted, got %v", err)
	assert.NotContains(t, api, "collected")

	for _, name := range []string{"kept-in-lvg", "kept-fresh"} {
		assert.NoError(t, d.cl.Get(ctx, client.ObjectKey{Name: name}, &v1alpha1.BlockDevice{}), name)
		assert.Contains(t, api, name)
	}

	assert.Equal(t, 2.0, missingGauge(t, gcNode, v1alpha1.ReasonDeviceNotFound))
}

func missingGauge(t *testing.T, node, reason string) float64 {
	t.Helper()
	families, err := metrics.Registry.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() != "sds_node_configurator_block_devices_missing" {
			continue
		}
		for _, m := range f.GetMetric() {
			labels := map[string]string{}
			for _, l := range m.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}
			if labels["node"] == node && labels["reason"] == reason {
				return m.GetGauge().GetValue()
			}
		}
	}
	t.Fatalf("no block_devices_missing series for node %q reason %q", node, reason)
	return 0
}
