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
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-common-lib/conditions"
	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
)

func TestBlockDeviceReconcile_ReusedLegacyBlockDeviceIsNotMarkedMissing(t *testing.T) {
	ctx := context.Background()
	d := setupDiscoverer()

	device := internal.Device{
		Name:          "/dev/sdb",
		KName:         "/dev/sdb",
		Type:          "disk",
		Serial:        "serial-b",
		PartTableType: "gpt",
		Size:          resource.MustParse("10Gi"),
	}
	legacy := &v1alpha1.BlockDevice{
		ObjectMeta: metav1.ObjectMeta{Name: "dev-legacy-name"},
		Status: v1alpha1.BlockDeviceStatus{
			NodeName: d.cfg.NodeName,
			Path:     "/dev/sdb",
			Type:     "disk",
			Serial:   "serial-b",
			Size:     resource.MustParse("10Gi"),
		},
	}
	require.NoError(t, d.cl.Create(ctx, legacy))

	d.sdsCache.StoreDevices([]internal.Device{device}, bytes.Buffer{})
	for pass := 1; pass <= 3; pass++ {
		_, err := d.blockDeviceReconcile(ctx)
		require.NoError(t, err)

		var list v1alpha1.BlockDeviceList
		require.NoError(t, d.cl.List(ctx, &list))
		require.Len(t, list.Items, 1, "pass %d", pass)
		assert.Equal(t, "dev-legacy-name", list.Items[0].Name, "pass %d", pass)
		assert.Nil(t, conditions.Get(list.Items[0].Status.Conditions, v1alpha1.BlockDeviceConditionMissing), "pass %d", pass)
	}

	var got v1alpha1.BlockDevice
	require.NoError(t, d.cl.Get(ctx, client.ObjectKey{Name: "dev-legacy-name"}, &got))
}
