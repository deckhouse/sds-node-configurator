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

package cache

import (
	"bytes"
	"maps"
	"slices"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
)

// TestCacheConcurrentAccess runs the scanner's writes against the readers of
// the controllers and the agent's own LV bookkeeping. It finds nothing without
// the race detector: run it with go test -race, under which every unguarded
// access fails it.
func TestCacheConcurrentAccess(t *testing.T) {
	c := New()
	log, err := logger.NewLoggerToWriter(&bytes.Buffer{}, logger.TraceLevel)
	require.NoError(t, err)

	const rounds = 100
	var wg sync.WaitGroup
	scan := func() {
		defer wg.Done()
		for range rounds {
			c.StoreDevices([]internal.Device{{Name: "sda"}}, bytes.Buffer{})
			c.StorePVs([]internal.PVData{{PVName: "/dev/sda", VGName: "vg"}}, bytes.Buffer{})
			c.StoreVGs([]internal.VGData{{VGName: "vg"}}, bytes.Buffer{})
			c.StoreLVs([]internal.LVData{{VGName: "vg", LVName: "lv"}}, bytes.Buffer{})
			c.StoreManagedVGs(slices.Values([]string{"vg"}))
			c.PrintTheCache(log)
		}
	}
	read := func() {
		defer wg.Done()
		for range rounds {
			_, _ = c.GetDevices()
			_, _ = c.GetPVs()
			_, _ = c.GetVGs()
			_, _ = c.GetLVs()
			_ = c.FindVG("vg")
			if lv := c.FindLV("vg", "new"); lv != nil {
				_ = lv.Exist
			}
			_ = maps.Clone(c.GetManagedVGs())
		}
	}
	bookkeep := func() {
		defer wg.Done()
		for range rounds {
			c.AddLV("vg", "new")
			c.MarkLVAsRemoved("vg", "new")
		}
	}

	wg.Add(3)
	go scan()
	go read()
	go bookkeep()
	wg.Wait()
}

func TestCacheMarkLVAsRemovedOfAnLVNoLongerCached(t *testing.T) {
	c := New()
	c.AddLV("vg", "lv")
	// The LV was not found by the scans after its lvremove, so StoreLVs dropped it.
	c.StoreLVs(nil, bytes.Buffer{})
	c.StoreLVs(nil, bytes.Buffer{})
	require.Nil(t, c.FindLV("vg", "lv"))

	assert.NotPanics(t, func() { c.MarkLVAsRemoved("vg", "lv") })
	assert.Nil(t, c.FindLV("vg", "lv"))
}

func TestCacheFindLVReturnsACopy(t *testing.T) {
	c := New()
	c.StoreLVs([]internal.LVData{{VGName: "vg", LVName: "lv"}}, bytes.Buffer{})

	found := c.FindLV("vg", "lv")
	require.NotNil(t, found)
	require.True(t, found.Exist)

	c.MarkLVAsRemoved("vg", "lv")

	assert.True(t, found.Exist, "an LV a caller already holds must not change under it")
	assert.False(t, c.FindLV("vg", "lv").Exist)
}
