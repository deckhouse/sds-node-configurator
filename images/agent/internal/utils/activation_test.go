/*
Copyright 2025 Flant JSC

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

package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
)

func TestIsLVActive(t *testing.T) {
	tests := []struct {
		name   string
		attr   string
		active bool
	}{
		{"active thick LV", "-wi-a-----", true},
		{"inactive thick LV", "-wi-------", false},
		{"active thin pool", "twi-a-t---", true},
		{"inactive thin pool", "twi---t---", false},
		{"active thin LV", "Vwi-a-t---", true},
		{"inactive thin LV", "Vwi---t---", false},
		{"empty attr", "", false},
		{"short attr", "-wi-", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.active, IsLVActive(tt.attr))
		})
	}
}

func TestIsLVThinPool(t *testing.T) {
	tests := []struct {
		name     string
		attr     string
		thinPool bool
	}{
		{"thin pool", "twi-a-t---", true},
		{"thick LV", "-wi-a-----", false},
		{"thin LV", "Vwi-a-t---", false},
		{"empty", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.thinPool, IsLVThinPool(tt.attr))
		})
	}
}

func TestIsLVThick(t *testing.T) {
	tests := []struct {
		name  string
		attr  string
		thick bool
	}{
		{"thick LV", "-wi-a-----", true},
		{"thin pool", "twi-a-t---", false},
		{"thin LV", "Vwi-a-t---", false},
		{"empty", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.thick, IsLVThick(tt.attr))
		})
	}
}

func TestFilterVGsByTag(t *testing.T) {
	vgs := []internal.VGData{
		{VGName: "vg-managed", VGTags: "storage.deckhouse.io/enabled=true"},
		{VGName: "vg-unmanaged", VGTags: ""},
		{VGName: "vg-legacy", VGTags: "linstor-something"},
		{VGName: "vg-other", VGTags: "some-other-tag"},
	}

	result := FilterVGsByTag(vgs, internal.LVMTags)

	assert.Len(t, result, 2)
	names := make([]string, 0, len(result))
	for _, vg := range result {
		names = append(names, vg.VGName)
	}
	assert.Contains(t, names, "vg-managed")
	assert.Contains(t, names, "vg-legacy")
}

func TestFilterVGsByTag_Empty(t *testing.T) {
	result := FilterVGsByTag(nil, internal.LVMTags)
	assert.Empty(t, result)
}
