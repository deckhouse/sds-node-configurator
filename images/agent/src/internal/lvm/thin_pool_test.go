/*
Copyright 2023 Flant JSC

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

package lvm

import (
	"testing"
)

func TestGetPrivateThinPoolDevice(t *testing.T) {
	var tests = []struct {
		group    string
		pool     string
		suffix   string
		expected string
	}{
		{"vg1", "tp1", "-tdata", "/dev/mapper/vg1-tp1-tdata"},
		{"vg-1", "tp-1", "_mdata", "/dev/mapper/vg--1-tp--1_mdata"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			pool := newVolumeGroup(tt.group).newLogicalVolume(tt.pool)
			path, err := pool.DeviceMapperPath()

			if err != nil {
				t.Errorf("device mapper path: %s", err)
			}

			res := path + tt.suffix
			if res != tt.expected {
				t.Errorf("got %s, want %s", res, tt.expected)
			}
		})
	}
}
