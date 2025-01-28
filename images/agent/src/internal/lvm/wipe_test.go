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
	"reflect"
	"slices"
	"testing"
)

var superblock = Superblock{
	DataBlockSize: 128,
	Devices: []Device{
		{
			DevId: 1,
			RangeMappings: []RangeMapping{
				{
					OriginBegin: 0,
					Length:      2},
			},
			SingleMappings: []SingleMapping{
				{OriginBlock: 7},
			},
		},
		{
			DevId: 2,
			RangeMappings: []RangeMapping{
				{
					OriginBegin: 0,
					Length:      2},
			},
			SingleMappings: []SingleMapping{
				{OriginBlock: 8},
				{OriginBlock: 9},
			},
		}}}

var expectedCoverageById = map[ThinDeviceId]RangeCover{
	ThinDeviceId(1): {
		{Start: 0, Count: 2},
		{Start: 7, Count: 1},
	},
	ThinDeviceId(2): {
		{Start: 0, Count: 2},
		{Start: 8, Count: 2},
	},
}

func TestSingleDevice(t *testing.T) {
	v, err := buildWipeMapBySuperblock(superblock, []ThinDeviceId{ThinDeviceId(1)})

	if err != nil {
		t.Errorf("building wipe map: %s", err)
		return
	}

	if len(v) != 1 {
		t.Error("unexpected length")
		return
	}

	if !slices.Equal(v[1], expectedCoverageById[1]) {
		t.Errorf("expected %v, got %v", expectedCoverageById, v[1])
	}
}
func TestMultipleDevices(t *testing.T) {
	v, err := buildWipeMapBySuperblock(superblock, []ThinDeviceId{ThinDeviceId(1), ThinDeviceId(2)})

	if err != nil {
		t.Errorf("building wipe map: %s", err)
		return
	}

	if len(v) != 2 {
		t.Error("unexpected length")
		return
	}

	if !reflect.DeepEqual(v, expectedCoverageById) {
		t.Errorf("expected %v, got %v", expectedCoverageById, v)
	}
}

func TestMissingDevice(t *testing.T) {
	_, err := buildWipeMapBySuperblock(superblock, []ThinDeviceId{ThinDeviceId(3)})

	if err == nil {
		t.Error("expected error")
		return
	}
}

type fakePool struct {
	thinPool
}

func (tp *fakePool) DumpThinPoolSuperblock() (Superblock, error) {
	return superblock, nil
}

func TestWipeMap(t *testing.T) {
	pool := fakePool{}
	input := []ThinVolume{
		&thinVolume{thinPool: &pool, thinDeviceId: 1}, &thinVolume{thinPool: &pool, thinDeviceId: 2},
	}
	expected := map[ThinVolume]RangeCover{}
	for _, v := range input {
		expected[v] = expectedCoverageById[v.ThinDeviceId()].Multiplied(superblock.DataBlockSize)
	}

	result, err := BuildWipeMap(input)

	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}

	if len(result) != len(input) {
		t.Errorf("unexpected len %d", len(result))
	}

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("expected: %v, got: %v", result, expected)
	}
}

func TestWipeMapSingleDevice(t *testing.T) {
	pool := fakePool{}
	input := []ThinVolume{
		&thinVolume{thinPool: &pool, thinDeviceId: 1},
	}

	expected := map[ThinVolume]RangeCover{}
	for _, v := range input {
		expected[v] = expectedCoverageById[v.ThinDeviceId()].Multiplied(superblock.DataBlockSize)
	}

	result, err := BuildWipeMap(input)

	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}

	if len(result) != 1 {
		t.Errorf("unexpected len %d", len(result))
	}

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("expected: %v, got: %v", result, expected)
	}
}
