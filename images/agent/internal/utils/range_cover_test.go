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
	"slices"
	"testing"
)

func TestMergeEmpty(t *testing.T) {
	result, err := RangeCover{}.Merged()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if len(result) != 0 {
		t.Errorf("expected empty range, got %v", result)
	}
}

func TestMergeOverlapped(t *testing.T) {
	_, err := RangeCover{
		Range{Start: 0, Count: 2},
		Range{Start: 1, Count: 1},
	}.Merged()
	if err == nil {
		t.Errorf("unexpected success: %v", err)
	}

	if err.Error() != "self overlapped range" {
		t.Errorf("expected error %v", err)
	}
}

func TestMergeNoMerge(t *testing.T) {
	original := RangeCover{
		Range{Start: 0, Count: 2},
		Range{Start: 7, Count: 1},
	}
	result, err := original.Merged()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if !slices.Equal(original, result) {
		t.Errorf("expected %v, got %v", original, result)
	}
}

func TestMergeMerge(t *testing.T) {
	original := RangeCover{
		Range{Start: 0, Count: 2},
		Range{Start: 2, Count: 1},
	}
	expected := RangeCover{
		Range{Start: 0, Count: 3},
	}
	result, err := original.Merged()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if !slices.Equal(expected, result) {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestMultiply(t *testing.T) {
	multiplier := int64(4)
	original := RangeCover{
		Range{Start: 0, Count: 2},
		Range{Start: 5, Count: 1},
	}
	expected := RangeCover{
		Range{Start: 0, Count: 8},
		Range{Start: 20, Count: 4},
	}

	result := original.Multiplied(multiplier)

	if !slices.Equal(result, expected) {
		t.Errorf("expected %v, got %v", expected, result)
	}
}
