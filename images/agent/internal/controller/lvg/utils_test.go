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

package lvg

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
)

func TestFindDuplicateVGNames(t *testing.T) {
	t.Run("no_duplicates_returns_empty_map", func(t *testing.T) {
		vgs := []internal.VGData{
			{VGName: "vg-a", VGUUID: "uuid-a"},
			{VGName: "vg-b", VGUUID: "uuid-b"},
			{VGName: "vg-c", VGUUID: "uuid-c"},
		}
		got := findDuplicateVGNames(vgs)
		assert.Empty(t, got)
	})

	t.Run("single_duplicate_pair_is_detected", func(t *testing.T) {
		vgs := []internal.VGData{
			{VGName: "vg-thin-data", VGUUID: "uuid-1"},
			{VGName: "vg-thin-data", VGUUID: "uuid-2"},
			{VGName: "vg-other", VGUUID: "uuid-x"},
		}
		got := findDuplicateVGNames(vgs)
		assert.Len(t, got, 1)
		assert.Equal(t, []string{"uuid-1", "uuid-2"}, got["vg-thin-data"])
	})

	t.Run("multiple_duplicate_groups_are_detected", func(t *testing.T) {
		vgs := []internal.VGData{
			{VGName: "vg-thin-data", VGUUID: "uuid-1"},
			{VGName: "vg-thin-data", VGUUID: "uuid-2"},
			{VGName: "vg-1", VGUUID: "uuid-3"},
			{VGName: "vg-1", VGUUID: "uuid-4"},
			{VGName: "vg-unique", VGUUID: "uuid-5"},
		}
		got := findDuplicateVGNames(vgs)
		assert.Len(t, got, 2)
		assert.Equal(t, []string{"uuid-1", "uuid-2"}, got["vg-thin-data"])
		assert.Equal(t, []string{"uuid-3", "uuid-4"}, got["vg-1"])
		_, hasUnique := got["vg-unique"]
		assert.False(t, hasUnique)
	})

	t.Run("triple_duplicate_returns_all_uuids_sorted", func(t *testing.T) {
		vgs := []internal.VGData{
			{VGName: "vg-x", VGUUID: "ccc"},
			{VGName: "vg-x", VGUUID: "aaa"},
			{VGName: "vg-x", VGUUID: "bbb"},
		}
		got := findDuplicateVGNames(vgs)
		assert.Equal(t, []string{"aaa", "bbb", "ccc"}, got["vg-x"])
	})

	t.Run("same_uuid_listed_twice_is_not_a_duplicate", func(t *testing.T) {
		vgs := []internal.VGData{
			{VGName: "vg-a", VGUUID: "uuid-a"},
			{VGName: "vg-a", VGUUID: "uuid-a"},
		}
		got := findDuplicateVGNames(vgs)
		assert.Empty(t, got, "two records with identical UUID describe the same VG and must not be reported as a duplicate name")
	})

	t.Run("vgs_with_empty_name_are_ignored", func(t *testing.T) {
		vgs := []internal.VGData{
			{VGName: "", VGUUID: "uuid-a"},
			{VGName: "", VGUUID: "uuid-b"},
			{VGName: "vg-a", VGUUID: "uuid-c"},
		}
		got := findDuplicateVGNames(vgs)
		assert.Empty(t, got)
	})

	t.Run("empty_input_returns_empty", func(t *testing.T) {
		got := findDuplicateVGNames(nil)
		assert.Empty(t, got)
	})
}

func TestDuplicateVGMessage(t *testing.T) {
	msg := duplicateVGMessage("vg-thin-data", []string{"uuid-1", "uuid-2"})
	assert.Contains(t, msg, `"vg-thin-data"`)
	assert.Contains(t, msg, "uuid-1, uuid-2")
	assert.Contains(t, msg, "vgrename --select vg_uuid=")
	assert.True(t, strings.HasPrefix(msg, "Multiple LVM VGs share the name"))
}

func TestFilterCandidatesByDuplicateVGs(t *testing.T) {
	t.Run("no_duplicates_returns_input_unchanged", func(t *testing.T) {
		in := []internal.LVMVolumeGroupCandidate{
			{ActualVGNameOnTheNode: "vg-a"},
			{ActualVGNameOnTheNode: "vg-b"},
		}
		got := filterCandidatesByDuplicateVGs(in, nil)
		assert.Equal(t, in, got)
	})

	t.Run("drops_candidates_for_duplicated_names", func(t *testing.T) {
		in := []internal.LVMVolumeGroupCandidate{
			{ActualVGNameOnTheNode: "vg-thin-data", VGUUID: "uuid-1"},
			{ActualVGNameOnTheNode: "vg-thin-data", VGUUID: "uuid-2"},
			{ActualVGNameOnTheNode: "vg-clean", VGUUID: "uuid-3"},
		}
		dup := map[string][]string{"vg-thin-data": {"uuid-1", "uuid-2"}}
		got := filterCandidatesByDuplicateVGs(in, dup)
		assert.Len(t, got, 1)
		assert.Equal(t, "vg-clean", got[0].ActualVGNameOnTheNode)
	})

	t.Run("empty_candidates_is_a_noop", func(t *testing.T) {
		got := filterCandidatesByDuplicateVGs(nil, map[string][]string{"vg-a": {"u1", "u2"}})
		assert.Empty(t, got)
	})
}
