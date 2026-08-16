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

package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func discoverer(name string, ran *[]string, res Result, err error) func(context.Context) (Result, error) {
	return func(context.Context) (Result, error) {
		*ran = append(*ran, name)
		return res, err
	}
}

// The order is the whole reason this function exists: the LVMVolumeGroup
// discoverer reads BlockDevice status the block-device one writes.
func TestDiscoverInOrder_RunsThemInOrder(t *testing.T) {
	var ran []string

	pass := DiscoverInOrder(
		discoverer("block-devices", &ran, Result{}, nil),
		discoverer("volume-groups", &ran, Result{}, nil),
	)

	res, err := pass(context.Background())

	require.NoError(t, err)
	assert.Equal(t, Result{}, res, "nobody asked for a requeue")
	assert.Equal(t, []string{"block-devices", "volume-groups"}, ran)
}

// The defect this closes: a path that ran only the block-device discoverer
// created BlockDevices with nothing left to notice them, and an LVMVolumeGroup
// past its unnamed-PV budget kept reporting a Physical Volume as missing until
// the agent restarted.
func TestDiscoverInOrder_RunsEveryDiscovererEvenWhenTheFirstIsQuiet(t *testing.T) {
	var ran []string

	pass := DiscoverInOrder(
		discoverer("block-devices", &ran, Result{}, nil),
		discoverer("volume-groups", &ran, Result{RequeueAfter: time.Second}, nil),
	)

	res, err := pass(context.Background())

	require.NoError(t, err)
	assert.Len(t, ran, 2, "a quiet first discoverer must not end the pass")
	assert.Equal(t, time.Second, res.RequeueAfter,
		"the pass carries the requeue of whichever discoverer asked")
}

func TestDiscoverInOrder_AsksForTheSoonestRequeue(t *testing.T) {
	var ran []string

	pass := DiscoverInOrder(
		discoverer("block-devices", &ran, Result{RequeueAfter: 30 * time.Second}, nil),
		discoverer("volume-groups", &ran, Result{RequeueAfter: 5 * time.Second}, nil),
	)

	res, err := pass(context.Background())

	require.NoError(t, err)
	assert.Equal(t, 5*time.Second, res.RequeueAfter)
}

// A discoverer that already ran keeps its requeue: it is a request about the
// node, not about the pass, and the failure below makes coming back more
// necessary rather than less.
func TestDiscoverInOrder_StopsAtTheFirstErrorAndKeepsTheRequeueBeforeIt(t *testing.T) {
	var (
		ran     []string
		boom    = errors.New("boom")
		wantErr = boom
	)

	pass := DiscoverInOrder(
		discoverer("block-devices", &ran, Result{RequeueAfter: 5 * time.Second}, nil),
		discoverer("volume-groups", &ran, Result{}, boom),
		discoverer("never-reached", &ran, Result{}, nil),
	)

	res, err := pass(context.Background())

	require.ErrorIs(t, err, wantErr)
	assert.Equal(t, []string{"block-devices", "volume-groups"}, ran,
		"a failed discoverer ends the pass")
	assert.Equal(t, 5*time.Second, res.RequeueAfter)
}

func TestSoonestRequeue(t *testing.T) {
	tests := []struct {
		name    string
		results []Result
		want    time.Duration
	}{
		{
			name: "nothing at all",
		},
		{
			name:    "nobody is asking",
			results: []Result{{}, {}},
		},
		{
			name:    "a zero never wins over a request",
			results: []Result{{}, {RequeueAfter: time.Second}},
			want:    time.Second,
		},
		{
			name:    "the sooner of two",
			results: []Result{{RequeueAfter: time.Minute}, {RequeueAfter: time.Second}},
			want:    time.Second,
		},
		{
			name:    "order does not matter",
			results: []Result{{RequeueAfter: time.Second}, {RequeueAfter: time.Minute}},
			want:    time.Second,
		},
		{
			name:    "a negative duration is not a request",
			results: []Result{{RequeueAfter: -time.Second}, {RequeueAfter: time.Minute}},
			want:    time.Minute,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, SoonestRequeue(tt.results...).RequeueAfter)
		})
	}
}
