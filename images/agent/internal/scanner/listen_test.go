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

package scanner

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pilebones/go-udev/netlink"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/config"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/controller"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/throttler"
)

type listenRun struct {
	events chan netlink.UEvent
	quit   chan struct{}
	done   chan error
	fills  atomic.Int32
	passes atomic.Int32
}

func startListen(t *testing.T, rescan time.Duration, fill func(n int32) error) *listenRun {
	t.Helper()
	r := &listenRun{
		events: make(chan netlink.UEvent),
		quit:   make(chan struct{}),
		done:   make(chan error, 1),
	}
	s := &scanner{debounce: 10 * time.Millisecond}
	cfg := config.Config{RescanInterval: rescan}
	go func() {
		r.done <- s.listen(
			context.Background(),
			logger.NewNop(),
			cfg,
			throttler.New(time.Hour),
			r.events,
			make(chan error),
			r.quit,
			func() chan struct{} { return r.quit },
			func() error { return fill(r.fills.Add(1)) },
			func(context.Context) (controller.Result, error) {
				r.passes.Add(1)
				return controller.Result{}, nil
			},
		)
	}()
	return r
}

func (r *listenRun) stop(t *testing.T) {
	t.Helper()
	close(r.quit)
	select {
	case err := <-r.done:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("listen did not return after quit")
	}
}

func TestListen_RescansTheNodeWithoutUdevEvents(t *testing.T) {
	r := startListen(t, 50*time.Millisecond, func(int32) error { return nil })

	require.Eventually(t, func() bool { return r.passes.Load() >= 4 }, 5*time.Second, 5*time.Millisecond,
		"thin pool usage changes without a udev event, so the node has to be re-scanned on its own")

	r.stop(t)
	assert.Equal(t, r.fills.Load(), r.passes.Load())
}

func TestListen_RetriesAFailedRescanOnTheNextPeriod(t *testing.T) {
	r := startListen(t, 50*time.Millisecond, func(n int32) error {
		if n == 2 {
			return errors.New("lvs failed")
		}
		return nil
	})

	require.Eventually(t, func() bool { return r.passes.Load() >= 3 }, 5*time.Second, 5*time.Millisecond)

	r.stop(t)
	assert.Equal(t, r.fills.Load()-1, r.passes.Load())
}

func TestListen_AnEventDrivenPassPostponesTheRescan(t *testing.T) {
	start := time.Now()
	r := startListen(t, 300*time.Millisecond, func(int32) error { return nil })

	require.Eventually(t, func() bool { return r.passes.Load() == 1 }, time.Second, time.Millisecond)

	time.Sleep(time.Until(start.Add(200 * time.Millisecond)))
	r.events <- netlink.UEvent{Action: netlink.CHANGE, Env: map[string]string{"DEVNAME": "sda"}}

	require.Eventually(t, func() bool { return r.passes.Load() == 3 }, time.Second, time.Millisecond)
	time.Sleep(time.Until(start.Add(450 * time.Millisecond)))
	assert.Equal(t, int32(3), r.passes.Load(), "the rescan is due 300ms after the last pass, not after the start")

	require.Eventually(t, func() bool { return r.passes.Load() == 4 }, time.Second, time.Millisecond)
	r.stop(t)
}
