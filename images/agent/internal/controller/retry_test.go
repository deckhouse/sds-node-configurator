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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deckhouse/sds-node-configurator/images/agent/internal/logger"
)

// retryInterval is short enough to keep the tests instant and long enough that a
// chain which is supposed to be sleeping has not woken up by the time an
// assertion looks at it.
const retryInterval = 5 * time.Millisecond

func requeue() Result { return Result{RequeueAfter: retryInterval} }

// A chain ends when the reconcile it drives stops asking to be requeued.
func TestRetryReconcile_StopsWhenTheReconcileStopsRequeueing(t *testing.T) {
	var calls atomic.Int32

	retryReconcile(context.Background(), logger.Logger{}, "test", func(context.Context) (Result, error) {
		if calls.Add(1) < 3 {
			return requeue(), nil
		}
		return Result{}, nil
	}, requeue())

	assert.Equal(t, int32(3), calls.Load())
}

// The defect this function was extracted for. Both chains used to loop on the
// caller's Result variable, so the first one to come back satisfied ended every
// chain in flight — including one still waiting on a Physical Volume that had no
// BlockDevice yet. Each chain has to reach its own conclusion.
func TestRetryReconcile_OneChainFinishingDoesNotEndAnother(t *testing.T) {
	var (
		wg       sync.WaitGroup
		unfinish atomic.Int32
	)

	// The chain that finishes at once.
	wg.Add(1)
	go func() {
		defer wg.Done()
		retryReconcile(context.Background(), logger.Logger{}, "settles", func(context.Context) (Result, error) {
			return Result{}, nil
		}, requeue())
	}()

	// The chain that must keep going regardless, until it has had its own say.
	const wanted = 5
	done := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		retryReconcile(context.Background(), logger.Logger{}, "keeps waiting", func(context.Context) (Result, error) {
			if unfinish.Add(1) < wanted {
				return requeue(), nil
			}
			close(done)
			return Result{}, nil
		}, requeue())
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("the second chain stopped after %d attempts instead of %d", unfinish.Load(), wanted)
	}

	wg.Wait()
	assert.Equal(t, int32(wanted), unfinish.Load())
}

// A chain outlives the pass that started it, so shutdown has to reach it — before
// it runs another round of host commands, not after.
func TestRetryReconcile_StopsOnContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var calls atomic.Int32

	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		retryReconcile(ctx, logger.Logger{}, "test", func(context.Context) (Result, error) {
			calls.Add(1)
			return requeue(), nil
		}, requeue())
	}()

	cancel()

	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("the chain ignored a cancelled context")
	}
}

// An error ends the chain and is reported. It used to be assigned to a variable
// shared with the other controller's chain and read by nobody, so a retry that
// could not run at all looked exactly like one that succeeded.
func TestRetryReconcile_StopsOnError(t *testing.T) {
	var calls atomic.Int32
	wantErr := errors.New("host command failed")

	retryReconcile(context.Background(), logger.Logger{}, "test", func(context.Context) (Result, error) {
		calls.Add(1)
		return requeue(), wantErr
	}, requeue())

	require.Equal(t, int32(1), calls.Load(), "the chain must not keep calling a reconcile that errors")
}

// A Result that asks for nothing must not start a chain at all — the caller only
// spawns one when RequeueAfter is positive, and the loop has to agree.
func TestRetryReconcile_DoesNothingWithoutARequeue(t *testing.T) {
	var calls atomic.Int32

	retryReconcile(context.Background(), logger.Logger{}, "test", func(context.Context) (Result, error) {
		calls.Add(1)
		return Result{}, nil
	}, Result{})

	assert.Zero(t, calls.Load())
}

// One chain per controller, however many passes ask for one.
//
// The LVMVolumeGroup discoverer asks to be requeued for as long as a Physical
// Volume waits for its BlockDevice — a state, not an event — while the main loop
// calls the reconcile on every udev event, so a burst used to leave a chain behind
// per event. They all drive the same reconcile, so the extra ones buy nothing; what
// they cost is a full discovery pass each, concurrently, and a retry budget counted
// in passes that they spend that much faster.
func TestStartRetry_AdmitsOneChainPerController(t *testing.T) {
	var (
		gate    retryGate
		started atomic.Int32
		release = make(chan struct{})
	)

	reconcile := func(context.Context) (Result, error) {
		started.Add(1)
		<-release
		return Result{}, nil
	}

	for range 8 {
		startRetry(context.Background(), logger.Logger{}, &gate, "test", reconcile, requeue())
	}

	require.Eventually(t, func() bool { return started.Load() == 1 }, time.Second, time.Millisecond,
		"the first chain has to reach the reconcile")
	assert.Equal(t, int32(1), started.Load(), "seven of the eight requests must not have started a chain of their own")

	close(release)

	// And the gate reopens once the chain is done, so a later pass can still get one.
	require.Eventually(t, func() bool { return !gateRunning(&gate) }, time.Second, time.Millisecond,
		"the gate has to reopen when the chain ends")
}

// A held request must not leave the gate closed behind it: the chain that did get
// in owns the flag, and only it may clear it.
func TestStartRetry_ReopensTheGateAfterTheChainEnds(t *testing.T) {
	var (
		gate  retryGate
		calls atomic.Int32
	)

	reconcile := func(context.Context) (Result, error) {
		calls.Add(1)
		return Result{}, nil
	}

	startRetry(context.Background(), logger.Logger{}, &gate, "test", reconcile, requeue())
	require.Eventually(t, func() bool { return !gateRunning(&gate) }, time.Second, time.Millisecond)

	startRetry(context.Background(), logger.Logger{}, &gate, "test", reconcile, requeue())
	require.Eventually(t, func() bool { return calls.Load() == 2 }, time.Second, time.Millisecond,
		"the second pass gets a chain of its own once the first one is gone")
}

func gateRunning(g *retryGate) bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	return g.running
}

// The lost wakeup. Between the moment a chain's last attempt stops asking to be
// requeued and the moment the gate reopens, the gate still says "running" while
// nothing is going to run again. A pass that asked for a requeue in that window
// used to be refused on behalf of a chain that no longer existed, and since the
// scanner's timer is never rearmed after its first firing, the next udev event was
// the only thing left — which a node whose devices have settled does not raise.
//
// Goetz et al., Java Concurrency in Practice, §14.2.3 "Missed signals".
func TestStartRetry_HonoursARequestThatArrivesAsTheChainIsEnding(t *testing.T) {
	var (
		gate    retryGate
		calls   atomic.Int32
		ending  = make(chan struct{})
		release = make(chan struct{})
	)

	reconcile := func(context.Context) (Result, error) {
		// The first attempt stops asking, so the chain is about to leave its loop.
		// Hold it inside the reconcile while a new request lands on the gate.
		if calls.Add(1) == 1 {
			close(ending)
			<-release
		}

		return Result{}, nil
	}

	startRetry(context.Background(), logger.Logger{}, &gate, "test", reconcile, requeue())

	<-ending
	require.True(t, gateRunning(&gate), "the chain is still inside its last attempt")

	startRetry(context.Background(), logger.Logger{}, &gate, "test", reconcile, requeue())
	close(release)

	require.Eventually(t, func() bool { return calls.Load() == 2 }, time.Second, time.Millisecond,
		"the request that landed while the chain was ending has to be honoured, not dropped")
	require.Eventually(t, func() bool { return !gateRunning(&gate) }, time.Second, time.Millisecond,
		"and the gate still reopens once there is nothing left to do")
}

// Requests that pile up while a chain runs all ask for the same pass, so what
// differs between them is only how long they are willing to wait. The chain comes
// back on the soonest of them.
func TestRetryGate_CollapsesHeldRequestsIntoTheSoonest(t *testing.T) {
	var gate retryGate

	require.True(t, gate.admit(Result{RequeueAfter: time.Minute}),
		"the first request starts the chain")

	assert.False(t, gate.admit(Result{RequeueAfter: time.Hour}))
	assert.False(t, gate.admit(Result{RequeueAfter: time.Second}))
	assert.False(t, gate.admit(Result{RequeueAfter: time.Minute}))

	next, ok := gate.next(context.Background())
	require.True(t, ok)
	assert.Equal(t, time.Second, next.RequeueAfter)

	_, ok = gate.next(context.Background())
	assert.False(t, ok, "the held request is consumed once")
	assert.False(t, gateRunning(&gate), "and the gate reopens")
}

// A cancelled context ends the chain whatever is held: the request is for a pass
// that is not going to happen.
func TestRetryGate_ACancelledContextEndsTheChain(t *testing.T) {
	var gate retryGate

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.True(t, gate.admit(requeue()))
	assert.False(t, gate.admit(requeue()), "the second request is held")

	_, ok := gate.next(ctx)
	assert.False(t, ok)
	assert.False(t, gateRunning(&gate))
}

// The wrapper answers with no requeue of its own, and that is its point rather
// than an omission: a caller driven by controller-runtime — the BlockDeviceFilter
// reconciler — puts a non-empty Result back into its own work queue, which is a
// second retry chain running full discovery passes beside the first. The requeue
// is not dropped, it is handed to the one chain this wrapper owns.
func TestWithSingleRetryChain_AnswersWithNoRequeueOfItsOwn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	var calls atomic.Int32
	discover := WithSingleRetryChain(ctx, logger.Logger{}, DiscoveryPassName,
		func(context.Context) (Result, error) {
			calls.Add(1)
			return requeue(), nil
		})

	res, err := discover(ctx)

	require.NoError(t, err)
	assert.Zero(t, res.RequeueAfter, "the chain has the requeue, the caller must not queue one too")
	assert.Eventually(t, func() bool { return calls.Load() > 1 }, time.Second, retryInterval,
		"and the chain does come back, or an operator widening a BlockDeviceFilter is never noticed")
}

// An error still reaches the caller: it is what the caller logs, and for a
// controller-runtime one it is what triggers that controller's own backoff.
func TestWithSingleRetryChain_PassesTheErrorThrough(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	failed := errors.New("the pass failed")
	discover := WithSingleRetryChain(ctx, logger.Logger{}, DiscoveryPassName,
		func(context.Context) (Result, error) { return Result{}, failed })

	_, err := discover(ctx)

	assert.ErrorIs(t, err, failed)
}

// Every caller shares the gate, so a pass run from one of them while the other's
// chain is already going does not start a second chain. This is the invariant
// lvg.maxUnnamedPVPasses is argued from, and before the wrapper existed it held
// only for the scanner.
func TestWithSingleRetryChain_SharesOneChainAcrossCallers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	var (
		started = make(chan struct{})
		release = make(chan struct{})
		inside  atomic.Int32
		peak    atomic.Int32
		once    sync.Once
	)

	discover := WithSingleRetryChain(ctx, logger.Logger{}, DiscoveryPassName,
		func(context.Context) (Result, error) {
			// Compare-and-swap rather than load-then-store: two passes are inside
			// this function at once by construction, and a plain store lets the
			// slower of them write its lower value over the higher one — which
			// would hide exactly the extra chain this test exists to catch.
			for n := inside.Add(1); ; {
				high := peak.Load()
				if n <= high || peak.CompareAndSwap(high, n) {
					break
				}
			}
			defer inside.Add(-1)

			once.Do(func() { close(started) })
			<-release

			return requeue(), nil
		})

	// The scanner's pass: it takes the gate and its chain keeps asking to come back.
	go func() { _, _ = discover(ctx) }()
	<-started

	// The BlockDeviceFilter reconciler's pass, while that chain is running.
	go func() { _, _ = discover(ctx) }()

	time.Sleep(10 * retryInterval)
	close(release)
	cancel()

	assert.LessOrEqual(t, peak.Load(), int32(2),
		"the two callers' own passes may overlap, but neither may leave a chain of its own behind")
}
