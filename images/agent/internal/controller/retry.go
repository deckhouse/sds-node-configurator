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
	"fmt"
	"sync"
	"time"

	"github.com/deckhouse/sds-node-configurator/images/agent/internal/logger"
)

// DiscoveryPassName labels the retry chain in the log. The pass is what is
// retried, so it is the pass that is named — a single discoverer's name here
// would say that only that discoverer is coming back.
const DiscoveryPassName = "the discovery pass"

// WithSingleRetryChain wraps a discovery pass so that a requeue it asks for is
// honoured by one retry chain, whoever ran the pass.
//
// It lives here, wrapped around the pass itself, rather than inside the scanner,
// because the scanner is not the only thing that runs a pass. The
// BlockDeviceFilter reconciler runs one too, and it is driven by
// controller-runtime: a Result it returns goes back into that controller's work
// queue, which is a second retry chain that no gate of the scanner's can see.
//
// Two chains is not a tidiness problem. lvg.maxUnnamedPVPasses bounds how long a
// Physical Volume with no BlockDevice keeps the pass coming back, it is counted in
// passes, and its size is argued from how many passes the informer cache needs to
// catch up — an argument that only holds while a pass is worth roughly a scan
// interval. Chains running concurrently spend that budget in wall-clock time
// proportional to how many of them there are, so the budget shrinks exactly under
// the load it exists to tolerate. On top of that each chain is a full discovery
// pass: host LVM commands, a losetup probe per loop Physical Volume, an uncached
// list of the node's BlockDevices.
//
// So the wrapper returns no requeue of its own. A caller that would otherwise put
// the pass back on a queue of its own is told there is nothing to requeue, and the
// chain this function owns is what comes back — which is also why the requeue must
// not simply be dropped: the BlockDeviceFilter path is the one place where an
// operator admitting a device produces a BlockDevice with no udev event behind it,
// and something has to notice it.
//
// The error is the one thing that still goes back, and with it the second chain:
// controller-runtime requeues a reconcile that returned one, under its own backoff,
// and that queue is as invisible to this gate as a Result would have been. It costs
// nothing today because no discoverer returns an error — bd.Discoverer.Discover
// returns one only when it is not asking for a requeue, and blockDeviceReconcile
// asks for a requeue on every error it has; lvg.Discoverer.Discover never returns
// one at all. Nothing enforces that, so a discoverer added later that answers
// (Result{}, err) brings the second chain back, and no test here would notice.
// Swallowing the error instead is not the answer: it is what the caller logs, and
// for a controller-runtime caller it is the only backoff a genuinely broken pass
// gets.
//
// ctx is the process's, not the caller's: the chain outlives the pass that started
// it, and a controller-runtime reconcile context is cancelled with the reconcile.
func WithSingleRetryChain(
	ctx context.Context,
	log logger.Logger,
	name string,
	discover func(context.Context) (Result, error),
) func(context.Context) (Result, error) {
	gate := &retryGate{}

	return func(callerCtx context.Context) (Result, error) {
		res, err := discover(callerCtx)

		// Before the error check, so a requeue a discoverer asked for is not lost to
		// a failure in one that ran after it. retryReconcile ends its chain on the
		// next error of its own, so at worst this costs one attempt.
		startRetry(ctx, log, gate, name, discover, res)

		return Result{}, err
	}
}

// startRetry hands the requeue to a chain of its own, and only if there is no
// chain running already.
//
// One chain, not one per pass that asked. The scanner runs a pass on every udev
// event and the BlockDeviceFilter reconciler runs one on every change to a filter,
// while the LVMVolumeGroup discoverer asks to be requeued for as long as a
// Physical Volume waits for its BlockDevice — which is a state, not an event, so a
// burst of either used to leave a chain behind for each one of them. They all
// drive the same pass, so the extra chains buy nothing; what they cost is real:
//
//   - each chain runs a full discovery pass — a losetup probe per loop Physical
//     Volume, an uncached BlockDevice read — so N chains are N of those,
//     concurrently, on a node that is already busy enough to be raising udev
//     events;
//   - lvg.maxUnnamedPVPasses counts passes, and its size is argued from how many
//     passes the informer cache needs to catch up. Overlapping chains spend that
//     budget in wall-clock time proportional to how many of them there are —
//     twelve passes went in twenty-three seconds on a stand at a five-second
//     interval — so the budget shrinks exactly when the load it is meant to
//     tolerate appears, and the retry gives up on a Physical Volume that was a
//     moment away from being named.
//
// Little is lost by refusing: the running chain calls the same pass and stops on
// the same condition, so it is already doing the work this request is asking for.
// That holds because the chain retries the pass rather than one discoverer of it;
// a chain per discoverer would refuse a request for the pair on behalf of a chain
// that only re-runs one half.
//
// "Little" and not "nothing", which is what retryGate.pending is for: a chain that
// is leaving its loop is still marked as running, so a request that lands in that
// window would be refused on behalf of a chain that is about to stop doing
// anything. The request is remembered instead of dropped, and the chain goes round
// again rather than exiting.
func startRetry(
	ctx context.Context,
	log logger.Logger,
	gate *retryGate,
	name string,
	reconcile func(context.Context) (Result, error),
	res Result,
) {
	if res.RequeueAfter <= 0 {
		return
	}

	if !gate.admit(res) {
		log.Debug(fmt.Sprintf("[discoveryRetry] %s already has a retry running, holding this request for it", name))
		return
	}

	go func() {
		for {
			retryReconcile(ctx, log, name, reconcile, res)

			next, ok := gate.next(ctx)
			if !ok {
				return
			}

			log.Debug(fmt.Sprintf("[discoveryRetry] %s was asked to come back while its chain was ending; going round again in %s", name, next.RequeueAfter.String()))
			res = next
		}
	}()
}

// retryGate admits one retry chain and remembers a request it turned away.
//
// A bare "is a chain running" flag is not enough, and the gap is a lost wakeup in
// the sense of Goetz et al., Java Concurrency in Practice, §14.2.3: between the
// moment a chain's last attempt stops asking to be requeued and the moment the
// flag is cleared, the flag still says "running" while nothing is going to run
// again. A pass that asked for a requeue in that window was refused on behalf of a
// chain that no longer existed, and its requeue was gone — with the scanner's
// timer never rearmed after its first firing, the next udev event was the only
// thing left, which a node whose devices have settled does not raise.
//
// So the request is held rather than refused, and the chain consumes it instead of
// exiting. Requests that pile up collapse into the soonest of them: they all ask
// for the same pass, so what differs between them is only how long they are willing
// to wait.
type retryGate struct {
	mu      sync.Mutex
	running bool
	// pending is the requeue asked for while a chain was running, and a zero
	// RequeueAfter means there is none.
	pending Result
}

// admit reports whether the caller may start the chain. When it may not, res is
// held for the chain that is running.
func (g *retryGate) admit(res Result) bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.running {
		g.pending = SoonestRequeue(g.pending, res)
		return false
	}

	g.running = true

	return true
}

// next hands the chain the request that arrived while it was running, and reports
// whether there was one. When there was not, the gate is reopened before this
// returns, so the caller must stop.
//
// A cancelled context ends the chain whatever is pending: the held request is for a
// pass that is not going to happen, and going round again would only reach
// retryReconcile's own ctx.Done check.
func (g *retryGate) next(ctx context.Context) (Result, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()

	pending := g.pending
	g.pending = Result{}

	if pending.RequeueAfter <= 0 || ctx.Err() != nil {
		g.running = false
		return Result{}, false
	}

	return pending, true
}

// retryReconcile keeps calling reconcile for as long as it asks to be requeued.
//
// The Result is this function's own parameter, and that is the whole point. It
// used to be the caller's variable, captured by goroutines that were started one
// per pass, one per controller: every retry chain looped on the same Result the
// scanner's main loop assigns on every udev event, and on the same err as the
// other chain. Three consequences, all of them live now that the LVMVolumeGroup
// discoverer requeues on a missing BlockDevice:
//
//   - a udev-driven pass returning an empty Result ended every retry chain in
//     flight, including chains that still had a Physical Volume to wait for —
//     the retry stopped for a reason that had nothing to do with what it was
//     waiting on;
//   - time.Sleep re-read the shared Result after the loop condition had already
//     tested it, so a zero written in between turned the retry into a tight loop
//     of full LVM scans and uncached BlockDevice lists;
//   - concurrent writes to the same variable are a data race under the Go memory
//     model (https://go.dev/ref/mem).
//
// lvg.maxUnnamedPVPasses counts passes against this loop, so its budget only
// means what its comment says once each chain owns its own state.
//
// ctx.Done() ends the chain: it outlives the pass that started it, so without
// that check a retry keeps running host commands through shutdown. An error ends
// it too, and says so — the shared err it used to be assigned to was read by
// nobody.
func retryReconcile(
	ctx context.Context,
	log logger.Logger,
	name string,
	reconcile func(context.Context) (Result, error),
	res Result,
) {
	for res.RequeueAfter > 0 {
		log.Warning(fmt.Sprintf("[discoveryRetry] %s reconcile needs a retry in %s", name, res.RequeueAfter.String()))

		select {
		case <-ctx.Done():
			log.Info(fmt.Sprintf("[discoveryRetry] %s retry stopped before its next attempt: %v", name, ctx.Err()))
			return
		case <-time.After(res.RequeueAfter):
		}

		next, err := reconcile(ctx)
		if err != nil {
			log.Error(err, fmt.Sprintf("[discoveryRetry] an error occurred while retrying %s reconcile, giving the retry up", name))
			return
		}

		res = next
	}

	log.Info(fmt.Sprintf("[discoveryRetry] successfully reconciled %s after a retry", name))
}
