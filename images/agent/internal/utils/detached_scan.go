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

// This file runs the lvm scans — pvs, vgs, lvs over every device on the node —
// so that a deadline abandons the scan instead of killing it.
//
// A scan that outlives its deadline is a scan that blocked in the kernel on a
// device it should not have opened: a multipath map with no usable path, where a
// read does not fail but waits. The filter in multipathfilter.go keeps lvm away
// from the maps it can name; this is for the one it could not, and for whatever
// else a node may hold that behaves the same way.
//
// Killing such a scan does not end it. lvm reads devices through libaio, the
// wait is in io_getevents, and SIGKILL moves the process from that
// interruptible sleep into exit_aio, where it waits for the in-flight request
// uninterruptibly: a D-state process that no signal removes, with the device
// still open. Measured on a SAN stand: four lvm processes in exit_aio for three
// days and eighteen hours. Left alone, the same scan sleeps in S, still holding
// the device open, but it goes away the moment the map is dealt with —
// `dmsetup message <map> 0 fail_if_no_path` returns EIO to it — and a node
// reboot stops being the only cure.
//
// Abandoning also means not starting another. Each pass would otherwise add a
// scan to the pile, and each one would open the same device. One outstanding
// scan per command is the cap: the next pass reports that the previous scan is
// still running and does not run its own.

package utils

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"sync"
	"time"
)

// ErrScanStillRunning is returned when a scan of the same kind, abandoned by an
// earlier deadline, has not finished yet.
var ErrScanStillRunning = errors.New("the previous scan is still running")

var outstandingScans struct {
	mu      sync.Mutex
	started map[string]time.Time
}

type scanOutcome struct {
	stdout bytes.Buffer
	stderr bytes.Buffer
	err    error
}

// runScanDetached starts cmd and waits for it until ctx ends. When the process
// finishes first its output and exit status are returned as cmd.Run would
// return them. When ctx ends first the call returns ctx.Err() and leaves the
// process to finish on its own: it is never signalled, its exit is reaped by the
// goroutine that waits for it, and until then no other scan under the same key
// is started.
//
// cmd must come from exec.Command, not exec.CommandContext, or the context would
// kill the process behind this function's back.
func runScanDetached(ctx context.Context, key string, cmd *exec.Cmd) (stdout, stderr bytes.Buffer, err error) {
	if err := claimScan(key); err != nil {
		return stdout, stderr, err
	}

	out := &scanOutcome{}
	cmd.Stdout = &out.stdout
	cmd.Stderr = &out.stderr
	if err := cmd.Start(); err != nil {
		releaseScan(key)
		return stdout, stderr, err
	}

	done := make(chan *scanOutcome, 1)
	go func() {
		out.err = cmd.Wait()
		releaseScan(key)
		done <- out
	}()

	select {
	case res := <-done:
		return res.stdout, res.stderr, res.err
	case <-ctx.Done():
		return stdout, stderr, ctx.Err()
	}
}

// OutstandingScans reports which scans, abandoned at their deadline, are still
// running, with the time each one started.
func OutstandingScans() map[string]time.Time {
	outstandingScans.mu.Lock()
	defer outstandingScans.mu.Unlock()

	out := make(map[string]time.Time, len(outstandingScans.started))
	for key, since := range outstandingScans.started {
		out[key] = since
	}

	return out
}

func claimScan(key string) error {
	outstandingScans.mu.Lock()
	defer outstandingScans.mu.Unlock()

	if outstandingScans.started == nil {
		outstandingScans.started = make(map[string]time.Time)
	}
	if since, running := outstandingScans.started[key]; running {
		return fmt.Errorf("%w: %s started %s ago and was abandoned by its deadline; it holds whatever device it blocked on until that device is dealt with",
			ErrScanStillRunning, key, time.Since(since).Round(time.Second))
	}
	outstandingScans.started[key] = time.Now()

	return nil
}

func releaseScan(key string) {
	outstandingScans.mu.Lock()
	defer outstandingScans.mu.Unlock()

	delete(outstandingScans.started, key)
}
