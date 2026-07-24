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

package framework

import (
	"context"
	"fmt"
	"time"
)

// Poll repeatedly invokes cond until it reports completion, ctx is cancelled, or
// timeout elapses. It is the assertion-free wait primitive for the framework
// package (which must not depend on Gomega/Ginkgo): callers always get an error,
// never a Ginkgo Fail. Spec code should prefer Gomega's Eventually instead.
//
// cond returns (done, err):
//   - done && err == nil  -> success, Poll returns nil.
//   - done && err != nil  -> fatal, Poll returns err immediately (no more polls).
//   - !done               -> err (if any) is remembered as the last error and
//     polling continues after interval.
//
// On timeout Poll returns a timeout error wrapping the last non-nil error from
// cond (or a bare timeout when cond never reported one). cond is invoked once
// before the first sleep, so a zero timeout still performs a single attempt.
func Poll(ctx context.Context, interval, timeout time.Duration, cond func(context.Context) (done bool, err error)) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		done, err := cond(ctx)
		if done {
			return err
		}
		if err != nil {
			lastErr = err
		}

		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if time.Now().After(deadline) {
			if lastErr != nil {
				return fmt.Errorf("timeout after %s: %w", timeout, lastErr)
			}
			return fmt.Errorf("timeout after %s waiting for condition", timeout)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
}
