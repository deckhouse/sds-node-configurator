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

package utils

import (
	"context"
	"os/exec"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunScanDetached_ReturnsWhatTheProcessWrote(t *testing.T) {
	cmd := exec.Command("/usr/bin/env", "sh", "-c", "echo out; echo err >&2; exit 3")

	stdout, stderr, err := runScanDetached(context.Background(), t.Name(), cmd)

	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr, "a process that ran and exited non-zero is reported as cmd.Run would")
	assert.Equal(t, 3, exitErr.ExitCode())
	assert.Equal(t, "out\n", stdout.String())
	assert.Equal(t, "err\n", stderr.String())
	assert.NoError(t, claimScan(t.Name()), "nothing is outstanding once the process has exited")
	releaseScan(t.Name())
}

func TestRunScanDetached_DeadlineAbandonsTheProcessWithoutASignal(t *testing.T) {
	cmd := exec.Command("/usr/bin/env", "sleep", "30")
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, _, err := runScanDetached(ctx, t.Name(), cmd)

	assert.ErrorIs(t, err, context.DeadlineExceeded, "the caller learns that its deadline passed, nothing else")
	require.NotNil(t, cmd.Process)
	assert.NoError(t, cmd.Process.Signal(syscall.Signal(0)), "the process is still there: it was not killed")

	_, _, again := runScanDetached(context.Background(), t.Name(), exec.Command("/usr/bin/env", "true"))
	assert.ErrorIs(t, again, ErrScanStillRunning, "no second scan of the same kind while the abandoned one lives")

	require.NoError(t, cmd.Process.Kill())
	assert.Eventually(t, func() bool { return claimScan(t.Name()) == nil }, 5*time.Second, 20*time.Millisecond,
		"once the process is gone the key is released and a new scan may start")
	releaseScan(t.Name())
}

func TestRunScanDetached_DifferentKindsDoNotBlockEachOther(t *testing.T) {
	cmd := exec.Command("/usr/bin/env", "sleep", "30")
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, _, err := runScanDetached(ctx, t.Name()+"-a", cmd)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	stdout, _, err := runScanDetached(context.Background(), t.Name()+"-b", exec.Command("/usr/bin/env", "echo", "ok"))
	require.NoError(t, err)
	assert.Equal(t, "ok\n", stdout.String())
}
