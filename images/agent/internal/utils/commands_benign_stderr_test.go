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
	"bytes"
	"errors"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/assert"
)

// exitError returns a genuine *exec.ExitError, i.e. "the command ran and exited
// non-zero" — the only kind of failure whose stderr is worth interpreting.
func exitError(t *testing.T) error {
	t.Helper()
	err := exec.Command("/usr/bin/env", "false").Run()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("expected an *exec.ExitError from a failing command, got %T (%v)", err, err)
	}
	return err
}

// startError returns the error os/exec produces when the command never ran at
// all. There is no stderr to filter in that case, so nothing about it may be
// read as success.
func startError(t *testing.T) error {
	t.Helper()
	err := exec.Command("/nonexistent/sds-node-configurator-test-binary").Run()
	if err == nil {
		t.Fatal("expected running a non-existent binary to fail")
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		t.Fatalf("expected a start failure, not an exit status: %v", err)
	}
	return err
}

// signalError returns the error for a process the kernel killed. os/exec reports
// it as an *exec.ExitError like an ordinary non-zero exit, which is precisely the
// trap: lvm never chose that outcome, so its stderr must not be trusted.
func signalError(t *testing.T) error {
	t.Helper()
	cmd := exec.Command("/usr/bin/env", "sleep", "30")
	if err := cmd.Start(); err != nil {
		t.Fatalf("unable to start the helper process: %v", err)
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("unable to kill the helper process: %v", err)
	}
	err := cmd.Wait()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("expected an *exec.ExitError for a killed process, got %T (%v)", err, err)
	}
	if exitErr.ExitCode() >= 0 {
		t.Fatalf("expected ExitCode -1 for a killed process, got %d", exitErr.ExitCode())
	}
	return err
}

// lvm.static under nsenter routinely exits non-zero on an operation that in fact
// succeeded, printing nothing but a leaked-file-descriptor warning. Reporting
// that as a failure is what made the create rollback delete a live PV's backing
// file, so a benign-only stderr has to count as success.
//
// The other half matters just as much: a command that never ran, or was killed,
// produces no diagnostic either — and there an empty stderr says nothing about
// whether the operation happened. Treating the two the same is what the earlier
// `err != nil && filtered.Len() > 0` check did, and it silently turned every
// diagnostic-less failure into a success.
func TestErrIfNotBenign(t *testing.T) {
	const benignFDLeak = "File descriptor 7 leaked on lvm.static invocation. Parent PID 1234: /opt/deckhouse/sds/bin/nsenter\n"

	t.Run("no error is no error", func(t *testing.T) {
		assert.NoError(t, errIfNotBenign("pvcreate /dev/loop0", nil, bytes.Buffer{}, benignAlwaysStdErr, silentExitIsFailure))
	})

	t.Run("a non-zero exit with only benign stderr is a success", func(t *testing.T) {
		var stderr bytes.Buffer
		stderr.WriteString(benignFDLeak)
		assert.NoError(t, errIfNotBenign("pvcreate /dev/loop0", exitError(t), stderr, benignAlwaysStdErr, silentExitIsFailure))
	})

	t.Run("a real diagnostic survives the filter", func(t *testing.T) {
		var stderr bytes.Buffer
		stderr.WriteString(benignFDLeak)
		stderr.WriteString("  Can't initialize physical volume /dev/loop0 of volume group vg-a without -ff\n")

		err := errIfNotBenign("pvcreate /dev/loop0", exitError(t), stderr, benignAlwaysStdErr, silentExitIsFailure)
		if assert.Error(t, err) {
			assert.Contains(t, err.Error(), "without -ff")
		}
	})

	t.Run("a silent non-zero exit is benign only where a silent no-op is known", func(t *testing.T) {
		// lvm ran, chose to exit non-zero and had nothing to say about it.
		//
		// For a resize that is tolerated and load-bearing: some LVM versions
		// report the no-op `lvextend -l 100%VG` this way, and a thin pool sized
		// 100% hits that no-op on every reconcile, so calling it a failure makes a
		// healthy pool flap VGConfigurationApplied.
		assert.NoError(t, errIfNotBenign("lvextend -l 100%VG /dev/vg/tp", exitError(t), bytes.Buffer{}, benignResizeStdErr, silentExitIsBenign))

		// pvcreate has no such no-op. An unexplained failure there must stay a
		// failure: reporting it as success would claim a PV label that was never
		// written, and the create rollback would then run against a device whose
		// state nobody knows.
		assert.Error(t, errIfNotBenign("pvcreate /dev/loop0", exitError(t), bytes.Buffer{}, benignAlwaysStdErr, silentExitIsFailure))
	})

	t.Run("a command that never ran is a failure", func(t *testing.T) {
		err := errIfNotBenign("pvcreate /dev/loop0", startError(t), bytes.Buffer{}, benignAlwaysStdErr, silentExitIsBenign)
		assert.Error(t, err, "a missing binary must not be filtered into a success")
	})

	t.Run("a killed command is a failure even with benign stderr", func(t *testing.T) {
		// An OOM-killed lvm.static may well have printed the fd-leak warning
		// before dying. It is an *exec.ExitError like any other, so only the
		// signal check stands between this and CreatePV reporting a PV label that
		// was never written.
		var stderr bytes.Buffer
		stderr.WriteString(benignFDLeak)
		assert.Error(t, errIfNotBenign("pvcreate /dev/loop0", signalError(t), stderr, benignAlwaysStdErr, silentExitIsBenign))
	})

	t.Run("a killed command is a failure with no stderr either", func(t *testing.T) {
		assert.Error(t, errIfNotBenign("pvcreate /dev/loop0", signalError(t), bytes.Buffer{}, benignAlwaysStdErr, silentExitIsBenign))
	})
}

// Every write command the file-device paths depend on has to interpret its stderr,
// not just pvcreate.
//
// vgcreate and vgextend were the two that did not, and leaving them out was worse
// than it looks, because both of their errors trigger something: a vgcreate error
// runs the create rollback (whose own doc comment names "a pvcreate/vgcreate that
// materially succeeded but returned a non-zero status" as the state it defends
// against), and a vgextend error is written to VGConfigurationApplied as
// VGExtendFailed — a reason deliberately absent from the conditions watcher's
// acceptableReasons, so a leaked file descriptor took a Volume Group that was
// serving every volume it had out of service.
//
// This asserts the policy each one runs under. The policy is the same for all four:
// only the nsenter artefacts are benign, and silence is a failure because none of
// these has a known no-op — unlike lvextend, whose 100%VG resize is a no-op on
// every single reconcile.
func TestWriteCommandsShareTheBenignStdErrPolicy(t *testing.T) {
	const (
		benignFDLeak = "File descriptor 7 leaked on lvm.static invocation. Parent PID 1234: /opt/deckhouse/sds/bin/nsenter"
		realFailure  = "  Volume group \"vg-a\" not found"
		noOpResize   = "  New size (953801 extents) matches existing size (953801 extents)."
	)

	// The commands that write LVM metadata and have no silent no-op.
	for _, cmdStr := range []string{
		"pvcreate /dev/loop0",
		"pvresize /dev/loop0",
		"vgcreate vg-a /dev/loop0",
		"vgextend vg-a /dev/loop0",
	} {
		t.Run(cmdStr, func(t *testing.T) {
			var benign bytes.Buffer
			benign.WriteString(benignFDLeak + "\n")
			assert.NoError(t, errIfNotBenign(cmdStr, exitError(t), benign, benignAlwaysStdErr, silentExitIsFailure),
				"an nsenter artefact must not be reported as a failure")

			var withDiagnostic bytes.Buffer
			withDiagnostic.WriteString(benignFDLeak + "\n")
			withDiagnostic.WriteString(realFailure + "\n")
			if err := errIfNotBenign(cmdStr, exitError(t), withDiagnostic, benignAlwaysStdErr, silentExitIsFailure); assert.Error(t, err) {
				assert.Contains(t, err.Error(), "not found", "a real diagnostic must survive")
			}

			assert.Error(t, errIfNotBenign(cmdStr, exitError(t), bytes.Buffer{}, benignAlwaysStdErr, silentExitIsFailure),
				"an unexplained failure must stay a failure for a command with no known no-op")

			var resize bytes.Buffer
			resize.WriteString(noOpResize + "\n")
			assert.Error(t, errIfNotBenign(cmdStr, exitError(t), resize, benignAlwaysStdErr, silentExitIsFailure),
				"the no-op resize wording is lvextend's alone and must not widen what counts as success here")

			assert.Error(t, errIfNotBenign(cmdStr, signalError(t), benign, benignAlwaysStdErr, silentExitIsFailure),
				"a killed command may have printed the artefact before dying; it says nothing about the outcome")
			assert.Error(t, errIfNotBenign(cmdStr, startError(t), bytes.Buffer{}, benignAlwaysStdErr, silentExitIsFailure),
				"a command that never ran cannot have succeeded")
		})
	}
}
