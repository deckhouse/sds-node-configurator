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

import "testing"

// This file is compiled only for tests of this package. It exists because the
// generated command mocks live in internal/mock_utils, which imports this package
// — so a test that needs a mocked Commands has to be in the external utils_test
// package and cannot reach an unexported identifier without help.

// ReTagForTest exposes reTag, whose ownership gate is what keeps the agent from
// rewriting the LVM tags of a loop-backed Volume Group it does not own.
var ReTagForTest = reTag

// WithEmptySysBlockForTest points the pathless-multipath scan at an empty tree
// for the duration of the test.
//
// Without it LVMGlobalFilterForOwnedLoops depends on the machine running the
// test: a box with a SAN attached can carry multipath maps whose paths are gone,
// and those legitimately add reject rules to the filter, so an assertion on the
// exact filter string would fail there and nowhere else.
func WithEmptySysBlockForTest(tb testing.TB) {
	tb.Helper()

	saved := sysBlockPath
	sysBlockPath = tb.TempDir()
	ResetPathlessMultipathCache()

	tb.Cleanup(func() {
		sysBlockPath = saved
		ResetPathlessMultipathCache()
	})
}
