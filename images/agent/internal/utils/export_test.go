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

// This file is compiled only for tests of this package. It exists because the
// generated command mocks live in internal/mock_utils, which imports this package
// — so a test that needs a mocked Commands has to be in the external utils_test
// package and cannot reach an unexported identifier without help.

// ReTagForTest exposes reTag, whose ownership gate is what keeps the agent from
// rewriting the LVM tags of a loop-backed Volume Group it does not own.
var ReTagForTest = reTag

// NoSuchDMDeviceForTest exposes how a removal decides that the mapping it was
// aimed at is already gone.
func NoSuchDMDeviceForTest(stderr string) bool {
	return reNoSuchDMDevice.MatchString(stderr)
}
