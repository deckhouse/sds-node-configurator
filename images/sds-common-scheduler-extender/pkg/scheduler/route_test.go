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

package scheduler

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNeedsReservationCache(t *testing.T) {
	for path, want := range map[string]bool{
		"/scheduler/filter":          true,
		"/scheduler/prioritize":      true,
		"/v1/lvg/filter-and-score":   true,
		"/v1/lvg/narrow-reservation": true,
		"/cache":                     false,
		"/stat":                      false,
		"/status":                    false,
		"/scheduler/filter/":         false,
	} {
		assert.Equal(t, want, NeedsReservationCache(path), path)
	}
}
