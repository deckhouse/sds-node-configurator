/*
Copyright 2025 Flant JSC

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

	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/consts"
)

func TestPodExtraPVCsAnnotationKey(t *testing.T) {
	// The key is a cross-module contract (producer lives in the virtualization
	// module). Pin the exact string so a rename cannot happen silently.
	assert.Equal(t, "scheduler.deckhouse.io/extra-pvcs", consts.PodExtraPVCsAnnotation)
}

func TestParseExtraPVCNames(t *testing.T) {
	tt := []struct {
		name  string
		value string
		want  []string
	}{
		{name: "empty value", value: "", want: nil},
		{name: "single name", value: "pvc-a", want: []string{"pvc-a"}},
		{name: "two names", value: "pvc-a,pvc-b", want: []string{"pvc-a", "pvc-b"}},
		{name: "surrounding spaces are trimmed", value: " pvc-a , pvc-b ", want: []string{"pvc-a", "pvc-b"}},
		{name: "empty entries are ignored", value: ",,pvc-a,,pvc-b,", want: []string{"pvc-a", "pvc-b"}},
		{name: "only separators and spaces", value: " , , ", want: nil},
		{name: "order is preserved", value: "b,a,c", want: []string{"b", "a", "c"}},
		{name: "duplicates are kept (dedup happens later)", value: "pvc-a,pvc-a", want: []string{"pvc-a", "pvc-a"}},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, parseExtraPVCNames(tc.value))
		})
	}
}
