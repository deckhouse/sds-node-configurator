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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParsePVUsedBytes(t *testing.T) {
	tests := []struct {
		raw     string
		want    int64
		wantErr bool
	}{
		{raw: "0", want: 0},
		{raw: "0 ", want: 0},
		{raw: " 4194304 ", want: 4194304},
		{raw: "4194304B", want: 4194304},
		{raw: "0.00", want: 0},
		{raw: "12.5", want: 13},
		{raw: "", wantErr: true},
		{raw: "  ", wantErr: true},
		{raw: "-1", wantErr: true},
		{raw: "abc", wantErr: true},
		{raw: "NaN", wantErr: true},
		{raw: "1.5g", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.raw, func(t *testing.T) {
			got, err := ParsePVUsedBytes(tc.raw)
			if tc.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}
