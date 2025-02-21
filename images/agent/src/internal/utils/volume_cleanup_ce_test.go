//go:build ce

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

package utils

import (
	"context"
	"testing"

	"agent/internal/logger"
)

func TestVolumeCleanup_Unsupported(t *testing.T) {
	log, err := logger.NewLogger(logger.WarningLevel)
	if err != nil {
		t.Fatalf("can't create logger: %v", err)
	}
	err = VolumeCleanup(context.Background(), log, nil, "", "", "")
	if err == nil {
		t.Fatalf("error expected")
	}

	expected := "volume cleanup is not supported in your edition"
	got := err.Error()
	if got != expected {
		t.Fatalf("error message expected '%s' got '%s'", expected, got)
	}
}
