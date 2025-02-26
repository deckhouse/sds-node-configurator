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

package utils_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"agent/internal/logger"
	. "agent/internal/utils"
)

var _ = Describe("Cleaning up volume", func() {
	var log logger.Logger
	BeforeEach(func() {
		logger, err := logger.NewLogger(logger.WarningLevel)
		log = logger
		Expect(err).NotTo(HaveOccurred())
	})
	It("Unsupported", func() {
		err := VolumeCleanup(context.Background(), log, nil, "", "", "", nil)
		Expect(err).To(MatchError("volume cleanup is not supported in your edition"))
	})
})
