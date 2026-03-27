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

package tests

import (
	"os"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/deckhouse/storage-e2e/pkg/setup"
)

var _ = BeforeSuite(func() {
	err := setup.Init()
	Expect(err).NotTo(HaveOccurred(), "Failed to initialize storage-e2e setup")
})

var _ = AfterSuite(func() {
	if err := setup.Close(); err != nil {
		GinkgoWriter.Printf("Warning: Failed to close logger: %v\n", err)
	}
})

// TestE2E is the single entry point for all Ginkgo specs in this package.
// Spec order follows file registration order: common_scheduler_test.go (Common Scheduler Extender)
// runs before sds_node_configurator_test.go (Sds Node Configurator).
func TestE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	suiteConfig, reporterConfig := GinkgoConfiguration()
	// Two root Describe(Ordered) blocks each create a cluster; stop after first failure (CI also sets CI=true).
	if os.Getenv("CI") != "" {
		suiteConfig.FailFast = true
	}
	reporterConfig.Verbose = true
	reporterConfig.ShowNodeEvents = false
	RunSpecs(t, "sds-node-configurator e2e", suiteConfig, reporterConfig)
}
