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

package tests

import (
	"os"
	"strings"
	"testing"

	e2ecfg "github.com/deckhouse/sds-node-configurator/e2e/cfg"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = BeforeSuite(func() {
	conf, cfgErr := e2ecfg.New()
	Expect(cfgErr).NotTo(HaveOccurred(), "Failed to load config")
	Expect(conf).NotTo(BeNil())
})

func TestSdsNodeConfigurator(t *testing.T) {
	e2eAssertCIGoTestTimeout(t)

	RegisterFailHandler(Fail)
	suiteConfig, reporterConfig := GinkgoConfiguration()
	suiteTimeout := e2eTestSuiteTimeout()
	suiteConfig.Timeout = suiteTimeout
	if suiteConfig.LabelFilter == "" {
		suiteConfig.LabelFilter = e2eGinkgoLabelFilter()
	}
	if os.Getenv("CI") != "" {
		suiteConfig.FailFast = true
	}
	reporterConfig.Verbose = true
	reporterConfig.ShowNodeEvents = false
	t.Logf("E2E suite timeout: %v (override with E2E_TEST_TIMEOUT)", suiteTimeout)
	if f := strings.TrimSpace(suiteConfig.LabelFilter); f != "" {
		t.Logf("Ginkgo label filter: %q (stress-test excluded unless filter includes it; override: -ginkgo.label-filter or E2E_GINKGO_LABEL_FILTER)", f)
	} else {
		t.Logf("Ginkgo label filter: (none) — all specs including stress-test (set E2E_GINKGO_LABEL_FILTER=all explicitly)")
	}
	RunSpecs(t, "Sds Node Configurator Suite", suiteConfig, reporterConfig)
}
