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
	"context"
	"os"
	"testing"
	"time"

	e2ecfg "github.com/deckhouse/sds-node-configurator/e2e/cfg"
	"github.com/deckhouse/storage-e2e/pkg/cluster"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = BeforeSuite(func() {
	conf, cfgErr := e2ecfg.New()
	Expect(cfgErr).NotTo(HaveOccurred(), "Failed to load config")
	Expect(conf).NotTo(BeNil())
	// Before any spec: Ginkgo may shuffle root Ordered Describes; nested cluster must exist first.
	e2eEnsureSharedNestedTestCluster()
})

var _ = AfterSuite(func() {
	res := e2eNestedTestClusterOrNil()
	if res != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		locked, lockErr := cluster.IsClusterLocked(ctx, res.Kubeconfig)
		if lockErr != nil {
			GinkgoWriter.Println(lockErr)
		}
		if locked {
			releaseErr := cluster.ReleaseClusterLock(ctx, res.Kubeconfig)
			if releaseErr != nil {
				GinkgoWriter.Println(releaseErr)
			}
			GinkgoWriter.Println("Released cluster lock")
		}
	}

	e2eCloseNodeSSHCache()
	e2eCleanupNestedTestClusterAfterSuite()
})

func TestSdsNodeConfigurator(t *testing.T) {
	RegisterFailHandler(Fail)
	suiteConfig, reporterConfig := GinkgoConfiguration()
	suiteTimeout := e2eTestSuiteTimeout()
	suiteConfig.Timeout = suiteTimeout
	if os.Getenv("CI") != "" {
		suiteConfig.FailFast = true
	}
	reporterConfig.Verbose = true
	reporterConfig.ShowNodeEvents = false
	t.Logf("E2E suite timeout: %v (override with E2E_TEST_TIMEOUT)", suiteTimeout)
	RunSpecs(t, "Sds Node Configurator Suite", suiteConfig, reporterConfig)
}
