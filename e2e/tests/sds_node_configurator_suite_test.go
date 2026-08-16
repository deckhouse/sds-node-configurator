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
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/deckhouse/sds-node-configurator/e2e/framework"
	"github.com/deckhouse/storage-e2e/pkg/e2e"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestSdsNodeConfigurator(t *testing.T) {
	RegisterFailHandler(Fail)
	suiteConfig, reporterConfig := GinkgoConfiguration()
	suiteConfig.Timeout = suiteTimeout()
	// FailFast is intentionally off (including in CI): we want the full suite to run
	// so a single early failure does not hide later broken specs.
	reporterConfig.Verbose = true
	reporterConfig.ShowNodeEvents = false
	RunSpecs(t, "Sds Node Configurator Suite", suiteConfig, reporterConfig)
}

// Shell access to the nodes is put on the cluster once, before any spec runs, so
// a run that dies in the first spec still leaves a cluster somebody can log into.
// It deliberately cannot fail the suite: this is a diagnostic aid, and a suite
// that goes red because a debugging convenience could not be installed would
// report the wrong thing entirely.
//
// The connection is opened and closed here rather than shared with the specs,
// because holding it would hold the cluster lease every spec needs to take.
var _ = BeforeSuite(func() {
	ctx := context.Background()

	cl, err := e2e.Connect(ctx, e2e.WithTestName("bootstrap-debug-access"))
	if err != nil {
		GinkgoWriter.Printf("BeforeSuite: not installing %s access, cluster connection failed: %v\n",
			framework.DebugNodeUserName, err)
		return
	}
	defer func() {
		if closeErr := cl.Close(context.Background()); closeErr != nil {
			GinkgoWriter.Printf("BeforeSuite: closing the cluster connection: %v\n", closeErr)
		}
	}()

	switch err := framework.EnsureDebugAccess(ctx, cl.RESTConfig()); {
	case err == nil:
		GinkgoWriter.Printf("BeforeSuite: %s can log into every node group and sudo without a password\n",
			framework.DebugNodeUserName)
	case errors.Is(err, framework.ErrDebugAccessCRDAbsent),
		errors.Is(err, framework.ErrDebugAccessDisabled),
		// The ordinary outcome outside CI, and the reason this is opt-in: the keys
		// live in a secret, so a run that was not given them installs nothing
		// rather than leaving an account and a sudo rule on somebody's cluster.
		errors.Is(err, framework.ErrDebugAccessNoKeys):
		GinkgoWriter.Printf("BeforeSuite: skipping %s access: %v\n",
			framework.DebugNodeUserName, err)
	case errors.Is(err, framework.ErrDebugAccessForeign):
		// Worth its own line rather than folding into the default: it is the one
		// outcome that says something about the cluster rather than about the
		// suite, and the answer is to leave that cluster alone.
		GinkgoWriter.Printf("BeforeSuite: not touching %s access, the cluster manages it elsewhere: %v. Set %s to silence this\n",
			framework.DebugNodeUserName, err, framework.SkipDebugAccessEnv)
	default:
		GinkgoWriter.Printf("BeforeSuite: unable to install %s access, continuing anyway: %v\n",
			framework.DebugNodeUserName, err)
	}
})

func suiteTimeout() time.Duration {
	const (
		localDefault = 90 * time.Minute
		ciDefault    = 3*time.Hour + 30*time.Minute
		ciMinimum    = 3*time.Hour + 30*time.Minute
	)

	inCI := os.Getenv("CI") != ""

	timeout := localDefault
	if inCI {
		timeout = ciDefault
	}

	if raw := strings.TrimSpace(os.Getenv("E2E_TEST_TIMEOUT")); raw != "" {
		if parsed, err := time.ParseDuration(raw); err == nil && parsed > 0 {
			timeout = parsed
		}
	}

	if inCI && timeout < ciMinimum {
		timeout = ciMinimum
	}

	return timeout
}
