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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	"github.com/onsi/ginkgo/v2/reporters"
	. "github.com/onsi/gomega"

	"github.com/deckhouse/storage-e2e/pkg/setup"
)

const (
	e2eJUnitReportFileName = "e2e-junit-report.xml"
	e2eJSONReportFileName  = "e2e-report.json"
)

var _ = BeforeSuite(func() {
	err := setup.Init()
	Expect(err).NotTo(HaveOccurred(), "Failed to initialize storage-e2e setup")
	// Before any spec: Ginkgo may shuffle root Ordered Describes; nested cluster must exist first.
	e2eEnsureSharedNestedTestCluster()
})

var _ = AfterSuite(func() {
	e2eCleanupNestedTestClusterAfterSuite()
	if err := setup.Close(); err != nil {
		GinkgoWriter.Printf("Warning: Failed to close logger: %v\n", err)
	}
})

// ReportAfterSuite runs after the entire suite completes (including AfterSuite)
// with the full Report of per-spec results. It persists CI-friendly artifacts
// (JUnit XML + JSON) and prints a plain-text FAILED SPECS summary so the
// failure list is guaranteed to be visible in the raw test log even when
// individual specs aborted, panicked or timed out and artifact upload didn't
// happen.
//
// Report destination directory is controlled by E2E_REPORT_DIR (defaults to
// the test binary's working directory — ${repo}/e2e/tests when run via
// `go test ./tests/`). CI sets E2E_REPORT_DIR explicitly so artifacts land
// alongside e2e-test-output.log in ${GITHUB_WORKSPACE}/e2e.
var _ = ReportAfterSuite("e2e suite report", func(report Report) {
	reportDir := os.Getenv("E2E_REPORT_DIR")
	if reportDir == "" {
		reportDir = "."
	}
	if err := os.MkdirAll(reportDir, 0o755); err != nil {
		fmt.Fprintf(GinkgoWriter, "Warning: unable to create E2E_REPORT_DIR=%s: %v\n", reportDir, err)
	}

	junitPath := filepath.Join(reportDir, e2eJUnitReportFileName)
	if err := reporters.GenerateJUnitReport(report, junitPath); err != nil {
		fmt.Fprintf(GinkgoWriter, "Warning: unable to write JUnit report to %s: %v\n", junitPath, err)
	} else {
		fmt.Fprintf(GinkgoWriter, "JUnit report: %s\n", junitPath)
	}

	jsonPath := filepath.Join(reportDir, e2eJSONReportFileName)
	if err := reporters.GenerateJSONReport(report, jsonPath); err != nil {
		fmt.Fprintf(GinkgoWriter, "Warning: unable to write JSON report to %s: %v\n", jsonPath, err)
	} else {
		fmt.Fprintf(GinkgoWriter, "JSON report:  %s\n", jsonPath)
	}

	var failed []string
	for _, sr := range report.SpecReports {
		if !sr.Failed() {
			continue
		}
		label := sr.FullText()
		if label == "" {
			label = strings.Join(sr.ContainerHierarchyTexts, " / ")
			if sr.LeafNodeText != "" {
				if label != "" {
					label += " / "
				}
				label += sr.LeafNodeText
			}
		}

		loc := sr.FailureLocation().String()
		msg := strings.TrimSpace(sr.FailureMessage())
		if msg == "" {
			msg = sr.State.String()
		}
		// Keep each entry on a single line so grep/CI log view stays readable.
		msg = strings.ReplaceAll(msg, "\n", " | ")
		failed = append(failed, fmt.Sprintf("  - [%s] %s (%s) -- %s", sr.State.String(), label, loc, msg))
	}

	sep := strings.Repeat("=", 72)
	fmt.Fprintln(GinkgoWriter, sep)
	if len(failed) == 0 {
		fmt.Fprintln(GinkgoWriter, "E2E SUITE SUMMARY: all specs passed")
	} else {
		fmt.Fprintf(GinkgoWriter, "E2E SUITE SUMMARY: %d FAILED SPEC(S)\n", len(failed))
		for _, line := range failed {
			fmt.Fprintln(GinkgoWriter, line)
		}
	}
	fmt.Fprintln(GinkgoWriter, sep)
})

func TestSdsNodeConfigurator(t *testing.T) {
	RegisterFailHandler(Fail)
	suiteConfig, reporterConfig := GinkgoConfiguration()

	// Always run every spec to completion, even when earlier specs have failed.
	// Per-spec failures are still captured via ReportAfterSuite (JUnit XML +
	// JSON + plain-text summary). `go test` will still exit non-zero if any
	// spec failed, so CI fails honestly — we just don't lose downstream spec
	// results that could help diagnose related regressions in the same run.
	suiteConfig.FailFast = false

	reporterConfig.Verbose = true
	reporterConfig.ShowNodeEvents = false
	RunSpecs(t, "Sds Node Configurator Suite", suiteConfig, reporterConfig)
}
