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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/deckhouse/storage-e2e/pkg/cluster"
)

// TestE2EWorkloadCleanup is invoked by storage-e2e CLI (cleanup-workloads) from the e2e-complete CI job.
// It removes test-created PVCs, LVGs, LocalStorageClasses, VirtualDisks, BlockDevices, etc.
// The cluster itself is left running for the next run-tests attempt.
func TestE2EWorkloadCleanup(t *testing.T) {
	if os.Getenv("E2E_CLUSTER_PHASE") != "cleanup" {
		t.Skip("E2E_CLUSTER_PHASE is not cleanup")
	}
	RegisterFailHandler(Fail)
	RunSpecs(t, "E2E Workload Cleanup")
}

var _ = Describe("CI workload cleanup", func() {
	It("removes test-created storage resources from the persistent cluster", func() {
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Minute)
		defer cancel()

		res, err := cluster.ConnectExistingClusterForMaintenance(ctx)
		Expect(err).NotTo(HaveOccurred(), "connect to test cluster for workload cleanup")

		e2eCIWorkloadCleanup(ctx, res)
	})
})
