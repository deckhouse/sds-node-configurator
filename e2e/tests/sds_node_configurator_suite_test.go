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
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/deckhouse/sds-node-configurator/e2e/framework"
	"github.com/deckhouse/sds-node-configurator/e2e/sdsclient"
	"github.com/deckhouse/storage-e2e/pkg/e2e"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

	wipeLeftoverLVM(ctx, cl)
})

// wipeLeftoverLVM clears Volume Groups and device-mapper entries a previous run
// left on the nodes. A run that was killed, or one that force-removed an
// LVMVolumeGroup the agent had not finished deleting, leaves the group on the
// node; if its disk was detached afterwards the dm entries survive as orphans
// pointing at a device that is gone.
//
// Only the suite's own e2e-vg- prefix is touched, so this is safe on a cluster
// that carries storage the suite did not create. Like everything else in
// BeforeSuite it cannot fail the run: starting dirty is worse than starting
// clean, but it is not a reason to report the suite as broken.
//
// A Volume Group an LVMVolumeGroup still describes is left alone. The prefix
// separates this suite from everything else on the node but not one run of it from
// another, and this sweep runs across every node before the first spec — so
// without that check a second run starting against the same cluster would take
// the first one's Volume Groups out from under it, mid-spec, and the first run
// would report it as the agent losing storage. Failing to list them is therefore a
// reason not to sweep at all rather than a reason to sweep blind.
//
// The list is taken again for every node, not once for the walk. The walk is one
// NodeExec per node of the cluster, so it is long next to the moment another run's
// BeforeSuite needs to create its first LVMVolumeGroup — and a group created after
// a single list was taken, on a node this walk has not reached yet, would be swept
// out from under that run with the same symptom the keep list exists to prevent.
// One extra List per node is not worth trading that against.
func wipeLeftoverLVM(ctx context.Context, cl *e2e.Cluster) {
	nodes, err := cl.Clientset().CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		GinkgoWriter.Printf("BeforeSuite: not sweeping leftover LVM, listing nodes failed: %v\n", err)
		return
	}

	k8sClient, err := sdsclient.New(cl.RESTConfig())
	if err != nil {
		GinkgoWriter.Printf("BeforeSuite: not sweeping leftover LVM, building the client failed: %v\n", err)
		return
	}

	// The nodes the sweep could not run on at all. Collected rather than reported
	// as they come, because the two things that stop it — passwordless sudo and the
	// lvm binary — are how the run is configured, not properties of one node, so
	// every node answers the same way and the report would carry one sentence per
	// node of the cluster.
	var cannotRun []string

	for i := range nodes.Items {
		node := nodes.Items[i].Name

		keep, keepErr := framework.LiveVolumeGroupNames(ctx, k8sClient)
		if keepErr != nil {
			GinkgoWriter.Printf("BeforeSuite: not sweeping leftover LVM on %s, cannot tell leftovers from live Volume Groups: %v\n",
				node, keepErr)
			AddReportEntry("leftover-lvm-sweep-failed/"+node, keepErr.Error())
			continue
		}
		if len(keep) > 0 {
			GinkgoWriter.Printf("BeforeSuite: on %s leaving %d Volume Group(s) alone, an LVMVolumeGroup still describes them: %v\n",
				node, len(keep), keep)
		}

		leftovers, err := framework.WipeE2ELVM(ctx, cl, node, keep)
		switch {
		case errors.Is(err, framework.ErrWipeCannotRun):
			// One cause for the whole cluster — no passwordless sudo, or lvm not on
			// PATH — so it is collected and said once at the end. A report entry per
			// node would be the same sentence as many times as the cluster has nodes,
			// which buries the per-node failures below that are actually per-node.
			GinkgoWriter.Printf("BeforeSuite: %v\n", err)
			cannotRun = append(cannotRun, node)
		case err != nil:
			GinkgoWriter.Printf("BeforeSuite: sweeping leftover LVM on %s failed: %v\n", node, err)
			// In the report as well as the log: a node the sweep could not reach
			// is the one whose leftovers will be blamed on whichever spec happens
			// to run there, and finding that out means reading the whole output.
			AddReportEntry("leftover-lvm-sweep-failed/"+node, err.Error())
		case len(leftovers) > 0:
			// vg: a Volume Group vgremove could not take; dm: a device-mapper node
			// that outlived one. The first is the more interesting of the two — it
			// means the node starts this run already carrying storage from the last.
			GinkgoWriter.Printf("BeforeSuite: %s still carries LVM the sweep could not remove: %v\n",
				node, leftovers)
			AddReportEntry("leftover-lvm/"+node, strings.Join(leftovers, ","))
		}
	}

	// Once, and still in the report: a cluster the sweep cannot touch accumulates
	// the leftovers this exists to clear, and the run that finally trips over them
	// will not be this one.
	if len(cannotRun) > 0 {
		AddReportEntry("leftover-lvm-sweep-unavailable",
			fmt.Sprintf("%s: %s", framework.ErrWipeCannotRun, strings.Join(cannotRun, ",")))
	}
}

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
