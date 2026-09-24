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

package hooks_common

import (
	"context"
	"errors"
	"testing"

	"github.com/deckhouse/deckhouse/pkg/log"
	"github.com/deckhouse/module-sdk/pkg"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// log.NewNop() returns a *log.Logger, which satisfies pkg.Logger.
var testLogger = log.NewNop()

// recordingMetrics is the smallest pkg.MetricsCollector that lets a test see
// what was written. module-sdk keeps its own collector internal, so there is
// nothing to reuse.
type recordingMetrics struct {
	sets []float64
}

func (m *recordingMetrics) Set(_ string, value float64, _ map[string]string, _ ...pkg.MetricCollectorOption) {
	m.sets = append(m.sets, value)
}
func (m *recordingMetrics) Inc(string, map[string]string, ...pkg.MetricCollectorOption)          {}
func (m *recordingMetrics) Add(string, float64, map[string]string, ...pkg.MetricCollectorOption) {}
func (m *recordingMetrics) Expire(string)                                                        {}

func newScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	return scheme
}

// lvg builds an LVMVolumeGroup carrying one spec.fileDevices entry per given
// directory. The rest of the spec is left out: the hook reads nothing else.
func lvg(name string, directories ...string) *unstructured.Unstructured {
	entries := make([]any, 0, len(directories))
	for i, dir := range directories {
		entries = append(entries, map[string]any{
			"name":      string(rune('a' + i)),
			"directory": dir,
			"size":      "1Gi",
		})
	}

	obj := &unstructured.Unstructured{Object: map[string]any{
		"spec": map[string]any{"fileDevices": entries},
	}}
	obj.SetGroupVersionKind(lvgGVK)
	obj.SetName(name)
	return obj
}

// blockDeviceLVG has no fileDevices at all, which is the ordinary shape and must
// not be counted.
func blockDeviceLVG(name string) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{Object: map[string]any{
		"spec": map[string]any{"actualVGNameOnTheNode": "data-vg"},
	}}
	obj.SetGroupVersionKind(lvgGVK)
	obj.SetName(name)
	return obj
}

func report(t *testing.T, cl client.Client) *recordingMetrics {
	t.Helper()
	metrics := &recordingMetrics{}
	reportLegacyFileDevicesDirectory(context.Background(), cl, metrics, testLogger)
	return metrics
}

// A cluster installed after the move, or one that never used file devices, must
// read zero — that is the value the shim's retirement is decided on.
func TestReportLegacyFileDevicesDirectory_NothingLeft(t *testing.T) {
	cl := fake.NewClientBuilder().WithScheme(newScheme()).
		WithObjects(
			blockDeviceLVG("vg-blocks"),
			lvg("vg-new", "/var/lib/deckhouse/sds/file-devices"),
			lvg("vg-custom", "/mnt/disk/sds"),
		).
		Build()

	assertSets(t, report(t, cl), 0)
}

func TestReportLegacyFileDevicesDirectory_CountsLegacyLVGs(t *testing.T) {
	cl := fake.NewClientBuilder().WithScheme(newScheme()).
		WithObjects(
			lvg("vg-legacy", legacyFileDevicesDirectory),
			lvg("vg-legacy-sub", legacyFileDevicesDirectory+"/pool-0"),
			// Counted once, not twice: the gauge counts Volume Groups, not entries.
			lvg("vg-mixed", "/var/lib/deckhouse/sds/file-devices", legacyFileDevicesDirectory),
			lvg("vg-new", "/var/lib/deckhouse/sds/file-devices"),
			// Shares the prefix without being inside the legacy tree.
			lvg("vg-sibling", legacyFileDevicesDirectory+"-2"),
			// The parent holds the module's static binaries, not backing files.
			lvg("vg-parent", "/opt/deckhouse/sds"),
			blockDeviceLVG("vg-blocks"),
		).
		Build()

	assertSets(t, report(t, cl), 3)
}

// An unreachable apiserver is not the answer "nothing is left": writing zero
// would resolve an alert that is still true, so the gauge is not touched at all.
func TestReportLegacyFileDevicesDirectory_ListFailureLeavesTheGaugeAlone(t *testing.T) {
	cl := fake.NewClientBuilder().WithScheme(newScheme()).
		WithObjects(lvg("vg-legacy", legacyFileDevicesDirectory)).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(_ context.Context, _ client.WithWatch, _ client.ObjectList, _ ...client.ListOption) error {
				return errors.New("list boom")
			},
		}).
		Build()

	if got := report(t, cl).sets; len(got) != 0 {
		t.Errorf("the gauge was written %v, want no write at all", got)
	}
}

// A spec shaped unlike the CRD must not stop the count of the objects that are
// shaped right.
func TestReportLegacyFileDevicesDirectory_ToleratesAMalformedSpec(t *testing.T) {
	malformed := &unstructured.Unstructured{Object: map[string]any{
		"spec": map[string]any{"fileDevices": "not-a-list"},
	}}
	malformed.SetGroupVersionKind(lvgGVK)
	malformed.SetName("vg-malformed")

	entryWithoutDirectory := &unstructured.Unstructured{Object: map[string]any{
		"spec": map[string]any{"fileDevices": []any{map[string]any{"name": "a"}}},
	}}
	entryWithoutDirectory.SetGroupVersionKind(lvgGVK)
	entryWithoutDirectory.SetName("vg-no-directory")

	cl := fake.NewClientBuilder().WithScheme(newScheme()).
		WithObjects(malformed, entryWithoutDirectory, lvg("vg-legacy", legacyFileDevicesDirectory)).
		Build()

	assertSets(t, report(t, cl), 1)
}

func TestIsWithinLegacyDirectory(t *testing.T) {
	tests := []struct {
		dir  string
		want bool
	}{
		{legacyFileDevicesDirectory, true},
		{legacyFileDevicesDirectory + "/pool-0", true},
		{legacyFileDevicesDirectory + "/", true},
		{legacyFileDevicesDirectory + "-2", false},
		{"/opt/deckhouse/sds", false},
		{"/var/lib/deckhouse/sds/file-devices", false},
		{"/", false},
	}
	for _, tc := range tests {
		if got := isWithinLegacyDirectory(tc.dir); got != tc.want {
			t.Errorf("isWithinLegacyDirectory(%q) = %v, want %v", tc.dir, got, tc.want)
		}
	}
}

func assertSets(t *testing.T, metrics *recordingMetrics, want float64) {
	t.Helper()
	if len(metrics.sets) != 1 {
		t.Fatalf("the gauge was written %d time(s), want exactly 1: %v", len(metrics.sets), metrics.sets)
	}
	if metrics.sets[0] != want {
		t.Errorf("the gauge = %v, want %v", metrics.sets[0], want)
	}
}
