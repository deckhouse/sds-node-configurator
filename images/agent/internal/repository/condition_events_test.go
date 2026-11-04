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

package repository_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"

	"github.com/deckhouse/sds-common-lib/conditions"
	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/monitoring"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/repository"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/test_utils"
)

// drainEvents returns what the fake recorder has received so far.
func drainEvents(rec *record.FakeRecorder) []string {
	var got []string
	for {
		select {
		case e := <-rec.Events:
			got = append(got, e)
		default:
			return got
		}
	}
}

// The agent on every node writes these, so an Event per pass, or per volume
// that is merely being created or resized, would add up to a stream. They
// record a move of the condition, and only a move that ends somewhere: Ready,
// or a failure.
var _ = Describe("condition Events", func() {
	var (
		ctx  context.Context
		fake *record.FakeRecorder
		rec  conditions.EventRecorder
	)

	BeforeEach(func() {
		ctx = context.Background()
		fake = record.NewFakeRecorder(20)
		rec = internal.WithoutProgressEvents(conditions.FromLegacyRecorder(fake))
	})

	It("records the LVMLogicalVolume becoming Ready, failing and recovering, and nothing in between", func() {
		cl := test_utils.NewFakeClient(&v1alpha1.LVMLogicalVolume{})
		llvCl := repository.NewLLVClient(cl, logger.NewNop(), rec)
		llv := &v1alpha1.LVMLogicalVolume{ObjectMeta: metav1.ObjectMeta{Name: "test-llv"}}
		Expect(cl.Create(ctx, llv)).To(Succeed())

		lvg := &v1alpha1.LVMVolumeGroup{}
		lvg.Spec.Local.NodeName = "node-1"
		lvg.Spec.ActualVGNameOnTheNode = "vg-1"
		size := resource.MustParse("1Gi")
		created := "Normal Reconciled the LVMLogicalVolume is in the Created phase"

		Expect(llvCl.UpdatePhaseIfNeeded(ctx, llv, v1alpha1.PhasePending, "")).To(Succeed())
		Expect(drainEvents(fake)).To(BeEmpty(), "a volume being created is not broken")

		Expect(llvCl.UpdatePhaseToCreatedIfNeeded(ctx, llv, lvg, size)).To(Succeed())
		Expect(drainEvents(fake)).To(Equal([]string{created}))

		Expect(llvCl.UpdatePhaseToCreatedIfNeeded(ctx, llv, lvg, size)).To(Succeed())
		Expect(drainEvents(fake)).To(BeEmpty(), "a pass that changes nothing records nothing")

		Expect(llvCl.UpdatePhaseIfNeeded(ctx, llv, v1alpha1.PhaseFailed, "lvcreate: exit status 5")).To(Succeed())
		Expect(drainEvents(fake)).To(Equal([]string{"Warning ReconcileFailed lvcreate: exit status 5"}))

		Expect(llvCl.UpdatePhaseIfNeeded(ctx, llv, v1alpha1.PhaseFailed, "lvcreate: exit status 3")).To(Succeed())
		Expect(drainEvents(fake)).To(BeEmpty(), "a re-worded failure is the same failure")

		Expect(llvCl.UpdatePhaseIfNeeded(ctx, llv, v1alpha1.PhaseResizing, "")).To(Succeed())
		Expect(drainEvents(fake)).To(BeEmpty(), "a volume being resized is not broken")

		Expect(llvCl.UpdatePhaseToCreatedIfNeeded(ctx, llv, lvg, size)).To(Succeed())
		Expect(drainEvents(fake)).To(Equal([]string{created}))

		changed, err := llvCl.SetDeletingIfNeeded(ctx, llv, "waiting for the finalizers example.com/consumer")
		Expect(err).NotTo(HaveOccurred())
		Expect(changed).To(BeTrue())
		Expect(drainEvents(fake)).To(BeEmpty(), "a deletion held by another controller is not broken")
	})

	It("records the moves of the conditions the agent owns on an LVMVolumeGroup", func() {
		cl := test_utils.NewFakeClient(&v1alpha1.LVMVolumeGroup{})
		lvgCl := repository.NewLVGClient(cl, logger.NewNop(), monitoring.GetMetrics(""), "node-1", "test", rec)
		lvg := &v1alpha1.LVMVolumeGroup{ObjectMeta: metav1.ObjectMeta{Name: "test-lvg"}}
		Expect(cl.Create(ctx, lvg)).To(Succeed())

		applied := internal.TypeVGConfigurationApplied
		set := func(conType string, status metav1.ConditionStatus, reason, message string) []string {
			GinkgoHelper()
			Expect(lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, status, conType, reason, message)).To(Succeed())
			return drainEvents(fake)
		}

		Expect(set(applied, metav1.ConditionFalse, internal.ReasonCreating, "trying to apply the configuration")).To(BeEmpty())
		Expect(set(applied, metav1.ConditionTrue, internal.ReasonApplied, "configuration has been applied")).
			To(Equal([]string{"Normal Applied configuration has been applied"}))
		Expect(set(internal.TypeVGReady, metav1.ConditionTrue, internal.ReasonUpdated, "ready")).
			To(Equal([]string{"Normal Updated ready"}))
		Expect(set(internal.TypeVGReady, metav1.ConditionTrue, internal.ReasonUpdated, "ready")).To(BeEmpty())
		Expect(set(applied, metav1.ConditionFalse, internal.ReasonValidationFailed, "no block devices match the selector")).
			To(Equal([]string{"Warning ValidationFailed no block devices match the selector"}))
		Expect(set(internal.TypeVGReady, metav1.ConditionFalse, internal.ReasonScanFailed, "unable to scan the VG")).
			To(Equal([]string{"Warning ScanFailed unable to scan the VG"}))
		Expect(set(internal.TypeVGReady, metav1.ConditionFalse, internal.ReasonScanFailed, "unable to scan the VG: timeout")).To(BeEmpty())
		Expect(set(internal.TypeVGReady, metav1.ConditionTrue, internal.ReasonUpdated, "ready")).
			To(Equal([]string{"Normal Updated ready"}))
		Expect(set(applied, metav1.ConditionFalse, internal.ReasonTerminating, "trying to delete VG")).To(BeEmpty())
	})

	It("keeps a nil recorder nil, so that the helpers record nothing", func() {
		Expect(internal.WithoutProgressEvents(nil)).To(BeNil())
	})
})
