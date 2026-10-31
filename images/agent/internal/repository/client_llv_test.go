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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-common-lib/conditions"
	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/repository"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/test_utils"
)

var _ = Describe("LLVClient.SetDeletingIfNeeded", func() {
	const message = "waiting for the finalizers example.com/consumer to be removed by their owners before deleting the Logical Volume"

	var (
		ctx        context.Context
		fakeClient client.WithWatch
		llvClient  *repository.LLVClient
		llv        *v1alpha1.LVMLogicalVolume
	)

	BeforeEach(func() {
		ctx = context.Background()
		fakeClient = test_utils.NewFakeClient(&v1alpha1.LVMLogicalVolume{})
		llvClient = repository.NewLLVClient(fakeClient, logger.NewNop())

		llv = &v1alpha1.LVMLogicalVolume{
			ObjectMeta: metav1.ObjectMeta{Name: "test-llv"},
			Status: &v1alpha1.LVMLogicalVolumeStatus{
				Phase: v1alpha1.PhaseCreated,
				Conditions: []metav1.Condition{{
					Type: conditions.TypeReady, Status: metav1.ConditionTrue, Reason: conditions.ReasonReconciled,
					Message: "created", LastTransitionTime: metav1.Now(),
				}},
			},
		}
		Expect(fakeClient.Create(ctx, llv)).To(Succeed())
	})

	It("reports a change only when it writes one", func() {
		changed, err := llvClient.SetDeletingIfNeeded(ctx, llv, message)
		Expect(err).NotTo(HaveOccurred())
		Expect(changed).To(BeTrue())

		got := &v1alpha1.LVMLogicalVolume{}
		Expect(fakeClient.Get(ctx, client.ObjectKeyFromObject(llv), got)).To(Succeed())
		ready := conditions.Get(got.Status.Conditions, conditions.TypeReady)
		Expect(ready).NotTo(BeNil())
		Expect(ready.Status).To(Equal(metav1.ConditionFalse))
		Expect(ready.Reason).To(Equal(conditions.ReasonDeleting))
		Expect(ready.Message).To(Equal(message))
		Expect(got.Status.Phase).To(Equal(v1alpha1.PhaseCreated))
	})

	// The object a reconcile gets comes from the informer cache, which can still
	// show the condition from before the write the server already holds.
	It("reports no change for a stale copy when the server already has the condition", func() {
		stale := llv.DeepCopy()

		_, err := llvClient.SetDeletingIfNeeded(ctx, llv, message)
		Expect(err).NotTo(HaveOccurred())

		written := &v1alpha1.LVMLogicalVolume{}
		Expect(fakeClient.Get(ctx, client.ObjectKeyFromObject(llv), written)).To(Succeed())

		changed, err := llvClient.SetDeletingIfNeeded(ctx, stale, message)
		Expect(err).NotTo(HaveOccurred())
		Expect(changed).To(BeFalse())

		again := &v1alpha1.LVMLogicalVolume{}
		Expect(fakeClient.Get(ctx, client.ObjectKeyFromObject(llv), again)).To(Succeed())
		Expect(again.ResourceVersion).To(Equal(written.ResourceVersion))
	})
})
