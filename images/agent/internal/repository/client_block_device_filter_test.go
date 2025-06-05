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

package repository_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/monitoring"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/repository"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/test_utils"
)

var _ = Describe("BlockDeviceFilterClient", func() {
	var ctx context.Context
	var metrics monitoring.Metrics
	var fakeClient client.WithWatch
	var filterClient *repository.BlockDeviceFilterClient
	controllerName := "testController"

	BeforeEach(func() {
		ctx = context.Background()
		metrics = monitoring.GetMetrics("")
		fakeClient = test_utils.NewFakeClient()
		filterClient = repository.NewBlockDeviceFilterClient(fakeClient, metrics)
		Expect(filterClient).ShouldNot(BeNil())
	})

	When("no filers", func() {
		It("returns empty list", func() {
			selector, err := filterClient.GetAPIBlockDeviceFilters(ctx, controllerName)
			Expect(err).ShouldNot(HaveOccurred())

			requirements, _ := selector.Requirements()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(requirements).Should(BeEmpty())
		})
	})

	When("one requirement", func() {
		testRequirement := metav1.LabelSelectorRequirement{
			Key:      v1alpha1.BlockDeviceWWNLabelKey,
			Operator: metav1.LabelSelectorOpNotIn,
			Values:   []string{"someWWN"},
		}
		JustBeforeEach(func() {
			Expect(fakeClient.Create(ctx, &v1alpha1.BlockDeviceFilter{
				ObjectMeta: metav1.ObjectMeta{
					Name: "block-device-filter",
				},
				Spec: v1alpha1.BlockDeviceFilterSpec{
					BlockDeviceSelector: &metav1.LabelSelector{
						MatchExpressions: []metav1.LabelSelectorRequirement{
							testRequirement,
						},
					},
				},
			})).ShouldNot(HaveOccurred())
		})

		It("returned one label", func() {
			selector, err := filterClient.GetAPIBlockDeviceFilters(ctx, controllerName)
			Expect(err).ShouldNot(HaveOccurred())

			requirements, _ := selector.Requirements()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(requirements).Should(HaveLen(1))
		})
	})
})
