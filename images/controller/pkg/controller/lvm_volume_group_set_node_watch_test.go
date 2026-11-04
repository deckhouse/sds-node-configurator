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

package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
)

func TestLVGSetRequestsForNodeLabels(t *testing.T) {
	ctx := context.Background()
	cl := newFakeClientWithLVGSetStatus()
	for name, selector := range map[string]*metav1.LabelSelector{
		"nvme":  {MatchLabels: map[string]string{"storage.example.com/local-nvme": "true"}},
		"hdd":   {MatchLabels: map[string]string{"storage.example.com/hdd": "true"}},
		"every": nil,
	} {
		require.NoError(t, cl.Create(ctx, &v1alpha1.LVMVolumeGroupSet{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec:       v1alpha1.LVMVolumeGroupSetSpec{NodeSelector: selector},
		}))
	}

	names := func(requests []reconcile.Request) []string {
		var result []string
		for _, r := range requests {
			result = append(result, r.Name)
		}
		return result
	}

	t.Run("a node with the label of one set", func(t *testing.T) {
		got := lvgSetRequestsForNodeLabels(ctx, cl, logger.NewNop(), map[string]string{"storage.example.com/local-nvme": "true"})
		assert.ElementsMatch(t, []string{"nvme", "every"}, names(got))
	})

	t.Run("a node that lost the label of a set", func(t *testing.T) {
		got := lvgSetRequestsForNodeLabels(ctx, cl, logger.NewNop(),
			map[string]string{"storage.example.com/hdd": "true"},
			map[string]string{})
		assert.ElementsMatch(t, []string{"hdd", "every"}, names(got))
	})

	t.Run("a node matching a set by both old and new labels is requested once", func(t *testing.T) {
		labels := map[string]string{"storage.example.com/local-nvme": "true"}
		got := lvgSetRequestsForNodeLabels(ctx, cl, logger.NewNop(), labels, labels)
		assert.ElementsMatch(t, []string{"nvme", "every"}, names(got))
	})
}
