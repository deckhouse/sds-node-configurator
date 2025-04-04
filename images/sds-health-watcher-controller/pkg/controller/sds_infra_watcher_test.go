package controller

import (
	"context"
	"testing"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/sds-health-watcher-controller/pkg/monitoring"
	"github.com/stretchr/testify/assert"
	coreV1 "k8s.io/api/core/v1"
	v1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/strings/slices"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestHealthWatcher(t *testing.T) {
	cl := NewFakeClient()
	ctx := context.Background()
	metrics := monitoring.GetMetrics("")

	t.Run("getNodesByNames", func(t *testing.T) {
		t.Run("returns_correct_used_nodes", func(t *testing.T) {
			lvgNodeNames := []string{"test-node1", "test-node2"}
			nodes := coreV1.NodeList{
				Items: []coreV1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "test-node1",
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "test-node2",
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "test-node3",
						},
					},
				},
			}

			for _, n := range nodes.Items {
				err := cl.Create(ctx, &n)
				if err != nil {
					t.Error()
				}
			}

			defer func() {
				for _, n := range nodes.Items {
					err := cl.Delete(ctx, &n)
					if err != nil {
						t.Error(err)
					}
				}
			}()

			usedNodes, missedNodes, err := getNodesByNames(ctx, cl, lvgNodeNames)
			if err != nil {
				t.Error(err)
			}

			if assert.Equal(t, 0, len(missedNodes)) {
				assert.Equal(t, 2, len(usedNodes))

				for _, name := range lvgNodeNames {
					_, ok := usedNodes[name]
					assert.True(t, ok)
				}
			}
		})

		t.Run("returns_correct_missed_nodes", func(t *testing.T) {
			lvgNodeNames := []string{"test-node1", "test-node2"}
			nodes := coreV1.NodeList{
				Items: []coreV1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "test-node1",
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "test-node4",
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "test-node3",
						},
					},
				},
			}

			for _, n := range nodes.Items {
				err := cl.Create(ctx, &n)
				if err != nil {
					t.Error()
				}
			}

			defer func() {
				for _, n := range nodes.Items {
					err := cl.Delete(ctx, &n)
					if err != nil {
						t.Error(err)
					}
				}
			}()

			expectedMissed := []string{"test-node2"}

			_, missedNodes, err := getNodesByNames(ctx, cl, lvgNodeNames)
			if err != nil {
				t.Error(err)
			}

			assert.ElementsMatch(t, expectedMissed, missedNodes)
		})
	})

	t.Run("GetLVMVolumeGroups_returns_lvgs", func(t *testing.T) {
		lvgsToCreate := []v1alpha1.LVMVolumeGroup{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-lvg-1",
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-lvg-2",
				},
			},
		}

		var err error
		for _, lvg := range lvgsToCreate {
			err = cl.Create(ctx, &lvg)
			if err != nil {
				t.Error(err)
			}
		}

		lvgs, err := GetLVMVolumeGroups(ctx, cl, metrics)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(lvgs))
	})

	t.Run("getNodeNamesFromLVGs_returns_correct", func(t *testing.T) {
		const (
			node1 = "node1"
			node2 = "node2"
		)
		lvgs := map[string]v1alpha1.LVMVolumeGroup{
			"first": {
				Status: v1alpha1.LVMVolumeGroupStatus{
					Nodes: []v1alpha1.LVMVolumeGroupNode{
						{
							Name: node1,
						},
					},
				},
			},
			"second": {
				Status: v1alpha1.LVMVolumeGroupStatus{
					Nodes: []v1alpha1.LVMVolumeGroupNode{
						{
							Name: node2,
						},
					},
				},
			},
		}

		names := getNodeNamesFromLVGs(lvgs)
		if assert.Equal(t, 2, len(names)) {
			assert.True(t, slices.Contains(names, node1))
			assert.True(t, slices.Contains(names, node2))
		}
	})

	t.Run("findLVMVolumeGroupsByNodeNames", func(t *testing.T) {
		const (
			node1 = "node1"
			node2 = "node2"
		)
		lvgs := map[string]v1alpha1.LVMVolumeGroup{
			"first": {
				ObjectMeta: metav1.ObjectMeta{
					Name: "first",
				},
				Status: v1alpha1.LVMVolumeGroupStatus{
					Nodes: []v1alpha1.LVMVolumeGroupNode{
						{
							Name: node1,
						},
					},
				},
			},
			"second": {
				ObjectMeta: metav1.ObjectMeta{
					Name: "second",
				},
				Status: v1alpha1.LVMVolumeGroupStatus{
					Nodes: []v1alpha1.LVMVolumeGroupNode{
						{
							Name: node2,
						},
					},
				},
			},
		}

		actual := findLVMVolumeGroupsByNodeNames(lvgs, []string{node1})
		if assert.Equal(t, 1, len(actual)) {
			lvg, exist := actual["first"]

			if assert.True(t, exist) {
				assert.Equal(t, lvg.Status.Nodes[0].Name, node1)
			}
		}
	})
}

func NewFakeClient() client.WithWatch {
	s := scheme.Scheme
	_ = metav1.AddMetaToScheme(s)
	_ = v1alpha1.AddToScheme(s)
	_ = v1.AddToScheme(s)

	builder := fake.NewClientBuilder().WithScheme(s)

	cl := builder.Build()
	return cl
}
