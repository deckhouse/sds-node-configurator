package utils

import (
	"context"
	"time"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-node-configurator/images/agent/internal/monitoring"
)

type BDClient struct {
	cl      client.Client
	metrics monitoring.Metrics
}

func NewBDClient(cl client.Client, metrics monitoring.Metrics) *BDClient {
	return &BDClient{
		cl:      cl,
		metrics: metrics,
	}
}

// GetAPIBlockDevices returns map of BlockDevice resources with BlockDevice as a key. You might specify a selector to get a subset or
// leave it as nil to get all the resources.
func (bdCl *BDClient) GetAPIBlockDevices(
	ctx context.Context,
	controllerName string,
	selector *metav1.LabelSelector,
) (map[string]v1alpha1.BlockDevice, error) {
	list := &v1alpha1.BlockDeviceList{}
	s, err := metav1.LabelSelectorAsSelector(selector)
	if err != nil {
		return nil, err
	}
	if s == labels.Nothing() {
		s = nil
	}
	start := time.Now()
	err = bdCl.cl.List(ctx, list, &client.ListOptions{LabelSelector: s})
	bdCl.metrics.APIMethodsDuration(controllerName, "list").Observe(bdCl.metrics.GetEstimatedTimeInSeconds(start))
	bdCl.metrics.APIMethodsExecutionCount(controllerName, "list").Inc()
	if err != nil {
		bdCl.metrics.APIMethodsErrors(controllerName, "list").Inc()
		return nil, err
	}

	result := make(map[string]v1alpha1.BlockDevice, len(list.Items))
	for _, item := range list.Items {
		result[item.Name] = item
	}

	return result, nil
}
