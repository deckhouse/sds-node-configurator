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

package utils

import (
	"context"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/monitoring"
)

type BlockDeviceFilterClient struct {
	cl      client.Client
	metrics monitoring.Metrics
}

func NewBlockDeviceFilterClient(cl client.Client, metrics monitoring.Metrics) *BlockDeviceFilterClient {
	return &BlockDeviceFilterClient{
		cl:      cl,
		metrics: metrics,
	}
}

// GetAPIBlockDevices returns map of BlockDevice resources with BlockDevice as a key. You might specify a selector to get a subset or
// leave it as nil to get all the resources.
func (c *BlockDeviceFilterClient) GetAPIBlockDeviceFilters(
	ctx context.Context,
	controllerName string,
) (labels.Selector, error) {
	list := &v1alpha1.BlockDeviceFilterList{}
	start := time.Now()
	err := c.cl.List(ctx, list, &client.ListOptions{LabelSelector: nil})
	c.metrics.APIMethodsDuration(controllerName, "list").Observe(c.metrics.GetEstimatedTimeInSeconds(start))
	c.metrics.APIMethodsExecutionCount(controllerName, "list").Inc()
	if err != nil {
		c.metrics.APIMethodsErrors(controllerName, "list").Inc()
		return nil, err
	}

	result := labels.NewSelector()
	for _, item := range list.Items {
		selector, err := metav1.LabelSelectorAsSelector(item.Spec.BlockDeviceSelector)
		if err != nil {
			return nil, fmt.Errorf("parsing selector %s: %w", item.Name, err)
		}

		requirements, _ := selector.Requirements()
		result = result.Add(requirements...)
	}

	return result, nil
}
