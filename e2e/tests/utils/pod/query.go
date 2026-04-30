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
package pod

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func ListPods(ctx context.Context, k8sClient client.Client, options ...client.ListOption) ([]corev1.Pod, error) {
	var podList corev1.PodList
	err := k8sClient.List(ctx, &podList, options...)
	if err != nil {
		return nil, fmt.Errorf("[ListPods] failed to list pods: %w", err)
	}

	return podList.Items, nil
}

func FindRunningPodOnNode(ctx context.Context, k8sClient client.Client, nodeName string, options ...client.ListOption) (corev1.Pod, error) {
	pods, err := ListPods(ctx, k8sClient, options...)
	if err != nil {
		return corev1.Pod{}, fmt.Errorf("[FindRunningPodOnNode] failed to list pods: %w", err)
	}

	pod, findErr := FindFirstRunningPodOnNode(pods, nodeName)
	if findErr != nil {
		return pod, fmt.Errorf("[FindRunningPodOnNode] failed to find running pods: %w", findErr)
	}

	return pod, nil
}
