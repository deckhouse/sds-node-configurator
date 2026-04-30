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
	"fmt"

	corev1 "k8s.io/api/core/v1"
)

func FindFirstRunningPodOnNode(pods []corev1.Pod, nodeName string) (corev1.Pod, error) {
	for _, pod := range pods {
		if pod.Spec.NodeName != nodeName || pod.DeletionTimestamp != nil {
			continue
		}
		if pod.Status.Phase != corev1.PodRunning {
			continue
		}

		return pod, nil
	}

	return corev1.Pod{}, fmt.Errorf("[FindFirstRunningPodOnNode] failed to find running pod")
}
