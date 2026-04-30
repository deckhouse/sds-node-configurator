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

	"k8s.io/api/core/v1"
)

func FindFirstRunningPodOnNode(pods []v1.Pod, nodeName string) (*v1.Pod, error) {
	for i := range pods {
		pod := &pods[i]
		if pod.Spec.NodeName != nodeName ||
			pod.DeletionTimestamp != nil ||
			pod.Status.Phase != v1.PodRunning {
			continue
		}

		return pod, nil
	}
	return nil, fmt.Errorf("no running pod found on node %s", nodeName)
}
