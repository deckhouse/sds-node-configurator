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

package scheduler

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/logger"
)

const (
	annotationBetaStorageProvisioner = "volume.beta.kubernetes.io/storage-provisioner"
	annotationStorageProvisioner     = "volume.kubernetes.io/storage-provisioner"
)

func shouldProcessPod(ctx context.Context, cl client.Client, log logger.Logger, pod *corev1.Pod, targetProvisioner string) (bool, error) {
	log.Trace(fmt.Sprintf("[ShouldProcessPod] targetProvisioner=%s, pod: %+v", targetProvisioner, pod))
	var discoveredProvisioner string

	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim != nil {
			log.Trace(fmt.Sprintf("[ShouldProcessPod] process volume: %+v that has pvc: %+v", volume, volume.PersistentVolumeClaim))
			pvcName := volume.PersistentVolumeClaim.ClaimName
			pvc := &corev1.PersistentVolumeClaim{}
			err := cl.Get(ctx, client.ObjectKey{Namespace: pod.Namespace, Name: pvcName}, pvc)
			if err != nil {
				return false, fmt.Errorf("[ShouldProcessPod] error getting PVC %s: %v", pvcName, err)
			}

			log.Trace(fmt.Sprintf("[ShouldProcessPod] get pvc: %+v", pvc))
			log.Trace(fmt.Sprintf("[ShouldProcessPod] check provisioner in pvc annotations: %+v", pvc.Annotations))

			discoveredProvisioner = pvc.Annotations[annotationStorageProvisioner]
			if discoveredProvisioner != "" {
				log.Trace(fmt.Sprintf("[ShouldProcessPod] discovered provisioner in pvc annotations: %s", discoveredProvisioner))
			} else {
				discoveredProvisioner = pvc.Annotations[annotationBetaStorageProvisioner]
				log.Trace(fmt.Sprintf("[ShouldProcessPod] discovered provisioner in beta pvc annotations: %s", discoveredProvisioner))
			}

			if discoveredProvisioner == "" && pvc.Spec.StorageClassName != nil && *pvc.Spec.StorageClassName != "" {
				log.Trace(fmt.Sprintf("[ShouldProcessPod] can't find provisioner in pvc annotations, check in storageClass with name: %s", *pvc.Spec.StorageClassName))
				storageClass := &storagev1.StorageClass{}
				err = cl.Get(ctx, client.ObjectKey{Name: *pvc.Spec.StorageClassName}, storageClass)
				if err != nil {
					return false, fmt.Errorf("[ShouldProcessPod] error getting StorageClass %s: %v", *pvc.Spec.StorageClassName, err)
				}
				discoveredProvisioner = storageClass.Provisioner
				log.Trace(fmt.Sprintf("[ShouldProcessPod] discover provisioner %s in storageClass: %+v", discoveredProvisioner, storageClass))
			}

			if discoveredProvisioner == "" && pvc.Spec.VolumeName != "" {
				log.Trace(fmt.Sprintf("[ShouldProcessPod] can't find provisioner in pvc annotations and StorageClass, check in PV with name: %s", pvc.Spec.VolumeName))
				pv := &corev1.PersistentVolume{}
				err := cl.Get(ctx, client.ObjectKey{Name: pvc.Spec.VolumeName}, pv)
				if err != nil {
					return false, fmt.Errorf("[ShouldProcessPod] error getting PV %s: %v", pvc.Spec.VolumeName, err)
				}

				if pv.Spec.CSI != nil {
					discoveredProvisioner = pv.Spec.CSI.Driver
				}

				log.Trace(fmt.Sprintf("[ShouldProcessPod] discover provisioner %s in PV: %+v", discoveredProvisioner, pv))
			}

			log.Trace(fmt.Sprintf("[ShouldProcessPod] discovered provisioner: %s", discoveredProvisioner))
			if discoveredProvisioner == targetProvisioner {
				log.Trace(fmt.Sprintf("[ShouldProcessPod] provisioner matches targetProvisioner %s. Pod: %s/%s", pod.Namespace, pod.Name, targetProvisioner))
				return true, nil
			}
			log.Trace(fmt.Sprintf("[ShouldProcessPod] provisioner %s doesn't match targetProvisioner %s. Skip volume %s.", discoveredProvisioner, targetProvisioner, volume.Name))
		}
	}
	log.Trace(fmt.Sprintf("[ShouldProcessPod] can't find targetProvisioner %s in pod volumes. Skip pod: %s/%s", targetProvisioner, pod.Namespace, pod.Name))
	return false, nil
}

func getNodeNames(inputData ExtenderArgs) ([]string, error) {
	if inputData.NodeNames != nil && len(*inputData.NodeNames) > 0 {
		return *inputData.NodeNames, nil
	}

	if inputData.Nodes != nil && len(inputData.Nodes.Items) > 0 {
		nodeNames := make([]string, 0, len(inputData.Nodes.Items))
		for _, node := range inputData.Nodes.Items {
			nodeNames = append(nodeNames, node.Name)
		}
		return nodeNames, nil
	}

	return nil, fmt.Errorf("no nodes provided")
}
