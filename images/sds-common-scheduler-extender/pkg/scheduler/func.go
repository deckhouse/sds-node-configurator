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
	"k8s.io/utils/strings/slices"
	"sigs.k8s.io/controller-runtime/pkg/client"

	d8commonapi "github.com/deckhouse/sds-common-lib/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/consts"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/logger"
)

const (
	annotationBetaStorageProvisioner = "volume.beta.kubernetes.io/storage-provisioner"
	annotationStorageProvisioner     = "volume.kubernetes.io/storage-provisioner"
)

// PVCRequest is a request for a PVC
type PVCRequest struct {
	DeviceType    string
	RequestedSize int64
}

// If the Pod has at least one volume with a provisioner handled by this scheduler extender then return true, otherwise return false.
func shouldProcessPod(ctx context.Context, cl client.Client, log logger.Logger, pod *corev1.Pod, targetProvisioners []string) (bool, error) {
	log.Trace(fmt.Sprintf("[ShouldProcessPod] targetProvisioners=%+v, pod: %+v", targetProvisioners, pod))
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

			// Get provisioner from PVC annotations
			discoveredProvisioner = pvc.Annotations[annotationStorageProvisioner]
			if discoveredProvisioner != "" {
				log.Trace(fmt.Sprintf("[ShouldProcessPod] discovered provisioner in pvc annotations: %s", discoveredProvisioner))
			} else {
				discoveredProvisioner = pvc.Annotations[annotationBetaStorageProvisioner]
				log.Trace(fmt.Sprintf("[ShouldProcessPod] discovered provisioner in beta pvc annotations: %s", discoveredProvisioner))
			}

			// Get provisioner from StorageClass
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

			// Get provisioner from PV
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

			if slices.Contains(targetProvisioners, discoveredProvisioner) {
				log.Trace(fmt.Sprintf("[ShouldProcessPod] provisioner matches targetProvisioners %+v. Pod: %s/%s", targetProvisioners, pod.Namespace, pod.Name))
				return true, nil
			}
			log.Trace(fmt.Sprintf("[ShouldProcessPod] provisioner %s doesn't match targetProvisioners %+v. Skip volume %s.", discoveredProvisioner, targetProvisioners, volume.Name))
		}
	}
	log.Trace(fmt.Sprintf("[ShouldProcessPod] can't find targetProvisioners %+v in pod volumes. Skip pod: %s/%s", targetProvisioners, pod.Namespace, pod.Name))
	return false, nil
}

// Get all node names from the request
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

// Get all PVCs used by the Pod
func getUsedPVC(ctx context.Context, cl client.Client, log logger.Logger, pod *corev1.Pod) (map[string]*corev1.PersistentVolumeClaim, error) {
	pvcMap, err := getAllPVCsFromNamespace(ctx, cl, pod.Namespace)
	if err != nil {
		log.Error(err, fmt.Sprintf("[getUsedPVC] unable to get all PVC for Pod %s in the namespace %s", pod.Name, pod.Namespace))
		return nil, err
	}

	for pvcName := range pvcMap {
		log.Trace(fmt.Sprintf("[getUsedPVC] PVC %s is in namespace %s", pod.Namespace, pvcName))
	}

	usedPvc := make(map[string]*corev1.PersistentVolumeClaim, len(pod.Spec.Volumes))
	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim != nil {
			log.Trace(fmt.Sprintf("[getUsedPVC] Pod %s/%s uses PVC %s", pod.Namespace, pod.Name, volume.PersistentVolumeClaim.ClaimName))
			pvc := pvcMap[volume.PersistentVolumeClaim.ClaimName]
			usedPvc[volume.PersistentVolumeClaim.ClaimName] = &pvc
		}
	}

	return usedPvc, err
}

// Get all PVCs from the namespace
func getAllPVCsFromNamespace(ctx context.Context, cl client.Client, namespace string) (map[string]corev1.PersistentVolumeClaim, error) {
	list := &corev1.PersistentVolumeClaimList{}
	err := cl.List(ctx, list, &client.ListOptions{Namespace: namespace})
	if err != nil {
		return nil, err
	}

	pvcs := make(map[string]corev1.PersistentVolumeClaim, len(list.Items))
	for _, pvc := range list.Items {
		pvcs[pvc.Name] = pvc
	}

	return pvcs, nil
}

// Filter out PVCs which are not managed by our modules (i.e. PVCs which are managed by other provisioners)
func filterNotManagedPVC(ctx context.Context, cl client.Client, log logger.Logger, pvcs map[string]*corev1.PersistentVolumeClaim, scs map[string]*storagev1.StorageClass, targetProvisioners []string) (map[string]*corev1.PersistentVolumeClaim, error) {
	useLinstor := true
	var err error
	for _, pvc := range pvcs {
		if scs[*pvc.Spec.StorageClassName].Provisioner == consts.SdsReplicatedVolumeProvisioner {
			useLinstor, err = getUseLinstor(ctx, cl, log)
			if err != nil {
				return nil, err
			}

			break
		}
	}

	filteredPVCs := make(map[string]*corev1.PersistentVolumeClaim, len(pvcs))
	for _, pvc := range pvcs {
		sc := scs[*pvc.Spec.StorageClassName]
		if !slices.Contains(targetProvisioners, sc.Provisioner) {
			log.Debug(fmt.Sprintf("[filterNotManagedPVC] filter out PVC %s/%s due to used StorageClass %s is not managed by our modules", pvc.Name, pvc.Namespace, sc.Name))
			continue
		}

		if useLinstor && sc.Provisioner == consts.SdsReplicatedVolumeProvisioner {
			log.Debug(fmt.Sprintf("[filterNotManagedPVC] filter out PVC %s/%s due to used StorageClass %s is managed by the Linstor", pvc.Name, pvc.Namespace, sc.Name))
			continue
		}

		filteredPVCs[pvc.Name] = pvc
	}

	return filteredPVCs, nil
}

// Get all StorageClasses used by the PVCs
func getStorageClassesUsedByPVCs(ctx context.Context, cl client.Client, pvcs map[string]*corev1.PersistentVolumeClaim) (map[string]*storagev1.StorageClass, error) {
	scs := &storagev1.StorageClassList{}
	err := cl.List(ctx, scs)
	if err != nil {
		return nil, err
	}

	scMap := make(map[string]storagev1.StorageClass, len(scs.Items))
	for _, sc := range scs.Items {
		scMap[sc.Name] = sc
	}

	result := make(map[string]*storagev1.StorageClass, len(pvcs))
	for _, pvc := range pvcs {
		if pvc.Spec.StorageClassName == nil {
			err = fmt.Errorf("no StorageClass specified for PVC %s", pvc.Name)
			return nil, err
		}

		scName := *pvc.Spec.StorageClassName
		if sc, match := scMap[scName]; match {
			result[sc.Name] = &sc
		}
	}

	return result, nil
}

// Get useLinstor value from the sds-replication-volume ModuleConfig
func getUseLinstor(ctx context.Context, cl client.Client, log logger.Logger) (bool, error) {
	mc := &d8commonapi.ModuleConfig{}
	err := cl.Get(ctx, client.ObjectKey{Name: "sds-replicated-volume"}, mc)
	if err != nil {
		if client.IgnoreNotFound(err) == nil {
			log.Debug("[getUseLinstor] ModuleConfig sds-replicated-volume not found. Assume useLinstor is true")
			return true, nil
		}
		return true, err
	}

	if value, exists := mc.Spec.Settings["useLinstor"]; exists && value == true {
		log.Debug("[getUseLinstor] ModuleConfig sds-replicated-volume found. Assume useLinstor is true")
		return true, nil
	}

	log.Debug("[getUseLinstor] ModuleConfig sds-replicated-volume found. Assume useLinstor is false")
	return false, nil
}

// Extract requested size from the PVC
func extractRequestedSize(
	ctx context.Context,
	cl client.Client,
	log logger.Logger,
	pvcs map[string]*corev1.PersistentVolumeClaim,
	scs map[string]*storagev1.StorageClass,
) (map[string]PVCRequest, error) {
	pvcRequests := make(map[string]PVCRequest, len(pvcs))
	for _, pvc := range pvcs {
		sc := scs[*pvc.Spec.StorageClassName]
		log.Debug(fmt.Sprintf("[extractRequestedSize] PVC %s/%s has status phase: %s", pvc.Namespace, pvc.Name, pvc.Status.Phase))

		switch pvc.Status.Phase {
		case corev1.ClaimPending:
			switch sc.Parameters[consts.LvmTypeParamKey] {
			case consts.Thick:
				pvcRequests[pvc.Name] = PVCRequest{
					DeviceType:    consts.Thick,
					RequestedSize: pvc.Spec.Resources.Requests.Storage().Value(),
				}
			case consts.Thin:
				pvcRequests[pvc.Name] = PVCRequest{
					DeviceType:    consts.Thin,
					RequestedSize: pvc.Spec.Resources.Requests.Storage().Value(),
				}
			}

		case corev1.ClaimBound:
			pv := &corev1.PersistentVolume{}
			if err := cl.Get(ctx, client.ObjectKey{Name: pvc.Spec.VolumeName}, pv); err != nil {
				return nil, fmt.Errorf("[extractRequestedSize] error getting PV %s: %v", pvc.Spec.VolumeName, err)
			}
			switch sc.Parameters[consts.LvmTypeParamKey] {
			case consts.Thick:
				pvcRequests[pvc.Name] = PVCRequest{
					DeviceType:    consts.Thick,
					RequestedSize: pvc.Spec.Resources.Requests.Storage().Value() - pv.Spec.Capacity.Storage().Value(),
				}
			case consts.Thin:
				pvcRequests[pvc.Name] = PVCRequest{
					DeviceType:    consts.Thin,
					RequestedSize: pvc.Spec.Resources.Requests.Storage().Value() - pv.Spec.Capacity.Storage().Value(),
				}
			}
		}
	}

	for name, req := range pvcRequests {
		log.Trace(fmt.Sprintf("[extractRequestedSize] pvc %s has requested size: %d, device type: %s", name, req.RequestedSize, req.DeviceType))
	}

	return pvcRequests, nil
}
