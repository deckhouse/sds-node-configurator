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
	snc "github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/consts"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/logger"
	"github.com/stretchr/testify/assert/yaml"
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

type LVMVolumeGroup struct {
	Name string `yaml:"name"`
	Thin struct {
		PoolName string `yaml:"poolName"`
	} `yaml:"thin"`
}
type LVMVolumeGroups []LVMVolumeGroup

// discoverProvisionerForPVC tries to detect a provisioner for the given PVC using:
// 1) PVC annotations
// 2) StorageClass referenced by the PVC
// 3) PV bound to the PVC
func discoverProvisionerForPVC(
	ctx context.Context,
	cl client.Client,
	log logger.Logger,
	pvc *corev1.PersistentVolumeClaim,
) (string, error) {
	var discoveredProvisioner string

	log.Trace(fmt.Sprintf("[discoverProvisionerForPVC] get pvc: %+v", pvc))
	log.Trace(fmt.Sprintf("[discoverProvisionerForPVC] check provisioner in pvc annotations: %+v", pvc.Annotations))

	// Get provisioner from PVC annotations
	discoveredProvisioner = pvc.Annotations[annotationStorageProvisioner]
	if discoveredProvisioner != "" {
		log.Trace(fmt.Sprintf("[discoverProvisionerForPVC] discovered provisioner in pvc annotations: %s", discoveredProvisioner))
		return discoveredProvisioner, nil
	}

	discoveredProvisioner = pvc.Annotations[annotationBetaStorageProvisioner]
	if discoveredProvisioner != "" {
		log.Trace(fmt.Sprintf("[discoverProvisionerForPVC] discovered provisioner in beta pvc annotations: %s", discoveredProvisioner))
		return discoveredProvisioner, nil
	}

	// Get provisioner from StorageClass
	if pvc.Spec.StorageClassName != nil && *pvc.Spec.StorageClassName != "" {
		log.Trace(fmt.Sprintf("[discoverProvisionerForPVC] can't find provisioner in pvc annotations, check in storageClass with name: %s", *pvc.Spec.StorageClassName))
		storageClass := &storagev1.StorageClass{}
		if err := cl.Get(ctx, client.ObjectKey{Name: *pvc.Spec.StorageClassName}, storageClass); err != nil {
			return "", fmt.Errorf("[discoverProvisionerForPVC] error getting StorageClass %s: %v", *pvc.Spec.StorageClassName, err)
		}
		discoveredProvisioner = storageClass.Provisioner
		log.Trace(fmt.Sprintf("[discoverProvisionerForPVC] discover provisioner %s in storageClass: %+v", discoveredProvisioner, storageClass))
		if discoveredProvisioner != "" {
			return discoveredProvisioner, nil
		}
	}

	// Get provisioner from PV
	if pvc.Spec.VolumeName != "" {
		log.Trace(fmt.Sprintf("[discoverProvisionerForPVC] can't find provisioner in pvc annotations and StorageClass, check in PV with name: %s", pvc.Spec.VolumeName))
		pv := &corev1.PersistentVolume{}
		if err := cl.Get(ctx, client.ObjectKey{Name: pvc.Spec.VolumeName}, pv); err != nil {
			return "", fmt.Errorf("[discoverProvisionerForPVC] error getting PV %s: %v", pvc.Spec.VolumeName, err)
		}

		if pv.Spec.CSI != nil {
			discoveredProvisioner = pv.Spec.CSI.Driver
		}

		log.Trace(fmt.Sprintf("[discoverProvisionerForPVC] discover provisioner %s in PV: %+v", discoveredProvisioner, pv))
	}

	return discoveredProvisioner, nil
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

// Get all PVCs from the Pod which are managed by our modules
//
// Params:
// ctx - context;
// cl - client;
// log - logger;
// pod - Pod;
// targetProvisioners - target provisioners;
//
// Return: map[pvcName]*corev1.PersistentVolumeClaim
func getManagedPVCsFromPod(ctx context.Context, cl client.Client, log logger.Logger, pod *corev1.Pod, targetProvisioners []string) (map[string]*corev1.PersistentVolumeClaim, error) {
	var discoveredProvisioner string
	managedPVCs := make(map[string]*corev1.PersistentVolumeClaim, len(pod.Spec.Volumes))
	var useLinstor *bool
	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim != nil {
			pvcName := volume.PersistentVolumeClaim.ClaimName
			log = log.WithValues("PVC", pvcName)

			pvc := &corev1.PersistentVolumeClaim{}
			err := cl.Get(ctx, client.ObjectKey{Namespace: pod.Namespace, Name: pvcName}, pvc)
			if err != nil {
				return nil, fmt.Errorf("[getManagedPVCsFromPod] error getting PVC: %v", err)
			}

			discoveredProvisioner, err = discoverProvisionerForPVC(ctx, cl, log, pvc)
			if err != nil {
				return nil, fmt.Errorf("[getManagedPVCsFromPod] error getting provisioner: %v", err)
			}
			log.Trace(fmt.Sprintf("[getManagedPVCsFromPod] discovered provisioner: %s", discoveredProvisioner))

			if !slices.Contains(targetProvisioners, discoveredProvisioner) {
				log.Debug(fmt.Sprintf("[getManagedPVCsFromPod] provisioner not matches targetProvisioners %+v", targetProvisioners))
				continue
			}

			if discoveredProvisioner == consts.SdsReplicatedVolumeProvisioner {
				if useLinstor == nil {
					useLinstor, err = getUseLinstor(ctx, cl, log)
					if err != nil {
						return nil, fmt.Errorf("[getManagedPVCsFromPod] error getting useLinstor: %v", err)
					}
				}

				if *useLinstor {
					log.Debug("[getManagedPVCsFromPod] filter out PVC due to used provisioner is managed by the Linstor")
					continue
				}
			}

			log.Debug("[getManagedPVCsFromPod] add PVC to the managed PVCs")
			managedPVCs[pvcName] = pvc
		}
	}

	return managedPVCs, nil
}

// Get all StorageClasses used by the PVCs
//
// Params:
// ctx - context;
// cl - client;
// pvcs - PVCs;
//
// Return: map[scName]*storagev1.StorageClass
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
func getUseLinstor(ctx context.Context, cl client.Client, log logger.Logger) (*bool, error) {
	// local variables to return pointers to
	_true := true
	_false := false
	mc := &d8commonapi.ModuleConfig{}
	err := cl.Get(ctx, client.ObjectKey{Name: "sds-replicated-volume"}, mc)
	if err != nil {
		if client.IgnoreNotFound(err) == nil {
			log.Debug("[getUseLinstor] ModuleConfig sds-replicated-volume not found. Assume useLinstor is true")
			return &_true, nil
		}
		return &_true, err
	}

	if value, exists := mc.Spec.Settings["useLinstor"]; exists && value == true {
		log.Debug("[getUseLinstor] ModuleConfig sds-replicated-volume found. Assume useLinstor is true")
		return &_true, nil
	}

	log.Debug("[getUseLinstor] ModuleConfig sds-replicated-volume found. Assume useLinstor is false")
	return &_false, nil
}

// extractRequestedSize extracts the requested size from the PVC based on the PVC status phase and the StorageClass parameters.
//
// Return: map[pvcName]PVCRequest
// Example:
//
//	{
//	  "pvc1": {
//	    "deviceType": "Thick",
//	    "requestedSize": 100
//	  }
//	}
//	{
//	  "pvc2": {
//	    "deviceType": "Thin",
//	    "requestedSize": 200
//	  }
//	}
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

	return pvcRequests, nil
}

// Get LVMVolumeGroups from StorageClasses
//
// Return: map[scName]LVMVolumeGroups
func GetLVGsFromStorageClasses(scs map[string]*storagev1.StorageClass) (map[string]LVMVolumeGroups, error) {
	result := make(map[string]LVMVolumeGroups, len(scs))

	for _, sc := range scs {
		lvgs, err := ExtractLVGsFromSC(sc)
		if err != nil {
			return nil, err
		}

		result[sc.Name] = append(result[sc.Name], lvgs...)
	}

	return result, nil
}

// Extract LVMVolumeGroups from StorageClass
func ExtractLVGsFromSC(sc *storagev1.StorageClass) (LVMVolumeGroups, error) {
	var lvmVolumeGroups LVMVolumeGroups
	err := yaml.Unmarshal([]byte(sc.Parameters[consts.LVMVolumeGroupsParamKey]), &lvmVolumeGroups)
	if err != nil {
		return nil, err
	}
	return lvmVolumeGroups, nil
}

// Remove LVMVolumeGroups, which are not used in StorageClasses
//
// Params:
// lvgs - all LVMVolumeGroups in the cache;
// scsLVGs - LVMVolumeGroups for each StorageClass
//
// Return: map[lvgName]*snc.LVMVolumeGroup
func RemoveUnusedLVGs(lvgs map[string]*snc.LVMVolumeGroup, scsLVGs map[string]LVMVolumeGroups) map[string]*snc.LVMVolumeGroup {
	result := make(map[string]*snc.LVMVolumeGroup, len(lvgs))
	usedLvgs := make(map[string]struct{}, len(lvgs))

	for _, scLvgs := range scsLVGs {
		for _, lvg := range scLvgs {
			usedLvgs[lvg.Name] = struct{}{}
		}
	}

	for _, lvg := range lvgs {
		if _, used := usedLvgs[lvg.Name]; used {
			result[lvg.Name] = lvg
		}
	}

	return result
}

// Params:
// lvgs - LVMVolumeGroups;
//
// Return: map[nodeName][]*snc.LVMVolumeGroup
func LVMVolumeGroupsByNodeName(lvgs map[string]*snc.LVMVolumeGroup) map[string][]*snc.LVMVolumeGroup {
	sorted := make(map[string][]*snc.LVMVolumeGroup, len(lvgs))
	for _, lvg := range lvgs {
		for _, node := range lvg.Status.Nodes {
			sorted[node.Name] = append(sorted[node.Name], lvg)
		}
	}

	return sorted
}

// Params:
// nodeLVGs - LVMVolumeGroups on the node;
// scLVGs - LVMVolumeGroups for the Storage Class;
//
// Return: *LVMVolumeGroup
// Example:
//
//	{
//	  "name": "vg0",
//	  "status": {
//	    "nodes": ["node1", "node2"],
//	  },
//	}
func findMatchedLVG(nodeLVGs []*snc.LVMVolumeGroup, scLVGs LVMVolumeGroups) *LVMVolumeGroup {
	nodeLVGNames := make(map[string]struct{}, len(nodeLVGs))
	for _, lvg := range nodeLVGs {
		nodeLVGNames[lvg.Name] = struct{}{}
	}

	for _, lvg := range scLVGs {
		if _, match := nodeLVGNames[lvg.Name]; match {
			return &lvg
		}
	}

	return nil
}

// Params:
// thinPools - ThinPools of the LVMVolumeGroup;
// name - name of the ThinPool to find;
//
// Return: *snc.LVMVolumeGroupThinPoolStatus
// Example:
//
//	{
//	  "name": "tp0",
//	  "availableSpace": 100,
//	}
func findMatchedThinPool(thinPools []snc.LVMVolumeGroupThinPoolStatus, name string) *snc.LVMVolumeGroupThinPoolStatus {
	for _, tp := range thinPools {
		if tp.Name == name {
			return &tp
		}
	}

	return nil
}
