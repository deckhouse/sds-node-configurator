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

package consts

const (
	SdsLocalVolumeProvisioner      = "local.csi.storage.deckhouse.io"
	SdsReplicatedVolumeProvisioner = "replicated.csi.storage.deckhouse.io"

	LvmTypeParamKey         = "local.csi.storage.deckhouse.io/lvm-type"
	LVMVolumeGroupsParamKey = "local.csi.storage.deckhouse.io/lvm-volume-groups"

	// PodExtraPVCsAnnotation lists, as a comma-separated value, the names of
	// PersistentVolumeClaims a Pod needs taken into account when scheduling but
	// does not mount itself (e.g. a KubeVirt hotplug disk, which is attached by a
	// separate attachment Pod pinned to the launcher's node). The PVCs are always
	// looked up in the Pod's own namespace. It is a best-effort scheduling hint:
	// unknown or foreign-provisioner names are ignored.
	PodExtraPVCsAnnotation = "scheduler.deckhouse.io/extra-pvcs"

	Thick = "Thick"
	Thin  = "Thin"
)
