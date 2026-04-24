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

	// LocalStorageTypeParamKey identifies the backing storage type of a
	// LocalStorageClass-derived StorageClass. The LSC controller in
	// sds-local-volume sets it to either "lvm" or "rawfile".
	LocalStorageTypeParamKey = "local.csi.storage.deckhouse.io/type"

	// LocalStorageTypeRawFile marks a StorageClass produced by a
	// LocalStorageClass with `spec.rawFile` (loop-device-backed) configuration.
	// Such StorageClasses MUST NOT be processed by the LVM-aware code paths of
	// this extender: they have no `lvm-type` / `lvm-volume-groups` parameters
	// and node placement is enforced by `allowedTopologies` on the StorageClass
	// itself.
	LocalStorageTypeRawFile = "rawfile"

	Thick = "Thick"
	Thin  = "Thin"
)
