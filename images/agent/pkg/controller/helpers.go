package controller

import (
	"storage-configurator/api/v1alpha1"
)

func CheckResourceDeprecated(resourceName string, actualResources map[string]struct{}) bool {
	_, ok := actualResources[resourceName]
	return !ok
}

func EqualSpecThinPools(first, second []v1alpha1.SpecThinPool) bool {
	if len(first) != len(second) {
		return false
	}

	for i, firstPool := range first {
		secondPool := second[i]

		if firstPool.Name != secondPool.Name ||
			firstPool.Size != secondPool.Size {
			return false
		}
	}

	return true
}

func EqualStatusThinPools(first, second []v1alpha1.StatusThinPool) bool {
	if len(first) != len(second) {
		return false
	}

	for i, firstPool := range first {
		secondPool := second[i]

		if firstPool.Name != secondPool.Name ||
			firstPool.ActualSize != secondPool.ActualSize {
			return false
		}
	}

	return true
}

func EqualLVMVolumeGroupNodes(first, second []v1alpha1.LvmVolumeGroupNode) bool {
	if len(first) != len(second) {
		return false
	}

	for i, firstNode := range first {
		secondNode := second[i]

		if firstNode.Name != secondNode.Name ||
			!EqualLVMVolumeGroupDevices(firstNode.Devices, secondNode.Devices) {
			return false
		}
	}

	return true
}

func EqualLVMVolumeGroupDevices(first, second []v1alpha1.LvmVolumeGroupDevice) bool {
	if len(first) != len(second) {
		return false
	}

	for i, firstDevice := range first {
		secondDevice := second[i]

		if firstDevice.BlockDevice != secondDevice.BlockDevice ||
			firstDevice.Path != secondDevice.Path ||
			firstDevice.PVSize != secondDevice.PVSize ||
			firstDevice.PVUuid != secondDevice.PVUuid ||
			firstDevice.DevSize != secondDevice.DevSize {
			return false
		}
	}

	return true
}

//func NotEmptyLVMVolumeGroupStatus(status v1alpha1.LvmVolumeGroupStatus) bool {
//	v1alpha1.BlockDeviceStatus{
//		Type:                  "",
//		FsType:                "",
//		NodeName:              "",
//		Consumable:            false,
//		PVUuid:                "",
//		VGUuid:                "",
//		LvmVolumeGroupName:    "",
//		ActualVGNameOnTheNode: "",
//		Wwn:                   "",
//		Serial:                "",
//		Path:                  "",
//		Size:                  "",
//		Model:                 "",
//		Rota:                  false,
//		HotPlug:               false,
//		MachineID:             "",
//	}
//	return status.Type != ""
//}
