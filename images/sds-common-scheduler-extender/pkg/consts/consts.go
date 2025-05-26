package consts

const (
	SdsReplicatedVolumeProvisioner = "replicated.csi.storage.deckhouse.io"
	SdsLocalVolumeProvisioner      = "local.csi.storage.deckhouse.io"

	LvmTypeParamKey         = "csi.storage.deckhouse.io/lvm-type"
	LVMVolumeGroupsParamKey = "csi.storage.deckhouse.io/lvm-volume-groups"

	Thick = "Thick"
	Thin  = "Thin"
)
