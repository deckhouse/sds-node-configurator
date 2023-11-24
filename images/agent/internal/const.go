package internal

const (
	DRBDName                  = "/dev/drbd"
	LoopDeviceType            = "loop"
	LVMDeviceType             = "lvm"
	LVMFSType                 = "LVM2_member"
	AvailableBlockDevice      = "available_block_device"
	SdsNodeConfigurator       = "storage.deckhouse.io/sds-node-configurator"
	LVMVGHealthOperational    = "Operational"
	LVMVGHealthNonOperational = "Nonoperational"
)

var (
	AllowedFSTypes       = [...]string{LVMFSType}
	InvalidDeviceTypes   = [...]string{LoopDeviceType, LVMDeviceType}
	BlockDeviceValidSize = "1G"
	ResizeDelta          = "32Mi"
	Finalizers           = []string{SdsNodeConfigurator}
	LVMTags              = []string{"storage.deckhouse.io/enabled=true", "linstor-"}
)
