package blockdev

const (
	DRBDName             = "/dev/drbd"
	LoopDeviceType       = "loop"
	MachineID            = "machine-ID"
	AvailableBlockDevice = "available_block_device"
)

var (
	lsblkCommand = []string{"lsblk", "-J", "-lpf", "-no", "name,MOUNTPOINT,PARTUUID,HOTPLUG,MODEL,SERIAL,SIZE,TYPE,WWN,KNAME,PKNAME,FSTYPE"}
)
