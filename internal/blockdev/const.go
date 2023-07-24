package blockdev

const (
	DRBDName       = "/dev/drbd"
	LoopDeviceType = "loop"
	MachineID      = "machine-ID"
)

var (
	lsblkCommand = []string{"lsblk", "-J", "-lpf", "-no", "name,MOUNTPOINT,PARTUUID,HOTPLUG,MODEL,SERIAL,SIZE,TYPE,WWN,KNAME,PKNAME,FSTYPE"}
)
