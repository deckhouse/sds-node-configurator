package blockdev

const (
	DRBDName = "/dev/drbd"
	AppName  = "storage-configurator"
)

var (
	lsblkCommand = []string{"lsblk", "-J", "-lpf", "-no", "name,MOUNTPOINT,PARTUUID,HOTPLUG,MODEL,SERIAL,SIZE,TYPE,WWN,KNAME,PKNAME"}
)
