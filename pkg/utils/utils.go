package utils

import (
	"fmt"
	"runtime"
	"storage-configurator/pkg/utils/sclogs"

	"k8s.io/klog"
)

func PrintVersion() {
	klog.Info(fmt.Sprintf(sclogs.GoVersion+"%s", runtime.Version()))
	klog.Info(fmt.Sprintf(sclogs.GoArchVersion+" %s/%s", runtime.GOOS, runtime.GOARCH))
}
