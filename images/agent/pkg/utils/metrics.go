package utils

import (
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/klog"
	"net/http"
	"storage-configurator/pkg/controller"
)

const (
	NameSpaceMetrics = "cs"
)

func NewDeviceMetrics() prometheus.GaugeVec {

	f := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:      controller.AvailableBlockDevice,
		Namespace: NameSpaceMetrics,
	}, []string{"device"})

	prometheus.MustRegister(f)
	return *f
}
func Health(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, err := fmt.Fprintf(w, "ok")
	if err != nil {
		klog.Error(err)
	}
}
