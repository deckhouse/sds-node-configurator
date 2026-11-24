/*
Copyright 2026 Flant JSC

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

package monitoring

import (
	"github.com/prometheus/client_golang/prometheus"
)

type LVGReadiness struct {
	Name      string
	Node      string
	Condition string
	Reason    string
	Ready     bool
}

type lvgReadyCollector struct {
	desc *prometheus.Desc
	list func() []LVGReadiness
}

func newLVGReadyCollector(list func() []LVGReadiness) *lvgReadyCollector {
	return &lvgReadyCollector{
		desc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "lvm_volume_group_ready"),
			"Whether the Ready condition of an LVMVolumeGroup is True (1) or not (0). condition and reason name the condition that decides it.",
			[]string{"lvm_volume_group", "node", "condition", "reason"},
			nil,
		),
		list: list,
	}
}

func (c *lvgReadyCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

func (c *lvgReadyCollector) Collect(ch chan<- prometheus.Metric) {
	for _, r := range c.list() {
		value := 0.0
		if r.Ready {
			value = 1
		}
		ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, value, r.Name, r.Node, r.Condition, r.Reason)
	}
}

func RegisterLVGReadiness(reg prometheus.Registerer, list func() []LVGReadiness) error {
	return reg.Register(newLVGReadyCollector(list))
}
