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

package tests

import (
	"testing"

	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestDaemonSetEnvIsTrue(t *testing.T) {
	t.Parallel()

	dsWithEnv := func(container, name, value string) *appsv1.DaemonSet {
		return &appsv1.DaemonSet{
			ObjectMeta: metav1.ObjectMeta{Name: "sds-node-configurator"},
			Spec: appsv1.DaemonSetSpec{
				Template: v1.PodTemplateSpec{
					Spec: v1.PodSpec{
						Containers: []v1.Container{
							{
								Name: container,
								Env: []v1.EnvVar{
									{Name: name, Value: value},
								},
							},
						},
					},
				},
			},
		}
	}

	for name, tc := range map[string]struct {
		ds            *appsv1.DaemonSet
		containerName string
		envName       string
		want          bool
	}{
		"true": {
			ds:            dsWithEnv("sds-node-configurator-agent", "ENABLE_NETLINK_BLOCK_DEVICE_DISCOVERY", "true"),
			containerName: "sds-node-configurator-agent",
			envName:       "ENABLE_NETLINK_BLOCK_DEVICE_DISCOVERY",
			want:          true,
		},
		"false": {
			ds:            dsWithEnv("sds-node-configurator-agent", "ENABLE_NETLINK_BLOCK_DEVICE_DISCOVERY", "false"),
			containerName: "sds-node-configurator-agent",
			envName:       "ENABLE_NETLINK_BLOCK_DEVICE_DISCOVERY",
			want:          false,
		},
		"invalid bool": {
			ds:            dsWithEnv("sds-node-configurator-agent", "ENABLE_NETLINK_BLOCK_DEVICE_DISCOVERY", "yes"),
			containerName: "sds-node-configurator-agent",
			envName:       "ENABLE_NETLINK_BLOCK_DEVICE_DISCOVERY",
			want:          false,
		},
		"missing env": {
			ds:            dsWithEnv("sds-node-configurator-agent", "OTHER", "true"),
			containerName: "sds-node-configurator-agent",
			envName:       "ENABLE_NETLINK_BLOCK_DEVICE_DISCOVERY",
			want:          false,
		},
		"wrong container": {
			ds:            dsWithEnv("other", "ENABLE_NETLINK_BLOCK_DEVICE_DISCOVERY", "true"),
			containerName: "sds-node-configurator-agent",
			envName:       "ENABLE_NETLINK_BLOCK_DEVICE_DISCOVERY",
			want:          false,
		},
		"nil daemonset": {
			ds:            nil,
			containerName: "sds-node-configurator-agent",
			envName:       "ENABLE_NETLINK_BLOCK_DEVICE_DISCOVERY",
			want:          false,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, daemonSetEnvIsTrue(tc.ds, tc.containerName, tc.envName))
		})
	}
}
