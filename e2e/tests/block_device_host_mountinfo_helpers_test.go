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
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestParseHexDeviceID(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		in      string
		want    string
		wantErr bool
	}{
		"simple":        {in: "fd 10\n", want: "253:16"},
		"zero major":    {in: "0 a\n", want: "0:10"},
		"trailing junk": {in: "warn: foo\nfd 10\n", want: "253:16"},
		"empty":         {in: "", wantErr: true},
		"whitespace":    {in: "   \n", wantErr: true},
		"one field":     {in: "fd\n", wantErr: true},
		"not hex":       {in: "zz 10\n", wantErr: true},
		"three fields":  {in: "fd 10 extra\n", wantErr: true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			got, err := parseHexDeviceID(tc.in)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestMountInfoContains(t *testing.T) {
	t.Parallel()
	line := "36 35 253:16 / /mnt/e2e-test rw,relatime shared:1 - ext4 /dev/vdb rw"
	assert.True(t, mountInfoContains(line, "253:16", "/mnt/e2e-test"))
	assert.False(t, mountInfoContains(line, "253:16", "/mnt/other"))
	assert.False(t, mountInfoContains(line, "1:2", "/mnt/e2e-test"))
	assert.False(t, mountInfoContains("", "253:16", "/mnt/e2e-test"))
	assert.False(t, mountInfoContains("short line", "253:16", "/mnt/e2e-test"))
}

func TestShellQuote(t *testing.T) {
	t.Parallel()
	assert.Equal(t, `'foo'`, shellQuote("foo"))
	assert.Equal(t, `'/dev/vdb'`, shellQuote("/dev/vdb"))
	assert.Equal(t, `'foo'"'"'bar'`, shellQuote("foo'bar"))
	assert.Equal(t, `''`, shellQuote(""))
}

func TestAgentContainerID(t *testing.T) {
	t.Parallel()
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "agent"},
		Status: v1.PodStatus{
			ContainerStatuses: []v1.ContainerStatus{
				{Name: "other", ContainerID: "containerd://aaa"},
				{Name: "sds-node-configurator-agent", ContainerID: "containerd://abc123def"},
			},
		},
	}
	got, err := agentContainerID(pod, "sds-node-configurator-agent")
	require.NoError(t, err)
	assert.Equal(t, "abc123def", got)

	_, err = agentContainerID(pod, "missing")
	require.Error(t, err)

	pod.Status.ContainerStatuses[1].ContainerID = ""
	_, err = agentContainerID(pod, "sds-node-configurator-agent")
	require.Error(t, err)
}
