/*
Copyright 2025 Flant JSC

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

package scheduler

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	v12 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/consts"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/logger"
)

func TestFilter(t *testing.T) {
	log := logger.Logger{}
	ctx := context.Background()
	t.Run("getManagedPVCsFromPod filters PVCs by provisioner", func(t *testing.T) {
		sc1 := "sc1"
		sc2 := "sc2"
		sc3 := "sc3"

		objects := []runtime.Object{
			&v1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "first",
					Namespace: "default",
				},
				Spec: v1.PersistentVolumeClaimSpec{
					StorageClassName: &sc1,
				},
			},
			&v1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "second",
					Namespace: "default",
				},
				Spec: v1.PersistentVolumeClaimSpec{
					StorageClassName: &sc2,
				},
			},
			&v1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "third",
					Namespace: "default",
				},
				Spec: v1.PersistentVolumeClaimSpec{
					StorageClassName: &sc3,
				},
			},
			&v12.StorageClass{
				ObjectMeta: metav1.ObjectMeta{
					Name: sc1,
				},
				Provisioner: consts.SdsLocalVolumeProvisioner,
			},
			&v12.StorageClass{
				ObjectMeta: metav1.ObjectMeta{
					Name: sc2,
				},
				Provisioner: consts.SdsLocalVolumeProvisioner,
			},
			&v12.StorageClass{
				ObjectMeta: metav1.ObjectMeta{
					Name: sc3,
				},
			},
		}

		pod := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-pod",
				Namespace: "default",
			},
			Spec: v1.PodSpec{
				Volumes: []v1.Volume{
					{
						Name: "volume1",
						VolumeSource: v1.VolumeSource{
							PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
								ClaimName: "first",
							},
						},
					},
					{
						Name: "volume2",
						VolumeSource: v1.VolumeSource{
							PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
								ClaimName: "second",
							},
						},
					},
					{
						Name: "volume3",
						VolumeSource: v1.VolumeSource{
							PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
								ClaimName: "third",
							},
						},
					},
				},
			},
		}

		s := scheme.Scheme
		_ = v1.AddToScheme(s)
		_ = v12.AddToScheme(s)

		cl := fake.NewFakeClient(objects...)
		targetProvisioners := []string{consts.SdsLocalVolumeProvisioner}
		filtered, err := getManagedPVCsFromPod(ctx, cl, log, pod, targetProvisioners)

		assert.NoError(t, err)
		if assert.Equal(t, 2, len(filtered)) {
			_, ok := filtered["first"]
			assert.True(t, ok)
			_, ok = filtered["second"]
			assert.True(t, ok)
			_, ok = filtered["third"]
			assert.False(t, ok)
		}
	})
}

func Test_scheduler_filter(t *testing.T) {
	tests := []struct {
		name string // description of this test case
		// Named input parameters for target function.
		w http.ResponseWriter
		r *http.Request
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(_ *testing.T) {
			// TODO: construct the receiver type.
			var s scheduler
			s.filter(tt.w, tt.r)
		})
	}
}
