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
	"testing"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/logger"
)

func TestShouldProcessPod(t *testing.T) {
	log := logger.Logger{}
	ctx := context.Background()

	tt := []struct {
		name                  string
		pod                   *corev1.Pod
		objects               []runtime.Object
		targetProvisioner     string
		expectedShouldProcess bool
		expectedError         bool
	}{
		{
			name: "Provisioner in PVC annotations",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pod1",
					Namespace: "default",
				},
				Spec: corev1.PodSpec{
					Volumes: []corev1.Volume{
						{
							Name: "volume1",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: "pvc1",
								},
							},
						},
					},
				},
			},
			objects: []runtime.Object{
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pvc1",
						Namespace: "default",
						Annotations: map[string]string{
							"volume.beta.kubernetes.io/storage-provisioner": "my-provisioner",
						},
					},
				},
			},
			targetProvisioner:     "my-provisioner",
			expectedShouldProcess: true,
		},
		{
			name: "Provisioner in StorageClass",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pod2",
					Namespace: "default",
				},
				Spec: corev1.PodSpec{
					Volumes: []corev1.Volume{
						{
							Name: "volume2",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: "pvc2",
								},
							},
						},
					},
				},
			},
			objects: []runtime.Object{
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pvc2",
						Namespace: "default",
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						StorageClassName: stringPtr("sc2"),
					},
				},
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: "sc2",
					},
					Provisioner: "my-provisioner",
				},
			},
			targetProvisioner:     "my-provisioner",
			expectedShouldProcess: true,
		},
		{
			name: "Provisioner in PV",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pod3",
					Namespace: "default",
				},
				Spec: corev1.PodSpec{
					Volumes: []corev1.Volume{
						{
							Name: "volume3",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: "pvc3",
								},
							},
						},
					},
				},
			},
			objects: []runtime.Object{
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pvc3",
						Namespace: "default",
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						VolumeName: "pv3",
					},
				},
				&corev1.PersistentVolume{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pv3",
					},
					Spec: corev1.PersistentVolumeSpec{
						PersistentVolumeSource: corev1.PersistentVolumeSource{
							CSI: &corev1.CSIPersistentVolumeSource{
								Driver: "my-provisioner",
							},
						},
					},
				},
			},
			targetProvisioner:     "my-provisioner",
			expectedShouldProcess: true,
		},
		{
			name: "No matching Provisioner",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pod4",
					Namespace: "default",
				},
				Spec: corev1.PodSpec{
					Volumes: []corev1.Volume{
						{
							Name: "volume4",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: "pvc4",
								},
							},
						},
					},
				},
			},
			objects: []runtime.Object{
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pvc4",
						Namespace: "default",
					},
				},
			},
			targetProvisioner:     "my-provisioner",
			expectedShouldProcess: false,
		},
		{
			name: "Error getting PVC",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pod5",
					Namespace: "default",
				},
				Spec: corev1.PodSpec{
					Volumes: []corev1.Volume{
						{
							Name: "volume5",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: "nonexistent-pvc",
								},
							},
						},
					},
				},
			},
			objects:               []runtime.Object{},
			targetProvisioner:     "my-provisioner",
			expectedShouldProcess: false,
			expectedError:         true,
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			s := scheme.Scheme
			_ = corev1.AddToScheme(s)
			_ = storagev1.AddToScheme(s)

			cl := fake.NewFakeClient(tc.objects...)
			shouldProcess, err := shouldProcessPod(ctx, cl, log, tc.pod, tc.targetProvisioner)
			if (err != nil) != tc.expectedError {
				t.Fatalf("Unexpected error: %v", err)
			}

			if shouldProcess != tc.expectedShouldProcess {
				t.Errorf("Expected shouldProcess to be %v, but got %v", tc.expectedShouldProcess, shouldProcess)
			}
		})
	}
}

func stringPtr(s string) *string {
	return &s
}
