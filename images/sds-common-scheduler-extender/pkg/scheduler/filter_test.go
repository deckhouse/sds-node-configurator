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
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	v12 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/consts"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/logger"
)

func TestFilter(t *testing.T) {
	log := logger.Logger{}
	t.Run("filterNotManagedPVC", func(t *testing.T) {
		sc1 := "sc1"
		sc2 := "sc2"
		sc3 := "sc3"
		scs := map[string]*v12.StorageClass{
			sc1: {
				ObjectMeta: metav1.ObjectMeta{
					Name: sc1,
				},
				Provisioner: consts.SdsLocalVolumeProvisioner,
			},
			sc2: {
				ObjectMeta: metav1.ObjectMeta{
					Name: sc2,
				},
				Provisioner: consts.SdsLocalVolumeProvisioner,
			},
			sc3: {
				ObjectMeta: metav1.ObjectMeta{
					Name: sc3,
				},
			},
		}
		pvcs := map[string]*v1.PersistentVolumeClaim{
			"first": {
				ObjectMeta: metav1.ObjectMeta{
					Name: "first",
				},
				Spec: v1.PersistentVolumeClaimSpec{
					StorageClassName: &sc1,
				},
			},
			"second": {
				ObjectMeta: metav1.ObjectMeta{
					Name: "second",
				},
				Spec: v1.PersistentVolumeClaimSpec{
					StorageClassName: &sc2,
				},
			},
			"third": {
				ObjectMeta: metav1.ObjectMeta{
					Name: "third",
				},
				Spec: v1.PersistentVolumeClaimSpec{
					StorageClassName: &sc3,
				},
			},
		}

		filtered := filterNotManagedPVC(log, pvcs, scs)

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
		t.Run(tt.name, func(t *testing.T) {
			// TODO: construct the receiver type.
			var s scheduler
			s.filter(tt.w, tt.r)
		})
	}
}
